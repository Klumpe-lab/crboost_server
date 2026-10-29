"""Where each tomogram of a particle set comes from, and its per-tilt star under the set's name.

A set made across projects records, per tomogram row of its ``tomograms.star``, the tomogram it is: the
project it lives in, its name there and its per-tilt star there (roadmap 40 D24). RELION carries unknown
columns, and the extraction jobs copy the rows they read, so what an extraction of a set writes keeps them.
The extraction drivers check each tomogram against its own project's registry by them, and Warp export finds
the tilt series' Warp files by them.

RELION reads a tomogram's per-tilt star by the block named after it (``data_<rlnTomoName>``). A tomogram
renamed in a set (two projects' ``Position_1``) or named in its source differently from the set gets a copy
of that star, the block renamed, in the set's ``tilt_series/``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import starfile

ORIGIN_PROJECT = "cbOriginProject"
ORIGIN_TOMO = "cbOriginTomoName"
ORIGIN_TILT_STAR = "cbOriginTiltSeriesStarFile"
ORIGIN_COLS = (ORIGIN_PROJECT, ORIGIN_TOMO, ORIGIN_TILT_STAR)
TILT_DIR = "tilt_series"
# Per-tilt columns naming files: a relative one is read from the project the star was written in, so the
# copy names them absolutely.
_TILT_PATH_COLS = (
    "rlnMicrographName",
    "rlnMicrographMovieName",
    "rlnMicrographNameEven",
    "rlnMicrographNameOdd",
    "rlnMicrographMetadata",
    "rlnCtfImage",
    "rlnCtfPowerSpectrum",
)


@dataclass(frozen=True)
class Origin:
    project: str  # absolute
    tomo: str  # its name there
    tilt_star: str  # its per-tilt star there, absolute


def origins_of(tomograms: pd.DataFrame) -> dict[str, Origin] | None:
    """{name in the star: its origin} when the star records origins; None when it records none. A row
    without one, in a star that records them, raises: its tomogram would be looked for in the wrong place."""
    if ORIGIN_PROJECT not in tomograms.columns:
        return None
    out: dict[str, Origin] = {}
    blank = []
    for _, row in tomograms.iterrows():
        name = str(row["rlnTomoName"])
        vals = [row.get(c) for c in ORIGIN_COLS]
        if any(v is None or (isinstance(v, float) and v != v) or not str(v).strip() for v in vals):
            blank.append(name)
            continue
        out[name] = Origin(*(str(v) for v in vals))
    if blank:
        raise ValueError(f"{len(blank)} tomogram rows record no origin ({', '.join(blank[:5])})")
    return out


def tilt_block(path: Path, name: str) -> pd.DataFrame:
    """The per-tilt table a star holds for tomogram `name` (block ``data_<name>``); raises naming the
    blocks it has instead."""
    blocks = starfile.read(path, always_dict=True)
    df = blocks.get(name)
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"{path} has no block data_{name} (it has {', '.join(map(str, blocks)) or 'none'})")
    return df


def write_renamed_tilt_star(src: Path, dst: Path, old: str, new: str, project: Path) -> None:
    """`src`'s block ``data_<old>`` written to `dst` as ``data_<new>``, its relative file names made absolute
    against `project` (the project the star was written in, where RELION read them from)."""
    df = tilt_block(src, old).copy()
    for col in _TILT_PATH_COLS:
        if col in df.columns:
            df[col] = [
                v if not isinstance(v, str) or not v or Path(v).is_absolute() else str(Path(project) / v)
                for v in df[col]
            ]
    dst.parent.mkdir(parents=True, exist_ok=True)
    starfile.write({new: df}, dst, overwrite=True)


def project_of(path: Path, fallback: Path) -> Path:
    """The project a file in a job (or set) directory belongs to: the nearest folder above it holding a
    ``project_params.json``; `fallback` when none does within a few levels."""
    for parent in list(Path(path).parents)[:6]:
        if (parent / "project_params.json").exists():
            return parent
    return Path(fallback)
