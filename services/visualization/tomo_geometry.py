"""One answer to "what is this tomogram's coordinate frame?".

Before the dashboard can draw a pick, cut a tile, or hand a tomogram to ArtiaX it
needs four facts: which ``tomograms.star`` describes it, where its reconstructed
MRC is, its binned dimensions, and its binned pixel size. Those used to be dug out
in three places with three different fallback chains (the recon-MRC resolver, the
preview manifest, and the imported-tomogram renderer's inline star arithmetic).
This module is the single provider — and the single place allowed to answer
"unknown".

**It never invents a pixel size or a dimension.** ``binned_apix is None`` with
``apix_provenance == "missing"`` is a first-class result the UI renders as a red
marker with overlays disabled, because the alternative — standing in 1.0 A/px, or
``[1, 1, 1]`` dims — mis-scales every pick by whatever the true value was and looks
exactly like a correct render.

Precedence
----------
``binned_apix``: ``rlnTomoTiltSeriesPixelSize x rlnTomoTomogramBinning`` from the
star row (``"star"``) -> the recon MRC header's voxel size when it is > 0
(``"mrc_header"``) -> ``None`` (``"missing"``).

``dims_xyz_px``: the recon MRC header whenever the volume is on disk. That is what
:func:`services.visualization.coords.binned_tomo_size_from_tomo_row` reads, and
that function backs the ArtiaX bridge's coordinate transform — so an overlay can
never disagree with the picks it round-trips. Only when the volume is absent do we
fall back to ``rlnTomoSize{X,Y,Z} / rlnTomoTomogramBinning``: those columns hold
the UNBINNED tilt-image size, whose Z is the tilt-image height rather than the
reconstruction's thickness, so the division is a last resort, not a first source.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from services.dashboard_data import find_job_by_type, job_dir_for
from services.models_base import JobType

logger = logging.getLogger(__name__)

SOURCE_RECONSTRUCT = "reconstruct"
SOURCE_IMPORTED = "imported"

APIX_STAR = "star"
APIX_MRC_HEADER = "mrc_header"
APIX_MISSING = "missing"


@dataclass(frozen=True)
class TomoGeometry:
    """One tomogram's coordinate frame, carrying the provenance of its pixel size."""

    tomo_name: str
    tomograms_star: Path  # a star ``artiax_bridge.frame_for_tomo`` will accept
    source: str  # SOURCE_RECONSTRUCT | SOURCE_IMPORTED
    recon_mrc: Path | None  # the binned reconstruction, when it is on disk
    dims_xyz_px: tuple[int, int, int] | None  # binned voxels
    binned_apix: float | None  # None => UNKNOWN; surfaced red, never 1.0
    apix_provenance: str  # APIX_STAR | APIX_MRC_HEADER | APIX_MISSING

    @property
    def is_usable(self) -> bool:
        """Whether picks can be mapped into this frame at all. False => the caller
        shows the missing-geometry marker and draws NO overlay."""
        return self.dims_xyz_px is not None and self.binned_apix is not None and self.binned_apix > 0


# path -> (mtime, parsed value). One entry per file, self-invalidating on mtime.
# Both readers sit on the dashboard render path, which re-runs for every selected
# TS on every refresh; a written star / finished volume does not change under us,
# so the parse is worth caching and the stat is not.
_STAR_CACHE: dict[str, tuple[float, pd.DataFrame | None]] = {}
_HEADER_CACHE: dict[str, tuple[float, tuple[tuple[int, int, int], float] | None]] = {}


def _mtime(p: Path) -> float | None:
    try:
        return p.stat().st_mtime
    except OSError:
        return None  # absent / unreadable — the caller's "unknown" branch handles it


def read_tomo_table(star_path: Path) -> pd.DataFrame | None:
    """The tomogram table of a ``tomograms.star`` (mtime-memoized), or None when the
    file is absent or unparsable. None is not a silent default: every caller turns it
    into a visible "no geometry" state."""
    p = Path(star_path)
    mt = _mtime(p)
    if mt is None:
        return None
    hit = _STAR_CACHE.get(str(p))
    if hit is not None and hit[0] == mt:
        return hit[1]
    df: pd.DataFrame | None = None
    try:
        import starfile

        data = starfile.read(p, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                df = v
                break
    except Exception:
        logger.exception("Could not read tomograms.star %s", p)
    _STAR_CACHE[str(p)] = (mt, df)
    return df


def _read_mrc_header(mrc_path: Path) -> tuple[tuple[int, int, int], float] | None:
    """``((nx, ny, nz), voxel_size_x_ang)`` from an MRC header, or None when the file
    is absent or unreadable. Header-only read, mtime-memoized — a finished volume's
    header is immutable and this sits on the render path."""
    p = Path(mrc_path)
    mt = _mtime(p)
    if mt is None:
        return None
    hit = _HEADER_CACHE.get(str(p))
    if hit is not None and hit[0] == mt:
        return hit[1]
    val: tuple[tuple[int, int, int], float] | None = None
    try:
        import mrcfile

        with mrcfile.open(str(p), header_only=True, mode="r") as m:
            val = ((int(m.header.nx), int(m.header.ny), int(m.header.nz)), float(m.voxel_size.x))
    except Exception:
        logger.exception("Could not read MRC header %s", p)
    _HEADER_CACHE[str(p)] = (mt, val)
    return val


def tomogram_star_sources(project_state, project_path: Path) -> list[tuple[str, Path]]:
    """The ``tomograms.star`` files that can describe this project's tomograms, in
    precedence order: the TS_RECONSTRUCT job's, then the project-level imported star.
    Both can be present (a preprocessing project that also imported volumes) — the
    caller takes the first that actually carries the tilt series it is asking about."""
    out: list[tuple[str, Path]] = []
    rec = find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    if rec:
        job_dir = job_dir_for(project_state, rec[0], rec[1], Path(project_path))
        if job_dir is not None:
            out.append((SOURCE_RECONSTRUCT, job_dir / "tomograms.star"))
    imported = project_state.imported_tomograms_star_path()
    if imported:
        out.append((SOURCE_IMPORTED, Path(imported)))
    return out


def _binning_of(row: pd.Series) -> float:
    """RELION's tomogram binning; absent => 1.0 (RELION's own convention). A corrupt
    non-positive / NaN value also reads as 1.0 rather than poisoning the arithmetic —
    the pixel size it multiplies carries its provenance either way."""
    try:
        b = float(row.get("rlnTomoTomogramBinning", 1.0))
    except (TypeError, ValueError):
        return 1.0
    return b if b > 0 else 1.0


def _star_apix(row: pd.Series, binning: float) -> float | None:
    """Binned recon pixel size from the star row, or None when the column is absent
    or non-positive. Same arithmetic as ``coords.pixel_size_from_tomo_row``, but
    tolerant — that one raises, and here "unset" is a state we render."""
    try:
        ts_px = float(row.get("rlnTomoTiltSeriesPixelSize", 0.0))
    except (TypeError, ValueError):
        return None
    return ts_px * binning if ts_px > 0 else None


def _star_dims(row: pd.Series, binning: float) -> tuple[int, int, int] | None:
    """Binned dims derived from the UNBINNED ``rlnTomoSize{X,Y,Z}``. Fallback only —
    see the module docstring for why the MRC header wins when the volume exists."""
    vals: list[int] = []
    for col in ("rlnTomoSizeX", "rlnTomoSizeY", "rlnTomoSizeZ"):
        if col not in row.index:
            return None
        try:
            vals.append(round(float(row[col]) / binning))
        except (TypeError, ValueError):
            return None
    return (vals[0], vals[1], vals[2]) if all(v > 0 for v in vals) else None


def _recon_path(row: pd.Series, project_path: Path) -> Path | None:
    """The reconstructed volume for this row, when it is actually on disk. Existence
    is re-checked per call (never memoized) so a volume that lands mid-session is
    picked up on the next render."""
    raw = row.get("rlnTomoReconstructedTomogram")
    if not isinstance(raw, str) or not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = Path(project_path) / p
    return p if p.exists() else None


def _geometry_from_row(row: pd.Series, tomo_name: str, star_path: Path, source: str, project_path: Path):
    binning = _binning_of(row)
    recon = _recon_path(row, project_path)
    header = _read_mrc_header(recon) if recon is not None else None

    dims = header[0] if header is not None else _star_dims(row, binning)

    apix = _star_apix(row, binning)
    provenance = APIX_STAR
    if apix is None:
        header_apix = float(header[1]) if header is not None else 0.0
        if header_apix > 0:
            apix, provenance = header_apix, APIX_MRC_HEADER
        else:
            provenance = APIX_MISSING

    return TomoGeometry(
        tomo_name=tomo_name,
        tomograms_star=Path(star_path),
        source=source,
        recon_mrc=recon,
        dims_xyz_px=dims,
        binned_apix=apix,
        apix_provenance=provenance,
    )


def geometry_for_ts(project_state, project_path: Path, ts_name: str) -> TomoGeometry | None:
    """The coordinate frame for one tilt series, or None when no known
    ``tomograms.star`` carries a row for it — i.e. nothing has reconstructed or
    imported this tomogram yet, so there is nothing to pick on."""
    project_path = Path(project_path)
    for source, star_path in tomogram_star_sources(project_state, project_path):
        df = read_tomo_table(star_path)
        if df is None:
            continue
        match = df[df["rlnTomoName"].astype(str) == str(ts_name)]
        if match.empty:
            continue
        return _geometry_from_row(match.iloc[0], str(ts_name), star_path, source, project_path)
    return None
