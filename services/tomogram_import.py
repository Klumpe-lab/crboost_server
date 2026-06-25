"""Tomogram import — a project-level utility (NOT a pipeline job).

Particle-only / data-less projects have no preprocessing pipeline to produce a
``tomograms.star``; this module lets the PARTICLES-header import utility inject one,
either by SYNTHESIZING it from a set of reconstructed ``.mrc`` files (reading dims +
voxel size from each header) or by REFERENCING an existing ``tomograms.star``.

There is deliberately no ``JobType`` here: nothing in the manual-picking flow resolves
the tomograms.star as a job *output* — the dashboard, ArtiaX and per-list optset build
read it directly — so modelling it as a SLURM/IO job bought nothing. The committed star
is recorded on ``ProjectState.imported_tomograms``.

Heavy deps (pandas, starfile, mrcfile) are imported lazily so importing this module
stays light.

Schema mirrors a real tsReconstruct ``data_global`` block (post_handedness_fix oracle):
synthesize writes the 13 picking/TM-relevant columns (omits halfmaps +
``rlnTomoTiltSeriesStarFile``, which are the denoise / TemplateMatch paths).
"""

from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import List, Optional


class TomoImportMode(str, Enum):
    REFERENCE = "reference"
    SYNTHESIZE = "synthesize"


TOMO_IMPORT_REFERENCE = TomoImportMode.REFERENCE.value
TOMO_IMPORT_SYNTHESIZE = TomoImportMode.SYNTHESIZE.value


def probe_mrc_metadata(paths: List[Path]) -> List[dict]:
    """Per-file MRC header metadata for the pre-commit preview (headers only, no data).

    Each entry: ``{name, path, nx, ny, nz, voxel_size, has_voxel_size, error}``. A
    missing voxel size (``has_voxel_size=False``) is surfaced — NOT silently defaulted —
    so the import widget can flag it and let the user supply a pixel size before
    committing (CLAUDE.md 'Surfacing uncertainty')."""
    import mrcfile

    out: List[dict] = []
    for raw in paths:
        p = Path(raw)
        rec = {
            "name": p.name,
            "path": str(p),
            "nx": None,
            "ny": None,
            "nz": None,
            "voxel_size": None,
            "has_voxel_size": False,
            "error": "",
        }
        try:
            with mrcfile.open(str(p), permissive=True, header_only=True) as mrc:
                rec["nx"], rec["ny"], rec["nz"] = int(mrc.header.nx), int(mrc.header.ny), int(mrc.header.nz)
                try:
                    vs = float(mrc.voxel_size.x)
                except Exception:
                    vs = 0.0
                if vs > 0:
                    rec["voxel_size"] = vs
                    rec["has_voxel_size"] = True
        except Exception as e:  # unreadable / not an MRC — report, don't crash the preview
            rec["error"] = str(e)
        out.append(rec)
    return out


def _absolutize(value, base: Path) -> str:
    if not value or not isinstance(value, str):
        return value
    p = Path(value)
    return str(p if p.is_absolute() else (base / p).resolve())


def _safe_tomo_name(stem: str, project_tag: str, seen: set) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", f"{project_tag}_{stem}" if project_tag else stem)
    name = base
    i = 2
    while name in seen:
        name = f"{base}_{i}"
        i += 1
    seen.add(name)
    return name


def _reference_rows(reference_star: str):
    import starfile

    ref = Path(reference_star).expanduser()
    if not ref.is_file():
        raise FileNotFoundError(f"reference tomograms.star not found: {ref}")
    data = starfile.read(ref, always_dict=True)
    df = None
    for block in data.values():
        if hasattr(block, "columns") and "rlnTomoName" in block.columns:
            df = block.copy()
            break
    if df is None:
        raise ValueError(f"no tomogram block (rlnTomoName) found in {ref}")
    base = ref.parent
    for col in (
        "rlnTomoReconstructedTomogram",
        "rlnTomoReconstructedTomogramHalf1",
        "rlnTomoReconstructedTomogramHalf2",
        "rlnTomoTiltSeriesStarFile",
    ):
        if col in df.columns:
            df[col] = df[col].apply(lambda v: _absolutize(v, base))
    return df


def _synthesize_rows(
    mrc_paths: List[Path],
    *,
    pixel_size_angstrom: float,
    tomogram_binning: float,
    optics_group_name: str,
    voltage: float,
    spherical_aberration: float,
    amplitude_contrast: float,
    invert_defocus_hand: bool,
    project_tag: str,
):
    import pandas as pd
    import mrcfile

    paths = sorted(p for p in (Path(x) for x in mrc_paths) if p.is_file())
    if not paths:
        return None

    binning = float(tomogram_binning or 1.0)
    hand = -1 if invert_defocus_hand else 1
    seen: set = set()
    rows = []
    for mrc_path in paths:
        with mrcfile.open(str(mrc_path), permissive=True, header_only=True) as mrc:
            nx, ny, nz = int(mrc.header.nx), int(mrc.header.ny), int(mrc.header.nz)
            try:
                recon_apix = float(mrc.voxel_size.x)
            except Exception:
                recon_apix = 0.0

        # Unbinned tilt-series pixel size: explicit override wins; else derive from the
        # recon voxel size and binning (recon voxel size == ts_px * binning). If neither
        # is available we RAISE rather than silently defaulting to 1.0 Å/px — the import
        # widget's pre-commit preview surfaces a missing voxel_size so the user supplies
        # `pixel_size_angstrom` first (CLAUDE.md 'Surfacing uncertainty').
        if pixel_size_angstrom > 0:
            ts_px = float(pixel_size_angstrom)
        elif recon_apix > 0:
            ts_px = recon_apix / binning
        else:
            raise ValueError(
                f"{mrc_path.name}: MRC header has no voxel size and no pixel-size override was given. "
                "Set the pixel size on the import before committing."
            )

        rows.append(
            {
                "rlnTomoName": _safe_tomo_name(mrc_path.stem, project_tag, seen),
                "rlnVoltage": float(voltage),
                "rlnSphericalAberration": float(spherical_aberration),
                "rlnAmplitudeContrast": float(amplitude_contrast),
                "rlnMicrographOriginalPixelSize": ts_px,
                "rlnTomoHand": hand,
                "rlnOpticsGroupName": optics_group_name,
                # Unbinned tomogram dims (RELION convention) = recon dims * binning.
                "rlnTomoSizeX": int(round(nx * binning)),
                "rlnTomoSizeY": int(round(ny * binning)),
                "rlnTomoSizeZ": int(round(nz * binning)),
                "rlnTomoTiltSeriesPixelSize": ts_px,
                "rlnTomoReconstructedTomogram": str(mrc_path.resolve()),
                "rlnTomoTomogramBinning": binning,
            }
        )
    return pd.DataFrame(rows)


def write_tomograms_star(
    out_path: Path,
    *,
    mode: str = TOMO_IMPORT_SYNTHESIZE,
    mrc_paths: Optional[List[Path]] = None,
    reference_star: str = "",
    pixel_size_angstrom: float = 0.0,
    tomogram_binning: float = 1.0,
    optics_group_name: str = "opticsGroup1",
    voltage: float = 300.0,
    spherical_aberration: float = 2.7,
    amplitude_contrast: float = 0.10,
    invert_defocus_hand: bool = True,
    project_tag: str = "",
) -> int:
    """Write ``out_path`` (a ``tomograms.star``). Returns the tomogram row count.

    ``synthesize`` builds it from ``mrc_paths``; ``reference`` copies an existing
    ``tomograms.star`` (absolutizing its file paths). Raises on no rows / unreadable
    inputs / a missing voxel size with no override (never silently defaults apix)."""
    import starfile

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == TOMO_IMPORT_REFERENCE:
        df = _reference_rows(reference_star)
    else:
        df = _synthesize_rows(
            mrc_paths or [],
            pixel_size_angstrom=pixel_size_angstrom,
            tomogram_binning=tomogram_binning,
            optics_group_name=optics_group_name,
            voltage=voltage,
            spherical_aberration=spherical_aberration,
            amplitude_contrast=amplitude_contrast,
            invert_defocus_hand=invert_defocus_hand,
            project_tag=project_tag,
        )

    if df is None or len(df) == 0:
        raise ValueError(f"tomogram import produced no rows (mode={mode!r})")

    # starfile maps the dict key to the block name -> 'global' becomes data_global.
    starfile.write({"global": df}, out_path, overwrite=True)
    return int(len(df))
