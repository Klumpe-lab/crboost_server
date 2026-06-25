from __future__ import annotations
from enum import Enum
from typing import ClassVar, List, Set
from pathlib import Path
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TomoImportMode(str, Enum):
    REFERENCE = "reference"
    SYNTHESIZE = "synthesize"


# source_mode values (kept as module constants for the inline writer)
TOMO_IMPORT_REFERENCE = TomoImportMode.REFERENCE.value
TOMO_IMPORT_SYNTHESIZE = TomoImportMode.SYNTHESIZE.value


class ImportTomogramsParams(AbstractJobParams):
    """Provider job: supplies reconstructed tomograms to a data-less / particle-only
    project as a ``tomograms.star`` (``TOMOGRAMS_STAR``), so Template Matching and
    manual picking can run without the preprocessing pipeline.

    It is written INLINE at submit time (no SLURM job, no relion binary), riding the
    same pattern ImportMovies uses in ``_submit_chain``. Two modes:

    - ``reference``  — copy an existing ``tomograms.star`` (e.g. another project's
      tsReconstruct output), absolutizing its file paths.
    - ``synthesize`` — build a ``tomograms.star`` from a glob of reconstructed ``.mrc``
      files, reading dims + voxel size from each MRC header.

    See PARTICLE_PROJECT_ROADMAP.md (P1). Schema mirrors a real tsReconstruct
    ``data_global`` block (post_handedness_fix oracle).
    """

    job_type: JobType = Field(default=JobType.IMPORT_TOMOGRAMS)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    RUNS_INLINE: ClassVar[bool] = True

    USER_PARAMS: ClassVar[Set[str]] = {
        "source_mode",
        "reference_star",
        "mrc_glob",
        "pixel_size_angstrom",
        "tomogram_binning",
        "optics_group_name",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = []
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.TOMOGRAMS_STAR, path_template="tomograms.star")
    ]

    source_mode: str = Field(default=TOMO_IMPORT_SYNTHESIZE)
    reference_star: str = Field(default="", description="Path to an existing tomograms.star (reference mode)")
    mrc_glob: str = Field(default="", description="Glob of reconstructed tomogram .mrc files (synthesize mode)")
    # Unbinned tilt-series pixel size (Å). 0 = derive from the MRC voxel size / binning.
    pixel_size_angstrom: float = Field(default=0.0, ge=0.0, le=100.0)
    # Tomogram binning relative to the unbinned tilt series. Default 1 treats the recon
    # MRC as the working frame (self-consistent for picking on that recon).
    tomogram_binning: float = Field(default=1.0, ge=1.0, le=64.0)
    optics_group_name: str = "opticsGroup1"


# --- inline writer ---------------------------------------------------------------
# Heavy deps (pandas, starfile, mrcfile) are imported lazily inside the functions so
# `import services.jobs.import_tomograms` stays light (the module is pulled by
# services.jobs.__init__, which is imported widely).


def write_tomograms_star_for_job(job_model: ImportTomogramsParams, job_dir: Path, project_dir: Path) -> int:
    """Write ``job_dir/tomograms.star``. Returns the tomogram row count. Inline (no SLURM)."""
    import starfile

    job_dir.mkdir(parents=True, exist_ok=True)
    out = job_dir / "tomograms.star"

    if job_model.source_mode == TOMO_IMPORT_REFERENCE:
        df = _reference_rows(job_model)
    else:
        df = _synthesize_rows(job_model, project_dir)

    if df is None or len(df) == 0:
        raise ValueError(
            f"ImportTomograms produced no rows (mode={job_model.source_mode!r}, "
            f"reference_star={job_model.reference_star!r}, mrc_glob={job_model.mrc_glob!r})"
        )

    # starfile maps the dict key to the block name -> 'global' becomes data_global (oracle parity).
    starfile.write({"global": df}, out, overwrite=True)
    return int(len(df))


def _absolutize(value, base: Path) -> str:
    if not value or not isinstance(value, str):
        return value
    p = Path(value)
    return str(p if p.is_absolute() else (base / p).resolve())


def _reference_rows(job_model: ImportTomogramsParams):
    import starfile

    ref = Path(job_model.reference_star).expanduser()
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

    # Make file references absolute against the reference star's directory so the
    # resolver/drivers find them regardless of cwd.
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


def _safe_tomo_name(stem: str, project_tag: str, seen: set) -> str:
    import re

    base = re.sub(r"[^A-Za-z0-9_]", "_", f"{project_tag}_{stem}")
    name = base
    i = 2
    while name in seen:
        name = f"{base}_{i}"
        i += 1
    seen.add(name)
    return name


def _synthesize_rows(job_model: ImportTomogramsParams, project_dir: Path):
    import glob as _glob
    import pandas as pd
    import mrcfile

    pattern = str(Path(job_model.mrc_glob).expanduser())
    paths = sorted(p for p in (Path(x) for x in _glob.glob(pattern)) if p.is_file())
    if not paths:
        return None

    binning = float(job_model.tomogram_binning or 1.0)
    hand = -1 if job_model.acquisition.invert_defocus_hand else 1
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
        # recon voxel size and binning. Recon voxel size == ts_px * binning.
        if job_model.pixel_size_angstrom > 0:
            ts_px = float(job_model.pixel_size_angstrom)
        elif recon_apix > 0:
            ts_px = recon_apix / binning
        else:
            ts_px = 1.0  # last-resort; runtime checklist flags this case

        rows.append(
            {
                "rlnTomoName": _safe_tomo_name(mrc_path.stem, project_dir.name, seen),
                "rlnVoltage": float(job_model.voltage),
                "rlnSphericalAberration": float(job_model.spherical_aberration),
                "rlnAmplitudeContrast": float(job_model.amplitude_contrast),
                "rlnMicrographOriginalPixelSize": ts_px,
                "rlnTomoHand": hand,
                "rlnOpticsGroupName": job_model.optics_group_name,
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
