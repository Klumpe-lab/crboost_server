# services/models_base.py
from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict


class JobStatus(str, Enum):
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    RUNNING = "Running"
    QUEUED = "Queued"
    SCHEDULED = "Scheduled"
    UNKNOWN = "Unknown"


class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    KRIOS_G4 = "Krios_G4"
    GLACIOS = "Glacios"
    TALOS = "Talos"
    CUSTOM = "Custom"


class AlignmentMethod(str, Enum):
    ARETOMO = "AreTomo"
    IMOD = "IMOD"
    RELION = "Relion"


class DenoiseMethod(str, Enum):
    """Backend tool for the denoise train/predict steps. Selectable per-job; both
    methods share the same two job types (denoisetrain, denoisepredict) and the same
    upstream (even/odd half-tomograms from tsReconstruct)."""

    CRYOCARE = "cryoCARE"
    ISONET = "IsoNet"


class IsoNetRefineMethod(str, Enum):
    """IsoNet `refine --method` strategy. Values match the CLI strings. AUTO selects
    isonet2-n2n when even/odd halves are present (missing-wedge correction + denoising)."""

    AUTO = "auto"
    N2N = "n2n"  # noise2noise denoising only
    ISONET2 = "isonet2"  # single-map missing-wedge correction
    ISONET2_N2N = "isonet2-n2n"  # even/odd: missing-wedge correction + denoising


class MissAlignSchedule(str, Enum):
    """miss-alignment macro-iteration schedule preset. Expands (in drivers/miss_align.py)
    to the tool's `iteration_settings` list — a coarse->fine schedule of {downsample,
    alignment-mode} entries whose length is the number of macro-iterations. FAST = quick
    sanity pass; DEFAULT = balanced coarse->fine->local; THOROUGH = the full 8-iteration
    schedule from the tool docs (production, runs for hours)."""

    FAST = "fast"
    DEFAULT = "default"
    THOROUGH = "thorough"


class JobCategory(str, Enum):
    IMPORT = "Import"
    EXTERNAL = "External"
    MOTIONCORR = "MotionCorr"
    CTFFIND = "CtfFind"


class JobType(str, Enum):
    IMPORT_MOVIES = "importmovies"
    FS_MOTION_CTF = "fsMotionAndCtf"
    TS_ALIGNMENT = "aligntiltsWarp"
    MISS_ALIGN = "missAlign"
    TS_IMPORT = "tsImport"
    TS_CTF = "tsCtf"
    TILT_FILTER = "tiltFilter"
    TS_RECONSTRUCT = "tsReconstruct"
    DENOISE_TRAIN = "denoisetrain"
    DENOISE_PREDICT = "denoisepredict"
    TEMPLATE_MATCH_PYTOM = "templatematching"
    TEMPLATE_EXTRACT_PYTOM = "tmextractcand"
    SUBTOMO_EXTRACTION = "subtomoExtraction"
    RECONSTRUCT_PARTICLE = "reconstructParticle"

    CLASS3D = "class3d"

    # Synthetic source: not a real pipeline job. Used by PathResolutionService to
    # surface <project>/MergedSources/optimisation_set.star as a producer candidate
    # for input_optimisation slots in aggregation projects. Never appears in
    # state.jobs, jobtype_paramclass, or services.jobs.spec.JOB_SPECS.
    MERGED_SOURCES = "mergedSources"

    @classmethod
    def from_string(cls, value: str) -> JobType:
        try:
            return cls(value)
        except ValueError:
            valid = [e.value for e in cls]
            raise ValueError(f"Unknown job type '{value}'. Valid types: {valid}") from None


class PickListType(str, Enum):
    """Kind of pick list in the per-(species, tomo) curation workbench. The
    overlay glyph each renders as is fixed by type (color varies per list);
    that mapping lives in the UI, not here."""

    AUTO = "auto"  # PyTOM candidates.star — read-only source
    FILTERED = "filtered"  # CC/top-N derived from a parent list
    MANUAL = "manual"  # placed in ArtiaX, ingested from .coords
    IMPORTED = "imported"  # user-supplied .coords/star, ingested
    MERGED = "merged"  # 2+ lists combined with radius dedup


class ListExtractionState(str, Enum):
    """Whether a workbench pick list's COORDINATES have been subtomo-extracted, so
    downstream refinement can read its particles. Manual/imported/merged lists are
    raw coordinates and can't go downstream until extracted; auto/filtered map to
    the existing subtomo job. DERIVED from durable facts on the PickList, never a
    stored boolean — see PickList.extraction_state() (avoids the stale-flag trap).
    Extraction is scoped PER LIST and triggered by the user per list (neither
    fully automatic — no throwaway re-extractions — nor manually tedious)."""

    NOT_EXTRACTED = "not_extracted"  # coordinates only; needs extraction to go downstream
    EXTRACTED = "extracted"  # extracted, and current with the list's picks
    STALE = "stale"  # extracted earlier, but picks changed since → re-extract


class MicroscopeParams(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    microscope_type: MicroscopeType = MicroscopeType.CUSTOM
    pixel_size_angstrom: float = Field(default=1.35, ge=0.5, le=10.0)
    acceleration_voltage_kv: float = Field(default=300.0)
    spherical_aberration_mm: float = Field(default=2.7, ge=0.0, le=10.0)
    amplitude_contrast: float = Field(default=0.10, ge=0.0, le=1.0)


class AcquisitionParams(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    dose_per_tilt: float = Field(default=3.0, ge=0.1, le=9.0)
    detector_dimensions: tuple[int, int] = (4096, 4096)
    tilt_axis_degrees: float = Field(default=-95.0, ge=-180.0, le=180.0)
    eer_fractions_per_frame: int | None = Field(default=None, ge=1, le=100)
    sample_thickness_nm: float = Field(default=300.0, ge=50.0, le=2000.0)
    gain_reference_path: str | None = None
    invert_tilt_angles: bool = False
    invert_defocus_hand: bool = True
    acquisition_software: str = Field(default="SerialEM")
    nominal_magnification: int | None = None
    spot_size: int | None = None
    camera_name: str | None = None
    binning: int | None = Field(default=1, ge=1)
    frame_dose: float | None = None
