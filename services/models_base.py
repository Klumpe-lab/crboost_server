# services/models_base.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, StrEnum
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
    # Per-pick-list extraction (roadmap 07). One instance per (species, tomogram,
    # slug) triple; NOT a scheme/roster job -- see services/jobs/extract_pick_list.py.
    EXTRACT_PICK_LIST = "extractPickList"
    RECONSTRUCT_PARTICLE = "reconstructParticle"

    CLASS3D = "class3d"

    # Synthetic source: not a real pipeline job. Used by PathResolutionService to
    # surface <project>/MergedSources/optimisation_set.star as a producer candidate
    # for input_optimisation slots in aggregation projects. Never appears in
    # state.jobs, jobtype_paramclass, or services.jobs.spec.JOB_SPECS.
    MERGED_SOURCES = "mergedSources"

    # Synthetic source, same contract as MERGED_SOURCES: the PARTICLES-header tomogram
    # import (services/tomogram_import.py) is a project-level artifact, not a job, but its
    # committed Tomograms/tomograms.star has to be a resolver producer candidate. It used
    # to borrow MERGED_SOURCES and be told apart by instance path, which made a dangling
    # imported star report itself as a missing merged-sources optimisation set (de-novo
    # roadmap D-8 / S5). Legacy `mergedSources:Tomograms` override keys still resolve.
    IMPORTED_TOMOGRAMS = "importedTomograms"

    @classmethod
    def from_string(cls, value: str) -> JobType:
        try:
            return cls(value)
        except ValueError:
            valid = [e.value for e in cls]
            raise ValueError(f"Unknown job type '{value}'. Valid types: {valid}") from None


@dataclass(frozen=True, slots=True)
class InstanceId:
    """Typed form of the job instance-id grammar: ``{job_type}`` or
    ``{job_type}__{suffix}``, where the suffix is a species id
    (``templatematching__ribosome``) or a numeric disambiguator
    (``templatematching__2``). This class is the ONE place the ``__``
    separator is known; nothing else may hand-split an instance id."""

    job_type: JobType
    species_id: str | None = None

    @staticmethod
    def split(raw: str) -> tuple[str, str | None]:
        """Lenient ``(base, suffix)`` decode; suffix is None for bare ids.
        Use when the base may not be a valid JobType (display of unknown ids)."""
        base, _, suffix = raw.partition("__")
        return base, (suffix or None)

    @classmethod
    def parse(cls, raw: str) -> InstanceId:
        """Strict decode; raises ValueError when the base is not a JobType."""
        base, suffix = cls.split(raw)
        return cls(JobType(base), suffix)

    @classmethod
    def matches(cls, raw: str, job_type: JobType) -> bool:
        """True when ``raw`` is an instance of ``job_type`` (bare or suffixed)."""
        return cls.split(raw)[0] == job_type.value

    def __str__(self) -> str:
        if self.species_id is None:
            return self.job_type.value
        return f"{self.job_type.value}__{self.species_id}"


def instance_id_to_job_type(instance_id: str) -> JobType:
    """Extract JobType from an instance_id.

    'templatematching'          -> JobType.TEMPLATE_MATCH_PYTOM
    'templatematching__2'       -> JobType.TEMPLATE_MATCH_PYTOM
    'templatematching__ribosome'-> JobType.TEMPLATE_MATCH_PYTOM
    """
    return InstanceId.parse(instance_id).job_type


def split_species_id(instance_id: str) -> str | None:
    """`templatematching__ribosome` → `ribosome`; bare instance_id → None."""
    return InstanceId.split(instance_id)[1]


def resolve_species(state, job_model, instance_id: str | None = None):
    """Find the ParticleSpecies a per-particle job is attached to, using
    three fallbacks in order:

    1. `instance_id` suffix (`templatematching__ribosome` → `ribosome`),
       accepted only when it names a species that exists in the registry
       (a numeric disambiguator like `subtomoExtraction__2` falls through).
    2. `job_model.species_id` field (set even when instance_id is bare).
    3. Single-species fallback: if exactly one species exists in the
       project, attribute the job to it.

    Returns (species or None, species_id or None). THE canonical chain —
    formerly triplicated across dashboard_data / template_metadata /
    aggregation.extraction."""
    if instance_id:
        sid = split_species_id(instance_id)
        if sid:
            sp = state.get_species(sid) if hasattr(state, "get_species") else None
            if sp is not None:
                return sp, sid
    sid2 = getattr(job_model, "species_id", None)
    if sid2:
        sp = state.get_species(sid2) if hasattr(state, "get_species") else None
        if sp is not None:
            return sp, sid2
        return None, sid2
    registry = getattr(state, "species_registry", None) or []
    if len(registry) == 1:
        sp = registry[0]
        return sp, sp.id
    return None, None


class PickListType(str, Enum):
    """Kind of pick list in the per-(species, tomo) curation workbench. The
    overlay glyph each renders as is fixed by type (color varies per list);
    that mapping lives in the UI, not here."""

    AUTO = "auto"  # PyTOM candidates.star — read-only source
    FILTERED = "filtered"  # CC/top-N derived from a parent list
    MANUAL = "manual"  # placed in ArtiaX, ingested from .coords
    IMPORTED = "imported"  # user-supplied .coords/star, ingested
    MERGED = "merged"  # 2+ lists combined with radius dedup


class SpeciesOrigin(StrEnum):
    """How a ParticleSpecies came to exist (`ParticleSpecies.origin`). Stored as a
    plain str — `""` on species that pre-date the field means WORKBENCH — and
    validated at the write site (`ProjectState.add_species`)."""

    WORKBENCH = "workbench"  # template-driven (the Species page "+", formerly the workbench "+")
    MANUAL = "manual"  # created de novo for hand picking (roster "+", Journey empty state)
    IMPORTED = "imported"


class PickSourceKind(StrEnum):
    """Where a pick list's coordinates came from (`PickList.source_kind`); the
    matching `source_ref` names the source (CE instance id · .coords stem · imported
    path · `"a+b+c"` parent slugs). Stored as a plain str, validated at the write
    sites (`services.particles.ingest`, the dashboard merge)."""

    TM = "tm"  # PyTOM candidates of a candidate-extract job (synthesized `auto` / `filtered` rows)
    ARTIAX = "artiax"  # .coords saved from a ChimeraX/ArtiaX session into the curation dir
    IMPORT = "import"  # user-supplied external .coords
    MERGE = "merge"  # union of 2+ lists


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


# Per-species overlay palette. Lives here rather than in the dashboard because a
# species' color is persisted model data (assigned at creation by
# ProjectState.add_species), not a render-time choice; services.dashboard_data
# re-exports it for the renderers that read it as an overlay constant.
#
# Saturated primaries on purpose: these sit over a greyscale tomogram (no
# mid-grays) so the dots pop at either end of the backdrop.
SPECIES_OVERLAY_COLORS = [
    "#ff1744",  # vivid red
    "#00e5ff",  # vivid cyan
    "#ffea00",  # vivid yellow
    "#d500f9",  # vivid magenta-purple
    "#76ff03",  # neon green
    "#2979ff",  # vivid blue
    "#ff9100",  # vivid orange
    "#f50057",  # vivid pink
]


def species_palette_color(species_id: str) -> str:
    """Deterministic palette color for a species id.

    Deliberately NOT `hash()`: PYTHONHASHSEED randomizes str hashing per process,
    so the same species would change color between server restarts. Summing the
    code points is stable across runs and matches what the dashboard already does
    for workbench-authored species.
    """
    return SPECIES_OVERLAY_COLORS[sum(map(ord, str(species_id))) % len(SPECIES_OVERLAY_COLORS)]
