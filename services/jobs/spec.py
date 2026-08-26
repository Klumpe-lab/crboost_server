"""Single source of truth for per-job-type pipeline metadata.

One `JobSpec` row per pipeline job type, in pipeline order. This table replaces
the previously unsynchronized per-concern tables (param-class map, pipeline
order, display names, roster phases, dependency gating, auto-added
prerequisites, driver scripts, plugin module list, legacy array-output stars).
Adding a job type means adding ONE row here.

Import from submodules only (never `from services.jobs import ...`) — this
module is imported by the package `__init__`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

from services.jobs._base import AbstractJobParams
from services.jobs.candidate_extract import CandidateExtractPytomParams
from services.jobs.class3d import Class3DParams
from services.jobs.denoise_predict import DenoisePredictParams
from services.jobs.denoise_train import DenoiseTrainParams
from services.jobs.extract_pick_list import ExtractPickListParams
from services.jobs.fs_motion_ctf import FsMotionCtfParams
from services.jobs.import_movies import ImportMoviesParams
from services.jobs.miss_align import MissAlignParams
from services.jobs.reconstruct_particle import ReconstructParticleParams
from services.jobs.subtomo_extraction import SubtomoExtractionParams
from services.jobs.template_match import TemplateMatchPytomParams
from services.jobs.tilt_filter import TiltFilterParams
from services.jobs.ts_alignment import TsAlignmentParams
from services.jobs.ts_ctf import TsCtfParams
from services.jobs.ts_import import TsImportParams
from services.jobs.ts_reconstruct import TsReconstructParams
from services.models_base import JobType

PHASE_PREPROCESSING: Final = "preprocessing"
PHASE_PARTICLES: Final = "particles"


@dataclass(frozen=True, slots=True)
class JobSpec:
    """Pipeline topology + identity for one job type.

    Execution flags (IS_INTERACTIVE, JOB_CATEGORY, RELION_JOB_TYPE, ...) stay
    on the param class; this row is everything the pipeline needs to know
    *about* the type without instantiating it.
    """

    job_type: JobType
    param_class: type[AbstractJobParams]
    display_name: str
    # Roster phase (PHASE_PREPROCESSING / PHASE_PARTICLES); None = hidden from
    # the user-visible roster (auto-injected prerequisites like tsImport).
    phase: str | None
    # Roster gating: job types that must be present before this one is addable.
    dependencies: tuple[JobType, ...] = ()
    # Auto-added when this type is inserted and the prerequisite is missing.
    prerequisite: JobType | None = None
    # Script under drivers/; None = command built natively (importmovies).
    driver: str | None = None
    # ui.job_plugins module basenames whose decorators customize this type's UI.
    plugins: tuple[str, ...] = ()
    # Legacy-job fallback: the stage's primary output star listing every TS the
    # job touched, used when `.task_manifest.json` is absent (pre-array-tracker
    # projects) to show real per-TS pills instead of a stuck "pending".
    array_output_star: str | None = None


JOB_SPECS: Final[tuple[JobSpec, ...]] = (
    JobSpec(
        JobType.IMPORT_MOVIES,
        ImportMoviesParams,
        "Import",
        PHASE_PREPROCESSING,
        driver=None,  # native relion_python_tomo_import command, built inline
    ),
    JobSpec(
        JobType.FS_MOTION_CTF,
        FsMotionCtfParams,
        "Motion & CTF",
        PHASE_PREPROCESSING,
        dependencies=(JobType.IMPORT_MOVIES,),
        driver="fs_motion_and_ctf.py",
        plugins=("fs_motion_and_ctf", "array_tasks"),
        array_output_star="fs_motion_and_ctf.star",
    ),
    JobSpec(
        JobType.TS_IMPORT,
        TsImportParams,
        "TS Import",
        None,  # silently-injected prerequisite; never in the roster
        driver="ts_import.py",
    ),
    # Optional insertable filter: runs after tsImport (its tomostar prerequisite
    # is auto-added) and reads the fs-motion star for the DL pass. Gated
    # positionally in the scheme before alignment; alignment does NOT declare a
    # dep on it, so a no-filter pipeline is unchanged (mirrors MISS_ALIGN).
    JobSpec(
        JobType.TILT_FILTER,
        TiltFilterParams,
        "Tilt Filter",
        PHASE_PREPROCESSING,
        dependencies=(JobType.FS_MOTION_CTF,),
        prerequisite=JobType.TS_IMPORT,
        driver="tilt_filter.py",
        plugins=("tilt_filter",),
    ),
    JobSpec(
        JobType.TS_ALIGNMENT,
        TsAlignmentParams,
        "Alignment",
        PHASE_PREPROCESSING,
        dependencies=(JobType.FS_MOTION_CTF,),
        prerequisite=JobType.TS_IMPORT,
        driver="ts_alignment.py",
        plugins=("array_tasks",),
        array_output_star="aligned_tilt_series.star",
    ),
    # Optional insertable refinement: requires alignment, but tsCtf does NOT
    # require it (tsCtf stays gated on TS_ALIGNMENT so the common no-missAlign
    # pipeline is unchanged).
    JobSpec(
        JobType.MISS_ALIGN,
        MissAlignParams,
        "Miss Align",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_ALIGNMENT,),
        driver="miss_align.py",
    ),
    JobSpec(
        JobType.TS_CTF,
        TsCtfParams,
        "TS CTF",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_ALIGNMENT,),
        driver="ts_ctf.py",
        plugins=("array_tasks",),
        array_output_star="ts_ctf_tilt_series.star",
    ),
    JobSpec(
        JobType.TS_RECONSTRUCT,
        TsReconstructParams,
        "Reconstruct",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_CTF,),
        driver="ts_reconstruct.py",
        plugins=("ts_reconstruct",),
        array_output_star="tomograms.star",
    ),
    JobSpec(
        JobType.DENOISE_TRAIN,
        DenoiseTrainParams,
        "Denoise Train",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_RECONSTRUCT,),
        driver="denoise_train.py",
    ),
    JobSpec(
        JobType.DENOISE_PREDICT,
        DenoisePredictParams,
        "Denoise Predict",
        PHASE_PREPROCESSING,
        dependencies=(JobType.DENOISE_TRAIN, JobType.TS_RECONSTRUCT),
        driver="denoise_predict.py",
        plugins=("array_tasks",),
    ),
    JobSpec(
        JobType.TEMPLATE_MATCH_PYTOM,
        TemplateMatchPytomParams,
        "Template Match",
        PHASE_PARTICLES,
        dependencies=(JobType.TS_CTF,),
        driver="template_match_pytom.py",
        plugins=("template_match", "array_tasks"),
    ),
    JobSpec(
        JobType.TEMPLATE_EXTRACT_PYTOM,
        CandidateExtractPytomParams,
        "Pick candidates",
        PHASE_PARTICLES,
        dependencies=(JobType.TEMPLATE_MATCH_PYTOM,),
        driver="extract_candidates_pytom.py",
        plugins=("candidate_extract", "array_tasks"),
    ),
    JobSpec(
        JobType.SUBTOMO_EXTRACTION,
        SubtomoExtractionParams,
        "Subtomo Extraction",
        PHASE_PARTICLES,
        dependencies=(JobType.TEMPLATE_EXTRACT_PYTOM,),
        driver="subtomo_extraction.py",
        plugins=("subtomo_extraction",),
    ),
    # Per-pick-list extraction (roadmap 07). phase=None: this is NOT a roster job --
    # one instance exists per pick list and its home is the Species page's Picks tab,
    # not the pipeline. The row itself is mandatory even so, because jobtype_paramclass()
    # is derived from JOB_SPECS and ProjectState.load drops instances of a type with no
    # param class. Keeping it out of the sweeps is the param class's IS_INTERACTIVE.
    JobSpec(JobType.EXTRACT_PICK_LIST, ExtractPickListParams, "Extract Pick List", None, driver="extract_pick_list.py"),
    JobSpec(
        JobType.RECONSTRUCT_PARTICLE,
        ReconstructParticleParams,
        "Reconstruct Particle",
        PHASE_PARTICLES,
        dependencies=(JobType.SUBTOMO_EXTRACTION,),
        driver="reconstruct_particle.py",
    ),
    JobSpec(
        JobType.CLASS3D,
        Class3DParams,
        "Class 3D",
        PHASE_PARTICLES,
        dependencies=(JobType.RECONSTRUCT_PARTICLE,),
        driver="class3d.py",
    ),
)

JOB_SPEC_BY_TYPE: Final[Mapping[JobType, JobSpec]] = MappingProxyType({s.job_type: s for s in JOB_SPECS})

PARAM_CLASS_BY_TYPE: Final[Mapping[JobType, type[AbstractJobParams]]] = MappingProxyType(
    {s.job_type: s.param_class for s in JOB_SPECS}
)

PIPELINE_ORDER: Final[tuple[JobType, ...]] = tuple(s.job_type for s in JOB_SPECS)

# Synthetic sources that are not pipeline jobs (no JobSpec row) but still need
# a human-readable name (e.g. MergedSources appears as a producer candidate).
_SYNTHETIC_DISPLAY_NAMES: Final[Mapping[JobType, str]] = MappingProxyType(
    {JobType.MERGED_SOURCES: "Merged Sources", JobType.IMPORTED_TOMOGRAMS: "Imported Tomograms"}
)


def display_name(job_type: JobType) -> str:
    spec = JOB_SPEC_BY_TYPE.get(job_type)
    if spec is not None:
        return spec.display_name
    return _SYNTHETIC_DISPLAY_NAMES.get(job_type, job_type.value)


def driver_launch_prefix(*, server_dir: Path, driver_script: Path) -> str:
    """`export PYTHONPATH=…; <python> <script>` — everything a driver launch shares
    before its arguments.

    Lives next to `JobSpec.driver` because this module already owns *which* script
    runs a job type; this is *how* it gets started. Both callers must not drift on the
    interpreter choice or the PYTHONPATH export; both are the pipeline entry points in
    `driver_invocation` below. (`backend.extract_pick_list` used to be a third, passing
    its own argument set instead of `--instance_id`; roadmap 07-S2 gave that job a real
    instance, so it goes through `driver_invocation` like everything else.)

    The venv interpreter is used when the repo has one, else bare `python3` from PATH.
    """
    python_exe = server_dir / "venv" / "bin" / "python3"
    if not python_exe.exists():
        python_exe = Path("python3")
    return f"export PYTHONPATH={server_dir}:${{PYTHONPATH}}; {python_exe} {driver_script}"


def driver_invocation(*, server_dir: Path, driver_script: Path, instance_id: str, project_path: Path) -> str:
    """The shell command that launches one PIPELINE driver process.

    Two callers build it and must not drift, since a driver cannot tell which entry
    point started it:

      - the orchestrator's `fn_exe` (what RELION runs for the job), and
      - `drivers/array_job_base.build_array_sbatch_script`'s `XXXcommandXXX`, where
        a supervisor re-invokes its OWN script per array task.

    Identity travels as `--instance_id` + `--project_path`, which is exactly what
    `get_driver_context` parses.
    """
    return (
        f"{driver_launch_prefix(server_dir=server_dir, driver_script=driver_script)} "
        f"--instance_id {instance_id} "
        f"--project_path {project_path}"
    )
