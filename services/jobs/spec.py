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

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

from services.configs.config_service import get_config_service
from services.io_slots import JobFileType
from services.jobs._base import AbstractJobParams
from services.jobs.candidate_extract import CandidateExtractPytomParams
from services.jobs.class3d import Class3DParams
from services.jobs.ctf_refine import CtfRefineParams
from services.jobs.denoise_predict import DenoisePredictParams
from services.jobs.denoise_train import DenoiseTrainParams
from services.jobs.extract_pick_list import ExtractPickListParams
from services.jobs.filament_pick import FilamentPickParams
from services.jobs.fs_motion_ctf import FsMotionCtfParams
from services.jobs.import_movies import ImportMoviesParams
from services.jobs.mask_create import MaskCreateParams
from services.jobs.miss_align import MissAlignParams
from services.jobs.postprocess import PostProcessParams
from services.jobs.reconstruct_particle import ReconstructParticleParams
from services.jobs.refine3d import Refine3DParams
from services.jobs.segment import SegmentParams
from services.jobs.subset import SubsetParams
from services.jobs.subtomo_extraction import SubtomoExtractionParams
from services.jobs.template_match import TemplateMatchPytomParams
from services.jobs.tilt_filter import TiltFilterParams
from services.jobs.ts_alignment import TsAlignmentParams
from services.jobs.ts_ctf import TsCtfParams
from services.jobs.ts_import import TsImportParams
from services.jobs.ts_reconstruct import TsReconstructParams
from services.jobs.warp_export import WarpExportParams
from services.models_base import InstanceId, JobType

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
    # Job types any one of which stands in for `dependencies`: the job reads that one instead (Pick candidates
    # picks from a Segment job's volumes instead of a Template Match's scores).
    alternatives: tuple[JobType, ...] = ()
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
    # Output types this job refines in place of its own input of the same type: a consumer
    # auto-bound to that input follows it here when this job is in the pipeline ahead of the
    # consumer (PathResolutionService._follow_refinements). Never inferred from the schemas -
    # several jobs consume and produce one type without being a drop-in refinement.
    refines: tuple[JobFileType, ...] = ()
    # The input slot whose star rows are this job's tilt series, for the jobs that can follow a
    # Subset ("Add <job>" on its tab, which sets this slot to the Subset). None: the job takes its
    # tilt series from elsewhere (alignment reads the registry) or works per particle.
    items_from: str | None = None
    # False: never added from the roster's add rows; made elsewhere (a Subset, from the tab of the
    # job it cuts).
    roster_add: bool = True
    # Set on a job type no longer made: what does its work now. Saved jobs of the type load, show and run again;
    # nothing adds a new one (the roster, the map, the API).
    retired: str = ""
    # How services/log_progress reads this job's run.out ("relion", "miss_align", ...); None = its
    # generic reader (the last progress bar of any kind).
    log_format: str | None = None
    # The job's folder link (services/job_links.py): `External/005_alignment_aretomo -> job005`. The type part
    # of the name, and the param field whose value follows it when the job has no species.
    dir_slug: str = ""
    dir_qualifier: str | None = None
    # One line on what the job does: the magazine popup's header.
    summary: str = ""
    # The type's glyph on the roster's rail: the body of a 12 x 12 SVG drawn in one 1.1 px line in currentColor
    # (ui/components/job_glyph.py draws it).
    glyph: str = ""


JOB_SPECS: Final[tuple[JobSpec, ...]] = (
    JobSpec(
        JobType.IMPORT_MOVIES,
        ImportMoviesParams,
        "Import",
        PHASE_PREPROCESSING,
        driver=None,  # native relion_python_tomo_import command, built inline
        dir_slug="import",
        summary="Reads the raw movies and their .mdoc files into the project.",
        glyph='<path d="M6 1.5v6M3.6 5.2 6 7.6l2.4-2.4M1.5 8.2v2.3h9V8.2"/>',
    ),
    JobSpec(
        JobType.FS_MOTION_CTF,
        FsMotionCtfParams,
        "Motion & CTF",
        PHASE_PREPROCESSING,
        dependencies=(JobType.IMPORT_MOVIES,),
        driver="fs_motion_and_ctf.py",
        plugins=("array_tasks",),
        array_output_star="fs_motion_and_ctf.star",
        dir_slug="motion_ctf",
        summary="Aligns each movie's frames and fits a CTF per tilt (WarpTools fs_motion_and_ctf).",
        glyph=('<rect x="1.5" y="4" width="6.5" height="6.5"/><path d="M3.5 4V2h6.5v6.5H8"/>'),
    ),
    JobSpec(
        JobType.TS_IMPORT,
        TsImportParams,
        "TS Import",
        None,  # silently-injected prerequisite; never in the roster
        driver="ts_import.py",
        dir_slug="ts_import",
        summary="Groups the motion-corrected tilts into tilt series and writes Warp's settings.",
        glyph='<path d="M6 1.5v6M3.6 5.2 6 7.6l2.4-2.4M1.5 8.2v2.3h9V8.2"/>',
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
        dir_slug="tilt_filter",
        summary="Scores every tilt and takes the bad ones (beam-blocked, drifted, empty) out of the series.",
        glyph='<path d="M1.5 2h9L7 6.4v3.4l-2 .9V6.4z"/>',
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
        dir_slug="alignment",
        dir_qualifier="alignment_method",
        summary="Finds each tilt series' geometry: shifts, the tilt axis, the tilt angles.",
        glyph='<path d="M6 10.5 2 2.8M6 10.5V1.5M6 10.5l4-7.7M3.2 8.6a3.6 3.6 0 0 1 5.6 0"/>',
    ),
    # Optional insertable refinement: requires alignment, but tsCtf does NOT
    # require it (tsCtf stays gated on TS_ALIGNMENT so the common no-missAlign
    # pipeline is unchanged).
    JobSpec(
        JobType.MISS_ALIGN,
        MissAlignParams,
        "Missalignment",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_ALIGNMENT,),
        driver="miss_align.py",
        plugins=("miss_align",),
        refines=(JobFileType.ALIGNED_TILT_SERIES_STAR, JobFileType.WARP_TILTSERIES_DIR),
        items_from="input_star",
        log_format="miss_align",
        dir_slug="missalign",
        summary="Refines an alignment with the learned miss-alignment model.",
        glyph=('<path d="M6 10.5 2.2 3M6 10.5 9.8 3"/><circle cx="6" cy="3.6" r="1.4"/>'),
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
        items_from="input_star",
        dir_slug="ctf",
        summary="Fits the defocus per tilt series, with the tilts' hand and geometry.",
        glyph=(
            '<circle cx="6" cy="6" r="4.6"/>'
            '<circle cx="6" cy="6" r="2.6"/>'
            '<circle cx="6" cy="6" r=".7" fill="currentColor"/>'
        ),
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
        items_from="input_star",
        dir_slug="reconstruct",
        summary="Back-projects each tilt series into a tomogram at the chosen pixel size.",
        glyph='<path d="M6 1.5 10.5 4v4.8L6 11.3 1.5 8.8V4zM1.5 4 6 6.5 10.5 4M6 6.5v4.8"/>',
    ),
    JobSpec(
        JobType.DENOISE_TRAIN,
        DenoiseTrainParams,
        "Denoise Train",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_RECONSTRUCT,),
        driver="denoise_train.py",
        items_from="input_star",
        log_format="denoise",
        dir_slug="denoise_train",
        dir_qualifier="denoise_method",
        summary="Trains a denoising model on the tomograms' even/odd halves.",
        glyph=(
            '<circle cx="2.6" cy="3" r="1.1"/>'
            '<circle cx="2.6" cy="9" r="1.1"/>'
            '<circle cx="9.4" cy="6" r="1.1"/>'
            '<path d="M3.6 3.5 8.4 5.6M3.6 8.5l4.8-2.1M2.6 4.1v3.8"/>'
        ),
    ),
    JobSpec(
        JobType.DENOISE_PREDICT,
        DenoisePredictParams,
        "Denoise Predict",
        PHASE_PREPROCESSING,
        # No DENOISE_TRAIN: easymode predicts with pretrained weights. cryoCARE and IsoNet name their
        # missing model as a missing input instead.
        dependencies=(JobType.TS_RECONSTRUCT,),
        driver="denoise_predict.py",
        plugins=("array_tasks",),
        items_from="input_star",
        dir_slug="denoise_predict",
        dir_qualifier="denoise_method",
        summary="Denoises every tomogram with a trained model, or with a pretrained one.",
        glyph='<path d="M1.8 10.2 7.4 4.6M8.6 1.4v2.2M7.5 2.5h2.2M10 5.2v1.4M9.3 5.9h1.4"/>',
    ),
    JobSpec(
        JobType.SEGMENT,
        SegmentParams,
        "Segment",
        PHASE_PREPROCESSING,
        dependencies=(JobType.TS_RECONSTRUCT,),
        driver="segment.py",
        plugins=("segment", "array_tasks"),
        items_from="input_tomograms",
        dir_slug="segment",
        dir_qualifier="method",
        summary="Segments each tomogram into membranes, filaments and other features.",
        glyph=(
            '<path d="M2 6.2C2 3.4 4 1.8 6.4 2.2c2.6.4 3.8 2.4 3.4 4.6-.4 2.2-2.4 3.4-4.6 3.2C3.2 9.8 2 8.4 2 6.2z"/>'
            '<path d="M4 8.6 8.2 4.4M3.2 6.2l3-3M6.2 9.4l3-3"/>'
        ),
    ),
    # Made from the tab of the job whose output it cuts, never from the roster; written at submit.
    JobSpec(
        JobType.SUBSET,
        SubsetParams,
        "Subset",
        PHASE_PREPROCESSING,
        driver=None,
        plugins=("subset",),
        roster_add=False,
        dir_slug="subset",
        summary="A cut of another job's tilt series, picked by hand or by a rule.",
        glyph='<path d="M1.5 2.5h9M1.5 5h9M1.5 7.5h5M1.5 10h2.5"/>',
    ),
    JobSpec(
        JobType.TEMPLATE_MATCH_PYTOM,
        TemplateMatchPytomParams,
        "Template Match",
        PHASE_PARTICLES,
        dependencies=(JobType.TS_CTF,),
        driver="template_match_pytom.py",
        plugins=("template_match", "array_tasks"),
        items_from="input_tomograms",
        dir_slug="tm",
        summary="Scores every orientation of a template at every voxel (PyTOM), one tomogram per GPU.",
        glyph=('<circle cx="6" cy="6" r="3.6"/><path d="M6 .8v2.6M6 8.6v2.6M.8 6h2.6M8.6 6h2.6"/>'),
    ),
    JobSpec(
        JobType.TEMPLATE_EXTRACT_PYTOM,
        CandidateExtractPytomParams,
        "Pick candidates",
        PHASE_PARTICLES,
        dependencies=(JobType.TEMPLATE_MATCH_PYTOM,),
        alternatives=(JobType.SEGMENT,),
        driver="extract_candidates_pytom.py",
        plugins=("candidate_extract", "array_tasks"),
        dir_slug="pick",
        summary="Picks one species' candidates from template-matching scores or from a segmented feature.",
        glyph=(
            '<circle cx="3" cy="3.2" r=".9" fill="currentColor"/>'
            '<circle cx="8.8" cy="2.8" r=".9" fill="currentColor"/>'
            '<circle cx="2.8" cy="9" r=".9" fill="currentColor"/>'
            '<circle cx="8.2" cy="8.4" r="2.2"/>'
            '<circle cx="8.2" cy="8.4" r=".7" fill="currentColor"/>'
        ),
    ),
    # Retired (roadmap 64 S6): Pick candidates traces a filament species' feature itself. One CPU job, no array.
    JobSpec(
        JobType.FILAMENT_PICK,
        FilamentPickParams,
        "Filament pick",
        PHASE_PARTICLES,
        dependencies=(JobType.SEGMENT,),
        driver="filament_pick.py",
        roster_add=False,
        retired="Pick candidates picks filaments: add it for a filament species, reading the segmentations.",
        dir_slug="filament_pick",
        summary="Places points along the filaments of one segmented feature.",
        glyph=(
            '<path d="M1.5 9.5C4 9.5 4 2.5 6.5 2.5s2.4 4 4 4"/>'
            '<circle cx="3.6" cy="7.4" r=".8" fill="currentColor"/>'
            '<circle cx="6.5" cy="2.5" r=".8" fill="currentColor"/>'
            '<circle cx="9.2" cy="5.6" r=".8" fill="currentColor"/>'
        ),
    ),
    JobSpec(
        JobType.SUBTOMO_EXTRACTION,
        SubtomoExtractionParams,
        "Subtomo Extraction",
        PHASE_PARTICLES,
        dependencies=(JobType.TEMPLATE_EXTRACT_PYTOM,),
        driver="subtomo_extraction.py",
        plugins=("subtomo_extraction",),
        dir_slug="extract",
        summary="Cuts the particles out of the tomograms (RELION).",
        glyph=(
            '<rect x="1.5" y="1.5" width="3.8" height="3.8"/>'
            '<rect x="6.7" y="1.5" width="3.8" height="3.8"/>'
            '<rect x="1.5" y="6.7" width="3.8" height="3.8"/>'
            '<rect x="6.7" y="6.7" width="3.8" height="3.8"/>'
        ),
    ),
    # Alternative producer of the particle optimisation set: WarpTools cuts the 2D particle
    # series with the full Warp model (local image warps included), which RELION extraction
    # cannot read. Same input and output envelope as SUBTOMO_EXTRACTION.
    JobSpec(
        JobType.WARP_EXPORT,
        WarpExportParams,
        "Warp Export",
        PHASE_PARTICLES,
        dependencies=(JobType.TEMPLATE_EXTRACT_PYTOM,),
        driver="warp_export.py",
        plugins=("array_tasks",),
        dir_slug="warp_export",
        summary="Exports the particles as 2-D stacks per tilt (WarpTools ts_export_particles).",
        glyph='<path d="M6.5 2H2v8.5h8.5V6M7.5 1.5h3v3M10.5 1.5 5.8 6.2"/>',
    ),
    # Per-pick-list extraction. phase=None: this is NOT a roster job --
    # one instance exists per pick list and its home is the Species page's Picks tab,
    # not the pipeline. The row itself is mandatory even so, because jobtype_paramclass()
    # is derived from JOB_SPECS and ProjectState.load drops instances of a type with no
    # param class. Keeping it out of the sweeps is the param class's IS_INTERACTIVE.
    JobSpec(
        JobType.EXTRACT_PICK_LIST,
        ExtractPickListParams,
        "Extract Pick List",
        None,
        driver="extract_pick_list.py",
        dir_slug="pick_list",
        summary="Cuts the particles of one pick list out of the tomograms.",
        glyph=(
            '<rect x="1.5" y="1.5" width="3.8" height="3.8"/>'
            '<rect x="6.7" y="1.5" width="3.8" height="3.8"/>'
            '<rect x="1.5" y="6.7" width="3.8" height="3.8"/>'
            '<rect x="6.7" y="6.7" width="3.8" height="3.8"/>'
        ),
    ),
    JobSpec(
        JobType.RECONSTRUCT_PARTICLE,
        ReconstructParticleParams,
        "Reconstruct Particle",
        PHASE_PARTICLES,
        dependencies=(JobType.SUBTOMO_EXTRACTION,),
        driver="reconstruct_particle.py",
        plugins=("particle_results",),
        log_format="reconstruct",
        dir_slug="recon_particle",
        summary="Averages the extracted particles into a first map.",
        glyph=('<circle cx="6" cy="6" r="4.6"/><ellipse cx="6" cy="6" rx="4.6" ry="1.7"/>'),
    ),
    JobSpec(
        JobType.CLASS3D,
        Class3DParams,
        "Class 3D",
        PHASE_PARTICLES,
        dependencies=(JobType.RECONSTRUCT_PARTICLE,),
        driver="class3d.py",
        plugins=("particle_results",),
        log_format="relion",
        dir_slug="class3d",
        summary="Sorts the particles into classes by 3-D classification.",
        glyph=(
            '<circle cx="6" cy="2.9" r="1.6"/><circle cx="2.9" cy="8.5" r="1.6"/><circle cx="9.1" cy="8.5" r="1.6"/>'
        ),
    ),
    JobSpec(
        JobType.REFINE3D,
        Refine3DParams,
        "Refine 3D",
        PHASE_PARTICLES,
        dependencies=(JobType.RECONSTRUCT_PARTICLE,),
        driver="refine3d.py",
        log_format="relion",
        dir_slug="refine3d",
        plugins=("particle_results",),
        summary="Gold-standard refinement of one class against two random halves.",
        glyph=('<circle cx="6" cy="6" r="4.6"/><circle cx="6" cy="6" r="2" fill="currentColor"/>'),
    ),
    # A solvent mask from any map a particle job made (a refinement's, a reconstruction's, a sharpened one), for
    # PostProcess, and for Class 3D and Refine 3D with mask_from_job set.
    JobSpec(
        JobType.MASK_CREATE,
        MaskCreateParams,
        "Mask",
        PHASE_PARTICLES,
        dependencies=(JobType.RECONSTRUCT_PARTICLE,),
        driver="mask_create.py",
        dir_slug="mask",
        summary="A soft solvent mask from a map.",
        glyph=('<circle cx="6" cy="6" r="4.6" stroke-dasharray="1.6 1.3"/><circle cx="6" cy="6" r="2.4"/>'),
    ),
    JobSpec(
        JobType.POSTPROCESS,
        PostProcessParams,
        "PostProcess",
        PHASE_PARTICLES,
        dependencies=(JobType.REFINE3D,),
        driver="postprocess.py",
        dir_slug="postprocess",
        plugins=("particle_results",),
        summary="Gold-standard FSC and the sharpened map from a refinement's halves.",
        glyph='<path d="M1.5 1.5v9h9M2.6 2.8c2.6 0 2.8 6 7.4 6.6"/>',
    ),
    # Per-tilt defocus, astigmatism and scale against a reference at the tilt series' pixel size (a Reconstruct
    # particle at binning 1, its Mask and PostProcess); a Refine 3D reads its optimisation set next.
    JobSpec(
        JobType.CTF_REFINE,
        CtfRefineParams,
        "CTF refine",
        PHASE_PARTICLES,
        dependencies=(JobType.REFINE3D,),
        driver="ctf_refine.py",
        log_format="reconstruct",
        dir_slug="ctf_refine",
        summary="Refines each tilt's defocus and astigmatism against a reference.",
        glyph=(
            '<ellipse cx="6" cy="6" rx="4.7" ry="3.5" transform="rotate(-30 6 6)"/>'
            '<ellipse cx="6" cy="6" rx="2.2" ry="1.6" transform="rotate(-30 6 6)"/>'
        ),
    ),
)

JOB_SPEC_BY_TYPE: Final[Mapping[JobType, JobSpec]] = MappingProxyType({s.job_type: s for s in JOB_SPECS})

PARAM_CLASS_BY_TYPE: Final[Mapping[JobType, type[AbstractJobParams]]] = MappingProxyType(
    {s.job_type: s.param_class for s in JOB_SPECS}
)

PIPELINE_ORDER: Final[tuple[JobType, ...]] = tuple(s.job_type for s in JOB_SPECS)

_TYPE_ORDER: Final[Mapping[JobType, int]] = MappingProxyType({jt: i for i, jt in enumerate(PIPELINE_ORDER)})


def instance_order(instance_id: str) -> tuple[int, int, str]:
    """Sort key of a job instance on the roster: its type's place in PIPELINE_ORDER, then the base
    instance, numbered instances by number, and named ones (a species) by name. Run allocates job
    directories in this order."""
    base, suffix = InstanceId.split(instance_id)
    try:
        type_order = _TYPE_ORDER.get(JobType(base), 999)
    except ValueError:
        type_order = 999
    if suffix is None:
        return (type_order, 0, "")
    try:
        return (type_order, int(suffix), "")
    except ValueError:
        return (type_order, 999, suffix)


# Synthetic sources that are not pipeline jobs (no JobSpec row) but still need
# a human-readable name (e.g. MergedSources appears as a producer candidate).
_SYNTHETIC_DISPLAY_NAMES: Final[Mapping[JobType, str]] = MappingProxyType(
    {
        JobType.MERGED_SOURCES: "Merged Sources",
        JobType.IMPORTED_TOMOGRAMS: "Imported Tomograms",
        JobType.IMPORTED_SEGMENTATIONS: "Imported Segmentations",
    }
)


def display_name(job_type: JobType) -> str:
    spec = JOB_SPEC_BY_TYPE.get(job_type)
    if spec is not None:
        return spec.display_name
    return _SYNTHETIC_DISPLAY_NAMES.get(job_type, job_type.value)


def instance_display_name(instance_id: str, job_model=None) -> str:
    """A job instance's name as the UI shows it: its label when it has one, else its type's name with its job
    directory once it has run ("Alignment (job004)"), else with its instance suffix ("Alignment #2")."""
    if job_model is not None:
        label = getattr(job_model, "display_label", None)
        if label:
            return label

    base, suffix = InstanceId.split(instance_id)

    try:
        base_name = display_name(JobType(base))
    except ValueError:
        base_name = base

    if job_model is not None:
        relion_job_name = getattr(job_model, "relion_job_name", None)
        if relion_job_name:
            job_dir = relion_job_name.rstrip("/").split("/")[-1]
            return f"{base_name} ({job_dir})"

    if suffix is None:
        return base_name

    if suffix.isdigit():
        return f"{base_name} #{suffix}"
    return f"{base_name} ({suffix})"


def driver_launch_prefix(*, server_dir: Path, driver_script: Path) -> str:
    """`export PYTHONPATH=… PYTHONNOUSERSITE=1; <python> <script>` — everything a driver launch shares
    before its arguments.

    Lives next to `JobSpec.driver` because this module already owns *which* script
    runs a job type; this is *how* it gets started. Both callers must not drift on the
    interpreter choice or the PYTHONPATH export; both are the pipeline entry points in
    `driver_invocation` below. (`backend.extract_pick_list` used to be a third, passing
    its own argument set instead of `--instance_id`; that job now has a real instance,
    so it goes through `driver_invocation` like everything else.)

    The interpreter is conf.yaml's `crboost_python` when set, else the one running this
    process — the server's own venv/conda/uv environment on the headnode, and the same
    interpreter again when a driver re-invokes itself on a compute node.
    """
    python_exe = get_config_service().config.crboost_python or sys.executable
    if not os.access(python_exe, os.X_OK):
        raise FileNotFoundError(
            f"Driver interpreter {python_exe!r} is not executable (crboost_python in config/conf.yaml; "
            f"empty = the interpreter running the server)"
        )
    # Only this checkout on the path, and no ~/.local site-packages: whatever a cluster module or the user's
    # shell put on PYTHONPATH must not shadow the venv's packages.
    return f"export PYTHONPATH={server_dir} PYTHONNOUSERSITE=1; {python_exe} {driver_script}"


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
