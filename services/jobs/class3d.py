from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.jobs._base import L, M, S, AbstractJobParams, SymmetryGroup, form
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmPreset


class Class3DParams(AbstractJobParams):
    """
    3D Classification using relion_refine (without --auto_refine).
    """

    job_type: JobType = Field(default=JobType.CLASS3D)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    USER_PARAMS: ClassVar[set[str]] = {
        "n_classes",
        "n_iterations",
        "tau_fudge",
        "healpix_order",
        "offset_range",
        "offset_step",
        "sigma_ang",
        "symmetry",
        "ini_high",
        "particle_diameter",
        "solvent_mask_path",
        "mask_from_job",
        "skip_align",
        "flatten_solvent",
        "firstiter_cc",
        "use_gpu",
        "preread_images",
        "mpi_ranks",
        "threads",
        "pool",
        "oversampling",
        "do_ctf",
        "do_norm",
        "do_scale",
        "zero_mask",
        "pad",
        "dont_combine_weights_via_disc",
        "random_seed",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = (
        "Classification",
        "Reference & mask",
        "Angles & offsets",
        "CTF & scale",
        "Compute",
    )
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_optimisation",
            accepts=[JobFileType.OPTIMISATION_SET_STAR],
            preferred_source="subtomoExtraction",
            needs_extracted=True,
        ),
        InputSlot(key="input_reference", accepts=[JobFileType.REFERENCE_MAP], preferred_source="reconstructParticle"),
        # A Mask job's mask, read only while mask_from_job is set; solvent_mask_path stays for a file made elsewhere.
        InputSlot(
            key="input_mask",
            accepts=[JobFileType.MASK],
            preferred_source="maskCreate",
            when="mask_from_job",
            beside=True,
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(
            key="output_optimisation",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="run_optimisation_set.star",
        )
    ]

    # Classification
    n_classes: int = Field(
        default=1,
        ge=1,
        description="Number of classes (1 for initial alignment, >1 for sorting)",
        title="Classes",
        json_schema_extra=form("Classification", S),
    )
    n_iterations: int = Field(
        default=15,
        ge=1,
        description="Number of iterations",
        title="Iterations",
        json_schema_extra=form("Classification", S),
    )
    tau_fudge: float = Field(
        default=1.0,
        description="Regularisation parameter (-1 = auto)",
        title="T",
        json_schema_extra=form("Classification", S),
    )

    # Angular sampling
    healpix_order: int = Field(
        default=3,
        ge=1,
        le=6,
        description="Angular sampling (2=15deg, 3=7.5deg, 4=3.75deg)",
        title="Healpix order",
        json_schema_extra=form("Angles & offsets", S),
    )
    offset_range: int = Field(
        default=5,
        ge=0,
        description="Translational search range (pixels)",
        title="Offset range",
        json_schema_extra=form("Angles & offsets", S, "px"),
    )
    offset_step: int = Field(
        default=2,
        ge=1,
        description="Translational search step (pixels)",
        title="Offset step",
        json_schema_extra=form("Angles & offsets", S, "px"),
    )
    sigma_ang: float = Field(
        default=-1.0,
        description="Local angular search sigma in degrees (-1 = no local search)",
        title="Sigma angle",
        json_schema_extra=form("Angles & offsets", S, "°"),
    )
    oversampling: int = Field(
        default=1,
        ge=0,
        description="Oversampling order",
        title="Oversampling",
        json_schema_extra=form("Angles & offsets", S),
    )

    # Symmetry / filtering
    symmetry: SymmetryGroup = Field(
        default=SymmetryGroup.C1,
        description="Point group symmetry",
        title="Symmetry",
        json_schema_extra=form("Classification", M),
    )
    ini_high: float = Field(
        default=45.0,
        ge=0,
        description="Initial low-pass filter (Angstroms)",
        title="Low-pass reference",
        json_schema_extra=form("Reference & mask", S, "Å"),
    )
    particle_diameter: float = Field(
        default=-1.0,
        description="Mask diameter (Angstroms, -1 = auto)",
        title="Mask diameter",
        json_schema_extra=form("Reference & mask", S, "Å"),
    )

    # Optional mask
    solvent_mask_path: str = Field(
        default="",
        description="Path to soft mask for references (optional)",
        title="Solvent mask",
        json_schema_extra=form("Reference & mask", L),
    )
    mask_from_job: bool = Field(
        default=False,
        description="Take the solvent mask from a Mask job (the mask input) instead of a file path",
        title="Mask from a job",
        json_schema_extra=form("Reference & mask"),
    )
    # Focused classification: classes from the poses the particles carry, no search, usually with a mask on the
    # region whose states differ (relion_refine --skip_align).
    skip_align: bool = Field(
        default=False,
        description="Classify without aligning: keep the input poses (focused classification)",
        title="Skip alignment",
        json_schema_extra=form("Angles & offsets"),
    )

    # Reference handling
    flatten_solvent: bool = Field(
        default=True,
        description="Apply mask to references during refinement",
        title="Flatten solvent",
        json_schema_extra=form("Reference & mask"),
    )
    zero_mask: bool = Field(
        default=True,
        description="Set outside-mask voxels to zero during refinement",
        title="Zero mask",
        json_schema_extra=form("Reference & mask"),
    )
    firstiter_cc: bool = Field(
        default=True,
        description="Use CC in first iteration (recommended when starting from rough reference)",
        title="First iteration CC",
        json_schema_extra=form("Reference & mask"),
    )

    # CTF / normalisation -- all on by default, matching standard tomo STA practice
    do_ctf: bool = Field(
        default=True, description="Apply CTF correction", title="CTF correction", json_schema_extra=form("CTF & scale")
    )
    do_norm: bool = Field(
        default=True, description="Normalise particle images", title="Normalise", json_schema_extra=form("CTF & scale")
    )
    do_scale: bool = Field(
        default=True,
        description="Correct for intensity scale differences",
        title="Scale",
        json_schema_extra=form("CTF & scale"),
    )
    dont_combine_weights_via_disc: bool = Field(
        default=True,
        description="Keep combination of weights in memory (faster, needs more RAM)",
        title="Weights in memory",
        json_schema_extra=form("Compute"),
    )
    pad: int = Field(
        default=2,
        ge=1,
        description="Padding factor for Fourier transforms (2 = standard)",
        title="Pad",
        json_schema_extra=form("Compute", S),
    )

    # Computation
    use_gpu: bool = Field(
        default=True, description="Use GPU acceleration", title="Use GPU", json_schema_extra=form("Compute")
    )
    preread_images: bool = Field(
        default=True,
        description="Pre-read all particles into RAM (every MPI follower)",
        title="Pre-read images",
        json_schema_extra=form("Compute"),
    )
    # One relion_refine, or a leader handing the particles out to followers that RELION spreads over the job's
    # GPUs (the SLURM gres) in turn: GPUs + 1 for one follower per GPU, 2 x GPUs + 1 to fill a large card.
    mpi_ranks: int = Field(
        default=1,
        ge=1,
        description="MPI processes: 1 = one relion_refine; N > 1 = relion_refine_mpi, a leader and N-1 followers over "
        "the job's GPUs (each follower pre-reads every particle)",
        title="MPI ranks",
        json_schema_extra=form("Compute", S),
    )
    threads: int = Field(
        default=4, ge=1, description="Threads per process (--j)", title="Threads", json_schema_extra=form("Compute", S)
    )
    pool: int = Field(
        default=30,
        ge=1,
        description="Number of particles to pool per thread",
        title="Pool",
        json_schema_extra=form("Compute", S),
    )
    # Reproducibility: -1 = RELION's own time-based seed; >= 0 is passed as
    # --random_seed so a run can be repeated exactly.
    random_seed: int = Field(
        default=-1,
        ge=-1,
        description="relion_refine --random_seed (-1 = time-based)",
        title="Random seed",
        json_schema_extra=form("Classification", S),
    )

    def __init__(self, **data):
        super().__init__(**data)
        # The GPU comes from conf.yaml's class3d profile, which names its type where a cluster mixes full GPUs
        # with MIG slices (roadmap 43 D2).
        if "slurm_overrides" not in data:
            self.slurm_overrides = {
                "mem": "64G",
                "cpus_per_task": 4,
                "time": "8:00:00",
                "preset": SlurmPreset.CUSTOM.value,
            }

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        input_ref = self.paths.get("input_reference", "")
        return [("in_optimisation", str(input_opt)), ("in_3dref", str(input_ref))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"optimisation_set": "subtomoExtraction", "reference": "reconstructParticle"}
