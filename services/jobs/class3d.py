from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams, SymmetryGroup
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmPreset


class Class3DParams(AbstractJobParams):
    """
    3D Classification using relion_refine (without --auto_refine).
    """

    job_type       : JobType               = Field(default=JobType.CLASS3D)
    JOB_CATEGORY   : ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str]         = "relion.external"
    USER_PARAMS    : ClassVar[Set[str]]    = {
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
        "flatten_solvent",
        "firstiter_cc",
        "use_gpu",
        "preread_images",
        "threads",
        "pool",
        "oversampling",
        "do_ctf",
        "do_norm",
        "do_scale",
        "zero_mask",
        "pad",
        "dont_combine_weights_via_disc",
    }
    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="subtomoExtraction"
        ),
        InputSlot(key="input_reference", accepts=[JobFileType.REFERENCE_MAP], preferred_source="reconstructParticle"),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(
            key="output_optimisation",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="run_optimisation_set.star",
        )
    ]

    # Classification
    n_classes   : int   = Field(default=1, ge=1, description="Number of classes (1 for initial alignment, >1 for sorting)")
    n_iterations: int   = Field(default=15, ge=1, description="Number of iterations")
    tau_fudge   : float = Field(default=1.0, description="Regularisation parameter (-1 = auto)")

    # Angular sampling
    healpix_order: int   = Field(default=3, ge=1, le=6, description="Angular sampling (2=15deg, 3=7.5deg, 4=3.75deg)")
    offset_range : int   = Field(default=5, ge=0, description="Translational search range (Angstroms)")
    offset_step  : int   = Field(default=2, ge=1, description="Translational search step (Angstroms)")
    sigma_ang    : float = Field(default=-1.0, description="Local angular search sigma in degrees (-1 = no local search)")
    oversampling : int   = Field(default=1, ge=0, description="Oversampling order")

    # Symmetry / filtering
    symmetry         : SymmetryGroup = Field(default=SymmetryGroup.C1, description="Point group symmetry")
    ini_high         : float         = Field(default=45.0, ge=0, description="Initial low-pass filter (Angstroms)")
    particle_diameter: float         = Field(default=-1.0, description="Mask diameter (Angstroms, -1 = auto)")

    # Optional mask
    solvent_mask_path: str = Field(default="", description="Path to soft mask for references (optional)")

    # Reference handling
    flatten_solvent: bool = Field(default=True, description="Apply mask to references during refinement")
    zero_mask      : bool = Field(default=True, description="Set outside-mask voxels to zero during refinement")
    firstiter_cc   : bool = Field(
        default=True, description="Use CC in first iteration (recommended when starting from rough reference)"
    )

    # CTF / normalisation -- all on by default, matching standard tomo STA practice
    do_ctf                        : bool = Field(default=True, description="Apply CTF correction")
    do_norm                       : bool = Field(default=True, description="Normalise particle images")
    do_scale                      : bool = Field(default=True, description="Correct for intensity scale differences")
    dont_combine_weights_via_disc: bool  = Field(default=True, description="Keep combination of weights in memory (faster, needs more RAM)")
    pad                          : int   = Field(default=2, ge=1, description="Padding factor for Fourier transforms (2 = standard)")

    # Computation
    use_gpu       : bool = Field(default=True, description="Use GPU acceleration")
    preread_images: bool = Field(default=True, description="Pre-read all particles into RAM")
    threads       : int  = Field(default=4, ge=1, description="Number of threads")
    pool          : int  = Field(default=30, ge=1, description="Number of particles to pool per thread")

    def __init__(self, **data):
        super().__init__(**data)
        if "slurm_overrides" not in data:
            self.slurm_overrides = {
                "gres": "gpu:1",
                "mem": "64G",
                "cpus_per_task": 4,
                "time": "8:00:00",
                "preset": SlurmPreset.CUSTOM.value,
            }

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        input_ref = self.paths.get("input_reference", "")
        return [("in_optimisation", str(input_opt)), ("in_3dref", str(input_ref))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"optimisation_set": "subtomoExtraction", "reference": "reconstructParticle"}
