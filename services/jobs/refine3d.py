from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.jobs._base import AbstractJobParams, SymmetryGroup
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class Refine3DParams(AbstractJobParams):
    """
    Gold-standard 3D refinement: relion_refine_mpi --auto_refine --split_random_halves.
    Runs until RELION's own convergence criterion; the final map has no iteration tag.
    """

    job_type       : JobType               = Field(default=JobType.REFINE3D)
    JOB_CATEGORY   : ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str]         = "relion.external"
    USER_PARAMS    : ClassVar[set[str]]    = {
        "healpix_order",
        "auto_local_healpix_order",
        "offset_range",
        "offset_step",
        "symmetry",
        "ini_high",
        "low_resol_join_halves",
        "particle_diameter",
        "solvent_mask_path",
        "solvent_correct_fsc",
        "flatten_solvent",
        "zero_mask",
        "firstiter_cc",
        "do_ctf",
        "do_norm",
        "do_scale",
        "dont_combine_weights_via_disc",
        "pad",
        "use_gpu",
        "preread_images",
        "mpi_ranks",
        "threads",
        "pool",
        "random_seed",
    }
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="subtomoExtraction"
        ),
        InputSlot(key="input_reference", accepts=[JobFileType.REFERENCE_MAP], preferred_source="reconstructParticle"),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(
            key="output_optimisation",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="run_optimisation_set.star",
        ),
        OutputSlot(key="output_map", produces=JobFileType.REFERENCE_MAP, path_template="run_class001.mrc"),
    ]

    # Angular / translational search
    healpix_order           : int = Field(
        default=3, ge=1, le=6, description="Initial angular sampling (2=15deg, 3=7.5deg, 4=3.75deg)"
    )
    auto_local_healpix_order: int = Field(
        default=4, ge=1, le=8, description="Sampling from which auto-refine searches locally (4=3.75deg, 5=1.8deg)"
    )
    offset_range            : int = Field(default=5, ge=0, description="Translational search range (pixels)")
    offset_step             : int = Field(default=1, ge=1, description="Translational search step (pixels)")

    # Symmetry / filtering
    symmetry             : SymmetryGroup = Field(default=SymmetryGroup.C1, description="Point group symmetry")
    ini_high             : float         = Field(default=40.0, ge=0, description="Initial low-pass filter (Angstroms)")
    low_resol_join_halves: float         = Field(
        default=40.0, ge=0, description="Half-maps are averaged below this resolution (Angstroms)"
    )
    particle_diameter    : float         = Field(default=-1.0, description="Mask diameter (Angstroms, -1 = auto)")

    # Optional mask
    solvent_mask_path  : str  = Field(default="", description="Path to soft mask for references (optional)")
    solvent_correct_fsc: bool = Field(default=True, description="With a solvent mask: report the mask-corrected FSC")

    # Reference handling
    flatten_solvent: bool = Field(default=True, description="Apply mask to references during refinement")
    zero_mask      : bool = Field(default=True, description="Set outside-mask voxels to zero during refinement")
    firstiter_cc   : bool = Field(
        default=True, description="Use CC in first iteration (reference greyscale not on the particles' scale)"
    )

    # CTF / normalisation
    do_ctf                       : bool = Field(default=True, description="Apply CTF correction")
    do_norm                      : bool = Field(default=True, description="Normalise particle images")
    do_scale                     : bool = Field(default=True, description="Correct for intensity scale differences")
    dont_combine_weights_via_disc: bool = Field(
        default=True, description="Keep combination of weights in memory (faster, needs more RAM)"
    )
    pad                          : int  = Field(default=2, ge=1, description="Fourier padding factor (2 = standard)")

    # Computation
    use_gpu       : bool = Field(default=True, description="Use GPU acceleration")
    preread_images: bool = Field(default=True, description="Pre-read particles into RAM (every MPI follower)")
    mpi_ranks     : int  = Field(default=3, ge=3, description="MPI processes: leader + one per half-set (min 3)")
    threads       : int  = Field(default=3, ge=1, description="Threads per MPI process (--j)")
    pool          : int  = Field(default=30, ge=1, description="Number of particles to pool per thread")
    random_seed   : int  = Field(default=-1, ge=-1, description="relion_refine --random_seed (-1 = time-based)")

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
