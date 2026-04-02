from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams, SymmetryGroup
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class ReconstructParticleParams(AbstractJobParams):
    """
    Subtomogram reconstruction using relion_tomo_reconstruct_particle.
    Produces an initial average (merged.mrc) and half-maps from extracted particles.
    """

    job_type: JobType = Field(default=JobType.RECONSTRUCT_PARTICLE)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {
        "box_size",
        "crop_size",
        "symmetry",
        "binning",
        "whiten",
        "no_ctf",
        "threads",
        "threads_in",
        "threads_out",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="subtomoExtraction"
        )
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_map", produces=JobFileType.REFERENCE_MAP, path_template="merged.mrc")
    ]

    # Reconstruction parameters
    box_size : int           = Field(default=384, description="Box size in pixels")
    crop_size: int           = Field(default=224, description="Cropped box size (-1 = no cropping)")
    symmetry : SymmetryGroup = Field(default=SymmetryGroup.C1, description="Point group symmetry")
    binning  : int           = Field(default=1, ge=1, description="Binning factor")

    # Noise / CTF
    whiten: bool = Field(default=False, description="Whiten noise by flattening power spectrum")
    no_ctf: bool = Field(default=False, description="Do not apply CTFs")

    # Threading (CPU-only job, no GPU)
    threads    : int = Field(default=6, ge=1, description="Total OMP threads (--j)")
    threads_in : int = Field(default=3, ge=1, description="Inner threads (slower, less memory)")
    threads_out: int = Field(default=2, ge=1, description="Outer threads (faster, more memory)")

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        return [("in_optimisation", str(input_opt))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"optimisation_set": "subtomoExtraction"}
