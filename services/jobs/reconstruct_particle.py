from __future__ import annotations
from enum import Enum
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams, SymmetryGroup
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class ReconstructTheme(str, Enum):
    """Backend selector for relion_tomo_reconstruct_particle.
    `default` = RELION 5 backend; `classic` = legacy RELION 4 algorithm
    (used by older published workflows for reproducibility)."""

    DEFAULT = "default"
    CLASSIC = "classic"


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
        "snr",
        "theme",
        "whiten",
        "no_ctf",
        "do_helix",
        "helical_twist",
        "helical_rise",
        "helical_z_percentage",
        "helical_tube_outer_diameter",
        "helical_nr_asu",
        "threads",
        "threads_in",
        "threads_out",
        "other_args",
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

    # Reconstruction backend + SNR
    snr  : float            = Field(default=0.0, ge=0.0, description="Assumed SNR (0 = auto)")
    theme: ReconstructTheme = Field(
        default=ReconstructTheme.DEFAULT, description="Backend: default (RELION 5) or classic (RELION 4 legacy)"
    )

    # Noise / CTF
    whiten: bool = Field(default=False, description="Whiten noise by flattening power spectrum")
    no_ctf: bool = Field(default=False, description="Do not apply CTFs")

    # Helical (only emitted when do_helix=True)
    do_helix                   : bool  = Field(default=False, description="Apply helical reconstruction")
    helical_twist              : float = Field(default=-1.0, description="Helical twist (deg); -1 disables")
    helical_rise               : float = Field(default=0.0, description="Helical rise (Å)")
    helical_z_percentage       : float = Field(default=30.0, description="Helical z-percentage")
    helical_tube_outer_diameter: float = Field(default=-1.0, description="Helical tube outer diameter (Å); -1 disables")
    helical_nr_asu             : int   = Field(default=1, ge=1, description="Number of asymmetric units along helix")

    # Threading (CPU-only job, no GPU)
    threads    : int = Field(default=6, ge=1, description="Total OMP threads (--j)")
    threads_in : int = Field(default=3, ge=1, description="Inner threads (slower, less memory)")
    threads_out: int = Field(default=2, ge=1, description="Outer threads (faster, more memory)")

    # Free-form passthrough for obscure flags (--no_psf, --margin, --mem, ...)
    other_args: str = Field(default="", description="Extra CLI flags appended verbatim to the relion command")

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
