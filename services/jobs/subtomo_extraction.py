from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class SubtomoExtractionParams(AbstractJobParams):
    """
    Subtomogram extraction using RELION's relion_tomo_subtomo.
    Creates pseudo-subtomograms from tilt series for downstream averaging/classification.
    """

    job_type        : JobType               = Field(default=JobType.SUBTOMO_EXTRACTION)
    JOB_CATEGORY    : ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str]          = "relion.external"

    USER_PARAMS    : ClassVar[Set[str]]     = {
        "binning",
        "box_size",
        "crop_size",
        "do_float16",
        "do_stack2d",
        "max_dose",
        "min_frames",
    }

    # NOTE: additional_sources and merge_only are intentionally NOT in
    # USER_PARAMS. They are widget-managed state (the merge panel sets
    # them directly and triggers its own save). Keeping them out means:
    #   - No immutability enforcement (the merge panel can write them
    #     even after the job has run, which is correct — merging is a
    #     post-hoc operation on existing outputs)
    #   - No automatic dirty-marking (the merge panel calls save_handler
    #     explicitly after changing them)

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_optimisation", accepts=[JobFileType.OPTIMISATION_SET_STAR], preferred_source="tmextractcand"
        )
    ]

    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_particles", produces=JobFileType.PARTICLES_STAR, path_template="particles.star"),
        OutputSlot(
            key="output_optimisation", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        ),
    ]

    additional_sources: List[str] = Field(
        default_factory=list, description="Extra optimisation_set.star files or job dirs to merge"
    )
    merge_only: bool = Field(default=False, description="If true, skip relion_tomo_subtomo and only merge")

    # Extraction parameters
    binning  : float = Field(default=1.0, description="Binning factor relative to unbinned data")
    box_size : int   = Field(default=384, description="Box size in binned pixels")
    crop_size: int   = Field(default=224, description="Cropped box size (-1 = no cropping)")
    # Output format
    do_float16: bool = Field(default=True, description="Write output in float16 to save space")
    do_stack2d: bool = Field(default=True, description="Write as 2D stacks (preferred for RELION 4.1+)")

    # Filtering
    max_dose: float = Field(default=-1.0, description="Max dose to include (-1 = all)")
    min_frames: int = Field(default=1, description="Min frames per tilt to include")

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        return [("in_optimisation", str(input_opt))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"optimisation_set": "tmextractcand"}
