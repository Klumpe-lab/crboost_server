from __future__ import annotations
from pathlib import Path
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TiltFilterParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TILT_FILTER)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_INTERACTIVE: ClassVar[bool] = True

    USER_PARAMS: ClassVar[Set[str]] = {"model_name", "image_size", "dl_batch_size", "prob_threshold", "prob_action"}

    # Runs after tsImport, before alignment, so the cut actually filters
    # alignment/CTF/reconstruct instead of only the display copy. The DL reads
    # the motion-corrected averages via the fs-motion star; the verdict is applied
    # by trimming the tomostar (drivers/tilt_filter.py), which every downstream
    # WarpTools step reads. When this job is absent, alignment's tomostar_dir slot
    # falls back to tsImport's tomostar and the pipeline is unchanged.
    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf"),
        InputSlot(key="input_tomostar", accepts=[JobFileType.TOMOSTAR_DIR], preferred_source="tsImport"),
    ]

    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_tomostar", produces=JobFileType.TOMOSTAR_DIR, path_template="tomostar/", is_dir=True)
    ]

    model_name: str = Field(default="default", description="DL model name for tilt quality classification")
    image_size: int = Field(default=384, ge=128, le=1024, description="Target image size for DL inference")
    dl_batch_size: int = Field(default=32, ge=1, le=256, description="Batch size for DL inference")
    prob_threshold: float = Field(default=0.1, ge=0.0, le=1.0, description="Probability threshold for classification")
    prob_action: str = Field(default="assignToGood", description="Action for low-confidence predictions")
    tilt_labels: Dict[str, str] = Field(default_factory=dict, description="Manual good/bad label overrides by tilt key")

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "crboost"

    @staticmethod
    def get_output_assets(job_dir: Path) -> Dict[str, Path]:
        return {
            "filtered_star": job_dir / "filtered" / "tiltseries_filtered.star",
            "labeled_star": job_dir / "filtered" / "tiltseries_labeled.star",
        }

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"ctf": "tsCtf"}
