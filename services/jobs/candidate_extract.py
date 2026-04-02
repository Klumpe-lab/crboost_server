from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams, ExtractionCutoffMethod
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class CandidateExtractPytomParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TEMPLATE_EXTRACT_PYTOM)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {
        "particle_diameter_ang",
        "max_num_particles",
        "cutoff_method",
        "cutoff_value",
        "apix_score_map",
        "score_filter_method",
        "score_filter_value",
        "mask_fold_path",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_tm_job", accepts=[JobFileType.TM_RESULTS_DIR], preferred_source="templatematching"),
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.TOMOGRAMS_STAR, JobFileType.DENOISED_TOMOGRAMS_STAR],
            preferred_source="templatematching",
            required=True,
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.CANDIDATES_STAR, path_template="candidates.star"),
        OutputSlot(
            key="optimisation_set", produces=JobFileType.OPTIMISATION_SET_STAR, path_template="optimisation_set.star"
        ),
    ]

    # Particle params
    particle_diameter_ang: float = Field(default=200.0)
    max_num_particles: int = Field(default=1500)

    # Thresholding
    cutoff_method: ExtractionCutoffMethod = Field(default=ExtractionCutoffMethod.FALSE_POSITIVES)
    cutoff_value: float = Field(default=1.0)

    # Score map pixel size
    apix_score_map: str = Field(default="auto")

    # Filtering
    score_filter_method: str = Field(default="None")
    score_filter_value: str = Field(default="None")

    # Optional mask folder
    mask_fold_path: str = Field(default="None")

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_tm_job = self.paths.get("input_tm_job", "")
        return [("in_mic", str(input_tm_job))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"tm_job": "templatematching"}
