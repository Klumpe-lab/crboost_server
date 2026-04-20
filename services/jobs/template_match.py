from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TemplateMatchPytomParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TEMPLATE_MATCH_PYTOM)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = False

    USER_PARAMS: ClassVar[Set[str]] = {
        "template_path",
        "mask_path",
        "angular_search",
        "symmetry",
        "defocus_weight",
        "dose_weight",
        "spectral_whitening",
        "random_phase_correction",
        "non_spherical_mask",
        "bandpass_filter",
        "gpu_split",
        "perdevice",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_tomograms",
            accepts=[JobFileType.DENOISED_TOMOGRAMS_STAR, JobFileType.TOMOGRAMS_STAR],
            preferred_source="denoisepredict",
        ),
        InputSlot(
            key="input_tiltseries",
            accepts=[JobFileType.FILTERED_TILT_SERIES_STAR, JobFileType.TS_CTF_TILT_SERIES_STAR],
            preferred_source="tiltFilter",
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_dir", produces=JobFileType.TM_RESULTS_DIR, path_template="tmResults/", is_dir=True),
        OutputSlot(key="output_tomograms", produces=JobFileType.TOMOGRAMS_STAR, path_template="tomograms.star"),
    ]

    # workbench is a nested model managed by the TemplateWorkbench widget,
    # NOT a user param — the widget handles its own persistence.
    # workbench: TemplateWorkbenchState = Field(default_factory=TemplateWorkbenchState)

    # Inputs (Strings here, resolved to Paths in resolve_paths)
    template_path: str = Field(default="")
    mask_path: str = Field(default="")

    # Algorithm Params
    angular_search: str = Field(default="12.0")
    symmetry: str = Field(default="C1")

    # Flags
    defocus_weight: bool = True
    dose_weight: bool = True
    spectral_whitening: bool = False
    random_phase_correction: bool = False
    non_spherical_mask: bool = False

    bandpass_filter: str = Field(default="None")
    gpu_split: str = Field(default="auto")
    perdevice: int = Field(default=1)
    array_throttle: int = Field(
        default=4, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tomogram template matching"
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        return [
            ("in_mic", str(self.paths.get("input_tomograms", ""))),
            ("in_3dref", str(self.template_path or "")),
            ("in_mask", str(self.mask_path or "")),
            ("in_coords", ""),
            ("in_mov", ""),
            ("in_part", ""),
        ]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """
        Override: TM's parent sbatch is a lightweight CPU-only supervisor that
        enumerates tomograms, submits a per-tomogram SLURM array, polls, and
        aggregates. The user-facing slurm config (per-task GPU resources) is
        consumed by the supervisor when it builds the array sbatch in
        drivers/template_match_pytom.py -- NOT by this supervisor's own sbatch.
        """
        sup = get_config_service().supervisor_slurm_defaults
        options = [
            ("do_queue", "Yes"),
            ("queuename", sup.partition),
            ("qsub", "sbatch"),
            ("qsubscript", "qsub.sh"),
            ("min_dedicated", "1"),
        ]
        for field_name, var_name in SlurmConfig.QSUB_EXTRA_MAPPING.items():
            options.append((var_name, str(getattr(sup, field_name))))
        return options

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "pytom"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"tomograms": "denoisepredict"}
