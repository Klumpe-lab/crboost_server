from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TsReconstructParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TS_RECONSTRUCT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {"rescale_angpixs", "halfmap_frames", "deconv", "perdevice", "array_throttle"}

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(
            key="input_star",
            accepts=[JobFileType.FILTERED_TILT_SERIES_STAR, JobFileType.TS_CTF_TILT_SERIES_STAR],
            preferred_source="tiltFilter",
        ),
        # input_processing must come from tsCtf (the canonical producer of the per-TS XMLs
        # with motion + alignment + CTF metadata). tiltFilter only filters STAR rows; it
        # does not modify the XMLs and its OUTPUT_SCHEMA's warp_tiltseries entry is a
        # conditional symlink that doesn't exist in older project layouts -- preferring
        # tiltFilter here causes the path resolver to fall through to a bogus
        # "External/pending_tiltFilter/warp_tiltseries" placeholder.
        InputSlot(key="input_processing", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="tsCtf"),
        InputSlot(
            key="warp_tiltseries_settings",
            accepts=[JobFileType.WARP_TILTSERIES_SETTINGS],
            preferred_source="aligntiltsWarp",
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.TOMOGRAMS_STAR, path_template="tomograms.star"),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
    ]

    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    halfmap_frames: int = Field(default=1, ge=0, le=1)
    deconv: int = Field(default=1, ge=0, le=1)
    perdevice: int = Field(default=1, ge=0, le=8)
    array_throttle: int = Field(
        default=20, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series reconstruction"
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """
        Override: ts_reconstruct's parent sbatch is a lightweight CPU-only supervisor.
        It only counts tilt-series, submits a child SLURM array job, polls until
        completion, and runs pure-Python metadata aggregation. The user-facing slurm
        config (project slurm_defaults + per-job slurm_overrides) describes PER-TASK
        resources and is consumed by the supervisor when it builds the array sbatch
        in drivers/ts_reconstruct.py -- NOT by this supervisor's own sbatch.
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
        return "warptools"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"ctf": "tsCtf"}
