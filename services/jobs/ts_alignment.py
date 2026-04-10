from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple

from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, AlignmentMethod
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TsAlignmentParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TS_ALIGNMENT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {
        "alignment_method",
        "rescale_angpixs",
        "tomo_dimensions",
        "sample_thickness_nm",
        "do_at_most",
        "perdevice",
        "patch_x",
        "patch_y",
        "axis_iter",
        "axis_batch",
        "imod_patch_size",
        "imod_overlap",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf"),
        InputSlot(key="tomostar_dir", accepts=[JobFileType.TOMOSTAR_DIR], preferred_source="tsImport"),
        InputSlot(
            key="warp_tiltseries_settings", accepts=[JobFileType.WARP_TILTSERIES_SETTINGS], preferred_source="tsImport"
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(
            key="output_star", produces=JobFileType.ALIGNED_TILT_SERIES_STAR, path_template="aligned_tilt_series.star"
        ),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
        OutputSlot(
            key="warp_tiltseries_settings",
            produces=JobFileType.WARP_TILTSERIES_SETTINGS,
            path_template="warp_tiltseries.settings",
        ),
    ]

    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    rescale_angpixs: float = Field(default=12.0, ge=2.0, le=50.0)
    tomo_dimensions: str = Field(default="4096x4096x2048")
    sample_thickness_nm: float = Field(default=180.0, ge=50.0, le=1000.0)
    do_at_most: int = Field(default=-1)
    perdevice: int = Field(default=1, ge=0, le=8)
    patch_x: int = Field(default=0, ge=0)
    patch_y: int = Field(default=0, ge=0)
    axis_iter: int = Field(default=0, ge=0)
    axis_batch: int = Field(default=0, ge=0)
    imod_patch_size: int = Field(default=200)
    imod_overlap: int = Field(default=50)
    array_throttle: int = Field(
        default=8, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series alignment"
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """
        Override: alignment supervisor is a lightweight CPU-only job.
        The user-facing slurm config describes PER-TASK resources for the array.
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
        return {"ts_import": "tsImport"}
