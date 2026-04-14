from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple

from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TsImportParams(AbstractJobParams):
    """
    Lightweight job that runs WarpTools ts_import + create_settings.

    Converts mdoc + processed frame-series into tomostar files and creates the
    warp_tiltseries.settings needed by all downstream TS-processing jobs.
    No GPU work — purely metadata assembly.
    """

    job_type: JobType = Field(default=JobType.TS_IMPORT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {"mdoc_pattern", "min_intensity", "do_at_most", "tomo_dimensions"}

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf"),
        InputSlot(
            key="input_processing", accepts=[JobFileType.WARP_FRAMESERIES_DIR], preferred_source="fsMotionAndCtf"
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="tomostar_dir", produces=JobFileType.TOMOSTAR_DIR, path_template="tomostar/", is_dir=True),
        OutputSlot(
            key="warp_tiltseries_settings",
            produces=JobFileType.WARP_TILTSERIES_SETTINGS,
            path_template="warp_tiltseries.settings",
        ),
    ]

    mdoc_pattern: str = Field(default="*.mdoc")
    # WarpTools `ts_import` runs a contiguous-run intensity walk outward from
    # 0-tilt and truncates all tilts past the first one whose
    # `AverageIntensity >= this * cos(angle) * MaxAverage * 0.999` check fails
    # (see ImportTiltseries.cs:335-350 and TS_REGISTRY_REFACTOR_PLAN.md).
    # Setting to 0 makes the threshold 0, but frames with negative-median
    # MRCs (common for zero-mean motion-corrected high-tilt averages) still
    # fail `>= 0` and get dropped. CLI validation rejects values < 0, so the
    # filter cannot be fully disabled. Raising this value filters MORE
    # aggressively. The ts_alignment / ts_ctf adapters now silently skip
    # rows for frames WarpTools dropped (matching legacy CryoBoost behavior
    # in src/warp/tsAlignment.py:132-140), so setting min_intensity > 0 no
    # longer crashes the pipeline — it just excludes more tilts from the
    # reconstruction.
    min_intensity: float = Field(default=0.0, ge=0.0, le=1.0)
    do_at_most: int = Field(default=-1)
    tomo_dimensions: str = Field(default="4096x4096x2048")

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """
        ts_import is lightweight metadata assembly — no GPU needed.
        Use the supervisor SLURM config (minimal resources).
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
        return {"motion": "fsMotionAndCtf"}
