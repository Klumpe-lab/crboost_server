from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.jobs._base import M, S, AbstractJobParams, form
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TsCtfParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TS_CTF)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[set[str]] = {
        "window",
        "range_min_max",
        "defocus_hand",
        "defocus_min_max",
        "perdevice",
        "do_phase",
        "array_throttle",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("CTF", "Processing")

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.ALIGNED_TILT_SERIES_STAR], preferred_source="aligntiltsWarp"),
        InputSlot(key="input_processing", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="aligntiltsWarp"),
        InputSlot(
            key="warp_tiltseries_settings",
            accepts=[JobFileType.WARP_TILTSERIES_SETTINGS],
            preferred_source="aligntiltsWarp",
        ),
    ]

    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(
            key="output_star", produces=JobFileType.TS_CTF_TILT_SERIES_STAR, path_template="ts_ctf_tilt_series.star"
        ),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
    ]

    do_phase: bool = Field(
        default=False, description="Estimate phase shifts (CTF phase plate or spurious phase)",
        title="Estimate phase shifts", json_schema_extra=form("CTF"),
    )
    window: int = Field(default=512, ge=128, le=2048, title="Window", json_schema_extra=form("CTF", S, "px"))
    # The same band as fsMotionAndCtf; ts_ctf only refines its defoci.
    range_min_max: str = Field(default="30:10", title="Resolution range", json_schema_extra=form("CTF", M, "Å"))
    defocus_hand: str = Field(default="set_flip", title="Defocus hand", json_schema_extra=form("CTF", M))
    defocus_min_max: str = Field(default="1.1:8", title="Defocus range", json_schema_extra=form("CTF", M, "µm"))
    perdevice: int = Field(default=1, ge=0, le=8, title="Per GPU", json_schema_extra=form("Processing", S))
    array_throttle: int = Field(
        default=40, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series CTF estimation"
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> list[tuple[str, str]]:
        """
        Override: tsCtf supervisor is a lightweight CPU-only job.
        The user-facing slurm config describes PER-TASK resources for the array.
        """
        sup = self.supervisor_slurm()
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
        return "warp_aretomo"

    @property
    def range_min(self) -> float:
        return float(self.range_min_max.split(":")[0])

    @property
    def range_max(self) -> float:
        return float(self.range_min_max.split(":")[1])

    @property
    def defocus_min(self) -> float:
        return float(self.defocus_min_max.split(":")[0])

    @property
    def defocus_max(self) -> float:
        return float(self.defocus_min_max.split(":")[1])

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"alignment": "aligntiltsWarp"}
