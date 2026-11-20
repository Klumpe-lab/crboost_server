from __future__ import annotations
from typing import ClassVar

from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.jobs._base import M, S, AbstractJobParams, form
from services.models_base import JobType, JobCategory, AlignmentMethod
from services.io_slots import InputSlot, OutputSlot, JobFileType


class TsAlignmentParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TS_ALIGNMENT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[set[str]] = {
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
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("Alignment", "AreTomo", "IMOD", "Processing")

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf"),
        # tsImport is the sole TOMOSTAR_DIR producer -- there is nothing to choose
        # between. The tilt-filter's cut is not a separate tomostar dir; it is a
        # per-frame verdict in the registry that this job's supervisor applies while
        # snapshotting these tomostars (see drivers/ts_alignment.py enumerate_items).
        InputSlot(key="tomostar_dir", accepts=[JobFileType.TOMOSTAR_DIR], preferred_source="tsImport"),
        InputSlot(
            key="warp_tiltseries_settings", accepts=[JobFileType.WARP_TILTSERIES_SETTINGS], preferred_source="tsImport"
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
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

    alignment_method: AlignmentMethod = Field(
        default=AlignmentMethod.ARETOMO, title="Method", json_schema_extra=form("Alignment", M)
    )
    rescale_angpixs: float = Field(
        default=12.0, ge=2.0, le=50.0, title="Pixel size", json_schema_extra=form("Alignment", S, "Å/px")
    )
    tomo_dimensions: str = Field(
        default="4096x4096x2048", title="Tomogram size", json_schema_extra=form("Alignment", M, "px")
    )
    sample_thickness_nm: float = Field(
        default=180.0, ge=50.0, le=1000.0, title="Sample thickness", json_schema_extra=form("Alignment", S, "nm")
    )
    do_at_most: int = Field(default=-1, title="Max series", json_schema_extra=form("Processing", S))
    perdevice: int = Field(default=1, ge=0, le=8, title="Per GPU", json_schema_extra=form("Processing", S))
    patch_x: int = Field(default=0, ge=0, title="Patches X", json_schema_extra=form("AreTomo", S))
    patch_y: int = Field(default=0, ge=0, title="Patches Y", json_schema_extra=form("AreTomo", S))
    axis_iter: int = Field(default=0, ge=0, title="Axis iterations", json_schema_extra=form("AreTomo", S))
    axis_batch: int = Field(default=0, ge=0, title="Axis batch", json_schema_extra=form("AreTomo", S))
    imod_patch_size: int = Field(default=200, title="Patch size", json_schema_extra=form("IMOD", S, "px"))
    imod_overlap: int = Field(default=50, title="Patch overlap", json_schema_extra=form("IMOD", S))
    array_throttle: int = Field(
        default=40, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series alignment"
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> list[tuple[str, str]]:
        """
        Override: alignment supervisor is a lightweight CPU-only job.
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

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"ts_import": "tsImport"}
