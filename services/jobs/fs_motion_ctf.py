from __future__ import annotations
from typing import ClassVar, Dict, List, Optional, Set, Tuple
from pydantic import Field

from services.computing.slurm_service import SlurmConfig
from services.configs.config_service import get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class FsMotionCtfParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.FS_MOTION_CTF)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    USER_PARAMS: ClassVar[Set[str]] = {
        "m_range_min_max",
        "m_bfac",
        "m_grid",
        "c_range_min_max",
        "c_defocus_min_max",
        "c_grid",
        "c_use_sum",
        "c_window",
        "out_average_halves",
        "out_skip_first",
        "out_skip_last",
        "perdevice",
        "do_at_most",
        "gain_operations",
        "do_phase",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.TILT_SERIES_STAR], preferred_source="importmovies")
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.FS_MOTION_CTF_STAR, path_template="fs_motion_and_ctf.star"),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_FRAMESERIES_DIR,
            path_template="warp_frameseries/",
            is_dir=True,
        ),
        OutputSlot(
            key="warp_frameseries_settings",
            produces=JobFileType.WARP_FRAMESERIES_SETTINGS,
            path_template="warp_frameseries.settings",
        ),
    ]

    do_phase: bool = Field(default=False, description="Estimate phase shifts (CTF phase plate or spurious phase)")
    m_range_min_max: str = Field(default="500:10", description="Motion estimation range min:max in Angstroms")
    m_bfac: int = Field(default=-500, description="B-factor for motion estimation (negative = more smoothing)")
    m_grid: str = Field(default="1x1x3", description="Motion estimation grid XxYxZ")
    c_range_min_max: str = Field(default="30:6.0", description="CTF fitting resolution range min:max in Angstroms")
    c_defocus_min_max: str = Field(default="1.1:8", description="Defocus search range min:max in microns")
    c_grid: str = Field(default="2x2x1", description="CTF estimation grid XxYxZ")
    c_use_sum: bool = Field(default=False, description="Use frame sum for CTF estimation instead of individual frames")
    c_window: int = Field(default=512, ge=128, description="CTF estimation window size in pixels")
    out_average_halves: bool = Field(default=True, description="Output half-set averages for independent validation")
    out_skip_first: int = Field(default=0, description="Skip this many initial tilts")
    out_skip_last: int = Field(default=0, description="Skip this many final tilts")
    perdevice: int = Field(default=2, ge=0, le=8, description="Parallel tilt series per GPU")
    do_at_most: int = Field(default=-1, description="Process at most N tilt series (-1 = all)")
    gain_operations: Optional[str] = Field(default=None, description="Gain reference operations (e.g. flip, rotate)")
    array_throttle: int = Field(
        default=20, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series motion/CTF"
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """
        Override: fs_motion_and_ctf supervisor is lightweight (reads STAR, dispatches array).
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

    @property
    def m_range_min(self) -> int:
        return int(self.m_range_min_max.split(":")[0])

    @property
    def m_range_max(self) -> int:
        return int(self.m_range_min_max.split(":")[1])

    @property
    def c_range_min(self) -> float:
        return float(self.c_range_min_max.split(":")[0])

    @property
    def c_range_max(self) -> float:
        return float(self.c_range_min_max.split(":")[1])

    @property
    def defocus_min_microns(self) -> float:
        return float(self.c_defocus_min_max.split(":")[0])

    @property
    def defocus_max_microns(self) -> float:
        return float(self.c_defocus_min_max.split(":")[1])

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"import": "importmovies"}
