from __future__ import annotations
from typing import ClassVar
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

    USER_PARAMS: ClassVar[set[str]] = {
        "rescale_angpixs",
        "halfmap_frames",
        "halfmap_tilts",
        "deconv",
        "perdevice",
        "array_throttle",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        # The tilt cut propagates natively (tiltFilter trims the tomostar
        # upstream of alignment), so tsCtf's star already contains only kept tilts.
        InputSlot(key="input_star", accepts=[JobFileType.TS_CTF_TILT_SERIES_STAR], preferred_source="tsCtf"),
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
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.TOMOGRAMS_STAR, path_template="tomograms.star"),
        OutputSlot(
            key="output_processing",
            produces=JobFileType.WARP_TILTSERIES_DIR,
            path_template="warp_tiltseries/",
            is_dir=True,
        ),
    ]

    rescale_angpixs: float = Field(
        default=12.0,
        ge=2.0,
        le=50.0,
        title="Pixel size (Å/px)",
        description="Voxel size of the reconstructed tomogram in Å (Warp --angpix); decimals are fine. The tilt "
        "images are Fourier-binned from the acquisition pixel size down to this, so doubling it gives 8× fewer "
        "voxels and drops everything finer than 2× this value (Nyquist). Set at project creation to the "
        "acquisition pixel size × the configured reconstruction binning.",
    )
    halfmap_frames: int = Field(default=1, ge=0, le=1)
    halfmap_tilts: int = Field(
        default=0,
        ge=0,
        le=1,
        description="1 = the even/odd half-tomograms are built from alternate tilts (by angle) instead of "
        "frame halves: they then carry independent alignment errors, which a tilt-split FSC between them "
        "measures. Warp writes both kinds to the same even/odd folders, so set halfmap_frames to 0; "
        "denoise training on these halves learns from tilt halves.",
    )
    deconv: int = Field(
        default=0,
        ge=0,
        le=1,
        description="1 = also write a CTF-deconvolved copy (reconstruction/deconv/). Warp's --deconv deconvolves "
        "the even/odd half-tomograms too, and those are what cryoCARE and IsoNet train and predict on, so keep "
        "it 0 when the pipeline denoises: IsoNet refuses deconvolved halves and makes its own deconvolved mask copy.",
    )
    perdevice: int = Field(default=1, ge=0, le=8)
    array_throttle: int = Field(
        default=20, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tilt-series reconstruction"
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def _get_queue_options(self) -> list[tuple[str, str]]:
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
        return "warp_aretomo"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"ctf": "tsCtf"}
