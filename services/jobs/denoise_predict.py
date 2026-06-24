from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, DenoiseMethod
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmPreset, SlurmConfig
from services.configs.config_service import get_config_service


class DenoisePredictParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.DENOISE_PREDICT)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    USER_PARAMS: ClassVar[Set[str]] = {
        "denoise_method",
        "ntiles_x",
        "ntiles_y",
        "ntiles_z",
        "denoising_tomo_name",
        "perdevice",
        "array_throttle",
        "isonet_deconv",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="model_path", accepts=[JobFileType.DENOISE_MODEL_TAR], preferred_source="denoisetrain"),
        InputSlot(key="input_star", accepts=[JobFileType.TOMOGRAMS_STAR], preferred_source="tsReconstruct"),
        InputSlot(key="reconstruct_base", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="tsReconstruct"),
    ]

    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.DENOISED_TOMOGRAMS_STAR, path_template="tomograms.star")
    ]

    denoise_method: DenoiseMethod = DenoiseMethod.CRYOCARE
    ntiles_x: int = Field(default=4, ge=1)
    ntiles_y: int = Field(default=4, ge=1)
    ntiles_z: int = Field(default=4, ge=1)
    denoising_tomo_name: str = ""
    perdevice: int = Field(default=1)
    array_throttle: int = Field(
        default=20, ge=1, le=64, description="Max concurrent SLURM array tasks for per-tomogram denoise prediction"
    )
    # IsoNet-only (ignored when denoise_method == cryoCARE):
    isonet_deconv: bool = Field(
        default=False,
        description="Whether IsoNet's CTF deconvolution was applied. MUST match the denoise-train "
        "setting (the model expects the same input). OFF by default — tsReconstruct already "
        "deconvolves. Only used when denoise_method = IsoNet.",
    )

    def __init__(self, **data):
        super().__init__(**data)
        # Set cryoCARE-specific SLURM defaults
        if "slurm_overrides" not in data:
            self.slurm_overrides = {
                "gres": "gpu:1",
                "mem": "64G",
                "cpus_per_task": 4,
                "time": "4:00:00",
                "preset": SlurmPreset.CUSTOM.value,
            }

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "isonet" if self.denoise_method == DenoiseMethod.ISONET else "cryocare"

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """Per-tomogram prediction is parallelized over a SLURM array, so the parent sbatch
        is a lightweight CPU-only supervisor (enumerate tomograms, submit the child array,
        poll, aggregate the denoised tomograms.star). User-facing slurm config (project
        slurm_defaults + per-job slurm_overrides) describes PER-TASK resources and is consumed
        by the supervisor when it builds the array sbatch in drivers/denoise_predict.py.
        Mirrors TsReconstructParams._get_queue_options."""
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

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        model_path = self.paths.get("model_path", "")
        return [("in_tomoset", str(input_star)), ("in_model", str(model_path))]

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"train": "denoisetrain"}
