from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, DenoiseMethod, IsoNetRefineMethod
from services.io_slots import InputSlot, OutputSlot, JobFileType


class DenoiseTrainParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.DENOISE_TRAIN)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    USER_PARAMS: ClassVar[Set[str]] = {
        "denoise_method",
        "tomograms_for_training",
        "number_training_subvolumes",
        "subvolume_dimensions",
        "perdevice",
        "isonet_method",
        "isonet_deconv",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.TOMOGRAMS_STAR], preferred_source="tsReconstruct")
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_model", produces=JobFileType.DENOISE_MODEL_TAR, path_template="denoising_model.tar.gz")
    ]

    denoise_method: DenoiseMethod = DenoiseMethod.CRYOCARE
    tomograms_for_training: str = Field(
        default="",
        description="Substring filter on the reconstructed-tomogram filename — only tomograms whose "
        "name CONTAINS this string are used for training (empty = all). It is a SUBSTRING match, so "
        "e.g. 'Position_1' also matches Position_10, Position_11, … Position_19.",
    )
    number_training_subvolumes: int = Field(default=600, ge=100)
    subvolume_dimensions: int = Field(default=64, ge=32)
    perdevice: int = Field(default=1)
    # IsoNet-only (ignored when denoise_method == cryoCARE):
    isonet_method: IsoNetRefineMethod = Field(
        default=IsoNetRefineMethod.AUTO,
        description="IsoNet refine strategy. 'auto' picks isonet2-n2n from even/odd halves "
        "(missing-wedge correction + denoising). Only used when denoise_method = IsoNet.",
    )
    isonet_deconv: bool = Field(
        default=False,
        description="Run IsoNet's CTF deconvolution before training. OFF by default because "
        "tsReconstruct already deconvolves (its 'deconv' param defaults to 1/on). Enable only if "
        "you reconstructed with deconv=0. Only used when denoise_method = IsoNet.",
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_tomoset", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "isonet" if self.denoise_method == DenoiseMethod.ISONET else "cryocare"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"reconstruct": "tsReconstruct"}
