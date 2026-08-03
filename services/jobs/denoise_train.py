from __future__ import annotations
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, DenoiseMethod, IsoNetRefineMethod
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmConfig


# ── Dynamic training walltime ─────────────────────────────────────────────────
# denoise_train is a SINGLE (non-array) job: it reduces ONE model from ALL the
# selected tilt-series, so its SLURM --time must cover every tomogram
# sequentially. A flat profile walltime (conf.yaml denoisetrain.time) silently
# truncates larger datasets -- the in-job watchdog kills the run at 0.9x the
# remaining walltime, which is what failed a 40-TS IsoNet refine (10 epochs x
# ~12 min + ~48 min prepare_star/make_mask, well over the 2 h allocation).
#
# Linear model `base + per_ts * n_selected_ts`, floored at the static profile
# time and capped. Sized for IsoNet refine (the slower path); cryoCARE
# (fixed-epoch training) lands under it and merely over-allocates harmlessly.
# The TS count is read in-memory from ProjectState -- no disk I/O, and known at
# deploy time before the upstream tomograms.star exists. A narrowing
# tomograms_for_training filter only makes the estimate conservative (safe).
_TRAIN_WALLTIME_BASE_MIN = 30  # fixed overhead: prepare_star / make_mask / extract / I/O
_TRAIN_WALLTIME_PER_TS_MIN = 5  # marginal training cost per tilt-series
_TRAIN_WALLTIME_CAP_MIN = 8 * 60  # never request more than the partition realistically allows
# IsoNet2 `refine` runs a FIXED ~10-epoch schedule (~12 min/epoch ≈ 2 h) whose cost is
# dominated by the epoch count, NOT the tomogram count (make_mask/predict add a smaller
# per-TS term). The cryoCARE 30-min base under-allocates it: 4 TS -> 30+20=50 min, floored
# to the 2 h profile default, which the 0.9x in-job watchdog then trims below the ~2 h the
# refine actually needs -> killed mid-epoch. IsoNet gets a large fixed base sized to clear
# the watchdog (~2 h schedule + setup, with /0.9 headroom).
_ISONET_TRAIN_WALLTIME_BASE_MIN = 165


def _hms_to_minutes(t: str) -> int:
    """SLURM walltime ('H:MM:SS' or 'D-H:MM:SS') -> whole minutes (any seconds round up)."""
    try:
        days, hms = t.split("-", 1) if "-" in t else ("0", t)
        parts = [int(p) for p in hms.split(":")]
        if len(parts) == 3:
            h, m, s = parts
        elif len(parts) == 2:
            h, m, s = 0, parts[0], parts[1]
        else:
            return 0
        return int(days) * 1440 + h * 60 + m + (1 if s else 0)
    except (ValueError, IndexError):
        return 0


def _minutes_to_hms(minutes: int) -> str:
    h, m = divmod(max(0, int(minutes)), 60)
    return f"{h}:{m:02d}:00"


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

    def _scaled_train_walltime(self, base_time: str) -> str:
        """Scale `base_time` to the selected tilt-series count, floored at `base_time`
        and capped. IsoNet uses a large fixed base (its runtime is epoch-bound, ~constant
        in TS count); cryoCARE keeps the small base and returns `base_time` when the count
        is unknown (0)."""
        is_isonet = self.denoise_method == DenoiseMethod.ISONET
        n_ts = getattr(self._project_state, "import_selected_tilt_series", 0) or 0
        # cryoCARE can't be estimated without a count -> keep the profile default. IsoNet's
        # cost is a fixed schedule regardless of count, so still apply its (large) base floor.
        if n_ts <= 0 and not is_isonet:
            return base_time
        base_min = _ISONET_TRAIN_WALLTIME_BASE_MIN if is_isonet else _TRAIN_WALLTIME_BASE_MIN
        minutes = base_min + _TRAIN_WALLTIME_PER_TS_MIN * max(n_ts, 0)
        minutes = max(minutes, _hms_to_minutes(base_time))  # never below today's profile/default
        minutes = min(minutes, _TRAIN_WALLTIME_CAP_MIN)
        return _minutes_to_hms(minutes)

    def get_effective_slurm_config(self) -> SlurmConfig:
        # Single-job training walltime must cover ALL tilt-series; scale it to the
        # dataset size unless the user has pinned an explicit time override.
        cfg = super().get_effective_slurm_config()
        if "time" not in self.slurm_overrides:
            cfg.time = self._scaled_train_walltime(cfg.time)
        return cfg
