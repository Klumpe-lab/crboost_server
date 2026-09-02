from __future__ import annotations
from typing import ClassVar
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

# IsoNet2 `refine` cost model, measured on copia_demo (clip-g4, 1 tomogram at 11.8 A/px,
# cube_size 96, unet-medium, batch_size 4). An earlier flat 165-min base assumed refine ran
# "a FIXED ~10-epoch schedule"; both halves of that were wrong and cost a run:
#
#   * EPOCHS. `--epochs` defaults to 50, not 10, and `--save_interval` (10) only controls the
#     checkpoint/preview cadence -- the "Training for 20 to 30 epochs" log lines are intervals,
#     not the whole schedule. copia_demo's 2:50 allocation died at epoch 22/50.
#   * SCALING. An epoch is ONE FULL PASS over every crop of every training tomogram: IsoNet2
#     sets steps_per_epoch to a 2e8 sentinel and trains min(len(train_loader), steps_per_epoch)
#     (IsoNet2 models/train.py), so nothing caps it. Epoch cost is LINEAR in the tomogram
#     count -- 750 batches = 6.4 min for one tomogram, ~2 h for twenty. It is NOT constant.
#
# The preview after each save_interval is constant, not linear: `--prev_tomo_idx` defaults to 1,
# so only the first tomogram is predicted (~6.2 min) however many are being trained on.
_ISONET_SETUP_MIN = 3  # prepare_star + preprocess
_ISONET_MASK_MIN_PER_TOMO = 1  # make_mask, measured at 53 s/tomogram
_ISONET_EPOCH_MIN_PER_TOMO = 6.4
_ISONET_PREVIEW_MIN = 6.2
_ISONET_SAVE_INTERVAL = 10  # container default; checkpoints + previews land on this cadence


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

    USER_PARAMS: ClassVar[set[str]] = {
        "denoise_method",
        "tomograms_for_training",
        "number_training_subvolumes",
        "subvolume_dimensions",
        "perdevice",
        "isonet_method",
        "isonet_epochs",
        "isonet_max_training_tomograms",
        "isonet_deconv",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.TOMOGRAMS_STAR], preferred_source="tsReconstruct")
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
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
    isonet_epochs: int = Field(
        default=20,
        ge=1,
        le=200,
        description="IsoNet refine training epochs. The single biggest wall-time lever: an epoch is "
        "a full pass over every crop of every training tomogram, so cost is epochs × tomograms "
        "(≈6.4 min per epoch per tomogram). The container's own default is 50; 20 is used here "
        "because on copia_demo the specimen-region loss was flat from epoch ~10 (0.223 → 0.222 by "
        "epoch 20) and everything after that was background fitting. Keep it a multiple of 10 — "
        "IsoNet checkpoints and writes a preview every 10 epochs. Only used when "
        "denoise_method = IsoNet.",
    )
    isonet_max_training_tomograms: int = Field(
        default=2,
        ge=0,
        le=100,
        description="Hard cap on how many tomograms IsoNet trains on (0 = no cap, use every "
        "tomogram that passes tomograms_for_training). A denoiser generalises from a couple of "
        "tomograms, but IsoNet's epoch cost is LINEAR in the count — uncapped, a 40-tomogram "
        "project needs ~4 h per epoch and no allocation can cover it. The cap is what keeps the "
        "job's wall-time bounded by dataset size. Only used when denoise_method = IsoNet.",
    )
    isonet_deconv: bool = Field(
        default=False,
        description="Run IsoNet's CTF deconvolution before training. OFF by default because "
        "tsReconstruct already deconvolves (its 'deconv' param defaults to 1/on). Enable only if "
        "you reconstructed with deconv=0. Only used when denoise_method = IsoNet.",
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_tomoset", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "isonet" if self.denoise_method == DenoiseMethod.ISONET else "cryocare"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"reconstruct": "tsReconstruct"}

    def isonet_work_minutes(self, n_tomograms: int) -> int:
        """Minutes of actual work IsoNet refine needs for `n_tomograms`. Also called by the
        driver, which knows the REAL staged count (post-`tomograms_for_training`) and warns
        when the watchdog budget can't cover it."""
        n = max(n_tomograms, 1)
        if self.isonet_max_training_tomograms:
            n = min(n, self.isonet_max_training_tomograms)
        previews = max(1, self.isonet_epochs // _ISONET_SAVE_INTERVAL)
        return int(
            _ISONET_SETUP_MIN
            + _ISONET_MASK_MIN_PER_TOMO * n
            + _ISONET_EPOCH_MIN_PER_TOMO * self.isonet_epochs * n
            + _ISONET_PREVIEW_MIN * previews
        )

    def _scaled_train_walltime(self, base_time: str) -> str:
        """Scale `base_time` to the tilt-series count, floored at `base_time` and capped.

        The count is ProjectState's in-memory `import_selected_tilt_series` — no disk I/O, and
        known at deploy time before the upstream tomograms.star exists. A narrowing
        `tomograms_for_training` filter makes this an OVER-estimate, which is the safe direction
        (the job just finishes early); the driver logs the real count once it has staged."""
        is_isonet = self.denoise_method == DenoiseMethod.ISONET
        n_ts = getattr(self._project_state, "import_selected_tilt_series", 0) or 0
        if is_isonet:
            # /0.9: run_command's watchdog only lets the job spend 90% of --time.
            minutes = int(self.isonet_work_minutes(n_ts) / 0.9) + 1
        elif n_ts <= 0:
            return base_time  # cryoCARE can't be estimated without a count -> profile default
        else:
            minutes = _TRAIN_WALLTIME_BASE_MIN + _TRAIN_WALLTIME_PER_TS_MIN * n_ts
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
