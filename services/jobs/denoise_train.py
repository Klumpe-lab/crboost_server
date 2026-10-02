from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, DenoiseMethod, IsoNetCTFMode, IsoNetRefineMethod
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmConfig, get_cached_qos_maxwall_minutes


# ── Dynamic training walltime ─────────────────────────────────────────────────
# denoise_train is a single (non-array) job: it trains one model from all the
# selected tilt-series, so its SLURM --time must cover every tomogram
# sequentially. A flat profile walltime (conf.yaml denoisetrain.time) silently
# truncates larger datasets -- the in-job watchdog kills the run at 0.9x the
# remaining walltime (a 40-TS IsoNet refine needs 10 epochs x ~12 min plus
# ~48 min prepare_star/make_mask, well over a 2 h allocation).
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

# IsoNet2 `refine` cost model. An epoch is a fixed 3000 subtomograms however many tomograms
# train: prepare_star writes rlnNumberSubtomo = 3000 / n_tomograms, so adding tomograms adds
# variety, not time. 750 batches of 4 at cube_size 96 took 12.1 min on clip-g3 (Quadro RTX 6000,
# 5.53 A/px) and 6.4 min on clip-g4. Cost grows with the cube volume. DDP splits the epoch across
# GPUs; the speed-up is taken as 75 % efficient per added GPU because the 4-core-per-GPU
# dataloaders may not keep up. Training runs with --with_preview False, so there is no
# per-save_interval preview predict (~17 min each on a 1440x1022x512 tomogram).
_ISONET_SETUP_MIN = 3  # prepare_star + preprocess
_ISONET_MASK_MIN_PER_TOMO = 2  # deconv + make_mask; make_mask alone measured at 53 s/tomogram
_ISONET_EPOCH_MIN = 12.1  # cube_size 96, one GPU, slowest measured node
_ISONET_CUBE_REF = 96
_ISONET_GPU_EFFICIENCY = 0.75


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


def gres_gpu_count(gres: str) -> int:
    """GPUs a SLURM --gres string asks for: 'gpu:4' or 'gpu:a100:2' -> the count, a bare 'gpu' -> 1,
    no GPU entry -> 1 (IsoNet always runs on at least one)."""
    for item in (gres or "").split(","):
        parts = item.strip().split(":")  # gpu[:type][:count]
        if parts[0] == "gpu":
            return int(parts[-1]) if len(parts) > 1 and parts[-1].isdigit() else 1
    return 1


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
        "isonet_ctf_mode",
        "isonet_mw_weight",
        "isonet_cube_size",
        "isonet_bfactor",
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
        default=50,
        ge=1,
        le=200,
        description="IsoNet refine training epochs. IsoNet2 advises against fewer than 50 (its isonet2-n2n "
        "example uses 70); the noise2noise loss stays noise-dominated, so a flat loss curve is not a sign "
        "of convergence. An epoch is a fixed 3000 subtomograms (≈12 min at cube 96 on one RTX 6000), so "
        "cost is epochs × cube volume ÷ GPUs — more GPUs in the SLURM tab is the way to buy speed. Keep it "
        "a multiple of 10: IsoNet keeps an epoch checkpoint every 10. Only used when denoise_method = IsoNet.",
    )
    isonet_max_training_tomograms: int = Field(
        default=0,
        ge=0,
        le=100,
        description="Cap on how many tomograms IsoNet trains on (0 = every tomogram that passes "
        "tomograms_for_training). An epoch is 3000 subtomograms split across the training tomograms, so "
        "more tomograms cost no training time — only ~2 min each of deconv/mask prep — and give the "
        "network more variety. Only used when denoise_method = IsoNet.",
    )
    isonet_ctf_mode: IsoNetCTFMode = Field(
        default=IsoNetCTFMode.NETWORK,
        description="IsoNet refine --CTF_mode: how the network handles the CTF. 'network' (IsoNet2's "
        "recommendation) applies a CTF-shaped filter to the network input so it learns CTF correction "
        "along with denoising; 'wiener' filters the target instead and needs snrfalloff/deconvstrength "
        "tuning; 'None' leaves the CTF uncorrected. The model carries it into predict. Only used when "
        "denoise_method = IsoNet.",
    )
    isonet_mw_weight: float = Field(
        default=200.0,
        ge=-1,
        description="IsoNet refine --mw_weight: weight of the loss inside the missing wedge relative to "
        "denoising. IsoNet2 recommends 20–200 to make the network actually fill the wedge; -1 or 0 "
        "disables the split loss (IsoNet's default), and the network then mostly denoises. Only used "
        "when denoise_method = IsoNet.",
    )
    isonet_cube_size: int = Field(
        default=96,
        ge=32,
        le=256,
        description="IsoNet refine --cube_size: training subtomogram edge in voxels (IsoNet's default 96; "
        "its isonet2-n2n example uses 128). Training time grows with the cube "
        "volume — 128 costs ≈2.4× 96 per epoch. Only used when denoise_method = IsoNet.",
    )
    isonet_bfactor: float = Field(
        default=0.0,
        ge=0,
        description="IsoNet refine --bfactor: high-frequency boost for the CTF correction. IsoNet2 recommends "
        "0 for cellular tomograms and 200–300 for isolated samples. Only used when denoise_method = IsoNet.",
    )
    isonet_deconv: bool = Field(
        default=True,
        description="Build IsoNet's training mask from a CTF-deconvolved copy of the full tomogram. Rule: ON "
        "for defocus-contrast data — IsoNet recommends it because a mask built from the raw reconstruction "
        "tends to be poor; OFF for phase-plate data. With even/odd halves (method auto / isonet2-n2n) refine "
        "and predict read the raw halves either way, so this only decides where training crops are taken; "
        "with method isonet2 (single map) it also deconvolves the training input. The even/odd methods need "
        "tsReconstruct run with deconv 0 (Warp's --deconv deconvolves the half-maps too), so IsoNet makes the "
        "copy itself at the tsCtf defocus nearest 0° tilt; single-map isonet2 uses tsReconstruct's "
        "reconstruction/deconv/ copy when it exists. Only used when denoise_method = IsoNet.",
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

    def isonet_work_minutes(self, n_tomograms: int, n_gpus: int = 1) -> int:
        """Minutes of actual work IsoNet refine needs for `n_tomograms` on `n_gpus`. Also called by
        the driver, which knows the REAL staged count (post-`tomograms_for_training`) and GPU count
        and warns when the watchdog budget can't cover it."""
        n = max(n_tomograms, 1)
        if self.isonet_max_training_tomograms:
            n = min(n, self.isonet_max_training_tomograms)
        speedup = 1 + _ISONET_GPU_EFFICIENCY * (max(n_gpus, 1) - 1)
        epoch_min = _ISONET_EPOCH_MIN * (self.isonet_cube_size / _ISONET_CUBE_REF) ** 3 / speedup
        return int(_ISONET_SETUP_MIN + _ISONET_MASK_MIN_PER_TOMO * n + epoch_min * self.isonet_epochs)

    def _scaled_train_walltime(self, base_time: str, gres: str, qos: str) -> str:
        """Scale `base_time` to the tilt-series count, floored at `base_time` and capped.

        The count is ProjectState's in-memory `import_selected_tilt_series` — no disk I/O, and
        known at deploy time before the upstream tomograms.star exists. A narrowing
        `tomograms_for_training` filter makes this an OVER-estimate, which is the safe direction
        (the job just finishes early); the driver logs the real count once it has staged. IsoNet's
        cap is the QOS's wall limit when the sacctmgr probe knows it, so a longer QOS buys a longer run."""
        is_isonet = self.denoise_method == DenoiseMethod.ISONET
        n_ts = getattr(self._project_state, "import_selected_tilt_series", 0) or 0
        cap = _TRAIN_WALLTIME_CAP_MIN
        if is_isonet:
            # /0.9: run_command's watchdog only lets the job spend 90% of --time.
            minutes = int(self.isonet_work_minutes(n_ts, gres_gpu_count(gres)) / 0.9) + 1
            cap = get_cached_qos_maxwall_minutes(qos) or cap
        elif n_ts <= 0:
            return base_time  # cryoCARE can't be estimated without a count -> profile default
        else:
            minutes = _TRAIN_WALLTIME_BASE_MIN + _TRAIN_WALLTIME_PER_TS_MIN * n_ts
        minutes = max(minutes, _hms_to_minutes(base_time))  # never below the profile/default
        minutes = min(minutes, cap)
        return _minutes_to_hms(minutes)

    def get_effective_slurm_config(self) -> SlurmConfig:
        # Single-job training walltime must cover ALL tilt-series; scale it to the
        # dataset size unless the user has pinned an explicit time override.
        cfg = super().get_effective_slurm_config()
        if "time" not in self.slurm_overrides:
            cfg.time = self._scaled_train_walltime(cfg.time, cfg.gres, cfg.qos)
        return cfg
