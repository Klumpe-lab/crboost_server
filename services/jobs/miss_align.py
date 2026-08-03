from __future__ import annotations
import logging
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, MissAlignSchedule
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmConfig, get_cached_qos_maxwall_minutes
from services.configs.config_service import get_config_service

logger = logging.getLogger(__name__)


# ── Macro-iteration presets ───────────────────────────────────────────────────
# Each preset expands to miss-alignment's `iteration_settings` list — the coarse->fine
# schedule the driver writes into config.yaml. List length == number of macro-iterations.
# alignment modes: "anchoring" (iterative) | "global" (single pass) | [N, N] (local
# NxN image-warping grid). Kept here (not in the driver) so the walltime estimate below
# and the driver's config.yaml read the SAME schedule. Values from docs/miss-alignment.md §5.
MISS_ALIGN_SCHEDULES: Dict[MissAlignSchedule, List[dict]] = {
    MissAlignSchedule.FAST: [{"downsample": 2, "alignment": "anchoring"}, {"downsample": 1, "alignment": "global"}],
    MissAlignSchedule.DEFAULT: [
        {"downsample": 3, "alignment": "anchoring"},
        {"downsample": 2, "alignment": "anchoring"},
        {"downsample": 1, "alignment": "global"},
        {"downsample": 1, "alignment": [3, 3]},
    ],
    MissAlignSchedule.THOROUGH: [
        {"downsample": 3, "alignment": "anchoring"},
        {"downsample": 2, "alignment": "anchoring"},
        {"downsample": 1, "alignment": "global"},
        {"downsample": 1, "alignment": "global"},
        {"downsample": 1, "alignment": [3, 3]},
        {"downsample": 1, "alignment": [3, 3]},
        {"downsample": 1, "alignment": [3, 3]},
        {"downsample": 1, "alignment": [3, 3]},
    ],
}


# ── Dynamic walltime ──────────────────────────────────────────────────────────
# miss_align is a SINGLE multi-GPU-NODE job (not a per-TS SLURM array): one `miss-alignment train`
# invocation trains + realigns ALL selected tilt-series across a coarse->fine schedule of macro-
# iterations. Each iteration = a fixed TRAINING budget (steps_per_epoch x max_epochs training steps,
# INDEPENDENT of n_ts — training samples one pooled dataset) + a per-TS ALIGNMENT phase. So the cost
# driver is steps x epochs x per-step-time, NOT n_ts x iters (the old flat model badly under-shot and
# the job died mid-iteration, before the per-iteration checkpoint — see below). per-step time is
# card-dependent and unknowable at submit, so we use a conservative RTX-class constant: UNDER-
# requesting is catastrophic (killed mid-iteration -> no checkpoint -> the resume restarts the same
# iteration forever), while OVER-requesting is harmless (SLURM frees the slot when the job ends).
_WALLTIME_BASE_MIN = 30  # fixed overhead: staging the warp_tiltseries copy, dim-stamp, I/O
_WALLTIME_PER_STEP_SEC = 3.5  # conservative per training-step seconds (RTX-class; faster cards finish early)
_WALLTIME_PER_TS_ALIGN_MIN = 5  # phase-2 alignment cost per (tilt-series x macro-iteration)
_WALLTIME_CAP_MIN = 24 * 60  # never request more than the partition realistically allows


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


class MissAlignParams(AbstractJobParams):
    """miss-alignment (warpem) — learned tilt-series alignment REFINEMENT.

    An OPTIONAL, insertable post-alignment job: it consumes aligntiltsWarp's warp_tiltseries/
    (which supplies the required initial coarse alignment), refines the per-TS Warp XMLs, and
    feeds tsCtf/tsReconstruct with ZERO format conversion (it is Warp-native). v1 = the tool's
    `train` subcommand, which both trains and aligns. See docs/miss-alignment.md.

    Routing note: because this job refines the multi-producer WARP_TILTSERIES_DIR in place,
    downstream tsCtf does NOT auto-prefer it (that would regress no-missAlign pipelines — see
    container_defs/MISS_ALIGNMENT_INTEGRATION.md). For P1, point tsCtf's `input_processing` at
    this job via the IO-tab source dropdown; auto-wiring via a per-instance source_override is P2.
    """

    job_type: JobType = Field(default=JobType.MISS_ALIGN)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_TOMO_JOB: ClassVar[bool] = True

    CONFIG_PREAMBLE: ClassVar[str] = (
        "**Learned alignment refinement (experimental).** Unlike the other steps, this is **one training "
        "job over _all_ your tilt-series at once**, not a per-tilt-series array. It trains a small 3-D CNN to "
        "score reconstruction quality, then nudges each series' geometry to maximise that score — repeated "
        "over a coarse→fine schedule of *macro-iterations*.\n\n"
        "**Why it can be slow.** Cost grows with **(tilt-series × macro-iterations)**, all inside a single "
        "job. Rough rule of thumb: beyond **~10 tomograms on the _default_ schedule** the estimate approaches "
        "a typical 8-hour queue wall-time limit (the _fast_ schedule roughly doubles that headroom). The "
        "requested wall-time is auto-capped to your queue's limit, so a large dataset can hit the cap and "
        "stop mid-run.\n\n"
        "**If it runs out of wall-time, just re-run this job.** It checkpoints **once per macro-iteration** "
        "and resumes from the last checkpoint (you lose at most one iteration's work). Automatic chained "
        "re-submission — splitting a long run across several jobs for you — is **planned (work in progress)**; "
        "for now the resume is manual (re-run).\n\n"
        "**Faster on big datasets (planned, WIP): train-then-infer.** The tool can *train* a model on a small "
        "**representative subset** of tilt-series, then **apply** it to align the rest without retraining "
        "(`infer`) — much cheaper. **Trade-off:** the model only ever saw the subset, so on a heterogeneous "
        "dataset a subset-trained model may align some series worse than a full joint run. Not wired up yet.\n\n"
        "**Tuning for speed — which resource feeds which stage.** Each macro-iteration runs four stages "
        "that bottleneck independently:\n"
        "- **Reconstruction pool** (builds the training subtomograms) → the *extra* GPUs. `num_gpus >= 2` "
        "dedicates GPU 0 to training and the rest to reconstruction so they stop fighting over one card.\n"
        "- **Data loading** (hands patches to the training GPU) → CPU **workers** (`dataloader_workers`). If the "
        "GPU sits idle waiting — the classic slowdown — raise workers; but each worker needs a CPU core, so "
        "raise `cpus_per_task` to match, and keep `pool_size >= 2 x batch x workers` or the run won't start.\n"
        "- **Training step** (the CNN forward/backward) → a **single GPU**; its per-step speed is set by the *card*, "
        "not the GPU count. More GPUs will NOT make this faster — a faster card (A100) will.\n"
        "- **Alignment** → auto-spreads over every allocated GPU.\n\n"
        "*Rules of thumb:* wall-time per macro-iteration = **`steps_per_epoch x max_epochs_per_iteration`** steps x "
        "per-step GPU time — so on a small dataset the biggest single win is **lowering `steps_per_epoch`** (and/or "
        "the schedule), not more hardware. For a handful of tilt-series, `num_gpus=2` + `dataloader_workers ~ "
        "cpus_per_task-2` + a lighter schedule is the sweet spot; reach for 4 GPUs / more workers only once "
        "reconstruction or data loading is the *actual* bottleneck.\n\n"
        "**Schedules:** *fast* = 2 iterations (quick sanity pass) · *default* = 4 (balanced) · "
        "*thorough* = 8 (best quality, slowest)."
    )

    USER_PARAMS: ClassVar[Set[str]] = {
        "iteration_preset",
        "max_epochs_per_iteration",
        "steps_per_epoch",
        "batch_size",
        "patch_size",
        "pool_size",
        "num_gpus",
        "dataloader_workers",
        "prepare_stacks_apix",
    }

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.ALIGNED_TILT_SERIES_STAR], preferred_source="aligntiltsWarp"),
        InputSlot(key="input_processing", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="aligntiltsWarp"),
        InputSlot(
            key="warp_tiltseries_settings",
            accepts=[JobFileType.WARP_TILTSERIES_SETTINGS],
            preferred_source="aligntiltsWarp",
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
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

    iteration_preset: MissAlignSchedule = Field(
        default=MissAlignSchedule.DEFAULT,
        description="Macro-iteration schedule. fast = quick sanity pass; default = balanced "
        "coarse->fine->local; thorough = the full 8-iteration schedule (production, hours).",
    )
    max_epochs_per_iteration: int = Field(
        default=30, ge=1, le=200, description="Early-stopping cap on training epochs per macro-iteration."
    )
    steps_per_epoch: int = Field(
        default=250,
        ge=10,
        le=5000,
        description="Training steps per epoch. Total work per macro-iteration = steps_per_epoch x "
        "max_epochs_per_iteration, so this is the single biggest wall-time lever. Default 250 suits the "
        "small datasets crboost usually runs; the tool's own default is 1000 (raise toward it for large / "
        "heterogeneous sets where the scoring model needs more training, lower it to fit a tight wall-time).",
    )
    batch_size: int = Field(default=32, ge=1, le=256, description="32 fits a 24 GB card at reconstruction size 128^3.")
    patch_size: int = Field(default=96, ge=32, le=256)
    pool_size: int = Field(
        default=1000,
        ge=16,
        description="Subtomogram pool size. Single-trainer constraint: pool_size >= 2*batch_size "
        "(enforced in the driver; violation raises).",
    )
    num_gpus: int = Field(
        default=1,
        ge=1,
        le=4,
        description="GPUs to allocate (single node). 1 = training and the reconstruction pool share "
        "one card. >=2 dedicates GPU 0 to training and the remaining GPU(s) to reconstruction so they "
        "run concurrently (roughly linear speedup) — at the cost of a longer queue, and the selected "
        "partition node must actually have this many GPUs.",
    )
    dataloader_workers: int = Field(
        default=0,
        ge=0,
        description="CPU data-loading workers feeding the training GPU. 0 = auto (recommended): "
        "min(cpus_per_task - 2, pool_size // (2*batch_size)). Set a positive value to pin it — it is "
        "still clamped to the pool constraint (pool_size // workers >= 2*batch_size), and a warning is "
        "logged if it exceeds allocated CPUs (oversubscription slows loading; raise cpus_per_task too).",
    )
    prepare_stacks_apix: float = Field(
        default=0.0,
        ge=0.0,
        description="If > 0, rebuild tilt stacks at this Å/px before aligning (needs raw frames + "
        "tomostar bound). 0 = use the existing aligned tiltstack/*.st (default; the proven path).",
    )

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "miss_alignment"

    @staticmethod
    def get_input_requirements() -> Dict[str, str]:
        return {"align": "aligntiltsWarp"}

    @staticmethod
    def _qos_safe_cap_minutes() -> int:
        """Walltime ceiling (minutes) any single missAlign job may request. Returns 0 -> no clamp.

        Prefer the user's REAL default-QOS MaxWallDurationPerJob, probed live via sacctmgr and cached
        by SlurmService.get_user_qos_limits() (the landing-page probe populates it). That's the true
        limit `sbatch` enforces, so a job can safely request up to it. Fall back to the conservative
        supervisor default walltime from conf.yaml only when the QOS hasn't been probed this session
        (which historically strangled the estimate below one macro-iteration -> QOSMaxWall-safe but
        stuck-looping; the live value fixes that)."""
        try:
            real = get_cached_qos_maxwall_minutes()
            if real > 0:
                return real
        except Exception:
            pass
        try:
            return _hms_to_minutes(get_config_service().supervisor_slurm_defaults.time)
        except Exception:
            return 0

    def _scaled_walltime(self, base_time: str) -> str:
        """Estimate walltime from the real cost drivers (steps_per_epoch x max_epochs x per-step
        time, plus a per-TS alignment phase), floored at `base_time` and capped. Returns
        `base_time` unchanged when the TS/iteration counts are unknown.

        Clamped to the partition's QOS-safe ceiling so a long run submits (else `sbatch` rejects
        the WHOLE chain with QOSMaxWallDurationPerJobLimit and pipeline start aborts). The whole
        run may not fit one wall-time; that's fine — the tool checkpoints per macro-iteration and
        resumes, SO LONG AS a single iteration fits the cap. If one iteration alone exceeds it,
        every run dies mid-iteration and the resume never advances — we warn loudly for that case
        with the concrete levers (fewer steps/epochs, or a faster GPU)."""
        n_ts = getattr(self._project_state, "import_selected_tilt_series", 0) or 0
        n_iters = len(MISS_ALIGN_SCHEDULES.get(self.iteration_preset, []))
        if n_ts <= 0 or n_iters <= 0:
            return base_time
        train_min_per_iter = self.steps_per_epoch * self.max_epochs_per_iteration * _WALLTIME_PER_STEP_SEC / 60.0
        min_per_iter = train_min_per_iter + _WALLTIME_PER_TS_ALIGN_MIN * n_ts
        minutes = int(_WALLTIME_BASE_MIN + n_iters * min_per_iter + 0.999)  # ceil
        minutes = max(minutes, _hms_to_minutes(base_time))  # never below today's profile/default
        minutes = min(minutes, _WALLTIME_CAP_MIN)
        qos_cap = self._qos_safe_cap_minutes()
        if qos_cap and minutes > qos_cap:
            if min_per_iter > qos_cap:
                logger.warning(
                    "missAlign: ONE macro-iteration (~%s) alone exceeds the QOS ceiling %s — the job can "
                    "never complete an iteration within a single wall-time, so the resume will restart the "
                    "same iteration forever. Lower steps_per_epoch/max_epochs_per_iteration or use a faster "
                    "GPU (num_gpus>=2 / an A100 constraint).",
                    _minutes_to_hms(int(min_per_iter)),
                    _minutes_to_hms(qos_cap),
                )
            else:
                logger.warning(
                    "missAlign: full run ~%s (%d iters) exceeds the QOS ceiling %s; clamping. One iteration "
                    "(~%s) still fits, so it completes ~%d iteration(s) per run and resumes from the last "
                    "checkpoint — just re-run until done.",
                    _minutes_to_hms(minutes),
                    n_iters,
                    _minutes_to_hms(qos_cap),
                    _minutes_to_hms(int(min_per_iter)),
                    max(1, int(qos_cap // max(1, int(min_per_iter)))),
                )
            minutes = qos_cap
        return _minutes_to_hms(minutes)

    def get_effective_slurm_config(self) -> SlurmConfig:
        # Single-job training walltime must cover ALL tilt-series across every macro-iteration;
        # scale it to the dataset size unless the user has pinned an explicit time override.
        cfg = super().get_effective_slurm_config()
        if "time" not in self.slurm_overrides:
            cfg.time = self._scaled_walltime(cfg.time)
        # num_gpus is the single source of truth for the GPU count so the driver's train/recon
        # device split has the cards it maps to. Respect an explicit gres override (SLURM tab).
        if "gres" not in self.slurm_overrides:
            cfg.gres = f"gpu:{self.num_gpus}"
        return cfg
