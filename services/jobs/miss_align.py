from __future__ import annotations
import logging
from typing import ClassVar, Dict, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, MissAlignSchedule
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmConfig
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
# miss_align is a SINGLE multi-GPU-NODE job (not a per-TS SLURM array), like denoise_train:
# one `miss-alignment train` invocation trains + realigns ALL selected tilt-series across a
# coarse->fine schedule of macro-iterations. Cost scales ~ n_ts * n_macro_iterations (each
# iteration trains a scoring model then optimizes the geometry per TS). Conservative linear
# model, floored at the profile time and capped; overallocation is harmless (SLURM frees the
# slot early), underallocation trips the in-job watchdog. Tune after the first real runs —
# smoke was 1 TS x 1 iter x 2 epochs ~ 3 min; production is hours (docs/miss-alignment.md §8).
_WALLTIME_BASE_MIN = 30  # fixed overhead: staging the warp_tiltseries copy, dim-stamp, I/O
_WALLTIME_PER_TS_ITER_MIN = 12  # marginal cost per (tilt-series x macro-iteration)
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
        "**Schedules:** *fast* = 2 iterations (quick sanity pass) · *default* = 4 (balanced) · "
        "*thorough* = 8 (best quality, slowest)."
    )

    USER_PARAMS: ClassVar[Set[str]] = {
        "iteration_preset",
        "max_epochs_per_iteration",
        "batch_size",
        "patch_size",
        "pool_size",
        "num_gpus",
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
        """Partition QOS-safe walltime ceiling in minutes.

        The site declares its QOS MaxWallDurationPerJob indirectly via the supervisor
        default walltime (config/conf.yaml sets it `<= the QOS MaxWall for this partition`).
        missAlign runs on the same GPU partition, so that value is the ceiling any single
        job can request. Returns 0 when unavailable -> no clamp."""
        try:
            return _hms_to_minutes(get_config_service().supervisor_slurm_defaults.time)
        except Exception:
            return 0

    def _scaled_walltime(self, base_time: str) -> str:
        """Scale `base_time` by n_selected_ts x n_macro_iterations, floored at `base_time`
        and capped. Returns `base_time` unchanged when either count is unknown/zero.

        The estimate is additionally clamped to the partition's QOS-safe ceiling: without
        it a large dataset scales past the QOS MaxWallDurationPerJob and `sbatch` rejects
        the WHOLE chain with QOSMaxWallDurationPerJobLimit (a single failed submit aborts
        pipeline start). We clamp + warn rather than fail: an over-long dataset then trips
        the in-job watchdog instead of never launching, which the user can act on."""
        n_ts = getattr(self._project_state, "import_selected_tilt_series", 0) or 0
        n_iters = len(MISS_ALIGN_SCHEDULES.get(self.iteration_preset, []))
        if n_ts <= 0 or n_iters <= 0:
            return base_time
        minutes = _WALLTIME_BASE_MIN + _WALLTIME_PER_TS_ITER_MIN * n_ts * n_iters
        minutes = max(minutes, _hms_to_minutes(base_time))  # never below today's profile/default
        minutes = min(minutes, _WALLTIME_CAP_MIN)
        qos_cap = self._qos_safe_cap_minutes()
        if qos_cap and minutes > qos_cap:
            logger.warning(
                "missAlign: scaled walltime %s (%d TS x %d iters) exceeds the QOS-safe ceiling %s; "
                "clamping so the job submits. If it hits the in-job watchdog, use the FAST schedule, "
                "pin an explicit `time` override, or split the run into fewer tilt-series.",
                _minutes_to_hms(minutes),
                n_ts,
                n_iters,
                _minutes_to_hms(qos_cap),
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
