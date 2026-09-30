from __future__ import annotations
import json
import logging
import re
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import ClassVar
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory, MissAlignSchedule
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.computing.slurm_service import SlurmConfig, get_cached_qos_maxwall_minutes
from services.configs.config_service import get_config_service

logger = logging.getLogger(__name__)


# ── Macro-iteration schedules ─────────────────────────────────────────────────
# A schedule lists one (pixel size, alignment mode) entry per macro-iteration, coarse to fine. The
# tool itself only knows a whole Fourier-crop factor of the stack it reads (`downsample`), so an
# entry given in Å is converted per stack: ds = max(1, round(Å / stack Å/px)).
#
# Text form, used by the presets below and by `custom_schedule` — entries separated by ';', each
#     <pixel> <alignment> [x<count>]
# pixel: "<Å>A" or "ds<N>"; alignment: anchoring | global | spline | [Nx,Ny] (a local image-warp grid
# per tilt) | [X,Y,Z,T] (a volume-warp grid).

ALIGNMENT_MODES = ("anchoring", "global", "spline")

# A conversion that lands further than this from the requested pixel size is reported.
PIXEL_ROUNDING_TOLERANCE = 0.15


@dataclass(frozen=True, slots=True)
class ScheduleEntry:
    """One macro-iteration: an alignment mode at a pixel size given in Å or as a stack downsample."""

    alignment: str | tuple[int, ...]
    angstrom: float | None = None
    downsample: int | None = None

    def __post_init__(self):
        if (self.angstrom is None) == (self.downsample is None):
            raise ValueError("a schedule entry takes exactly one of a pixel size in Å or a downsample")
        if self.angstrom is not None and self.angstrom <= 0:
            raise ValueError(f"pixel size must be positive, got {self.angstrom}")
        if self.downsample is not None and self.downsample < 1:
            raise ValueError(f"downsample must be >= 1, got {self.downsample}")
        if isinstance(self.alignment, tuple):
            if len(self.alignment) not in (2, 4) or any(n < 1 for n in self.alignment):
                raise ValueError(f"a grid alignment is [Nx,Ny] or [X,Y,Z,T] with positive sizes, got {self.alignment}")
        elif self.alignment not in ALIGNMENT_MODES:
            raise ValueError(
                f"unknown alignment {self.alignment!r}: use anchoring, global, spline, [Nx,Ny] or [X,Y,Z,T]"
            )

    @property
    def is_grid(self) -> bool:
        return isinstance(self.alignment, tuple)

    @property
    def is_volume_warp(self) -> bool:
        return isinstance(self.alignment, tuple) and len(self.alignment) == 4

    def effective_downsample(self, stack_apix: float) -> int:
        if self.downsample is not None:
            return self.downsample
        return max(1, round(self.angstrom / stack_apix))

    def pixel_angstrom(self, stack_apix: float) -> float:
        return stack_apix * self.effective_downsample(stack_apix)

    def rounding_note(self, stack_apix: float) -> str | None:
        """Why the pixel size used differs from the one asked for, when it does by more than the tolerance."""
        if self.angstrom is None:
            return None
        used = self.pixel_angstrom(stack_apix)
        deviation = used / self.angstrom - 1
        if abs(deviation) <= PIXEL_ROUNDING_TOLERANCE:
            return None
        return (
            f"{self.angstrom:g} Å asked, {used:.4g} Å used ({deviation:+.0%}): no whole downsample of the "
            f"{stack_apix:.4g} Å/px stack comes closer"
        )

    def config_entry(self, stack_apix: float) -> dict:
        """The tool's `iteration_settings` entry for a stack of `stack_apix` Å/px."""
        alignment = list(self.alignment) if self.is_grid else self.alignment
        return {"downsample": self.effective_downsample(stack_apix), "alignment": alignment}

    def __str__(self) -> str:
        pixel = f"ds{self.downsample}" if self.downsample is not None else f"{self.angstrom:g}A"
        alignment = "[" + ",".join(str(n) for n in self.alignment) + "]" if self.is_grid else self.alignment
        return f"{pixel} {alignment}"


_ENTRY_RE = re.compile(
    r"^(?:(?P<angstrom>\d+(?:\.\d+)?)\s*(?:A|Å)|ds\s*(?P<downsample>\d+))\s+"
    r"(?P<alignment>[a-z]+|\[\s*\d+(?:\s*,\s*\d+)*\s*\])"
    r"(?:\s*[x×]\s*(?P<count>\d+))?$",
    re.IGNORECASE,
)


def parse_schedule(text: str) -> tuple[ScheduleEntry, ...]:
    """Schedule text -> one entry per macro-iteration. Raises ValueError naming the entry it cannot read."""
    entries: list[ScheduleEntry] = []
    for raw in text.split(";"):
        item = raw.strip()
        if not item:
            continue
        m = _ENTRY_RE.match(item)
        if m is None:
            raise ValueError(
                f"cannot read schedule entry {item!r}: write '<pixel> <alignment> [xN]', "
                f"e.g. '20A anchoring', 'ds2 global', '10A [3,3] x5'"
            )
        alignment_text = m["alignment"].lower()
        if alignment_text.startswith("["):
            alignment: str | tuple[int, ...] = tuple(int(v) for v in alignment_text.strip("[] ").split(","))
        else:
            alignment = alignment_text
        entry = ScheduleEntry(
            alignment=alignment,
            angstrom=float(m["angstrom"]) if m["angstrom"] else None,
            downsample=int(m["downsample"]) if m["downsample"] else None,
        )
        count = int(m["count"] or 1)
        if count < 1:
            raise ValueError(f"repeat count must be >= 1 in {item!r}")
        entries.extend([entry] * count)
    if not entries:
        raise ValueError("the schedule is empty")
    return tuple(entries)


MISS_ALIGN_SCHEDULES: dict[MissAlignSchedule, tuple[ScheduleEntry, ...]] = {
    MissAlignSchedule.FAST: parse_schedule("ds2 anchoring; ds1 global"),
    MissAlignSchedule.DEFAULT: parse_schedule("ds3 anchoring; ds2 anchoring; ds1 global; ds1 [3,3]"),
    # The tool's config template.
    MissAlignSchedule.THOROUGH: parse_schedule("ds3 anchoring; ds2 anchoring; ds1 global x2; ds1 [3,3] x4"),
    # The preprint's lamella schedule (EMPIAR-10499), on its 10 Å stacks: global-only refinement gave a
    # modest STA gain, the local [3,3] rounds the large one.
    MissAlignSchedule.PAPER: parse_schedule("30A anchoring; 20A anchoring x2; 10A global; 10A [3,3] x5"),
}


def parse_milestones(text: str) -> list[int]:
    """'5,15' -> [5, 15]: positive, strictly increasing epochs."""
    try:
        values = [int(v) for v in text.replace(" ", "").split(",") if v]
    except ValueError:
        raise ValueError(f"lr_milestones must be comma-separated whole epochs, got {text!r}") from None
    if not values or any(v < 1 for v in values) or any(b <= a for a, b in pairwise(values)):
        raise ValueError(f"lr_milestones must be positive and increasing, got {text!r}")
    return values


def parse_z_box(text: str) -> float | str:
    """'full' | 'auto' | a positive extent in Å."""
    value = text.strip().lower()
    if value in ("full", "auto"):
        return value
    try:
        angstrom = float(value.removesuffix("a").removesuffix("å").strip())
    except ValueError:
        raise ValueError(f"z_box must be 'full', 'auto' or a size in Å, got {text!r}") from None
    if angstrom <= 0:
        raise ValueError(f"z_box must be positive, got {text!r}")
    return angstrom


def parse_device_list(text: str, n_gpus: int) -> list[int]:
    """'1,1,2,2' -> [1, 1, 2, 2], each index within the allocated GPUs."""
    try:
        devices = [int(v) for v in text.replace(" ", "").split(",") if v]
    except ValueError:
        raise ValueError(f"reconstruction_devices must be comma-separated GPU indices, got {text!r}") from None
    if not devices or any(d < 0 or d >= n_gpus for d in devices):
        raise ValueError(f"reconstruction_devices {text!r} must name GPUs 0..{n_gpus - 1}")
    return devices


# ── Dynamic walltime ──────────────────────────────────────────────────────────
# miss_align is a SINGLE multi-GPU-NODE job (not a per-TS SLURM array): one `miss-alignment train`
# invocation trains + realigns ALL selected tilt-series across a coarse->fine schedule of macro-
# iterations. Each iteration = a fixed TRAINING budget (steps_per_epoch x max_epochs training steps,
# split across the training GPUs, INDEPENDENT of n_ts) + a per-TS ALIGNMENT phase spread over every
# GPU. Per-step time is card-dependent and unknowable at submit, so we use a conservative RTX-class
# constant: UNDER-requesting is catastrophic (killed mid-iteration -> the resume restarts the same
# iteration), while OVER-requesting is harmless (SLURM frees the slot when the job ends).
_WALLTIME_BASE_MIN = 30  # fixed overhead: staging, dim-stamp, the z-box reconstructions, I/O
_WALLTIME_PER_STEP_SEC = 3.5  # conservative per training-step seconds (RTX-class; an A100 measured 1.34)
_WALLTIME_PER_TS_ALIGN_MIN = 5  # alignment per (tilt-series x macro-iteration) at 6.2 Å, one GPU
_ALIGN_REFERENCE_APIX = 6.2  # the alignment cost above scales with (this / effective Å)^3
_WALLTIME_CAP_MIN = 24 * 60  # ceiling when the QOS limit is unknown


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
    days, rest = divmod(max(0, int(minutes)), 1440)
    h, m = divmod(rest, 60)
    return f"{days}-{h}:{m:02d}:00" if days else f"{h}:{m:02d}:00"


class MissAlignParams(AbstractJobParams):
    """miss-alignment (warpem) — learned tilt-series alignment REFINEMENT.

    An OPTIONAL, insertable post-alignment job: it consumes aligntiltsWarp's warp_tiltseries/
    (which supplies the required initial coarse alignment), refines the per-TS Warp XMLs, and
    emits the same star + tilt_series/ + warp_tiltseries/ an alignment job emits. Runs the tool's
    `train` subcommand, which both trains and aligns. See docs/miss-alignment.md.

    Routing: JobSpec.refines makes a downstream tsCtf bound to aligntiltsWarp follow this job
    instead; its settings input stays on aligntiltsWarp, whose settings file has the tomostar/
    sibling tsCtf stages from.
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
        "**Schedules.** *fast* = 2 iterations (quick sanity pass) · *default* = 4 · *thorough* = the tool's "
        "8-iteration template · *paper* = the preprint's lamella ladder, written in Å (30 Å anchoring, 20 Å "
        "anchoring ×2, 10 Å global, 10 Å local `[3,3]` ×5) · *custom* = your own list in `custom_schedule`. "
        "Entries in Å are converted to the nearest whole downsample of the stack; the summary above the "
        "fields shows the pixel size each iteration actually uses. The local `[N,N]` rounds are where the "
        "preprint's subtomogram-averaging gains came from.\n\n"
        "**Training budget.** Per macro-iteration the model trains `steps_per_epoch × "
        "max_epochs_per_iteration` steps. The learning rate halves at each of `lr_milestones`, and early "
        "stopping only starts after the last one — with the epoch cap at or below it, neither happens. The "
        "tool's own budget is 1000 steps × 30 epochs: about 11 h per iteration on an A100.\n\n"
        "**Long runs.** The default queue limit is usually 8 h; set a longer QOS in the SLURM tab (e.g. "
        "`g_long`) and the requested wall-time follows the estimate up to that QOS's limit. The job "
        "checkpoints once per macro-iteration: **if it runs out of time, re-run it** and it continues from "
        "the last finished iteration (the schedule and inputs must be unchanged).\n\n"
        "**Volume box (`z_box`).** The tool trains and scores patches across the whole volume box; the "
        "preprint advises fitting it to the sample, since empty patches dominate otherwise. *auto* "
        "reconstructs each series coarsely, finds the lamella and fits the box to it; the full box is "
        "written back to the output.\n\n"
        "**Which resource feeds which stage.**\n"
        "- **Training** → `training_gpus` (one by default). Several training GPUs split each epoch "
        "between them; the tool scales the learning rate by their number.\n"
        "- **Reconstruction pool** (builds the training subtomograms) → the other GPUs, or the "
        "`reconstruction_devices` list (e.g. `1,1,2,2` = two workers on each of GPUs 1 and 2).\n"
        "- **Data loading** → CPU `dataloader_workers` per training GPU; each worker needs a CPU core "
        "(raise `cpus_per_task` to match) and `pool_size >= 2 × batch × workers × training_gpus`.\n"
        "- **Alignment** → spreads over every allocated GPU.\n\n"
        "**Stacks (`prepare_stacks_apix`).** 0 aligns the stacks the alignment job wrote. A value > 0 has "
        "the tool rebuild the stacks from the frame averages at that Å/px first (the preprint used 10 Å)."
    )

    USER_PARAMS: ClassVar[set[str]] = {
        "iteration_preset",
        "custom_schedule",
        "max_epochs_per_iteration",
        "steps_per_epoch",
        "lr_milestones",
        "seed",
        "batch_size",
        "patch_size",
        "pool_size",
        "num_gpus",
        "training_gpus",
        "reconstruction_devices",
        "dataloader_workers",
        "prepare_stacks_apix",
        "z_box",
    }

    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.ALIGNED_TILT_SERIES_STAR], preferred_source="aligntiltsWarp"),
        InputSlot(key="input_processing", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="aligntiltsWarp"),
        InputSlot(
            key="warp_tiltseries_settings",
            accepts=[JobFileType.WARP_TILTSERIES_SETTINGS],
            preferred_source="aligntiltsWarp",
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
    ]

    iteration_preset: MissAlignSchedule = Field(
        default=MissAlignSchedule.DEFAULT,
        description="Macro-iteration schedule. fast = quick sanity pass; default = balanced coarse->fine->local; "
        "thorough = the tool's 8-iteration template; paper = the preprint's lamella ladder in Å; custom = "
        "custom_schedule.",
    )
    custom_schedule: str = Field(
        default="",
        description="Used when iteration_preset is custom. Entries separated by ';', each "
        "'<pixel> <alignment> [xN]': pixel '<Å>A' (converted to the nearest whole downsample of the stack) or "
        "'ds<N>'; alignment anchoring | global | spline | [Nx,Ny] (local warp) | [X,Y,Z,T] (volume warp). "
        "Example: 30A anchoring; 20A anchoring x2; 10A global; 10A [3,3] x5",
    )
    max_epochs_per_iteration: int = Field(
        default=30,
        ge=1,
        le=200,
        description="Cap on training epochs per macro-iteration. Must exceed the last lr_milestone for the "
        "second learning-rate cut and early stopping to happen.",
    )
    steps_per_epoch: int = Field(
        default=250,
        ge=10,
        le=5000,
        description="Training steps per epoch (split across training_gpus). Total work per macro-iteration = "
        "steps_per_epoch x max_epochs_per_iteration, so this is the single biggest wall-time lever. The tool's "
        "own default is 1000.",
    )
    lr_milestones: str = Field(
        default="5,15",
        description="Epochs at which the learning rate halves (comma-separated). Early stopping (patience 5) "
        "only arms from the last milestone on.",
    )
    seed: int = Field(
        default=45132,
        ge=0,
        description="Training seed. Two runs that differ only in seed measure the run-to-run noise floor.",
    )
    batch_size: int = Field(
        default=32, ge=1, le=256, description="Per training GPU. 32 fits a 24 GB card at reconstruction size 128^3."
    )
    patch_size: int = Field(
        default=96,
        ge=32,
        le=256,
        description="Edge of the training / scoring cube in pixels at each iteration's pixel size; its field "
        "of view in Å grows with the downsample (see the schedule summary).",
    )
    pool_size: int = Field(
        default=1000,
        ge=16,
        description="Subtomogram pool size. Must hold 2 x batch_size for every data-loading worker of every "
        "training GPU (checked by the driver).",
    )
    num_gpus: int = Field(
        default=1,
        ge=1,
        le=4,
        description="GPUs to allocate (single node). 1 = training and the reconstruction pool share one card. "
        "With more, the first training_gpus train and the rest reconstruct, concurrently — at the cost of a "
        "longer queue, and the partition's nodes must have this many GPUs.",
    )
    training_gpus: int = Field(
        default=1,
        ge=1,
        le=4,
        description="GPUs that train the model (at most num_gpus). Each runs steps_per_epoch/training_gpus "
        "steps per epoch with its own batch; the tool multiplies the learning rate by this count.",
    )
    reconstruction_devices: str = Field(
        default="",
        description="GPU index per reconstruction worker, e.g. '1,1,2,2' = two workers each on GPUs 1 and 2 "
        "(indices 0..num_gpus-1; may repeat training GPUs). Empty = one worker on each GPU that does not "
        "train, or on each training GPU when all of them train.",
    )
    dataloader_workers: int = Field(
        default=0,
        ge=0,
        description="CPU data-loading workers per training GPU. 0 = auto: the allocated CPUs left after the "
        "reconstruction workers and one per trainer, within the pool limit. A positive value is still clamped "
        "to the pool limit.",
    )
    prepare_stacks_apix: float = Field(
        default=0.0,
        ge=0.0,
        description="If > 0, the tool rebuilds the tilt stacks from the frame averages at this Å/px before "
        "aligning, and schedule entries in Å are converted against it. 0 = align the alignment job's stacks.",
    )
    z_box: str = Field(
        default="full",
        description="Z extent of the volume box the tool trains and scores in: 'full' (the tomogram's), "
        "'auto' (per tilt-series, the lamella's full extent in a coarse reconstruction — out to where its "
        "contrast falls back to the quiet slices on either side), or a size in Å. The box stays centred on "
        "the volume centre; the output XMLs get the full box back.",
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "miss_alignment"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"align": "aligntiltsWarp"}

    # ── Resolved settings (the driver and the job tab read the same ones) ────

    def schedule(self) -> tuple[ScheduleEntry, ...]:
        """The macro-iterations this job runs. Raises ValueError for an unreadable custom schedule."""
        if self.iteration_preset == MissAlignSchedule.CUSTOM:
            return parse_schedule(self.custom_schedule)
        return MISS_ALIGN_SCHEDULES[MissAlignSchedule(self.iteration_preset)]

    def milestones(self) -> list[int]:
        return parse_milestones(self.lr_milestones)

    def epoch_cap_warning(self) -> str | None:
        """Why this training budget never cuts the learning rate twice nor stops early, if it doesn't."""
        try:
            last = self.milestones()[-1]
        except ValueError:
            return None
        if self.max_epochs_per_iteration > last:
            return None
        return (
            f"max_epochs_per_iteration ({self.max_epochs_per_iteration}) does not exceed the last learning-rate "
            f"milestone ({last}): the rate never drops past that milestone's cut and early stopping never "
            f"starts, so every iteration trains the full {self.max_epochs_per_iteration} epochs."
        )

    def planned_stack_apix(self) -> float | None:
        """Pixel size of the stacks the tool will read: prepare_stacks_apix when set, else the alignment
        job's stack pixel size from project state. None when neither is known (the driver reads the stacks)."""
        if self.prepare_stacks_apix > 0:
            return self.prepare_stacks_apix
        state = self._project_state
        if state is None:
            return None
        for instance_id in state.pipeline_order or list(state.jobs):
            job = state.jobs.get(instance_id)
            if job is not None and job.job_type == JobType.TS_ALIGNMENT:
                return float(getattr(job, "rescale_angpixs", 0) or 0) or None
        return None

    # ── Walltime ─────────────────────────────────────────────────────────────

    def _qos_safe_cap_minutes(self, qos: str) -> int:
        """Walltime ceiling (minutes) this job may request; 0 = no ceiling known here.

        The limit `sbatch` enforces is the QOS's MaxWallDurationPerJob: the named QOS's when the job
        asks for one, else the user's default QOS, as probed via sacctmgr (the landing-page probe fills
        the cache). A named QOS the probe has not seen gets no ceiling — sbatch refuses a request over
        its limit at submit. The default QOS unprobed: the conservative supervisor walltime from
        conf.yaml, else a day."""
        real = get_cached_qos_maxwall_minutes(qos)
        if real > 0:
            return real
        if qos.strip():
            return 0
        return _hms_to_minutes(get_config_service().supervisor_slurm_defaults.time) or _WALLTIME_CAP_MIN

    def _walltime_plan(self, base_time: str, qos: str) -> tuple[int, float, int, int] | None:
        """(estimated minutes, longest macro-iteration in minutes, wall-time ceiling in minutes (0 = none),
        iterations), or None when the TS count or the schedule is unknown.

        Cost model: training steps split across the training GPUs, plus a per-TS alignment phase that
        grows with (6.2 Å / effective Å)³ and spreads over every GPU; floored at `base_time`."""
        n_ts = getattr(self._project_state, "import_selected_tilt_series", 0) or 0
        try:
            entries = self.schedule()
        except ValueError:
            return None
        if n_ts <= 0 or not entries:
            return None
        stack_apix = self.planned_stack_apix() or _ALIGN_REFERENCE_APIX
        train_min = (
            self.steps_per_epoch * self.max_epochs_per_iteration * _WALLTIME_PER_STEP_SEC / 60.0 / self.training_gpus
        )
        iteration_minutes = [
            train_min
            + _WALLTIME_PER_TS_ALIGN_MIN
            * n_ts
            * max(1.0, (_ALIGN_REFERENCE_APIX / e.pixel_angstrom(stack_apix)) ** 3)
            / self.num_gpus
            for e in entries
        ]
        minutes = max(int(_WALLTIME_BASE_MIN + sum(iteration_minutes) + 0.999), _hms_to_minutes(base_time))
        return minutes, max(iteration_minutes), self._qos_safe_cap_minutes(qos), len(entries)

    def _scaled_walltime(self, base_time: str, qos: str) -> str:
        """The estimated walltime, capped at the QOS limit; `base_time` unchanged when it cannot be estimated.

        The whole run may not fit one wall-time; the tool checkpoints per macro-iteration and resumes, SO
        LONG AS a single iteration fits the cap. If one iteration alone exceeds it, every run dies
        mid-iteration and the resume never advances (walltime_warning says so in the job tab)."""
        plan = self._walltime_plan(base_time, qos)
        if plan is None:
            return base_time
        minutes, longest, cap, n_iters = plan
        if cap and minutes > cap:
            logger.warning(
                "missAlign: full run ~%s (%d iterations, longest ~%s) exceeds the wall-time limit %s; clamping.",
                _minutes_to_hms(minutes),
                n_iters,
                _minutes_to_hms(int(longest)),
                _minutes_to_hms(cap),
            )
            minutes = cap
        return _minutes_to_hms(minutes)

    def walltime_warning(self) -> str | None:
        """Why one submission cannot finish this run, if it can't — for the job tab."""
        cfg = super().get_effective_slurm_config()
        plan = self._walltime_plan(cfg.time, cfg.qos)
        if plan is None:
            return None
        minutes, longest, cap, n_iters = plan
        limit = _hms_to_minutes(str(self.slurm_overrides["time"])) if "time" in self.slurm_overrides else cap
        if not limit:
            return None
        where = "the time override" if "time" in self.slurm_overrides else f"QOS {cfg.qos or '(default)'}"
        if longest > limit:
            return (
                f"One macro-iteration needs ~{_minutes_to_hms(int(longest))} (conservative estimate) but {where} "
                f"allows {_minutes_to_hms(limit)} per run: the job would die inside it and every re-run restarts "
                f"it. Set a longer QOS in the SLURM tab (e.g. g_long), or lower steps_per_epoch / epochs."
            )
        if minutes > limit:
            return (
                f"The full run (~{_minutes_to_hms(minutes)}, {n_iters} iterations) exceeds {where}'s "
                f"{_minutes_to_hms(limit)}: re-run the job when it stops; it resumes after the last finished "
                f"iteration."
            )
        return None

    def get_effective_slurm_config(self) -> SlurmConfig:
        # Single-job training walltime must cover ALL tilt-series across every macro-iteration;
        # scale it to the dataset size unless the user has pinned an explicit time override.
        cfg = super().get_effective_slurm_config()
        if "time" not in self.slurm_overrides:
            cfg.time = self._scaled_walltime(cfg.time, cfg.qos)
        # num_gpus is the single source of truth for the GPU count so the driver's train/recon
        # device split has the cards it maps to. Respect an explicit gres override (SLURM tab).
        if "gres" not in self.slurm_overrides:
            cfg.gres = f"gpu:{self.num_gpus}"
        return cfg


# ── Local warps downstream ────────────────────────────────────────────────────

# Input slots a job's data descends through, most direct first: tsCtf/tsReconstruct read the Warp XMLs
# (input_processing), particle picking reads tomograms, extraction and averaging read optimisation sets,
# denoising reads the reconstruction job's XMLs (reconstruct_base).
_LINEAGE_SLOTS = ("input_processing", "input_tomograms", "input_optimisation", "reconstruct_base")


def _owning_instance(state, path: str) -> str | None:
    """The job whose directory holds `path` (absolute, or relative to the project)."""
    root = Path(state.project_path).resolve()
    target = (Path(path) if Path(path).is_absolute() else root / path).resolve()
    for instance_id, job in state.jobs.items():
        if job.relion_job_name and target.is_relative_to(root / job.relion_job_name.rstrip("/")):
            return instance_id
    return None


def local_warps_upstream(state, instance_id: str) -> tuple[str, float] | None:
    """(missAlign instance, largest local image-warp node in Å) when the data `instance_id` works on was
    reconstructed from a missAlign alignment that wrote local warp grids; None otherwise. Follows the
    jobs' recorded input paths upstream (extraction -> picking -> tsReconstruct -> tsCtf -> missAlign)
    and reads that job's missalign_changes.json."""
    if state.project_path is None:
        return None
    current: str | None = instance_id
    for _ in range(8):
        job = state.jobs.get(current) if current else None
        if job is None:
            return None
        if job.job_type == JobType.MISS_ALIGN and current != instance_id:
            report = Path(state.project_path) / job.relion_job_name.rstrip("/") / "missalign_changes.json"
            if not report.is_file():
                return None
            rows = json.loads(report.read_text()).get("tilt_series", {}).values()
            worst = max((r["local_warp"]["max_angstrom"] for r in rows if r.get("local_warp")), default=0.0)
            return (current, float(worst)) if worst > 0 else None
        upstream = next((job.paths[k] for k in _LINEAGE_SLOTS if (job.paths or {}).get(k)), None)
        current = _owning_instance(state, upstream) if upstream else None
    return None
