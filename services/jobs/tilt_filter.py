from __future__ import annotations
import asyncio
import logging
import statistics
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import ClassVar
from pydantic import BaseModel, Field

from services.configs.config_service import TiltFilterModelConfig, get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobStatus, JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.result import err, ok

logger = logging.getLogger(__name__)


class FilterMode(StrEnum):
    """How the tilt filter reaches its verdict. Manual and DL review wait for a human's Approve;
    DL auto runs the model as a chain job that commits its own verdict."""

    MANUAL = "manual"  # hand labels only; predictions are ignored
    DL_REVIEW = "dl_review"  # DL predictions, run on request, stand in for the labels nobody set
    DL_AUTO = "dl_auto"  # a chain job predicts after fsMotion and commits; alignment follows it


# The modes in which a prediction stands in for a tilt nobody labelled.
DL_MODES = (FilterMode.DL_REVIEW, FilterMode.DL_AUTO)

# What a DL-auto job committed, written into its job dir for the server to read.
COMMIT_RECORD = "commit.json"


class TiltFilterCommit(BaseModel):
    """What the last commit stamped, for the job row's "D of T dropped". A record only: the
    committed flag is execution_status SUCCEEDED, which the dashboard reads too. Approve writes
    it, and a DL-auto job writes it as COMMIT_RECORD for the reconciler to load."""

    at: datetime = Field(default_factory=datetime.now)
    mode: FilterMode
    kept: int
    dropped: int
    model: str | None = None  # the model behind the predictions (DL modes, when its run succeeded)
    threshold: float | None = None  # DL modes only


class TiltFilterPredictRun(BaseModel):
    """One DL prediction run: a one-off SLURM job in its own directory under
    TiltFilter/dl_run/. It writes P(bad) per tilt into the registry and never the verdict,
    so its status lives here, apart from the job's execution_status (which says whether a
    verdict is committed). Settled by PipelineRunnerService.reconcile_tilt_filter_predict."""

    job_dir: str
    model: str
    slurm_job_id: str | None = None
    status: JobStatus = JobStatus.QUEUED
    submitted_at: datetime = Field(default_factory=datetime.now)
    error: str = ""


def next_predict_run_dir(project_path: Path) -> Path:
    """Create and return TiltFilter/dl_run/NNN for the next prediction run. One directory
    per run keeps each run's log and exit markers its own."""
    root = project_path / "TiltFilter" / "dl_run"
    root.mkdir(parents=True, exist_ok=True)
    taken = [int(p.name) for p in root.iterdir() if p.is_dir() and p.name.isdigit()]
    run_dir = root / f"{max(taken, default=0) + 1:03d}"
    run_dir.mkdir()
    return run_dir


def calibrated_threshold(key: str | None) -> float | None:
    """The P(bad) cut conf.yaml records for a model (`key`, or the default model when None);
    None when it records none or the model is not registered."""
    registry = get_config_service().tilt_filter
    entry = registry.models.get(key or registry.default_model or "")
    return None if entry is None else entry.threshold


def _default_threshold() -> float:
    """A new job's threshold: the default model's recorded cut, else 0.5 (marked uncalibrated)."""
    cut = calibrated_threshold(None)
    return 0.5 if cut is None else cut


class TiltFilterParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TILT_FILTER)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"

    # Empty on purpose: USER_PARAMS freezes a field once the job leaves SCHEDULED/FAILED, and
    # this job's SUCCEEDED only means a verdict is committed. `model`, `threshold` and
    # `dl_batch_size` steer the next prediction run and the review, so they stay editable after
    # a commit (a committed project is exactly where the predictions get compared with the
    # human labels). Their writes are saved explicitly, since nothing marks the project dirty.
    USER_PARAMS: ClassVar[set[str]] = set()

    # The DL reads the motion-corrected averages via the fs-motion star; that is this
    # job's only input. The verdict is a per-frame `is_filtered_out` stamp in the
    # TiltSeries registry, not a file this job produces: alignment applies the cut when
    # it snapshots the tomostar dir (drivers/ts_alignment.py), and CTF/reconstruct
    # inherit it from that snapshot.
    #
    # Not a TOMOSTAR_DIR producer. Consuming tsImport's tomostar would force an ordering
    # this interactive job cannot honour -- the user reaches the gallery as soon as
    # fsMotion's PNGs exist, routinely before tsImport has run -- and a pending filter
    # would show up as alignment's tomostar source and silently resolve to the unfiltered
    # tilt set. With the verdict in the registry, tsImport is the sole tomostar producer,
    # the commit has no upstream dependency, and filter-off vs filter-on differ only by
    # the drop set.
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(key="input_star", accepts=[JobFileType.FS_MOTION_CTF_STAR], preferred_source="fsMotionAndCtf")
    ]

    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = []

    # Manual by default: it needs no model. Chosen on the job row.
    mode: FilterMode = Field(default=FilterMode.MANUAL, description="Hand labels, or DL predictions a human reviews")

    # Older project files carry `model_name` / `prob_threshold` / `prob_action` / `image_size`;
    # they are ignored on load. `prob_threshold` was a cut on the winning class's probability,
    # so `threshold` must never take its value.
    model: str | None = Field(
        default=None, description="Tilt classifier: a key of conf.yaml's tilt_filter.models; None runs the default"
    )
    # Starts from the default model's recorded cut; the first run of another model moves it to that
    # model's cut (backend.submit_tilt_filter_predict).
    threshold: float = Field(
        default_factory=_default_threshold,
        ge=0.0,
        le=1.0,
        description="A tilt with P(bad) at or above this is predicted bad",
    )
    dl_batch_size: int = Field(default=32, ge=1, le=256, description="Batch size for DL inference")
    tilt_labels: dict[str, str] = Field(default_factory=dict, description="Manual good/bad label overrides by tilt key")
    predict_run: TiltFilterPredictRun | None = None
    last_commit: TiltFilterCommit | None = None
    # Why the last automatic DL step failed: a DL-auto job's FATAL line, recorded when the reconciler
    # settles it, or why DL review's automatic prediction run could not be submitted.
    auto_error: str = ""

    @property
    def IS_INTERACTIVE(self) -> bool:
        """The base class's flag, per instance. A review mode keeps the filter out of the chain:
        deploy skips it, Stop and the pipeline_active vote ignore it, and Approve commits. In DL
        auto it is a chain job like any other. Read on the class (the builder's singleton check),
        this is the property object, which is truthy: one tilt filter per project in every mode."""
        return self.mode != FilterMode.DL_AUTO

    @property
    def predict_in_flight(self) -> bool:
        return self.predict_run is not None and self.predict_run.status in (JobStatus.QUEUED, JobStatus.RUNNING)

    @property
    def auto_in_flight(self) -> bool:
        """The DL-auto chain job is queued or running. Only that job gives the filter these statuses."""
        return self.execution_status in (JobStatus.QUEUED, JobStatus.RUNNING)

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_star = self.paths.get("input_star", "")
        return [("in_mic", str(input_star))]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "crboost"

    @staticmethod
    def get_output_assets(job_dir: Path) -> dict[str, Path]:
        return {
            "filtered_star": job_dir / "filtered" / "tiltseries_filtered.star",
            "labeled_star": job_dir / "filtered" / "tiltseries_labeled.star",
        }

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"ctf": "tsCtf"}


# ── the classifier: registry lookup and liveness (shared by the submit, the driver and the panel) ──


def resolve_model(key: str | None) -> tuple[str, TiltFilterModelConfig]:
    """The conf.yaml registry entry a prediction run uses: `key`, or `default_model` when the
    job never picked one. Raises ValueError saying what is missing: no models configured, no
    default, an unknown key, or a weights file that is not on disk."""
    registry = get_config_service().tilt_filter
    if not registry.models:
        raise ValueError("No tilt classifier is configured (tilt_filter.models in conf.yaml).")
    name = key or registry.default_model
    if not name:
        raise ValueError("No model chosen, and conf.yaml sets no tilt_filter.default_model.")
    entry = registry.models.get(name)
    if entry is None:
        raise ValueError(f"Model '{name}' is not in conf.yaml's tilt_filter.models ({', '.join(registry.models)}).")
    if not entry.weights_path.is_file():
        raise ValueError(f"Model '{name}': weights file not found at {entry.weights_path}.")
    return name, entry


# A series whose P(bad) spread stays below this got the same number for every tilt, which is how a
# network that diverged answers. A live, decisive model is nearly flat on a series with no bad tilt
# (spreads of 1e-3 to 1e-2), so the cut sits far below that.
LIVENESS_MIN_STD = 1e-6


def prediction_liveness(p_bad_by_series: Mapping[str, Sequence[float]]) -> tuple[bool, float] | None:
    """Whether a run's predictions carry information, and their mean P(bad).

    Dead means every series with 2+ predictions has a P(bad) spread below LIVENESS_MIN_STD:
    the network gives every tilt the same number, whatever the image. A single varying series
    is enough to count as live. None when no series has 2 predictions to compare."""
    spreads = [statistics.pstdev(v) for v in p_bad_by_series.values() if len(v) >= 2]
    if not spreads:
        return None
    mean = statistics.fmean(p for v in p_bad_by_series.values() for p in v)
    return any(s >= LIVENESS_MIN_STD for s in spreads), mean


# ── commit-time verdict (shared by Approve and the DL-auto job) ──


async def stamp_verdict(registry, labels: Mapping[str, str]) -> dict:
    """Commit a tilt-filter verdict: stamp each frame's keep/drop decision (`labels`, frame id →
    "good" / "bad"; anything but "good" drops) into the TiltSeries registry and save it.
    Alignment applies the stamps when it snapshots the tomostar dir. Approve and the DL-auto
    job both commit through here.

    Re-stamps in both directions on every commit, so a tilt labelled good again is restored.
    Downstream jobs pick the new verdict up when they are (re)queued -- the forward-only
    staleness convention. A label naming a frame the registry lacks refuses the commit before
    anything is stamped: its verdict would be lost.

    Returns ok(kept=..., dropped=...) or err(...). A failure means the verdict was not
    recorded, so the caller must not mark the job succeeded."""
    if not registry.tilt_series_ids():
        return err("Cannot commit: the TiltSeries registry is empty. Reload the project to backfill it from mdocs.")

    unknown: list[str] = []
    for key in labels:
        try:
            registry.get_frame(key)
        except KeyError:  # named in the refusal below
            unknown.append(key)
    if unknown:
        shown = ", ".join(unknown[:3]) + (f" (+{len(unknown) - 3} more)" if len(unknown) > 3 else "")
        return err(
            f"Cannot commit: {len(unknown)} labelled tilts are not in the registry ({shown}). Reload the project."
        )

    kept = dropped = 0
    for key, label in labels.items():
        bad = label != "good"
        registry.set_frame_filtered(key, bad, reason="tilt-filter" if bad else None)
        dropped += bad
        kept += not bad

    try:
        await asyncio.to_thread(registry.save)
    except Exception:
        logger.exception("tilt-filter commit: registry save failed")
        return err("Cannot commit: writing the tilt verdict to the registry failed. See the server log.")
    return ok(kept=kept, dropped=dropped)


# ── the review: labels, Approve, Re-open (shared by the gallery and the job row) ──


def effective_label(
    key: str, p_bad: float | None, labels: Mapping[str, str], threshold: float, mode: FilterMode
) -> str:
    """A tilt's label as a commit stamps it: a human's label wins; in the DL modes, the
    prediction at the threshold; otherwise good. `p_bad` is None or NaN for a tilt without a
    prediction (NaN compares false)."""
    human = labels.get(key)
    if human:
        return human
    if mode in DL_MODES and p_bad is not None and p_bad >= threshold:
        return "bad"
    return "good"


def verdict_labels(registry, job_model: TiltFilterParams) -> dict[str, str]:
    """Every registry frame's effective label, as a commit stamps it, plus the human labels of
    tilts the registry lacks, which stamp_verdict then refuses by name."""
    labels = dict(job_model.tilt_labels)
    for ts in registry.all_tilt_series():
        for f in ts.frames:
            labels[f.id] = effective_label(f.id, f.p_bad, job_model.tilt_labels, job_model.threshold, job_model.mode)
    return labels


def _alignment_with_status(state, statuses: Iterable[JobStatus]) -> tuple[str, JobStatus] | None:
    """An alignment job whose status is one of `statuses`, as (instance_id, status). Alignment
    is where the verdict is consumed: its supervisor snapshots the tomostars with the cut."""
    wanted = set(statuses)
    for iid, jm in state.jobs.items():
        if jm.job_type == JobType.TS_ALIGNMENT and jm.execution_status in wanted:
            return iid, jm.execution_status
    return None


def _verdict_locked_reason(iid: str, status: JobStatus) -> str:
    if status == JobStatus.QUEUED:
        return f"Alignment ({iid}) is queued and will not wait for a review; stop the pipeline to review again."
    if status == JobStatus.RUNNING:
        return f"Alignment ({iid}) is running with the current verdict, which cannot change under it."
    return f"Alignment ({iid}) has run with the current verdict; delete the alignment job to change it."


async def commit_verdict(state, project_path: Path, instance_id: str) -> dict:
    """Approve: commit the tilt filter's verdict as the review stands. The one commit path; the
    gallery and the job row both come here.

    Every registry frame gets its effective label from the job's labels, mode and threshold and
    the frame's prediction, so the commit reads what the review and the Tilts tab show, whether
    the gallery is open or not. A tilt series fsMotion left out has no prediction and counts as
    kept. Sets SUCCEEDED and `last_commit`; the caller saves the project. Refused until fsMotion
    has succeeded (there is nothing to review before), while a prediction run is in flight (its
    predictions are about to change) and once alignment is running or has run (it has taken
    the verdict it keeps)."""
    from services.tilt_series import get_registry_for

    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"Job '{instance_id}' not found.")
    if job_model.mode == FilterMode.DL_AUTO:
        return err("In DL auto the filter's own job commits the verdict; switch to DL review to approve by hand.")
    if not any(
        jm.job_type == JobType.FS_MOTION_CTF and jm.execution_status == JobStatus.SUCCEEDED
        for jm in state.jobs.values()
    ):
        return err("fsMotion has not succeeded yet; the review needs its motion-corrected tilts.")
    if job_model.predict_in_flight:
        return err("A DL prediction run is in flight; approve once its predictions have landed.")
    locked = _alignment_with_status(state, (JobStatus.RUNNING, JobStatus.SUCCEEDED))
    if locked:
        return err(_verdict_locked_reason(*locked))
    try:
        registry = get_registry_for(project_path)
    except Exception:
        logger.exception("tilt-filter commit: registry unavailable")
        return err("Cannot commit: the TiltSeries registry could not be loaded. Reload the project and retry.")

    res = await stamp_verdict(registry, verdict_labels(registry, job_model))
    if not res["success"]:
        return res

    # Older projects carry output slots from when this job produced its own tomostar.
    for dead in ("output_tomostar", "output_star", "output_processing"):
        job_model.paths.pop(dead, None)

    dl = job_model.mode in DL_MODES
    run = job_model.predict_run
    job_model.execution_status = JobStatus.SUCCEEDED
    job_model.last_commit = TiltFilterCommit(
        mode=job_model.mode,
        kept=res["kept"],
        dropped=res["dropped"],
        model=run.model if dl and run is not None and run.status == JobStatus.SUCCEEDED else None,
        threshold=job_model.threshold if dl else None,
    )
    state.mark_dirty()
    return res


def reopen_lock(state) -> str | None:
    """Why a committed verdict can no longer be re-opened, or None: alignment is queued (it will
    not wait for a review), running or has run (it has taken the verdict)."""
    locked = _alignment_with_status(state, (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED))
    return _verdict_locked_reason(*locked) if locked else None


def reopen_review(state, instance_id: str) -> dict:
    """Undo a commit: the filter reads uncommitted again, so the next Run waits for a review (or,
    in DL auto, runs the filter's job again). The registry keeps the committed verdict until the
    next commit re-stamps it. Refused once alignment is queued, running or has run: the review
    could no longer hold it back."""
    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"Job '{instance_id}' not found.")
    if job_model.execution_status != JobStatus.SUCCEEDED:
        return err("The tilt filter is not approved.")
    lock = reopen_lock(state)
    if lock:
        return err(lock)
    job_model.execution_status = JobStatus.SCHEDULED
    # A DL-auto job's SLURM id goes too: the reconciler tracks a non-terminal job with one, and
    # would read its old exit marker back into SUCCEEDED.
    job_model.slurm_job_id = None
    job_model.last_commit = None
    state.mark_dirty()
    return ok()


def review_barrier(state, run_ids: Iterable[str]) -> str | None:
    """The tilt filter a run has to wait for: one in `run_ids` whose verdict is not committed
    and that waits for a human (Manual or DL review). None when the run holds no such filter."""
    for iid in run_ids:
        jm = state.jobs.get(iid)
        if (
            jm is not None
            and jm.job_type == JobType.TILT_FILTER
            and jm.mode != FilterMode.DL_AUTO
            and jm.execution_status != JobStatus.SUCCEEDED
        ):
            return iid
    return None


def dl_auto_filters(state, run_ids: Iterable[str]) -> list[str]:
    """The DL-auto tilt filters among `run_ids`."""
    return [
        iid
        for iid in run_ids
        if (jm := state.jobs.get(iid)) is not None
        and jm.job_type == JobType.TILT_FILTER
        and jm.mode == FilterMode.DL_AUTO
    ]


def verdict_edges(state, run_ids: Iterable[str]) -> list[tuple[str, str]]:
    """(filter, alignment) for each DL-auto tilt filter and alignment job among `run_ids`. The
    filter commits its verdict into the registry, which alignment reads; no output slot joins
    them, so the chain needs this edge to start alignment after the filter."""
    ids = list(run_ids)
    alignments = [i for i in ids if (jm := state.jobs.get(i)) is not None and jm.job_type == JobType.TS_ALIGNMENT]
    return [(f, a) for f in dl_auto_filters(state, ids) for a in alignments]
