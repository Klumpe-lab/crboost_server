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
    """How the tilt filter reaches its verdict. Both modes wait for a human's Approve."""

    MANUAL = "manual"  # hand labels only; predictions are ignored
    DL_REVIEW = "dl_review"  # DL predictions, run on request, stand in for the labels nobody set


class TiltFilterCommit(BaseModel):
    """What the last Approve committed, for the job row's "Approved · D of T dropped". A record
    only: the committed flag is execution_status SUCCEEDED, which the dashboard reads too."""

    at: datetime = Field(default_factory=datetime.now)
    mode: FilterMode
    kept: int
    dropped: int
    model: str | None = None  # the model behind the predictions (DL review, when its last run succeeded)
    threshold: float | None = None  # DL review only


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


class TiltFilterParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.TILT_FILTER)

    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    IS_INTERACTIVE: ClassVar[bool] = True

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
    threshold: float = Field(
        default=0.5, ge=0.0, le=1.0, description="A tilt with P(bad) at or above this is predicted bad"
    )
    dl_batch_size: int = Field(default=32, ge=1, le=256, description="Batch size for DL inference")
    tilt_labels: dict[str, str] = Field(default_factory=dict, description="Manual good/bad label overrides by tilt key")
    predict_run: TiltFilterPredictRun | None = None
    last_commit: TiltFilterCommit | None = None

    @property
    def predict_in_flight(self) -> bool:
        return self.predict_run is not None and self.predict_run.status in (JobStatus.QUEUED, JobStatus.RUNNING)

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
    if not Path(entry.path).is_file():
        raise ValueError(f"Model '{name}': weights file not found at {entry.path}.")
    return name, entry


# A series whose P(bad) spread stays below this has predictions that do not depend on the image.
LIVENESS_MIN_STD = 0.05


def prediction_liveness(p_bad_by_series: Mapping[str, Sequence[float]]) -> tuple[bool, float] | None:
    """Whether a run's predictions carry information, and their mean P(bad).

    Dead means every series with 2+ predictions has a P(bad) spread below LIVENESS_MIN_STD:
    the network answers the same whatever the image. One clean series under a live model can
    have a narrow spread, so a single varying series is enough to count as live. None when no
    series has 2 predictions to compare."""
    spreads = [statistics.pstdev(v) for v in p_bad_by_series.values() if len(v) >= 2]
    if not spreads:
        return None
    mean = statistics.fmean(p for v in p_bad_by_series.values() for p in v)
    return any(s >= LIVENESS_MIN_STD for s in spreads), mean


# ── commit-time verdict (shared by the DL and manual-label paths) ──


async def finalize_pipeline_output(state, job_model, ts_data, project_path: Path) -> dict:
    """Commit the tilt-filter verdict: stamp every tilt's keep/drop decision into the
    TiltSeries registry, which is what alignment reads when it snapshots the tomostar
    dir. Runs at commit for both the DL-assisted and manual-labelling paths (the SLURM
    driver only runs for the DL pass; manual labelling never dispatches it).

    Has no upstream dependency: the registry exists from import onward, so
    the user can commit before, after, or without tsImport having run. Re-stamps in both
    directions on every commit, so un-labelling a tilt restores it. Downstream jobs pick
    the new verdict up when they are (re)queued -- the forward-only staleness convention.

    Returns ok(kept=..., dropped=...) or err(...); the UI caller surfaces the outcome.
    A failure here means the cut was not recorded, so the caller must not mark the job
    succeeded."""
    from services.tilt_series import get_registry_for

    df = ts_data.all_tilts_df
    if "cryoBoostDlLabel" not in df.columns or "cryoBoostKey" not in df.columns:
        return err("Cannot commit: the tilt table has no labels (expected cryoBoostKey + cryoBoostDlLabel columns).")

    try:
        registry = get_registry_for(project_path)
    except Exception:
        logger.exception("tilt-filter commit: registry unavailable")
        return err("Cannot commit: the TiltSeries registry could not be loaded. Reload the project and retry.")

    if not registry.tilt_series_ids():
        return err("Cannot commit: the TiltSeries registry is empty. Reload the project to backfill it from mdocs.")

    kept = dropped = 0
    unknown: list[str] = []
    for stem, is_filt in zip(df["cryoBoostKey"], (df["cryoBoostDlLabel"] != "good"), strict=True):
        try:
            registry.set_frame_filtered(str(stem), bool(is_filt), reason="tilt-filter" if is_filt else None)
        except KeyError:
            # A labelled tilt the registry has never heard of means the verdict for it
            # would be lost silently -- report it rather than quietly under-filtering.
            unknown.append(str(stem))
            continue
        if is_filt:
            dropped += 1
        else:
            kept += 1

    if unknown:
        shown = ", ".join(unknown[:3]) + (f" (+{len(unknown) - 3} more)" if len(unknown) > 3 else "")
        return err(
            f"Cannot commit: {len(unknown)} labelled tilts are not in the registry ({shown}). Reload the project."
        )

    try:
        await asyncio.to_thread(registry.save)
    except Exception:
        logger.exception("tilt-filter commit: registry save failed")
        return err("Cannot commit: writing the tilt verdict to the registry failed. See the server log.")

    # Older projects carry output slots from when this job produced its own tomostar.
    for dead in ("output_tomostar", "output_star", "output_processing"):
        job_model.paths.pop(dead, None)

    return ok(kept=kept, dropped=dropped)


# ── the review: labels, Approve, Re-open (shared by the gallery and the job row) ──


def predictions_for(project_path: Path, keys: Iterable[str]) -> dict[str, float]:
    """P(bad) per tilt key from the latest prediction run, as the registry holds it. Tilts
    without a prediction, or unknown to the registry, are absent."""
    from services.tilt_series import get_registry_for

    registry = get_registry_for(Path(project_path))
    p_bad: dict[str, float] = {}
    for key in keys:
        try:
            p = registry.get_frame(key).p_bad
        except KeyError:  # a tilt the registry does not know has no prediction to show
            continue
        if p is not None:
            p_bad[key] = p
    return p_bad


def effective_label(
    key: str, p_bad: float | None, labels: Mapping[str, str], threshold: float, mode: FilterMode
) -> str:
    """A tilt's label as Approve commits it: a human's label wins; in DL review, the prediction
    at the threshold; otherwise good. `p_bad` is None or NaN for a tilt without a prediction
    (NaN compares false)."""
    human = labels.get(key)
    if human:
        return human
    if mode == FilterMode.DL_REVIEW and p_bad is not None and p_bad >= threshold:
        return "bad"
    return "good"


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

    Each tilt gets its effective label from the job's labels, mode and threshold and the
    registry's predictions, so the verdict does not depend on the gallery being open. Sets
    SUCCEEDED and `last_commit`; the caller saves the project. Refused while a prediction run
    is in flight (its predictions are about to change) and once alignment is running or has run
    (it has taken the verdict it keeps)."""
    from services.tilt_series_service import fs_motion_star, load_tilt_series

    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"Job '{instance_id}' not found.")
    if job_model.predict_in_flight:
        return err("A DL prediction run is in flight; approve once its predictions have landed.")
    locked = _alignment_with_status(state, (JobStatus.RUNNING, JobStatus.SUCCEEDED))
    if locked:
        return err(_verdict_locked_reason(*locked))
    star = fs_motion_star(project_path, state)
    if star is None:
        return err("The motion-correction output star is not on disk yet; run fsMotion first.")
    try:
        ts_data = await asyncio.to_thread(load_tilt_series, star, project_path)
    except Exception:
        logger.exception("tilt-filter commit: could not load %s", star)
        return err(f"Cannot commit: the tilt table {star.name} could not be read. See the server log.")

    df = ts_data.all_tilts_df
    dl = job_model.mode == FilterMode.DL_REVIEW
    p_bad = predictions_for(project_path, df["cryoBoostKey"].unique()) if dl else {}
    df["cryoBoostDlLabel"] = [
        effective_label(key, p_bad.get(key), job_model.tilt_labels, job_model.threshold, job_model.mode)
        for key in df["cryoBoostKey"]
    ]
    res = await finalize_pipeline_output(state, job_model, ts_data, project_path)
    if not res["success"]:
        return res

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


def reopen_review(state, instance_id: str) -> dict:
    """Undo an Approve: the filter reads unapproved again, so the next Run waits for a review.
    The registry keeps the committed verdict until the next Approve re-stamps it. Refused once
    alignment is queued, running or has run: the review could no longer hold it back."""
    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"Job '{instance_id}' not found.")
    if job_model.execution_status != JobStatus.SUCCEEDED:
        return err("The tilt filter is not approved.")
    locked = _alignment_with_status(state, (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED))
    if locked:
        return err(_verdict_locked_reason(*locked))
    job_model.execution_status = JobStatus.SCHEDULED
    job_model.last_commit = None
    state.mark_dirty()
    return ok()


def review_barrier(state, run_ids: Iterable[str]) -> str | None:
    """The tilt filter a run has to wait for: one in `run_ids` whose verdict is not committed.
    None when the run holds no tilt filter or its filter is approved."""
    for iid in run_ids:
        jm = state.jobs.get(iid)
        if jm is not None and jm.job_type == JobType.TILT_FILTER and jm.execution_status != JobStatus.SUCCEEDED:
            return iid
    return None
