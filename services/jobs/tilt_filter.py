from __future__ import annotations
import asyncio
import logging
import statistics
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import ClassVar
from pydantic import BaseModel, Field

from services.configs.config_service import TiltFilterModelConfig, get_config_service
from services.jobs._base import AbstractJobParams
from services.models_base import JobStatus, JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.result import err, ok

logger = logging.getLogger(__name__)


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
