"""
Server-side pipeline observer + restart recovery.

PipelineMonitor is the single observer of pipeline status across all
projects loaded in this server process. Reconciliation runs here rather
than in per-tab pollers so every surface sees the same state and failures
are caught even when no tab is open.

Responsibilities:

  1. On startup, scan the bases the user works out of (the landing page's
     base, the recent roots, the configured DefaultProjectBase) for projects
     with `pipeline_active=True` and load them into the in-memory registry.
     Their chains live in SLURM and went on running; the tick below
     re-observes them. Outside DefaultProjectBase only the user's own
     projects are taken. A roster scan hands its rows to `adopt`, which does
     the same for an active project found anywhere later.

  2. Run a single async tick (3 s) over every project in the registry
     with `pipeline_active=True`, calling
     `PipelineRunnerService.reconcile_afterok`. That call is the canonical
     reconciler: it reads the `RELION_JOB_EXIT_*` markers, squeue and
     sacct, writes the changes to in-memory `job_model.execution_status`,
     persists `project_params.json`, and clears `state.pipeline_active`
     once no job is queued or running.

  3. On the same tick, settle any in-flight tilt-filter DL prediction run
     (`reconcile_tilt_filter_predict`), with or without an active pipeline.

All UI surfaces (landing page, workspace pipeline indicator, project hub
dialog) read `state.pipeline_active` and `job_model.execution_status`
in memory and do not reconcile. The per-tab `StatusPoller` is a thin
UI-refresh tick so the roster and run-slot rebuild when state changes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from services.configs.user_prefs_service import get_prefs_service
from services.event_log import events
from services.models_base import JobStatus
from services.project_resolve import known_project_bases
from services.scheduling_and_orchestration import stop_after_iteration

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)

TICK_INTERVAL_SEC = 3.0
# How often the monitor looks for a landed iteration a stop waits for (disk: job folders' globs and stats).
STOP_CHECK_SEC = 15.0


def _due_stops(running: dict[str, Path]) -> dict[str, int]:
    """Of these running jobs' folders, those whose asked stop is due, with the iteration it stops after."""
    return {iid: it for iid, job_dir in running.items() if (it := stop_after_iteration.due(job_dir)) is not None}


def _predict_in_flight(state) -> bool:
    """Whether a tilt-filter DL prediction run of this project is queued or running."""
    return any(getattr(jm, "predict_in_flight", False) for jm in state.jobs.values())


def _owner_of(data: dict) -> str | None:
    """Whose project a saved project_params.json (or a roster scan row) says it is."""
    return data.get("owner") or data.get("created_by") or data.get("creator")


def _projects_with_active_pipeline(base_path: Path, owner: str | None = None) -> list[Path]:
    """The project dirs under `base_path` whose saved state has `pipeline_active` set, and, when
    `owner` is given, belong to that user. Disk only, for a thread."""
    found: list[Path] = []
    for item in base_path.iterdir():
        if not item.is_dir():
            continue
        params_file = item / "project_params.json"
        if not params_file.exists():
            continue
        try:
            with open(params_file) as f:
                data = json.load(f)
        except (OSError, ValueError):
            # Unreadable or mid-write: not a project recovery can load; the next roster scan adopts it.
            continue
        if bool(data.get("pipeline_active")) and (owner is None or _owner_of(data) == owner):
            found.append(item)
    return found


def _resolved(path: Path) -> Path:
    try:
        return path.resolve()
    except OSError:  # unreachable mount: compare it unresolved
        return path


class PipelineMonitor:
    def __init__(self, backend: CryoBoostBackend):
        self._backend = backend
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        # Projects `adopt` is loading, and its load tasks (held, or the loop may drop them).
        self._adopting: set[Path] = set()
        self._adopt_tasks: set[asyncio.Task] = set()
        self._stop_checked_at = 0.0

    async def start(self) -> None:
        await self._discover_and_recover()
        self._task = asyncio.create_task(self._loop(), name="pipeline-monitor")
        logger.info("PipelineMonitor started (tick=%.1fs)", TICK_INTERVAL_SEC)

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("PipelineMonitor stop: task raised")
        self._task = None
        logger.info("PipelineMonitor stopped")

    # ── Recovery ─────────────────────────────────────────────────────────

    async def _discover_and_recover(self) -> None:
        """Load every project persisted with `pipeline_active=True` in the bases the user works out
        of into the registry, so the tick reconciles it. Without this, a project outside the
        configured base sits unreconciled after a restart until someone opens it, and the projects
        overview shows its state from before the restart."""
        default = self._backend.config_service.default_project_base
        default_key = _resolved(Path(default)) if default else None
        # No page has loaded the user's prefs yet: the bases come from the server-side copy.
        await asyncio.to_thread(get_prefs_service().load_from_file_if_unloaded)
        candidates: list[Path] = []
        for base_path in known_project_bases():
            # DefaultProjectBase as it always was: every active project there. Elsewhere (a base
            # the landing page pointed at, a recent root) only the user's own.
            owner = None if _resolved(base_path) == default_key else self._backend.username
            try:
                if not base_path.is_dir():
                    logger.info("Recovery: base path %s missing; skipping", base_path)
                    continue
                # Off the loop: it parses every project_params.json under the base, which stalled
                # startup by half a second on a real base.
                candidates += await asyncio.to_thread(_projects_with_active_pipeline, base_path, owner)
            except Exception:
                logger.exception("Recovery: failed to walk %s", base_path)

        if not candidates:
            logger.info("Recovery: no projects with pipeline_active=True found")
            return

        logger.info("Recovery: %d project(s) had pipeline_active=True", len(candidates))
        for project_path in candidates:
            try:
                await self._recover_one(project_path)
            except Exception:
                logger.exception("Recovery: failed for %s", project_path.name)

    def adopt(self, rows: list[dict]) -> None:
        """Load the active projects of a roster scan that this process has not loaded, the user's own
        only, so the next tick reconciles them. A project is otherwise reconciled only once someone
        opens it (or recovery found it at startup), and every roster shows it as it was on disk.
        Returns at once; the loads run in the background."""
        from services.project_state import _project_states, outside_base_problem

        for row in rows:
            if not row.get("pipeline_active") or _owner_of(row) != self._backend.username:
                continue
            path = Path(row.get("resolved_path") or row["path"])
            if path in _project_states or path in self._adopting:
                continue
            # A server that keeps to its base leaves the others' projects to the server that runs them.
            if outside_base_problem(path):
                continue
            self._adopting.add(path)
            task = asyncio.create_task(self._adopt_one(path), name=f"pipeline-monitor-adopt:{path.name}")
            self._adopt_tasks.add(task)
            task.add_done_callback(self._adopt_tasks.discard)

    async def _adopt_one(self, project_path: Path) -> None:
        try:
            await self._recover_one(project_path)
        except Exception:
            logger.exception("Adopt: failed to load %s", project_path.name)
        finally:
            self._adopting.discard(project_path)

    async def _recover_one(self, project_path: Path) -> None:
        from services.project_state import preload_project_state

        # The chain lives entirely in SLURM, so loading the state into the registry is all
        # recovery needs: the next tick's reconcile_afterok re-observes it.
        await preload_project_state(project_path)
        logger.info("Recovery[%s]: state loaded; the next tick reconciles it", project_path.name)

    # ── Tick loop ────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._tick_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("PipelineMonitor tick failed")
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=TICK_INTERVAL_SEC)
            except TimeoutError:
                continue

    async def _tick_once(self) -> None:
        from services.project_state import _project_states

        # Snapshot to avoid mutation-during-iteration when a new project
        # opens or closes mid-tick.
        # A project newer code wrote is the newer server's to follow: this one could not save what it sees.
        targets = [
            (path, state)
            for path, state in list(_project_states.items())
            if (state.pipeline_active or _predict_in_flight(state)) and not state.read_only_reason
        ]
        if not targets:
            return

        for project_path, state in targets:
            if _predict_in_flight(state):
                try:
                    await self._backend.pipeline_runner.reconcile_tilt_filter_predict(str(project_path))
                except Exception:
                    logger.exception("Monitor: tilt-filter prediction reconcile failed for %s", project_path)
            if not state.pipeline_active:
                continue
            try:
                await self._backend.pipeline_runner.reconcile_afterok(str(project_path))
            except Exception:
                logger.exception("Monitor: reconcile_afterok failed for %s", project_path)
        await self._stop_after_iterations(targets)

    async def _stop_after_iterations(self, targets: list) -> None:
        """Cancel each running Refine 3D / Class 3D whose stop was asked once a later iteration has landed
        (`stop_after_iteration`), and record where it stopped. Every STOP_CHECK_SEC, its disk reads in a thread."""
        now = time.monotonic()
        if now - self._stop_checked_at < STOP_CHECK_SEC:
            return
        self._stop_checked_at = now
        for project_path, state in targets:
            running = {
                iid: project_path / jm.relion_job_name.rstrip("/")
                for iid, jm in list(state.jobs.items())
                if state.pipeline_active
                and jm.job_type in stop_after_iteration.STOPPABLE
                and jm.execution_status == JobStatus.RUNNING
                and jm.relion_job_name
            }
            if not running:
                continue
            due = await asyncio.to_thread(_due_stops, running)
            for iid, iteration in due.items():
                res = await self._backend.pipeline_runner.cancel_job(project_path, iid)
                if not res["success"]:
                    logger.warning(
                        "Stop after iteration: %s %s not cancelled: %s", project_path.name, iid, res["error"]
                    )
                    continue
                await asyncio.to_thread(stop_after_iteration.settle, running[iid], iteration)
                events.info(
                    "%s · %s stopped after iteration %d, as asked; run again, it goes on from there",
                    project_path.name,
                    iid,
                    iteration,
                )
