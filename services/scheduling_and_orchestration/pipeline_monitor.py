"""
Server-side pipeline observer + restart recovery.

PipelineMonitor is the *single observer* of pipeline status across all
projects loaded in this server process. It replaces the per-tab status
pollers that previously each called sync_all_jobs on their own 3-s timer
and that diverged from each other (landing-page roster read raw
project_params.json with no reconciliation; the workspace tab reconciled;
neither did anything when failure happened in a closed tab).

Responsibilities:

  1. On startup, scan the configured project base for projects with
     `pipeline_active=True` and load them into the in-memory registry.

  2. For each such project, reconcile state with on-disk markers and,
     if the schemer was killed mid-pipeline (server restart), re-deploy
     a fresh scheme for the remaining incomplete jobs. Existing
     External/jobNNN dirs are reused, so .task_status/*.ok markers from
     the prior run let per-TS retries skip already-succeeded tasks.

  3. Run a single async tick (3 s) over every project in the registry
     with `pipeline_active=True`, calling
     `PipelineRunnerService.sync_all_jobs`. That call is the canonical
     reconciler: it patches `default_pipeline.star` Running→Succeeded/Failed
     from `RELION_JOB_EXIT_*` markers, writes the changes to in-memory
     `job_model.execution_status`, persists `project_params.json`, and on
     any per-job failure invokes `stop_and_cleanup` (which clears
     `state.pipeline_active`).

All UI surfaces (landing page, workspace pipeline indicator, project hub
dialog) now read from `state.pipeline_active` and `job_model.execution_status`
in memory; they no longer reconcile themselves. The per-tab status
refresher (`StatusPoller`) is kept as a thin UI-refresh tick so the roster
and run-slot rebuild themselves when state changes — but it does NOT call
`sync_all_jobs` anymore.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)

TICK_INTERVAL_SEC = 3.0


class PipelineMonitor:
    def __init__(self, backend: "CryoBoostBackend"):
        self._backend = backend
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._recovered_paths: set[Path] = set()

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
        """Scan the default project base for projects whose persisted
        `pipeline_active=True` flag is the schemer's last word. Load each
        one into the registry, reconcile against disk, and re-deploy the
        remainder if its schemer is gone."""
        base = self._backend.config_service.default_project_base
        if not base:
            logger.info("Recovery: no default_project_base configured; skipping")
            return
        base_path = Path(base)
        if not base_path.exists() or not base_path.is_dir():
            logger.info("Recovery: base path %s missing; skipping", base_path)
            return

        candidates: list[Path] = []
        try:
            for item in base_path.iterdir():
                if not item.is_dir():
                    continue
                params_file = item / "project_params.json"
                if not params_file.exists():
                    continue
                try:
                    with open(params_file) as f:
                        data = json.load(f)
                except Exception:
                    continue
                if bool(data.get("pipeline_active")):
                    candidates.append(item)
        except Exception:
            logger.exception("Recovery: failed to walk %s", base_path)
            return

        if not candidates:
            logger.info("Recovery: no projects with pipeline_active=True found")
            return

        logger.info("Recovery: %d project(s) had pipeline_active=True", len(candidates))
        for project_path in candidates:
            try:
                await self._recover_one(project_path)
            except Exception:
                logger.exception("Recovery: failed for %s", project_path.name)

    async def _recover_one(self, project_path: Path) -> None:
        from services.models_base import JobStatus, JobType
        from services.project_state import get_project_state_for

        state = get_project_state_for(project_path)  # loads + registers
        logger.info("Recovery[%s]: state loaded, reconciling...", project_path.name)

        # Step 1: reconcile what's on disk. sync_all_jobs walks
        # RELION_JOB_EXIT_* markers, patches default_pipeline.star, and
        # — critically — its self-heal branch at pipeline_runner.py:132
        # clears `pipeline_active` for us because is_active() now reports
        # False (in-memory _active_processes evaporated with uvicorn).
        await self._backend.pipeline_runner.sync_all_jobs(str(project_path))
        state = get_project_state_for(project_path)

        # Step 2: figure out what's left to run. Anything still RUNNING
        # is owned by SLURM and will write its own exit marker — leave it
        # alone; later ticks will catch the transition. Anything SCHEDULED
        # never started (or the schemer never advanced to it).
        running_left: list[str] = []
        scheduled_left: list[str] = []
        for iid, model in state.jobs.items():
            if model.job_type in (JobType.IMPORT_MOVIES, JobType.TS_IMPORT):
                continue
            if getattr(model, "IS_INTERACTIVE", False):
                continue
            if model.execution_status == JobStatus.RUNNING:
                running_left.append(iid)
            elif model.execution_status == JobStatus.SCHEDULED:
                scheduled_left.append(iid)

        if running_left:
            logger.info(
                "Recovery[%s]: %d job(s) still RUNNING in SLURM; deferring re-deploy until they settle",
                project_path.name,
                len(running_left),
            )
            # Re-mark active so the tick loop keeps watching this project.
            # The in-memory model is the source of truth; the persisted
            # flag was already cleared by sync_all_jobs's self-heal.
            state.pipeline_active = True
            state.mark_dirty()
            await self._backend.state_service.save_project(project_path=project_path, force=True)
            self._recovered_paths.add(project_path.resolve())
            return

        if not scheduled_left:
            logger.info("Recovery[%s]: nothing incomplete to resume", project_path.name)
            return

        logger.info(
            "Recovery[%s]: re-deploying %d incomplete job(s): %s",
            project_path.name,
            len(scheduled_left),
            scheduled_left,
        )
        result = await self._backend.pipeline_orchestrator.deploy_and_run_scheme(
            project_dir=project_path, selected_instance_ids=scheduled_left
        )
        if result.get("success"):
            logger.info("Recovery[%s]: re-deploy ok (pid=%s)", project_path.name, result.get("pid"))
            self._recovered_paths.add(project_path.resolve())
        else:
            logger.warning("Recovery[%s]: re-deploy failed: %s", project_path.name, result)

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
            except asyncio.TimeoutError:
                continue

    async def _tick_once(self) -> None:
        from services.project_state import _project_states

        # Snapshot to avoid mutation-during-iteration when a new project
        # opens or closes mid-tick.
        targets = [path for path, state in list(_project_states.items()) if state.pipeline_active]
        if not targets:
            return

        for project_path in targets:
            # sbatch errors land on the schemer's stderr while the row in
            # default_pipeline.star still reads "Running" — sync_all_jobs
            # alone can't detect this because no exit marker is written
            # for a job that never sbatched cleanly. Used to live in the
            # per-tab status_poller; moved here so it fires even when no
            # workspace tab is open.
            try:
                sbatch_errors = self._backend.pipeline_runner.get_sbatch_errors(project_path)
            except Exception:
                sbatch_errors = []
            if sbatch_errors:
                logger.warning(
                    "Monitor[%s]: sbatch error detected, tearing down: %s", project_path.name, sbatch_errors[0]
                )
                try:
                    await self._backend.pipeline_runner.stop_pipeline(project_path)
                    await self._backend.pipeline_runner.reset_submission_failure(project_path)
                except Exception:
                    logger.exception("Monitor[%s]: sbatch-error cleanup failed", project_path.name)

            try:
                await self._backend.pipeline_runner.sync_all_jobs(str(project_path))
            except Exception:
                logger.exception("Monitor: sync_all_jobs failed for %s", project_path)

            # Second-chance recovery: if a project we deferred earlier
            # (jobs were RUNNING) has now caught up — no more RUNNING,
            # SCHEDULED remain, no active schemer — re-deploy the
            # remainder. Idempotent: subsequent ticks will see pipeline
            # active = True (deploy sets it) and skip this branch.
            if project_path.resolve() in self._recovered_paths:
                await self._maybe_resume_deferred(project_path)

    async def _maybe_resume_deferred(self, project_path: Path) -> None:
        from services.models_base import JobStatus, JobType
        from services.project_state import get_project_state_for

        if self._backend.pipeline_runner.is_active(project_path):
            return

        state = get_project_state_for(project_path)
        if not state.pipeline_active:
            # Reconcile already wrapped this project up — nothing to do.
            self._recovered_paths.discard(project_path.resolve())
            return

        running: list[str] = []
        scheduled: list[str] = []
        for iid, model in state.jobs.items():
            if model.job_type in (JobType.IMPORT_MOVIES, JobType.TS_IMPORT):
                continue
            if getattr(model, "IS_INTERACTIVE", False):
                continue
            if model.execution_status == JobStatus.RUNNING:
                running.append(iid)
            elif model.execution_status == JobStatus.SCHEDULED:
                scheduled.append(iid)

        if running or not scheduled:
            return

        # Clear pipeline_active so deploy_and_run_scheme's guard passes;
        # deploy will set it back to True on success.
        state.pipeline_active = False
        state.mark_dirty()
        await self._backend.state_service.save_project(project_path=project_path, force=True)

        logger.info("Monitor[%s]: resuming deferred re-deploy of %s", project_path.name, scheduled)
        result = await self._backend.pipeline_orchestrator.deploy_and_run_scheme(
            project_dir=project_path, selected_instance_ids=scheduled
        )
        if not result.get("success"):
            logger.warning("Monitor[%s]: deferred re-deploy failed: %s", project_path.name, result)
        self._recovered_paths.discard(project_path.resolve())
