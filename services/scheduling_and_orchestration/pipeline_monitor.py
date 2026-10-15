"""
Server-side pipeline observer + restart recovery.

PipelineMonitor is the single observer of pipeline status across all
projects loaded in this server process. Reconciliation runs here rather
than in per-tab pollers so every surface sees the same state and failures
are caught even when no tab is open.

Responsibilities:

  1. On startup, scan the configured project base for projects with
     `pipeline_active=True` and load them into the in-memory registry.
     Their chains live in SLURM and went on running; the tick below
     re-observes them.

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
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)

TICK_INTERVAL_SEC = 3.0


def _predict_in_flight(state) -> bool:
    """Whether a tilt-filter DL prediction run of this project is queued or running."""
    return any(getattr(jm, "predict_in_flight", False) for jm in state.jobs.values())


class PipelineMonitor:
    def __init__(self, backend: CryoBoostBackend):
        self._backend = backend
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

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
        """Scan the default project base for projects persisted with
        `pipeline_active=True` and load each one into the registry, so the
        tick reconciles it."""
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
        from services.project_state import get_project_state_for

        # The chain lives entirely in SLURM, so loading the state into the registry is all
        # recovery needs: the next tick's reconcile_afterok re-observes it.
        get_project_state_for(project_path)
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
        targets = [
            (path, state)
            for path, state in list(_project_states.items())
            if state.pipeline_active or _predict_in_flight(state)
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
