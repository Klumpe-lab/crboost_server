"""
BackgroundTaskRegistry — server-side tracker for long-running side-jobs.

Recurring problem this addresses: a user clicks a button (rebuild atlas,
render previews, re-generate IMOD models, kick off any non-pipeline async
work), the work spins off, the button maybe goes back to its idle state,
and then nothing — the user has no idea whether it succeeded, failed,
is still in flight, or what tomogram it's currently chewing on. The
pattern repeats across the server (candidate-preview rebuild, IMOD
generation, plus anything we'll add later).

Design:

  - `BackgroundTaskRegistry` is a singleton owned by `services/` (no
    backend dependency, no UI imports). Lives for the server's lifetime.
  - `submit(coro_factory, ...)` registers a task, kicks off an asyncio
    task wrapping the user's work, and returns a `task_id`.
  - The task_factory receives a `progress_cb(current, total, message)`
    that updates the registry's per-task `BackgroundTask` record.
  - UI components (the tray at `ui/background_task_tray.py`) poll the
    registry on their own cadence and render whatever's active + recent.

The registry never touches NiceGUI directly. Reads are pull-based; the
UI tray is responsible for re-rendering on a timer. This keeps the
registry trivial to test (no client context coupling) and means
background tasks survive UI navigation, tab close, dialog close, etc.

Lifecycle:

  - Active tasks stay in the registry until they finish (success / fail /
    cancel) and a small post-completion grace window elapses.
  - Finished tasks are kept long enough for the user to notice them
    (default 5 min), then pruned.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

_FINISHED_RETENTION_SEC = 300.0
_MAX_FINISHED = 100


ProgressCallback = Callable[[int, int, str], None]
CoroFactory = Callable[[ProgressCallback], Awaitable[Any]]


@dataclass
class BackgroundTask:
    id: str
    title: str
    subtitle: Optional[str] = None
    project_path: Optional[str] = None
    dedup_key: Optional[str] = None
    status: str = "running"  # "running" | "succeeded" | "failed" | "cancelled"
    progress_current: int = 0
    progress_total: int = 0
    progress_message: str = ""
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    result_message: Optional[str] = None
    error: Optional[str] = None
    _asyncio_task: Optional[asyncio.Task] = field(default=None, repr=False)

    @property
    def progress_pct(self) -> Optional[int]:
        if self.progress_total <= 0:
            return None
        return min(100, int(round(100.0 * self.progress_current / self.progress_total)))

    @property
    def is_running(self) -> bool:
        return self.status == "running"

    @property
    def duration_sec(self) -> float:
        end = self.finished_at or datetime.now()
        return (end - self.started_at).total_seconds()


class BackgroundTaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, BackgroundTask] = {}

    def submit(
        self,
        coro_factory: CoroFactory,
        *,
        title: str,
        subtitle: Optional[str] = None,
        project_path: Optional[str] = None,
        dedup_key: Optional[str] = None,
    ) -> str:
        """Register and start a background task. `coro_factory` receives the
        progress_cb; it should return an awaitable.

        progress_cb signature: `(current: int, total: int, message: str)`.
        Pass `total <= 0` to indicate indeterminate progress; the UI will
        show a spinning indicator rather than a filled bar.

        If `dedup_key` is provided and a running task already carries the
        same key, the existing task id is returned instead of starting a
        new one. Use this to prevent concurrent renders that would race on
        the same on-disk artifact.
        """
        if dedup_key:
            for t in self._tasks.values():
                if t.is_running and t.dedup_key == dedup_key:
                    logger.info(
                        "BackgroundTaskRegistry: dedup hit on '%s' — returning existing task %s",
                        dedup_key, t.id,
                    )
                    return t.id

        task_id = uuid.uuid4().hex
        record = BackgroundTask(
            id=task_id,
            title=title,
            subtitle=subtitle,
            project_path=project_path,
            dedup_key=dedup_key,
        )
        self._tasks[task_id] = record

        def progress_cb(current: int, total: int, message: str = "") -> None:
            record.progress_current = int(current)
            record.progress_total = int(total)
            record.progress_message = str(message or "")

        async def runner() -> None:
            try:
                result = await coro_factory(progress_cb)
                record.status = "succeeded"
                if isinstance(result, str):
                    record.result_message = result
            except asyncio.CancelledError:
                record.status = "cancelled"
                raise
            except Exception as e:
                record.status = "failed"
                record.error = str(e)
                logger.exception("Background task %s (%s) failed", task_id, title)
            finally:
                record.finished_at = datetime.now()
                self._prune()

        record._asyncio_task = asyncio.create_task(runner(), name=f"bg-task:{task_id}")
        return task_id

    def get(self, task_id: str) -> Optional[BackgroundTask]:
        return self._tasks.get(task_id)

    def all(self) -> list[BackgroundTask]:
        return list(self._tasks.values())

    def for_project(self, project_path: Optional[str]) -> list[BackgroundTask]:
        if not project_path:
            return self.all()
        return [t for t in self._tasks.values() if t.project_path == project_path]

    def snapshot_for_project(
        self, project_path: Optional[str], *, recent_window_sec: float = 30.0
    ) -> tuple[list[BackgroundTask], list[BackgroundTask]]:
        """Return (active, recent_finished) for the given project.
        `recent_finished` includes failed/cancelled within the window."""
        now = datetime.now()
        active: list[BackgroundTask] = []
        recent: list[BackgroundTask] = []
        for t in self.for_project(project_path):
            if t.is_running:
                active.append(t)
            elif t.finished_at and (now - t.finished_at).total_seconds() < recent_window_sec:
                recent.append(t)
        active.sort(key=lambda t: t.started_at)
        recent.sort(key=lambda t: t.finished_at or t.started_at, reverse=True)
        return active, recent

    def cancel(self, task_id: str) -> bool:
        record = self._tasks.get(task_id)
        if record is None or not record.is_running:
            return False
        if record._asyncio_task is not None:
            record._asyncio_task.cancel()
            return True
        return False

    def dismiss(self, task_id: str) -> None:
        """Remove a finished task from the registry. No-op for running tasks
        (cancel first)."""
        record = self._tasks.get(task_id)
        if record is None or record.is_running:
            return
        del self._tasks[task_id]

    def _prune(self) -> None:
        now = datetime.now()
        # Drop finished tasks older than retention window or beyond cap.
        finished = [
            (t.finished_at or t.started_at, t.id)
            for t in self._tasks.values()
            if not t.is_running
        ]
        # Remove anything past the time horizon.
        for ts, tid in finished:
            if (now - ts).total_seconds() > _FINISHED_RETENTION_SEC:
                self._tasks.pop(tid, None)
        # And cap the rest.
        finished = sorted(
            [(t.finished_at or t.started_at, t.id) for t in self._tasks.values() if not t.is_running]
        )
        if len(finished) > _MAX_FINISHED:
            for _, tid in finished[: len(finished) - _MAX_FINISHED]:
                self._tasks.pop(tid, None)


_registry: Optional[BackgroundTaskRegistry] = None


def get_background_task_registry() -> BackgroundTaskRegistry:
    global _registry
    if _registry is None:
        _registry = BackgroundTaskRegistry()
    return _registry
