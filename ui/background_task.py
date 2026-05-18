"""
BackgroundTask — UI-side wrapper around services.background_tasks.BackgroundTaskRegistry.

Captures the standard "click button → submit task → show toast → optionally
auto-swap on completion" pattern. Without this, every call site duplicates:

  1. registry.submit(...)
  2. detect dedup hit and emit different notify text
  3. install a ui.timer to poll for completion
  4. handle success vs failure UI

Collapsed to:

  task_id = BackgroundTask(
      title="Resample template",
      subtitle="box 64 → 32",
      dedup_key=f"resample:{template_id}",
      project_path=str(project_path),
  ).submit(_run, on_complete=refresh)

Persistence model:

  The registry is a server-side singleton and survives page navigation;
  tasks continue to run regardless of which page is mounted. What is
  per-mount is the inline UI: toasts only fire on the page they're
  triggered from, and on_complete callbacks ride a ui.timer that dies
  with the page. On re-mount, callers can use `BackgroundTask.existing()`
  to detect a still-running task with a matching dedup_key and
  `BackgroundTask.attach()` to wire a fresh completion handler.

The floating tray (ui/background_task_tray.py) is the cross-page
progress surface; mount it on every route where users can see or trigger
background work.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Optional

from nicegui import ui

from services.background_tasks import (
    BackgroundTaskRecord,
    ProgressCallback,
    get_background_task_registry,
)

logger = logging.getLogger(__name__)

_DEFAULT_POLL_SEC = 2.0


class BackgroundTask:
    """Builder + submission for a single user-triggered background task.

    Instances are cheap — construct one per click. The task itself lives in
    the global registry and outlives this object."""

    def __init__(
        self,
        *,
        title: str,
        subtitle: Optional[str] = None,
        dedup_key: Optional[str] = None,
        project_path: Optional[str] = None,
    ) -> None:
        self.title = title
        self.subtitle = subtitle
        self.dedup_key = dedup_key
        self.project_path = project_path

    def submit(
        self,
        work: Callable[[ProgressCallback], Awaitable[Any]],
        *,
        on_complete: Optional[Callable[[BackgroundTaskRecord], None]] = None,
        on_progress: Optional[Callable[[BackgroundTaskRecord], None]] = None,
        poll_interval: float = _DEFAULT_POLL_SEC,
        show_start_toast: bool = True,
    ) -> str:
        """Submit `work` to the registry; show the standard toast.

        `work` is an async callable receiving a `progress_cb(done, total, msg)`
        and returning any value (str returns are displayed as the task's
        summary in the tray).

        If `on_complete` or `on_progress` is provided, a ui.timer polls
        the registry every `poll_interval` seconds:
          - while task.is_running: invoke `on_progress(task)` (if set)
          - on settlement (status != "running"): invoke `on_complete(task)`
            (if set), then cancel the timer.
        If the user navigates away before completion, the timer dies — the
        task keeps running but on_progress/on_complete will not fire until
        a re-mount attaches a fresh poll (see `attach`).

        `show_start_toast=False` suppresses the start notification; useful
        when the caller renders its own inline status.

        Returns the task_id (same id the registry would return).
        """
        registry = get_background_task_registry()

        already_running = None
        if self.dedup_key:
            already_running = next(
                (t for t in registry.all() if t.is_running and t.dedup_key == self.dedup_key),
                None,
            )

        task_id = registry.submit(
            work,
            title=self.title,
            subtitle=self.subtitle,
            project_path=self.project_path,
            dedup_key=self.dedup_key,
        )

        if show_start_toast:
            try:
                if already_running is not None:
                    ui.notify(
                        f"{self.title} already in flight — see tray (bottom-right)",
                        type="info", timeout=2500,
                    )
                else:
                    ui.notify(
                        f"{self.title} started — track progress in the tray (bottom-right)",
                        type="info", timeout=2500,
                    )
            except RuntimeError:
                # Client gone — task still submitted, just no toast.
                pass

        if on_complete is not None or on_progress is not None:
            _install_completion_timer(task_id, on_complete, on_progress, poll_interval)

        return task_id

    @staticmethod
    def existing(dedup_key: str) -> Optional[BackgroundTaskRecord]:
        """Return the running task with this dedup_key, or None.

        Use on panel re-mount to detect a task submitted before navigation
        so the panel can attach a fresh poll and show in-flight state
        instead of re-offering the trigger UI."""
        registry = get_background_task_registry()
        return next(
            (t for t in registry.all() if t.is_running and t.dedup_key == dedup_key), None
        )

    @staticmethod
    def attach(
        task_id: str,
        *,
        on_complete: Optional[Callable[[BackgroundTaskRecord], None]] = None,
        on_progress: Optional[Callable[[BackgroundTaskRecord], None]] = None,
        poll_interval: float = _DEFAULT_POLL_SEC,
    ) -> None:
        """Wire callbacks to an existing in-flight task.

        Used on re-mount to re-establish handlers after the previous
        poll timer died with the previous page."""
        _install_completion_timer(task_id, on_complete, on_progress, poll_interval)


def _install_completion_timer(
    task_id: str,
    on_complete: Optional[Callable[[BackgroundTaskRecord], None]],
    on_progress: Optional[Callable[[BackgroundTaskRecord], None]],
    poll_interval: float,
) -> None:
    """Install a ui.timer that polls for `task_id`:
      - while running: invoke on_progress(task) per tick
      - on settlement: invoke on_complete(task) once and cancel.

    Errors raised by either callback are logged, never propagated — a
    callback bug must not break the timer."""
    registry = get_background_task_registry()

    def _tick() -> None:
        task = registry.get(task_id)
        if task is None:
            # Task evicted from registry before we observed completion;
            # nothing to deliver.
            timer.cancel()
            return
        if task.is_running:
            if on_progress is not None:
                try:
                    on_progress(task)
                except Exception:
                    logger.exception("BackgroundTask on_progress callback failed for task %s", task_id)
            return
        timer.cancel()
        if on_complete is not None:
            try:
                on_complete(task)
            except Exception:
                logger.exception("BackgroundTask on_complete callback failed for task %s", task_id)

    timer = ui.timer(poll_interval, _tick)
