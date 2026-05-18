"""
BackgroundTaskTray — floating bottom-right tray showing in-flight and
recently-completed BackgroundTaskRecord entries.

Goals:
  - Give every spun-off job (preview renders, IMOD generation, atlas
    rebuilds, anything else > a couple seconds) a visible home so the
    user doesn't experience the "click → spin → silence" black hole.
  - Survive dialog open/close and navigation: the tray is mounted at
    workspace-page scope, so closing the Journey dashboard doesn't lose
    in-flight work indication.
  - Per-card progress bar + cancel/dismiss controls. Recently-finished
    tasks linger for ~30 s so the user can see the outcome even if they
    were on another screen at completion time.

Rendering is poll-based (1.5 s timer) since the registry is a passive
data store. The polling cost is one dict iteration + one DOM rebuild
per active task — negligible.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Hashable, Optional

from nicegui import ui

from services.background_tasks import BackgroundTaskRecord, get_background_task_registry
from ui.components.reactive import FingerprintedView

logger = logging.getLogger(__name__)

_TRAY_REFRESH_SEC = 1.5
_RECENT_WINDOW_SEC = 30.0


class _BackgroundTaskTray(FingerprintedView):
    """Tray view that ticks every 1.5 s but only rebuilds when the visible
    task set or any task's progress fingerprint has changed.

    Without the signature gate this used to `container.clear()` and re-
    render on every tick — cheap when empty but still a constant tick of
    DOM churn on every workspace tab.
    """

    def __init__(self, container: Any, project_path_provider: Callable[[], Optional[str]]) -> None:
        super().__init__(container)
        self._project_path_provider = project_path_provider
        # Cached for render() — populated by signature() each tick.
        self._visible: list[BackgroundTaskRecord] = []

    def signature(self) -> Hashable:
        project_path = (self._project_path_provider() or "") or None
        registry = get_background_task_registry()
        active, recent = registry.snapshot_for_project(project_path, recent_window_sec=_RECENT_WINDOW_SEC)
        self._visible = list(active) + list(recent)
        return tuple(
            (
                t.id,
                t.status,
                t.progress_current,
                t.progress_total,
                t.progress_message,
                t.is_running,
                t.result_message,
                t.error,
            )
            for t in self._visible
        )

    def render(self) -> None:
        for task in self._visible:
            _render_task_card(task, self.refresh)


def mount_background_task_tray(project_path_provider: Callable[[], Optional[str]]) -> None:
    """Mount a floating tray at the page root. `project_path_provider` is
    re-evaluated on every refresh so the tray follows project switches
    without rebuilding."""
    container = ui.element("div").style(
        "position: fixed; bottom: 14px; right: 14px; z-index: 9000; "
        "max-width: 380px; min-width: 280px; display: flex; flex-direction: column; gap: 6px; "
        # `pointer-events: none` on the outer container so empty space
        # between cards doesn't block clicks on whatever's behind. Cards
        # themselves opt back in via pointer-events: auto.
        "pointer-events: none;"
    )

    view = _BackgroundTaskTray(container, project_path_provider)
    view.refresh()
    ui.timer(_TRAY_REFRESH_SEC, view.refresh)


def _render_task_card(task: BackgroundTaskRecord, refresh_tray: Callable[[], None]) -> None:
    color, icon = _status_glyph(task.status)
    card_bg = "#ffffff"
    card_border = "#e2e8f0"
    if task.status == "succeeded":
        card_border = "#a7f3d0"
    elif task.status == "failed":
        card_border = "#fecaca"
    elif task.status == "cancelled":
        card_border = "#fde68a"

    with ui.element("div").style(
        f"background: {card_bg}; border: 1px solid {card_border}; "
        "border-radius: 6px; padding: 8px 10px; "
        "box-shadow: 0 4px 12px rgba(15,23,42,0.08); "
        "pointer-events: auto; font-family: ui-sans-serif, system-ui;"
    ):
        # Title row: icon · title · controls
        with ui.row().classes("w-full items-center").style("gap: 6px; flex-wrap: nowrap;"):
            ui.icon(icon, size="14px").style(f"color: {color}; flex-shrink: 0;")
            with ui.column().classes("flex-1 min-w-0").style("gap: 1px;"):
                ui.label(task.title).style(
                    "font-size: 11px; font-weight: 600; color: #1e293b; "
                    "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
                )
                if task.subtitle:
                    ui.label(task.subtitle).style(
                        "font-size: 9px; color: #64748b; "
                        "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
                    )

            if task.is_running:
                ui.button(icon="close", on_click=lambda t=task: _cancel(t, refresh_tray)).props(
                    "flat dense round size=xs"
                ).classes("text-slate-400 hover:text-red-500").tooltip("Cancel")
            else:
                ui.button(icon="close", on_click=lambda t=task: _dismiss(t, refresh_tray)).props(
                    "flat dense round size=xs"
                ).classes("text-slate-300 hover:text-slate-600").tooltip("Dismiss")

        # Progress + status line
        if task.is_running:
            pct = task.progress_pct
            if pct is not None:
                with ui.element("div").style(
                    "width: 100%; height: 4px; background: #f1f5f9; border-radius: 2px; "
                    "overflow: hidden; margin-top: 6px;"
                ):
                    ui.element("div").style(
                        f"width: {pct}%; height: 100%; background: #3b82f6; transition: width 0.3s;"
                    )
                with ui.row().classes("w-full items-center").style("gap: 6px; margin-top: 3px;"):
                    ui.label(f"{task.progress_current}/{task.progress_total}").style(
                        "font-size: 9px; font-family: ui-monospace, monospace; color: #475569;"
                    )
                    if task.progress_message:
                        ui.label(task.progress_message).style(
                            "font-size: 9px; color: #94a3b8; flex: 1; "
                            "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; "
                            "font-family: ui-monospace, monospace;"
                        )
            else:
                # Indeterminate
                with ui.row().classes("w-full items-center").style("gap: 6px; margin-top: 4px;"):
                    ui.spinner("dots", size="xs").style("color: #3b82f6;")
                    msg = task.progress_message or "working…"
                    ui.label(msg).style(
                        "font-size: 9px; color: #64748b; "
                        "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
                    )
        else:
            # Finished — show outcome line.
            outcome = _outcome_text(task)
            if outcome:
                ui.label(outcome).style(
                    "font-size: 9px; color: #64748b; margin-top: 3px; "
                    "font-family: ui-monospace, monospace; "
                    # Multi-line for error messages; default wrap.
                    "word-break: break-word;"
                )


def _status_glyph(status: str) -> tuple[str, str]:
    if status == "succeeded":
        return "#16a34a", "check_circle"
    if status == "failed":
        return "#dc2626", "error"
    if status == "cancelled":
        return "#f59e0b", "block"
    return "#3b82f6", "sync"  # running


def _outcome_text(task: BackgroundTaskRecord) -> str:
    secs = task.duration_sec
    duration = f"{secs:.0f}s" if secs >= 1 else f"{secs*1000:.0f}ms"
    if task.status == "succeeded":
        if task.result_message:
            return f"{task.result_message} · {duration}"
        return f"done · {duration}"
    if task.status == "failed":
        return f"failed: {task.error or 'unknown error'} · {duration}"
    if task.status == "cancelled":
        return f"cancelled · {duration}"
    return ""


def _cancel(task: BackgroundTaskRecord, refresh_tray: Callable[[], None]) -> None:
    if get_background_task_registry().cancel(task.id):
        try:
            ui.notify(f"Cancelled: {task.title}", type="warning", timeout=2000)
        except RuntimeError:
            pass
    refresh_tray()


def _dismiss(task: BackgroundTaskRecord, refresh_tray: Callable[[], None]) -> None:
    get_background_task_registry().dismiss(task.id)
    refresh_tray()
