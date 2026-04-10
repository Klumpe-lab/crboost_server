# ui/components/array_task_tracker.py
"""
Generic per-item task tracker for SLURM array jobs.

Any job that fans out into per-item array tasks can use this component.
The only requirement is the file-based contract written by the supervisor:

    {job_dir}/
    ├── .task_manifest.json   # {"items": [...], "item_label": "Tilt Series", ...}
    ├── .task_status/
    │   ├── {name}.ok
    │   └── {name}.fail
    ├── task_0.out
    └── task_0.err

Register as an extra tab via the plugin system:

    @register_extra_tab(JobType.YOUR_JOB, key="tasks", label="Tasks", icon="view_list")
    def render(job_type, job_model, backend, ui_mgr):
        render_array_task_tracker(job_model, ui_mgr)
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from nicegui import ui

from ui.styles import MONO

# ── Status constants ──

_OK = "ok"
_FAIL = "fail"
_RUNNING = "running"
_PENDING = "pending"

_CHIP = {
    _OK: ("check_circle", "#16a34a", "#f0fdf4"),
    _FAIL: ("error", "#dc2626", "#fef2f2"),
    _RUNNING: ("sync", "#2563eb", "#eff6ff"),
    _PENDING: ("schedule", "#9ca3af", "#f9fafb"),
}


# ── Public API ──


def render_array_task_tracker(job_model, ui_mgr) -> None:
    """Render the full task tracker widget inside the current NiceGUI context."""
    job_dir = _resolve_job_dir(job_model)
    if job_dir is None:
        _render_placeholder("Job directory not yet assigned.")
        return

    manifest = _read_manifest(job_dir)
    if manifest is None:
        _render_placeholder("Waiting for supervisor to start...")
        return

    items = manifest.get("items", [])
    item_label = manifest.get("item_label", "Item")
    if not items:
        _render_placeholder("No items in task manifest.")
        return

    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0;"):
        # ── Summary bar ──
        summary_container = ui.row().classes("w-full items-center px-4 py-2 bg-gray-50 border-b border-gray-100")
        summary_container.style("gap: 12px; flex-shrink: 0;")

        # ── Progress bar ──
        progress_bar = (
            ui.linear_progress(value=0, show_value=False)
            .classes("w-full")
            .style("flex-shrink: 0; height: 3px;")
            .props("color=positive")
        )

        # ── Task rows (built once, updated in-place) ──
        with ui.scroll_area().classes("w-full").style("flex: 1; min-height: 0;"):
            with ui.column().classes("w-full gap-0").style("padding: 0;"):
                row_widgets = _build_task_rows(items, job_dir)

    # Initial status update
    statuses = _scan_statuses(job_dir, items)
    _update_summary(summary_container, statuses, item_label)
    _update_progress(progress_bar, statuses)
    _apply_statuses_to_rows(row_widgets, statuses)

    # Poll timer — only updates summary/progress/row badges, never rebuilds rows
    def _poll():
        s = _scan_statuses(job_dir, items)
        _update_summary(summary_container, s, item_label)
        _update_progress(progress_bar, s)
        _apply_statuses_to_rows(row_widgets, s)

    ui.timer(5.0, _poll)


# ── Internals ──


def _resolve_job_dir(job_model) -> Optional[Path]:
    stored = (job_model.paths or {}).get("job_dir")
    if stored:
        p = Path(stored)
        if p.is_dir():
            return p
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn and hasattr(job_model, "_project_state") and job_model._project_state:
        p = job_model._project_state.project_path / rjn
        if p.is_dir():
            return p
    return None


def _read_manifest(job_dir: Path) -> Optional[dict]:
    manifest_path = job_dir / ".task_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text())
    except Exception:
        return None


def _scan_statuses(job_dir: Path, items: List[str]) -> Dict[str, str]:
    status_dir = job_dir / ".task_status"
    ok_set: set = set()
    fail_set: set = set()
    if status_dir.is_dir():
        for p in status_dir.iterdir():
            if p.suffix == ".ok":
                ok_set.add(p.stem)
            elif p.suffix == ".fail":
                fail_set.add(p.stem)

    statuses: Dict[str, str] = {}
    for name in items:
        if name in ok_set:
            statuses[name] = _OK
        elif name in fail_set:
            statuses[name] = _FAIL
        else:
            statuses[name] = _RUNNING if (ok_set or fail_set) else _PENDING
    return statuses


def _update_summary(container: ui.row, statuses: Dict[str, str], item_label: str) -> None:
    n_ok = sum(1 for s in statuses.values() if s == _OK)
    n_fail = sum(1 for s in statuses.values() if s == _FAIL)
    n_running = sum(1 for s in statuses.values() if s == _RUNNING)
    n_pending = sum(1 for s in statuses.values() if s == _PENDING)
    total = len(statuses)

    parts = []
    if n_ok:
        parts.append(f'<span style="color: #16a34a; font-weight: 600;">{n_ok} done</span>')
    if n_running:
        parts.append(f'<span style="color: #2563eb; font-weight: 600;">{n_running} running</span>')
    if n_fail:
        parts.append(f'<span style="color: #dc2626; font-weight: 600;">{n_fail} failed</span>')
    if n_pending:
        parts.append(f'<span style="color: #9ca3af;">{n_pending} pending</span>')

    html = f'<span style="{MONO} font-size: 11px;">{" · ".join(parts)} / {total} {item_label.lower()}</span>'
    container.clear()
    with container:
        ui.html(html, sanitize=False)


def _update_progress(bar: ui.linear_progress, statuses: Dict[str, str]) -> None:
    total = len(statuses)
    if total == 0:
        bar.set_value(0)
        return
    n_done = sum(1 for s in statuses.values() if s in (_OK, _FAIL))
    bar.set_value(n_done / total)
    bar.props(f"color={'negative' if any(s == _FAIL for s in statuses.values()) else 'positive'}")


# ── Row building (once) and updating (on poll) ──


def _shorten_ts_names(items: List[str]) -> Dict[str, str]:
    """Strip the common prefix from TS names for cleaner display.

    'agg5_20251113_412_Position_11'   -> 'Position_11'
    'agg5_20251113_412_Position_11_2' -> 'Position_11_2'

    If all names share a prefix ending with '_Position_', strip up to and
    including that prefix's project part. Falls back to the full name.
    """
    if not items:
        return {}

    # Find longest common prefix
    prefix = os.path.commonprefix(items)
    # Snap to the last underscore so we don't cut mid-word
    last_sep = prefix.rfind("_")
    if last_sep > 0:
        prefix = prefix[: last_sep + 1]
    else:
        prefix = ""

    # Only strip if it actually shortens things meaningfully
    if len(prefix) < 4:
        return {name: name for name in items}

    return {name: name[len(prefix) :] or name for name in items}


def _build_task_rows(items: List[str], job_dir: Path) -> Dict[str, dict]:
    """Build all expansion rows ONCE. Returns {name: {icon, label, expansion, bg_style}} refs."""
    display_names = _shorten_ts_names(items)
    row_widgets: Dict[str, dict] = {}
    for idx, name in enumerate(items):
        icon_name, icon_color, bg_color = _CHIP[_PENDING]
        short_name = display_names.get(name, name)

        with (
            ui.expansion()
            .classes("w-full")
            .style(f"border-bottom: 1px solid #f1f5f9; background: {bg_color}; min-height: 0;")
            .props("dense") as exp
        ):
            exp.props('header-class="p-0"')

            with exp.add_slot("header"):
                with ui.row().classes("w-full items-center").style("gap: 8px; padding: 4px 12px;"):
                    icon_el = ui.icon(icon_name, size="16px").style(f"color: {icon_color};")
                    ui.label(short_name).style(f"{MONO} font-size: 11px; color: #334155; font-weight: 500;")
                    ui.space()
                    status_label = ui.label(_PENDING).style(
                        f"{MONO} font-size: 9px; color: {icon_color}; text-transform: uppercase; font-weight: 600;"
                    )

            _render_inline_log(job_dir, idx)

        row_widgets[name] = {"icon": icon_el, "label": status_label, "expansion": exp}

    return row_widgets


def _apply_statuses_to_rows(row_widgets: Dict[str, dict], statuses: Dict[str, str]) -> None:
    """Update icon, label text, and background color on existing rows without rebuilding."""
    for name, widgets in row_widgets.items():
        status = statuses.get(name, _PENDING)
        icon_name, icon_color, bg_color = _CHIP.get(status, _CHIP[_PENDING])

        widgets["icon"]._props["name"] = icon_name
        widgets["icon"].style(f"color: {icon_color};")
        widgets["icon"].update()

        widgets["label"].set_text(status)
        widgets["label"].style(
            f"{MONO} font-size: 9px; color: {icon_color}; text-transform: uppercase; font-weight: 600;"
        )

        widgets["expansion"].style(f"border-bottom: 1px solid #f1f5f9; background: {bg_color}; min-height: 0;")


# ── Inline log viewer ──


def _render_inline_log(job_dir: Path, task_idx: int) -> None:
    stdout_path = job_dir / f"task_{task_idx}.out"
    stderr_path = job_dir / f"task_{task_idx}.err"

    stdout_text = _read_tail(stdout_path, max_lines=200)
    stderr_text = _read_tail(stderr_path, max_lines=100)

    with ui.column().classes("w-full").style("padding: 4px 12px 8px; gap: 6px;"):
        if not stdout_text and not stderr_text:
            ui.label("No log output yet.").style(f"{MONO} font-size: 10px; color: #94a3b8;")
            return

        if stdout_text:
            ui.label("stdout").style(f"{MONO} font-size: 9px; color: #64748b; font-weight: 600; margin: 0;")
            ui.html(
                f'<pre style="{MONO} font-size: 10px; line-height: 1.4; color: #334155; '
                f"white-space: pre-wrap; word-break: break-all; margin: 0; "
                f"max-height: 300px; overflow-y: auto; background: #f8fafc; "
                f'padding: 6px 8px; border-radius: 4px; border: 1px solid #e2e8f0;">'
                f"{_escape_html(stdout_text)}</pre>",
                sanitize=False,
            )

        if stderr_text:
            ui.label("stderr").style(f"{MONO} font-size: 9px; color: #dc2626; font-weight: 600; margin: 0;")
            ui.html(
                f'<pre style="{MONO} font-size: 10px; line-height: 1.4; color: #b91c1c; '
                f"white-space: pre-wrap; word-break: break-all; margin: 0; "
                f"max-height: 200px; overflow-y: auto; background: #fef2f2; "
                f'padding: 6px 8px; border-radius: 4px; border: 1px solid #fecaca;">'
                f"{_escape_html(stderr_text)}</pre>",
                sanitize=False,
            )


def _read_tail(path: Path, max_lines: int = 200) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(errors="replace")
        lines = text.splitlines()
        if len(lines) > max_lines:
            return f"[... truncated {len(lines) - max_lines} lines ...]\n" + "\n".join(lines[-max_lines:])
        return text
    except Exception:
        return ""


def _escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _render_placeholder(message: str) -> None:
    with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
        ui.icon("hourglass_empty", size="48px")
        ui.label(message).style("font-size: 13px;")
