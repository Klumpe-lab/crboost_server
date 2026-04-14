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
    def render(job_type, instance_id, job_model, backend, ui_mgr):
        render_array_task_tracker(instance_id, job_model, ui_mgr)
"""

from pathlib import Path
from typing import Dict, List

from nicegui import ui

from ui.components.task_utils import (
    shorten_ts_names,
    sort_ts_by_position,
    ts_anchor_id,
    read_tail as _read_tail,
    escape_html as _escape_html,
    read_manifest as _read_manifest,
    scan_statuses as _scan_statuses,
    resolve_job_dir,
)
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


def render_array_task_tracker(instance_id: str, job_model, ui_mgr) -> None:
    """Render the full task tracker widget inside the current NiceGUI context.

    `instance_id` is used to anchor per-TS DOM ids and to consume a one-shot
    `ui_mgr.focus_ts_by_instance[instance_id]` entry (set when the user clicks
    a TS row in the roster) so the matching row is auto-expanded + scrolled
    into view on arrival.
    """
    job_dir = resolve_job_dir(job_model)
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

    # One-shot deep-link target. Consumed on arrival so subsequent polls /
    # manual scrolls don't keep pulling the view back to this row.
    focus_target = None
    if ui_mgr is not None and hasattr(ui_mgr, "focus_ts_by_instance"):
        focus_target = ui_mgr.focus_ts_by_instance.pop(instance_id, None)

    # Display in (stage, beam) ascending order. The manifest is the source of
    # truth for array-index → ts_name mapping (used to read task_{idx}.out),
    # so we keep that mapping intact and only reorder for display.
    display_order = sort_ts_by_position(items)
    item_to_task_idx = {name: i for i, name in enumerate(items)}

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
                row_widgets = _build_task_rows(display_order, item_to_task_idx, job_dir, instance_id, focus_target)

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

    # Scroll the focused TS into view after the DOM settles.
    if focus_target is not None:
        anchor = ts_anchor_id(instance_id, focus_target)
        ui.run_javascript(
            "setTimeout(() => {"
            f"  const el = document.getElementById({anchor!r});"
            "  if (el) el.scrollIntoView({behavior: 'smooth', block: 'center'});"
            "}, 80);"
        )


# ── Internals ──


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


def _build_task_rows(
    display_order: List[str],
    item_to_task_idx: Dict[str, int],
    job_dir: Path,
    instance_id: str,
    focus_target: str | None,
) -> Dict[str, dict]:
    """Build all expansion rows ONCE. Returns {name: {icon, label, expansion, bg_style}} refs.

    Rows are rendered in `display_order`. The task-output file for each row
    uses the ORIGINAL manifest index via `item_to_task_idx`, since SLURM task
    ids are tied to the manifest order.
    """
    display_names = shorten_ts_names(display_order)
    row_widgets: Dict[str, dict] = {}
    for name in display_order:
        icon_name, icon_color, bg_color = _CHIP[_PENDING]
        short_name = display_names.get(name, name)
        task_idx = item_to_task_idx[name]
        anchor_id = ts_anchor_id(instance_id, name)
        is_focus = focus_target == name

        with (
            ui.expansion(value=is_focus)
            .classes("w-full")
            .style(f"border-bottom: 1px solid #f1f5f9; background: {bg_color}; min-height: 0;")
            .props(f'dense id="{anchor_id}"') as exp
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

            _render_inline_log(job_dir, task_idx)

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


def _render_placeholder(message: str) -> None:
    with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
        ui.icon("hourglass_empty", size="48px")
        ui.label(message).style("font-size: 13px;")
