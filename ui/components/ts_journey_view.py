# ui/components/ts_journey_view.py
"""
Full-page tilt-series journey view.

Shows a matrix of all tilt series (rows) x pipeline stages (columns)
with per-cell status indicators. Clicking a cell expands inline logs
for that TS at that stage.

Data is read from the file-based contract written by array job supervisors:
    {job_dir}/.task_manifest.json
    {job_dir}/.task_status/{ts_name}.ok|.fail
    {job_dir}/task_{idx}.out / task_{idx}.err
"""

import logging
from pathlib import Path
from typing import Dict, List, Tuple

from nicegui import ui

from services.models_base import JobType
from ui.components.task_utils import (
    shorten_ts_names,
    read_tail,
    escape_html,
    read_manifest,
    scan_statuses,
    resolve_job_dir,
)
from ui.styles import MONO

logger = logging.getLogger(__name__)

FONT = "font-family: 'IBM Plex Sans', sans-serif;"

# Pipeline stages that use the array pattern, in processing order
_ARRAY_STAGES: List[Tuple[JobType, str]] = [
    (JobType.FS_MOTION_CTF, "FS/CTF"),
    (JobType.TS_ALIGNMENT, "Align"),
    (JobType.TS_CTF, "CTF"),
    (JobType.TS_RECONSTRUCT, "Recon"),
]

_STATUS_COLORS = {
    "ok": ("#16a34a", "#f0fdf4"),
    "fail": ("#dc2626", "#fef2f2"),
    "running": ("#2563eb", "#eff6ff"),
    "pending": ("#d1d5db", "#f9fafb"),
}

_STATUS_ICONS = {"ok": "check_circle", "fail": "error", "running": "sync", "pending": "radio_button_unchecked"}


def render_ts_journey_view(backend, ui_mgr) -> None:
    """Top-level journey view renderer."""
    from services.project_state import get_project_state

    state = get_project_state()
    project_path = ui_mgr.project_path

    if not project_path:
        _render_empty("No project loaded.")
        return

    # Determine which stages are in the pipeline
    active_stages = []
    stage_jobs = {}
    for job_type, label in _ARRAY_STAGES:
        for iid, jm in state.jobs.items():
            if getattr(jm, "job_type", None) == job_type or iid.split("__")[0] == job_type.value:
                active_stages.append((job_type, label))
                stage_jobs[job_type] = jm
                break

    if not active_stages:
        _render_empty("No array jobs in the pipeline yet. Add FS Motion/CTF, Alignment, CTF, or Reconstruct.")
        return

    # Collect journey data
    journey, ts_names = _collect_journey_data(project_path, stage_jobs)

    if not ts_names:
        _render_empty("No tilt series data found. Run at least one array job first.")
        return

    display_names = shorten_ts_names(ts_names)

    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0;"):
        # Title bar
        with (
            ui.row()
            .classes("w-full items-center px-4 border-b border-gray-200 bg-white gap-3")
            .style("flex-shrink: 0; min-height: 36px; padding-top: 6px; padding-bottom: 6px;")
        ):
            ui.icon("view_timeline", size="20px").style("color: #475569;")
            ui.label("Tilt Series Journey").style(f"{FONT} font-size: 14px; font-weight: 600; color: #1e293b;")
            ui.space()
            ui.label(f"{len(ts_names)} tilt series").style(f"{MONO} font-size: 11px; color: #64748b;")

        # Column summary
        summary_container = ui.element("div")
        summary_container.style(
            "display: grid; flex-shrink: 0; padding: 6px 16px; "
            "background: #f8fafc; border-bottom: 1px solid #e2e8f0; "
            "align-items: center; gap: 0;"
        )

        # Matrix
        with ui.scroll_area().classes("w-full").style("flex: 1; min-height: 0;"):
            matrix_container = ui.element("div").style("padding: 0;")

        # Build the grid
        n_cols = len(active_stages)
        col_template = f"140px repeat({n_cols}, 1fr)"

        # Render summary header
        with summary_container:
            summary_container.style(
                f"grid-template-columns: {col_template}; "
                "display: grid; flex-shrink: 0; padding: 6px 16px; "
                "background: #f8fafc; border-bottom: 1px solid #e2e8f0; "
                "align-items: center; gap: 0;"
            )
            # Empty cell for TS name column
            ui.label("").style("grid-column: 1;")
            for i, (jt, label) in enumerate(active_stages):
                col_statuses = [journey.get(ts, {}).get(jt, "pending") for ts in ts_names]
                n_ok = sum(1 for s in col_statuses if s == "ok")
                n_fail = sum(1 for s in col_statuses if s == "fail")
                total = len(col_statuses)

                if n_fail > 0:
                    summary_color = "#dc2626"
                elif n_ok == total:
                    summary_color = "#16a34a"
                elif n_ok > 0:
                    summary_color = "#2563eb"
                else:
                    summary_color = "#9ca3af"

                with ui.column().classes("items-center gap-0"):
                    ui.label(label).style(f"{FONT} font-size: 10px; font-weight: 600; color: #475569;")
                    ui.label(f"{n_ok}/{total}").style(
                        f"{MONO} font-size: 10px; font-weight: 600; color: {summary_color};"
                    )

        # Render matrix rows
        # Store widget refs for polling updates
        cell_widgets: Dict[str, Dict[JobType, dict]] = {}

        with matrix_container:
            matrix_container.style(f"display: grid; grid-template-columns: {col_template}; gap: 0;")

            for ts_name in ts_names:
                short_name = display_names.get(ts_name, ts_name)
                ts_statuses = journey.get(ts_name, {})

                # TS name cell
                ui.label(short_name).style(
                    f"{MONO} font-size: 11px; color: #334155; font-weight: 500; "
                    f"padding: 5px 8px 5px 16px; border-bottom: 1px solid #f1f5f9; "
                    f"white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
                )

                cell_widgets[ts_name] = {}
                for jt, _label in active_stages:
                    status = ts_statuses.get(jt, "pending")
                    color, bg = _STATUS_COLORS.get(status, _STATUS_COLORS["pending"])
                    icon_name = _STATUS_ICONS.get(status, "radio_button_unchecked")

                    cell = ui.element("div").style(
                        f"display: flex; align-items: center; justify-content: center; "
                        f"padding: 4px; border-bottom: 1px solid #f1f5f9; "
                        f"background: {bg}; cursor: pointer;"
                    )
                    with cell:
                        icon_el = ui.icon(icon_name, size="16px").style(f"color: {color};")

                    cell_widgets[ts_name][jt] = {"cell": cell, "icon": icon_el}

                    # Click handler: show inline logs
                    jm = stage_jobs.get(jt)
                    if jm:
                        job_dir = resolve_job_dir(jm, project_path)
                        if job_dir:
                            manifest = read_manifest(job_dir)
                            if manifest:
                                items = manifest.get("items", [])
                                try:
                                    task_idx = items.index(ts_name)
                                except ValueError:
                                    task_idx = None
                                if task_idx is not None:
                                    cell.on(
                                        "click",
                                        lambda _e, jd=job_dir, ti=task_idx, tn=short_name, sl=_label: _show_log_dialog(
                                            jd, ti, tn, sl
                                        ),
                                    )

    # Poll timer
    def _poll():
        new_journey, _ = _collect_journey_data(project_path, stage_jobs)
        _update_cells(cell_widgets, new_journey, active_stages)
        _update_summary(summary_container, new_journey, ts_names, active_stages, col_template)

    ui.timer(5.0, _poll)


def _collect_journey_data(
    project_path: Path, stage_jobs: Dict[JobType, object]
) -> Tuple[Dict[str, Dict[JobType, str]], List[str]]:
    """Collect per-TS status across all stages.

    Returns:
        journey: {ts_name: {JobType: status_string}}
        ts_names: ordered list of all tilt series names (union across stages)
    """
    journey: Dict[str, Dict[JobType, str]] = {}
    all_ts: List[str] = []
    seen_ts: set = set()

    for jt, jm in stage_jobs.items():
        job_dir = resolve_job_dir(jm, project_path)
        if job_dir is None:
            continue
        manifest = read_manifest(job_dir)
        if manifest is None:
            continue
        items = manifest.get("items", [])
        if not items:
            continue

        statuses = scan_statuses(job_dir, items)

        for ts_name in items:
            if ts_name not in seen_ts:
                all_ts.append(ts_name)
                seen_ts.add(ts_name)
            if ts_name not in journey:
                journey[ts_name] = {}
            journey[ts_name][jt] = statuses.get(ts_name, "pending")

    return journey, all_ts


def _update_cells(
    cell_widgets: Dict[str, Dict[JobType, dict]],
    journey: Dict[str, Dict[JobType, str]],
    active_stages: List[Tuple[JobType, str]],
) -> None:
    """Update cell icons and colors in-place without DOM rebuild."""
    for ts_name, stage_cells in cell_widgets.items():
        ts_statuses = journey.get(ts_name, {})
        for jt, _label in active_stages:
            widgets = stage_cells.get(jt)
            if not widgets:
                continue
            status = ts_statuses.get(jt, "pending")
            color, bg = _STATUS_COLORS.get(status, _STATUS_COLORS["pending"])
            icon_name = _STATUS_ICONS.get(status, "radio_button_unchecked")

            widgets["icon"]._props["name"] = icon_name
            widgets["icon"].style(f"color: {color};")
            widgets["icon"].update()
            widgets["cell"].style(
                f"display: flex; align-items: center; justify-content: center; "
                f"padding: 4px; border-bottom: 1px solid #f1f5f9; "
                f"background: {bg}; cursor: pointer;"
            )


def _update_summary(container, journey, ts_names, active_stages, col_template):
    """Update the summary header row in-place."""
    container.clear()
    with container:
        container.style(
            f"grid-template-columns: {col_template}; "
            "display: grid; flex-shrink: 0; padding: 6px 16px; "
            "background: #f8fafc; border-bottom: 1px solid #e2e8f0; "
            "align-items: center; gap: 0;"
        )
        ui.label("").style("grid-column: 1;")
        for jt, label in active_stages:
            col_statuses = [journey.get(ts, {}).get(jt, "pending") for ts in ts_names]
            n_ok = sum(1 for s in col_statuses if s == "ok")
            n_fail = sum(1 for s in col_statuses if s == "fail")
            total = len(col_statuses)

            if n_fail > 0:
                summary_color = "#dc2626"
            elif n_ok == total:
                summary_color = "#16a34a"
            elif n_ok > 0:
                summary_color = "#2563eb"
            else:
                summary_color = "#9ca3af"

            with ui.column().classes("items-center gap-0"):
                ui.label(label).style(f"{FONT} font-size: 10px; font-weight: 600; color: #475569;")
                ui.label(f"{n_ok}/{total}").style(f"{MONO} font-size: 10px; font-weight: 600; color: {summary_color};")


def _show_log_dialog(job_dir: Path, task_idx: int, ts_name: str, stage_label: str) -> None:
    """Show a dialog with stdout/stderr for a specific TS at a specific stage."""
    stdout_path = job_dir / f"task_{task_idx}.out"
    stderr_path = job_dir / f"task_{task_idx}.err"

    stdout_text = read_tail(stdout_path, max_lines=300)
    stderr_text = read_tail(stderr_path, max_lines=150)

    with ui.dialog() as dialog, ui.card().classes("w-[48rem]").style("max-height: 80vh; padding: 12px;"):
        with ui.row().classes("w-full items-center gap-2 mb-2"):
            ui.label(f"{ts_name}").style(f"{MONO} font-size: 13px; font-weight: 600; color: #1e293b;")
            if stage_label:
                ui.label(f"@ {stage_label}").style(f"{FONT} font-size: 12px; color: #64748b;")
            ui.space()
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm")

        if not stdout_text and not stderr_text:
            ui.label("No log output yet.").style(f"{MONO} font-size: 11px; color: #94a3b8;")
        else:
            with ui.scroll_area().style("max-height: 60vh;"):
                if stdout_text:
                    ui.label("stdout").style(f"{MONO} font-size: 9px; color: #64748b; font-weight: 600;")
                    ui.html(
                        f'<pre style="{MONO} font-size: 10px; line-height: 1.4; color: #334155; '
                        f"white-space: pre-wrap; word-break: break-all; margin: 0; "
                        f"max-height: 400px; overflow-y: auto; background: #f8fafc; "
                        f'padding: 6px 8px; border-radius: 4px; border: 1px solid #e2e8f0;">'
                        f"{escape_html(stdout_text)}</pre>",
                        sanitize=False,
                    )
                if stderr_text:
                    ui.label("stderr").style(
                        f"{MONO} font-size: 9px; color: #dc2626; font-weight: 600; margin-top: 8px;"
                    )
                    ui.html(
                        f'<pre style="{MONO} font-size: 10px; line-height: 1.4; color: #b91c1c; '
                        f"white-space: pre-wrap; word-break: break-all; margin: 0; "
                        f"max-height: 200px; overflow-y: auto; background: #fef2f2; "
                        f'padding: 6px 8px; border-radius: 4px; border: 1px solid #fecaca;">'
                        f"{escape_html(stderr_text)}</pre>",
                        sanitize=False,
                    )

    dialog.open()


def _render_empty(message: str) -> None:
    with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-3"):
        ui.icon("view_timeline", size="56px")
        ui.label(message).style(f"{FONT} font-size: 13px; color: #94a3b8; text-align: center; max-width: 400px;")
