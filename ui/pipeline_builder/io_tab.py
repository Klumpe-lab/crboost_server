# ui/pipeline_builder/io_tab.py
from typing import Callable

from nicegui import ui

from services.project_state import JobType
from ui.ui_state import UIStateManager
from ui.utils import snake_to_title
from ui.pipeline_builder.io_config_component import render_io_config

MONO = "font-family: 'IBM Plex Mono', monospace;"
FONT = "font-family: 'IBM Plex Sans', sans-serif;"
_CLR_LABEL = "#475569"
_CLR_SUBLABEL = "#94a3b8"


def render_io_tab(
    job_type: JobType, instance_id: str, job_model, is_frozen: bool, ui_mgr: UIStateManager, save_handler: Callable
):
    if is_frozen:
        _render_readonly_paths(job_model)
    else:
        render_io_config(job_type, instance_id, on_change=save_handler, active_instance_ids=set(ui_mgr.selected_jobs))


def _render_readonly_paths(job_model):
    paths_data = job_model.paths
    if not paths_data:
        ui.label("No paths resolved yet.").style(f"{FONT} font-size: 10px; color: {_CLR_SUBLABEL}; font-style: italic;")
        return

    async def copy_path(p: str) -> None:
        try:
            ui.clipboard.write(p)
            ui.notify("Copied", type="positive", timeout=900)
        except Exception:
            safe = p.replace("`", "\\`")
            await ui.run_javascript(f"navigator.clipboard.writeText(`{safe}`)", respond=False)
            ui.notify("Copied", type="positive", timeout=900)

    for key, value in paths_data.items():
        v_str = str(value)
        with ui.row().classes("w-full items-baseline gap-2").style("min-height: 20px;"):
            ui.label(snake_to_title(key)).style(
                f"{FONT} font-size: 9px; color: {_CLR_LABEL}; flex-shrink: 0; width: 72px; white-space: nowrap;"
            )
            ui.label(v_str).style(
                f"{MONO} font-size: 10px; color: #64748b; flex: 1; min-width: 0; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
            ).tooltip(v_str)
            (
                ui.button(icon="content_copy", on_click=lambda v=v_str: copy_path(v))
                .props("flat dense round size=xs")
                .style("color: #94a3b8; flex-shrink: 0;")
                .tooltip("Copy")
            )
