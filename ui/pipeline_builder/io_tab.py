# ui/pipeline_builder/io_tab.py
from typing import Callable

from nicegui import ui

from services.project_state import JobType
from ui.ui_state import UIStateManager
from ui.utils import snake_to_title
from ui.pipeline_builder.io_config_component import render_io_config


def render_io_tab(
    job_type: JobType, instance_id: str, job_model, is_frozen: bool, ui_mgr: UIStateManager, save_handler: Callable
):
    with ui.column().classes("w-full p-4"):
        if is_frozen:
            ui.label("Job is running or completed. I/O configuration is locked.").classes(
                "text-[11px] text-gray-500 italic mb-3"
            )
            _render_readonly_paths(job_model)
        else:
            render_io_config(
                job_type, instance_id, on_change=save_handler, active_instance_ids=set(ui_mgr.selected_jobs)
            )


def _render_readonly_paths(job_model):
    paths_data = job_model.paths
    if not paths_data:
        ui.label("No paths resolved yet.").classes("text-xs text-gray-400 italic")
        return

    async def copy_path(p: str) -> None:
        try:
            ui.clipboard.write(p)
            ui.notify("Copied", type="positive", timeout=900)
        except Exception:
            safe = p.replace("`", "\\`")
            await ui.run_javascript(f"navigator.clipboard.writeText(`{safe}`)", respond=False)
            ui.notify("Copied", type="positive", timeout=900)

    with ui.column().classes("w-full border border-gray-200 rounded-md overflow-hidden"):
        for i, (key, value) in enumerate(paths_data.items()):
            bg_class = "bg-white" if i % 2 == 0 else "bg-gray-50"
            with ui.row().classes(
                f"w-full {bg_class} border-b border-gray-100 last:border-0 items-center gap-2 px-2 py-1"
            ):
                ui.label(snake_to_title(key)).classes("text-[10px] font-bold text-gray-500 uppercase w-32 shrink-0")
                with ui.row().classes("flex-1 min-w-0 items-center gap-2"):
                    v_str = str(value)
                    ui.label(v_str).classes("text-xs font-mono text-gray-700 truncate flex-1 min-w-0").tooltip(v_str)
                    ui.button(icon="content_copy", on_click=lambda v=v_str: copy_path(v)).props(
                        "flat dense round size=sm"
                    ).classes("text-gray-500 hover:text-gray-800").tooltip("Copy path")
