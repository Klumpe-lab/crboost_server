# ui/pipeline_builder/io_tab.py
from typing import Callable

from nicegui import ui

from services.project_state import JobType
from ui.ui_state import UIStateManager
from ui.utils import snake_to_title
from ui.pipeline_builder.io_config_component import render_io_config
from ui.job_plugins._field_styles import (
    field_grid,
    kv_row,
    SANS,
    CLR_SUBLABEL,
)


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
        ui.label("No paths resolved yet.").style(
            f"{SANS} font-size: 10px; color: {CLR_SUBLABEL}; font-style: italic;"
        )
        return

    with field_grid():
        for key, value in paths_data.items():
            kv_row(snake_to_title(key), str(value))
