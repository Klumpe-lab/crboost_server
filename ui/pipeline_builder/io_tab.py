# ui/pipeline_builder/io_tab.py
from typing import Callable

from services.project_state import JobType
from ui.ui_state import UIStateManager
from ui.pipeline_builder.io_config_component import render_io_config


def render_io_tab(
    job_type: JobType, instance_id: str, job_model, is_frozen: bool, ui_mgr: UIStateManager, save_handler: Callable
):
    # One sectioned Inputs/Outputs layout for every job state. A completed/frozen
    # job renders the SAME view, read-only (static source faces, no edit
    # affordances) — not a separate flat path dump — so the look never changes
    # out from under the user when a job finishes.
    render_io_config(
        job_type,
        instance_id,
        on_change=save_handler,
        active_instance_ids=set(ui_mgr.selected_jobs),
        read_only=is_frozen,
    )
