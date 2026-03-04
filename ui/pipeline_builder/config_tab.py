# ui/pipeline_builder/config_tab.py
"""
Config/parameters tab: job-specific params only (via plugin system or default).
I/O and SLURM live in their own tabs now.
"""

from typing import Callable

from nicegui import ui

from services.project_state import JobStatus, JobType, get_project_state
from ui.job_plugins import get_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card
from ui.ui_state import UIStateManager


def is_job_frozen(job_type: JobType) -> bool:
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    if not job_model:
        return False
    return job_model.execution_status in [JobStatus.RUNNING, JobStatus.SUCCEEDED]


def render_config_tab(
    job_type: JobType, job_model, is_frozen: bool, ui_mgr: UIStateManager, backend, save_handler: Callable
):
    """Render job-specific parameters (plugin or generic fallback)."""
    renderer = get_params_renderer(job_type)
    if renderer:
        renderer(job_type, job_model, is_frozen, save_handler, ui_mgr=ui_mgr, backend=backend)
    else:
        render_default_params_card(job_type, job_model, is_frozen, save_handler)
