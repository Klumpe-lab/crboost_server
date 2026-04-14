# ui/pipeline_builder/config_tab.py
from typing import Callable

from services.project_state import JobStatus, JobType, get_project_state
from ui.job_plugins import get_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card
from ui.ui_state import UIStateManager


def is_job_frozen(instance_id: str) -> bool:
    state = get_project_state()
    job_model = state.jobs.get(instance_id)
    if not job_model:
        return False
    return job_model.execution_status in (JobStatus.RUNNING, JobStatus.SUCCEEDED)


def render_config_tab(
    job_type: JobType, job_model, is_frozen: bool, ui_mgr: UIStateManager, backend, save_handler: Callable
):
    # array_throttle is rendered in the SLURM section, not here
    exclude = {"array_throttle"} if "array_throttle" in getattr(job_model, "USER_PARAMS", set()) else None
    renderer = get_params_renderer(job_type)
    if renderer:
        renderer(job_type, job_model, is_frozen, save_handler, ui_mgr=ui_mgr, backend=backend, exclude=exclude)
    else:
        render_default_params_card(job_type, job_model, is_frozen, save_handler, exclude=exclude, ui_mgr=ui_mgr)
