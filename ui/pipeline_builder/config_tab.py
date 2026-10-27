# ui/pipeline_builder/config_tab.py
from collections.abc import Callable

from services.path_resolution_service import jobs_in_use
from services.project_state import JobStatus, JobType
from ui.current_project import current_project_state
from ui.job_plugins import get_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card
from ui.ui_state import UIStateManager, get_instance_display_name


def is_job_frozen(instance_id: str) -> bool:
    state = current_project_state()
    job_model = state.jobs.get(instance_id)
    if not job_model:
        return False
    # Queued counts: its inputs were fixed at submit, and the model refuses parameter edits on any
    # job that is neither Scheduled nor Failed.
    return job_model.execution_status in (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED)


def in_use_notice(state, instance_id: str) -> str | None:
    """Why `instance_id` cannot be removed or deleted now, or None when it can: it is queued or
    running, or a queued or running job reads it, directly or further down (`jobs_in_use`)."""
    holder = jobs_in_use(state).get(instance_id)
    if holder is None:
        return None
    name = get_instance_display_name(instance_id, state.jobs.get(instance_id))
    holder_model = state.jobs[holder]
    status = holder_model.execution_status.value.lower()
    if holder == instance_id:
        return f"{name} is {status}; cancel it first."
    return (
        f"{name} feeds {get_instance_display_name(holder, holder_model)}, which is {status}; "
        "it can go once that has finished or been cancelled."
    )


def render_config_tab(
    job_type: JobType,
    job_model,
    is_frozen: bool,
    ui_mgr: UIStateManager,
    backend,
    save_handler: Callable,
    instance_id: str | None = None,
    callbacks: dict[str, Callable] | None = None,
) -> Callable[[], None] | None:
    """Returns the refresh a plugin renderer hands back, if any: the job tab reruns it after an edit in
    any of its sections."""
    # array_throttle is rendered in the SLURM section, not here
    exclude = {"array_throttle"} if "array_throttle" in getattr(job_model, "USER_PARAMS", set()) else None
    renderer = get_params_renderer(job_type)
    if renderer:
        return renderer(
            job_type,
            job_model,
            is_frozen,
            save_handler,
            ui_mgr=ui_mgr,
            backend=backend,
            exclude=exclude,
            instance_id=instance_id,
            callbacks=callbacks,
        )
    return render_default_params_card(
        job_type,
        job_model,
        is_frozen,
        save_handler,
        exclude=exclude,
        ui_mgr=ui_mgr,
        instance_id=instance_id,
        callbacks=callbacks,
    )
