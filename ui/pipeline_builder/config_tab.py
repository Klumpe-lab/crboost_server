# ui/pipeline_builder/config_tab.py
from collections.abc import Callable

from services.job_params import is_frozen
from services.project_state import JobType
from ui.current_project import current_project_state
from ui.job_plugins import get_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card
from ui.ui_state import UIStateManager


def is_job_frozen(instance_id: str) -> bool:
    """The setting doors' rule (`job_params.is_frozen`): only a Scheduled or Failed job takes edits."""
    job_model = current_project_state().jobs.get(instance_id)
    return job_model is not None and is_frozen(job_model)


def render_config_tab(
    job_type: JobType,
    job_model,
    is_frozen: bool,
    ui_mgr: UIStateManager,
    backend,
    save_handler: Callable,
    instance_id: str | None = None,
    callbacks: dict[str, Callable] | None = None,
    observe: Callable[[Callable[[], None]], None] | None = None,
) -> Callable[[], None] | None:
    """Returns the refresh a plugin renderer hands back, if any: the job tab reruns it after an edit in
    any of its sections. `observe` takes a renderer's polls of what it draws unbound (choice rows), which
    the page runs every 3 s."""
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
            observe=observe,
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
        observe=observe,
    )
