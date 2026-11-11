# ui/pipeline_builder/logs_tab.py
"""The Log section of a job page without array tasks: its run.out / run.err in the job page's log view.
An array job's own log is the supervisor's row of its Tasks panel (ui/components/array_task_tracker.py)."""

from nicegui import ui

from services.project_state import JobStatus, JobType, get_project_state_for
from ui.components.log_pane import LogViewer, job_log_placeholder
from ui.components.reactive import owned_timer
from ui.ui_state import UIStateManager


def render_logs_tab(job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager):
    ui_mgr.cleanup_job_logs_timer(instance_id)
    project_path = ui_mgr.project_path
    if not job_model.relion_job_name or project_path is None:
        job_log_placeholder(instance_id, ui_mgr)
        return

    job_name = job_model.relion_job_name
    job_dir = project_path / job_name.rstrip("/")
    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0;") as box:
        viewer = LogViewer(project_path, slurm_id=True)
    viewer.show(job_dir / "run.out", job_dir / "run.err")
    viewer.set_slurm_id(job_model.slurm_job_id)
    # Whether the last tick read: the log is read once more after the job ends, for its last lines.
    was_live = {"on": True}

    async def tick() -> None:
        job = get_project_state_for(project_path).jobs.get(instance_id)
        if job is None:
            return
        if job.relion_job_name != job_name:
            # Run again into another job directory: the page is rebuilt on it.
            ui_mgr.cleanup_job_logs_timer(instance_id)
            ui_mgr.request_rebuild()
            return
        viewer.set_slurm_id(job.slurm_job_id)
        live = job.execution_status.is_live
        if live or was_live["on"]:
            await viewer.refresh(live=job.execution_status == JobStatus.RUNNING)
        was_live["on"] = live

    # The polls run only while this job's page is the one shown (its view scope).
    owned_timer(0.01, tick, box, once=True)
    ui_mgr.get_job_widget_refs(instance_id).logs_timer = ui_mgr.job_scope(instance_id).timer(3.0, tick, box)
