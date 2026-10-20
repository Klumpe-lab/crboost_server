# ui/pipeline_builder/log_progress_chip.py
"""A running job's progress as its tool reports it (services/log_progress.py), in the job page header.

Reads run.out every 15 s while the job's page is shown and the job runs; hidden otherwise. Array
jobs show their per-tilt-series counts in the roster and get nothing here."""

import asyncio

from nicegui import ui

from services.array_status_cache import is_array_job
from services.log_progress import read as read_log_progress
from services.project_state import JobStatus, JobType
from ui.current_project import current_project_state
from ui.styles import MONO
from ui.ui_state import UIStateManager

# run.out is on the shared filesystem: the disk cadence.
_POLL_S = 15.0
_CLR = "#3b82f6"
_CLR_QUIET = "#d97706"


def render_log_progress_chip(job_type: JobType, instance_id: str, ui_mgr: UIStateManager) -> None:
    state = current_project_state()
    project_path = ui_mgr.project_path
    label = ui.label("").style(
        f"{MONO} font-size: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; "
        "min-width: 0; flex-shrink: 0; max-width: 45%;"
    )
    label.set_visibility(False)
    shown: list = [None]

    async def tick() -> None:
        job = state.jobs.get(instance_id)
        progress = None
        if (
            project_path is not None
            and job is not None
            and job.execution_status == JobStatus.RUNNING
            and job.relion_job_name
            and not is_array_job(job)
        ):
            job_dir = project_path / job.relion_job_name.rstrip("/")
            progress = await asyncio.to_thread(read_log_progress, job_type, job_dir)
        view = None
        if progress is not None:
            parts = [progress.stage]
            if progress.fraction is not None:
                parts.append(progress.short)
            if progress.eta:
                parts.append(f"≈ {progress.eta} left")
            if progress.quiet_min is not None:
                parts.append(f"no output for {progress.quiet_min} min")
            text = " · ".join(parts)
            title = f"{progress.last_line}\n(from run.out; the time left assumes the current pace)"
            view = (text, title, progress.quiet_min is not None)
        if view == shown[0] or label.is_deleted:
            return
        shown[0] = view
        if view is not None:
            label.props["title"] = view[1]
            label.set_text(view[0])
            label.style(f"color: {_CLR_QUIET if view[2] else _CLR};")
        label.set_visibility(view is not None)

    scope = ui_mgr.job_scope(instance_id)
    scope.timer(0.1, tick, label, once=True)
    scope.timer(_POLL_S, tick, label)
