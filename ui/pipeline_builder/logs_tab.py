"""
Logs tab: stdout/stderr display with auto-polling for running jobs.
"""

import asyncio

from nicegui import ui

from services.project_state import JobStatus, JobType
from ui.ui_state import UIStateManager


def render_logs_tab(job_type: JobType, job_model, backend, ui_mgr: UIStateManager):
    """Render the logs tab."""
    widget_refs = ui_mgr.get_job_widget_refs(job_type)

    # Cleanup existing timer
    ui_mgr.cleanup_job_logs_timer(job_type)

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job scheduled. Logs will appear here once running.")

        # Even if the job hasn't started yet, if the pipeline is active
        # we should poll -- the job will get a relion_job_name once the
        # schemer reaches it, and we want logs to appear automatically.
        if ui_mgr.is_running:
            widget_refs.logs_timer = ui.timer(
                3.0, lambda: asyncio.create_task(
                    _refresh_job_logs_with_placeholder_swap(job_type, backend, ui_mgr)
                )
            )
        return

    with ui.grid(columns=2).classes("w-full h-full gap-4"):
        # Stdout
        with ui.column().classes("h-full overflow-hidden flex flex-col border border-gray-200 rounded-lg"):
            ui.label("Standard Output").classes(
                "text-xs font-bold text-gray-500 uppercase px-3 py-2 bg-gray-50 border-b border-gray-200 w-full"
            )
            stdout_log = (
                ui.log(max_lines=1000)
                .classes("w-full p-3 font-mono text-xs bg-white")
                .style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
            )

        # Stderr
        with ui.column().classes("h-full overflow-hidden flex flex-col border border-red-100 rounded-lg"):
            ui.label("Standard Error").classes(
                "text-xs font-bold text-red-500 uppercase px-3 py-2 bg-red-50 border-b border-red-100 w-full"
            )
            stderr_log = (
                ui.log(max_lines=1000)
                .classes("w-full p-3 font-mono text-xs bg-white text-red-700")
                .style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
            )

    widget_refs.monitor_logs = {"stdout": stdout_log, "stderr": stderr_log}

    # Initial fetch
    asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))

    # Poll while this job is running OR the pipeline is active (job might
    # transition to running while we're watching)
    if job_model.execution_status == JobStatus.RUNNING or ui_mgr.is_running:
        widget_refs.logs_timer = ui.timer(
            3.0, lambda: asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))
        )


async def _refresh_job_logs_with_placeholder_swap(
    job_type: JobType, backend, ui_mgr: UIStateManager
):
    """Poll variant for the 'job scheduled' placeholder state.

    Once the job gets a relion_job_name (meaning the schemer has reached it),
    trigger a re-render of the logs tab to swap the placeholder for real log
    widgets.  Until then, do nothing.
    """
    from services.project_state import get_project_state_for

    project_path = ui_mgr.project_path
    if not project_path:
        return

    state = get_project_state_for(project_path)
    job_model = state.jobs.get(job_type)

    if not job_model or not job_model.relion_job_name:
        # Job hasn't started yet -- keep waiting
        if not ui_mgr.is_running:
            # Pipeline finished without this job ever running -- stop polling
            ui_mgr.cleanup_job_logs_timer(job_type)
        return

    # Job now has a relion_job_name -- re-render the logs tab to get real log widgets
    ui_mgr.cleanup_job_logs_timer(job_type)
    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    content_container = widget_refs.content_container
    if content_container:
        content_container.clear()
        with content_container:
            render_logs_tab(job_type, job_model, backend, ui_mgr)


async def _refresh_job_logs(job_type: JobType, backend, ui_mgr: UIStateManager):
    """Refresh the logs display for a job."""
    from services.project_state import get_project_state_for

    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    monitor = widget_refs.monitor_logs

    if not monitor or "stdout" not in monitor:
        return

    if monitor["stdout"].is_deleted:
        ui_mgr.cleanup_job_logs_timer(job_type)
        return

    # Use registry directly instead of tab-context get_project_state().
    # Timer callbacks usually have tab context, but being explicit is safer.
    project_path = ui_mgr.project_path
    if not project_path:
        return

    state = get_project_state_for(project_path)
    job_model = state.jobs.get(job_type)

    if not job_model or not job_model.relion_job_name:
        # Job hasn't been assigned a relion path yet. If pipeline is still
        # running, keep the timer alive -- it'll get a name eventually.
        if not ui_mgr.is_running:
            ui_mgr.cleanup_job_logs_timer(job_type)
        return

    logs = await backend.get_job_logs(str(project_path), job_model.relion_job_name)

    MAX_LOG_LINES = 500

    stdout = logs.get("stdout", "No output") or "No output yet"
    stderr = logs.get("stderr", "No errors") or "No errors yet"

    stdout_lines = stdout.split("\n")
    stderr_lines = stderr.split("\n")

    if len(stdout_lines) > MAX_LOG_LINES:
        stdout = f"[... truncated {len(stdout_lines) - MAX_LOG_LINES} lines ...]\n" + "\n".join(
            stdout_lines[-MAX_LOG_LINES:]
        )
    if len(stderr_lines) > MAX_LOG_LINES:
        stderr = f"[... truncated {len(stderr_lines) - MAX_LOG_LINES} lines ...]\n" + "\n".join(
            stderr_lines[-MAX_LOG_LINES:]
        )

    monitor["stdout"].clear()
    monitor["stdout"].push(stdout)
    monitor["stderr"].clear()
    monitor["stderr"].push(stderr)

    # Stop polling if job finished and pipeline is done
    if job_model.execution_status in (JobStatus.SUCCEEDED, JobStatus.FAILED) and not ui_mgr.is_running:
        ui_mgr.cleanup_job_logs_timer(job_type)