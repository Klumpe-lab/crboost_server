# ui/pipeline_builder/logs_tab.py
import asyncio

from nicegui import ui

from services.project_state import JobStatus, JobType
from ui.ui_state import UIStateManager


def render_logs_tab(job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager):
    widget_refs = ui_mgr.get_job_widget_refs(instance_id)
    ui_mgr.cleanup_job_logs_timer(instance_id)

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job scheduled. Logs will appear here once running.")

        if ui_mgr.is_running:
            widget_refs.logs_timer = ui.timer(
                3.0, lambda: asyncio.create_task(_refresh_job_logs_with_placeholder_swap(instance_id, backend, ui_mgr))
            )
        return

    is_running = job_model.execution_status == JobStatus.RUNNING

    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0;"):
        with ui.grid(columns=2).classes("w-full gap-4 p-4").style("flex: 1 1 0%; min-height: 0; overflow: hidden;"):
            with ui.column().classes("h-full overflow-hidden flex flex-col border border-gray-200 rounded-lg"):
                ui.label("Standard Output").classes(
                    "text-xs font-bold text-gray-500 uppercase px-3 py-2 bg-gray-50 border-b border-gray-200 w-full"
                )
                stdout_log = (
                    ui.log(max_lines=1000)
                    .classes("w-full p-3 font-mono text-xs bg-white")
                    .style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
                )

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

    asyncio.create_task(_refresh_job_logs(instance_id, backend, ui_mgr))

    if is_running or ui_mgr.is_running:
        widget_refs.logs_timer = ui.timer(
            3.0, lambda: asyncio.create_task(_refresh_job_logs(instance_id, backend, ui_mgr))
        )


async def _refresh_job_logs_with_placeholder_swap(instance_id: str, backend, ui_mgr: UIStateManager):
    from services.project_state import get_project_state_for

    project_path = ui_mgr.project_path
    if not project_path:
        return

    state = get_project_state_for(project_path)
    job_model = state.jobs.get(instance_id)

    if not job_model or not job_model.relion_job_name:
        if not ui_mgr.is_running:
            ui_mgr.cleanup_job_logs_timer(instance_id)
        return

    ui_mgr.cleanup_job_logs_timer(instance_id)
    # Trigger a full rebuild so the header (Cancel button, job path) reflects
    # the new Running state. Guard against the background-task slot error --
    # if we're in a timer callback without a live client, request_rebuild
    # will call rebuild_pipeline_ui which itself guards the ui.timer creation.
    ui_mgr.request_rebuild()


async def _refresh_job_logs(instance_id: str, backend, ui_mgr: UIStateManager):
    from services.project_state import get_project_state_for

    widget_refs = ui_mgr.get_job_widget_refs(instance_id)
    monitor = widget_refs.monitor_logs

    if not monitor or "stdout" not in monitor:
        return

    if monitor["stdout"].is_deleted:
        ui_mgr.cleanup_job_logs_timer(instance_id)
        return

    project_path = ui_mgr.project_path
    if not project_path:
        return

    state = get_project_state_for(project_path)
    job_model = state.jobs.get(instance_id)

    if not job_model or not job_model.relion_job_name:
        if not ui_mgr.is_running:
            ui_mgr.cleanup_job_logs_timer(instance_id)
        return

    current_polling_path = monitor.get("_job_path")
    if current_polling_path and current_polling_path != job_model.relion_job_name:
        ui_mgr.cleanup_job_logs_timer(instance_id)
        ui_mgr.request_rebuild()
        return
    monitor["_job_path"] = job_model.relion_job_name

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

    if job_model.execution_status in (JobStatus.SUCCEEDED, JobStatus.FAILED) and not ui_mgr.is_running:
        ui_mgr.cleanup_job_logs_timer(instance_id)
