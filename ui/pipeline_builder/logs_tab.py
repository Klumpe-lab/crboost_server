"""
Logs tab: stdout/stderr display with auto-polling for running jobs.
"""

import asyncio

from nicegui import ui

from services.project_state import JobStatus, JobType
from ui.ui_state import UIStateManager


def render_logs_tab(job_type: JobType, job_model, backend, ui_mgr: UIStateManager):
    widget_refs = ui_mgr.get_job_widget_refs(job_type)

    ui_mgr.cleanup_job_logs_timer(job_type)

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job scheduled. Logs will appear here once running.")

        if ui_mgr.is_running:
            widget_refs.logs_timer = ui.timer(
                3.0, lambda: asyncio.create_task(
                    _refresh_job_logs_with_placeholder_swap(job_type, backend, ui_mgr)
                )
            )
        return

    is_running = job_model.execution_status == JobStatus.RUNNING

    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0;"):

        if is_running:
            with ui.row().classes("w-full items-center justify-between px-4 py-2 bg-orange-50 border-b border-orange-200").style("flex-shrink: 0;"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("radio_button_checked", size="14px").classes("text-orange-500")
                    ui.label("Job is running").classes("text-xs font-semibold text-orange-700")
                    if job_model.relion_job_name:
                        ui.label(job_model.relion_job_name.rstrip("/")).classes("text-xs font-mono text-orange-500")

                ui.button(
                    "Cancel Job",
                    icon="stop_circle",
                    on_click=lambda: _handle_cancel_job(job_type, job_model, backend, ui_mgr),
                ).props("dense flat no-caps").style(
                    "color: #ea580c; border: 1px solid #fed7aa; border-radius: 3px; "
                    "padding: 2px 10px; font-size: 11px; font-weight: 500;"
                )

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

    asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))

    if is_running or ui_mgr.is_running:
        widget_refs.logs_timer = ui.timer(
            3.0, lambda: asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))
        )


async def _handle_cancel_job(job_type: JobType, job_model, backend, ui_mgr: UIStateManager):
    project_path = ui_mgr.project_path
    job_dir_str = (
        str(project_path / job_model.relion_job_name.rstrip("/"))
        if job_model.relion_job_name else "unknown"
    )

    with ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
        from ui.ui_state import get_job_display_name
        ui.label(f"Cancel {get_job_display_name(job_type)}?").classes("text-base font-bold text-gray-800")
        ui.label(job_dir_str).classes("text-xs font-mono text-gray-500 mt-1")
        ui.label(
            "The SLURM job will be cancelled, this job marked Failed, "
            "and the pipeline stopped so you can requeue."
        ).classes("text-sm text-gray-600 mt-2")
        with ui.row().classes("mt-4 gap-2 justify-end w-full"):
            ui.button("Dismiss", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
            ui.button(
                "Cancel Job", on_click=lambda: dialog.submit(True)
            ).props("dense no-caps").style(
                "background: #f97316; color: white; padding: 4px 16px; border-radius: 3px;"
            )

    confirmed = await dialog
    if not confirmed:
        return

    result = await backend.pipeline_runner.cancel_job(project_path, job_type)
    await backend.pipeline_runner.status_sync.sync_all_jobs(str(project_path))

    # Unlock the pipeline UI -- cancel_job stops the schemer and clears
    # pipeline_active, so the Run button should become available again.
    ui_mgr.set_pipeline_running(False)
    ui_mgr.request_rebuild()

    if result.get("success"):
        ui.notify(result.get("message", "Job cancelled."), type="warning", timeout=5000)
    else:
        ui.notify(f"Cancel failed: {result.get('error')}", type="negative", timeout=8000)


async def _refresh_job_logs_with_placeholder_swap(
    job_type: JobType, backend, ui_mgr: UIStateManager
):
    from services.project_state import get_project_state_for

    project_path = ui_mgr.project_path
    if not project_path:
        return

    state = get_project_state_for(project_path)
    job_model = state.jobs.get(job_type)

    if not job_model or not job_model.relion_job_name:
        if not ui_mgr.is_running:
            ui_mgr.cleanup_job_logs_timer(job_type)
        return

    ui_mgr.cleanup_job_logs_timer(job_type)
    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    content_container = widget_refs.content_container
    if content_container:
        content_container.clear()
        with content_container:
            render_logs_tab(job_type, job_model, backend, ui_mgr)


async def _refresh_job_logs(job_type: JobType, backend, ui_mgr: UIStateManager):
    from services.project_state import get_project_state_for

    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    monitor = widget_refs.monitor_logs

    if not monitor or "stdout" not in monitor:
        return

    if monitor["stdout"].is_deleted:
        ui_mgr.cleanup_job_logs_timer(job_type)
        return

    project_path = ui_mgr.project_path
    if not project_path:
        return

    state = get_project_state_for(project_path)
    job_model = state.jobs.get(job_type)

    if not job_model or not job_model.relion_job_name:
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

    if job_model.execution_status in (JobStatus.SUCCEEDED, JobStatus.FAILED) and not ui_mgr.is_running:
        ui_mgr.cleanup_job_logs_timer(job_type)

