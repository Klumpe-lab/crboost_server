# ui/pipeline_builder/logs_tab.py
import asyncio

from nicegui import ui

from services.project_state import JobStatus, JobType
from ui.styles import MONO
from ui.ui_state import UIStateManager

_TAB_ACTIVE = (
    f"{MONO} font-size: 10px; font-weight: 600; padding: 2px 10px; border-radius: 0; "
    "background: #f1f5f9; color: #1e293b; min-width: 0;"
)
_TAB_INACTIVE = (
    f"{MONO} font-size: 10px; font-weight: 400; padding: 2px 10px; border-radius: 0; "
    "background: transparent; color: #94a3b8; min-width: 0;"
)


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
        # ── Mini tab bar ──
        with (
            ui.row()
            .classes("w-full items-center")
            .style("gap: 0; flex-shrink: 0; border-bottom: 1px solid #e2e8f0; padding: 0 8px;")
        ):
            stdout_btn = ui.button("stdout").props("flat dense no-caps")
            stdout_btn.style(_TAB_ACTIVE)

            stderr_btn = ui.button("stderr").props("flat dense no-caps")
            stderr_btn.style(_TAB_INACTIVE)

            stderr_dot = (
                ui.icon("circle", size="6px")
                .style("color: #ef4444; margin-left: -4px; display: none;")
                .tooltip("stderr has output")
            )

        # ── Log panels (stacked, toggle visibility) ──
        stdout_log = (
            ui.log(max_lines=1000)
            .classes("w-full p-2")
            .style(
                f"flex: 1; overflow-y: auto; {MONO} font-size: 10px; "
                "line-height: 1.4; background: #fafafa; min-height: 0;"
            )
        )
        stderr_log = (
            ui.log(max_lines=1000)
            .classes("w-full p-2")
            .style(
                f"flex: 1; overflow-y: auto; {MONO} font-size: 10px; "
                "line-height: 1.4; color: #b91c1c; background: #fefafa; min-height: 0;"
            )
        )
        stderr_log.set_visibility(False)

    def switch_log(tab):
        is_stdout = tab == "stdout"
        stdout_log.set_visibility(is_stdout)
        stderr_log.set_visibility(not is_stdout)
        stdout_btn.style(_TAB_ACTIVE if is_stdout else _TAB_INACTIVE)
        stderr_btn.style(_TAB_INACTIVE if is_stdout else _TAB_ACTIVE)

    stdout_btn.on_click(lambda: switch_log("stdout"))
    stderr_btn.on_click(lambda: switch_log("stderr"))

    widget_refs.monitor_logs = {"stdout": stdout_log, "stderr": stderr_log, "_stderr_dot": stderr_dot}

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

    # Show/hide stderr indicator dot — only show for actual error output,
    # not for placeholder messages when the log file doesn't exist yet.
    stderr_dot = monitor.get("_stderr_dot")
    if stderr_dot and not stderr_dot.is_deleted:
        _no_error_messages = {"No errors yet", "No errors", "run.err not found.", ""}
        has_stderr = bool(stderr.strip()) and stderr.strip() not in _no_error_messages
        stderr_dot.style(f"color: #ef4444; margin-left: -4px; display: {'inline-flex' if has_stderr else 'none'};")

    if job_model.execution_status in (JobStatus.SUCCEEDED, JobStatus.FAILED) and not ui_mgr.is_running:
        ui_mgr.cleanup_job_logs_timer(instance_id)
