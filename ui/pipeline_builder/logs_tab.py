# ui/pipeline_builder/logs_tab.py
import asyncio

from nicegui import ui

from services.project_state import JobStatus, JobType
from ui.components.buttons import icon_button
from ui.components.log_pane import LiveLog
from ui.components.segmented import render_segmented
from ui.styles import MONO, SANS
from ui.ui_state import UIStateManager

_LOG_FILES = {"stdout": "run.out", "stderr": "run.err"}
# What the log readers put in place of a log that has no lines yet.
_NO_LOG = {"", "No output yet", "No errors yet", "run.out not found.", "run.err not found."}


def render_logs_tab(job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager):
    widget_refs = ui_mgr.get_job_widget_refs(instance_id)
    ui_mgr.cleanup_job_logs_timer(instance_id)

    # The log polls run only while this job's page is the one shown (its view scope).
    scope = ui_mgr.job_scope(instance_id)

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center gap-2") as placeholder:
            ui.icon("schedule", size="24px").style("color: #cbd5e1;")
            ui.label("Scheduled · its logs show here once it runs").style(f"{SANS} font-size: 11px; color: #94a3b8;")

        if ui_mgr.is_running:
            widget_refs.logs_timer = scope.timer(
                3.0,
                lambda: asyncio.create_task(_refresh_job_logs_with_placeholder_swap(instance_id, backend, ui_mgr)),
                placeholder,
            )
        return

    is_running = job_model.execution_status == JobStatus.RUNNING
    job_dir = job_model.relion_job_name.rstrip("/")

    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0;") as logs_box:
        # Header: the stream switch (stderr's line count on it), the file shown, a live mark
        # while the job runs, and copy-the-whole-file.
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px; flex-shrink: 0; padding: 6px 10px 0;"):
            seg = render_segmented(
                [("stdout", "stdout"), ("stderr", "stderr")], "stdout", lambda k: switch_log(k), classes="cb-seg-sm"
            )
            file_label = ui.label(f"{job_dir}/run.out").style(
                f"{MONO} font-size: 9px; color: #94a3b8; overflow: hidden; text-overflow: ellipsis; "
                "white-space: nowrap; min-width: 0;"
            )
            with ui.row().classes("items-center no-wrap").style("gap: 4px; flex-shrink: 0;") as live_mark:
                ui.element("div").classes("cb-live-pulse").style(
                    "width: 6px; height: 6px; border-radius: 50%; background: #3b82f6;"
                )
                ui.label("live").style(f"{SANS} font-size: 9px; color: #3b82f6;")
            live_mark.set_visibility(is_running)
            ui.space()
            # Copies the whole current log (the panel shows the last 500 lines).
            icon_button(
                "content_copy",
                lambda: _copy_active_log(monitor, backend, ui_mgr),
                tooltip="Copy the full log to the clipboard",
            )

        # ── Log wells (stacked, toggle visibility), each one element that wraps long lines. ──
        stdout_log = LiveLog()
        stderr_log = LiveLog()
        stderr_log.set_visibility(False)

    monitor = {
        "stdout": stdout_log,
        "stderr": stderr_log,
        "_seg": seg,
        "_live": live_mark,
        "_active": "stdout",
        # Set here as well as on refresh so the copy button has a target before the first tick.
        "_job_path": job_model.relion_job_name,
    }

    def switch_log(tab):
        is_stdout = tab == "stdout"
        monitor["_active"] = tab
        stdout_log.set_visibility(is_stdout)
        stderr_log.set_visibility(not is_stdout)
        file_label.set_text(f"{job_dir}/{_LOG_FILES[tab]}")
        seg.set_active(tab)

    widget_refs.monitor_logs = monitor

    asyncio.create_task(_refresh_job_logs(instance_id, backend, ui_mgr))

    if is_running or ui_mgr.is_running:
        widget_refs.logs_timer = scope.timer(
            3.0, lambda: asyncio.create_task(_refresh_job_logs(instance_id, backend, ui_mgr)), logs_box
        )


async def _copy_active_log(monitor: dict, backend, ui_mgr: UIStateManager) -> None:
    """Copy the whole log. The panel is fed a tail (the poll must stay cheap — a supervisor's
    run.err reaches megabytes), so the full text is read here, once, on demand."""
    tab = monitor.get("_active", "stdout")
    job_path = monitor.get("_job_path")
    project_path = ui_mgr.project_path
    if not job_path or not project_path:
        ui.notify(f"No {tab} to copy yet", type="warning")
        return
    logs = await backend.get_job_logs(str(project_path), job_path, tail_bytes=0)
    text = logs.get(tab, "")
    if not text:
        ui.notify(f"No {tab} to copy yet", type="warning")
        return
    ui.clipboard.write(text)
    ui.notify(f"Copied {tab} ({len(text.splitlines())} lines)", type="positive", timeout=1500)


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

    # Written only when the text changed: an unchanged tick sends nothing to the browser.
    monitor["stdout"].set_text(stdout)
    monitor["stderr"].set_text(stderr)

    # stderr's line count on its segment, so output there is visible from stdout; nothing for
    # the readers' own placeholders. "+" when the panel shows only the tail.
    seg = monitor.get("_seg")
    if seg is not None:
        has_stderr = stderr.strip() not in _NO_LOG
        n = len([line for line in stderr_lines if line.strip()])
        seg.set_badge("stderr", f"{min(n, MAX_LOG_LINES)}{'+' if n > MAX_LOG_LINES else ''}" if has_stderr else "")
    live = monitor.get("_live")
    if live is not None and not live.is_deleted:
        live.set_visibility(job_model.execution_status == JobStatus.RUNNING)

    if job_model.execution_status in (JobStatus.SUCCEEDED, JobStatus.FAILED) and not ui_mgr.is_running:
        ui_mgr.cleanup_job_logs_timer(instance_id)
