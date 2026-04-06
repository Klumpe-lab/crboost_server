# ui/pipeline_builder/job_tab_component.py
import asyncio
import logging
from datetime import datetime
from typing import Dict, Callable, Optional

from nicegui import ui

from services.project_state import JobStatus, JobType, get_project_state, get_state_service
from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service
from ui.job_plugins import get_extra_tabs, get_full_panel_renderer
from ui.status_indicator import BoundStatusBadge, BoundStatusDot
from ui.ui_state import (
    get_ui_state_manager,
    UIStateManager,
    MonitorTab,
    get_job_display_name,
    get_instance_display_name,
    instance_id_to_job_type,
)
from ui.pipeline_builder.config_tab import render_config_tab, is_job_frozen
from ui.pipeline_builder.io_tab import render_io_tab
from ui.pipeline_builder.slurm_tab import render_slurm_tab
from ui.pipeline_builder.logs_tab import render_logs_tab
from ui.pipeline_builder.files_tab import render_files_tab

logger = logging.getLogger(__name__)


_SECTION_LABEL_STYLE = (
    "font-family: 'IBM Plex Sans', sans-serif; font-size: 11px; font-weight: 600; color: #1e293b; margin-bottom: 4px;"
)


class DebouncedSaver:
    def __init__(self, delay: float = 1.0):
        self._delay = delay
        self._task: Optional[asyncio.Task] = None

    def trigger(self):
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = asyncio.create_task(self._delayed_save())

    async def _delayed_save(self):
        try:
            await asyncio.sleep(self._delay)
            await get_state_service().save_project()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.info("Debounced save failed: %s", e)


def create_save_handler() -> Callable:
    return DebouncedSaver(delay=1.0).trigger


def _build_tab_list(job_type: JobType):
    tabs = [(MonitorTab.CONFIG.value, "Config"), (MonitorTab.LOGS.value, "Logs"), (MonitorTab.FILES.value, "Files")]
    for et in get_extra_tabs(job_type):
        tabs.append((et.key, et.label))
    return tabs


def _render_tab_content(
    tab_key: str,
    job_type: JobType,
    instance_id: str,
    job_model,
    is_frozen: bool,
    save_handler: Callable,
    backend,
    ui_mgr: UIStateManager,
):
    if tab_key == MonitorTab.CONFIG.value:
        with ui.scroll_area().classes("w-full h-full"):
            with ui.column().classes("w-full gap-0").style("padding: 10px 16px 16px;"):
                # ── SLURM (collapsible) ──
                with (
                    ui.expansion("SLURM / Requested Resources")
                    .props("dense default-opened")
                    .classes("w-full")
                    .style("border: none; box-shadow: none; background: transparent; margin-bottom: 6px;") as slurm_exp
                ):
                    slurm_exp.props('header-class="text-xs font-semibold text-gray-700 p-0"')
                    with ui.column().classes("w-full gap-0").style("padding: 4px 0 0;"):
                        render_slurm_tab(job_model, is_frozen, save_handler)
                # ── I/O (collapsible) ──
                with (
                    ui.expansion("I/O")
                    .props("dense default-opened")
                    .classes("w-full")
                    .style("border: none; box-shadow: none; background: transparent; margin-bottom: 6px;") as io_exp
                ):
                    io_exp.props('header-class="text-xs font-semibold text-gray-700 p-0"')
                    with ui.column().classes("w-full gap-0").style("padding: 4px 0 0;"):
                        render_io_tab(job_type, instance_id, job_model, is_frozen, ui_mgr, save_handler)
                # ── Parameters ──
                ui.label("Parameters").style(_SECTION_LABEL_STYLE)
                render_config_tab(job_type, job_model, is_frozen, ui_mgr, backend, save_handler)
    elif tab_key == MonitorTab.LOGS.value:
        render_logs_tab(job_type, instance_id, job_model, backend, ui_mgr)
    elif tab_key == MonitorTab.FILES.value:
        render_files_tab(job_type, job_model, ui_mgr)
    else:
        for et in get_extra_tabs(job_type):
            if tab_key == et.key:
                et.render(job_type, job_model, backend, ui_mgr)
                return
        ui.label(f"Unknown tab: {tab_key}").classes("text-red-500 p-4")


def render_job_tab(
    job_type: JobType, instance_id: str, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]
) -> None:
    state = get_project_state()
    job_model = state.jobs.get(instance_id)

    if not job_model:
        ui.label(f"Error: Job model for '{instance_id}' not found.").classes("text-xs text-red-600")
        return

    # --- Full-panel interactive jobs (tilt filter, etc.) bypass tab chrome ---
    full_renderer = get_full_panel_renderer(job_type)
    if full_renderer:
        _render_interactive_job(job_type, instance_id, job_model, backend, ui_mgr, callbacks, full_renderer)
        return

    job_ui_state = ui_mgr.get_job_ui_state(instance_id)
    widget_refs = ui_mgr.get_job_widget_refs(instance_id)

    frozen = is_job_frozen(instance_id)
    active_tab = job_ui_state.active_monitor_tab

    if frozen and active_tab == MonitorTab.CONFIG.value and not job_ui_state.user_switched_tab:
        active_tab = MonitorTab.LOGS.value
        job_ui_state.active_monitor_tab = MonitorTab.LOGS.value

    is_running = job_model.execution_status == JobStatus.RUNNING

    # --- Header ---
    with (
        ui.row()
        .classes("w-full items-center border-b border-gray-200 bg-white px-4 gap-2")
        .style("flex-shrink: 0; min-height: 38px; padding-top: 5px; padding-bottom: 5px;")
    ):
        BoundStatusBadge(instance_id)

        switcher_container = ui.element("div").style(
            "display: flex; border: 1px solid #e2e8f0; border-radius: 4px; overflow: hidden; flex-shrink: 0;"
        )
        widget_refs.switcher_container = switcher_container
        _render_tab_switcher(switcher_container, job_type, instance_id, active_tab, backend, ui_mgr, callbacks)

        if job_model.relion_job_name and ui_mgr.project_path:
            full_path = str(ui_mgr.project_path / job_model.relion_job_name.rstrip("/"))
            ui.label(full_path).style(
                "font-size: 10px; font-family: 'IBM Plex Mono', monospace; color: #94a3b8; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0;"
            )
            ui.button(
                icon="content_copy",
                on_click=lambda p=full_path: ui.run_javascript(f"navigator.clipboard.writeText({repr(p)})"),
            ).props("flat dense round size=xs").classes("text-gray-400 hover:text-gray-600").tooltip("Copy path")

        ui.space()

        if is_running:
            ui.button(
                "Cancel",
                icon="stop_circle",
                on_click=lambda: _handle_stop_job(job_type, instance_id, job_model, backend, ui_mgr, callbacks),
            ).props("dense flat no-caps").style(
                "color: #ea580c; border: 1px solid #fed7aa; border-radius: 3px; "
                "padding: 1px 8px; font-size: 10px; font-weight: 500;"
            )

        ui.button(icon="refresh", on_click=lambda: _force_status_refresh(callbacks)).props(
            "flat dense round size=sm"
        ).classes("text-gray-400 hover:text-gray-800")

        if ui_mgr.is_project_created:
            ui.button(
                icon="delete",
                on_click=lambda: _handle_delete(job_type, instance_id, job_model, backend, ui_mgr, callbacks),
            ).props("flat round dense size=sm color=red").tooltip("Delete this job")

    # --- Content ---
    content_container = ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;")
    widget_refs.content_container = content_container

    save_handler = create_save_handler()

    with content_container:
        _render_tab_content(active_tab, job_type, instance_id, job_model, frozen, save_handler, backend, ui_mgr)


def _render_interactive_job(
    job_type: JobType,
    instance_id: str,
    job_model,
    backend,
    ui_mgr: UIStateManager,
    callbacks: Dict[str, Callable],
    full_renderer: Callable,
):
    """Lightweight chrome for interactive tool-type jobs (no tab strip)."""
    # --- Compact header ---
    with (
        ui.row()
        .classes("w-full items-center px-6 py-2 border-b border-gray-200 bg-white gap-3")
        .style("flex-shrink: 0;")
    ):
        BoundStatusDot(instance_id)
        ui.label(get_job_display_name(job_type)).classes("text-sm font-semibold text-gray-800")
        if job_model.relion_job_name and ui_mgr.project_path:
            full_path = str(ui_mgr.project_path / job_model.relion_job_name.rstrip("/"))
            ui.label(full_path).classes("text-xs font-mono text-gray-400")
        ui.space()
        if ui_mgr.is_project_created:
            ui.button(
                icon="delete",
                on_click=lambda: _handle_delete(job_type, instance_id, job_model, backend, ui_mgr, callbacks),
            ).props("flat round dense color=red").tooltip("Delete this job")

    # --- Full panel content ---
    save_handler = create_save_handler()
    with ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;"):
        full_renderer(job_type, instance_id, job_model, backend, ui_mgr, save_handler)


def _render_tab_switcher(
    container,
    job_type: JobType,
    instance_id: str,
    active_tab: str,
    backend,
    ui_mgr: UIStateManager,
    callbacks: Dict[str, Callable],
):
    container.clear()
    tabs = _build_tab_list(job_type)
    with container:
        for i, (tab_key, label) in enumerate(tabs):
            is_active = active_tab == tab_key
            is_last = i == len(tabs) - 1
            btn = ui.button(
                label,
                on_click=lambda t=tab_key: _handle_tab_switch(job_type, instance_id, t, backend, ui_mgr, callbacks),
            )
            btn.props("flat dense no-caps")
            border_r = "" if is_last else "border-right: 1px solid #e2e8f0; "
            if is_active:
                btn.style(
                    f"font-size: 10px; font-weight: 600; padding: 3px 10px; border-radius: 0; "
                    f"background: #f1f5f9; color: #1e293b; {border_r}min-width: 0;"
                )
            else:
                btn.style(
                    f"font-size: 10px; font-weight: 500; padding: 3px 10px; border-radius: 0; "
                    f"background: white; color: #64748b; {border_r}min-width: 0;"
                )


def _handle_tab_switch(
    job_type: JobType, instance_id: str, tab_key: str, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]
):
    ui_mgr.set_job_monitor_tab(instance_id, tab_key, user_initiated=True)
    widget_refs = ui_mgr.get_job_widget_refs(instance_id)

    if widget_refs.switcher_container:
        _render_tab_switcher(widget_refs.switcher_container, job_type, instance_id, tab_key, backend, ui_mgr, callbacks)

    if tab_key != MonitorTab.LOGS.value:
        ui_mgr.cleanup_job_logs_timer(instance_id)

    content_container = widget_refs.content_container
    if content_container:
        state = get_project_state()
        job_model = state.jobs.get(instance_id)
        if job_model is None:
            return
        frozen = is_job_frozen(instance_id)
        save_handler = create_save_handler()
        content_container.clear()
        with content_container:
            _render_tab_content(tab_key, job_type, instance_id, job_model, frozen, save_handler, backend, ui_mgr)


async def _handle_stop_job(
    job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]
):
    project_path = ui_mgr.project_path
    job_dir = (project_path / job_model.relion_job_name.rstrip("/")) if job_model.relion_job_name else None

    with ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
        ui.label(f"Cancel {get_instance_display_name(instance_id, job_model)}?").classes(
            "text-base font-bold text-gray-800"
        )
        if job_dir:
            ui.label(str(job_dir)).classes("text-xs font-mono text-gray-500 mt-1")
        ui.label("The SLURM job will be cancelled, this job marked Failed, and the pipeline stopped.").classes(
            "text-sm text-gray-600 mt-2"
        )
        with ui.row().classes("mt-4 gap-2 justify-end w-full"):
            ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
            ui.button("Stop Job", on_click=lambda: dialog.submit(True)).props("dense no-caps").style(
                "background: #f97316; color: white; padding: 4px 16px; border-radius: 3px;"
            )

    confirmed = await dialog
    if not confirmed:
        return

    result = await backend.pipeline_runner.cancel_job(project_path, instance_id)
    await backend.pipeline_runner.sync_all_jobs(str(project_path))

    if "stop_all_timers" in callbacks:
        callbacks["stop_all_timers"]()
    ui_mgr.set_pipeline_running(False)
    if "rebuild_pipeline_ui" in callbacks:
        callbacks["rebuild_pipeline_ui"]()

    if result.get("success"):
        ui.notify(result.get("message", "Job cancelled."), type="warning", timeout=5000)
    else:
        ui.notify(f"Cancel failed: {result.get('error')}", type="negative", timeout=8000)


def _handle_delete(
    job_type: JobType, instance_id: str, job_model, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]
):
    # Interactive jobs use the roster's custom removal flow.
    if getattr(job_model, "IS_INTERACTIVE", False):
        remove_cb = callbacks.get("remove_instance_from_pipeline")
        if remove_cb:
            from services.project_state import get_project_state

            state = get_project_state()
            if state and instance_id in state.jobs:
                del state.jobs[instance_id]
                state.job_path_mapping.pop(instance_id, None)
                state.mark_dirty()
                asyncio.create_task(get_state_service().save_project())
            remove_cb(instance_id)
            ui.notify("Tilt filter removed. Labels preserved.", type="info")
        return

    deletion_service = get_deletion_service()
    project_path = ui_mgr.project_path

    preview = None
    if project_path and job_model.relion_job_name:
        preview = deletion_service.preview_deletion(
            project_path, job_model.relion_job_name, job_resolver=backend.pipeline_orchestrator.job_resolver
        )

    with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
        ui.label(f"Delete {get_instance_display_name(instance_id, job_model)}?").classes("text-lg font-bold")
        ui.label("This will move the job files to Trash/ and remove it from the pipeline.").classes(
            "text-sm text-gray-600 mb-2"
        )

        if preview and preview.get("success") and preview.get("downstream_count", 0) > 0:
            downstream = preview.get("downstream_jobs", [])
            with ui.card().classes("w-full bg-orange-50 border border-orange-200 p-3 mb-2"):
                with ui.row().classes("items-center gap-2 mb-2"):
                    ui.icon("warning", size="20px").classes("text-orange-600")
                    ui.label(f"{len(downstream)} job(s) will become orphaned:").classes(
                        "text-sm font-bold text-orange-800"
                    )
                with ui.column().classes("gap-1 ml-6"):
                    for detail in downstream:
                        with ui.row().classes("items-center gap-2"):
                            ui.label(detail.get("path", "Unknown")).classes("text-xs font-mono text-gray-700")
                            if detail.get("type"):
                                ui.label(f"({detail['type']})").classes("text-xs text-gray-500")
                            ui.label(f"- {detail.get('status', 'Unknown')}").classes("text-xs text-gray-500")
                ui.label("These jobs will have broken input references and may fail if re-run.").classes(
                    "text-xs text-orange-700 mt-2"
                )
        else:
            ui.label("No downstream jobs will be affected.").classes("text-sm text-green-600 bg-green-50 p-2 rounded")

        with ui.row().classes("w-full justify-end mt-4 gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")

            async def confirm():
                dialog.close()
                ui.notify("Deleting job...", type="info", timeout=1500)
                try:
                    result = await backend.delete_job(
                        instance_id_to_job_type(instance_id).value, instance_id=instance_id
                    )
                    if result.get("success"):
                        orphans = result.get("orphaned_jobs", [])
                        if orphans:
                            ui.notify(
                                f"Job deleted. {len(orphans)} downstream job(s) orphaned.", type="warning", timeout=5000
                            )
                        else:
                            ui.notify("Job deleted successfully.", type="positive")
                        remove_cb = callbacks.get("remove_instance_from_pipeline")
                        if remove_cb:
                            remove_cb(instance_id)
                    else:
                        ui.notify(f"Delete failed: {result.get('error')}", type="negative", timeout=8000)
                except Exception as e:
                    ui.notify(f"Error: {e}", type="negative")
                    import traceback

                    traceback.print_exc()

            delete_btn = ui.button("Delete", color="red", on_click=confirm)
            if preview and preview.get("downstream_count", 0) > 0:
                delete_btn.props('icon="delete_forever"')

    dialog.open()


def _force_status_refresh(callbacks: Dict[str, Callable]):
    ui.notify("Refreshing statuses...", timeout=1)
    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())
