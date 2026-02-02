# ui/pipeline_builder/job_tab_component.py
"""
Job tab component -- coordinator that assembles config/logs/files tabs.
"""

import asyncio
from datetime import datetime
from typing import Dict, Callable

from nicegui import ui

from services.project_state import JobStatus, JobType, get_project_state, get_state_service
from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service
from ui.status_indicator import ReactiveStatusBadge, render_status_badge, render_status_dot
from ui.ui_state import get_ui_state_manager, UIStateManager, MonitorTab, get_job_display_name

from ui.pipeline_builder.config_tab import render_config_tab, is_job_frozen
from ui.pipeline_builder.logs_tab import render_logs_tab
from ui.pipeline_builder.files_tab import render_files_tab


# ===========================================
# Helpers
# ===========================================

def snake_to_title(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split("_"))


async def auto_save_state():
    try:
        await get_state_service().save_project()
        print("[UI] Auto-saved project state")
    except Exception as e:
        print(f"[UI] Auto-save failed: {e}")


def create_save_handler() -> Callable:
    return lambda: asyncio.create_task(auto_save_state())


# ===========================================
# Main Render Function
# ===========================================

def render_job_tab(job_type: JobType, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]) -> None:
    state = get_project_state()
    job_model = state.jobs.get(job_type)

    if not job_model:
        ui.label(f"Error: Job model for {job_type.value} not found.").classes("text-xs text-red-600")
        return

    job_ui_state = ui_mgr.get_job_ui_state(job_type)
    widget_refs = ui_mgr.get_job_widget_refs(job_type)

    frozen = is_job_frozen(job_type)
    active_tab = job_ui_state.active_monitor_tab

    if frozen and active_tab == MonitorTab.CONFIG and not job_ui_state.user_switched_tab:
        active_tab = MonitorTab.LOGS
        job_ui_state.active_monitor_tab = MonitorTab.LOGS

    # --- Header ---
    with ui.column().classes("w-full border-b border-gray-200 bg-white pl-6 pr-6 pt-4 pb-4"):
        with ui.row().classes("w-full justify-between items-center"):
            with ui.column().classes("gap-0"):
                with ui.row().classes("items-center gap-2"):
                    ui.label(state.project_name).classes("text-lg font-bold text-gray-800")
                    ReactiveStatusBadge(job_type)
                created = (
                    state.created_at.strftime("%Y-%m-%d %H:%M")
                    if isinstance(state.created_at, datetime)
                    else str(state.created_at)
                )
                modified = (
                    state.modified_at.strftime("%Y-%m-%d %H:%M")
                    if isinstance(state.modified_at, datetime)
                    else str(state.modified_at)
                )
                ui.label(f"Created: {created} · Modified: {modified}").classes("text-xs text-gray-400")

            with ui.row().classes("items-center gap-4"):
                switcher_container = ui.row().classes("bg-gray-100 p-1 rounded-lg gap-0 border border-gray-200")
                widget_refs.switcher_container = switcher_container
                _render_tab_switcher(switcher_container, job_type, active_tab, backend, ui_mgr, callbacks)
                ui.button(icon="refresh", on_click=lambda: _force_status_refresh(callbacks)).props(
                    "flat dense round"
                ).classes("text-gray-400 hover:text-gray-800")
                if ui_mgr.is_project_created:
                    ui.button(icon="delete", on_click=lambda: asyncio.create_task(_handle_delete(job_type, job_model, backend, ui_mgr, callbacks))).props(
                        "flat round dense color=red"
                    ).tooltip("Delete this job")

    # --- Content ---
    content_container = ui.column().classes("w-full overflow-hidden").style(
        "flex: 1 1 0%; min-height: 0;"
    )
    widget_refs.content_container = content_container

    save_handler = create_save_handler()

    with content_container:
        if active_tab == MonitorTab.CONFIG:
            with ui.scroll_area().classes("w-full h-full p-2"):

                render_config_tab(job_type, job_model, frozen, ui_mgr, backend, save_handler)
        elif active_tab == MonitorTab.LOGS:
            render_logs_tab(job_type, job_model, backend, ui_mgr)
        elif active_tab == MonitorTab.FILES:
            render_files_tab(job_type, job_model, ui_mgr)


# ===========================================
# Tab Switcher
# ===========================================

def _render_tab_switcher(container, job_type, active_tab, backend, ui_mgr, callbacks):
    container.clear()
    tabs = [(MonitorTab.CONFIG, "Parameters"), (MonitorTab.LOGS, "Logs"), (MonitorTab.FILES, "Files")]

    with container:
        for tab, label in tabs:
            is_active = active_tab == tab
            btn = ui.button(label, on_click=lambda t=tab: _handle_tab_switch(job_type, t, backend, ui_mgr, callbacks))
            btn.props("flat dense no-caps")
            base_style = (
                "font-size: 12px; font-weight: 500; padding: 4px 16px; border-radius: 6px; transition: all 0.2s;"
            )
            if is_active:
                btn.style(f"{base_style} background: white; color: #111827; box-shadow: 0 1px 3px rgba(0,0,0,0.1);")
            else:
                btn.style(f"{base_style} background: transparent; color: #6b7280;")


def _handle_tab_switch(job_type, tab, backend, ui_mgr, callbacks):
    ui_mgr.set_job_monitor_tab(job_type, tab, user_initiated=True)
    widget_refs = ui_mgr.get_job_widget_refs(job_type)

    if widget_refs.switcher_container:
        _render_tab_switcher(widget_refs.switcher_container, job_type, tab, backend, ui_mgr, callbacks)

    if tab != MonitorTab.LOGS:
        ui_mgr.cleanup_job_logs_timer(job_type)

    content_container = widget_refs.content_container
    if content_container:
        state = get_project_state()
        job_model = state.jobs.get(job_type)
        frozen = is_job_frozen(job_type)
        save_handler = create_save_handler()

        content_container.clear()
        with content_container:
            if tab == MonitorTab.CONFIG:
                with ui.scroll_area().classes("w-full h-full p-2"):
                    render_config_tab(job_type, job_model, frozen, ui_mgr, backend, save_handler)
            elif tab == MonitorTab.LOGS:
                render_logs_tab(job_type, job_model, backend, ui_mgr)
            elif tab == MonitorTab.FILES:
                render_files_tab(job_type, job_model, ui_mgr)


# ===========================================
# Delete Handler
# ===========================================

async def _handle_delete(job_type, job_model, backend, ui_mgr, callbacks):
    """Handle job deletion with orphan preview."""
    deletion_service = get_deletion_service()
    project_path = ui_mgr.project_path

    preview = None
    if project_path and job_model.relion_job_name:
        preview = deletion_service.preview_deletion(
            project_path, job_model.relion_job_name, job_resolver=backend.pipeline_orchestrator.job_resolver
        )

    with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
        ui.label(f"Delete {get_job_display_name(job_type)}?").classes("text-lg font-bold")
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
            ui.label("No downstream jobs will be affected.").classes(
                "text-sm text-green-600 bg-green-50 p-2 rounded"
            )

        with ui.row().classes("w-full justify-end mt-4 gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")

            async def confirm():
                dialog.close()
                ui.notify("Deleting job...", type="info", timeout=1500)
                try:
                    result = await backend.delete_job(job_type.value)
                    if result.get("success"):
                        orphans = result.get("orphaned_jobs", [])
                        if orphans:
                            ui.notify(f"Job deleted. {len(orphans)} downstream job(s) orphaned.", type="warning", timeout=5000)
                        else:
                            ui.notify("Job deleted successfully.", type="positive")
                        remove_cb = callbacks.get("remove_job_from_pipeline")
                        if remove_cb:
                            remove_cb(job_type)
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


# ===========================================
# Status Refresh
# ===========================================

def _force_status_refresh(callbacks: Dict[str, Callable]):
    ui.notify("Refreshing statuses...", timeout=1)
    render_status_badge.refresh()
    render_status_dot.refresh()
    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())