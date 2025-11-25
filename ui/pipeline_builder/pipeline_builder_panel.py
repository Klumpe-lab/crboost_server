# ui/pipeline_builder/pipeline_builder_panel.py
"""
Pipeline builder panel orchestrator.
Handles layout, job selection, and coordinates sub-components with polling loop.
"""
import asyncio
from pathlib import Path
from backend import CryoBoostBackend
from nicegui import ui
from services.project_state import JobStatus, JobType, get_state_service, get_project_state
from ui.utils import JobConfig
from ui.pipeline_builder.job_tab_component import get_status_hex_color, render_job_tab, get_status_class, _update_badge_color
from ui.pipeline_builder.continuation_controls import build_continuation_controls
from typing import Dict, Any


def build_pipeline_builder_panel(backend: CryoBoostBackend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    state_service = get_state_service()

    panel_state = {
        "job_tabs_container": None,
        "run_button": None,
        "stop_button": None,
        "status_label": None,
        "status_timer": None,
        "continuation_container": None,
    }

    if "active_job_tab" not in shared_state: shared_state["active_job_tab"] = None
    if "job_cards" not in shared_state: shared_state["job_cards"] = {}

    def add_job_to_pipeline(job_type: JobType):
        if job_type in shared_state["selected_jobs"]: return
        shared_state["selected_jobs"].append(job_type)
        shared_state["selected_jobs"].sort(key=lambda j: JobConfig.PIPELINE_ORDER.index(j))

        state = state_service.state
        if job_type not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            job_star_path = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)

        if not shared_state.get("continuation_mode", False):
            job_model = state.jobs.get(job_type)
            if job_model:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None

        if job_type not in shared_state["job_cards"]:
            shared_state["job_cards"][job_type] = {"active_monitor_tab": "config"}

        shared_state["active_job_tab"] = job_type.value
        rebuild_pipeline_ui()

    def toggle_job_in_pipeline(job_type: JobType):
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return
        if job_type in shared_state["selected_jobs"]: remove_job_from_pipeline(job_type)
        else: add_job_to_pipeline(job_type)
        update_job_tag_button(job_type)

    def remove_job_from_pipeline(job_type: JobType):
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return
        if job_type in shared_state["selected_jobs"]:
            shared_state["selected_jobs"].remove(job_type)
            if job_type in shared_state["job_cards"]:
                if shared_state["job_cards"][job_type].get("logs_timer"):
                    shared_state["job_cards"][job_type]["logs_timer"].cancel()
                del shared_state["job_cards"][job_type]
            if shared_state["active_job_tab"] == job_type.value:
                shared_state["active_job_tab"] = (shared_state["selected_jobs"][0].value if shared_state["selected_jobs"] else None)
            rebuild_pipeline_ui()

    def stop_all_timers():
        if panel_state.get("status_timer"):
            panel_state["status_timer"].cancel()
            panel_state["status_timer"] = None
        for job_type in shared_state.get("job_cards", {}):
            job_state = shared_state["job_cards"][job_type]
            if job_state.get("logs_timer"):
                job_state["logs_timer"].cancel()
                job_state["logs_timer"] = None

    def rebuild_pipeline_ui():
        update_status_label()
        if panel_state["continuation_container"]:
            should_show = shared_state.get("continuation_mode", False)
            panel_state["continuation_container"].set_visibility(should_show)
            if should_show:
                panel_state["continuation_container"].clear()
                with panel_state["continuation_container"]:
                    build_continuation_controls(backend, shared_state, {**callbacks, "rebuild_pipeline_ui": rebuild_pipeline_ui, "check_and_update_statuses": check_and_update_statuses})

        if "job_tags_container" in shared_state:
            should_hide = shared_state["pipeline_running"] or shared_state.get("continuation_mode", False)
            shared_state["job_tags_container"].set_visibility(not should_hide)

        container = panel_state["job_tabs_container"]
        if not container: return
        container.clear()

        if not shared_state["selected_jobs"]:
            with container: ui.label("Select jobs from the tags above to build your pipeline").classes("text-xs text-gray-500 italic text-center p-8")
            return

        if shared_state["active_job_tab"] not in [j.value for j in shared_state["selected_jobs"]]:
            shared_state["active_job_tab"] = shared_state["selected_jobs"][0].value

        with container: build_unified_job_tabs()

    async def check_and_update_statuses():
        project_path = shared_state.get("current_project_path")
        if not project_path: return

        # Sync backend
        await backend.pipeline_runner.status_sync.sync_all_jobs(project_path)
        
        state = get_project_state()
        
        for job_type in shared_state["selected_jobs"]:
            job_model = state.jobs.get(job_type)
            card = shared_state.get("job_cards", {}).get(job_type, {})
            
            # Update Tab Dot
            if dot := card.get("ui_status_dot"):
                if not dot.is_deleted:
                    # 1. Update CSS class for animation
                    dot.classes(remove="pulse-running pulse-success pulse-failed pulse-scheduled")
                    dot.classes(get_status_class(job_model.execution_status))
                    
                    # 2. FORCE UPDATE BACKGROUND COLOR INLINE
                    # This ensures it is visible even if CSS fails
                    color = get_status_hex_color(job_model.execution_status)
                    dot.style(f"background-color: {color};")
            
            # Update Badge if active
            if job_type.value == shared_state.get("active_job_tab"):
                if badge := card.get("ui_status_label"):
                    if not badge.is_deleted:
                        badge.set_text(job_model.execution_status.value)
                        _update_badge_color(badge, job_model.execution_status)

        # Check completion
        if shared_state.get("pipeline_running"):
            overview = await backend.get_pipeline_overview(project_path)
            if overview.get("is_complete"):
                shared_state["pipeline_running"] = False
                stop_all_timers()
                ui.notify("Pipeline execution finished.", type="positive")
                rebuild_pipeline_ui()

    def update_status_label():
        if panel_state["status_label"]:
            count = len(shared_state["selected_jobs"])
            if count == 0: panel_state["status_label"].set_text("No jobs selected")
            elif shared_state["pipeline_running"]: panel_state["status_label"].set_text(f"Pipeline running...")
            else: panel_state["status_label"].set_text(f"{count} jobs · Ready to run")

    def build_unified_job_tabs():
        state = get_project_state()
        def switch_tab(job_type: JobType):
            shared_state["active_job_tab"] = job_type.value
            rebuild_pipeline_ui()

        with ui.row().classes("w-full border-b border-gray-200").style("gap: 0;"):
            for job_type in shared_state["selected_jobs"]:
                name = JobConfig.get_job_display_name(job_type)
                is_active = shared_state["active_job_tab"] == job_type.value
                job_model = state.jobs.get(job_type)
                status_class = get_status_class(job_model.execution_status) if job_model else "pulse-scheduled"
                status_color = get_status_hex_color(job_model.execution_status) if job_model else "#fbbf24"

                with ui.button(on_click=lambda j=job_type: switch_tab(j)).props("flat no-caps dense").style(
                        f"padding: 8px 20px; border-radius: 0; background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#6b7280'}; border-top: 2px solid {'#3b82f6' if is_active else 'transparent'}; "
                        f"border-right: 1px solid #e5e7eb; font-weight: {500 if is_active else 400};"
                    ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        
                        # FORCE background-color here
                        status_dot = ui.element("div") \
                            .classes(f"status-dot {status_class}") \
                            .style(f"width: 8px; height: 8px; border-radius: 50%; display: inline-block; background-color: {status_color};")
                        
                        if job_type in shared_state["job_cards"]:
                            shared_state["job_cards"][job_type]["ui_status_dot"] = status_dot

        with ui.column().classes("w-full flex-grow overflow-hidden"):
            for job_type in shared_state["selected_jobs"]:
                if shared_state["active_job_tab"] == job_type.value:
                    render_job_tab(job_type, backend, shared_state, {**callbacks, "check_and_update_statuses": check_and_update_statuses})

    def update_job_tag_button(job_type: JobType):
        btn = shared_state.get("job_buttons", {}).get(job_type)
        if not btn: return
        is_selected = job_type in shared_state["selected_jobs"]
        btn.style(
            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
        )

    async def handle_run_pipeline():
        if not shared_state["project_created"]:
            ui.notify("Create a project first", type="warning")
            return

        panel_state["run_button"].props("loading")
        result = await backend.start_pipeline(
            project_path=shared_state["current_project_path"],
            scheme_name=shared_state["current_scheme_name"],
            selected_jobs=[j.value for j in shared_state["selected_jobs"]],
            required_paths=[],
        )
        panel_state["run_button"].props(remove="loading")

        if result.get("success"):
            shared_state["pipeline_running"] = True
            state = state_service.state
            for job_type in shared_state["selected_jobs"]:
                job_model = state.jobs.get(job_type)
                if job_model and job_model.execution_status != JobStatus.SUCCEEDED:
                    job_model.execution_status = JobStatus.SCHEDULED

            if panel_state.get("status_timer"): panel_state["status_timer"].cancel()
            panel_state["status_timer"] = ui.timer(3.0, lambda: asyncio.create_task(check_and_update_statuses()))
            
            await check_and_update_statuses()
            ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
            panel_state["run_button"].props("disable")
            panel_state["stop_button"].props(remove="disable")
            rebuild_pipeline_ui()
        else:
            ui.notify(f"Failed: {result.get('error')}", type="negative")

    with ui.column().classes("w-full h-full overflow-hidden").style("gap: 0px; font-family: 'IBM Plex Sans', sans-serif;"):
        with ui.column().classes("w-full p-3 bg-white border-b border-gray-200 shrink-0"):
            panel_state["continuation_container"] = ui.column().classes("w-full")
            panel_state["continuation_container"].set_visibility(False)

            with ui.row().classes("w-full items-center justify-between mb-4").style("gap: 12px;"):
                job_tags_container = ui.row().classes("flex-1 flex-wrap").style("gap: 8px;")
                shared_state["job_tags_container"] = job_tags_container
                with job_tags_container:
                    for job_type in JobConfig.get_ordered_jobs():
                        name = JobConfig.get_job_display_name(job_type)
                        is_selected = job_type in shared_state["selected_jobs"]
                        btn = ui.button(name, on_click=lambda j=job_type: toggle_job_in_pipeline(j)).props("no-caps dense flat").style(
                                f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
                                f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
                                f"color: {'#1e40af' if is_selected else '#6b7280'}; "
                                f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
                            )
                        shared_state.setdefault("job_buttons", {})[job_type] = btn

                with ui.row().classes("items-center").style("gap: 10px;"):
                    panel_state["status_label"] = ui.label("No jobs selected").classes("text-xs text-gray-600")
                    panel_state["run_button"] = ui.button("Run Pipeline", icon="play_arrow", on_click=handle_run_pipeline).props("dense flat no-caps").style("background: #f3f4f6; color: #1f2937; padding: 6px 20px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;")
                    panel_state["stop_button"] = ui.button("Stop", icon="stop").props("dense flat no-caps disable").style("background: #f3f4f6; color: #1f2937; padding: 6px 20px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;")

        panel_state["job_tabs_container"] = ui.column().classes("w-full flex-grow overflow-hidden")
        with panel_state["job_tabs_container"]:
            ui.label("Select jobs from the tags above to build your pipeline").classes("text-xs text-gray-500 italic text-center p-8")

    callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
    callbacks["stop_all_timers"] = stop_all_timers
    callbacks["enable_run_button"] = lambda: panel_state["run_button"].props(remove="disable") if panel_state["run_button"] else None

    return panel_state
