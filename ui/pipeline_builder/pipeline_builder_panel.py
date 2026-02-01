# ui/pipeline_builder/pipeline_builder_panel.py
"""
Pipeline builder panel.
Refactored to implement "Create Project First" workflow and ensure state synchronization.
"""
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable, List, Set

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobType, get_state_service
from ui.status_indicator import ReactiveStatusDot
import pandas as pd
from ui.ui_state import (
    PIPELINE_ORDER,
    get_ui_state_manager,
    get_job_display_name,
    get_ordered_jobs,
)
from ui.pipeline_builder.job_tab_component import (
    render_job_tab,
    render_status_dot,
    render_status_badge,
)

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable, List, Set

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobType, get_state_service
from ui.status_indicator import ReactiveStatusDot, render_status_dot, render_status_badge  # CHANGED
import pandas as pd
from ui.ui_state import (
    PIPELINE_ORDER,
    get_ui_state_manager,
    get_job_display_name,
    get_ordered_jobs,
)
from ui.pipeline_builder.job_tab_component import render_job_tab  # CHANGED - removed render_status_dot, render_status_badge


JOB_DEPENDENCIES: Dict[JobType, List[JobType]] = {
    JobType.IMPORT_MOVIES: [],
    JobType.FS_MOTION_CTF: [JobType.IMPORT_MOVIES],
    JobType.TS_ALIGNMENT: [JobType.FS_MOTION_CTF],
    JobType.TS_CTF: [JobType.TS_ALIGNMENT],
    JobType.TS_RECONSTRUCT: [JobType.TS_CTF],
    JobType.DENOISE_TRAIN: [JobType.TS_RECONSTRUCT],
    JobType.DENOISE_PREDICT: [JobType.DENOISE_TRAIN, JobType.TS_RECONSTRUCT],
    JobType.TEMPLATE_MATCH_PYTOM: [JobType.TS_CTF],  # Can use reconstruct OR denoise
    JobType.TEMPLATE_EXTRACT_PYTOM: [JobType.TEMPLATE_MATCH_PYTOM],
    JobType.SUBTOMO_EXTRACTION: [JobType.TEMPLATE_EXTRACT_PYTOM],
}

def get_missing_dependencies(job_type: JobType, selected_jobs: Set[JobType]) -> List[JobType]:
    """Check if a job's dependencies are in the selected set."""
    deps = JOB_DEPENDENCIES.get(job_type, [])
    # For jobs with multiple possible deps (like TEMPLATE_MATCH), we only need ONE
    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        # Needs at least one tomogram source
        tomogram_sources = {JobType.TS_RECONSTRUCT, JobType.DENOISE_PREDICT}
        if not (tomogram_sources & selected_jobs):
            return [JobType.TS_RECONSTRUCT]  # Suggest reconstruct
        return []
    
    return [d for d in deps if d not in selected_jobs]

def render_job_flow(selected_jobs: List[JobType], on_toggle: Callable[[JobType], None]):
    """
    Render the visual job flow - shows all available jobs with visual cues
    for ordering and selection state.
    """
    selected_set = set(selected_jobs)
    
    for i, job_type in enumerate(PIPELINE_ORDER):
        is_selected = job_type in selected_set
        name = get_job_display_name(job_type)
        
        # Check if dependencies are satisfied
        missing_deps = get_missing_dependencies(job_type, selected_set)
        has_warning = is_selected and len(missing_deps) > 0
        
        # Determine visual state
        if is_selected:
            if has_warning:
                bg = "#fef3c7"
                border = "#f59e0b"
                text = "#92400e"
                icon_color = "text-yellow-600"
            else:
                bg = "#dbeafe"
                border = "#3b82f6"
                text = "#1e40af"
                icon_color = "text-blue-600"
        else:
            bg = "#f9fafb"
            border = "#e5e7eb"
            text = "#6b7280"
            icon_color = "text-gray-400"
        
        with ui.row().classes("items-center").style("gap: 2px;"):
            with ui.button(on_click=lambda j=job_type: on_toggle(j)).props("flat dense no-caps").style(
                f"background: {bg}; color: {text}; padding: 4px 12px; "
                f"border-radius: 16px; font-weight: 500; font-size: 11px; "
                f"border: 1.5px solid {border}; min-height: 28px;"
            ):
                with ui.row().classes("items-center gap-1"):
                    if is_selected:
                        ui.icon("check_circle", size="14px").classes(icon_color)
                    else:
                        ui.icon("radio_button_unchecked", size="14px").classes(icon_color)
                    
                    ui.label(name)
                    
                    if has_warning:
                        ui.icon("warning", size="12px").classes("text-yellow-600").tooltip(
                            f"Missing: {', '.join(get_job_display_name(d) for d in missing_deps)}"
                        )
            
            if i < len(PIPELINE_ORDER) - 1:
                next_job = PIPELINE_ORDER[i + 1]
                is_connected = is_selected and next_job in selected_set
                
                ui.icon(
                    "arrow_forward",
                    size="14px"
                ).classes(
                    "text-blue-400" if is_connected else "text-gray-300"
                ).style("margin: 0 2px;")


def build_pipeline_builder_panel(
    backend: CryoBoostBackend,
    callbacks: Dict[str, Callable],
) -> None:
    """
    Build the pipeline builder panel.
    """
    ui_mgr = get_ui_state_manager()
    state_service = get_state_service()
    
    # ===========================================
    # Internal Functions
    # ===========================================
    
    def add_job_to_pipeline(job_type: JobType):
        """Add a job to the UI list and ensure its params are initialized in State."""
        result = ui_mgr.add_job(job_type)
        if not result:
            return

        state = state_service.state
        if job_type not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            job_star_path = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)
        
        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())

        ui_mgr.set_active_job(job_type)
        rebuild_pipeline_ui()
        
    def remove_job_from_pipeline(job_type: JobType):
        """Remove a job from the pipeline."""
        if not ui_mgr.remove_job(job_type):
            return
        
        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())
        
        rebuild_pipeline_ui()
    
    def toggle_job_in_pipeline(job_type: JobType):
        """Toggle a job's presence in the pipeline."""
        if ui_mgr.is_job_selected(job_type):
            remove_job_from_pipeline(job_type)
        else:
            selected = set(ui_mgr.selected_jobs)
            missing = get_missing_dependencies(job_type, selected)
            
            if missing:
                missing_names = ", ".join(get_job_display_name(j) for j in missing)
                ui.notify(
                    f"Warning: {get_job_display_name(job_type)} typically requires: {missing_names}",
                    type="warning",
                    timeout=4000,
                )
            
            add_job_to_pipeline(job_type)
    
    def update_status_label():
        """Update the pipeline status label."""
        label = ui_mgr.panel_refs.status_label
        if not label:
            return
        
        count = len(ui_mgr.selected_jobs)
        if count == 0:
            label.set_text("No jobs selected")
        elif ui_mgr.is_running:
            label.set_text("Pipeline running...")
        else:
            label.set_text(f"{count} jobs ready")
    
    def rebuild_pipeline_ui():
        """Rebuild the entire pipeline UI."""
        update_status_label()

        # Rebuild job flow visualization
        flow_container = ui_mgr.panel_refs.job_tags_container
        if flow_container:
            should_hide = not ui_mgr.is_project_created or ui_mgr.is_running
            flow_container.set_visibility(not should_hide)
            
            if not should_hide:
                flow_container.clear()
                with flow_container:
                    render_job_flow(ui_mgr.selected_jobs, toggle_job_in_pipeline)
        
        # Rebuild job tabs
        tabs_container = ui_mgr.panel_refs.job_tabs_container
        if not tabs_container:
            return
        
        tabs_container.clear()
        
        # Placeholder for "Create Project First"
        if not ui_mgr.is_project_created:
            with tabs_container:
                with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-4 mt-16"):
                    ui.icon("create_new_folder", size="64px").classes("text-gray-300")
                    ui.label("Step 1: Create a Project").classes("text-xl font-bold text-gray-500")
                    ui.label("Configure your project on the left to begin building the pipeline.").classes("text-sm")
            return

        selected = ui_mgr.selected_jobs
        if not selected:
            with tabs_container:
                ui.label("Click jobs above to add them to your pipeline").classes(
                    "text-xs text-gray-500 italic text-center p-8"
                )
            return
        
        # Ensure active tab is valid
        active = ui_mgr.active_job
        if active not in selected:
            ui_mgr.set_active_job(selected[0])
        
        with tabs_container:
            build_unified_job_tabs()
    
    def build_unified_job_tabs():
        """Build the tab strip and content for selected jobs."""
        
        def switch_tab(job_type: JobType):
            ui_mgr.set_active_job(job_type)
            rebuild_pipeline_ui()
        
        # Tab strip
        with ui.row().classes("w-full border-b border-gray-200").style("gap: 0;"):
            for job_type in ui_mgr.selected_jobs:
                name = get_job_display_name(job_type)
                is_active = ui_mgr.active_job == job_type
                
                with ui.button(on_click=lambda j=job_type: switch_tab(j)).props(
                    "flat no-caps dense"
                ).style(
                    f"padding: 8px 20px; border-radius: 0; "
                    f"background: {'white' if is_active else '#fafafa'}; "
                    f"color: {'#1f2937' if is_active else '#6b7280'}; "
                    f"border-top: 2px solid {'#3b82f6' if is_active else 'transparent'}; "
                    f"border-right: 1px solid #e5e7eb; "
                    f"font-weight: {500 if is_active else 400};"
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        ReactiveStatusDot(job_type)
        
        # Tab content
        with ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;"):
            active = ui_mgr.active_job
            if active:
                render_job_tab(
                    job_type=active,
                    backend=backend,
                    ui_mgr=ui_mgr,
                    callbacks={
                        **callbacks,
                        "check_and_update_statuses": check_and_update_statuses,
                        "rebuild_pipeline_ui": rebuild_pipeline_ui,
                        "remove_job_from_pipeline": remove_job_from_pipeline,
                    }
                )
    
    async def check_and_update_statuses():
        """Refresh job statuses from the pipeline file."""
        project_path = ui_mgr.project_path
        if not project_path:
            return
        
        await backend.pipeline_runner.status_sync.sync_all_jobs(str(project_path))
        
        render_status_dot.refresh()
        render_status_badge.refresh()
        
        if ui_mgr.is_running:
            overview = await backend.get_pipeline_overview(str(project_path))
            
            total = overview.get("total", 0)
            completed = overview.get("completed", 0)
            failed = overview.get("failed", 0)
            running = overview.get("running", 0)
            scheduled = overview.get("scheduled", 0)
            
            has_activity = (completed > 0 or failed > 0)
            is_truly_complete = running == 0 and has_activity and scheduled == 0
            
            if is_truly_complete:
                ui_mgr.set_pipeline_running(False)
                stop_all_timers()
                try:
                    if failed > 0:
                        ui.notify(f"Pipeline finished with {failed} failed job(s).", type="warning")
                    else:
                        ui.notify("Pipeline execution finished.", type="positive")
                except RuntimeError:
                    pass
                rebuild_pipeline_ui()
    
    def stop_all_timers():
        """Stop all polling timers."""
        ui_mgr.cleanup_all_timers()
    
    async def safe_status_check():
        """Wrapper that catches errors from status checks."""
        try:
            await check_and_update_statuses()
        except Exception as e:
            print(f"[UI] Status check failed (pipeline still running): {e}")

    async def handle_run_pipeline():
        """
        The critical trigger.
        1. Saves current parameter state to disk.
        2. Calls backend.start_pipeline which triggers the Orchestrator's Just-In-Time scheme generation.
        """
        if not ui_mgr.is_project_created:
            ui.notify("Create a project first", type="warning")
            return

        # 1. Save UI State to Disk (project_params.json)
        await state_service.save_project()

        # 2. Execute via Backend
        selected_job_strings = [j.value for j in ui_mgr.selected_jobs]
        
        # Visual feedback
        if ui_mgr.panel_refs.run_button:
            ui_mgr.panel_refs.run_button.props("loading")

        try:
            result = await backend.start_pipeline(
                project_path=str(ui_mgr.project_path),
                scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
                selected_jobs=selected_job_strings,
                required_paths=[]
            )

            if result.get("success"):
                ui_mgr.set_pipeline_running(True)
                ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                
                # Start polling for status updates
                ui_mgr.status_timer = ui.timer(3.0, lambda: asyncio.create_task(safe_status_check()))
            else:
                ui.notify(f"Failed to start: {result.get('error')}", type="negative")

        except Exception as e:
            ui.notify(f"Error: {e}", type="negative")
        finally:
            if ui_mgr.panel_refs.run_button:
                ui_mgr.panel_refs.run_button.props(remove="loading")
    
    # ===========================================
    # Main Layout
    # ===========================================
        
    with ui.column().classes("w-full h-full overflow-hidden").style(
        "gap: 0px; font-family: 'IBM Plex Sans', sans-serif;"
    ):
        # Header section
        with ui.column().classes("w-full p-3 bg-white border-b border-gray-200 shrink-0"):
            # Continuation controls container (hidden by default)
            cont_container = ui.column().classes("w-full")
            cont_container.set_visibility(False)
            ui_mgr.panel_refs.continuation_container = cont_container
            
            # Job flow visualization
            job_tags_container = ui.row().classes("w-full flex-wrap items-center mb-3").style("gap: 4px;")
            ui_mgr.panel_refs.job_tags_container = job_tags_container
            
            with job_tags_container:
                render_job_flow(ui_mgr.selected_jobs, toggle_job_in_pipeline)
            
            # Pipeline controls row
            with ui.row().classes("w-full items-center justify-end").style("gap: 12px;"):
                status_label = ui.label("No jobs selected").classes("text-xs text-gray-600")
                ui_mgr.panel_refs.status_label = status_label
                
                run_btn = ui.button(
                    "Run Pipeline",
                    icon="play_arrow",
                    on_click=handle_run_pipeline  # NOT lambda: asyncio.create_task(...)
                ).props("dense flat no-caps").style(
                    "background: #f3f4f6; color: #1f2937; padding: 6px 20px; "
                    "border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                )
                ui_mgr.panel_refs.run_button = run_btn
                
                stop_btn = ui.button("Stop", icon="stop").props(
                    "dense flat no-caps disable"
                ).style(
                    "background: #f3f4f6; color: #1f2937; padding: 6px 20px; "
                    "border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                )
                ui_mgr.panel_refs.stop_button = stop_btn
        
        # Job tabs container
        tabs_container = ui.column().classes("w-full flex-grow overflow-hidden")
        ui_mgr.panel_refs.job_tabs_container = tabs_container
        
        with tabs_container:
            pass
    
    # ===========================================
    # Register callbacks
    # ===========================================
    
    ui_mgr.set_rebuild_callback(rebuild_pipeline_ui)
    
    callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
    callbacks["stop_all_timers"] = stop_all_timers
    callbacks["check_and_update_statuses"] = lambda: asyncio.create_task(check_and_update_statuses())
    callbacks["enable_run_button"] = lambda: (
        ui_mgr.panel_refs.run_button.props(remove="disable")
        if ui_mgr.panel_refs.run_button else None
    )
    callbacks["add_job_to_pipeline"] = add_job_to_pipeline
    callbacks["remove_job_from_pipeline"] = remove_job_from_pipeline

    # Trigger initial rebuild to show "Create Project" or existing jobs
    rebuild_pipeline_ui()
