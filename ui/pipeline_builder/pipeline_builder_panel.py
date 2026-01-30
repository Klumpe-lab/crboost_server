# ui/pipeline_builder/pipeline_builder_panel.py
"""
Pipeline builder panel.
Refactored to implement "Create Project First" workflow and ensure state synchronization.
"""
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobType, get_state_service
from ui.status_indicator import ReactiveStatusDot
import pandas as pd
from ui.ui_state import (
    get_ui_state_manager,
    get_job_display_name,
    get_ordered_jobs,
)
from ui.pipeline_builder.job_tab_component import (
    render_job_tab,
    render_status_dot,
    render_status_badge,
)


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
            result = ui_mgr.add_job(job_type) # Adds to UI list
            if not result: return

            # Initialize params in ProjectState (Load from Blueprint if not exists)
            state = state_service.state
            if job_type not in state.jobs:
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
                job_star_path = template_base / job_type.value / "job.star"
                
                # Load defaults into memory
                state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)
            
            # Save state immediately
            if ui_mgr.is_project_created:
                asyncio.create_task(state_service.save_project())

            # [REMOVED] Logic calling continuation_service. 
            # We don't touch Relion files here. We just update our JSON state.

            ui_mgr.set_active_job(job_type)
            update_job_tag_button(job_type)
            rebuild_pipeline_ui()
        
    def remove_job_from_pipeline(job_type: JobType):
        """Remove a job from the pipeline."""
        if not ui_mgr.remove_job(job_type):
            return
        
        # PERSISTENCE FIX: Save removal to project_params.json
        if ui_mgr.is_project_created:
             asyncio.create_task(state_service.save_project())
        
        update_job_tag_button(job_type)
        rebuild_pipeline_ui()
    
    def toggle_job_in_pipeline(job_type: JobType):
        """Toggle a job's presence in the pipeline."""
        if ui_mgr.is_job_selected(job_type):
            remove_job_from_pipeline(job_type)
        else:
            add_job_to_pipeline(job_type)
    
    def update_job_tag_button(job_type: JobType):
        """Update the visual state of a job tag button."""
        btn = ui_mgr.panel_refs.job_tag_buttons.get(job_type.value)
        if not btn:
            return
        
        is_selected = ui_mgr.is_job_selected(job_type)
        btn.style(
            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
        )
    
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
            label.set_text(f"{count} jobs · Ready to run")
    
    def rebuild_pipeline_ui():
        """Rebuild the entire pipeline UI."""
        update_status_label()

        tags_container = ui_mgr.panel_refs.job_tags_container
        if tags_container:
            # FORCE WORKFLOW: Hide tags if project NOT created or if running
            should_hide = not ui_mgr.is_project_created or ui_mgr.is_running
            tags_container.set_visibility(not should_hide)
        
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
                ui.label("Select jobs from the tags above to build your pipeline").classes(
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
                        # render_status_dot(job_type)
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
            
            # Check if pipeline is truly complete:
            # - At least one job must have run (completed > 0 or failed > 0)
            # - No jobs currently running
            # - Not all jobs still scheduled
            total     = overview.get("total", 0)
            completed = overview.get("completed", 0)
            failed    = overview.get("failed", 0)
            running   = overview.get("running", 0)
            scheduled = overview.get("scheduled", 0)
            
            # Pipeline is complete when:
            # 1. Nothing is running AND
            # 2. Something has actually happened (not everything still scheduled)
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
                            # Context lost, safe to ignore for background notification
                            pass
                        rebuild_pipeline_ui()
    
    def stop_all_timers():
        """Stop all polling timers."""
        ui_mgr.cleanup_all_timers()
    
    # In pipeline_builder_panel.py, replace handle_run_pipeline with:
    async def safe_status_check():
        """Wrapper that catches errors from status checks."""
        try:
            await check_and_update_statuses()
        except Exception as e:
            # Log but don't crash - the pipeline is still running
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
            # We pass the list of selected jobs. The Orchestrator will decide
            # how to link them based on ProjectState history.
            
            selected_job_strings = [j.value for j in ui_mgr.selected_jobs]
            
            # Visual feedback
            if ui_mgr.panel_refs.run_button:
                ui_mgr.panel_refs.run_button.props("loading")

            try:
                result = await backend.start_pipeline(
                    project_path=str(ui_mgr.project_path),
                    scheme_name=f"run_{datetime.now().strftime('%H%M%S')}", # Unique run ID
                    selected_jobs=selected_job_strings,
                    required_paths=[] # Backend handles glob binds now
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
        
    with ui.column().classes("w-full h-full overflow-hidden").style(
        "gap: 0px; font-family: 'IBM Plex Sans', sans-serif;"
    ):
        # Header section
        with ui.column().classes("w-full p-3 bg-white border-b border-gray-200 shrink-0"):
            # Continuation controls container (hidden by default)
            cont_container = ui.column().classes("w-full")
            cont_container.set_visibility(False)
            ui_mgr.panel_refs.continuation_container = cont_container
            
            # Job selection and controls row
            with ui.row().classes("w-full items-center justify-between mb-4").style("gap: 12px;"):
                # Job tags
                job_tags_container = ui.row().classes("flex-1 flex-wrap").style("gap: 8px;")
                ui_mgr.panel_refs.job_tags_container = job_tags_container
                
                with job_tags_container:
                    for job_type in get_ordered_jobs():
                        name = get_job_display_name(job_type)
                        is_selected = ui_mgr.is_job_selected(job_type)
                        
                        btn = ui.button(
                            name,
                            on_click=lambda j=job_type: toggle_job_in_pipeline(j)
                        ).props("no-caps dense flat").style(
                            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
                            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
                            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
                            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
                        )
                        ui_mgr.panel_refs.job_tag_buttons[job_type.value] = btn
                
                # Pipeline controls
                with ui.row().classes("items-center").style("gap: 10px;"):
                    status_label = ui.label("No jobs selected").classes("text-xs text-gray-600")
                    ui_mgr.panel_refs.status_label = status_label
                    
                    run_btn = ui.button(
                        "Run Pipeline",
                        icon="play_arrow",
                        on_click=handle_run_pipeline
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
            # Default placeholder will be handled by rebuild_pipeline_ui
            pass
    
    # ===========================================
    # Register callbacks
    # ===========================================
    
    ui_mgr.set_rebuild_callback(rebuild_pipeline_ui)
    
    callbacks["rebuild_pipeline_ui"]       = rebuild_pipeline_ui
    callbacks["stop_all_timers"]           = stop_all_timers
    callbacks["check_and_update_statuses"] = lambda: asyncio.create_task(check_and_update_statuses())
    callbacks["enable_run_button"]         = lambda: (
        ui_mgr.panel_refs.run_button.props(remove="disable")
        if ui_mgr.panel_refs.run_button else None
    )
    callbacks["add_job_to_pipeline"] = add_job_to_pipeline
    callbacks["remove_job_from_pipeline"] = remove_job_from_pipeline

    # Trigger initial rebuild to show "Create Project" or existing jobs
    rebuild_pipeline_ui()
