# ui/pipeline_builder/pipeline_builder_panel.py
"""
Pipeline builder panel.
Refactored to implement "Create Project First" workflow and ensure state synchronization.
"""
import asyncio
from pathlib import Path
from typing import Dict, Any, Callable

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobStatus, JobType, get_state_service, get_project_state
from ui.status_indicator import ReactiveStatusDot
from ui.ui_state import (
    get_ui_state_manager,
    UIStateManager,
    get_job_display_name,
    get_ordered_jobs,
    MonitorTab,
)
from ui.pipeline_builder.job_tab_component import (
    render_job_tab,
    render_status_dot,
    render_status_badge,
)
from ui.pipeline_builder.continuation_controls import build_continuation_controls


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
        """Add a job to the pipeline and initialize its state."""
        result = ui_mgr.add_job(job_type)
        
        if not result:
            return
        
        # Initialize in project state (backend)
        state = state_service.state
        if job_type not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            job_star_path = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(
                job_type,
                job_star_path if job_star_path.exists() else None
            )
            
            # PERSISTENCE FIX: Save the new job to project_params.json immediately
            if ui_mgr.is_project_created:
                asyncio.create_task(state_service.save_project())
        
        # Reset status for new jobs (unless loading existing)
        if not ui_mgr.is_continuation_mode:
            job_model = state.jobs.get(job_type)
            if job_model:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
        
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

        cont_container = ui_mgr.panel_refs.continuation_container
        if cont_container:
            should_show = ui_mgr.is_continuation_mode
            cont_container.set_visibility(should_show)
            if should_show:
                cont_container.clear()
                with cont_container:
                    build_continuation_controls(
                        backend,
                        ui_mgr,
                        {
                            **callbacks,
                            "rebuild_pipeline_ui": rebuild_pipeline_ui,
                            "check_and_update_statuses": check_and_update_statuses,
                        }
                    )
        
        # Handle job tags visibility
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
        with ui.column().classes("w-full flex-grow overflow-hidden"):
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
                if failed > 0:
                    ui.notify(f"Pipeline finished with {failed} failed job(s).", type="warning")
                else:
                    ui.notify("Pipeline execution finished.", type="positive")
                rebuild_pipeline_ui()
    
    def stop_all_timers():
        """Stop all polling timers."""
        ui_mgr.cleanup_all_timers()
    
    # In pipeline_builder_panel.py, replace handle_run_pipeline with:

    async def handle_run_pipeline():
        """Handle the Run Pipeline button click."""
        if not ui_mgr.is_project_created:
            ui.notify("Create a project first", type="warning")
            return
        
        if not ui_mgr.selected_jobs:
            ui.notify("Add at least one job to the pipeline", type="warning")
            return
        
        run_btn = ui_mgr.panel_refs.run_button
        if run_btn:
            run_btn.props("loading")
        
        try:
            project_path = ui_mgr.project_path
            scheme_name = ui_mgr.scheme_name or f"scheme_{state_service.state.project_name}"
            selected_job_strings = [j.value for j in ui_mgr.selected_jobs]
            
            # Collect bind paths
            state = state_service.state

            additional_bind_paths = set()
            if state.movies_glob:
                    try:
                        additional_bind_paths.add(str(Path(state.movies_glob).parent.resolve()))
                    except:
                        pass
            if state.mdocs_glob:
                try:
                    additional_bind_paths.add(str(Path(state.mdocs_glob).parent.resolve()))
                except:
                    pass
            if state.acquisition.gain_reference_path:
                try:
                    additional_bind_paths.add(str(Path(state.acquisition.gain_reference_path).parent.resolve()))
                except:
                    pass
                
            # Add data source paths if available
            di = ui_mgr.data_import
            if di.movies_glob:
                try:
                    additional_bind_paths.add(str(Path(di.movies_glob).parent.resolve()))
                except:
                    pass
            if di.mdocs_glob:
                try:
                    additional_bind_paths.add(str(Path(di.mdocs_glob).parent.resolve()))
                except:
                    pass
            
            # STEP 1: Create/update the scheme with currently selected jobs
            print(f"[PIPELINE] Creating scheme '{scheme_name}' with jobs: {selected_job_strings}")
            
            scheme_result = await backend.pipeline_orchestrator.create_custom_scheme(
                project_dir           = project_path,
                new_scheme_name       = scheme_name,
                base_template_path    = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep",
                selected_jobs         = selected_job_strings,
                additional_bind_paths = list(additional_bind_paths),
            )
            
            if not scheme_result.get("success"):
                ui.notify(f"Failed to create scheme: {scheme_result.get('error')}", type="negative")
                return
            
            print(f"[PIPELINE] Scheme created at: {project_path}/Schemes/{scheme_name}")
            
            # STEP 2: Now start the pipeline
            result = await backend.start_pipeline(
                project_path=str(project_path),
                scheme_name=scheme_name,
                selected_jobs=selected_job_strings,
                required_paths=[],
            )
            
            if result.get("success"):
                ui_mgr.set_pipeline_running(True)
                
                # Reset statuses for jobs that haven't succeeded
                for job_type in ui_mgr.selected_jobs:
                    job_model = state.jobs.get(job_type)
                    if job_model and job_model.execution_status != JobStatus.SUCCEEDED:
                        job_model.execution_status = JobStatus.SCHEDULED
                
                # Start status polling
                ui_mgr.status_timer = ui.timer(
                    3.0,
                    lambda: asyncio.create_task(check_and_update_statuses())
                )
                
                await check_and_update_statuses()
                ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                
                if run_btn:
                    run_btn.props("disable")
                stop_btn = ui_mgr.panel_refs.stop_button
                if stop_btn:
                    stop_btn.props(remove="disable")
                
                rebuild_pipeline_ui()
            else:
                ui.notify(f"Failed to start: {result.get('error')}", type="negative")
        
        except Exception as e:
            import traceback
            traceback.print_exc()
            ui.notify(f"Error: {e}", type="negative")
        
        finally:
            if run_btn:
                run_btn.props(remove="loading")
    
    # ===========================================
    # Build the UI
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

    # Trigger initial rebuild to show "Create Project" or existing jobs
    rebuild_pipeline_ui()
