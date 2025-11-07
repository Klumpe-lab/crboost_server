# ui/pipeline_builder_panel.py (REFACTORED)
import asyncio
from datetime import datetime
import json
from pathlib import Path
from backend import CryoBoostBackend
from nicegui import ui
# NEW: Import JobStatus to use the enum
from services.parameter_models import JobType, jobtype_paramclass, JobStatus
from app_state import state as app_state, is_job_synced_with_global
from ui.utils import JobConfig, _snake_to_title
from typing import Dict, Any


def get_job_directory(job_type: JobType, job_index: int) -> str:
    """Gets the RELATIVE job directory path (e.g., External/job003)"""
    param_classes = jobtype_paramclass()
    param_class = param_classes.get(job_type)
    if not param_class:
        # Fallback for unknown job types
        return f"External/job{job_index:03d}"
    
    category = param_class.JOB_CATEGORY.value
    return f"{category}/job{job_index:03d}"


def build_pipeline_builder_panel(backend:CryoBoostBackend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    """Build the right panel for pipeline construction and monitoring"""

    panel_state = {
        "pipeline_container": None,
        "job_tabs_container": None,
        "run_button": None,
        "stop_button": None,
        "status_label": None,
        "status_timer": None, 
    }

    if "active_job_tab" not in shared_state:
        shared_state["active_job_tab"] = None
    
    if "job_cards" not in shared_state:
        shared_state["job_cards"] = {}


    def add_job_to_pipeline(job_type: JobType):
        """Add a job to the selected pipeline"""
        if job_type in shared_state["selected_jobs"]:
            return
        
        shared_state["selected_jobs"].append(job_type)
        shared_state["selected_jobs"].sort(key=lambda j: JobConfig.PIPELINE_ORDER.index(j))

        # FIX: Ensure job model is properly created in app state
        if job_type.value not in app_state.jobs:
            try:
                from app_state import prepare_job_params
                job_model = prepare_job_params(job_type.value)
                if not job_model:
                    print(f"[WARN] Failed to create job model for {job_type.value}")
                    # Create a default model as fallback
                    param_classes = jobtype_paramclass()
                    param_class = param_classes.get(job_type)
                    if param_class:
                        job_model = param_class.from_pipeline_state(app_state)
                        app_state.jobs[job_type.value] = job_model
                        print(f"[UI] Created default job model for {job_type.value}")
            except Exception as e:
                print(f"[ERROR] Failed to prepare job params for {job_type.value}: {e}")
                import traceback
                traceback.print_exc()
                # Create fallback model
                param_classes = jobtype_paramclass()
                param_class = param_classes.get(job_type)
                if param_class:
                    job_model = param_class.from_pipeline_state(app_state)
                    app_state.jobs[job_type.value] = job_model
                    print(f"[UI] Created fallback job model for {job_type.value}")

        # NEW: Default job card state. Status is SCHEDULED (your "yellow")
        if job_type not in shared_state["job_cards"]:
            shared_state["job_cards"][job_type] = {
                "status": JobStatus.SCHEDULED, # Default status
                "job_name": None,        # Will be filled by status checker
                "job_number": 0,       # Will be filled by status checker
                "is_synced": True,
                "active_monitor_tab": "config", # Default sub-tab
            }

        shared_state["active_job_tab"] = job_type.value
        rebuild_pipeline_ui()
        

    def toggle_job_in_pipeline(job_type: JobType):
        """Toggle a job in/out of the pipeline"""
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            remove_job_from_pipeline(job_type)
        else:
            add_job_to_pipeline(job_type)
        
        update_job_tag_button(job_type)


    def remove_job_from_pipeline(job_type: JobType):
        """Remove a job from the selected pipeline"""
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            shared_state["selected_jobs"].remove(job_type)
            if job_type in shared_state["job_cards"]:
                # Clean up the job card state
                job_state = shared_state["job_cards"][job_type]
                if job_state.get("logs_timer"):
                    job_state["logs_timer"].cancel()
                del shared_state["job_cards"][job_type]

            if shared_state["active_job_tab"] == job_type.value:
                shared_state["active_job_tab"] = (
                    shared_state["selected_jobs"][0].value if shared_state["selected_jobs"] else None
                )
            
            rebuild_pipeline_ui()

    
    def stop_all_timers():
        """Stop all timers to prevent errors on UI clear."""
        print("[UI] Stopping all timers...")
        if panel_state.get("status_timer"):
            panel_state["status_timer"].cancel()
            panel_state["status_timer"] = None
        
        for job_type in shared_state.get("job_cards", {}):
            job_state = shared_state["job_cards"][job_type]
            if job_state.get("logs_timer"):
                job_state["logs_timer"].cancel()
                job_state["logs_timer"] = None


    def rebuild_pipeline_ui():
            """Rebuild the entire pipeline UI"""
            update_status_label()

            if "job_tags_container" in shared_state:
                # Hide job tags when running or in continuation mode
                should_hide = shared_state["pipeline_running"] or shared_state.get("continuation_mode", False)
                shared_state["job_tags_container"].set_visibility(not should_hide)

            container = panel_state["job_tabs_container"]
            if not container:
                return

            # Stop ALL timers before clearing their parent container
            stop_all_timers() # This function is still necessary
            container.clear()

            if not shared_state["selected_jobs"]:
                with container:
                    ui.label("Select jobs from the tags above to build your pipeline").classes(
                        "text-xs text-gray-500 italic text-center p-8"
                    )
                return

            if shared_state["active_job_tab"] not in [j.value for j in shared_state["selected_jobs"]]:
                shared_state["active_job_tab"] = shared_state["selected_jobs"][0].value

            with container:
                # This is the new unified tab builder
                build_unified_job_tabs()
            
            # --- FIX: REMOVED TIMER CREATION FROM HERE ---
            # The main status timer is now managed by handle_run_pipeline
            # and the log timers are managed by render_logs_tab

    async def check_and_update_statuses():
        """Check job statuses using the new backend overview method."""
        
        if not shared_state["pipeline_running"]:
            if panel_state.get("status_timer"):
                print("[UI-STATUS] Pipeline stopped, deactivating timer.")
                panel_state["status_timer"].deactivate()
            return
        
        project_path = shared_state["current_project_path"]
        if not project_path:
            return

        print(f"[UI-STATUS] Checking statuses for project: {project_path}")
        overview = await backend.get_pipeline_overview(project_path)
        
        if overview["status"] != "ok":
            print(f"[UI-STATUS] Error getting overview: {overview.get('message')}")
            return
            
        pipeline_jobs = overview.get("jobs", {}) 
        any_changed = False

        # FIX: Handle job models instead of dictionaries
        for job_type_str, job_model in pipeline_jobs.items():
            try:
                job_type = JobType(job_type_str)
            except ValueError:
                print(f"[UI-STATUS] Unknown job type '{job_type_str}' in pipeline.star. Skipping.")
                continue

            if job_type in shared_state["job_cards"]:
                card_data = shared_state["job_cards"][job_type]
                
                # FIX: Access properties from job model
                new_status = job_model.execution_status
                new_job_name = job_model.relion_job_name
                new_job_number = job_model.relion_job_number
                
                status_changed = card_data.get("status") != new_status
                name_changed = card_data.get("job_name") != new_job_name
                number_changed = card_data.get("job_number") != new_job_number
                
                if status_changed or name_changed or number_changed:
                    print(f"[UI-STATUS] {job_type.value}: status={card_data.get('status')}->{new_status}, name={card_data.get('job_name')}->{new_job_name}, number={card_data.get('job_number')}->{new_job_number}")
                    card_data["status"] = new_status
                    card_data["job_name"] = new_job_name  # This is needed for logs/files tabs!
                    card_data["job_number"] = new_job_number  # This is needed for logs/files tabs!
                    any_changed = True

        # Handle jobs in UI but not yet in pipeline.star (e.g., scheduled)
        for job_type in shared_state["selected_jobs"]:
            if job_type.value not in pipeline_jobs:
                card_data = shared_state["job_cards"][job_type]
                if card_data.get("status") != JobStatus.SCHEDULED:
                    print(f"[UI-STATUS] {job_type.value}: {card_data.get('status')} -> {JobStatus.SCHEDULED} (Not in pipeline.star)")
                    card_data["status"] = JobStatus.SCHEDULED
                    # FIX: Clear job_name/job_number for scheduled jobs
                    card_data["job_name"] = None
                    card_data["job_number"] = 0
                    any_changed = True

        if any_changed:
            print("[UI-STATUS] Status change detected. Applying surgical updates.")
            # Now, update the UI elements directly
            for job_type in shared_state["selected_jobs"]:
                card_data = shared_state["job_cards"][job_type]
                
                # Update status dot
                dot = card_data.get("ui_status_dot")
                if dot:
                    dot.style(f"background: {get_status_indicator_color(job_type)};")
                
                # Update status label *if it's the active tab*
                if job_type.value == shared_state["active_job_tab"]:
                    label = card_data.get("ui_status_label")
                    if label:
                        status_text = card_data['status'].value if isinstance(card_data['status'], JobStatus) else str(card_data['status'])
                        label.set_text(f"Status: {status_text}")

            # If the status of the *active* tab changed, we must
            # rebuild its content (e.g., to freeze params, start/stop log timer)
            active_tab_changed = shared_state["active_job_tab"] in [j.value for j in shared_state["selected_jobs"] if shared_state["job_cards"][j].get("status_changed", False)]
            if active_tab_changed:
                print(f"[UI-STATUS] Active tab '{shared_state['active_job_tab']}' changed status. Rebuilding it.")
                # This is a "soft" rebuild of just the content
                try:
                    active_job_type = JobType(shared_state['active_job_tab'])
                    card_data = shared_state["job_cards"][active_job_type]
                    switch_monitor_tab(active_job_type, card_data.get("active_monitor_tab", "config"))
                except Exception as e:
                    print(f"[UI-STATUS] Error rebuilding active tab: {e}")
            
            # Clear the changed flags
            for job_type in shared_state["selected_jobs"]:
                if "status_changed" in shared_state["job_cards"][job_type]:
                    del shared_state["job_cards"][job_type]["status_changed"]
                    
        else:
            print(f"[UI-STATUS] No status changes detected")


    def update_status_label():
        """Update the status label showing job count"""
        if panel_state["status_label"]:
            count = len(shared_state["selected_jobs"])
            if count == 0:
                panel_state["status_label"].set_text("No jobs selected")
            elif shared_state["pipeline_running"]:
                # You could get more granular here using the overview if needed
                panel_state["status_label"].set_text(f"Pipeline running...")
            else:
                panel_state["status_label"].set_text(f"{count} jobs · Ready to run")

    
    def get_status_indicator_color(job_type: JobType) -> str:
        """Get the color for the status dot based on JobStatus enum."""
        job_state = shared_state["job_cards"].get(job_type)
        if not job_state:
            return "#6b7280" # Gray
            
        status = job_state.get("status", JobStatus.SCHEDULED)
        
        # Handle both enum and string
        if isinstance(status, str):
            try:
                status = JobStatus(status)
            except ValueError:
                status = JobStatus.UNKNOWN
        
        # Your requested colors:
        colors = {
            JobStatus.SCHEDULED: "#fbbf24", # Yellow
            JobStatus.RUNNING: "#3b82f6",   # Blue
            JobStatus.SUCCEEDED: "#10b981", # Green
            JobStatus.FAILED: "#ef4444",    # Red
            JobStatus.UNKNOWN: "#6b7280",   # Gray
        }
        
        return colors.get(status, "#6b7280") # Default to gray


    def build_unified_job_tabs():
        """
        Builds the new unified tabs.
        Each tab contains sub-tabs for "Config", "Logs", and "Files".
        The "Config" tab is disabled based on job status.
        """
        def switch_tab(job_type: JobType):
            shared_state["active_job_tab"] = job_type.value
            rebuild_pipeline_ui() # Rebuild to show the correct active tab

        # --- Main Job Tabs (horizontal) ---
        with ui.row().classes("w-full border-b").style("gap: 0;"):
            for job_type in shared_state["selected_jobs"]:
                name = JobConfig.get_job_display_name(job_type)
                is_active = shared_state["active_job_tab"] == job_type.value

                with (
                    ui.button(on_click=lambda j=job_type: switch_tab(j))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 8px 20px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#6b7280'}; "
                        # Use the status color for the border
                        f"border-top: 2px solid {get_status_indicator_color(job_type) if is_active else 'transparent'}; "
                        f"border-left: 1px solid #e5e7eb; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-weight: {500 if is_active else 400};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        status_dot = ui.element("div").style(
                            f"width: 6px; height: 6px; border-radius: 50%; "
                            f"background: {get_status_indicator_color(job_type)};"
                        )
                        if job_type in shared_state["job_cards"]:
                            shared_state["job_cards"][job_type]["ui_status_dot"] = status_dot

        # --- Tab Content Area ---
        with ui.column().classes("w-full"):
            for job_type in shared_state["selected_jobs"]:
                if shared_state["active_job_tab"] == job_type.value:
                    build_unified_tab_content(job_type)

    def build_unified_tab_content(job_type: JobType):
        job_state = shared_state["job_cards"].get(job_type)
        if not job_state:
            ui.label(f"Error: Job state for {job_type.value} not found.")
            return

        job_model = app_state.jobs.get(job_type.value)
        if not job_model:
            # FIX: Try to create the job model on the fly
            print(f"[UI] Job model for {job_type.value} not found, creating it...")
            try:
                from app_state import prepare_job_params
                job_model = prepare_job_params(job_type.value)
                if not job_model:
                    # Create fallback
                    param_classes = jobtype_paramclass()
                    param_class = param_classes.get(job_type)
                    if param_class:
                        job_model = param_class.from_pipeline_state(app_state)
                        app_state.jobs[job_type.value] = job_model
                        print(f"[UI] Created fallback job model for {job_type.value}")
                    else:
                        ui.label(f"Error: Unknown job type {job_type.value}").classes("text-xs text-red-600")
                        return
            except Exception as e:
                print(f"[UI] Error creating job model for {job_type.value}: {e}")
                ui.label(f"Error: Failed to create job model for {job_type.value}").classes("text-xs text-red-600")
                return

        job_status = job_state.get("status", JobStatus.SCHEDULED)
        # FIX: Handle both enum and string
        if isinstance(job_status, str):
            try:
                job_status = JobStatus(job_status)
            except ValueError:
                job_status = JobStatus.UNKNOWN
        
        is_frozen = job_status not in [JobStatus.SCHEDULED]
        active_sub_tab = job_state.get("active_monitor_tab", "config")
        if is_frozen and active_sub_tab == "config":
            active_sub_tab = "logs"
            job_state["active_monitor_tab"] = "logs"

        # ... rest of the function remains the same

        tab_buttons = {}
        with ui.row().classes("w-full p-4 pb-0 items-center").style("gap: 8px;"):
            for tab_name, tab_label in [("config", "Parameters"), ("logs", "Logs"), ("files", "Files")]:
                is_active = active_sub_tab == tab_name
                btn = (
                    ui.button(tab_label, on_click=lambda t=tab_name, j=job_type: switch_monitor_tab(j, t))
                    .props("dense flat no-caps")
                    .style(
                        f"padding: 6px 12px; border-radius: 3px; font-weight: 500; font-size: 11px; "
                        f"background: {'#3b82f6' if is_active else '#f3f4f6'}; "
                        f"color: {'white' if is_active else '#1f2937'}; "
                        f"border: 1px solid {'#3b82f6' if is_active else '#e5e7eb'};"
                    )
                )
                tab_buttons[tab_name] = btn

            job_state["tab_buttons"] = tab_buttons
            # FIX: Use the properly typed job_status
            status_text = job_status.value if isinstance(job_status, JobStatus) else str(job_status)
            status_label = ui.label(f"Status: {status_text}").classes("text-xs font-medium text-gray-600 ml-auto")
            job_state["ui_status_label"] = status_label
            ui.button("Refresh", icon="refresh",
                            on_click=force_status_refresh) \
                        .props("dense flat no-caps") \
                        .style("background: #f3f4f6; color: #1f2937; padding: 4px 12px; border-radius: 3px; font-size: 11px;")

        # ... rest of function

        content_container = ui.column().classes("w-full p-4")
        job_state["monitor_content_container"] = content_container 

        with content_container:
            if active_sub_tab == "config":
                render_config_tab(job_type, job_model, is_frozen)
            elif active_sub_tab == "logs":
                render_logs_tab(job_type)
            elif active_sub_tab == "files":
                render_files_tab(job_type)


    def render_config_tab(job_type: JobType, job_model, is_frozen: bool):
        """Renders the 'Parameters' sub-tab."""
        job_state = shared_state["job_cards"][job_type]
        
        # FIX: Validate job_model
        if job_model is None:
            ui.label(f"Error: Job model for {job_type.value} is None").classes("text-xs text-red-600")
            return
        
        if is_frozen:
            status_color_hex = get_status_indicator_color(job_type)
            icon_name = "check_circle" # Success
            if job_state["status"] == JobStatus.FAILED: icon_name = "error"
            if job_state["status"] == JobStatus.RUNNING: icon_name = "sync" # Running
            
            # FIX: Handle both enum and string status
            status_value = job_state["status"]
            if isinstance(status_value, JobStatus):
                status_text = status_value.value
            else:
                status_text = str(status_value)
            
            with ui.row().classes("w-full items-center mb-3 p-2").style(
                f"background: #fafafa; border-left: 3px solid {status_color_hex}; border-radius: 3px;"
            ):
                ui.icon(icon_name).style(f"color: {status_color_hex};")
                ui.label(
                    f"Job status is {status_text}. Parameters are frozen."
                ).classes("text-xs text-gray-700")

        # --- Inputs & Outputs Section ---
        ui.label("Inputs & Outputs").classes("text-xs font-semibold text-black mb-2")
        with ui.column().classes("w-full mb-4 p-3").style("background: #fafafa; border-radius: 3px; gap: 8px;"):
            # (This section is unchanged and correct)
            if shared_state.get("project_created"):
                paths_data = shared_state.get("params_snapshot", {}).get(job_type.value, {}).get("paths", {})
                if paths_data:
                    for key, value in paths_data.items():
                        with ui.row().classes("w-full items-start").style("gap: 8px;"):
                            ui.label(f"{_snake_to_title(key)}:").classes("text-xs font-medium text-gray-600").style(
                                "min-width: 140px;"
                            )
                            ui.label(str(value)).classes("text-xs text-gray-800 font-mono flex-1")
                else:
                    ui.label("Paths not yet calculated or found in snapshot.").classes("text-xs text-gray-500 italic")
            else:
                ui.label("Paths will be generated when project is created").classes("text-xs text-gray-500 italic")

        # --- Parameters Section ---
        ui.label("Parameters").classes("text-xs font-semibold text-black mb-2")
        param_updaters = {}
        
        # FIX: Validate job_model has model_dump method
        try:
            job_params = job_model.model_dump()
        except AttributeError as e:
            ui.label(f"Error: Job model for {job_type.value} is invalid - {e}").classes("text-xs text-red-600")
            return
            
        with ui.grid(columns=3).classes("w-full").style("gap: 10px;"):
            for param_name, value in job_params.items():
                label = _snake_to_title(param_name)
                element = None

                if isinstance(value, bool):
                    element = ui.checkbox(label, value=value).props("dense")
                    if not is_frozen:
                        element.bind_value(job_model, param_name)
                        element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))
                    else:
                        element.disable()

                elif isinstance(value, (int, float)):
                    validation_rules = {
                        "Enter valid number": lambda v: v == ""
                        or v.replace(".", "", 1).replace("-", "", 1).isdigit()
                    }
                    
                    element = ui.input(
                        label=label, 
                        value=str(value),
                        validation=validation_rules if not is_frozen else None,
                    ).props("outlined dense").classes("w-full")

                    element.enabled = not is_frozen

                    if is_frozen:
                        element.classes("bg-gray-50")
                    else:
                        current_display_value = str(value)
                        def create_binding(field_name, is_float=False, ui_element=element):
                            def model_to_ui():
                                nonlocal current_display_value
                                current_val = getattr(job_model, field_name)
                                new_display = str(current_val) if current_val is not None else ""
                                if new_display != current_display_value:
                                    ui_element.value = new_display
                                    current_display_value = new_display
                            def ui_to_model():
                                nonlocal current_display_value
                                try:
                                    val = ui_element.value.strip()
                                    parsed = (float(val) if is_float else int(float(val))) if val else 0
                                    if "do_at_most" in field_name and not val: parsed = -1
                                    setattr(job_model, field_name, parsed)
                                    current_display_value = str(parsed)
                                    update_job_card_sync_indicator(job_type)
                                except (ValueError, Exception):
                                    current = getattr(job_model, field_name, 0)
                                    ui_element.value = str(current)
                                    current_display_value = str(current)
                            ui_element.on("blur", ui_to_model)
                            return model_to_ui
                        
                        updater_fn = create_binding(param_name, isinstance(value, float))
                        param_updaters[param_name] = updater_fn

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        options = ["AreTomo", "IMOD", "Relion"]
                        element = ui.select(
                            label=label, 
                            options=options, 
                            value=value
                        ).props("outlined dense").classes("w-full")
                        
                        element.enabled = not is_frozen 

                    else:
                        element = ui.input(
                            label=label, 
                            value=value
                        ).props("outlined dense").classes("w-full")
                    
                        element.enabled = not is_frozen 
                    
                    if is_frozen:
                        element.classes("bg-gray-50")
                    else:
                        if isinstance(element, ui.input):
                            element.bind_value(job_model, param_name)
                            element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))
                        elif isinstance(element, ui.select):
                            element.bind_value(job_model, param_name)
                            element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))
        
        job_state["param_updaters"] = param_updaters

    async def force_status_refresh():
        """Force a status refresh and UI update"""
        ui.notify("Refreshing statuses...", timeout=1)
        await check_and_update_statuses()


    def switch_monitor_tab(job_type: JobType, tab_name: str):
        """Switch to a different sub-tab (Config, Logs, Files)"""
        job_state = shared_state["job_cards"][job_type]
        job_state["active_monitor_tab"] = tab_name
        
        # Just redraw the content for the active tab
        # This is more efficient than rebuilding the whole UI
        
        # Stop any log timers if we're navigating away from the logs tab
        if tab_name != "logs" and job_state.get("logs_timer"):
            job_state["logs_timer"].cancel()
            job_state["logs_timer"] = None
            
        # Find the content container and re-render it
        container = job_state.get("monitor_content_container")
        if container:
            job_model = app_state.jobs.get(job_type.value)
            job_status = job_state.get("status", JobStatus.SCHEDULED)
            is_frozen = job_status not in [JobStatus.SCHEDULED]

            container.clear()
            with container:
                if tab_name == "config":
                    render_config_tab(job_type, job_model, is_frozen)
                elif tab_name == "logs":
                    render_logs_tab(job_type)
                elif tab_name == "files":
                    render_files_tab(job_type)
        else:
            # Fallback if container not found (e.g., during initial build)
            rebuild_pipeline_ui()


    def update_job_tag_button(job_type: JobType):
        """Update the visual state of a job tag button"""
        btn = shared_state.get("job_buttons", {}).get(job_type)
        if not btn:
            return

        is_selected = job_type in shared_state["selected_jobs"]
        btn.style(
            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
        )


    def render_logs_tab(job_type: JobType):
        """Render logs sub-tab content"""
        job_state = shared_state["job_cards"][job_type]
        
        # Debug logging
        print(f"[UI-LOGS] Rendering logs tab for {job_type.value}")
        print(f"[UI-LOGS] job_name: {job_state.get('job_name')}")
        print(f"[UI-LOGS] job_number: {job_state.get('job_number')}")
        print(f"[UI-LOGS] status: {job_state.get('status')}")
        
        # Stop any previous log timer for this tab
        if job_state.get("logs_timer"):
            job_state["logs_timer"].cancel()
            job_state["logs_timer"] = None

        job_name = job_state.get("job_name") # e.g., "External/job003/"
        if not job_name:
            ui.label("Job has not run yet. Logs will appear here once it starts.").classes("text-xs text-gray-500 italic")
            return

        with ui.grid(columns=2).classes("w-full").style("gap: 10px; height: calc(100vh - 450px); min-height: 400px;"):
            with ui.column().classes("w-full h-full"):
                ui.label("stdout").classes("text-xs font-medium mb-1")
                stdout_log = (
                    ui.log(max_lines=500)
                    .classes("w-full h-full border rounded bg-gray-50 p-2 text-xs font-mono")
                    .style("font-family: 'IBM Plex Mono', monospace;")
                )
            with ui.column().classes("w-full h-full"):
                ui.label("stderr").classes("text-xs font-medium mb-1")
                stderr_log = (
                    ui.log(max_lines=500)
                    .classes("w-full h-full border rounded bg-red-50 p-2 text-xs font-mono")
                    .style("font-family: 'IBM Plex Mono', monospace;")
                )

        job_state["monitor_logs"] = {"stdout": stdout_log, "stderr": stderr_log}
        
        # Load logs immediately
        asyncio.create_task(refresh_job_logs(job_type))
        
        # Start a new timer *only* if the job is running
        if job_state.get("status") == JobStatus.RUNNING:
            print(f"[UI-LOGS] Job {job_type.value} is running. Starting log timer.")
            job_state["logs_timer"] = ui.timer(
                5.0, # Poll logs every 5 seconds
                lambda j=job_type: asyncio.create_task(refresh_job_logs(j)),
                active=True
            )

    def render_files_tab(job_type: JobType):
        """Render files sub-tab content - full file browser"""
        job_state = shared_state["job_cards"][job_type]
        project_path = shared_state.get("current_project_path")
        job_name = job_state.get("job_name") # e.g., "External/job003/"
        
        # Debug logging
        print(f"[UI-FILES] Rendering files tab for {job_type.value}")
        print(f"[UI-FILES] project_path: {project_path}")
        print(f"[UI-FILES] job_name: {job_name}")
        print(f"[UI-FILES] status: {job_state.get('status')}")
        
        if not project_path or not job_name:
            ui.label("Job has not run yet. Files will appear here once it starts.").classes("text-xs text-gray-500 italic")
            return
        
        job_dir = Path(project_path) / job_name.rstrip("/")
        print(f"[UI-FILES] job_dir: {job_dir}")
        print(f"[UI-FILES] job_dir exists: {job_dir.exists()}")

        ui.label("Job Directory Browser").classes("text-xs font-semibold text-black mb-2")
        current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono mb-2")
        file_list_container = (
            ui.column()
            .classes("w-full border rounded p-2 bg-gray-50")
            .style("height: calc(100vh - 450px); min-height: 400px; overflow-y: auto;")
        )

        # ... rest of the files tab code

        def view_file(file_path: Path):
            """Show file content in a dialog"""
            # (This function is unchanged from your original)
            try:
                text_extensions = [
                    ".script", ".txt", ".xml", ".settings", ".log", ".star",
                    ".json", ".yaml", ".sh", ".py", ".out", ".err", ".md",
                    ".tlt", ".aln", "",
                ]
                if file_path.suffix.lower() in text_extensions:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read(50000) # Limit size
                else:
                    content = "Cannot preview binary file."

                with ui.dialog() as dialog, ui.card().classes("w-[60rem] max-w-full"):
                    ui.label(file_path.name).classes("text-sm font-medium mb-2")
                    ui.code(content).classes("w-full max-h-96 overflow-auto text-xs")
                    ui.button("Close", on_click=dialog.close).props("flat")
                dialog.open()
            except Exception as e:
                ui.notify(f"Error reading file: {e}", type="negative")

        def browse_directory(path: Path):
            """Browse directory"""
            # (This function is unchanged from your original)
            file_list_container.clear()
            current_path_label.set_text(str(path))
            try:
                if not path.exists():
                    with file_list_container:
                        ui.label("Directory not yet created").classes("text-xs text-gray-500 italic p-4")
                        ui.button("Check again", icon="refresh", on_click=lambda: browse_directory(path)).props(
                            "dense flat no-caps"
                        ).style(
                            "background: #f3f4f6; color: #1f2937; padding: 4px 12px; border-radius: 3px; font-size: 11px; margin-top: 8px;"
                        )
                    return

                with file_list_container:
                    # Add "Up" button if not in the root job_dir
                    if path != job_dir and path.parent.exists() and job_dir in path.parents:
                        with (
                            ui.row()
                            .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded w-full")
                            .on("click", lambda p=path.parent: browse_directory(p))
                        ):
                            ui.icon("folder_open").classes("text-sm")
                            ui.label("..").classes("text-xs")

                    items = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name))
                    if not items:
                        ui.label("Directory is empty.").classes("text-xs text-gray-500 italic")

                    for item in items:
                        if item.is_dir():
                            with (
                                ui.row()
                                .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded w-full")
                                .on("click", lambda i=item: browse_directory(i))
                            ):
                                ui.icon("folder").classes("text-sm text-blue-600")
                                ui.label(item.name).classes("text-xs")
                        else:
                            with ui.row().classes("items-center gap-2 w-full"):
                                with (
                                    ui.row()
                                    .classes(
                                        "items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded flex-grow"
                                    )
                                    .on("click", lambda i=item: view_file(i))
                                ):
                                    ui.icon("insert_drive_file").classes("text-sm text-gray-600")
                                    ui.label(item.name).classes("text-xs")
                                size_kb = item.stat().st_size // 1024
                                ui.label(f"{size_kb} KB").classes("text-xs text-gray-500 ml-auto")
            except Exception as e:
                with file_list_container:
                    ui.label(f"Error listing directory: {e}").classes("text-xs text-red-600")
        
        browse_directory(job_dir)


    async def refresh_job_logs(job_type: JobType):
        """Refresh logs for a job using the new backend method."""
        card_data = shared_state["job_cards"].get(job_type)
        if not card_data:
            return

        job_name = card_data.get("job_name")
        if not job_name:
            # Job hasn't started, so no job_name yet. Nothing to refresh.
            return

        monitor = card_data.get("monitor_logs")
        if not monitor or monitor["stdout"].is_deleted:
            print(f"[UI-LOGS] Monitor for {job_type.value} is missing or deleted. Stopping timer.")
            if card_data.get("logs_timer"):
                card_data["logs_timer"].cancel()
                card_data["logs_timer"] = None
            return

        print(f"[UI-LOGS] Refreshing logs for {job_name}")
        
        logs = await backend.get_job_logs(
            shared_state["current_project_path"], 
            job_name
        )
        
        stdout_content = logs.get("stdout", "No output")
        if not stdout_content.strip():
            stdout_content = "No output yet"
        monitor["stdout"].clear()
        monitor["stdout"].push(stdout_content)
        
        stderr_content = logs.get("stderr", "No errors")
        if not stderr_content.strip():
            stderr_content = "No errors yet"
        monitor["stderr"].clear()
        monitor["stderr"].push(stderr_content)


    def update_job_card_sync_indicator(job_type: JobType):
        """Update sync indicator"""
        if job_type not in shared_state["job_cards"]:
            return
        is_synced = is_job_synced_with_global(job_type)
        shared_state["job_cards"][job_type]["is_synced"] = is_synced


    async def handle_run_pipeline():
            """Handle run button click"""
            if not shared_state["project_created"]:
                ui.notify("Create a project first", type="warning")
                return
            
            panel_state["run_button"].props("loading")
            
            result = await backend.start_pipeline(
                project_path=shared_state["current_project_path"],
                scheme_name=shared_state["current_scheme_name"],
                selected_jobs=[j.value for j in shared_state["selected_jobs"]],
                required_paths=[], # This seems to be handled by the backend anyway
            )
            
            panel_state["run_button"].props(remove="loading")
            
            if result.get("success"):
                shared_state["pipeline_running"] = True
                
                # Reset all non-successful job statuses to scheduled
                # This ensures that on a re-run, failed jobs are tried again
                for job_type in shared_state["selected_jobs"]:
                    if job_type in shared_state["job_cards"]:
                        if shared_state["job_cards"][job_type]["status"] != JobStatus.SUCCEEDED:
                            shared_state["job_cards"][job_type]["status"] = JobStatus.SCHEDULED
                
                # --- FIX: Start the main status timer HERE ---
                if panel_state.get("status_timer"):
                    print("[UI] Activating existing main status timer.")
                    panel_state["status_timer"].activate()
                else:
                    print("[UI] Creating new main status timer.")
                    panel_state["status_timer"] = ui.timer(
                        5.0, # Poll every 5 seconds
                        lambda: asyncio.create_task(check_and_update_statuses()), 
                        active=True
                    )
                # --- END FIX ---

                await check_and_update_statuses() # Do an initial check
                
                ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                panel_state["run_button"].props("disable")
                panel_state["stop_button"].props(remove="disable")
                
                # This rebuilds the UI once to "freeze" it
                rebuild_pipeline_ui() 
            else:
                ui.notify(f"Failed: {result.get('error')}", type="negative")

    
    # --- Main Panel Layout (This part is mostly unchanged) ---
    with (
        ui.column()
        .classes("w-full h-full overflow-y-auto")
        .style("padding: 10px; gap: 0px; font-family: 'IBM Plex Sans', sans-serif;")
    ):
        with ui.row().classes("w-full items-center justify-between mb-4").style("gap: 12px;"):
            job_tags_container = ui.row().classes("flex-1 flex-wrap").style("gap: 8px;")
            shared_state["job_tags_container"] = job_tags_container

            with job_tags_container:
                for job_type in JobConfig.get_ordered_jobs():
                    name = JobConfig.get_job_display_name(job_type)
                    is_selected = job_type in shared_state["selected_jobs"]
                    btn = (
                        ui.button(name, on_click=lambda j=job_type: toggle_job_in_pipeline(j))
                        .props("no-caps dense flat")
                        .style(
                            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
                            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
                            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
                            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
                        )
                    )
                    shared_state.setdefault("job_buttons", {})[job_type] = btn

            with ui.row().classes("items-center").style("gap: 10px;"):
                panel_state["status_label"] = ui.label("No jobs selected").classes("text-xs text-gray-600")
                panel_state["run_button"] = (
                    ui.button("Run Pipeline", icon="play_arrow", on_click=handle_run_pipeline)
                    .props("dense flat no-caps")
                    .style(
                        "background: #f3f4f6; color: #1f2937; padding: 6px 20px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                    )
                )
                panel_state["stop_button"] = (
                    ui.button("Stop", icon="stop") # TODO: Implement stop logic
                    .props("dense flat no-caps disable")
                    .style(
                        "background: #f3f4f6; color: #1f2937; padding: 6px 20px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                    )
                )

        panel_state["job_tabs_container"] = ui.column().classes("w-full")

        # Initial placeholder
        with panel_state["job_tabs_container"]:
            ui.label("Select jobs from the tags above to build your pipeline").classes(
                "text-xs text-gray-500 italic text-center p-8"
            )

    # Expose callbacks to main_ui.py
    callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
    callbacks["update_job_card_sync_indicator"] = update_job_card_sync_indicator
    callbacks["stop_all_timers"] = stop_all_timers # NEW: Expose timer stop
    def enable_run_button():
        if panel_state["run_button"]:
            # The Quasar prop is 'disable', not 'disabled'
            panel_state["run_button"].props(remove="disable")

    callbacks["enable_run_button"] = enable_run_button

    return panel_state
