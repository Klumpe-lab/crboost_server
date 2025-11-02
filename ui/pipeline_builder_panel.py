# ui/pipeline_builder_panel.py (UPDATED)
import asyncio
import json
from pathlib import Path
from nicegui import ui
from services.parameter_models import JobType
from app_state import state as app_state, sync_job_with_global, is_job_synced_with_global
from ui.utils import JobConfig, _snake_to_title
from typing import Dict, Any


# +++ HELPER FUNCTION FROM OLD FILE +++
def get_job_directory(job_type: JobType, job_index: int) -> str:
    """Get the job directory name based on type"""
    if job_type == JobType.IMPORT_MOVIES:
        return f"Import/job{job_index:03d}"
    else:
        # This logic may need to be more specific if other jobs have unique paths
        job_name = job_type.value.capitalize()
        return f"{job_name}/job{job_index:03d}"


def build_pipeline_builder_panel(backend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    """Build the right panel for pipeline construction and monitoring"""

    panel_state = {
        "pipeline_container": None  # This will hold the tabbed interface
    }

    # Ensure 'active_job_tab' is initialized
    if "active_job_tab" not in shared_state:
        shared_state["active_job_tab"] = None

    def add_job_to_pipeline(job_type: JobType):
        """Add a job to the selected pipeline"""
        if job_type in shared_state["selected_jobs"]:
            return

        shared_state["selected_jobs"].append(job_type)
        shared_state["selected_jobs"].sort(key=lambda j: JobConfig.PIPELINE_ORDER.index(j))

        # Try to prepare job params, but don't fail if not implemented
        if job_type.value not in app_state.jobs:
            try:
                from app_state import prepare_job_params
                prepare_job_params(job_type.value)
            except Exception as e:
                print(f"[WARN] Job {job_type.value} not implemented yet: {e}")

        # Always initialize job_card state, even for unimplemented jobs
        if job_type not in shared_state["job_cards"]:
            shared_state["job_cards"][job_type] = {
                "job_index": len(shared_state["selected_jobs"]),
                "status": "pending",
                "active_monitor_tab": "logs",
                "is_synced": True,  # Default to synced for unimplemented jobs
            }

        shared_state["active_job_tab"] = job_type.value
        rebuild_pipeline_ui()

    def toggle_job_in_pipeline(job_type: JobType):
        """Toggle a job in/out of the pipeline"""
        if job_type in shared_state["selected_jobs"]:
            remove_job_from_pipeline(job_type)
        else:
            add_job_to_pipeline(job_type)
        
        update_job_button(job_type)


    def update_job_button(job_type: JobType):
        """Update the appearance of a job button based on selection state"""
        btn = shared_state.get("job_buttons", {}).get(job_type)
        if not btn:
            return
        
        name = JobConfig.get_job_display_name(job_type)
        is_selected = job_type in shared_state["selected_jobs"]
        
        if is_selected:
            btn.props("color=primary")
            btn.classes(remove="outline", add="")
            btn.set_text(f"− {name}")
        else:
            btn.props("color=grey")
            btn.classes(remove="", add="outline")
            btn.set_text(f"+ {name}")


    def remove_job_from_pipeline(job_type: JobType):
        """Remove a job from the selected pipeline"""
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            shared_state["selected_jobs"].remove(job_type)
            if job_type in shared_state["job_cards"]:
                del shared_state["job_cards"][job_type]

            # Update active tab if it was the one removed
            if shared_state["active_job_tab"] == job_type.value:
                shared_state["active_job_tab"] = (
                    shared_state["selected_jobs"][0].value if shared_state["selected_jobs"] else None
                )

            update_job_button(job_type)  # <-- ADD THIS LINE
            rebuild_pipeline_ui()

    def rebuild_pipeline_ui():
        """
        Rebuild the entire pipeline UI (config or monitoring tabs)
        based on the 'pipeline_running' state.
        """
        # Hide job selector when pipeline is running (DON'T recreate it!)
        job_selector_container = shared_state.get("job_selector_container")
        if job_selector_container:
            job_selector_container.set_visibility(not shared_state["pipeline_running"])
        
        container = panel_state["pipeline_container"]
        container.clear()
    

        if not shared_state["selected_jobs"]:
            with container:
                ui.label("No jobs selected").classes("text-xs text-gray-500 italic text-center p-4")
            return

        # Ensure active tab is valid
        if shared_state["active_job_tab"] not in [j.value for j in shared_state["selected_jobs"]]:
            shared_state["active_job_tab"] = shared_state["selected_jobs"][0].value

        with container:
            if not shared_state["pipeline_running"]:
                build_configuration_tabs()
            else:
                build_monitoring_tabs()






    def build_configuration_tabs():
        """Builds the PARAMETER EDITING tabs (before run)"""

        with ui.tabs().props("dense active-color=primary indicator-color=primary align=left") as tabs:
            tabs.bind_value(shared_state, "active_job_tab")
            for idx, job_type in enumerate(shared_state["selected_jobs"]):
                name = JobConfig.get_job_display_name(job_type)

                with ui.tab(name=job_type.value).props("no-caps"):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(f"{idx + 1}. {name}").classes("text-sm")

                        # Only show sync badge for implemented jobs
                        if job_type.value in app_state.jobs:
                            job_card_state = shared_state["job_cards"][job_type]

                            sync_badge = ui.badge("out of sync", color="orange").classes("text-xs")
                            sync_badge.bind_visibility_from(job_card_state, "is_synced", backward=lambda s: not s)
                            job_card_state["sync_badge"] = sync_badge

                            sync_button = (
                                ui.button(
                                    icon="sync",
                                    on_click=lambda e, j=job_type: (e.stopPropagation(), asyncio.create_task(confirm_sync_job(j))),
                                ).props("flat dense round size=xs").classes("text-blue-600")
                            )
                            sync_button.bind_visibility_from(job_card_state, "is_synced", backward=lambda s: not s)
                            job_card_state["sync_button"] = sync_button

                        # Remove button (always show before project creation)
                        if not shared_state["project_created"]:
                            ui.button(
                                icon="close",
                                on_click=lambda e, j=job_type: (e.stopPropagation(), remove_job_from_pipeline(j)),
                            ).props("flat dense round size=xs").classes("text-red-600 -mr-2")

        with ui.tab_panels().bind_value(shared_state, "active_job_tab").props("animated").classes("w-full bg-transparent"):
            for job_type in shared_state["selected_jobs"]:
                with ui.tab_panel(name=job_type.value).classes("p-0 pt-4"):
                    job_model = app_state.jobs.get(job_type.value)
                    if job_model:
                        build_parameters_section(job_type, job_model)
                    else:
                        # Show placeholder for unimplemented jobs
                        with ui.card().classes("w-full p-8 text-center"):
                            ui.label(f"{JobConfig.get_job_display_name(job_type)}").classes("text-lg font-semibold mb-2")
                            ui.label("This job is not yet implemented").classes("text-sm text-gray-500")

    def build_monitoring_tabs():
        """Builds the JOB MONITORING tabs (after run) - SIMPLIFIED"""

        with ui.tabs().props("dense active-color=primary indicator-color=primary align=left") as tabs:
            tabs.bind_value(shared_state, "active_job_tab")
            for idx, job_type in enumerate(shared_state["selected_jobs"]):
                name = JobConfig.get_job_display_name(job_type)

                with ui.tab(name=job_type.value).props("no-caps"):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(f"{idx + 1}. {name}").classes("text-sm")

        with (
            ui.tab_panels()
            .bind_value(shared_state, "active_job_tab")
            .props("animated")
            .classes("w-full bg-transparent")
        ):
            for idx, job_type in enumerate(shared_state["selected_jobs"]):
                with ui.tab_panel(name=job_type.value).classes("p-0 pt-4"):
                    # Build the monitoring section (Logs/Params/Files)
                    build_monitoring_section(job_type, idx + 1)

    def build_parameters_section(job_type: JobType, job_model):
        """Build inline parameters section with proper two-way binding"""
        param_updaters = {}

        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center mb-2"):
                ui.label("Parameters").classes("text-sm font-semibold flex-grow")

                # +++ FIX 3: BIND TO THE STATE DICTIONARY, NOT THE ENUM +++
                job_card_state = shared_state["job_cards"][job_type]

                # Sync button
                sync_button = (
                    ui.button(icon="sync", on_click=lambda j=job_type: asyncio.create_task(confirm_sync_job(j)))
                    .props("flat dense round size=sm")
                    .classes("text-blue-600")
                )
                sync_button.tooltip("Sync with global parameters")
                sync_button.bind_visibility_from(job_card_state, "is_synced", backward=lambda s: not s)
                job_card_state["sync_button"] = sync_button

            with ui.grid(columns=3).classes("gap-3 w-full"):
                for param_name, value in job_model.model_dump().items():
                    label = _snake_to_title(param_name)
                    element = None

                    if isinstance(value, bool):
                        element = ui.checkbox(label, value=value).props("dense")
                        element.bind_value(job_model, param_name)
                        # Also trigger sync check on change
                        element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))

                    elif isinstance(value, (int, float)):
                        element = (
                            ui.input(
                                label=label,
                                value=str(value),
                                validation={
                                    "Enter valid number": lambda v: v == ""
                                    or v.replace(".", "", 1).replace("-", "", 1).isdigit()
                                },
                            )
                            .props("dense outlined")
                            .classes("w-full")
                        )
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

                                    if "do_at_most" in field_name and not val:
                                        parsed = -1

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
                            element = (
                                ui.select(label=label, options=options, value=value)
                                .props("dense outlined")
                                .classes("w-full")
                            )
                        else:
                            element = ui.input(label=label, value=value).props("dense outlined").classes("w-full")
                        element.bind_value(job_model, param_name)
                        # Also trigger sync check on change
                        element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))

            # Store updaters
            job_card_state["param_updaters"] = param_updaters

    def build_monitoring_section(job_type: JobType, job_index: int):
        """Build monitoring section with simple button tabs"""

        job_state = shared_state["job_cards"][job_type]

        # Initialize active tab if not set
        if "active_monitor_tab" not in job_state:
            job_state["active_monitor_tab"] = "logs"

        with ui.card().classes("w-full"):
            # Simple button switcher
            with ui.row().classes("w-full gap-2 mb-3"):
                ui.button("Logs", on_click=lambda: switch_monitor_tab(job_type, "logs")).props("dense size=sm").classes(
                    "flex-1"
                )
                ui.button("Parameters", on_click=lambda: switch_monitor_tab(job_type, "params")).props(
                    "dense size=sm"
                ).classes("flex-1")
                ui.button("Files", on_click=lambda: switch_monitor_tab(job_type, "files")).props(
                    "dense size=sm"
                ).classes("flex-1")

            # Container for tab content
            content_container = ui.column().classes("w-full")
            job_state["monitor_content"] = content_container

            # Initial render
            render_monitor_tab(job_type, job_index)

    def switch_monitor_tab(job_type: JobType, tab_name: str):
        """Switch to a different monitor tab"""
        job_state = shared_state["job_cards"][job_type]
        job_state["active_monitor_tab"] = tab_name

        job_index = job_state["job_index"]
        render_monitor_tab(job_type, job_index)

    def render_monitor_tab(job_type: JobType, job_index: int):
        """Render the active monitor tab content"""
        job_state = shared_state["job_cards"][job_type]
        active_tab = job_state.get("active_monitor_tab", "logs")
        content = job_state["monitor_content"]

        content.clear()

        with content:
            if active_tab == "logs":
                render_logs_tab(job_type, job_index)
            elif active_tab == "params":
                render_params_tab(job_type)
            elif active_tab == "files":
                render_files_tab(job_type, job_index)

    def render_logs_tab(job_type: JobType, job_index: int):
        """Render logs tab content"""
        job_state = shared_state["job_cards"][job_type]

        with ui.row().classes("w-full justify-end mb-2"):
            ui.button(
                "Refresh", icon="refresh", on_click=lambda: asyncio.create_task(refresh_job_logs(job_type))
            ).props("dense size=sm outline")

        with ui.grid(columns=2).classes("w-full gap-3"):
            with ui.column().classes("w-full"):
                ui.label("stdout").classes("text-xs font-medium mb-1")
                stdout_log = ui.log(max_lines=200).classes(
                    "w-full h-48 border rounded bg-gray-50 p-2 text-xs font-mono"
                )
            with ui.column().classes("w-full"):
                ui.label("stderr").classes("text-xs font-medium mb-1")
                stderr_log = ui.log(max_lines=200).classes("w-full h-48 border rounded bg-red-50 p-2 text-xs font-mono")

        # Store log elements
        job_state["monitor"] = {"stdout": stdout_log, "stderr": stderr_log}
        # Auto-refresh logs once on load
        asyncio.create_task(refresh_job_logs(job_type, notify=False))

    def render_params_tab(job_type: JobType):
        """Render parameters tab content"""
        ui.label("Job Parameters (Snapshot)").classes("text-xs font-medium mb-2")
        ui.label("Parameters used when job was started:").classes("text-xs text-gray-600 mb-2")
        params_json = json.dumps(shared_state["params_snapshot"].get(job_type, {}), indent=2)
        ui.code(params_json, language="json").classes("w-full text-xs max-h-96 overflow-auto")

    def render_files_tab(job_type: JobType, job_index: int):
        """Render files tab content"""
        build_file_browser(job_type, job_index)

    def build_file_browser(job_type: JobType, job_index: int):
        """Build simple file browser (implementation copied from previous version)"""
        ui.label("Job Directory Browser").classes("text-xs font-medium mb-2")

        project_path = shared_state.get("current_project_path")
        if not project_path:
            ui.label("Project path not set.").classes("text-xs text-red-500")
            return

        job_dir_rel = get_job_directory(job_type, job_index)
        job_dir = Path(project_path) / job_dir_rel

        status_label = ui.label("Checking directory...").classes("text-xs text-gray-500 font-mono mb-1")
        current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono mb-2")
        file_list_container = ui.column().classes("w-full border rounded p-2 bg-gray-50 max-h-96 overflow-auto")

        def browse_directory(path: Path):
            """Browse directory synchronously"""
            file_list_container.clear()
            current_path_label.set_text(str(path))

            try:
                if not path.exists():
                    status_label.set_text(f"Directory not yet created: {path.name}")
                    with file_list_container:
                        ui.label("Directory will be created when job starts").classes("text-xs text-blue-600")
                        ui.button(
                            "Check again", icon="refresh", on_click=lambda: browse_directory(path)
                        ).props("outline dense size=sm mt-2")
                    return

                status_label.set_text("Directory found")

                with file_list_container:
                    # Parent directory
                    if path != job_dir and path.parent.exists():
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
                                    .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded flex-grow")
                                    .on("click", lambda i=item: view_file(i))
                                ):
                                    ui.icon("insert_drive_file").classes("text-sm text-gray-600")
                                    ui.label(item.name).classes("text-xs")
                                size_kb = item.stat().st_size // 1024
                                ui.label(f"{size_kb} KB").classes("text-xs text-gray-500 ml-auto")
            except Exception as e:
                with file_list_container:
                    ui.label(f"Error listing directory: {e}").classes("text-xs text-red-600")

        # Initial call (also synchronous now)
        browse_directory(job_dir)

        def view_file(file_path: Path):
            """Show file content in a dialog"""
            try:
                text_extensions = [
                    ".script", ".txt", ".log", ".star", ".json", ".yaml", 
                    ".sh", ".py", ".out", ".err", ".md", ".tlt", ".aln", ""
                ]
                
                if file_path.suffix.lower() in text_extensions:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read(50000)  # Limit to 50KB
                else:
                    content = "Cannot preview binary file."

                with ui.dialog() as dialog, ui.card().classes("w-[60rem] max-w-full"):
                    ui.label(file_path.name).classes("text-sm font-medium mb-2")
                    ui.code(content).classes("w-full max-h-96 overflow-auto text-xs")
                    ui.button("Close", on_click=dialog.close).props("flat")
                dialog.open()
                
            except Exception as e:
                ui.notify(f"Error reading file: {e}", type="negative")
    async def confirm_sync_job(job_type: JobType):
        """Show confirmation dialog before syncing"""
        with ui.dialog() as dialog, ui.card():
            ui.label(f"Sync {JobConfig.get_job_display_name(job_type)} with global parameters?").classes("text-sm")
            ui.label("This will overwrite job-specific parameter changes.").classes("text-xs text-gray-600")
            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                ui.button(
                    "Sync", on_click=lambda: (asyncio.create_task(sync_job_and_update_ui(job_type)), dialog.close())
                ).props("color=primary")
        dialog.open()

    async def sync_job_and_update_ui(job_type: JobType):
        """Wrapper to sync job and then force-update its UI elements"""
        await sync_job_with_global(job_type)

        # Force-call all UI updater functions
        card_data = shared_state["job_cards"].get(job_type, {})
        if "param_updaters" in card_data:
            for updater_fn in card_data["param_updaters"].values():
                updater_fn()  # This updates the UI input fields

        # Update the sync badge/button visibility
        update_job_card_sync_indicator(job_type)
        ui.notify(f"Synced {JobConfig.get_job_display_name(job_type)}", type="positive")

    async def refresh_job_logs(job_type: JobType, notify: bool = True):
        """Manually refresh logs for a job"""
        card_data = shared_state["job_cards"].get(job_type)
        if not card_data or "monitor" not in card_data or not card_data["monitor"]:
            if notify:
                print(f"Cannot refresh logs for {job_type.value}: monitor UI not yet built.")
            return

        monitor = card_data["monitor"]
        job_index = card_data["job_index"]

        logs = await backend.get_pipeline_job_logs(shared_state["current_project_path"], job_type.value, str(job_index))
        monitor["stdout"].clear()
        monitor["stdout"].push(logs.get("stdout", "No output"))
        monitor["stderr"].clear()
        monitor["stderr"].push(logs.get("stderr", "No errors"))

        if notify:
            ui.run(lambda: ui.notify("Logs refreshed", type="positive"))

    def update_job_card_sync_indicator(job_type: JobType):
        """Update sync indicator on job card"""
        if job_type not in shared_state["job_cards"]:
            return  # Job hasn't been added yet

        is_synced = is_job_synced_with_global(job_type)

        # Store in state for binding
        shared_state["job_cards"][job_type]["is_synced"] = is_synced

    # ==================================
    # UI CONSTRUCTION
    # ==================================
    # In build_pipeline_builder_panel, replace the UI construction section:

    with ui.column().classes("w-full h-full gap-3 p-3 overflow-y-auto"):
        # PIPELINE BUILDER HEADER
        ui.label("PIPELINE BUILDER").classes("text-xs font-bold text-gray-700 uppercase tracking-wide")
        
        # Job selection buttons (hidden when running)
        shared_state["job_selector_container"] = ui.card().classes(
            "w-full p-4 bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200"
        )
        
        with shared_state["job_selector_container"]:
            ui.label("Available Jobs").classes("text-sm font-medium mb-3 text-gray-700")
            with ui.row().classes("w-full gap-2 flex-wrap"):
                for job_type in JobConfig.get_ordered_jobs():
                    name = JobConfig.get_job_display_name(job_type)
                    desc = JobConfig.get_job_description(job_type)
                    
                    btn = ui.button(
                        on_click=lambda j=job_type: toggle_job_in_pipeline(j)
                    ).props("dense").classes("text-xs")
                    
                    shared_state.setdefault("job_buttons", {})[job_type] = btn
                    update_job_button(job_type)
                    btn.tooltip(desc)
        
        # Pipeline jobs container (for tabs)
        ui.label("Pipeline Jobs").classes("text-sm font-medium text-gray-700 mt-4")
        panel_state["pipeline_container"] = ui.column().classes(
            "w-full max-h-[60vh] overflow-y-auto border rounded-lg p-3 bg-gray-50/50"
        )
        
        # DON'T call rebuild here - it will be called when jobs are added
        # rebuild_pipeline_ui()  # <-- REMOVE THIS LINE
        
        # Just show initial empty state
        with panel_state["pipeline_container"]:
            ui.label("No jobs selected").classes("text-xs text-gray-500 italic text-center p-4")
        callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
        callbacks["update_job_card_sync_indicator"] = update_job_card_sync_indicator

        return panel_state