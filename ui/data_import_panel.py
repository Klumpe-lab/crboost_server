# ui/data_import_panel.py
"""
Data import panel - refactored to use typed UIStateManager.
"""
import asyncio
import glob
from pathlib import Path
from typing import Dict, Any, Callable, Optional

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobType, get_state_service, get_project_state
from ui.ui_state import get_ui_state_manager, UIStateManager
from local_file_picker import local_file_picker

# === HARDCODED DEFAULTS (fill these in for your setup) ===
DEFAULT_PROJECT_NAME      = "demo_project"
DEFAULT_PROJECT_BASE_PATH = "/users/artem.kushner/dev/crboost_server/projects"
DEFAULT_MOVIES_DIR        = "/users/artem.kushner/dev/001_CopiaTestSet/frames"
DEFAULT_MDOCS_DIR         = "/users/artem.kushner/dev/001_CopiaTestSet/mdoc"
DEFAULT_MOVIES_EXT        = "*.eer"
DEFAULT_MDOCS_EXT         = "*.mdoc"

# Pre-compute the glob patterns
DEFAULT_MOVIES_GLOB = f"{DEFAULT_MOVIES_DIR}/{DEFAULT_MOVIES_EXT}" if DEFAULT_MOVIES_DIR else ""
DEFAULT_MDOCS_GLOB = f"{DEFAULT_MDOCS_DIR}/{DEFAULT_MDOCS_EXT}" if DEFAULT_MDOCS_DIR else ""
def build_data_import_panel(
    backend: CryoBoostBackend,
    callbacks: Dict[str, Callable],
) -> None:
    """
    Build the data import panel (left side of the UI).
    
    Handles:
    - Project name and path input
    - Movies/mdocs glob patterns
    - Parameter autodetection
    - Project creation and loading
    """
    ui_mgr = get_ui_state_manager()
    state_service = get_state_service()
    
    # Validation Functions
    # ===========================================
    
    def validate_glob_pattern(pattern: str) -> tuple[bool, int, str]:
        """Validate a glob pattern and return (is_valid, count, message)."""
        if not pattern or not pattern.strip():
            return False, 0, "No pattern specified"
        
        try:
            matches = glob.glob(pattern)
            count = len(matches)
            if count == 0:
                return False, 0, "No files match pattern"
            return True, count, f"{count} files found"
        except Exception as e:
            return False, 0, f"Invalid pattern: {e}"
    
    def update_input_validation(input_el, is_valid: bool, message: str):
        """Safely update input validation state."""
        if not input_el:
            return
        try:
            # Use the validation property instead of props
            if is_valid:
                input_el.props(remove="error")
            else:
                input_el.props("error")
            # Update hint via tooltip or helper text
            input_el.tooltip(message)
        except Exception as e:
            print(f"[UI] Validation update error: {e}")
    
    def update_movies_validation():
        """Update movies input validation state."""
        pattern = ui_mgr.data_import.movies_glob
        is_valid, count, msg = validate_glob_pattern(pattern)
        ui_mgr.update_data_import(movies_valid=is_valid)
        
        input_el = ui_mgr.panel_refs.movies_input
        if input_el:
            update_input_validation(input_el, is_valid, msg)
            # Update the hint label if we have one
            hint_label = ui_mgr.panel_refs.movies_hint_label
            if hint_label:
                hint_label.set_text(msg)
                if is_valid:
                    hint_label.classes(remove="text-red-500")
                    hint_label.classes("text-green-600")
                else:
                    hint_label.classes(remove="text-green-600")
                    hint_label.classes("text-red-500")
    
    def update_mdocs_validation():
        """Update mdocs input validation state."""
        pattern = ui_mgr.data_import.mdocs_glob
        is_valid, count, msg = validate_glob_pattern(pattern)
        ui_mgr.update_data_import(mdocs_valid=is_valid)
        
        input_el = ui_mgr.panel_refs.mdocs_input
        if input_el:
            update_input_validation(input_el, is_valid, msg)
            hint_label = ui_mgr.panel_refs.mdocs_hint_label
            if hint_label:
                hint_label.set_text(msg)
                if is_valid:
                    hint_label.classes(remove="text-red-500")
                    hint_label.classes("text-green-600")
                else:
                    hint_label.classes(remove="text-green-600")
                    hint_label.classes("text-red-500")
    
    def can_create_project() -> bool:
        """Check if all required fields are filled for project creation."""
        di = ui_mgr.data_import
        return bool(
            di.project_name.strip()
            and di.project_base_path.strip()
            and di.movies_glob.strip()
            and di.mdocs_glob.strip()
            and len(ui_mgr.selected_jobs) > 0
        )
    
    def update_create_button_state():
        """Enable/disable create button based on form validity."""
        btn = ui_mgr.panel_refs.create_button
        if btn:
            if can_create_project() and not ui_mgr.is_project_created:
                btn.props(remove="disable")
            else:
                btn.props("disable")
    
    # ===========================================
    # File Picker Handlers
    # ===========================================
    
    async def pick_movies_path():
            """Open directory picker for movies location."""
            picker = local_file_picker(
                directory=DEFAULT_MOVIES_DIR or "~",
                mode="directory",
                glob_pattern_annotation="Select folder containing frames (*.eer, *.tif, *.mrc)"
            )
            result = await picker
            
            if result and len(result) > 0:
                selected_dir = Path(result[0])
                pattern = str(selected_dir / DEFAULT_MOVIES_EXT)
                
                ui_mgr.update_data_import(movies_glob=pattern)
                
                input_el = ui_mgr.panel_refs.movies_input
                if input_el:
                    input_el.set_value(pattern)
                
                update_movies_validation()
                update_create_button_state()
        
    async def pick_mdocs_path():
        """Open directory picker for mdocs location."""
        picker = local_file_picker(
            directory=DEFAULT_MDOCS_DIR or "~",
            mode="directory",
            glob_pattern_annotation="Select folder containing *.mdoc files"
        )
        result = await picker
        
        if result and len(result) > 0:
            selected_dir = Path(result[0])
            pattern = str(selected_dir / DEFAULT_MDOCS_EXT)
            
            ui_mgr.update_data_import(mdocs_glob=pattern)
            
            input_el = ui_mgr.panel_refs.mdocs_input
            if input_el:
                input_el.set_value(pattern)
            
            update_mdocs_validation()
            update_create_button_state()

    async def pick_project_path():
        """Open directory picker for project base path."""
        picker = local_file_picker(
            directory="~",
            mode="directory",
        )
        result = await picker
        
        if result and len(result) > 0:
            dir_path = result[0]
            
            ui_mgr.update_data_import(project_base_path=dir_path)
            
            input_el = ui_mgr.panel_refs.project_path_input
            if input_el:
                input_el.set_value(dir_path)
            
            update_create_button_state()
    
    # ===========================================
    # Action Handlers
    # ===========================================
    
    async def handle_autodetect():
        """Auto-detect parameters from mdoc files."""
        mdocs_glob = ui_mgr.data_import.mdocs_glob
        if not mdocs_glob:
            ui.notify("Please specify mdoc files first", type="warning")
            return
        
        # Validate the glob pattern first
        is_valid, count, msg = validate_glob_pattern(mdocs_glob)
        if not is_valid:
            ui.notify(f"Invalid mdoc pattern: {msg}", type="warning")
            return
        
        btn = ui_mgr.panel_refs.autodetect_button
        if btn:
            btn.props("loading")
        
        try:
            params = await backend.autodetect_parameters(mdocs_glob)
            
            # Update UI state with detected values
            microscope = params.get("microscope", {})
            acquisition = params.get("acquisition", {})
            
            ui_mgr.update_detected_params(
                pixel_size=microscope.get("pixel_size_angstrom"),
                voltage=microscope.get("acceleration_voltage_kv"),
                dose_per_tilt=acquisition.get("dose_per_tilt"),
                tilt_axis=acquisition.get("tilt_axis_degrees"),
            )
            
            # Refresh the params display
            refresh_params_display()
            
            ui.notify(f"Parameters detected from {count} mdoc files", type="positive")
        except Exception as e:
            ui.notify(f"Autodetection failed: {e}", type="negative")
        finally:
            if btn:
                btn.props(remove="loading")
    
    async def handle_create_project():
        """Create a new project."""
        if not can_create_project():
            ui.notify("Please fill all required fields and select at least one job", type="warning")
            return
        
        di = ui_mgr.data_import
        btn = ui_mgr.panel_refs.create_button
        
        if btn:
            btn.props("loading")
        
        try:
            result = await backend.create_project_and_scheme(
                project_name=di.project_name,
                project_base_path=di.project_base_path,
                selected_jobs=[j.value for j in ui_mgr.selected_jobs],
                movies_glob=di.movies_glob,
                mdocs_glob=di.mdocs_glob,
            )
            
            if result.get("success"):
                project_path = Path(result["project_path"])
                scheme_name = f"scheme_{di.project_name}"
                
                ui_mgr.set_project_created(project_path, scheme_name)
                
                ui.notify(f"Project '{di.project_name}' created successfully", type="positive")
                
                # Update status indicator
                update_status_indicator()
                
                # Trigger pipeline UI rebuild
                if "rebuild_pipeline_ui" in callbacks:
                    callbacks["rebuild_pipeline_ui"]()
            else:
                ui.notify(f"Failed: {result.get('error')}", type="negative")
        except Exception as e:
            ui.notify(f"Error creating project: {e}", type="negative")
        finally:
            if btn:
                btn.props(remove="loading")
            update_create_button_state()
    
    async def handle_load_project():
        """Load an existing project."""
        picker = local_file_picker(
            directory="~",
            mode="directory",
        )
        result = await picker
        
        if not result or len(result) == 0:
            return
        
        project_dir = Path(result[0])
        
        # Find project_params.json
        params_file = project_dir / "project_params.json"
        if not params_file.exists():
            ui.notify("No project_params.json found in selected directory", type="warning")
            return
        
        btn = ui_mgr.panel_refs.load_button
        if btn:
            btn.props("loading")
        
        try:
            load_result = await backend.load_existing_project(str(project_dir))
            
            if load_result.get("success"):
                # Update UI state
                project_name = load_result.get("project_name", project_dir.name)
                selected_jobs = [JobType(j) for j in load_result.get("selected_jobs", [])]
                
                ui_mgr.load_from_project(
                    project_path=project_dir,
                    scheme_name=f"scheme_{project_name}",
                    jobs=selected_jobs,
                )
                
                # Update form fields
                movies_glob = load_result.get("movies_glob", "")
                mdocs_glob = load_result.get("mdocs_glob", "")
                
                ui_mgr.update_data_import(
                    project_name=project_name,
                    project_base_path=str(project_dir.parent),
                    movies_glob=movies_glob,
                    mdocs_glob=mdocs_glob,
                )
                
                # Update input values using set_value
                if ui_mgr.panel_refs.project_name_input:
                    ui_mgr.panel_refs.project_name_input.set_value(project_name)
                if ui_mgr.panel_refs.project_path_input:
                    ui_mgr.panel_refs.project_path_input.set_value(str(project_dir.parent))
                if ui_mgr.panel_refs.movies_input:
                    ui_mgr.panel_refs.movies_input.set_value(movies_glob)
                if ui_mgr.panel_refs.mdocs_input:
                    ui_mgr.panel_refs.mdocs_input.set_value(mdocs_glob)
                
                # Update validations
                update_movies_validation()
                update_mdocs_validation()
                
                refresh_params_display()
                update_status_indicator()
                
                ui.notify(f"Project '{project_name}' loaded", type="positive")
                
                # Rebuild pipeline UI
                if "rebuild_pipeline_ui" in callbacks:
                    callbacks["rebuild_pipeline_ui"]()
                
                # Start status refresh
                if "check_and_update_statuses" in callbacks:
                    asyncio.create_task(callbacks["check_and_update_statuses"]())
            else:
                ui.notify(f"Failed to load: {load_result.get('error')}", type="negative")
        except Exception as e:
            import traceback
            traceback.print_exc()
            ui.notify(f"Error loading project: {e}", type="negative")
        finally:
            if btn:
                btn.props(remove="loading")
    
    def refresh_params_display():
        """Refresh the detected parameters display."""
        container = ui_mgr.panel_refs.params_display_container
        if not container:
            return
        
        container.clear()
        
        state = get_project_state()
        
        params = [
            ("Pixel Size", f"{state.microscope.pixel_size_angstrom:.3f} Å"),
            ("Voltage", f"{state.microscope.acceleration_voltage_kv:.0f} kV"),
            ("Cs", f"{state.microscope.spherical_aberration_mm:.1f} mm"),
            ("Dose/Tilt", f"{state.acquisition.dose_per_tilt:.1f} e-/A²"),
            ("Tilt Axis", f"{state.acquisition.tilt_axis_degrees:.1f}°"),
        ]
        
        with container:
            for label, value in params:
                with ui.row().classes("w-full justify-between py-1"):
                    ui.label(label).classes("text-xs text-gray-500")
                    ui.label(value).classes("text-xs font-medium text-gray-700")
    
    def update_status_indicator():
        """Update the status indicator at the bottom."""
        indicator = ui_mgr.panel_refs.status_indicator
        if indicator:
            if ui_mgr.is_project_created:
                indicator.set_text(f"Project: {ui_mgr.data_import.project_name}")
                indicator.classes(remove="text-gray-500")
                indicator.classes("text-green-600")
            else:
                indicator.set_text("Ready")
                indicator.classes(remove="text-green-600")
                indicator.classes("text-gray-500")
    
    # ===========================================
    # Input Change Handlers
    # ===========================================
    
    def on_project_name_change(e):
        value = e.value if hasattr(e, 'value') else e.sender.value
        ui_mgr.update_data_import(project_name=value or "")
        # Auto-set import prefix
        if value:
            ui_mgr.update_data_import(import_prefix=f"{value}_")
        update_create_button_state()
    
    def on_project_path_change(e):
        value = e.value if hasattr(e, 'value') else e.sender.value
        ui_mgr.update_data_import(project_base_path=value or "")
        update_create_button_state()
    
    def on_movies_change(e):
        value = e.value if hasattr(e, 'value') else e.sender.value
        ui_mgr.update_data_import(movies_glob=value or "")
        update_movies_validation()
        update_create_button_state()
    
    def on_mdocs_change(e):
        value = e.value if hasattr(e, 'value') else e.sender.value
        ui_mgr.update_data_import(mdocs_glob=value or "")
        update_mdocs_validation()
        update_create_button_state()
    
    # ===========================================
    # Build the UI
    # ===========================================
    
    # Initialize panel refs for hint labels (not in dataclass by default)
    ui_mgr.panel_refs.movies_hint_label = None
    ui_mgr.panel_refs.mdocs_hint_label = None
    ui_mgr.panel_refs.status_indicator = None
    
    with ui.column().classes("w-full h-full p-4 overflow-y-auto").style(
        "font-family: 'IBM Plex Sans', sans-serif;"
    ):
        # Section: Project Info
        ui.label("Project").classes("text-sm font-bold text-gray-700 mb-2")
        
        with ui.card().classes("w-full p-4 mb-4 border border-gray-200 shadow-none"):
            # Project name
# Project name - use default directly
            project_name_input = ui.input(
                label="Project Name",
                value=DEFAULT_PROJECT_NAME or ui_mgr.data_import.project_name,
                on_change=on_project_name_change,
            ).props("outlined dense").classes("w-full mb-3")
            ui_mgr.panel_refs.project_name_input = project_name_input
            
            # Project path with picker
            with ui.row().classes("w-full items-end gap-2 mb-3"):
                project_path_input = ui.input(
                    label="Project Location",
                    value=DEFAULT_PROJECT_BASE_PATH or ui_mgr.data_import.project_base_path,
                    on_change=on_project_path_change,
                ).props("outlined dense").classes("flex-1")
                ui_mgr.panel_refs.project_path_input = project_path_input
                
                ui.button(
                    icon="folder_open",
                    on_click=pick_project_path
                ).props("flat dense").classes("mb-1")
            
            # Action buttons
            with ui.row().classes("w-full gap-2"):
                create_btn = ui.button(
                    "Create Project",
                    icon="add",
                    on_click=handle_create_project,
                ).props("dense no-caps disable").classes("flex-1").style(
                    "background: #3b82f6; color: white;"
                )
                ui_mgr.panel_refs.create_button = create_btn
                
                load_btn = ui.button(
                    "Load Existing",
                    icon="folder_open",
                    on_click=handle_load_project,
                ).props("dense flat no-caps").classes("flex-1")
                ui_mgr.panel_refs.load_button = load_btn
        
        # Section: Data Sources
        ui.label("Data Sources").classes("text-sm font-bold text-gray-700 mb-2")
        
        with ui.card().classes("w-full p-4 mb-4 border border-gray-200 shadow-none"):
            # Movies glob
            with ui.column().classes("w-full mb-3"):
                with ui.row().classes("w-full items-end gap-2"):
                    movies_input = ui.input(
                        label="Movies Pattern (glob)",
                        value=DEFAULT_MOVIES_GLOB or ui_mgr.data_import.movies_glob,
                        placeholder="/path/to/frames/*.eer",
                        on_change=on_movies_change,
                    ).props("outlined dense").classes("flex-1")
                    ui_mgr.panel_refs.movies_input = movies_input
                    
                    ui.button(
                        icon="folder_open",
                        on_click=pick_movies_path
                    ).props("flat dense").classes("mb-1")
                
                # Hint label for movies
                movies_hint = ui.label("No pattern specified").classes("text-xs text-gray-500 mt-1")
                ui_mgr.panel_refs.movies_hint_label = movies_hint
            
            # Mdocs glob
            with ui.column().classes("w-full mb-3"):
                with ui.row().classes("w-full items-end gap-2"):
                    mdocs_input = ui.input(
                        label="Mdoc Pattern (glob)",
                        value=DEFAULT_MDOCS_GLOB or ui_mgr.data_import.mdocs_glob,
                        placeholder="/path/to/mdocs/*.mdoc",
                        on_change=on_mdocs_change,
                    ).props("outlined dense").classes("flex-1")
                    ui_mgr.panel_refs.mdocs_input = mdocs_input
                    
                    ui.button(
                        icon="folder_open",
                        on_click=pick_mdocs_path
                    ).props("flat dense").classes("mb-1")
                
                # Hint label for mdocs
                mdocs_hint = ui.label("No pattern specified").classes("text-xs text-gray-500 mt-1")
                ui_mgr.panel_refs.mdocs_hint_label = mdocs_hint
            
            # Autodetect button
            autodetect_btn = ui.button(
                "Autodetect from Mdocs",
                icon="auto_fix_high",
                on_click=handle_autodetect,
            ).props("dense flat no-caps").classes("w-full").style(
                "background: #f3f4f6; border: 1px solid #e5e7eb;"
            )
            ui_mgr.panel_refs.autodetect_button = autodetect_btn
        
        # Section: Detected Parameters
        ui.label("Detected Parameters").classes("text-sm font-bold text-gray-700 mb-2")
        
        with ui.card().classes("w-full p-4 border border-gray-200 shadow-none"):
            params_container = ui.column().classes("w-full")
            ui_mgr.panel_refs.params_display_container = params_container
            
            # Initial render
            refresh_params_display()
        
        # Status indicator
        with ui.row().classes("w-full mt-4 items-center gap-2"):
            ui.icon("info", size="16px").classes("text-gray-400")
            status_text = "Ready" if not ui_mgr.is_project_created else f"Project: {ui_mgr.data_import.project_name}"
            status_indicator = ui.label(status_text).classes("text-xs text-gray-500")
            ui_mgr.panel_refs.status_indicator = status_indicator
    
# === Initialize with defaults ===
    if DEFAULT_PROJECT_NAME and not ui_mgr.data_import.project_name:
        ui_mgr.update_data_import(
            project_name=DEFAULT_PROJECT_NAME,
            project_base_path=DEFAULT_PROJECT_BASE_PATH,
            movies_glob=str(Path(DEFAULT_MOVIES_DIR) / DEFAULT_MOVIES_EXT) if DEFAULT_MOVIES_DIR else "",
            mdocs_glob=str(Path(DEFAULT_MDOCS_DIR) / DEFAULT_MDOCS_EXT) if DEFAULT_MDOCS_DIR else "",
        )
        # Update input fields
# Run initial validation after UI is built
    # Also sync the state manager with the defaults we used
# Run initial validation after UI is built
    ui_mgr.update_data_import(
        project_name=DEFAULT_PROJECT_NAME,
        project_base_path=DEFAULT_PROJECT_BASE_PATH,
        movies_glob=DEFAULT_MOVIES_GLOB,
        mdocs_glob=DEFAULT_MDOCS_GLOB,
    )
    update_movies_validation()
    update_mdocs_validation()
    update_create_button_state()
    
    # Subscribe to state changes - when jobs are added/removed, update button
    def on_ui_state_change(state):
        print(f"[DATA_IMPORT] State changed! Jobs: {len(state.selected_jobs)}, can_create: {can_create_project()}")
        update_create_button_state()
    
    ui_mgr.subscribe(on_ui_state_change)
    print(f"[DATA_IMPORT] Subscribed to UI state changes")