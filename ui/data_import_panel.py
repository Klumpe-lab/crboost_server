# ui/data_import_panel.py
"""
Data import panel.
Refactored for better logical grouping, configuration loading, and "Recent Projects" browser.
"""

import asyncio
import glob
from pathlib import Path
from typing import Dict, Any, Callable

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import get_project_state
from ui.ui_state import get_ui_state_manager
from ui.local_file_picker import local_file_picker

# === DEFAULTS ===
DEFAULT_MOVIES_EXT = "*.eer"
DEFAULT_MDOCS_EXT = "*.mdoc"


def build_data_import_panel(backend: CryoBoostBackend, callbacks: Dict[str, Callable]) -> None:
    """
    Build the data import panel.
    """
    ui_mgr = get_ui_state_manager()
    
    # Use a mutable dictionary to store local widget references
    local_refs = {
        "recent_projects_container": None,
        # Fallbacks for the file picker logic if config is empty
        "default_movies_ext": "*.eer", 
        "default_mdocs_ext": "*.mdoc"
    }

    # Initialize defaults if not set
    async def init_defaults():
        # 1. Fetch Defaults from Backend
        defaults = await backend.get_default_data_globs()
        config_movies = defaults.get("movies", "")
        config_mdocs = defaults.get("mdocs", "")

        # 2. Update File Picker helpers (extract extension from full path if possible)
        if config_movies and "*" in config_movies:
            # simple logic: try to grab the extension part for the file picker default
            local_refs["default_movies_ext"] = "*" + config_movies.split("*")[-1]
        
        if config_mdocs and "*" in config_mdocs:
            local_refs["default_mdocs_ext"] = "*" + config_mdocs.split("*")[-1]

        # 3. Apply Project Base Path (if not already set by state)
        if not ui_mgr.data_import.project_base_path:
            default_path = await backend.get_default_project_base()
            ui_mgr.update_data_import(project_base_path=default_path)
            if ui_mgr.panel_refs.project_path_input:
                ui_mgr.panel_refs.project_path_input.value = default_path
        
        # 4. Apply Data Source Globs (if not already set by state)
        if not ui_mgr.data_import.movies_glob and config_movies:
            ui_mgr.update_data_import(movies_glob=config_movies)
            if ui_mgr.panel_refs.movies_input:
                ui_mgr.panel_refs.movies_input.value = config_movies
            update_movies_validation()
        
        if not ui_mgr.data_import.mdocs_glob and config_mdocs:
            ui_mgr.update_data_import(mdocs_glob=config_mdocs)
            if ui_mgr.panel_refs.mdocs_input:
                ui_mgr.panel_refs.mdocs_input.value = config_mdocs
            # Trigger validation
            update_mdocs_validation()

        # Refresh the recent projects list on load
        await refresh_recent_projects()

    def validate_glob_pattern(pattern: str) -> tuple[bool, int, str]:
        if not pattern or not pattern.strip():
            return False, 0, "No pattern specified"
        try:
            matches = glob.glob(pattern)
            files = [m for m in matches if Path(m).is_file()]
            count = len(files)

            if count == 0:
                if len(matches) > 0:
                    return False, 0, "Pattern matches directories, but no files"
                return False, 0, "No files match pattern on server"

            return True, count, f"{count} files found"
        except Exception as e:
            return False, 0, f"Invalid pattern: {e}"

    def update_input_validation(input_el, is_valid: bool, message: str):
        if not input_el:
            return

        if is_valid:
            input_el.props("error=false")
            input_el.props("error-message=''")
        else:
            input_el.props("error=true")
            input_el.props(f"error-message='{message}'")

    def update_movies_validation():
        pattern = ui_mgr.data_import.movies_glob
        is_valid, count, msg = validate_glob_pattern(pattern)
        ui_mgr.update_data_import(movies_valid=is_valid)

        input_el = ui_mgr.panel_refs.movies_input
        if input_el:
            update_input_validation(input_el, is_valid, msg)
            hint_label = ui_mgr.panel_refs.movies_hint_label
            if hint_label:
                hint_label.set_text(msg)
                if is_valid:
                    hint_label.classes(remove="text-red-500 text-gray-500").classes("text-green-600")
                else:
                    hint_label.classes(remove="text-green-600 text-gray-500").classes("text-red-500")

        update_create_button_state()

    def update_mdocs_validation():
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
                    hint_label.classes(remove="text-red-500 text-gray-500").classes("text-green-600")
                else:
                    hint_label.classes(remove="text-green-600 text-gray-500").classes("text-red-500")

        update_create_button_state()

    def get_missing_requirements() -> list[str]:
        """Helper to find out EXACTLY what is missing."""
        di = ui_mgr.data_import
        missing = []
        if not (di.project_name and di.project_name.strip()):
            missing.append("Project Name")
        if not (di.project_base_path and di.project_base_path.strip()):
            missing.append("Project Path")
        if not di.movies_glob:
            missing.append("Movies Pattern")
        elif not di.movies_valid:
            missing.append("Valid Movies")
        if not di.mdocs_glob:
            missing.append("Mdocs Pattern")
        elif not di.mdocs_valid:
            missing.append("Valid Mdocs")
        return missing

    def can_create_project() -> bool:
        missing = get_missing_requirements()
        return len(missing) == 0

    def update_create_button_state():
        btn = ui_mgr.panel_refs.create_button
        status_label = ui_mgr.panel_refs.status_indicator

        missing = get_missing_requirements()

        if btn:
            if len(missing) == 0 and not ui_mgr.is_project_created:
                btn.enable()
                btn.classes(remove="opacity-50 cursor-not-allowed")
                btn.style("background: #2563eb; color: white;")
                if status_label:
                    status_label.set_text("Ready to Create")
                    status_label.classes(remove="text-red-500").classes("text-green-600")
            else:
                btn.disable()
                btn.classes("opacity-50 cursor-not-allowed")
                btn.style("background: #93c5fd; color: white;")
                if status_label:
                    if ui_mgr.is_project_created:
                        status_label.set_text("Project Created")
                    else:
                        status_label.set_text(f"Missing: {', '.join(missing)}")
                        status_label.classes(remove="text-green-600").classes("text-red-500")

    def update_locking_state():
        is_running = ui_mgr.is_running
        if ui_mgr.panel_refs.autodetect_button:
            if is_running:
                ui_mgr.panel_refs.autodetect_button.disable()
            else:
                ui_mgr.panel_refs.autodetect_button.enable()
        if ui_mgr.panel_refs.movies_input:
            if is_running:
                ui_mgr.panel_refs.movies_input.disable()
            else:
                ui_mgr.panel_refs.movies_input.enable()
        if ui_mgr.panel_refs.mdocs_input:
            if is_running:
                ui_mgr.panel_refs.mdocs_input.disable()
            else:
                ui_mgr.panel_refs.mdocs_input.enable()
        update_create_button_state()

    # --- File Pickers ---
    async def pick_movies_path():
        directory = "~"
        picker = local_file_picker(directory=directory, mode="directory")
        result = await picker
        if result and len(result) > 0:
            selected_dir = Path(result[0])
            pattern = str(selected_dir / DEFAULT_MOVIES_EXT)
            ui_mgr.update_data_import(movies_glob=pattern)
            if ui_mgr.panel_refs.movies_input:
                ui_mgr.panel_refs.movies_input.value = pattern
            update_movies_validation()

    async def pick_mdocs_path():
        directory = "~"
        picker = local_file_picker(directory=directory, mode="directory")
        result = await picker
        if result and len(result) > 0:
            selected_dir = Path(result[0])
            pattern = str(selected_dir / DEFAULT_MDOCS_EXT)
            ui_mgr.update_data_import(mdocs_glob=pattern)
            if ui_mgr.panel_refs.mdocs_input:
                ui_mgr.panel_refs.mdocs_input.value = pattern
            update_mdocs_validation()

    async def pick_project_path():
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if result and len(result) > 0:
            dir_path = result[0]
            ui_mgr.update_data_import(project_base_path=dir_path)
            if ui_mgr.panel_refs.project_path_input:
                ui_mgr.panel_refs.project_path_input.set_value(dir_path)
            update_locking_state()
            # If base path changes, refresh the recent projects list
            await refresh_recent_projects()

    # --- Project Browser Logic ---
    async def refresh_recent_projects():
        container = local_refs["recent_projects_container"]
        if not container:
            return
            
        container.clear()
        base_path = ui_mgr.data_import.project_base_path
        if not base_path:
            return

        with container:
            ui.spinner("dots").classes("self-center")
        
        # Run scan in background so UI doesn't freeze
        projects = await backend.scan_for_projects(base_path)
        
        container.clear()
        with container:
            if not projects:
                ui.label(f"No projects found in {Path(base_path).name}").classes("text-xs text-gray-400 italic p-2")
            else:
                for proj in projects:
                    with ui.card().classes("w-full p-2 mb-2 border border-gray-200 cursor-pointer hover:bg-blue-50 transition-colors"):
                        with ui.row().classes("w-full items-center justify-between no-wrap"):
                            with ui.column().classes("gap-0"):
                                ui.label(proj["name"]).classes("text-sm font-bold text-gray-700")
                                ui.label(f"Modified: {proj['modified']}").classes("text-[10px] text-gray-400")
                            
                            ui.button(icon="arrow_forward", on_click=lambda p=proj["path"]: load_specific_project(p)).props("flat dense round size=sm").classes("text-blue-500")

    async def load_specific_project(path_str: str):
        """Wrapper to load a specific path from the list"""
        await handle_load_project(Path(path_str))

    # --- Handlers ---
    async def handle_autodetect():
        if ui_mgr.is_running:
            return
        mdocs_glob = ui_mgr.data_import.mdocs_glob
        if not mdocs_glob:
            ui.notify("Please specify mdoc files first", type="warning")
            return
        is_valid, count, msg = validate_glob_pattern(mdocs_glob)
        if not is_valid:
            ui.notify(f"Invalid mdoc pattern: {msg}", type="warning")
            return

        btn = ui_mgr.panel_refs.autodetect_button
        if btn:
            btn.props("loading")
        try:
            params = await backend.autodetect_parameters(mdocs_glob)
            microscope = params.get("microscope", {})
            acquisition = params.get("acquisition", {})
            ui_mgr.update_detected_params(
                pixel_size=microscope.get("pixel_size_angstrom"),
                voltage=microscope.get("acceleration_voltage_kv"),
                dose_per_tilt=acquisition.get("dose_per_tilt"),
                tilt_axis=acquisition.get("tilt_axis_degrees"),
            )
            refresh_params_display()
            ui.notify(f"Parameters detected from {count} files", type="positive")
        except Exception as e:
            ui.notify(f"Autodetection failed: {e}", type="negative")
        finally:
            if btn:
                btn.props(remove="loading")

    async def handle_create_project():
        update_movies_validation()
        update_mdocs_validation()
        if not can_create_project():
            ui.notify(f"Cannot create: Missing {get_missing_requirements()}", type="warning")
            return
        
        di = ui_mgr.data_import
        btn = ui_mgr.panel_refs.create_button
        if btn: btn.props("loading")
        
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
                
                if "rebuild_pipeline_ui" in callbacks:
                    callbacks["rebuild_pipeline_ui"]()
            else:
                ui.notify(f"Failed: {result.get('error')}", type="negative")
        except Exception as e:
            import traceback
            traceback.print_exc()
            ui.notify(f"Error creating project: {e}", type="negative")
        finally:
            if btn: btn.props(remove="loading")
            update_locking_state()

    async def handle_load_project_click():
        """User clicked the manual load button"""
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if result and len(result) > 0:
            await handle_load_project(Path(result[0]))

    async def handle_load_project(project_dir: Path):
        """Shared logic for loading a project"""
        params_file = project_dir / "project_params.json"
        if not params_file.exists():
            ui.notify("No project_params.json found in directory", type="warning")
            return

        btn = ui_mgr.panel_refs.load_button
        if btn:
            btn.props("loading")
        
        try:
            load_result = await backend.load_existing_project(str(project_dir))
            if load_result.get("success"):
                project_name = load_result.get("project_name", project_dir.name)
                selected_jobs = []
                for j_str in load_result.get("selected_jobs", []):
                    try:
                        from services.project_state import JobType
                        selected_jobs.append(JobType(j_str))
                    except:
                        pass

                movies_glob = load_result.get("movies_glob", "")
                mdocs_glob = load_result.get("mdocs_glob", "")
                
                # Update UI manager state
                ui_mgr.load_from_project(
                    project_path=project_dir, 
                    scheme_name=f"scheme_{project_name}", 
                    jobs=selected_jobs
                )
                ui_mgr.update_data_import(
                    project_name=project_name,
                    project_base_path=str(project_dir.parent),
                    movies_glob=movies_glob,
                    mdocs_glob=mdocs_glob,
                )
                
                ui.notify(f"Project '{project_name}' loaded", type="positive")
                ui.navigate.to("/workspace")
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
        container = ui_mgr.panel_refs.params_display_container
        if not container:
            return
        container.clear()
        
        # Check if parameters have actually been detected/set in the UI state
        d_pix = ui_mgr.data_import.detected_pixel_size
        d_dose = ui_mgr.data_import.detected_dose_per_tilt
        
        has_detection = d_pix is not None or d_dose is not None

        if not has_detection:
            with container:
                ui.icon("radar", size="24px").classes("text-gray-300 self-center mb-1")
                ui.label("Import MDOCs to detect parameters").classes("text-xs text-gray-400 italic text-center")
            return

        # If we have detection, fetch values from ProjectState (which was updated during autodetect)
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
                with ui.row().classes("w-full justify-between py-1 border-b border-gray-100 last:border-0"):
                    ui.label(label).classes("text-xs text-gray-500")
                    ui.label(value).classes("text-xs font-medium text-gray-700 font-mono")

    # --- Input Change Handlers ---
    def on_project_name_change(e):
        value = e.value if hasattr(e, 'value') else str(e) if e else ""
        ui_mgr.update_data_import(project_name=value)
        update_create_button_state()

    def on_project_path_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        ui_mgr.update_data_import(project_base_path=value)
        update_create_button_state()
        # Trigger async refresh of projects list
        asyncio.create_task(refresh_recent_projects())

    def on_movies_change(e):
        value = e.value if hasattr(e, 'value') else str(e) if e else ""
        ui_mgr.update_data_import(movies_glob=value)
        update_movies_validation()

    def on_mdocs_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        ui_mgr.update_data_import(mdocs_glob=value)
        update_mdocs_validation()

    # Clear old refs
    ui_mgr.panel_refs.movies_hint_label = None
    ui_mgr.panel_refs.mdocs_hint_label = None
    ui_mgr.panel_refs.status_indicator = None

    # --- MAIN LAYOUT ---
    with ui.column().classes("w-full p-6 gap-6").style("font-family: 'IBM Plex Sans', sans-serif;"):
        
        # Split Layout: Left (Setup) vs Right (Resume)
        with ui.row().classes("w-full gap-8"):
            
            # --- LEFT COLUMN: NEW PROJECT SETUP ---
            with ui.column().classes("flex-1 gap-4"):
                ui.label("Start New Project").classes("text-sm font-bold text-slate-800 uppercase tracking-wider border-b border-slate-200 w-full pb-1")
                
                # 1. Project Identity
                with ui.card().classes("w-full p-4 border border-gray-200 shadow-none bg-white rounded-lg gap-3"):
                    with ui.column().classes("w-full gap-1"):
                        ui.label("Project Name").classes("text-xs font-semibold text-gray-600")
                        project_name_input = (
                            ui.input(value=ui_mgr.data_import.project_name, on_change=on_project_name_change)
                            .props("outlined dense placeholder='e.g., HIV_Tomo_Batch1'")
                            .classes("w-full")
                        )
                        ui_mgr.panel_refs.project_name_input = project_name_input

                    with ui.column().classes("w-full gap-1"):
                        ui.label("Base Location").classes("text-xs font-semibold text-gray-600")
                        with ui.row().classes("w-full items-center gap-2"):
                            project_path_input = (
                                ui.input(
                                    value=ui_mgr.data_import.project_base_path,
                                    on_change=on_project_path_change,
                                )
                                .props("outlined dense")
                                .classes("flex-1")
                            )
                            ui_mgr.panel_refs.project_path_input = project_path_input
                            ui.button(icon="folder", on_click=pick_project_path).props("flat dense round color=grey")

                # 2. Data Sources
                with ui.card().classes("w-full p-4 border border-gray-200 shadow-none bg-white rounded-lg gap-3"):
                    # Movies
                    with ui.column().classes("w-full gap-1"):
                        ui.label("Raw Frames").classes("text-xs font-semibold text-gray-600")
                        with ui.row().classes("w-full items-center gap-2"):
                            movies_input = (
                                ui.input(
                                    value=ui_mgr.data_import.movies_glob,
                                    placeholder="*.eer",
                                    on_change=on_movies_change,
                                )
                                .props("outlined dense")
                                .classes("flex-1")
                            )
                            ui_mgr.panel_refs.movies_input = movies_input
                            ui.button(icon="folder", on_click=pick_movies_path).props("flat dense round color=grey")
                        
                        movies_hint = ui.label("No pattern").classes("text-[10px] text-gray-400 pl-1")
                        ui_mgr.panel_refs.movies_hint_label = movies_hint

                    # Mdocs
                    with ui.column().classes("w-full gap-1"):
                        ui.label("SerialEM Mdocs").classes("text-xs font-semibold text-gray-600")
                        with ui.row().classes("w-full items-center gap-2"):
                            mdocs_input = (
                                ui.input(
                                    value=ui_mgr.data_import.mdocs_glob,
                                    placeholder="*.mdoc",
                                    on_change=on_mdocs_change,
                                )
                                .props("outlined dense")
                                .classes("flex-1")
                            )
                            ui_mgr.panel_refs.mdocs_input = mdocs_input
                            ui.button(icon="folder", on_click=pick_mdocs_path).props("flat dense round color=grey")
                        
                        mdocs_hint = ui.label("No pattern").classes("text-[10px] text-gray-400 pl-1")
                        ui_mgr.panel_refs.mdocs_hint_label = mdocs_hint
                    
                    # Autodetect Action
                    with ui.row().classes("w-full justify-end pt-1"):
                        autodetect_btn = (
                            ui.button("Autodetect Parameters", icon="auto_fix_high", on_click=handle_autodetect)
                            .props("dense flat no-caps size=sm")
                            .classes("text-blue-600 hover:bg-blue-50")
                        )
                        ui_mgr.panel_refs.autodetect_button = autodetect_btn

                # 3. Detected Parameters (Conditioned on autodetect)
                with ui.card().classes("w-full p-4 border border-blue-100 bg-blue-50/30 shadow-none rounded-lg"):
                    ui.label("Detected Configuration").classes("text-xs font-bold text-blue-800 uppercase tracking-wide mb-2")
                    params_container = ui.column().classes("w-full gap-1")
                    ui_mgr.panel_refs.params_display_container = params_container
                    refresh_params_display()


            # --- RIGHT COLUMN: RESUME EXISTING ---
            with ui.column().classes("w-80 gap-4"):
                ui.label("Resume Existing").classes("text-sm font-bold text-slate-800 uppercase tracking-wider border-b border-slate-200 w-full pb-1")
                
                # Removed h-full here to allow card to wrap content naturally
                with ui.card().classes("w-full flex flex-col p-0 border border-gray-200 shadow-none bg-gray-50 rounded-lg overflow-hidden"):
                    # Header inside card
                    ui.label("Projects in Base Location").classes("text-xs font-semibold text-gray-500 p-3 bg-gray-100 border-b border-gray-200")
                    
                    # Scrollable List
                    # Removed flex-1, kept h-96. This forces the scroll area to be 384px tall exactly.
                    with ui.scroll_area().classes("w-full p-2 h-96 bg-white"):
                        # Container for project list updates
                        local_refs["recent_projects_container"] = ui.column().classes("w-full gap-1")
                        
                    # Footer Action
                    with ui.row().classes("w-full p-3 border-t border-gray-200 bg-white"):
                         load_btn = (
                            ui.button("Browse Disk...", icon="folder_open", on_click=handle_load_project_click)
                            .props("outline no-caps dense")
                            .classes("w-full text-slate-600")
                        )
                         ui_mgr.panel_refs.load_button = load_btn

        # --- BOTTOM ACTION BAR ---
        with ui.row().classes("w-full mt-4 pt-4 border-t border-gray-200 justify-between items-center"):
            
            # Status text
            status_indicator = ui.label("Enter details to begin...").classes("text-sm text-gray-500 font-medium")
            ui_mgr.panel_refs.status_indicator = status_indicator
            
            # Main Action Button
            create_btn = (
                ui.button("Create Project", icon="add_circle", on_click=handle_create_project)
                .props("no-caps size=lg")
                .classes("px-8 shadow-md transition-all duration-200")
                .style("background: #93c5fd; color: white;")
            )
            ui_mgr.panel_refs.create_button = create_btn

    # Initial update & triggers
    update_movies_validation()
    update_mdocs_validation()
    update_create_button_state()

    def on_ui_state_change(state):
        update_locking_state()

    ui_mgr.subscribe(on_ui_state_change)
    
    # Initialize defaults (Project base path)
    ui.timer(0.1, init_defaults, once=True)
