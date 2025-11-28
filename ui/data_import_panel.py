# ui/data_import_panel.py
"""
Data import panel.
Refactored with DEBUG LOGGING and VISUAL FEEDBACK to diagnose inactive button.
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
DEFAULT_PROJECT_NAME = ""
DEFAULT_PROJECT_BASE_PATH = "/users/artem.kushner/dev/crboost_server/projects"
DEFAULT_MOVIES_DIR = ""
DEFAULT_MDOCS_DIR = ""
DEFAULT_MOVIES_EXT = "*.eer"
DEFAULT_MDOCS_EXT = "*.mdoc"


def build_data_import_panel(backend: CryoBoostBackend, callbacks: Dict[str, Callable]) -> None:
    """
    Build the data import panel.
    """
    ui_mgr = get_ui_state_manager()

    # --- VALIDATION LOGIC ---
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
        if missing:
            print(f"[DEBUG] Cannot create project. Missing: {missing}")
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
        directory = DEFAULT_MOVIES_DIR if DEFAULT_MOVIES_DIR else "~"
        picker = local_file_picker(directory=directory, mode="directory")
        result = await picker
        print(f"[DEBUG] File picker result: {result}")
        if result and len(result) > 0:
            selected_dir = Path(result[0])
            pattern = str(selected_dir / DEFAULT_MOVIES_EXT)
            print(f"[DEBUG] Setting movies_glob to: {pattern}")
            ui_mgr.update_data_import(movies_glob=pattern)
            print(f"[DEBUG] State after update: {ui_mgr.data_import.movies_glob}")
            if ui_mgr.panel_refs.movies_input:
                print(f"[DEBUG] Setting input.value")
                ui_mgr.panel_refs.movies_input.value = pattern
            else:
                print(f"[DEBUG] WARNING: movies_input ref is None!")
            update_movies_validation()

    async def pick_mdocs_path():
        directory = DEFAULT_MDOCS_DIR if DEFAULT_MDOCS_DIR else "~"
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
        print("[DEBUG] handle_create_project START")
        update_movies_validation()
        update_mdocs_validation()
        if not can_create_project():
            ui.notify(f"Cannot create: Missing {get_missing_requirements()}", type="warning")
            return
        
        di = ui_mgr.data_import
        btn = ui_mgr.panel_refs.create_button
        if btn: btn.props("loading")
        
        try:
            print("[DEBUG] Calling backend.create_project_and_scheme...")
            result = await backend.create_project_and_scheme(
                project_name=di.project_name,
                project_base_path=di.project_base_path,
                selected_jobs=[j.value for j in ui_mgr.selected_jobs], 
                movies_glob=di.movies_glob,
                mdocs_glob=di.mdocs_glob,
            )
            print(f"[DEBUG] Backend returned: {result}")
            
            if result.get("success"):
                project_path = Path(result["project_path"])
                scheme_name = f"scheme_{di.project_name}"
                print(f"[DEBUG] Setting project created: {project_path}")
                ui_mgr.set_project_created(project_path, scheme_name)
                ui.notify(f"Project '{di.project_name}' created successfully", type="positive")
                
                print(f"[DEBUG] Callbacks available: {list(callbacks.keys())}")
                if "rebuild_pipeline_ui" in callbacks:
                    print("[DEBUG] Calling rebuild_pipeline_ui callback...")
                    callbacks["rebuild_pipeline_ui"]()
                    print("[DEBUG] Callback returned")
            else:
                print(f"[DEBUG] Result not success: {result.get('error')}")
                ui.notify(f"Failed: {result.get('error')}", type="negative")
        except Exception as e:
            print(f"[DEBUG] EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            ui.notify(f"Error creating project: {e}", type="negative")
        finally:
            print("[DEBUG] In finally block, removing loading state")
            if btn: btn.props(remove="loading")
            update_locking_state()
            print("[DEBUG] handle_create_project END")

    async def handle_load_project():
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if not result or len(result) == 0:
            return

        project_dir = Path(result[0])
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
                project_name = load_result.get("project_name", project_dir.name)
                selected_jobs = []
                for j_str in load_result.get("selected_jobs", []):
                    try:
                        from services.project_state import JobType

                        selected_jobs.append(JobType(j_str))
                    except:
                        pass

                ui_mgr.load_from_project(
                    project_path=project_dir, scheme_name=f"scheme_{project_name}", jobs=selected_jobs
                )

                movies_glob = load_result.get("movies_glob", "")
                mdocs_glob = load_result.get("mdocs_glob", "")
                ui_mgr.update_data_import(
                    project_name=project_name,
                    project_base_path=str(project_dir.parent),
                    movies_glob=movies_glob,
                    mdocs_glob=mdocs_glob,
                )
                if ui_mgr.panel_refs.project_name_input:
                    ui_mgr.panel_refs.project_name_input.set_value(project_name)
                if ui_mgr.panel_refs.project_path_input:
                    ui_mgr.panel_refs.project_path_input.set_value(str(project_dir.parent))
                if ui_mgr.panel_refs.movies_input:
                    ui_mgr.panel_refs.movies_input.set_value(movies_glob)
                if ui_mgr.panel_refs.mdocs_input:
                    ui_mgr.panel_refs.mdocs_input.set_value(mdocs_glob)

                update_movies_validation()
                update_mdocs_validation()
                refresh_params_display()
                ui.notify(f"Project '{project_name}' loaded", type="positive")
                if "rebuild_pipeline_ui" in callbacks:
                    callbacks["rebuild_pipeline_ui"]()
                if "check_and_update_statuses" in callbacks:
                    callbacks["check_and_update_statuses"]() 
            else:
                ui.notify(f"Failed to load: {load_result.get('error')}", type="negative")
        except Exception as e:
            import traceback

            traceback.print_exc()
            ui.notify(f"Error loading project: {e}", type="negative")
        finally:
            if btn:
                btn.props(remove="loading")
            update_locking_state()

    def refresh_params_display():
        container = ui_mgr.panel_refs.params_display_container
        if not container:
            return
        container.clear()
        state = get_project_state()
        has_values = state.microscope.pixel_size_angstrom > 0 or state.acquisition.dose_per_tilt > 0
        if not has_values and not ui_mgr.is_project_created:
            with container:
                ui.label("No parameters detected yet.").classes("text-xs text-gray-400 italic")
            return
        params = [
            ("Pixel Size", f"{state.microscope.pixel_size_angstrom:.3f} Å"),
            ("Voltage", f"{state.microscope.acceleration_voltage_kv:.0f} kV"),
            ("Cs", f"{state.microscope.spherical_aberration_mm:.1f} mm"),
            ("Dose/Tilt", f"{state.acquisition.dose_per_tilt:.1f} e-/A²"),
            ("Tilt Axis", f"{state.acquisition.tilt_axis_degrees:.1f}°"),
        ]
        with container:
            for label, value in params:
                with ui.row().classes("w-full justify-between py-1 border-b border-gray-50 last:border-0"):
                    ui.label(label).classes("text-xs text-gray-500")
                    ui.label(value).classes("text-xs font-medium text-gray-700 font-mono")

    # --- Input Change Handlers (FIXED) ---
    # NiceGUI input on_change receives the new value directly when using value binding,
    # but with on_change callback it receives an event. We need to handle both cases.

    def on_project_name_change(e):
        print(f"[DEBUG] on_project_name_change FIRED. e.value = '{getattr(e, 'value', e)}'")
        value = e.value if hasattr(e, 'value') else str(e) if e else ""
        ui_mgr.update_data_import(project_name=value)
        print(f"[DEBUG] After update, project_name = '{ui_mgr.data_import.project_name}'")
        update_create_button_state()

    def on_project_path_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        print(f"[DEBUG] Project path changed to: '{value}'")
        ui_mgr.update_data_import(project_base_path=value)
        update_create_button_state()

    def on_movies_change(e):
        print(f"[DEBUG] on_movies_change FIRED. Event type: {type(e)}")
        print(f"[DEBUG] Event object: {e}")
        print(f"[DEBUG] hasattr 'value': {hasattr(e, 'value')}")
        if hasattr(e, 'value'):
            print(f"[DEBUG] e.value = '{e.value}'")
        
        value = e.value if hasattr(e, 'value') else str(e) if e else ""
        print(f"[DEBUG] Extracted value: '{value}'")
        
        ui_mgr.update_data_import(movies_glob=value)
        print(f"[DEBUG] After update, ui_mgr.data_import.movies_glob = '{ui_mgr.data_import.movies_glob}'")
        
        update_movies_validation()

    def on_mdocs_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        print(f"[DEBUG] Mdocs glob changed to: '{value}'")
        ui_mgr.update_data_import(mdocs_glob=value)
        update_mdocs_validation()

    # --- BUILD UI ---
    ui_mgr.panel_refs.movies_hint_label = None
    ui_mgr.panel_refs.mdocs_hint_label = None
    ui_mgr.panel_refs.status_indicator = None

    with ui.column().classes("w-full h-full p-4 overflow-y-auto").style("font-family: 'IBM Plex Sans', sans-serif;"):
        # Project Info
        ui.label("1. Project Details").classes("text-sm font-bold text-gray-800 mb-2")
        with ui.card().classes("w-full p-5 mb-6 border border-gray-200 shadow-sm rounded-lg"):
            project_name_input = (
                ui.input(label="Project Name", value=ui_mgr.data_import.project_name, on_change=on_project_name_change)
                .props("outlined dense")
                .classes("w-full mb-4")
            )
            ui_mgr.panel_refs.project_name_input = project_name_input

            with ui.row().classes("w-full items-start gap-2"):
                project_path_input = (
                    ui.input(
                        label="Base Location",
                        value=ui_mgr.data_import.project_base_path or DEFAULT_PROJECT_BASE_PATH,
                        on_change=on_project_path_change,
                    )
                    .props("outlined dense")
                    .classes("flex-1")
                )
                ui_mgr.panel_refs.project_path_input = project_path_input
                ui.button(icon="folder_open", on_click=pick_project_path).props("flat dense").classes(
                    "mt-1 text-gray-600"
                )

            with ui.row().classes("w-full gap-3 mt-4"):
                load_btn = (
                    ui.button("Load Existing Project", icon="folder_open", on_click=handle_load_project)
                    .props("dense outline no-caps")
                    .classes("flex-1 text-gray-700")
                )
                ui_mgr.panel_refs.load_button = load_btn

        # Data Sources
        ui.label("2. Data Import").classes("text-sm font-bold text-gray-800 mb-2")
        with ui.card().classes("w-full p-5 mb-6 border border-gray-200 shadow-sm rounded-lg"):
            # Movies Input
            with ui.column().classes("w-full mb-4 gap-1"):
                ui.label("Raw Frames (EER/TIFF)").classes("text-xs font-semibold text-gray-600")
                with ui.row().classes("w-full items-start gap-2"):
                    movies_input = (
                        ui.input(
                            value=ui_mgr.data_import.movies_glob,
                            placeholder="/path/to/frames/*.eer",
                            on_change=on_movies_change,
                        )
                        .props("outlined dense")
                        .classes("flex-1")
                    )
                    ui_mgr.panel_refs.movies_input = movies_input
                    ui.button(icon="folder_open", on_click=pick_movies_path).props("flat dense").classes(
                        "mt-1 text-gray-600"
                    )
                movies_hint = ui.label("No pattern specified").classes("text-xs text-gray-400 italic")
                ui_mgr.panel_refs.movies_hint_label = movies_hint

            # Mdocs Input
            with ui.column().classes("w-full mb-4 gap-1"):
                ui.label("SerialEM Mdocs").classes("text-xs font-semibold text-gray-600")
                with ui.row().classes("w-full items-start gap-2"):
                    mdocs_input = (
                        ui.input(
                            value=ui_mgr.data_import.mdocs_glob,
                            placeholder="/path/to/mdocs/*.mdoc",
                            on_change=on_mdocs_change,
                        )
                        .props("outlined dense")
                        .classes("flex-1")
                    )
                    ui_mgr.panel_refs.mdocs_input = mdocs_input
                    ui.button(icon="folder_open", on_click=pick_mdocs_path).props("flat dense").classes(
                        "mt-1 text-gray-600"
                    )
                mdocs_hint = ui.label("No pattern specified").classes("text-xs text-gray-400 italic")
                ui_mgr.panel_refs.mdocs_hint_label = mdocs_hint

            # Autodetect Action
            with ui.row().classes("w-full items-center justify-between mt-2 pt-4 border-t border-gray-100"):
                ui.label("Scan mdocs to populate microscope params").classes("text-xs text-gray-500")
                autodetect_btn = (
                    ui.button("Autodetect Params", icon="auto_fix_high", on_click=handle_autodetect)
                    .props("dense flat no-caps")
                    .classes("text-blue-600 bg-blue-50")
                )
                ui_mgr.panel_refs.autodetect_button = autodetect_btn

        # Detected Params
        ui.label("3. Parameter Overview").classes("text-sm font-bold text-gray-800 mb-2")
        with ui.card().classes("w-full p-5 mb-6 border border-gray-200 shadow-sm rounded-lg bg-gray-50"):
            params_container = ui.column().classes("w-full")
            ui_mgr.panel_refs.params_display_container = params_container
            refresh_params_display()

        with ui.column().classes("w-full items-center gap-1"):
            status_indicator = ui.label("Waiting for input...").classes("text-xs font-bold text-gray-500 mb-1")
            ui_mgr.panel_refs.status_indicator = status_indicator

            create_btn = (
                ui.button("Create Project & Configure Pipeline", icon="arrow_forward", on_click=handle_create_project)
                .props("no-caps")
                .classes("w-full h-10 text-base shadow-md transition-all")
                .style("background: #93c5fd; color: white;")
            )
            ui_mgr.panel_refs.create_button = create_btn

    # Initialize validation state from current values (important for page reload)
    # Also sync the initial project_base_path if it has a default
    if ui_mgr.data_import.project_base_path:
        pass  # already set
    elif DEFAULT_PROJECT_BASE_PATH:
        ui_mgr.update_data_import(project_base_path=DEFAULT_PROJECT_BASE_PATH)

    update_movies_validation()
    update_mdocs_validation()
    update_create_button_state()

    def on_ui_state_change(state):
        update_locking_state()

    ui_mgr.subscribe(on_ui_state_change)
