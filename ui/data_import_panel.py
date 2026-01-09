# ui/data_import_panel.py
"""
Data import panel with debounced saves and history management.
"""

import asyncio
import glob
from pathlib import Path
from typing import Dict, Any, Callable, Optional

from nicegui import ui, app

from backend import CryoBoostBackend
from services.project_state import get_project_state
from services.user_prefs_service import get_prefs_service
from ui.ui_state import get_ui_state_manager
from ui.local_file_picker import local_file_picker


DEFAULT_MOVIES_EXT = "*.eer"
DEFAULT_MDOCS_EXT = "*.mdoc"


def build_data_import_panel(backend: CryoBoostBackend, callbacks: Dict[str, Callable]) -> None:
    """Build the data import panel."""
    ui_mgr = get_ui_state_manager()
    prefs_service = get_prefs_service()

    # Local state
    local_refs = {
        "recent_projects_container": None,
        "history_container": None,
        "default_movies_ext": "*.eer",
        "default_mdocs_ext": "*.mdoc",
        "save_timer": None,
        "scan_timer": None,
        "last_scanned_path": None,  # Track what we last scanned to avoid duplicates
    }

    # =========================================================================
    # DEBOUNCE HELPERS
    # =========================================================================

    def debounced_save():
        """Save preferences 500ms after last change"""
        if local_refs["save_timer"]:
            local_refs["save_timer"].cancel()

        def do_save():
            prefs_service.save_to_app_storage(app.storage.user)
            local_refs["save_timer"] = None

        local_refs["save_timer"] = ui.timer(0.5, do_save, once=True)

    def debounced_scan(path: str):
        """Scan for projects 800ms after user stops typing"""
        if local_refs["scan_timer"]:
            local_refs["scan_timer"].cancel()

        def do_scan():
            # Use the path that was passed in, not stale state
            asyncio.create_task(scan_and_display_projects(path))
            local_refs["scan_timer"] = None

        local_refs["scan_timer"] = ui.timer(0.8, do_scan, once=True)

    # =========================================================================
    # VALIDATION
    # =========================================================================

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
            input_el.props("error=false error-message=''")
        else:
            input_el.props(f"error=true error-message='{message}'")

    def update_movies_validation():
        pattern = ui_mgr.data_import.movies_glob
        is_valid, count, msg = validate_glob_pattern(pattern)
        ui_mgr.update_data_import(movies_valid=is_valid)

        if ui_mgr.panel_refs.movies_input:
            update_input_validation(ui_mgr.panel_refs.movies_input, is_valid, msg)
        if ui_mgr.panel_refs.movies_hint_label:
            ui_mgr.panel_refs.movies_hint_label.set_text(msg)
            if is_valid:
                ui_mgr.panel_refs.movies_hint_label.classes(remove="text-red-500 text-gray-500").classes(
                    "text-green-600"
                )
            else:
                ui_mgr.panel_refs.movies_hint_label.classes(remove="text-green-600 text-gray-500").classes(
                    "text-red-500"
                )
        update_create_button_state()

    def update_mdocs_validation():
        pattern = ui_mgr.data_import.mdocs_glob
        is_valid, count, msg = validate_glob_pattern(pattern)
        ui_mgr.update_data_import(mdocs_valid=is_valid)

        if ui_mgr.panel_refs.mdocs_input:
            update_input_validation(ui_mgr.panel_refs.mdocs_input, is_valid, msg)
        if ui_mgr.panel_refs.mdocs_hint_label:
            ui_mgr.panel_refs.mdocs_hint_label.set_text(msg)
            if is_valid:
                ui_mgr.panel_refs.mdocs_hint_label.classes(remove="text-red-500 text-gray-500").classes(
                    "text-green-600"
                )
            else:
                ui_mgr.panel_refs.mdocs_hint_label.classes(remove="text-green-600 text-gray-500").classes(
                    "text-red-500"
                )
        update_create_button_state()

    def get_missing_requirements() -> list[str]:
        sync_state_from_inputs()
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
        return len(get_missing_requirements()) == 0

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
        for ref in [ui_mgr.panel_refs.autodetect_button, ui_mgr.panel_refs.movies_input, ui_mgr.panel_refs.mdocs_input]:
            if ref:
                if is_running:
                    ref.disable()
                else:
                    ref.enable()
        update_create_button_state()

    # =========================================================================
    # SYNC STATE FROM INPUTS (fixes browser autocomplete)
    # =========================================================================

    def sync_state_from_inputs():
        refs = ui_mgr.panel_refs
        changed = False

        for attr, ref, updater in [
            ("project_name", refs.project_name_input, lambda v: ui_mgr.update_data_import(project_name=v)),
            ("project_base_path", refs.project_path_input, lambda v: ui_mgr.update_data_import(project_base_path=v)),
            ("movies_glob", refs.movies_input, lambda v: ui_mgr.update_data_import(movies_glob=v)),
            ("mdocs_glob", refs.mdocs_input, lambda v: ui_mgr.update_data_import(mdocs_glob=v)),
        ]:
            if ref and hasattr(ref, "value"):
                current = ref.value or ""
                if current != getattr(ui_mgr.data_import, attr):
                    updater(current)
                    changed = True

        if changed:
            update_movies_validation()
            update_mdocs_validation()
            update_create_button_state()

    # =========================================================================
    # PROJECT SCANNING
    # =========================================================================

    async def scan_and_display_projects(base_path: str):
        """Scan a specific path and display results"""
        container = local_refs["recent_projects_container"]
        if not container:
            return
        
        # Avoid duplicate scans
        if local_refs["last_scanned_path"] == base_path:
            return
        local_refs["last_scanned_path"] = base_path
        
        container.clear()
        
        if not base_path or not base_path.strip():
            with container:
                ui.label("Enter a base location").classes("text-xs text-gray-400 italic p-2")
            return
        
        path_obj = Path(base_path)
        if not path_obj.exists():
            with container:
                ui.label("Path does not exist").classes("text-xs text-gray-400 italic p-2")
            return
        if not path_obj.is_dir():
            with container:
                ui.label("Path is not a directory").classes("text-xs text-gray-400 italic p-2")
            return

        with container:
            ui.spinner("dots").classes("self-center")
        
        projects = await backend.scan_for_projects(base_path)
        
        container.clear()
        with container:
            if not projects:
                ui.label(f"No projects in {path_obj.name}").classes("text-xs text-gray-400 italic p-2")
            else:
                for proj in projects:
                    # Create a proper closure for the click handler
                    def make_load_handler(path_str: str):
                        async def handler():
                            await handle_load_project(Path(path_str))
                        return handler
                    
                    with ui.card().classes("w-full p-2 mb-2 border border-gray-200 cursor-pointer hover:bg-blue-50 transition-colors"):
                        with ui.row().classes("w-full items-center justify-between no-wrap"):
                            with ui.column().classes("gap-0"):
                                ui.label(proj["name"]).classes("text-sm font-bold text-gray-700")
                                ui.label(f"Modified: {proj['modified']}").classes("text-[10px] text-gray-400")
                            ui.button(icon="arrow_forward", on_click=make_load_handler(proj["path"])).props("flat dense round size=sm").classes("text-blue-500")

    # =========================================================================
    # HISTORY UI
    # =========================================================================

    def refresh_history_ui():
        """Refresh the history display"""
        container = local_refs["history_container"]
        if not container:
            return

        container.clear()
        roots = prefs_service.prefs.recent_project_roots

        with container:
            if not roots:
                ui.label("No saved locations").classes("text-xs text-gray-400 italic")
            else:
                for root in roots[:10]:  # Show max 10
                    with ui.row().classes("w-full items-center gap-2 py-1 border-b border-gray-100"):
                        with ui.column().classes("flex-1 gap-0 min-w-0"):
                            if root.label:
                                ui.label(root.label).classes("text-xs font-medium text-gray-700 truncate")
                            ui.label(root.path).classes("text-[10px] text-gray-400 font-mono truncate")

                        # Use this location button
                        ui.button(icon="arrow_forward", on_click=lambda p=root.path: use_history_path(p)).props(
                            "flat dense round size=xs"
                        ).classes("text-blue-500")

                        # Delete button
                        ui.button(icon="close", on_click=lambda p=root.path: remove_history_path(p)).props(
                            "flat dense round size=xs"
                        ).classes("text-gray-400 hover:text-red-500")

    def use_history_path(path: str):
        """Use a path from history"""
        ui_mgr.update_data_import(project_base_path=path)
        if ui_mgr.panel_refs.project_path_input:
            ui_mgr.panel_refs.project_path_input.value = path
        prefs_service.update_fields(project_base_path=path)
        debounced_save()
        # Force immediate scan
        local_refs["last_scanned_path"] = None
        asyncio.create_task(scan_and_display_projects(path))

    def remove_history_path(path: str):
        """Remove a path from history"""
        prefs_service.prefs.remove_recent_root(path)
        prefs_service.save_to_app_storage(app.storage.user)
        refresh_history_ui()

    def clear_all_history():
        """Clear all history"""
        prefs_service.prefs.clear_recent_roots()
        prefs_service.save_to_app_storage(app.storage.user)
        refresh_history_ui()
        ui.notify("History cleared", type="info")

    # =========================================================================
    # FILE PICKERS
    # =========================================================================

    async def pick_movies_path():
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if result and len(result) > 0:
            selected_dir = Path(result[0])
            pattern = str(selected_dir / local_refs["default_movies_ext"])
            ui_mgr.update_data_import(movies_glob=pattern)
            if ui_mgr.panel_refs.movies_input:
                ui_mgr.panel_refs.movies_input.value = pattern
            update_movies_validation()
            prefs_service.update_fields(movies_glob=pattern)
            debounced_save()

    async def pick_mdocs_path():
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if result and len(result) > 0:
            selected_dir = Path(result[0])
            pattern = str(selected_dir / local_refs["default_mdocs_ext"])
            ui_mgr.update_data_import(mdocs_glob=pattern)
            if ui_mgr.panel_refs.mdocs_input:
                ui_mgr.panel_refs.mdocs_input.value = pattern
            update_mdocs_validation()
            prefs_service.update_fields(mdocs_glob=pattern)
            debounced_save()

    async def pick_project_path():
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if result and len(result) > 0:
            dir_path = result[0]
            ui_mgr.update_data_import(project_base_path=dir_path)
            if ui_mgr.panel_refs.project_path_input:
                ui_mgr.panel_refs.project_path_input.value = dir_path
            update_locking_state()

            # Explicit picker action: save immediately AND add to history
            prefs_service.update_fields(project_base_path=dir_path)
            # Check if it has projects before adding to history
            projects = await backend.scan_for_projects(dir_path)
            if projects:
                prefs_service.prefs.add_recent_root(dir_path)
            prefs_service.save_to_app_storage(app.storage.user)
            refresh_history_ui()

            # Scan immediately
            local_refs["last_scanned_path"] = None
            await scan_and_display_projects(dir_path)

    # =========================================================================
    # INPUT HANDLERS
    # =========================================================================

    def on_project_name_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        ui_mgr.update_data_import(project_name=value or "")
        update_create_button_state()

    def on_project_path_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        value = value or ""
        ui_mgr.update_data_import(project_base_path=value)
        update_create_button_state()

        # Debounced save (fields only, not history)
        prefs_service.update_fields(project_base_path=value)
        debounced_save()

        # Debounced scan - pass the current value explicitly
        debounced_scan(value)

    def on_project_path_blur(e):
        """On blur: add to history ONLY if valid and contains projects"""
        value = ui_mgr.data_import.project_base_path
        if not value or not value.strip():
            return

        path = Path(value)
        if not path.is_absolute() or not path.exists() or not path.is_dir():
            return

        # Check if it contains projects before adding to history
        async def check_and_add():
            projects = await backend.scan_for_projects(value)
            if projects:
                prefs_service.prefs.add_recent_root(value)
                prefs_service.save_to_app_storage(app.storage.user)
                refresh_history_ui()

        asyncio.create_task(check_and_add())

    def on_movies_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        ui_mgr.update_data_import(movies_glob=value or "")
        update_movies_validation()
        prefs_service.update_fields(movies_glob=value or "")
        debounced_save()

    def on_mdocs_change(e):
        value = e.value if hasattr(e, "value") else str(e) if e else ""
        ui_mgr.update_data_import(mdocs_glob=value or "")
        update_mdocs_validation()
        prefs_service.update_fields(mdocs_glob=value or "")
        debounced_save()

    # =========================================================================
    # ACTION HANDLERS
    # =========================================================================

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
        sync_state_from_inputs()
        update_movies_validation()
        update_mdocs_validation()
        if not can_create_project():
            ui.notify(f"Cannot create: Missing {get_missing_requirements()}", type="warning")
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

                # Add to history with project name as label
                prefs_service.prefs.add_recent_root(di.project_base_path, label=di.project_name)
                prefs_service.save_to_app_storage(app.storage.user)

                if "rebuild_pipeline_ui" in callbacks:
                    callbacks["rebuild_pipeline_ui"]()
            else:
                ui.notify(f"Failed: {result.get('error')}", type="negative")
        except Exception as e:
            import traceback

            traceback.print_exc()
            ui.notify(f"Error creating project: {e}", type="negative")
        finally:
            if btn:
                btn.props(remove="loading")
            update_locking_state()

    async def handle_load_project_click():
        picker = local_file_picker(directory="~", mode="directory")
        result = await picker
        if result and len(result) > 0:
            await handle_load_project(Path(result[0]))

    async def handle_load_project(project_dir: Path):
        params_file = project_dir / "project_params.json"
        if not params_file.exists():
            ui.notify("No project_params.json found in directory", type="warning")
            return

        btn = ui_mgr.panel_refs.load_button
        if btn:
            btn.props("loading")

        try:
            # 1. Load via backend (updates ProjectState)
            load_result = await backend.load_existing_project(str(project_dir))
            
            if load_result.get("success"):
                # 2. Wait for status sync to complete
                await backend.pipeline_runner.status_sync.sync_all_jobs(str(project_dir))
                
                # 3. Get the ACTUAL state from backend (single source of truth)
                state = backend.state_service.state
                
                # 4. Sync UI state with backend state
                ui_mgr.load_from_project(
                    project_path=state.project_path,  # Use backend's path, not our argument
                    scheme_name=f"scheme_{state.project_name}",
                    jobs=list(state.jobs.keys())  # Use actual jobs from state
                )
                
                # 5. Update data import form with loaded values
                ui_mgr.update_data_import(
                    project_name=state.project_name,
                    project_base_path=str(state.project_path.parent) if state.project_path else "",
                    movies_glob=state.movies_glob,
                    mdocs_glob=state.mdocs_glob,
                )

                # 6. Save preferences
                prefs_service.update_fields(
                    project_base_path=str(state.project_path.parent) if state.project_path else "",
                    movies_glob=state.movies_glob,
                    mdocs_glob=state.mdocs_glob,
                )
                prefs_service.prefs.add_recent_root(
                    str(state.project_path.parent) if state.project_path else "", 
                    label=state.project_name
                )
                prefs_service.save_to_app_storage(app.storage.user)

                ui.notify(f"Project '{state.project_name}' loaded", type="positive")
                
                # 7. Small delay to ensure state propagates
                await asyncio.sleep(0.1)
                
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

        d_pix = ui_mgr.data_import.detected_pixel_size
        d_dose = ui_mgr.data_import.detected_dose_per_tilt
        has_detection = d_pix is not None or d_dose is not None

        if not has_detection:
            with container:
                ui.icon("radar", size="24px").classes("text-gray-300 self-center mb-1")
                ui.label("Import MDOCs to detect parameters").classes("text-xs text-gray-400 italic text-center")
            return

        state = get_project_state()
        params = [
            ("Pixel Size", f"{state.microscope.pixel_size_angstrom:.3f} A"),
            ("Voltage", f"{state.microscope.acceleration_voltage_kv:.0f} kV"),
            ("Cs", f"{state.microscope.spherical_aberration_mm:.1f} mm"),
            ("Dose/Tilt", f"{state.acquisition.dose_per_tilt:.1f} e-/A^2"),
            ("Tilt Axis", f"{state.acquisition.tilt_axis_degrees:.1f} deg"),
        ]
        with container:
            for label, value in params:
                with ui.row().classes("w-full justify-between py-1 border-b border-gray-100 last:border-0"):
                    ui.label(label).classes("text-xs text-gray-500")
                    ui.label(value).classes("text-xs font-medium text-gray-700 font-mono")

    # =========================================================================
    # INIT
    # =========================================================================

    async def init_defaults():
        prefs = prefs_service.prefs

        defaults = await backend.get_default_data_globs()
        config_movies = defaults.get("movies", "")
        config_mdocs = defaults.get("mdocs", "")

        if config_movies and "*" in config_movies:
            local_refs["default_movies_ext"] = "*" + config_movies.split("*")[-1]
        if config_mdocs and "*" in config_mdocs:
            local_refs["default_mdocs_ext"] = "*" + config_mdocs.split("*")[-1]

        # Apply saved prefs or config defaults
        if not ui_mgr.data_import.project_base_path:
            saved_path = prefs.project_base_path
            if saved_path:
                ui_mgr.update_data_import(project_base_path=saved_path)
            else:
                default_path = await backend.get_default_project_base()
                ui_mgr.update_data_import(project_base_path=default_path)

            if ui_mgr.panel_refs.project_path_input:
                ui_mgr.panel_refs.project_path_input.value = ui_mgr.data_import.project_base_path

        if not ui_mgr.data_import.movies_glob:
            glob_to_use = prefs.movies_glob or config_movies
            if glob_to_use:
                ui_mgr.update_data_import(movies_glob=glob_to_use)
                if ui_mgr.panel_refs.movies_input:
                    ui_mgr.panel_refs.movies_input.value = glob_to_use
                update_movies_validation()

        if not ui_mgr.data_import.mdocs_glob:
            glob_to_use = prefs.mdocs_glob or config_mdocs
            if glob_to_use:
                ui_mgr.update_data_import(mdocs_glob=glob_to_use)
                if ui_mgr.panel_refs.mdocs_input:
                    ui_mgr.panel_refs.mdocs_input.value = glob_to_use
                update_mdocs_validation()

        # Initial scan
        await scan_and_display_projects(ui_mgr.data_import.project_base_path)

        # Refresh history UI
        refresh_history_ui()

    # =========================================================================
    # LAYOUT
    # =========================================================================

    # Clear old refs
    ui_mgr.panel_refs.movies_hint_label = None
    ui_mgr.panel_refs.mdocs_hint_label = None
    ui_mgr.panel_refs.status_indicator = None

    with ui.column().classes("w-full p-6 gap-6").style("font-family: 'IBM Plex Sans', sans-serif;"):
        with ui.row().classes("w-full gap-8"):
            # --- LEFT COLUMN: NEW PROJECT SETUP ---
            with ui.column().classes("flex-1 gap-4"):
                ui.label("Start New Project").classes(
                    "text-sm font-bold text-slate-800 uppercase tracking-wider border-b border-slate-200 w-full pb-1"
                )

                # Project Identity
                with ui.card().classes("w-full p-4 border border-gray-200 shadow-none bg-white rounded-lg gap-3"):
                    with ui.column().classes("w-full gap-1"):
                        ui.label("Project Name").classes("text-xs font-semibold text-gray-600")
                        project_name_input = (
                            ui.input(value=ui_mgr.data_import.project_name, on_change=on_project_name_change)
                            .props("outlined dense placeholder='e.g., HIV_Tomo_Batch1'")
                            .classes("w-full")
                        )
                        project_name_input.on("blur", lambda e: sync_state_from_inputs())
                        ui_mgr.panel_refs.project_name_input = project_name_input

                    with ui.column().classes("w-full gap-1"):
                        ui.label("Base Location").classes("text-xs font-semibold text-gray-600")
                        with ui.row().classes("w-full items-center gap-2"):
                            project_path_input = (
                                ui.input(value=ui_mgr.data_import.project_base_path, on_change=on_project_path_change)
                                .props("outlined dense")
                                .classes("flex-1")
                            )
                            project_path_input.on("blur", on_project_path_blur)
                            ui_mgr.panel_refs.project_path_input = project_path_input
                            ui.button(icon="folder", on_click=pick_project_path).props("flat dense round color=grey")

                # Data Sources
                with ui.card().classes("w-full p-4 border border-gray-200 shadow-none bg-white rounded-lg gap-3"):
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
                            movies_input.on("blur", lambda e: sync_state_from_inputs())
                            ui_mgr.panel_refs.movies_input = movies_input
                            ui.button(icon="folder", on_click=pick_movies_path).props("flat dense round color=grey")

                        movies_hint = ui.label("No pattern").classes("text-[10px] text-gray-400 pl-1")
                        ui_mgr.panel_refs.movies_hint_label = movies_hint

                    with ui.column().classes("w-full gap-1"):
                        ui.label("SerialEM Mdocs").classes("text-xs font-semibold text-gray-600")
                        with ui.row().classes("w-full items-center gap-2"):
                            mdocs_input = (
                                ui.input(
                                    value=ui_mgr.data_import.mdocs_glob, placeholder="*.mdoc", on_change=on_mdocs_change
                                )
                                .props("outlined dense")
                                .classes("flex-1")
                            )
                            mdocs_input.on("blur", lambda e: sync_state_from_inputs())
                            ui_mgr.panel_refs.mdocs_input = mdocs_input
                            ui.button(icon="folder", on_click=pick_mdocs_path).props("flat dense round color=grey")

                        mdocs_hint = ui.label("No pattern").classes("text-[10px] text-gray-400 pl-1")
                        ui_mgr.panel_refs.mdocs_hint_label = mdocs_hint

                    with ui.row().classes("w-full justify-end pt-1"):
                        autodetect_btn = (
                            ui.button("Autodetect Parameters", icon="auto_fix_high", on_click=handle_autodetect)
                            .props("dense flat no-caps size=sm")
                            .classes("text-blue-600 hover:bg-blue-50")
                        )
                        ui_mgr.panel_refs.autodetect_button = autodetect_btn

                # Detected Parameters
                with ui.card().classes("w-full p-4 border border-blue-100 bg-blue-50/30 shadow-none rounded-lg"):
                    ui.label("Detected Configuration").classes(
                        "text-xs font-bold text-blue-800 uppercase tracking-wide mb-2"
                    )
                    params_container = ui.column().classes("w-full gap-1")
                    ui_mgr.panel_refs.params_display_container = params_container
                    refresh_params_display()

            # --- RIGHT COLUMN: RESUME EXISTING ---
            with ui.column().classes("w-80 gap-4"):
                ui.label("Resume Existing").classes(
                    "text-sm font-bold text-slate-800 uppercase tracking-wider border-b border-slate-200 w-full pb-1"
                )

                # Projects in current location
                with ui.card().classes(
                    "w-full flex flex-col p-0 border border-gray-200 shadow-none bg-gray-50 rounded-lg overflow-hidden"
                ):
                    ui.label("Projects in Base Location").classes(
                        "text-xs font-semibold text-gray-500 p-3 bg-gray-100 border-b border-gray-200"
                    )

                    with ui.scroll_area().classes("w-full p-2 h-48 bg-white"):
                        local_refs["recent_projects_container"] = ui.column().classes("w-full gap-1")

                    with ui.row().classes("w-full p-3 border-t border-gray-200 bg-white"):
                        load_btn = (
                            ui.button("Browse Disk...", icon="folder_open", on_click=handle_load_project_click)
                            .props("outline no-caps dense")
                            .classes("w-full text-slate-600")
                        )
                        ui_mgr.panel_refs.load_button = load_btn

                # Recent Locations History
                with ui.card().classes(
                    "w-full flex flex-col p-0 border border-gray-200 shadow-none bg-gray-50 rounded-lg overflow-hidden"
                ):
                    with ui.row().classes(
                        "w-full items-center justify-between p-3 bg-gray-100 border-b border-gray-200"
                    ):
                        ui.label("Recent Locations").classes("text-xs font-semibold text-gray-500")
                        ui.button(icon="delete_sweep", on_click=clear_all_history).props(
                            "flat dense round size=xs"
                        ).classes("text-gray-400 hover:text-red-500").tooltip("Clear all history")

                    with ui.scroll_area().classes("w-full p-2 h-40 bg-white"):
                        local_refs["history_container"] = ui.column().classes("w-full gap-0")

        # --- BOTTOM ACTION BAR ---
        with ui.row().classes("w-full mt-4 pt-4 border-t border-gray-200 justify-between items-center"):
            status_indicator = ui.label("Enter details to begin...").classes("text-sm text-gray-500 font-medium")
            ui_mgr.panel_refs.status_indicator = status_indicator

            create_btn = (
                ui.button("Create Project", icon="add_circle", on_click=handle_create_project)
                .props("no-caps size=lg")
                .classes("px-8 shadow-md transition-all duration-200")
                .style("background: #93c5fd; color: white;")
            )
            ui_mgr.panel_refs.create_button = create_btn

    update_movies_validation()
    update_mdocs_validation()
    update_create_button_state()
    ui_mgr.subscribe(lambda state: update_locking_state())

    ui.timer(0.1, init_defaults, once=True)
    ui.timer(1.0, sync_state_from_inputs)  # Catch browser autocomplete
