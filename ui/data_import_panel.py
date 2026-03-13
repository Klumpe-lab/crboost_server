# ui/data_import_panel.py
"""
Data import panel with debounced saves and history management.
"""

import asyncio
import glob
from pathlib import Path
from typing import Dict, Callable

from nicegui import ui, app

from backend import CryoBoostBackend
from services.configs.user_prefs_service import get_prefs_service
from services.project_state import get_project_state
from ui.ui_state import get_ui_state_manager
from ui.local_file_picker import local_file_picker
from ui.glob_directory_input import GlobDirectoryInput


DEFAULT_MOVIES_EXT = "*.eer"
DEFAULT_MDOCS_EXT = "*.mdoc"

FONT = "font-family: 'IBM Plex Sans', sans-serif;"
MONO = "font-family: 'IBM Plex Mono', monospace;"
LABEL = "font-family: system-ui, -apple-system, sans-serif;"

# Blue-tinted slate palette -- feels more like scientific instrument UI
# than pure neutral grays
CLR_HEADING = "#0f172a"  # slate-950: dark, authoritative
CLR_LABEL = "#475569"  # slate-600
CLR_SUBLABEL = "#94a3b8"  # slate-400
CLR_GHOST = "#cbd5e1"  # slate-300
CLR_BORDER = "#e2e8f0"  # slate-200, slightly blue-tinted
CLR_ACCENT = "#2563eb"  # blue-600
CLR_ACCENT_LIGHT = "#dbeafe"
CLR_ACCENT_TEXT = "#1e40af"
CLR_META = "#64748b"  # slate-500 -- readable metadata
CLR_SUCCESS = "#0d9488"  # teal-600: academic, not shouty green
CLR_ERROR = "#be4343"  # muted red, not aggressive


def build_data_import_panel(backend: CryoBoostBackend, callbacks: Dict[str, Callable]) -> None:
    ui_mgr = get_ui_state_manager()
    prefs_service = get_prefs_service()

    local_refs = {
        "recent_projects_container": None,
        "history_container": None,
        "history_dropdown_el": None,
        "history_dropdown_visible": False,
        "projects_path_label": None,
        "default_movies_ext": "*.eer",
        "default_mdocs_ext": "*.mdoc",
        "save_timer": None,
        "scan_timer": None,
    }

    # =========================================================================
    # DEBOUNCE
    # =========================================================================

    def debounced_save():
        if local_refs["save_timer"]:
            local_refs["save_timer"].cancel()

        def do_save():
            prefs_service.save_to_app_storage(app.storage.user)
            local_refs["save_timer"] = None

        local_refs["save_timer"] = ui.timer(0.5, do_save, once=True)

    def debounced_scan(path: str):
        if local_refs["scan_timer"]:
            local_refs["scan_timer"].cancel()

        def do_scan():
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
            color = CLR_SUCCESS if is_valid else CLR_ERROR
            ui_mgr.panel_refs.movies_hint_label.style(f"{FONT} font-size: 9px; color: {color}; padding-left: 2px;")
        update_create_button_state()

    def update_mdocs_validation():
        pattern = ui_mgr.data_import.mdocs_glob
        is_valid, count, msg = validate_glob_pattern(pattern)
        ui_mgr.update_data_import(mdocs_valid=is_valid)
        if ui_mgr.panel_refs.mdocs_input:
            update_input_validation(ui_mgr.panel_refs.mdocs_input, is_valid, msg)
        if ui_mgr.panel_refs.mdocs_hint_label:
            ui_mgr.panel_refs.mdocs_hint_label.set_text(msg)
            color = CLR_SUCCESS if is_valid else CLR_ERROR
            ui_mgr.panel_refs.mdocs_hint_label.style(f"{FONT} font-size: 9px; color: {color}; padding-left: 2px;")
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
        if not btn:
            return
        if len(missing) == 0 and not ui_mgr.is_project_created:
            btn.enable()
            btn.classes(remove="opacity-50 cursor-not-allowed")
            btn.style(
                f"{FONT} font-size: 11px; font-weight: 500; padding: 4px 16px; "
                f"border-radius: 6px; background: {CLR_ACCENT}; color: white; "
                "letter-spacing: 0.01em;"
            )
            if status_label:
                status_label.set_text("Ready to create")
                status_label.style(f"{FONT} font-size: 10px; color: {CLR_SUCCESS};")
        else:
            btn.disable()
            btn.classes("opacity-50 cursor-not-allowed")
            btn.style(
                f"{FONT} font-size: 11px; font-weight: 500; padding: 4px 16px; "
                "border-radius: 6px; background: #93c5fd; color: white; "
                "letter-spacing: 0.01em;"
            )
            if status_label:
                if ui_mgr.is_project_created:
                    status_label.set_text("Project created")
                    status_label.style(f"{FONT} font-size: 10px; color: {CLR_SUCCESS};")
                else:
                    status_label.set_text(f"Missing: {', '.join(missing)}")
                    status_label.style(f"{FONT} font-size: 10px; color: {CLR_ERROR};")

    def update_locking_state():
        is_running = ui_mgr.is_running
        for ref in [ui_mgr.panel_refs.autodetect_button, ui_mgr.panel_refs.movies_input, ui_mgr.panel_refs.mdocs_input]:
            if ref:
                ref.disable() if is_running else ref.enable()
        update_create_button_state()

    # =========================================================================
    # SYNC
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
    # HISTORY DROPDOWN
    # =========================================================================

    def toggle_history_dropdown():
        el = local_refs["history_dropdown_el"]
        if el is None:
            return
        if local_refs["history_dropdown_visible"]:
            _close_history_dropdown()
        else:
            refresh_history_ui()
            el.style(remove="display: none;")
            el.style("display: block;")
            local_refs["history_dropdown_visible"] = True

    def _close_history_dropdown():
        el = local_refs["history_dropdown_el"]
        if el:
            el.style(remove="display: block;")
            el.style("display: none;")
        local_refs["history_dropdown_visible"] = False

    def refresh_history_ui():
        container = local_refs["history_container"]
        if not container:
            return
        container.clear()
        roots = prefs_service.prefs.recent_project_roots
        with container:
            if not roots:
                ui.label("No saved locations").style(
                    f"{FONT} font-size: 10px; color: {CLR_GHOST}; font-style: italic; padding: 8px 12px;"
                )
            else:
                for root in roots[:10]:
                    with ui.row().classes("w-full items-center gap-1 py-1 px-3 hover:bg-slate-50 transition-colors"):
                        ui.label(root.path).style(f"{MONO} font-size: 10px; color: {CLR_LABEL};").classes(
                            "flex-1 truncate min-w-0"
                        )
                        ui.button(icon="arrow_forward", on_click=lambda p=root.path: use_history_path(p)).props(
                            "flat dense round size=xs"
                        ).classes("text-blue-400 hover:text-blue-600 shrink-0")
                        ui.button(icon="close", on_click=lambda p=root.path: remove_history_path(p)).props(
                            "flat dense round size=xs"
                        ).classes("text-slate-300 hover:text-red-400 shrink-0")
            # Clear all footer
            with ui.row().classes("w-full justify-end px-3 py-1.5 border-t border-slate-100"):
                ui.button("Clear all", on_click=clear_all_history).props("flat dense no-caps").style(
                    f"{FONT} font-size: 10px; color: {CLR_SUBLABEL};"
                )

    def use_history_path(path: str):
        _close_history_dropdown()
        ui_mgr.update_data_import(project_base_path=path)
        if ui_mgr.panel_refs.project_path_input:
            ui_mgr.panel_refs.project_path_input.value = path
        if local_refs["projects_path_label"]:
            local_refs["projects_path_label"].set_text(path or "no location set")
        prefs_service.update_fields(project_base_path=path)
        debounced_save()
        asyncio.create_task(scan_and_display_projects(path))

    def remove_history_path(path: str):
        prefs_service.prefs.remove_recent_root(path)
        prefs_service.save_to_app_storage(app.storage.user)
        refresh_history_ui()

    def clear_all_history():
        prefs_service.prefs.clear_recent_roots()
        prefs_service.save_to_app_storage(app.storage.user)
        refresh_history_ui()
        ui.notify("History cleared", type="info")

    # =========================================================================
    # PROJECT SCANNING
    # =========================================================================

    async def scan_and_display_projects(base_path: str):
        container = local_refs["recent_projects_container"]
        if not container:
            return

        if local_refs["projects_path_label"]:
            local_refs["projects_path_label"].set_text(
                base_path if base_path and base_path.strip() else "no location set"
            )

        container.clear()

        if not base_path or not base_path.strip():
            with container:
                ui.label("Set a base location to scan for projects").style(
                    f"{FONT} font-size: 10px; color: {CLR_GHOST}; font-style: italic; padding: 6px 0;"
                )
            return

        path_obj = Path(base_path)
        if not path_obj.exists():
            with container:
                ui.label("Path does not exist").style(
                    f"{FONT} font-size: 10px; color: {CLR_GHOST}; font-style: italic; padding: 6px 0;"
                )
            return
        if not path_obj.is_dir():
            with container:
                ui.label("Not a directory").style(
                    f"{FONT} font-size: 10px; color: {CLR_GHOST}; font-style: italic; padding: 6px 0;"
                )
            return

        with container:
            ui.spinner("dots").classes("self-center my-1")

        projects = await backend.scan_for_projects(base_path)

        container.clear()
        with container:
            if not projects:
                ui.label(f"No projects found in {path_obj.name}/").style(
                    f"{FONT} font-size: 10px; color: {CLR_GHOST}; font-style: italic; padding: 6px 0;"
                )
            else:
                for proj in projects:

                    def make_load_handler(path_str: str):
                        async def handler():
                            await handle_load_project(Path(path_str))

                        return handler

                    def make_delete_handler(path_str: str, name: str):
                        async def handler():
                            await _confirm_delete_project(Path(path_str), name)

                        return handler

                    with (
                        ui.row()
                        .classes(
                            "w-full items-center py-1.5 px-5 gap-2 "
                            "hover:bg-slate-50 transition-colors cursor-pointer group"
                        )
                        .style("min-height: 30px;")
                    ):
                        with ui.column().classes("flex-1 gap-0 min-w-0").on("click", make_load_handler(proj["path"])):
                            ui.label(proj["name"]).style(
                                f"{FONT} font-size: 11px; font-weight: 500; color: {CLR_HEADING};"
                            ).classes("truncate")

                            meta_parts = []
                            if proj.get("created_at"):
                                meta_parts.append(proj["created_at"])
                            if proj.get("creator"):
                                meta_parts.append(proj["creator"])
                            meta_str = "  \u00b7  ".join(meta_parts) if meta_parts else proj["modified"]
                            ui.label(meta_str).style(f"{MONO} font-size: 9px; color: {CLR_META};")

                        ui.button(
                            icon="delete_outline", on_click=make_delete_handler(proj["path"], proj["name"])
                        ).props("flat dense round size=xs").classes(
                            "text-slate-200 hover:text-red-400 "
                            "opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
                        )
                        ui.button(icon="arrow_forward", on_click=make_load_handler(proj["path"])).props(
                            "flat dense round size=xs"
                        ).classes("text-slate-300 hover:text-blue-500 shrink-0")

    async def _confirm_delete_project(project_dir: Path, project_name: str):
        with ui.dialog() as dialog, ui.card().classes("w-96"):
            ui.label(f"Delete '{project_name}'?").style(
                f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};"
            )
            ui.label("This will permanently remove the project directory and all its contents.").style(
                f"{FONT} font-size: 12px; color: {CLR_LABEL}; margin-top: 4px;"
            )
            ui.label(str(project_dir)).style(
                f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; margin-top: 6px; "
                "padding: 5px 7px; background: #f8fafc; border-radius: 4px; "
                "word-break: break-all;"
            )
            with ui.row().classes("w-full justify-end mt-3 gap-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat no-caps").style(f"{FONT} font-size: 12px;")

                async def do_delete():
                    dialog.close()
                    try:
                        import shutil

                        shutil.rmtree(project_dir)
                        from services.project_state import remove_project_state

                        remove_project_state(project_dir)
                        ui.notify(f"Deleted '{project_name}'", type="positive")

                        async def _rescan():
                            await scan_and_display_projects(ui_mgr.data_import.project_base_path)

                        ui.timer(0.1, lambda: asyncio.create_task(_rescan()), once=True)
                    except Exception as e:
                        ui.notify(f"Failed to delete: {e}", type="negative")

                ui.button("Delete permanently", on_click=do_delete).props("no-caps unelevated").style(
                    f"{FONT} font-size: 12px; background: {CLR_ERROR}; color: white; "
                    "border-radius: 6px; padding: 3px 14px;"
                )
        dialog.open()

    # =========================================================================
    # FILE PICKERS
    # =========================================================================

    async def pick_movies_path():
        start = ui_mgr.data_import.movies_glob
        start_dir = str(Path(start).parent) if start and "*" in start else (start or "~")
        result = await local_file_picker(directory=start_dir, mode="directory")
        if result:
            if ui_mgr.panel_refs.movies_input:
                ui_mgr.panel_refs.movies_input.set_directory(result[0])

    async def pick_mdocs_path():
        start = ui_mgr.data_import.mdocs_glob
        start_dir = str(Path(start).parent) if start and "*" in start else (start or "~")
        result = await local_file_picker(directory=start_dir, mode="directory")
        if result:
            if ui_mgr.panel_refs.mdocs_input:
                ui_mgr.panel_refs.mdocs_input.set_directory(result[0])

    async def pick_project_path():
        start = ui_mgr.data_import.project_base_path or "~"
        result = await local_file_picker(directory=start, mode="directory")
        if result and len(result) > 0:
            dir_path = result[0]
            ui_mgr.update_data_import(project_base_path=dir_path)
            if ui_mgr.panel_refs.project_path_input:
                ui_mgr.panel_refs.project_path_input.value = dir_path
            update_locking_state()
            prefs_service.update_fields(project_base_path=dir_path)
            projects = await backend.scan_for_projects(dir_path)
            if projects:
                prefs_service.prefs.add_recent_root(dir_path)
            prefs_service.save_to_app_storage(app.storage.user)
            refresh_history_ui()
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
        prefs_service.update_fields(project_base_path=value)
        debounced_save()
        debounced_scan(value)

    def on_project_path_blur(e):
        value = ui_mgr.data_import.project_base_path
        if not value or not value.strip():
            return
        path = Path(value)
        if not path.is_absolute() or not path.exists() or not path.is_dir():
            return

        async def check_and_add():
            projects = await backend.scan_for_projects(value)
            if projects:
                prefs_service.prefs.add_recent_root(value)
                prefs_service.save_to_app_storage(app.storage.user)
                refresh_history_ui()

        asyncio.create_task(check_and_add())

    def on_movies_change(glob_str: str):
        ui_mgr.update_data_import(movies_glob=glob_str)
        update_movies_validation()
        prefs_service.update_fields(movies_glob=glob_str)
        debounced_save()

    def on_mdocs_change(glob_str: str):
        ui_mgr.update_data_import(mdocs_glob=glob_str)
        update_mdocs_validation()
        prefs_service.update_fields(mdocs_glob=glob_str)
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
        start = ui_mgr.data_import.project_base_path or "~"
        result = await local_file_picker(directory=start, mode="directory")
        if not result or len(result) == 0:
            return
        dir_path = result[0]
        ui_mgr.update_data_import(project_base_path=dir_path)
        if ui_mgr.panel_refs.project_path_input:
            ui_mgr.panel_refs.project_path_input.value = dir_path
        if local_refs["projects_path_label"]:
            local_refs["projects_path_label"].set_text(dir_path)
        prefs_service.update_fields(project_base_path=dir_path)
        debounced_save()
        await scan_and_display_projects(dir_path)

    async def handle_load_project(project_dir: Path):
        params_file = project_dir / "project_params.json"
        if not params_file.exists():
            ui.notify("No project_params.json found in directory", type="warning")
            return
        try:
            load_result = await backend.load_existing_project(str(project_dir))
            if load_result.get("success"):
                await backend.pipeline_runner.sync_all_jobs(str(project_dir))
                state = backend.state_service.state_for(project_dir)
                ui_mgr.load_from_project(
                    project_path=state.project_path,
                    scheme_name=f"scheme_{state.project_name}",
                    jobs=list(state.jobs.keys()),
                )
                ui_mgr.update_data_import(
                    project_name=state.project_name,
                    project_base_path=str(state.project_path.parent) if state.project_path else "",
                    movies_glob=state.movies_glob,
                    mdocs_glob=state.mdocs_glob,
                )
                prefs_service.update_fields(
                    project_base_path=str(state.project_path.parent) if state.project_path else "",
                    movies_glob=state.movies_glob,
                    mdocs_glob=state.mdocs_glob,
                )
                prefs_service.prefs.add_recent_root(
                    str(state.project_path.parent) if state.project_path else "", label=state.project_name
                )
                prefs_service.save_to_app_storage(app.storage.user)

                if state.pipeline_active:
                    ui_mgr.set_pipeline_running(True)
                    ui.notify(
                        f"Project '{state.project_name}' loaded -- pipeline was running, resuming monitoring.",
                        type="warning",
                        timeout=6000,
                    )
                else:
                    ui.notify(f"Project '{state.project_name}' loaded", type="positive")

                await asyncio.sleep(0.1)
                ui.navigate.to("/workspace")
            else:
                ui.notify(f"Failed to load: {load_result.get('error')}", type="negative")
        except Exception as e:
            import traceback

            traceback.print_exc()
            ui.notify(f"Error loading project: {e}", type="negative")

    # =========================================================================
    # PARAMS DISPLAY
    # =========================================================================

    def refresh_params_display():
        container = ui_mgr.panel_refs.params_display_container
        if not container:
            return
        container.clear()

        d_pix = ui_mgr.data_import.detected_pixel_size
        d_dose = ui_mgr.data_import.detected_dose_per_tilt
        has_detection = d_pix is not None or d_dose is not None

        with container:
            if not has_detection:
                ui.label("Inferred from mdocs when available").style(
                    f"{FONT} font-size: 9px; color: {CLR_GHOST}; font-style: italic;"
                )
                return

            state = get_project_state()
            params = [
                ("Pixel Size", f"{state.microscope.pixel_size_angstrom:.3f}", "\u212b"),
                ("Voltage", f"{state.microscope.acceleration_voltage_kv:.0f}", "kV"),
                ("Cs", f"{state.microscope.spherical_aberration_mm:.1f}", "mm"),
                ("Dose/Tilt", f"{state.acquisition.dose_per_tilt:.1f}", "e\u207b/\u212b\u00b2"),
                ("Tilt Axis", f"{state.acquisition.tilt_axis_degrees:.1f}", "\u00b0"),
            ]
            with ui.row().classes("w-full flex-wrap gap-x-5 gap-y-0"):
                for label, value, unit in params:
                    with ui.column().classes("gap-0"):
                        ui.label(label).style(
                            f"{LABEL} font-size: 8px; font-weight: 400; "
                            f"color: {CLR_SUBLABEL}; line-height: 1; margin-bottom: 1px;"
                        )
                        with ui.row().classes("items-baseline gap-0.5"):
                            ui.label(value).style(
                                f"{MONO} font-size: 12px; color: {CLR_HEADING}; font-weight: 600; line-height: 1.2;"
                            )
                            ui.label(unit).style(
                                f"{FONT} font-size: 8px; color: {CLR_SUBLABEL}; line-height: 1; margin-left: 1px;"
                            )

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

        if ui_mgr.panel_refs.movies_input:
            ui_mgr.panel_refs.movies_input.set_extension(local_refs["default_movies_ext"])
        if ui_mgr.panel_refs.mdocs_input:
            ui_mgr.panel_refs.mdocs_input.set_extension(local_refs["default_mdocs_ext"])

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
                if ui_mgr.panel_refs.movies_input:
                    ui_mgr.panel_refs.movies_input.set_from_glob(glob_to_use)
                else:
                    ui_mgr.update_data_import(movies_glob=glob_to_use)
                    update_movies_validation()

        if not ui_mgr.data_import.mdocs_glob:
            glob_to_use = prefs.mdocs_glob or config_mdocs
            if glob_to_use:
                if ui_mgr.panel_refs.mdocs_input:
                    ui_mgr.panel_refs.mdocs_input.set_from_glob(glob_to_use)
                else:
                    ui_mgr.update_data_import(mdocs_glob=glob_to_use)
                    update_mdocs_validation()

        await scan_and_display_projects(ui_mgr.data_import.project_base_path)

    # =========================================================================
    # AUTO-DETECT TRIGGER
    # =========================================================================

    async def _auto_detect_if_ready():
        if not ui_mgr.data_import.mdocs_valid:
            return
        if ui_mgr.data_import.detected_pixel_size is not None:
            return
        if ui_mgr.is_running:
            return
        mdocs_glob = ui_mgr.data_import.mdocs_glob
        if not mdocs_glob:
            return
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
            ui.notify("Parameters auto-detected from mdocs", type="positive", timeout=2500)
        except Exception as e:
            print(f"[AUTO_DETECT] Failed: {e}")

    _original_update_mdocs = update_mdocs_validation

    def update_mdocs_validation_with_autodetect():
        _original_update_mdocs()
        if ui_mgr.data_import.mdocs_valid:
            asyncio.create_task(_auto_detect_if_ready())

    update_mdocs_validation = update_mdocs_validation_with_autodetect

    # =========================================================================
    # LAYOUT
    # =========================================================================

    ui_mgr.panel_refs.movies_hint_label = None
    ui_mgr.panel_refs.mdocs_hint_label = None
    ui_mgr.panel_refs.status_indicator = None
    ui_mgr.panel_refs.autodetect_button = None
    ui_mgr.panel_refs.load_button = None

    card_style = (
        "background: white; border-radius: 8px; "
        f"border: 1px solid {CLR_BORDER}; "
        "box-shadow: 0 1px 3px rgba(15,23,42,0.06);"
    )
    section_style = f"border: 1px solid {CLR_BORDER}; border-radius: 6px; padding: 8px 10px; background: #f8fafc;"
    field_label_style = (
        f"{LABEL} font-size: 10px; font-weight: 400; color: {CLR_LABEL}; letter-spacing: 0.01em; margin-bottom: 2px;"
    )
    input_mono_style = f"{MONO} font-size: 11px; flex: 1; border-bottom: 1px solid {CLR_GHOST}; padding: 1px 2px;"
    input_sans_style = f"{FONT} font-size: 12px; width: 100%; border-bottom: 1px solid {CLR_GHOST}; padding: 1px 2px;"

    with ui.column().classes("w-full gap-2").style(FONT):
        # =====================================================================
        # CARD 1: Project Setup
        # =====================================================================
        with ui.column().classes("w-full gap-0 px-5 py-4").style(card_style):
            ui.label("Project Setup").style(
                f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING}; "
                "letter-spacing: -0.02em; margin-bottom: 10px;"
            )

            with ui.row().classes("w-full gap-5 mb-3"):
                with ui.column().classes("gap-0").style("flex: 1;"):
                    ui.label("Project Name").style(field_label_style)
                    project_name_input = (
                        ui.input(
                            value=ui_mgr.data_import.project_name,
                            on_change=on_project_name_change,
                            placeholder="e.g. HIV_Tomo_Batch1",
                        )
                        .props("dense borderless hide-bottom-space")
                        .style(input_sans_style)
                    )
                    project_name_input.on("blur", lambda e: sync_state_from_inputs())
                    ui_mgr.panel_refs.project_name_input = project_name_input

                with ui.column().classes("gap-0").style("flex: 2;"):
                    ui.label("Base Location").style(field_label_style)
                    with ui.row().classes("w-full items-center gap-1"):
                        project_path_input = (
                            ui.input(value=ui_mgr.data_import.project_base_path, on_change=on_project_path_change)
                            .props("dense borderless hide-bottom-space")
                            .style(input_mono_style)
                        )
                        project_path_input.on("blur", on_project_path_blur)
                        ui_mgr.panel_refs.project_path_input = project_path_input
                        with ui.element("div").style("position: relative;"):
                            ui.button(icon="folder", on_click=pick_project_path).props(
                                "flat dense round size=xs"
                            ).classes("text-slate-400 hover:text-slate-600")
                            ui.tooltip("New project directory will be created here").style(f"{FONT} font-size: 10px;")

            with ui.column().classes("w-full gap-2").style(section_style):
                # Raw Frames
                with ui.column().classes("w-full gap-0"):
                    ui.label("Raw Frames").style(field_label_style)
                    with ui.row().classes("w-full items-center gap-1"):
                        movies_input = GlobDirectoryInput(
                            extension=local_refs["default_movies_ext"],
                            initial_glob=ui_mgr.data_import.movies_glob,
                            on_change=on_movies_change,
                            placeholder="/path/to/movies",
                        )
                        ui_mgr.panel_refs.movies_input = movies_input
                        ui.button(icon="folder", on_click=pick_movies_path).props("flat dense round size=xs").classes(
                            "text-slate-400 hover:text-slate-600"
                        )
                    movies_hint = ui.label("No pattern").style(
                        f"{FONT} font-size: 9px; color: {CLR_SUBLABEL}; padding-left: 2px; margin-top: 1px;"
                    )
                    ui_mgr.panel_refs.movies_hint_label = movies_hint

                # SerialEM Mdocs
                with ui.column().classes("w-full gap-0"):
                    ui.label("SerialEM Mdocs").style(field_label_style)
                    with ui.row().classes("w-full items-center gap-1"):
                        mdocs_input = GlobDirectoryInput(
                            extension=local_refs["default_mdocs_ext"],
                            initial_glob=ui_mgr.data_import.mdocs_glob,
                            on_change=on_mdocs_change,
                            placeholder="/path/to/mdocs",
                        )
                        ui_mgr.panel_refs.mdocs_input = mdocs_input
                        ui.button(icon="folder", on_click=pick_mdocs_path).props("flat dense round size=xs").classes(
                            "text-slate-400 hover:text-slate-600"
                        )
                    mdocs_hint = ui.label("No pattern").style(
                        f"{FONT} font-size: 9px; color: {CLR_SUBLABEL}; padding-left: 2px; margin-top: 1px;"
                    )
                    ui_mgr.panel_refs.mdocs_hint_label = mdocs_hint

                # Detected configuration
                with (
                    ui.column().classes("w-full gap-0").style(f"border-top: 1px solid {CLR_BORDER}; padding-top: 7px;")
                ):
                    ui.label("Detected Configuration").style(
                        f"{LABEL} font-size: 10px; font-weight: 400; "
                        f"color: {CLR_LABEL}; letter-spacing: 0.01em; margin-bottom: 3px;"
                    )
                    params_container = ui.column().classes("w-full")
                    ui_mgr.panel_refs.params_display_container = params_container
                    refresh_params_display()

            with ui.row().classes("w-full items-center justify-between mt-3"):
                status_indicator = ui.label("Enter details to begin...").style(
                    f"{FONT} font-size: 10px; color: {CLR_SUBLABEL};"
                )
                ui_mgr.panel_refs.status_indicator = status_indicator

                create_btn = (
                    ui.button("Create Project", on_click=handle_create_project)
                    .props("no-caps unelevated")
                    .style(
                        f"{FONT} font-size: 11px; font-weight: 500; padding: 4px 16px; "
                        "border-radius: 6px; background: #93c5fd; color: white; "
                        "letter-spacing: 0.01em;"
                    )
                )
                ui_mgr.panel_refs.create_button = create_btn

        # =====================================================================
        # CARD 2: Projects in Base Location
        # =====================================================================
        with ui.column().classes("w-full gap-0").style(card_style):
            # Title row: "Projects in [/path/to/location ▾]"  +  Browse
            with ui.row().classes("w-full items-center justify-between px-5 pt-3 pb-2"):
                # Left: title with interactive path dropdown trigger
                with ui.row().classes("items-baseline gap-1 flex-wrap min-w-0"):
                    ui.label("Projects in").style(
                        f"{FONT} font-size: 13px; font-weight: 600; "
                        f"color: {CLR_HEADING}; letter-spacing: -0.02em; white-space: nowrap;"
                    )

                    # Positioned wrapper for the dropdown
                    with ui.element("div").style("position: relative; display: inline-block; max-width: 460px;"):
                        # Clickable path pill
                        with (
                            ui.row()
                            .classes("items-center gap-0.5 cursor-pointer rounded")
                            .style(f"padding: 1px 4px; border-bottom: 1px dashed {CLR_GHOST};")
                            .on("click", toggle_history_dropdown)
                        ):
                            path_label_inline = ui.label(
                                ui_mgr.data_import.project_base_path or "no location set"
                            ).style(
                                f"{MONO} font-size: 11px; color: {CLR_ACCENT}; "
                                "max-width: 420px; overflow: hidden; "
                                "text-overflow: ellipsis; white-space: nowrap;"
                            )
                            local_refs["projects_path_label"] = path_label_inline
                            ui.icon("expand_more", size="14px").style(f"color: {CLR_SUBLABEL}; flex-shrink: 0;")

                        # History dropdown (hidden by default)
                        history_dropdown_el = ui.element("div").style(
                            "position: absolute; top: calc(100% + 4px); left: 0; "
                            "z-index: 9999; display: none; background: white; "
                            f"border: 1px solid {CLR_BORDER}; border-radius: 6px; "
                            "box-shadow: 0 4px 16px rgba(15,23,42,0.10); "
                            "min-width: 320px; max-width: 480px; "
                            "max-height: 220px; overflow-y: auto;"
                        )
                        local_refs["history_dropdown_el"] = history_dropdown_el
                        with history_dropdown_el:
                            local_refs["history_container"] = ui.column().classes("w-full p-0 gap-0")

                # Right: Browse button
                with (
                    ui.button(on_click=handle_load_project_click)
                    .props("flat dense no-caps")
                    .classes("text-slate-400 hover:text-blue-600 shrink-0")
                ):
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("folder_open", size="13px")
                        ui.label("Browse").style(f"{FONT} font-size: 10px; font-weight: 500;")

            # Project list -- no horizontal padding on scroll area,
            # rows carry their own px-5 to align with the title
            with ui.scroll_area().classes("w-full").style("min-height: 80px; max-height: 300px;"):
                local_refs["recent_projects_container"] = ui.column().classes("w-full gap-0")

    # =========================================================================
    # WIRING
    # =========================================================================

    update_movies_validation()
    update_mdocs_validation()
    update_create_button_state()
    ui_mgr.subscribe(lambda state: update_locking_state())

    ui.timer(0.1, init_defaults, once=True)
    ui.timer(1.0, sync_state_from_inputs)
