# ui/data_import_panel.py
"""
Data import panel with debounced saves and history management.
"""

import asyncio
import glob
import logging
from pathlib import Path
from typing import Dict, Callable

from nicegui import ui, app

from backend import CryoBoostBackend
from services.configs.user_prefs_service import get_prefs_service

from ui.ui_state import get_ui_state_manager
from ui.local_file_picker import local_file_picker
from ui.glob_directory_input import GlobDirectoryInput
from ui.dataset_overview_panel import build_dataset_overview_panel, build_dry_run_summary
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)


DEFAULT_MOVIES_EXT = "*.eer"
DEFAULT_MDOCS_EXT = "*.mdoc"
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
        "dataset_overview_container": None,
        "current_dataset_overview": None,
        "mdocs_separate": False,
        "mdocs_separate_container": None,
        "parsing_spinner": None,
        "data_history_container": None,
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

    # -- Async glob validation (never blocks the event loop) ----------------

    _glob_tasks: Dict[str, asyncio.Task] = {}

    def _validate_glob_quick(pattern: str) -> tuple[bool, str]:
        """Instant syntax-only check — no filesystem I/O."""
        if not pattern or not pattern.strip():
            return False, "No pattern specified"
        return True, "Checking..."

    async def _validate_glob_full(pattern: str) -> tuple[bool, int, str]:
        """Full validation with file count — runs glob in a thread."""

        def _do():
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

        return await asyncio.to_thread(_do)

    def update_input_validation(input_el, is_valid: bool, message: str):
        if not input_el:
            return
        if is_valid:
            input_el.props("error=false error-message=''")
        else:
            safe_msg = message.replace("'", "").replace('"', "").replace("\\", "/")
            input_el.props(f"error=true error-message='{safe_msg}'")

    def _set_hint(hint_label, msg, color):
        if hint_label and not getattr(hint_label, "is_deleted", False):
            hint_label.set_text(msg)
            hint_label.style(f"{FONT} font-size: 9px; color: {color}; padding-left: 2px;")

    def update_movies_validation():
        pattern = ui_mgr.data_import.movies_glob
        quick_ok, quick_msg = _validate_glob_quick(pattern)
        if not quick_ok:
            ui_mgr.update_data_import(movies_valid=False)
            _set_hint(ui_mgr.panel_refs.movies_hint_label, quick_msg, CLR_ERROR)
            if ui_mgr.panel_refs.movies_input:
                update_input_validation(ui_mgr.panel_refs.movies_input, False, quick_msg)
            update_create_button_state()
            return

        _set_hint(ui_mgr.panel_refs.movies_hint_label, quick_msg, CLR_SUBLABEL)
        # Cancel previous check for this field
        prev = _glob_tasks.get("movies")
        if prev and not prev.done():
            prev.cancel()

        async def _finish():
            is_valid, count, msg = await _validate_glob_full(pattern)
            if ui_mgr.data_import.movies_glob != pattern:
                return  # pattern changed while we were checking
            ui_mgr.update_data_import(movies_valid=is_valid)
            if ui_mgr.panel_refs.movies_input:
                update_input_validation(ui_mgr.panel_refs.movies_input, is_valid, msg)
            _set_hint(ui_mgr.panel_refs.movies_hint_label, msg, CLR_SUCCESS if is_valid else CLR_ERROR)
            if is_valid and pattern:
                data_dir = str(Path(pattern).parent) if "*" in pattern else pattern
                prefs_service.prefs.add_recent_data_path(data_dir)
                prefs_service.save_to_app_storage(app.storage.user)
                refresh_data_history_ui()
            update_create_button_state()

        _glob_tasks["movies"] = asyncio.create_task(_finish())

    def update_mdocs_validation():
        pattern = ui_mgr.data_import.mdocs_glob
        quick_ok, quick_msg = _validate_glob_quick(pattern)
        if not quick_ok:
            ui_mgr.update_data_import(mdocs_valid=False)
            _set_hint(ui_mgr.panel_refs.mdocs_hint_label, quick_msg, CLR_ERROR)
            if ui_mgr.panel_refs.mdocs_input:
                update_input_validation(ui_mgr.panel_refs.mdocs_input, False, quick_msg)
            update_create_button_state()
            return

        _set_hint(ui_mgr.panel_refs.mdocs_hint_label, quick_msg, CLR_SUBLABEL)
        prev = _glob_tasks.get("mdocs")
        if prev and not prev.done():
            prev.cancel()

        async def _finish():
            is_valid, count, msg = await _validate_glob_full(pattern)
            if ui_mgr.data_import.mdocs_glob != pattern:
                return
            ui_mgr.update_data_import(mdocs_valid=is_valid)
            if ui_mgr.panel_refs.mdocs_input:
                update_input_validation(ui_mgr.panel_refs.mdocs_input, is_valid, msg)
            _set_hint(ui_mgr.panel_refs.mdocs_hint_label, msg, CLR_SUCCESS if is_valid else CLR_ERROR)
            update_create_button_state()
            # Trigger autodetect + dataset parsing now that validation is confirmed
            if is_valid:
                await _auto_detect_if_ready()
                await parse_and_display_dataset()

        _glob_tasks["mdocs"] = asyncio.create_task(_finish())

    def get_missing_requirements() -> list[str]:
        sync_state_from_inputs()
        di = ui_mgr.data_import
        missing = []
        if not (di.project_name and di.project_name.strip()):
            missing.append("Project Name")
        if not (di.project_base_path and di.project_base_path.strip()):
            missing.append("Project Path")
        if not di.movies_glob:
            missing.append("Data Path")
        elif not di.movies_valid:
            missing.append("Valid Frames")
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

    _syncing = False

    def sync_state_from_inputs():
        nonlocal _syncing
        if _syncing:
            return
        _syncing = True
        try:
            refs = ui_mgr.panel_refs
            changed = False
            upd = ui_mgr.update_data_import
            for attr, ref, updater in [
                ("project_name", refs.project_name_input, lambda v: upd(project_name=v)),
                ("project_base_path", refs.project_path_input, lambda v: upd(project_base_path=v)),
                ("movies_glob", refs.movies_input, lambda v: upd(movies_glob=v)),
                ("mdocs_glob", refs.mdocs_input, lambda v: upd(mdocs_glob=v)),
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
        finally:
            _syncing = False

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
                    with (
                        ui.row()
                        .classes(
                            "w-full items-center gap-1 py-1.5 px-3 hover:bg-slate-50 transition-colors cursor-pointer"
                        )
                        .on("click", lambda p=root.path: use_history_path(p))
                    ):
                        with ui.column().classes("flex-1 gap-0 min-w-0"):
                            if root.label:
                                ui.label(root.label).style(
                                    f"{FONT} font-size: 10px; font-weight: 500; color: {CLR_HEADING};"
                                ).classes("truncate")
                            short = Path(root.path).name or root.path
                            ui.label(short).style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};").classes("truncate")
                        ui.button(
                            icon="close",
                            on_click=lambda e, p=root.path: (
                                e.sender.parent_slot.parent.set_visibility(False),
                                remove_history_path(p),
                            ),
                        ).props("flat dense round size=xs").classes(
                            "text-slate-200 hover:text-red-400 shrink-0 opacity-0 group-hover:opacity-100"
                        )
            if roots:
                with ui.row().classes("w-full justify-end px-3 py-1 border-t border-slate-100"):
                    ui.button("Clear all", on_click=clear_all_history).props("flat dense no-caps").style(
                        f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};"
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

    # =========================================================================
    # DATA PATH HISTORY
    # =========================================================================

    def refresh_data_history_ui():
        container = local_refs["data_history_container"]
        if not container:
            return
        container.clear()
        paths = prefs_service.prefs.recent_data_paths
        with container:
            if not paths:
                ui.label("No recent data directories").style(
                    f"{FONT} font-size: 9px; color: {CLR_GHOST}; font-style: italic; padding: 6px 12px;"
                )
            else:
                for entry in paths[:10]:
                    short = Path(entry.path).name or entry.path
                    with (
                        ui.row()
                        .classes(
                            "w-full items-center gap-1 py-1 px-3 "
                            "hover:bg-slate-50 transition-colors "
                            "cursor-pointer group"
                        )
                        .on("click", lambda p=entry.path: use_data_path(p))
                    ):
                        ui.icon("science", size="12px").style(f"color: {CLR_GHOST}; flex-shrink: 0;")
                        with ui.column().classes("flex-1 gap-0 min-w-0"):
                            ui.label(short).style(f"{MONO} font-size: 10px; color: {CLR_LABEL};").classes("truncate")
                            if entry.path != short:
                                ui.label(entry.path).style(f"{MONO} font-size: 8px; color: {CLR_GHOST};").classes(
                                    "truncate"
                                )
                        ui.button(icon="close", on_click=lambda e, p=entry.path: remove_data_path(p)).props(
                            "flat dense round size=xs"
                        ).classes("text-slate-200 hover:text-red-400 shrink-0 opacity-0 group-hover:opacity-100").on(
                            "click.stop", lambda: None
                        )
            if paths:
                with ui.row().classes("w-full justify-end px-3 py-1 border-t border-slate-100"):
                    ui.button("Clear", on_click=clear_data_history).props("flat dense no-caps").style(
                        f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};"
                    )

    def use_data_path(path: str):
        if ui_mgr.panel_refs.movies_input:
            ui_mgr.panel_refs.movies_input.set_directory(path)

    def remove_data_path(path: str):
        prefs_service.prefs.remove_recent_data_path(path)
        prefs_service.save_to_app_storage(app.storage.user)
        refresh_data_history_ui()

    def clear_data_history():
        prefs_service.prefs.clear_recent_data_paths()
        prefs_service.save_to_app_storage(app.storage.user)
        refresh_data_history_ui()

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

                    with (
                        ui.row()
                        .classes(
                            "w-full items-center py-1.5 px-3 gap-1 "
                            "hover:bg-slate-50 transition-colors cursor-pointer group"
                        )
                        .style("min-height: 28px;")
                    ) as proj_row:

                        def make_delete_handler(path_str: str, name: str, row_el=proj_row):
                            async def handler():
                                await _confirm_delete_project(Path(path_str), name, row_el)

                            return handler

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

    async def _confirm_delete_project(project_dir: Path, project_name: str, row_el=None):
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
                    # Grey out the project row so user can't click/re-delete
                    if row_el and not getattr(row_el, "is_deleted", False):
                        row_el.clear()
                        with row_el:
                            ui.spinner("dots", size="xs").style(f"color: {CLR_SUBLABEL};")
                            ui.label(f"Deleting {project_name}...").style(
                                f"{FONT} font-size: 10px; color: {CLR_SUBLABEL}; font-style: italic;"
                            )
                        row_el.style(add="opacity: 0.5; pointer-events: none; cursor: default;")
                    try:
                        import shutil

                        await asyncio.to_thread(shutil.rmtree, project_dir)
                        from services.project_state import remove_project_state

                        remove_project_state(project_dir)
                        ui.notify(f"Deleted '{project_name}'", type="positive")
                        await scan_and_display_projects(ui_mgr.data_import.project_base_path)
                    except Exception as e:
                        ui.notify(f"Failed to delete: {e}", type="negative")
                        # Restore the row on failure
                        await scan_and_display_projects(ui_mgr.data_import.project_base_path)

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
        # When mdocs are co-located, auto-derive mdocs_glob from the same directory
        if not local_refs["mdocs_separate"] and glob_str:
            parent = str(Path(glob_str).parent) if "*" in glob_str else glob_str
            derived_mdocs = str(Path(parent) / local_refs["default_mdocs_ext"])
            ui_mgr.update_data_import(mdocs_glob=derived_mdocs)
            prefs_service.update_fields(mdocs_glob=derived_mdocs)
            if ui_mgr.panel_refs.mdocs_input:
                ui_mgr.panel_refs.mdocs_input.set_from_glob(derived_mdocs)
            update_mdocs_validation()

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
        is_valid, msg = _validate_glob_quick(mdocs_glob)
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
            ui.notify("Parameters detected from mdoc files", type="positive")
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
        overview = local_refs.get("current_dataset_overview")

        # Auto-save selections before attempting creation (survives failures)
        if overview and di.mdocs_glob:
            from services.configs.dataset_selection_cache import save_selections

            save_selections(di.mdocs_glob, overview)

        # Collect import data from the already-parsed dataset overview
        selected_mdoc_paths = None
        import_summary = None
        detected_params = None
        if overview:
            selected_ts = overview.get_selected_tilt_series()
            selected_mdoc_paths = [str(ts.mdoc_path) for ts in selected_ts]
            import_summary = {
                "total_positions": len(overview.positions),
                "selected_positions": sum(1 for p in overview.positions if p.selected),
                "total_tilt_series": overview.total_tilt_series,
                "selected_tilt_series": overview.selected_tilt_series,
                "source_directory": overview.source_directory or "",
                "frame_extension": overview.frame_extension or "",
                "position_details": [
                    {
                        "stage_position": p.stage_position,
                        "beam_count": p.beam_count,
                        "tilt_count": p.total_tilts,
                        "selected": p.selected,
                    }
                    for p in overview.positions
                ],
                "tilt_series_details": [
                    {
                        "stage_position": ts.stage_position,
                        "beam_position": ts.beam_position,
                        "tilt_count": ts.tilt_count,
                        "selected": ts.selected,
                        "mdoc_filename": ts.mdoc_filename,
                    }
                    for p in overview.positions
                    for ts in p.tilt_series
                ],
                "tilt_metadata": {
                    Path(t.frame_filename).stem: t.mdoc_stats
                    for ts in selected_ts
                    for t in ts.tilts
                    if t.mdoc_stats
                },
            }
            sel_summary = overview.selected_acquisition_summary()
            detected_params = {}
            if sel_summary.pixel_sizes:
                detected_params["pixel_size_angstrom"] = sel_summary.pixel_sizes[0]
            if sel_summary.voltages:
                detected_params["acceleration_voltage_kv"] = sel_summary.voltages[0]
            if sel_summary.doses:
                detected_params["dose_per_tilt"] = sel_summary.doses[0]
            if sel_summary.tilt_axes:
                detected_params["tilt_axis_degrees"] = sel_summary.tilt_axes[0]

        # Show progress overlay in the dataset overview area
        progress_container = local_refs.get("dataset_overview_container")
        if progress_container:
            progress_container.clear()
            with progress_container:
                with ui.row().classes("w-full justify-center items-center py-6"):
                    ui.spinner("dots", size="md").style(f"color: {CLR_ACCENT};")
                    ui.label("Creating project — importing data and initializing...").style(
                        f"{FONT} font-size: 11px; color: {CLR_LABEL}; margin-left: 8px;"
                    )

        if btn:
            btn.props("loading")
            btn.disable()

        try:
            result = await backend.create_project_and_scheme(
                project_name=di.project_name,
                project_base_path=di.project_base_path,
                selected_jobs=[j.value for j in ui_mgr.selected_jobs],
                movies_glob=di.movies_glob,
                mdocs_glob=di.mdocs_glob,
                selected_mdoc_paths=selected_mdoc_paths,
                import_summary=import_summary,
                detected_params=detected_params,
            )
            if result.get("success"):
                project_path = Path(result["project_path"])
                scheme_name = f"scheme_{di.project_name}"
                ui_mgr.set_project_created(project_path, scheme_name)

                # Show success state before navigating
                if progress_container:
                    progress_container.clear()
                    with progress_container:
                        with ui.row().classes("w-full justify-center items-center py-6"):
                            ui.icon("check_circle", size="md").style(f"color: {CLR_SUCCESS};")
                            ui.label(f"Project '{di.project_name}' created — opening workspace...").style(
                                f"{FONT} font-size: 11px; color: {CLR_SUCCESS}; margin-left: 8px;"
                            )

                prefs_service.prefs.add_recent_root(di.project_base_path, label=di.project_name)
                prefs_service.save_to_app_storage(app.storage.user)
                await asyncio.sleep(0.8)
                # Navigate via JS — the NiceGUI slot context may be stale after the sleep
                ui.run_javascript('window.location.href = "/workspace"')
                return
            else:
                ui.notify(f"Failed: {result.get('error')}", type="negative")
        except Exception as e:
            import traceback

            traceback.print_exc()
            try:
                ui.notify(f"Error creating project: {e}", type="negative")
            except RuntimeError:
                pass  # slot already deleted (e.g. page navigated away)
        finally:
            try:
                if btn:
                    btn.props(remove="loading")
                update_locking_state()
            except RuntimeError:
                pass

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
            if local_refs["mdocs_separate"]:
                glob_to_use = prefs.mdocs_glob or config_mdocs
                if glob_to_use:
                    if ui_mgr.panel_refs.mdocs_input:
                        ui_mgr.panel_refs.mdocs_input.set_from_glob(glob_to_use)
                    else:
                        ui_mgr.update_data_import(mdocs_glob=glob_to_use)
                        update_mdocs_validation()
            elif ui_mgr.data_import.movies_glob:
                # Same-dir mode: derive mdocs from movies directory
                mg = ui_mgr.data_import.movies_glob
                parent = str(Path(mg).parent) if "*" in mg else mg
                derived = str(Path(parent) / local_refs["default_mdocs_ext"])
                ui_mgr.update_data_import(mdocs_glob=derived)
                update_mdocs_validation()

        refresh_history_ui()
        refresh_data_history_ui()
        await scan_and_display_projects(ui_mgr.data_import.project_base_path)

    # =========================================================================
    # AUTO-DETECT TRIGGER
    # =========================================================================

    async def _auto_detect_if_ready():
        """Cache detected params in ui_mgr for project creation."""
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
        except Exception as e:
            logger.info("Auto-detect failed: %s", e)

    # =========================================================================
    # DATASET OVERVIEW
    # =========================================================================

    async def parse_and_display_dataset():
        """Parse dataset structure from mdocs and display the overview panel."""
        container = local_refs["dataset_overview_container"]
        if not container:
            return

        mdocs_glob = ui_mgr.data_import.mdocs_glob
        if not mdocs_glob or not ui_mgr.data_import.mdocs_valid:
            container.clear()
            local_refs["current_dataset_overview"] = None
            return

        movies_glob = ui_mgr.data_import.movies_glob
        frames_dir = str(Path(movies_glob).parent) if movies_glob and "*" in movies_glob else None

        # Show spinner while parsing
        container.clear()
        with container:
            with ui.row().classes("w-full justify-center py-3"):
                ui.spinner("dots", size="sm").style(f"color: {CLR_ACCENT};")
                ui.label("Scanning dataset...").style(
                    f"{FONT} font-size: 10px; color: {CLR_SUBLABEL}; margin-left: 6px;"
                )

        try:
            from services.configs.dataset_selection_cache import apply_selections, save_selections

            overview = await backend.parse_dataset_overview(mdocs_glob, frames_dir)
            # Restore previously saved selections for this dataset
            restored = apply_selections(mdocs_glob, overview)

            local_refs["current_dataset_overview"] = overview
            container.clear()
            with container:
                # "Save Selection" button row
                with ui.row().classes("w-full items-center justify-between mb-1"):
                    if restored:
                        ui.label("Restored saved selection").style(
                            f"{FONT} font-size: 9px; color: {CLR_SUCCESS}; font-style: italic;"
                        )
                    else:
                        ui.element("div")  # spacer

                    def _do_save_selection():
                        ov = local_refs.get("current_dataset_overview")
                        mg = ui_mgr.data_import.mdocs_glob
                        if ov and mg:
                            save_selections(mg, ov)
                            ui.notify("Selection saved", type="positive")

                    ui.button("Save selection", on_click=_do_save_selection).props("flat dense no-caps").style(
                        f"{FONT} font-size: 10px; color: {CLR_LABEL}; padding: 2px 8px;"
                    )

                build_dataset_overview_panel(overview, on_change=update_create_button_state)
        except Exception as e:
            logger.info("Dataset parsing failed: %s", e)
            container.clear()
            local_refs["current_dataset_overview"] = None

    async def show_dry_run_dialog():
        overview = local_refs.get("current_dataset_overview")
        if not overview:
            ui.notify("Parse a dataset first", type="warning")
            return
        with ui.dialog() as dialog, ui.card().classes("w-[540px]"):
            build_dry_run_summary(overview)
            with ui.row().classes("w-full justify-end mt-3"):
                ui.button("Close", on_click=dialog.close).props("flat no-caps").style(f"{FONT} font-size: 12px;")
        dialog.open()

    # NOTE: autodetect + dataset parsing is triggered from within
    # update_mdocs_validation's async _finish() callback when the
    # glob check succeeds — no separate wrapper needed.

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

    with ui.row().classes("w-full gap-3 items-start").style(FONT):
        # =================================================================
        # LEFT COLUMN: Project Setup (wider)
        # =================================================================
        with ui.column().classes("gap-2").style("flex: 3; min-width: 0;"):
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
                        with ui.row().classes("items-center gap-1"):
                            ui.label("Base Location").style(field_label_style)
                            with ui.icon("help_outline", size="12px").style(f"color: {CLR_GHOST}; cursor: help;"):
                                ui.tooltip(
                                    "Where the project directory will be created. "
                                    "Processing artifacts and results are stored here."
                                ).style(f"{FONT} font-size: 10px;")
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
                                ui.tooltip("New project directory will be created here").style(
                                    f"{FONT} font-size: 10px;"
                                )

                with ui.column().classes("w-full gap-2").style(section_style):
                    # Raw Frames & Mdocs (combined input)
                    with ui.column().classes("w-full gap-0"):
                        with ui.row().classes("w-full items-center justify-between"):
                            with ui.row().classes("items-center gap-1"):
                                ui.label("Raw Frames & SerialEM Mdocs").style(field_label_style)
                                with ui.icon("help_outline", size="12px").style(f"color: {CLR_GHOST}; cursor: help;"):
                                    ui.tooltip(
                                        "Directory containing your primary data "
                                        "(frame files and .mdoc metadata). "
                                        "These files will never be modified."
                                    ).style(f"{FONT} font-size: 10px;")

                            def toggle_mdocs_separate(e):
                                local_refs["mdocs_separate"] = e.value
                                container = local_refs["mdocs_separate_container"]
                                if container:
                                    container.set_visibility(e.value)

                            with ui.row().classes("items-center gap-1"):
                                ui.label("mdocs elsewhere").style(f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};")
                                (
                                    ui.switch(value=False, on_change=toggle_mdocs_separate)
                                    .props("dense")
                                    .style("transform: scale(0.65);")
                                )

                        with ui.row().classes("w-full items-center gap-1"):
                            movies_input = GlobDirectoryInput(
                                extension=local_refs["default_movies_ext"],
                                initial_glob=ui_mgr.data_import.movies_glob,
                                on_change=on_movies_change,
                                placeholder="/path/to/frames",
                            )
                            ui_mgr.panel_refs.movies_input = movies_input
                            ui.button(icon="folder", on_click=pick_movies_path).props(
                                "flat dense round size=xs"
                            ).classes("text-slate-400 hover:text-slate-600")
                        movies_hint = ui.label("No pattern").style(
                            f"{FONT} font-size: 9px; color: {CLR_SUBLABEL}; padding-left: 2px; margin-top: 1px;"
                        )
                        ui_mgr.panel_refs.movies_hint_label = movies_hint

                    # Separate mdocs input (hidden by default)
                    mdocs_separate_container = ui.column().classes("w-full gap-0")
                    mdocs_separate_container.set_visibility(False)
                    local_refs["mdocs_separate_container"] = mdocs_separate_container
                    with mdocs_separate_container:
                        ui.label("SerialEM Mdocs (separate location)").style(field_label_style)
                        with ui.row().classes("w-full items-center gap-1"):
                            mdocs_input = GlobDirectoryInput(
                                extension=local_refs["default_mdocs_ext"],
                                initial_glob=ui_mgr.data_import.mdocs_glob,
                                on_change=on_mdocs_change,
                                placeholder="/path/to/mdocs",
                            )
                            ui_mgr.panel_refs.mdocs_input = mdocs_input
                            ui.button(icon="folder", on_click=pick_mdocs_path).props(
                                "flat dense round size=xs"
                            ).classes("text-slate-400 hover:text-slate-600")
                        mdocs_hint = ui.label("No pattern").style(
                            f"{FONT} font-size: 9px; color: {CLR_SUBLABEL}; padding-left: 2px; margin-top: 1px;"
                        )
                        ui_mgr.panel_refs.mdocs_hint_label = mdocs_hint

                # Dataset overview (populated when mdocs are validated)
                dataset_overview_container = ui.column().classes("w-full gap-0 mt-1")
                local_refs["dataset_overview_container"] = dataset_overview_container

                with ui.row().classes("w-full items-center justify-between mt-3"):
                    status_indicator = ui.label("Enter details to begin...").style(
                        f"{FONT} font-size: 10px; color: {CLR_SUBLABEL};"
                    )
                    ui_mgr.panel_refs.status_indicator = status_indicator

                    with ui.row().classes("items-center gap-2"):
                        (
                            ui.button("Preview Import", on_click=show_dry_run_dialog)
                            .props("no-caps flat")
                            .style(f"{FONT} font-size: 11px; font-weight: 500; padding: 4px 12px; color: {CLR_LABEL};")
                        )
                        create_btn = (
                            ui.button("Create Project", on_click=handle_create_project)
                            .props("no-caps unelevated")
                            .style(
                                f"{FONT} font-size: 11px; font-weight: 500; "
                                "padding: 4px 16px; border-radius: 6px; "
                                "background: #93c5fd; color: white; "
                                "letter-spacing: 0.01em;"
                            )
                        )
                        ui_mgr.panel_refs.create_button = create_btn

        # =================================================================
        # RIGHT COLUMN: Projects + History sidebar
        # =================================================================
        with ui.column().classes("gap-2").style("flex: 1; min-width: 240px; max-width: 360px;"):
            # ----- Existing Projects -----
            with ui.column().classes("w-full gap-0").style(card_style):
                with ui.row().classes("w-full items-center justify-between px-4 pt-3 pb-2"):
                    ui.label("Projects").style(
                        f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; letter-spacing: -0.02em;"
                    )
                    with (
                        ui.button(on_click=handle_load_project_click)
                        .props("flat dense no-caps")
                        .classes("text-slate-400 hover:text-blue-600 shrink-0")
                    ):
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("folder_open", size="12px")
                            ui.label("Browse").style(f"{FONT} font-size: 9px; font-weight: 500;")

                with ui.scroll_area().classes("w-full").style("min-height: 60px; max-height: 240px;"):
                    local_refs["recent_projects_container"] = ui.column().classes("w-full gap-0")

            # ----- Recent Project Locations -----
            with ui.column().classes("w-full gap-0").style(card_style):
                with ui.row().classes("w-full items-center px-4 pt-3 pb-1"):
                    ui.icon("folder_special", size="14px").style(f"color: {CLR_SUBLABEL};")
                    ui.label("Recent Locations").style(
                        f"{FONT} font-size: 11px; font-weight: 600; color: {CLR_HEADING}; margin-left: 4px;"
                    )
                with ui.scroll_area().classes("w-full").style("min-height: 32px; max-height: 160px;"):
                    local_refs["history_container"] = ui.column().classes("w-full p-0 gap-0")

            # ----- Recent Data Paths -----
            with ui.column().classes("w-full gap-0").style(card_style):
                with ui.row().classes("w-full items-center px-4 pt-3 pb-1"):
                    ui.icon("science", size="14px").style(f"color: {CLR_SUBLABEL};")
                    ui.label("Recent Data").style(
                        f"{FONT} font-size: 11px; font-weight: 600; color: {CLR_HEADING}; margin-left: 4px;"
                    )
                with ui.scroll_area().classes("w-full").style("min-height: 32px; max-height: 160px;"):
                    local_refs["data_history_container"] = ui.column().classes("w-full p-0 gap-0")

            # Hidden refs for legacy code
            history_dropdown_el = ui.element("div").style("display: none;")
            local_refs["history_dropdown_el"] = history_dropdown_el
            path_label_inline = ui.label("").style("display: none;")
            local_refs["projects_path_label"] = path_label_inline

    # =========================================================================
    # WIRING
    # =========================================================================

    update_movies_validation()
    update_mdocs_validation()
    update_create_button_state()
    ui_mgr.subscribe(lambda state: update_locking_state())

    ui.timer(0.1, init_defaults, once=True)
    ui.timer(1.0, sync_state_from_inputs)
