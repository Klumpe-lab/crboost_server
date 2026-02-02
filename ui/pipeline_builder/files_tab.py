# ui/pipeline_builder/files_tab.py
"""
Mac Finder-style file browser with preview panel.
"""

import re
import base64
from pathlib import Path

from nicegui import ui

from services.project_state import JobType
from ui.ui_state import UIStateManager


def render_files_tab(job_type: JobType, job_model, ui_mgr: UIStateManager):
    """Render the file browser + preview layout."""

    if not ui_mgr.project_path:
        with ui.column().classes("w-full p-4"):
            ui.label("Error: Project path not loaded").classes("text-red-600")
        return

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job not started. Files will appear here once the job runs.")
        return

    project_path = ui_mgr.project_path
    job_dir = (project_path / job_model.relion_job_name.strip("/")).resolve()

    state = {"current_path": job_dir, "selected_file": None}

    # ============================================
    # Sorting
    # ============================================

    def _extract_numeric_tuple(filename: str) -> tuple:
        pattern = r"-?\d+\.?\d*"
        matches = re.findall(pattern, filename)
        numbers = []
        for match in matches:
            try:
                numbers.append(float(match) if "." in match else int(match))
            except ValueError:
                continue
        if not numbers:
            return ()
        if len(numbers) >= 2:
            return (numbers[-1], *numbers[:-1])
        return tuple(numbers)

    def _smart_sort_items(items: list) -> list:
        dirs = sorted([i for i in items if i.is_dir()], key=lambda p: p.name.lower())
        files = [i for i in items if not i.is_dir()]

        with_nums = []
        without_nums = []
        for f in files:
            nums = _extract_numeric_tuple(f.name)
            if nums:
                with_nums.append((f, nums))
            else:
                without_nums.append(f)

        with_nums.sort(key=lambda x: x[1])
        without_nums.sort(key=lambda p: p.name.lower())

        return dirs + [f for f, _ in with_nums] + without_nums

    # ============================================
    # File list
    # ============================================

    @ui.refreshable
    def render_file_list():
        file_list_container.clear()
        current = state["current_path"]

        if not current.exists():
            with file_list_container:
                ui.label("Directory does not exist").classes("p-4 text-gray-400 italic")
            return

        try:
            items = _smart_sort_items(list(current.iterdir()))
        except Exception as e:
            with file_list_container:
                ui.label(f"Error: {e}").classes("p-4 text-red-500")
            return

        with file_list_container:
            if not items:
                ui.label("Empty directory").classes("p-4 text-gray-400 italic")
                return

            if current != job_dir and job_dir in current.parents:
                with (
                    ui.row()
                    .classes("w-full items-center gap-2 cursor-pointer hover:bg-gray-50 p-2 border-b border-gray-100")
                    .on("click", lambda: navigate_to(current.parent))
                ):
                    ui.icon("folder_open", size="16px").classes("text-gray-400")
                    ui.label("..").classes("text-xs font-bold")

            for item in items:
                is_dir = item.is_dir()
                is_selected = state["selected_file"] == item
                icon = "folder" if is_dir else _get_file_icon(item.suffix)
                color = "text-blue-400" if is_dir else "text-gray-400"
                bg_class = "bg-blue-100" if is_selected else "hover:bg-gray-50"

                with ui.row().classes(
                    f"w-full items-center gap-2 cursor-pointer {bg_class} p-2 border-b border-gray-100"
                ) as row:
                    ui.icon(icon, size="16px").classes(color)
                    ui.label(item.name).classes("text-xs text-gray-700 flex-1 truncate")

                    if not is_dir:
                        try:
                            size = item.stat().st_size
                            ui.label(_format_file_size(size)).classes("text-[10px] text-gray-400")
                        except Exception:
                            pass

                    if is_dir:
                        row.on("click", lambda i=item: navigate_to(i))
                    else:
                        row.on("click", lambda i=item: select_file(i))

    # ============================================
    # Preview
    # ============================================

    @ui.refreshable
    def render_preview():
        preview_container.clear()
        selected = state["selected_file"]

        if not selected:
            with preview_container:
                with ui.column().classes("w-full h-full items-center justify-center gap-3"):
                    ui.icon("preview", size="48px").classes("text-gray-300")
                    ui.label("Select a file to preview").classes("text-sm text-gray-400")
            return

        with preview_container:
            with ui.row().classes(
                "w-full items-center justify-between p-3 bg-gray-50 border-b border-gray-200 shrink-0"
            ):
                with ui.column().classes("gap-0 flex-1 min-w-0"):
                    ui.label(selected.name).classes("text-xs font-bold text-gray-700 truncate")
                    try:
                        size = selected.stat().st_size
                        ui.label(_format_file_size(size)).classes("text-[10px] text-gray-400")
                    except Exception:
                        pass

            suffix = selected.suffix.lower()
            if suffix in {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff"}:
                _render_image_preview(selected)
            elif suffix in {
                ".txt",
                ".log",
                ".star",
                ".json",
                ".yaml",
                ".sh",
                ".py",
                ".out",
                ".err",
                ".md",
                ".tlt",
                ".aln",
                "",
            } or suffix.startswith("."):
                _render_text_preview(selected)
            else:
                _render_unsupported_preview(selected)

    # ============================================
    # Preview renderers
    # ============================================

    def _render_image_preview(file_path: Path):
        try:
            with open(file_path, "rb") as f:
                image_data = f.read()
            b64_data = base64.b64encode(image_data).decode()
            mime_type = "image/png" if file_path.suffix.lower() == ".png" else "image/jpeg"
            data_uri = f"data:{mime_type};base64,{b64_data}"
        except Exception as e:
            with ui.column().classes("w-full h-full items-center justify-center p-4"):
                ui.icon("error", size="48px").classes("text-red-400")
                ui.label(f"Failed to load image: {e}").classes("text-sm text-red-600")
            return

        zoom_state = {"level": 1.0}

        def zoom_in():
            zoom_state["level"] = min(zoom_state["level"] * 1.2, 5.0)
            img.style(f"transform: scale({zoom_state['level']}); transition: transform 0.2s;")

        def zoom_out():
            zoom_state["level"] = max(zoom_state["level"] / 1.2, 0.3)
            img.style(f"transform: scale({zoom_state['level']}); transition: transform 0.2s;")

        def reset_zoom():
            zoom_state["level"] = 1.0
            img.style("transform: scale(1); transition: transform 0.2s;")

        with ui.row().classes("w-full justify-center gap-1 p-2 bg-white border-b border-gray-200"):
            ui.button(icon="zoom_in", on_click=zoom_in).props("flat dense round size=sm").tooltip("Zoom In")
            ui.button(icon="zoom_out", on_click=zoom_out).props("flat dense round size=sm").tooltip("Zoom Out")
            ui.button(icon="center_focus_strong", on_click=reset_zoom).props("flat dense round size=sm").tooltip(
                "Reset"
            )

        with (
            ui.element("div")
            .classes("w-full bg-gray-900")
            .style(
                "height: calc(80vh - 100px); overflow: auto; display: flex; align-items: center; justify-content: center;"
            )
        ):
            img = ui.image(data_uri).style(
                "max-width: 100%; max-height: 100%; object-fit: contain; transform-origin: center; cursor: zoom-in;"
            )

    def _render_text_preview(file_path: Path):
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read(50000)
            with ui.scroll_area().classes("w-full").style("flex: 1 1 0%; min-height: 0;"):
                ui.code(content).classes("w-full text-xs p-3")
        except Exception as e:
            ui.label(f"Cannot read: {e}").classes("text-red-500 p-4")

    def _render_unsupported_preview(file_path: Path):
        with ui.column().classes("w-full h-full items-center justify-center gap-2 p-4"):
            ui.icon("description", size="48px").classes("text-gray-300")
            ui.label("No preview available").classes("text-sm text-gray-400")
            ui.label(f"Type: {file_path.suffix or 'unknown'}").classes("text-xs text-gray-500")

    # ============================================
    # Navigation
    # ============================================

    def navigate_to(path: Path):
        state["current_path"] = path
        state["selected_file"] = None
        path_label.set_text(str(path.relative_to(job_dir) if job_dir in path.parents else path.name))
        render_file_list.refresh()
        render_preview.refresh()

    def select_file(file_path: Path):
        state["selected_file"] = file_path
        render_file_list.refresh()
        render_preview.refresh()

    # ============================================
    # Helpers
    # ============================================

    def _get_file_icon(suffix: str) -> str:
        icon_map = {
            ".png": "image",
            ".jpg": "image",
            ".jpeg": "image",
            ".mrc": "view_in_ar",
            ".star": "table_chart",
            ".json": "data_object",
            ".txt": "description",
            ".log": "article",
            ".sh": "terminal",
            ".py": "code",
        }
        return icon_map.get(suffix.lower(), "insert_drive_file")

    def _format_file_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"

    # ============================================
    # Layout
    # ============================================

    with ui.row().classes("w-full border border-gray-200 rounded-lg bg-white").style("height: 80vh; gap: 0;"):
        with ui.column().classes("border-r border-gray-200").style("width: 35%; height: 100%; overflow-y: auto;"):
            with ui.row().classes("w-full items-center justify-between p-3 bg-gray-50 border-b border-gray-200"):
                ui.label("Job Files").classes("text-[10px] font-black text-gray-400 uppercase")
                path_label = ui.label(job_dir.name).classes("text-xs text-gray-600 font-mono truncate")
                ui.button(
                    icon="refresh", on_click=lambda: (render_file_list.refresh(), render_preview.refresh())
                ).props("flat dense round size=sm").classes("text-gray-400 hover:text-blue-500")

            file_list_container = ui.column().classes("w-full gap-0")

        with ui.column().classes("bg-white").style("width: 65%; height: 100%;"):
            preview_container = ui.column().classes("w-full h-full")

    render_file_list()
    render_preview()


def view_file_dialog(file_path: Path):
    """Simple file content viewer dialog."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read(50000)
        with ui.dialog() as dialog, ui.card().classes("w-[70vw] max-w-4xl"):
            with ui.row().classes("w-full items-center justify-between mb-2"):
                ui.label(file_path.name).classes("text-sm font-bold")
                ui.button(icon="close", on_click=dialog.close).props("flat round dense")
            ui.code(content).classes("w-full max-h-[60vh] overflow-auto text-xs")
            dialog.open()
    except Exception as e:
        ui.notify(f"Cannot read file: {e}", type="negative")
