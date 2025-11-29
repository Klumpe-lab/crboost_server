# ui/workspace_page.py
from nicegui import ui
from backend import CryoBoostBackend
from services.project_state import get_project_state
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.ui_state import get_ui_state_manager

def build_workspace_page(backend: CryoBoostBackend):
    ui_mgr = get_ui_state_manager()
    state = get_project_state()
    
    # Header
    with ui.header().classes("bg-white border-b border-gray-200 text-gray-800 h-auto px-4 py-2"):
        with ui.row().classes("w-full items-center justify-between"):
            # Left: Project info
            with ui.row().classes("items-center gap-6"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("layers", size="20px").classes("text-blue-600")
                    ui.label(state.project_name).classes("font-bold text-sm")
                
                # Project metadata - compact display
                with ui.row().classes("items-center gap-4 text-xs text-gray-500"):
                    if state.project_path:
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("folder", size="14px").classes("text-gray-400")
                            ui.label(str(state.project_path)).classes("font-mono")
                    
                    if state.movies_glob:
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("movie", size="14px").classes("text-gray-400")
                            ui.label(state.movies_glob).classes("font-mono")
                    
                    if state.mdocs_glob:
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("description", size="14px").classes("text-gray-400")
                            ui.label(state.mdocs_glob).classes("font-mono")
                
            # Right: Close button
            ui.button("Close Project", icon="close", on_click=lambda: ui.navigate.to("/")).props("flat dense no-caps text-color=red").classes("text-xs")

    # Main Content
    callbacks = {}
    with ui.column().classes("w-full h-[calc(100vh-3rem)] p-0"):
        build_pipeline_builder_panel(backend, callbacks)