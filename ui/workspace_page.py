# ui/pages/workspace_page.py
from nicegui import ui
from backend import CryoBoostBackend
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.ui_state import get_ui_state_manager

def build_workspace_page(backend: CryoBoostBackend):
    ui_mgr = get_ui_state_manager()
    
    # Header
    with ui.header().classes("bg-white border-b border-gray-200 text-gray-800 h-14 px-4 flex items-center justify-between"):
        with ui.row().classes("items-center gap-2"):
            ui.icon("layers", size="24px").classes("text-blue-600")
            ui.label(f"Project: {ui_mgr.data_import.project_name}").classes("font-bold")
            
        with ui.row().classes("items-center gap-2"):
             ui.button("Close Project", icon="close", on_click=lambda: ui.navigate.to("/")).props("flat dense no-caps text-color=red")

    # Main Content Area
    # We reuse the pipeline builder panel, which contains the logic for 
    # Job selection (left/top) and Job Details (main area).
    # Since we removed the split pane, the pipeline builder now takes the full screen.
    
    callbacks = {} # Add specific callbacks if needed
    
    with ui.column().classes("w-full h-[calc(100vh-3.5rem)] p-0"):
        build_pipeline_builder_panel(backend, callbacks)