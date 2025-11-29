# ui/landing_page.py
from nicegui import ui
from backend import CryoBoostBackend
from ui.data_import_panel import build_data_import_panel

def build_landing_page(backend: CryoBoostBackend):
    
    def on_project_ready():
        ui.navigate.to("/workspace")

    # Full-height layout, no centering - use the space
    with ui.column().classes("w-full min-h-screen bg-gray-50"):
        
        # Compact header
        with ui.row().classes("w-full bg-slate-800 px-6 py-3 items-center justify-between"):
            with ui.row().classes("items-center gap-3"):
                with ui.element('div').classes("bg-blue-500 p-1.5 rounded"):
                    ui.icon("science", size="24px").classes("text-white")
                with ui.column().classes("gap-0"):
                    ui.label("CryoBoost Orchestrator").classes("text-base font-bold text-white")
                    ui.label("Project Setup").classes("text-xs text-slate-400")

        # Main content - centered but wider
        with ui.row().classes("w-full flex-1 justify-center px-4 py-4"):
            with ui.card().classes("w-full max-w-4xl p-0 shadow-lg border border-gray-200 rounded-lg overflow-hidden"):
                callbacks = {
                    "rebuild_pipeline_ui": on_project_ready, 
                    "check_and_update_statuses": lambda: None 
                }
                build_data_import_panel(backend, callbacks)