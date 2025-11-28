# ui/pages/landing_page.py
from nicegui import ui
from backend import CryoBoostBackend
from ui.data_import_panel import build_data_import_panel

def build_landing_page(backend: CryoBoostBackend):
    
    # Callback to transition to the workspace
    def on_project_ready():
        ui.navigate.to("/workspace")

    # Layout: Centered, clean, focused on data entry
    with ui.column().classes("w-full min-h-screen items-center justify-center bg-gray-50 p-8"):
        
        # Main Container - Increased max-height logic
        with ui.card().classes("w-full max-w-3xl p-0 shadow-xl border border-gray-200 rounded-xl overflow-hidden"):
            
            # Header with distinct color
            with ui.row().classes("w-full bg-slate-800 p-6 items-center justify-between"):
                with ui.row().classes("items-center gap-4"):
                    with ui.element('div').classes("bg-blue-500 p-2 rounded-lg"):
                        ui.icon("science", size="32px").classes("text-white")
                    with ui.column().classes("gap-1"):
                        ui.label("CryoBoost Orchestrator").classes("text-xl font-bold text-white")
                        ui.label("Phase 1: Project Setup").classes("text-xs font-medium text-slate-300 uppercase tracking-wider")

            # Content Area - Taller height
            # Changed h-[600px] to h-auto with a large min-height to avoid scrolling
            with ui.row().classes("w-full bg-white h-[850px]"): 
                 
                 callbacks = {
                     "rebuild_pipeline_ui": on_project_ready, 
                     # For load existing:
                     "check_and_update_statuses": lambda: None 
                 }
                 
                 # Constrain the data import panel
                 with ui.column().classes("w-full h-full"):
                     build_data_import_panel(backend, callbacks)