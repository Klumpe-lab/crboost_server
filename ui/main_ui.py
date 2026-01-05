# ui/main_ui.py
"""
Main UI router.
Implements the 2-Phase workflow: 
1. Landing Page (New/Load) 
2. Workspace Page (Pipeline)
"""
from nicegui import ui, Client, app

from backend import CryoBoostBackend
from ui.ui_state import get_ui_state_manager
from ui.landing_page import build_landing_page
from ui.workspace_page import build_workspace_page


def create_ui_router(backend: CryoBoostBackend):
    """Create the UI router with distinct phases."""

    # --- SHARED STYLES ---
    ui.add_head_html("""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
            @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap');
            
            body, .nicegui-content {
                font-family: 'IBM Plex Sans', sans-serif !important;
                font-size: 12px !important;
                margin: 0 !important;
                padding: 0 !important;
            }
            .q-btn { font-family: 'IBM Plex Sans', sans-serif !important; text-transform: none !important; }
            
            /* Status Animation */
            .pulse-running {
                animation: pulse-blue 1.5s ease-in-out infinite;
            }
            @keyframes pulse-blue {
                0%, 100% { transform: scale(1); opacity: 1; }
                50% { transform: scale(1.2); opacity: 0.7; }
            }
        </style>
    """)

    # --- PAGE 1: LANDING (Setup) ---
    @ui.page("/")
    async def landing_page(client: Client):
        # 1. Reset UI State
        ui_mgr = get_ui_state_manager()
        ui_mgr.reset()
        
        # 2. Reset Backend Project State (Fixes the "Ghost Project" issue)
        from services.project_state import reset_project_state
        reset_project_state()
        
        build_landing_page(backend)

    # --- PAGE 2: WORKSPACE (Pipeline) ---
    @ui.page("/workspace")
    async def workspace_page(client: Client):
        ui_mgr = get_ui_state_manager()
        
        # Safety check: if no project is loaded, go back to start
        if not ui_mgr.is_project_created:
             ui.navigate.to("/")
             return

        # Render the full pipeline UI
        build_workspace_page(backend)

    # --- AUX PAGES ---
    @ui.page("/cluster-info")
    async def cluster_info_page(client: Client):
        with ui.column().classes("p-8"):
             ui.label("Cluster Info Stub")
             ui.button("Back", on_click=lambda: ui.navigate.back())