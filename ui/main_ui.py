# ui/main_ui.py
"""
Main UI router.
Implements the 2-Phase workflow: 
1. Landing Page (New/Load) 
2. Workspace Page (Pipeline)
"""
from nicegui import ui, Client, app

from backend import CryoBoostBackend
from services.configs.user_prefs_service import get_prefs_service
from services.project_state import get_project_state, reset_project_state
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
            
            html, body {
                height: 100vh !important;
                margin: 0 !important;
                padding: 0 !important;
                overflow: hidden !important;
            }
            
            .nicegui-content {
                height: 100vh !important;
                display: flex !important;
                flex-direction: column !important;
                font-family: 'IBM Plex Sans', sans-serif !important;
                font-size: 12px !important;
                margin: 0 !important;
                padding: 0 !important;
            }
            
            .q-btn { 
                font-family: 'IBM Plex Sans', sans-serif !important; 
                text-transform: none !important; 
            }
            
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
        ui_mgr = get_ui_state_manager()
        current_state = get_project_state()
        
        # CRITICAL GUARD: If pipeline is running, don't let stale tabs nuke state
        if current_state.pipeline_active:
            print(f"[LANDING] Blocked state reset - pipeline is active")
            ui.notify(
                "A pipeline is currently running. Redirecting to workspace.", 
                type="warning",
                position="top"
            )
            # Small delay to let notification show
            await asyncio.sleep(0.5)
            ui.navigate.to("/workspace")
            return
        
        # Safe to reset UI state (per-client)
        ui_mgr.reset()
        
        # Safe to reset project state (pipeline not running)
        reset_project_state()
        
        # Hydrate user preferences from storage BEFORE building UI
        prefs_service = get_prefs_service()
        prefs = prefs_service.load_from_app_storage(app.storage.user)
        
        # Pre-populate UI state with saved preferences
        if prefs.project_base_path:
            ui_mgr.update_data_import(project_base_path=prefs.project_base_path)
        if prefs.movies_glob:
            ui_mgr.update_data_import(movies_glob=prefs.movies_glob)
        if prefs.mdocs_glob:
            ui_mgr.update_data_import(mdocs_glob=prefs.mdocs_glob)
        
        build_landing_page(backend)

    # --- PAGE 2: WORKSPACE (Pipeline) ---
    @ui.page("/workspace")
    async def workspace_page(client: Client):
        ui_mgr = get_ui_state_manager()
        
        # Safety check: if no project is loaded, go back to start
        # BUT if pipeline is running, try to recover state first
        if not ui_mgr.is_project_created:
            # Check if there's actually a project in backend state
            current_state = get_project_state()
            if current_state.project_path and current_state.project_path.exists():
                # Recover: sync UI state from backend state
                print(f"[WORKSPACE] Recovering UI state from backend: {current_state.project_name}")
                ui_mgr.load_from_project(
                    project_path=current_state.project_path,
                    scheme_name="recovered",
                    jobs=list(current_state.jobs.keys())
                )
            else:
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


# Need to import asyncio for the sleep
import asyncio