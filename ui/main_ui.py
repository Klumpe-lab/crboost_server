"""
Main UI router.
Implements the 2-Phase workflow:
1. Landing Page (New/Load)
2. Workspace Page (Pipeline)

Phase 2: Each page handler calls await client.connected() before
accessing app.storage.tab (via get_ui_state_manager).
"""

import asyncio

from nicegui import ui, Client, app

from backend import CryoBoostBackend
from services.configs.user_prefs_service import get_prefs_service

from services.project_state import get_project_state
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

            /* Ensure the NiceGUI span wrapper doesn't clip the dot animation */
            span:has(> .status-dot) {
                overflow: visible !important;
                line-height: 0;
            }

            .status-dot {
                transform-origin: center;
            }

            @keyframes cb-pulse {
                0%, 100% { transform: scale(1); opacity: 1; }
                50%      { transform: scale(1.4); opacity: 0.6; }
            }

            @keyframes cb-pulse-glow {
                0%, 100% {
                    transform: scale(1);
                    opacity: 1;
                    filter: drop-shadow(0 0 0px rgba(59, 130, 246, 0));
                }
                50% {
                    transform: scale(1.5);
                    opacity: 0.7;
                    filter: drop-shadow(0 0 6px rgba(59, 130, 246, 0.8));
                }
            }

            .pulse-running {
                animation: cb-pulse-glow 1.5s ease-in-out infinite;
            }
            .pulse-success {
                animation: cb-pulse 2.0s ease-in-out infinite;
            }
            .pulse-failed {
                animation: cb-pulse 1.4s ease-in-out infinite;
            }
            .pulse-orphaned {
                animation: cb-pulse 1.6s ease-in-out infinite;
            }
            .pulse-scheduled {
                /* intentionally static */
            }
        </style>
    """)

    # --- PAGE 1: LANDING (Setup) ---
    @ui.page("/")
    async def landing_page(client: Client):
        await client.connected()

        ui_mgr = get_ui_state_manager()

        current_state = get_project_state()

        if current_state.pipeline_active:
            print(f"[LANDING] Blocked state reset - pipeline is active")
            ui.notify("A pipeline is currently running. Redirecting to workspace.", type="warning", position="top")
            await asyncio.sleep(0.5)
            ui.navigate.to("/workspace")
            return

        ui_mgr.reset()

        # Hydrate user preferences from storage BEFORE building UI
        prefs_service = get_prefs_service()
        prefs = prefs_service.load_from_app_storage(app.storage.user)

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
        await client.connected()

        ui_mgr = get_ui_state_manager()

        if not ui_mgr.is_project_created:
            if ui_mgr.project_path and ui_mgr.project_path.exists():
                from services.project_state import get_project_state_for

                recovered_state = get_project_state_for(ui_mgr.project_path)
                print(f"[WORKSPACE] Recovering UI state from registry: {recovered_state.project_name}")
                ui_mgr.load_from_project(
                    project_path=recovered_state.project_path,
                    scheme_name="recovered",
                    jobs=list(recovered_state.jobs.keys()),
                )
            else:
                ui.navigate.to("/")
                return

        build_workspace_page(backend)

    # --- AUX PAGES ---
    @ui.page("/cluster-info")
    async def cluster_info_page(client: Client):
        with ui.column().classes("p-8"):
            ui.label("Cluster Info Stub")
            ui.button("Back", on_click=lambda: ui.navigate.back())
