"""
Main UI router.
"""

import asyncio

from nicegui import ui, Client, app

from backend import CryoBoostBackend
from services.configs.user_prefs_service import get_prefs_service
from services.project_state import get_project_state_for
from ui.ui_state import get_ui_state_manager
from ui.data_import_panel import build_data_import_panel
from ui.workspace_page import build_workspace_page


def create_ui_router(backend: CryoBoostBackend):

    ui.add_head_html("""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
            @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap');

            html, body {
                height: 100%;
                margin: 0;
                padding: 0;
                overflow: hidden;
            }

            .nicegui-content {
                height: 100%;
                overflow: hidden;
                display: flex;
                flex-direction: column;
                font-family: 'IBM Plex Sans', sans-serif;
                font-size: 12px;
                margin: 0;
                padding: 0;
            }

            .q-btn {
                font-family: 'IBM Plex Sans', sans-serif !important;
                text-transform: none !important;
            }

            span:has(> .status-dot) {
                overflow: visible !important;
                line-height: 0;
            }

            .status-dot { transform-origin: center; }

            @keyframes cb-pulse {
                0%, 100% { transform: scale(1); opacity: 1; }
                50%       { transform: scale(1.4); opacity: 0.6; }
            }

            @keyframes cb-pulse-glow {
                0%, 100% {
                    transform: scale(1); opacity: 1;
                    filter: drop-shadow(0 0 0px rgba(59,130,246,0));
                }
                50% {
                    transform: scale(1.5); opacity: 0.7;
                    filter: drop-shadow(0 0 6px rgba(59,130,246,0.8));
                }
            }

            .pulse-running   { animation: cb-pulse-glow 1.5s ease-in-out infinite; }
            .pulse-success   { animation: cb-pulse 2.0s ease-in-out infinite; }
            .pulse-failed    { animation: cb-pulse 1.4s ease-in-out infinite; }
            .pulse-orphaned  { animation: cb-pulse 1.6s ease-in-out infinite; }

            /* Braille-glyph spinner, server-tick-free. The braille glyph is
               emitted as the element's text content (so it shows even on the
               oldest browsers); CSS rotates the glyph in place. The
               `animation` property is also set inline on the elements
               themselves (see pipeline_roster._status_widget) so CSS-class
               specificity issues or cached stylesheets can't disable the
               spin. Replaces the prior 0.17 s ui.timer + ui.run_javascript
               broadcast — see ui/components/reactive.py. */
            @keyframes cb-braille-rotate {
                from { transform: rotate(0deg); }
                to   { transform: rotate(360deg); }
            }
        </style>
    """)

    # --- PAGE 1: LANDING ---
    @ui.page("/")
    async def landing_page(client: Client):
        await client.connected()

        ui_mgr = get_ui_state_manager()
        ui_mgr.reset()

        prefs_service = get_prefs_service()
        prefs = prefs_service.load_from_app_storage(app.storage.user)
        if prefs.project_base_path:
            ui_mgr.update_data_import(project_base_path=prefs.project_base_path)
        if prefs.movies_glob:
            ui_mgr.update_data_import(movies_glob=prefs.movies_glob)
        if prefs.mdocs_glob:
            ui_mgr.update_data_import(mdocs_glob=prefs.mdocs_glob)

        def on_project_ready():
            ui.navigate.to("/workspace")

        callbacks = {"rebuild_pipeline_ui": on_project_ready, "check_and_update_statuses": lambda: None}

        with (
            ui.column()
            .classes("w-full bg-gray-50 px-6 py-3")
            .style("height: 100%; overflow-y: auto; box-sizing: border-box;")
        ):
            build_data_import_panel(backend, callbacks)

        # Mount the background-task tray here too so tasks still in flight
        # when the user returns to the landing page (or are started from
        # actions on this page) remain visible. project_path may be None
        # before a project is loaded; the registry's snapshot then shows
        # all tasks across projects, which matches user expectation on
        # a project-selector screen.
        from ui.background_task_tray import mount_background_task_tray

        mount_background_task_tray(
            project_path_provider=lambda: str(ui_mgr.project_path) if ui_mgr.project_path else None
        )

    # --- PAGE 2: WORKSPACE ---
    @ui.page("/workspace")
    async def workspace_page(client: Client):
        await client.connected()

        ui_mgr = get_ui_state_manager()

        if not ui_mgr.is_project_created:
            if ui_mgr.project_path and ui_mgr.project_path.exists():
                recovered_state = get_project_state_for(ui_mgr.project_path)
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
