"""
Main UI router.
"""

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

            /* The RUNNING-job "working" indicator is a self-contained inline
               SVG/SMIL pulsating dot (ui/status_indicator._running_spinner_html)
               — no @keyframes here, because CSS-based spinners (glyph rotate,
               ::before content-cycle, class-animated dots) kept rendering blank
               on the v-html-injected roster spans, most likely a cache-stale
               Python-injected stylesheet. SVG carries its own animation. */

            /* ── Config + I/O dropdowns ──────────────────────────────────
               Clean 1px slate-bordered box + themed popup, replacing the
               default Quasar Material underline/float look. Applied via the
               .cb-select class in ui/job_plugins/_field_styles.py (config
               parameter selects) and ui/pipeline_builder/io_config_component.py
               (I/O source menus + their popups). */
            .cb-select .q-field__control {
                min-height: 24px; padding: 0 6px;
                border: 1px solid #e2e8f0; border-radius: 4px;
                background: #fff; transition: border-color .12s ease;
            }
            .cb-select .q-field__control:hover { border-color: #cbd5e1; }
            .cb-select.q-field--focused .q-field__control { border-color: #94a3b8; }
            .cb-select .q-field__control:before,
            .cb-select .q-field__control:after { display: none !important; }
            .cb-select .q-field__native,
            .cb-select .q-field__input {
                font-family: 'IBM Plex Sans', sans-serif; font-size: 11px;
                color: #1e293b; padding: 0; line-height: 22px;
            }
            .cb-select .q-field__marginal,
            .cb-select .q-field__append { height: 22px; }
            .cb-select .q-field__append .q-icon { font-size: 16px; color: #94a3b8; }
            .cb-select.q-field--disabled .q-field__control { background: #f8fafc; }

            .cb-select-popup {
                border: 1px solid #e2e8f0; border-radius: 5px;
                box-shadow: 0 6px 18px rgba(15,23,42,.10);
            }
            .cb-select-popup .q-item {
                min-height: 26px; padding: 3px 10px;
                font-family: 'IBM Plex Sans', sans-serif; font-size: 11px; color: #334155;
            }
            .cb-select-popup .q-item:hover { background: #f1f5f9; }
            .cb-select-popup .q-item.q-manual-focusable--focused,
            .cb-select-popup .q-item--active { background: #eef2f6; color: #1e293b; }

            /* I/O source selector (custom button + menu of status-dot rows) */
            .cb-io-src:hover { border-color: #cbd5e1 !important; }
            /* Clear hover feedback on each candidate row in the source dropdown. */
            .cb-io-menu-item { transition: background .1s ease; }
            .cb-io-menu-item:hover { background: #e2e8f0; }

            /* Draggable divider between the job roster and the params/main area.
               A thin transparent grab-zone with a subtle line that lights up on
               hover/drag; resize logic is client-side JS (workspace_page). */
            .cb-roster-resizer {
                width: 6px; flex-shrink: 0; height: 100%; cursor: col-resize;
                background: transparent; position: relative; z-index: 21;
            }
            .cb-roster-resizer::after {
                content: ''; position: absolute; left: 50%; top: 0;
                transform: translateX(-50%); width: 1px; height: 100%;
                background: transparent; transition: background .12s ease, width .12s ease;
            }
            .cb-roster-resizer:hover::after,
            .cb-roster-resizer.dragging::after { background: #93c5fd; width: 2px; }
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
            from ui.landing_status_strip import mount_landing_status_strip

            mount_landing_status_strip(backend)
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
