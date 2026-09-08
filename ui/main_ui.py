"""
Main UI router.
"""

from dataclasses import replace
from pathlib import Path

from nicegui import ui, Client, app

from backend import CryoBoostBackend
from services.configs.user_prefs_service import get_prefs_service
from services.project_resolve import resolve_project
from services.result import ErrorCode
from ui.dashboard.css import ensure_assets_loaded
from services.project_state import get_project_state_for
from ui.open_project import disambiguating_base, load_project_into_tab, workspace_url
from ui.routing import Route, RouteWriter, apply_route, parse_route, route_to_path
from ui.ui_state import get_ui_state_manager
from ui.components.buttons import house_button
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

            /* The .cb-select / .cb-field control chrome lives in ui/dashboard/css.py
               (_CB_CSS) ONLY — every page calls ensure_assets_loaded(). A duplicate
               copy used to sit here; it existed only to drift (deleted 2026-08-21,
               picking-UI roadmap 08 S1). */

            /* NOTE (2026-08-21): do NOT add new app-wide rules to this block.
               This <style> is baked into the page shell, and the shell is
               served stale on this deployment — new rules here never reached
               the browser (the .cb-btn / spinner-kill incident). App-wide
               control chrome goes in ui/dashboard/css.py (_CB_CSS), which
               ensure_assets_loaded() injects per client AFTER connect, over
               the socket; structural styling goes inline on the elements. */
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
        ensure_assets_loaded()

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

    # --- PAGE 2: WORKSPACE (roadmap 17 — addressable at /p/<project>/<view>/<target>) ---

    async def _open_routed_workspace(client: Client, route: Route) -> None:
        """One body behind all four `/p/...` shapes: resolve the project from its
        directory name, load it into this tab, build the workspace, then drive it onto
        the route's view + target."""
        await client.connected()
        ensure_assets_loaded()

        # resolve_project reads the user's bases out of the prefs singleton, which is
        # only populated once someone loads it from this browser's storage.
        get_prefs_service().load_from_app_storage(app.storage.user)

        ui_mgr = get_ui_state_manager()
        found = resolve_project(route.project, base=route.base, current=ui_mgr.project_path)
        if not found["success"]:
            if found.get("code") == ErrorCode.PROJECT_AMBIGUOUS:
                _render_project_chooser(route, found.get("candidates") or [])
                return
            searched = found.get("searched") or []
            where = f" Searched: {', '.join(searched)}." if searched else ""
            ui.notify(f"{found['error']}{where}", type="warning", timeout=9000)
            ui.navigate.to("/")
            return

        # Unconditional: StateService.load_project is load-if-absent (the in-memory state
        # stays authoritative), so re-entering a project this tab already holds costs a
        # dict lookup and correctly re-stamps it onto the "recently viewed" MRU.
        target = found["path"]
        if not await load_project_into_tab(backend, ui_mgr, target):
            ui.navigate.to("/")
            return

        # The canonical base, not the one the URL happened to carry: a link that pinned a
        # base it did not need gets tidied to the short form by the first write.
        writer = RouteWriter(target.name, disambiguating_base(target))
        writer.adopt(route)
        writer.suppressed = True
        callbacks = build_workspace_page(backend, writer)

        async def _apply() -> None:
            try:
                await apply_route(callbacks, route, target)
            finally:
                writer.suppressed = False

        # After the first paint: the lazy views (journey, gallery, viewer, protocols)
        # build themselves inside their own callbacks, and those need the DOM to exist.
        ui.timer(0.05, _apply, once=True)

    def _render_project_chooser(route: Route, candidates: list[str]) -> None:
        """Two bases hold a project of this name. Do not guess (CLAUDE.md, *Surfacing
        uncertainty*) — show both absolute paths, each a link that pins its base."""
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("alt_route", size="48px").classes("text-amber-400")
            ui.label(f"'{route.project}' exists in {len(candidates)} places").classes("text-base text-gray-700")
            ui.label("Pick the one you meant — the link will pin it.").classes("text-xs text-gray-500")
            with ui.column().classes("gap-2 items-stretch"):
                for cand in candidates:
                    base = str(Path(cand).parent)
                    url = route_to_path(replace(route, base=base))
                    house_button(cand, lambda u=url: ui.navigate.to(u)).style(
                        "font-family: 'IBM Plex Mono', monospace; justify-content: flex-start;"
                    )

    @ui.page("/p/{project}")
    async def project_page(client: Client, project: str):
        await _open_routed_workspace(client, parse_route(project, [], client.request.query_params))

    @ui.page("/p/{project}/{view}")
    async def project_view_page(client: Client, project: str, view: str):
        await _open_routed_workspace(client, parse_route(project, [view], client.request.query_params))

    @ui.page("/p/{project}/{view}/{a}")
    async def project_view_a_page(client: Client, project: str, view: str, a: str):
        await _open_routed_workspace(client, parse_route(project, [view, a], client.request.query_params))

    @ui.page("/p/{project}/{view}/{a}/{b}")
    async def project_view_ab_page(client: Client, project: str, view: str, a: str, b: str):
        await _open_routed_workspace(client, parse_route(project, [view, a, b], client.request.query_params))

    # Kept as a redirect so every existing `ui.navigate.to("/workspace")` in the codebase
    # keeps working: it means "this tab's project", which the addressable URL spells out.
    @ui.page("/workspace")
    async def workspace_page(client: Client):
        await client.connected()
        ensure_assets_loaded()

        get_prefs_service().load_from_app_storage(app.storage.user)
        ui_mgr = get_ui_state_manager()
        if not ui_mgr.project_path or not ui_mgr.project_path.exists():
            ui.navigate.to("/")
            return
        if not ui_mgr.is_project_created:
            recovered_state = get_project_state_for(ui_mgr.project_path)
            ui_mgr.load_from_project(
                project_path=recovered_state.project_path,
                scheme_name="recovered",
                jobs=list(recovered_state.jobs.keys()),
            )
        ui.navigate.to(workspace_url(ui_mgr.project_path))

    # --- AUX PAGES ---
    @ui.page("/cluster-info")
    async def cluster_info_page(client: Client):
        await client.connected()
        ensure_assets_loaded()
        with ui.column().classes("p-8"):
            ui.label("Cluster Info Stub")
            house_button("Back", lambda: ui.navigate.back())
