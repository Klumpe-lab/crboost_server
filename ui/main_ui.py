"""
Main UI router.
"""

import logging
import time
from dataclasses import replace
from pathlib import Path

from nicegui import ui, Client, app

from backend import CryoBoostBackend
from services.configs.config_service import get_config_service
from services.configs.user_prefs_service import get_prefs_service
from services.project_resolve import resolve_project
from services.result import ErrorCode
from ui.dashboard.css import ensure_assets_loaded
from services.project_state import get_project_state_for
from ui.open_project import load_project_into_tab, workspace_url
from services.routes import Route, disambiguating_base, parse_route, route_to_path
from ui.routing import RouteWriter, apply_route
from ui.ui_state import get_ui_state_manager
from ui.agent_plan_page import render_agent_plan_page
from ui.napari_open_page import render_napari_open_page
from ui.components.buttons import house_button
from ui.data_import_panel import build_data_import_panel
from ui.workspace_page import build_workspace_page

logger = logging.getLogger(__name__)

# The page shown while a project loads: the workspace's whole viewport, a spinner in its middle.
_OPENING_STYLE = (
    "position: fixed; inset: 0; display: flex; flex-direction: column; align-items: center; "
    "justify-content: center; gap: 10px; background: #ffffff;"
)


def create_ui_router(backend: CryoBoostBackend):

    # --- PAGE 1: LANDING ---
    @ui.page("/")
    async def landing_page(client: Client):
        await client.connected()
        ensure_assets_loaded()

        ui_mgr = get_ui_state_manager()
        ui_mgr.reset()

        prefs_service = get_prefs_service()
        prefs = prefs_service.load_from_app_storage(app.storage.user)
        confined = get_config_service().creation_base
        if confined is not None:
            # A server that makes new projects in one base (or keeps to it) shows that base, whatever the prefs
            # last pointed at.
            ui_mgr.update_data_import(project_base_path=str(confined))
        elif prefs.project_base_path:
            ui_mgr.update_data_import(project_base_path=prefs.project_base_path)
        if prefs.movies_glob:
            ui_mgr.update_data_import(movies_glob=prefs.movies_glob)
        if prefs.mdocs_glob:
            ui_mgr.update_data_import(mdocs_glob=prefs.mdocs_glob)

        def on_project_ready():
            ui.navigate.to("/workspace")

        callbacks = {"rebuild_pipeline_ui": on_project_ready, "check_and_update_statuses": lambda: None}

        # The viewport itself, like the workspace: the projects roster fills what the status
        # strip leaves and ends at the bottom edge, whatever the strip's height.
        with (
            ui.column()
            .classes("bg-gray-50 px-6 py-3")
            .style("position: fixed; inset: 0; overflow: hidden; flex-wrap: nowrap; box-sizing: border-box;")
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

    # --- PAGE 2: WORKSPACE (addressable at /p/<project>/<view>/<target>) ---

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
        # The first open of a project parses its state and tilt-series registry, seconds on a
        # large one: say so at once instead of leaving the new page blank meanwhile.
        started = time.monotonic()
        with ui.element("div").style(_OPENING_STYLE) as opening:
            ui.spinner(size="lg", color="#94a3b8")
            ui.label(f"Opening {target.name}…").style("font-size: 11px; color: #64748b;")
        if not await load_project_into_tab(backend, ui_mgr, target):
            ui.navigate.to("/")
            return
        loaded = time.monotonic()

        # The canonical base, not the one the URL happened to carry: a link that pinned a
        # base it did not need gets tidied to the short form by the first write.
        writer = RouteWriter(target.name, disambiguating_base(target))
        writer.adopt(route)
        writer.suppressed = True
        opening.delete()
        callbacks = build_workspace_page(backend, writer)
        built = time.monotonic()
        logger.info(
            "%s · opened in %.1f s (load %.1f s, build %.1f s)",
            target.name,
            built - started,
            loaded - started,
            built - loaded,
        )

        async def _apply() -> None:
            try:
                await apply_route(callbacks, route, target)
            finally:
                writer.suppressed = False

        # After the first paint: the lazy views (journey, gallery, viewer, protocols)
        # build themselves inside their own callbacks, and those need the DOM to exist.
        ui.timer(0.05, _apply, once=True)

    def _render_project_chooser(route: Route, candidates: list[str]) -> None:
        """Two bases hold a project of this name. Do not guess: show both absolute
        paths, each a link that pins its base."""
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
            ui_mgr.load_from_project(project_path=recovered_state.project_path, scheme_name="recovered")
        ui.navigate.to(workspace_url(ui_mgr.project_path))

    # --- AUX PAGES ---
    @ui.page("/agent/plan/{plan_id}")
    async def agent_plan_page(client: Client, plan_id: str):
        await client.connected()
        ensure_assets_loaded()
        render_agent_plan_page(plan_id)

    @ui.page("/napari/open", title="napari")
    async def napari_open_page(client: Client, volume: str = ""):
        await client.connected()
        ensure_assets_loaded()
        render_napari_open_page(backend, volume)

    @ui.page("/cluster-info")
    async def cluster_info_page(client: Client):
        await client.connected()
        ensure_assets_loaded()
        with ui.column().classes("p-8"):
            ui.label("Cluster Info Stub")
            house_button("Back", lambda: ui.navigate.back())
