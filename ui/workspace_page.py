import asyncio

from nicegui import ui

from backend import CryoBoostBackend
from ui.background_task_tray import mount_background_task_tray
from ui.components.reactive import SingleFlight
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.species_workbench_panel import build_species_workbench_panel
from ui.ui_state import get_ui_state_manager


def build_workspace_page(backend: CryoBoostBackend):
    ui_mgr = get_ui_state_manager()
    ui_mgr.prepare_for_page_rebuild()

    if not ui_mgr.is_project_created:
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("error_outline", size="64px").classes("text-red-400")
            ui.label("No project loaded").classes("text-xl text-gray-600")
            ui.button("Return to Start", icon="home", on_click=lambda: ui.navigate.to("/"))
        return

    if not ui_mgr.project_path or not ui_mgr.project_path.exists():
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("folder_off", size="64px").classes("text-orange-400")
            ui.label("Project path is invalid").classes("text-xl text-gray-600")
            ui.label(str(ui_mgr.project_path)).classes("text-sm text-gray-400 font-mono")
            ui.button("Return to Start", icon="home", on_click=lambda: ui.navigate.to("/"))
        return

    callbacks = {}
    _mode = {"current": "pipeline"}
    _refs = {}

    def _switch_to(mode_name: str):
        """Swap the visible view in main_area: pipeline / workbench / journey.
        Each is a sibling container toggled via CSS display. Journey also hides
        the 300px job roster for full width and pauses its live-refresh timer
        when not visible (handled by the roster's set_active_mode and the
        journey panel's on_journey_active)."""
        containers = {
            "pipeline": _refs.get("pipeline_container"),
            "workbench": _refs.get("workbench_container"),
            "journey": _refs.get("journey_container"),
        }
        # If already on this mode, toggle back to pipeline.
        if _mode["current"] == mode_name and mode_name != "pipeline":
            mode_name = "pipeline"

        _mode["current"] = mode_name
        for name, c in containers.items():
            if c is None:
                continue
            if name == mode_name:
                c.style("display: flex; flex-direction: column;")
            else:
                c.style("display: none;")

        if mode_name == "pipeline":
            invalidate = callbacks.get("invalidate_tm_tabs")
            if invalidate:
                invalidate()

        # Nav-icon highlight + roster hide/restore (roster owns _roster_visible).
        set_mode = callbacks.get("set_active_mode")
        if set_mode:
            set_mode(_mode["current"])

        # Pause/resume the journey's 4 s live-refresh with its visibility.
        on_journey = callbacks.get("on_journey_active")
        if on_journey:
            on_journey(_mode["current"] == "journey")

    def _toggle_workbench():
        _switch_to("workbench")

    def ensure_pipeline_mode():
        if _mode["current"] != "pipeline":
            _switch_to("pipeline")

    _journey_flight = SingleFlight()
    _journey_built = {"done": False}

    async def _show_journey():
        # SingleFlight: the icon button can be destroyed+recreated mid-click by
        # a roster refresh, so several clicks may land — collapse them to one.
        async with _journey_flight("toggle") as acquired:
            if not acquired:
                return
            _switch_to("journey")  # toggles back to pipeline if already on journey
            if _mode["current"] != "journey":
                return
            if _journey_built["done"]:
                return
            _journey_built["done"] = True
            jc = _refs.get("journey_container")
            if jc is None:
                return
            # Paint a spinner immediately, yield so it flushes to the client,
            # then build. The first per-TS render blocks ~1-2 s; the CSS spinner
            # keeps animating client-side meanwhile. Later switches are an
            # instant display swap (the panel persists).
            with (
                jc,
                ui.element("div").style(
                    "width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; gap: 10px;"
                ),
            ):
                ui.spinner(size="lg", color="indigo")
                ui.label("Loading journey…").classes("text-sm text-gray-500")
            await asyncio.sleep(0.03)
            from ui.tomo_dashboard_dialog import build_journey_panel

            build_journey_panel(jc, callbacks)

    callbacks["toggle_workbench"] = _toggle_workbench
    callbacks["ensure_pipeline_mode"] = ensure_pipeline_mode
    callbacks["toggle_journey"] = _show_journey

    with ui.element("div").style(
        "position: fixed; inset: 0; display: flex; flex-direction: row; "
        "overflow: hidden; gap: 0; margin: 0; padding: 0;"
    ):
        primary_sidebar = ui.element("div").style(
            "width: 60px; min-width: 60px; height: 100%; flex-shrink: 0; "
            "background: #f8fafc; display: flex; flex-direction: column; "
            "align-items: center; gap: 0; overflow: visible; z-index: 20; "
            "border-right: 1px solid #e2e8f0;"
        )

        roster_panel = ui.element("div").style(
            "width: 300px; min-width: 300px; height: 100%; flex-shrink: 0; "
            "background: #ffffff; border-right: 1px solid #e5e7eb; "
            "overflow-y: auto; overflow-x: hidden; "
            "flex-direction: column; gap: 0; display: flex;"
        )

        main_area = ui.element("div").style(
            "flex: 1; min-width: 0; height: 100%; overflow: hidden; display: flex; flex-direction: row; gap: 0;"
        )

        with main_area:
            pipeline_container = ui.element("div").style(
                "width: 100%; height: 100%; display: flex; flex-direction: column;"
            )
            _refs["pipeline_container"] = pipeline_container
            with pipeline_container:
                build_pipeline_builder_panel(
                    backend,
                    callbacks,
                    primary_sidebar=primary_sidebar,
                    roster_panel=roster_panel,
                    toggle_workbench=_toggle_workbench,
                    ensure_pipeline_mode=ensure_pipeline_mode,
                    toggle_journey=_show_journey,
                )

            workbench_container = ui.element("div").style(
                "width: 100%; height: 100%; display: none; flex-direction: column;"
            )
            _refs["workbench_container"] = workbench_container
            with workbench_container:
                build_species_workbench_panel(backend)

            # Journey: built lazily on first switch (see _show_journey) so the
            # heavy per-TS render doesn't tax every workspace load. Starts hidden.
            journey_container = ui.element("div").style(
                "width: 100%; height: 100%; display: none; flex-direction: column;"
            )
            _refs["journey_container"] = journey_container

    # Floating background-tasks tray at workspace scope so spun-off
    # renders/builds remain visible across dialog open/close and view
    # toggles. Survives the entire workspace session; teardown happens
    # when the page rebuilds.
    mount_background_task_tray(
        project_path_provider=lambda: str(ui_mgr.project_path) if ui_mgr.project_path else None
    )
