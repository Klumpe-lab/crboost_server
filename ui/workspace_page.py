from nicegui import ui
from backend import CryoBoostBackend
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
        """Switch between pipeline / workbench views."""
        containers = {"pipeline": _refs.get("pipeline_container"), "workbench": _refs.get("workbench_container")}
        # If already on this mode, toggle back to pipeline
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

        set_wb = callbacks.get("set_wb_active")
        if set_wb:
            set_wb(_mode["current"] == "workbench")

    def _toggle_workbench():
        _switch_to("workbench")

    def ensure_pipeline_mode():
        if _mode["current"] == "workbench":
            _toggle_workbench()

    callbacks["toggle_workbench"] = _toggle_workbench
    callbacks["ensure_pipeline_mode"] = ensure_pipeline_mode

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
                )

            workbench_container = ui.element("div").style(
                "width: 100%; height: 100%; display: none; flex-direction: column;"
            )
            _refs["workbench_container"] = workbench_container
            with workbench_container:
                build_species_workbench_panel(backend)
