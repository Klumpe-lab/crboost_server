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

    _SB_MUTE = "#94a3b8"
    _SB_ACT = "#3b82f6"
    _SB_ABG = "#eff6ff"

    def _set_wb_btn_style(active: bool):
        wb_btn = callbacks.get("wb_btn")
        if wb_btn:
            wb_btn.style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {_SB_ABG if active else 'transparent'}; "
                f"color: {(_SB_ACT if active else _SB_MUTE)}; min-width: 0;"
            )

    def _toggle_workbench():
        pipeline_c = _refs.get("pipeline_container")
        workbench_c = _refs.get("workbench_container")
        if pipeline_c is None or workbench_c is None:
            return
        if _mode["current"] == "pipeline":
            _mode["current"] = "workbench"
            pipeline_c.style("display: none;")
            workbench_c.style("display: flex; flex-direction: column;")
            _set_wb_btn_style(True)
        else:
            _mode["current"] = "pipeline"
            pipeline_c.style("display: flex; flex-direction: column;")
            workbench_c.style("display: none;")
            _set_wb_btn_style(False)
            # Invalidate cached TM tab renders so they pick up new species
            invalidate = callbacks.get("invalidate_tm_tabs")
            if invalidate:
                invalidate()

    def ensure_pipeline_mode():
        """Switch to pipeline view if currently in workbench. No-op otherwise."""
        if _mode["current"] == "workbench":
            _toggle_workbench()

    callbacks["toggle_workbench"] = _toggle_workbench
    callbacks["ensure_pipeline_mode"] = ensure_pipeline_mode

    with ui.element("div").style(
        "position: fixed; inset: 0; display: flex; flex-direction: row; "
        "overflow: hidden; gap: 0; margin: 0; padding: 0;"
    ):
        primary_sidebar = ui.element("div").style(
            "width: 42px; min-width: 42px; height: 100%; flex-shrink: 0; "
            "background: #f8fafc; display: flex; flex-direction: column; "
            "align-items: center; gap: 0; overflow: visible; z-index: 20; "
            "border-right: 1px solid #e2e8f0;"
        )

        roster_panel = ui.element("div").style(
            "width: 224px; min-width: 224px; height: 100%; flex-shrink: 0; "
            "background: #ffffff; border-right: 1px solid #e5e7eb; "
            "overflow-y: auto; overflow-x: hidden; "
            "flex-direction: column; gap: 0; display: none;"
        )

        main_area = ui.element("div").style(
            "flex: 1; min-width: 0; height: 100%; overflow: hidden; "
            "display: flex; flex-direction: row; gap: 0;"
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

        _refs["wb_btn"] = callbacks.get("wb_btn")
