from nicegui import ui
from backend import CryoBoostBackend
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
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
            "width: 182px; min-width: 182px; height: 100%; flex-shrink: 0; "
            "background: #ffffff; border-right: 1px solid #e5e7eb; "
            "overflow-y: auto; overflow-x: hidden; "
            "flex-direction: column; gap: 0; display: none;"
        )

        main_area = ui.element("div").style(
            "flex: 1; min-width: 0; height: 100%; overflow: hidden; "
            "display: flex; flex-direction: column; gap: 0;"
        )
        with main_area:
            build_pipeline_builder_panel(
                backend, callbacks,
                primary_sidebar=primary_sidebar,
                roster_panel=roster_panel,
            )
