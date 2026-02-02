# ui/landing_page.py
from nicegui import ui
from backend import CryoBoostBackend
from ui.data_import_panel import build_data_import_panel
from ui.ui_state import get_ui_state_manager


def build_landing_page(backend: CryoBoostBackend):
    def on_project_ready():
        ui.navigate.to("/workspace")

    # Full-height layout
    with ui.column().classes("w-full min-h-screen bg-gray-50"):
        # Compact header
        with ui.row().classes("w-full bg-slate-800 px-6 py-3 items-center justify-between"):
            with ui.row().classes("items-center gap-3"):
                with ui.element("div").classes("bg-blue-500 p-1.5 rounded"):
                    ui.icon("science", size="24px").classes("text-white")
                with ui.column().classes("gap-0"):
                    ui.label("CryoBoost Orchestrator").classes("text-base font-bold text-white")
                    ui.label("Project Setup").classes("text-xs text-slate-400")

        # Main content - centered but wider
        with ui.row().classes("w-full flex-1 justify-center px-4 py-4"):
            with ui.card().classes("w-full max-w-4xl p-0 shadow-lg border border-gray-200 rounded-lg overflow-hidden"):
                callbacks = {"rebuild_pipeline_ui": on_project_ready, "check_and_update_statuses": lambda: None}
                build_data_import_panel(backend, callbacks)


def create_landing_page_route(backend: CryoBoostBackend):
    """
    Creates the landing page route with proper state handling.

    CRITICAL: We must NOT blindly reset state when navigating to /.
    A stale tab refreshing could nuke a running pipeline.
    """

    @ui.page("/")
    async def landing_page(client):
        from services.project_state import get_project_state, reset_project_state

        ui_mgr = get_ui_state_manager()
        current_state = get_project_state()

        # GUARD: If pipeline is running, redirect to workspace
        if current_state.pipeline_active:
            ui.notify("A pipeline is currently running. Redirecting to workspace.", type="warning", position="top")
            ui.navigate.to("/workspace")
            return

        # Reset UI state (this is always safe - it's per-client)
        ui_mgr.reset()

        # Only reset project state if:
        # 1. No project is currently loaded, OR
        # 2. User explicitly wants a fresh start (no pipeline running, check above passed)
        #
        # The key insight: if someone has a project loaded but navigates to /,
        # they probably want to either:
        # a) Start fresh (fine to reset if no pipeline running)
        # b) Load a different project (reset will happen naturally)
        #
        # The dangerous case (pipeline running) is already guarded above.

        if current_state.project_path is None:
            # No project loaded, safe to ensure clean state
            reset_project_state()
        else:
            # Project exists but user navigated to landing page
            # Could be intentional "start over" or accidental
            # Since pipeline_active=False (checked above), it's safe to reset
            print(f"[LANDING] User navigated away from project: {current_state.project_name}")
            reset_project_state()

        # Build the page
        build_landing_page(backend)

    return landing_page
