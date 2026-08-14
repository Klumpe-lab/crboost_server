# ui/current_project.py
"""Tab-context accessor for the active project's state.

UI code (event handlers, render paths) calls ``current_project_state()`` to get
the ProjectState of the project loaded in the current browser tab. This is the
only place allowed to resolve state from UI context; services and background
tasks must use ``get_project_state_for(path)`` / ``StateService.state_for(path)``
with an explicit path — a bare tab-context lookup in a background task silently
yields a blank throwaway state (the historical W2 ArtiaX bug class).
"""

from services.project_state import ProjectState, get_project_state_for


def current_project_state() -> ProjectState:
    """Resolve the active project's ProjectState from the current tab's UIStateManager.

    Falls back to a detached blank ProjectState when no project is loaded yet
    (landing page before create/load) or there is no client context.
    """
    try:
        from ui.ui_state import get_ui_state_manager

        ui_mgr = get_ui_state_manager()
        if ui_mgr.project_path:
            return get_project_state_for(ui_mgr.project_path)
    except RuntimeError:
        # No client connection (background task, server startup, etc.)
        pass
    return ProjectState()
