"""Open another project in this tab's workspace (roadmap 16 S4 — hoisted from the project hub's
`_switch_project`). The workspace is built per project, so this always ends in a navigation;
callers that own a dialog close it first."""

from __future__ import annotations

from pathlib import Path

from nicegui import ui

from services.project_state import get_project_state_for


async def open_project_in_workspace(backend, ui_mgr, target: Path | str) -> bool:
    """Load `target` through the facade, point the tab's UI state at it and navigate to
    /workspace. False (with a toast) when it is not a project or fails to load."""
    target = Path(target).expanduser()
    if not (target / "project_params.json").exists():
        ui.notify(f"No project_params.json in {target}", type="warning")
        return False
    res = await backend.load_existing_project(str(target))
    if not res.get("success"):
        ui.notify(f"Failed to load {target.name}: {res.get('error')}", type="negative")
        return False
    state = get_project_state_for(target)
    ui_mgr.load_from_project(
        project_path=state.project_path or target, scheme_name="loaded", jobs=list(state.jobs.keys())
    )
    if state.pipeline_active:
        ui_mgr.set_pipeline_running(True)
    ui.navigate.to("/workspace")
    return True
