"""Open another project in this tab's workspace (roadmap 16 S4 — hoisted from the project hub's
`_switch_project`). The workspace is built per project, so this always ends in a navigation;
callers that own a dialog close it first."""

from __future__ import annotations

from pathlib import Path

from nicegui import app, ui

from services.configs.user_prefs_service import get_prefs_service
from services.project_state import get_project_state_for
from ui.routing import Route, route_to_path


def remember_project_opened(target: Path | str, label: str | None = None) -> None:
    """Push a project onto the per-user "recently viewed" MRU (prefs.recent_projects),
    which is what the projects roster's default sort orders by. Every path that lands a
    user in a workspace calls this — the landing row, the project hub's switcher, project
    creation and `open_project_in_workspace`."""
    prefs_service = get_prefs_service()
    prefs_service.prefs.add_recent_project(str(target), label=label)
    prefs_service.save_to_app_storage(app.storage.user)


def disambiguating_base(project_path: Path | str) -> str | None:
    """The `?base=` a link to this project needs, or None when it needs none.

    `resolve_project` searches the user's bases by directory name; when that already
    lands on exactly this path the URL stays clean. When it lands elsewhere, finds
    nothing, or finds the name in two places, the link must carry the base or it would
    not resolve on the recipient's side."""
    from services.project_resolve import resolve_project

    path = Path(project_path)
    found = resolve_project(path.name)
    if found.get("success") and str(found["path"]) == str(path):
        return None
    return str(path.parent)


def workspace_url(project_path: Path | str) -> str:
    """The addressable URL for a project's workspace (roadmap 17)."""
    path = Path(project_path)
    return route_to_path(Route(project=path.name, base=disambiguating_base(path)))


async def load_project_into_tab(backend, ui_mgr, target: Path | str) -> bool:
    """Load `target` through the facade and point this tab's UI state at it. False (with
    a toast) when it is not a project or fails to load. No navigation — the routed page
    handlers (roadmap 17) are already on the right URL, and `open_project_in_workspace`
    adds the navigate for the callers that still travel."""
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
    remember_project_opened(state.project_path or target, label=state.project_name)
    return True


async def open_project_in_workspace(backend, ui_mgr, target: Path | str) -> bool:
    """`load_project_into_tab` plus the travel to that project's workspace URL."""
    if not await load_project_into_tab(backend, ui_mgr, target):
        return False
    state = get_project_state_for(Path(target).expanduser())
    ui.navigate.to(workspace_url(state.project_path or Path(target).expanduser()))
    return True
