"""Addressable URLs for the workspace.

The workspace is one page whose seven views are sibling containers swapped by CSS
``display`` (``ui/workspace_page.py``). Everything needed to *navigate* it already exists
as the ``callbacks`` dict; what this module adds is the address bar:

* :class:`Route` + :func:`parse_route` / :func:`route_to_path` (``services/routes.py``) — the
  pure mapping between a URL and a (view, target) tuple, shared with the agent API's links.
* :func:`apply_route` — drives one route onto a freshly built workspace, through the very
  same callbacks a click would use (never around them: the lazy views are
  ``SingleFlight``-guarded and idempotent only when entered that way).
* :class:`RouteWriter` — writes the URL back as the user clicks, via the History API.
  Never ``ui.navigate.to``: a navigate rebuilds the page and throws away the lazily built
  Journey / gallery / viewer, which is the whole cost the CSS-display design avoids.

This module deliberately imports nothing from ``ui.*`` at module level — half of ``ui``
imports it, so every such import would be a cycle. The few vocabulary lookups
(:func:`apply_route`'s tab-key validation) are function-local for that reason.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from nicegui import ui

from services.routes import JOURNEY_SECTIONS, SPECIES_TABS, Route, View, route_to_path

logger = logging.getLogger(__name__)


# ── Applying a route to a built workspace ────────────────────────────────────────


def _job_tab_keys(instance_id: str) -> tuple[str, ...]:
    """The subsection keys THIS job actually has: the three every job carries plus the
    extras its plugin registered (`tasks` on the array jobs). Read, never listed here —
    a hard-coded list would drift the moment a plugin registers a new tab."""
    from services.models_base import instance_id_to_job_type
    from ui.job_plugins import get_extra_tabs
    from ui.ui_state import MonitorTab

    keys = tuple(t.value for t in MonitorTab)
    try:
        job_type = instance_id_to_job_type(instance_id)
    except (KeyError, ValueError):
        return keys
    return keys + tuple(et.key for et in get_extra_tabs(job_type))


async def apply_route(callbacks: dict, route: Route, project_path: Path) -> None:
    """Drive a freshly built workspace onto ``route``.

    Called from the routed page handler on a fresh workspace, and again on every
    Back/Forward. Several views can be on at once; the route's view is turned on (the
    `show_*` callbacks never turn one off) and named in the address bar, and the others
    stay as they are.

    A route may name something this project does not have: a deleted species, a job that
    was never added, a tilt-series with no journey column. Every one of those lands on
    the view with a named toast — never a blank pane, never a silent fall back to "the
    first one" (CLAUDE.md, *Surfacing uncertainty*).
    """
    from services.project_state import get_project_state_for

    if route.invalid:
        ui.notify(f"Unknown view '{route.invalid}' in the URL — showing the pipeline.", type="warning")

    state = get_project_state_for(project_path)

    match route.view:
        case View.PIPELINE:
            _call(callbacks, "ensure_pipeline_mode")

        case View.JOB:
            if not route.a:
                _call(callbacks, "ensure_pipeline_mode")
            elif route.a not in state.jobs:
                _call(callbacks, "ensure_pipeline_mode")
                ui.notify(f"This project has no job '{route.a}'.", type="warning")
            elif route.b:
                valid = _job_tab_keys(route.a)
                if route.b in valid:
                    _call(callbacks, "open_job_subsection", route.a, route.b)
                else:
                    _call(callbacks, "open_job", route.a)
                    ui.notify(f"'{route.a}' has no '{route.b}' tab ({', '.join(valid)}).", type="warning")
            else:
                _call(callbacks, "open_job", route.a)

        case View.JOURNEY:
            await _acall(callbacks, "show_journey")
            section = route.section
            if section and section not in JOURNEY_SECTIONS:
                ui.notify(f"Unknown journey section '{section}'.", type="warning")
                section = None
            if route.a or section:
                # journey_show_ts is registered by build_journey_panel, which show_journey
                # has finished by now; it reports a tilt-series with no column itself.
                await _acall(callbacks, "journey_show_ts", route.a or "", section)

        case View.TOMOGRAMS:
            await _acall(callbacks, "show_gallery")

        case View.SPECIES:
            _call(callbacks, "show_workbench")
            if route.a:
                if state.get_species(route.a) is None:
                    ui.notify(f"No species '{route.a}' in this project's registry.", type="warning")
                else:
                    _call(callbacks, "open_species", route.a)
                    if route.b:
                        if route.b in SPECIES_TABS:
                            _call(callbacks, "species_select_tab", route.b)
                        else:
                            ui.notify(f"Unknown species tab '{route.b}' ({', '.join(SPECIES_TABS)}).", type="warning")

        case View.PICKS:
            species = route.a
            if species and state.get_species(species) is None:
                ui.notify(f"No species '{species}' in this project's registry.", type="warning")
                # Drop it rather than hand the viewer an id it will silently swap for its
                # first species — the URL then re-writes itself to whatever it did show.
                species = None
            await _acall(callbacks, "open_pick_viewer", species, route.b)

        case View.PROTOCOLS:
            await _acall(callbacks, "show_protocols")

        case View.TABLE:
            await _acall(callbacks, "show_table")


def _call(callbacks: dict, key: str, *args) -> None:
    fn = callbacks.get(key)
    if fn is None:
        logger.info("apply_route: no '%s' callback registered", key)
        return
    fn(*args)


async def _acall(callbacks: dict, key: str, *args) -> None:
    fn = callbacks.get(key)
    if fn is None:
        logger.info("apply_route: no '%s' callback registered", key)
        return
    await fn(*args)


# ── Writing the URL back as the user clicks ──────────────────────────────────────


class RouteWriter:
    """Keeps the address bar in step with the view on screen.

    One per workspace build, handed to the views as ``callbacks["set_url"]`` — each view
    knows its own target, so nothing has to reach across views. History API only:
    ``pushState`` when the *view* changes (so Back walks views), ``replaceState`` when a
    view refines its own target (so Back does not walk every tilt-series click) and for
    the route the page was opened with.

    ``suppressed`` is raised while :func:`apply_route` runs, so applying a popped history
    entry does not push it back on.
    """

    def __init__(self, project: str, base: str | None = None) -> None:
        self.project = project
        self.base = base
        self.suppressed = False
        self._last: Route | None = None

    def route(self, view: View, a: str | None = None, b: str | None = None, section: str | None = None) -> Route:
        return Route(project=self.project, view=view, a=a, b=b, base=self.base, section=section)

    @property
    def current(self) -> Route:
        return self._last or self.route(View.PIPELINE)

    def adopt(self, route: Route) -> None:
        """Record a route as current WITHOUT touching history — used when the browser
        already moved (a popstate) so the next write diffs against the right entry."""
        self._last = Route(
            project=self.project, view=route.view, a=route.a, b=route.b, base=self.base, section=route.section
        )

    def write(self, view: View, a: str | None = None, b: str | None = None, section: str | None = None) -> None:
        if self.suppressed:
            return
        route = self.route(view, a, b, section)
        if route == self._last:
            return
        push = self._last is not None and route.view is not self._last.view
        self._last = route
        path = route_to_path(route)
        try:
            ui.run_javascript(f"history.{'pushState' if push else 'replaceState'}(null,'',{json.dumps(path)})")
        except RuntimeError as e:
            # No client context (a background completion nudging a view) — the address
            # bar simply doesn't move; nothing downstream depends on it.
            logger.info("set_url skipped for %s: %s", path, e)
