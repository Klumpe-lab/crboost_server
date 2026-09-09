"""Addressable URLs for the workspace (roadmap 17).

The workspace is ONE page whose six views are sibling containers swapped by CSS
``display`` (``ui/workspace_page.py``). Everything needed to *navigate* it already exists
as the ``callbacks`` dict; what this module adds is the address bar:

* :class:`Route` + :func:`parse_route` / :func:`route_to_path` — the pure mapping between
  a URL and a (view, target) tuple. Round-trip is the contract:
  ``parse_route(r.project, segments_of(route_to_path(r)), query_of(...)) == r``.
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit

from nicegui import ui

logger = logging.getLogger(__name__)


class View(StrEnum):
    """The addressable views. The value is the URL segment — except ``PIPELINE``, which
    is the bare ``/p/<project>`` (it is where a workspace always starts)."""

    PIPELINE = "pipeline"
    JOB = "job"
    JOURNEY = "journey"
    TOMOGRAMS = "tomograms"
    SPECIES = "species"
    PICKS = "picks"
    PROTOCOLS = "protocols"


# Views whose URL carries targets, and how many segments they take.
_TARGETS: dict[View, int] = {View.JOB: 2, View.JOURNEY: 1, View.SPECIES: 2, View.PICKS: 2}

# Mirrors of two vocabularies that live in modules which import THIS one, so they cannot
# be imported back without a cycle. Job subsection keys are not here — those are read at
# validation time (`_job_tab_keys`), because plugins register new ones.
# `ui/species/page.py :: TABS`
SPECIES_TABS: tuple[str, ...] = ("overview", "templates", "picks", "jobs")
# `ui/tomo_dashboard_dialog.py :: _DASHBOARD_PANEL_KEYS` (also the `data-section` values
# `_scroll_section_into_view` queries)
JOURNEY_SECTIONS: tuple[str, ...] = (
    "dataset",
    "fs_ctf",
    "tilt_filter",
    "ts_align",
    "ts_ctf",
    "tilt_qc",
    "reconstruct",
    "particles",
)


@dataclass(frozen=True, slots=True)
class Route:
    """Where the user is, as the URL says it.

    ``a`` / ``b`` are the view's positional targets: job → (instance_id, tab),
    journey → (ts_name,), species → (species_id, tab), picks → (species_id, ts_name).
    ``invalid`` is never serialised — it carries a segment the URL named that this build
    does not know, so :func:`apply_route` can say so instead of silently landing
    somewhere plausible.
    """

    project: str
    view: View = View.PIPELINE
    a: str | None = None
    b: str | None = None
    base: str | None = None
    section: str | None = None
    invalid: str | None = None


def route_to_path(route: Route) -> str:
    """The canonical URL for a route. Every segment is percent-encoded — project
    directory names may hold spaces; ``instance_id``'s ``__`` is already URL-safe."""
    segs: list[str] = ["p", route.project]
    if route.view is not View.PIPELINE:
        segs.append(route.view.value)
        for part in (route.a, route.b)[: _TARGETS.get(route.view, 0)]:
            if not part:
                break
            segs.append(part)
    path = "/" + "/".join(quote(s, safe="") for s in segs)
    query = {k: v for k, v in (("base", route.base), ("s", route.section)) if v}
    return f"{path}?{urlencode(query)}" if query else path


def parse_route(project: str, segments: Sequence[str], query: Mapping[str, str]) -> Route:
    """URL → :class:`Route`. Path segments arrive already unquoted (FastAPI does it)."""
    base = (query.get("base") or "").strip() or None
    section = (query.get("s") or "").strip() or None
    raw = [s for s in segments if s]
    if not raw:
        return Route(project=project, view=View.PIPELINE, base=base, section=section)

    head, *rest = raw
    try:
        view = View(head)
    except ValueError:
        # Not a view we serve — land on the pipeline and say which segment was wrong.
        return Route(project=project, view=View.PIPELINE, base=base, section=section, invalid=head)

    n = _TARGETS.get(view, 0)
    if len(rest) > n:
        logger.info("Route /p/%s/%s: ignoring %d extra segment(s) %s", project, head, len(rest) - n, rest[n:])
    a = rest[0] if n >= 1 and len(rest) >= 1 else None
    b = rest[1] if n >= 2 and len(rest) >= 2 else None
    return Route(project=project, view=view, a=a, b=b, base=base, section=section)


def route_of_url(path: str, query: Mapping[str, str] | None = None) -> Route | None:
    """Parse a whole ``/p/<project>/…`` URL (path plus optional ``?query``) — the
    ``popstate`` path, where the browser hands back a string rather than route params.
    ``None`` when it is not a workspace URL at all."""
    parts = urlsplit(path)
    segs = [unquote(s) for s in parts.path.split("/") if s]
    if len(segs) < 2 or segs[0] != "p":
        return None
    q = dict(parse_qsl(parts.query)) if query is None else dict(query)
    return parse_route(segs[1], segs[2:], q)


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
    Back/Forward — where the workspace is already on some view. The view entry points are
    toggles (`toggle_journey` and friends switch *back* to the pipeline when their view is
    already shown), so entering one goes through `_enter`, which skips the call when the
    workspace says it is already there.

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
            await _enter(callbacks, "toggle_journey", "journey")
            section = route.section
            if section and section not in JOURNEY_SECTIONS:
                ui.notify(f"Unknown journey section '{section}'.", type="warning")
                section = None
            if route.a or section:
                # journey_show_ts is registered by build_journey_panel, which toggle_journey
                # has finished by now; it reports a tilt-series with no column itself.
                await _acall(callbacks, "journey_show_ts", route.a or "", section)

        case View.TOMOGRAMS:
            await _enter(callbacks, "toggle_gallery", "gallery")

        case View.SPECIES:
            if _mode(callbacks) != "workbench":
                _call(callbacks, "toggle_workbench")
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
            await _enter(callbacks, "toggle_protocols", "protocols")


def _mode(callbacks: dict) -> str | None:
    """Which view the workspace is showing right now (`workspace_page._switch_to`'s own
    mode string, which is "workbench" for the Species page)."""
    fn = callbacks.get("current_mode")
    return fn() if fn else None


async def _enter(callbacks: dict, key: str, mode_name: str) -> None:
    """Show a lazily built view. The callback is a toggle, so calling it when the view is
    already up would switch back to the pipeline — a real hazard on Back/Forward."""
    if _mode(callbacks) == mode_name:
        return
    await _acall(callbacks, key)


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
