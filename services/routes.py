"""Addressable URLs for the workspace: the pure mapping between a URL and a (view, target) tuple, and the link
to a project's view. Round-trip is the contract:
``parse_route(r.project, segments_of(route_to_path(r)), query_of(...)) == r``.

The UI drives a route onto a built workspace and writes the address bar back (``ui/routing.py``); the agent API
builds the same links for what it touched.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit

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
SPECIES_TABS: tuple[str, ...] = ("overview", "templates", "picks", "analytics", "kitchen", "jobs")
# `ui/tomo_dashboard_dialog.py :: _DASHBOARD_PANEL_KEYS` (also the `data-section` values
# `_scroll_section_into_view` queries)
JOURNEY_SECTIONS: tuple[str, ...] = (
    "dataset",
    "fs_ctf",
    "tilt_filter",
    "ts_align",
    "ts_refine",
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


def workspace_path(
    project_path: Path | str, view: View = View.PIPELINE, a: str | None = None, b: str | None = None
) -> str:
    """The URL path (with its query) of a project's view."""
    path = Path(project_path)
    return route_to_path(Route(project=path.name, view=view, a=a, b=b, base=disambiguating_base(path)))
