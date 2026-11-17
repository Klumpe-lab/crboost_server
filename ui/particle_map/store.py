"""The particle map's page state and what it asks the shared stores for.

Per browser tab: the cards shown large, where dragged cards were left, the view the browser last reported and the
picked node (the card made large last, which the address bar names). What is worth keeping across reloads is
written to the user's storage, per project, beside the Jobs map's own. The data come from two stores shared by
every tab: `LineageStore` (the sets, the class rows, the counts) and the Jobs map's `FactsStore` (what a large
card shows of a job's results), so both maps read a job's results once.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from nicegui import app

from services.tilt_series import get_registry_for
from ui.branch_map.facts import FactsStore, kick
from ui.branch_map.model import Project

logger = logging.getLogger(__name__)

_PREFS_KEY = "particle_map"


@dataclass
class PageState:
    all_large: bool = False
    large: set[str] = field(default_factory=set)
    moved: dict[str, tuple[float, float]] = field(default_factory=dict)
    zoom: float | None = None
    tx: float = 0.0
    ty: float = 0.0
    picked: str | None = None

    def signature(self) -> tuple:
        """What the render reads; not the view and the dragged positions, which the browser keeps."""
        return (self.all_large, tuple(sorted(self.large)), self.picked)


def _prefs(project_path: Path) -> dict:
    try:
        store = app.storage.user.setdefault(_PREFS_KEY, {})
    except RuntimeError:  # no request context (a background completion): nothing to keep
        return {}
    return store.setdefault(str(project_path), {})


def restore(project_path: Path) -> PageState:
    p = _prefs(project_path)
    st = PageState()
    st.moved = {k: (float(v[0]), float(v[1])) for k, v in (p.get("moved") or {}).items()}
    st.large = set(p.get("large") or [])
    st.all_large = bool(p.get("all_large", False))
    st.picked = p.get("picked") or None
    view = p.get("view")
    if view:
        st.zoom, st.tx, st.ty = float(view[0]), float(view[1]), float(view[2])
    return st


def persist(project_path: Path, st: PageState) -> None:
    p = _prefs(project_path)
    p["moved"] = {k: [v[0], v[1]] for k, v in st.moved.items()}
    p["large"] = sorted(st.large)
    p["all_large"] = st.all_large
    p["picked"] = st.picked
    p["view"] = [st.zoom, st.tx, st.ty] if st.zoom is not None else None


def request_facts(project_path: Path, facts: FactsStore, pr: Project, state, large: list[str], after) -> None:
    """Read the results of the large job cards off the loop, once per job status, through the Jobs map's store and
    with the registry's series and stamp, as the Jobs map asks for them: one cache for both maps."""
    if not large:
        return
    try:
        registry = get_registry_for(project_path)
        series = list(registry.all_tilt_series())
        stamp = (registry.loaded_stamp, len(series))
    except Exception:  # no registry yet
        logger.exception("particle map: the registry of %s", project_path)
        return
    for nid in large:
        j = pr.by_id.get(nid)
        jm = state.jobs.get(nid)
        if j is None or jm is None or j.status not in ("done", "failed", "running"):
            continue
        if facts.job_stale(j) and not facts.loading(("job", j.id)):
            kick(facts.load_job(j, jm, series, stamp, pr.ts_ids), after)
