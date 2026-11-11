"""What a project snapshot captures: one scene per view and target, each the workspace URL that shows it,
and how the frozen pages link to each other.

A scene is one view on its own (`solo`: the panes kept; the others are dropped from the frozen page):
the Jobs view per job, the jobs table, the Journey per tilt series, the tomogram wall and its Tilts tab,
and per species its Overview, Picks, Analytics and Jobs tabs. What is left out reads files no snapshot
carries or only acts: the Protocols view, the pick viewer, a species' templates (a 3-D viewer that loads
volumes) and its STA kitchen.

`nav` is how the frozen pages move between scenes (`frozen.js`): the rail's icons, the job roster's
rows, the Journey's tilt series, the wall's tiles, the segmented tabs.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

from services.dashboard_data import collect_dashboard_journey
from services.jobs.spec import instance_display_name
from services.routes import Route, View, route_to_path
from ui.dashboard.strip import _ordered, _ts_title

# The species page's tabs a snapshot carries (`ui/species/page.py` TABS, less templates and kitchen).
SPECIES_TABS = ("overview", "picks", "analytics", "jobs")
# The workspace panes each view shows (`ui/workspace_page.py`: `cb-roster-panel`, `cb-view-<name>`).
_JOBS_PANES = ["roster", "pipeline"]


@dataclass
class Scene:
    key: str
    title: str
    path: str
    solo: list[str]
    # A segmented tab to switch to once the view shows (`data-cb-seg`, inside the first solo pane).
    seg: str | None = None
    # The groups the frozen pages' navigation names it by.
    group: str = ""
    target: str = ""


@dataclass
class Plan:
    project: str
    scenes: list[Scene] = field(default_factory=list)

    def file_of(self, key: str) -> str:
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", key).strip("_")[:60]
        return f"{slug}-{hashlib.sha1(key.encode()).hexdigest()[:6]}.html"

    def nav(self) -> dict:
        """What every frozen page's navigation is wired from: the file of each scene, the page
        `index.html` copies, the rail's icons, the jobs and tilt series by name, and the tabs of
        each segmented control by the scene they leave from."""
        files = {s.key: self.file_of(s.key) for s in self.scenes}

        def first(group: str) -> str | None:
            return next((files[s.key] for s in self.scenes if s.group == group), None)

        seg: dict[str, dict[str, str]] = {}
        for s in self.scenes:
            if s.group == "gallery":
                seg[s.key] = {o.seg or "tomograms": files[o.key] for o in self.scenes if o.group == "gallery"}
            elif s.group == "species":
                sid = s.target.split("/")[0]
                seg[s.key] = {
                    o.target.split("/")[1]: files[o.key]
                    for o in self.scenes
                    if o.group == "species" and o.target.split("/")[0] == sid
                }
        index = next((s.key for s in self.scenes if s.group == "journey"), self.scenes[0].key if self.scenes else "")
        return {
            "project": self.project,
            "files": files,
            "index": index,
            "rail": {
                "pipeline_btn": first("job"),
                "table_btn": first("table"),
                "wb_btn": first("species"),
                "gallery_btn": first("gallery"),
                "dashboard_btn": first("journey"),
            },
            "jobs": {s.target: files[s.key] for s in self.scenes if s.group == "job"},
            "journey": [
                {"ts": s.target, "title": s.title, "file": files[s.key]} for s in self.scenes if s.group == "journey"
            ],
            "seg": seg,
        }

    def to_dict(self) -> dict:
        return {"project": self.project, "scenes": [asdict(s) for s in self.scenes]}


def plan_for(state, project_path: Path, route: Route) -> Plan:
    """The scenes of the project `route` names (its project and base are kept in every scene's URL)."""
    project_path = Path(project_path)

    def url(view: View, a: str | None = None, b: str | None = None) -> str:
        return route_to_path(Route(project=route.project, view=view, a=a, b=b, base=route.base))

    plan = Plan(project=state.project_name or project_path.name)
    add = plan.scenes.append
    for iid in state.pipeline_order:
        jm = state.jobs.get(iid)
        if jm is not None:
            name = instance_display_name(iid, jm)
            add(Scene(f"job/{iid}", f"Jobs · {name}", url(View.JOB, iid), _JOBS_PANES, group="job", target=iid))
    if state.pipeline_order:
        add(Scene("table", "Jobs table", url(View.TABLE), ["table"], group="table"))

    _journey, ts_names = collect_dashboard_journey(state, project_path)
    for ts in _ordered(list(ts_names)):
        add(Scene(f"journey/{ts}", _ts_title(ts), url(View.JOURNEY, ts), ["journey"], group="journey", target=ts))

    if ts_names:
        add(Scene("tomograms", "Tomograms", url(View.TOMOGRAMS), ["gallery"], seg="tomograms", group="gallery"))
        add(Scene("tilts", "Tilts", url(View.TOMOGRAMS), ["gallery"], seg="tilts", group="gallery"))

    for sp in state.species_registry:
        for tab in SPECIES_TABS:
            add(
                Scene(
                    f"species/{sp.id}/{tab}",
                    f"{sp.name} · {tab}",
                    url(View.SPECIES, sp.id, tab),
                    ["workbench"],
                    group="species",
                    target=f"{sp.id}/{tab}",
                )
            )
    return plan
