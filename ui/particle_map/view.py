"""The particle map: a view of its own beside the Jobs view, opened from the roster's PARTICLES header. The project's
particle jobs and its particle sets as cards on the Jobs map's paper, in columns by how far down the chains they
sit; a Class 3D's classes as rows with their own ports, a set made of a class hanging off that row. Drag, pan and
zoom as on the Jobs map (its script, under the map name `particles`). A click on a job card's header opens the job's
page, which closes back onto this view; the size toggle (a set's header) makes a card large, and the card made large
last is the picked node the address bar names (`/p/<project>/particles/<node>`, a set as `set:<id>`).

`ParticleMapView` is a `FingerprintedView` whose signature reads memory only: the particle jobs' statuses and
inputs, the lineage and facts stores' versions, the page state. Its 3 s tick redraws when they moved; its 15 s poll
has the lineage store stat the stars (in a thread) and read what changed (in workers). Both run only while the view
shows.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.particles.lineage import SET_NODE_PREFIX, is_particle_job, job_refs, lineage_store_for
from services.project_state import get_project_state_for
from services.routes import View
from ui.branch_map.facts import facts_store_for, kick
from ui.branch_map.geometry import GUTTER, PAD, de_overlap, out_y_of, path_d, port_y, tidy
from ui.branch_map.model import project_from_state
from ui.branch_map.state import MapState
from ui.branch_map.style import AMBER, BLUE, CABLE, HAIR, PAPER, PAPER_DOT, SOFT, label, text
from ui.branch_map.view import install_pan_zoom, zoom_controls
from ui.components.buttons import house_button
from ui.components.reactive import FingerprintedView
from ui.particle_map import cards
from ui.particle_map.model import ParticleMap, particle_map
from ui.particle_map.store import PageState, persist, request_facts, restore

logger = logging.getLogger(__name__)

MAP = "particles"
_TICK_S = 3.0
_POLL_S = 15.0


class ParticleMapView(FingerprintedView):
    """The particle map over one project, in one browser tab."""

    MAP = MAP

    def __init__(self, container, project_path: Path, callbacks: dict[str, Callable]) -> None:
        super().__init__(container)
        self.project_path = Path(project_path)
        self.callbacks = callbacks
        self.st: PageState = restore(self.project_path)
        self.lineage = lineage_store_for(self.project_path)
        self.facts = facts_store_for(self.project_path)
        self.active = False
        self._pm: ParticleMap | None = None
        self._ids: frozenset | None = None

    # ── the signature: everything the render reads, from memory ──
    def signature(self) -> Any:
        state = get_project_state_for(self.project_path)
        per_job = tuple(
            (
                iid,
                jm.execution_status,
                jm.relion_job_number,
                tuple(sorted((jm.source_overrides or {}).items())),
                tuple(sorted((k, str(v)) for k, v in (jm.paths or {}).items() if k.startswith("input"))),
            )
            for iid, jm in sorted(state.jobs.items())
            if jm.job_type is not None and is_particle_job(jm.job_type)
        )
        return (
            tuple(state.pipeline_order),
            per_job,
            self.lineage.version,
            self.facts.version,
            tuple(sorted(self.facts.set_parents)),
            self.st.signature(),
        )

    # ── the workspace's hooks ──
    def set_active(self, on: bool) -> None:
        self.active = on
        if on:
            self.poll()
            self.refresh()

    def tick(self) -> None:
        if self.active:
            self.refresh()

    def poll(self) -> None:
        if not self.active:
            return
        state = get_project_state_for(self.project_path)
        kick(self.lineage.poll(job_refs(state)), self.refresh)

    async def show(self, node: str | None) -> None:
        """Shown by a route or a click: read the lineage first, so a node the URL names can be found, then pick
        it (a node the project does not have is named in a toast)."""
        state = get_project_state_for(self.project_path)
        await self.lineage.poll(job_refs(state))
        if node:
            known = node in state.jobs or (
                node.startswith(SET_NODE_PREFIX) and self.lineage.set_by_id(node[len(SET_NODE_PREFIX) :])
            )
            if known:
                self.pick(node, write_url=False)
            else:
                ui.notify(f"This project has no particle job or set '{node}'.", type="warning")
        self.refresh()

    def target(self) -> str | None:
        """The node the address bar names: the picked one, while the map still has it."""
        p = self.st.picked
        if p is None or self._pm is None:
            return p
        return p if p in self._pm.nodes else None

    # ── the page state ──
    def redraw(self) -> None:
        self.mark_dirty()
        self.refresh()

    def relayout(self) -> None:
        self.st.zoom = None
        self.redraw()

    def pick(self, nid: str, *, write_url: bool = True) -> None:
        """Make `nid` large and the picked node."""
        self.st.picked = nid
        if not self.st.all_large:
            self.st.large.add(nid)
        persist(self.project_path, self.st)
        if write_url:
            set_url = self.callbacks.get("set_url")
            if set_url is not None:
                set_url(View.PARTICLES, nid)
        self.redraw()

    def toggle_size(self, nid: str) -> None:
        st = self.st
        if st.all_large:
            st.all_large = False
            st.large = set(self._pm.nodes if self._pm else ()) - {nid}
            if st.picked == nid:
                st.picked = None
        elif nid in st.large:
            st.large.discard(nid)
            if st.picked == nid:
                st.picked = None
        else:
            self.pick(nid)
            return
        persist(self.project_path, st)
        set_url = self.callbacks.get("set_url")
        if set_url is not None:
            set_url(View.PARTICLES, st.picked)
        self.redraw()

    # ── the browser's events ──
    def ours(self, e) -> bool:
        return e.args.get("map", "jobs") == self.MAP

    def on_move(self, e) -> None:
        if not self.ours(e):
            return
        nid = e.args.get("nid")
        if nid:
            self.st.moved[nid] = (float(e.args["x"]), float(e.args["y"]))
            persist(self.project_path, self.st)

    def on_view(self, e) -> None:
        if not self.ours(e):
            return
        self.st.zoom, self.st.tx, self.st.ty = float(e.args["zoom"]), float(e.args["tx"]), float(e.args["ty"])
        persist(self.project_path, self.st)

    # ── render ──
    def render(self) -> None:
        state = get_project_state_for(self.project_path)
        pr = project_from_state(state, self.project_path, set_parents=self.facts.set_parents)
        pm = self._pm = particle_map(pr, state, self.lineage)
        ids = frozenset(pm.nodes)
        if ids != self._ids:
            # A node came or went (the sets read, a job added): the browser fits the map again.
            self.st.zoom = None
            self._ids = ids
        with (
            ui.element("div")
            .classes("cb-pm")
            .style("display: flex; flex-direction: column; flex: 1 1 0; min-height: 0; min-width: 0;")
        ):
            if not pm.nodes:
                _render_empty(self.lineage.sets is None)
                return
            _render_map(self, pm, state)


# ── the empty project ────────────────────────────────────────────────────────


def _render_empty(reading: bool) -> None:
    with ui.element("div").style(
        "flex: 1 1 0; min-height: 0; display: flex; align-items: center; justify-content: center; "
        f"background: {PAPER}; background-image: radial-gradient({PAPER_DOT} 1px, transparent 1.2px); "
        "background-size: 16px 16px;"
    ):
        with ui.column().style("gap: 6px; max-width: 300px;"):
            if reading:
                text("Reading the particle sets…", 10, SOFT)
            else:
                ui.html('<div class="cb-bm-prompt"><span class="p">~&gt;</span> no particles yet</div>', sanitize=False)
                text(
                    "Particle jobs and particle sets appear here once the project has them: add a job from the "
                    "roster's PARTICLES rows, or make a set in the STA kitchen.",
                    10,
                    SOFT,
                )


# ── the map ──────────────────────────────────────────────────────────────────


def _cable(a: str, ax: float, ay: float, b: str, bx: float, by: float, pos: dict, *, color: str, width: float) -> str:
    """A cable from card `a`'s port to card `b`'s input, carrying both ends as offsets from the cards' corners so
    the browser can redraw it while a card is dragged."""
    (x0, y0), (x1, y1) = pos[a], pos[b]
    d = path_d(x0 + ax, y0 + ay, x1 + bx, y1 + by)
    return (
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" '
        f'data-a="{a}" data-ax="{ax}" data-ay="{ay}" data-b="{b}" data-bx="{bx}" data-by="{by}"/>'
    )


def _render_map(view: ParticleMapView, pm: ParticleMap, state) -> None:
    st = view.st
    nodes = pm.nodes
    is_large = {nid: st.all_large or nid in st.large for nid in nodes}
    drawn = {nid: cards.measure(pm, n, view.facts, large=is_large[nid]) for nid, n in nodes.items()}
    request_facts(
        view.project_path,
        view.facts,
        pm.pr,
        state,
        [nid for nid in nodes if is_large[nid] and nodes[nid].job is not None],
        view.refresh,
    )

    def sources(nid: str) -> list[str]:
        return [s for s in nodes[nid].sources() if s in nodes and s != nid]

    def order_key(nid: str) -> tuple:
        n = nodes[nid]
        if n.set is not None:
            return (1, n.set.id)
        return (0, pm.pr.stage_index(n.job.kind), n.job.number, nid)

    def kids_of(nid: str) -> list[str]:
        return sorted((k for k, n in nodes.items() if n.main_source == nid and k != nid), key=order_key)

    depth: dict[str, int] = {}

    def depth_of(nid: str) -> int:
        if nid not in depth:
            depth[nid] = 0  # guards a cycle in the recorded inputs
            depth[nid] = max((depth_of(s) + 1 for s in sources(nid)), default=0)
        return depth[nid]

    roots = sorted((nid for nid, n in nodes.items() if n.main_source not in nodes), key=order_key)
    outer = {nid: d.box.h for nid, d in drawn.items()}
    top = tidy(roots, kids_of, outer.__getitem__)
    for nid in nodes:
        top.setdefault(nid, 0.0)
    col = {nid: depth_of(nid) for nid in top}
    ncols = max(col.values()) + 1
    colw = [max((drawn[n].box.w for n in top if col[n] == c), default=cards.CARD_W) for c in range(ncols)]
    colx = [PAD + sum(colw[:c]) + c * GUTTER for c in range(ncols)]
    de_overlap(top, col, outer, ncols)
    shift = -min(top.values()) + PAD
    pos = {nid: (colx[col[nid]], top[nid] + shift) for nid in top}
    for nid, (mx, my) in st.moved.items():
        if nid in pos:
            pos[nid] = (mx, my)
    width = max(pos[n][0] + drawn[n].box.w for n in pos) + PAD
    height = max(pos[n][1] + drawn[n].box.h for n in pos) + PAD

    # ── cables: from a class row's port, else from the footer's ──
    picked = st.picked if st.picked in nodes else None
    svg = []
    for nid, n in nodes.items():
        for i, r in enumerate(n.rows):
            if r.source is None or r.source not in nodes:
                continue
            src = drawn[r.source]
            ys = [cards.class_port_y(src, c) for c in r.classes] if r.classes else [out_y_of(src.box.h)]
            on = picked is not None and picked in (nid, r.source)
            for y0 in ys:
                if y0 is None:
                    continue  # a class the Class 3D's last iteration does not have: the row names it
                svg.append(
                    _cable(
                        r.source,
                        src.box.w,
                        y0,
                        nid,
                        0,
                        port_y(i),
                        pos,
                        color=BLUE if on else CABLE,
                        width=2 if on else 1.5,
                    )
                )

    # ── the controls ──
    with (
        ui.element("div")
        .classes("cb-bm-controls")
        .style(
            "display: flex; align-items: center; gap: 6px; flex-wrap: wrap; padding: 5px 12px; "
            f"box-shadow: 0 1px 0 {HAIR}; flex-shrink: 0; position: relative; z-index: 1;"
        )
    ):
        label("Cards", 9, SOFT, case="none", track="0")
        house_button(
            "all small",
            lambda _e: (
                setattr(st, "all_large", False),
                st.large.clear(),
                persist(view.project_path, st),
                view.relayout(),
            ),
            kind="default" if st.all_large or st.large else "accent",
        )
        house_button(
            "all large",
            lambda _e: (setattr(st, "all_large", True), persist(view.project_path, st), view.relayout()),
            kind="accent" if st.all_large else "default",
        )
        if st.moved:
            house_button(
                "reset layout",
                lambda _e: (st.moved.clear(), persist(view.project_path, st), view.relayout()),
                tooltip="Put dragged cards back",
            )
        n_jobs = sum(1 for n in nodes.values() if n.job is not None)
        n_sets = len(nodes) - n_jobs
        text(f"{n_jobs} particle jobs · {n_sets} sets", 9, SOFT, extra="margin-left: 8px;")
        for p in pm.problems:
            text(p, 9, AMBER, extra="margin-left: 8px;")
        ui.element("div").style("flex: 1 1 0;")
        label("Zoom", 9, SOFT, case="none", track="0", extra="margin-left: 8px;")
        zoom_controls(MAP)

    # ── the canvas ──
    canvas = (
        ui.element("div")
        .classes("cb-bm-canvas")
        .props(f'data-map="{MAP}"')
        .style(
            "position: relative; width: 100%; flex: 1 1 0; min-height: 0; overflow: hidden; "
            f"background: {PAPER}; user-select: none; touch-action: none; cursor: grab; "
            f"background-image: radial-gradient({PAPER_DOT} 1px, transparent 1.2px); background-size: 16px 16px;"
        )
    )
    if st.zoom is not None:
        canvas.props(f'data-zoom="{st.zoom}" data-tx="{st.tx}" data-ty="{st.ty}"')
    map_state = MapState(strips=False)
    open_from = view.callbacks.get("open_job_from")
    with canvas:
        with (
            ui.element("div")
            .classes("cb-bm-world")
            .style(f"position: absolute; left: 0; top: 0; width: {width}px; height: {height}px; transform-origin: 0 0;")
        ):
            ui.html(
                f'<svg width="{width}" height="{height}" style="position:absolute;left:0;top:0;overflow:visible">'
                f"{''.join(svg)}</svg>",
                sanitize=False,
            )
            for nid, (x, y) in pos.items():
                n = nodes[nid]
                on_open = None
                if n.job is not None and open_from is not None:
                    on_open = lambda _e, iid=nid: open_from(View.PARTICLES.value, iid)  # noqa: E731
                cards.render(
                    pm,
                    n,
                    drawn[nid],
                    map_state,
                    x=x,
                    y=y,
                    on_open=on_open,
                    on_size=lambda _e, k=nid: view.toggle_size(k),
                    picked=nid == picked,
                ).props(f'data-nid="{nid}"')


# ── mounting ─────────────────────────────────────────────────────────────────


def mount_particle_map(container, project_path: Path, callbacks: dict[str, Callable], ui_mgr) -> ParticleMapView:
    """Build the map into `container` (the particles frame's body). Its timers are `view_scope("particles")`
    timers: a hidden view does nothing."""
    view = ParticleMapView(container, project_path, callbacks)
    ui.on("cb-bm-move", view.on_move)
    ui.on("cb-bm-view", view.on_view)
    install_pan_zoom()
    scope = ui_mgr.view_scope(MAP)
    scope.timer(_TICK_S, view.tick, container)
    scope.timer(_POLL_S, view.poll, container)
    return view


__all__ = ["ParticleMapView", "mount_particle_map"]
