"""The branch map as the Jobs view's content: cards on dotted paper in columns by how far down the chains they
sit, cables from output ports to input ports, the picker above, the controls between. Drag a card to move it,
drag the paper to pan, the wheel zooms; the positions survive a redraw and a reload. A click on a card's header
opens that job's page; the add button at its output lists the job types that can read it; the new job hangs off
that card with its inputs settled where the choice is forced, and ✓ in its header adds it to the project with
the inputs the card shows and the parameters typed on it. A scheduled job's input rows fan their candidates out
the same way and write the job's input through the setting door; a job that has run shows what it ran with.

`BranchMapView` is a `FingerprintedView`: its timer ticks every 3 s while the Jobs view shows, and the map is
drawn again only when a job's status, the roster, an input, the array statuses, the facts read off the loop or
the page state moved."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from nicegui import app, ui

from services import array_status_cache
from services.job_params import DENOISER, set_input, set_params
from services.path_resolution_service import PathResolutionService, instance_override_key
from services.pipeline_edit import species_defaults
from services.project_state import get_project_state_for
from services.tilt_series import get_registry_for
from ui.branch_map.blocks import box_of, draw_middle
from ui.branch_map.card import (
    CardSpec,
    Resolved,
    light,
    render_card,
    render_trunk,
    resolve,
    spec_of,
    spec_of_new,
    trunk_box,
    unsettled,
)
from ui.branch_map.facts import FactsStore, facts_store_for, kick
from ui.branch_map.geometry import CARD_W, GUTTER, PAD, Box, de_overlap, out_y_of, path_d, port_y, tidy
from ui.branch_map.model import NEW, TRUNK, Job, Project, Slot, project_from_state
from ui.branch_map.picker import render_picker
from ui.branch_map.state import MapState
from ui.branch_map.style import (
    BLUE,
    BODY,
    HAIR,
    INK,
    MENU_STYLE,
    MUTED,
    PAPER,
    PAPER_DOT,
    SLATE,
    SOFT,
    label,
    text,
    tok,
)
from ui.components.buttons import house_button
from ui.components.reactive import FingerprintedView
from ui.pipeline_builder.pipeline_constants import PHASE_JOBS, PHASE_PARTICLES

logger = logging.getLogger(__name__)

_TICK_S = 3.0
_PREFS_KEY = "branch_map"


# ── the map ──────────────────────────────────────────────────────────────────


class BranchMapView(FingerprintedView):
    """The map over one project, in one browser tab."""

    def __init__(self, container, panel, callbacks: dict[str, Callable]) -> None:
        super().__init__(container)
        self.panel = panel
        self.callbacks = callbacks
        self.ui_mgr = panel.ui_mgr
        self.project_path = panel.ui_mgr.project_path
        self.st = MapState()
        self.store: FactsStore = facts_store_for(self.project_path)
        self.visible = True
        self._pr: Project | None = None
        self._resolver: PathResolutionService | None = None
        self._restore()
        selected = callbacks.get("selected_ts")
        if selected is not None:
            self.st.active_ts = selected()
        callbacks.setdefault("follow_ts", {})["map"] = self._follow_ts

    # ── the signature: everything the render reads ──
    def signature(self) -> Any:
        state = get_project_state_for(self.project_path)
        per_job = []
        for iid, jm in sorted(state.jobs.items()):
            arr = array_status_cache.get(self.project_path, iid)
            arr_sig = None
            if arr is not None:
                arr_sig = hash((arr.items, tuple(arr.statuses.get(t, "") for t in arr.items)))
            per_job.append(
                (
                    iid,
                    jm.execution_status,
                    jm.relion_job_number,
                    jm.relion_job_name,
                    jm.species_id,
                    tuple(sorted((jm.source_overrides or {}).items())),
                    bool(jm.is_orphaned),
                    arr_sig,
                )
            )
        try:
            n_ts = len(get_registry_for(self.project_path).tilt_series_ids())
        except Exception:  # no registry yet: the map has no tilt series
            n_ts = 0
        return (
            tuple(state.pipeline_order),
            tuple(per_job),
            n_ts,
            state.registry_rev,
            state.species_identity(),
            self.st.signature(),
            self.store.version,
        )

    def tick(self) -> None:
        if self.visible:
            self.refresh()

    def set_visible(self, on: bool) -> None:
        self.visible = on
        if on:
            self.refresh()

    def redraw(self) -> None:
        """A user action changed the page: draw it again now."""
        self.mark_dirty()
        self.refresh()

    def relayout(self) -> None:
        """The whole map changes shape: let the browser fit it again."""
        self.st.zoom = None
        self.redraw()

    # ── what the browser keeps across reloads ──
    def _prefs(self) -> dict:
        try:
            store = app.storage.user.setdefault(_PREFS_KEY, {})
        except RuntimeError:  # no request context (a background completion): nothing to keep
            return {}
        return store.setdefault(str(self.project_path), {})

    def _restore(self) -> None:
        p = self._prefs()
        st = self.st
        st.moved = {k: (float(v[0]), float(v[1])) for k, v in (p.get("moved") or {}).items()}
        st.large = set(p.get("large") or [])
        st.all_large = bool(p.get("all_large", False))
        st.strips = bool(p.get("strips", True))
        st.trunk = bool(p.get("trunk", True))
        st.picker_pinned = bool(p.get("picker_pinned", False))
        st.picker_open = st.picker_pinned
        view = p.get("view")
        if view:
            st.zoom, st.tx, st.ty = float(view[0]), float(view[1]), float(view[2])

    def persist(self) -> None:
        p = self._prefs()
        st = self.st
        p["moved"] = {k: [v[0], v[1]] for k, v in st.moved.items()}
        p["large"] = sorted(st.large)
        p["all_large"] = st.all_large
        p["strips"] = st.strips
        p["trunk"] = st.trunk
        p["picker_pinned"] = st.picker_pinned
        p["view"] = [st.zoom, st.tx, st.ty] if st.zoom is not None else None

    # ── the browser's events ──
    def on_move(self, e) -> None:
        nid = e.args.get("nid")
        if nid:
            self.st.moved[nid] = (float(e.args["x"]), float(e.args["y"]))
            self.persist()

    def on_view(self, e) -> None:
        self.st.zoom, self.st.tx, self.st.ty = float(e.args["zoom"]), float(e.args["tx"]), float(e.args["ty"])
        self.persist()

    def on_mark(self, e) -> None:
        """A click on a magazine's mark, anywhere on the page."""
        ts = e.args.get("ts")
        if self._pr is not None and ts in self._pr.ts_ids:
            self.pick(None if ts == self.st.active_ts else ts)

    # ── the selected tilt series: the workspace's ──
    def pick(self, ts: str | None) -> None:
        self.st.active_ts = ts
        if ts is not None and not self.st.picker_pinned:
            self.st.picker_open = False
        if ts:
            select = self.callbacks.get("select_ts")
            if select is not None:
                select(ts, "map")
        self.redraw()

    def _follow_ts(self, ts: str) -> None:
        """The Journey or the Tomograms view moved the selection: the map follows while it shows, else on its
        next showing."""
        self.st.active_ts = ts
        if self.visible and self.ui_mgr.view_scope("pipeline").active:
            self.refresh()

    # ── render ──
    def render(self) -> None:
        state = get_project_state_for(self.project_path)
        self._resolver = PathResolutionService(state)
        pr = self._pr = project_from_state(state, self.project_path, set_parents=self.store.set_parents)
        wanted = set()
        for jm in state.jobs.values():
            for override in (jm.source_overrides or {}).values():
                if override.startswith("mergedSources:"):
                    wanted.add(override.split(":", 1)[1].removeprefix("particle_set__"))
            for recorded in (jm.paths or {}).values():
                if "/Analytics/sets/" in str(recorded):
                    wanted.add(str(recorded).split("/Analytics/sets/", 1)[1].split("/", 1)[0])
        missing = [s for s in wanted if s not in self.store.set_parents]
        if missing:
            kick(self.store.load_sets(missing), self.refresh)
        if not pr.jobs:
            _render_empty()
            return
        _render_map(self, pr, state)


# ── the empty project ────────────────────────────────────────────────────────


def _lamella() -> list[tuple[str, str]]:
    labels = [" "] * 42
    for col, s in ((9, "-60°"), (24, "0°"), (36, "+60°")):
        labels[col : col + len(s)] = s
    rows = [("hi", "".join(labels).rstrip())]
    for r in range(6, 0, -1):
        rows.append(("hi", " " * (24 - 2 * r) + "\\" + " " * (2 * r - 1) + "|" + " " * (2 * r - 1) + "/"))
    rows.append(("hi", " " * 23 + "\\|/"))
    face = "-" * 29
    rows += [
        ("", " " * 9 + "+" + "-" * 14 + "+" + "-" * 14 + "+"),
        ("", " " * 8 + "/" + "   o     .       o     .  o  " + "/|"),
        ("", " " * 7 + "/" + "  .    o     .  o     .    . " + "/ +"),
        ("", " " * 6 + "+" + face + "+ /"),
        ("", " " * 6 + "|" + "  lamella · 180 nm".ljust(29) + "|/"),
        ("", " " * 6 + "+" + face + "+"),
    ]
    return rows


def _render_empty() -> None:
    """A project without jobs yet: the figure, the prompt, where to add one."""
    lines = "\n".join(f'<span class="hi">{s}</span>' if c == "hi" else s for c, s in _lamella())
    with (
        ui.element("div")
        .classes("cb-bm-empty")
        .style(
            "flex: 1 1 0; min-height: 0; display: flex; align-items: center; justify-content: center; gap: 18px; "
            f"background: {PAPER}; background-image: radial-gradient({PAPER_DOT} 1px, transparent 1.2px); "
            "background-size: 16px 16px;"
        )
    ):
        ui.html(f'<pre class="cb-bm-ascii">{lines}</pre>', sanitize=False)
        with ui.column().style("gap: 6px; max-width: 220px;"):
            ui.html('<div class="cb-bm-prompt"><span class="p">~&gt;</span> no jobs yet</div>', sanitize=False)
            text("Add the first job from the roster's rows; every job you add appears here as a card.", 10, SOFT)


# ── the map ──────────────────────────────────────────────────────────────────


def _menu_row(pointer: bool = True) -> ui.element:
    return (
        ui.element("div")
        .classes("cb-bm-menu-row")
        .style(
            "display: flex; align-items: center; gap: 8px; padding: 5px 12px; "
            f"cursor: {'pointer' if pointer else 'default'};"
        )
    )


def _cable(
    a: str, ax: float, ay: float, b: str, bx: float, by: float, pos: dict, *, color: str, width: float, dash: str = ""
) -> str:
    """A cable from card `a`'s output to card `b`'s input, carrying both ends as offsets from the cards' corners so
    the browser can redraw it when a card is dragged."""
    (x0, y0), (x1, y1) = pos[a], pos[b]
    d = path_d(x0 + ax, y0 + ay, x1 + bx, y1 + by)
    dasha = f' stroke-dasharray="{dash}"' if dash else ""
    return (
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{dasha} '
        f'data-a="{a}" data-ax="{ax}" data-ay="{ay}" data-b="{b}" data-bx="{bx}" data-by="{by}"/>'
    )


def _render_map(view: BranchMapView, pr: Project, state) -> None:
    st = view.st
    store = view.store
    panel = view.panel
    resolved = resolve(pr, st)
    by_key = {r.slot.key: r for r in resolved}
    main = resolved[0] if resolved else None
    shared = pr.trunk() if st.trunk else []
    if len(shared) < 2:
        shared = []

    def anchor(nid: str) -> str:
        return TRUNK if nid in shared else nid

    def set_param(k: str, v) -> None:
        st.params[k] = v  # the field holds the typed value; no redraw follows

    new_defaults: dict = {}
    if st.adding and st.species_id:
        species = state.get_species(st.species_id)
        if species is not None:
            new_defaults = {k: v for k, v in species_defaults(pr.vocab.job_types[st.adding], species).items()}

    # ── which nodes, their boxes and kids ──
    specs: dict[str, CardSpec] = {j.id: spec_of(pr, j) for j in pr.jobs if j.id not in shared}
    if st.adding:
        specs[NEW] = spec_of_new(pr, st, resolved)
    is_large = {nid: st.all_large or nid in st.large for nid in specs}
    if st.adding:
        is_large[NEW] = True
    boxes: dict[str, Box] = {}
    middles: dict[str, list] = {}
    for nid, sp in specs.items():
        boxes[nid], middles[nid] = box_of(
            pr,
            sp,
            st,
            store,
            large=is_large[nid],
            on_param=set_param if nid == NEW else None,
            new_defaults=new_defaults if nid == NEW else None,
        )
    if shared:
        boxes[TRUNK] = trunk_box(pr, shared, st)
    _request_facts(view, pr, state, [nid for nid in specs if nid != NEW and is_large[nid]])

    def kids_of(nid: str) -> list[str]:
        if nid == NEW:
            return []
        base = shared[-1] if nid == TRUNK else nid
        kids = sorted(pr.main_children(pr.by_id[base]), key=lambda k: (pr.stage_index(k.kind), k.number, k.id))
        ids = [k.id for k in kids]
        if main is not None and main.source == base:
            ids.append(NEW)
        return ids

    def sources(nid: str) -> list[str]:
        if nid == TRUNK:
            return []
        if nid == NEW:
            return [anchor(r.source) for r in resolved if r.source is not None]
        j = pr.by_id[nid]
        return [anchor(s) for s in (j.parent, *j.side) if s is not None and s in pr.by_id]

    depth: dict[str, int] = {}

    def depth_of(nid: str) -> int:
        # Columns by how far down the chains a job sits: one past the deepest job it reads.
        if nid not in depth:
            depth[nid] = 0  # guards a cycle in the recorded inputs
            depth[nid] = max((depth_of(s) + 1 for s in sources(nid) if s != nid), default=0)
        return depth[nid]

    roots = [TRUNK] if shared else []
    roots += [j.id for j in pr.jobs if (j.parent is None or j.parent not in pr.by_id) and j.id not in shared]
    if st.adding and (main is None or main.source is None):
        roots.insert(0, NEW)
    outer = {nid: b.h + b.band for nid, b in boxes.items()}  # the node: its magazine's band and the card
    top = tidy(roots, kids_of, outer.__getitem__)
    for nid in specs:
        top.setdefault(nid, 0.0)  # reached by no main lineage (its parent sits in the folded start)
    col = {nid: depth_of(nid) for nid in top}
    ncols = max(col.values()) + 1
    colw = [max((boxes[n].w for n in top if col[n] == c), default=CARD_W) for c in range(ncols)]
    colx = [PAD + sum(colw[:c]) + c * GUTTER for c in range(ncols)]
    de_overlap(top, col, outer, ncols)
    shift = -min(top.values()) + PAD
    pos = {nid: (colx[col[nid]], top[nid] + shift + boxes[nid].band) for nid in top}  # the card's corner
    for nid, (mx, my) in st.moved.items():
        if nid in pos:
            pos[nid] = (mx, my)
    width = max(pos[n][0] + boxes[n].w for n in pos) + PAD
    height = max(pos[n][1] + boxes[n].h for n in pos) + PAD

    def out_at(nid: str) -> tuple[float, float]:
        return boxes[nid].w, out_y_of(boxes[nid].h)

    def in_at(i: int) -> tuple[float, float]:
        return 0, port_y(i)

    # ── the open input and its candidates ──
    open_nid, open_key = st.open_slot if st.open_slot else (None, None)
    if open_nid is not None and (
        open_nid not in specs or open_key not in {s.slot.key for s in specs[open_nid].sockets}
    ):
        open_nid, open_key = None, None
    open_r: Resolved | None = by_key.get(open_key) if open_nid == NEW else None
    cand_keys: dict[str, str] = {}  # candidate job id → the source key set_input takes
    if open_nid is not None and open_nid != NEW:
        cands, cand_keys = _candidates_of(view, pr, state, open_nid, open_key)
        j_open = pr.by_id[open_nid]
        current = next((r.source for r in j_open.inputs if r.slot.key == open_key), None)
        open_slot_obj = next(s.slot for s in specs[open_nid].sockets if s.slot.key == open_key)
        open_r = Resolved(open_slot_obj, "explicit" if current else "ambiguous", current, cands, "")
    fan = {c.id for c in open_r.candidates} if open_r else set()
    lit = {a.id for a in pr.path_of(pr.by_id[main.source])} | {NEW} if main is not None and main.source else set()
    lit = {anchor(n) for n in lit}

    # ── cables ──
    svg = []
    for nid, sp in specs.items():
        if nid == NEW:
            continue
        rows = pr.rows_of(pr.by_id[nid])
        for i, (sk, r) in enumerate(zip(sp.sockets, rows, strict=True)):
            if r.source is None or sk.bracket or sk.slot.quiet or r.source not in pr.by_id:
                continue
            a = anchor(r.source)
            if a not in pos:
                continue
            on = nid in lit and a in lit and (i == 0 or sk.slot.bundle)
            (ax, ay), (bx, by) = out_at(a), in_at(i)
            svg.append(
                _cable(
                    a,
                    ax,
                    ay,
                    nid,
                    bx,
                    by,
                    pos,
                    color=BLUE if on else tok("bm-cable", "#94a3b8"),
                    width=2 if on else 1.5,
                    dash="4 3" if (i > 0 and not sk.slot.bundle) else "",
                )
            )
    if st.adding:
        for i, r in enumerate(resolved):
            if r.source is None or specs[NEW].sockets[i].bracket or r.slot.quiet:
                continue
            a = anchor(r.source)
            if a not in pos:
                continue
            (ax, ay), (bx, by) = out_at(a), in_at(i)
            dash = "" if r.state == "explicit" else "6 3"
            svg.append(_cable(a, ax, ay, NEW, bx, by, pos, color=BLUE, width=2, dash=dash))
    if open_r is not None and open_nid is not None:
        i = [s.slot.key for s in specs[open_nid].sockets].index(open_key)
        for c in open_r.candidates:
            a = anchor(c.id)
            if a not in pos or c.id == open_r.source or a == open_nid:
                continue
            (ax, ay), (bx, by) = out_at(a), in_at(i)
            svg.append(
                _cable(a, ax, ay, open_nid, bx, by, pos, color=tok("bm-cable-fan", "#93c5fd"), width=1.5, dash="4 4")
            )

    # ── handlers ──
    def next_open() -> tuple[str, str] | None:
        key = next((r.slot.key for r in resolve(pr, st) if r.state in ("ambiguous", "missing")), None)
        return (NEW, key) if key else None

    def on_socket(nid: str, key: str) -> None:
        st.open_slot = None if st.open_slot == (nid, key) else (nid, key)
        view.redraw()

    def choose(nid: str, key: str, src: str) -> None:
        if nid == NEW:
            st.picks[key] = src
            if resolved and key == resolved[0].slot.key:
                # A new main choice lets the other inputs follow it again.
                st.picks = {key: src}
            st.open_slot = next_open()
            view.redraw()
            return
        source_key = cand_keys.get(src)
        if source_key is None:
            ui.notify(f"{pr.ident(pr.by_id[src])} is not a source this input can read.", type="warning")
            return
        res = set_input(state, nid, key, source=source_key)
        if not res["success"]:
            ui.notify(res["error"], type="negative", timeout=8000)
            return
        asyncio.create_task(panel.backend.save_project(view.project_path))
        st.open_slot = None
        view.redraw()

    async def start(kind: str, parent: str, slot: Slot) -> None:
        jt = pr.vocab.job_types[kind]
        species_id = None
        if jt in PHASE_JOBS[PHASE_PARTICLES]:
            species_id = await panel.choose_species(jt)
            if species_id is None:
                return
        st.adding, st.method, st.picks, st.params = (
            kind,
            (pr.methods.get(kind, ((None, ""),))[0][0]),
            {slot.key: parent},
            {},
        )
        st.species_id = species_id
        st.params_rev += 1
        st.moved.pop(NEW, None)
        st.open_slot = next_open()
        view.relayout()

    def discard() -> None:
        st.adding, st.open_slot, st.species_id = None, None, None
        st.picks.clear()
        st.params.clear()
        view.relayout()

    async def add() -> None:
        await _add_new_job(view, pr, state, resolved)

    def set_method(method: str) -> None:
        st.method = method
        st.picks = {k: v for k, v in st.picks.items() if k in {s.key for s in pr.slots_of(st.adding, method)}}
        st.open_slot = next_open()
        view.redraw()

    def toggle_size(nid: str) -> None:
        if st.all_large:
            st.all_large = False
            st.large = {n for n in specs if n != NEW} - {nid}
        else:
            st.large ^= {nid}
        view.persist()
        view.redraw()

    def open_job(iid: str) -> None:
        panel.switch_tab(iid)

    def plus_menu(add_el: ui.element, j: Job) -> None:
        with add_el:
            menu = (
                ui.menu()
                .props('no-parent-event anchor="bottom right" self="top right"')
                .style(MENU_STYLE + " min-width: 300px;")
            )
            with menu, ui.column().style("gap: 0; padding: 6px 0;"):
                text(f"Add a job that reads {pr.ident(j)}", 9, SOFT, bold=True, extra="padding: 3px 12px 6px;")
                for kind, slot, why in pr.offered_after(j):
                    row = _menu_row(bool(slot))
                    row.style(f"opacity: {1 if slot else 0.5};")
                    with row:
                        ui.icon(pr.icon.get(kind, "add"), size="13px").style(f"color: {SLATE};")
                        text(pr.names.get(kind, kind), 10, INK, bold=bool(slot), extra="width: 128px;")
                        if slot:
                            others = [
                                s
                                for s in pr.slots_of(kind)
                                if s.key != slot.key and not s.bundle and s.required and not s.quiet
                            ]
                            text(
                                f"as its {slot.label}"
                                + (f" · also needs {', '.join(s.label for s in others)}" if others else ""),
                                9,
                                SOFT,
                            )
                        else:
                            text(why, 9, MUTED)
                    if slot:
                        row.on("click", lambda _e, k=kind, s=slot, m=menu: (m.close(), start(k, j.id, s))[1])
        add_el.on("click.stop", lambda _e, m=menu: m.open())

    def method_menu(tokel: ui.element) -> None:
        with tokel:
            menu = (
                ui.menu()
                .props('no-parent-event anchor="bottom left" self="top left"')
                .style(MENU_STYLE + " min-width: 220px;")
            )
            with menu, ui.column().style("gap: 0; padding: 6px 0;"):
                text("Run with", 9, SOFT, bold=True, extra="padding: 3px 12px 6px;")
                for value, words in pr.methods.get(st.adding, ()):
                    row = _menu_row()
                    with row:
                        text(words, 10, INK, bold=value == st.method, extra="width: 90px;")
                        text(pr.gloss.get(words, ""), 9, SOFT, extra="max-width: 200px; white-space: normal;")
                    row.on("click", lambda _e, v=value, mn=menu: (mn.close(), set_method(v)))
        tokel.on("click.stop", lambda _e, m=menu: m.open())

    def list_menu(nid: str, el: ui.element, key: str) -> None:
        if nid == NEW:
            r = by_key[key]
            cands, current = r.candidates, r.source
        else:
            cands, _keys = _candidates_of(view, pr, state, nid, key)
            current = next((x.source for x in pr.by_id[nid].inputs if x.slot.key == key), None)
        slot_label = next(s.slot.label for s in specs[nid].sockets if s.slot.key == key)
        with el:
            menu = (
                ui.menu()
                .props('no-parent-event anchor="bottom right" self="top right"')
                .style(MENU_STYLE + " min-width: 300px;")
            )
            with menu, ui.column().style("gap: 0; padding: 6px 0;"):
                text(f"{slot_label}: the jobs it can read", 9, SOFT, bold=True, extra="padding: 3px 12px 6px;")
                if not cands:
                    text("none in the project yet", 9, MUTED, extra="padding: 3px 12px;")
                for c in cands:
                    row = _menu_row()
                    with row:
                        light(c.status, 6)
                        text(pr.ident(c), 10, INK, bold=c.id == current, extra="width: 150px;")
                        text(" · ".join(pr.pedigree(c, cands)), 9, BODY, extra="flex: 1 1 0;")
                        tally, color = pr.ts_text(c)
                        text(tally, 9, color, mono=True)
                    row.on("click", lambda _e, n=c.id, k=key, m=menu, nd=nid: (m.close(), choose(nd, k, n)))
        el.on("click.stop", lambda _e, m=menu: m.open())

    def job_menu(el: ui.element, j: Job) -> None:
        with el:
            menu = (
                ui.menu()
                .props('no-parent-event anchor="bottom left" self="top left"')
                .style(MENU_STYLE + " min-width: 200px;")
            )
            with menu, ui.column().style("gap: 0; padding: 6px 0;"):
                text(pr.ident(j), 9, SOFT, bold=True, extra="padding: 3px 12px 6px;")
                row = _menu_row()
                with row:
                    ui.icon("open_in_new", size="13px").style(f"color: {SLATE};")
                    text("Open the job's page", 10, INK)
                row.on("click", lambda _e, m=menu: (m.close(), open_job(j.id)))
                if j.kind not in ("tiltFilter", "importmovies", "tsImport"):
                    row = _menu_row()
                    with row:
                        ui.icon("content_copy", size="13px").style(f"color: {SLATE};")
                        text("Clone", 10, INK, extra="width: 60px;")
                        text(
                            "a new job with these parameters, SLURM settings and inputs set by hand",
                            9,
                            SOFT,
                            extra="white-space: normal; max-width: 200px;",
                        )
                    row.on(
                        "click",
                        lambda _e, m=menu: (
                            m.close(),
                            panel.add_instance_to_pipeline(
                                j.job_type, species_id=j.species, clone_of=j.id, open_page=False
                            ),
                        ),
                    )
                if j.on_roster and j.kind != "tsImport":
                    row = _menu_row()
                    with row:
                        ui.icon("close", size="13px").style(f"color: {tok('bad-dot', '#dc2626')};")
                        text("Remove", 10, tok("bad-dot", "#b91c1c"))
                    row.on("click", lambda _e, m=menu: (m.close(), panel.roster._on_remove_click(j.id))[1])
        el.on("click.stop", lambda _e, m=menu: m.open())

    # ── the picker, then the controls ──
    render_picker(pr, st, view.redraw, view.pick)
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
            lambda _e: (setattr(st, "all_large", False), st.large.clear(), view.persist(), view.relayout()),
            kind="default" if st.all_large or st.large else "accent",
        )
        house_button(
            "all large",
            lambda _e: (setattr(st, "all_large", True), view.persist(), view.relayout()),
            kind="accent" if st.all_large else "default",
        )
        house_button(
            "magazines" if st.strips else "no magazines",
            lambda _e: (setattr(st, "strips", not st.strips), view.persist(), view.relayout()),
            tooltip="One mark per tilt series above every card",
        )
        house_button(
            "shared start folded" if st.trunk else "shared start unfolded",
            lambda _e: (setattr(st, "trunk", not st.trunk), view.persist(), view.relayout()),
        )
        if st.moved:
            house_button(
                "reset layout",
                lambda _e: (st.moved.clear(), view.persist(), view.relayout()),
                tooltip="Put dragged cards back",
            )
        ui.element("div").style("flex: 1 1 0;")
        label("Zoom", 9, SOFT, case="none", track="0", extra="margin-left: 8px;")
        house_button("−").on("click", js_handler="() => window.cbBranchMap.zoomBy(1 / 1.25)")
        ui.label("100 %").classes("cb-bm-zoom-label").style(
            "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif); font-size: 10px; "
            f"color: {BODY}; width: 40px; text-align: center;"
        )
        house_button("+").on("click", js_handler="() => window.cbBranchMap.zoomBy(1.25)")
        house_button("fit").on("click", js_handler="() => window.cbBranchMap.fit()")

    # ── the canvas: dotted paper; the world inside it pans and zooms in the browser ──
    canvas = (
        ui.element("div")
        .classes("cb-bm-canvas")
        .style(
            "position: relative; width: 100%; flex: 1 1 0; min-height: 0; overflow: hidden; "
            f"background: {PAPER}; user-select: none; touch-action: none; cursor: grab; "
            f"background-image: radial-gradient({PAPER_DOT} 1px, transparent 1.2px); background-size: 16px 16px;"
        )
    )
    if st.zoom is not None:
        canvas.props(f'data-zoom="{st.zoom}" data-tx="{st.tx}" data-ty="{st.ty}"')
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
                if nid == TRUNK:
                    render_trunk(
                        pr,
                        shared,
                        boxes[TRUNK],
                        st,
                        x=x,
                        y=y,
                        on_plus=lambda add_el: plus_menu(add_el, pr.by_id[shared[-1]]),
                        on_click=lambda _e: (setattr(st, "trunk", False), view.persist(), view.redraw()),
                        on_open=open_job,
                        dim=bool(fan) and TRUNK not in fan,
                    ).props(f'data-nid="{TRUNK}"')
                    continue
                if nid == NEW:
                    render_card(
                        pr,
                        specs[NEW],
                        boxes[NEW],
                        st,
                        x=x,
                        y=y,
                        middle=lambda s=middles[NEW]: draw_middle(s),
                        on_socket=lambda k: on_socket(NEW, k),
                        on_list=lambda el, k: list_menu(NEW, el, k),
                        on_method=method_menu,
                        open_slot=open_key if open_nid == NEW else None,
                        on_discard=lambda _e: discard(),
                        on_add=lambda _e: add(),
                        todo=[r.slot.label for r in unsettled(resolved)],
                    ).props(f'data-nid="{NEW}"')
                    continue
                j = pr.by_id[nid]
                is_cand = nid in fan
                editable = j.status == "scheduled" and not j.frozen
                card = render_card(
                    pr,
                    specs[nid],
                    boxes[nid],
                    st,
                    x=x,
                    y=y,
                    middle=(lambda s=middles[nid]: draw_middle(s)) if is_large[nid] else None,
                    on_socket=(lambda k, n=nid: on_socket(n, k)) if editable else None,
                    on_list=(lambda el, k, n=nid: list_menu(n, el, k)) if editable else None,
                    on_plus=lambda add_el, j=j: plus_menu(add_el, j),
                    on_open=None if is_cand else (lambda _e, n=nid: open_job(n)),
                    on_size=None if is_cand else (lambda _e, n=nid: toggle_size(n)),
                    on_menu=lambda el, j=j: job_menu(el, j),
                    candidate=is_cand,
                    chosen=open_r is not None and open_r.source == nid,
                    dim=bool(fan) and not is_cand and nid != open_nid,
                    on_chain=nid in lit,
                    open_slot=open_key if open_nid == nid else None,
                    use_label=f"use as {open_r.slot.label}" if is_cand and open_r is not None else None,
                    on_use=(lambda n=nid: choose(open_nid, open_key, n)) if is_cand and open_r is not None else None,
                )
                card.props(f'data-nid="{nid}"')
                if is_cand:
                    card.on("click", lambda _e, n=nid: choose(open_nid, open_key, n))


def _candidates_of(view: BranchMapView, pr: Project, state, nid: str, key: str) -> tuple[list[Job], dict[str, str]]:
    """The jobs on the map one input of an existing job can read, as the resolver offers them, and the source key
    the setting door takes for each."""
    jm = state.jobs.get(nid)
    if jm is None or view._resolver is None:
        return [], {}
    try:
        cands = view._resolver.get_candidates_for_slot(
            jm.job_type, key, consumer_species_id=jm.species_id, consumer_instance_id=nid
        )
    except Exception:
        logger.exception("branch map: candidates of %s.%s", nid, key)
        return [], {}
    jobs: list[Job] = []
    keys: dict[str, str] = {}
    for c in cands:
        j = pr.by_id.get(c.producer_instance_id)
        if j is None or j.id in keys:
            continue
        jobs.append(j)
        keys[j.id] = c.source_key
    return jobs, keys


def _request_facts(view: BranchMapView, pr: Project, state, large: list[str]) -> None:
    """Read the facts a large card needs off the loop, once per job status (a running job's again every while)."""
    store = view.store
    if not large:
        return
    try:
        registry = get_registry_for(view.project_path)
        series = list(registry.all_tilt_series())
        stamp = (registry.loaded_stamp, len(series))
    except Exception:  # no registry yet
        logger.exception("branch map: the registry of %s", view.project_path)
        return
    by_id = {ts.id: ts for ts in series}
    for nid in large:
        j = pr.by_id[nid]
        jm = state.jobs.get(nid)
        if jm is None or j.status not in ("done", "failed", "running"):
            continue
        if store.job_stale(j) and not store.loading(("job", j.id)):
            kick(store.load_job(j, jm, series, stamp, pr.ts_ids), view.refresh)
        ts = view.st.active_ts
        if ts and store.ts_stale(j, ts) and not store.loading(("ts", j.id, ts)):
            kick(store.load_ts(j, jm, by_id.get(ts), ts), view.refresh)


async def _add_new_job(view: BranchMapView, pr: Project, state, resolved: list[Resolved]) -> None:
    """The ✓: the job goes into the project through the pipeline-edit door with the inputs the card shows pinned,
    then its parameters as typed and its method through the setting door, then a save."""
    st = view.st
    panel = view.panel
    kind = st.adding
    if kind is None:
        return
    jt = pr.vocab.job_types[kind]
    reading: dict[str, str] = {}
    model_source: str | None = None
    for r in resolved:
        if r.source is None or r.source not in pr.by_id:
            continue
        src = pr.by_id[r.source]
        reading[r.slot.key] = instance_override_key(src.job_type, src.id)
        if r.slot.key == "model_path":
            model_source = src.id
    res = panel.add_instance_to_pipeline(jt, species_id=st.species_id, reading=reading, open_page=False)
    if not res["success"]:
        return  # refused, and said so
    iid = res["instance_id"]
    values: dict = dict(st.params)
    # The typed values as the form holds them: a toggle's yes / no back to a bool.
    for fs in pr.param_fields.get(kind, ()):
        if fs.name in values and fs.group == "toggle":
            values[fs.name] = values[fs.name] == "yes"
    method_field = {
        "aligntiltsWarp": "alignment_method",
        "denoisetrain": "denoise_method",
        "segment": "method",
        "tiltFilter": "mode",
    }
    if kind == "denoisepredict":
        if st.method == "easymode":
            values[DENOISER] = "easymode"
        elif model_source is not None:
            values[DENOISER] = f"train:{model_source}"
    elif kind in method_field and st.method:
        values[method_field[kind]] = st.method
    if values:
        set_res = set_params(state, iid, values)
        if not set_res["success"]:
            ui.notify(f"{iid} added; some parameters were not set: {set_res['error']}", type="warning", timeout=9000)
    await panel.backend.save_project(view.project_path)
    st.adding, st.open_slot, st.species_id = None, None, None
    st.picks.clear()
    st.params.clear()
    st.moved.pop(NEW, None)
    view.relayout()


# ── mounting ─────────────────────────────────────────────────────────────────

# Pan, zoom and drag, in the browser: a pointer-down on a card moves it (its cables follow, by the offsets each
# path carries), one on the paper pans, the wheel zooms about the pointer. A drag's end and a view change go to the
# server as events, so the next redraw keeps them; a click on a magazine's mark sends its tilt series. Installed
# once per page; a MutationObserver picks up every canvas the page builds afterwards.
_JS = """
(function () {
  if (window.cbBranchMap) return;
  const M = (window.cbBranchMap = {});
  const pathD = (x0, y0, x1, y1) => {
    const mx = (x0 + x1) / 2;
    return `M ${x0} ${y0} C ${mx} ${y0}, ${mx} ${y1}, ${x1} ${y1}`;
  };
  const pos = (card) => ({ x: parseFloat(card.style.left) || 0, y: parseFloat(card.style.top) || 0 });
  const cardOf = (world, nid) => world.querySelector(`.cb-bm-card[data-nid="${nid}"]`);
  function relink(world, nid) {
    world.querySelectorAll(`path[data-a="${nid}"], path[data-b="${nid}"]`).forEach((p) => {
      const A = cardOf(world, p.dataset.a), B = cardOf(world, p.dataset.b);
      if (!A || !B) return;
      const a = pos(A), b = pos(B);
      p.setAttribute('d', pathD(a.x + +p.dataset.ax, a.y + +p.dataset.ay, b.x + +p.dataset.bx, b.y + +p.dataset.by));
    });
  }
  function apply(c) {
    c.querySelector('.cb-bm-world').style.transform = `translate(${c._tx}px, ${c._ty}px) scale(${c._z})`;
    document.querySelectorAll('.cb-bm-zoom-label').forEach((l) => (l.textContent = Math.round(c._z * 100) + ' %'));
  }
  function emitView(c) {
    clearTimeout(c._t);
    c._t = setTimeout(() => emitEvent('cb-bm-view', { zoom: c._z, tx: c._tx, ty: c._ty }), 300);
  }
  function fit(c) {
    const w = c.querySelector('.cb-bm-world');
    const W = parseFloat(w.style.width), H = parseFloat(w.style.height);
    const z = Math.min(1, (c.clientWidth - 24) / W, (c.clientHeight - 24) / H);
    c._z = z; c._tx = Math.max(12, (c.clientWidth - W * z) / 2); c._ty = 12;
    apply(c); emitView(c);
  }
  function zoomAt(c, px, py, f) {
    const z = Math.min(3, Math.max(0.2, c._z * f));
    c._tx = px - (px - c._tx) * (z / c._z); c._ty = py - (py - c._ty) * (z / c._z); c._z = z;
    apply(c); emitView(c);
  }
  const canvas = () => document.querySelector('.cb-bm-canvas');
  M.fit = () => { const c = canvas(); if (c) fit(c); };
  M.zoomBy = (f) => { const c = canvas(); if (c) zoomAt(c, c.clientWidth / 2, c.clientHeight / 2, f); };
  document.addEventListener('click', (e) => {
    const mark = e.target.closest && e.target.closest('.cb-bm-card [data-ts]');
    if (mark) { e.stopPropagation(); emitEvent('cb-bm-ts', { ts: mark.dataset.ts }); }
  }, { capture: true });
  function init(c) {
    if (c.dataset.init) return;
    c.dataset.init = '1';
    c._z = parseFloat(c.dataset.zoom); c._tx = parseFloat(c.dataset.tx); c._ty = parseFloat(c.dataset.ty);
    const sized = () => c.clientWidth > 40 && c.clientHeight > 40;
    if (!isNaN(c._z)) apply(c);
    else if (sized()) fit(c);
    else {
      const ro = new ResizeObserver(() => { if (sized()) { ro.disconnect(); fit(c); } });
      ro.observe(c);
    }
    c.addEventListener('wheel', (e) => {
      e.preventDefault();
      const r = c.getBoundingClientRect();
      zoomAt(c, e.clientX - r.left, e.clientY - r.top, Math.exp(-e.deltaY * 0.0012));
    }, { passive: false });
    c.addEventListener('pointerdown', (e) => {
      if (e.button !== 0) return;
      const own = '.cb-bm-add, .cb-bm-list, .cb-bm-port, .cb-bm-pill, .cb-bm-tok, .cb-bm-size, .cb-bm-x, .cb-bm-ok, ' +
        '.cb-bm-num, .q-menu, input, button, .q-field, [data-ts]';
      if (e.target.closest(own)) return;
      const card = e.target.closest('.cb-bm-card');
      const world = c.querySelector('.cb-bm-world');
      const start = { x: e.clientX, y: e.clientY };
      const p0 = card ? pos(card) : { x: c._tx, y: c._ty };
      let moved = false;
      const move = (ev) => {
        const dx = ev.clientX - start.x, dy = ev.clientY - start.y;
        if (!moved && Math.hypot(dx, dy) < 4) return;
        moved = true;
        if (card) {
          card.style.left = p0.x + dx / c._z + 'px'; card.style.top = p0.y + dy / c._z + 'px';
          card.classList.add('cb-bm-dragging'); relink(world, card.dataset.nid);
        } else { c._tx = p0.x + dx; c._ty = p0.y + dy; apply(c); }
      };
      const up = () => {
        window.removeEventListener('pointermove', move); window.removeEventListener('pointerup', up);
        if (!moved) return;
        if (card) {
          card.classList.remove('cb-bm-dragging');
          const p = pos(card); emitEvent('cb-bm-move', { nid: card.dataset.nid, x: p.x, y: p.y });
        } else emitView(c);
        const swallow = (ce) => { ce.stopPropagation(); ce.preventDefault(); };
        window.addEventListener('click', swallow, { capture: true, once: true });
        setTimeout(() => window.removeEventListener('click', swallow, { capture: true }), 60);
      };
      window.addEventListener('pointermove', move); window.addEventListener('pointerup', up);
    });
  }
  const scan = () => document.querySelectorAll('.cb-bm-canvas:not([data-init])').forEach(init);
  new MutationObserver(scan).observe(document.body, { childList: true, subtree: true });
  scan();
})();
"""


def mount_branch_map(container, panel, callbacks: dict[str, Callable]) -> BranchMapView:
    """Build the map into `container` (the pipeline frame's body beside the job pages) and keep it current while
    the Jobs view shows: its timer is a `view_scope("pipeline")` timer, so a hidden Jobs view does nothing."""
    view = BranchMapView(container, panel, callbacks)
    ui.on("cb-bm-move", view.on_move)
    ui.on("cb-bm-view", view.on_view)
    ui.on("cb-bm-ts", view.on_mark)
    ui.run_javascript(_JS)
    panel.ui_mgr.view_scope("pipeline").timer(_TICK_S, view.tick, container)
    view.refresh()
    return view


__all__ = ["BranchMapView", "mount_branch_map"]
