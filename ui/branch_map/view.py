"""The branch map as the Jobs view's content: every job a card, in a grid in the order the jobs were made, wrapping
at the view's width, scrolled down; nothing is dragged or zoomed. Cables run from each job's output to what reads
it. A click on a card selects its job (in the roster too) and lights its lineage, everything it was made from and
everything made from it, and dims the rest; a click on the selected card opens its page. `f` or `/` finds jobs.
The header's "+" adds a job that reads it: the new job's card appears beside its parent, its inputs chosen in a
palette that lights the jobs each can read, and ✓ adds it. A scheduled job's input rows choose what they read the
same way. With "by species" each species is a band of its own.

The cards are HTML strings (ui/branch_map/html.py), one element each; the browser lays them out and draws the
cables, the lineage, find and the menus (ui/branch_map/assets.py) over a graph sent once per change. The view keeps
its cards by job: a change redraws the cards it changed, and the skeleton (the bands and grids) is built again only
when the jobs or the grouping change. Its timer ticks every 3 s while the Jobs view shows the map, and the cards are
looked at again only when a job's status, the roster, an input, the array statuses, the facts read off the loop or
the page state moved."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Callable
from typing import Any

from nicegui import app, ui

from services import array_status_cache
from services.job_params import DENOISER, set_input, set_params
from services.path_resolution_service import PathResolutionService, instance_override_key
from services.pipeline_edit import species_defaults
from services.project_state import get_project_state_for
from services.tilt_series import get_registry_for
from ui.branch_map.assets import MAP_JS
from ui.branch_map.blocks import new_sections
from ui.branch_map.card import Resolved, render_new_card, resolve, spec_of_new, unsettled
from ui.branch_map.facts import FactsStore, facts_store_for, kick
from ui.branch_map.html import band_html, card_html, reads_from_job, trunk_html
from ui.branch_map.model import NEW, TRUNK, Job, Project, project_from_state
from ui.branch_map.picker import picker_body, render_ts_chip
from ui.branch_map.state import MapState
from ui.branch_map.style import HAIR, INK, MENU_STYLE, PAPER, SOFT, text
from ui.components.buttons import icon_button
from ui.components.check import house_check
from ui.pipeline_builder.pipeline_constants import PHASE_JOBS, PHASE_PARTICLES

logger = logging.getLogger(__name__)

_TICK_S = 3.0
_PREFS_KEY = "branch_map"
_NO_SPECIES = "every species"
# More words a job type is found by, besides its name and its kind.
_ALIASES = {
    "importmovies": "movies frames",
    "fsMotionAndCtf": "motion ctf fs",
    "tsImport": "tilt series import",
    "tiltFilter": "filter tilts",
    "aligntiltsWarp": "align aretomo imod",
    "missAlign": "missalignment refine alignment",
    "tsCtf": "ctf defocus",
    "tsReconstruct": "reconstruct tomograms recon",
    "denoisetrain": "denoise train cryocare",
    "denoisepredict": "denoise predict",
    "segment": "segmentation membranes",
    "templatematching": "tm template matching pytom",
    "tmextractcand": "picks candidates pick",
    "filamentPick": "filament pick",
    "subtomoExtraction": "extract subtomograms",
    "warpExport": "export particles",
    "reconstructParticle": "reconstruct average map",
    "class3d": "classification classify classes",
    "refine3d": "refinement autorefine",
    "maskCreate": "mask",
    "postprocess": "pp sharpen fsc",
    "ctfRefine": "ctf refine",
}
_NO_CLONE = ("tiltFilter", "importmovies", "tsImport")


def _words(*parts: str) -> list[str]:
    out: list[str] = []
    for p in parts:
        for w in re.split(r"[^a-z0-9]+", (p or "").lower()):
            if w and w not in out:
                out.append(w)
    return out


class BranchMapView:
    """The map over one project, in one browser tab."""

    MAP = "jobs"  # the name its events carry

    def __init__(self, container, panel, callbacks: dict[str, Callable]) -> None:
        self.container = container
        self.panel = panel
        self.callbacks = callbacks
        self.ui_mgr = panel.ui_mgr
        self.project_path = panel.ui_mgr.project_path
        self.client = ui.context.client
        self.st = MapState()
        self.store: FactsStore = facts_store_for(self.project_path)
        self.visible = True
        self._pr: Project | None = None
        self._resolver: PathResolutionService | None = None
        self._lead: ui.element | None = None
        self._tools: ui.element | None = None
        self._head_sig: tuple | None = None
        self._last_sig: Any = None
        self._shape: tuple | None = None
        self._cards: dict[str, ui.html] = {}  # node id → its card's element
        self._html: dict[str, str] = {}  # node id → the HTML that element holds
        self._large_cls: set[str] = set()
        self._new_box: ui.element | None = None
        self._new_sig: tuple | None = None
        self._new_at: tuple | None = None  # (its grid, its place in it)
        self._grids: list[tuple[str | None, ui.element]] = []  # per band: its title, its grid
        self._graph = ""
        self._slot = "null"
        self._cand_keys: dict[str, str] = {}  # the open input's candidates → the source key set_input takes
        self._restore()
        selected = callbacks.get("selected_ts")
        if selected is not None:
            self.st.active_ts = selected()
        callbacks.setdefault("follow_ts", {})["map"] = self._follow_ts
        with container:
            self._empty = ui.element("div").style("display: flex; flex: 1 1 0; min-height: 0;")
            self._empty.set_visibility(False)
            self._scroll = ui.element("div").classes("cb-bm-scroll")
            with self._scroll:
                self._world = ui.element("div").classes("cb-bm-world")

    # ── the signature: everything the cards read ──
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

    def refresh(self) -> None:
        """Look again: the cards whose HTML moved are sent, the rest stay."""
        if not self.visible or self.container.is_deleted:
            return
        try:
            sig = self.signature()
        except Exception:  # a state mid-change: look at everything again
            logger.exception("branch map: signature of %s", self.project_path)
            sig = None
        if sig is not None and sig == self._last_sig:
            return
        self._last_sig = sig
        self._update()

    def tick(self) -> None:
        if self.visible:
            self.refresh()

    def redraw(self) -> None:
        """A user action changed the page: look again now."""
        self._last_sig = None
        self.refresh()

    def set_visible(self, on: bool) -> None:
        self.visible = on
        for slot in (self._lead, self._tools):
            if slot is not None and not slot.is_deleted:
                slot.set_visibility(on)
        if on:
            self.refresh()

    def _js(self, code: str) -> None:
        self.client.run_javascript(code)

    def paint_selected(self) -> None:
        """The job the panel has selected, lit with its lineage (the browser paints it)."""
        self._js(f"window.cbBm && cbBm.select({json.dumps(self.panel.selected)}, true)")

    # ── what the user's storage keeps across reloads ──
    def _prefs(self) -> dict:
        try:
            store = app.storage.user.setdefault(_PREFS_KEY, {})
        except RuntimeError:  # no request context (a background completion): nothing to keep
            return {}
        return store.setdefault(str(self.project_path), {})

    def _restore(self) -> None:
        p = self._prefs()
        st = self.st
        st.large = set(p.get("large") or [])
        st.all_large = bool(p.get("all_large", False))
        st.strips = bool(p.get("strips", True))
        st.trunk = bool(p.get("trunk", True))
        st.by_species = bool(p.get("by_species", False))

    def persist(self) -> None:
        p = self._prefs()
        st = self.st
        p["large"] = sorted(st.large)
        p["all_large"] = st.all_large
        p["strips"] = st.strips
        p["trunk"] = st.trunk
        p["by_species"] = st.by_species
        for gone in ("moved", "view"):  # kept by the map that was dragged and zoomed
            p.pop(gone, None)

    # ── the selected tilt series: the workspace's ──
    def pick(self, ts: str | None) -> None:
        self.st.active_ts = ts
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

    # ── drawing ──
    def _update(self) -> None:
        state = get_project_state_for(self.project_path)
        self._resolver = PathResolutionService(state)
        pr = self._pr = project_from_state(state, self.project_path, set_parents=self.store.set_parents)
        self._load_sets(state)
        self._render_head(pr)
        if not pr.jobs:
            self._scroll.set_visibility(False)
            if not self._empty.default_slot.children:
                with self._empty:
                    _render_empty()
            self._empty.set_visibility(True)
            self._world.clear()
            self._shape, self._graph, self._slot = None, "", "null"
            self._cards.clear()
            return
        self._empty.set_visibility(False)
        self._scroll.set_visibility(True)
        st = self.st
        resolved = resolve(pr, st)
        shared = pr.trunk() if st.trunk else []
        if len(shared) < 2:
            shared = []

        def anchor(nid: str) -> str:
            return TRUNK if nid in shared else nid

        order = self._order(pr, shared)
        bands = self._bands(pr, order)
        shape = tuple((title, tuple(nids)) for title, nids in bands)
        if shape != self._shape:
            self._build(pr, bands)
            self._shape = shape
        large = {n for n in order if n != TRUNK and (st.all_large or n in st.large)}
        for nid in order:
            if nid == TRUNK:
                html = trunk_html(pr, shared, strips=st.strips, active_ts=st.active_ts)
            else:
                j = pr.by_id[nid]
                html = card_html(
                    pr,
                    j,
                    large=nid in large,
                    strips=st.strips,
                    active_ts=st.active_ts,
                    editable=j.status == "scheduled" and not j.frozen,
                    facts=self.store.job_facts(j) if nid in large else None,
                    ts_facts=self.store.ts_facts(j, st.active_ts) if nid in large and st.active_ts else (False, None),
                )
            el = self._cards.get(nid)
            if el is None or el.is_deleted:
                continue
            if self._html.get(nid) != html:
                el.set_content(html)
                self._html[nid] = html
            if (nid in large) != (nid in self._large_cls):
                el.classes(add="large") if nid in large else el.classes(remove="large")
                self._large_cls ^= {nid}
        self._place_new(pr, resolved, anchor)
        if st.adding:
            self._draw_new(pr, state, resolved)
        graph = self._graph_json(pr, shared, resolved, anchor)
        if graph != self._graph:
            self._graph = graph
            self._js(f"cbBm.setGraph({graph})")
        self._send_slot(pr, state, resolved, anchor)
        self._request_facts(pr, state, sorted(large))

    def _order(self, pr: Project, shared: list[str]) -> list[str]:
        """Every node in the order the jobs were made (by number, then the ones not run yet), the folded start
        first."""
        ids = [j.id for j in pr.jobs if j.id not in shared]
        if shared:
            ids.insert(0, TRUNK)
        return ids

    def _species_of(self, pr: Project, nid: str) -> str:
        return "" if nid == TRUNK else pr.by_id[nid].species or ""

    def _bands(self, pr: Project, order: list[str]) -> list[tuple[str | None, list[str]]]:
        if not self.st.by_species:
            return [(None, order)]
        groups: dict[str, list[str]] = {}
        for nid in order:
            groups.setdefault(self._species_of(pr, nid), []).append(nid)
        keys = sorted(groups, key=lambda s: (s != "", order.index(groups[s][0])))
        return [(pr.species_names.get(s, s) if s else _NO_SPECIES, groups[s]) for s in keys]

    def _build(self, pr: Project, bands: list[tuple[str | None, list[str]]]) -> None:
        """The skeleton: the cables' layer, then per band its head and its grid of empty cards (filled after)."""
        self._world.clear()
        self._cards.clear()
        self._html.clear()
        self._large_cls.clear()
        self._grids.clear()
        self._new_box, self._new_sig, self._new_at = None, None, None
        with self._world:
            ui.html("<svg></svg>", sanitize=False).classes("cb-bm-cables")
            for title, nids in bands:
                if title is not None:
                    n = sum(1 for x in nids if x != TRUNK)
                    ui.html(band_html(title, n), sanitize=False).classes("cb-bm-band")
                with ui.element("div").classes("cb-bm-grid") as grid:
                    for nid in nids:
                        el = ui.html("", sanitize=False).classes("cb-bm-node")
                        el.props["data-nid"] = nid
                        self._cards[nid] = el
                self._grids.append((title, grid))
        self._js("cbBm.mount()")

    def _place_new(self, pr: Project, resolved: list[Resolved], anchor) -> None:
        """The new job's card, in the grid right after the job its main input reads (first in its species' band,
        or the map, before it reads one); moved, not rebuilt, when that choice changes, and gone with the job."""
        st = self.st
        if not st.adding:
            if self._new_box is not None and not self._new_box.is_deleted:
                self._new_box.delete()
            self._new_box, self._new_sig, self._new_at = None, None, None
            return
        if not self._grids:
            return
        main = resolved[0].source if resolved else None
        parent = anchor(main) if main else None
        parent_el = self._cards.get(parent) if parent else None
        grid = next((g for _, g in self._grids if parent_el is not None and parent_el in g.default_slot.children), None)
        if grid is None:
            title = pr.species_names.get(st.species_id or "", st.species_id) if st.by_species else None
            grid = next((g for t, g in self._grids if t == title), self._grids[0][1])
        others = [c for c in grid.default_slot.children if c is not self._new_box]
        index = others.index(parent_el) + 1 if parent_el in others else 0
        fresh = self._new_box is None or self._new_box.is_deleted
        if fresh:
            with grid:
                self._new_box = ui.element("div").classes("cb-bm-node large new")
                self._new_box.props["data-nid"] = NEW
            self._new_sig, self._new_at = None, None
        if self._new_at != (id(grid), index):
            self._new_box.move(target_container=grid, target_index=index)
            self._new_at = (id(grid), index)
        if fresh:
            self._js("cbBm.mount()")

    # ── what the browser walks: the nodes' words and menus, the edges ──
    def _graph_json(self, pr: Project, shared: list[str], resolved: list[Resolved], anchor) -> str:
        nodes: dict[str, dict] = {}

        def plus_of(j: Job) -> list[list]:
            rows = []
            for kind, slot, why in pr.offered_after(j):
                words = why
                if slot is not None:
                    others = [s for s in pr.slots_of(kind) if s.key != slot.key and not s.bundle and s.required]
                    others = [s for s in others if not s.quiet]
                    words = f"as its {slot.label}" + (
                        f" · also needs {', '.join(s.label for s in others)}" if others else ""
                    )
                rows.append(
                    [kind, pr.names.get(kind, kind), pr.icon.get(kind, "add"), slot.key if slot else None, words]
                )
            return rows

        for j in pr.jobs:
            if j.id in shared:
                continue
            name = pr.names.get(j.kind, j.kind)
            sp = pr.species_names.get(j.species or "", j.species or "")
            main = j.inputs[0] if j.inputs else None
            src = main.value if main is not None and main.value and not reads_from_job(pr, j) else ""
            nodes[j.id] = {
                "name": name,
                "num": j.number,
                "kind": j.kind.lower(),
                "status": j.status,
                "sp": sp,
                "first": (_words(name) or [""])[0],
                "nw": _words(name, j.kind, _ALIASES.get(j.kind, "")),
                "meta": _words(sp, j.status, pr.method_of(j), " ".join(pr.chips(j))),
                "src": _words(src.removeprefix("set ")),
                "chips": " · ".join(pr.chips(j)),
                "ident": pr.ident(j),
                "plus": plus_of(j),
                "clone": j.kind not in _NO_CLONE,
                "remove": j.on_roster and j.kind != "tsImport",
            }
        alias = {}
        if shared:
            last = pr.by_id[shared[-1]]
            names = [pr.names.get(pr.by_id[t].kind, t) for t in shared]
            nodes[TRUNK] = {
                "name": "Shared start",
                "num": "",
                "kind": "trunk",
                "status": "done",
                "sp": "",
                "first": "shared",
                "nw": _words("shared start", *names, *(_ALIASES.get(pr.by_id[t].kind, "") for t in shared)),
                "meta": [],
                "src": [],
                "chips": " → ".join(names),
                "ident": "the shared start",
                "job": shared[-1],
                "plusFrom": shared[-1],
                "plus": plus_of(last),
                "clone": False,
                "remove": False,
            }
            alias = {t: TRUNK for t in shared}
        if self.st.adding:
            nodes[NEW] = {"name": "+ " + pr.names.get(self.st.adding, self.st.adding), "virtual": True}
        edges = []
        for j in pr.jobs:
            if j.id in shared:
                continue
            main_src = j.inputs[0].source if j.inputs else None
            for i, r in enumerate(j.inputs):
                if r.source is None or r.source not in pr.by_id or r.source == j.id or r.slot.quiet:
                    continue
                if i > 0 and r.source == main_src:
                    continue  # rides the main cable
                edges.append({"a": anchor(r.source), "b": j.id, "k": "main" if i == 0 else "side", "s": r.slot.key})
        if self.st.adding:
            main_src = resolved[0].source if resolved else None
            for i, r in enumerate(resolved):
                if r.source is None or r.source not in pr.by_id or r.slot.quiet or (i > 0 and r.source == main_src):
                    continue
                edges.append(
                    {
                        "a": anchor(r.source),
                        "b": NEW,
                        "k": "main" if i == 0 else "side",
                        "s": r.slot.key,
                        "n": 1,
                        "d": 0 if r.state == "explicit" else 1,
                    }
                )
        g: dict[str, Any] = {"nodes": nodes, "edges": edges, "alias": alias}
        if self.st.strips and pr.ts_ids:
            g["ts"] = pr.ts_ids
            g["tst"] = {t: pr.ts_titles[t] for t in pr.ts_ids if pr.ts_titles.get(t, t) != t}
        return json.dumps(g, separators=(",", ":"))

    # ── choosing what an input reads ──
    def _send_slot(self, pr: Project, state, resolved: list[Resolved], anchor) -> None:
        """The open input's sources to the browser's palette, when they moved."""
        payload = self._slot_payload(pr, state, resolved, anchor)
        sent = json.dumps(payload, separators=(",", ":"))
        if sent != self._slot:
            self._slot = sent
            self._js(f"cbBm.setSlot({sent})")

    def _slot_payload(self, pr: Project, state, resolved: list[Resolved], anchor) -> dict | None:
        """The open input: whose, which, what it reads now, and every job it could read (each with what tells it
        from the others). An input that is no longer there closes."""
        st = self.st
        if st.open_slot is None:
            return None
        nid, key = st.open_slot
        if nid == NEW and st.adding:
            r = next((x for x in resolved if x.slot.key == key), None)
            if r is None:
                st.open_slot = None
                return None
            cands, current, label = r.candidates, r.source, r.slot.label
            job = "the new " + pr.names.get(st.adding, st.adding)
        else:
            j = pr.by_id.get(nid)
            row = next((x for x in j.inputs if x.slot.key == key), None) if j is not None else None
            if row is None or j.status != "scheduled" or j.frozen:
                st.open_slot = None
                return None
            cands, self._cand_keys = _candidates_of(self, pr, state, nid, key)
            current, label, job = row.source, row.slot.label, pr.ident(j)

        def words(c: Job) -> str:
            parts = list(pr.pedigree(c, cands))
            if pr.ts_ids:
                parts.append(pr.ts_text(c)[0])
            return " · ".join(p for p in parts if p)

        return {
            "nid": nid,
            "key": key,
            "label": label,
            "job": job,
            "current": current,
            "cands": [
                {
                    "id": anchor(c.id),
                    "src": c.id,
                    "name": pr.names.get(c.kind, c.kind),
                    "num": c.number,
                    "status": c.status,
                    "t": words(c),
                }
                for c in cands
                if anchor(c.id) != nid
            ],
        }

    def _next_open(self) -> tuple[str, str] | None:
        pr = self._pr
        if pr is None:
            return None
        key = next((r.slot.key for r in resolve(pr, self.st) if r.state in ("ambiguous", "missing")), None)
        return (NEW, key) if key else None

    def _open_slot(self, nid: str, key: str) -> None:
        self.st.open_slot = None if self.st.open_slot == (nid, key) else (nid, key)
        self.redraw()

    def _choose(self, nid: str, key: str, src: str) -> None:
        pr, st = self._pr, self.st
        if pr is None or src not in pr.by_id:
            return
        if nid == NEW:
            resolved = resolve(pr, st)
            st.picks[key] = src
            if resolved and key == resolved[0].slot.key:
                st.picks = {key: src}  # a new main choice lets the other inputs follow it again
            st.open_slot = self._next_open()
            self.redraw()
            return
        source_key = self._cand_keys.get(src)
        if source_key is None:
            ui.notify(f"{pr.ident(pr.by_id[src])} is not a source this input can read.", type="warning")
            return
        state = get_project_state_for(self.project_path)
        res = set_input(state, nid, key, source=source_key)
        if not res["success"]:
            ui.notify(res["error"], type="negative", timeout=8000)
            return
        asyncio.create_task(self.panel.backend.save_project(self.project_path))
        st.open_slot = None
        self.redraw()

    # ── the job being added ──
    def _draw_new(self, pr: Project, state, resolved: list[Resolved]) -> None:
        box = self._new_box
        st = self.st
        if box is None or box.is_deleted:
            return
        # Which input is open is painted by the browser: choosing one does not draw the form again.
        sig = (
            st.adding,
            st.method,
            st.species_id,
            tuple(sorted(st.picks.items())),
            st.params_rev,
            tuple((r.slot.key, r.state, r.source, len(r.candidates)) for r in resolved),
        )
        if sig == self._new_sig:
            return
        self._new_sig = sig
        new_defaults: dict = {}
        if st.species_id:
            species = state.get_species(st.species_id)
            if species is not None:
                new_defaults = dict(species_defaults(pr.vocab.job_types[st.adding], species))

        def set_param(k: str, v) -> None:
            st.params[k] = v  # the field holds the typed value; no redraw follows

        box.clear()
        with box:
            render_new_card(
                pr,
                spec_of_new(pr, st, resolved),
                sections=new_sections(pr, st, on_param=set_param, new_defaults=new_defaults),
                on_socket=lambda k: self._open_slot(NEW, k),
                on_method=self._method_menu,
                on_discard=lambda _e: self._discard(),
                on_add=lambda _e: asyncio.create_task(_add_new_job(self, pr, state, resolve(pr, st))),
                todo=[r.slot.label for r in unsettled(resolved)],
            )

    def _method_menu(self, tokel: ui.element) -> None:
        pr, st = self._pr, self.st
        with tokel:
            menu = (
                ui.menu()
                .props('no-parent-event anchor="bottom left" self="top left"')
                .style(MENU_STYLE + " min-width: 220px;")
            )
            with menu, ui.column().style("gap: 0; padding: 6px 0;"):
                text("Run with", 9, SOFT, bold=True, extra="padding: 3px 12px 6px;")
                for value, words in pr.methods.get(st.adding, ()):
                    row = (
                        ui.element("div")
                        .classes("cb-bm-menu-row")
                        .style("display: flex; align-items: center; gap: 8px; padding: 5px 12px; cursor: pointer;")
                    )
                    with row:
                        text(words, 10, INK, bold=value == st.method, extra="width: 90px;")
                        text(pr.gloss.get(words, ""), 9, SOFT, extra="max-width: 200px; white-space: normal;")
                    row.on("click", lambda _e, v=value, mn=menu: (mn.close(), self._set_method(v)))
        tokel.on("click.stop", lambda _e, m=menu: m.open())

    def _set_method(self, method: str) -> None:
        pr, st = self._pr, self.st
        st.method = method
        st.picks = {k: v for k, v in st.picks.items() if k in {s.key for s in pr.slots_of(st.adding, method)}}
        st.open_slot = self._next_open()
        self.redraw()

    async def _start(self, kind: str, parent: str, slot_key: str) -> None:
        pr = self._pr
        if pr is None or kind not in pr.vocab.job_types or parent not in pr.by_id:
            return
        jt = pr.vocab.job_types[kind]
        species_id = None
        if jt in PHASE_JOBS[PHASE_PARTICLES]:
            species_id = await self.panel.choose_species(jt)
            if species_id is None:
                return
        st = self.st
        st.adding, st.method, st.picks, st.params = (
            kind,
            pr.methods.get(kind, ((None, ""),))[0][0],
            {slot_key: parent},
            {},
        )
        st.species_id = species_id
        st.params_rev += 1
        st.open_slot = self._next_open()
        self.redraw()

    def _discard(self) -> None:
        st = self.st
        st.adding, st.open_slot, st.species_id = None, None, None
        st.picks.clear()
        st.params.clear()
        self.redraw()

    # ── the flags on the Pipeline title line ──
    def _head_slots(self) -> tuple[ui.element, ui.element] | None:
        """The map's two boxes on the pipeline frame's title line (after the name, at the right end), made once."""
        if self._lead is None:
            view_head = self.callbacks.get("view_head")
            if view_head is None:
                return None
            head = view_head("pipeline")
            with head["lead"]:
                self._lead = ui.element("div").style("display: flex; align-items: center; gap: 4px; min-width: 0;")
            with head["tools"]:
                self._tools = ui.element("div").classes("cb-head-tools")
            self._lead.set_visibility(self.visible)
            self._tools.set_visibility(self.visible)
        return self._lead, self._tools

    def _render_head(self, pr: Project) -> None:
        """The picked tilt series after the title; at the right end the tilt-series picker, the cards' size, the
        magazines, the folded start, and the check that bands the map by species."""
        slots = self._head_slots()
        if slots is None:
            return
        st = self.st
        n_species = len({j.species for j in pr.jobs if j.species})
        foldable = len(pr.trunk()) >= 2
        sig = (
            len(pr.ts_ids),
            bool(pr.jobs),
            st.active_ts,
            st.all_large,
            bool(st.large),
            st.strips,
            st.trunk,
            st.by_species,
            n_species,
            foldable,
        )
        if sig == self._head_sig:
            return
        self._head_sig = sig
        lead, tools = slots
        lead.clear()
        tools.clear()
        with lead:
            if st.active_ts:
                render_ts_chip(pr, st.active_ts, lambda: self.pick(None))
        if not pr.jobs:
            return
        with tools:
            if pr.ts_ids:
                ts_btn = icon_button(
                    "view_module",
                    None,
                    tooltip=f"Tilt series ({len(pr.ts_ids)}): pick one to ring it "
                    "in every magazine and turn the large cards to it",
                )
                if st.active_ts:
                    ts_btn.classes("cb-flag-on")
                with ts_btn:
                    menu = (
                        ui.menu()
                        .props('anchor="bottom right" self="top right"')
                        .style(MENU_STYLE + " width: 460px; max-width: 80vw;")
                    )
                    with menu:
                        body = ui.element("div").style("padding: 6px 10px; max-height: 320px; overflow-y: auto;")

                    def fill(e, body=body, menu=menu) -> None:
                        # Built when opened: one chip per tilt series with its trail, as the jobs stand now.
                        if not e.value or self._pr is None:
                            return
                        body.clear()
                        with body:
                            text(f"Tilt series · {len(self._pr.ts_ids)}", 9, SOFT, bold=True)
                            picker_body(self._pr, self.st, lambda ts: (menu.close(), self.pick(ts)))

                    menu.on_value_change(fill)
                ui.element("div").style(f"width: 1px; height: 14px; background: {HAIR}; margin: 0 3px;")
            any_large = st.all_large or bool(st.large)
            size = icon_button(
                "unfold_less" if any_large else "unfold_more",
                lambda: self._set_size(not any_large),
                tooltip="Every card small" if any_large else "Every card large: parameters and results on the card",
            )
            if st.all_large:
                size.classes("cb-flag-on")
            if pr.ts_ids:
                mags = icon_button(
                    "view_week",
                    lambda: self._flip("strips"),
                    tooltip=("Magazines on: " if st.strips else "Magazines off: ")
                    + "one mark per tilt series above every card",
                )
                if st.strips:
                    mags.classes("cb-flag-on")
            if foldable:
                trunk = icon_button(
                    "compress",
                    lambda: self._flip("trunk"),
                    tooltip="The shared start of every chain "
                    + ("is folded into one card: unfold it" if st.trunk else "is unfolded: fold it into one card"),
                )
                if st.trunk:
                    trunk.classes("cb-flag-on")
            ui.element("div").style(f"width: 1px; height: 14px; background: {HAIR}; margin: 0 6px 0 3px;")
            house_check(
                "by species",
                value=st.by_species,
                on_change=lambda e: self._set_by_species(bool(e.value)),
                disabled=n_species < 2,
                tooltip="A band per species, each its own grid"
                if n_species > 1
                else "One species or none in this project: nothing to group",
            )

    def _set_size(self, large: bool) -> None:
        self.st.all_large = large
        if not large:
            self.st.large.clear()
        self.persist()
        self.redraw()

    def _flip(self, attr: str) -> None:
        setattr(self.st, attr, not getattr(self.st, attr))
        self.persist()
        self.redraw()

    def _set_by_species(self, on: bool) -> None:
        if self.st.by_species == on:
            return
        self.st.by_species = on
        self.persist()
        self.redraw()

    def _toggle_size(self, nid: str) -> None:
        st = self.st
        if self._pr is None or nid not in self._pr.by_id:
            return
        if st.all_large:
            st.all_large = False
            st.large = {j.id for j in self._pr.jobs} - {nid}
        else:
            st.large ^= {nid}
        self.persist()
        self.redraw()

    # ── reading off the loop ──
    def _load_sets(self, state) -> None:
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

    def _request_facts(self, pr: Project, state, large: list[str]) -> None:
        """Read the facts a large card needs off the loop, once per job status (a running job's again every
        while); the card is drawn again when they land."""
        store = self.store
        if not large:
            return
        try:
            registry = get_registry_for(self.project_path)
            series = list(registry.all_tilt_series())
            stamp = (registry.loaded_stamp, len(series))
        except Exception:  # no registry yet
            logger.exception("branch map: the registry of %s", self.project_path)
            return
        by_id = {ts.id: ts for ts in series}
        for nid in large:
            j = pr.by_id[nid]
            jm = state.jobs.get(nid)
            if jm is None or j.status not in ("done", "failed", "running"):
                continue
            if store.job_stale(j) and not store.loading(("job", j.id)):
                kick(store.load_job(j, jm, series, stamp, pr.ts_ids), self.refresh)
            ts = self.st.active_ts
            if ts and store.ts_stale(j, ts) and not store.loading(("ts", j.id, ts)):
                kick(store.load_ts(j, jm, by_id.get(ts), ts), self.refresh)

    # ── the browser's events ──
    def ours(self, e) -> bool:
        return e.args.get("map", "jobs") == self.MAP

    def on_click(self, e) -> None:
        """A click on a card: selects its job, or opens its page when it is selected; on the paper: no job."""
        if not self.ours(e):
            return
        nid = e.args.get("nid")
        if nid is None:
            self.panel.select(None)
        elif self._pr is not None and nid in self._pr.by_id:
            self.panel.activate(nid)

    def on_select(self, e) -> None:
        """The arrow keys, find's ↵, Esc: select (or let go) without opening."""
        if not self.ours(e):
            return
        nid = e.args.get("nid")
        if nid is None or (self._pr is not None and nid in self._pr.by_id):
            self.panel.select(nid)

    def on_size(self, e) -> None:
        if self.ours(e):
            self._toggle_size(e.args.get("nid"))

    def on_mark(self, e) -> None:
        """A click on a magazine's mark."""
        if not self.ours(e):
            return
        ts = e.args.get("ts")
        if self._pr is not None and ts in self._pr.ts_ids:
            self.pick(None if ts == self.st.active_ts else ts)

    def on_start(self, e) -> None:
        if self.ours(e):
            asyncio.create_task(self._start(e.args.get("kind"), e.args.get("parent"), e.args.get("slot")))

    def on_job(self, e) -> None:
        """The number's menu: open, clone or remove the job."""
        if not self.ours(e) or self._pr is None:
            return
        j = self._pr.by_id.get(e.args.get("nid"))
        if j is None:
            return
        act = e.args.get("act")
        if act == "open":
            self.panel.switch_tab(j.id)
        elif act == "clone" and j.kind not in _NO_CLONE:
            self.panel.add_instance_to_pipeline(j.job_type, species_id=j.species, clone_of=j.id, open_page=False)
        elif act == "remove" and j.on_roster:
            self.panel.roster._on_remove_click(j.id)

    def on_slot(self, e) -> None:
        if self.ours(e):
            self._open_slot(e.args.get("nid"), e.args.get("key"))

    def on_choose(self, e) -> None:
        if self.ours(e):
            self._choose(e.args.get("nid"), e.args.get("key"), e.args.get("src"))

    def on_slot_close(self, e) -> None:
        if self.ours(e) and self.st.open_slot is not None:
            self.st.open_slot = None
            self._slot = "null"  # the browser closed it already
            self.redraw()

    def on_trunk(self, e) -> None:
        if self.ours(e):
            self.st.trunk = False
            self.persist()
            self.redraw()


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
            f"background: {PAPER};"
        )
    ):
        ui.html(f'<pre class="cb-bm-ascii">{lines}</pre>', sanitize=False)
        with ui.column().style("gap: 6px; max-width: 220px;"):
            ui.html('<div class="cb-bm-prompt"><span class="p">~&gt;</span> no jobs yet</div>', sanitize=False)
            text("Add the first job from the roster's rows; every job you add appears here as a card.", 10, SOFT)


# ── the input's candidates, and adding the new job ───────────────────────────


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
    view.redraw()


# ── mounting ─────────────────────────────────────────────────────────────────


def mount_branch_map(container, panel, callbacks: dict[str, Callable]) -> BranchMapView:
    """Build the map into `container` (the pipeline frame's body beside the job pages) and keep it current while
    the Jobs view shows it: its timer is a `view_scope("pipeline")` timer, so a hidden Jobs view does nothing."""
    ui.run_javascript(MAP_JS)
    view = BranchMapView(container, panel, callbacks)
    for name, handler in (
        ("cb-bm-click", view.on_click),
        ("cb-bm-select", view.on_select),
        ("cb-bm-size", view.on_size),
        ("cb-bm-ts", view.on_mark),
        ("cb-bm-start", view.on_start),
        ("cb-bm-job", view.on_job),
        ("cb-bm-slot", view.on_slot),
        ("cb-bm-choose", view.on_choose),
        ("cb-bm-slot-close", view.on_slot_close),
        ("cb-bm-trunk", view.on_trunk),
    ):
        ui.on(name, handler)
    panel.ui_mgr.view_scope("pipeline").timer(_TICK_S, view.tick, container)
    view.refresh()
    view.paint_selected()
    return view


__all__ = ["BranchMapView", "mount_branch_map"]
