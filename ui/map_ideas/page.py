"""`/dev/map-ideas`: the job map top-down, a job's lineage, and a find bar, on a real project read and never written.

Layout   grid (the default) · tree · lanes · layers (ui/map_ideas/layouts.py); nothing is dragged, the browser lays
         the cards out; grouping by species is the user's own check on the Pipeline view's title line
Lineage  dim the rest · up and down in two inks · trace
Find     spotlight · palette · dock: `f` (or `/`) while the Jobs view has the focus

`?project=<name or path>` (default AGG412_STA), `?layout=`, `?lineage=`, `?find=`, `?group=species`, `?large=all`,
`?paper=dots` (the dragging map's dotted paper, to compare what it costs).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from html import escape
from urllib.parse import urlencode

from nicegui import Client, ui

from ui.components.check import house_check
from ui.map_ideas.assets import CSS, JS
from ui.map_ideas.cards import card_html, roster_html, source_html
from ui.map_ideas.data import DEFAULT_PROJECT, MapData, graph_json, load
from ui.map_ideas.layouts import LAYOUTS

logger = logging.getLogger(__name__)

LAYOUT_WORDS = {
    "grid": (
        "Grid",
        "Every card in the order the jobs were made, wrapping at the view's width: nothing to scroll sideways, ever. "
        "No cables at rest; select a job and its lineage's cables are drawn across the grid.",
    ),
    "tree": (
        "Tree",
        "Each chain runs down its own column, a parent centred over what reads it. Siblings that end there, the same "
        "job type run several ways, stack into one column instead of spreading sideways. A set or a map file that "
        "several jobs read is a pill at the top of their tree; one read by a single job rides on its card as a tab.",
    ),
    "lanes": (
        "Lanes",
        "The tree pinned left: the deepest chain runs straight down under its parent, every other branch steps "
        "right, and the jobs that end at a job (its PostProcesses, a Class 3D tried once) hang beside it. A long "
        "chain costs height, never width.",
    ),
    "layers": (
        "Layers",
        "A row per generation: a job sits one row below the deepest job it reads, main input or side input. Rows "
        "wrap at the view's width, so the map grows down, not sideways.",
    ),
}
LINEAGE_WORDS = {
    "dim": ("Dim the rest", "The selected job ringed, its lineage kept, everything else faded."),
    "updown": (
        "Up and down",
        "What it was made from ringed in ink and tagged ↑, what was made from it dashed in blue and tagged ↓; "
        "the rest half faded.",
    ),
    "trace": ("Trace", "Nothing fades: the lineage's cards get a blue edge and its cables march."),
}
FIND_WORDS = {
    "spotlight": (
        "Spotlight",
        "The bar alone, over the middle of the view; matches light up as you type. ↵ walks them.",
    ),
    "palette": ("Palette", "The bar with the matches listed under it; ↵ selects one and shows its lineage."),
    "dock": ("Dock", "A small bar at the view's corner that stays while you click around; the matches stay lit."),
}
# Today's map on AGG412_STA (30 jobs), measured 2026-11-19 in a headless Chromium against the running server.
TODAY = [
    ("open the map, 30 small cards", "2,692 elements · 511 KB · 0.52 s server"),
    ("one card large", "the whole map again: 600 KB · 0.17 s"),
    ("every card large", "5,549 elements · 4.1 MB · 0.72 s server · 2.7 s to settle"),
    ("select a card", "0.9 KB · painted in place"),
]


@dataclass
class State:
    layout: str = "grid"
    lin: str = "dim"
    find: str = "spotlight"
    group: str = "none"
    large: set[str] = field(default_factory=set)


def _seg(name: str, options: dict[str, tuple[str, str]], current: str, on_pick) -> None:
    with ui.element("div").classes("cbmi-ctl"):
        ui.label(name).classes("cbmi-ctl-name")
        box = ui.element("div").classes("cbmi-seg")
        spans: dict[str, ui.label] = {}
        with box:
            for key, (label, words) in options.items():
                el = ui.label(label).classes("on" if key == current else "")
                el.props["title"] = words
                spans[key] = el

        def pick(key: str) -> None:
            for k, el in spans.items():
                el.classes(add="on") if k == key else el.classes(remove="on")
            on_pick(key)

        for key, el in spans.items():
            el.on("click", lambda _e, k=key: pick(k))


async def render_map_ideas(client: Client) -> None:
    q = client.request.query_params
    st = State(
        layout=q.get("layout") if q.get("layout") in LAYOUT_WORDS else "grid",
        lin=q.get("lineage") if q.get("lineage") in LINEAGE_WORDS else "dim",
        find=q.get("find") if q.get("find") in FIND_WORDS else "spotlight",
        group="species" if q.get("group") == "species" else "none",
    )
    project = q.get("project") or DEFAULT_PROJECT
    dots = q.get("paper") == "dots"
    ui.add_css(CSS)
    ui.run_javascript(JS)
    ui.query("body").style("background: #fff;")
    page = ui.element("div").classes("cbmi-page")
    with page:
        loading = ui.label(f"Reading {project} …").classes("cbmi-meta")
    try:
        d: MapData = await load(project)
    except Exception as e:
        logger.exception("map ideas: loading %s", project)
        loading.set_text(f"Could not read {project}: {e}")
        return
    loading.delete()
    if q.get("large") == "all":
        st.large = {j.id for j in d.pr.jobs}
    pr = d.pr
    cards: dict[str, ui.html] = {}

    def url_state() -> None:
        params = {"project": project, "layout": st.layout, "lineage": st.lin, "find": st.find, "group": st.group}
        if dots:
            params["paper"] = "dots"
        ui.run_javascript(f"history.replaceState(null, '', '?{urlencode(params)}')")

    def note_html() -> str:
        lay, lin, fnd = LAYOUT_WORDS[st.layout], LINEAGE_WORDS[st.lin], FIND_WORDS[st.find]
        return (
            f"<b>{lay[0]}.</b> {escape(lay[1])}<br><b>Lineage: {lin[0]}.</b> {escape(lin[1])} "
            f"<b>Find: {fnd[0]}.</b> {escape(fnd[1])}"
        )

    with page:
        ui.label("The map, top-down: layouts, lineage, find").classes("cbmi-h1")
        n_sets = sum(1 for s in d.sources.values() if s.kind == "set")
        ui.label(
            f"{pr.name} · {len(pr.jobs)} jobs · {len(pr.ts_ids)} tilt series · {n_sets} particle sets read · "
            "read only: nothing here is written"
        ).classes("cbmi-meta")
        with ui.element("div").classes("cbmi-controls"):
            _seg("layout", LAYOUT_WORDS, st.layout, lambda k: (setattr(st, "layout", k), rebuild()))
            _seg("lineage", LINEAGE_WORDS, st.lin, lambda k: (setattr(st, "lin", k), client_mode("lin", k)))
            _seg("find", FIND_WORDS, st.find, lambda k: (setattr(st, "find", k), client_mode("find", k)))
            _seg(
                "side inputs",
                {"all": ("Drawn", "Every side input's cable, faint"), "sel": ("On select", "Only the lineage's")},
                "all",
                lambda k: client_mode("side", k),
            )
            _seg(
                "cards",
                {"small": ("Small", "Every card small"), "large": ("Large", "Every card large")},
                "large" if st.large else "small",
                lambda k: asyncio.create_task(set_all(k == "large")),
            )
            _seg(
                "width",
                {"natural": ("Natural", "Cards at their size; scroll"), "fit": ("Fit", "Shrink to the view's width")},
                "natural",
                lambda k: ui.run_javascript(f"cbmi.set('fit', {json.dumps(k == 'fit')})"),
            )
        notes = ui.html(note_html(), sanitize=False).classes("cbmi-note")

        frame = ui.element("div").classes("cbmi-frame")
        frame.props["data-cb-focus"] = ""
        with frame:
            with ui.element("div").classes("cbmi-panel cbmi-roster"):
                with ui.element("div").classes("cbmi-panel-head"):
                    ui.label("Jobs").classes("cbmi-panel-title")
                    ui.label(f"{sum(1 for j in pr.jobs if j.on_roster)}").classes("cbmi-headtext")
                with ui.element("div").classes("cbmi-rlist-wrap"):
                    ui.html(roster_html(d), sanitize=False)
            with ui.element("div").classes("cbmi-panel cbmi-pipe"):
                with ui.element("div").classes("cbmi-panel-head"):
                    ui.label("Pipeline").classes("cbmi-panel-title")
                    chip = '<span id="cbmi-linchip" class="cbmi-headtext"></span>'
                    ui.html(chip, sanitize=False).style("min-width: 0;")
                    ui.element("div").style("flex: 1 1 0;")
                    built = ui.label("").classes("cbmi-headtext")
                    n_species = len({j.species for j in pr.jobs if j.species})
                    house_check(
                        "by species",
                        value=st.group == "species",
                        on_change=lambda e: (setattr(st, "group", "species" if e.value else "none"), rebuild()),
                        disabled=n_species < 2,
                        tooltip="A band per species, its jobs under it"
                        if n_species > 1
                        else "One species in this project: nothing to group",
                    )
                    ui.html('<span id="cbmi-live" class="cbmi-headtext"></span>', sanitize=False)
                with ui.element("div").classes("cbmi-scroll" + (" cbmi-dots" if dots else "")):
                    world = ui.element("div").classes("cbmi-world")

        ui.html(
            "Click a card or a roster row: its lineage. Double-click: its job page, in the app. ↑ ↓ walk to what it "
            "read and what read it, ← → to its siblings. <b>f</b> or <b>/</b> while the Jobs view has the focus (click "
            "in it first): find. Esc clears. The expand icon on a card's header makes it large; the others move.",
            sanitize=False,
        ).classes("cbmi-note")
        rows = "".join(f"<tr><td>{escape(a)}</td><td>{escape(b)}</td></tr>" for a, b in TODAY)
        ui.html(
            "<table><tr><th>today's map on AGG412_STA</th><th></th></tr>" + rows + "</table>"
            "<div>This page: one element per card; what a click changes is one card's update (see the Pipeline title "
            "line); select, lineage and find cost the server nothing.</div>",
            sanitize=False,
        ).classes("cbmi-perf")

    def node(nid: str) -> None:
        if nid in d.sources:
            el = ui.html(source_html(d.sources[nid]), sanitize=False).classes("cbmi-node cbmi-srcnode")
        else:
            large = nid in st.large
            j = d.job(nid)
            el = ui.html(card_html(d, j, large=large, facts=d.store.job_facts(j)), sanitize=False)
            el.classes("cbmi-node" + (" large" if large else ""))
        el.props["data-nid"] = nid
        cards[nid] = el

    def rebuild() -> None:
        t0 = time.perf_counter()
        world.clear()
        cards.clear()
        with world:
            ui.html('<svg class="cbmi-svg"></svg>', sanitize=False).classes("cbmi-cables")
            LAYOUTS[st.layout](d, node, by_species=st.group == "species")
        if st.layout == "grid":
            world.classes(add="cbmi-grid-wrap")
        else:
            world.classes(remove="cbmi-grid-wrap")
        ms = (time.perf_counter() - t0) * 1000
        kb = sum(len(el.content) for el in cards.values()) / 1024
        built.set_text(f"{len(cards)} cards · {len(client.elements)} elements · built in {ms:.0f} ms · {kb:.0f} KB")
        notes.set_content(note_html())
        url_state()
        ui.run_javascript("cbmi.mount()")

    def client_mode(key: str, value: str) -> None:
        ui.run_javascript(f"cbmi.set({json.dumps(key)}, {json.dumps(value)})")
        if key in ("lin", "find"):
            notes.set_content(note_html())
            url_state()

    def redraw(nid: str) -> None:
        el = cards.get(nid)
        if el is None or el.is_deleted or nid in d.sources:
            return
        large = nid in st.large
        if large:
            el.classes(add="large")
        else:
            el.classes(remove="large")
        j = d.job(nid)
        el.set_content(card_html(d, j, large=large, facts=d.store.job_facts(j)))

    async def read_facts(nid: str) -> None:
        """A large card's results, read in a thread once; the card is drawn again when they land."""
        j = d.job(nid)
        jm = d.state.jobs.get(nid)
        if jm is None or j.status not in ("done", "failed", "running"):
            return
        if not d.store.job_stale(j) or d.store.loading(("job", nid)):
            return
        await d.store.load_job(j, jm, d.series, d.stamp, pr.ts_ids)
        if nid in st.large:
            redraw(nid)

    async def on_size(e) -> None:
        nid = e.args.get("nid")
        if nid not in cards or nid in d.sources:
            return
        if nid in st.large:
            st.large.discard(nid)
        else:
            st.large.add(nid)
        redraw(nid)
        if nid in st.large:
            await read_facts(nid)

    async def set_all(large: bool) -> None:
        ids = [j.id for j in pr.jobs if j.id in cards]
        st.large = set(ids) if large else set()
        for nid in ids:
            redraw(nid)
        if large:
            await asyncio.gather(*(read_facts(n) for n in ids))

    def on_open(e) -> None:
        nid = e.args.get("nid")
        if nid in d.sources:
            ui.notify(f"{d.sources[nid].label}: {d.sources[nid].detail}")
        elif nid in pr.by_id:
            ui.notify(f"{pr.ident(pr.by_id[nid])}: in the app, this opens its job page.")

    ui.on("cbmi_size", on_size)
    ui.on("cbmi_open", on_open)
    rebuild()
    ui.run_javascript(
        f"cbmi.setGraph({graph_json(d)}); cbmi.set('lin', {json.dumps(st.lin)}); "
        f"cbmi.set('find', {json.dumps(st.find)}); cbmi.hookPerf();"
    )
    if st.large:
        await asyncio.gather(*(read_facts(n) for n in list(st.large)))
