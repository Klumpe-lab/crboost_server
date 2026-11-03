"""Six directions for a job's inputs once jobs are nodes of a graphical builder: a Class 3D just added to a mature
project (dozens of jobs, two species, several kitchen sets), choosing its particles, their images and a reference.

Static mock data only (some jobs here do not exist). One switch at the top sets the case for every design: every
project has one matching export; 412_Slot6 has two matching exports; 412_Grid3's export does not match the others.
Clicks change only this page.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from nicegui import ui

from ui.components.buttons import house_button
from ui.io_layouts_preview import CASE_TITLES, CASES, _projects, _resolve
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE

FONT = "font-family: 'IBM Plex Sans', sans-serif;"
MONO = "font-family: 'IBM Plex Mono', monospace;"
INK = "#0f172a"
BODY = "#334155"
MUTED = "#94a3b8"
SOFT = "#64748b"
LINE = "#e2e8f0"
BLUE = "#2563eb"
GREEN = "#10b981"
AMBER = "#d97706"
RED = "#ef4444"
SPECIES = {"412": ("412 capsid", "#0ea5e9"), "ribo": ("ribosome", "#a855f7")}
JOB_SPECIES = "412"
PROJECT_HUES = ["#1e3a5f", "#2f5a85", "#4677a8", "#6493c4", "#86afd8", "#a9c8e8", "#cbdff2"]

PARTICLES = [
    {
        "id": "s002",
        "kind": "set",
        "title": "s002 · 412_and_other_junk",
        "species": "412",
        "picks": 2708,
        "counts": [1210, 402, 318, 296, 151, 214, 117],
        "sub": "kitchen set · 7 projects",
        "origin": "the 412 draft of Oct 29 · 3 filters",
    },
    {
        "id": "s003",
        "kind": "set",
        "title": "s003 · class 1",
        "species": "412",
        "picks": 1102,
        "counts": [520, 160, 120, 110, 60, 90, 42],
        "sub": "kitchen set · 7 projects",
        "origin": "subset class 1 of group 412 · classes",
    },
    {
        "id": "job044",
        "kind": "job",
        "title": "Warp export #3 · job044",
        "species": "412",
        "picks": 2886,
        "counts": [2886],
        "sub": "job output · MyrGFP_Grid2 only",
        "origin": "every candidate of job043, unfiltered",
    },
    {
        "id": "s004",
        "kind": "set",
        "title": "s004 · ribo_good",
        "species": "ribo",
        "picks": 5410,
        "counts": [3900, 1510],
        "sub": "kitchen set · 2 projects",
        "origin": "the ribosome draft of Oct 21",
    },
]
REFS = [
    {
        "id": "job048",
        "title": "Reconstruct particle · job048",
        "species": "412",
        "sub": "from s001 · 2,708 picks",
        "facts": "box 256 · 3.1 Å",
    },
    {
        "id": "job052",
        "title": "Class 3D #1 · class 2 · job052",
        "species": "412",
        "sub": "from s002 · 940 particles",
        "facts": "box 256 · 3.1 Å",
    },
    {
        "id": "job017",
        "title": "Reconstruct particle · job017",
        "species": "ribo",
        "sub": "from s004 · 5,410 picks",
        "facts": "box 96 · 4.2 Å",
    },
]


@dataclass
class State:
    """One design's choices on this page. `kind` and `picks` are what `_resolve` reads."""

    kind: str
    particles: str = "s002"
    reference: str = "job048"
    active: str = "particles"
    picks: dict[str, str] = field(default_factory=dict)
    extra: dict = field(default_factory=dict)


def _particles(st: State) -> dict:
    return next(p for p in PARTICLES if p["id"] == st.particles)


def _ref(st: State) -> dict:
    return next(r for r in REFS if r["id"] == st.reference)


@dataclass
class Facets:
    """What the chosen particles give this job: `status` is ready / choose / differs / as-written / species."""

    status: str
    images: int
    picks: int
    dropped: int
    ask: list
    odd: list
    box: int
    apix: float
    chosen: dict


def _facets(st: State) -> Facets:
    p = _particles(st)
    res = _resolve(st)
    scale = p["picks"] / 2708
    images = round(res.images * scale)
    dropped = max(p["picks"] - images - round(sum(x.picks for x in res.ask) * scale), 0)
    if p["kind"] == "job":
        status = "as-written"
    elif p["species"] != JOB_SPECIES:
        status = "species"
    elif res.odd:
        status = "differs"
    elif res.ask:
        status = "choose"
    else:
        status = "ready"
    return Facets(status, images, p["picks"], dropped, res.ask, res.odd, res.box, res.apix, res.chosen)


# ── small parts ──────────────────────────────────────────────────────────────


def _t(text: str, size: int = 11, color: str = BODY, *, bold: bool = False, mono: bool = False, extra: str = ""):
    family = MONO if mono else FONT
    weight = "font-weight: 600;" if bold else ""
    return ui.label(text).style(f"{family} font-size: {size}px; color: {color}; {weight} line-height: 1.3; {extra}")


def _dot(color: str, size: int = 7) -> None:
    ui.element("div").style(
        f"width: {size}px; height: {size}px; border-radius: 50%; background: {color}; flex-shrink: 0;"
    )


def _species_chip(sp: str) -> None:
    name, color = SPECIES[sp]
    with ui.element("div").style(
        f"display: inline-flex; align-items: center; gap: 4px; padding: 0 6px; border-radius: 8px; "
        f"background: {color}1a; border: 1px solid {color}55; height: 15px;"
    ):
        _dot(color, 6)
        _t(name, 9, color, bold=True)


def _map_thumb(px: int, *, dim: bool = False) -> None:
    """A capsid-like central slice drawn in CSS, standing in for a map's thumbnail."""
    ui.element("div").style(
        f"width: {px}px; height: {px}px; border-radius: 4px; flex-shrink: 0; opacity: {0.4 if dim else 1}; "
        "background: radial-gradient(circle at 50% 50%, #3b4252 0 30%, #d8dee9 33% 50%, #eceff4 53%, "
        "#3b4252 58% 100%);"
    )


def _composition(counts: list[int], width: int, height: int = 6) -> None:
    """One bar, a segment per project in proportion to its picks."""
    with ui.element("div").style(
        f"display: flex; width: {width}px; height: {height}px; border-radius: 2px; overflow: hidden; flex-shrink: 0;"
    ):
        for i, c in enumerate(counts):
            ui.element("div").style(f"flex: {c} 0 0; background: {PROJECT_HUES[i % len(PROJECT_HUES)]};")


CARD_LOOK = {
    "chosen": f"border: 2px solid {BLUE}; background: #eff6ff;",
    "option": f"border: 1px solid {LINE}; background: #fff;",
    "dim": f"border: 1px dashed {LINE}; background: #f8fafc; opacity: 0.5;",
    "amber": f"border: 1px solid {AMBER}; background: #fffbeb;",
    "red": f"border: 1px solid {RED}; background: #fef2f2;",
    "offer": f"border: 1px dashed {BLUE}; background: #fff;",
}


def _card(look: str, width: str = "240px", extra: str = "") -> ui.element:
    return ui.element("div").style(
        f"{CARD_LOOK[look]} border-radius: 6px; padding: 7px 9px; width: {width}; box-sizing: border-box; "
        f"cursor: pointer; display: flex; flex-direction: column; gap: 3px; {extra}"
    )


def _particles_card(p: dict, look: str, on_click, width: str = "240px") -> None:
    card = _card(look, width)
    with card:
        with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
            _t(p["title"], 11, INK, bold=True, extra="flex: 1 1 0; min-width: 0;")
            _species_chip(p["species"])
        _t(f"{p['picks']:,} picks · {p['sub']}", 9, SOFT)
        _composition(p["counts"], 200)
        _t(p["origin"], 9, MUTED)
        if p["species"] != JOB_SPECIES:
            _t("another species than this job", 9, AMBER, bold=True)
    card.on("click", lambda _e: on_click(p["id"]))


def _ref_card(r: dict, look: str, on_click, width: str = "240px") -> None:
    card = _card(look, width)
    with card:
        with ui.element("div").style("display: flex; gap: 8px; align-items: center;"):
            _map_thumb(42, dim=look == "dim")
            with ui.column().style("gap: 1px; min-width: 0; flex: 1 1 0;"):
                _t(r["title"], 11, INK, bold=True)
                _t(f"{r['sub']} · {r['facts']}", 9, SOFT)
                _species_chip(r["species"])
    card.on("click", lambda _e: on_click(r["id"]))


def _export_card(project, export, look: str, on_click, width: str = "200px") -> None:
    card = _card(look, width)
    with card:
        _t(f"Warp export {export.job}", 11, INK, bold=True)
        _t(f"{project.name} · box {export.box} · {export.apix} Å", 9, SOFT)
        _t(f"{export.n_images} images of {project.picks} picks", 9, SOFT)
        # A strip of grey squares stands in for a few of the export's particle images.
        with ui.element("div").style("display: flex; gap: 2px; margin-top: 2px;"):
            for k in range(8):
                shade = 70 + (k * 37 + len(export.job) * 11) % 60
                ui.element("div").style(
                    f"width: 16px; height: 16px; border-radius: 2px; background: hsl(215 12% {shade}%);"
                )
    card.on("click", lambda _e: on_click(project.name, export.job))


def _offer_card(text: str, sub: str, width: str = "240px") -> None:
    card = _card("offer", width)
    with card:
        _t(f"+ {text}", 11, BLUE, bold=True)
        _t(sub, 9, SOFT)
    card.on("click", lambda _e: ui.notify("Mock: would add the job"))


def _verdict(f: Facets) -> tuple[str, str]:
    """The one-line state of the particles input and its colour."""
    if f.status == "as-written":
        return "reads the job's particles as written", BODY
    if f.status == "species":
        return "another species than this job: chosen by hand only", AMBER
    if f.status == "differs":
        odd = ", ".join(f"{p.name} box {f.chosen[p.name].box}" for p in f.odd)
        return f"exports differ ({odd}; the rest box {f.box}): run one Warp export on this set", RED
    if f.status == "choose":
        return f"choose the images of {', '.join(p.name for p in f.ask)}", AMBER
    drop = f" · {f.dropped} dropped by Warp" if f.dropped else ""
    return f"{f.images:,} of {f.picks:,} with images · box {f.box} · {f.apix} Å{drop}", GREEN


# ── 1 · sockets and cables ───────────────────────────────────────────────────

CARD_H = 74
GAP = 10
NODE_X = 470
NODE_W = 300
ROW_H = 46
HEAD_H = 40


def design_sockets(st: State, redraw) -> None:
    f = _facets(st)
    is_set = _particles(st)["kind"] == "set"
    sockets = [("particles", "particles", _particles(st)["title"])]
    if is_set:
        sockets.append(("images", "images of the set", _verdict(f)[0]))
    sockets.append(("reference", "reference", _ref(st)["title"]))
    if st.active not in {k for k, _, _ in sockets}:
        st.active = "particles"

    options: list[tuple[str, callable]] = []  # (look, render)
    chosen_index: list[int] = []

    def choose_particles(pid):
        st.particles = pid
        redraw()

    def choose_ref(rid):
        st.reference = rid
        redraw()

    def choose_export(name, job):
        st.picks[name] = job
        redraw()

    if st.active == "particles":
        for p in PARTICLES:
            look = "chosen" if p["id"] == st.particles else "option"
            if look == "chosen":
                chosen_index.append(len(options))
            options.append((look, lambda w, p=p, look=look: _particles_card(p, look, choose_particles, w)))
    elif st.active == "reference":
        for r in REFS:
            look = "chosen" if r["id"] == st.reference else ("dim" if r["species"] != JOB_SPECIES else "option")
            if r["id"] == st.reference:
                chosen_index.append(len(options))
            options.append((look, lambda w, r=r, look=look: _ref_card(r, look, choose_ref, w)))
    else:
        settled = [n for n in f.chosen if all(n != p.name for p in f.odd)]

        n_settled = len(settled)

        def group_card(w, n=n_settled):
            with _card("chosen", w):
                _t(f"{n} projects · one matching export each", 11, INK, bold=True)
                _t(f"box {f.box} · {f.apix} Å · filled in, nothing else to choose", 9, SOFT)

        chosen_index.append(0)
        options.append(("chosen", group_card))
        for proj in f.ask:
            for e in proj.exports:
                options.append(("amber", lambda w, proj=proj, e=e: _export_card(proj, e, "amber", choose_export, w)))
        for proj in f.odd:
            e = f.chosen[proj.name]

            def odd_card(w, proj=proj, e=e):
                with _card("red", w):
                    _t(f"{proj.name} · Warp export {e.job}", 11, INK, bold=True)
                    _t(f"box {e.box}: does not match the others (box {f.box})", 9, RED)

            options.append(("red", odd_card))
            options.append(
                (
                    "offer",
                    lambda w: _offer_card("Run one Warp export on this set", "one box and pixel for all picks", w),
                )
            )

    height = max(len(options) * (CARD_H + GAP), HEAD_H + len(sockets) * ROW_H) + 20
    active_row = [k for k, _, _ in sockets].index(st.active)
    sock_y = 10 + HEAD_H + active_row * ROW_H + ROW_H / 2
    paths = []
    for i, (look, _) in enumerate(options):
        y = 10 + i * (CARD_H + GAP) + CARD_H / 2
        chosen = i in chosen_index
        color = BLUE if chosen else {"amber": AMBER, "red": RED, "offer": BLUE}.get(look, "#cbd5e1")
        width = 2.5 if chosen else 1.2
        dash = "" if chosen else 'stroke-dasharray="4 4"'
        paths.append(
            f'<path d="M 252 {y} C 360 {y}, 360 {sock_y}, {NODE_X - 6} {sock_y}" fill="none" stroke="{color}" '
            f'stroke-width="{width}" {dash}/>'
        )
    with ui.element("div").style(f"position: relative; width: {NODE_X + NODE_W + 20}px; height: {height}px;"):
        ui.html(
            f'<svg width="{NODE_X}" height="{height}" style="position:absolute;left:0;top:0;">{"".join(paths)}</svg>',
            sanitize=False,
        )
        for i, (_look, render) in enumerate(options):
            with ui.element("div").style(f"position: absolute; left: 0; top: {10 + i * (CARD_H + GAP)}px;"):
                render("250px")
        with ui.element("div").style(
            f"position: absolute; left: {NODE_X}px; top: 10px; width: {NODE_W}px; border-radius: 8px; "
            f"border: 1px solid #cbd5e1; background: #fff; box-shadow: 0 6px 18px rgba(15,23,42,0.08);"
        ):
            with ui.element("div").style(
                f"height: {HEAD_H}px; display: flex; align-items: center; gap: 8px; padding: 0 10px; "
                "background: #edf4fb; border-radius: 8px 8px 0 0;"
            ):
                _t("Class 3D", 12, INK, bold=True)
                _t("new", 9, MUTED)
                _species_chip(JOB_SPECIES)
            for key, label, value in sockets:
                active = key == st.active
                color = BLUE if active else ("#cbd5e1" if key != "images" else _verdict(f)[1])
                row = ui.element("div").style(
                    f"height: {ROW_H}px; display: flex; align-items: center; gap: 8px; padding: 0 10px; "
                    f"cursor: pointer; position: relative; background: {'#f1f5f9' if active else 'transparent'};"
                )
                with row:
                    ui.element("div").style(
                        f"position: absolute; left: -7px; width: 12px; height: 12px; border-radius: 50%; "
                        f"background: #fff; border: 3px solid {color};"
                    )
                    with ui.column().style("gap: 0; min-width: 0;"):
                        _t(label, 9, SOFT, bold=True)
                        _t(value, 10, _verdict(f)[1] if key == "images" else INK, extra="white-space: normal;")
                row.on("click", lambda _e, k=key: (setattr(st, "active", k), redraw()))


# ── 2 · pantry and slots ─────────────────────────────────────────────────────


def design_pantry(st: State, redraw) -> None:
    f = _facets(st)
    only_fits = st.extra.setdefault("only_fits", True)

    def slot(key: str) -> ui.element:
        active = st.active == key
        return ui.element("div").style(
            f"width: 330px; min-height: 128px; border-radius: 10px; padding: 10px 12px; box-sizing: border-box; "
            f"cursor: pointer; display: flex; flex-direction: column; gap: 5px; "
            f"border: {'2px solid ' + BLUE if active else '1px solid #cbd5e1'}; "
            f"background: {'#eff6ff' if active else '#fff'};"
        )

    with ui.element("div").style("display: flex; gap: 14px; align-items: stretch; flex-wrap: wrap;"):
        p = _particles(st)
        s = slot("particles")
        with s:
            _t("PARTICLES", 9, SOFT, bold=True, extra="letter-spacing: 0.06em;")
            with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                _t(p["title"], 13, INK, bold=True)
                _species_chip(p["species"])
            _composition(p["counts"], 300, 8)
            text, color = _verdict(f)
            _t(text, 10, color, bold=color != GREEN)
            if p["kind"] == "set":
                # One tile per project: green filled in, amber to choose, red does not match.
                with ui.element("div").style("display: flex; gap: 4px; flex-wrap: wrap;"):
                    for proj in _projects(st.kind):
                        tone = (
                            RED
                            if any(proj.name == o.name for o in f.odd)
                            else AMBER
                            if any(proj.name == a.name for a in f.ask)
                            else GREEN
                        )
                        tile = ui.element("div").style(
                            f"padding: 1px 5px; border-radius: 4px; background: {tone}1f; border: 1px solid {tone}88;"
                        )
                        with tile:
                            _t(proj.name, 8, INK)
                        tile.tooltip(
                            f"{proj.name}: "
                            + (
                                f.chosen[proj.name].job + f" · box {f.chosen[proj.name].box}"
                                if proj.name in f.chosen
                                else "two matching exports: choose below"
                            )
                        )
            if f.ask:
                with ui.element("div").style("display: flex; gap: 6px; margin-top: 4px;"):
                    for proj in f.ask:
                        for e in proj.exports:
                            _export_card(proj, e, "amber", lambda n, j: (st.picks.__setitem__(n, j), redraw()), "150px")
            if f.status == "differs":
                house_button("Run one Warp export on this set", lambda: ui.notify("Mock: would add the job"))
        s.on("click", lambda _e: (setattr(st, "active", "particles"), redraw()))
        r = _ref(st)
        s = slot("reference")
        with s:
            _t("REFERENCE", 9, SOFT, bold=True, extra="letter-spacing: 0.06em;")
            with ui.element("div").style("display: flex; gap: 10px; align-items: center;"):
                _map_thumb(78)
                with ui.column().style("gap: 2px;"):
                    _t(r["title"], 12, INK, bold=True)
                    _t(r["sub"], 10, SOFT)
                    _t(r["facts"], 10, SOFT)
                    _t("low-passed to 45 Å at the start", 9, MUTED)
        s.on("click", lambda _e: (setattr(st, "active", "reference"), redraw()))
        with ui.element("div").style(
            "width: 150px; min-height: 128px; border-radius: 10px; border: 1px dashed #cbd5e1; display: flex; "
            "align-items: center; justify-content: center; text-align: center; padding: 8px;"
        ):
            _t("mask (optional) — drop one here", 10, MUTED)

    with ui.element("div").style("display: flex; align-items: center; gap: 8px; margin-top: 12px;"):
        _t(f"Pantry · what this project holds for the {st.active}", 11, INK, bold=True)
        tog = ui.element("div").style(
            f"padding: 1px 8px; border-radius: 9px; cursor: pointer; border: 1px solid {LINE}; "
            f"background: {'#e0f2fe' if only_fits else '#fff'};"
        )
        with tog:
            _t("only what fits this job" if only_fits else "everything", 9, BODY)
        tog.on("click", lambda _e: (st.extra.__setitem__("only_fits", not only_fits), redraw()))
    items = PARTICLES if st.active == "particles" else REFS
    for sp, (name, color) in SPECIES.items():
        group = [x for x in items if x["species"] == sp]
        if only_fits and sp != JOB_SPECIES:
            if group:
                _t(f"+ {len(group)} of {name} hidden (another species)", 9, MUTED, extra="margin: 4px 0 0 2px;")
            continue
        if not group:
            continue
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; margin-top: 6px;"):
            _dot(color)
            _t(name, 10, color, bold=True)
        with ui.element("div").style("display: flex; gap: 8px; flex-wrap: wrap;"):
            for x in group:
                if st.active == "particles":
                    look = "chosen" if x["id"] == st.particles else "option"
                    _particles_card(x, look, lambda pid: (setattr(st, "particles", pid), redraw()), "230px")
                else:
                    look = "chosen" if x["id"] == st.reference else "option"
                    _ref_card(x, look, lambda rid: (setattr(st, "reference", rid), redraw()), "230px")


# ── 3 · the lineage river ────────────────────────────────────────────────────


def _station(title: str, sub: str, color: str = "#cbd5e1", *, last: bool = False) -> ui.element:
    """One stop on a vertical line: a dot on the line and a card beside it."""
    outer = ui.element("div").style("display: flex; gap: 10px; align-items: stretch;")
    with outer:
        with ui.element("div").style("width: 14px; display: flex; flex-direction: column; align-items: center;"):
            ui.element("div").style(
                f"width: 12px; height: 12px; border-radius: 50%; background: #fff; border: 3px solid {color}; "
                "margin-top: 8px; flex-shrink: 0;"
            )
            if not last:
                ui.element("div").style("flex: 1 1 0; width: 2px; background: #cbd5e1; min-height: 14px;")
        body = ui.column().style("gap: 4px; padding-bottom: 10px; min-width: 0;")
        with body:
            _t(title, 11, INK, bold=True)
            if sub:
                _t(sub, 9, SOFT)
    return body


def design_river(st: State, redraw) -> None:
    f = _facets(st)
    p = _particles(st)
    with ui.element("div").style("display: flex; gap: 40px; align-items: flex-start; flex-wrap: wrap;"):
        with ui.column().style("gap: 0; width: 470px;"):
            _t("where the particles come from", 9, SOFT, bold=True, extra="margin-bottom: 6px;")
            if p["kind"] == "set":
                _station("Template matching in 7 projects", "job043, job017, job017, job020, job023, job019, job020")
                _station("STA kitchen · 412 pool", "14,861 picks · filters, verdicts and a subset choose")
            body = _station(p["title"], f"{p['picks']:,} picks · {p['origin']}", BLUE)
            with body:
                others = [x for x in PARTICLES if x["id"] != p["id"]]
                with ui.element("div").style("display: flex; gap: 6px; flex-wrap: wrap;"):
                    for x in others:
                        chip = ui.element("div").style(
                            f"padding: 1px 7px; border-radius: 9px; border: 1px solid {LINE}; cursor: pointer; "
                            f"opacity: {0.5 if x['species'] != JOB_SPECIES else 1};"
                        )
                        with chip:
                            _t(f"or {x['title']}", 9, BODY)
                        chip.on("click", lambda _e, x=x: (setattr(st, "particles", x["id"]), redraw()))
            if p["kind"] == "set":
                text, color = _verdict(f)
                body = _station("Particle images", text, color)
                with body:
                    with ui.element("div").style("display: flex; gap: 8px; flex-wrap: wrap; align-items: stretch;"):
                        settled = len(f.chosen) - len(f.odd)
                        with _card("chosen", "150px", "cursor: default;"):
                            _t(f"{settled} projects", 11, INK, bold=True)
                            _t(f"one matching export each · box {f.box}", 9, SOFT)
                        for proj in f.ask:
                            # The fork: the river splits; the branch picked carries on, the other fades.
                            for e in proj.exports:
                                _export_card(
                                    proj, e, "amber", lambda n, j: (st.picks.__setitem__(n, j), redraw()), "150px"
                                )
                        for proj in f.odd:
                            with _card("red", "150px", "cursor: default;"):
                                _t(proj.name, 11, INK, bold=True)
                                _t(f"{f.chosen[proj.name].job} · box {f.chosen[proj.name].box}", 9, RED)
                        if f.odd:
                            _offer_card("One Warp export on the set", "one box for every pick", "170px")
                    if st.picks:
                        _t("branch taken: " + ", ".join(f"{k} → {v}" for k, v in st.picks.items()), 9, MUTED)
            _station("this Class 3D", "reads them with the reference on the right", INK, last=True)
        with ui.column().style("gap: 0; width: 330px;"):
            _t("where the reference comes from", 9, SOFT, bold=True, extra="margin-bottom: 6px;")
            r = _ref(st)
            _station(r["sub"].split(" · ")[0].replace("from ", "set "), "the picks it was made of")
            body = _station(r["title"], r["facts"], BLUE)
            with body:
                with ui.element("div").style("display: flex; gap: 8px; align-items: center;"):
                    _map_thumb(64)
                    with ui.column().style("gap: 3px;"):
                        for x in REFS:
                            if x["id"] == r["id"]:
                                continue
                            chip = ui.element("div").style(
                                f"padding: 1px 7px; border-radius: 9px; border: 1px solid {LINE}; cursor: pointer; "
                                f"opacity: {0.5 if x['species'] != JOB_SPECIES else 1};"
                            )
                            with chip:
                                _t(f"or {x['title']}", 9, BODY)
                            chip.on("click", lambda _e, x=x: (setattr(st, "reference", x["id"]), redraw()))
            _station("this Class 3D", "", INK, last=True)


# ── 4 · questions with answer cards ──────────────────────────────────────────


def design_questions(st: State, redraw) -> None:
    f = _facets(st)
    answered = st.extra.setdefault("answered", {"particles": True, "reference": True})

    def question(key: str, ask: str, summary: str, render, *, needs: bool = False) -> None:
        is_open = not answered.get(key, False) or needs
        with ui.column().style("gap: 6px; width: 100%;"):
            with ui.element("div").style("display: flex; align-items: baseline; gap: 10px;"):
                _t(ask, 14, AMBER if needs else INK, bold=True)
                if not is_open:
                    _t(summary, 11, BODY)
                    link = _t("change", 10, BLUE, extra="cursor: pointer;")
                    link.on("click", lambda _e: (answered.__setitem__(key, False), redraw()))
            if is_open:
                with ui.element("div").style("display: flex; gap: 10px; flex-wrap: wrap;"):
                    render()

    def pick_particles(pid):
        st.particles = pid
        answered["particles"] = True
        redraw()

    def pick_ref(rid):
        st.reference = rid
        answered["reference"] = True
        redraw()

    p = _particles(st)

    def particle_answers():
        for x in PARTICLES:
            look = "chosen" if x["id"] == st.particles else ("dim" if x["species"] != JOB_SPECIES else "option")
            _particles_card(x, look, pick_particles, "250px")

    question("particles", "Which particles?", f"{p['title']} · {p['picks']:,} picks", particle_answers)
    for proj in f.ask:

        def export_answers(proj=proj):
            for e in proj.exports:
                _export_card(proj, e, "amber", lambda n, j: (st.picks.__setitem__(n, j), redraw()), "220px")
            with ui.column().style("gap: 2px; max-width: 260px;"):
                _t("Both have the same parameters.", 10, BODY)
                _t("job036 has one image more; nothing else tells them apart. Pick the one you mean.", 10, SOFT)

        question(
            f"img-{proj.name}", f"Whose images for {proj.name}'s {proj.picks} picks?", "", export_answers, needs=True
        )
    if f.status == "differs":

        def differ_answers():
            for proj in f.odd:
                e = f.chosen[proj.name]
                with _card("red", "250px", "cursor: default;"):
                    _t(f"{proj.name} · Warp export {e.job}", 11, INK, bold=True)
                    _t(f"box {e.box}; the other projects' exports are box {f.box}", 9, RED)
            _offer_card("Run one Warp export on this set", "one box and pixel for all 2,708 picks", "250px")

        question("differ", "These exports cannot be read together", "", differ_answers, needs=True)
    r = _ref(st)

    def ref_answers():
        for x in REFS:
            look = "chosen" if x["id"] == st.reference else ("dim" if x["species"] != JOB_SPECIES else "option")
            _ref_card(x, look, pick_ref, "250px")

    question("reference", "Against which reference?", f"{r['title']} · {r['facts']}", ref_answers)
    text, color = _verdict(f)
    with ui.element("div").style(
        f"margin-top: 6px; padding: 6px 10px; border-radius: 6px; background: {color}14; border: 1px solid {color}55;"
    ):
        _t(("Ready: " if f.status in ("ready", "as-written") else "Not yet: ") + text, 11, color, bold=True)


# ── 5 · the coverage matrix ──────────────────────────────────────────────────


def design_matrix(st: State, redraw) -> None:
    f = _facets(st)
    p = _particles(st)
    with ui.element("div").style("display: flex; align-items: center; gap: 8px; margin-bottom: 6px;"):
        _t("particles", 9, SOFT, bold=True)
        for x in PARTICLES:
            on = x["id"] == st.particles
            chip = ui.element("div").style(
                f"padding: 2px 8px; border-radius: 9px; cursor: pointer; "
                f"border: {'2px solid ' + BLUE if on else '1px solid ' + LINE}; "
                f"background: {'#eff6ff' if on else '#fff'};"
            )
            with chip:
                _t(f"{x['title']} · {x['picks']:,}", 10, INK if on else BODY, bold=on)
            chip.on("click", lambda _e, x=x: (setattr(st, "particles", x["id"]), redraw()))
    if p["kind"] != "set":
        _t("A job's output: read as written, one project, no facets to choose.", 11, BODY)
        return
    cols = [("positions", False), ("images", True), ("poses", False), ("halves", True)]
    grid = (
        "display: grid; grid-template-columns: 150px 60px repeat(4, minmax(120px, 1fr)); "
        "gap: 3px; align-items: stretch; max-width: 900px;"
    )
    with ui.element("div").style(grid):
        _t("project", 9, SOFT, bold=True)
        _t("picks", 9, SOFT, bold=True)
        for name, needed in cols:
            with ui.element("div").style(
                f"padding: 3px 6px; border-radius: 4px; background: {'#dbeafe' if needed else '#f1f5f9'};"
            ):
                _t(name + (" · this job needs" if needed else ""), 9, INK if needed else SOFT, bold=needed)
        projects = _projects(st.kind)
        scale = p["picks"] / 2708
        for i, proj in enumerate(projects):
            with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                ui.element("div").style(f"width: 8px; height: 8px; background: {PROJECT_HUES[i]}; border-radius: 2px;")
                _t(proj.name, 10, INK)
            _t(f"{round(proj.picks * scale):,}", 10, BODY, mono=True)

            def cell(text: str, tone: str, sub: str = "") -> ui.element:
                c = ui.element("div").style(
                    f"padding: 3px 6px; border-radius: 4px; background: {tone}14; border: 1px solid {tone}44; "
                    "display: flex; flex-direction: column; gap: 1px;"
                )
                with c:
                    _t(text, 10, INK)
                    if sub:
                        _t(sub, 8, SOFT)
                return c

            cell("as picked", "#64748b", "template matching")
            e = f.chosen.get(proj.name)
            if e is None:
                with ui.element("div").style(
                    f"padding: 3px; border-radius: 4px; border: 1px solid {AMBER}; background: #fffbeb; "
                    "display: flex; gap: 3px;"
                ):
                    for alt in proj.exports:
                        b = ui.element("div").style(
                            "flex: 1 1 0; padding: 2px 4px; border-radius: 3px; background: #fff; cursor: pointer; "
                            f"border: 1px solid {AMBER}88;"
                        )
                        with b:
                            _t(alt.job, 10, INK, bold=True)
                            _t(f"{alt.n_images} img", 8, SOFT)
                        b.on("click", lambda _e, n=proj.name, j=alt.job: (st.picks.__setitem__(n, j), redraw()))
            elif any(o.name == proj.name for o in f.odd):
                cell(f"{e.job} · box {e.box}", RED, "does not match")
            else:
                dropped = proj.picks - e.n_images
                cell(f"{e.job} · box {e.box}", GREEN, f"{dropped} dropped" if dropped else "all picks")
            cell("from the images", "#64748b", "as exported")
            cell("from the set", GREEN, "by uid")
    text, color = _verdict(f)
    with ui.element("div").style("display: flex; align-items: center; gap: 10px; margin-top: 8px;"):
        _dot(color, 9)
        _t(text, 11, color, bold=True)
        if f.status == "differs":
            house_button("Run one Warp export on this set", lambda: ui.notify("Mock: would add the job"))


# ── 6 · the project map ──────────────────────────────────────────────────────

STAGES = [
    ("import", 2),
    ("motion", 3),
    ("align", 4),
    ("ctf", 4),
    ("filter", 3),
    ("recon", 6),
    ("denoise", 5),
    ("segment", 4),
    ("TM", 8),
    ("candidates", 8),
    ("exports", 6),
    ("reconstruct", 3),
    ("class 3D", 4),
    ("refine 3D", 3),
]
NAMED = {("exports", 0): "job044", ("reconstruct", 0): "job048", ("class 3D", 0): "job052"}
FITS = {"particles": {"exports", "class 3D", "refine 3D"}, "reference": {"reconstruct", "class 3D", "refine 3D"}}


def design_map(st: State, redraw) -> None:
    f = _facets(st)
    rng = random.Random(7)
    col_w, top, lane_h = 62, 112, 300
    nodes = {}
    for c, (stage, n) in enumerate(STAGES):
        for k in range(n):
            y = top + (k + 0.5) * lane_h / n + rng.uniform(-6, 6)
            nodes[(stage, k)] = (20 + c * col_w, y)
    edges = []
    for c in range(1, len(STAGES)):
        stage, n = STAGES[c]
        prev, pn = STAGES[c - 1]
        for k in range(n):
            for j in rng.sample(range(pn), min(pn, rng.choice([1, 2]))):
                edges.append((nodes[(prev, j)], nodes[(stage, k)]))
    width = 20 + len(STAGES) * col_w + 150
    new_xy = (width - 70, top + lane_h / 2)
    kitchen_y = 46
    sets = [x for x in PARTICLES if x["kind"] == "set"]
    set_xy = {x["id"]: (20 + (9 + i * 1.4) * col_w, kitchen_y) for i, x in enumerate(sets)}
    bubbles = [p.name for p in _projects(st.kind) if p.name != "MyrGFP_Grid2"]
    bubble_xy = {b: (24 + i * 78, 14) for i, b in enumerate(bubbles)}

    lit = FITS["particles" if st.active == "particles" else "reference"]
    target = st.particles if st.active == "particles" else st.reference
    chosen_xy = set_xy.get(target) if st.active == "particles" else None
    for (stage, k), xy in nodes.items():
        if NAMED.get((stage, k)) == target:
            chosen_xy = xy
    svg = [f'<path d="M {a[0]} {a[1]} L {b[0]} {b[1]}" stroke="#e2e8f0" stroke-width="1"/>' for a, b in edges]
    for bx, by in bubble_xy.values():
        for sx, sy in set_xy.values():
            svg.append(f'<path d="M {bx + 30} {by + 10} L {sx} {sy}" stroke="#eef2f6" stroke-width="1"/>')
    if chosen_xy:
        x0, y0 = chosen_xy
        x1, y1 = new_xy
        svg.append(
            f'<path d="M {x0} {y0} C {x0 + 120} {y0}, {x1 - 120} {y1}, {x1 - 22} {y1}" stroke="{BLUE}" '
            'stroke-width="2.5" fill="none"/>'
        )
    height = top + lane_h + 30
    with ui.element("div").style("display: flex; gap: 10px; align-items: center; margin-bottom: 4px;"):
        for key, label in (("particles", "choosing particles"), ("reference", "choosing the reference")):
            on = st.active == key
            chip = ui.element("div").style(
                f"padding: 2px 8px; border-radius: 9px; cursor: pointer; "
                f"border: {'2px solid ' + BLUE if on else '1px solid ' + LINE};"
            )
            with chip:
                _t(label, 10, INK if on else BODY, bold=on)
            chip.on("click", lambda _e, k=key: (setattr(st, "active", k), redraw()))
        text, color = _verdict(f)
        _t(text, 10, color, bold=True)
    with ui.element("div").style(
        f"position: relative; width: {width}px; height: {height}px; background: #fbfcfe; border-radius: 8px; "
        f"border: 1px solid {LINE}; overflow: hidden;"
    ):
        ui.html(
            f'<svg width="{width}" height="{height}" style="position:absolute;left:0;top:0;">{"".join(svg)}</svg>',
            sanitize=False,
        )
        ui.label("STA kitchen · sets of every species").style(
            f"position: absolute; left: {set_xy['s002'][0] - 40}px; top: {kitchen_y - 34}px; {FONT} font-size: 9px; "
            f"color: {SOFT};"
        )
        for c, (stage, _n) in enumerate(STAGES):
            ui.label(stage).style(
                f"position: absolute; left: {12 + c * col_w}px; top: {top + lane_h + 8}px; {FONT} font-size: 8px; "
                f"color: {MUTED}; width: {col_w}px;"
            )
        for b, (bx, by) in bubble_xy.items():
            tone = AMBER if any(a.name == b for a in f.ask) else RED if any(o.name == b for o in f.odd) else "#94a3b8"
            bub = ui.element("div").style(
                f"position: absolute; left: {bx}px; top: {by}px; padding: 1px 6px; border-radius: 9px; "
                f"background: {tone}1a; border: 1px solid {tone}; cursor: pointer;"
            )
            with bub:
                _t(b, 8, INK)
            proj = next(p for p in _projects(st.kind) if p.name == b)
            if any(a.name == b for a in f.ask):
                with bub:
                    menu = ui.menu().props('no-parent-event anchor="bottom left" self="top left"')
                    with menu, ui.element("div").style("display: flex; gap: 6px; padding: 8px;"):
                        for e in proj.exports:
                            _export_card(proj, e, "amber", lambda n, j: (st.picks.__setitem__(n, j), redraw()), "170px")
                bub.on("click", lambda _e, m=menu: m.open())
                bub.tooltip(f"{b}: two matching exports. Click to choose.")
            else:
                e = f.chosen.get(b)
                bub.tooltip(f"another project · its export {e.job}, box {e.box}" if e else b)
        for x in sets:
            sx, sy = set_xy[x["id"]]
            color = SPECIES[x["species"]][1]
            on = st.active == "particles" and st.particles == x["id"]
            fits = st.active == "particles" and x["species"] == JOB_SPECIES
            node = ui.element("div").style(
                f"position: absolute; left: {sx - 34}px; top: {sy - 11}px; width: 68px; height: 22px; "
                f"border-radius: 11px; display: flex; align-items: center; justify-content: center; cursor: pointer; "
                f"background: {color if on else color + ('33' if fits else '12')}; "
                f"border: 2px solid {BLUE if on else (color if fits else '#e2e8f0')};"
            )
            with node:
                _t(x["id"], 9, "#fff" if on else INK, bold=True)
            node.tooltip(f"{x['title']} · {x['picks']:,} picks · {SPECIES[x['species']][0]} · {x['origin']}")
            node.on(
                "click",
                lambda _e, x=x: (setattr(st, "particles", x["id"]), setattr(st, "active", "particles"), redraw()),
            )
        for (stage, k), (x, y) in nodes.items():
            name = NAMED.get((stage, k))
            on = name is not None and name == target
            fit = stage in lit
            size = 14 if fit else 9
            node = ui.element("div").style(
                f"position: absolute; left: {x - size / 2}px; top: {y - size / 2}px; width: {size}px; "
                f"height: {size}px; border-radius: 3px; cursor: {'pointer' if fit else 'default'}; "
                f"background: {BLUE if on else ('#0ea5e9' if fit else '#cbd5e1')}; "
                f"opacity: {1 if fit else 0.55}; box-shadow: {'0 0 0 3px #bfdbfe' if on else 'none'};"
            )
            label = name or f"{stage} job"
            node.tooltip(f"{label} · {stage}" + ("" if fit else " · cannot feed this input"))
            key = "particles" if st.active == "particles" else "reference"
            known = {x["id"] for x in (PARTICLES if key == "particles" else REFS)}
            if fit and name in known:
                node.on("click", lambda _e, n=name, k=key: (setattr(st, k, n), redraw()))
            elif fit:
                node.on("click", lambda _e: ui.notify("Mock: an unnamed job of the map"))
        nx, ny = new_xy
        with ui.element("div").style(
            f"position: absolute; left: {nx - 22}px; top: {ny - 30}px; width: 86px; padding: 6px; border-radius: 8px; "
            f"background: #fff; border: 2px solid {INK}; box-shadow: 0 0 0 4px #e0e7ff;"
        ):
            _t("Class 3D", 10, INK, bold=True)
            _t("new · 412", 8, SOFT)


DESIGNS = (
    (
        "1 · Sockets and cables",
        "The job is a node; each thing it needs is a socket on its edge. Click a socket and its candidates fan out as "
        "cards, each wired to it; the chosen one carries the solid cable. A set brings a second socket, its images.",
        design_sockets,
    ),
    (
        "2 · Pantry and slots",
        "What the job needs are big slots to fill; below them a pantry of everything the project holds for the "
        "active slot, grouped by species. The particles slot shows its projects as tiles: green filled in, amber to "
        "choose (the two exports appear right there), red not matching.",
        design_pantry,
    ),
    (
        "3 · Lineage river",
        "Each input as the path it took to get here: template matching → kitchen → set → images → this job; the "
        "reference as a second river joining. An ambiguity is a fork in the river; picking a branch carries it on.",
        design_river,
    ),
    (
        "4 · Questions",
        "The job asks in words, one question per decision, answers as cards. Settled questions shrink to one line; "
        "a question appears only when something is ambiguous (whose images for 412_Slot6?).",
        design_questions,
    ),
    (
        "5 · Coverage matrix",
        "Projects down, facets across; the columns this job needs are lit. Each cell says which job gives that "
        "project's picks the facet; an ambiguous cell holds the two choices, a red one does not match.",
        design_matrix,
    ),
    (
        "6 · Project map",
        "The project's ~70 jobs as a small map, the kitchen's sets of every species on a lane above with the other "
        "projects as bubbles. What can feed the active input lights up; click to wire it. An ambiguous project's "
        "bubble turns amber and opens its two exports.",
        design_map,
    ),
)


def render_io_ideas() -> None:
    page = {"kind": "choice"}
    states: dict[str, State] = {}

    @ui.refreshable
    def body() -> None:
        for title, blurb, fn in DESIGNS:
            st = states.get(title)
            if st is None or st.kind != page["kind"]:
                st = states[title] = State(page["kind"])
            with ui.column().style("gap: 6px; width: 100%;"):
                ui.label(title).style(PAGE_SECTION_STYLE)
                ui.label(blurb).style(f"{FONT} font-size: 10px; color: {SOFT}; max-width: 900px;")
                _render_design(fn, st)

    with ui.column().style("padding: 18px 24px; gap: 26px; width: 100%; max-width: 1500px;"):
        with ui.column().style("gap: 6px;"):
            ui.label("A new Class 3D in a project of ~70 jobs: six ways to give it its inputs").style(
                PAGE_SECTION_STYLE + " font-size: 14px;"
            )
            ui.label(
                "Mock data, some jobs invented. Two species, four sets, three maps. The case below applies to every "
                "design; clicks change only this page."
            ).style(f"{FONT} font-size: 10px; color: {SOFT};")

            @ui.refreshable
            def switches() -> None:
                with ui.element("div").style("display: flex; gap: 6px;"):
                    for kind in CASES:
                        house_button(
                            CASE_TITLES[kind],
                            lambda _e, k=kind: switch(k),
                            kind="accent" if page["kind"] == kind else "default",
                        )

            def switch(kind: str) -> None:
                page["kind"] = kind
                switches.refresh()
                body.refresh()

            switches()
        body()


def _render_design(fn, st: State) -> None:
    @ui.refreshable
    def panel() -> None:
        with ui.element("div").style(
            "background: #fff; border: 1px solid #dbe3ec; border-radius: 8px; padding: 14px 16px; width: 100%; "
            "box-sizing: border-box; overflow-x: auto;"
        ):
            fn(st, panel.refresh)

    panel()
