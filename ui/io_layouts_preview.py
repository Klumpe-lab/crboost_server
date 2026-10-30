"""Five layouts for the inputs of a particle job that reads a particle set, side by side, to choose one from.

Static mock data only: a Class 3D reading the 412 set with its images taken from one Warp export per project.
Each layout is shown in three cases: every project has one matching export; one project has two matching exports
(the job must ask which); one project's export does not match the others (the job offers one Warp export over the
whole set instead). The menus and buttons change only this page.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from nicegui import ui

from ui.components.buttons import house_button
from ui.job_plugins._field_styles import LABEL_STYLE, LABEL_W, PAGE_SECTION_STYLE, config_section

FONT = "font-family: 'IBM Plex Sans', sans-serif;"
MONO = "font-family: 'IBM Plex Mono', monospace;"
SMALL = f"{FONT} font-size: 9px;"
TEXT = f"{FONT} font-size: 11px; color: #1e293b;"
MUTED = "#94a3b8"
PATH = "#64748b"
GREEN = "#10b981"
AMBER = "#d97706"
RED = "#ef4444"
DOT = "width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0;"
ROW = "display: flex; align-items: center; gap: 8px; width: 100%; min-width: 0; min-height: 22px;"
DETAIL = f"padding-left: {LABEL_W + 8}px; width: 100%;"
BOX = (
    "display: flex; align-items: center; gap: 6px; cursor: pointer; padding: 0 5px; "
    "border: 1px solid #e2e8f0; border-radius: 4px; background: #fff; height: 20px; box-sizing: border-box;"
)
MENU = "border: 1px solid #e2e8f0; border-radius: 6px; box-shadow: 0 8px 24px rgba(15,23,42,0.14); min-width: 220px;"

CASES = ("clear", "choice", "mismatch")
CASE_TITLES = {
    "clear": "Every project has one matching export",
    "choice": "412_Slot6 has two matching exports",
    "mismatch": "412_Grid3's export does not match",
}


@dataclass
class Export:
    job: str
    box: int
    apix: float
    n_images: int


@dataclass
class Project:
    name: str
    picks: int
    exports: list[Export]


@dataclass
class Case:
    """One mock case and what the user picked on this page."""

    kind: str
    picks: dict[str, str] = field(default_factory=dict)
    opened: bool = False


def _projects(kind: str) -> list[Project]:
    rows = [
        ("MyrGFP_Grid2", 1210, "job044", 1205),
        ("412_Slot6", 402, "job031", 401),
        ("412_20251113", 318, "job022", 317),
        ("412_20260204", 296, "job025", 295),
        ("FCIso_unmilled2", 151, "job028", 151),
        ("412_Grid3", 214, "job024", 212),
        ("SK_20251017_412", 117, "job026", 116),
    ]
    out = []
    for name, picks, job, n in rows:
        exports = [Export(job, 256, 3.1, n)]
        if kind == "choice" and name == "412_Slot6":
            exports.append(Export("job036", 256, 3.1, 402))
        if kind == "mismatch" and name == "412_Grid3":
            exports = [Export(job, 300, 3.1, n)]
        out.append(Project(name, picks, exports))
    return out


@dataclass
class Resolved:
    """What a case resolves to: the export each project reads, the projects still to choose for, the projects
    whose export does not match the rest, and the counts."""

    chosen: dict[str, Export]
    ask: list[Project]
    odd: list[Project]
    picks: int
    images: int
    box: int
    apix: float


def _resolve(case: Case) -> Resolved:
    projects = _projects(case.kind)
    chosen: dict[str, Export] = {}
    ask = []
    for p in projects:
        if len(p.exports) == 1:
            chosen[p.name] = p.exports[0]
        elif case.picks.get(p.name):
            chosen[p.name] = next(e for e in p.exports if e.job == case.picks[p.name])
        else:
            ask.append(p)
    geometry = {}
    for name, e in chosen.items():
        geometry.setdefault((e.box, e.apix), []).append(name)
    main = max(geometry, key=lambda g: len(geometry[g]))
    odd = [p for p in projects if p.name in chosen and (chosen[p.name].box, chosen[p.name].apix) != main]
    images = sum(e.n_images for e in chosen.values())
    return Resolved(chosen, ask, odd, sum(p.picks for p in projects), images, main[0], main[1])


def _dot(color: str) -> None:
    ui.element("div").style(f"{DOT} background: {color};")


def _small(text: str, color: str = MUTED, *, bold: bool = False):
    weight = " font-weight: 600;" if bold else ""
    return ui.label(text).style(f"{SMALL} color: {color};{weight} flex-shrink: 0;")


def _text(text: str, color: str = "#1e293b"):
    return ui.label(text).style(
        f"{FONT} font-size: 11px; color: {color}; min-width: 0; overflow: hidden; text-overflow: ellipsis; "
        "white-space: nowrap;"
    )


def _detail(text: str, color: str = MUTED) -> None:
    ui.label(text).style(f"{DETAIL} {SMALL} color: {color}; white-space: normal;")


def _slot_name(text: str) -> None:
    ui.label(text).style(LABEL_STYLE + " white-space: normal;")


def _selector(color: str, name: str, word: str = "") -> ui.element:
    box = ui.element("div").style(f"flex: 1 1 0; min-width: 0; {BOX}")
    with box:
        _dot(color)
        _text(name)
        if word:
            _small(word)
        ui.element("div").style("flex: 1 1 0; min-width: 4px;")
        ui.icon("expand_more", size="14px").style(f"color: {MUTED}; flex-shrink: 0;")
    return box


def _export_menu(anchor: ui.element, case: Case, project: Project, redraw) -> None:
    """A menu on `anchor` choosing which of `project`'s exports the job reads."""
    with anchor:
        menu = ui.menu().props('no-parent-event anchor="bottom left" self="top left"').style(MENU)
        with menu:
            ui.label(f"Images for {project.name}'s {project.picks} picks").style(
                f"{SMALL} color: {PATH}; padding: 6px 10px 2px;"
            )
            for e in project.exports:
                row = ui.element("div").style(
                    "display: flex; align-items: center; gap: 8px; padding: 4px 10px; cursor: pointer;"
                )
                with row:
                    _dot(GREEN)
                    _text(f"Warp export {e.job}")
                    _small(f"box {e.box} · {e.apix} Å · {e.n_images} images")

                def pick(_e, job=e.job):
                    case.picks[project.name] = job
                    menu.close()
                    redraw()

                row.on("click", pick)
    anchor.on("click", lambda _e: menu.open())


def _set_row(res: Resolved) -> None:
    with ui.element("div").style(ROW):
        _slot_name("Input Optimisation")
        _selector(GREEN, "s002 412_and_other_junk", f"set · {res.picks:,} picks · 7 projects")
        _small("set by hand")


def _reference_row() -> None:
    with ui.element("div").style(ROW):
        _slot_name("Input Reference")
        _selector(GREEN, "Reconstruct particle (job048)", "done")
        _small("automatic")


def _images_word(res: Resolved) -> str:
    dropped = res.picks - res.images - sum(p.picks for p in res.ask)
    return f"{res.images:,} of {res.picks:,} picks" + (f" · {dropped} dropped by Warp" if dropped else "")


def _one_export_button(res: Resolved) -> None:
    house_button(
        "Run one Warp export on this set",
        lambda: ui.notify("Mock: would add a Warp export reading s002"),
        tooltip=f"One Warp export over all {res.picks:,} picks of the set, with one box and pixel size. "
        "This Class 3D then reads its particles.",
    )


def _odd_text(res: Resolved) -> str:
    odd = ", ".join(f"{p.name} {res.chosen[p.name].job} box {res.chosen[p.name].box}" for p in res.odd)
    return f"exports differ: {odd}; the other {7 - len(res.odd)} are box {res.box}"


# ── A · facet lines ──────────────────────────────────────────────────────────


def layout_lines(case: Case, redraw) -> None:
    res = _resolve(case)
    _set_row(res)
    with ui.element("div").style(f"{DETAIL} display: flex; flex-direction: column; gap: 2px;"):
        with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
            ui.label("images").style(f"{SMALL} color: {PATH}; width: 40px; flex-shrink: 0;")
            if res.odd:
                _dot(RED)
                ui.label(_odd_text(res)).style(f"{SMALL} color: {RED}; white-space: normal;")
            elif res.ask:
                for p in res.ask:
                    _dot(AMBER)
                    _small(f"{p.name} has {len(p.exports)} matching exports:", AMBER, bold=True)
                    sel = ui.element("div").style(f"{BOX} height: 18px;")
                    with sel:
                        _small("pick one", AMBER)
                        ui.icon("expand_more", size="12px").style(f"color: {MUTED};")
                    _export_menu(sel, case, p, redraw)
            else:
                _dot(GREEN)
                exports = f"Warp export ×{len(res.chosen)} · box {res.box} · {res.apix} Å · {_images_word(res)}"
                ui.label(exports).style(f"{SMALL} color: #334155;")
        if res.odd:
            with ui.element("div").style("padding-left: 46px;"):
                _one_export_button(res)
        for name, text in (
            ("poses", "as exported (template-match angles)"),
            ("halves", "from the set · 1,354 / 1,343"),
        ):
            with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                ui.label(name).style(f"{SMALL} color: {PATH}; width: 40px; flex-shrink: 0;")
                _dot(GREEN)
                ui.label(text).style(f"{SMALL} color: #334155;")
        if case.picks:
            ui.label("picked here: " + ", ".join(f"{k} {v}" for k, v in case.picks.items())).style(
                f"{SMALL} color: {MUTED}; padding-left: 46px;"
            )
    _reference_row()


# ── B · summary line + per-project table ─────────────────────────────────────


def layout_table(case: Case, redraw) -> None:
    res = _resolve(case)
    _set_row(res)
    opened = case.opened or bool(res.ask or res.odd)
    with ui.element("div").style(f"{DETAIL} display: flex; align-items: center; gap: 6px;"):
        color = RED if res.odd else AMBER if res.ask else GREEN
        _dot(color)
        if res.odd:
            summary = "images: exports differ in box size"
        elif res.ask:
            summary = f"images: choose for {', '.join(p.name for p in res.ask)}"
        else:
            summary = f"images from 7 Warp exports · box {res.box} · {res.apix} Å · {_images_word(res)}"
        ui.label(summary).style(f"{SMALL} color: {'#334155' if color == GREEN else color};")
        toggle = ui.label("hide projects" if opened else "show projects").style(
            f"{SMALL} color: #2563eb; cursor: pointer;"
        )

        def flip(_e):
            case.opened = not opened
            redraw()

        toggle.on("click", flip)
    if opened:
        grid = (
            "display: grid; grid-template-columns: minmax(0, 1.4fr) 44px minmax(0, 1fr) 70px 46px; "
            "column-gap: 8px; row-gap: 1px; align-items: center;"
        )
        with ui.element("div").style(f"{DETAIL} {grid} margin-top: 2px;"):
            for head in ("project", "picks", "images from", "box · Å", "dropped"):
                _small(head, PATH, bold=True)
            for p in _projects(case.kind):
                _small(p.name, "#334155")
                _small(f"{p.picks:,}", "#334155")
                e = res.chosen.get(p.name)
                if e is None:
                    sel = ui.element("div").style(f"{BOX} height: 18px;")
                    with sel:
                        _dot(AMBER)
                        _small(f"{len(p.exports)} match · pick", AMBER, bold=True)
                        ui.icon("expand_more", size="12px").style(f"color: {MUTED};")
                    _export_menu(sel, case, p, redraw)
                    _small("—")
                    _small("—")
                    continue
                odd = p in res.odd
                with ui.element("div").style("display: flex; align-items: center; gap: 4px; min-width: 0;"):
                    _dot(RED if odd else GREEN)
                    _small(e.job + (" (picked)" if p.name in case.picks else ""), "#334155")
                _small(f"{e.box} · {e.apix}", RED if odd else "#334155", bold=odd)
                _small(str(p.picks - e.n_images) if p.picks > e.n_images else "")
        if res.odd:
            with ui.element("div").style(f"{DETAIL} margin-top: 4px;"):
                _one_export_button(res)
    _reference_row()


# ── C · coverage bar ─────────────────────────────────────────────────────────


def layout_bar(case: Case, redraw) -> None:
    res = _resolve(case)
    _set_row(res)
    projects = _projects(case.kind)
    with ui.element("div").style(f"{DETAIL} display: flex; align-items: center; gap: 8px;"):
        ui.label("images").style(f"{SMALL} color: {PATH}; width: 40px; flex-shrink: 0;")
        bar = ui.element("div").style(
            "display: flex; height: 10px; width: 220px; border-radius: 3px; overflow: hidden; flex-shrink: 0; "
            "outline: 1px solid #e2e8f0;"
        )
        with bar:
            for p in projects:
                e = res.chosen.get(p.name)
                if e is None:
                    seg = ui.element("div").style(f"flex: {p.picks} 0 0; background: {AMBER}; cursor: pointer;")
                    seg.tooltip(f"{p.name}: {len(p.exports)} matching exports, pick one (click)")
                    _export_menu(seg, case, p, redraw)
                    continue
                color = RED if p in res.odd else GREEN
                seg = ui.element("div").style(f"flex: {e.n_images} 0 0; background: {color};")
                seg.tooltip(f"{p.name}: Warp export {e.job} · box {e.box} · {e.apix} Å · {e.n_images} images")
                if p.picks > e.n_images:
                    gap = ui.element("div").style(f"flex: {p.picks - e.n_images} 0 0; background: #cbd5e1;")
                    gap.tooltip(f"{p.name}: {p.picks - e.n_images} picks dropped by Warp")
        if res.odd:
            _small(_odd_text(res), RED)
        elif res.ask:
            _small("click the amber part to choose", AMBER, bold=True)
        else:
            _small(f"box {res.box} · {res.apix} Å · {_images_word(res)}", "#334155")
    if res.odd:
        with ui.element("div").style(f"{DETAIL} padding-left: {LABEL_W + 56}px;"):
            _one_export_button(res)
    with ui.element("div").style(f"{DETAIL} display: flex; align-items: center; gap: 8px;"):
        ui.label("halves").style(f"{SMALL} color: {PATH}; width: 40px; flex-shrink: 0;")
        with ui.element("div").style(
            "display: flex; height: 10px; width: 220px; border-radius: 3px; overflow: hidden; flex-shrink: 0;"
        ):
            ui.element("div").style("flex: 1354 0 0; background: #60a5fa;").tooltip("half 1 · 1,354")
            ui.element("div").style("flex: 1343 0 0; background: #2563eb;").tooltip("half 2 · 1,343")
        _small("from the set · poses as exported", "#334155")
    _reference_row()


# ── D · one sentence ─────────────────────────────────────────────────────────


def layout_sentence(case: Case, redraw) -> None:
    res = _resolve(case)
    with ui.element("div").style(ROW):
        _slot_name("Reads")
        with ui.element("div").style(
            f"{FONT} font-size: 11px; color: #334155; display: flex; flex-wrap: wrap; align-items: center; "
            "gap: 4px; line-height: 22px;"
        ):
            ui.label("set")
            sel = ui.element("div").style(BOX)
            with sel:
                _dot(GREEN)
                _text("s002 412_and_other_junk")
                ui.icon("expand_more", size="14px").style(f"color: {MUTED};")
            ui.label(f"({res.picks:,} picks) as particle images from")
            if res.odd:
                chip = ui.element("div").style(f"{BOX} border-color: {RED};")
                with chip:
                    _dot(RED)
                    _text("Warp exports that differ", RED)
            elif res.ask:
                for p in res.ask:
                    chip = ui.element("div").style(f"{BOX} border-color: {AMBER};")
                    with chip:
                        _dot(AMBER)
                        _text(f"{p.name}: job031 or job036?", AMBER)
                        ui.icon("expand_more", size="14px").style(f"color: {MUTED};")
                    _export_menu(chip, case, p, redraw)
            else:
                chip = ui.element("div").style(BOX)
                with chip:
                    _dot(GREEN)
                    _text(f"7 Warp exports · box {res.box} · {res.apix} Å")
                    ui.icon("expand_more", size="14px").style(f"color: {MUTED};")
            ui.label(", poses as exported, halves from the set,")
            ui.label("against")
            ref = ui.element("div").style(BOX)
            with ref:
                _dot(GREEN)
                _text("job048 merged.mrc")
                ui.icon("expand_more", size="14px").style(f"color: {MUTED};")
            ui.label(".")
    if res.odd:
        _detail(_odd_text(res), RED)
        with ui.element("div").style(DETAIL):
            _one_export_button(res)
    elif not res.ask:
        _detail(_images_word(res))


# ── E · chips with a popover ─────────────────────────────────────────────────


def _chip(color: str, text: str) -> ui.element:
    chip = ui.element("div").style(
        f"display: flex; align-items: center; gap: 5px; padding: 1px 7px; border-radius: 9px; cursor: pointer; "
        f"border: 1px solid {color}55; background: {color}12;"
    )
    with chip:
        _dot(color)
        _small(text, "#1e293b" if color == GREEN else color, bold=color != GREEN)
    return chip


def layout_chips(case: Case, redraw) -> None:
    res = _resolve(case)
    _set_row(res)
    with ui.element("div").style(f"{DETAIL} display: flex; align-items: center; gap: 5px; flex-wrap: wrap;"):
        if res.odd:
            chip = _chip(RED, "images · box differs")
        elif res.ask:
            chip = _chip(AMBER, f"images · {res.ask[0].name}?")
        else:
            chip = _chip(GREEN, f"images {res.images:,}/{res.picks:,} · box {res.box} · {res.apix} Å")
        with chip:
            pop = ui.menu().props('no-parent-event anchor="bottom left" self="top left"').style(MENU)
            with pop, ui.column().style("padding: 8px 10px; gap: 3px;"):
                _small("images per project", PATH, bold=True)
                for p in _projects(case.kind):
                    with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                        e = res.chosen.get(p.name)
                        if e is None:
                            _dot(AMBER)
                            _small(p.name, "#334155")
                            for x in p.exports:
                                b = house_button(x.job, None)

                                def pick(_e, name=p.name, job=x.job):
                                    case.picks[name] = job
                                    pop.close()
                                    redraw()

                                b.on("click", pick)
                            continue
                        _dot(RED if p in res.odd else GREEN)
                        _small(f"{p.name} · {e.job} · box {e.box} · {e.n_images}/{p.picks}", "#334155")
                if res.odd:
                    _one_export_button(res)
        chip.on("click", lambda _e: pop.open())
        _chip(GREEN, "poses · as exported")
        _chip(GREEN, "halves · from the set")
        dropped = res.picks - res.images - sum(p.picks for p in res.ask)
        if dropped and not res.odd:
            _small(f"{dropped} dropped by Warp")
    _reference_row()


LAYOUTS = (
    ("A · Facet lines", "One line per thing the job needs, under the set. Problems replace the line.", layout_lines),
    (
        "B · Summary + projects",
        "One summary line; the per-project table opens on click, and opens by itself when something needs you.",
        layout_table,
    ),
    (
        "C · Coverage bars",
        "One bar per facet, a segment per project (hover names it). Grey = dropped, amber = choose, red = differs.",
        layout_bar,
    ),
    (
        "D · One sentence",
        "The input reads as a sentence with the choices inline. No separate set and reference rows.",
        layout_sentence,
    ),
    ("E · Chips", "The set row plus one chip per facet; a chip opens the per-project list.", layout_chips),
)


def render_io_layouts() -> None:
    with ui.column().style("padding: 18px 24px; gap: 22px; width: 100%; max-width: 1800px;"):
        ui.label("Particle-set inputs: five layouts").style(PAGE_SECTION_STYLE + " font-size: 14px;")
        ui.label(
            "Mock data. A Class 3D reading set s002 (2,708 picks from 7 projects), images from one Warp export per "
            "project. Menus and buttons change only this page."
        ).style(f"{SMALL} font-size: 10px; color: {PATH};")
        for title, blurb, fn in LAYOUTS:
            with ui.column().style("gap: 6px; width: 100%;"):
                ui.label(title).style(PAGE_SECTION_STYLE)
                ui.label(blurb).style(f"{SMALL} font-size: 10px; color: {PATH};")
                with ui.element("div").style(
                    "display: grid; grid-template-columns: repeat(auto-fill, minmax(540px, 1fr)); gap: 12px; "
                    "width: 100%; align-items: start;"
                ):
                    for kind in CASES:
                        _render_case(fn, Case(kind))


def _render_case(fn, case: Case) -> None:
    with ui.column().style("gap: 3px; min-width: 0;"):
        ui.label(CASE_TITLES[case.kind]).style(f"{SMALL} color: {PATH}; font-weight: 600;")

        @ui.refreshable
        def body() -> None:
            with config_section("Inputs"):
                fn(case, body.refresh)

        body()
