"""The pieces every prototype layout is made of: the header line, a parameter group in each toggle style, the Inputs &
Outputs block in each style, SLURM as a strip or a grid. Display only: a toggle flips on click, nothing is saved.

One geometry for every field (the rule the layouts try out): a 132 px label column in the builder's slot voice (mono,
caps, muted), then the box in one of three widths (S 64 px a number, M 150 px a choice or a short code, L to the
column's edge for a path or a list), then a 30 px unit column, so S and M boxes line up down a group whatever their
unit."""

from __future__ import annotations

from nicegui import ui

from services.models_base import JobStatus
from ui.branch_map.style import BAND, CARD_BORDER, HAIR, INK, LINE, MUTED, SLATE, SOFT
from ui.components.copyable import copy_on_click
from ui.job_page_ideas.data import In, L, M, Param, S, Sample
from ui.job_plugins._field_styles import PANEL_HEAD_STYLE, PANEL_STYLE, PANEL_TITLE_STYLE
from ui.status_indicator import _dot_html

BODY = "#334155"
MONO = "font-family: var(--cb-mono, 'IBM Plex Mono', monospace);"
SANS = "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif);"
LABEL_W = 132
UNIT_W = 30
WIDTH = {S: "64px", M: "150px"}
SLOT = f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .06em; color: {SOFT}; white-space: nowrap;"
SMALL = f"{SANS} font-size: 9.5px; color: {MUTED};"
BOX = (
    f"height: 18px; box-sizing: border-box; border: 1px solid {CARD_BORDER}; border-radius: var(--cb-r-sm, 2px); "
    f"background: #fff; padding: 0 6px; {MONO} font-size: 10.5px; color: {INK}; line-height: 16px; "
    "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
)
# A path or a file's tail on one line, cut with an ellipsis.
PATH_TEXT = (
    f"{MONO} font-size: 9.5px; color: {SOFT}; min-width: 0; overflow: hidden; text-overflow: ellipsis; "
    "white-space: nowrap;"
)
ROW = "display: flex; align-items: center; gap: 8px; min-height: 20px; min-width: 0;"
GROUP_TITLE = f"{SANS} font-size: 10.5px; font-weight: 600; color: {INK}; margin: 2px 0 3px;"

CSS = f"""
.cbjp-tog {{ cursor: pointer; user-select: none; }}
.cbjp-check {{ width: 10px; height: 10px; box-sizing: border-box; border: 1px solid {SLATE};
  border-radius: var(--cb-r-sm, 2px); display: inline-flex; align-items: center; justify-content: center;
  color: transparent; font-size: 8px; line-height: 1; flex-shrink: 0; }}
.cbjp-tog.on .cbjp-check {{ background: {BODY}; border-color: {BODY}; color: #fff; }}
.cbjp-seg {{ display: inline-flex; border: 1px solid {CARD_BORDER}; border-radius: var(--cb-r-sm, 2px);
  overflow: hidden; height: 18px; box-sizing: border-box; }}
.cbjp-seg span {{ {MONO} font-size: 9px; padding: 0 7px; line-height: 16px; color: {MUTED}; }}
.cbjp-tog.on .cbjp-seg .y, .cbjp-tog:not(.on) .cbjp-seg .n {{ background: {BODY}; color: #fff; }}
.cbjp-flag {{ {MONO} font-size: 9px; line-height: 16px; padding: 0 7px; border: 1px solid {CARD_BORDER};
  border-radius: var(--cb-r-sm, 2px); color: {MUTED}; background: #fff; }}
.cbjp-flag.on {{ background: {BODY}; border-color: {BODY}; color: #fff; }}
.cbjp-sec > * {{ align-self: stretch; }}
.cbjp-flow {{ column-width: 330px; column-gap: 28px; }}
.cbjp-flow > * {{ break-inside: avoid; }}
.cbjp-copy {{ color: {MUTED}; cursor: pointer; }}
.cbjp-copy:hover {{ color: #2563eb; }}
.cbjp-fold > .cbjp-fold-body {{ display: none; }}
.cbjp-fold.open > .cbjp-fold-body {{ display: block; }}
"""

TOGGLE_JS = "(e) => e.currentTarget.classList.toggle('on')"


def light(status: JobStatus | None, px: int = 8) -> None:
    """The roster's status light (square in Harbor Mono), or a hollow square for something that is not a job."""
    if status is None:
        ui.element("div").style(
            f"width: {px}px; height: {px}px; box-sizing: border-box; border: 1.5px solid {SLATE}; "
            "border-radius: var(--cb-pip-radius, 2px); flex-shrink: 0;"
        )
        return
    ui.html(_dot_html(status, size_px=px), sanitize=False).style("display: flex; flex-shrink: 0;")


def copy_icon(path: str, what: str = "path") -> None:
    el = ui.icon("content_copy", size="11px").classes("cbjp-copy")
    copy_on_click(el, path)
    el.tooltip(f"Copy the {what}:\n{path}").style("white-space: pre-line;")


def pill(text: str) -> None:
    ui.label(text).style(
        f"{MONO} font-size: 8px; text-transform: uppercase; letter-spacing: .06em; color: {SOFT}; "
        f"border: 1px solid {LINE}; border-radius: var(--cb-r-sm, 2px); padding: 0 4px; line-height: 13px;"
    )


def section(title: str, *, right: str = "", extra: str = ""):
    """A section of the page: its title stripe and its content (one box deep)."""
    box = ui.column().classes("w-full cbjp-sec").style(f"gap: 6px; {PANEL_STYLE} {extra}")
    with box:
        with ui.element("div").style(f"{PANEL_HEAD_STYLE} display: flex; align-items: center;"):
            ui.label(title).style(PANEL_TITLE_STYLE)
            if right:
                ui.label(right).style(f"{SMALL} margin-left: auto;")
    return box


# ── the header line ──────────────────────────────────────────────────────────


def header(s: Sample) -> None:
    """Status, name, tool, number, and the folder relative to the project (the full path behind the copy)."""
    with ui.element("div").style(f"{ROW} gap: 8px; padding: 0 2px 4px;"):
        light(s.status, 9)
        ui.label(s.name).style(f"{SANS} font-size: 12px; font-weight: 600; color: {INK};")
        if s.tool:
            pill(s.tool)
        if s.number:
            ui.label(s.number).style(f"{MONO} font-size: 10px; color: {MUTED};")
        if s.species:
            ui.label(s.species).style(f"{MONO} font-size: 9px; color: {SOFT};")
        if s.job_dir:
            ui.label(s.rel(str(s.job_dir))).style(f"{MONO} font-size: 9.5px; color: {SOFT}; margin-left: 10px;")
            copy_icon(str(s.job_dir), "job folder")


# ── parameters ───────────────────────────────────────────────────────────────


def field_row(p: Param) -> None:
    with ui.element("div").style(ROW):
        ui.label(p.label).style(f"{SLOT} width: {LABEL_W}px; flex-shrink: 0;")
        if p.width == L:
            ui.label(p.value or "none").style(
                f"{BOX} flex: 1 1 0; min-width: 0; {'' if p.value else f'color: {MUTED}; font-style: italic;'}"
            )
            return
        ui.label(p.value).style(f"{BOX} width: {WIDTH[p.width]}; flex-shrink: 0;")
        ui.label(p.unit).style(f"{SANS} font-size: 9px; color: {MUTED}; width: {UNIT_W}px; flex-shrink: 0;")


def _toggle_el(p: Param, cls: str = "") -> ui.element:
    el = ui.element("div").classes(f"cbjp-tog {cls} {'on' if p.on else ''}")
    el.on("click", js_handler=TOGGLE_JS)
    return el


def toggles(ps: list[Param], style: str) -> None:
    """A group's on/off options.

    grid   checkboxes in fixed 160 px cells starting at the value column, so they line up across groups.
    rows   one row each, label in the label column, a yes|no switch where a box would be.
    flags  one chip each, filled when on, wrapping from the value column (`flags`).
    """
    if not ps:
        return
    if style == "flags":
        flags(ps)
        return
    if style == "rows":
        for p in ps:
            with ui.element("div").style(ROW):
                ui.label(p.label).style(f"{SLOT} width: {LABEL_W}px; flex-shrink: 0;")
                with _toggle_el(p):
                    ui.html(
                        "<span class='cbjp-seg'><span class='y'>yes</span><span class='n'>no</span></span>",
                        sanitize=False,
                    )
        return
    with ui.element("div").style(
        "display: grid; grid-template-columns: repeat(auto-fill, 160px); gap: 4px 6px; "
        f"margin: 3px 0 2px {LABEL_W + 8}px;"
    ):
        for p in ps:
            with _toggle_el(p).style("display: flex; align-items: center; gap: 6px; height: 16px;"):
                ui.html("<span class='cbjp-check'>✓</span>", sanitize=False)
                ui.label(p.label).style(f"{SANS} font-size: 10px; color: {BODY}; white-space: nowrap;")


def flags(ps: list[Param]) -> None:
    if not ps:
        return
    with ui.element("div").style(f"display: flex; flex-wrap: wrap; gap: 4px; margin: 3px 0 2px {LABEL_W + 8}px;"):
        for p in ps:
            el = ui.element("div").classes(f"cbjp-tog cbjp-flag {'on' if p.on else ''}")
            el.on("click", js_handler=TOGGLE_JS)
            with el:
                ui.label(p.label)


def group(g, tog: str, *, title: bool = True) -> None:
    with ui.element("div").style("display: flex; flex-direction: column; gap: 3px; margin-bottom: 10px;"):
        if title:
            ui.label(g.title).style(GROUP_TITLE)
        for p in g.fields:
            field_row(p)
        toggles(g.toggles, tog)


def parameters(s: Sample, tog: str, *, flow: bool = True) -> None:
    with ui.element("div").classes("cbjp-flow" if flow else "").style("width: 100%;"):
        for g in s.groups:
            group(g, tog)


# ── inputs & outputs ─────────────────────────────────────────────────────────


def _socket(filled: bool) -> None:
    ui.element("div").style(
        f"width: 8px; height: 8px; box-sizing: border-box; border-radius: var(--cb-bm-port-radius, 2px); "
        f"flex-shrink: 0; {'background: ' + SLATE + ';' if filled else 'border: 1.5px dashed ' + MUTED + ';'}"
    )


def _source(i: In) -> None:
    if i.source is None:
        ui.label("optional, none" if not i.required else "not chosen").style(
            f"{SANS} font-size: 10.5px; color: {MUTED if not i.required else '#b45309'}; font-style: italic;"
        )
        return
    light(i.source.status if i.source.status is not None else None, 7)
    ui.label(i.source.name).style(f"{SANS} font-size: 10.5px; color: {INK}; white-space: nowrap;")
    if i.source.number:
        ui.label(i.source.number).style(f"{MONO} font-size: 9.5px; color: {MUTED};")


def io_sockets(s: Sample) -> None:
    """The builder card's rows: a socket, the slot word, the source as the map names it; the path behind a copy
    icon (its tail on hover). Outputs below on the card footer's band."""
    for i in s.inputs:
        with ui.element("div").style(f"{ROW} gap: 7px;"):
            _socket(i.source is not None)
            ui.label(i.word).style(f"{SLOT} width: 104px; flex-shrink: 0;")
            _source(i)
            if i.source is not None and i.source.name == "File":
                ui.label(s.rel(i.path)).style(PATH_TEXT)
            ui.element("div").style("flex: 1;")
            if i.path:
                copy_icon(i.path, "input path")
        if i.note or i.detail:
            with ui.element("div").style(
                f"display: flex; flex-direction: column; gap: 1px; margin: -1px 0 3px {15 + 104 + 7}px;"
            ):
                if i.note:
                    ui.label(i.note).style(f"{SANS} font-size: 9.5px; color: {SOFT};")
                for word, text in i.detail:
                    with ui.element("div").style("display: flex; gap: 6px; align-items: baseline;"):
                        ui.label(word).style(f"{SLOT} font-size: 8px; width: 44px;")
                        ui.label(text).style(f"{SANS} font-size: 9.5px; color: {BODY};")
    with ui.element("div").style(
        f"background: {BAND}; border-top: 1px solid {HAIR}; margin: 6px -10px -6px; padding: 5px 10px 6px; "
        "display: flex; flex-direction: column; gap: 2px;"
    ):
        for o in s.outputs:
            with ui.element("div").style(f"{ROW} gap: 7px; min-height: 18px;"):
                ui.label("→").style(f"{SANS} font-size: 10px; color: {MUTED}; width: 8px;")
                ui.label(o.word).style(
                    f"{SANS} font-size: 10.5px; color: {BODY}; width: 150px; flex-shrink: 0; white-space: nowrap; "
                    "overflow: hidden; text-overflow: ellipsis;"
                )
                ui.label(s.rel(o.path) if o.path else "after it runs").style(
                    f"{MONO} font-size: 9.5px; color: {SOFT}; min-width: 0; overflow: hidden; "
                    "text-overflow: ellipsis; white-space: nowrap; direction: rtl; text-align: left;"
                )
                ui.element("div").style("flex: 1;")
                if o.path:
                    copy_icon(o.path, "output path")


def io_table(s: Sample) -> None:
    """One table for both: in and out rows, the file's tail in its own column, cut from the left."""
    cell = f"{SANS} font-size: 10.5px; color: {INK}; padding: 2px 8px 2px 0; white-space: nowrap;"
    with ui.element("div").style(
        "display: grid; grid-template-columns: 26px 104px auto 1fr 14px; align-items: center; row-gap: 1px;"
    ):
        for i in s.inputs:
            ui.label("in").style(f"{SLOT} font-size: 7.5px;")
            ui.label(i.word).style(SLOT)
            with ui.element("div").style(f"{ROW} gap: 6px; padding-right: 10px;"):
                _source(i)
            ui.label(_tail(i.path)).style(PATH_TEXT)
            if i.path:
                copy_icon(i.path)
            else:
                ui.element("div")
        ui.element("div").style(f"grid-column: 1 / -1; height: 1px; background: {HAIR}; margin: 4px 0;")
        for o in s.outputs:
            ui.label("out").style(f"{SLOT} font-size: 7.5px;")
            ui.label(o.word).style(SLOT)
            ui.label("this job").style(f"{cell} color: {MUTED};")
            ui.label(_tail(o.path) or "after it runs").style(PATH_TEXT)
            if o.path:
                copy_icon(o.path)
            else:
                ui.element("div")


def _tail(path: str) -> str:
    """The last two parts of a path: job008/tomograms.star."""
    if not path:
        return ""
    parts = path.rstrip("/").split("/")
    return "/".join(parts[-2:])


def io_chain(s: Sample) -> None:
    """The job in its place on the map: its sources as small cards on the left, cables into this job's card, its
    outputs off the right side."""
    rows = max(len(s.inputs), 1)
    h = rows * 30
    with ui.element("div").style("display: flex; align-items: stretch; gap: 0;"):
        with ui.element("div").style("display: flex; flex-direction: column; gap: 6px; width: 210px;"):
            for i in s.inputs:
                with ui.element("div").style(
                    f"{ROW} gap: 6px; height: 24px; padding: 0 7px; "
                    f"border: 1px {'solid' if i.source else 'dashed'} {CARD_BORDER}; "
                    f"border-radius: var(--cb-bm-card-radius, 4px); background: #fff;"
                ):
                    _source(i)
                    ui.element("div").style("flex: 1;")
                    ui.label(i.word).style(f"{SLOT} font-size: 7.5px;")
        dash = "stroke-dasharray='3 3'"
        paths = "".join(
            f"<path d='M0 {12 + k * 30} C 22 {12 + k * 30}, 22 {h / 2}, 44 {h / 2}' stroke='{MUTED}' fill='none' "
            f"stroke-width='1.2' {dash if s.inputs[k].source is None else ''}/>"
            for k in range(len(s.inputs))
        )
        ui.html(f"<svg width='44' height='{h}' style='display:block'>{paths}</svg>", sanitize=False)
        with ui.element("div").style(
            f"align-self: center; {ROW} gap: 6px; height: 26px; padding: 0 9px; border: 1.5px solid {SLATE}; "
            "border-radius: var(--cb-bm-card-radius, 4px); background: #fff;"
        ):
            light(s.status, 8)
            ui.label(s.name).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK}; white-space: nowrap;")
        with ui.element("div").style(
            "align-self: center; display: flex; flex-direction: column; gap: 2px; margin-left: 10px; min-width: 0;"
        ):
            for o in s.outputs:
                with ui.element("div").style(f"{ROW} gap: 6px; min-height: 16px;"):
                    ui.label("→").style(f"{SANS} font-size: 10px; color: {MUTED};")
                    ui.label(o.word).style(f"{SANS} font-size: 10.5px; color: {BODY}; white-space: nowrap;")
                    if o.path:
                        copy_icon(o.path, "output path")
    notes = [i for i in s.inputs if i.note or i.detail]
    for i in notes:
        with ui.element("div").style("margin-top: 6px; display: flex; flex-direction: column; gap: 1px;"):
            ui.label(f"{i.word}: {i.note}").style(f"{SANS} font-size: 9.5px; color: {SOFT};")
            for word, text in i.detail:
                ui.label(f"{word} · {text}").style(f"{SANS} font-size: 9.5px; color: {BODY};")


IO = {"sockets": io_sockets, "table": io_table, "chain": io_chain}


def inputs_outputs(s: Sample, style: str) -> None:
    with section("Inputs & Outputs", right="fixed at submit" if s.fixed else "chosen at Run"):
        IO[style](s)


# ── SLURM ────────────────────────────────────────────────────────────────────

# A field that holds its default (nothing asked) and adds nothing to read: shown only on "all".
_QUIET = {"nodes": "1", "ntasks_per_node": "1"}


def _slurm_cells(s: Sample) -> list[tuple[str, str]]:
    return [(label, v) for f, label, v in s.slurm if _QUIET.get(f) != v]


def slurm_strip(s: Sample) -> None:
    """One wrapping row of label-over-value cells; nodes and tasks only when not 1; the supervisor one folded line."""
    with section("SLURM resources", right=f"profile {s.profile}" if s.profile else ""):
        with ui.element("div").style("display: flex; flex-wrap: wrap; gap: 6px 14px;"):
            if s.throttle is not None:
                _cell("At once", str(s.throttle), "56px")
            for label, v in _slurm_cells(s):
                _cell(label, v, "auto")
        _supervisor(s)


def _cell(label: str, value: str, width: str) -> None:
    with ui.element("div").style("display: flex; flex-direction: column; gap: 1px;"):
        ui.label(label).style(f"{SLOT} font-size: 7.5px;")
        ui.label(value or "—").style(f"{BOX} min-width: 56px; width: {width}; {'' if value else f'color: {MUTED};'}")


def slurm_grid(s: Sample) -> None:
    """Two columns of label / value pairs at the page's field geometry."""
    with section("SLURM resources", right=f"profile {s.profile}" if s.profile else ""):
        cells = ([("At once", str(s.throttle))] if s.throttle is not None else []) + _slurm_cells(s)
        with ui.element("div").style("display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 3px 16px;"):
            for label, v in cells:
                with ui.element("div").style(ROW):
                    ui.label(label).style(f"{SLOT} width: 66px; flex-shrink: 0;")
                    ui.label(v or "—").style(
                        f"{BOX} flex: 1 1 0; min-width: 0; max-width: 150px; {'' if v else f'color: {MUTED};'}"
                    )
        _supervisor(s)


def _supervisor(s: Sample) -> None:
    if not s.supervisor:
        return
    shown = dict(s.supervisor)
    summary = " · ".join(shown[k] for k in ("Partition", "GRES", "Memory", "Time limit", "QOS") if shown.get(k))
    fold = ui.element("div").classes("cbjp-fold").style("margin-top: 4px;")
    with fold:
        head = ui.element("div").style(f"{ROW} gap: 6px; cursor: pointer; min-height: 16px;")
        head.on("click", js_handler="(e) => e.currentTarget.parentElement.classList.toggle('open')")
        with head:
            ui.label("Supervisor").style(f"{SLOT}")
            ui.label(summary).style(f"{MONO} font-size: 9.5px; color: {MUTED};")
            ui.label("▸").style(f"font-size: 9px; color: {MUTED};")
        with ui.element("div").classes("cbjp-fold-body").style("margin-top: 3px;"):
            ui.label(
                "The supervisor dispatches the array's tasks and gathers their results; set in conf.yaml, the same "
                "for every array job."
            ).style(f"{SANS} font-size: 9.5px; color: {SOFT}; margin-bottom: 3px;")
            with ui.element("div").style(
                "display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 2px 16px;"
            ):
                for label, v in s.supervisor:
                    with ui.element("div").style(ROW):
                        ui.label(label).style(f"{SLOT} width: 76px;")
                        ui.label(v).style(f"{MONO} font-size: 10px; color: {SOFT};")


SLURM = {"strip": slurm_strip, "grid": slurm_grid}
