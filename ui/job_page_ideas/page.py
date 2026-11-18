"""`/dev/job-page-ideas`: the job page's upper half (Parameters, Inputs & Outputs, SLURM) laid out again, on real jobs
read and never written.

Round 2 (the maintainer took A2 Rail, B2 Rows, D2 Table):
H  header ladders (page, section, accented section, group): today's and three after Oxide's console, each on the
   picked page.
E  long paths: Cut, Wrap, Open.
F  checkboxes: today's against one whose label sits level with its box.

Round 1, kept below for reference:
A  the page: Strips, Rail, Flow. B  on/off options: Grid, Rows, Flags. C  the field geometry. D  Inputs & Outputs:
Sockets, Table, Chain.

The switches at the top apply to every option on the page: `?job=tm&head=rule&path=open&io=table&tog=rows`.
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from nicegui import ui

from ui.branch_map.style import INK, SOFT
from ui.job_page_ideas import heads, parts
from ui.job_page_ideas.data import L, M, SAMPLES, Param, S, load
from ui.job_page_ideas.parts import MONO, SANS, section

logger = logging.getLogger(__name__)

PANE_W = 1180  # the job pane at 1600 px with the rail and roster open
HALF_W = 560
JOBS = [("motion", "Motion & CTF"), ("tm", "Template match"), ("class3d", "Class 3D"), ("recon", "Reconstruct")]
IOS = [("sockets", "D1 Sockets"), ("table", "D2 Table"), ("chain", "D3 Chain")]
TOGS = [("grid", "B1 Grid"), ("rows", "B2 Rows"), ("flags", "B3 Flags")]
HEADS = [("today", "Today"), ("rule", "H1 Rule"), ("tab", "H2 Tab"), ("band", "H3 Band")]
PATHS = [("cut", "E1 Cut"), ("wrap", "E2 Wrap"), ("open", "E3 Open")]
RAIL_W = 360


def _panel(title: str, words: str, width: int):
    """One variant: its name, a line on it, then a frame as wide as where it would live. Classed for
    scripts/dev_screenshot.py --panels."""
    box = (
        ui.element("div")
        .classes("cb-mock-panel")
        .style("display: flex; flex-direction: column; gap: 6px; margin-top: 18px;")
    )
    with box:
        ui.label(title).classes("cb-mock-panel-title").style(
            f"{MONO} font-size: 9.5px; text-transform: uppercase; letter-spacing: .08em; color: {INK};"
        )
        ui.label(words).style(f"{SANS} font-size: 10.5px; color: {SOFT}; line-height: 1.45; max-width: 900px;")
        frame = ui.element("div").style(
            f"width: {width}px; background: #fff; border: 1px solid #e2e8f0; padding: 10px 14px 14px; "
            "display: flex; flex-direction: column; gap: 10px; box-sizing: border-box;"
        )
    return frame


def _heading(letter: str, title: str, words: str) -> None:
    ui.label(f"{letter} · {title}").style(f"{SANS} font-size: 13px; font-weight: 600; color: {INK}; margin-top: 30px;")
    ui.label(words).style(f"{SANS} font-size: 10.5px; color: {SOFT}; line-height: 1.45; max-width: 900px;")


def _switch(name: str, options: list[tuple[str, str]], current: str, q: dict) -> None:
    with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
        ui.label(name).style(
            f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; color: {SOFT}; width: 60px;"
        )
        with ui.element("div").style("display: inline-flex; border: 1px solid #cbd5e1;"):
            for i, (key, label) in enumerate(options):
                on = key == current
                href = "?" + urlencode({**q, name: key})
                ui.link(label, href).style(
                    f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; padding: 2px 9px; "
                    f"line-height: 14px; text-decoration: none; color: {INK if on else SOFT}; "
                    f"background: {'#dbe7f3' if on else '#fff'}; {'border-left: 1px solid #cbd5e1;' if i else ''}"
                )


def _folded(*titles: str) -> None:
    """The folded sections under the upper half, as they are today: only their title stripes."""
    for t in titles:
        with section(f"▸  {t}"):
            pass


# ── A: the page ──────────────────────────────────────────────────────────────


def layout_strips(s, io: str, tog: str) -> None:
    parts.header(s)
    parts.inputs_outputs(s, io)
    with section("Parameters"):
        parts.parameters(s, tog)
    parts.slurm_strip(s)
    _folded("Tasks", "Files")


def layout_rail(s, io: str, tog: str) -> None:
    parts.header(s)
    with ui.element("div").style(
        "display: grid; grid-template-columns: minmax(0, 1fr) 360px; gap: 18px; align-items: start;"
    ):
        with section("Parameters"):
            parts.parameters(s, tog)
        with ui.element("div").style("display: flex; flex-direction: column; gap: 10px;"):
            parts.inputs_outputs(s, io)
            parts.slurm_grid(s)
    _folded("Tasks", "Files")


def layout_flow(s, io: str, tog: str) -> None:
    parts.header(s)
    with ui.element("div").style("column-width: 360px; column-gap: 22px;"):
        for build in (
            lambda: parts.inputs_outputs(s, io),
            *[(lambda g=g: _group_section(g, tog)) for g in s.groups],
            lambda: parts.slurm_grid(s),
        ):
            with ui.element("div").style("break-inside: avoid; margin-bottom: 10px;"):
                build()
    _folded("Tasks", "Files")


def _group_section(g, tog: str) -> None:
    with section(g.title):
        parts.group(g, tog, title=False)


# ── H, E, F ──────────────────────────────────────────────────────────────────


def _ladder(s) -> None:
    """The look's four levels, one under the other."""
    with ui.element("div").style(
        "display: flex; flex-direction: column; gap: 10px; padding-bottom: 14px; margin-bottom: 6px; "
        "border-bottom: 1px dashed #cbd5e1;"
    ):
        heads.page_title(f"{s.name}", f"{s.number} · {s.rel(str(s.job_dir)) if s.job_dir else ''}")
        with section("Parameters", right=f"{len(s.groups)} groups"):
            heads.group_title(s.groups[0].title)
            for p in s.groups[0].fields[:2]:
                parts.field_row(p)
        with section("Log", right="failed 12:31", accent=True):
            ui.label("A section that asks to be looked at.").style(f"{SANS} font-size: 10px; color: {SOFT};")


def _paths(s, mode: str) -> None:
    rows = [(i.word, i.path) for i in s.inputs if i.path] + [(o.word, o.path) for o in s.outputs if o.path]
    rows.append(("outside", "/groups/<lab>/software/Napari/Napari_crboost/examples/412_shell_rotavg_job004.mrc"))
    with ui.element("div").style(
        "display: grid; grid-template-columns: 90px minmax(0, 1fr) 14px; align-items: start; gap: 4px 8px;"
    ):
        for word, p in rows:
            ui.label(word).style(parts.SLOT + " line-height: 14px;")
            heads.path(p, s.project, mode)
            parts.copy_icon(p)


def _checks() -> None:
    """Today's checkbox and ours, a red line along each box's bottom edge: today's label floats above it, ours sits
    on it."""
    with ui.element("div").classes("cbjp-guide").style("display: flex; gap: 60px;"):
        for title in ("today", "level"):
            with ui.element("div").style("display: flex; flex-direction: column; gap: 12px; width: 200px;"):
                ui.label(title).style(parts.SLOT)
                for lbl, on in (("Defocus weight", True), ("Spectral whitening", False), ("Pre-read images", True)):
                    if title == "today":
                        ui.checkbox(lbl, value=on).props("dense size=xs").style(f"{SANS} font-size: 10px;")
                    else:
                        heads.house_check(lbl, on)


# ── C: the geometry ──────────────────────────────────────────────────────────


def geometry() -> None:
    examples = [
        Param("n", "Classes", "6", False, False, S, ""),
        Param("n", "Mask diameter", "600", False, False, S, "Å"),
        Param("n", "Offset range", "3", False, False, S, "px"),
        Param("n", "Defocus range", "1.1:8", False, False, M, "µm"),
        Param("n", "Symmetry", "I1 · point group I1", False, False, M, ""),
        Param("n", "Template", "run_it015_class001_apix6.20_box128_black.mrc", False, False, L, ""),
        Param("n", "Leave out", "", False, False, L, ""),
    ]
    for p in examples:
        parts.field_row(p)


# ── the page ─────────────────────────────────────────────────────────────────


def render_job_page_ideas() -> None:
    """The page; its switches come from the address's query (`?job=…&head=…&path=…&io=…&tog=…`)."""
    params = ui.context.client.request.query_params

    def pick(name: str, options: list[tuple[str, str]], default: str) -> str:
        v = params.get(name, "")
        return v if v in dict(options) else default

    job = pick("job", JOBS, "class3d")
    head = pick("head", HEADS, "rule")
    path = pick("path", PATHS, "open")
    io = pick("io", IOS, "table")
    tog = pick("tog", TOGS, "rows")
    q = {"job": job, "head": head, "path": path, "io": io, "tog": tog}
    ui.add_css(parts.CSS + heads.PATH_CSS + heads.CHECK_CSS)
    parts.PATHS["mode"] = path
    s = load(job)
    with ui.column().style("padding: 18px 26px 60px; gap: 6px; background: #f1f5f9; min-height: 100vh;"):
        ui.label("Job page: Parameters, Inputs & Outputs, SLURM").style(
            f"{SANS} font-size: 16px; font-weight: 600; color: {INK};"
        )
        ui.label(
            "Real jobs, read and never written. Nothing here saves; a toggle flips on click so its look can be judged. "
            f"Showing {s.name} {s.number} of {s.project.name}. Taken in round 1: A2 Rail, B2 Rows, D2 Table."
        ).style(f"{SANS} font-size: 10.5px; color: {SOFT};")
        with ui.element("div").style("display: flex; flex-direction: column; gap: 5px; margin-top: 8px;"):
            _switch("job", JOBS, job, q)
            _switch("head", HEADS, head, q)
            _switch("path", PATHS, path, q)
            _switch("io", IOS, io, q)
            _switch("tog", TOGS, tog, q)

        _heading(
            "H",
            "Headers",
            "A ladder per look, most important first: the page (the job's title line), a section (Parameters, Inputs & "
            "Outputs, SLURM, Tasks), an accented section (one that asks to be looked at: a failed job's Log, Results), "
            "a group inside a section (Angles & offsets). The group is one step under the section, never a box. "
            "Under each ladder, the picked page drawn in it.",
        )
        for key, title, words in (
            (
                "today",
                "Today · Harbor Mono",
                "For comparison: the pale blue-grey band, its 2 px accent on the left and the hairline under it. The "
                "same at every level; a group is only bold text.",
            ),
            (
                "rule",
                "H1 · Rule",
                "Oxide's console: the section title in mono caps, a hairline running on from it to the edge, the meta "
                "at the far end. No fill, no corners. Accent: a square pip and the title and rule in blue. Group: sans "
                "semibold, no line.",
            ),
            (
                "tab",
                "H2 · Tab",
                "The section title in a flat inverted tab (ink, white mono caps) sitting on a full-width ink rule. "
                "Accent: the tab and rule in blue. Group: mono caps behind a small hollow square.",
            ),
            (
                "band",
                "H3 · Band",
                "A flat square band, ruled in ink along its top instead of on its left. Accent: a 2 px blue top "
                "rule on a bluer band. Group: sans semibold over a dotted hairline.",
            ),
        ):
            heads.use(key)
            with _panel(title, words, PANE_W):
                _ladder(s)
                layout_rail(s, io, tog)
        heads.use(head)

        _heading(
            "E",
            "Long paths",
            "Inside the project a path is written from the project folder; outside it, whole. Its file name is never "
            "cut. Shown in the rail's width, where paths are cut today. Hover shows the whole path; the copy icon "
            "beside it copies it.",
        )
        for key, title, words in (
            (
                "cut",
                "E1 · Cut",
                "One line: the folder gives way with an ellipsis at its end, the file name never does.",
            ),
            ("wrap", "E2 · Wrap", "Every part, wrapping only after a slash: complete, at the cost of lines."),
            ("open", "E3 · Open", "E1, and a click on it opens it to E2 in place; another click closes it."),
        ):
            with _panel(title, words, RAIL_W):
                _paths(s, key)

        _heading(
            "F",
            "Checkboxes",
            "Today's (Quasar's, as the job tabs draw it) beside one drawn by us: a 10 px box and its label centred on "
            "one 14 px line. The red lines are the box's top and bottom.",
        )
        with _panel("F · Today against level", "Click either to flip it.", HALF_W):
            _checks()

        ui.label("Round 1").style(f"{SANS} font-size: 15px; font-weight: 600; color: {INK}; margin-top: 40px;")
        _heading(
            "A",
            "The page",
            "Every option: one field geometry (C), the I/O block in the style chosen above (D), status lights as the "
            "roster's, the folder relative to the project with the full path behind the copy icon, Outputs inside "
            "Inputs & Outputs on the footer band. What changes is where the blocks go, so the right side stops "
            "outgrowing the left.",
        )
        for title, words, build in (
            (
                "A1 · Strips",
                "One column in reading order: what goes in and out, the parameters (groups in balanced columns, two or "
                "three across), the resources as one wrapping strip. Nodes and tasks show only when not 1; the "
                "supervisor is one folded line. Nothing sits beside anything, so nothing leaves a hole.",
                layout_strips,
            ),
            (
                "A2 · Rail",
                "Today's two columns, the right one a 360 px rail made short: I/O in the builder's rows, SLURM as a "
                "two-column grid, the supervisor folded. The parameters get the width back and flow into two columns.",
                layout_rail,
            ),
            (
                "A3 · Flow",
                "No columns of sections: every block (I/O, each parameter group as a section of its own, SLURM) flows "
                "into balanced columns as wide as the pane allows, the browser evening their heights.",
                layout_flow,
            ),
        ):
            with _panel(title, words, PANE_W):
                build(s, io, tog)

        _heading(
            "B",
            "On/off options",
            "Each toggle stays in the group it belongs to (CTF correction with CTF, Pre-read images with Compute), "
            "never "
            "one pile at the end. Three looks; click one to flip it.",
        )
        for key, title, words in (
            (
                "grid",
                "B1 · Grid",
                "Checkboxes in fixed 160 px cells from the value column: they line up across groups and jobs.",
            ),
            (
                "rows",
                "B2 · Rows",
                "A row each, like any field: the label in the label column, yes | no where the box would be.",
            ),
            (
                "flags",
                "B3 · Flags",
                "Chips from the value column, filled when on: the densest; reads as a set of switches.",
            ),
        ):
            with _panel(title, words, HALF_W):
                parts.parameters(s, key, flow=False)

        _heading(
            "C",
            "One field geometry",
            "A 132 px label column in the builder's slot voice; three box widths, S 64 px a number, M 150 px a choice "
            "or a short code, L to the column's edge for a path or a list; a 30 px unit column after S and M, so their "
            "boxes "
            "line up down a group whatever the unit. Symmetry is M, like every other choice.",
        )
        with _panel("C · Widths", "Examples from the three jobs.", HALF_W):
            geometry()

        _heading(
            "D",
            "Inputs & Outputs",
            "One block. Inputs as the builder's card says them; outputs under them on the card's footer band. A path "
            "is "
            "never printed whole: the source names it, the copy icon holds it, hovering shows it.",
        )
        for key, title, words in (
            (
                "sockets",
                "D1 · Sockets",
                "The large card's rows: socket, slot word, the source's light, name and number. A set's lineage and "
                "facets indent under it.",
            ),
            ("table", "D2 · Table", "One grid for in and out, the file's last two parts in a column of their own."),
            (
                "chain",
                "D3 · Chain",
                "The job in its place: its sources as small cards cabled into it, its outputs off its right side.",
            ),
        ):
            with _panel(title, words, HALF_W):
                parts.inputs_outputs(s, key)


__all__ = ["SAMPLES", "render_job_page_ideas"]
