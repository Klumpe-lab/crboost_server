"""`/dev/job-page-ideas`: the job page's upper half (Parameters, Inputs & Outputs, SLURM) laid out again, on real jobs
read and never written.

A  the page: Strips (one column, I/O and SLURM as bands), Rail (a narrow right rail), Flow (every block in balanced
   columns). Each at the job pane's real width.
B  on/off options: Grid, Rows, Flags.
C  the field geometry every option uses.
D  Inputs & Outputs: Sockets (the builder's card rows), Table, Chain.

The switches at the top (job, I/O style, toggle style) apply to every option on the page: `?job=tm&io=sockets&tog=grid`.
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from nicegui import ui

from ui.branch_map.style import INK, SOFT
from ui.job_page_ideas import parts
from ui.job_page_ideas.data import L, M, SAMPLES, Param, S, load
from ui.job_page_ideas.parts import MONO, SANS, section

logger = logging.getLogger(__name__)

PANE_W = 1180  # the job pane at 1600 px with the rail and roster open
HALF_W = 560
JOBS = [("motion", "Motion & CTF"), ("tm", "Template match"), ("class3d", "Class 3D"), ("recon", "Reconstruct")]
IOS = [("sockets", "D1 Sockets"), ("table", "D2 Table"), ("chain", "D3 Chain")]
TOGS = [("grid", "B1 Grid"), ("rows", "B2 Rows"), ("flags", "B3 Flags")]


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
    """The page; its switches come from the address's query (`?job=…&io=…&tog=…`)."""
    params = ui.context.client.request.query_params
    job, io, tog = params.get("job", ""), params.get("io", ""), params.get("tog", "")
    job = job if job in dict(JOBS) else "class3d"
    io = io if io in dict(IOS) else "sockets"
    tog = tog if tog in dict(TOGS) else "grid"
    q = {"job": job, "io": io, "tog": tog}
    ui.add_css(parts.CSS)
    s = load(job)
    with ui.column().style("padding: 18px 26px 60px; gap: 6px; background: #f1f5f9; min-height: 100vh;"):
        ui.label("Job page: Parameters, Inputs & Outputs, SLURM").style(
            f"{SANS} font-size: 16px; font-weight: 600; color: {INK};"
        )
        ui.label(
            "Real jobs, read and never written. Nothing here saves; a toggle flips on click so its look can be judged. "
            f"Showing {s.name} {s.number} of {s.project.name}."
        ).style(f"{SANS} font-size: 10.5px; color: {SOFT};")
        with ui.element("div").style("display: flex; flex-direction: column; gap: 5px; margin-top: 8px;"):
            _switch("job", JOBS, job, q)
            _switch("io", IOS, io, q)
            _switch("tog", TOGS, tog, q)

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
