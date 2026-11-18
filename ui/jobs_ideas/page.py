"""`/dev/jobs-ideas`: prototypes for how jobs are shown, on a real project read and never written.

A  the roster with a gutter of job types (four variants)
B  the magazine popup a job with tasks opens, and the task card beside it (three variants, one switch)
C  the pipeline as a graph (four variants)
D  toasts (four variants beside today's)

`?project=<path>` picks the project (default: an AGG412 rerun), `?sim=1` repaints a few jobs as a run in flight.
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlencode

from nicegui import Client, ui

from ui.jobs_ideas.data import DEFAULT_PROJECT, load
from ui.jobs_ideas.graphs import Graphs
from ui.jobs_ideas.look import BODY, CSS, HEAD, INK, LINE, MONO, MUTED, PANEL_LINE, SANS, SOFT
from ui.jobs_ideas.popups import ANCHOR_JS, Popups
from ui.jobs_ideas.roster import Roster
from ui.jobs_ideas.toasts import render_toasts

logger = logging.getLogger(__name__)

_ESC_JS = (
    "if (!window.cbjiEsc) { window.cbjiEsc = true; document.addEventListener('keydown', e => { "
    "if (e.key === 'Escape') emitEvent('cbji_esc'); }); "
    "window.addEventListener('scroll', () => emitEvent('cbji_esc'), {passive: true}); }"
)

ROSTERS = [
    (
        "bands",
        "A1 · Bands",
        "Each type a band, every other one shaded. The glyph sits on the type's first row; a "
        "`+` hangs off its corner once the type has a job.",
    ),
    (
        "rail",
        "A2 · Rail",
        "The gutter is a rail in the title-stripe tint, one cell per type, hairlines between types. "
        "Hover a type and its glyph turns into the `+`.",
    ),
    (
        "grid",
        "A3 · Grid",
        "Two gutter cells, the glyph and the `+`, always there. The phases are stripes across the "
        "roster, hairlines between types.",
    ),
    (
        "ledger",
        "A4 · Ledger",
        "A2's gutter, and each row carries its magazine in miniature instead of a count; a click "
        "on it opens the magazine.",
    ),
]
POPUPS = [
    ("beside", "B1 · Beside", "The card beside the row; the task card beside the card."),
    ("sheet", "B2 · Sheet", "One sheet mid-screen over a light veil; the task docked inside on the right."),
    ("tiles", "B3 · Tiles", "As B1, the marks bigger and named Pos·Beam: readable without a hover."),
]
GRAPHS = [
    (
        "subway",
        "C1 · Subway",
        "Stages as columns left to right, each branch a line of its own. A station is the job's "
        "light; under it its number and its magazine as a strip. Dashed: a second input (CTF into template matching).",
    ),
    (
        "gitlog",
        "C2 · Git log",
        "One row per job in job-number order, the branches in a narrow gutter on the left. "
        "Reads as a table, so it carries tool, species, count and magazine in columns.",
    ),
    (
        "icicle",
        "C3 · Icicle",
        "Stages as columns, each job a block as tall as the branches under it: a shared start "
        "spans every branch that reads it. A thin bridge crosses a stage a branch skips.",
    ),
    (
        "tree",
        "C4 · Tree listing",
        "An indented listing in box-drawing lines; a chain stays on one level and only a fork "
        "indents, so a twelve-job chain does not walk off to the right.",
    ),
]


def _panel(title: str, words: str):
    """One variant: its name and a line on it, then its content. Classed for scripts/dev_screenshot.py --panels."""
    box = ui.element("div").classes("cb-mock-panel").style("display: flex; flex-direction: column; gap: 6px;")
    with box:
        ui.label(title).classes("cb-mock-panel-title").style(
            f"{MONO} font-size: 9.5px; text-transform: uppercase; letter-spacing: .08em; color: {INK};"
        )
        ui.label(words).style(f"{SANS} font-size: 10px; color: {SOFT}; line-height: 1.4; max-width: 640px;")
    return box


def _section(letter: str, title: str, words: str) -> None:
    with ui.element("div").style("display: flex; align-items: baseline; gap: 10px; margin-top: 26px;"):
        ui.label(f"{letter} · {title}").style(f"{SANS} font-size: 13px; font-weight: 600; color: {INK};")
    ui.label(words).style(f"{SANS} font-size: 10.5px; color: {SOFT}; line-height: 1.45; max-width: 900px;")


def _seg(options: list[tuple[str, str]], current: str, on_pick) -> None:
    with ui.element("div").style(f"display: inline-flex; border: 1px solid {PANEL_LINE};"):
        for i, (key, label) in enumerate(options):
            on = key == current
            ui.label(label).classes("cbji-seg").style(
                f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; padding: 2px 8px; "
                f"line-height: 14px; color: {INK if on else SOFT}; background: {'#dbe7f3' if on else '#fff'}; "
                f"{'border-left: 1px solid ' + PANEL_LINE + ';' if i else ''}"
            ).on("click", lambda _e, k=key: on_pick(k))


async def render_jobs_ideas(client: Client) -> None:
    q = client.request.query_params
    project = Path(q.get("project") or DEFAULT_PROJECT)
    sim = q.get("sim") == "1"
    ui.add_css(CSS)
    ui.run_javascript(_ESC_JS)
    ui.query("body").style("background: #fff;")
    page = ui.element("div").style("padding: 20px 28px 80px; display: flex; flex-direction: column; gap: 6px;")
    with page:
        loading = ui.label(f"Reading {project} …").style(f"{MONO} font-size: 10px; color: {MUTED};")
    try:
        data = await load(project, simulate=sim)
    except Exception as e:
        logger.exception("jobs ideas: loading %s", project)
        loading.set_text(f"Could not read {project}: {e}")
        return
    loading.delete()
    pop = Popups(data)
    state = {"icons": "drawn"}

    def icons() -> str:
        return state["icons"]

    with page:
        ui.label("Jobs: the roster, the magazine, the graph, the toasts").style(
            f"{SANS} font-size: 16px; font-weight: 600; color: {INK};"
        )
        with ui.element("div").style("display: flex; gap: 18px; align-items: center; flex-wrap: wrap;"):
            ui.label(f"{data.pr.name} · {len(data.pr.ts_ids)} tilt series · {len(data.pr.jobs)} jobs").style(
                f"{MONO} font-size: 9.5px; color: {BODY};"
            )

            def _nav(**kw) -> None:
                params = {"project": str(project), "sim": "1" if sim else "0", **kw}
                ui.navigate.to(f"/dev/jobs-ideas?{urlencode(params)}")

            with ui.element("div").style("display: flex; gap: 6px; align-items: center;"):
                ui.label("States").style(f"{MONO} font-size: 8.5px; text-transform: uppercase; color: {MUTED};")
                _seg(
                    [("0", "as on disk"), ("1", "a run in flight (simulated)")],
                    "1" if sim else "0",
                    lambda k: _nav(sim=k),
                )
            with ui.element("div").style("display: flex; gap: 6px; align-items: center;"):
                ui.label("Glyphs").style(f"{MONO} font-size: 8.5px; text-transform: uppercase; color: {MUTED};")

                @ui.refreshable
                def icon_seg() -> None:
                    _seg([("drawn", "drawn"), ("material", "material (today's map)")], state["icons"], _pick_icons)

                def _pick_icons(k: str) -> None:
                    state["icons"] = k
                    icon_seg.refresh()
                    rosters.refresh()
                    graphs.refresh()

                icon_seg()
            with ui.element("div").style("display: flex; gap: 6px; align-items: center;"):
                ui.label("Magazine").style(f"{MONO} font-size: 8.5px; text-transform: uppercase; color: {MUTED};")

                @ui.refreshable
                def pop_seg() -> None:
                    _seg([(k, t.split(" · ")[1]) for k, t, _ in POPUPS], pop.variant, _pick_pop)

                def _pick_pop(k: str) -> None:
                    pop.close()
                    pop.variant = k
                    pop_seg.refresh()

                pop_seg()
        if data.simulated:
            ui.label(
                "Simulated: the last array jobs are repainted as running (with failures and a queue), queued and "
                "failed. Nothing on disk says so."
            ).style(
                f"{SANS} font-size: 10px; color: #8a4b06; background: #fdf3e3; padding: 3px 8px; align-self: "
                "flex-start;"
            )

        _section(
            "A",
            "The roster, with a gutter of job types",
            "Left: the canonical job types in pipeline order, one glyph each; right: the jobs of each type, a grey "
            "template row where there are none (click it to add one). No type headers, no clone icons, no drop-down. "
            "A job's name opens its page; its count (A4: its strip) opens the magazine. × shows on hover.",
        )

        @ui.refreshable
        def rosters() -> None:
            with ui.element("div").style("display: flex; gap: 22px; flex-wrap: wrap; align-items: flex-start;"):
                for key, title, words in ROSTERS:
                    with _panel(title, words).style("width: 318px;"):
                        Roster(data, pop, key, icons).render()

        rosters()

        _section(
            "B",
            "The magazine",
            "What a job with tasks opens: its description, what it reads, its folder, its tasks counted, and the "
            "magazine as square as the count allows. Green done, amber waiting in the queue, blue blinking running, "
            "red failed, grey not run (or skipped, lighter). Hover a mark for its task card beside the magazine; a "
            "click pins it. Esc or scrolling closes. Switch the variant at the top; any count in A or job in C opens "
            "it.",
        )
        with ui.element("div").style("display: flex; gap: 8px; flex-wrap: wrap;"):
            arrays = [j for j in data.pr.jobs if j.id in data.has_tasks]
            for j in arrays:
                with (
                    ui.element("div")
                    .classes("cbji-row")
                    .props(f'data-cbji-anchor="{j.id}"')
                    .style(
                        f"display: flex; gap: 5px; align-items: center; border: 1px solid {PANEL_LINE}; padding: 2px "
                        "7px;"
                    )
                ) as chip:
                    ui.label(f"{data.name(j)} {j.number}").style(f"{MONO} font-size: 9.5px; color: {BODY};")
                chip.on("click", lambda e, iid=j.id: pop.open(iid, e.args), js_handler=ANCHOR_JS)

        _section(
            "C",
            "The pipeline as a graph",
            "Laid out by rule: a job's column is its type's place in the pipeline, its line is the job it reads. "
            "Nothing to drag, nothing that moves between visits. A click on a job with tasks opens its magazine.",
        )

        @ui.refreshable
        def graphs() -> None:
            g = Graphs(data, pop, icons)
            for key, title, words in GRAPHS:
                with _panel(title, words).style("width: 100%; margin-top: 10px;"):
                    with ui.element("div").style(
                        f"border: 1px solid {PANEL_LINE}; align-self: flex-start; max-width: 100%;"
                    ):
                        with ui.element("div").style(
                            f"height: 24px; background: {HEAD}; border-bottom: 1px solid {LINE}; display: flex; "
                            "align-items: center; padding: 0 8px;"
                        ):
                            ui.label("Pipeline").style(
                                f"{MONO} font-size: 9px; text-transform: uppercase; letter-spacing: .08em; color: "
                                f"{INK};"
                            )
                        g.render(key)

        graphs()

        _section(
            "D",
            "Toasts",
            "Second round, in the app's own chrome: the popup card's edge, the title stripe, the square status pip, "
            "the "
            "label voice; dark ink on light everywhere. Each is a rule over every ui.notify the app raises, the kind "
            "(word and pip) taken from the type the call already passes. Shown at rest; the buttons fire the real one.",
        )
        with _panel("D · Toasts", "T0 for copies; T1–T3 for everything."):
            render_toasts()

        _section(
            "E",
            "The Pipeline view first",
            "Built in the app, not here: opening a project shows the Pipeline view with no job open, unless this tab "
            "already had one of this project's jobs open (ui/ui_state.py load_from_project).",
        )
