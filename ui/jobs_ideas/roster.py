"""The job roster with a gutter of job types: on the left the canonical list of types, one glyph each, in pipeline
order; on the right the jobs of each type, as many rows as there are jobs, a grey template row where there are
none. No per-type header rows, no clone icons, no drop-down under a row: a job with tasks opens its magazine.

Four variants, one row grammar:
- `bands`   each type a band, shaded every other type; the add `+` a subscript on the glyph
- `rail`    the gutter a rail in the title-stripe tint, one cell per type; hovering a type turns its glyph into `+`
- `grid`    the gutter two cells wide, glyph and `+`, hairlines between types across the roster
- `ledger`  as `rail`, but each row carries its magazine in miniature instead of a count
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from ui.branch_map.model import Job
from ui.jobs_ideas.data import ADDABLE, DISPLAY, PHASES, Data
from ui.jobs_ideas.glyphs import glyph
from ui.jobs_ideas.look import BODY, GLYPH, HEAD, INK, LINE, MONO, MUTED, PANEL_LINE, SOFT, light, micro_magazine, tally
from ui.jobs_ideas.popups import ANCHOR_JS, Popups

RH = 22  # one row
WIDTH = 318

_NAME = f"{MONO} font-size: 10px; color: {BODY}; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
_NUM = f"{MONO} font-size: 8px; color: {MUTED}; flex-shrink: 0;"
_TAG = (
    f"{MONO} font-size: 7.5px; text-transform: uppercase; letter-spacing: .05em; color: #4f6782; "
    "border: 1px solid #dbe3ec; border-radius: 2px; padding: 0 3px; line-height: 11px; flex-shrink: 0; "
    "max-width: 15ch; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
)
_PHASE = (
    f"{MONO} font-size: 8px; text-transform: uppercase; letter-spacing: .08em; color: {MUTED}; "
    "padding: 9px 0 3px 8px; line-height: 1;"
)


class Roster:
    def __init__(self, data: Data, popups: Popups, variant: str, icons: Callable[[], str]) -> None:
        self.d = data
        self.pop = popups
        self.variant = variant
        self.icons = icons

    # ── the row grammar ──

    def _row(self, j: Job, siblings: int, first: bool) -> None:
        d = self.d
        has_tasks = j.id in d.has_tasks
        row = (
            ui.element("div")
            .classes("cbji-row")
            .props(f'data-cbji-anchor="{j.id}"')
            .style(f"display: flex; align-items: center; gap: 6px; height: {RH}px; padding: 0 6px 0 8px; min-width: 0;")
            .on("click", lambda _e, j=j: ui.notify(f"Opens {d.name(j)} {j.number or '(new)'}'s page"))
        )
        with row:
            ui.html(light(j.status), sanitize=False).style("display: flex; flex-shrink: 0;")
            ui.label(d.name(j)).style(f"{_NAME} flex: 0 1 auto; min-width: 0;")
            if j.number:
                ui.label(j.number).style(_NUM)
            # What tells instances of one type apart: the species, else the method, else the tool when it differs.
            tag = d.pr.species_names.get(j.species, j.species) if j.species else (j.method if siblings > 1 else "")
            if tag:
                ui.label(tag).style(_TAG).props(f'title="{tag}"')
            ui.element("div").style("flex: 1 1 auto;")
            if has_tasks:
                tasks = d.tasks(j)
                if self.variant == "ledger":
                    trig = ui.html(micro_magazine([s for _, s in tasks], width=58, height=9), sanitize=False)
                    trig.classes("cbji-tally").style("display: flex; padding: 2px; cursor: pointer;")
                else:
                    text, ink = tally(d.counts(j), j.status)
                    trig = (
                        ui.label(text)
                        .classes("cbji-tally")
                        .style(
                            f"{MONO} font-size: 8.5px; color: {ink}; cursor: pointer; line-height: 14px; "
                            "flex-shrink: 0;"
                        )
                    )
                trig.props('title="Its tilt series: the magazine"')
                trig.on("click", lambda e, iid=j.id: self.pop.open(iid, e.args), js_handler=ANCHOR_JS)
            ui.icon("close", size="11px").classes("cbji-x").style(f"color: {MUTED}; flex-shrink: 0;").on(
                "click.stop", lambda _e, j=j: ui.notify(f"Removes {d.name(j)} {j.number} (to Trash)")
            ).props('title="Remove"')

    def _ghost(self, kind: str) -> None:
        name = DISPLAY.get(kind, kind)
        with (
            ui.element("div")
            .classes("cbji-ghost")
            .style(f"display: flex; align-items: center; gap: 6px; height: {RH}px; padding: 0 6px 0 8px;")
            .on("click", lambda _e: ui.notify(f"Adds a {name}"))
        ):
            ui.html(
                '<span style="display:inline-block;width:7px;height:7px;box-sizing:border-box;'
                'border:1px dashed #c5d3e2;border-radius:1px;"></span>',
                sanitize=False,
            ).style("display: flex;")
            ui.label(name).style(f"{MONO} font-size: 10px; color: #b3c0cf; white-space: nowrap;")
            ui.element("div").style("flex: 1 1 auto;")
            ui.label("+ add").classes("cbji-ghost-add").style(
                f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; color: {SOFT};"
            )

    def _glyph(self, kind: str, ink: str = GLYPH) -> str:
        return glyph(kind, ink=ink, icons=self.icons())

    def _add(self, kind: str, size: int = 9, extra: str = "", cls: str = "") -> None:
        name = DISPLAY.get(kind, kind)
        ui.icon("add", size=f"{size}px").classes(f"cbji-add {cls}").style(extra).props(
            f'title="Add another {name}"'
        ).on("click.stop", lambda _e: ui.notify(f"Adds another {name}"))

    # ── the variants ──

    def render(self) -> None:
        with ui.element("div").style(
            f"width: {WIDTH}px; border: 1px solid {PANEL_LINE}; background: #fff; display: flex; "
            "flex-direction: column; align-self: flex-start;"
        ):
            with ui.element("div").style(
                f"display: flex; align-items: center; gap: 6px; height: 24px; padding: 0 8px; background: {HEAD}; "
                f"border-bottom: 1px solid {LINE};"
            ):
                ui.label("Jobs").style(
                    f"{MONO} font-size: 9px; text-transform: uppercase; letter-spacing: .08em; color: {INK};"
                )
            with ui.element("div").style("display: flex; flex-direction: column; padding-bottom: 6px;"):
                getattr(self, f"_{self.variant}")()

    def _types(self):
        """(phase label, [(kind, jobs)]) in canonical order; a type that is only made elsewhere shows when it has a
        job."""
        for _pid, label, kinds in PHASES:
            out = []
            for kind in kinds:
                jobs = self.d.instances(kind)
                if jobs or kind in ADDABLE:
                    out.append((kind, jobs))
            yield label, out

    def _bands(self) -> None:
        g = 26
        k = 0
        for label, types in self._types():
            ui.label(label).style(_PHASE)
            for kind, jobs in types:
                shade = "#f6f8fb" if k % 2 else "#ffffff"
                k += 1
                with ui.element("div").style(f"display: flex; background: {shade};"):
                    with ui.element("div").style(f"width: {g}px; flex-shrink: 0; position: relative;"):
                        ui.html(self._glyph(kind, GLYPH if jobs else "#b3c0cf"), sanitize=False).style(
                            f"position: absolute; left: {(g - 12) / 2}px; top: {(RH - 12) / 2}px;"
                        ).props(f'title="{DISPLAY.get(kind, kind)}"')
                        if jobs and kind in ADDABLE:
                            self._add(kind, 9, "position: absolute; left: 15px; top: 11px; background: transparent;")
                    with ui.element("div").style("flex: 1 1 auto; min-width: 0;"):
                        if not jobs:
                            self._ghost(kind)
                        for i, j in enumerate(jobs):
                            self._row(j, len(jobs), i == 0)

    def _rail(self) -> None:
        """Hovering a type turns its glyph into the `+` that adds another of it."""
        g = 28
        for n, (_label, types) in enumerate(self._types()):
            if n:
                # The phases meet at a gap in the rail.
                ui.element("div").style(f"height: 8px; border-top: 1px solid {LINE};")
            for kind, jobs in types:
                with ui.element("div").classes("cbji-band").style("display: flex; border-bottom: 1px solid #fff;"):
                    with ui.element("div").style(
                        f"width: {g}px; flex-shrink: 0; position: relative; background: {HEAD}; "
                        f"border-right: 1px solid {LINE};"
                    ):
                        ink = GLYPH if jobs else "#b3c0cf"
                        ui.html(self._glyph(kind, ink), sanitize=False).classes("cbji-hover-glyph").style(
                            f"position: absolute; left: {(g - 12) / 2}px; top: {(RH - 12) / 2}px; transition: "
                            "opacity .1s;"
                        ).props(f'title="{DISPLAY.get(kind, kind)}"')
                        if jobs and kind in ADDABLE:
                            self._add(
                                kind,
                                13,
                                f"position: absolute; left: {(g - 14) / 2}px; top: {(RH - 14) / 2}px; opacity: 0; "
                                "transition: opacity .1s; color: #12324f;",
                                cls="cbji-hover-add",
                            )
                    with ui.element("div").style(f"flex: 1 1 auto; min-width: 0; border-bottom: 1px solid {LINE};"):
                        if not jobs:
                            self._ghost(kind)
                        for i, j in enumerate(jobs):
                            self._row(j, len(jobs), i == 0)

    def _grid(self) -> None:
        for label, types in self._types():
            with ui.element("div").style(
                f"display: flex; align-items: center; height: 18px; background: {HEAD}; border-top: 1px solid {LINE}; "
                f"border-bottom: 1px solid {LINE};"
            ):
                ui.label(label).style(
                    f"{MONO} font-size: 8px; text-transform: uppercase; letter-spacing: .08em; color: {SOFT}; "
                    "padding-left: 8px;"
                )
            for kind, jobs in types:
                with ui.element("div").style(f"display: flex; border-bottom: 1px solid {LINE};"):
                    with ui.element("div").style(
                        f"width: 22px; flex-shrink: 0; display: flex; justify-content: center; padding-top: "
                        f"{(RH - 12) / 2}px; background: #f8fafc; border-right: 1px solid {LINE};"
                    ):
                        ui.html(self._glyph(kind, GLYPH if jobs else "#b3c0cf"), sanitize=False).props(
                            f'title="{DISPLAY.get(kind, kind)}"'
                        )
                    with ui.element("div").style(
                        f"width: 16px; flex-shrink: 0; display: flex; justify-content: center; padding-top: 5px; "
                        f"background: #f8fafc; border-right: 1px solid {LINE};"
                    ):
                        if jobs and kind in ADDABLE:
                            self._add(kind, 11)
                    with ui.element("div").style("flex: 1 1 auto; min-width: 0;"):
                        if not jobs:
                            self._ghost(kind)
                        for i, j in enumerate(jobs):
                            self._row(j, len(jobs), i == 0)

    def _ledger(self) -> None:
        self._rail()
