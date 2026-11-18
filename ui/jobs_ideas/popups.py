"""The magazine popup: what opens from a job with tasks in place of the roster's old drop-down. A card with the job's
description, what it reads and where it lives, and its magazine laid out as square as the count allows; hovering a
mark opens the task card beside it (the tilt series' state, its log's tail, the ways on to it), a click pins it.

Three variants, one class:
- `beside`  the card next to the row it came from, the task card next to the card
- `sheet`   one sheet in the middle of the screen, the task card docked inside it on the right
- `tiles`   as `beside`, the marks bigger and named (stage position · beam), readable without hovering
"""

from __future__ import annotations

import asyncio
import html
import json

from nicegui import ui

from ui.branch_map.model import Job
from ui.components.buttons import house_button
from ui.components.copyable import copy_on_click
from ui.jobs_ideas.data import DESCRIPTION, Data, grid_cols, task_detail
from ui.jobs_ideas.glyphs import glyph
from ui.jobs_ideas.look import (
    BODY,
    GLYPH,
    HEAD,
    INK,
    LINE,
    MARK,
    MONO,
    MUTED,
    PANEL_LINE,
    SANS,
    SOFT,
    STATUS_WORD,
    light,
)

# What a click on a trigger sends: the rectangle of the row (or node) it sits in, and the window's size.
ANCHOR_JS = (
    "(e) => { e.stopPropagation(); const t = e.currentTarget.closest('[data-cbji-anchor]') || e.currentTarget; "
    "const r = t.getBoundingClientRect(); "
    "emit({x: r.right, y: r.top, l: r.left, vw: window.innerWidth, vh: window.innerHeight}); }"
)
_HOVER_JS = (
    "(e) => { const t = e.target.closest('[data-ts]'); if (!t) return; const r = t.getBoundingClientRect(); "
    "emit({ts: t.dataset.ts, y: r.top, vh: window.innerHeight}); }"
)

_CARD = (
    "position: fixed; z-index: 60; background: var(--cb-pop-bg, #fff); "
    "border: 1px solid var(--cb-pop-line, #c9d6e3); border-radius: var(--cb-pop-radius, 2px); "
    "box-shadow: var(--cb-pop-shadow, 0 6px 18px rgba(16,42,67,.10)); display: flex; flex-direction: column; "
    "overflow: hidden;"
)
_STRIPE = (
    f"display: flex; align-items: center; gap: 6px; padding: 5px 8px; background: {HEAD}; "
    f"border-bottom: 1px solid {LINE}; min-height: 24px; box-sizing: border-box;"
)
_LABEL = (
    f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; color: {SOFT}; "
    "width: 62px; flex-shrink: 0; line-height: 15px;"
)
_TAG = (
    f"{MONO} font-size: 8px; text-transform: uppercase; letter-spacing: .06em; color: #4f6782; background: #f1f5f9; "
    "border: 1px solid #dbe3ec; border-radius: 2px; padding: 0 4px; line-height: 13px; flex-shrink: 0;"
)


def _h(s) -> str:
    return html.escape(str(s))


class Popups:
    """The two cards, at most one of each on screen. Mounted once per page, at its top level."""

    def __init__(self, data: Data) -> None:
        self.data = data
        self.variant = "tiles"
        self.root = ui.element("div").style("position: fixed; left: 0; top: 0; width: 0; height: 0; z-index: 60;")
        self.mag: ui.element | None = None
        self.task: ui.element | None = None
        self.backdrop: ui.element | None = None
        self.job: Job | None = None
        self.box = (0, 0, 0)  # the magazine card: left, top, width
        self.pinned: str | None = None
        self.shown: str | None = None
        self.dock: ui.element | None = None
        self._read = 0
        ui.on("cbji_esc", lambda _e: self.close())

    # ── opening and closing ──

    def close(self) -> None:
        for el in (self.mag, self.task, self.backdrop):
            if el is not None and not el.is_deleted:
                el.delete()
        self.mag = self.task = self.backdrop = self.dock = None
        self.job, self.pinned, self.shown = None, None, None

    def _close_task(self) -> None:
        if self.task is not None and not self.task.is_deleted:
            self.task.delete()
        self.task, self.pinned, self.shown = None, None, None

    def open(self, iid: str, anchor: dict) -> None:
        """The magazine of job `iid`, placed by the variant beside `anchor` (the row's rectangle)."""
        if self.job is not None and self.job.id == iid:
            self.close()  # a second click on the same trigger closes it
            return
        self.close()
        j = self.data.job(iid)
        self.job = j
        tasks = self.data.tasks(j)
        n = len(tasks)
        vw, vh = anchor.get("vw", 1600), anchor.get("vh", 900)
        if self.variant == "tiles":
            cols = grid_cols(n, max_cols=12)
            cw, ch, gap = 20, 17, 2
        else:
            cols = grid_cols(n, max_cols=18)
            cw = ch = 10
            gap = 2
        rows = -(-n // cols) if n else 0
        grid_w = cols * cw + (cols - 1) * gap
        grid_h = rows * ch + max(rows - 1, 0) * gap
        width = max(320, grid_w + 24)
        height = 170 + grid_h + (34 if j.inputs else 0)
        with self.root:
            if self.variant == "sheet":
                self.backdrop = (
                    ui.element("div")
                    .style("position: fixed; inset: 0; background: rgba(18,50,79,.10); z-index: 59;")
                    .on("click", lambda _e: self.close())
                )
                total_w = width + 330
                left = max(12, (vw - total_w) / 2)
                top = max(16, (vh - max(height, 380)) / 2)
                self.mag = (
                    ui.element("div")
                    .classes("cbji-pop")
                    .style(f"{_CARD} left: {left}px; top: {top}px; width: {total_w}px; max-height: {vh - 32}px;")
                )
                with self.mag:
                    self._head(j)
                    with ui.element("div").style("display: flex; min-height: 0; flex: 1 1 auto;"):
                        with ui.element("div").style(f"width: {width}px; flex-shrink: 0; overflow: auto;"):
                            self._body(j, tasks, cols, cw, ch, gap)
                        self.dock = ui.element("div").style(
                            f"flex: 1 1 auto; border-left: 1px solid {LINE}; min-width: 0; overflow: auto;"
                        )
                        with self.dock:
                            self._dock_empty()
                self.box = (left, top, total_w)
                return
            left = anchor["x"] + 6
            if left + width > vw - 8:
                left = max(8, anchor.get("l", left) - width - 6)
            top = min(anchor["y"] - 4, vh - height - 12)
            top = max(8, top)
            self.mag = (
                ui.element("div")
                .classes("cbji-pop")
                .style(f"{_CARD} left: {left}px; top: {top}px; width: {width}px; max-height: {vh - 16}px;")
            )
            with self.mag:
                self._head(j)
                with ui.element("div").style("overflow: auto;"):
                    self._body(j, tasks, cols, cw, ch, gap)
            self.box = (left, top, width)

    # ── the magazine card ──

    def _head(self, j: Job) -> None:
        d = self.data
        with ui.element("div").style(_STRIPE):
            ui.html(light(j.status, 8), sanitize=False).style("display: flex;")
            ui.html(glyph(j.kind, ink=GLYPH), sanitize=False).style("display: flex;")
            ui.label(d.name(j)).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK};")
            if j.number:
                ui.label(j.number).style(f"{MONO} font-size: 9px; color: {MUTED};")
            if j.tool:
                ui.label(j.tool).style(_TAG)
            if j.method and j.method.lower() != j.tool.lower():
                ui.label(j.method).style(_TAG)
            ui.element("div").style("flex: 1 1 auto;")
            ui.label(STATUS_WORD.get(j.status, j.status)).style(
                f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; color: {SOFT};"
            )
            ui.icon("close", size="12px").classes("cbji-x-btn").on("click", lambda _e: self.close())

    def _body(self, j: Job, tasks: list, cols: int, cw: int, ch: int, gap: int) -> None:
        d = self.data
        with ui.element("div").style("padding: 8px 10px 4px 10px; display: flex; flex-direction: column; gap: 3px;"):
            ui.label(DESCRIPTION.get(j.kind, "")).style(
                f"{SANS} font-size: 10.5px; color: {BODY}; line-height: 1.4; margin-bottom: 4px;"
            )
            reads = [r for r in j.inputs if r.source or r.value][:3]
            if reads:
                with ui.element("div").style("display: flex; gap: 6px;"):
                    ui.label("Reads").style(_LABEL)
                    with ui.element("div").style("display: flex; flex-direction: column; min-width: 0;"):
                        for r in reads:
                            src = d.pr.by_id.get(r.source) if r.source else None
                            what = f"{d.name(src)} {src.number}" if src else (r.value or "")
                            ui.label(f"{what} · {r.slot.label}").style(
                                f"{MONO} font-size: 9.5px; color: {BODY}; line-height: 15px; white-space: nowrap; "
                                "overflow: hidden; text-overflow: ellipsis;"
                            )
            if j.species:
                with ui.element("div").style("display: flex; gap: 6px;"):
                    ui.label("Species").style(_LABEL)
                    ui.label(d.pr.species_names.get(j.species, j.species)).style(
                        f"{MONO} font-size: 9.5px; color: {BODY}; line-height: 15px;"
                    )
            if j.job_dir is not None:
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label("Folder").style(_LABEL)
                    p = ui.label(str(j.job_dir)).style(
                        f"{MONO} font-size: 9.5px; color: {SOFT}; line-height: 15px; white-space: nowrap; "
                        "overflow: hidden; text-overflow: ellipsis; direction: rtl; text-align: left; min-width: 0;"
                    )
                    p.props["title"] = f"{j.job_dir}\nClick to copy"
                    copy_on_click(p, str(j.job_dir))
            counts: dict[str, int] = {}
            for _, s in tasks:
                counts[s] = counts.get(s, 0) + 1
            with ui.element("div").style("display: flex; gap: 6px; align-items: baseline;"):
                ui.label("Tasks").style(_LABEL)
                parts = []
                for key in ("ok", "running", "pending", "fail", "skip", "unrun"):
                    if counts.get(key):
                        fill, word = MARK[key]
                        parts.append(
                            f'<span style="display:inline-flex;align-items:center;gap:3px;margin-right:9px;">'
                            f'<span style="width:7px;height:7px;background:{fill};display:inline-block;'
                            f'border-radius:1px;"></span>{counts[key]} {_h(word.split(":")[0])}</span>'
                        )
                ui.html("".join(parts), sanitize=False).style(f"{MONO} font-size: 9.5px; color: {BODY};")
        # The magazine.
        tiles = self.variant == "tiles"
        cells = []
        for ts, s in tasks:
            fill, word = MARK.get(s, MARK["unrun"])
            cls = "cbji-run" if s == "running" else ""
            # The task card names the hovered mark; only the tiles, read without hovering, keep a native title.
            tip = f' title="{_h(d.pr.ts_title(ts))} · {_h(word)}"' if tiles else ""
            text = ""
            ink = "#fff"
            if tiles:
                text = _h(d.ts_short(ts))
                ink = "#12324f" if s in ("unrun", "skip", "pending") else "#fff"
            cells.append(
                f'<div data-ts="{_h(ts)}" class="{cls}"{tip} style="width:{cw}px;height:{ch}px;'
                f"background:{fill};border-radius:1px;color:{ink};font:500 7px var(--cb-mono,'IBM Plex Mono',"
                f'monospace);display:flex;align-items:center;justify-content:center;letter-spacing:-.02em;">'
                f"{text}</div>"
            )
        grid = (
            f'<div class="cbji-mag" style="display:grid;grid-template-columns:repeat({cols},{cw}px);gap:{gap}px;'
            f'padding:6px 10px 8px 10px;">{"".join(cells)}</div>'
        )
        mag = ui.html(grid, sanitize=False)
        mag.on("mouseover", self._on_hover, js_handler=_HOVER_JS, throttle=0.06)
        mag.on("click", self._on_pin, js_handler=_HOVER_JS)
        if not tasks:
            ui.label("No tasks yet: the list is written when the job starts.").style(
                f"{SANS} font-size: 10px; color: {MUTED}; padding: 0 10px 8px;"
            )
        with ui.element("div").style(
            f"display: flex; gap: 6px; align-items: center; padding: 6px 10px 8px; border-top: 1px solid {LINE};"
        ):
            nowrap = "white-space: nowrap; flex-shrink: 0;"
            house_button("Open job", lambda: ui.notify(f"Opens {d.name(j)} {j.number}'s page"), kind="accent").style(
                nowrap
            )
            house_button("Tasks", lambda: ui.notify("Opens the job's Tasks tab")).style(nowrap)
            house_button("Logs", lambda: ui.notify("Opens the job's Logs tab")).style(nowrap)
            ui.element("div").style("flex: 1 1 auto;")
            ui.label("hover: a task · click: pin").style(f"{MONO} font-size: 8px; color: {MUTED}; white-space: nowrap;")

    # ── the task card ──

    def _dock_empty(self) -> None:
        ui.label("Hover a tilt series for its task; a click pins it.").style(
            f"{SANS} font-size: 10px; color: {MUTED}; padding: 14px 12px;"
        )

    async def _on_hover(self, e) -> None:
        if self.pinned is None:
            await self._show_task(e.args)

    async def _on_pin(self, e) -> None:
        ts = e.args.get("ts")
        if self.pinned == ts:
            self.pinned = None
            await self._show_task(e.args)
            return
        self.pinned = ts
        await self._show_task(e.args, force=True)

    async def _show_task(self, args: dict, force: bool = False) -> None:
        ts = args.get("ts")
        j = self.job
        if j is None or not ts or (ts == self.shown and not force):
            self._mark_on()
            return
        self.shown = ts
        self._read += 1
        ticket = self._read
        detail = await asyncio.to_thread(task_detail, self.data, j, ts)
        if ticket != self._read or self.job is not j:
            return  # the pointer moved on while the log was read
        state = dict(self.data.tasks(j)).get(ts, "unrun")
        if self.variant == "sheet":
            if self.dock is None or self.dock.is_deleted:
                return
            self.dock.clear()
            with self.dock:
                self._task_body(j, ts, state, detail)
            self._mark_on()
            return
        if self.task is not None and not self.task.is_deleted:
            self.task.delete()
        left, top, width = self.box
        w = 340
        vw = 10_000
        x = left + width + 6
        if x + w > vw:
            x = left - w - 6
        vh = args.get("vh", 900)
        y = max(8, min(args.get("y", top) - 30, vh - 360))
        with self.root:
            self.task = (
                ui.element("div")
                .classes("cbji-pop")
                .style(f"{_CARD} left: {x}px; top: {y}px; width: {w}px; max-height: {vh - y - 12}px; overflow-y: auto;")
            )
            with self.task:
                self._task_body(j, ts, state, detail)
        self._mark_on()
        ui.run_javascript(
            "(() => { const c = document.querySelectorAll('.cbji-pop'); const t = c[c.length - 1]; if (!t) return;"
            " const r = t.getBoundingClientRect(); if (r.right > window.innerWidth - 8) {"
            f" t.style.left = Math.max(8, {left} - r.width - 6) + 'px'; }} }})()"
        )

    def _mark_on(self) -> None:
        on = self.pinned or self.shown or ""
        ui.run_javascript(
            "document.querySelectorAll('.cbji-mag [data-ts]').forEach(el => "
            f"el.classList.toggle('cbji-on', el.dataset.ts === {json.dumps(on)}));"
        )

    def _task_body(self, j: Job, ts: str, state: str, detail: dict) -> None:
        d = self.data
        fill, word = MARK.get(state, MARK["unrun"])
        with ui.element("div").style(_STRIPE):
            ui.html(
                f'<span class="{"cbji-run" if state == "running" else ""}" style="width:9px;height:9px;'
                f'background:{fill};display:inline-block;border-radius:1px;"></span>',
                sanitize=False,
            ).style("display: flex;")
            ui.label(d.pr.ts_title(ts)).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK};")
            ui.label(word.split(":")[0]).style(
                f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; color: {SOFT};"
            )
            ui.element("div").style("flex: 1 1 auto;")
            if self.pinned == ts:
                ui.label("pinned").style(_TAG)
            if self.variant != "sheet":
                ui.icon("close", size="12px").classes("cbji-x-btn").on("click", lambda _e: self._close_task())
        with ui.element("div").style("padding: 8px 10px; display: flex; flex-direction: column; gap: 3px;"):
            for lab, val in (
                ("Series", ts),
                ("Job", f"{d.name(j)} {j.number}"),
                ("Task", "–" if detail["idx"] is None else f"#{detail['idx']}"),
            ):
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label(lab).style(_LABEL)
                    ui.label(val).style(
                        f"{MONO} font-size: 9.5px; color: {BODY}; line-height: 15px; white-space: nowrap; "
                        "overflow: hidden; text-overflow: ellipsis;"
                    )
            if detail["log"]:
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label("Log").style(_LABEL)
                    p = ui.label(detail["log"]).style(
                        f"{MONO} font-size: 9.5px; color: {SOFT}; line-height: 15px; white-space: nowrap; "
                        "overflow: hidden; text-overflow: ellipsis; direction: rtl; text-align: left; min-width: 0;"
                    )
                    p.props["title"] = f"{detail['log']}\nClick to copy"
                    copy_on_click(p, detail["log"])
        well = (
            f"{MONO} font-size: 8.5px; line-height: 1.45; white-space: pre-wrap; word-break: break-all; margin: 0 "
            "10px; "
            f"padding: 6px 8px; background: #f6f8fb; border: 1px solid {LINE}; border-radius: 2px; max-height: 190px; "
            "overflow: auto;"
        )
        cap = f"{_LABEL} width: auto; padding: 2px 10px 1px;"
        if detail["err_tail"]:
            ui.label("stderr, last lines").style(cap)
            ui.html(
                f'<pre style="{well} '
                f'color:#8a4b06;background:#fdf8ee;margin-bottom:6px;">{_h(detail["err_tail"])}</pre>',
                sanitize=False,
            )
        if detail["tail"]:
            ui.label("log, last lines").style(cap)
            ui.html(f'<pre style="{well} color:#334155;">{_h(detail["tail"])}</pre>', sanitize=False)
        elif state in ("unrun", "pending"):
            ui.label("No log yet: the task has not started.").style(
                f"{SANS} font-size: 10px; color: {MUTED}; padding: 0 10px;"
            )
        with ui.element("div").style("display: flex; gap: 6px; padding: 8px 10px; flex-wrap: wrap;"):
            house_button("Task in Tasks tab", lambda: ui.notify("Opens the Tasks tab, scrolled to this tilt series"))
            house_button("Journey", lambda: ui.notify("Opens this tilt series' Journey"))
            house_button("Full log", lambda: ui.notify("Opens the log in the Logs tab"))
        ui.element("div").style(f"height: 1px; border-top: 1px solid transparent; color: {PANEL_LINE};")
