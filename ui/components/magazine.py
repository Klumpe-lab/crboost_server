"""The roster's popups: a job's magazine, the task card beside it, and the other cards a roster row opens (the tilt
filter's settings).

The magazine is what a job with tasks opens from its count on the roster: the job's one-line summary, what it read,
its folder, its tasks counted by state, and one tile per tilt series laid out as square as the count allows, named
`stage·beam`: done (muted green), waiting in the queue (amber), running (blue, blinking), failed (red), skipped
(grey). Hovering a tile opens the task card beside the magazine: the tilt series, its task, the tail of its log and
stderr, and the ways on to it (the job's Tasks tab on that task, its Journey). A click pins the card to that tile.

Parented at the page's layout slot (`dialog_host`): the roster rebuilds its rows on every change and the popups must
outlive that. Esc, a scroll outside them, or the cross closes them. Statuses come from `array_status_cache` (the
status poller refreshes it); while the magazine is open it repaints when they move. Logs are read in a thread.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import math
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from nicegui import ui

from services import array_status_cache
from services.array_tasks import read_tail, sort_ts_by_position, ts_pretty_name
from services.jobs.spec import JOB_SPEC_BY_TYPE, instance_display_name
from services.tilt_series import get_registry_for
from services.tilt_series.build import parse_position
from ui.branch_map.model import TOOLS
from ui.components.buttons import house_button
from ui.components.copyable import copy_on_click
from ui.components.dialogs import dialog_host
from ui.components.job_glyph import job_glyph
from ui.current_project import current_project_state
from ui.job_plugins import get_extra_tabs
from ui.job_plugins._field_styles import MONO, SANS, voice
from ui.status_indicator import _dot_html
from ui.ui_state import get_job_display_name

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel

logger = logging.getLogger(__name__)

INK = "var(--cb-ink-1, #12324f)"
BODY = "var(--cb-ink-2, #1e293b)"
SOFT = "var(--cb-ink-3, #5b7089)"
MUTED = "var(--cb-ink-4, #94a3b8)"
LINE = "var(--cb-line, #e4eaf1)"
HEAD = "var(--cb-head, #f2f6fb)"
GLYPH_INK = "var(--cb-ink-3, #5b7089)"

# The small upper-case words (a fact's name, a state): the stat voice; the tool and "pinned" tags: the tag voice.
WORD = voice(
    "stat", font="'IBM Plex Mono', monospace", size="8.5px", weight=400, ink=SOFT, case="uppercase", track=".07em"
)
TAG = voice(
    "tag",
    font="'IBM Plex Mono', monospace",
    size="8px",
    weight=500,
    ink="var(--cb-tag-ink, #4f6782)",
    case="uppercase",
    track=".06em",
)

# One tile per tilt series: its fill, its text ink, the word its state is called.
MARK = {
    "ok": ("var(--cb-mag-done, #5a9c80)", "var(--cb-mag-on-ink, #ffffff)", "done"),
    "fail": ("var(--cb-bad-dot, #ef4444)", "var(--cb-mag-on-ink, #ffffff)", "failed"),
    "running": ("var(--cb-run-dot, #3b82f6)", "var(--cb-mag-on-ink, #ffffff)", "running"),
    "pending": ("var(--cb-warn-dot, #f59e0b)", INK, "waiting in the queue"),
    "skip": ("var(--cb-mag-skip, #c5d0dc)", INK, "skipped"),
}
TILE_W, TILE_H, TILE_GAP = 20, 17, 2
MAX_COLS = 12

# What a click on a trigger sends: the rectangle of the row (`data-cb-anchor`) it sits in, and the window's size.
ANCHOR_JS = (
    "(e) => { e.stopPropagation(); const t = e.currentTarget.closest('[data-cb-anchor]') || e.currentTarget; "
    "const r = t.getBoundingClientRect(); "
    "emit({x: r.right, y: r.top, l: r.left, vw: window.innerWidth, vh: window.innerHeight}); }"
)
_TILE_JS = (
    "(e) => { const t = e.target.closest('[data-ts]'); if (!t) return; const r = t.getBoundingClientRect(); "
    "emit({ts: t.dataset.ts, y: r.top, vh: window.innerHeight, vw: window.innerWidth}); }"
)
# Esc closes the popups before anything else hears it (the workspace's Esc closes panes); a scroll outside them
# closes them too, since they are pinned to where the row was.
_KEYS_JS = (
    "if (!window.cbMagKeys) { window.cbMagKeys = true;"
    " window.addEventListener('keydown', e => { if (e.key === 'Escape' && document.querySelector('.cb-mag-pop')) {"
    "  e.stopImmediatePropagation(); e.preventDefault(); emitEvent('cb_mag_close'); } }, true);"
    " document.addEventListener('scroll', e => { const t = e.target;"
    "  if (document.querySelector('.cb-mag-pop') && !(t.closest && t.closest('.cb-mag-pop')))"
    "   emitEvent('cb_mag_close'); }, true); }"
)

_CARD = (
    "position: fixed; z-index: 2000; background: var(--cb-pop-bg, #fff); "
    "border: 1px solid var(--cb-pop-line, #c9d6e3); border-radius: var(--cb-pop-radius, 2px); "
    "box-shadow: var(--cb-pop-shadow, 0 6px 18px rgba(16,42,67,.10)); display: flex; flex-direction: column; "
    "overflow: hidden;"
)
_STRIPE = (
    f"display: flex; align-items: center; gap: 6px; padding: 5px 8px; background: {HEAD}; "
    f"border-bottom: 1px solid {LINE}; min-height: 24px; box-sizing: border-box; flex-shrink: 0;"
)
_LABEL = f"{WORD} width: 62px; flex-shrink: 0; line-height: 15px;"
_VALUE = f"{MONO} font-size: 9.5px; color: {BODY}; line-height: 15px; white-space: nowrap; overflow: hidden; "
_PATH = (
    f"{MONO} font-size: 9.5px; color: {SOFT}; line-height: 15px; white-space: nowrap; overflow: hidden; "
    "text-overflow: ellipsis; direction: rtl; text-align: left; min-width: 0;"
)
_TAG = (
    f"{TAG} background: var(--cb-tag, #f1f5f9); border: 1px solid var(--cb-tag-line, #dbe3ec); "
    "border-radius: var(--cb-tag-radius, 2px); padding: 0 4px; line-height: 13px; flex-shrink: 0;"
)
_WELL = (
    f"{MONO} font-size: 8.5px; line-height: 1.45; white-space: pre-wrap; word-break: break-all; margin: 0 10px; "
    f"padding: 6px 8px; background: var(--cb-well, #f6f8fb); border: 1px solid {LINE}; border-radius: 2px; "
    "max-height: 190px; overflow: auto;"
)
_NOWRAP = "white-space: nowrap; flex-shrink: 0;"


def _h(s) -> str:
    return html.escape(str(s))


def tile_labels(project_path: Path) -> dict[str, str]:
    """`stage·beam` of every tilt series, from the project's registry: the shortest name that tells them apart within
    one session. For a thread: the registry may re-read its index."""
    try:
        registry = get_registry_for(project_path)
        return {ts.id: f"{ts.stage_position}·{ts.beam_position}" for ts in registry.all_tilt_series()}
    except (OSError, ValueError):  # no registry to read: the names are parsed from the ids instead
        logger.info("magazine: no registry labels for %s", project_path, exc_info=True)
        return {}


def tile_label(ts: str, labels: dict[str, str]) -> str:
    if ts in labels:
        return labels[ts]
    parsed = parse_position(ts)
    if parsed is None:
        return ts[-4:]
    stage, beam = parsed
    return f"{stage}·{beam}" if beam else str(stage)


def _tasks(project_path: Path, iid: str) -> list[tuple[str, str]]:
    """(tilt series, state) of each task, in stage-position order; empty before the job has written its list."""
    status = array_status_cache.get(project_path, iid)
    if status is None:
        return []
    return [(ts, status.statuses.get(ts, "pending")) for ts in sort_ts_by_position(list(status.items))]


def _task_detail(project_path: Path, iid: str, ts: str) -> dict:
    """The task's index in the array and the tails of its log and stderr. For a thread: it reads files."""
    status = array_status_cache.get(project_path, iid)
    out: dict = {"idx": None, "log": None, "tail": "", "err": None, "err_tail": ""}
    if status is None or ts not in status.items:
        return out
    idx = list(status.items).index(ts)
    out["idx"] = idx
    log, err = status.job_dir / f"task_{idx}.out", status.job_dir / f"task_{idx}.err"
    if log.exists():
        out["log"], out["tail"] = str(log), _last(read_tail(log, 40), 14)
    if err.exists() and err.stat().st_size:
        out["err"], out["err_tail"] = str(err), _last(read_tail(err, 40), 8)
    return out


def _tool(jm) -> str:
    """The tool the job runs, as the branch map names it; nothing for the types run inline."""
    try:
        raw = jm.get_tool_name()
    except NotImplementedError:  # a type with no tool of its own
        return ""
    return TOOLS.get(raw, raw)


def _last(text: str, n: int) -> str:
    return "\n".join(text.rstrip().splitlines()[-n:])


def _reads(state, jm, project_path: Path) -> list[str]:
    """The jobs whose outputs this job's recorded inputs sit in, by name; empty before it has run."""
    dirs = {}
    for iid, other in state.jobs.items():
        name = getattr(other, "relion_job_name", None)
        if name and other is not jm:
            dirs[str(project_path / name.rstrip("/")) + "/"] = instance_display_name(iid, other)
    out: list[str] = []
    for value in (jm.paths or {}).values():
        if not isinstance(value, str):
            continue
        hit = next((n for d, n in dirs.items() if value.startswith(d)), None)
        if hit and hit not in out:
            out.append(hit)
    return out


class RosterPopups:
    """The roster's floating cards, at most one magazine (with its task card) or one other card at a time."""

    def __init__(self, panel: PipelineBuilderPanel) -> None:
        self.panel = panel
        self.root: ui.element | None = None
        self.card: ui.element | None = None
        self.task: ui.element | None = None
        self.key: str | None = None  # what the open card shows: a job's magazine, or another card's key
        self.box = (0.0, 0.0, 0.0)  # the magazine card: left, top, width
        self.pinned: str | None = None
        self.shown: str | None = None
        self._painted: tuple | None = None
        self._read = 0
        self._labels: dict[str, str] = {}

    # ── the host and closing ──

    def _host(self) -> ui.element:
        if self.root is None or self.root.is_deleted:
            with dialog_host():
                self.root = ui.element("div").style("position: fixed; left: 0; top: 0; width: 0; height: 0;")
            ui.on("cb_mag_close", lambda _e: self.close())
            ui.run_javascript(_KEYS_JS)
        return self.root

    def close(self) -> None:
        for el in (self.card, self.task):
            if el is not None and not el.is_deleted:
                el.delete()
        self.card = self.task = None
        self.key = self.pinned = self.shown = None
        self._painted = None

    def follow(self, roster: set[str]) -> None:
        """Close a magazine whose job has left the roster."""
        if self.key and self.key.startswith("mag:") and self.key[4:] not in roster:
            self.close()

    def _place(self, anchor: dict, width: float, height: float) -> tuple[float, float]:
        vw, vh = anchor.get("vw", 1600), anchor.get("vh", 900)
        left = anchor.get("x", 0) + 6
        if left + width > vw - 8:
            left = max(8, anchor.get("l", left) - width - 6)
        top = max(8, min(anchor.get("y", 0) - 4, vh - height - 12))
        return left, top

    # ── any card ──

    def open_card(self, key: str, anchor: dict, width: int, build: Callable[[], None], title: str) -> None:
        """A card beside the row: a title stripe and what `build` puts under it. A second click closes it."""
        if self.key == key:
            self.close()
            return
        self.close()
        left, top = self._place(anchor, width, 160)
        with self._host():
            self.card = (
                ui.element("div")
                .classes("cb-mag-pop")
                .style(
                    f"{_CARD} left: {left}px; top: {top}px; width: {width}px; "
                    f"max-height: {anchor.get('vh', 900) - 16}px;"
                )
            )
            with self.card:
                with ui.element("div").style(_STRIPE):
                    ui.label(title).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK};")
                    ui.element("div").style("flex: 1 1 auto;")
                    ui.icon("close", size="12px").classes("cb-pop-x").on("click", lambda _e: self.close())
                with ui.element("div").style("overflow: auto; padding: 4px 0 6px;"):
                    build()
        self.key = key

    # ── the magazine ──

    async def open_magazine(self, iid: str, anchor: dict) -> None:
        key = f"mag:{iid}"
        if self.key == key:
            self.close()
            return
        self.close()
        project_path = self.panel.ui_mgr.project_path
        if project_path is None:
            return
        self._labels = await asyncio.to_thread(tile_labels, project_path)
        if self.key is not None:
            return  # another card opened while the names were read
        tasks = _tasks(project_path, iid)
        cols = max(1, min(MAX_COLS, math.ceil(math.sqrt(max(len(tasks), 1)))))
        rows = -(-len(tasks) // cols) if tasks else 0
        width = max(320, cols * (TILE_W + TILE_GAP) + 20)
        height = 190 + rows * (TILE_H + TILE_GAP)
        left, top = self._place(anchor, width, height)
        with self._host():
            self.card = (
                ui.element("div")
                .classes("cb-mag-pop")
                .style(
                    f"{_CARD} left: {left}px; top: {top}px; width: {width}px; "
                    f"max-height: {anchor.get('vh', 900) - 16}px;"
                )
            )
        self.key = key
        self.box = (left, top, width)
        self._paint(iid, tasks)
        self.panel.ui_mgr.view_scope("pipeline").timer(3.0, lambda: self._follow_states(iid), self.card)

    def _follow_states(self, iid: str) -> None:
        """Repaint the open magazine when its tasks' states moved (the status poller refreshes the cache)."""
        if self.key != f"mag:{iid}" or self.card is None or self.card.is_deleted:
            return
        project_path = self.panel.ui_mgr.project_path
        jm = current_project_state().jobs.get(iid)
        if project_path is None or jm is None:
            return
        tasks = _tasks(project_path, iid)
        if (jm.execution_status, tuple(tasks)) != self._painted:
            self._paint(iid, tasks)

    def _paint(self, iid: str, tasks: list[tuple[str, str]]) -> None:
        state = current_project_state()
        jm = state.jobs.get(iid)
        project_path = self.panel.ui_mgr.project_path
        if jm is None or project_path is None or self.card is None:
            return
        self._painted = (jm.execution_status, tuple(tasks))
        self.card.clear()
        name = get_job_display_name(jm.job_type)
        relion = getattr(jm, "relion_job_name", None)
        number = relion.rstrip("/").split("/")[-1].removeprefix("job") if relion else ""
        with self.card:
            with ui.element("div").style(_STRIPE):
                ui.html(_dot_html(jm.execution_status, size_px=6), sanitize=False).style("display: flex;")
                ui.html(job_glyph(jm.job_type, ink=GLYPH_INK), sanitize=False).style("display: flex;")
                ui.label(name).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK};")
                if number:
                    ui.label(number).style(f"{MONO} font-size: 9px; color: {MUTED};")
                tool = _tool(jm)
                if tool:
                    ui.label(tool).style(_TAG)
                ui.element("div").style("flex: 1 1 auto;")
                status = getattr(jm.execution_status, "value", str(jm.execution_status))
                ui.label(status).style(WORD)
                ui.icon("close", size="12px").classes("cb-pop-x").on("click", lambda _e: self.close())
            with ui.element("div").style("overflow: auto; min-height: 0;"):
                self._facts(iid, jm, state, project_path, tasks)
                self._tiles(iid, tasks)
            self._footer(iid)
        self._mark_on()

    def _facts(self, iid: str, jm, state, project_path: Path, tasks: list[tuple[str, str]]) -> None:
        spec = JOB_SPEC_BY_TYPE.get(jm.job_type)
        with ui.element("div").style("padding: 8px 10px 4px 10px; display: flex; flex-direction: column; gap: 3px;"):
            if spec is not None and spec.summary:
                ui.label(spec.summary).style(
                    f"{SANS} font-size: 10.5px; color: {BODY}; line-height: 1.4; margin-bottom: 4px;"
                )
            reads = _reads(state, jm, project_path)
            if reads:
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label("Reads").style(_LABEL)
                    ui.label(", ".join(reads)).style(f"{_VALUE} text-overflow: ellipsis;")
            species = state.get_species(jm.species_id) if getattr(jm, "species_id", None) else None
            if species is not None:
                with ui.element("div").style("display: flex; gap: 6px;"):
                    ui.label("Species").style(_LABEL)
                    ui.label(species.name).style(_VALUE)
            relion = getattr(jm, "relion_job_name", None)
            if relion:
                folder = str(project_path / relion.rstrip("/"))
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label("Folder").style(_LABEL)
                    p = ui.label(folder).style(_PATH)
                    p.props["title"] = f"{folder}\nClick to copy"
                    copy_on_click(p, folder)
            counts: dict[str, int] = {}
            for _, s in tasks:
                counts[s] = counts.get(s, 0) + 1
            with ui.element("div").style("display: flex; gap: 6px; align-items: baseline;"):
                ui.label("Tasks").style(_LABEL)
                parts = []
                for key in ("ok", "running", "pending", "fail", "skip"):
                    if counts.get(key):
                        fill, _ink, word = MARK[key]
                        parts.append(
                            '<span style="display:inline-flex;align-items:center;gap:3px;margin-right:9px;">'
                            f'<span style="width:7px;height:7px;background:{fill};display:inline-block;'
                            f'border-radius:1px;"></span>{counts[key]} {_h(word)}</span>'
                        )
                ui.html("".join(parts) or "none yet", sanitize=False).style(f"{MONO} font-size: 9.5px; color: {BODY};")

    def _tiles(self, iid: str, tasks: list[tuple[str, str]]) -> None:
        if not tasks:
            ui.label("No tasks yet: the list is written when the job starts.").style(
                f"{SANS} font-size: 10px; color: {MUTED}; padding: 4px 10px 10px;"
            )
            return
        cols = max(1, min(MAX_COLS, math.ceil(math.sqrt(len(tasks)))))
        cells = []
        for ts, s in tasks:
            fill, ink, _word = MARK.get(s, MARK["pending"])
            cls = "cb-mag-run" if s == "running" else ""
            cells.append(
                f'<div data-ts="{_h(ts)}" class="{cls}" '
                f'style="width:{TILE_W}px;height:{TILE_H}px;background:{fill};color:{ink};border-radius:1px;'
                "font:500 7px var(--cb-mono,'IBM Plex Mono',monospace);display:flex;align-items:center;"
                f'justify-content:center;letter-spacing:-.02em;">{_h(tile_label(ts, self._labels))}</div>'
            )
        grid = ui.html(
            f'<div class="cb-mag" style="display:grid;grid-template-columns:repeat({cols},{TILE_W}px);'
            f'gap:{TILE_GAP}px;padding:6px 10px 8px 10px;">{"".join(cells)}</div>',
            sanitize=False,
        )
        grid.on("mouseover", lambda e: self._on_hover(iid, e), js_handler=_TILE_JS, throttle=0.06)
        grid.on("click", lambda e: self._on_pin(iid, e), js_handler=_TILE_JS)

    def _footer(self, iid: str) -> None:
        panel = self.panel

        def go(fn):
            def _go():
                self.close()
                fn()

            return _go

        with ui.element("div").style(
            f"display: flex; gap: 6px; align-items: center; padding: 6px 10px 8px; border-top: 1px solid {LINE}; "
            "flex-shrink: 0;"
        ):
            house_button("Open job", go(lambda: panel.switch_tab(iid)), kind="accent").style(_NOWRAP)
            jm = current_project_state().jobs.get(iid)
            if jm is not None and any(t.key == "tasks" for t in get_extra_tabs(jm.job_type)):
                house_button("Tasks", go(lambda: panel.switch_to_job_subsection(iid, "tasks"))).style(_NOWRAP)
            house_button("Logs", go(lambda: panel.switch_to_job_subsection(iid, "logs"))).style(_NOWRAP)

    # ── the task card ──

    async def _on_hover(self, iid: str, e) -> None:
        if self.pinned is None:
            await self._show_task(iid, e.args)

    async def _on_pin(self, iid: str, e) -> None:
        ts = e.args.get("ts")
        if self.pinned == ts:
            self.pinned = None
            await self._show_task(iid, e.args, force=True)
            return
        self.pinned = ts
        await self._show_task(iid, e.args, force=True)

    async def _show_task(self, iid: str, args: dict, force: bool = False) -> None:
        ts = args.get("ts")
        project_path = self.panel.ui_mgr.project_path
        if self.key != f"mag:{iid}" or not ts or project_path is None or (ts == self.shown and not force):
            self._mark_on()
            return
        self.shown = ts
        self._read += 1
        ticket = self._read
        detail = await asyncio.to_thread(_task_detail, project_path, iid, ts)
        if ticket != self._read or self.key != f"mag:{iid}":
            return  # the pointer moved on, or the magazine closed, while the log was read
        state = dict(_tasks(project_path, iid)).get(ts, "pending")
        if self.task is not None and not self.task.is_deleted:
            self.task.delete()
        left, top, width = self.box
        w = 340
        vw, vh = args.get("vw", 1600), args.get("vh", 900)
        x = left + width + 6
        if x + w > vw - 8:
            x = max(8, left - w - 6)
        y = max(8, min(args.get("y", top) - 30, vh - 380))
        with self._host():
            self.task = (
                ui.element("div")
                .classes("cb-mag-pop")
                .style(f"{_CARD} left: {x}px; top: {y}px; width: {w}px; max-height: {vh - y - 12}px; overflow-y: auto;")
            )
            with self.task:
                self._task_body(iid, ts, state, detail)
        self._mark_on()

    def _mark_on(self) -> None:
        on = self.pinned or self.shown or ""
        ui.run_javascript(
            "document.querySelectorAll('.cb-mag [data-ts]').forEach(el => "
            f"el.classList.toggle('cb-mag-on', el.dataset.ts === {json.dumps(on)}));"
        )

    def _task_body(self, iid: str, ts: str, state: str, detail: dict) -> None:
        panel = self.panel
        jm = current_project_state().jobs.get(iid)
        fill, _ink, word = MARK.get(state, MARK["pending"])
        with ui.element("div").style(_STRIPE):
            ui.html(
                f'<span class="{"cb-mag-run" if state == "running" else ""}" style="width:9px;height:9px;'
                f'background:{fill};display:inline-block;border-radius:1px;"></span>',
                sanitize=False,
            ).style("display: flex;")
            ui.label(ts_pretty_name(ts)).style(f"{SANS} font-size: 11px; font-weight: 600; color: {INK};")
            ui.label(word).style(WORD)
            ui.element("div").style("flex: 1 1 auto;")
            if self.pinned == ts:
                ui.label("pinned").style(_TAG)
            ui.icon("close", size="12px").classes("cb-pop-x").on("click", lambda _e: self._close_task())
        with ui.element("div").style("padding: 8px 10px; display: flex; flex-direction: column; gap: 3px;"):
            rows = [("Series", ts), ("Job", instance_display_name(iid, jm))]
            rows.append(("Task", "–" if detail["idx"] is None else f"#{detail['idx']}"))
            for lab, val in rows:
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label(lab).style(_LABEL)
                    ui.label(val).style(f"{_VALUE} text-overflow: ellipsis;")
            if detail["log"]:
                with ui.element("div").style("display: flex; gap: 6px; min-width: 0;"):
                    ui.label("Log").style(_LABEL)
                    p = ui.label(detail["log"]).style(_PATH)
                    p.props["title"] = f"{detail['log']}\nClick to copy"
                    copy_on_click(p, detail["log"])
        cap = f"{_LABEL} width: auto; padding: 2px 10px 1px;"
        if detail["err_tail"]:
            ui.label("stderr, last lines").style(cap)
            ui.html(
                f'<pre style="{_WELL} color:var(--cb-warn-ink, #8a4b06);'
                f'background:var(--cb-warn-bg, #fdf8ee);margin-bottom:6px;">'
                f"{_h(detail['err_tail'])}</pre>",
                sanitize=False,
            )
        if detail["tail"]:
            ui.label("log, last lines").style(cap)
            ui.html(f'<pre style="{_WELL} color:var(--cb-ink-2, #334155);">{_h(detail["tail"])}</pre>', sanitize=False)
        elif state == "pending":
            ui.label("No log yet: the task has not started.").style(
                f"{SANS} font-size: 10px; color: {MUTED}; padding: 0 10px;"
            )

        def in_tasks() -> None:
            self.close()
            panel.ui_mgr.focus_ts_by_instance[iid] = ts
            panel.switch_to_job_subsection(iid, "tasks")

        async def journey() -> None:
            self.close()
            show = panel.callbacks.get("show_journey")
            if show is None:
                return
            await show()
            show_ts = panel.callbacks.get("journey_show_ts")
            if show_ts is not None:
                await show_ts(ts, None)

        with ui.element("div").style("display: flex; gap: 6px; padding: 8px 10px; flex-wrap: wrap;"):
            house_button("In the Tasks tab", in_tasks).style(_NOWRAP)
            house_button("Journey", journey).style(_NOWRAP)

    def _close_task(self) -> None:
        if self.task is not None and not self.task.is_deleted:
            self.task.delete()
        self.task, self.pinned, self.shown = None, None, None
        self._mark_on()
