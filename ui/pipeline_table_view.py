"""Pipeline table — the roster's jobs as rows, in dependency order.

A workspace view (rail: the branch glyph under Jobs), swapped into the main area like the
Tomograms wall. The gutter on the left draws each job's main lineage git-log style
(`services/pipeline_table.py` lays out the lanes): a dot per job and a line down to each job that
reads it, solid when that input is fixed at submit or set by hand, dashed while automatic
selection may still move it. Beside it the job and its species, its state, tilt series done and
elapsed time. Read-only: clicking a row opens the job.

The statuses are watched in memory every 3 s. When they move, else every 15 s, the rows are
worked out again (the resolver, on the event loop) and the array manifests and the job dirs'
markers re-read off it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Hashable
from pathlib import Path
from typing import Any

from nicegui import ui

from services import array_status_cache
from services.array_status_cache import is_array_job
from services.array_tasks import TaskProgress
from services.models_base import JobStatus
from services.path_resolution_service import InputState
from services.pipeline_table import RunTimes, TableRow, pipeline_rows, run_times
from services.project_state import ProjectState, get_project_state_for
from ui.components.reactive import FingerprintedView, SingleFlight, owned_timer
from ui.components.species_pill import render_species_mark
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.status_indicator import _BADGE_STYLES, _DOT_COLORS
from ui.styles import MONO, SANS
from ui.ui_state import get_instance_display_name
from ui.utils import snake_to_title

logger = logging.getLogger(__name__)

_TICK_S = 3.0  # in-memory statuses; a change re-reads the table at once
_READ_S = 15.0  # disk: the resolver's file checks, TS progress, run times

_LIVE = (JobStatus.QUEUED, JobStatus.RUNNING)

# Gutter geometry. The dash period (6) divides the row height and its half, so a dashed lane runs
# on unbroken from one row's slice into the next.
_LANE_W = 12
_ROW_H = 24
_DOT_R = 3.5
_LINE = "#94a3b8"
_DASH = "3 3"

_ROW_STYLE = (
    f"grid-column: 1 / -1; display: grid; grid-template-columns: subgrid; align-items: center; "
    f"height: {_ROW_H}px; cursor: pointer; border-radius: 3px;"
)
_HEAD_STYLE = (
    f"{SANS} font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; "
    "color: #94a3b8; white-space: nowrap;"
)
_CELL = "display: flex; align-items: center; gap: 5px; min-width: 0; white-space: nowrap;"
_TS_TIP = "Tilt series finished, of the job's list. ▸ running now · ⊘ skipped · ! failed"
_ELAPSED_TIP = (
    "From when the job could start (its submit, or the last job it reads finishing) to its exit marker. "
    "Includes time waiting in the SLURM queue."
)


def gutter_svg(row: TableRow, lanes: int, fill: str, ring: str, pulse: bool) -> str:
    """One row's slice of the lanes: the lines passing it, the line arriving at its dot, the lines
    leaving it (straight down, or curving out into a fork lane) and the dot on top."""
    mid, h = _ROW_H / 2, _ROW_H

    def x(lane: int) -> float:
        return _LANE_W / 2 + lane * _LANE_W

    def stroke(solid: bool) -> str:
        dash = "" if solid else f' stroke-dasharray="{_DASH}"'
        return f'stroke="{_LINE}" stroke-width="1.5" fill="none"{dash}'

    cx = x(row.lane)
    parts = [f'<line x1="{x(i)}" y1="0" x2="{x(i)}" y2="{h}" {stroke(i in row.solid)}/>' for i in row.through]
    if row.arrives:
        parts.append(f'<line x1="{cx}" y1="0" x2="{cx}" y2="{mid}" {stroke(row.explicit)}/>')
    if row.continues:
        parts.append(f'<line x1="{cx}" y1="{mid}" x2="{cx}" y2="{h}" {stroke(row.lane in row.solid)}/>')
    for f in row.forks:
        parts.append(f'<path d="M{cx} {mid} Q{x(f)} {mid} {x(f)} {h}" {stroke(f in row.solid)}/>')
    anim = (
        f'<animate attributeName="r" values="{_DOT_R};{_DOT_R + 1.5};{_DOT_R}" dur="1.1s" repeatCount="indefinite"/>'
        if pulse
        else ""
    )
    parts.append(
        f'<circle cx="{cx}" cy="{mid}" r="{_DOT_R}" fill="{fill}" stroke="{ring}" stroke-width="1.5">{anim}</circle>'
    )
    w = lanes * _LANE_W
    return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display: block;">{"".join(parts)}</svg>'


def _fmt_span(seconds: float) -> str:
    s = max(0, int(seconds))
    if s < 60:
        return f"{s}s"
    m = s // 60
    if m < 60:
        return f"{m}m"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h}h {m:02d}m"
    d, h = divmod(h, 24)
    return f"{d}d {h}h"


def _elapsed_text(status: JobStatus, times: RunTimes | None, now: float) -> str:
    if times is None or times.start is None:
        return ""
    if status == JobStatus.RUNNING:
        return _fmt_span(now - times.start)
    if status in (JobStatus.SUCCEEDED, JobStatus.FAILED) and times.end is not None:
        return _fmt_span(times.end - times.start)
    return ""


def _read_disk(jobs: dict[str, Any], project_path: Path, rows: list[TableRow]) -> dict[str, RunTimes]:
    """What the table reads from disk besides the array statuses (the shared cache's): the run times.
    Runs in a worker thread, on its own copy of the rows' jobs, so an add or a removal meanwhile
    cannot change the dict under it."""
    return run_times(jobs, project_path, rows)


def _inputs_tip(row: TableRow, state: ProjectState) -> str:
    """The job cell's hover: every input another job of the table feeds, the side inputs the gutter
    leaves out included, and how each was set."""
    if not row.inputs:
        return ""
    fixed = state.jobs[row.instance_id].inputs_fixed
    lines = ["Reads"]
    for e in row.inputs:
        how = InputState.FIXED_AT_SUBMIT if fixed else InputState.SET_BY_HAND if e.explicit else InputState.AUTOMATIC
        producer = get_instance_display_name(e.producer, state.jobs.get(e.producer))
        lines.append(f"{snake_to_title(e.slot)} ← {producer} · {how.value}")
    return "\n".join(lines)


def _render_progress(p: TaskProgress | None) -> None:
    """The roster row's tally in one cell: ok/total (green when settled, blue while partial), then
    running, skipped and failed counts."""
    with ui.element("div").style(_CELL):
        if p is None:
            return
        if p.n_fail or p.n_settled == p.total:
            color = "#16a34a"
        else:
            color = "#2563eb" if p.n_settled else "#9ca3af"
        ui.label(f"{p.n_ok}/{p.total}").style(f"{MONO} font-size: 10px; font-weight: 600; color: {color};")
        if p.n_running:
            ui.label(f"▸{p.n_running}").style(f"{MONO} font-size: 9px; font-weight: 700; color: #2563eb;")
        if p.n_skip:
            ui.label(f"⊘{p.n_skip}").style(f"{MONO} font-size: 9px; font-weight: 600; color: #94a3b8;")
        if p.n_fail:
            ui.label(f"{p.n_fail}!").style(f"{MONO} font-size: 9px; font-weight: 700; color: #dc2626;")


def _legend_line(solid: bool) -> str:
    dash = "" if solid else f' stroke-dasharray="{_DASH}"'
    return (
        '<svg width="20" height="8" viewBox="0 0 20 8" style="display: block;">'
        f'<line x1="1" y1="4" x2="19" y2="4" stroke="{_LINE}" stroke-width="1.5"{dash}/></svg>'
    )


class PipelineTablePage:
    """Built once (lazily, on the first click of its rail icon), like the Tomograms wall."""

    def __init__(self, container, project_path: Path, callbacks: dict | None = None) -> None:
        self.container = container
        self.project_path = Path(project_path)
        self.callbacks = callbacks or {}
        self.rows: list[TableRow] = []
        self.progress: dict[str, TaskProgress | None] = {}
        self.times: dict[str, RunTimes] = {}
        self.quick: tuple | None = None  # the in-memory signature the last read was taken at
        self.error: str | None = None  # why the last read failed; the rows stay those of the read before
        self.elapsed_labels: dict[str, Any] = {}  # instance_id -> label; set in place, not rebuilt
        self.view: _TableView | None = None
        self._read_at = 0.0
        self._active = False
        self._flight = SingleFlight()
        # Parented at the page layout: _build clears the container, which deletes a timer made in it.
        self._timer = owned_timer(_TICK_S, self._tick, container)
        self._timer.deactivate()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def show(self) -> None:
        async with self._flight("show") as acquired:
            if not acquired:
                return
            if self.view is None:
                self._build()
                await asyncio.sleep(0.03)  # flush the spinner before the disk pass
            await self._read()
            self._paint_elapsed()

    def set_active(self, on: bool) -> None:
        """The workspace's _apply_views hook: the table polls only while it is on screen."""
        self._active = on
        if on:
            self._timer.activate()
        else:
            self._timer.deactivate()

    async def _tick(self) -> None:
        if not self._active or self.view is None:
            return
        if self.quick_signature() != self.quick or time.monotonic() - self._read_at >= _READ_S:
            await self._read()
        self._paint_elapsed()

    async def _read(self) -> None:
        async with self._flight("read") as acquired:
            if not acquired:
                return
            # Explicit-path resolution, not the tab accessor: this also runs from the timer.
            state = get_project_state_for(self.project_path)
            quick = self.quick_signature()
            try:
                # The rows on the loop, where every edit of the jobs runs too; only the disk reads go
                # to a thread.
                rows = pipeline_rows(state)
                jobs = {r.instance_id: state.jobs[r.instance_id] for r in rows}
                await array_status_cache.refresh(self.project_path, jobs)
                self.progress = {
                    iid: (s.progress if (s := array_status_cache.get(self.project_path, iid)) else None)
                    for iid, jm in jobs.items()
                    if is_array_job(jm)
                }
                self.times = await asyncio.to_thread(_read_disk, jobs, self.project_path, rows)
                self.rows = rows
                self.error = None
            except Exception as e:
                # Shown on the page and retried on the next tick: a table that silently stops
                # moving reads as a pipeline that stopped.
                logger.exception("Could not read the pipeline table of %s", self.project_path)
                self.error = str(e) or type(e).__name__
            self.quick = quick
            self._read_at = time.monotonic()
            if self.view is not None:
                self.view.refresh()

    def quick_signature(self) -> tuple:
        """What the table draws from memory: which jobs in which order, their statuses, names and
        species, how their inputs are set, the review hold. Cheap enough for every tick."""
        state = get_project_state_for(self.project_path)
        jobs = tuple(
            (
                iid,
                jm.execution_status,
                jm.is_orphaned,
                jm.species_id,
                jm.display_label,
                jm.relion_job_name,
                jm.slurm_job_id,
                jm.inputs_fixed,
                tuple(sorted((jm.source_overrides or {}).items())),
            )
            for iid in state.pipeline_order or []
            if (jm := state.jobs.get(iid)) is not None
        )
        hold = state.review_hold
        return jobs, () if hold is None else tuple(hold.parked), state.species_identity()

    def _paint_elapsed(self) -> None:
        state = get_project_state_for(self.project_path)
        now = time.time()
        for iid, label in self.elapsed_labels.items():
            jm = state.jobs.get(iid)
            label.set_text("" if jm is None else _elapsed_text(jm.execution_status, self.times.get(iid), now))

    def open_job(self, instance_id: str) -> None:
        fn = self.callbacks.get("open_job")
        if fn is not None:
            fn(instance_id)

    # ── build ─────────────────────────────────────────────────────────────────

    def _build(self) -> None:
        ensure_assets_loaded()
        self.container.clear()
        with (
            self.container,
            ui.column()
            .classes("w-full")
            .style("height: 100%; overflow: auto; padding: 10px 18px 24px; gap: 10px; box-sizing: border-box;"),
        ):
            # The title line's right padding keeps the view's close cross clear.
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 14px; flex-wrap: wrap; padding-right: 4px;"
            ):
                ui.label("Pipeline").style(PAGE_SECTION_STYLE).tooltip(
                    "Every job on the roster, each after the jobs it reads. The lines follow the input a "
                    "job's tilt series come from; its other inputs are in the hover of its name and in its "
                    "I/O tab. Click a row to open the job."
                )
                for solid, text in ((True, "fixed at submit or set by hand"), (False, "automatic, may still change")):
                    with ui.element("div").style("display: flex; align-items: center; gap: 5px;"):
                        ui.html(_legend_line(solid), sanitize=False)
                        ui.label(text).style(f"{SANS} font-size: 9px; color: #64748b;")
            body = ui.element("div").style("width: 100%;")
            with body, ui.element("div").classes("cb-empty"):
                ui.spinner(size="lg", color="indigo")
                ui.label("Reading the pipeline…").classes("text-xs")
        self.view = _TableView(body, self)


class _TableView(FingerprintedView):
    """The table itself. Rebuilt when the rows, the in-memory statuses or the TS tallies move;
    the elapsed times are set in place on every tick instead."""

    def __init__(self, container, page: PipelineTablePage) -> None:
        super().__init__(container)
        self.page = page

    def signature(self) -> Hashable:
        p = self.page
        return tuple(p.rows), p.quick, tuple(sorted(p.progress.items(), key=lambda kv: kv[0])), p.error

    def render(self) -> None:
        p = self.page
        p.elapsed_labels = {}
        if p.error:
            ui.label(f"Could not read the pipeline: {p.error}").style(
                f"{SANS} font-size: 10px; color: #b45309; margin-bottom: 6px;"
            )
        if not p.rows:
            if not p.error:
                with ui.element("div").classes("cb-empty"):
                    ui.label("No jobs on the roster yet: add them in the Jobs view.")
            return
        state = get_project_state_for(p.project_path)
        hold = state.review_hold
        parked = set(hold.parked) if hold is not None else set()
        lanes = 1 + max(lane for r in p.rows for lane in (r.lane, *r.through, *r.forks))
        with ui.element("div").style(
            f"display: grid; grid-template-columns: {lanes * _LANE_W}px repeat(4, max-content); "
            "column-gap: 14px; width: max-content; max-width: 100%;"
        ):
            with ui.element("div").style(_ROW_STYLE + " cursor: default; height: 20px;"):
                ui.element("div")
                for text, tip in (("job", None), ("state", None), ("TS", _TS_TIP), ("elapsed", _ELAPSED_TIP)):
                    head = ui.label(text).style(_HEAD_STYLE)
                    if tip:
                        head.tooltip(tip)
            for row in p.rows:
                jm = state.jobs.get(row.instance_id)
                if jm is not None:  # removed since the read; the next tick reads the table again
                    self._render_row(row, jm, state, lanes, row.instance_id in parked)

    def _render_row(self, row: TableRow, jm, state: ProjectState, lanes: int, parked: bool) -> None:
        p = self.page
        iid = row.instance_id
        status = jm.execution_status
        if jm.is_orphaned:
            fill, ring = "#f97316", "#ffffff"
        elif parked and status == JobStatus.SCHEDULED:
            fill, ring = "#ffffff", "#f59e0b"
        else:
            fill, ring = _DOT_COLORS.get(status, "#9ca3af"), "#ffffff"

        el = ui.element("div").classes("cb-pt-row").style(_ROW_STYLE)
        el.on("click", lambda _e, i=iid: p.open_job(i))
        with el:
            ui.html(gutter_svg(row, lanes, fill, ring, status == JobStatus.RUNNING), sanitize=False).style(
                "line-height: 0;"
            )
            with ui.element("div").style(_CELL + " gap: 6px;"):
                ui.label(get_instance_display_name(iid, jm)).style(f"{MONO} font-size: 11px; color: #1e293b;")
                species = state.get_species(jm.species_id) if jm.species_id else None
                tip = _inputs_tip(row, state)
                if species is not None:
                    # Named in the cell's hover: a hover of its own would stack on that one.
                    render_species_mark(species, tooltip="")
                    tip = f"Species: {species.name}" + (f"\n{tip}" if tip else "")
                if tip:
                    ui.tooltip(tip).style("white-space: pre-line;").props("delay=400")
            self._render_state(row, jm, state, parked)
            _render_progress(p.progress.get(iid))
            p.elapsed_labels[iid] = ui.label("").style(f"{MONO} font-size: 10px; color: #475569;")

    @staticmethod
    def _render_state(row: TableRow, jm, state: ProjectState, parked: bool) -> None:
        status = jm.execution_status
        if jm.is_orphaned:
            text, color = "missing inputs", "color: #c2410c;"
            tip = "; ".join(str(m) for m in jm.missing_inputs) or "An input has no source."
        elif parked and status == JobStatus.SCHEDULED:
            text, color = "waits for review", "color: #b45309;"
            tip = "Parked behind the tilt-filter review; Approve submits it."
        else:
            text, color = status.value.lower(), _BADGE_STYLES.get(status, ("", "color: #1f2937;"))[1]
            tip = None
            if status in _LIVE and jm.slurm_job_id:
                live = {iid for iid, m in state.jobs.items() if m.execution_status in _LIVE}
                after = sorted({e.producer for e in row.inputs} & live)
                if status == JobStatus.RUNNING:
                    tip = f"SLURM {jm.slurm_job_id}"
                elif after:
                    tip = f"SLURM {jm.slurm_job_id} · after {', '.join(after)}"
                else:
                    tip = f"SLURM {jm.slurm_job_id} · in the queue"
        lab = ui.label(text).style(f"{SANS} font-size: 10px; {color} white-space: nowrap;")
        if tip:
            lab.tooltip(tip)
