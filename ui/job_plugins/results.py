"""A job's Results, at the top of its page once it has run, open.

What the job made, as `services/job_results.py` reads it (the branch map's large card reads the same): the line that
says it, what stands out (by name), its headline numbers, its pictures in one of two shapes, one plot, a table of its
tilt series; then what is the job's own (`results_part`: Class 3D's classes and subsets, the FSC readouts).

- **The wall**: one frame per tilt series, in position order, all the same shape, each named with its one number
  under it. A marked frame (an outlier, a failed task) says why on hover. A frame whose picture is still to make is
  the render's skeleton; it fills in place. Past three rows the wall folds. A click opens the Journey on that tilt
  series. A before/after wall has one switch for every frame.
- **The map**: a volume's three central slices in three square cells, the viewers' chips in its corner.

The section reads again when the job's status or its tasks' tally moves (both in memory), and every 15 s while the
job runs; the read is in a thread, and the pictures still to make are renders in a worker.
"""

from __future__ import annotations

import asyncio
import html
import logging
import math
import time
from collections.abc import Callable
from pathlib import Path

from nicegui import ui

from services import array_status_cache
from services.dashboard_data import tilt_thumb_dir, vis_asset_url
from services.job_results import (
    OUTLIER_RULE,
    THUMB,
    JobResults,
    MapPic,
    ResultPlot,
    ResultTable,
    Wall,
    WallTile,
    job_results,
    map_slices_render,
    registry_dicts,
)
from services.jobs.spec import JOB_SPEC_BY_TYPE, PHASE_PREPROCESSING
from services.models_base import JobStatus, JobType
from services.project_state import get_project_state_for
from services.tilt_series import get_registry_for
from ui.analytics.common import TOOLTIP_STYLE
from ui.components import feature_layers as fl
from ui.components.plot import SERIES, Axis, Plot, Series, plot
from ui.components.reactive import SingleFlight, owned_timer
from ui.components.render_slot import RenderSlot, keep_current
from ui.components.segmented import Segmented
from ui.components.viewer_chips import viewer_chips
from ui.dashboard.figures import class_color
from ui.job_plugins import load_plugins
from ui.job_plugins._field_styles import MONO, SANS, SECTION_STYLE, section_head

logger = logging.getLogger(__name__)

# A wall longer than this folds to three rows (ui/dashboard/css.py), past which `all N` opens it.
FOLD = 21
FRAME_MIN_PX = 150
MAP_CELL_PX = 120
_LIVE_READ_S = 15.0

_HEADLINE = (
    f"{SANS} font-size: 12px; font-weight: 500; color: var(--cb-ink-1, #12324f); font-variant-numeric: tabular-nums;"
)
_WARN = f"{SANS} font-size: 10.5px; color: var(--cb-warn-ink, #8a4b06); font-variant-numeric: tabular-nums;"
_BAD = f"{SANS} font-size: 10.5px; color: var(--cb-bad-ink, #991b1b); font-variant-numeric: tabular-nums;"
_NOTE = f"{SANS} font-size: 10.5px; color: var(--cb-ink-3, #5b7089); font-variant-numeric: tabular-nums;"
_SMALL = f"{SANS} font-size: 9.5px; color: var(--cb-ink-4, #94a3b8);"
_TILE_LABEL = (
    f"{SANS} font-size: 9px; font-weight: 500; color: var(--cb-ink-4, #94a3b8); text-transform: var(--cb-label-case, "
    "none); letter-spacing: var(--cb-label-track, 0);"
)
_TILE_VALUE = f"{MONO} font-size: 13px; color: var(--cb-ink-1, #12324f); font-variant-numeric: tabular-nums;"
_PLOT_WH = (560, 170)
WARN_DOT = "var(--cb-warn-dot, #d97706)"


# ── what a job keeps of its own ──────────────────────────────────────────────

_PARTS: dict[str, Callable] = {}


def results_part(job_type: JobType):
    """Decorator: what a job type's Results shows under the shared part, built once per status.
    fn(job_type, instance_id, job_model, backend, ui_mgr)."""

    def decorator(fn: Callable) -> Callable:
        _PARTS[job_type.value] = fn
        return fn

    return decorator


def _has_run(jm, progress) -> bool:
    """A job with something to show: finished, failed, or running with a task done (or a job of its own, which
    writes as it goes)."""
    if not jm.relion_job_name:
        return False
    st = jm.execution_status
    if st in (JobStatus.SUCCEEDED, JobStatus.FAILED):
        return True
    if st == JobStatus.RUNNING:
        return progress is None or progress.n_settled > 0
    return False


def _meta(jm, progress) -> str:
    st = jm.execution_status
    if progress is not None and progress.total:
        words = f"{progress.n_ok} of {progress.total}"
        if progress.n_fail:
            words += f" · {progress.n_fail} failed"
        if progress.n_running or progress.n_pending:
            words += " · running"
        return words
    return {JobStatus.SUCCEEDED: "finished", JobStatus.FAILED: "failed", JobStatus.RUNNING: "so far"}.get(st, "")


class ResultsSection:
    """The section, hidden until the job has something to show. `on_shown(bool)` tells the page when it appears or
    goes (the page's grid gives it the first row). `open()` brings it into view (a URL naming "results")."""

    def __init__(
        self,
        job_type: JobType,
        instance_id: str,
        job_model,
        backend,
        ui_mgr,
        callbacks: dict | None,
        on_shown: Callable[[bool], None],
    ) -> None:
        self.job_type = job_type
        self.iid = instance_id
        self.jm = job_model
        self.backend = backend
        self.ui_mgr = ui_mgr
        self.callbacks = callbacks or {}
        self._on_shown = on_shown
        load_plugins()
        self._part = _PARTS.get(job_type.value)
        self._sig = None
        self._part_status = None
        self._shown = False
        self._flight = SingleFlight()
        self.panel = ui.column().classes("w-full cb-jsec-results").style(SECTION_STYLE)
        self.panel.props(f"id=cb-jsec-{instance_id}-results")
        with self.panel:
            self.head = section_head("Results", right=" ", accent=True, tooltip=OUTLIER_RULE)
            self.body = ui.column().classes("w-full").style("gap: 9px; min-width: 0;")
            self.own = ui.column().classes("w-full").style("gap: 8px; min-width: 0;")
        self.panel.set_visibility(False)
        ui_mgr.job_scope(instance_id).timer(3.0, self.tick, self.panel)
        owned_timer(0.05, self.tick, self.panel, once=True)

    # ── when to read ──
    def _job(self):
        state = get_project_state_for(Path(self.ui_mgr.project_path)) if self.ui_mgr.project_path else None
        return (state.jobs.get(self.iid) if state is not None else None) or self.jm

    def _progress(self):
        arr = array_status_cache.get(self.ui_mgr.project_path, self.iid)
        return arr.progress if arr is not None else None

    def _signature(self, jm, progress) -> tuple:
        live = jm.execution_status.is_live
        return (
            jm.execution_status,
            jm.relion_job_name,
            tuple(progress) if progress is not None else None,
            int(time.monotonic() // _LIVE_READ_S) if live else 0,
        )

    async def tick(self) -> None:
        if self.panel.is_deleted or not self.ui_mgr.project_path:
            return
        jm, progress = self._job(), self._progress()
        sig = self._signature(jm, progress)
        if sig == self._sig:
            return
        self._sig = sig
        if not _has_run(jm, progress):
            self._show(False)
            return
        await self._load(jm, progress)

    def reload(self) -> None:
        self._sig = None
        owned_timer(0.05, self.tick, self.panel, once=True)

    def open(self) -> None:
        ui.run_javascript(
            f"setTimeout(() => document.getElementById({f'cb-jsec-{self.iid}-results'!r})"
            "?.scrollIntoView({block: 'start', behavior: 'smooth'}), 60);"
        )

    def _show(self, on: bool) -> None:
        if on != self._shown:
            self._shown = on
            self.panel.set_visibility(on)
            self._on_shown(on)

    # ── reading ──
    async def _load(self, jm, progress) -> None:
        async with self._flight("load") as acquired:
            if not acquired:
                return
            pp = Path(self.ui_mgr.project_path)
            job_dir = pp / jm.relion_job_name.rstrip("/")
            arr = array_status_cache.get(pp, self.iid)
            tasks = dict(arr.statuses) if arr is not None else None
            spec = JOB_SPEC_BY_TYPE.get(self.job_type)
            needs_registry = spec is not None and spec.phase == PHASE_PREPROCESSING
            thumbs = tilt_thumb_dir(get_project_state_for(pp), pp)

            def read() -> JobResults:
                reg = {}
                if needs_registry or self.job_type == JobType.IMPORT_MOVIES:
                    reg = registry_dicts(list(get_registry_for(pp).all_tilt_series()), set(tasks) if tasks else None)
                return job_results(self.job_type.value, jm, job_dir, reg, self.iid, tasks=tasks, thumb_dir=thumbs)

            try:
                res = await asyncio.to_thread(read)
            except Exception:
                logger.exception("Results of %s", self.iid)
                res = None
            if self.panel.is_deleted:
                return
            self._draw(res, jm, progress, job_dir)

    # ── drawing ──
    def _draw(self, res: JobResults | None, jm, progress, job_dir: Path) -> None:
        shown = res is None or not res.empty or self._part is not None
        self._show(shown)
        if not shown:
            return
        meta = getattr(self.head, "meta_label", None)
        if meta is not None:
            meta.set_text(_meta(jm, progress))
        self.body.clear()
        with self.body:
            if res is None:
                ui.label("Reading this job's results failed: the server's log says why.").style(_BAD)
            else:
                self._words(res)
                if res.tiles:
                    self._tiles(res.tiles)
                if res.wall is not None and res.wall.tiles:
                    self._wall(res.wall, job_dir)
                if res.maps:
                    self._maps(res.maps)
                if res.plot is not None and res.plot.values:
                    self._plot(res.plot)
                if res.table is not None and res.table.rows:
                    self._table(res.table)
        if self._part is not None and self._part_status != jm.execution_status:
            self._part_status = jm.execution_status
            self.own.clear()
            with self.own:
                self._part(self.job_type, self.iid, jm, self.backend, self.ui_mgr)

    @staticmethod
    def _words(res: JobResults) -> None:
        if res.headline:
            ui.label(res.headline).style(_HEADLINE)
        for line in res.warn:
            ui.label(line).style(_BAD if line.startswith("failed:") else _WARN)
        # `lines` are the branch map's card's own words: what the page shows in its pictures and table.
        for line in res.notes:
            ui.label(line).style(_NOTE)

    @staticmethod
    def _tiles(tiles: list[tuple[str, str]]) -> None:
        with ui.element("div").style("display: flex; gap: 22px; flex-wrap: wrap; align-items: flex-end;"):
            for label, value in tiles:
                with ui.element("div").style("display: flex; flex-direction: column; gap: 1px;"):
                    ui.label(label).style(_TILE_LABEL)
                    ui.label(value).style(_TILE_VALUE)

    # ── the wall ──
    def _wall(self, w: Wall, job_dir: Path) -> None:
        pp = Path(self.ui_mgr.project_path)
        features = sorted({f for t in w.tiles for f, _u, _p in t.overlays})
        state = get_project_state_for(pp)
        sp = state.get_species(getattr(self.jm, "species_id", None)) if getattr(self.jm, "species_id", None) else None
        self._dot_color = getattr(sp, "color", None) or "var(--cb-accent, #4a8fd4)"
        n = len(w.tiles)
        with (
            ui.element("div")
            .classes("cb-rw-wrap")
            .style("display: flex; flex-direction: column; gap: 6px; width: 100%; min-width: 0;")
        ):
            if w.pair or w.value_title or w.pass_missing:
                with ui.element("div").style("display: flex; align-items: center; gap: 10px; min-height: 18px;"):
                    if w.value_title:
                        ui.label(f"under each: {w.value_title}").style(_SMALL)
                    if w.pass_missing:
                        self._kick_previews(job_dir)
                    if w.pair:
                        ui.element("div").style("flex: 1;")
                        before, after = w.pair
                        seg = Segmented([("before", before), ("after", after)], "after", lambda k: flip(k))
            style = (
                f"display: grid; grid-template-columns: repeat(auto-fill, minmax({FRAME_MIN_PX}px, 1fr)); gap: 8px; "
                f"width: 100%; align-items: start; {fl.css_vars(pp, features) if features else ''}"
            )
            grid = ui.element("div").classes("cb-rw" + (" cb-rw-folded" if n > FOLD else "")).style(style)
            with grid:
                for t in w.tiles:
                    self._tile(t, w)
            if n > FOLD:
                more = ui.label(f"all {n} ▾").classes("cb-rw-more")

                def fold() -> None:
                    folded = "cb-rw-folded" in grid.classes
                    if folded:
                        grid.classes(remove="cb-rw-folded")
                    else:
                        grid.classes(add="cb-rw-folded")
                    more.set_text("fewer ▴" if folded else f"all {n} ▾")

                more.on("click", lambda _e: fold())

        def flip(key: str) -> None:
            seg.set_active(key)
            if key == "before":
                grid.classes(add="cb-rw-show-before")
            else:
                grid.classes(remove="cb-rw-show-before")

    def _kick_previews(self, job_dir: Path) -> None:
        """Pick candidates' pictures come from the job's preview pass: started here when it has not run, and the wall
        read again once it lands."""
        from ui.particles.pick_viewer import _preview_render

        render, why = _preview_render(self.iid, self.jm, job_dir, Path(self.ui_mgr.project_path))
        if render is None:
            ui.label(f"no previews: {why}").style(_SMALL)
            return
        ui.label("the previews are being made (the task tray shows them)").style(_SMALL)
        keep_current(render, self.body, self.reload, on_failed=self.reload)

    def _tile(self, t: WallTile, w: Wall) -> None:
        cls = "cb-gal-tile cb-rw-tile" + (f" cb-rw-{t.flag}" if t.flag else "")
        tile = ui.element("div").classes(cls)
        tile._props["title"] = t.tip + (f"\n{t.why}" if t.why else "")
        aspect = t.aspect or w.aspect or 1.0
        with tile:
            frame = ui.element("div").classes("cb-gal-frame").style(f"aspect-ratio: {aspect:.4f};")
            with frame:
                if t.url:
                    self._picture(t, w)
                elif t.volume and t.slab:
                    self._slab_slot(t, w)
                else:
                    ui.label(self._no_picture(t, w)).classes("cb-gal-note")
            with ui.element("div").classes("cb-gal-cap"):
                ui.label(t.label).classes("cb-gal-name")
                if t.value:
                    ui.label(t.value).classes("cb-rw-val")
        if w.section is not None:
            tile.on("click", lambda _e, ts=t.ts, section=w.section: self._journey(ts, section))

    @staticmethod
    def _no_picture(t: WallTile, w: Wall) -> str:
        if t.note:
            return t.note
        if t.status == "fail":
            return "failed"
        if t.status in ("pending", "running"):
            return "waiting" if t.status == "pending" else "running"
        if t.status == "skip":
            return "skipped"
        if w.thumbs:
            return "no thumbnail"
        if w.pass_missing:
            return "being made"
        return "no picture"

    def _picture(self, t: WallTile, w: Wall) -> None:
        """The picture, with what goes over it: a segmentation's fills, the picks. On a before/after wall the before
        picture sits under it and the switch shows one or the other."""
        fit = "fill" if t.dots or t.overlays else "contain"
        if w.pair and t.before_url:
            with ui.element("div").classes("cb-rw-before").style("position: absolute; inset: 0;"):
                img = ui.element("img").classes("cb-gal-img").style(f"object-fit: {fit};")
                img.props.update(src=t.before_url, alt="", loading="lazy", decoding="async")
        with ui.element("div").classes("cb-rw-after").style("position: absolute; inset: 0;"):
            img = ui.element("img").classes("cb-gal-img").style(f"object-fit: {fit};")
            img.props.update(src=t.url, alt=t.ts, loading="lazy", decoding="async")
            for feature, mask_url, _path in t.overlays:
                ui.element("div").style(fl.fill_style(feature, mask_url))
            if t.dots:
                self._dots(t.dots)

    def _dots(self, dots: list[tuple[float, float]]) -> None:
        """The picks, one round-capped zero-length stroke each in a stroke that does not scale: a dot of the same few
        pixels on any frame, in the species' colour over a dark rim."""
        d = "".join(f"M{x:.2f} {y:.2f}h0" for x, y in dots)
        color = self._dot_color
        ui.html(
            '<svg viewBox="0 0 100 100" preserveAspectRatio="none" style="position:absolute;inset:0;width:100%;'
            'height:100%;pointer-events:none;overflow:visible">'
            f'<path d="{html.escape(d)}" stroke="#0b1220" stroke-opacity="0.7" stroke-width="4.4" '
            'stroke-linecap="round" vector-effect="non-scaling-stroke" fill="none"/>'
            f'<path d="{html.escape(d)}" stroke="{html.escape(color)}" stroke-width="2.6" stroke-linecap="round" '
            'vector-effect="non-scaling-stroke" fill="none"/></svg>',
            sanitize=False,
        ).style("position: absolute; inset: 0; pointer-events: none;")

    def _slab_slot(self, t: WallTile, w: Wall) -> None:
        """A frame whose slab is not on disk yet: the render's skeleton, filled with the slab once it lands."""
        from ui.particles.pick_viewer import xy_slab_render

        pp = Path(self.ui_mgr.project_path)
        slab = Path(t.slab)

        async def fill() -> None:
            t.url = await asyncio.to_thread(vis_asset_url, str(slab), THUMB)
            if w.pair and t.before_url is None:
                t.before_url = t.url
            self._picture(t, w)

        RenderSlot(xy_slab_render(slab, Path(t.volume), t.ts, pp, tile=True), fill, classes="cb-rslot-fill")

    async def _journey(self, ts: str, section: str | None) -> None:
        async with self._flight("journey") as acquired:
            if not acquired:
                return
            show = self.callbacks.get("show_journey")
            if show is None:
                ui.notify("The Journey is not available here", type="warning")
                return
            await show()
            show_ts = self.callbacks.get("journey_show_ts")
            if show_ts is not None:
                await show_ts(ts, section=section)

    # ── the maps ──
    def _maps(self, maps: list[MapPic]) -> None:
        with ui.element("div").style("display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-start;"):
            for m in maps:
                self._map(m)

    def _map(self, m: MapPic) -> None:
        w = 3 * MAP_CELL_PX + 2 * 4
        with ui.element("div").style("display: flex; flex-direction: column; gap: 3px;"):
            with ui.element("div").style("display: flex; align-items: baseline; gap: 6px; min-height: 14px;"):
                if m.swatch is not None:
                    ui.element("span").style(
                        f"width: 8px; height: 8px; border-radius: var(--cb-r-sm, 2px); background: "
                        f"{class_color(m.swatch)}; align-self: center;"
                    )
                name = ui.label(m.label).style(f"{SANS} font-size: 10px; font-weight: 600; color: var(--cb-ink-2);")
                if m.tip:
                    name.tooltip(m.tip).style(TOOLTIP_STYLE)
                if m.value:
                    ui.label(m.value).style(_SMALL.replace("9.5px", "10px") + " font-variant-numeric: tabular-nums;")
            box = ui.element("div").style(f"position: relative; width: {w}px; height: {MAP_CELL_PX}px;")
            with box:
                if m.url:
                    _map_cells(m.url)
                else:
                    pp = self.ui_mgr.project_path

                    async def fill(m=m) -> None:
                        url = await asyncio.to_thread(vis_asset_url, m.png)
                        _map_cells(url)

                    RenderSlot(map_slices_render(m.volume, m.png, pp), fill, classes="cb-rslot-fill")
            # The viewers' chips on a line of their own under the slices, never over them.
            with ui.element("div").style(f"position: relative; width: {w}px; height: 20px;"):
                viewer_chips(m.volume, what="map", chimerax=True)

    # ── the plot and the table ──
    def _plot(self, p: ResultPlot) -> None:
        with ui.element("div").style(f"width: 100%; max-width: {_PLOT_WH[0]}px;"):
            plot(_house_plot(p), *_PLOT_WH)

    @staticmethod
    def _table(t: ResultTable) -> None:
        wrap = ui.element("div").classes("cb-atable-wrap").style("max-height: 320px; border-top: none;")
        with wrap, ui.element("table").classes("cb-atable"):
            with ui.element("thead"), ui.element("tr"):
                for i, h in enumerate(t.head):
                    with ui.element("th").classes("n" if i in t.numeric else ""):
                        ui.label(h)
            with ui.element("tbody"):
                for row in t.rows:
                    with ui.element("tr"):
                        for i, cell in enumerate(row):
                            with ui.element("td").classes("n" if i in t.numeric else "nm"):
                                ui.label(cell)


def _map_cells(url: str) -> None:
    """The three slices of one picture (XY | XZ | YZ side by side) as three square cells a few pixels apart."""
    with ui.element("div").style(f"display: flex; gap: 4px; width: 100%; height: {MAP_CELL_PX}px;"):
        for i, axis in enumerate(("XY", "XZ", "YZ")):
            with (
                ui.element("div")
                .classes("cb-map-cell")
                .style(
                    f"position: relative; width: {MAP_CELL_PX}px; height: {MAP_CELL_PX}px; "
                    f'background: var(--cb-readout-canvas, #0f172a) url("{url}") no-repeat; '
                    f"background-size: 300% 100%; background-position: {i * 50}% 0; "
                    "border-radius: var(--cb-r-sm, 2px);"
                )
            ):
                ui.label(axis).classes("cb-map-axis")


def _nice(lo: float, hi: float, n: int = 4) -> list[float]:
    span = (hi - lo) or abs(hi) or 1.0
    raw = span / n
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    first = math.ceil(lo / step) * step
    out = []
    v = first
    while v <= hi + step * 1e-6:
        out.append(round(v, 10))
        v += step
    return out


def _fmt(v: float) -> str:
    return f"{v:.0f}" if abs(v) >= 100 else f"{v:.3g}"


def _house_plot(p: ResultPlot) -> Plot:
    """The reader's plot in the house plot: a value per tilt series as dots or bars over their order (the readout
    names the tilt series), a histogram as bars over its bins, a curve as a line."""
    vals = list(p.values)
    lo = p.lo if p.lo is not None else min(vals)
    hi = p.hi if p.hi is not None else max(vals) * 1.05
    if p.kind == "hist":
        lo, hi = 0.0, max(vals) * 1.1
    lo, hi = min(lo, *[r for r, _w in p.refs]) if p.refs else lo, max(hi, *[r for r, _w in p.refs]) if p.refs else hi
    if p.kind == "hist":
        xs = [float(x) for x in p.labels]
        x_lo, x_hi = (p.lo, p.hi) if p.lo is not None and p.hi is not None else (min(xs), max(xs))
        x_axis = Axis(x_lo, x_hi, [(v, _fmt(v)) for v in _nice(x_lo, x_hi, 6)], p.x_title, fmt=lambda v: f"{v:.1f} σ")
        y_axis = Axis(0.0, hi, [(v, _fmt(v)) for v in _nice(0.0, hi)], p.y_title, fmt=lambda v: f"{v:.0f}")
        series = [Series(xs, vals, SERIES[0], p.y_title, mode="bars")]
        refs = []
        marks = [(r, w) for r, w in p.refs]
        return Plot(series, x_axis, y_axis, refs=refs, marks=marks)
    xs = [float(i) for i in range(len(vals))]
    labels = p.labels

    def name_of(v: float) -> str:
        i = min(max(round(v), 0), len(labels) - 1)
        return labels[i] if labels else f"{v:g}"

    unit = f" {p.unit}" if p.unit else ""
    x_axis = Axis(-0.5, len(vals) - 0.5, [], p.x_title, fmt=name_of)
    y_axis = Axis(lo, hi, [(v, _fmt(v)) for v in _nice(lo, hi)], p.y_title, fmt=lambda v: f"{v:.3g}{unit}")
    mode = {"dots": "dots", "bars": "bars"}.get(p.kind, "line")
    colors = [WARN_DOT if i in p.flags else SERIES[0] for i in range(len(vals))] if mode != "line" else None
    series = [Series(xs, vals, SERIES[0], p.y_title.split(",")[0], mode=mode, colors=colors)]
    return Plot(series, x_axis, y_axis, refs=list(p.refs))
