# ui/tilt_filter_panel.py
"""
The tilt-filter job page (full-panel job plugin): a review panel over the shared tilt gallery.

On top, the numbers of the review (the same in every mode), then the review's state and every
action (the job row holds the settings only), with the DL run in one line, then the
distributions, collapsed under one toggle. Under it, the Tilts tab's gallery
(``ui/tilt_previews.py``) in review mode: a click on a tilt toggles its exclusion, and a red
border is every exclusion, whoever set it. A label, a threshold move or Clear labels re-derives
the states and updates the cards, the header strips, the numbers and the charts in place;
nothing re-renders. A landed DL run or a commit re-collects. The page reads what the Tilts tab
reads: the registry and the job's review, collected off the event loop.
"""

from __future__ import annotations

import asyncio
import html
import logging
import math
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from nicegui import ui

from services.configs.config_service import get_config_service
from services.dashboard_data import tilt_thumb_dir
from services.jobs.tilt_filter import DL_MODES, FilterMode, calibrated_threshold, effective_label, prediction_liveness
from services.models_base import JobStatus
from services.project_state import get_project_state_for
from services.tilt_series_service import ensure_tilt_thumbnails
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.fields import house_number
from ui.components.reactive import FingerprintedView, SingleFlight, owned_timer
from ui.dashboard.css import ensure_assets_loaded
from ui.dashboard.figures import (
    SMALL_CHART_PX,
    TILT_STATES,
    build_excluded_by_tilt_chart,
    build_excluded_per_series_chart,
    build_score_histogram,
)
from ui.pipeline_builder.tilt_filter_row import render_tilt_filter_review
from ui.status_indicator import _running_spinner_html
from ui.styles import MONO, SANS
from ui.tilt_previews import (
    CONFIDENCE_TIP,
    DARK_RULE,
    JUDGED_METRICS,
    METRIC_LABELS,
    OUTLIER_RULE,
    TiltGallery,
    collect_tilt_groups,
    registry_tilt_series,
    thumbnail_task_key,
    tilt_context,
)

logger = logging.getLogger(__name__)

_INK, _LABEL, _MUTED, _RED, _AMBER = "#0f172a", "#475569", "#94a3b8", "#be4343", "#b45309"
# The distributions' stage-tilt bins and confidence-score bins.
_BIN_DEG = 10
_SCORE_BINS = 20

# The numbers on top: (key, label, tooltip; empty where the tooltip follows the review).
_TILES = (
    ("tilts", "tilts", "Every tilt of the project."),
    ("series", "series", "Tilt series with a tilt to show."),
    ("excluded", "to exclude", ""),
    ("used", "in tomograms", "Tilts in alignment's output, over the tilt series alignment has run on."),
    ("dark", "dark", DARK_RULE),
    ("outliers", "outliers", ""),
)


def _n_labels(n: int) -> str:
    return f"{n} manual label" if n == 1 else f"{n} manual labels"


async def _confirm_clear_labels(labels: dict[str, str], mode: FilterMode) -> bool:
    """Ask before Clear labels: it removes every manual label on this job, with no undo."""
    n = len(labels)
    n_bad = sum(1 for v in labels.values() if v == "bad")
    after = (
        "Every tilt then follows its confidence score, and stays included where it has none."
        if mode in DL_MODES
        else "Every tilt is then included."
    )
    with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-96"):
        ui.label(f"Clear {_n_labels(n)}?").style(f"{SANS} font-size: 13px; font-weight: 600; color: {_INK};")
        ui.label(f"{n_bad} exclude and {n - n_bad} keep a tilt. {after} This cannot be undone.").style(
            f"{SANS} font-size: 12px; color: {_LABEL}; margin-top: 4px;"
        )
        with ui.row().classes("w-full justify-end mt-3 gap-2"):
            house_button("Cancel", lambda: dlg.submit(False))
            house_button(f"Clear {_n_labels(n)}", lambda: dlg.submit(True), kind="danger")
    go = await dlg
    dlg.delete()
    return bool(go)


def render_tilt_filter_job_panel(job_type, instance_id, job_model, backend, ui_mgr, save_handler) -> None:
    """Entry point for the tilt filter when rendered as a pipeline job (full-panel plugin)."""
    project_path = ui_mgr.project_path
    if not project_path:
        ui.label("No project loaded").classes("text-red-500 p-4")
        return
    TiltFilterPage(backend, Path(project_path), instance_id).mount()


class TiltFilterPage:
    """One tilt-filter job page: the panel and the gallery, and what keeps them current."""

    def __init__(self, backend, project_path: Path, instance_id: str) -> None:
        self.backend = backend
        self.project_path = project_path
        self.iid = instance_id
        self.gallery = TiltGallery(
            project_path, review=True, on_toggle=self._on_toggle, on_metrics=self._update_numbers, size_control=True
        )
        self.groups: list[dict] = []
        self.facts: dict = {}
        self.error: str | None = None
        self.awaiting = 0  # tilts with an average but no preview PNG yet
        self._png_dir: Path | None = None
        self._ts_of: dict[str, str] = {}  # frame id → tilt series id
        self._seen: tuple | None = None  # what the last collection saw of the job (_job_key)
        self._applied_threshold: float | None = None
        self._flight = SingleFlight()
        self._tiles: dict[str, Any] = {}
        self._tile_labels: dict[str, Any] = {}
        self._tile_tips: dict[str, Any] = {}
        self._charts: dict[str, Any] = {}
        self._charts_box: Any = None
        self._charts_chev: Any = None
        self._charts_open = False
        self._agreement_box: Any = None
        self._dl_line: _DlRunLine | None = None
        self._controls: Any = None
        self._body: Any = None

    def _state_and_job(self):
        state = get_project_state_for(self.project_path)
        return state, state.jobs.get(self.iid)

    @staticmethod
    def _job_key(jm) -> tuple:
        """What the gallery's data depends on in the job: a landed run, a commit or a re-open."""
        run = jm.predict_run
        return (jm.execution_status, None if run is None else (run.job_dir, run.status))

    # ── Layout ──

    def mount(self) -> None:
        ensure_assets_loaded()
        _state, jm = self._state_and_job()
        if jm is None:
            ui.label(f"Job '{self.iid}' not found.").classes("text-red-500 p-4")
            return
        self._applied_threshold = jm.threshold
        page = ui.element("div").style("width: 100%; flex: 1 1 0; min-height: 0; overflow-y: auto; overflow-x: hidden;")
        with page:
            with ui.element("div").style("display: flex; flex-direction: column; gap: 10px; padding: 10px 14px 4px;"):
                self._render_numbers()
                self._render_review(jm)
                self._render_charts_toggle()
            self._controls = ui.element("div").style("padding: 4px 14px 0; min-width: 0;")
            self._body = ui.element("div").style("min-width: 0;")
            with self._body, ui.element("div").classes("cb-empty"):
                ui.spinner(size="lg", color="indigo")
                ui.label("Reading tilts…").classes("text-xs")
        # Owned timers: a mode switch rebuilds the page.
        owned_timer(3.0, self._observe, page)
        owned_timer(15.0, self._poll_previews, page)
        owned_timer(0.05, self._reload, page, once=True)

    def _render_numbers(self) -> None:
        """The review's numbers, the same in every mode; the hovers break them down. Built once;
        updates set their text."""
        with ui.element("div").classes("cb-stats cb-stats-inline").style("margin: 0; gap: 18px;"):
            for key, label, tip in _TILES:
                with ui.element("div").classes("cb-stat"):
                    self._tile_labels[key] = ui.label(label).classes("cb-stat-label")
                    self._tiles[key] = ui.label("—").classes("cb-stat-value")
                    self._tile_tips[key] = ui.tooltip(tip).style("white-space: pre-line; max-width: 420px;")

    def _render_review(self, jm) -> None:
        """The review's state and actions, Clear labels, the threshold (editable in DL review,
        where it moves the red borders live; set on the job row in DL auto) and the DL run."""
        with ui.element("div").style("display: flex; align-items: center; gap: 12px; flex-wrap: wrap; min-width: 0;"):
            render_tilt_filter_review(self.backend, self.project_path, self.iid)
            house_button(
                "Clear labels",
                self._clear_labels,
                tooltip="Remove every manual label: each tilt goes back to its confidence score (DL) or to "
                "included (Manual).",
            )
            if jm.mode == FilterMode.DL_REVIEW:
                self._render_threshold(jm)
            elif jm.mode == FilterMode.DL_AUTO:
                ui.label().bind_text_from(
                    jm, "threshold", backward=lambda v: f"threshold {v:.2f}, set on the job row"
                ).style(f"{MONO} font-size: 9px; color: {_LABEL};")
            if jm.mode in DL_MODES:
                box = ui.element("div").style("min-width: 0;")
                self._dl_line = _DlRunLine(self, box)
                self._dl_line.refresh()
                owned_timer(3.0, self._dl_line.refresh, box)

    def _render_threshold(self, jm) -> None:
        # The cut belongs to the model whose scores the page shows.
        shown_model = jm.predict_run.model if jm.predict_run is not None else jm.model
        cut = calibrated_threshold(shown_model)
        hint = "Tilts with a confidence score at or above this are excluded, unless a manual label keeps them."
        if cut is not None:
            hint += f" conf.yaml records {cut:.2f} as the cut for {shown_model or 'the default model'}."
        with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
            house_number(
                "threshold",
                model=jm,
                attr="threshold",
                min=0.0,
                max=1.0,
                step=0.05,
                format="%.2f",
                width="w-16",
                hint=hint,
                on_change=self._on_threshold,
            )
            if cut is None:
                ui.label("uncalibrated").style(f"{SANS} font-size: 9px; color: {_AMBER};").tooltip(
                    "No confidence-score cut is recorded for this model in conf.yaml, so this threshold has not "
                    "been measured on labelled tilts."
                )

    def _render_charts_toggle(self) -> None:
        """The distributions, collapsed by default under one toggle and built on the first open."""
        head = ui.element("div").style(
            "display: inline-flex; align-items: center; gap: 4px; cursor: pointer; user-select: none; "
            "width: fit-content;"
        )
        with head:
            self._charts_chev = ui.label("▸").classes("cb-tp-chev")
            ui.label("charts").style(f"{SANS} font-size: 10px; font-weight: 600; color: #334155;")
        head.on("click", self._toggle_charts)
        self._charts_box = ui.element("div").style("display: none; min-width: 0;")

    def _toggle_charts(self) -> None:
        self._charts_open = not self._charts_open
        self._charts_box.style(f"display: {'block' if self._charts_open else 'none'}; min-width: 0;")
        if self._charts_open:
            self._charts_chev.classes(add="open")
        else:
            self._charts_chev.classes(remove="open")
        if not self._charts_open:
            return
        if not self._charts:
            with self._charts_box:
                self._build_charts()
            self._update_charts()
            return
        # Hidden, a chart keeps its old size; it measures its box again once shown.
        for chart in self._charts.values():
            if not chart.is_deleted:
                chart.run_chart_method("resize")

    def _build_charts(self) -> None:
        _state, jm = self._state_and_job()
        dl = jm is not None and jm.mode in DL_MODES
        # One legend for every chart: each state keeps its colour across them.
        items = "".join(
            '<span style="display:inline-flex;align-items:center;gap:4px;">'
            f'<span style="width:8px;height:8px;border-radius:50%;background:{color};"></span>'
            f"{html.escape(name)}</span>"
            for key, name, color in TILT_STATES
            if dl or key != "kept"
        )
        ui.html(
            '<div style="display:flex;flex-wrap:wrap;gap:4px 14px;font-family:IBM Plex Sans,sans-serif;font-size:9px;'
            f'color:#475569;padding:4px 0 2px;">{items}</div>',
            sanitize=False,
        )
        with ui.element("div").style("display: flex; gap: 14px; flex-wrap: wrap; min-width: 0;"):
            self._charts["tilt"] = self._small_chart(
                "excluded by stage tilt",
                "Per 10° of stage tilt: the tilts excluded, those alignment's output lacks with no verdict against "
                "them (Warp's import), and the dark exposures kept. A tilt counts once, under the first that "
                "applies.",
            )
            if dl:
                self._charts["score"] = self._small_chart(
                    "confidence score",
                    f"{CONFIDENCE_TIP} How many tilts score in each bin, excluded or kept as the review stands (a "
                    "manual label can keep a high score or exclude a low one); the dashed line is the threshold.",
                )
            self._charts["series"] = self._small_chart(
                "excluded per tilt series",
                "Each tilt series' excluded tilts and those alignment's output lacks, in position order; hover a bar "
                "for its name.",
            )
            if dl:
                self._agreement_box = ui.element("div").style("flex: 0 0 auto; min-width: 0;")

    @staticmethod
    def _small_chart(title: str, tip: str):
        with ui.element("div").style("flex: 1 1 240px; min-width: 0;"):
            ui.label(title).style(f"{SANS} font-size: 9px; font-weight: 600; color: #334155;").tooltip(tip)
            return ui.echart(_empty_options()).style(f"height: {SMALL_CHART_PX}px; width: 100%;")

    # ── Collection ──

    async def _collect(self) -> None:
        """The registry and the review, snapshot on the loop; the derivation in a thread."""
        state, jm = self._state_and_job()
        tilt_series, self.error = registry_tilt_series(self.project_path)
        ctx = tilt_context(state)
        png_dir = self._png_dir = tilt_thumb_dir(state, self.project_path)
        self.groups, self.facts = await asyncio.to_thread(collect_tilt_groups, tilt_series, png_dir, ctx)
        self._seen = self._job_key(jm) if jm is not None else None
        self.gallery.set_data(self.groups, self.facts)
        self._ts_of = {t["key"]: g["ts"] for g in self.groups for t in g["tilts"]}
        self.awaiting = sum(1 for g in self.groups for t in g["tilts"] if t["png"] is None and t["mrc"])
        if self.awaiting and not any(g["n_png"] for g in self.groups):
            # No previews at all: the pass that makes them normally starts when fsMotion lands.
            ensure_tilt_thumbnails(self.project_path, state)

    async def _reload(self) -> None:
        async with self._flight("reload") as acquired:
            if not acquired:
                return
            await self._collect()
            self._render_gallery()

    def _render_gallery(self) -> None:
        if self._body is None or self._body.is_deleted:
            return
        self._controls.clear()
        if self.groups:
            with self._controls:
                self.gallery.render_controls()
        self._body.clear()
        with self._body:
            self.gallery.render(self._notes())
        self._update_numbers()

    def _notes(self) -> list[tuple[str, str, bool]]:
        notes: list[tuple[str, str, bool]] = []
        if self.error:
            notes.append((self.error, _AMBER, False))
        if self.awaiting:
            running = self._thumbs_running()
            tail = "; they are being rendered in the background." if running else "."
            notes.append((f"{self.awaiting} tilts have no preview image yet{tail}", _MUTED, running))
        return notes

    def _thumbs_running(self) -> bool:
        if self._png_dir is None:
            return False
        return BackgroundTask.existing(thumbnail_task_key(self.project_path, self._png_dir)) is not None

    async def _observe(self) -> None:
        """A landed DL run, a commit or a re-open changes what the cards show: re-collect, and
        re-render the groups (the gallery keeps the open ones open)."""
        _state, jm = self._state_and_job()
        if jm is None or self._seen is None or self._job_key(jm) == self._seen:
            return
        await self._reload()

    async def _poll_previews(self) -> None:
        """While tilts wait for their previews, pick up the ones that landed."""
        if not self.awaiting or self._body is None or self._body.is_deleted:
            return
        before = sum(g["n_png"] for g in self.groups)
        async with self._flight("reload") as acquired:
            if not acquired:
                return
            await self._collect()
            if sum(g["n_png"] for g in self.groups) != before:
                self._render_gallery()

    # ── The review, live ──

    def _restate(self, ts_ids=None) -> None:
        """Re-derive the tilts after a label or threshold change, and update the cards, the
        headers, the numbers and the charts in place."""
        state, _jm = self._state_and_job()
        self.gallery.restate(tilt_context(state), ts_ids)
        self._update_numbers()

    async def _on_toggle(self, key: str) -> None:
        """A click on a tilt flips it between excluded and included: a manual label saying the
        opposite of what the review says now. Refused once the review is approved."""
        _state, jm = self._state_and_job()
        tilt = self.gallery.tilt(key)
        if jm is None or tilt is None:
            return
        if jm.execution_status == JobStatus.SUCCEEDED:
            ui.notify("Approved: Re-open to change labels.", type="info")
            return
        s = tilt["state"]
        current = effective_label(key, s.p_bad, jm.tilt_labels, jm.threshold, jm.mode)
        jm.tilt_labels[key] = "good" if current == "bad" else "bad"
        self._restate([self._ts_of[key]] if key in self._ts_of else None)
        # Not a USER_PARAMS field, so nothing marks the project dirty: force the save.
        await self.backend.save_project(self.project_path, force=True, debounce_s=1.0)

    async def _on_threshold(self, _e) -> None:
        # The field refuses out-of-range and half-typed values, which leave the threshold as it was.
        _state, jm = self._state_and_job()
        if jm is None or jm.threshold == self._applied_threshold:
            return
        self._applied_threshold = jm.threshold
        self._restate()
        await self.backend.save_project(self.project_path, force=True, debounce_s=1.0)

    async def _clear_labels(self) -> None:
        async with self._flight("clear") as acquired:
            if not acquired:
                return
            _state, jm = self._state_and_job()
            if jm is None:
                return
            labels = jm.tilt_labels
            if not labels:
                ui.notify("No manual labels to clear.", type="info")
                return
            if not await _confirm_clear_labels(labels, jm.mode):
                return
            n = len(labels)
            labels.clear()
            self._restate()
            ui.notify(f"{_n_labels(n)} cleared.", type="info")
            await self.backend.save_project(self.project_path, force=True, debounce_s=1.0)

    # ── Numbers and charts ──

    def _states(self) -> list:
        return [s for g in self.groups for s, _title in g["ticks"]]

    def _update_numbers(self) -> None:
        if not self._tiles or self._tiles["tilts"].is_deleted:
            return
        _state, jm = self._state_and_job()
        states = self._states()
        committed = jm is not None and jm.execution_status == JobStatus.SUCCEEDED
        aligned = [g["summary"] for g in self.groups if g["summary"].used is not None]
        ticked = self.gallery.metrics_on & set(JUDGED_METRICS)
        per_metric = Counter(k for g in self.groups for t in g["tilts"] for k in t["outliers"])
        values = {
            "tilts": str(len(states)),
            "series": str(len(self.groups)),
            "excluded": str(sum(1 for s in states if s.excluded)),
            "used": str(sum(s.used for s in aligned)) if aligned else "—",
            "dark": str(sum(1 for s in states if s.is_dark)),
            "outliers": str(sum(1 for g in self.groups for t in g["tilts"] if t["outliers"] & ticked))
            if ticked
            else "—",
        }
        for key, text in values.items():
            self._tiles[key].set_text(text)
        self._tile_labels["excluded"].set_text("excluded" if committed else "to exclude")
        self._tile_tips["excluded"].set_text(_excluded_tip(jm, states, committed))
        self._tile_tips["outliers"].set_text(_outliers_tip(per_metric, ticked))
        self._update_charts()

    def _update_charts(self) -> None:
        if not self._charts:
            return
        _state, jm = self._state_and_job()
        states = self._states()
        builders = {
            "tilt": lambda: _tilt_options(states),
            "score": lambda: _score_options(states, jm.threshold if jm is not None else None),
            "series": lambda: _series_options(self.groups),
        }
        for key, chart in self._charts.items():
            if not chart.is_deleted:
                _set_options(chart, builders[key]())
        if self._agreement_box is not None and not self._agreement_box.is_deleted and jm is not None:
            self._agreement_box.clear()
            with self._agreement_box:
                _render_agreement(states, jm)


def _excluded_tip(jm, states, committed: bool) -> str:
    if committed:
        return "Excluded by the approved verdict: alignment, CTF and reconstruction leave them out."
    n_manual = sum(1 for s in states if s.review == "human_bad")
    lines = ["Approve labels excludes them from alignment, CTF and reconstruction."]
    if jm is not None and jm.mode in DL_MODES:
        n_model = sum(1 for s in states if s.review == "model_bad")
        n_keep = sum(1 for s in states if s.review == "human_good" and s.p_bad is not None and s.p_bad >= jm.threshold)
        lines.append(
            f"{n_model} by the confidence score at or above {jm.threshold:.2f} · {n_manual} by a manual label · "
            f"{n_keep} manual labels keep a tilt the score would exclude."
        )
    else:
        lines.append(f"{n_manual} by a manual label.")
    return "\n".join(lines)


def _outliers_tip(per_metric: Counter, ticked: set[str]) -> str:
    head = (
        "Tilts with an outlier in a ticked metric."
        if ticked
        else "No metric with outliers is ticked: tick one in the gallery's metrics menu to mark its outliers."
    )
    every = " · ".join(f"{METRIC_LABELS[k]} {per_metric[k]}" for k in JUDGED_METRICS if per_metric.get(k))
    return "\n".join([head, f"Every metric: {every}." if every else "No metric has outliers.", OUTLIER_RULE])


def _empty_options() -> dict:
    return {"animation": False, "xAxis": {"show": False}, "yAxis": {"show": False}, "series": []}


def _set_options(chart, options: dict) -> None:
    chart.options.clear()
    chart.options.update(options)
    chart.update()


def _deg(value: int) -> str:
    return f"−{-value}" if value < 0 else str(value)


def _state_key(s) -> str | None:
    """The chart state a tilt counts under: the first of excluded, not in alignment's output and
    dark (kept); None for the rest."""
    if s.excluded:
        return "excluded"
    if s.in_tomogram is False:
        return "out"
    if s.is_dark:
        return "dark"
    return None


def _tilt_options(states) -> dict:
    """Per 10° bin of signed stage tilt, each tilt under the first state that applies."""
    if not states:
        return _empty_options()
    lo = math.floor(min(s.angle for s in states) / _BIN_DEG)
    hi = math.floor(max(s.angle for s in states) / _BIN_DEG)
    counts = {key: [0] * (hi - lo + 1) for key in ("excluded", "out", "dark")}
    for s in states:
        key = _state_key(s)
        if key is not None:
            counts[key][math.floor(s.angle / _BIN_DEG) - lo] += 1
    bins = [f"{_deg(b * _BIN_DEG)}…{_deg((b + 1) * _BIN_DEG)}" for b in range(lo, hi + 1)]
    return build_excluded_by_tilt_chart(bins, counts)


def _score_options(states, threshold: float | None) -> dict:
    """The confidence score's histogram, excluded or kept as the review stands."""
    scored = [s for s in states if s.p_bad is not None]
    if not scored:
        return _empty_options()
    counts = {"excluded": [0] * _SCORE_BINS, "kept": [0] * _SCORE_BINS}
    for s in scored:
        counts["excluded" if s.excluded else "kept"][min(_SCORE_BINS - 1, int(s.p_bad * _SCORE_BINS))] += 1
    return build_score_histogram(counts, threshold)


def _series_options(groups) -> dict:
    """Per tilt series, in position order: its excluded tilts and those alignment's output lacks."""
    if not groups:
        return _empty_options()
    counts = {
        "excluded": [len(g["summary"].excluded) for g in groups],
        "out": [sum(1 for s in g["summary"].left_out if not s.excluded) for g in groups],
    }
    return build_excluded_per_series_chart([g["label"] for g in groups], counts)


def _render_agreement(states, jm) -> None:
    """Manual labels against the DL calls, as a count table: shown once both exist."""
    scored = [s for s in states if s.p_bad is not None]
    labels = jm.tilt_labels
    if not scored or not labels:
        return
    rows = {"exclude": [0, 0, 0], "keep": [0, 0, 0]}
    for s in scored:
        label = labels.get(s.frame_id)
        column = 0 if label == "bad" else 1 if label == "good" else 2
        rows["exclude" if s.p_bad >= jm.threshold else "keep"][column] += 1
    th = f"padding:0 0 2px 10px;font-weight:400;color:{_MUTED};text-align:right;white-space:nowrap;"
    td = f"padding:1px 0 1px 10px;{MONO} text-align:right;color:#334155;"
    body = "".join(
        f'<tr><td style="color:{_LABEL};white-space:nowrap;">DL {call}</td>'
        + "".join(f'<td style="{td}">{n}</td>' for n in rows[call])
        + "</tr>"
        for call in ("exclude", "keep")
    )
    ui.label("manual labels vs DL calls").style(f"{SANS} font-size: 9px; font-weight: 600; color: #334155;").tooltip(
        f"Scored tilts by the DL call at the threshold {jm.threshold:.2f} (rows) and the manual label (columns). "
        "At Approve a manual label wins."
    )
    ui.html(
        '<table style="border-collapse:collapse;margin-top:6px;font-family:IBM Plex Sans,sans-serif;font-size:9px;">'
        f'<tr><th></th><th style="{th}">manual exclude</th><th style="{th}">manual keep</th>'
        f'<th style="{th}">no label</th></tr>{body}</table>',
        sanitize=False,
    )


class _DlRunLine(FingerprintedView):
    """The DL run in one line beside the actions: run NNN and its state (the moving dot and the
    minutes while it is queued or running), its details in the tooltip; red when it failed or
    when the model gives every tilt the same confidence score."""

    def __init__(self, page: TiltFilterPage, container: Any) -> None:
        super().__init__(container)
        self.page = page

    def _predictions(self) -> tuple[int, int]:
        _state, jm = self.page._state_and_job()
        p = [s.p_bad for s in self.page._states() if s.p_bad is not None]
        return len(p), sum(1 for v in p if jm is not None and v >= jm.threshold)

    def _liveness(self) -> tuple[bool, float] | None:
        by_series = {g["ts"]: [s.p_bad for s, _t in g["ticks"] if s.p_bad is not None] for g in self.page.groups}
        return prediction_liveness({k: v for k, v in by_series.items() if v})

    def signature(self):
        _state, jm = self.page._state_and_job()
        if jm is None:
            return None
        run = jm.predict_run
        # While a run is in flight, the elapsed minutes move the line.
        ticking = run is not None and jm.predict_in_flight
        elapsed = int((datetime.now() - run.submitted_at).total_seconds() // 60) if ticking else None
        key = None if run is None else (run.job_dir, run.model, run.status, run.slurm_job_id, run.error)
        return (jm.model, jm.threshold, key, elapsed, self._predictions(), id(self.page.groups))

    def render(self):
        _state, jm = self.page._state_and_job()
        if jm is None:
            return
        run = jm.predict_run
        model = jm.model or get_config_service().tilt_filter.default_model or "no model"
        style = f"{MONO} font-size: 9px; white-space: nowrap;"
        if run is None:
            tail = "; the DL-auto job scores the tilts on Run" if jm.mode == FilterMode.DL_AUTO else ""
            ui.label(f"no DL run yet{tail}").style(f"{style} color: {_MUTED};").tooltip(
                f"Run DL scores every tilt with {model}."
            )
            return
        name = Path(run.job_dir).name
        in_flight = jm.predict_in_flight
        liveness = self._liveness()
        if run.status == JobStatus.FAILED:
            text, color = f"DL run {name} failed", _RED
        elif liveness is not None and not liveness[0]:
            text, color = f"DL run {name}: every tilt scores {liveness[1]:.2f}, so its calls mean nothing", _RED
        elif in_flight:
            minutes = int((datetime.now() - run.submitted_at).total_seconds() // 60)
            text, color = f"DL run {name} {run.status.value.lower()} · {minutes} min", _LABEL
        else:
            text, color = f"DL run {name} · {run.status.value.lower()} {run.submitted_at:%H:%M}", _MUTED
        with ui.element("div").style("display: flex; align-items: center; gap: 5px; min-width: 0;"):
            if in_flight:
                ui.html(_running_spinner_html(10, "#2563eb"), sanitize=False)
            with ui.label(text).style(f"{style} color: {color};"):
                ui.tooltip(self._details(jm, run, model)).style("white-space: pre-line;")

    def _details(self, jm, run, model: str) -> str:
        slurm = f" · SLURM job {run.slurm_job_id}" if run.slurm_job_id else ""
        lines = [f"Model: {run.model or model}", f"Submitted {run.submitted_at:%Y-%m-%d %H:%M}{slurm}", run.job_dir]
        if run.status == JobStatus.FAILED:
            lines.append(run.error or f"No reason recorded; see {run.job_dir}/run.err.")
        elif run.status == JobStatus.SUCCEEDED:
            n, k = self._predictions()
            lines.append(f"{n} tilts scored · {k} at or above {jm.threshold:.2f}")
        return "\n".join(lines)
