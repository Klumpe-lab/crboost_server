# ui/tilt_filter_panel.py
"""
The tilt-filter job page (full-panel job plugin): a review panel over the shared tilt gallery.

The panel on top holds the review's state and every action (the job row holds the settings
only), the DL run, the numbers and the distributions of the tilts out of the tomograms or
flagged. Under it, the Tilts tab's gallery (``ui/tilt_previews.py``) in review mode: a flag in
each image's corner labels the tilt. A label, a threshold move or Clear labels re-derives the
states and updates the cards, the header strips, the tiles and the charts in place; nothing
re-renders. A landed DL run or a commit re-collects. The page reads what the Tilts tab reads:
the registry and the job's review, collected off the event loop.
"""

from __future__ import annotations

import asyncio
import logging
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
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.dashboard.css import ensure_assets_loaded
from ui.dashboard.figures import P_BAD_LABELS, TILT_CAUSES, build_p_bad_histogram, build_tilt_band_chart
from ui.pipeline_builder.tilt_filter_row import render_tilt_filter_review
from ui.status_indicator import _running_spinner_html
from ui.styles import MONO, SANS
from ui.tilt_previews import (
    DARK_RULE,
    OUTLIER_RULE,
    TiltGallery,
    collect_tilt_groups,
    registry_tilt_series,
    thumbnail_task_key,
    tilt_context,
)

logger = logging.getLogger(__name__)

_INK, _LABEL, _MUTED, _RED, _AMBER = "#0f172a", "#475569", "#94a3b8", "#be4343", "#b45309"
# The distributions' |stage tilt| bands and P(bad) bins.
_BAND_DEG = 10
_P_BAD_BINS = 20


def _n_labels(n: int) -> str:
    return f"{n} label" if n == 1 else f"{n} labels"


async def _confirm_clear_labels(labels: dict[str, str], mode: FilterMode) -> bool:
    """Ask before Clear labels: it removes every label set by hand on this job, with no undo."""
    n = len(labels)
    n_bad = sum(1 for v in labels.values() if v == "bad")
    after = (
        "Every tilt then reads as the model predicts it, and good where it has no prediction."
        if mode in DL_MODES
        else "Every tilt then reads good."
    )
    with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-96"):
        ui.label(f"Clear {_n_labels(n)}?").style(f"{SANS} font-size: 13px; font-weight: 600; color: {_INK};")
        ui.label(f"{n_bad} bad and {n - n_bad} good, set by hand. {after} This cannot be undone.").style(
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
        self.gallery = TiltGallery(project_path, review=True, on_flag=self._on_flag, size_control=True)
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
        self._agreement: Any = None
        self._band_chart: Any = None
        self._hist_chart: Any = None
        self._dl_card: _DlRunCard | None = None
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
        with ui.element("div").style("width: 100%; flex: 1 1 0; min-height: 0; overflow-y: auto; overflow-x: hidden;"):
            with ui.element("div").style("display: flex; flex-direction: column; gap: 22px; padding: 12px 14px 6px;"):
                self._render_review(jm)
                if jm.mode in DL_MODES:
                    box = ui.element("div").style("min-width: 0;")
                    self._dl_card = _DlRunCard(self, box)
                    self._dl_card.refresh()
                    ui.timer(3.0, self._dl_card.refresh)
                self._render_numbers(jm)
                self._render_charts(jm)
            self._controls = ui.element("div").style("padding: 4px 14px 0; min-width: 0;")
            self._body = ui.element("div").style("min-width: 0;")
            with self._body, ui.element("div").classes("cb-empty"):
                ui.spinner(size="lg", color="indigo")
                ui.label("Reading tilts…").classes("text-xs")
        ui.timer(3.0, self._observe)
        ui.timer(15.0, self._poll_previews)
        ui.timer(0.05, self._reload, once=True)

    def _render_review(self, jm) -> None:
        """The review's state and actions, Clear labels, and the threshold: editable here in DL
        review, where it moves the labels live; set on the job row in DL auto."""
        with ui.element("div").style("display: flex; align-items: center; gap: 14px; flex-wrap: wrap; min-width: 0;"):
            render_tilt_filter_review(self.backend, self.project_path, self.iid)
            house_button(
                "Clear labels",
                self._clear_labels,
                tooltip="Remove every label set by hand; each tilt goes back to the model's call (DL modes) or to "
                "good (Manual).",
            )
            if jm.mode == FilterMode.DL_REVIEW:
                self._render_threshold(jm)
            elif jm.mode == FilterMode.DL_AUTO:
                ui.label().bind_text_from(
                    jm, "threshold", backward=lambda v: f"threshold {v:.2f}, set on the job row"
                ).style(f"{MONO} font-size: 10px; color: {_LABEL};")

    def _render_threshold(self, jm) -> None:
        # The cut belongs to the model whose predictions the page shows.
        shown_model = jm.predict_run.model if jm.predict_run is not None else jm.model
        cut = calibrated_threshold(shown_model)
        hint = "A tilt with P(bad) at or above this is the model's bad; your labels win."
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
                    "No P(bad) cut is recorded for this model in conf.yaml, so this threshold has not been measured "
                    "on labelled tilts."
                )

    def _render_numbers(self, jm) -> None:
        """The tiles, and in DL review the agreement line. Built once; updates set their text."""
        with ui.element("div").style("display: flex; flex-direction: column; gap: 4px; min-width: 0;"):
            with ui.element("div").classes("cb-stats").style("margin-bottom: 0;"):
                for key, label, tip in (
                    ("tilts", "tilts", "Every tilt of the project, and its tilt series."),
                    ("used", "in the tomogram", "Tilts in alignment's output, over the series alignment has run on."),
                    (
                        "flagged",
                        "flagged · yours · the model's",
                        "Marked bad and not approved yet: Approve drops them.",
                    ),
                    ("dropped", "dropped", "Dropped by the committed verdict."),
                    ("dark", "dark", DARK_RULE),
                    ("outliers", "outliers", OUTLIER_RULE),
                ):
                    with ui.element("div").classes("cb-stat").tooltip(tip):
                        ui.label(label).classes("cb-stat-label")
                        self._tiles[key] = ui.label("—").classes("cb-stat-value")
            if jm.mode == FilterMode.DL_REVIEW:
                self._agreement = ui.label("").style(f"{MONO} font-size: 10px; color: {_LABEL};")

    def _render_charts(self, jm) -> None:
        with ui.element("div").style("display: flex; gap: 18px; flex-wrap: wrap; min-width: 0;"):
            with ui.element("div").style("flex: 1 1 360px; min-width: 0;"):
                self._chart_title(
                    "out of the tomograms, or flagged, by |stage tilt|",
                    "Per 10° band of |stage tilt|: the tilts the verdict drops, the review flags or Warp's import "
                    "left out, and the dark tilts kept. A tilt counts once, under the first that applies.",
                )
                self._band_chart = ui.echart(_empty_options()).style("height: 210px; width: 100%;")
            if jm.mode in DL_MODES:
                with ui.element("div").style("flex: 1 1 360px; min-width: 0;"):
                    self._chart_title(
                        "P(bad), by your label",
                        "How many tilts the model puts in each P(bad) bin, by your label; the dashed line is the "
                        "threshold. Log counts: the untouched tilts outnumber the labelled ones.",
                    )
                    self._hist_chart = ui.echart(_empty_options()).style("height: 210px; width: 100%;")

    @staticmethod
    def _chart_title(text: str, tip: str) -> None:
        ui.label(text).style(f"{SANS} font-size: 10px; font-weight: 600; color: #334155;").tooltip(tip)

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
        headers, the tiles and the charts in place."""
        state, _jm = self._state_and_job()
        self.gallery.restate(tilt_context(state), ts_ids)
        self._update_numbers()

    async def _on_flag(self, key: str) -> None:
        """The flag flips the tilt's label: bad where the review says good, good where it says
        bad. The tilt counts as labelled from then on."""
        _state, jm = self._state_and_job()
        tilt = self.gallery.tilt(key)
        if jm is None or tilt is None:
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
                ui.notify("No labels to clear.", type="info")
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
        aligned = [g["summary"] for g in self.groups if g["summary"].used is not None]
        yours = sum(1 for s in states if s.review == "human_bad")
        model = sum(1 for s in states if s.review == "model_bad")
        values = {
            "tilts": f"{len(states)} · {len(self.groups)} series",
            "used": str(sum(s.used for s in aligned)) if aligned else "—",
            "flagged": f"{yours + model} · {yours} · {model}",
            "dropped": str(sum(1 for s in states if s.drop is not None)),
            "dark": str(sum(1 for s in states if s.is_dark)),
            "outliers": str(sum(1 for g in self.groups for t in g["tilts"] if t["outliers"])),
        }
        for key, text in values.items():
            self._tiles[key].set_text(text)
        if self._agreement is not None and jm is not None:
            self._agreement.set_text(self._agreement_text(jm, states))
        self._update_charts(jm, states)

    @staticmethod
    def _agreement_text(jm, states) -> str:
        predicted = [s for s in states if s.p_bad is not None]
        if not predicted:
            return ""
        model_bad = {s.frame_id for s in predicted if s.p_bad >= jm.threshold}
        frames = {s.frame_id for s in states}
        yours = {k for k, v in jm.tilt_labels.items() if v == "bad" and k in frames}
        return (
            f"the model flags {len(model_bad)} · your labels say {len(yours)} bad · both {len(model_bad & yours)} · "
            f"only yours {len(yours - model_bad)} · only the model's {len(model_bad - yours)} "
            "(untouched tilts count as good)"
        )

    def _update_charts(self, jm, states) -> None:
        if self._band_chart is not None and not self._band_chart.is_deleted:
            _set_options(self._band_chart, _band_options(states))
        if self._hist_chart is not None and not self._hist_chart.is_deleted and jm is not None:
            _set_options(self._hist_chart, _hist_options(states, jm))


def _empty_options() -> dict:
    return {"animation": False, "xAxis": {"show": False}, "yAxis": {"show": False}, "series": []}


def _set_options(chart, options: dict) -> None:
    chart.options.clear()
    chart.options.update(options)
    chart.update()


def _band_options(states) -> dict:
    """Per |stage tilt| band, each tilt under the first cause that applies (TILT_CAUSES)."""
    if not states:
        return _empty_options()
    n_bands = max(int(abs(s.angle) // _BAND_DEG) for s in states) + 1
    counts = {key: [0] * n_bands for key, _name, _color in TILT_CAUSES}
    for s in states:
        if s.drop is not None:
            cause = "dropped"
        elif s.review == "model_bad":
            cause = "model"
        elif s.review == "human_bad":
            cause = "yours"
        elif s.in_tomogram is False:
            cause = "warp"
        elif s.is_dark:
            cause = "dark"
        else:
            continue
        counts[cause][int(abs(s.angle) // _BAND_DEG)] += 1
    bands = [f"{i * _BAND_DEG}–{(i + 1) * _BAND_DEG}°" for i in range(n_bands)]
    return build_tilt_band_chart(bands, counts)


def _hist_options(states, jm) -> dict:
    """The P(bad) histogram by your label, with the job's threshold."""
    predicted = [s for s in states if s.p_bad is not None]
    if not predicted:
        return _empty_options()
    counts = {key: [0] * _P_BAD_BINS for key, _name, _color in P_BAD_LABELS}
    for s in predicted:
        label = jm.tilt_labels.get(s.frame_id)
        key = "bad" if label == "bad" else "good" if label == "good" else "untouched"
        counts[key][min(_P_BAD_BINS - 1, int(s.p_bad * _P_BAD_BINS))] += 1
    return build_p_bad_histogram(counts, jm.threshold)


class _DlRunCard(FingerprintedView):
    """The DL run, dressed up: the model as chosen on the row, the latest run's number, state,
    SLURM id, when it was submitted and for how long it has run, then what it predicted or why
    it failed; the liveness banner under it when the model gives every tilt the same P(bad)."""

    def __init__(self, page: TiltFilterPage, container: Any) -> None:
        super().__init__(container)
        self.page = page

    def _predictions(self) -> tuple[int, int]:
        _state, jm = self.page._state_and_job()
        p = [s.p_bad for s in self.page._states() if s.p_bad is not None]
        return len(p), sum(1 for v in p if jm is not None and v >= jm.threshold)

    def signature(self):
        _state, jm = self.page._state_and_job()
        if jm is None:
            return None
        run = jm.predict_run
        # While a run is in flight, the elapsed time moves the card every 10 s.
        ticking = run is not None and jm.predict_in_flight
        elapsed = int((datetime.now() - run.submitted_at).total_seconds() // 10) if ticking else None
        key = None if run is None else (run.job_dir, run.model, run.status, run.slurm_job_id, run.error)
        return (jm.model, jm.threshold, key, elapsed, self._predictions(), id(self.page.groups))

    def render(self):
        _state, jm = self.page._state_and_job()
        if jm is None:
            return
        run = jm.predict_run
        model = jm.model or get_config_service().tilt_filter.default_model or "no model"
        with ui.element("div").style(
            "display: flex; flex-direction: column; gap: 3px; padding: 7px 10px; border: 1px solid #e2e8f0; "
            "border-radius: 5px; background: #f8fafc; max-width: 760px;"
        ):
            with ui.element("div").style("display: flex; align-items: center; gap: 8px; flex-wrap: wrap;"):
                ui.label("DL run").style(f"{SANS} font-size: 10px; font-weight: 600; color: #334155;")
                ui.label(model).style(f"{MONO} font-size: 10px; color: {_LABEL};").tooltip(
                    "The model as chosen on the job row; a run keeps the one it started with."
                )
                if run is not None:
                    self._render_run_line(run, jm.predict_in_flight)
            if run is None:
                tail = " The DL-auto job predicts when the pipeline runs." if jm.mode == FilterMode.DL_AUTO else ""
                ui.label(f"No DL run yet: Run DL predicts P(bad) for every tilt.{tail}").style(
                    f"{SANS} font-size: 10px; color: {_MUTED};"
                )
            elif run.status == JobStatus.FAILED:
                ui.label(run.error or f"No reason recorded; see {run.job_dir}/run.err.").style(
                    f"{SANS} font-size: 10px; color: {_RED}; word-break: break-word;"
                )
            elif run.status == JobStatus.SUCCEEDED:
                n, k = self._predictions()
                ui.label(f"{n} predictions · {k} at or above {jm.threshold:.2f}").style(
                    f"{MONO} font-size: 10px; color: {_LABEL};"
                )
        self._render_liveness()

    @staticmethod
    def _render_run_line(run, in_flight: bool) -> None:
        name = Path(run.job_dir).name
        with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
            if in_flight:
                ui.html(_running_spinner_html(12, "#2563eb"), sanitize=False)
            parts = [f"run {name}", run.status.value.lower()]
            if run.slurm_job_id:
                parts.append(f"SLURM {run.slurm_job_id}")
            parts.append(f"submitted {run.submitted_at:%H:%M}")
            if in_flight:
                minutes = int((datetime.now() - run.submitted_at).total_seconds() // 60)
                parts.append(f"{minutes} min")
            if run.model:
                parts.append(run.model)
            ui.label(" · ".join(parts)).style(f"{MONO} font-size: 10px; color: {_LABEL};")

    def _render_liveness(self) -> None:
        by_series = {g["ts"]: [s.p_bad for s, _t in g["ticks"] if s.p_bad is not None] for g in self.page.groups}
        liveness = prediction_liveness({k: v for k, v in by_series.items() if v})
        if liveness is None or liveness[0]:
            return
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 6px; margin-top: 4px; padding: 5px 8px; max-width: 760px; "
            "border: 1px solid #fecaca; border-radius: 5px; background: #fef2f2;"
        ):
            ui.icon("warning", size="14px").style(f"color: {_RED};")
            ui.label(
                f"The model gives every tilt the same P(bad), {liveness[1]:.2f}: its verdicts are meaningless."
            ).style(f"{SANS} font-size: 10px; color: {_RED};")
