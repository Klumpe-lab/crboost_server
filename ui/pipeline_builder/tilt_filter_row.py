"""The tilt filter on its job row and at the top of its job page.

- `TiltFilterStatus`: where the filter is, in a few words, coloured, with the reason as its
  tooltip. The collapsed roster row shows it where an array job shows its progress.
- `TiltFilterControls`: the row's dropdown, settings only: the method (Manual or DL), for DL
  whether to stop for review or apply the model automatically, the model, and the threshold
  for applying automatically.
- `TiltFilterReview`: the state and every action (Run DL, Cancel DL, Approve, Re-open), on the
  job page.

Each is its own FingerprintedView on its own 3 s tick. The roster repaints only on the status
poller's tick, which stops once a pipeline winds down, and these have to move when a DL run
lands or the review is approved.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nicegui import ui

from services.configs.config_service import get_config_service
from services.jobs.tilt_filter import DL_MODES, FilterMode, calibrated_threshold, reopen_lock, resolve_model
from services.models_base import JobStatus, JobType
from services.project_state import get_project_state_for
from services.tilt_series import get_registry_for
from ui.components.buttons import house_button
from ui.components.fields import HOUSE_LABEL_CLS, house_number, house_select
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.components.segmented import render_segmented
from ui.styles import MONO, SANS as FONT

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel

logger = logging.getLogger(__name__)

_GREY, _BLUE, _AMBER, _GREEN, _RED = "#94a3b8", "#2563eb", "#b45309", "#15803d", "#dc2626"
_METHODS = [("manual", "Manual"), ("dl", "DL")]
_WHEN_DL = [(FilterMode.DL_REVIEW.value, "Stop for review"), (FilterMode.DL_AUTO.value, "Apply automatically")]


def _notify(message: str, kind: str) -> None:
    try:
        ui.notify(message, type=kind)
    except RuntimeError:  # the tab closed while the backend call was awaited; nobody to tell
        logger.info("notify dropped (client gone): %s", message)


def _statuses(state, job_type: JobType) -> list[JobStatus]:
    return [jm.execution_status for jm in state.jobs.values() if jm.job_type == job_type]


def _flagged(project_path: Path, threshold: float) -> int:
    """Tilts the latest predictions put at or above the threshold."""
    registry = get_registry_for(Path(project_path))
    return sum(
        1 for ts in registry.all_tilt_series() for f in ts.frames if f.p_bad is not None and f.p_bad >= threshold
    )


def _parked(state, instance_id: str):
    """The run's hold when it is this filter's, else None."""
    hold = state.review_hold
    return hold if hold is not None and hold.barrier == instance_id else None


def _waiting_tip(jm, hold) -> str:
    n, since = len(hold.parked), hold.held_at.strftime("%H:%M")
    if jm.execution_status == JobStatus.SUCCEEDED:
        # Approve's resume was refused (its reason was shown then); the next Run submits them.
        return f"{n} job(s) parked at {since} were not submitted. Run the pipeline to start them."
    return f"{n} job(s), alignment onward, have waited since {since} and start on Approve."


def _committed_status(state, jm) -> tuple[str, str, str]:
    lc = jm.last_commit
    auto = lc.mode == FilterMode.DL_AUTO if lc is not None else jm.mode == FilterMode.DL_AUTO
    text = "committed" if lc is None else f"{lc.dropped}/{lc.kept + lc.dropped} dropped"
    who = "The DL-auto job committed this verdict" if auto else "Approved"
    if lc is not None and lc.threshold is not None:
        who += f" at P(bad) ≥ {lc.threshold:.2f}"
    if auto and lc is None:
        who += "; its counts were not recorded"
    alignment = _statuses(state, JobType.TS_ALIGNMENT)
    if JobStatus.RUNNING in alignment or JobStatus.SUCCEEDED in alignment:
        then = "Alignment has taken it; delete the alignment job to change it."
    elif JobStatus.QUEUED in alignment:
        then = "Alignment is queued and takes it when it starts."
    elif jm.mode == FilterMode.DL_AUTO:
        then = "Alignment drops the bad tilts when it runs. Re-open on the job page to run the filter's job again."
    else:
        then = "Alignment drops the bad tilts when it runs. Re-open on the job page to review again."
    return text, _GREEN, f"{who}. {then}"


def _auto_status(jm) -> tuple[str, str, str]:
    """The DL-auto job before it has committed."""
    cut = f"P(bad) ≥ {jm.threshold:.2f}"
    slurm = f"SLURM job {jm.slurm_job_id or '…'}"
    if jm.execution_status == JobStatus.QUEUED:
        return "auto · queued", _BLUE, f"{slurm} waits for fsMotion, then drops the tilts at {cut}."
    if jm.execution_status == JobStatus.RUNNING:
        return "auto · running", _BLUE, f"{slurm} predicts every tilt and drops those at {cut}; alignment follows."
    if jm.execution_status == JobStatus.FAILED:
        return "auto · failed", _RED, jm.auto_error or "No reason recorded; see the job's log."
    tip = f"On Run, a DL job after fsMotion drops the tilts at {cut} (your labels win), and alignment follows it."
    return f"DL auto {jm.threshold:.2f}", _GREY, tip


def _review_status(state, jm, instance_id: str, project_path: Path) -> tuple[str, str, str]:
    """Where the filter is, as (text, colour, tooltip)."""
    if jm.execution_status == JobStatus.SUCCEEDED:
        return _committed_status(state, jm)
    if jm.mode == FilterMode.DL_AUTO:
        return _auto_status(jm)
    fs_motion = _statuses(state, JobType.FS_MOTION_CTF)
    if JobStatus.SUCCEEDED not in fs_motion:
        started = any(s in (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.FAILED) for s in fs_motion)
        if started or _parked(state, instance_id) is not None:
            tip = "The review needs fsMotion's motion-corrected tilts"
            tip += "; fsMotion failed, so run it again." if JobStatus.FAILED in fs_motion else "."
            return "waiting for fsMotion", _GREY, tip
        name = "manual" if jm.mode == FilterMode.MANUAL else "DL review"
        return name, _GREY, "Nothing to review yet: a Run stops at this filter until you approve its labels."
    run = jm.predict_run
    if jm.predict_in_flight:
        name = Path(run.job_dir).name
        return "predicting…", _BLUE, f"DL run {name} {run.status.value.lower()} · SLURM job {run.slurm_job_id or '…'}"
    if jm.mode == FilterMode.DL_REVIEW and run is not None:
        if run.status == JobStatus.FAILED:
            return "DL failed", _RED, run.error or f"No reason recorded; see {run.job_dir}/run.err."
        if run.status == JobStatus.SUCCEEDED:
            n = _flagged(project_path, jm.threshold)
            return f"{n} flagged", _AMBER, f"{n} tilts at P(bad) ≥ {jm.threshold:.2f}. Review them, then Approve."
    return "review", _AMBER, "Label the tilts on the job page, then Approve."


def filter_status(state, jm, instance_id: str, project_path: Path) -> tuple[str, str, str]:
    """Where the filter is, as (text, colour, tooltip), with the jobs a run parked behind it:
    the collapsed row's words, the dropdown's and the job page's state line."""
    text, color, tip = _review_status(state, jm, instance_id, project_path)
    hold = _parked(state, instance_id)
    if hold is not None:
        text = f"{text} · {len(hold.parked)} parked"
        tip = f"{tip} {_waiting_tip(jm, hold)}"
    return text, color, tip


def status_signature(state, jm, instance_id: str) -> tuple:
    """Every input filter_status reads."""
    run = jm.predict_run
    lc = jm.last_commit
    hold = _parked(state, instance_id)
    return (
        jm.mode,
        jm.execution_status,
        jm.threshold,
        jm.slurm_job_id,
        jm.auto_error,
        None if run is None else (run.job_dir, run.status, run.slurm_job_id, run.error),
        None if lc is None else (lc.mode, lc.kept, lc.dropped, lc.threshold),
        tuple(_statuses(state, JobType.FS_MOTION_CTF)),
        tuple(_statuses(state, JobType.TS_ALIGNMENT)),
        None if hold is None else (tuple(hold.parked), hold.held_at),
    )


def notify_resume(res: dict) -> None:
    """Report what Approve did with the jobs parked behind the filter, if it had any."""
    resume = res.get("resume")
    if resume is None:
        return
    if not resume["success"]:
        _notify(f"The parked jobs were not submitted: {resume['error']}", "negative")
    elif resume.get("submitted"):
        _notify(f"{len(resume['submitted'])} parked job(s) submitted.", "positive")


def _state_and_job(project_path: Path | None, instance_id: str):
    if project_path is None:
        return None, None
    state = get_project_state_for(project_path)
    return state, state.jobs.get(instance_id)


# ── the state, in a few words ──


class TiltFilterStatus(FingerprintedView):
    """Where the filter is (filter_status), coloured, the reason as its tooltip."""

    def __init__(self, project_path: Path, instance_id: str, container: Any) -> None:
        super().__init__(container)
        self._project_path = project_path
        self._iid = instance_id

    def signature(self):
        state, jm = _state_and_job(self._project_path, self._iid)
        return None if jm is None else status_signature(state, jm, self._iid)

    def render(self):
        state, jm = _state_and_job(self._project_path, self._iid)
        if jm is None:
            return
        text, color, tip = filter_status(state, jm, self._iid, self._project_path)
        ui.label(text).style(
            f"{MONO} font-size: 9px; font-weight: 600; color: {color}; min-width: 0; "
            "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
        ).tooltip(tip)


def render_tilt_filter_status(project_path: Path, instance_id: str) -> TiltFilterStatus:
    """The status words in the current slot, with their own 3 s observer beside them, so a
    repaint of the words does not delete the timer."""
    body = ui.element("div").style("display: flex; align-items: center; min-width: 0; flex-shrink: 1;")
    view = TiltFilterStatus(project_path, instance_id, body)
    view.refresh()
    ui.timer(3.0, view.refresh)
    return view


# ── the row's dropdown: settings only ──


def _setting(name: str, tip: str):
    """One line of the dropdown: the label column, then whatever the caller builds into it."""
    row = ui.element("div").style("display: flex; align-items: center; gap: 6px; min-width: 0;")
    with row:
        ui.label(name).classes(HOUSE_LABEL_CLS).style("width: 58px; flex-shrink: 0;").tooltip(tip)
    return row


class TiltFilterControls(FingerprintedView):
    """The filter's settings: Manual or DL; for DL, stop for review or apply automatically, and
    the model; the threshold when applying automatically. Then the state and Review →. Every
    action lives on the job page."""

    def __init__(self, panel: PipelineBuilderPanel, instance_id: str, flight: SingleFlight, container: Any) -> None:
        super().__init__(container)
        self._panel = panel
        self._iid = instance_id
        # Lives on the roster: this view is rebuilt with every roster repaint, a click's flight must not be.
        self._flight = flight

    def signature(self):
        _state, jm = _state_and_job(self._panel.ui_mgr.project_path, self._iid)
        if jm is None:
            return None
        # Not the threshold: its box is bound to it, and a repaint would take the box from under the cursor.
        return (jm.mode, jm.model, jm.auto_in_flight)

    def render(self):
        project_path = self._panel.ui_mgr.project_path
        _state, jm = _state_and_job(project_path, self._iid)
        if jm is None:
            return
        dl = jm.mode in DL_MODES
        frozen = jm.auto_in_flight
        with _setting("method", "Hand labels only, or a DL model's predictions."):
            render_segmented(_METHODS, "dl" if dl else "manual", self._on_method, classes="cb-seg-sm")
        if dl:
            with _setting(
                "when DL",
                "Stop for review: a Run waits at this filter until you approve the labels (predictions pre-filled). "
                "Apply automatically: a chain job predicts after fsMotion and commits; alignment follows.",
            ):
                render_segmented(_WHEN_DL, jm.mode.value, self._on_when, classes="cb-seg-sm")
            self._render_model(jm, frozen)
            if jm.mode == FilterMode.DL_AUTO:
                self._render_threshold(jm, frozen)
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; min-width: 0; margin-top: 1px;"):
            render_tilt_filter_status(project_path, self._iid)
            house_button("Review →", lambda: self._panel.switch_tab(self._iid), tooltip="Open the job page.")

    def _render_model(self, jm, frozen: bool) -> None:
        registry = get_config_service().tilt_filter
        chosen = jm.model or registry.default_model
        options = list(registry.models)
        if chosen and chosen not in options:
            options.append(chosen)  # gone from conf.yaml: still shown, and the marker says so
        with _setting("model", "The tilt classifier: one of conf.yaml's tilt_filter.models."):
            sel = house_select("", options, value=chosen, width="w-40", on_change=self._on_model)
            if frozen:
                sel.disable()
                sel.tooltip("Fixed while the DL-auto job runs.")
            try:
                resolve_model(chosen)
            except ValueError as e:
                ui.icon("error", size="13px").style(f"color: {_RED}; flex-shrink: 0;").tooltip(str(e))

    def _render_threshold(self, jm, frozen: bool) -> None:
        cut = calibrated_threshold(jm.model)
        with _setting("threshold", "A tilt with P(bad) at or above this is dropped, unless you labelled it good."):
            box = house_number(
                "", model=jm, attr="threshold", min=0, max=1, step=0.05, width="w-16", on_change=self._on_threshold
            )
            if frozen:
                box.disable()
                box.tooltip("Fixed while the DL-auto job runs.")
            if cut is None:
                ui.label("uncalibrated").style(f"{FONT} font-size: 9px; color: {_AMBER};").tooltip(
                    "conf.yaml records no P(bad) cut for this model, so this threshold has not been measured."
                )
            else:
                ui.label(f"cut {cut:.2f}").style(f"{FONT} font-size: 9px; color: {_GREY};").tooltip(
                    f"conf.yaml records {cut:.2f} as the cut for {jm.model or 'the default model'}."
                )

    # ── handlers ──

    def _on_method(self, key: str):
        # Manual → DL stops for review first.
        return self._set_mode(FilterMode.MANUAL if key == "manual" else FilterMode.DL_REVIEW)

    def _on_when(self, key: str):
        return self._set_mode(FilterMode(key))

    async def _set_mode(self, mode: FilterMode) -> None:
        async with self._flight(f"tilt-filter-mode:{self._iid}") as acquired:
            if not acquired:
                return
            res = await self._panel.backend.set_tilt_filter_mode(self._panel.ui_mgr.project_path, self._iid, mode)
            if res["success"]:
                notify_resume(res)
                self._panel.rerender_job(self._iid)
            else:
                _notify(res["error"], "warning")
            self.refresh()

    async def _on_model(self, e) -> None:
        _state, jm = _state_and_job(self._panel.ui_mgr.project_path, self._iid)
        if jm is None or e.value == (jm.model or get_config_service().tilt_filter.default_model):
            return
        jm.model = e.value
        # Not a USER_PARAMS field, so nothing marks the project dirty: force the save.
        await self._panel.backend.save_project(self._panel.ui_mgr.project_path, force=True, debounce_s=1.0)
        self.refresh()

    async def _on_threshold(self, _e) -> None:
        # The binding has written the value, or refused a half-typed one and kept the last good one.
        await self._panel.backend.save_project(self._panel.ui_mgr.project_path, force=True, debounce_s=1.0)


def render_tilt_filter_controls(panel: PipelineBuilderPanel, instance_id: str, flight: SingleFlight, indent: int):
    """The dropdown under the tilt filter's roster row, with its own 3 s observer. The timer
    sits beside the view's container, so a repaint of the view does not delete it."""
    with ui.element("div").style(f"padding: 2px 4px 5px {indent}px; min-width: 0;"):
        body = ui.element("div").style("display: flex; flex-direction: column; gap: 3px; min-width: 0;")
        view = TiltFilterControls(panel, instance_id, flight, body)
        view.refresh()
        ui.timer(3.0, view.refresh)


# ── the job page: the state and every action ──


class TiltFilterReview(FingerprintedView):
    """The review's state line and its actions, for the job page: Run DL / Run DL again /
    Cancel DL (DL modes), Approve (not in DL auto, whose job commits), Re-open."""

    def __init__(self, backend, project_path: Path, instance_id: str, container: Any) -> None:
        super().__init__(container)
        self._backend = backend
        self._project_path = project_path
        self._iid = instance_id
        self._flight = SingleFlight()

    def signature(self):
        state, jm = _state_and_job(self._project_path, self._iid)
        return None if jm is None else status_signature(state, jm, self._iid)

    def render(self):
        state, jm = _state_and_job(self._project_path, self._iid)
        if jm is None:
            return
        text, color, tip = filter_status(state, jm, self._iid, self._project_path)
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; flex-wrap: wrap; min-width: 0;"):
            ui.label(text).style(
                f"{MONO} font-size: 11px; font-weight: 600; color: {color}; white-space: nowrap;"
            ).tooltip(tip)
            for label, handler, kind, hint, enabled in self._actions(state, jm):
                if enabled:
                    house_button(label, handler, kind=kind, tooltip=hint)
                else:
                    # A disabled button takes no pointer events, so its reason sits on a wrapper.
                    with ui.element("span").tooltip(hint):
                        house_button(label, handler, kind=kind).disable()

    def _actions(self, state, jm) -> list[tuple[str, Callable, str, str, bool]]:
        """(label, handler, kind, tooltip, enabled) per action the filter's state allows."""
        inputs_ready = JobStatus.SUCCEEDED in _statuses(state, JobType.FS_MOTION_CTF)
        committed = jm.execution_status == JobStatus.SUCCEEDED
        in_flight = jm.predict_in_flight
        auto = jm.mode == FilterMode.DL_AUTO
        out: list[tuple[str, Callable, str, str, bool]] = []
        if in_flight:
            out.append(("Cancel DL", self._cancel_dl, "default", "Cancel the running DL prediction.", True))
        elif jm.mode in DL_MODES and inputs_ready and not jm.auto_in_flight:
            again = jm.predict_run is not None and jm.predict_run.status == JobStatus.SUCCEEDED
            hint = "Predict P(bad) for every tilt as a SLURM job. Labels you set stay as they are."
            out.append(("Run DL again" if again else "Run DL", self._run_dl, "default", hint, True))
        if inputs_ready and not committed and not in_flight and not auto:
            hint = "Commit the labels as the review shows them; alignment drops the bad tilts."
            hold = _parked(state, self._iid)
            if hold is not None:
                hint += f" The {len(hold.parked)} parked job(s) start."
            out.append(("Approve", self._approve, "accent", hint, True))
        if committed:
            lock = reopen_lock(state)
            hint = "Run the filter's job again on the next Run." if auto else "Make the next Run wait for a review."
            out.append(("Re-open", self._reopen, "default", lock or hint, lock is None))
        return out

    async def _run_dl(self) -> None:
        async with self._flight("run") as acquired:
            if not acquired:
                return
            res = await self._backend.submit_tilt_filter_predict(self._project_path, self._iid)
            if not res["success"]:
                _notify(res["error"], "negative")
            self.refresh()

    async def _cancel_dl(self) -> None:
        async with self._flight("cancel") as acquired:
            if not acquired:
                return
            res = await self._backend.cancel_tilt_filter_predict(self._project_path, self._iid)
            if res["success"]:
                _notify("Cancel sent; the run reads failed once SLURM has ended it.", "info")
            else:
                _notify(res["error"], "negative")
            self.refresh()

    async def _approve(self) -> None:
        async with self._flight("approve") as acquired:
            if not acquired:
                return
            res = await self._backend.approve_tilt_filter(self._project_path, self._iid)
            if res["success"]:
                _notify(f"Approved: {res['kept']} tilts kept, {res['dropped']} dropped.", "positive")
                notify_resume(res)
            else:
                _notify(res["error"], "negative")
            self.refresh()

    async def _reopen(self) -> None:
        async with self._flight("reopen") as acquired:
            if not acquired:
                return
            res = await self._backend.reopen_tilt_filter(self._project_path, self._iid)
            if not res["success"]:
                _notify(res["error"], "negative")
            self.refresh()


def render_tilt_filter_review(backend, project_path: Path, instance_id: str) -> TiltFilterReview:
    """The review block in the current slot, with its own 3 s observer beside it."""
    body = ui.element("div").style("min-width: 0;")
    view = TiltFilterReview(backend, project_path, instance_id, body)
    view.refresh()
    ui.timer(3.0, view.refresh)
    return view
