"""The tilt filter's controls on its job row: the Manual / DL review switch, the review status
and its actions (Run DL, Cancel DL, Approve, Re-open).

A line under the roster row, drawn by its own FingerprintedView on its own 3 s tick. The roster
repaints only on the status poller's tick, which stops once a pipeline winds down, and this line
has to move when a DL run lands or the review is approved in the panel.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nicegui import ui

from services.jobs.tilt_filter import FilterMode
from services.models_base import JobStatus, JobType
from services.project_state import get_project_state_for
from services.tilt_series import get_registry_for
from ui.components.buttons import house_button
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.components.segmented import render_segmented
from ui.styles import SANS as FONT

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel

logger = logging.getLogger(__name__)

_MODES = [(FilterMode.MANUAL.value, "Manual"), (FilterMode.DL_REVIEW.value, "DL review")]
_GREY, _BLUE, _AMBER, _GREEN, _RED = "#94a3b8", "#2563eb", "#b45309", "#15803d", "#dc2626"
# Alignment in any of these has taken, or will take without waiting, the committed verdict.
_ALIGNMENT_STARTED = (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED)


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


def _review_status(state, jm, project_path: Path) -> tuple[str, str, str]:
    """What the review is waiting for, as (chip text, colour, tooltip)."""
    run = jm.predict_run
    if jm.execution_status == JobStatus.SUCCEEDED:
        lc = jm.last_commit
        text = "Approved" if lc is None else f"Approved · {lc.dropped} of {lc.kept + lc.dropped} dropped"
        alignment = _statuses(state, JobType.TS_ALIGNMENT)
        if JobStatus.RUNNING in alignment or JobStatus.SUCCEEDED in alignment:
            tip = "Alignment has taken this verdict; delete the alignment job to change it."
        elif JobStatus.QUEUED in alignment:
            tip = "Alignment is queued and takes this verdict when it starts."
        else:
            tip = "Alignment drops the bad tilts when it runs. Re-open to make the next Run wait for a review."
        return text, _GREEN, tip
    if JobStatus.SUCCEEDED not in _statuses(state, JobType.FS_MOTION_CTF):
        return "Waiting for inputs", _GREY, "The review needs fsMotion's motion-corrected tilts."
    if jm.predict_in_flight:
        name = Path(run.job_dir).name
        return "Predicting…", _BLUE, f"DL run {name} {run.status.value.lower()} · SLURM job {run.slurm_job_id or '…'}"
    if jm.mode == FilterMode.DL_REVIEW and run is not None:
        if run.status == JobStatus.FAILED:
            return "Prediction failed", _RED, run.error or f"No reason recorded; see {run.job_dir}/run.err."
        if run.status == JobStatus.SUCCEEDED:
            n = _flagged(project_path, jm.threshold)
            tip = f"{n} tilts at P(bad) ≥ {jm.threshold:.2f}. Review them in the panel, then Approve."
            return f"Predictions ready · {n} flagged", _AMBER, tip
    return "Waiting for review", _AMBER, "Label the tilts in the panel (click the row), then Approve."


class TiltFilterControls(FingerprintedView):
    """The mode switch, the review status and the actions of one tilt-filter job."""

    def __init__(self, panel: PipelineBuilderPanel, instance_id: str, flight: SingleFlight, container: Any) -> None:
        super().__init__(container)
        self._panel = panel
        self._iid = instance_id
        # Lives on the roster: this view is rebuilt with every roster repaint, a click's flight must not be.
        self._flight = flight

    def _state_and_job(self):
        project_path = self._panel.ui_mgr.project_path
        if project_path is None:
            return None, None
        state = get_project_state_for(project_path)
        return state, state.jobs.get(self._iid)

    def signature(self):
        state, jm = self._state_and_job()
        if jm is None:
            return None
        run = jm.predict_run
        lc = jm.last_commit
        return (
            jm.mode,
            jm.execution_status,
            jm.threshold,
            None if run is None else (run.job_dir, run.status, run.slurm_job_id, run.error),
            None if lc is None else (lc.kept, lc.dropped),
            tuple(_statuses(state, JobType.FS_MOTION_CTF)),
            tuple(_statuses(state, JobType.TS_ALIGNMENT)),
        )

    def render(self):
        state, jm = self._state_and_job()
        if jm is None:
            return
        text, color, tip = _review_status(state, jm, self._panel.ui_mgr.project_path)
        with ui.element("div").style("display: flex; align-items: center; gap: 6px; min-width: 0;"):
            render_segmented(_MODES, jm.mode.value, self._on_mode)
            ui.label(text).style(
                f"{FONT} font-size: 9px; color: {color}; min-width: 0; "
                "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
            ).tooltip(tip)
        actions = self._actions(state, jm)
        if actions:
            with ui.element("div").style("display: flex; align-items: center; gap: 4px; flex-wrap: wrap;"):
                for label, handler, kind, hint in actions:
                    house_button(label, handler, kind=kind, tooltip=hint)

    def _actions(self, state, jm) -> list[tuple[str, Callable, str, str]]:
        inputs_ready = JobStatus.SUCCEEDED in _statuses(state, JobType.FS_MOTION_CTF)
        committed = jm.execution_status == JobStatus.SUCCEEDED
        in_flight = jm.predict_in_flight
        out: list[tuple[str, Callable, str, str]] = []
        if in_flight:
            out.append(("Cancel DL", self._cancel_dl, "default", "Cancel the running DL prediction."))
        elif jm.mode == FilterMode.DL_REVIEW and inputs_ready:
            again = jm.predict_run is not None and jm.predict_run.status == JobStatus.SUCCEEDED
            hint = "Predict P(bad) for every tilt as a SLURM job. Labels you set stay as they are."
            out.append(("Run DL again" if again else "Run DL", self._run_dl, "default", hint))
        if inputs_ready and not committed and not in_flight:
            hint = "Commit the labels as the review shows them; alignment drops the bad tilts."
            out.append(("Approve", self._approve, "accent", hint))
        if committed and not any(s in _ALIGNMENT_STARTED for s in _statuses(state, JobType.TS_ALIGNMENT)):
            out.append(("Re-open", self._reopen, "default", "Make the next Run wait for a review again."))
        return out

    # ── handlers ──

    def _on_mode(self, key: str) -> None:
        _state, jm = self._state_and_job()
        if jm is None or key == jm.mode:
            return
        if jm.predict_in_flight:
            _notify("A DL prediction run is in flight; switch once it has landed or been cancelled.", "warning")
            return
        jm.mode = FilterMode(key)
        # Not a USER_PARAMS field, so nothing marks the project dirty: force the save.
        asyncio.create_task(
            self._panel.backend.save_project(self._panel.ui_mgr.project_path, force=True, debounce_s=1.0)
        )
        self.refresh()
        self._panel.rerender_job(self._iid)

    async def _run_dl(self) -> None:
        async with self._flight(f"tilt-filter-run:{self._iid}") as acquired:
            if not acquired:
                return
            res = await self._panel.backend.submit_tilt_filter_predict(self._panel.ui_mgr.project_path, self._iid)
            if not res["success"]:
                _notify(res["error"], "negative")
            self.refresh()

    async def _cancel_dl(self) -> None:
        async with self._flight(f"tilt-filter-cancel:{self._iid}") as acquired:
            if not acquired:
                return
            res = await self._panel.backend.cancel_tilt_filter_predict(self._panel.ui_mgr.project_path, self._iid)
            if res["success"]:
                _notify("Cancel sent; the run reads failed once SLURM has ended it.", "info")
            else:
                _notify(res["error"], "negative")
            self.refresh()

    async def _approve(self) -> None:
        async with self._flight(f"tilt-filter-approve:{self._iid}") as acquired:
            if not acquired:
                return
            res = await self._panel.backend.approve_tilt_filter(self._panel.ui_mgr.project_path, self._iid)
            if res["success"]:
                _notify(f"Approved: {res['kept']} tilts kept, {res['dropped']} dropped.", "positive")
            else:
                _notify(res["error"], "negative")
            self.refresh()

    async def _reopen(self) -> None:
        async with self._flight(f"tilt-filter-reopen:{self._iid}") as acquired:
            if not acquired:
                return
            res = await self._panel.backend.reopen_tilt_filter(self._panel.ui_mgr.project_path, self._iid)
            if not res["success"]:
                _notify(res["error"], "negative")
            self.refresh()


def render_tilt_filter_controls(panel: PipelineBuilderPanel, instance_id: str, flight: SingleFlight, indent: int):
    """The controls line under the tilt filter's roster row, with its own 3 s observer. The
    timer sits beside the view's container, so a repaint of the view does not delete it."""
    with ui.element("div").style(f"padding: 1px 4px 4px {indent}px; min-width: 0;"):
        body = ui.element("div").style("display: flex; flex-direction: column; gap: 3px; min-width: 0;")
        view = TiltFilterControls(panel, instance_id, flight, body)
        view.refresh()
        ui.timer(3.0, view.refresh)
