"""Jobs tab (roadmap 10 S4) — this species' particle-phase pipeline jobs.

Rows = job type · instance id · status dot · drift chips · "open" (the pipeline view on
that instance via `callbacks["open_job"]`). Drift = the species' `symmetry` / Ø versus
the job's snapshot (`symmetry` on TM / Reconstruct / Class3D, `particle_diameter_ang` on
Pick candidates) and TM template / mask ≠ the species' selected ones — DISPLAYED ONLY:
job params are snapshotted at creation and never auto-propagated (D-5). The add row
("Add … for this species") goes through `callbacks["add_instance_for_species"]`
(`PipelineBuilderPanel.add_instance_for_species`, SingleFlight-guarded). One
`FingerprintedView` gated on the registry rev, the attributed instances + statuses, the
drift messages and the running flag; `jobs_for_species` (services/particles) does the
attribution.
"""

from __future__ import annotations

import os
from typing import Any

from nicegui import ui

from services.jobs.spec import display_name
from services.models_base import JobStatus, JobType
from services.particles.species_jobs import PARTICLE_JOB_TYPES, jobs_for_species
from services.project_state import ParticleSpecies, get_project_state_for
from services.templating.template_metadata import get_effective_mask_path, get_effective_template_path
from ui.components.chip import render_chip
from ui.components.reactive import FingerprintedView
from ui.species.tab import TabContext
from ui.status_indicator import _dot_html, _running_spinner_html
from ui.styles import MONO, SANS
from ui.ui_state import get_ui_state_manager

_LABEL_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_HINT_CLS = "text-[10px] text-gray-400"


def _sym(v) -> str:
    return str(getattr(v, "value", v) or "C1")


def _drift(sp: ParticleSpecies, jm) -> list[tuple[str, str]]:
    """(chip label, message) per species-vs-job divergence; empty = in sync."""
    out: list[tuple[str, str]] = []
    species_sym = sp.symmetry or "C1"
    match getattr(jm, "job_type", None):
        case JobType.TEMPLATE_MATCH_PYTOM:
            job_sym = _sym(getattr(jm, "symmetry", None))
            if job_sym != species_sym:
                out.append(("sym", f"job {job_sym} ≠ species {species_sym}"))
            want_t = get_effective_template_path(sp)
            have_t = getattr(jm, "template_path", "") or ""
            if want_t and have_t != want_t:
                out.append(("template", f"job {os.path.basename(have_t) or '—'} ≠ selected {os.path.basename(want_t)}"))
            want_m = get_effective_mask_path(sp)
            have_m = getattr(jm, "mask_path", "") or ""
            if want_m and have_m != want_m:
                out.append(("mask", f"job {os.path.basename(have_m) or '—'} ≠ selected {os.path.basename(want_m)}"))
        case JobType.TEMPLATE_EXTRACT_PYTOM:
            if sp.diameter_ang:
                job_d = float(getattr(jm, "particle_diameter_ang", 0.0) or 0.0)
                if abs(job_d - float(sp.diameter_ang)) > 1e-6:
                    out.append(("Ø", f"job {job_d:g} Å ≠ species {sp.diameter_ang:g} Å"))
        case JobType.RECONSTRUCT_PARTICLE | JobType.CLASS3D:
            job_sym = _sym(getattr(jm, "symmetry", None))
            if job_sym != species_sym:
                out.append(("sym", f"job {job_sym} ≠ species {species_sym}"))
    return out


class _JobsView(FingerprintedView):
    def __init__(self, container: ui.element, tab: JobsTab) -> None:
        super().__init__(container)
        self._tab = tab
        # Captured once (page context); the tick reads it without a storage lookup.
        self._ui_mgr = get_ui_state_manager()

    def _rows(self):
        state = get_project_state_for(self._tab.ctx.project_path)
        sp = state.get_species(self._tab.ctx.species_id)
        return state, sp, (jobs_for_species(state, sp.id) if sp is not None else [])

    def signature(self) -> Any:
        state, sp, rows = self._rows()
        if sp is None:
            return (self._tab.ctx.species_id, None)
        return (
            sp.id,
            state.registry_rev,
            tuple(
                (iid, str(getattr(jm, "execution_status", "")), bool(getattr(jm, "is_orphaned", False)))
                for iid, jm in rows
            ),
            tuple(tuple(_drift(sp, jm)) for _iid, jm in rows),
            self._ui_mgr.is_running,
        )

    def render(self) -> None:
        _state, sp, rows = self._rows()
        if sp is None:
            return
        callbacks = self._tab.ctx.callbacks
        open_job = callbacks.get("open_job")
        with ui.row().classes("w-full items-baseline gap-2 px-1"):
            ui.label("JOBS").classes(_LABEL_CLS)
            ui.label("particle-phase jobs attributed to this species").classes(_HINT_CLS).tooltip(
                "Attribution = instance suffix (`templatematching__<id>`), the job's species_id, or the "
                "single-species fallback (resolve_species)."
            )
        if not rows:
            ui.label("No jobs for this species yet — add one below.").classes(_HINT_CLS + " px-1")
        for iid, jm in rows:
            status = getattr(jm, "execution_status", JobStatus.UNKNOWN)
            dot = (
                _running_spinner_html(14, "#3b82f6")
                if status == JobStatus.RUNNING
                else _dot_html(status, is_orphaned=bool(getattr(jm, "is_orphaned", False)))
            )
            with (
                ui.row()
                .classes("w-full items-center gap-2 px-1")
                .style("min-height: 26px; border-bottom: 1px solid #f1f5f9; flex-wrap: nowrap;")
            ):
                ui.html(dot, sanitize=False, tag="span").style("display: inline-flex; align-items: center;")
                ui.label(display_name(jm.job_type)).style(f"{SANS} font-size: 11px; font-weight: 600; color: #1e293b;")
                ui.label(iid).style(f"{MONO} font-size: 10px; color: #64748b;")
                ui.label(str(getattr(status, "value", status))).style(f"{SANS} font-size: 9px; color: #94a3b8;")
                for label, message in _drift(sp, jm):
                    render_chip(label, "drift", status="warn", tooltip=message + " — jobs keep their creation snapshot")
                ui.space()
                if open_job is not None:
                    ui.label("open ↗").style(
                        f"{SANS} font-size: 10px; color: #6366f1; cursor: pointer; text-decoration: underline dotted;"
                    ).on("click", lambda _e, i=iid: open_job(i)).tooltip("Show this job in the pipeline view")
        if self._ui_mgr.is_running:
            ui.label("Pipeline running — adding jobs is paused.").classes(_HINT_CLS + " px-1 mt-2")
            return
        with ui.row().classes("w-full items-center gap-1 px-1 mt-2 flex-wrap"):
            ui.label("Add for this species:").classes(_HINT_CLS)
            for job_type in PARTICLE_JOB_TYPES:
                ui.button(display_name(job_type), on_click=lambda _e, jt=job_type: self._tab.add(jt)).props(
                    "flat dense no-caps size=sm color=indigo"
                ).style("font-size: 10px; padding: 0 6px;")


class JobsTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self._view: _JobsView | None = None

    def build(self, container: ui.element) -> None:
        with container:
            slot = ui.column().classes("w-full gap-1 p-2")
        self._view = _JobsView(slot, self)
        self._view.refresh()

    def refresh(self) -> None:
        if self._view is not None:
            self._view.refresh()

    async def add(self, job_type: JobType) -> None:
        add = self.ctx.callbacks.get("add_instance_for_species")
        if add is None:
            ui.notify("Pipeline panel not available in this view", type="warning")
            return
        await add(job_type, self.ctx.species_id)
        self.refresh()
