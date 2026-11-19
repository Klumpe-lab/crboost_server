"""Results tabs of the particle jobs. Class 3D and Reconstruct particle: the job's readout
(ui/analytics/results_section.py). Refine 3D and
PostProcess: the resolution RELION reports, said with what it measures, and the FSC curves
(services/particles/fsc_readout.py, read in a thread)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from nicegui import ui

from services.models_base import JobType, resolve_species
from services.project_state import get_project_state_for
from services.particles.fsc_readout import crossing, postprocess_fsc, refine3d_fsc
from services.result import ErrorCode
from ui.analytics.common import ERROR_CLS, HINT_CLS
from ui.analytics.results_section import ResultsSection
from ui.components.plot import SERIES, fsc_plot, plot
from ui.components.reactive import owned_timer
from ui.job_plugins._field_styles import HELPER_STYLE, LABEL_VOICE, PANEL_TITLE_STYLE
from ui.styles import MONO, SANS
from ui.dashboard.css import ensure_assets_loaded
from ui.job_plugins import register_extra_tab


def _render(instance_id: str, job_model, backend, ui_mgr) -> None:
    ensure_assets_loaded()
    project_path = ui_mgr.project_path
    if not project_path:
        ui.label("No project loaded").classes("text-red-500 p-4")
        return
    state = get_project_state_for(Path(project_path))
    _species, species_id = resolve_species(state, job_model, instance_id)
    with ui.column().classes("w-full gap-2 p-2"):
        ResultsSection(backend, Path(project_path), species_id or "", [(instance_id, instance_id)]).build()


@register_extra_tab(JobType.CLASS3D, key="results", label="Results", icon="insights")
def render_class3d_results(job_type, instance_id, job_model, backend, ui_mgr) -> None:
    _render(instance_id, job_model, backend, ui_mgr)


@register_extra_tab(JobType.RECONSTRUCT_PARTICLE, key="results", label="Results", icon="insights")
def render_reconstruct_results(job_type, instance_id, job_model, backend, ui_mgr) -> None:
    _render(instance_id, job_model, backend, ui_mgr)


# ── Refine 3D and PostProcess: the resolution and the FSC ──

_PLOT_WH = (560, 200)
_HEADLINE = (
    f"{MONO} font-size: 18px; font-weight: 600; color: var(--cb-ink-1, #12324f); font-variant-numeric: tabular-nums;"
)
_BODY = f"{SANS} font-size: 11px; color: var(--cb-ink-2, #1e293b);"
_WARN = f"{SANS} font-size: 11px; color: var(--cb-warn-ink, #8a4b06);"


def _fsc_tab(instance_id: str, job_model, ui_mgr, reader) -> None:
    ensure_assets_loaded()
    project_path = ui_mgr.project_path
    rel = getattr(job_model, "relion_job_name", None)
    if not project_path or not rel:
        ui.label("The job has not run yet: no results.").classes(HINT_CLS)
        return
    job_dir = Path(project_path) / rel.rstrip("/")
    box = ui.column().classes("w-full p-2").style("gap: 10px;")
    with box:
        ui.label("reading the job's results…").classes(HINT_CLS)

    async def load() -> None:
        res = await asyncio.to_thread(reader, job_dir)
        if box.is_deleted:
            return
        box.clear()
        with box:
            _fsc_body(res)

    owned_timer(0.05, load, box, once=True)


def _fsc_body(res: dict) -> None:
    if not res["success"]:
        expected = res.get("code") == ErrorCode.NOT_WRITTEN_YET
        ui.label(res["error"]).classes(HINT_CLS if expected else ERROR_CLS)
        return
    curves = res["curves"]
    main = next(iter(curves), None)
    never = main is not None and crossing(curves[main]["freq"], curves[main]["fsc"], 0.143) is None
    with ui.element("div").style("display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap;"):
        ui.label(f"{res['resolution']:.2f} Å" if res["resolution"] else "no resolution").style(_HEADLINE)
        ui.label(res["what"]).style(_BODY)
        where = "converged" if res["converged"] else f"so far, from {res['source']}"
        if res["kind"] == "refine3d":
            ui.label(where).style(HELPER_STYLE)
        if res.get("bfactor") is not None:
            ui.label(f"B {res['bfactor']:g} Å²").style(f"{MONO} font-size: 11px; color: var(--cb-ink-3, #5b7089);")
    if res.get("box") and res.get("apix"):
        ui.label(f"{res['box']} px box at {res['apix']:g} Å/px · Nyquist {res['nyquist']:.1f} Å").style(
            f"{MONO} font-size: 10px; color: var(--cb-ink-3, #5b7089); font-variant-numeric: tabular-nums;"
        )
    if never:
        ui.label(
            "The FSC never drops below 0.143: the number above is the sampling limit of these images, not a "
            "resolution. Refine at a finer pixel size to measure one."
        ).style(_WARN)
    if res.get("failure"):
        ui.label(res["failure"]).classes(ERROR_CLS)
    if not curves:
        ui.label("No FSC table in the job's star file.").classes(HINT_CLS)
        return
    with ui.element("div").style("display: flex; flex-direction: column; gap: 4px; width: 100%; max-width: 560px;"):
        ui.label("Fourier shell correlation").style(PANEL_TITLE_STYLE)
        with ui.element("div").style("display: flex; gap: 12px; flex-wrap: wrap;"):
            for i, name in enumerate(curves):
                with ui.element("div").style("display: flex; align-items: center; gap: 5px;"):
                    ui.element("span").style(
                        f"width: 12px; height: 2px; background: {SERIES[i % len(SERIES)]}; display: inline-block;"
                    )
                    ui.label(name).style(f"{SANS} font-size: 10px; color: var(--cb-ink-2, #1e293b);")
        plot(fsc_plot(curves, *_PLOT_WH, mark=main), *_PLOT_WH)
        ui.label("resolution and FSC under the pointer").style(f"{LABEL_VOICE} margin-top: 2px;")


@register_extra_tab(JobType.REFINE3D, key="results", label="Results", icon="insights")
def render_refine3d_results(job_type, instance_id, job_model, backend, ui_mgr) -> None:
    _fsc_tab(instance_id, job_model, ui_mgr, refine3d_fsc)


@register_extra_tab(JobType.POSTPROCESS, key="results", label="Results", icon="insights")
def render_postprocess_results(job_type, instance_id, job_model, backend, ui_mgr) -> None:
    _fsc_tab(instance_id, job_model, ui_mgr, postprocess_fsc)
