"""Results tab of the Class 3D and Reconstruct particle jobs: the job's readout
(ui/analytics/results_section.py), the same panel the Species page's Analytics tab shows."""

from __future__ import annotations

from pathlib import Path

from nicegui import ui

from services.models_base import JobType, resolve_species
from services.project_state import get_project_state_for
from ui.analytics.results_section import ResultsSection
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
