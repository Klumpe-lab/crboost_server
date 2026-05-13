"""Candidate Extraction plugin.

IMOD model generation, MIP previews, and 3dmod copy commands live inside
the unified Tomogram Dashboard's Candidate Extract section card
(ui/tomo_dashboard_dialog.py), since they operate across all extract
instances and want to be inspectable without first navigating to a specific
job tab.

The v2 template summary card at the top surfaces the species's particle
metadata (diameter, symmetry) the user is picking for. particle_diameter_ang
on this job remains as a per-job override; new v2 projects should leave
it at the default and edit species.diameter_ang via the workbench.
"""

from nicegui import ui

from services.models_base import JobType
from services.project_state import get_project_state_for
from services.templating.template_metadata import resolve_species_from_job
from ui.components.template_summary_card import render_template_summary_card
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.TEMPLATE_EXTRACT_PYTOM)
def render_candidate_extract_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    instance_id = ctx.get("instance_id")
    species = None
    if ui_mgr and ui_mgr.project_path:
        state = get_project_state_for(ui_mgr.project_path)
        species, _ = resolve_species_from_job(state, job_model, instance_id)

    if species is not None:
        render_template_summary_card(species)
    else:
        with ui.card().classes("w-full border border-dashed border-amber-300 bg-amber-50 mt-1"):
            with ui.row().classes("w-full items-center px-3 py-2 gap-2"):
                ui.icon("warning", size="14px").classes("text-amber-600")
                ui.label("No species linked to this job").classes("text-xs text-amber-800 font-semibold")
            with ui.column().classes("w-full px-3 pb-2 gap-1"):
                ui.label(
                    "Without a species link the candidate-extract summary can't be shown. "
                    "Assign a species or use a `__<species_id>` instance suffix."
                ).classes("text-[11px] text-amber-700")
    render_default_params_card(job_type, job_model, is_frozen, save_handler, ui_mgr=ui_mgr)