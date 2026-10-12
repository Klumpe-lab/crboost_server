"""Warp Export plugin: the species line on top, then the plain parameter form.

binning, box_size and diameter_ang are snapshot from the species at creation
(ui/pipeline_builder/pipeline_builder_panel.py); nothing propagates afterwards.
"""

from services.models_base import JobType, resolve_species
from services.project_state import get_project_state_for
from ui.components.species_pill import render_species_line, species_opener
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_config_preamble, render_default_params


@register_params_renderer(JobType.WARP_EXPORT)
def render_warp_export_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    species = None
    if ui_mgr and ui_mgr.project_path:
        state = get_project_state_for(ui_mgr.project_path)
        species, _ = resolve_species(state, job_model, ctx.get("instance_id"))

    render_species_line(species, on_open=species_opener(ctx.get("callbacks"), species.id) if species else None)
    render_config_preamble(job_model)
    # ctx exclude = array_throttle (rendered in the SLURM section, not here).
    render_default_params(job_type, job_model, is_frozen, save_handler, exclude=set(ctx.get("exclude") or ()))
