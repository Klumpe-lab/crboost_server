"""Subtomo Extraction plugin -- default params only.

The cross-project merge ("Particle Merge") used to live here, but for
aggregation projects it now lives in a standalone workspace card
(ui/aggregation_merge_card.py). For normal pipelines, merging across
extraction outputs is rare enough that the dedicated UI was removed; if
you need it back per-job, re-mount render_merge_panel from
ui/pipeline_builder/merge_panel_component.
"""

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.SUBTOMO_EXTRACTION)
def render_subtomo_extraction_params(job_type, job_model, is_frozen, save_handler, **ctx):
    render_default_params_card(job_type, job_model, is_frozen, save_handler)