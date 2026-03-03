"""Subtomo Extraction plugin -- default params + Merge panel."""

from nicegui import ui

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.SUBTOMO_EXTRACTION)
def render_subtomo_extraction_params(job_type, job_model, is_frozen, save_handler, **ctx):
    render_default_params_card(job_type, job_model, is_frozen, save_handler)

    ui.separator().classes("my-4")
    from ui.pipeline_builder.merge_panel_component import render_merge_panel

    render_merge_panel(job_model, is_frozen, save_handler)