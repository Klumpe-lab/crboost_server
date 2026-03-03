"""Candidate Extraction plugin -- default params + candidate visualization."""

from nicegui import ui

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.TEMPLATE_EXTRACT_PYTOM)
def render_candidate_extract_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    render_default_params_card(job_type, job_model, is_frozen, save_handler)

    if ui_mgr:
        ui.separator().classes("my-4")
        from ui.pipeline_builder.candidate_vis_component import render_candidate_vis_panel

        render_candidate_vis_panel(job_model, ui_mgr)