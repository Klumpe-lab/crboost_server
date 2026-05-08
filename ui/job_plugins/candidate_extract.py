"""Candidate Extraction plugin -- default params only.

IMOD model generation, MIP previews, and 3dmod copy commands now live inside
the unified Tomogram Dashboard's Candidate Extract section card
(ui/tomo_dashboard_dialog.py), since they operate across all extract
instances and want to be inspectable without first navigating to a specific
job tab.
"""

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.TEMPLATE_EXTRACT_PYTOM)
def render_candidate_extract_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    render_default_params_card(job_type, job_model, is_frozen, save_handler)