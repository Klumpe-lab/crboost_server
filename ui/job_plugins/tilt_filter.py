from ui.job_plugins import register_full_panel_renderer
from services.models_base import JobType
from ui.tilt_filter_panel import render_tilt_filter_job_panel


@register_full_panel_renderer(JobType.TILT_FILTER)
def _render_tilt_filter(job_type, instance_id, job_model, backend, ui_mgr, save_handler):
    render_tilt_filter_job_panel(job_type, instance_id, job_model, backend, ui_mgr, save_handler)
