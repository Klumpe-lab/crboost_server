"""Template Matching plugin -- default params + Template Workbench."""

from nicegui import ui

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
def render_template_match_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None):
    # Standard parameter grid
    render_default_params_card(job_type, job_model, is_frozen, save_handler)

    # Template Workbench (migrated from config_tab hardcoded check)
    if ui_mgr and backend and ui_mgr.project_path:
        ui.separator().classes("my-6")
        ui.label("Template Workbench").classes("text-sm font-bold text-gray-900 mb-3")
        with ui.card().classes("w-full p-0 border border-gray-200 shadow-none bg-white"):
            from ui.template_workbench import TemplateWorkbench

            TemplateWorkbench(backend, str(ui_mgr.project_path))