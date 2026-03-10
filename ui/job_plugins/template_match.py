"""
Template Matching plugin.

Species is locked at job creation time. template_path and mask_path are
rendered as scoped selectors showing only MRC files produced under
templates/{species_id}/ by the Template Workbench.
"""

from pathlib import Path
from nicegui import ui

from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card, render_species_badge, _render_group_label


@register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
def render_template_match_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None):
    project_path = str(ui_mgr.project_path) if ui_mgr and ui_mgr.project_path else None

    # Species badge -- read-only, locked at creation
    render_species_badge(job_model, project_path)

    # All standard params except the two path fields we handle specially below
    render_default_params_card(
        job_type, job_model, is_frozen, save_handler, exclude={"template_path", "mask_path"}, ui_mgr=ui_mgr
    )

    # Species-scoped template/mask selectors
    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white mt-2"):
        with ui.row().classes("w-full items-center px-3 py-2 bg-gray-50 border-b border-gray-100"):
            ui.icon("folder_open", size="18px").classes("text-gray-500")
            ui.label("Template Files").classes("text-sm font-bold text-gray-800")

        with ui.column().classes("w-full p-4 gap-4"):
            species_id = getattr(job_model, "species_id", None)
            _render_species_path_selector(
                "Template Path",
                "template_path",
                job_model,
                is_frozen,
                save_handler,
                species_id=species_id,
                project_path=project_path,
            )
            _render_species_path_selector(
                "Mask Path",
                "mask_path",
                job_model,
                is_frozen,
                save_handler,
                species_id=species_id,
                project_path=project_path,
            )


def _render_species_path_selector(
    label: str,
    field_name: str,
    job_model,
    is_frozen: bool,
    save_handler,
    species_id: str | None,
    project_path: str | None,
):
    """
    Dropdown populated exclusively with MRC files from templates/{species_id}/.
    Falls back to a read-only text display if the folder is empty or unreachable.
    """
    current = getattr(job_model, field_name, "") or ""

    # Build options: absolute_path -> filename
    options: dict[str, str] = {}
    if species_id and project_path:
        species_dir = Path(project_path) / "templates" / species_id
        if species_dir.exists():
            for f in sorted(species_dir.glob("*.mrc")):
                options[str(f)] = f.name

    with ui.row().classes("w-full items-center gap-3"):
        ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")

        if not options:
            ui.label("No MRC files found — run the Template Workbench first.").classes("text-xs text-orange-400 italic")
            return

        # If current value isn't in the scoped options (e.g. stale path from
        # before species was assigned) treat it as unset.
        resolved_current = current if current in options else None

        sel = (
            ui.select(options=options, value=resolved_current, label=label)
            .props("outlined dense")
            .classes("flex-1 text-xs font-mono")
        )

        if is_frozen:
            sel.disable()
        else:

            def _on_change(e, fn=field_name):
                setattr(job_model, fn, e.value or "")
                save_handler()

            sel.on_value_change(_on_change)
