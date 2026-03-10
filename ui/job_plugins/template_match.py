"""Template Matching plugin -- species selector."""

from nicegui import ui
from services.project_state import get_project_state, get_state_service
from services.models_base import JobType
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
def render_template_match_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None):
    render_default_params_card(job_type, job_model, is_frozen, save_handler)

    if ui_mgr is None or not ui_mgr.project_path:
        return

    ui.separator().classes("my-4")
    ui.label("Species").classes("text-xs font-semibold text-gray-700 uppercase tracking-wide px-1")

    state = get_project_state()
    species_list = state.species_registry

    if not species_list:
        with ui.row().classes("items-center gap-2 px-1 py-2"):
            ui.icon("info", size="14px").classes("text-gray-400")
            ui.label("No species registered. Open the Template Workbench to create one.").classes(
                "text-xs text-gray-500 italic"
            )
        return

    options = {s.id: s.name for s in species_list}
    current = getattr(job_model, "species_id", None)

    summary_container = ui.column().classes("w-full gap-1 mt-2")

    def _refresh_summary(sid: str):
        summary_container.clear()
        fresh_state = get_project_state()
        sp = fresh_state.get_species(sid)
        if sp is None:
            return
        with summary_container:
            for label, val in [("Template", sp.template_path), ("Mask", sp.mask_path)]:
                with ui.row().classes("items-center gap-2 px-1"):
                    ui.label(f"{label}:").classes("text-[10px] text-gray-500 w-14 flex-shrink-0")
                    ui.label(val or "—").classes(
                        "text-[10px] font-mono truncate" + (" text-orange-400" if not val else " text-gray-700")
                    )

    def _on_species_selected(e):
        sid = e.value
        fresh_state = get_project_state()
        species = fresh_state.get_species(sid)
        if species is None:
            return
        job_model.species_id = sid
        job_model.template_path = species.template_path
        job_model.mask_path = species.mask_path
        # Do NOT set display_label -- let the natural job name convention apply
        job_model.display_label = None
        save_handler()
        _refresh_summary(sid)

    select = (
        ui.select(options=options, value=current, label="Particle species", on_change=_on_species_selected)
        .props("outlined dense")
        .classes("w-full")
    )
    if is_frozen:
        select.disable()

    if current:
        _refresh_summary(current)
