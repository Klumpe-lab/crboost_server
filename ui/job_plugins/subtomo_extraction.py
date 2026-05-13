"""Subtomo Extraction plugin.

The cross-project merge ("Particle Merge") used to live here, but for
aggregation projects it now lives in a standalone workspace card
(ui/aggregation_merge_card.py). For normal pipelines, merging across
extraction outputs is rare enough that the dedicated UI was removed; if
you need it back per-job, re-mount render_merge_panel from
ui/pipeline_builder/merge_panel_component.

Job-specific fields (`box_size`, `crop_size`, `binning`) stay on the
job — these ARE subtomo decisions. But there's a real cross-link worth
surfacing inline: subtomo box (in Å) vs species particle diameter. The
dashboard's `_apply_sanity_rules` already does this post-hoc; this
plugin surfaces the same check at submission time so the user sees it
before they hit Run.
"""

from typing import Optional

from nicegui import ui

from services.models_base import JobType
from services.project_state import get_project_state_for
from services.templating.template_metadata import resolve_species_from_job
from ui.components.template_summary_card import render_template_summary_card
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.SUBTOMO_EXTRACTION)
def render_subtomo_extraction_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    instance_id = ctx.get("instance_id")
    species = None
    state = None
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
                    "Without a species link the box-vs-diameter check can't run. "
                    "Assign a species or use a `__<species_id>` instance suffix."
                ).classes("text-[11px] text-amber-700")

    # Inline box-vs-diameter warning. Mirrors the dashboard's sanity rule
    # so a user editing box_size sees the same advice before submission.
    warning_container = ui.column().classes("w-full")
    _render_box_vs_diameter_warning(warning_container, state, species, job_model)

    # Wrap save_handler so the warning re-renders when box_size / binning changes.
    def _on_save():
        save_handler()
        _render_box_vs_diameter_warning(warning_container, state, species, job_model)

    render_default_params_card(job_type, job_model, is_frozen, _on_save, ui_mgr=ui_mgr)


def _render_box_vs_diameter_warning(container, state, species, job_model) -> None:
    container.clear()
    if species is None or state is None:
        return
    diameter = getattr(species, "diameter_ang", None)
    if not diameter or diameter <= 0:
        return

    bx = int(getattr(job_model, "box_size", 0) or 0)
    if bx <= 0:
        return

    binning = float(getattr(job_model, "binning", 1.0) or 1.0)
    native_px = _native_pixel_size(state)
    if not native_px or native_px <= 0:
        # Unknown native px — can't compute box in Å; bail rather than
        # show a misleading warning.
        return
    eff_px = native_px * binning
    box_ang = bx * eff_px
    ratio = box_ang / float(diameter)

    level: Optional[str] = None
    msg = ""
    if ratio < 1.5:
        level = "error"
        msg = (
            f"Box {box_ang:g} Å is {ratio:.2f}× particle diameter {diameter:g} Å — "
            "particle won't fit. Aim for 1.5–3×."
        )
    elif ratio > 3.0:
        level = "warn"
        msg = (
            f"Box {box_ang:g} Å is {ratio:.2f}× particle diameter {diameter:g} Å — "
            "wasted compute. Aim for 1.5–3×."
        )

    if level is None:
        return

    with container:
        bg = "bg-red-50 border-red-200" if level == "error" else "bg-amber-50 border-amber-200"
        icon_color = "text-red-600" if level == "error" else "text-amber-600"
        text_color = "text-red-800" if level == "error" else "text-amber-800"
        with ui.row().classes(f"w-full {bg} border rounded px-3 py-2 gap-2 items-start mt-1"):
            ui.icon("error" if level == "error" else "warning", size="16px").classes(f"{icon_color} mt-0.5")
            with ui.column().classes("gap-0.5"):
                ui.label("Box vs particle diameter").classes(f"text-xs font-semibold {text_color}")
                ui.label(msg).classes(f"text-[11px] {text_color}")
                ui.label(
                    f"box_size={bx} px • binning={binning:g} • effective apix={eff_px:.3g} Å/px"
                ).classes("text-[11px] text-gray-500 font-mono")


def _native_pixel_size(state) -> Optional[float]:
    mic = getattr(state, "microscope", None)
    if mic is None:
        return None
    px = getattr(mic, "pixel_size_angstrom", None)
    try:
        return float(px) if px else None
    except (TypeError, ValueError):
        return None
