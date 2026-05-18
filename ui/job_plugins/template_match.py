"""
Template Matching plugin (v3).

Species is locked at job creation time. The species's templates and masks
are surfaced as two independent dropdowns so the user picks both
explicitly — defaulting to whichever the species has selected. Search
symmetry is also rendered here (locked to C1–C6 since PyTOM only honors
Cn; persisted non-Cn values are coerced to C1 by the param-class
validator). The default-renderer skips template_path / mask_path /
symmetry because we render all three in the dedicated card below.

If species.templates is empty the dropdown shows an empty-state hint
pointing the user to the workbench.
"""

from pathlib import Path

from nicegui import ui

from services.jobs.template_match import TM_SYMMETRY_CHOICES
from services.models_base import JobType
from services.project_state import get_project_state_for
from services.templating.template_metadata import read_template_header, resolve_species_from_job
from ui.components.template_summary_card import render_template_summary_card
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card, render_species_badge


@register_params_renderer(JobType.TEMPLATE_MATCH_PYTOM)
def render_template_match_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **_ctx):
    project_path = str(ui_mgr.project_path) if ui_mgr and ui_mgr.project_path else None
    instance_id = _ctx.get("instance_id")

    render_species_badge(job_model, project_path)

    species = None
    if ui_mgr and ui_mgr.project_path:
        state = get_project_state_for(ui_mgr.project_path)
        species, _resolved_sid = resolve_species_from_job(state, job_model, instance_id)

    if species is not None:
        render_template_summary_card(species)
    else:
        with ui.card().classes("w-full border border-dashed border-amber-300 bg-amber-50 mt-1"):
            with ui.row().classes("w-full items-center px-3 py-2 gap-2"):
                ui.icon("warning", size="14px").classes("text-amber-600")
                ui.label("No species linked to this job").classes("text-xs text-amber-800 font-semibold")
            with ui.column().classes("w-full px-3 pb-2 gap-1"):
                ui.label(
                    "Without a species link the template / mask dropdowns can't populate. "
                    "Assign a species via the species workbench, or add a `__<species_id>` suffix to the instance id."
                ).classes("text-[11px] text-amber-700")

    # Render the regular params with template_path/mask_path/symmetry excluded —
    # we handle these specially below (or via the species header).
    render_default_params_card(
        job_type, job_model, is_frozen, save_handler, exclude={"template_path", "mask_path", "symmetry"}, ui_mgr=ui_mgr
    )

    if species is None:
        return

    # ── Template + mask + search symmetry ─────────────────────────────────
    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white mt-2"):
        with ui.row().classes("w-full items-center px-3 py-2 bg-gray-50 border-b border-gray-100 gap-2"):
            ui.icon("category", size="14px").classes("text-gray-500")
            ui.label("Template, mask & search symmetry").classes("text-sm font-bold text-gray-800")
            ui.label(
                "(template/mask default to species selection; symmetry is TM-only and ignores particle symmetry)"
            ).classes("text-[11px] text-gray-400 italic ml-2")

        with ui.column().classes("w-full p-3 gap-3"):
            _render_template_dropdown(species, job_model, is_frozen, save_handler)
            _render_mask_dropdown(species, job_model, is_frozen, save_handler)
            _render_symmetry_dropdown(job_model, is_frozen, save_handler)


def _render_template_dropdown(species, job_model, is_frozen: bool, save_handler) -> None:
    templates = list(getattr(species, "templates", []) or [])
    current = getattr(job_model, "template_path", "") or ""
    selected = species.get_selected_template() if hasattr(species, "get_selected_template") else None
    default_path = (selected.template_path if selected else "") or ""

    if not templates:
        with ui.row().classes("w-full items-center gap-3"):
            ui.label("Template").classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")
            ui.label("No templates registered — open the workbench to add one.").classes(
                "text-xs text-orange-500 italic"
            )
        return

    options: dict[str, str] = {}
    for t in templates:
        h = read_template_header(t.template_path)
        bits = []
        if h.apix_ang:
            bits.append(f"{h.apix_ang:.3g} Å/px")
        if h.box_px:
            bits.append(f"box {h.box_px}")
        bits.append(t.polarity)
        if t.lowpass_resolution_ang:
            bits.append(f"lp {t.lowpass_resolution_ang:g}Å")
        suffix_bits = " · ".join(bits)
        label = f"{Path(t.template_path).name}  ·  {suffix_bits}"
        options[t.template_path] = label

    # Resolve the value: explicit per-job override > species selected > first entry
    value = current or default_path
    if value not in options and options:
        value = next(iter(options.keys()))

    with ui.row().classes("w-full items-center gap-3"):
        ui.label("Template").classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")
        sel = ui.select(options=options, value=value).props("outlined dense").classes("flex-1 text-xs font-mono")
        if is_frozen:
            sel.disable()
        else:

            def _on_change(e):
                job_model.template_path = e.value or ""
                save_handler()

            sel.on_value_change(_on_change)


def _render_symmetry_dropdown(job_model, is_frozen: bool, save_handler) -> None:
    options = {s: f"{s}{'  ·  no symmetry (default)' if s == 'C1' else ''}" for s in TM_SYMMETRY_CHOICES}
    value = getattr(job_model, "symmetry", "C1") or "C1"
    if value not in options:
        value = "C1"
    with ui.row().classes("w-full items-center gap-3"):
        ui.label("Symmetry").classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")
        sel = (
            ui.select(options=options, value=value)
            .props("outlined dense")
            .classes("flex-1 text-xs font-mono")
            .tooltip(
                "PyTOM honors only Cn here. Use C1 for asymmetric search (default); "
                "Cn shrinks the angular search for n-fold particles."
            )
        )
        if is_frozen:
            sel.disable()
        else:

            def _on_change(e):
                job_model.symmetry = e.value or "C1"
                save_handler()

            sel.on_value_change(_on_change)


def _render_mask_dropdown(species, job_model, is_frozen: bool, save_handler) -> None:
    masks = list(getattr(species, "masks", []) or [])
    current = getattr(job_model, "mask_path", "") or ""
    selected = species.get_selected_mask() if hasattr(species, "get_selected_mask") else None
    default_path = (selected.mask_path if selected else "") or ""

    if not masks:
        with ui.row().classes("w-full items-center gap-3"):
            ui.label("Mask").classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")
            ui.label("No masks registered — create or import one via the workbench.").classes(
                "text-xs text-orange-500 italic"
            )
        return

    # Include a "(none)" option so the user can explicitly skip a mask.
    options: dict[str, str] = {"": "(none)"}
    for m in masks:
        method = m.method or "manual"
        label = f"{Path(m.mask_path).name}  ·  {method}"
        options[m.mask_path] = label

    value = current or default_path or ""
    if value not in options:
        value = ""

    with ui.row().classes("w-full items-center gap-3"):
        ui.label("Mask").classes("text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right")
        sel = ui.select(options=options, value=value).props("outlined dense").classes("flex-1 text-xs font-mono")
        if is_frozen:
            sel.disable()
        else:

            def _on_change(e):
                job_model.mask_path = e.value or ""
                save_handler()

            sel.on_value_change(_on_change)
