"""Reusable read-only template + particle metadata card for one species.

Shown wherever a template is referenced (TM job-config, candidate-extract
job-config, pixel-sanity panel, species panel). Replaces the old
"path strings dangling in space" UX where the user couldn't tell which
template they were about to use without opening the workbench.

Reads the v3 schema — resolves the species's *selected* template and
mask via `species.get_selected_template()` / `species.get_selected_mask()`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

from nicegui import ui

from services.templating.template_metadata import (
    get_effective_mask_path,
    get_effective_template_path,
    get_selected_mask,
    get_selected_template,
    read_template_header,
)


_KEY_LABEL_CLASS = "text-[10px] font-bold text-gray-400 uppercase tracking-wider"
_VALUE_CLASS = "text-xs text-gray-800"
_VALUE_DIM_CLASS = "text-xs text-gray-400 italic"


def render_template_summary_card(
    species,
    *,
    on_edit: Optional[Callable[[], None]] = None,
    compact: bool = False,
) -> None:
    """Render a compact read-only metadata card for one species.

    Parameters
    ----------
    species:
        The `ParticleSpecies` to summarize. May have either v2 schema
        (`species.template`) or v1 schema (`species.template_path`); the
        card resolves both.
    on_edit:
        Optional callback for the "Edit in workbench" link. If None, the
        link is hidden — useful for the pixel-sanity panel where the
        card is informational only.
    compact:
        If True, render a single-row strip suitable for inlining in a
        denser table-like context (sanity panel rows). Default is the
        full multi-row card.
    """
    if species is None:
        with ui.row().classes("w-full px-3 py-2 bg-gray-50 rounded gap-2 items-center"):
            ui.icon("warning", size="14px").classes("text-gray-400")
            ui.label("No species linked").classes(_VALUE_DIM_CLASS)
        return

    template_path = get_effective_template_path(species)
    mask_path = get_effective_mask_path(species)
    header = read_template_header(template_path) if template_path else None

    species_color = getattr(species, "color", "#3b82f6") or "#3b82f6"
    species_name = getattr(species, "name", None) or getattr(species, "id", "?")

    if compact:
        _render_compact(species, species_color, species_name, template_path, mask_path, header)
        return

    # Outer card with a thick indigo left border so the v2 surface is
    # visually unmistakable next to the (similar-looking) species badge
    # that already lived above it.
    with ui.card().classes("w-full overflow-hidden bg-white mt-1").style(
        "border: 1px solid #e5e7eb; border-left: 4px solid #4f46e5; box-shadow: 0 1px 2px rgba(0,0,0,0.04);"
    ):
        # Section title bar — clearly labelled "TEMPLATE" so this can't be
        # confused with the particle species pill above the renderer.
        with ui.row().classes("w-full items-center px-3 py-2 bg-indigo-50 border-b border-indigo-100 gap-2"):
            ui.icon("category", size="16px").classes("text-indigo-600")
            ui.label("Species template").classes("text-[11px] font-bold text-indigo-900 uppercase tracking-wider")
            ui.element("div").classes("flex-1")
            ui.element("div").style(
                f"width: 10px; height: 10px; border-radius: 50%; "
                f"background: {species_color}; flex-shrink: 0;"
            )
            ui.label(species_name).classes("text-xs font-semibold text-gray-700")
            if on_edit is not None:
                ui.button("Edit", icon="edit", on_click=on_edit).props("flat dense no-caps size=sm")

        # No template registered yet — show a prominent empty-state with a
        # call to action, not a barely-visible italic line.
        if not template_path:
            with ui.column().classes("w-full px-3 py-4 items-start gap-2"):
                with ui.row().classes("w-full items-center gap-2"):
                    ui.icon("hourglass_empty", size="18px").classes("text-gray-400")
                    ui.label("No template registered for this species").classes(
                        "text-sm font-semibold text-gray-700"
                    )
                ui.label(
                    "Open the template workbench to fetch from PDB/EMDB, generate a basic shape, "
                    "or import an existing .mrc."
                ).classes("text-xs text-gray-500")
            return

        with ui.column().classes("w-full px-3 py-3 gap-2"):
            _render_template_row(species, template_path, header)
            _render_mask_row(species, mask_path)
            _render_particle_row(species)
            _render_provenance_row(species)


def _render_template_row(species, template_path: str, header) -> None:
    tpl = get_selected_template(species)
    polarity = getattr(tpl, "polarity", None) if tpl else None
    lowpass = getattr(tpl, "lowpass_resolution_ang", None) if tpl else None

    with ui.row().classes("w-full items-baseline gap-3"):
        ui.label("Template").classes(_KEY_LABEL_CLASS + " w-20 shrink-0 text-right")
        with ui.column().classes("flex-1 gap-0.5"):
            with ui.row().classes("items-center gap-2"):
                if polarity:
                    _polarity_chip(polarity)
                ui.label(Path(template_path).name).classes(_VALUE_CLASS + " font-mono")

            metric_parts: list[str] = []
            if header and header.apix_ang:
                metric_parts.append(f"apix {header.apix_ang:.3g} Å/px")
            else:
                metric_parts.append("apix ?")
            if header and header.box_px:
                metric_parts.append(f"box {header.box_px} px")
                if header.nx and header.ny and header.nz and not (header.nx == header.ny == header.nz):
                    metric_parts.append(f"(non-cube {header.nx}×{header.ny}×{header.nz})")
            else:
                metric_parts.append("box ?")
            # Lowpass status. None = unfiltered (surface this — it's a thing
            # users want to know before submitting a TM job).
            if lowpass:
                metric_parts.append(f"lowpass {lowpass:g} Å")
            else:
                metric_parts.append("lowpass: none")
            if not header or (header.apix_ang is None and header.box_px is None):
                metric_parts.append("header unreadable")
            ui.label(" • ".join(metric_parts)).classes("text-[11px] text-gray-500")


def _render_mask_row(species, mask_path: str) -> None:
    mask_obj = get_selected_mask(species)

    with ui.row().classes("w-full items-baseline gap-3"):
        ui.label("Mask").classes(_KEY_LABEL_CLASS + " w-20 shrink-0 text-right")
        if not mask_path:
            ui.label("none").classes(_VALUE_DIM_CLASS)
            return

        method = getattr(mask_obj, "method", None) if mask_obj else None
        with ui.column().classes("flex-1 gap-0.5"):
            with ui.row().classes("items-center gap-2"):
                ui.label(Path(mask_path).name).classes(_VALUE_CLASS + " font-mono")
                if method:
                    _method_chip(method)

            # If we recorded the relion-mask knobs, surface them on a
            # second line so the user can see *how* this mask was built.
            if mask_obj is not None:
                knob_parts: list[str] = []
                threshold = getattr(mask_obj, "threshold", None)
                extend = getattr(mask_obj, "extend_pixels", None)
                soft = getattr(mask_obj, "soft_edge_pixels", None)
                mlp = getattr(mask_obj, "lowpass_ang", None)
                if threshold is not None:
                    knob_parts.append(f"thr {threshold:g}")
                if extend is not None:
                    knob_parts.append(f"ext {extend:g} px")
                if soft is not None:
                    knob_parts.append(f"soft {soft:g} px")
                if mlp is not None:
                    knob_parts.append(f"lp {mlp:g} Å")
                if knob_parts:
                    ui.label(" • ".join(knob_parts)).classes("text-[11px] text-gray-500")


def _render_particle_row(species) -> None:
    diameter = getattr(species, "diameter_ang", None)
    symmetry = getattr(species, "symmetry", None) or "C1"
    parts: list[str] = []
    if diameter:
        parts.append(f"diameter {diameter:g} Å")
    else:
        parts.append("diameter not set")
    parts.append(f"symmetry {symmetry}")

    with ui.row().classes("w-full items-baseline gap-3"):
        ui.label("Particle").classes(_KEY_LABEL_CLASS + " w-20 shrink-0 text-right")
        ui.label(" • ".join(parts)).classes(_VALUE_CLASS)


def _render_provenance_row(species) -> None:
    tpl = get_selected_template(species)
    if tpl is None:
        return
    source = getattr(tpl, "source", None) or ""
    imported_from = getattr(tpl, "imported_from", None) or ""
    notes = getattr(tpl, "notes", "") or getattr(species, "notes", "")
    if not source and not imported_from and not notes:
        return
    with ui.row().classes("w-full items-baseline gap-3"):
        ui.label("Source").classes(_KEY_LABEL_CLASS + " w-20 shrink-0 text-right")
        with ui.column().classes("flex-1 gap-0.5"):
            if source:
                ui.label(source).classes(_VALUE_CLASS)
            # `imported_from` retains the original outside-the-project path
            # the user pointed at when importing. Tiny so it doesn't crowd
            # the source line, but still recoverable for forensics.
            if imported_from:
                ui.label(f"imported from {imported_from}").classes("text-[11px] text-gray-400 font-mono")
            if notes:
                ui.label(notes).classes("text-[11px] text-gray-500 italic")


def _polarity_chip(polarity: str) -> None:
    """Tiny chip distinguishing white/black-density templates. Polarity used
    to be filename-encoded magic; in v2 it's a typed field."""
    if polarity == "white":
        bg = "#fff7ed"
        fg = "#9a3412"
        label = "white"
    else:
        bg = "#1f2937"
        fg = "#f9fafb"
        label = polarity or "black"
    ui.label(label).style(
        f"background: {bg}; color: {fg}; "
        f"font-size: 9px; font-weight: 700; text-transform: uppercase; "
        f"padding: 1px 6px; border-radius: 3px; letter-spacing: 0.5px;"
    )


def _method_chip(method: str) -> None:
    """Tiny chip indicating how the mask was generated."""
    palette = {
        "spherical": ("#e0f2fe", "#075985"),
        "cylindrical": ("#e0e7ff", "#3730a3"),
        "relion": ("#dcfce7", "#166534"),
        "manual": ("#fef3c7", "#92400e"),
        "imported": ("#f1f5f9", "#475569"),
    }
    bg, fg = palette.get(method, ("#f3f4f6", "#374151"))
    ui.label(method).style(
        f"background: {bg}; color: {fg}; "
        f"font-size: 9px; font-weight: 700; text-transform: uppercase; "
        f"padding: 1px 6px; border-radius: 3px; letter-spacing: 0.5px;"
    )


def _render_compact(species, species_color: str, species_name: str, template_path: str, mask_path: str, header) -> None:
    """Single-row compact rendering for inlining in dense contexts (sanity
    panel rows). Trades richness for vertical density."""
    parts: list[str] = []
    if header and header.apix_ang:
        parts.append(f"{header.apix_ang:.3g} Å/px")
    if header and header.box_px:
        parts.append(f"box {header.box_px}")
    diameter = getattr(species, "diameter_ang", None)
    if diameter:
        parts.append(f"Ø {diameter:g} Å")
    sym = getattr(species, "symmetry", None) or "C1"
    parts.append(f"sym {sym}")

    with ui.row().classes("w-full items-center gap-2 px-2 py-1 bg-gray-50 rounded"):
        ui.element("div").style(
            f"width: 8px; height: 8px; border-radius: 50%; background: {species_color}; flex-shrink: 0;"
        )
        ui.label(species_name).classes("text-xs font-semibold text-gray-700")
        if template_path:
            ui.label(Path(template_path).name).classes("text-xs text-gray-500 font-mono")
        else:
            ui.label("(no template)").classes(_VALUE_DIM_CLASS)
        if parts:
            ui.label(" • ".join(parts)).classes("text-[11px] text-gray-500 ml-auto")
