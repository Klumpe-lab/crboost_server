"""The species overlay-colour swatch (picking-UI roadmap 01 S2).

Hoisted out of `ui.species.overview_tab._render_color_swatch`, which was welded to a
saved species (it mutated + saved on pick). The creation dialog needs the same control
*before* a species exists, so the persistence closure is now the caller's `on_pick`.

Species share one tomogram canvas, so colour is how a user tells two picks apart —
editable, constrained to the palette that stays legible over greyscale
(`SPECIES_OVERLAY_COLORS`).
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from services.models_base import SPECIES_OVERLAY_COLORS


def _dot_style(color: str) -> str:
    return (
        f"width: 12px; height: 12px; border-radius: 50%; background: {color}; "
        f"flex-shrink: 0; cursor: pointer; box-shadow: 0 0 0 2px #fff, 0 0 0 3px #e5e7eb;"
    )


class ColorSwatch:
    """Handle on a rendered swatch. `set_color` repaints just the dot — one attribute
    driving one visual property needs no rebuild (and a rebuild would destroy the menu
    mid-click)."""

    def __init__(self, dot: ui.element) -> None:
        self._dot = dot

    def set_color(self, color: str) -> None:
        self._dot.style(_dot_style(color))


def render_color_swatch(current: str, on_pick: Callable[[str], None], *, tooltip: str = "Overlay color") -> ColorSwatch:
    """Colour dot + palette menu. `on_pick(color)` fires on a choice; the dot repaints
    itself, so the caller's handler only has to record the value."""
    dot = ui.element("div").style(_dot_style(current)).tooltip(tooltip)
    swatch = ColorSwatch(dot)
    with dot, ui.menu().props("auto-close"), ui.row().classes("p-2 gap-1 flex-wrap").style("max-width: 128px;"):
        for color in SPECIES_OVERLAY_COLORS:
            selected = color.lower() == (current or "").lower()

            def _pick(c=color):
                swatch.set_color(c)
                on_pick(c)

            ui.element("div").style(
                f"width: 16px; height: 16px; border-radius: 50%; background: {color}; cursor: pointer; "
                f"box-shadow: 0 0 0 2px #fff, 0 0 0 {'3px #111827' if selected else '3px #e5e7eb'};"
            ).on("click", _pick)
    return swatch
