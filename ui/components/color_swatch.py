"""The species overlay-colour swatch.

The creation dialog needs this control before a species exists, so persistence is the
caller's `on_pick`, not the swatch's.

Species share one tomogram canvas, so colour is how a user tells two species' picks apart
there. The swatch is the one place the chrome draws that colour: a 10 px square and its
value, the overlay palette (`SPECIES_OVERLAY_COLORS`) on click.
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from services.models_base import SPECIES_OVERLAY_COLORS
from ui.styles import MONO

_RING = "box-shadow: 0 0 0 1px rgba(15, 29, 46, 0.25);"


def _square_style(color: str) -> str:
    return f"width: 10px; height: 10px; flex-shrink: 0; background: {color}; {_RING}"


class ColorSwatch:
    """Handle on a rendered swatch. `set_color` repaints the square and its value — one
    attribute driving one visual property needs no rebuild (and a rebuild would destroy the
    menu mid-click)."""

    def __init__(self, square: ui.element, value: ui.label) -> None:
        self._square = square
        self._value = value

    def set_color(self, color: str) -> None:
        self._square.style(_square_style(color))
        self._value.set_text(color)


def render_color_swatch(
    current: str, on_pick: Callable[[str], None], *, tooltip: str = "Overlay colour"
) -> ColorSwatch:
    """The square, its value and the palette menu. `on_pick(color)` fires on a choice; the
    swatch repaints itself, so the caller's handler only has to record the value."""
    with ui.element("div").style(
        "display: inline-flex; align-items: center; gap: 6px; cursor: pointer; flex-shrink: 0;"
    ) as box:
        square = ui.element("div").style(_square_style(current))
        value = ui.label(current).style(f"{MONO} font-size: 9.5px; color: var(--cb-x-5b7089, #5b7089);")
        swatch = ColorSwatch(square, value)
        with ui.menu().props("auto-close"), ui.element("div").style("display: flex; gap: 4px; padding: 6px;"):
            for color in SPECIES_OVERLAY_COLORS:
                selected = color.lower() == (current or "").lower()

                def _pick(c=color):
                    swatch.set_color(c)
                    on_pick(c)

                ring = "2px #ffffff, 0 0 0 3px #12324f" if selected else "1px rgba(15, 29, 46, 0.2)"
                ui.element("div").style(
                    f"width: 14px; height: 14px; background: {color}; cursor: pointer; box-shadow: 0 0 0 {ring};"
                ).on("click", _pick)
    box.tooltip(tooltip)
    return swatch
