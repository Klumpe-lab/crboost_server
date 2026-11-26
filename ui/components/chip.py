"""The shared status chip.

`.cb-chip` capsule — LABEL · value — tinted by `status` (`ok | warn | error | info |
neutral`, CSS in `ui/dashboard/css.py`). Used by the Journey, the tomogram-import dialog
and the Species page. Below it, the polarity and method tags of templates and masks.
"""

from __future__ import annotations

from nicegui import ui

from ui.styles import MONO


def render_chip(
    label: str, value: str, *, status: str = "neutral", tooltip: str | None = None, icon: str | None = None
) -> ui.element:
    """One status chip. `status` ∈ {ok, warn, error, info, neutral}."""
    with ui.element("span").classes(f"cb-chip cb-chip-{status}") as chip:
        if icon:
            ui.icon(icon, size="11px").classes("cb-chip-icon")
        ui.label(label).classes("cb-chip-label")
        ui.label(value).classes("cb-chip-value")
        if tooltip:
            chip.tooltip(tooltip)
    return chip


# ── Polarity and method ──────────────────────────────────────────────────────
# How a template or mask was made, said in words: ink and slate, no fill. Shared by the
# template workbench and the Species tab.

_BODY = "#1e293b"
_SOFT = "var(--cb-x-5b7089, #5b7089)"


def render_polarity_chip(polarity: str, *, tooltip: str | None = None) -> ui.element:
    """Density polarity of a template as a word behind a small square: filled for black (dark
    density on a light background), hollow for white."""
    word = polarity or "black"
    square = (
        f"background: {_BODY}; border: 1px solid {_BODY};"
        if word == "black"
        else f"background: var(--cb-white, #ffffff); border: 1px solid {_SOFT};"
    )
    with ui.element("span").style(
        f"display: inline-flex; align-items: center; gap: 5px; {MONO} font-size: 9.5px; color: {_BODY}; "
        "white-space: nowrap;"
    ) as tag:
        ui.element("i").style(f"width: 7px; height: 7px; display: inline-block; box-sizing: border-box; {square}")
        ui.label(word)
    if tooltip:
        tag.tooltip(tooltip)
    return tag


def render_method_chip(method: str | None, *, tooltip: str | None = None) -> ui.element:
    """How a mask was made (spherical / relion / imported / …), in plain mono text."""
    tag = ui.label(method or "—").style(f"{MONO} font-size: 9.5px; color: {_BODY}; white-space: nowrap;")
    if tooltip:
        tag.tooltip(tooltip)
    return tag
