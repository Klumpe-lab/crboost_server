"""The one status chip (roadmap 10 S3).

`.cb-chip` capsule — LABEL · value — tinted by `status` (`ok | warn | error | info |
neutral`, CSS in `ui/dashboard/css.py`). Hoisted from the Journey's `_render_chip` and
the tomogram-import dialog's `_chip` copy when the Species page became the third user.
"""

from __future__ import annotations

from nicegui import ui


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
