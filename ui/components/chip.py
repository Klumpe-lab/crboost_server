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


# ── Tiny tag chips (picking-UI roadmap 03 S1) ────────────────────────────────
# Hoisted from `TemplateWorkbench._polarity_chip` / `._method_chip` when the Species
# page's bindings block became the second surface that has to state how a template or
# mask was made. Structurally one thing: a 9 px uppercase tag.

_TAG_STYLE = (
    "font-size: 9px; font-weight: 700; text-transform: uppercase; "
    "padding: 1px 5px; border-radius: 3px; letter-spacing: 0.5px;"
)

_METHOD_PALETTE = {
    "spherical": ("#e9d5ff", "#581c87"),
    "cylindrical": ("#e9d5ff", "#581c87"),
    "relion": ("#e9d5ff", "#581c87"),
    "manual": ("#e5e7eb", "#374151"),
    "imported": ("#e5e7eb", "#374151"),
}


def render_tag_chip(text: str, bg: str, fg: str, *, tooltip: str | None = None) -> ui.element:
    chip = ui.label(text).style(f"background: {bg}; color: {fg}; {_TAG_STYLE}")
    if tooltip:
        chip.tooltip(tooltip)
    return chip


def render_polarity_chip(polarity: str, *, tooltip: str | None = None) -> ui.element:
    """Density polarity of a template — white-on-black vs black-on-white."""
    if polarity == "white":
        return render_tag_chip("white", "#fff7ed", "#9a3412", tooltip=tooltip)
    return render_tag_chip(polarity or "black", "#1f2937", "#f9fafb", tooltip=tooltip)


def render_method_chip(method: str | None, *, tooltip: str | None = None) -> ui.element:
    """How a mask was made (spherical / relion / imported / …)."""
    bg, fg = _METHOD_PALETTE.get(method or "", ("#e5e7eb", "#374151"))
    return render_tag_chip(method or "—", bg, fg, tooltip=tooltip)
