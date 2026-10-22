"""House action button — the one text-button chrome for the app.

Slate outline on white, 10 px Plex Sans, 22 px tall — the same austere voice as the
section headers, table heads and the segmented strip. Three variants only:

    house_button("Generate", ...)                     # any section-level action
    house_button("Create", ..., kind="accent")        # the one primary action of a dialog/page
    house_button("Delete", ..., kind="danger")        # destructive actions

The core look is inline on the element, not a stylesheet class: page-shell CSS
(main_ui add_head_html) is served stale on this deployment, and a button whose whole
identity lives in a class renders as a default blue Quasar pill until the shell
refreshes. Same approach as ui/job_plugins/_field_styles.py.
`.cb-btn*` classes stay on the element for the hover states in ui/dashboard/css.py,
which is injected per client over the socket and therefore always fresh.

New UI must use this instead of ad-hoc `color=primary` / styled `ui.button`s.
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

_BASE_STYLE = (
    "font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; font-weight: 600; "
    "letter-spacing: 0.02em; color: #24405e; background: #ffffff; "
    "border: 1px solid var(--cb-btn-line, #b8cce0); border-radius: var(--cb-field-radius, 4px); "
    "height: 22px; min-height: 22px; padding: 0 10px; min-width: 84px;"
)
_KIND_STYLE = {
    "default": "",
    "accent": "background: var(--cb-btn-accent, #1f4e79); border-color: var(--cb-btn-accent, #1f4e79); color: #f8fafc;",
    "danger": "color: #b91c1c; border-color: #fca5a5;",
}


def house_button(
    label: str,
    on_click: Callable | None = None,
    *,
    kind: str = "default",
    tooltip: str | None = None,
    icon: str | None = None,
) -> ui.button:
    # color=None: NiceGUI defaults to color='primary', whose bg-primary class paints
    # the Material blue pill with !important — it must not exist, not be overridden.
    # `icon` (a Material name) leads the label — the one icon+text button in the house
    # vocabulary is `Curate picks`, where the glyph is the door to ArtiaX.
    btn = ui.button(label, icon=icon, on_click=on_click, color=None).props("unelevated dense no-caps")
    btn.classes("cb-btn" + ("" if kind == "default" else f" cb-btn--{kind}"))
    btn.style(_BASE_STYLE + _KIND_STYLE[kind])
    if tooltip:
        btn.tooltip(tooltip)
    return btn


# The icon-only button of the house vocabulary: a 20 px square that sits flush with 16-20 px fields and
# segmented strips in a header or toolbar. Muted until hovered; `danger` (deletes) turns red on hover only.
# Hover, pressed and armed states live in ui/dashboard/css.py (`.cb-icon-btn`).
_ICON_STYLE = (
    "width: 20px; height: 20px; min-width: 20px; min-height: 20px; padding: 0; flex-shrink: 0; "
    "border-radius: var(--cb-field-radius, 4px); color: #6b829b;"
)


def icon_button(
    icon: str, on_click: Callable | None = None, *, tooltip: str | None = None, kind: str = "default"
) -> ui.button:
    """`kind`: "default" or "danger". `color=None` for the same reason as `house_button`."""
    btn = ui.button(icon=icon, on_click=on_click, color=None).props("flat dense")
    btn.classes("cb-icon-btn" + ("" if kind == "default" else f" cb-icon-btn--{kind}"))
    btn.style(_ICON_STYLE)
    if tooltip:
        btn.tooltip(tooltip)
    return btn
