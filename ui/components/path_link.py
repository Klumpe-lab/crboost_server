"""One row for "a file this thing is bound to" (picking-UI roadmap 03 S1).

basename (bold) · full path (truncated, full value on hover) · copy · optional deep
link to wherever the binding is *changed*. Absent is a first-class state: with no path
the row still renders and says so, because a species with no template is legitimate and
a species whose template silently vanished is not (CLAUDE.md "Surfacing uncertainty").

Clipboard goes through `ui.components.copyable.copy_button` — the app already has one
copy affordance with a `navigator.clipboard` fallback; this is not a second one.
"""

from __future__ import annotations

import os
from collections.abc import Callable

from nicegui import ui

from ui.components.copyable import copy_button

_MONO = "font-family: 'IBM Plex Mono', monospace;"
_SANS = "font-family: 'IBM Plex Sans', sans-serif;"

_NAME_STYLE = f"{_MONO} font-size: 11px; font-weight: 600; color: #1e293b; flex-shrink: 0;"
_PATH_STYLE = (
    f"{_MONO} font-size: 10px; color: #94a3b8; flex: 1 1 0; min-width: 0; "
    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; direction: rtl; text-align: left;"
)
_EMPTY_STYLE = f"{_SANS} font-size: 10px; color: #94a3b8; font-style: italic;"
_LINK_STYLE = f"{_SANS} font-size: 10px; color: #6366f1; cursor: pointer; text-decoration: underline dotted;"


def render_open_link(on_open: Callable[[], None], label: str = "open ↗", *, tooltip: str | None = None) -> None:
    """The deep link half of a path row, on its own for callers that place it elsewhere."""
    lbl = ui.label(label).style(_LINK_STYLE).on("click", lambda _e: on_open())
    if tooltip:
        lbl.tooltip(tooltip)


def render_path_link(
    path: str,
    *,
    on_open: Callable[[], None] | None = None,
    open_label: str = "open ↗",
    open_tooltip: str | None = None,
    empty_text: str = "not set",
    note: str | None = None,
) -> None:
    """One bound-file row. `on_open` None (no view switcher registered) renders the row
    without the link rather than a dead one — same handling as `species_opener`. `note`
    (provenance, "source" free text) rides in the hover rather than taking row width."""
    hover = f"{path}\n{note}" if note else path
    with ui.row().classes("items-center gap-2 no-wrap").style("width: 100%; min-width: 0;"):
        if not path:
            ui.label(empty_text).style(_EMPTY_STYLE)
        else:
            ui.label(os.path.basename(path)).style(_NAME_STYLE).tooltip(hover)
            # direction: rtl keeps the *tail* of a long path visible — the part that
            # distinguishes two files in sibling directories.
            ui.label(path).style(_PATH_STYLE).tooltip(hover)
            # Copying a path is never also a selection: inside a clickable table row the
            # bare button's click would bubble and select the row behind it.
            with ui.element("div").style("display: flex; flex-shrink: 0;").on("click.stop", lambda _e: None):
                copy_button(path, tooltip="Copy full path")
        if on_open is not None:
            render_open_link(on_open, open_label, tooltip=open_tooltip)
