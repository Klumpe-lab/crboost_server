"""One row for "a file this thing is bound to".

filename (truncated, never the directory) · copy · optional deep
link to wherever the binding is *changed*. The absolute path shows only on hover, on the
filename and on the copy button. Absent is a first-class state: with no path
the row still renders and says so, because a species with no template is legitimate and
a species whose template silently vanished is not (CLAUDE.md "Surfacing uncertainty").

Clipboard goes through `ui.components.copyable.copy_button`, the app's one copy
affordance with a `navigator.clipboard` fallback.
"""

from __future__ import annotations

import os
from collections.abc import Callable

from nicegui import ui

from ui.components.copyable import copy_button

_MONO = "font-family: 'IBM Plex Mono', monospace;"
_SANS = "font-family: 'IBM Plex Sans', sans-serif;"

# Only the filename is drawn. It is the flexing cell in a table row, so it ellipsises
# rather than pushing the columns beside it off screen; the directory shows on hover only.
_NAME_STYLE = (
    f"{_MONO} font-size: 10px; font-weight: 500; color: #1e293b; flex: 1 1 0; min-width: 0; "
    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: default;"
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
    """One bound-file row: filename, copy, optional deep link — and nothing else.

    The directory never takes row width. Both the filename and the copy button carry the
    absolute path in their hover, so it is one mouse-over away at the point where you
    would reach for it anyway. `on_open` None (no view switcher registered) renders the
    row without the link rather than a dead one — same handling as `species_opener`.
    `note` (provenance, "source" free text) rides in the same hover.
    """
    hover = f"{path}\n{note}" if note else path
    with ui.row().classes("items-center gap-1 no-wrap").style("width: 100%; min-width: 0;"):
        if not path:
            ui.label(empty_text).style(_EMPTY_STYLE)
        else:
            ui.label(os.path.basename(path)).style(_NAME_STYLE).tooltip(hover)
            # Copying a path is never also a selection: inside a clickable table row the
            # bare button's click would bubble and select the row behind it.
            with ui.element("div").style("display: flex; flex-shrink: 0;").on("click.stop", lambda _e: None):
                copy_button(path, tooltip=f"Copy full path\n{path}")
        if on_open is not None:
            render_open_link(on_open, open_label, tooltip=open_tooltip)
