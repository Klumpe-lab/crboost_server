"""
Reusable copy-to-clipboard affordances.

App-wide rule: wherever an absolute path is shown, the path text itself copies it on a
click, so the user never has to hand-select truncated text and no row carries a copy icon.
A path is shown with `ui/components/path_text.py` (cut in its middle, unrolled on hover);
`copy_on_click` makes any other text copy itself. `copy_button` is for what is
not shown as text: a whole log, a command, a file's contents.
"""

from __future__ import annotations

import shlex
from collections.abc import Callable

from nicegui import ui

from ui.components.buttons import icon_button

_MONO = "font-family: 'IBM Plex Mono', monospace;"


def _copied(text: str) -> None:
    """The toast a copy raises: COPIED over what was copied, or its size when it is more than a line."""
    text = str(text)
    lines = text.count("\n") + 1
    shown = text if lines == 1 and len(text) <= 240 else f"{lines} lines, {len(text):,} characters"
    ui.notify(shown, type="positive", classes="cb-toast-copied", timeout=1600)


async def _write_clipboard(text: str) -> None:
    try:
        ui.clipboard.write(text)
        _copied(text)
        return
    except Exception:
        pass
    safe = str(text).replace("`", "\\`")
    try:
        await ui.run_javascript(f"navigator.clipboard.writeText(`{safe}`)", respond=False)
        _copied(text)
    except Exception as e:
        ui.notify(f"Clipboard failed: {e}", type="negative", timeout=2500)


def copy_button(text: str, *, tooltip: str = "Copy to clipboard"):
    async def _do():
        await _write_clipboard(text)

    return icon_button("content_copy", _do, tooltip=tooltip)


def imod_command(volume) -> str:
    """The shell line that opens ``volume`` in IMOD's 3dmod."""
    return f"3dmod {shlex.quote(str(volume))}"


def chimerax_command(volume) -> str:
    """The shell line that opens ``volume`` in ChimeraX."""
    return f"chimerax {shlex.quote(str(volume))}"


def copy_on_click(element, text: str | Callable[[], str]):
    """Make `element` copy `text` when clicked (a callable is read at the click); it turns blue and underlines on
    hover (`.cb-copy-path`, ui/dashboard/css.py). `click.stop` so a clickable parent (a roster row that opens or
    previews its project) does not also fire."""

    async def _do():
        await _write_clipboard(text() if callable(text) else text)

    return element.classes("cb-copy-path").on("click.stop", _do)
