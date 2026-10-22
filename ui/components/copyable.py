"""
Reusable copy-to-clipboard affordances.

App-wide rule: wherever an absolute path is shown, the path text itself copies it on a
click (`copy_on_click`, `copyable_path` for inline path rows), so the user never has to
hand-select truncated text and no row carries a copy icon. `copy_button` is for what is
not shown as text: a whole log, a command, a file's contents.
"""

from __future__ import annotations

import shlex

from nicegui import ui

from ui.components.buttons import icon_button

_MONO = "font-family: 'IBM Plex Mono', monospace;"


async def _write_clipboard(text: str) -> None:
    try:
        ui.clipboard.write(text)
        ui.notify("Copied", type="positive", timeout=900)
        return
    except Exception:
        pass
    safe = str(text).replace("`", "\\`")
    try:
        await ui.run_javascript(f"navigator.clipboard.writeText(`{safe}`)", respond=False)
        ui.notify("Copied", type="positive", timeout=900)
    except Exception as e:
        ui.notify(f"Clipboard failed: {e}", type="negative", timeout=2500)


def copy_button(text: str, *, tooltip: str = "Copy to clipboard"):
    async def _do():
        await _write_clipboard(text)

    return icon_button("content_copy", _do, tooltip=tooltip)


def imod_command(volume) -> str:
    """The shell line that opens ``volume`` in IMOD's 3dmod."""
    return f"3dmod {shlex.quote(str(volume))}"


def copy_3dmod_chip(volume) -> None:
    """The `3dmod` chip in a tomogram preview's bottom-right corner: a click copies the
    command that opens this volume in 3dmod, to paste into a terminal with X11.

    Absolutely positioned, so the preview box must be positioned. `click.stop`, so a
    preview whose own click zooms does not also fire. Colors and hover live in
    `.cb-3dmod-chip` (ui/dashboard/css.py)."""
    cmd = imod_command(volume)

    async def _copy():
        await _write_clipboard(cmd)

    chip = (
        ui.label("3dmod")
        .classes("cb-3dmod-chip")
        .style(
            "position: absolute; right: 4px; bottom: 4px; z-index: 2; padding: 0 6px; border-radius: 3px; "
            f"{_MONO} font-size: 9px; line-height: 16px; cursor: pointer; user-select: none;"
        )
        .on("click.stop", _copy)
    )
    with chip:
        ui.tooltip(f"Copy the command that opens this tomogram in 3dmod:\n{cmd}").style("white-space: pre-line;")


def copy_on_click(element, text: str):
    """Make `element` copy `text` when clicked; it turns blue and underlines on hover
    (`.cb-copy-path`, ui/dashboard/css.py). `click.stop` so a clickable parent (a roster
    row that opens or previews its project) does not also fire."""

    async def _do():
        await _write_clipboard(text)

    return element.classes("cb-copy-path").on("click.stop", _do)


def copyable_path(
    path: str, *, color: str = "#475569", font_size: str = "10px", wrap: bool = True, copy_tooltip: str = "Copy path"
):
    """The FULL path (wrapping so it is always visible); a click copies it, `copy_tooltip` on hover.

    Set wrap=False to ellipsize on one line instead (the hover then shows the path too).
    """
    if wrap:
        text_style = "word-break: break-all; white-space: normal; line-height: 1.35;"
    else:
        text_style = "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
    label = ui.label(str(path)).style(
        f"{_MONO} font-size: {font_size}; color: {color}; flex: 1 1 0; min-width: 0; {text_style}"
    )
    label.props["title"] = copy_tooltip if wrap else f"{copy_tooltip}\n{path}"
    return copy_on_click(label, str(path))
