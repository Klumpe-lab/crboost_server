"""The viewer chips in a tomogram preview's bottom-right corner: `napari` opens the volume in napari on a GPU
node (ui/napari_session_dialog.py); `3dmod` copies the command that opens it in IMOD, to paste into a terminal
with X11.

Absolutely positioned, so the preview box must be positioned. Each click is `click.stop`, so a preview whose own
click zooms does not also fire. Colours and hover live in `.cb-viewer-chip` (ui/dashboard/css.py)."""

from __future__ import annotations

from nicegui import ui

from backend import get_backend
from ui.components.copyable import _write_clipboard, imod_command
from ui.job_plugins._field_styles import MONO
from ui.napari_session_dialog import open_napari_dialog

_CHIP_STYLE = (
    "padding: 0 6px; border-radius: var(--cb-r-sm, 3px); "
    f"{MONO} font-size: 9px; line-height: 16px; cursor: pointer; user-select: none;"
)


def _chip(text: str, tooltip: str, on_click) -> None:
    chip = ui.label(text).classes("cb-viewer-chip").style(_CHIP_STYLE).on("click.stop", on_click)
    with chip:
        ui.tooltip(tooltip).style("white-space: pre-line;")


def viewer_chips(volume) -> None:
    """The `napari` and `3dmod` chips for the volume at `volume` (an absolute path)."""
    cmd = imod_command(volume)

    async def _napari() -> None:
        await open_napari_dialog(get_backend(), volume)

    async def _copy_3dmod() -> None:
        await _write_clipboard(cmd)

    with (
        ui.row()
        .classes("no-wrap")
        .style("position: absolute; right: 4px; bottom: 4px; z-index: 2; gap: 4px; align-items: center;")
    ):
        _chip("napari", "Open this tomogram in napari on a GPU node", _napari)
        _chip("3dmod", f"Copy the command that opens this tomogram in 3dmod:\n{cmd}", _copy_3dmod)
