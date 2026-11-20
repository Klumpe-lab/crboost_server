"""The viewer chips in a volume preview's bottom-right corner (a tomogram, a class map, a reconstruction):
`napari` opens the volume in napari (EisGreis), in the user's own remote desktop or on a GPU node; `3dmod` and
`ChimeraX` copy the command that opens it in IMOD or ChimeraX, to paste into a terminal with X11.

With an Open OnDemand portal configured (conf.yaml `ood:`), `napari` is a link that opens a tab which ends up in
napari showing the volume (ui/napari_open_page.py), and `▾` beside it opens the session dialog (a full GPU, Stop).
Without one, `napari` opens the dialog (ui/napari_session_dialog.py), which hands over the SSH tunnel.

Absolutely positioned, so the preview box must be positioned. Each click is `click.stop`, so a preview whose own
click zooms does not also fire. Colours and hover live in `.cb-viewer-chip` (ui/dashboard/css.py)."""

from __future__ import annotations

from nicegui import ui

from backend import get_backend
from services.configs.config_service import get_config_service
from ui.components.copyable import _write_clipboard, chimerax_command, imod_command
from ui.job_plugins._field_styles import MONO
from ui.napari_open_page import open_url
from ui.napari_session_dialog import open_napari_dialog

_CHIP_STYLE = (
    "padding: 0 6px; border-radius: var(--cb-r-sm, 3px); "
    f"{MONO} font-size: 9px; line-height: 16px; cursor: pointer; user-select: none;"
)


def _chip(text: str, tooltip: str, on_click) -> None:
    chip = ui.label(text).classes("cb-viewer-chip").style(_CHIP_STYLE).on("click.stop", on_click)
    with chip:
        ui.tooltip(tooltip).style("white-space: pre-line;")


def _link_chip(text: str, tooltip: str, href: str) -> None:
    """A chip that is a link opening `href` in a new tab: the tab opens from the click itself, so no popup
    blocker stands in the way. The click stops at the chip, as `_chip`'s does."""
    chip = (
        ui.link(text, href, new_tab=True)
        .classes("cb-viewer-chip")
        .style(_CHIP_STYLE + " text-decoration: none;")
        .on("click", js_handler="(e) => e.stopPropagation()")
    )
    with chip:
        ui.tooltip(tooltip).style("white-space: pre-line;")


def viewer_chips(volume, *, what: str = "tomogram", chimerax: bool = False) -> None:
    """The `napari` and `3dmod` chips for the volume at `volume` (an absolute path), and `ChimeraX` when asked (maps
    of particles, which are looked at as surfaces). `what` names the volume in the tooltips."""
    cmd = imod_command(volume)
    cx_cmd = chimerax_command(volume)

    async def _napari() -> None:
        await open_napari_dialog(get_backend(), volume)

    async def _copy_3dmod() -> None:
        await _write_clipboard(cmd)

    async def _copy_chimerax() -> None:
        await _write_clipboard(cx_cmd)

    with (
        ui.row()
        .classes("no-wrap")
        .style("position: absolute; right: 4px; bottom: 4px; z-index: 2; gap: 4px; align-items: center;")
    ):
        if get_config_service().ood.enabled:
            _link_chip(
                "napari",
                f"Open this {what} in napari, in a new browser tab (through the cluster's OnDemand portal). "
                f"Starts your napari session when none runs; when one does, shows this {what} in it.",
                open_url(volume),
            )
            _chip("▾", "Your napari session: status, start on a full GPU, Stop", _napari)
        else:
            _chip("napari", f"Open this {what} in napari: in your own remote desktop, or on a GPU node", _napari)
        _chip("3dmod", f"Copy the command that opens this {what} in 3dmod:\n{cmd}", _copy_3dmod)
        if chimerax:
            _chip("ChimeraX", f"Copy the command that opens this {what} in ChimeraX:\n{cx_cmd}", _copy_chimerax)
