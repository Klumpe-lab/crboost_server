"""
Reusable copy-to-clipboard affordances.

App-wide rule: wherever an absolute path (or a shell command) is shown, render
it with a small copy button so the user never has to hand-select truncated text.
Use `copyable_path` for inline path rows and `copy_button` to bolt a copy icon
onto anything else (including inside popovers/tooltips).
"""

from __future__ import annotations

from nicegui import ui

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


def copy_button(text: str, *, tooltip: str = "Copy to clipboard", color: str = "#94a3b8", size: str = "xs"):
    async def _do():
        await _write_clipboard(text)

    return (
        ui.button(icon="content_copy", on_click=_do)
        .props(f"flat dense round size={size}")
        .style(f"color: {color}; flex-shrink: 0;")
        .tooltip(tooltip)
    )


def copyable_path(
    path: str,
    *,
    color: str = "#475569",
    font_size: str = "10px",
    wrap: bool = True,
    copy_tooltip: str = "Copy path",
    copy_color: str = "#94a3b8",
):
    """Inline row: the FULL path (wrapping so it is always visible) + a copy icon.

    Set wrap=False to ellipsize on one line instead (still copyable via the icon).
    """
    with ui.row().classes("items-center").style("gap: 4px; flex: 1 1 0; min-width: 0;"):
        if wrap:
            text_style = "word-break: break-all; white-space: normal; line-height: 1.35;"
        else:
            text_style = "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        ui.label(str(path)).style(
            f"{_MONO} font-size: {font_size}; color: {color}; flex: 1 1 0; min-width: 0; {text_style}"
        )
        copy_button(str(path), tooltip=copy_tooltip, color=copy_color)
