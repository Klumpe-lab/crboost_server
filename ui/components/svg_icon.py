"""Inline `static/icons/*.svg` loading.

The roster grew a private loader because it was the only surface drawing house SVGs;
the Species page's empty state needs the same glyph at a different size, so the read
lives here and both call it. Icons are 15x15 viewBox and use the literal token
`currentColor`, which `color` string-replaces so one asset serves the active and
muted states.
"""

from __future__ import annotations

import functools
import re

from nicegui import ui

from services.configs.config_service import REPO_ROOT

_ICON_DIR = REPO_ROOT / "static" / "icons"
_MISSING = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"/>'
_SIZE_ATTR = re.compile(r'\b(width|height)="\d+(?:\.\d+)?"')


@functools.cache
def _icon_text(name: str) -> str | None:
    """An asset's markup, read once per process: icons ship with the code, and rosters render
    dozens per rebuild."""
    try:
        return (_ICON_DIR / name).read_text()
    except FileNotFoundError:
        return None


def load_icon_svg(name: str, color: str | None = None, *, size: int | None = None) -> str:
    """Markup of `static/icons/<name>`, with `currentColor` swapped for `color` and the
    asset's intrinsic width/height overridden by `size` (the viewBox does the scaling).
    A missing asset renders as an empty box rather than breaking the surface."""
    svg = _icon_text(name)
    if svg is None:
        return _MISSING
    if color:
        svg = svg.replace("currentColor", color)
    if size is not None:
        head, sep, tail = svg.partition(">")
        svg = _SIZE_ATTR.sub(lambda m: f'{m.group(1)}="{size}"', head) + sep + tail
    return svg


def render_glyph(icon: str, *, size: int, color: str | None = None):
    """A glyph in the current slot: a `static/icons` asset (a name ending in `.svg`) or a Material
    icon name, `size` px square, in `color`, else in the text colour around it."""
    if icon.endswith(".svg"):
        el = ui.html(load_icon_svg(icon, color, size=size), sanitize=False)
    else:
        el = ui.icon(icon, size=f"{size}px")
        if color:
            el.style(f"color: {color};")
    return el.style("display: flex; flex: 0 0 auto;")
