"""Inline `static/icons/*.svg` loading (picking-UI roadmap 01 S0).

The roster grew a private loader because it was the only surface drawing house SVGs;
the Species page's empty state needs the same glyph at a different size, so the read
lives here and both call it. Icons are 15x15 viewBox and use the literal token
`currentColor`, which `color` string-replaces so one asset serves the active and
muted states.
"""

from __future__ import annotations

import re
from pathlib import Path

_ICON_DIR = Path("static/icons")
_MISSING = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"/>'
_SIZE_ATTR = re.compile(r'\b(width|height)="\d+(?:\.\d+)?"')


def load_icon_svg(name: str, color: str | None = None, *, size: int | None = None) -> str:
    """Markup of `static/icons/<name>`, with `currentColor` swapped for `color` and the
    asset's intrinsic width/height overridden by `size` (the viewBox does the scaling).
    A missing asset renders as an empty box rather than breaking the surface."""
    try:
        svg = (_ICON_DIR / name).read_text()
    except FileNotFoundError:
        return _MISSING
    if color:
        svg = svg.replace("currentColor", color)
    if size is not None:
        head, sep, tail = svg.partition(">")
        svg = _SIZE_ATTR.sub(lambda m: f'{m.group(1)}="{size}"', head) + sep + tail
    return svg
