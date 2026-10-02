"""Tilt previews: every tilt of the project as the PNG the tilt filter makes of its
motion-corrected average, grouped by tilt series.

The Tomograms view shows them two ways (``ui/tomo_gallery.py``): the Tilts tab lists every
series as a grid of cards, and a tile on the tomogram wall can swap its slice for a mosaic
of its own series' tilts, in the box the slice occupied. Both come from here: the
collection (the registry plus one listing of the PNG directory), the card and mosaic HTML,
the mosaic's fit, and the click script.

A project holds thousands of tilts, so a series is one HTML string with one delegated click
handler rather than an element per tilt. A tilt opens the tilt filter's full-size viewer,
which re-renders the averaged MRC.
"""

from __future__ import annotations

import html
import logging
import math
import urllib.parse
from collections.abc import Iterable
from pathlib import Path

from services.dashboard_data import position_label
from services.tilt_series import get_registry_for
from ui.components.dialogs import dialog_host

logger = logging.getLogger(__name__)

# One delegated handler per series: the tilt under the click, by its frame id.
TILT_CLICK_JS = """(event) => {
    const tilt = event.target.closest('[data-key]');
    if (tilt) emit({key: tilt.dataset.key});
}"""


# ── Collection ────────────────────────────────────────────────────────────────


def registry_tilt_series(project_path: Path) -> tuple[list, str | None]:
    """The registry's tilt series, and the error to state when it cannot be read.

    Call on the event loop: the cached registry is shared with every reader, and a re-sync
    started from a worker thread could change it under one of them."""
    try:
        return list(get_registry_for(project_path).all_tilt_series()), None
    except Exception:
        logger.exception("Tilt previews: the tilt-series registry of %s could not be read", project_path)
        return [], "The tilt-series registry could not be read; see the server log."


def collect_tilt_groups(tilt_series: Iterable, png_dir: Path) -> list[dict]:
    """One group per tilt series with something to show, ordered like the tomogram wall:
    ``{ts, label, order, tilts, n_png}``, each tilt ``{key, angle, index, png, mrc}``.

    A tilt shows when it has a preview PNG or a motion-corrected average, the full-size
    viewer's source. Its PNG is named after that average (the thumbnail pass converts the
    averages), else after the frame id. Tilts run by angle, most negative first. Disk: one
    listing of the PNG directory. Safe in a thread."""
    pngs = {p.stem: p for p in png_dir.glob("*.png")}
    groups: list[dict] = []
    for ts in tilt_series:
        tilts = []
        for frame in ts.frames:
            avg = next((o.averaged_mrc for o in frame.outputs.values() if o.output_type == "fs_motion_ctf"), None)
            png = (pngs.get(Path(avg).stem) if avg is not None else None) or pngs.get(frame.id)
            if avg is None and png is None:
                continue
            tilts.append(
                {
                    "key": frame.id,
                    "angle": frame.nominal_tilt_angle_deg,
                    "index": frame.tilt_index,
                    "png": str(png) if png is not None else None,
                    "mrc": str(avg) if avg is not None else "",
                }
            )
        if not tilts:
            continue
        tilts.sort(key=lambda t: (t["angle"], t["index"]))
        label, order = position_label(ts.id)
        groups.append(
            {"ts": ts.id, "label": label, "order": order, "tilts": tilts, "n_png": sum(1 for t in tilts if t["png"])}
        )
    groups.sort(key=lambda g: (g["order"], g["ts"]))
    return groups


def thumbnail_task_key(project_path: Path, png_dir: Path) -> str:
    """The dedup key the thumbnail pass runs under (``ensure_tilt_thumbnails`` and the tilt
    filter's Generate button), so a page can tell while it runs."""
    return f"tilt-filter-thumbnails:{project_path}:{png_dir}"


# ── HTML ──────────────────────────────────────────────────────────────────────


def _tip(tilt: dict) -> str:
    return f"{tilt['angle']:+.1f}° · tilt {tilt['index']} · {tilt['key']}"


def _image(tilt: dict) -> str:
    """The tilt's PNG as a square filling its box's width, or a dark square where it has none."""
    if tilt["png"]:
        url = f"/api/tilt-thumb?path={urllib.parse.quote(tilt['png'], safe='')}"
        return (
            f'<img src="{url}" loading="lazy" alt="" style="width:100%;aspect-ratio:1;object-fit:cover;display:block;">'
        )
    return '<div style="width:100%;aspect-ratio:1;background:#1e293b;"></div>'


def card_grid_html(tilts: list[dict], card_px: int) -> str:
    """One series' tilts as cards for the Tilts tab: the preview over a caption row with the
    angle and the acquisition index."""
    cards = "".join(
        f'<div class="cb-tp-card" data-key="{html.escape(t["key"])}" title="{html.escape(_tip(t))}">'
        f"{_image(t)}"
        f'<div class="cb-tp-cap"><b>{t["angle"]:+.1f}°</b><span>tilt {t["index"]}</span></div>'
        "</div>"
        for t in tilts
    )
    return (
        f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax({card_px}px,1fr));gap:4px;">'
        f"{cards}</div>"
    )


def mosaic_layout(n: int, aspect: float) -> tuple[int, int]:
    """(cols, rows) giving n square cells the largest size in a frame of aspect W/H. Ties go
    to more columns: 41 tilts in a square frame are 7 × 6."""
    best, best_side = (1, n), -1.0
    for cols in range(1, n + 1):
        rows = math.ceil(n / cols)
        side = min(aspect / cols, 1 / rows)  # in units of the frame height
        if side >= best_side:
            best, best_side = (cols, rows), side
    return best


def mosaic_html(tilts: list[dict], aspect: float | None) -> tuple[str, str | None]:
    """A series' tilts as square cells fitted into a tile's frame of aspect W/H, centred both
    ways, in angle order from the top left. Returns the HTML and, for a frame with no known
    extent (``aspect`` None), the CSS aspect-ratio the frame takes so the grid fills it.

    The grid sits in a box placed absolutely over the frame, so its size is a percentage of
    the frame and follows the tile size with no re-render. Each cell's 1 px padding is the
    gutter; a CSS gap would add to the percentages and overflow the frame."""
    cols, rows = mosaic_layout(len(tilts), aspect or 1.0)
    frame_aspect = aspect or cols / rows
    width_pct = cols * min(100 / cols, 100 / (frame_aspect * rows))
    cells = "".join(
        f'<div class="cb-tp-cell" data-key="{html.escape(t["key"])}" title="{html.escape(_tip(t))}" '
        f'style="padding:1px;box-sizing:border-box;min-width:0;">{_image(t)}</div>'
        for t in tilts
    )
    grid = (
        '<div style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);'
        f'width:{width_pct:.3f}%;display:grid;grid-template-columns:repeat({cols},minmax(0,1fr));">'
        f"{cells}</div>"
    )
    return grid, (None if aspect else f"{cols}/{rows}")


# ── The viewer ────────────────────────────────────────────────────────────────


def open_tilt_viewer(tilt: dict, project_path: Path) -> None:
    """The tilt filter's full-size viewer on one tilt's averaged MRC (512 px to full size),
    parented at the page layout so a re-render of the tile or group that opened it cannot
    delete it mid-load."""
    from ui.tilt_filter_panel import _show_upsample

    with dialog_host():
        _show_upsample(tilt["mrc"], tilt["key"], Path(project_path))
