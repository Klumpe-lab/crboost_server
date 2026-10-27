"""The Journey's local-warp strip: one tile per tilt of a refined series, in angle order, the tilt's
thumbnail faint under an arrow per grid node (the node's displacement with the tilt's mean removed) and
a hollow arrow from the centre for the whole-tilt shift; a tilt whose shift is over the threshold is
ringed. A click on a tile opens its zoom: the tilt larger with the warp interpolated over the image,
← → stepping the tilts.

Arrows show where image content moved (`services/analysis/alignment_changes.py`, `RefinementChanges`),
in the image's own axes, Y down the rows as the thumbnails show them. One exaggeration serves every
series of a project (`exaggeration`), stated once with a scale arrow, so tiles compare across series.
"""

from __future__ import annotations

import math

from nicegui import background_tasks, ui

from services.analysis.alignment_changes import RefinementChanges, TiltChange, interpolated_field, node_fractions
from services.workers import WorkKind, run_in_worker
from ui.components.buttons import icon_button
from ui.components.dialogs import dialog_host

# The tile: the thumbnail's width and the room around it for arrows at the image's edge nodes.
TILE_IMG_PX = 64
TILE_PAD_PX = 6
# The zoom: the tilt's width and the interpolated field's samples per axis.
ZOOM_IMG_PX = 520
ZOOM_SAMPLES = 12
# The chart's colours: gray-blue for the whole-tilt shift, warm for the warp.
SHIFT_COLOR = "#4e79a7"
WARP_COLOR = "#dd8452"
# A hollow arrow longer than this stops at it (its tooltip carries the number).
_SHIFT_CAP_PX = TILE_IMG_PX / 2

_ZOOM_KEYS_JS = """(event) => {
    if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
    event.preventDefault();
    emit({key: event.key});
}"""


def nice_down(value: float) -> float:
    """The largest 1, 2 or 5 × 10^k not above `value`."""
    if value <= 0:
        raise ValueError(f"need a positive value, got {value}")
    power = 10 ** math.floor(math.log10(value))
    return next(m * power for m in (5, 2, 1) if m * power <= value)


def exaggeration(
    largest_warp_angstrom: float, image_width_angstrom: float, spacing_px: float, image_px: float
) -> float:
    """How much arrows are lengthened so the project's largest warp spans about half the node spacing of
    a picture `image_px` wide."""
    true_px = largest_warp_angstrom * image_px / image_width_angstrom
    return nice_down(0.5 * spacing_px / true_px)


def _fmt_factor(factor: float) -> str:
    return f"{factor:g}"


def _arrow(x0: float, y0: float, dx: float, dy: float, color: str) -> str:
    """A thin arrow from (x0, y0) by (dx, dy); a dot when it is shorter than a pixel."""
    length = math.hypot(dx, dy)
    if length < 1.0:
        return f'<circle cx="{x0:.1f}" cy="{y0:.1f}" r="0.9" fill="{color}"/>'
    ux, uy = dx / length, dy / length
    head = min(3.4, 0.6 * length)
    half = 0.6 * head
    tx, ty = x0 + dx, y0 + dy
    bx, by = tx - ux * head, ty - uy * head
    return (
        f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{bx:.1f}" y2="{by:.1f}" stroke="{color}" stroke-width="1.2" '
        f'stroke-linecap="round"/><polygon points="{tx:.1f},{ty:.1f} {bx - uy * half:.1f},{by + ux * half:.1f} '
        f'{bx + uy * half:.1f},{by - ux * half:.1f}" fill="{color}"/>'
    )


def _hollow_arrow(x0: float, y0: float, dx: float, dy: float, cap: float) -> str:
    """The whole-tilt shift: an outlined arrow from the centre, stopped at `cap`; a ring when too short
    to have a head."""
    length = math.hypot(dx, dy)
    head, shaft, wing = 5.0, 1.3, 3.6
    if length < head + 1:
        return f'<circle cx="{x0:.1f}" cy="{y0:.1f}" r="1.8" fill="none" stroke="{SHIFT_COLOR}" stroke-width="1"/>'
    if length > cap:
        dx, dy, length = dx * cap / length, dy * cap / length, cap
    ux, uy = dx / length, dy / length
    nx, ny = -uy, ux
    bx, by = x0 + ux * (length - head), y0 + uy * (length - head)
    points = [
        (x0 + nx * shaft, y0 + ny * shaft),
        (bx + nx * shaft, by + ny * shaft),
        (bx + nx * wing, by + ny * wing),
        (x0 + dx, y0 + dy),
        (bx - nx * wing, by - ny * wing),
        (bx - nx * shaft, by - ny * shaft),
        (x0 - nx * shaft, y0 - ny * shaft),
    ]
    joined = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    return (
        f'<polygon points="{joined}" fill="rgba(255,255,255,0.75)" stroke="{SHIFT_COLOR}" stroke-width="1.1" '
        'stroke-linejoin="round"/>'
    )


def scale_arrow_svg(length_px: float) -> str:
    """The legend's scale arrow, `length_px` long."""
    width = length_px + 4
    return (
        f'<svg width="{width:.0f}" height="8" style="display: inline-block; vertical-align: middle;">'
        f"{_arrow(1, 4, length_px, 0, WARP_COLOR)}</svg>"
    )


def _picture(
    changes: RefinementChanges,
    tilt: TiltChange,
    thumb: str,
    width_px: float,
    pad: float,
    factor: float,
    cap: float,
    *,
    nodes: bool = True,
) -> tuple[str, float]:
    """A tilt's thumbnail under its node arrows (`nodes`) and its hollow shift arrow, as HTML; and its
    height."""
    image_x, image_y = changes.image_angstrom
    height_px = width_px * image_y / image_x
    px_per_a = width_px / image_x
    fx = node_fractions(changes.width, changes.margin_x)
    fy = node_fractions(changes.height, changes.margin_y)
    marks = []
    if nodes and changes.has_warp:
        for j, y in enumerate(fy):
            for i, x in enumerate(fx):
                marks.append(
                    _arrow(
                        pad + x * width_px,
                        pad + y * height_px,
                        tilt.warp_x[j, i] * px_per_a * factor,
                        tilt.warp_y[j, i] * px_per_a * factor,
                        WARP_COLOR,
                    )
                )
    marks.append(
        _hollow_arrow(
            pad + width_px / 2,
            pad + height_px / 2,
            tilt.shift_x * px_per_a * factor,
            tilt.shift_y * px_per_a * factor,
            cap,
        )
    )
    box_w, box_h = width_px + 2 * pad, height_px + 2 * pad
    img = (
        f'<img src="{thumb}" alt="" draggable="false" style="position: absolute; left: {pad}px; top: {pad}px; '
        f'width: {width_px}px; height: {height_px:.0f}px; object-fit: fill; opacity: 0.45;"/>'
        if thumb
        else ""
    )
    svg = (
        f'<svg width="{box_w:.0f}" height="{box_h:.0f}" '
        'style="position: absolute; left: 0; top: 0; overflow: visible;">'
        f"{''.join(marks)}</svg>"
    )
    return (f'<div style="position: relative; width: {box_w:.0f}px; height: {box_h:.0f}px;">{img}{svg}</div>', box_h)


def _tip(tilt: TiltChange, number: int | None) -> str:
    head = f"Tilt {number} · " if number is not None else ""
    warp = f" · warp {tilt.warp_rms:.1f} Å rms, {tilt.warp_max:.1f} Å max" if tilt.warp_max > 0 else ""
    return f"{head}{tilt.angle:+.1f}° · {tilt.movie}\nwhole-tilt shift {tilt.shift:.1f} Å{warp}"


def render_warp_strip(
    changes: RefinementChanges,
    numbers: list[int | None],
    thumbs: list[str],
    *,
    factor: float,
    zoom_factor: float,
    scale_angstrom: float,
    threshold: float | None,
) -> None:
    """The strip into the current slot: its legend line, then a tile per tilt of `changes` (`numbers` and
    `thumbs` parallel to its tilts). `factor` lengthens the tiles' arrows, `zoom_factor` the zoom's."""
    image_x = changes.image_angstrom[0]
    with ui.element("div").classes("cb-warp-legend"):
        ui.label(f"Local warp per tilt · arrows ×{_fmt_factor(factor)}")
        ui.html(scale_arrow_svg(scale_angstrom * TILE_IMG_PX / image_x * factor), sanitize=False)
        ui.label(f"= {scale_angstrom:g} Å")
        ring = f" · ringed: shift over {threshold:.3g} Å (2 px)" if threshold is not None else ""
        ui.label(f"· hollow: the whole-tilt shift, stopped at the tile's edge{ring}")
    with ui.element("div").style("display: flex; flex-wrap: wrap; gap: 6px;"):
        for k, tilt in enumerate(changes.tilts):
            over = threshold is not None and tilt.shift > threshold
            html, _ = _picture(changes, tilt, thumbs[k], TILE_IMG_PX, TILE_PAD_PX, factor, _SHIFT_CAP_PX)
            tile = ui.element("div").classes("cb-warp-tile" + (" over" if over else ""))
            tile.style(
                "display: flex; flex-direction: column; align-items: center; border-radius: 4px; "
                + (f"box-shadow: 0 0 0 2px {SHIFT_COLOR};" if over else "box-shadow: 0 0 0 1px #dbe3ec;")
            )
            tile.props["title"] = _tip(tilt, numbers[k])
            with tile:
                ui.html(html, sanitize=False)
                ui.label(f"{tilt.angle:+.0f}°").classes("cb-warp-angle")
            tile.on(
                "click",
                lambda _e, k=k: open_warp_zoom(changes, numbers, thumbs, k, factor=zoom_factor, threshold=threshold),
            )


def open_warp_zoom(
    changes: RefinementChanges,
    numbers: list[int | None],
    thumbs: list[str],
    index: int,
    *,
    factor: float,
    threshold: float | None,
) -> None:
    """A dialog with tilt `index` at `ZOOM_IMG_PX`, its warp interpolated over the image (worker) and its
    hollow shift arrow; ← → or the chevrons step the tilts."""
    at = {"k": index, "gen": 0}
    with dialog_host():
        dlg = ui.dialog().classes("cb-warp-zoom-dialog")
        with dlg, ui.element("div").classes("cb-warp-zoom"):
            with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                icon_button("chevron_left", lambda: _step(-1), tooltip="Previous tilt (←)")
                title = ui.label().classes("cb-warp-zoom-title")
                icon_button("chevron_right", lambda: _step(1), tooltip="Next tilt (→)")
            body = ui.element("div")
            note = ui.label().classes("cb-warp-zoom-note")

    def _step(delta: int) -> None:
        k = at["k"] + delta
        if 0 <= k < len(changes.tilts):
            at["k"] = k
            _fill()

    def _on_key(e) -> None:
        _step(-1 if (e.args or {}).get("key") == "ArrowLeft" else 1)

    def _fill() -> None:
        at["gen"] += 1
        gen, k = at["gen"], at["k"]
        tilt = changes.tilts[k]
        number = f"Tilt {numbers[k]} · " if numbers[k] is not None else ""
        title.set_text(f"{number}{tilt.angle:+.1f}° · {tilt.movie}")
        over = threshold is not None and tilt.shift > threshold
        warp = f"warp {tilt.warp_rms:.1f} Å rms, {tilt.warp_max:.1f} Å max · " if changes.has_warp else ""
        note.set_text(
            f"whole-tilt shift {tilt.shift:.1f} Å{' (over 2 px)' if over else ''} · {warp}"
            f"arrows ×{_fmt_factor(factor)} · {k + 1} of {len(changes.tilts)}"
        )
        body.clear()
        with body:
            ui.label("interpolating the warp…").classes("cb-section-placeholder")
        if changes.has_warp:
            background_tasks.create(_draw(gen, k), name="warp-zoom")
        else:
            _place(gen, k, "")

    async def _draw(gen: int, k: int) -> None:
        tilt = changes.tilts[k]
        fx, fy, u, v = await run_in_worker(
            WorkKind.RENDER,
            interpolated_field,
            tilt.warp_x,
            tilt.warp_y,
            changes.margin_x,
            changes.margin_y,
            ZOOM_SAMPLES,
        )
        image_x, image_y = changes.image_angstrom
        height = ZOOM_IMG_PX * image_y / image_x
        px_per_a = ZOOM_IMG_PX / image_x * factor
        pad = _zoom_pad()
        field = "".join(
            _arrow(pad + x * ZOOM_IMG_PX, pad + y * height, u[j, i] * px_per_a, v[j, i] * px_per_a, WARP_COLOR)
            for j, y in enumerate(fy)
            for i, x in enumerate(fx)
        )
        _place(gen, k, field)

    def _place(gen: int, k: int, field: str) -> None:
        if gen != at["gen"] or body.is_deleted:
            return
        tilt = changes.tilts[k]
        pad = _zoom_pad()
        # The node arrows are left out: the field passes through them.
        html, box_h = _picture(changes, tilt, thumbs[k], ZOOM_IMG_PX, pad, factor, ZOOM_IMG_PX / 2, nodes=False)
        overlay = (
            f'<svg width="{ZOOM_IMG_PX + 2 * pad}" height="{box_h:.0f}" '
            f'style="position: absolute; left: 0; top: 0; overflow: visible;">{field}</svg>'
        )
        body.clear()
        with body:
            ui.html(f'<div style="position: relative;">{html}{overlay}</div>', sanitize=False)

    dlg.on("keydown", _on_key, js_handler=_ZOOM_KEYS_JS)
    dlg.on("hide", lambda _e: dlg.delete())
    _fill()
    dlg.open()


def _zoom_pad() -> float:
    return ZOOM_IMG_PX / ZOOM_SAMPLES / 2
