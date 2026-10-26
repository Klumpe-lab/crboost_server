"""The layers list, as every view's layers menu holds it: the Tomograms wall and its zoom, the
Journey's Reconstruct card and the pick viewer.

Two kinds of annotation share one list and one logic: the pick layers, by species, and the
segmentations. Each row is a check, the layer's key in its colour (a click on it opens the colours,
where the view lets it be recoloured), its name and a count. Every layer follows the version
(`services/segmentation/display.py`): it shows over a picture of the reconstruction it was made on
and is hidden over another; a click on its row switches it by hand, and "Follow the version" at the
top puts every hand-switched layer back. A layer shown over another reconstruction is flagged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

from nicegui import ui

from services.models_base import SPECIES_OVERLAY_COLORS
from services.segmentation import display
from ui.components.hover_menu import hover_menu, tool_trigger
from ui.components.segmented import render_segmented
from ui.components.svg_icon import load_icon_svg

OPACITIES: tuple[tuple[str, str], ...] = (("0.25", "25%"), ("0.4", "40%"), ("0.6", "60%"))
FEATURES_TIP = (
    "Segmented features: each fills where its probability is at least 0.5 in any plane of the slab the "
    "picture shows, so a membrane thinner than the slab still shows."
)
_FOLLOW_TIP = (
    "Each layer shows over the versions of the reconstruction it was picked or segmented on (a denoised "
    "version counts as the reconstruction it was denoised from) and is hidden over another; a layer whose "
    "reconstruction is not known shows on every version. A click on a layer switches it by hand on every "
    "version; a click here puts every layer back on following the version."
)


class LayerRow(NamedTuple):
    """One layer's row. `recon` is the reconstruct instance it was made on (None: not known); `group`
    the species a pick layer belongs to; `shape` a pick layer's dot glyph; `on_color` recolours the
    layer (None: its colour is not this list's to change)."""

    id: str
    name: str
    count: str
    color: str
    recon: str | None
    recon_label: str = ""
    group: str = ""
    shape: str = "circle"
    on_color: Callable[[str], None] | None = None


def shape_svg(shape: str, color: str, size: int = 10) -> str:
    """A pick layer's key: the glyph its dots are drawn with, at list size."""
    c, r = size / 2, size / 2 - 1
    paint = f'fill="{color}" stroke="rgba(15,23,42,0.35)" stroke-width="1"'
    if shape == "square":
        mark = f'<rect x="1.5" y="1.5" width="{size - 3}" height="{size - 3}" {paint}/>'
    elif shape == "diamond":
        mark = f'<polygon points="{c},1 {size - 1},{c} {c},{size - 1} 1,{c}" {paint}/>'
    elif shape == "triangle":
        mark = f'<polygon points="{c},1 {size - 1},{size - 1} 1,{size - 1}" {paint}/>'
    else:
        mark = f'<circle cx="{c}" cy="{c}" r="{r}" {paint}/>'
    return f'<svg width="{size}" height="{size}" style="display: block;">{mark}</svg>'


def swatch_svg(color: str, size: int = 10) -> str:
    """A segmentation's key: a rounded square, where a pick layer's is its dot."""
    return (
        f'<svg width="{size}" height="{size}" style="display: block;"><rect x="1" y="1" width="{size - 2}" '
        f'height="{size - 2}" rx="2" fill="{color}" stroke="rgba(15,23,42,0.35)" stroke-width="1"/></svg>'
    )


def palette_row(on_pick: Callable[[str], None], mark: Callable[[str, int], str]) -> ui.element:
    """The colours a layer can take, as a row under the layer's row, hidden until a click on the layer's
    key shows it: in place, because a menu of its own would close a hover menu the list sits in.
    `mark(color, size)` draws a colour as the layer's own key."""
    row = ui.element("div").classes("cb-gal-pk-palette")
    row.set_visibility(False)
    with row:
        for color in SPECIES_OVERLAY_COLORS:
            swatch = ui.html(mark(color, 14), sanitize=False).classes("cb-gal-pk-marker")
            swatch.on("click.stop", lambda _e, c=color: on_pick(c))
    return row


def shown(project_path, row: LayerRow, shows: str | None) -> bool:
    """Whether the layer draws over a picture of reconstruction `shows`."""
    return display.is_shown(project_path, row.id, display.on_source(row.recon, shows))


def render_layer_list(
    project_path, picks: list[LayerRow], features: list[LayerRow], *, shows: str | None, on_change: Callable[[], None]
) -> None:
    """The rows into the current slot, read against a picture of reconstruction `shows`: "Follow the
    version", the pick layers under their species, the segmentations and the fills' opacity.
    `on_change` runs after any change, for the view to repaint the list and what it draws."""
    rows = [*picks, *features]
    if not rows:
        ui.label("No picks or segmentations yet.").classes("cb-hmenu-empty")
        return

    def _set(targets: list[LayerRow], on: bool) -> None:
        for r in targets:
            display.set_shown(project_path, r.id, on, display.on_source(r.recon, shows))
        on_change()

    def _follow() -> None:
        display.unpin(project_path, (r.id for r in rows))
        on_change()

    def _opacity(value: str) -> None:
        display.set_opacity(project_path, value)
        on_change()

    following = not any(display.is_pinned(project_path, r.id) for r in rows)
    follow = ui.element("div").classes("cb-gal-pk-row cb-gal-pk-follow")
    follow.on("click", lambda _e: _follow())
    follow.props["title"] = _FOLLOW_TIP
    with follow:
        ui.icon("check_box" if following else "check_box_outline_blank", size="14px")
        ui.label("Follow the version")

    if picks:
        _section("particle.svg", "Particles")
        group = None
        for r in picks:
            if r.group != group:
                group = r.group
                members = [m for m in picks if m.group == group]
                head = ui.element("div").classes("cb-gal-pk-species cb-lyr-group")
                head.props["title"] = "Show or hide all of this species' layers"
                # Any shown: hide them all; none shown: show them all.
                head.on("click", lambda _e, ms=members: _set(ms, not any(shown(project_path, m, shows) for m in ms)))
                with head:
                    ui.html(load_icon_svg("particle.svg", "#94a3b8", size=11), sanitize=False).style("display: flex;")
                    ui.label(group)
            _row(project_path, r, shows, lambda c, s=r.shape: shape_svg(s, c), "Picked", _set)
    if features:
        _section("segmentation.svg", "Segmentations", FEATURES_TIP)
        for r in features:
            _row(project_path, r, shows, swatch_svg, "Segmented", _set)
        with ui.element("div").classes("cb-gal-feat-opacity"):
            ui.label("opacity")
            render_segmented(list(OPACITIES), display.opacity(project_path), _opacity)


def _section(glyph: str, title: str, tip: str | None = None) -> None:
    head = ui.element("div").classes("cb-lyr-sec")
    with head:
        ui.html(load_icon_svg(glyph, "#6b829b", size=11), sanitize=False).style("display: flex;")
        ui.label(title)
    if tip:
        head.props["title"] = tip


def _row(project_path, r: LayerRow, shows: str | None, mark: Callable[..., str], verb: str, set_shown) -> None:
    on = shown(project_path, r, shows)
    item = ui.element("div").classes("cb-gal-pk-row" + ("" if on else " off"))
    item.on("click", lambda _e: set_shown([r], not on))
    with item:
        ui.icon("check_box" if on else "check_box_outline_blank", size="14px")
        marker = ui.html(mark(r.color, 10), sanitize=False).classes("cb-gal-pk-marker")
        ui.label(r.name).classes("cb-gal-pk-name")
        ui.label(r.count).classes("cb-gal-pk-n")
        if on and not display.on_source(r.recon, shows):
            ui.icon("warning", size="12px").classes("cb-gal-pk-warn").tooltip(
                f"{verb} on {r.recon_label or r.recon}, a different reconstruction from the one this version "
                "shows: another alignment puts the specimen elsewhere in the volume, so it can sit off what it marks."
            )
    if r.on_color is None:
        marker.classes(add="fixed")
        return
    marker.props["title"] = "Pick this layer's colour"
    palette = palette_row(r.on_color, mark)
    marker.on("click.stop", lambda _e: palette.set_visibility(not palette.visible))


def layers_tool(
    project_path,
    rows_of: Callable[[Callable[[], None]], tuple[list[LayerRow], list[LayerRow]]],
    *,
    shows: Callable[[], str | None],
    on_change: Callable[[], None],
    align: str = "left",
) -> Callable[[], None]:
    """A layers glyph whose dropdown holds `render_layer_list` for `rows_of(changed)` (picks, features),
    read against `shows()`; `changed` is what a row's colour pick calls. Returns the list's repaint; a
    change repaints it, then runs `on_change`."""
    trigger, _ = tool_trigger("layers.svg")
    _menu, body = hover_menu(trigger, align=align)

    def _changed() -> None:
        repaint()
        on_change()

    def repaint() -> None:
        if body.is_deleted:
            return
        body.clear()
        picks, features = rows_of(_changed)
        with body:
            render_layer_list(project_path, picks, features, shows=shows(), on_change=_changed)

    repaint()
    return repaint
