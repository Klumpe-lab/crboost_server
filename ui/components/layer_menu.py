"""The layers list, as every view's layers menu holds it: the Tomograms wall and its zoom, the
Journey's Reconstruct card and the pick viewer.

Two kinds of annotation share one list and one logic, in one table so that every row's cells line up:
the pick layers, by species, and the segmentations. A kind's head has an eye that shows or hides all of
its layers. Each row is an eye, the layer's key in its colour (a click on it opens the colours, where
the view lets it be recoloured), its name and a count; a view that can switch its picture adds the
reconstruction each layer was made on, a view with a gallery a button that shows a pick list in it.
Every layer follows the version (`services/segmentation/display.py`): it shows over a picture of the
reconstruction it was made on and is hidden over another; a click on its row switches it by hand, and
"Follow the version" at the top puts every hand-switched layer back. A layer shown over another
reconstruction is flagged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

from nicegui import ui

from services.models_base import SPECIES_OVERLAY_COLORS
from services.segmentation import display
from ui.components.hover_menu import hover_menu, tool_trigger
from ui.components.segmented import render_segmented

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
    layer (None: its colour is not this list's to change); `on_gallery` shows a pick list in the view's
    gallery, `in_gallery` whether it is the one there."""

    id: str
    name: str
    count: str
    color: str
    recon: str | None
    recon_label: str = ""
    group: str = ""
    shape: str = "circle"
    on_color: Callable[[str], None] | None = None
    on_gallery: Callable[[], None] | None = None
    in_gallery: bool = False


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
    """The colours a layer can take, as a row of swatches; a click on one calls `on_pick` with it. The
    list shows it under the layer's row, hidden until a click on the layer's key: in place, because a
    menu of its own would close a hover menu the list sits in. `mark(color, size)` draws a colour as
    the layer's own key."""
    row = ui.element("div").classes("cb-gal-pk-palette")
    with row:
        for color in SPECIES_OVERLAY_COLORS:
            swatch = ui.html(mark(color, 14), sanitize=False).classes("cb-gal-pk-marker")
            swatch.on("click.stop", lambda _e, c=color: on_pick(c))
    return row


def shown(project_path, row: LayerRow, shows: str | None) -> bool:
    """Whether the layer draws over a picture of reconstruction `shows`."""
    return display.is_shown(project_path, row.id, display.on_source(row.recon, shows))


def render_layer_list(
    project_path,
    picks: list[LayerRow],
    features: list[LayerRow],
    *,
    shows: str | None,
    on_change: Callable[[], None],
    sources: dict[str, Callable[[], None]] | None = None,
) -> None:
    """The rows into the current slot, read against a picture of reconstruction `shows`: "Follow the
    version", the pick layers under their species, the segmentations and the fills' opacity, all in one
    table, so every row's cells sit in the same columns. `sources` holds the reconstructions the view can
    show, each with the call that shows it; given, each row names the reconstruction its layer was made
    on, and a click there shows it. `on_change` runs after any change, for the view to repaint the list
    and what it draws."""
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

    cols = _Columns(
        count=any(r.count for r in rows),
        source=sources is not None,
        gallery=any(r.on_gallery is not None for r in rows),
    )
    following = not any(display.is_pinned(project_path, r.id) for r in rows)
    with ui.element("table").classes("cb-lyr" + (" cb-lyr-wide" if cols.wide else "")):
        with ui.element("tbody"):
            follow = ui.element("tr").classes("cb-lyr-follow")
            follow.on("click", lambda _e: _follow())
            follow.props["title"] = _FOLLOW_TIP
            with follow:
                with _cell("cb-lyr-c-eye"):
                    ui.icon("check_box" if following else "check_box_outline_blank", size="14px")
                with _cell(span=cols.n - 1):
                    ui.label("Follow the version")
        if picks:
            _kind(
                project_path,
                "Particles",
                picks,
                shows,
                _set,
                sources,
                cols,
                shaped=True,
                verb="Picked",
                count_head="picks",
            )
        if features:
            _kind(
                project_path, "Segmentations", features, shows, _set, sources, cols, verb="Segmented", tip=FEATURES_TIP
            )
            with ui.element("tbody"), ui.element("tr").classes("cb-lyr-opacity"):
                _cell()
                with _cell(span=cols.n - 1), ui.element("div").classes("cb-lyr-opacity-in"):
                    ui.label("opacity")
                    render_segmented(list(OPACITIES), display.opacity(project_path), _opacity)


class _Columns(NamedTuple):
    """The columns after a row's eye, key and name, which every row of the list shares: a count, the
    reconstruction the layer was made on, the gallery button."""

    count: bool
    source: bool
    gallery: bool

    @property
    def n(self) -> int:
        return 3 + self.count + self.source + self.gallery

    @property
    def wide(self) -> bool:
        """A view that can switch its picture, or has a gallery: its columns get names."""
        return self.source or self.gallery


def _cell(cls: str = "", span: int = 1) -> ui.element:
    """A table cell into the current row, `span` columns wide."""
    td = ui.element("td")
    if cls:
        td.classes(cls)
    if span > 1:
        td.props["colspan"] = span
    return td


def _kind(
    project_path,
    title: str,
    rows: list[LayerRow],
    shows: str | None,
    set_shown: Callable[[list[LayerRow], bool], None],
    sources: dict[str, Callable[[], None]] | None,
    cols: _Columns,
    *,
    verb: str,
    shaped: bool = False,
    count_head: str = "",
    tip: str | None = None,
) -> None:
    """One kind's rows. Its head is an eye that shows or hides every layer of the kind (any shown: it
    hides them all) and the kind's name, with the names of the columns this kind fills; then a row per
    layer, under its group's name (a pick layer's species, a click on which switches all of the species'
    layers)."""
    has_count = any(r.count for r in rows)
    has_gallery = any(r.on_gallery is not None for r in rows)
    any_on = any(shown(project_path, r, shows) for r in rows)
    with ui.element("tbody"):
        with ui.element("tr").classes("cb-lyr-head"):
            with _cell("cb-lyr-c-eye"):
                eye = ui.icon("visibility" if any_on else "visibility_off", size="14px")
                eye.classes("cb-lyr-eye" + ("" if any_on else " off"))
                eye.props["title"] = f"{'Hide' if any_on else 'Show'} all {title.lower()}"
                eye.on("click", lambda _e: set_shown(rows, not any_on))
            with _cell(span=2):
                name = ui.label(title).classes("cb-lyr-title")
                if tip:
                    name.props["title"] = tip
            # A head cell per shared column, named where the list is wide and this kind fills it.
            for on, head, cls in (
                (cols.count, count_head if has_count else "", "cb-lyr-c-num"),
                (cols.source, "made on", "cb-lyr-c-src"),
                (cols.gallery, "gallery" if has_gallery else "", "cb-lyr-c-gal"),
            ):
                if on:
                    with _cell(cls):
                        if cols.wide and head:
                            ui.label(head)
        group = None
        for r in rows:
            if r.group and r.group != group:
                group = r.group
                members = [m for m in rows if m.group == group]
                group_row = ui.element("tr").classes("cb-lyr-group")
                group_row.props["title"] = "Show or hide all of this species' layers"
                # Any shown: hide them all; none shown: show them all.
                group_row.on(
                    "click", lambda _e, ms=members: set_shown(ms, not any(shown(project_path, m, shows) for m in ms))
                )
                with group_row:
                    _cell()
                    with _cell(span=cols.n - 1):
                        ui.label(group)
            mark = (lambda c, size=10, s=r.shape: shape_svg(s, c, size)) if shaped else swatch_svg
            _row(project_path, r, shows, mark, verb, set_shown, cols, sources=sources)


def _row(
    project_path,
    r: LayerRow,
    shows: str | None,
    mark: Callable[..., str],
    verb: str,
    set_shown,
    cols: _Columns,
    *,
    sources: dict[str, Callable[[], None]] | None,
) -> None:
    on = shown(project_path, r, shows)
    item = ui.element("tr").classes("cb-lyr-row" + ("" if on else " off"))
    item.on("click", lambda _e: set_shown([r], not on))
    with item:
        with _cell("cb-lyr-c-eye"):
            ui.icon("visibility" if on else "visibility_off", size="14px").classes("cb-lyr-eye")
        with _cell("cb-lyr-c-key"):
            marker = ui.html(mark(r.color, 10), sanitize=False).classes("cb-gal-pk-marker")
        with _cell(), ui.element("div").classes("cb-lyr-name"):
            ui.label(r.name).classes("cb-gal-pk-name")
            if on and not display.on_source(r.recon, shows):
                ui.icon("warning", size="12px").classes("cb-gal-pk-warn").tooltip(
                    f"{verb} on {r.recon_label or r.recon}, a different reconstruction from the one this version "
                    "shows: another alignment puts the specimen elsewhere in the volume, so it can sit off what it "
                    "marks."
                )
        if cols.count:
            with _cell("cb-lyr-c-num"):
                ui.label(r.count)
        if sources is not None:
            with _cell("cb-lyr-c-src"):
                _source_cell(r, shows, sources)
        if cols.gallery:
            with _cell("cb-lyr-c-gal"):
                _gallery_cell(r)
    if r.on_color is None:
        marker.classes(add="fixed")
        return
    marker.props["title"] = "Pick this layer's colour"
    colours = ui.element("tr").classes("cb-lyr-palette")
    colours.set_visibility(False)
    with colours:
        _cell()
        with _cell(span=cols.n - 1):
            palette_row(r.on_color, mark)
    marker.on("click.stop", lambda _e: colours.set_visibility(not colours.visible))


def _source_cell(r: LayerRow, shows: str | None, sources: dict[str, Callable[[], None]]) -> None:
    """The reconstruction a layer was made on: lit while the view shows it, else a click shows it."""
    if r.recon is None:
        cell = ui.label("any").classes("cb-lyr-src none")
        cell.props["title"] = "Not known which reconstruction this was made on, so it shows on every version"
        return
    name = r.recon_label or r.recon
    cell = ui.label(name).classes("cb-lyr-src")
    if r.recon == shows:
        cell.classes(add="here")
        cell.props["title"] = f"Made on {name}, the reconstruction shown"
        # Not the row's click, which switches the layer.
        cell.on("click", js_handler="(e) => e.stopPropagation()")
    elif r.recon in sources:
        go = sources[r.recon]
        cell.props["title"] = f"Made on {name}: show it"
        cell.on("click.stop", lambda _e: go())
    else:
        cell.classes(add="none")
        cell.props["title"] = f"Made on {name}, which has no version of this tomogram"


def _gallery_cell(r: LayerRow) -> None:
    """A pick list's way into the view's gallery, lit while its cutouts are there."""
    if r.on_gallery is None:
        return
    cell = ui.icon("grid_view", size="14px").classes("cb-lyr-gal" + (" here" if r.in_gallery else ""))
    if r.in_gallery:
        cell.props["title"] = "Its cutouts are in the gallery"
        # Not the row's click, which switches the layer.
        cell.on("click", js_handler="(e) => e.stopPropagation()")
        return
    cell.props["title"] = "Show its cutouts in the gallery"
    cell.on("click.stop", lambda _e, go=r.on_gallery: go())


def layers_tool(
    project_path,
    rows_of: Callable[[Callable[[], None]], tuple[list[LayerRow], list[LayerRow]]],
    *,
    shows: Callable[[], str | None],
    on_change: Callable[[], None],
    align: str = "left",
    text: str | None = None,
    boxed: bool = False,
    sources: dict[str, Callable[[], None]] | None = None,
) -> Callable[[], None]:
    """A layers glyph (named `text`, `boxed` as a field) whose dropdown holds `render_layer_list` for
    `rows_of(changed)` (picks, features), read against `shows()`, with `sources` (`render_layer_list`);
    `changed` is what a row's colour pick calls. Returns the list's repaint; a change repaints it, then
    runs `on_change`."""
    trigger, _ = tool_trigger("layers.svg", text, boxed=boxed)
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
            render_layer_list(project_path, picks, features, shows=shows(), on_change=_changed, sources=sources)

    repaint()
    return repaint
