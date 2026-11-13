"""The large card's blocks between the input rows and the footer: the job's parameters in the job tab's
arrangement (read-only for a job in the project, the tab's own fields for the job being added), its results over
every tilt series, and the picked tilt series' Journey for this job. Each block is a list of `(height, draw)`
parts, so the card's height is known before it is drawn and the cables land on it."""

from __future__ import annotations

from collections.abc import Callable
from math import ceil

from nicegui import ui

from ui.branch_map.card import CardSpec
from ui.branch_map.facts import FactsStore
from ui.branch_map.geometry import LARGE_W, SIDE, Box, card_height
from ui.branch_map.model import FieldSpec, Job, Project
from ui.branch_map.plots import svg_plot, svg_tilt
from ui.branch_map.state import MapState
from ui.branch_map.style import HAIR, INK, MUTED, RED, RUN, SOFT, hrow, label, text, voice
from ui.job_plugins._field_styles import VALUE_WRAP_STYLE, _attach_input, num_html

Part = tuple[float, Callable[[], None]]
Section = tuple[str, list[Part]]
PIC_H = 104
PLOT_H = 56
TILE_H = 32
TILT_PLOT_H = 58
TILT_LABEL_H = 12
ROW_RO = 17  # a read-only parameter row
ROW_ED = 22  # an editable parameter row, the job tab's chrome
SEC_PAD_T, SEC_LABEL_H, SEC_GAP, SEC_PAD_B = 6, 14, 4, 5
PLABEL_W = 100  # the parameter label column on the card (the tab's is 110)
LINE_H = 13


def _h(content: str) -> None:
    label(
        content, 8, MUTED, case="none", track="0.06em", extra=f"line-height: {SEC_LABEL_H}px; height: {SEC_LABEL_H}px;"
    )


def _cap(content: str) -> None:
    text(content, 9, SOFT, extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;")


def _tiles(rows: list[tuple[str, str]]) -> None:
    """The Journey's stat tiles (`.cb-stat`, ui/dashboard/css.py): a 9 px label over an 11 px mono value."""
    with (
        ui.element("div")
        .classes("cb-stats")
        .style(f"gap: 20px; margin: 0; height: {TILE_H}px; overflow: hidden; flex-wrap: nowrap; line-height: 1.3;")
    ):
        for lbl, value in rows:
            with ui.element("div").classes("cb-stat").style("flex-shrink: 0;"):
                ui.label(lbl).classes("cb-stat-label")
                ui.html(num_html(value), sanitize=False).classes("cb-stat-value")


def section_height(parts: list[Part]) -> float:
    return SEC_PAD_T + SEC_LABEL_H + SEC_GAP + sum(h for h, _ in parts) + SEC_GAP * max(0, len(parts) - 1) + SEC_PAD_B


def draw_section(lbl: str, parts: list[Part]) -> None:
    with ui.element("div").style(
        f"padding: {SEC_PAD_T}px 0 {SEC_PAD_B}px; border-top: 1px solid {HAIR}; display: flex; flex-direction: column; "
        f"gap: {SEC_GAP}px; flex-shrink: 0;"
    ):
        _h(lbl)
        for h, draw in parts:
            with ui.element("div").style(f"height: {h}px; flex-shrink: 0; line-height: 0; overflow: hidden;"):
                draw()


def _pictures(pics: list[tuple[str, str]], *, arrow: bool, h: int) -> None:
    """Thumbnails in a row, each named on hover."""
    with hrow(8 if arrow else 6):
        for i, (url, tip) in enumerate(pics):
            if arrow and i:
                text("→", 12, MUTED)
            img = ui.html(
                f'<img src="{url}" style="height: {h}px; width: auto; max-width: 100%; '
                'border-radius: var(--cb-img-radius, 5px); display: block;">',
                sanitize=False,
            ).style("line-height: 0; flex-shrink: 1; min-width: 0;")
            img.tooltip(tip)


# ── parameters ───────────────────────────────────────────────────────────────


def _param_groups(fields: tuple[FieldSpec, ...]) -> tuple[list[FieldSpec], list[FieldSpec], list[FieldSpec]]:
    """The job tab's arrangement (default_renderer.render_default_params): path fields in full-width rows, then
    numeric, enum and text fields in one grid, then the toggles in a row of their own."""
    paths = [f for f in fields if f.group == "paths"]
    grid = (
        [f for f in fields if f.group == "numeric"]
        + [f for f in fields if f.group == "enum"]
        + [f for f in fields if f.group == "text"]
    )
    toggles = [f for f in fields if f.group == "toggle"]
    return paths, grid, toggles


def _value_words(v) -> str:
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, float):
        return f"{v:g}"
    if hasattr(v, "value"):
        return str(v.value)
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v) if v else "—"
    return "—" if v is None or v == "" else str(v)


def _ro_field(fs: FieldSpec, value, *, label_w: int) -> None:
    with hrow(8, f"height: {ROW_RO}px;"):
        lbl = label(
            fs.label,
            9,
            SOFT,
            weight=400,
            case="none",
            track="0",
            extra=f"width: {label_w}px; flex-shrink: 0; "
            "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;",
        )
        if fs.hint:
            lbl.tooltip(fs.hint)
        text(
            _value_words(value),
            10,
            INK,
            mono=True,
            extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0;",
        )


def _ed_field(fs: FieldSpec, value, on_change, *, label_w: int, box_w: int) -> None:
    """One field in the job tab's own chrome: the 10 px label, the `.cb-select` box with mono text."""
    with ui.element("div").style(f"display: flex; align-items: center; gap: 8px; height: {ROW_ED}px; min-width: 0;"):
        lbl = ui.label(fs.label).style(
            f"{voice('label', 10, '#64748b', weight=500)} line-height: 1.4; flex-shrink: 0; white-space: nowrap; "
            f"width: {label_w}px; overflow: hidden; text-overflow: ellipsis;"
        )
        if fs.hint:
            lbl.tooltip(fs.hint)
        if fs.kind == "number":
            inp = ui.number(value=value, on_change=on_change)
        elif fs.kind == "select":
            opts = list(fs.options)
            shown = _value_words(value)
            inp = ui.select(opts, value=shown if shown in opts else opts[0], on_change=on_change)
            inp.props('popup-content-class="cb-select-popup"')
        else:
            inp = ui.input(value="" if value is None else str(value), on_change=on_change)
        _attach_input(inp, is_frozen=False, narrow=False)
        # A fixed width: a width-less q-field in a grid cell grows past the cell (and the card's edge).
        inp.style(VALUE_WRAP_STYLE + f" flex: 0 0 auto; width: {box_w}px; max-width: {box_w}px;")


def params_parts(
    fields: tuple[FieldSpec, ...], values: dict, inner_w: int, *, editable: bool, on_change=None
) -> list[Part]:
    """The parameter block laid out as the job tab lays it out: paths full width, the rest in two columns, toggles
    in a wrapping row. Read-only for a job in the project, the tab's own fields for the job being added."""
    paths, grid, toggles = _param_groups(fields)
    row_h = ROW_ED if editable else ROW_RO
    col_w = (inner_w - 14) / 2
    label_w = PLABEL_W if editable else 96
    parts: list[Part] = []

    def one(fs: FieldSpec, lw: int) -> None:
        v = values.get(fs.name, fs.default)
        if editable:
            box_w = int((inner_w if fs.group == "paths" else col_w) - lw - 10)
            _ed_field(
                fs, v, (lambda e, k=fs.name: on_change(k, e.value)) if on_change else None, label_w=lw, box_w=box_w
            )
        else:
            _ro_field(fs, v, label_w=lw)

    def paths_block() -> None:
        with ui.element("div").style("display: flex; flex-direction: column;"):
            for fs in paths:
                one(fs, label_w)

    def grid_block() -> None:
        with ui.element("div").style(f"display: grid; grid-template-columns: {col_w}px {col_w}px; column-gap: 14px;"):
            for fs in grid:
                one(fs, label_w)

    def toggles_block() -> None:
        with ui.element("div").style(f"display: grid; grid-template-columns: {col_w}px {col_w}px; column-gap: 14px;"):
            for fs in toggles:
                one(fs, label_w)

    if paths:
        parts.append((row_h * len(paths), paths_block))
    if grid:
        parts.append((row_h * ceil(len(grid) / 2), grid_block))
    if toggles:
        parts.append((row_h * ceil(len(toggles) / 2), toggles_block))
    if not fields:
        parts.append((LINE_H, lambda: _cap("no parameters of its own")))
    return parts


# ── results over every tilt series ───────────────────────────────────────────


def results_parts(pr: Project, j: Job, inner_w: int, store: FactsStore) -> list[Part]:
    """What the job made over every tilt series: headline tiles, the pictures or plot with a caption, the tally,
    what else the job has to say. The magazine stays above the card, whatever its size. Until the facts are read
    the block says so."""
    parts: list[Part] = []
    f = store.job_facts(j)
    if f is None:
        parts.append((LINE_H, lambda: _cap("reading the results…")))
    else:
        if f.tiles:
            parts.append((TILE_H, lambda: _tiles(f.tiles)))
        if f.pictures:
            parts.append((PIC_H, lambda: _pictures(f.pictures, arrow=f.arrow, h=PIC_H)))
            if f.picture_caption:
                parts.append((LINE_H, lambda: _cap(f.picture_caption)))
        if f.plot is not None and f.plot.values:
            parts.append((PLOT_H, lambda: ui.html(svg_plot(f.plot, inner_w, PLOT_H), sanitize=False)))
            if f.plot.caption:
                parts.append((LINE_H, lambda: _cap(f.plot.caption)))
        for line in f.lines:
            parts.append((LINE_H, lambda line=line: _cap(line)))
    parts.append((LINE_H, lambda: text(pr.tally_words(j), 9, RED if j.status == "failed" else SOFT)))
    return parts


# ── one tilt series ──────────────────────────────────────────────────────────


def ts_parts(pr: Project, j: Job, ts: str, inner_w: int, store: FactsStore) -> list[Part]:
    """The Journey's section of this job for the picked tilt series: the stat tiles, the pictures of that tomogram,
    the per-tilt plots two to a row, the lines."""
    state = pr.ts_state(j, ts)
    parts: list[Part] = []
    if state in ("none", "out"):
        return [(LINE_H, lambda: _cap(f"{pr.ts_title(ts)} is not in this job"))]
    if state == "pending":
        return [(LINE_H, lambda: _cap(f"{pr.ts_title(ts)} is still waiting in this job"))]
    if state == "fail":
        parts.append((LINE_H, lambda: text(f"{pr.ts_title(ts)} failed in this job", 9, RED)))
    if state == "running":
        parts.append((LINE_H, lambda: text(f"{pr.ts_title(ts)} is on the GPU now", 9, RUN)))
    known, f = store.ts_facts(j, ts)
    if not known:
        parts.append((LINE_H, lambda: _cap("reading this tilt series…")))
        return parts
    if f is None:
        parts.append((LINE_H, lambda: _cap("nothing recorded for this tilt series")))
        return parts
    if f.tiles:
        parts.append((TILE_H, lambda: _tiles(f.tiles)))
    if f.pictures:
        parts.append((PIC_H, lambda: _pictures(f.pictures, arrow=f.arrow, h=PIC_H)))
    if f.plots:
        col_w = int((inner_w - 10) / 2)
        rows = ceil(len(f.plots) / 2)

        def plots() -> None:
            with ui.element("div").style(f"display: grid; grid-template-columns: {col_w}px {col_w}px; gap: 4px 10px;"):
                for tp in f.plots:
                    with ui.element("div").style("display: flex; flex-direction: column;"):
                        ui.label(tp.label).classes("cb-plot-label").style(
                            f"line-height: {TILT_LABEL_H}px; height: {TILT_LABEL_H}px; padding: 0; "
                            "white-space: nowrap; cursor: default;"
                        )
                        ui.html(svg_tilt(tp, col_w, TILT_PLOT_H), sanitize=False).style("line-height: 0;")

        parts.append((rows * (TILT_LABEL_H + TILT_PLOT_H) + (rows - 1) * 4, plots))
    for line in f.lines:
        parts.append((LINE_H, lambda line=line: _cap(line)))
    return parts


def middle_sections(
    pr: Project,
    st: MapState,
    spec: CardSpec,
    inner_w: int,
    store: FactsStore,
    *,
    on_param=None,
    new_defaults: dict | None = None,
) -> list[Section]:
    """A large card's middle: the parameters, the results, the picked tilt series: each a labelled block whose
    height is known before it is drawn. `new_defaults`: the species' values the job being added will take, shown
    in its form where nothing was typed."""
    secs: list[Section] = []
    if spec.ghost:
        fields = pr.param_fields.get(spec.kind, ())
        values = {**(new_defaults or {}), **st.params}
        secs.append(("PARAMETERS", params_parts(fields, values, inner_w, editable=True, on_change=on_param)))
        return secs
    j = spec.job
    assert j is not None
    fields = pr.param_fields.get(j.kind, ())
    when = "not run yet" if j.status in ("scheduled", "queued") else "as run"
    secs.append((f"PARAMETERS · {when}", params_parts(fields, j.param_values, inner_w, editable=False)))
    if j.status in ("done", "failed", "running"):
        secs.append(("RESULTS · every tilt series", results_parts(pr, j, inner_w, store)))
        if st.active_ts:
            secs.append((f"TILT SERIES · {pr.ts_title(st.active_ts)}", ts_parts(pr, j, st.active_ts, inner_w, store)))
    return secs


def middle_height(secs: list[Section]) -> float:
    return sum(section_height(parts) for _, parts in secs)


def draw_middle(secs: list[Section]) -> None:
    for lbl, parts in secs:
        draw_section(lbl, parts)


def box_of(
    pr: Project,
    spec: CardSpec,
    st: MapState,
    store: FactsStore,
    *,
    large: bool,
    on_param=None,
    new_defaults: dict | None = None,
) -> tuple[Box, list[Section]]:
    """A card's measurements before it is drawn, and the sections of its middle block when large."""
    from ui.branch_map.card import strip_band

    secs = (
        middle_sections(pr, st, spec, LARGE_W - 2 * SIDE, store, on_param=on_param, new_defaults=new_defaults)
        if large
        else []
    )
    extra = middle_height(secs)
    band = 0 if spec.job is None else strip_band(pr, st)
    return Box(LARGE_W if large else 236, card_height(len(spec.sockets), extra), extra, band), secs
