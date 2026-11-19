"""The form of the job being added, on its card (ui/branch_map/card.py): its parameters as the job tab arranges
them, in the tab's own fields, with the species' values where nothing was typed. Each block is a list of
`(height, draw)` parts under a labelled section."""

from __future__ import annotations

from collections.abc import Callable
from math import ceil

from nicegui import ui

from ui.branch_map.geometry import LARGE_W, SIDE
from ui.branch_map.model import FieldSpec, Project
from ui.branch_map.state import MapState
from ui.branch_map.style import HAIR, INK, MUTED, SOFT, hrow, label, text, voice
from ui.job_plugins._field_styles import VALUE_WRAP_STYLE, _attach_input

Part = tuple[float, Callable[[], None]]
Section = tuple[str, list[Part]]
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
            lbl.props["title"] = fs.hint
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
            lbl.props["title"] = fs.hint
        if fs.kind == "number":
            inp = ui.number(value=value, on_change=on_change)
        elif fs.kind == "select":
            opts = list(fs.options)
            shown = _value_words(value)
            inp = ui.select(opts, value=shown if shown in opts else opts[0], on_change=on_change)
            inp.props('popup-content-class="cb-select-popup"')
        else:
            inp = ui.input(value="" if value is None else str(value), on_change=on_change)
        _attach_input(inp, is_frozen=False, width="M")
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


def new_sections(pr: Project, st: MapState, *, on_param, new_defaults: dict | None) -> list[Section]:
    """The job being added: its parameters, editable; the species' values where nothing was typed."""
    fields = pr.param_fields.get(st.adding or "", ())
    values = {**(new_defaults or {}), **st.params}
    return [("PARAMETERS", params_parts(fields, values, LARGE_W - 2 * SIDE, editable=True, on_change=on_param))]
