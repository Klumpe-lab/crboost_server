"""
Shared visual language for job pages and the panels built like them.

The header ladder (Rule): `section_head` / `section` / `config_section` / `panel_head` for a section, its title in
mono caps with a hairline to the edge; `group_title` for a group inside one. The field geometry: a 132 px label
column in the slot voice, the box at S / M / L (`box_style`), a 30 px unit column; an on/off parameter is a yes | no
row. All structural styling is inline (no dependency on a stylesheet arriving).
"""

import re
from contextlib import contextmanager
from html import escape
from enum import Enum
from collections.abc import Callable
from typing import get_args

from nicegui import ui

from services.job_params import check_value, field_bounds
from ui.components.svg_icon import render_glyph

# ── Tokens ──────────────────────────────────────────────────────────────────
MONO = "font-family: 'IBM Plex Mono', monospace;"
SANS = "font-family: 'IBM Plex Sans', sans-serif;"

CLR_LABEL = "#64748b"  # slate-500
CLR_VALUE = "#1e293b"  # slate-800
CLR_HEADER = "#475569"  # slate-600
CLR_SUBLABEL = "#94a3b8"  # slate-400
CLR_BORDER = "#e2e8f0"  # slate-200

# The label column of label/value rows outside the job form (the agent's plan page, the napari dialog); a look may
# set it (`--cb-label-w`). The job form's own is FIELD_LABEL_W.
LABEL_W = 110


def voice(kind: str, *, font: str, size: str, weight: int | str, ink: str, case: str = "none", track: str = "0") -> str:
    """A voice's type as CSS variables (`--cb-<kind>-font/size/weight/case/track/ink`, ui/dashboard/css.py
    `_THEME_CSS`), today's look in the fallbacks: a look sets the variables and every label, control, badge or
    column head in that voice changes at once (roadmap 51 §6)."""
    return (
        f"font-family: var(--cb-{kind}-font, {font}); font-size: var(--cb-{kind}-size, {size}); "
        f"font-weight: var(--cb-{kind}-weight, {weight}); text-transform: var(--cb-{kind}-case, {case}); "
        f"letter-spacing: var(--cb-{kind}-track, {track}); color: var(--cb-{kind}-ink, {ink});"
    )


LABEL_VOICE = voice("label", font="'IBM Plex Sans', sans-serif", size="10px", weight=500, ink=CLR_LABEL)
LABEL_STYLE = (
    f"{LABEL_VOICE} line-height: 1.4; flex-shrink: 0; white-space: nowrap; width: var(--cb-label-w, {LABEL_W}px);"
)
# The job form's field geometry: a 132 px label column in the slot voice, then the box in one of three widths (S a
# number, M a choice or a short code such as 500:10 or 2x2x1, L to the column's edge for a path or a list), then a
# 30 px unit column after S and M, so their boxes line up down a group whatever the unit.
FIELD_LABEL_W = 132
UNIT_W = 30
S, M, L = "S", "M", "L"
BOX_WIDTH = {S: 64, M: 150}
FIELD_LABEL_STYLE = (
    f"{LABEL_VOICE} line-height: 1.4; flex-shrink: 0; width: {FIELD_LABEL_W}px; white-space: nowrap; "
    "overflow: hidden; text-overflow: ellipsis;"
)
FIELD_ROW_STYLE = "display: flex; align-items: center; gap: 8px; width: 100%; min-width: 0; min-height: 20px;"
UNIT_STYLE = (
    f"{SANS} font-size: 9px; color: var(--cb-unit-ink, {CLR_SUBLABEL}); width: {UNIT_W}px; flex-shrink: 0; "
    "white-space: nowrap;"
)
# Where a note under a field row starts: under the box, past the label column and the gap.
NOTE_INDENT = FIELD_LABEL_W + 8


def box_style(width: str) -> str:
    """The box's place in a field row for a width class."""
    if width in BOX_WIDTH:
        return f"flex: 0 0 {BOX_WIDTH[width]}px; width: {BOX_WIDTH[width]}px; min-width: 0;"
    return "flex: 1 1 0; min-width: 0;"


# A read-only value in the box's place, drawn as the box (a frozen job's path).
FROZEN_BOX_STYLE = (
    "height: 20px; box-sizing: border-box; display: flex; align-items: center; padding: 0 5px; "
    "border: 1px solid var(--cb-field-line, #d3dde8); border-radius: var(--cb-field-radius, 2px); background: #f8fafc;"
)

VALUE_WRAP_STYLE = f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 1 1 0; min-width: 0;"
# A fixed width, not max-width: a width-less q-input in a flex row shrink-wraps to a couple of
# characters. 120 px holds ~16 monospace digits.
VALUE_WRAP_NARROW = f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 0 0 auto; width: 120px;"
HELPER_STYLE = f"{SANS} font-size: 9px; color: {CLR_SUBLABEL};"
SUFFIX_STYLE = f"{SANS} font-size: 9px; color: var(--cb-unit-ink, {CLR_SUBLABEL}); flex-shrink: 0;"


_DECIMALS = re.compile(r"(\d)\.(\d+)")
# A trailing unit after the last number: letters, symbols, a slash (µm, Å/px, e⁻/Å², %).
_UNIT = re.compile(r"(\d(?:</span>)?\s?)([^\d\s<>&][^\d<>&]*)$")


def num_html(text: object) -> str:
    """A formatted value ("5.02 µm", "4.48 – 5.30 µm", "1.55 Å/px") as HTML with its decimals and its unit a step
    back (`.cb-dec`, `.cb-unit`, ui/dashboard/css.py), which only a look that sets `--cb-unit-ink` makes visible
    (roadmap 51 S5). The text is escaped; nothing else changes."""
    out = _DECIMALS.sub(r'\1<span class="cb-dec">.\2</span>', escape(str(text)))
    return _UNIT.sub(r'\1<span class="cb-unit">\2</span>', out)


ROW_STYLE = "display: flex; align-items: baseline; gap: 8px; width: 100%; min-width: 0; min-height: 19px; padding: 0;"

# Legacy alias for callers that reference it directly.
PATH_LABEL_W = LABEL_W

# Sub-section headers (rendered INSIDE a card's content area, e.g.
# "Per-Task Resources" / "Supervisor" inside SLURM). Mixed case at a slightly
# larger size, not uppercase + letter-spacing, so the visual hierarchy is
# bold-vs-regular rather than caps-vs-lowercase.
SECTION_HEADER_STYLE = (
    f"{SANS} font-size: 11px; font-weight: 600; color: #33506e; line-height: 1; margin: 8px 0 4px 0; display: block;"
)
SECTION_HEADER_FIRST_STYLE = SECTION_HEADER_STYLE.replace("margin: 8px 0 4px 0", "margin: 0 0 4px 0")

# Page-structure section title — the Overview's "Species info" / the workbench's
# "Templates", "Masks", "Source", "Viewer". One notch above SECTION_HEADER_STYLE
# (12 px vs 11 px), still mixed case; separation from the previous section is
# WHITESPACE in the page layout, never an underline rule.
PAGE_SECTION_STYLE = (
    f"{SANS} font-size: 12px; font-weight: 600; color: var(--cb-head-ink, #12324f); line-height: 1; display: block;"
)

# A page's large blocks outside the job page (a dialog's block, a Species tab's panel): a panel under its Rule head
# (`section_head`). Colours, radius, padding and lines are the style's CSS variables (`--cb-*`,
# ui/dashboard/css.py `_THEME_CSS`); each fallback is the default style's value. One box deep: the ring, fill and
# shadow are the section's (`--cb-section-*`), which a look makes transparent inside a card. The padding goes in as
# longhands: a `var()` padding shorthand loses its other longhands in Chromium once one of them is set.
PANEL_STYLE = (
    "background: var(--cb-section-fill, #ffffff); border-radius: var(--cb-radius, 5px); "
    "padding-top: var(--cb-pad-y, 9px); padding-right: var(--cb-pad-x, 11px); "
    "padding-bottom: calc(var(--cb-pad-y, 9px) + 2px); padding-left: var(--cb-pad-x, 11px); box-sizing: border-box; "
    "outline: 1px solid var(--cb-section-ring, #dbe3ec); outline-offset: -1px; "
    "box-shadow: var(--cb-section-shadow, 0 1px 2px rgba(16, 42, 67, 0.06));"
)
# A dialog's or a page's own title (User settings, napari): the page rank, above the Rule ladder.
PANEL_TITLE_STYLE = (
    f"{SANS} font-size: var(--cb-head-size, 12px); font-weight: 600; color: var(--cb-head-ink, #12324f); "
    "letter-spacing: var(--cb-head-track, 0); line-height: 1; display: block;"
)


# The title line on top of each main panel (the job roster, the job page, every view): plain white,
# a hairline under it. One style, so no panel's title line reads different from the others. It leads
# with the panel's glyph (`view_head_icon`); a view puts its own tools on it (ui/workspace_page.py
# `_view_frame`), which wrap inside their slot when the panel is narrow.
VIEW_HEAD_STYLE = (
    "flex: 0 0 auto; display: flex; align-items: center; gap: 4px; min-height: 30px; padding: 3px 6px 3px 12px; "
    "margin-bottom: 6px; background: var(--cb-view-head, #ffffff); "
    "box-shadow: 0 var(--cb-view-head-line-w, 1px) 0 var(--cb-view-head-line, #dbe3ec);"
)
# A panel's name, in the condensed cut of the house face (loaded in ui/dashboard/css.py) a size above
# the page's titles, so it reads apart from the tools beside it.
VIEW_TITLE_STYLE = (
    "font-family: 'IBM Plex Sans Condensed', 'IBM Plex Sans', sans-serif; font-size: 13px; font-weight: 600; "
    "color: var(--cb-view-head-ink, #12324f); letter-spacing: 0.01em; white-space: nowrap; overflow: hidden; "
    "text-overflow: ellipsis; min-width: 0;"
)
VIEW_ICON_COLOR = "#4a6584"


def view_head_icon(icon: str):
    """A panel's glyph at the start of its title line: its rail icon (a `static/icons` asset or a
    Material name)."""
    return render_glyph(icon, size=14, color=VIEW_ICON_COLOR).style("margin-right: 2px;")


# A sub-block inside a Config section (SLURM's "Per-Task Resources" / "Supervisor"): spacing only, no box.
GROUP_STYLE = "width: 100%; margin-top: 10px;"
GROUP_MUTED_STYLE = GROUP_STYLE

LABEL_PATH_STYLE = LABEL_STYLE  # path labels share the same column width


# ── Section heads: the Rule ladder ──────────────────────────────────────────
# A section's head is its title in mono caps with a hairline running on from it to the right edge, and the
# section's meta (what it is fixed at, its profile) at the far end; no fill, no corners. An accented section, one
# that asks to be looked at (a failed job's Log, Results), has a square pip and its title and rule in the accent.
# A group inside a section is a step quieter: sans semibold, no line. The hairline is an element of its own, so
# the look needs no stylesheet to arrive.
SECTION_TITLE_STYLE = (
    "font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 9.5px; font-weight: 600; "
    "text-transform: uppercase; letter-spacing: .09em; line-height: 14px; white-space: nowrap; flex-shrink: 0;"
)
SECTION_META_STYLE = (
    "font-family: var(--cb-mono, 'IBM Plex Mono', monospace); font-size: 8.5px; text-transform: uppercase; "
    "letter-spacing: .09em; color: var(--cb-ink-4, #94a3b8); white-space: nowrap; flex-shrink: 0;"
)
_HEAD_INK = "var(--cb-head-ink, #12324f)"
_ACCENT = "var(--cb-head-accent, #2f6fb3)"
SECTION_RULE_STYLE = "flex: 1 1 auto; min-width: 12px; height: 1px; background: var(--cb-head-rule, #d6e0ea);"
_ACCENT_RULE_STYLE = "flex: 1 1 auto; min-width: 12px; height: 1px; background: var(--cb-head-accent-soft, #a9c6e4);"
SECTION_HEAD_STYLE = "display: flex; align-items: center; gap: 8px; min-height: 18px; width: 100%; min-width: 0;"
# A section: its head over its content, one box deep (no ring, fill or padding of its own).
SECTION_STYLE = "gap: 7px; width: 100%; min-width: 0;"
GROUP_TITLE_STYLE = (
    "font-family: var(--cb-sans, 'IBM Plex Sans', sans-serif); font-size: 10.5px; font-weight: 600; "
    "color: var(--cb-ink-2, #1e293b); line-height: 14px; margin: 3px 0 2px; display: block;"
)


def section_head(
    title: str, *, right: str = "", accent: bool = False, tooltip: str | None = None, caret: str | None = None
) -> ui.element:
    """A section's head: title, hairline to the edge, `right` as meta at the far end. Returns the row: controls built
    `with` it land at its right end, after the meta. `caret` puts a fold's caret before the title (the row's `caret`
    attribute, for the fold to turn); `tooltip` goes on the title."""
    row = ui.element("div").classes("cb-shead").style(SECTION_HEAD_STYLE)
    with row:
        row.caret = None
        if caret is not None:
            row.caret = ui.label(caret).style(
                "font-size: 9px; color: var(--cb-ink-4, #94a3b8); width: 8px; flex-shrink: 0; line-height: 14px;"
            )
        if accent:
            ui.element("div").style(f"width: 6px; height: 6px; background: {_ACCENT}; flex-shrink: 0;")
        lbl = ui.label(title).style(f"{SECTION_TITLE_STYLE} color: {_ACCENT if accent else _HEAD_INK};")
        if tooltip:
            lbl.tooltip(tooltip)
        row.title_label = lbl
        ui.element("div").style(_ACCENT_RULE_STYLE if accent else SECTION_RULE_STYLE)
        if right:
            row.meta_label = ui.label(right).style(SECTION_META_STYLE)
    return row


@contextmanager
def section(title: str, *, right: str = "", accent: bool = False, tooltip: str | None = None):
    """A section: its head (`section_head`), then what the caller builds under it. Yields the section's column;
    its head is the column's `head`."""
    with ui.column().classes("w-full").style(SECTION_STYLE) as col:
        col.head = section_head(title, right=right, accent=accent, tooltip=tooltip)
        yield col


def group_title(text: str, *, tooltip: str | None = None) -> ui.label:
    """A group's title inside a section (Angles & offsets, Reference & mask): sans semibold, no line."""
    lbl = ui.label(text).style(GROUP_TITLE_STYLE)
    if tooltip:
        lbl.tooltip(tooltip)
    return lbl


# ── Containers ──────────────────────────────────────────────────────────────
@contextmanager
def panel_head(title: str, *, tooltip: str | None = None, right: str = "", accent: bool = False):
    """A section's head with controls on it: the Rule head, the controls the caller builds in the yielded row at its
    right end."""
    with section_head(title, right=right, accent=accent, tooltip=tooltip) as row:
        with ui.row().classes("items-center no-wrap").style("gap: 6px;") as controls:
            yield controls
    del row


@contextmanager
def config_section(title: str, *, tooltip: str | None = None, right: str = "", accent: bool = False):
    """One section of a page (a job page's Parameters, a dialog's block): its Rule head over its content."""
    with section(title, right=right, accent=accent, tooltip=tooltip) as el:
        with ui.column().classes("w-full").style("gap: 0;"):
            yield el


@contextmanager
def field_grid():
    """A group's field rows, one under the other at the field geometry (`FIELD_LABEL_W`, `box_style`)."""
    el = ui.element("div").style("display: flex; flex-direction: column; gap: 3px; width: 100%; min-width: 0;")
    with el:
        yield el


@contextmanager
def field_group(*, muted: bool = False):
    el = ui.element("div").style(GROUP_MUTED_STYLE if muted else GROUP_STYLE)
    with el:
        yield el


@contextmanager
def toggle_row():
    """A group's on/off rows: like any field row, the switch where the box would be."""
    el = ui.element("div").style("display: flex; flex-direction: column; gap: 3px; width: 100%; min-width: 0;")
    with el:
        yield el


# ── Field cells ─────────────────────────────────────────────────────────────
_INPUT_PROPS_BASE = "dense borderless hide-bottom-space"
# Style applied to the inner <input> via Quasar's input-style prop.
_INPUT_INNER_STYLE = (
    "font-family: 'IBM Plex Mono', monospace; font-size: 10.5px; color: #1e293b; padding: 1px 2px; min-height: 0;"
)
_INPUT_INNER_FROZEN = _INPUT_INNER_STYLE.replace("color: #1e293b", "color: #94a3b8")


def _attach_input(inp, *, is_frozen: bool, width: str):
    # .cb-select (ui/dashboard/css.py) draws the same 1px box the enum selects have, so an
    # input's extent is visible and a number can't look cut off or rounded when it isn't.
    inp.classes("cb-select")
    inp.props(_INPUT_PROPS_BASE)
    inp.props(f'input-style="{_INPUT_INNER_FROZEN if is_frozen else _INPUT_INNER_STYLE}"')
    inp.style(f"{MONO} font-size: 10.5px; color: {CLR_VALUE}; {box_style(width)}")
    if is_frozen:
        inp.props("readonly")


def _select_chrome(sel, width: str) -> None:
    sel.props(_INPUT_PROPS_BASE)
    sel.props('popup-content-class="cb-select-popup"')
    sel.classes("cb-select")
    sel.style(box_style(width))


def box_input(inp, width: str, *, frozen: bool = False):
    """An input or number no job model binds, drawn as a field row's box at `width` (S / M / L)."""
    _attach_input(inp, is_frozen=frozen, width=width)
    return inp


def box_select(sel, width: str):
    """A select no job model binds, drawn as a field row's box at `width` (S / M / L)."""
    _select_chrome(sel, width)
    return sel


def _label(text: str, hint: str | None, **_legacy):
    lbl = ui.label(text).style(FIELD_LABEL_STYLE)
    if hint:
        lbl.tooltip(hint)
    return lbl


def _unit(width: str, unit: str) -> None:
    """The unit column after an S or M box; empty when the field has none, so the boxes line up."""
    if width in BOX_WIDTH:
        ui.label(unit).style(UNIT_STYLE)


def text_field(
    label: str,
    job_model,
    attr: str,
    *,
    is_frozen: bool,
    save_handler: Callable,
    hint: str | None = None,
    suffix: str = "",
    narrow: bool = False,
    on_change: bool = True,
    width: str | None = None,
):
    """A text field's row; `width` S / M / L (M by default, S when `narrow`), `suffix` its unit."""
    width = width or (S if narrow else M)
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        inp = ui.input().bind_value(job_model, attr)
        _attach_input(inp, is_frozen=is_frozen, width=width)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
        _unit(width, suffix)
    return inp


def _numeric_kind(job_model, attr: str) -> tuple[bool, bool]:
    """(is_int, allows_none) for a model field, looking through `X | None`.

    `bool` is a subclass of `int` but never reaches here (toggle_field owns it), so the
    check is identity, not issubclass — keep it that way."""
    field = type(job_model).model_fields.get(attr)
    annotation = getattr(field, "annotation", None)
    args = get_args(annotation)
    allows_none = type(None) in args
    concrete = [a for a in args if a is not type(None)] if args else [annotation]
    return len(concrete) == 1 and concrete[0] is int, allows_none


def _numeric_bounds(job_model, attr: str) -> tuple[float | None, float | None]:
    """The widget's (min, max): the field's declared ge/le (`job_params.field_bounds`). An open bound
    (gt/lt) has no representable widget minimum; `check_value` still enforces it."""
    bounds = field_bounds(job_model, attr)
    return bounds.get("ge"), bounds.get("le")


def numeric_forward(job_model, attr: str) -> Callable:
    """element → model transform for `numeric_field`.

    `ui.number` always hands back a JS float (or None when the box is cleared) and
    `AbstractJobParams` does not validate on assignment, so without this an `int` field
    silently accepts e.g. `0.05`, `model_dump()` writes it to project_params.json, and the
    next load fails validation, dropping the whole job instance (the driver then reports
    `FATAL: Instance '<id>' not found`). Coerce at the point of entry instead of
    discovering it a restart later.

    Out-of-range values are refused the same way garbage is: an int field declaring `ge=1`
    that accepts a 0 writes a value pydantic would reject on the next load and that the
    tool downstream rejects harder — `max_num_particles: 0` reaches PyTOM as `-n 0`, which
    dies with "must be larger than 0" on a compute node. Refusing (rather than clamping)
    is what keeps mid-typing safe: the prefixes of "1500" that fall below a bound leave
    the last good value in place until a whole in-range number is typed. The type and
    bounds are the setting door's (`job_params.check_value`), the ones the API is held to.
    """
    is_int, allows_none = _numeric_kind(job_model, attr)

    def _forward(value):
        if value is None or value == "":
            # An empty box is not a value. Clearing an optional field means None; on a
            # required one it means "still typing" — keep the last good value rather than
            # writing a None that only fails at load time.
            return None if allows_none else getattr(job_model, attr)
        try:
            number = float(value)
        except (TypeError, ValueError):
            return getattr(job_model, attr)  # mid-typing garbage; the next valid value lands
        number = round(number) if is_int else number  # round() with no ndigits returns an int
        checked, why = check_value(job_model, attr, number)
        return getattr(job_model, attr) if why else checked

    return _forward


def bind_numeric(inp, job_model, attr: str):
    """Two-way bind a `ui.number` to a numeric model field without fighting the person typing.

    NiceGUI re-asserts a plain attribute's value onto its bound element every refresh tick. Each
    time `numeric_forward` refuses a partial entry -- "1" on the way to "11.06" in a field with
    ge=2, or the empty value a browser reports for "11." -- the model keeps its old value and the
    next tick writes that back into the box mid-keystroke, eating the dot or replacing the digits.
    While the box has focus, the element's own value is reported back instead, so nothing is
    written into it. On blur, ui.number clamps to min/max, the valid value lands, and anything
    still invalid (a cleared required field) snaps back to the model's value."""
    editing = False

    def _set_editing(flag: bool) -> None:
        nonlocal editing
        editing = flag

    inp.on("focus", lambda _e: _set_editing(True))
    inp.on("blur", lambda _e: _set_editing(False))
    return inp.bind_value(
        job_model, attr, forward=numeric_forward(job_model, attr), backward=lambda v: inp.value if editing else v
    )


def numeric_field(
    label: str,
    job_model,
    attr: str,
    *,
    is_frozen: bool,
    save_handler: Callable,
    hint: str | None = None,
    suffix: str = "",
    fmt: str | None = None,
    narrow: bool = True,
    on_change: bool = True,
    width: str | None = None,
):
    """A number's row; `width` S by default, `suffix` its unit."""
    width = width or (S if narrow else M)
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        val = getattr(job_model, attr)
        is_int, _ = _numeric_kind(job_model, attr)
        # precision=0 makes the widget itself snap to whole numbers on blur, so an integer
        # field LOOKS like one; numeric_forward is what guarantees the model never holds
        # a fractional value, blur or not.
        lo, hi = _numeric_bounds(job_model, attr)
        # min/max are the widget's own affordance (spinner stops, browser validation);
        # numeric_forward is what guarantees the model never takes an out-of-range value.
        # No display format by default: ui.number re-applies it on blur, and "%.4g" turned 12345 into 12340
        # and 11.065 into 11.06.
        inp = ui.number(value=val, format=fmt, precision=0 if is_int else None, min=lo, max=hi)
        bind_numeric(inp, job_model, attr)
        _attach_input(inp, is_frozen=is_frozen, width=width)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
        _unit(width, suffix)
    return inp


def enum_forward(job_model, attr: str, enum_type) -> Callable:
    """element → model transform for `enum_field`, the enum sibling of `numeric_forward`.

    `ui.select` hands back the raw option string and `AbstractJobParams` does not validate
    on assignment, so a plain binding leaves e.g. the str `"IsoNet"` where a `DenoiseMethod`
    member belongs. Comparisons still pass (these are str-Enums), which is why it hides —
    but `isinstance(value, DenoiseMethod)` does not, and denoise-predict's
    `inherited_from_train` uses that to decide whether it can read the trained method;
    with a raw str it says no and predict silently falls back to its cryoCARE default.
    """

    def _forward(value):
        checked, why = check_value(job_model, attr, value)
        if why:
            return getattr(job_model, attr)  # not a value of the field; keep the last good one
        try:
            return enum_type(checked)
        except ValueError:
            return getattr(job_model, attr)  # not a member; keep the last good value

    return _forward


def enum_field(
    label: str,
    job_model,
    attr: str,
    enum_type,
    *,
    is_frozen: bool,
    save_handler: Callable,
    hint: str | None = None,
    width: str = M,
    unit: str = "",
):
    """A static Enum's choice row; `width` M by default."""
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        options = [e.value for e in enum_type]
        sel = ui.select(options=options, value=getattr(job_model, attr)).bind_value(
            job_model, attr, forward=enum_forward(job_model, attr, enum_type)
        )
        # `.cb-select` (themed in ui/dashboard/css.py) gives a clean 1px-bordered box and
        # a themed popup instead of the default Quasar Material underline/float.
        _select_chrome(sel, width)
        if is_frozen:
            sel.disable()
        else:
            sel.on_value_change(lambda _e: save_handler())
        _unit(width, unit)
    return sel


def choice_row(
    label: str,
    job_model,
    attr: str,
    options: dict[str, str],
    *,
    is_frozen: bool,
    save_handler: Callable,
    value: str | None = None,
    hint: str | None = None,
    placeholder: str = "none registered",
    empty_value: str = "",
    on_change: Callable[[str], None] | None = None,
    width: str = L,
):
    """A select over a RUNTIME options dict (value -> display label), in the same row
    chrome as every other field. `enum_field` covers the static-Enum case; this covers
    the case where the choices come from project state (a species' templates and masks,
    an annotated symmetry list).

    `value` is the entry to show when it differs from `getattr(job_model, attr)` — the
    caller owns the display resolution (e.g. "job override > species selection > first
    entry"), and showing a resolved default is NOT the same as writing it.

    An empty `options` renders the row disabled with `placeholder` as its only entry:
    absent is stated in the same shape as present, never silently preselected
    (CLAUDE.md "Surfacing uncertainty"). Writes go through an explicit change handler
    rather than `bind_value` so `empty_value` decides what "nothing chosen" means.
    `on_change`, when given, takes the chosen value instead of the write to `attr`.
    A shown value that is not among `options` shows `empty_value` if that is one of them,
    else an empty box (NiceGUI refuses a value outside the options). `width` L by default.
    """
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        if not options:
            sel = ui.select(options={"": placeholder}, value="")
        else:
            shown = value if value is not None else getattr(job_model, attr)
            if shown not in options:
                shown = empty_value if empty_value in options else None
            sel = ui.select(options=options, value=shown)
        _select_chrome(sel, width)
        if is_frozen or not options:
            sel.disable()
        else:

            def _on_change(e):
                if on_change is not None:
                    on_change(e.value or empty_value)
                    return
                setattr(job_model, attr, e.value or empty_value)
                save_handler()

            sel.on_value_change(_on_change)
        _unit(width, "")
    return sel


def multi_choice_row(
    label: str,
    job_model,
    attr: str,
    options: dict[str, str],
    *,
    is_frozen: bool,
    save_handler: Callable,
    hint: str | None = None,
    placeholder: str = "none available",
    on_change: Callable[[list[str]], None] | None = None,
    width: str = L,
):
    """`choice_row` for a list field: any number of the options, shown comma-separated in the
    box. The field's values must be among `options` (the caller adds any it holds that are not,
    labelled as such, so they stay visible and removable). Writes a new sorted list, so the model
    sees an assignment and marks itself dirty. `on_change`, when given, takes the chosen list
    instead of that write. `width` L by default."""
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        if not options:
            sel = ui.select(options={"": placeholder}, value="")
        else:
            current = [v for v in getattr(job_model, attr) or [] if v in options]
            sel = ui.select(options=options, value=current, multiple=True)
        _select_chrome(sel, width)
        if is_frozen or not options:
            sel.disable()
        else:

            def _on_change(e):
                if on_change is not None:
                    on_change(sorted(e.value or []))
                    return
                setattr(job_model, attr, sorted(e.value or []))
                save_handler()

            sel.on_value_change(_on_change)
    return sel


def toggle_field(label: str, job_model, attr: str, *, is_frozen: bool, save_handler: Callable, hint: str | None = None):
    """An on/off parameter's row: the label in the label column, a yes | no switch where a box would be."""
    from ui.components.check import yes_no

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        switch = yes_no(model=job_model, attr=attr, disabled=is_frozen, on_change=lambda _v: save_handler())
    return switch


def path_row(
    label: str,
    job_model,
    attr: str,
    *,
    is_frozen: bool,
    save_handler: Callable,
    hint: str | None = None,
    on_change: bool = True,
    project=None,
):
    """A path parameter's row, the box to the column's edge. A frozen job's path shows cut in its middle
    (`path_text`: a click copies it, hovering unrolls it); an editable one is a text box."""
    from ui.components.path_text import path_text

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        value = getattr(job_model, attr) or ""
        if is_frozen and value:
            with ui.element("div").style(f"{FROZEN_BOX_STYLE} {box_style(L)}"):
                return path_text(value, project)
        inp = ui.input().bind_value(job_model, attr)
        _attach_input(inp, is_frozen=is_frozen, width=L)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
    return inp


def kv_row(label: str, value: str, *, hint: str | None = None):
    """Read-only label/value pair (used by the I/O readonly view)."""
    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(label, hint)
        ui.label(value).style(
            f"{MONO} font-size: 10px; color: {CLR_HEADER}; flex: 1 1 0; "
            "min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        ).tooltip(value)


def is_enum_type(field_type) -> bool:
    return field_type is not None and isinstance(field_type, type) and issubclass(field_type, Enum)
