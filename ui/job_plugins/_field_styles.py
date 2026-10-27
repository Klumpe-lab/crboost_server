"""
Shared visual language for job-page parameter renderers.

All structural styling is inline (no dependency on a CSS class file). This
keeps the layout robust even when static/main.css fails to refresh in the
browser. Each field renders as a single horizontal row -- label inline-left
of the input -- and rows stack vertically inside their collapsible.
"""

from contextlib import contextmanager
from enum import Enum
from collections.abc import Callable
from typing import get_args

from nicegui import ui

from ui.components.svg_icon import render_glyph

# ── Tokens ──────────────────────────────────────────────────────────────────
MONO = "font-family: 'IBM Plex Mono', monospace;"
SANS = "font-family: 'IBM Plex Sans', sans-serif;"

CLR_LABEL = "#64748b"  # slate-500
CLR_VALUE = "#1e293b"  # slate-800
CLR_HEADER = "#475569"  # slate-600
CLR_SUBLABEL = "#94a3b8"  # slate-400
CLR_BORDER = "#e2e8f0"  # slate-200

# Single label column width applied across Parameters / I/O / SLURM so
# labels line up vertically and the three sections read as one form.
LABEL_W = 110

LABEL_STYLE = (
    f"{SANS} font-size: 10px; font-weight: 500; color: {CLR_LABEL}; "
    "line-height: 1.4; flex-shrink: 0; white-space: nowrap; "
    f"width: {LABEL_W}px;"
)
VALUE_WRAP_STYLE = f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 1 1 0; min-width: 0;"
# A fixed width, not max-width: a width-less q-input in a flex row shrink-wraps to a couple of
# characters. 120 px holds ~16 monospace digits.
VALUE_WRAP_NARROW = f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 0 0 auto; width: 120px;"
HELPER_STYLE = f"{SANS} font-size: 9px; color: {CLR_SUBLABEL};"
SUFFIX_STYLE = f"{SANS} font-size: 9px; color: {CLR_SUBLABEL}; flex-shrink: 0;"

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

# A page's large blocks (a job page's sections): a panel and its title in the panel ink, as the
# Journey's sections. Fields keep their white boxes on it. Colours, radius, padding and lines are the
# style's CSS variables (`--cb-*`, ui/dashboard/css.py `_THEME_CSS`); each fallback is the default
# style's value. The outline is drawn over the content, so it costs no layout and the stripe can't
# cover it.
PANEL_STYLE = (
    "background: var(--cb-panel, #ffffff); border-radius: var(--cb-radius, 5px); "
    "padding: var(--cb-pad-y, 9px) var(--cb-pad-x, 11px) calc(var(--cb-pad-y, 9px) + 2px); box-sizing: border-box; "
    "outline: 1px solid var(--cb-panel-line, #dbe3ec); outline-offset: -1px; "
    "box-shadow: var(--cb-panel-shadow, 0 1px 2px rgba(16, 42, 67, 0.06));"
)
PANEL_TITLE_STYLE = (
    f"{SANS} font-size: var(--cb-head-size, 12px); font-weight: 600; color: var(--cb-head-ink, #12324f); "
    "letter-spacing: var(--cb-head-track, 0); line-height: 1; display: block;"
)
# A panel's title line as a stripe across its top, edge to edge: the margins undo PANEL_STYLE's padding, so
# it belongs on the panel's first child (the title label, or the row holding it). A style may rule it off
# below and mark its left end (--cb-head-line, --cb-accent).
PANEL_HEAD_STYLE = (
    "background: var(--cb-head, #edf4fb); "
    "margin: calc(-1 * var(--cb-pad-y, 9px)) calc(-1 * var(--cb-pad-x, 11px)) 0; "
    "padding: var(--cb-head-pad, 5px) var(--cb-pad-x, 11px); width: calc(100% + 2 * var(--cb-pad-x, 11px)); "
    "border-radius: var(--cb-radius, 5px) var(--cb-radius, 5px) 0 0; box-sizing: border-box; "
    "box-shadow: 0 1px 0 var(--cb-head-line, #c9dbee), "
    "inset var(--cb-accent-w, 2px) 0 0 var(--cb-accent, #4a8fd4), "
    "inset 0 1px 0 var(--cb-head-glint, rgba(255, 255, 255, 0.9));"
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


def fold_panel(panel, head, open_: bool) -> None:
    """A folding panel (a job page's Log and Files, the workbench's Activity log): folded, its title
    stripe is the whole panel."""
    panel.style(f"padding-bottom: {'calc(var(--cb-pad-y, 9px) + 2px)' if open_ else '0'};")
    head.style(
        f"border-radius: {'var(--cb-radius, 5px) var(--cb-radius, 5px) 0 0' if open_ else 'var(--cb-radius, 5px)'};"
    )


# A sub-block inside a Config section (SLURM's "Per-Task Resources" / "Supervisor"): spacing only, no box.
GROUP_STYLE = "width: 100%; margin-top: 10px;"
GROUP_MUTED_STYLE = GROUP_STYLE

LABEL_PATH_STYLE = LABEL_STYLE  # path labels share the same column width

# The one phrasing for the block of fields a job inherited from its species at
# creation (template / mask / symmetry / Ø / extraction geometry). They are still job
# parameters — snapshot-at-creation, nothing auto-propagates — so they are weakly
# separated inside the same Parameters card, never boxed in a nested card of their own.
SPECIES_SECTION_TITLE = "From species — override for this run"


# ── Section heads ───────────────────────────────────────────────────────────
def section_header(text: str, *, first: bool = False):
    style = SECTION_HEADER_FIRST_STYLE if first else SECTION_HEADER_STYLE
    ui.label(text).style(style)


# ── Containers ──────────────────────────────────────────────────────────────
@contextmanager
def panel_head(title: str, *, tooltip: str | None = None):
    """A panel's title stripe with controls on it: the title on the left, the controls the caller builds
    in the yielded row on the right, all centred on one line. The first child of a `PANEL_STYLE` panel."""
    with ui.row().classes("items-center no-wrap").style(f"gap: 8px; min-height: 30px; {PANEL_HEAD_STYLE}"):
        lbl = ui.label(title).style(PANEL_TITLE_STYLE)
        if tooltip:
            lbl.tooltip(tooltip)
        ui.space()
        with ui.row().classes("items-center no-wrap").style("gap: 6px;") as controls:
            yield controls


@contextmanager
def config_section(title: str, *, tooltip: str | None = None):
    """One section of a job's page (Parameters / Inputs / Outputs / SLURM resources): its
    title over its content on a pale gray-blue panel, the Journey's section look. No border;
    the page's column gap separates sections."""
    with ui.column().classes("w-full").style(f"gap: 8px; {PANEL_STYLE}") as el:
        lbl = ui.label(title).style(f"{PANEL_TITLE_STYLE} {PANEL_HEAD_STYLE}")
        if tooltip:
            lbl.tooltip(tooltip)
        with ui.column().classes("w-full").style("gap: 0;"):
            yield el


@contextmanager
def field_grid():
    """Auto-packing multi-column grid of field rows. Fields flow into as many
    ~300px columns as the width allows (typically 2-4), so short numeric/enum
    fields don't leave the right half of the panel empty and the section stays
    short. Path fields stay full-width in their own column (they are rendered
    outside field_grid); section headers sit above each grid."""
    el = ui.element("div").style(
        "display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); "
        "column-gap: 18px; row-gap: 2px; width: 100%; align-items: start;"
    )
    with el:
        yield el


@contextmanager
def field_group(*, muted: bool = False):
    el = ui.element("div").style(GROUP_MUTED_STYLE if muted else GROUP_STYLE)
    with el:
        yield el


@contextmanager
def toggle_row():
    el = ui.element("div").style(
        "display: flex; flex-wrap: wrap; column-gap: 18px; row-gap: 4px; "
        "width: 100%; align-items: center; margin-top: 6px;"
    )
    with el:
        yield el


# ── Field cells ─────────────────────────────────────────────────────────────
_INPUT_PROPS_BASE = "dense borderless hide-bottom-space"
# Style applied to the inner <input> via Quasar's input-style prop.
_INPUT_INNER_STYLE = (
    "font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: #1e293b; padding: 1px 2px; min-height: 0;"
)
_INPUT_INNER_FROZEN = _INPUT_INNER_STYLE.replace("color: #1e293b", "color: #94a3b8")


def _attach_input(inp, *, is_frozen: bool, narrow: bool):
    # .cb-select (ui/dashboard/css.py) draws the same 1px box the enum selects have, so an
    # input's extent is visible and a number can't look cut off or rounded when it isn't.
    inp.classes("cb-select")
    inp.props(_INPUT_PROPS_BASE)
    inp.props(f'input-style="{_INPUT_INNER_FROZEN if is_frozen else _INPUT_INNER_STYLE}"')
    inp.style(VALUE_WRAP_NARROW if narrow else VALUE_WRAP_STYLE)
    if is_frozen:
        inp.props("readonly")


def _label(text: str, hint: str | None, *, path_width: bool = False):
    style = LABEL_PATH_STYLE if path_width else LABEL_STYLE
    lbl = ui.label(text).style(style)
    if hint:
        lbl.tooltip(hint)
    return lbl


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
):
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint)
        inp = ui.input().bind_value(job_model, attr)
        _attach_input(inp, is_frozen=is_frozen, narrow=narrow)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
        if suffix:
            ui.label(suffix).style(SUFFIX_STYLE)
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
    """The field's declared (ge, le), or (None, None). Only ge/le are modelled — no field
    under services/jobs/ uses the open-interval gt/lt, and an open bound has no
    representable widget minimum anyway."""
    field = type(job_model).model_fields.get(attr)
    lo = hi = None
    for m in getattr(field, "metadata", None) or []:
        lo = getattr(m, "ge", lo) if getattr(m, "ge", None) is not None else lo
        hi = getattr(m, "le", hi) if getattr(m, "le", None) is not None else hi
    return lo, hi


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
    the last good value in place until a whole in-range number is typed.
    """
    is_int, allows_none = _numeric_kind(job_model, attr)
    lo, hi = _numeric_bounds(job_model, attr)

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
        if (lo is not None and number < lo) or (hi is not None and number > hi):
            return getattr(job_model, attr)
        return number

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
):
    with ui.element("div").style(ROW_STYLE):
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
        _attach_input(inp, is_frozen=is_frozen, narrow=narrow)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
        if suffix:
            ui.label(suffix).style(SUFFIX_STYLE)
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
        try:
            return enum_type(value)
        except ValueError:
            return getattr(job_model, attr)  # not a member; keep the last good value

    return _forward


def enum_field(
    label: str, job_model, attr: str, enum_type, *, is_frozen: bool, save_handler: Callable, hint: str | None = None
):
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint)
        options = [e.value for e in enum_type]
        sel = ui.select(options=options, value=getattr(job_model, attr)).bind_value(
            job_model, attr, forward=enum_forward(job_model, attr, enum_type)
        )
        # `.cb-select` (themed in ui/dashboard/css.py) gives a clean 1px-bordered box and
        # a themed popup instead of the default Quasar Material underline/float.
        sel.props(_INPUT_PROPS_BASE)
        sel.props('popup-content-class="cb-select-popup"')
        sel.classes("cb-select")
        sel.style("flex: 1 1 0; min-width: 0;")
        if is_frozen:
            sel.disable()
        else:
            sel.on_value_change(lambda _e: save_handler())
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
    """
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint)
        if not options:
            sel = ui.select(options={"": placeholder}, value="")
        else:
            shown = value if value is not None else getattr(job_model, attr)
            sel = ui.select(options=options, value=shown if shown in options else empty_value)
        sel.props(_INPUT_PROPS_BASE)
        sel.props('popup-content-class="cb-select-popup"')
        sel.classes("cb-select")
        sel.style("flex: 1 1 0; min-width: 0;")
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
):
    """`choice_row` for a list field: any number of the options, shown comma-separated in the
    box. The field's values must be among `options` (the caller adds any it holds that are not,
    labelled as such, so they stay visible and removable). Writes a new sorted list, so the model
    sees an assignment and marks itself dirty."""
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint)
        if not options:
            sel = ui.select(options={"": placeholder}, value="")
        else:
            current = [v for v in getattr(job_model, attr) or [] if v in options]
            sel = ui.select(options=options, value=current, multiple=True)
        sel.props(_INPUT_PROPS_BASE)
        sel.props('popup-content-class="cb-select-popup"')
        sel.classes("cb-select")
        sel.style("flex: 1 1 0; min-width: 0;")
        if is_frozen or not options:
            sel.disable()
        else:

            def _on_change(e):
                setattr(job_model, attr, sorted(e.value or []))
                save_handler()

            sel.on_value_change(_on_change)
    return sel


def toggle_field(label: str, job_model, attr: str, *, is_frozen: bool, save_handler: Callable, hint: str | None = None):
    cb = ui.checkbox(label).bind_value(job_model, attr).props("dense size=xs")
    cb.style(f"{SANS} font-size: 10px; color: {CLR_HEADER};")
    if hint:
        cb.tooltip(hint)
    if is_frozen:
        cb.disable()
    else:
        cb.on_value_change(lambda _e: save_handler())
    return cb


def path_row(
    label: str,
    job_model,
    attr: str,
    *,
    is_frozen: bool,
    save_handler: Callable,
    hint: str | None = None,
    on_change: bool = True,
):
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint, path_width=True)
        inp = ui.input().bind_value(job_model, attr)
        _attach_input(inp, is_frozen=is_frozen, narrow=False)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
    return inp


def kv_row(label: str, value: str, *, hint: str | None = None):
    """Read-only label/value pair (used by the I/O readonly view)."""
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint, path_width=True)
        ui.label(value).style(
            f"{MONO} font-size: 10px; color: {CLR_HEADER}; flex: 1 1 0; "
            "min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        ).tooltip(value)


def is_enum_type(field_type) -> bool:
    return field_type is not None and isinstance(field_type, type) and issubclass(field_type, Enum)
