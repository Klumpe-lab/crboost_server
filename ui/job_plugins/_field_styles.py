"""
Shared visual language for job-page parameter renderers.

All structural styling is inline (no dependency on a CSS class file). This
keeps the layout robust even when static/main.css fails to refresh in the
browser. Each field renders as a single horizontal row -- label inline-left
of the input -- and rows stack vertically inside their collapsible.
"""

from contextlib import contextmanager
from enum import Enum
from typing import Callable, Optional

from nicegui import ui

# ── Tokens ──────────────────────────────────────────────────────────────────
MONO = "font-family: 'IBM Plex Mono', monospace;"
SANS = "font-family: 'IBM Plex Sans', sans-serif;"

CLR_LABEL = "#64748b"      # slate-500
CLR_VALUE = "#1e293b"      # slate-800
CLR_HEADER = "#475569"     # slate-600
CLR_SUBLABEL = "#94a3b8"   # slate-400
CLR_BORDER = "#e2e8f0"     # slate-200

LABEL_STYLE = (
    f"{SANS} font-size: 10px; font-weight: 500; color: {CLR_LABEL}; "
    "line-height: 1.4; flex-shrink: 0; white-space: nowrap;"
)
VALUE_WRAP_STYLE = (
    f"{MONO} font-size: 11px; color: {CLR_VALUE}; "
    "flex: 1 1 0; min-width: 0;"
)
VALUE_WRAP_NARROW = (
    f"{MONO} font-size: 11px; color: {CLR_VALUE}; "
    "flex: 0 0 auto; max-width: 96px;"
)
HELPER_STYLE = f"{SANS} font-size: 9px; color: {CLR_SUBLABEL};"
SUFFIX_STYLE = f"{SANS} font-size: 9px; color: {CLR_SUBLABEL}; flex-shrink: 0;"

ROW_STYLE = (
    "display: flex; align-items: baseline; gap: 8px; width: 100%; "
    "min-width: 0; min-height: 20px; padding: 1px 0;"
)

PATH_LABEL_W = 96   # px

SECTION_HEADER_STYLE = (
    f"{SANS} font-size: 10px; font-weight: 700; color: {CLR_HEADER}; "
    "letter-spacing: 0.06em; text-transform: uppercase; line-height: 1; "
    "margin: 8px 0 4px 0; display: block;"
)
SECTION_HEADER_FIRST_STYLE = SECTION_HEADER_STYLE.replace("margin: 8px 0 4px 0", "margin: 0 0 4px 0")

GROUP_STYLE = (
    "width: 100%; border: 1px solid #eef2f6; border-radius: 4px; "
    "padding: 6px 8px 8px; margin-top: 6px;"
)
GROUP_MUTED_STYLE = GROUP_STYLE + " background: #fafbfc;"

LABEL_PATH_STYLE = LABEL_STYLE + f" width: {PATH_LABEL_W}px; text-align: right;"


# ── Section heads ───────────────────────────────────────────────────────────
def section_header(text: str, *, first: bool = False):
    style = SECTION_HEADER_FIRST_STYLE if first else SECTION_HEADER_STYLE
    ui.label(text).style(style)


def section_rule():
    ui.element("div").style(f"width: 100%; height: 1px; background: {CLR_BORDER}; margin: 4px 0 6px 0;")


# ── Containers ──────────────────────────────────────────────────────────────
@contextmanager
def field_grid():
    """Vertical stack of field rows. Name kept for backwards compat with callers
    that pre-date the layout pivot."""
    el = ui.element("div").style(
        "display: flex; flex-direction: column; gap: 4px; width: 100%;"
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
    "font-family: 'IBM Plex Mono', monospace; font-size: 11px; "
    "color: #1e293b; padding: 1px 2px; min-height: 0;"
)
_INPUT_INNER_FROZEN = _INPUT_INNER_STYLE.replace("color: #1e293b", "color: #94a3b8")


def _attach_input(inp, *, is_frozen: bool, narrow: bool):
    inp.props(_INPUT_PROPS_BASE)
    inp.props(f'input-style="{_INPUT_INNER_FROZEN if is_frozen else _INPUT_INNER_STYLE}"')
    inp.style(VALUE_WRAP_NARROW if narrow else VALUE_WRAP_STYLE)
    if is_frozen:
        inp.props("readonly")


def _label(text: str, hint: Optional[str], *, path_width: bool = False):
    style = LABEL_PATH_STYLE if path_width else LABEL_STYLE
    lbl = ui.label(text).style(style)
    if hint:
        lbl.tooltip(hint)
    return lbl


def text_field(
    label: str, job_model, attr: str, *,
    is_frozen: bool, save_handler: Callable,
    hint: Optional[str] = None, suffix: str = "",
    narrow: bool = False, on_change: bool = True,
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


def numeric_field(
    label: str, job_model, attr: str, *,
    is_frozen: bool, save_handler: Callable,
    hint: Optional[str] = None, suffix: str = "",
    fmt: str = "%.4g", narrow: bool = True, on_change: bool = True,
):
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint)
        val = getattr(job_model, attr)
        inp = ui.number(value=val, format=fmt).bind_value(job_model, attr)
        _attach_input(inp, is_frozen=is_frozen, narrow=narrow)
        if not is_frozen:
            if on_change:
                inp.on_value_change(lambda _e: save_handler())
            else:
                inp.on("blur", lambda _e: save_handler())
        if suffix:
            ui.label(suffix).style(SUFFIX_STYLE)
    return inp


def enum_field(
    label: str, job_model, attr: str, enum_type, *,
    is_frozen: bool, save_handler: Callable,
    hint: Optional[str] = None,
):
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint)
        options = [e.value for e in enum_type]
        sel = ui.select(options=options, value=getattr(job_model, attr)).bind_value(job_model, attr)
        sel.props(_INPUT_PROPS_BASE)
        sel.style(VALUE_WRAP_STYLE)
        if is_frozen:
            sel.disable()
        else:
            sel.on_value_change(lambda _e: save_handler())
    return sel


def toggle_field(
    label: str, job_model, attr: str, *,
    is_frozen: bool, save_handler: Callable,
    hint: Optional[str] = None,
):
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
    label: str, job_model, attr: str, *,
    is_frozen: bool, save_handler: Callable,
    hint: Optional[str] = None, on_change: bool = True,
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


def kv_row(label: str, value: str, *, hint: Optional[str] = None):
    """Read-only label/value pair (used by the I/O readonly view)."""
    with ui.element("div").style(ROW_STYLE):
        _label(label, hint, path_width=True)
        ui.label(value).style(
            f"{MONO} font-size: 10px; color: {CLR_HEADER}; flex: 1 1 0; "
            "min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        ).tooltip(value)


def is_enum_type(field_type) -> bool:
    return field_type is not None and isinstance(field_type, type) and issubclass(field_type, Enum)
