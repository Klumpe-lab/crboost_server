"""House form fields — the one input chrome for dialogs and registry-style forms.

A field is a 9 px uppercase label to the LEFT of the control, and the control in the
app's `.cb-field` box: 16 px tall, 10 px Plex text, 1 px slate border, no native spin
arrows. The `.cb-field` rules (and the spinner kill) live in ui/dashboard/css.py and
are injected per client over the socket by `ensure_assets_loaded()` — the one CSS
channel that cannot go stale on this deployment.

    house_number("Box size", model=params, attr="box_size", width="w-24")
    house_text("Name", model=species, attr="name", width="w-56")
    house_select("Symmetry", options=syms, model=params, attr="symmetry")
    house_field("Custom", lambda: ui.input(...))          # anything else QField-shaped

`house_number` given `model=`/`attr=` binds through `numeric_forward`
(ui/job_plugins/_field_styles.py): `ui.number` always hands back a JS float, and an
`int` model field silently holding e.g. 0.05 fails validation on the NEXT load — which
used to drop the whole job instance ("Instance not found" in the driver). Building the
binding here makes that trap unreachable by construction, not by review.

Scope: dialogs, the Species page, landing-page forms. Job-tab parameter forms stay on
ui/job_plugins/_field_styles.py (`.cb-select` scale, 110 px label column) — the two
scales differ one notch by design; do not collapse them.
"""

from __future__ import annotations

from collections.abc import Callable

from nicegui import ui

from ui.job_plugins._field_styles import _numeric_kind, numeric_forward

# Same voice as the workbench creation forms and the table heads.
HOUSE_LABEL_CLS = "text-[9px] font-bold text-gray-400 uppercase tracking-wide"


def house_field(label: str, build: Callable, *, width: str = "w-20", hint: str | None = None):
    """One form control: label left, the built QField element in `.cb-field` chrome.

    `build` returns any QField-based element (ui.input / ui.number / ui.select / …);
    an empty `label` renders the box alone, still in the row so widths line up.
    A `width` containing `w-full` stretches the row itself, the box taking whatever
    the label leaves (path fields); otherwise the row hugs its content."""
    full = "w-full" in width
    with ui.row().classes("items-center gap-1 no-wrap" + (" w-full" if full else "")):
        lbl = ui.label(label).classes(HOUSE_LABEL_CLS) if label else None
        el = build()
        el.props("dense").classes(f"cb-field {width}")
        if full:
            el.classes("flex-1 min-w-0")
        if hint:
            if lbl is not None:
                lbl.tooltip(hint)
            el.tooltip(hint)
    return el


def house_number(
    label: str,
    *,
    model=None,
    attr: str | None = None,
    width: str = "w-24",
    hint: str | None = None,
    on_change: Callable | None = None,
    **kwargs,
) -> ui.number:
    """Numeric field. With `model`/`attr` it binds via `numeric_forward` so an int
    field can never end up holding a float (the job-dropping corruption trap);
    without them the caller owns the value/handlers via `**kwargs`."""

    def build():
        if model is not None and attr is not None:
            is_int, _ = _numeric_kind(model, attr)
            kwargs.setdefault("precision", 0 if is_int else None)
            inp = ui.number(**kwargs)
            inp.bind_value(model, attr, forward=numeric_forward(model, attr))
        else:
            inp = ui.number(**kwargs)
        if on_change is not None:
            inp.on_value_change(on_change)
        return inp

    return house_field(label, build, width=width, hint=hint)


def house_text(
    label: str,
    *,
    model=None,
    attr: str | None = None,
    width: str = "w-40",
    hint: str | None = None,
    on_change: Callable | None = None,
    **kwargs,
) -> ui.input:
    def build():
        inp = ui.input(**kwargs)
        if model is not None and attr is not None:
            inp.bind_value(model, attr)
        if on_change is not None:
            inp.on_value_change(on_change)
        return inp

    return house_field(label, build, width=width, hint=hint)


def house_select(
    label: str,
    options,
    *,
    model=None,
    attr: str | None = None,
    width: str = "w-40",
    hint: str | None = None,
    on_change: Callable | None = None,
    **kwargs,
) -> ui.select:
    def build():
        sel = ui.select(options=options, **kwargs)
        # Always the themed popup — the default Material menu is the tell that a
        # select skipped the vocabulary.
        sel.props('popup-content-class="cb-select-popup"')
        if model is not None and attr is not None:
            sel.bind_value(model, attr)
        if on_change is not None:
            sel.on_value_change(on_change)
        return sel

    return house_field(label, build, width=width, hint=hint)
