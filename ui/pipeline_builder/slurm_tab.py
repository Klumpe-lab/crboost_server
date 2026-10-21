# ui/pipeline_builder/slurm_tab.py
"""
SLURM resources panel.

Layout:
  • Thin top bar: profile badge + "Reset to profile" + array throttle
    (throttle only appears for array jobs).
  • Resource fields stacked one per row (label inline-left of input) —
    per-task fields for array jobs, plain fields otherwise.
  • Array jobs additionally show a read-only Supervisor resource summary
    below, set apart by whitespace.

Presets exist on the model and can be applied programmatically; the panel
has no preset UI.
"""

from collections.abc import Callable

from nicegui import ui

from ui.components.buttons import house_button
from ui.components.reactive import Debounced
from ui.job_plugins._field_styles import (
    field_grid,
    field_group,
    bind_numeric,
    section_header,
    LABEL_STYLE,
    ROW_STYLE,
    VALUE_WRAP_NARROW,
    MONO,
    CLR_SUBLABEL,
)


def _is_array_job(job_model) -> bool:
    return "array_throttle" in getattr(job_model, "USER_PARAMS", set())


# Quiet time after a SLURM field edit before the panel rebuilds to show its effect (refresh_after_edit).
_REFRESH_AFTER_EDIT_S = 0.8


def render_slurm_tab(job_model, is_frozen: bool, save_handler: Callable) -> Callable[[], None]:
    """Render the panel; returns its refresh. The job tab reruns that after an edit in any section, since
    the effective settings can follow the job's parameters (missAlign's time limit and GRES)."""
    shown = None
    # A field's edit reaches the rest of the panel once focus has stayed out of its fields for a
    # moment: rebuilding at once replaced the field the user had just clicked into. While one of
    # them has focus, a rebuild asked for by an edit elsewhere waits for its blur too.
    refresh_after_edit = Debounced(lambda: refresh(), _REFRESH_AFTER_EDIT_S)
    focused = {"on": False}

    def hold_refresh(_e=None) -> None:
        focused["on"] = True
        refresh_after_edit.cancel()

    def release_refresh(_e=None) -> None:
        focused["on"] = False
        refresh_after_edit()

    @ui.refreshable
    def content():
        nonlocal shown
        shown = _signature(job_model)
        _render_slurm_content(job_model, is_frozen, save_handler, refresh, hold_refresh, release_refresh)

    def refresh():
        # A rebuild replaces every input, so only rebuild when a value it shows has changed.
        if focused["on"]:
            return
        if _signature(job_model) != shown:
            content.refresh()

    content()
    return refresh


def _signature(job_model) -> tuple:
    """Everything the panel shows that can change while it is open."""
    return (
        job_model.get_effective_slurm_config().model_dump(),
        dict(job_model.slurm_overrides or {}),
        getattr(job_model, "array_throttle", None),
    )


def _render_slurm_content(
    job_model, is_frozen: bool, save_handler: Callable, refresh: Callable[[], None], hold: Callable, release: Callable
):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    is_array = _is_array_job(job_model)
    has_profile = job_model.has_resource_profile()

    # ── Top bar: profile badge + reset link + (array-only) throttle ────────
    if has_profile or is_array:
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 12px; width: 100%; min-height: 22px; margin-bottom: 4px;"
        ):
            if has_profile:
                job_type_val = job_model.job_type.value if job_model.job_type else "?"
                ui.label(f"profile: {job_type_val}").style(
                    f"{MONO} font-size: 9px; color: #475569; background: #e3ebf4; "
                    "padding: 1px 6px; border-radius: 3px; flex-shrink: 0;"
                )
                if overrides and not is_frozen:

                    def reset_to_profile():
                        job_model.clear_slurm_overrides()
                        save_handler()
                        refresh()

                    house_button("Reset to profile", reset_to_profile)

            if is_array:
                with ui.element("div").style("display: flex; align-items: baseline; gap: 6px; flex-shrink: 0;"):
                    lbl = ui.label("Max concurrent").style(LABEL_STYLE)
                    lbl.tooltip("SLURM --array throttle: max tasks running simultaneously")
                    # precision=0 + numeric_forward: array_throttle is an `int` and the
                    # model does not validate on assignment, so a fractional entry here
                    # would only surface as a load failure that drops the whole instance.
                    inp = bind_numeric(
                        ui.number(value=getattr(job_model, "array_throttle", 20), format="%d", precision=0),
                        job_model,
                        "array_throttle",
                    )
                    inp.props(
                        "dense borderless hide-bottom-space "
                        "input-style=\"font-family: 'IBM Plex Mono', monospace; font-size: 11px; "
                        'color: #1e293b; padding: 1px 2px; min-height: 0;"'
                    )
                    inp.style(VALUE_WRAP_NARROW).classes("cb-select")
                    if is_frozen:
                        inp.props("readonly")
                    else:

                        def _on_throttle_blur(_e):
                            save_handler()
                            release()

                        inp.on("focus", hold)
                        inp.on("blur", _on_throttle_blur)

    # ── Resource fields ────────────────────────────────────────────────────
    if is_array:
        with field_group():
            section_header("Per-Task Resources", first=True)
            _render_resource_fields(effective_config, job_model, is_frozen, save_handler, hold, release)
        _render_supervisor_summary()
    else:
        _render_resource_fields(effective_config, job_model, is_frozen, save_handler, hold, release)
    _render_slurm_note(job_model)


def _render_slurm_note(job_model):
    """The job class's SLURM_NOTE: one muted line, the detail in its tooltip."""
    note = getattr(job_model, "SLURM_NOTE", ())
    if not note:
        return
    line, detail = note
    with ui.row().classes("w-full items-center gap-1").style("margin-top: 6px; cursor: help;"):
        ui.icon("info_outline", size="11px").style(f"color: {CLR_SUBLABEL};")
        ui.label(line).style(f"font-size: 10px; color: {CLR_SUBLABEL};")
        ui.tooltip(detail).style("white-space: pre-line; max-width: 420px; font-size: 11px; line-height: 1.45;")


_RESOURCE_FIELDS = [
    ("partition", "Partition"),
    ("constraint", "Constraint"),
    ("nodes", "Nodes"),
    ("ntasks_per_node", "Tasks/node"),
    ("cpus_per_task", "CPUs/task"),
    ("gres", "GRES"),
    ("mem", "Memory"),
    ("time", "Time limit"),
    ("qos", "QOS"),
]


def _render_resource_fields(effective_config, job_model, is_frozen, save_handler, hold, release):
    """Stack the SLURM resource fields one per row, label inline-left."""
    with field_grid():
        for fname, label in _RESOURCE_FIELDS:
            # Don't bind directly to job_model -- that would write to the
            # underlying SlurmConfig instead of the override layer. Bind to the
            # effective value, then route blur into slurm_overrides.
            current = getattr(effective_config, fname)
            with ui.element("div").style(ROW_STYLE):
                lbl = ui.label(label).style(LABEL_STYLE)
                lbl.tooltip(f"SLURM --{fname.replace('_', '-')}")
                inp = ui.input(value=str(current))
                inp.props(
                    "dense borderless hide-bottom-space "
                    "input-style=\"font-family: 'IBM Plex Mono', monospace; font-size: 11px; "
                    'color: #1e293b; padding: 1px 2px; min-height: 0;"'
                )
                inp.style(VALUE_WRAP_NARROW).classes("cb-select")
                if is_frozen:
                    inp.props("readonly")
                else:

                    def _make_blur(name):
                        def handler(e):
                            # Focus passing through is not an edit: pinning the value shown would stop
                            # it following the job's settings (a computed time limit, say).
                            if e.sender.value != str(getattr(job_model.get_effective_slurm_config(), name)):
                                job_model.set_slurm_override(name, e.sender.value)
                                save_handler()
                            release()

                        return handler

                    inp.on("focus", hold)
                    inp.on("blur", _make_blur(fname))


def _render_supervisor_summary():
    """Read-only supervisor SLURM config, set apart from the per-task fields by whitespace."""
    try:
        from services.configs.config_service import get_config_service

        sup = get_config_service().supervisor_slurm_defaults
    except Exception:
        return

    with field_group(muted=True):
        with ui.row().classes("w-full items-center gap-1").style("margin-bottom: 4px;"):
            section_header("Supervisor", first=True)
            ui.icon("info_outline", size="11px").style(f"color: {CLR_SUBLABEL}; cursor: help;").tooltip(
                "The supervisor dispatches array tasks and aggregates results.\n"
                "Its resources are configured globally in conf.yaml."
            )

        with field_grid():
            for label, val in [
                ("Partition", sup.partition),
                ("Memory", sup.mem),
                ("Time limit", sup.time),
                ("GRES", sup.gres or "(none)"),
                ("CPUs/task", str(sup.cpus_per_task)),
            ]:
                with ui.element("div").style(ROW_STYLE):
                    ui.label(label).style(LABEL_STYLE)
                    ui.label(str(val)).style(
                        f"{MONO} font-size: 11px; color: {CLR_SUBLABEL}; font-style: italic; flex: 1 1 0;"
                    )
