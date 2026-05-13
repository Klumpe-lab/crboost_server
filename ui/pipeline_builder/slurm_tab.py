# ui/pipeline_builder/slurm_tab.py
"""
SLURM resources panel.

Layout:
  • Thin top bar: profile badge + "Reset to profile" + array throttle
    (throttle only appears for array jobs).
  • Resource fields stacked one per row (label inline-left of input) —
    per-task fields for array jobs, plain fields otherwise.
  • Array jobs additionally show a walltime estimate and a read-only
    Supervisor resource summary in their own bordered groups.

The S/M/L preset row was removed by user request; presets remain on the
model and can still be applied programmatically, but no longer have UI.
"""

import math
from typing import Callable

from nicegui import ui

from ui.job_plugins._field_styles import (
    field_grid,
    field_group,
    section_header,
    LABEL_STYLE,
    ROW_STYLE,
    VALUE_WRAP_NARROW,
    MONO,
    SANS,
    CLR_SUBLABEL,
)


def _is_array_job(job_model) -> bool:
    return "array_throttle" in getattr(job_model, "USER_PARAMS", set())


def _parse_time_to_minutes(time_str: str) -> float:
    """Parse SLURM time format (H:MM:SS or D-H:MM:SS) to minutes."""
    try:
        if "-" in time_str:
            days, rest = time_str.split("-", 1)
            parts = rest.split(":")
            hours = int(parts[0]) if len(parts) > 0 else 0
            mins = int(parts[1]) if len(parts) > 1 else 0
            secs = int(parts[2]) if len(parts) > 2 else 0
            return int(days) * 1440 + hours * 60 + mins + secs / 60
        parts = time_str.split(":")
        if len(parts) == 3:
            return int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) / 60
        if len(parts) == 2:
            return int(parts[0]) + int(parts[1]) / 60
        return float(parts[0])
    except (ValueError, IndexError):
        return 0


def _format_duration(minutes: float) -> str:
    if minutes < 60:
        return f"{minutes:.0f}min"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    days = hours / 24
    return f"{days:.1f}d"


def render_slurm_tab(job_model, is_frozen: bool, save_handler: Callable):
    _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    is_array = _is_array_job(job_model)
    has_profile = job_model.has_resource_profile()

    # ── Top bar: profile badge + reset link + (array-only) throttle ────────
    if has_profile or is_array:
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 12px; width: 100%; "
            "min-height: 22px; margin-bottom: 4px;"
        ):
            if has_profile:
                job_type_val = job_model.job_type.value if job_model.job_type else "?"
                ui.label(f"profile: {job_type_val}").style(
                    f"{MONO} font-size: 9px; color: #059669; background: #ecfdf5; "
                    "padding: 1px 6px; border-radius: 3px; flex-shrink: 0;"
                )
                if overrides and not is_frozen:

                    def reset_to_profile():
                        job_model.clear_slurm_overrides()
                        save_handler()
                        _render_slurm_content.refresh()

                    ui.button("Reset to profile", on_click=reset_to_profile).props(
                        "unelevated no-caps dense flat"
                    ).style(f"{SANS} font-size: 9px; padding: 0 4px; color: #059669; min-width: 0;")

            if is_array:
                if has_profile:
                    ui.element("div").style("width: 1px; height: 12px; background: #e2e8f0;")
                with ui.element("div").style(
                    "display: flex; align-items: baseline; gap: 6px; flex-shrink: 0;"
                ):
                    lbl = ui.label("Max concurrent").style(LABEL_STYLE)
                    lbl.tooltip("SLURM --array throttle: max tasks running simultaneously")
                    inp = ui.number(value=getattr(job_model, "array_throttle", 20), format="%d").bind_value(
                        job_model, "array_throttle"
                    )
                    inp.props(
                        "dense borderless hide-bottom-space "
                        'input-style="font-family: \'IBM Plex Mono\', monospace; font-size: 11px; '
                        "color: #1e293b; padding: 1px 2px; min-height: 0;\""
                    )
                    inp.style(VALUE_WRAP_NARROW)
                    if is_frozen:
                        inp.props("readonly")
                    else:

                        def _on_throttle_blur(_e):
                            save_handler()
                            _render_slurm_content.refresh()

                        inp.on("blur", _on_throttle_blur)

    # ── Resource fields ────────────────────────────────────────────────────
    if is_array:
        with field_group():
            section_header("Per-Task Resources", first=True)
            _render_resource_fields(effective_config, job_model, is_frozen, save_handler)
            _render_walltime_estimate(job_model, effective_config)
        _render_supervisor_summary()
    else:
        _render_resource_fields(effective_config, job_model, is_frozen, save_handler)


_RESOURCE_FIELDS = [
    ("partition", "Partition"),
    ("constraint", "Constraint"),
    ("nodes", "Nodes"),
    ("ntasks_per_node", "Tasks/node"),
    ("cpus_per_task", "CPUs/task"),
    ("gres", "GRES"),
    ("mem", "Memory"),
    ("time", "Time limit"),
]


def _render_resource_fields(effective_config, job_model, is_frozen, save_handler):
    """Stack the eight SLURM resource fields one per row, label inline-left."""
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
                    'input-style="font-family: \'IBM Plex Mono\', monospace; font-size: 11px; '
                    "color: #1e293b; padding: 1px 2px; min-height: 0;\""
                )
                inp.style(VALUE_WRAP_NARROW)
                if is_frozen:
                    inp.props("readonly")
                else:

                    def _make_blur(name):
                        def handler(e):
                            job_model.set_slurm_override(name, e.sender.value)
                            save_handler()
                            _render_slurm_content.refresh()

                        return handler

                    inp.on("blur", _make_blur(fname))


def _render_walltime_estimate(job_model, effective_config):
    """Estimated total wall time for the array based on TS count + throttle."""
    n_ts = 0
    if job_model._project_state is not None:
        n_ts = getattr(job_model._project_state, "import_selected_tilt_series", 0)

    if n_ts <= 0:
        return

    per_task_min = _parse_time_to_minutes(effective_config.time)
    if per_task_min <= 0:
        return

    throttle = getattr(job_model, "array_throttle", 4) or 1
    batches = math.ceil(n_ts / throttle)
    total_min = batches * per_task_min

    with ui.row().classes("w-full items-center gap-1").style("margin-top: 6px;"):
        ui.icon("schedule", size="11px").style(f"color: {CLR_SUBLABEL};")
        ui.label(
            f"{n_ts} TS × {_format_duration(per_task_min)}/task, "
            f"throttle {throttle} → ~{_format_duration(total_min)} wall"
        ).style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL};")


def _render_supervisor_summary():
    """Read-only supervisor SLURM config in a muted bordered group."""
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
                        f"{MONO} font-size: 11px; color: {CLR_SUBLABEL}; "
                        "font-style: italic; flex: 1 1 0;"
                    )
