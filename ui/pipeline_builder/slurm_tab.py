# ui/pipeline_builder/slurm_tab.py
"""
SLURM resources: compact horizontal layout with preset pills.

For array jobs (those with array_throttle in USER_PARAMS), shows:
  - Preset pills + array throttle control
  - Per-task resource fields in a bordered group
  - Walltime estimate
  - Read-only supervisor resource summary in a separate group
"""

import math
from typing import Callable

from nicegui import ui

from services.computing.slurm_service import SlurmPreset

MONO = "font-family: 'IBM Plex Mono', monospace;"
FONT = "font-family: 'IBM Plex Sans', sans-serif;"
_CLR_LABEL = "#64748b"
_CLR_BORDER = "#cbd5e1"
_CLR_SUBLABEL = "#94a3b8"

_INPUT_STYLE = (
    f"{MONO} font-size: 11px; border-bottom: 1px solid {_CLR_BORDER}; "
    "border-radius: 0; padding: 1px 2px; line-height: 1.4;"
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
    """Render the SLURM configuration section."""
    _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, "value") else str(raw_preset)
    is_array = _is_array_job(job_model)
    has_profile = job_model.has_resource_profile()

    # ── Row 1: Presets + profile badge + throttle ──
    with ui.row().classes("w-full items-center gap-1"):
        ui.label("Preset").style(f"{FONT} font-size: 9px; color: {_CLR_SUBLABEL}; flex-shrink: 0;")
        for preset, label in [(SlurmPreset.SMALL, "S"), (SlurmPreset.MEDIUM, "M"), (SlurmPreset.LARGE, "L")]:
            is_active = current_preset == preset.value

            def apply_preset(p=preset):
                job_model.apply_slurm_preset(p)
                save_handler()
                _render_slurm_content.refresh()

            btn = (
                ui.button(label, on_click=apply_preset)
                .props("unelevated no-caps dense")
                .style(
                    f"{FONT} font-size: 9px; padding: 1px 8px; border-radius: 3px; min-width: 0; "
                    f"background: {'#475569' if is_active else '#f1f5f9'}; "
                    f"color: {'white' if is_active else '#64748b'};"
                )
            )
            if is_frozen:
                btn.props("disable")

        if has_profile:
            ui.element("div").style("width: 1px; height: 14px; background: #e2e8f0; margin: 0 4px;")
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

                ui.button("Reset to profile", on_click=reset_to_profile).props("unelevated no-caps dense flat").style(
                    f"{FONT} font-size: 9px; padding: 1px 6px; color: #059669; min-width: 0;"
                )

        if is_array:
            ui.element("div").style("width: 1px; height: 14px; background: #e2e8f0; margin: 0 4px;")
            with ui.column().classes("gap-0"):
                ui.label("Max concurrent").style(f"{FONT} font-size: 9px; color: {_CLR_LABEL}; line-height: 1;")
                inp = ui.number(value=getattr(job_model, "array_throttle", 20), format="%d").bind_value(
                    job_model, "array_throttle"
                )
                inp.props("dense borderless hide-bottom-space")
                # Wider so two-digit values (e.g. 20) aren't cramped.
                inp.style(f"{_INPUT_STYLE} width: 60px;")
                if is_frozen:
                    inp.props("readonly").style(f"color: {_CLR_SUBLABEL};")
                else:
                    # Save + refresh on blur, not on every keystroke.
                    # on_value_change + refresh destroys the <input> while the
                    # user is typing, which stole focus after the first digit.
                    def _on_throttle_blur(_e):
                        save_handler()
                        _render_slurm_content.refresh()

                    inp.on("blur", _on_throttle_blur)
                inp.tooltip("SLURM --array throttle: max tasks running simultaneously")

    # ── Per-Task resources (bordered group for array jobs) ──
    if is_array:
        with ui.element("div").style(
            "width: 100%; border: 1px solid #e2e8f0; border-radius: 4px; padding: 4px 6px 5px; margin-top: 4px;"
        ):
            ui.label("Per-Task Resources").style(
                f"{FONT} font-size: 9px; font-weight: 600; color: {_CLR_SUBLABEL}; margin-bottom: 2px;"
            )
            _render_resource_fields(effective_config, job_model, is_frozen, save_handler)
            _render_walltime_estimate(job_model, effective_config)
    else:
        _render_resource_fields(effective_config, job_model, is_frozen, save_handler)

    # ── Supervisor resources (bordered group, array jobs only) ──
    if is_array:
        _render_supervisor_summary()


def _render_resource_fields(effective_config, job_model, is_frozen, save_handler):
    """Render the SLURM parameter input fields."""
    fields = [
        ("partition", "Partition", "10ch"),
        ("constraint", "Constraint", "14ch"),
        ("nodes", "Nodes", "5ch"),
        ("ntasks_per_node", "Tasks/node", "5ch"),
        ("cpus_per_task", "CPUs/task", "5ch"),
        ("gres", "GRES", "10ch"),
        ("mem", "Memory", "7ch"),
        ("time", "Time limit", "9ch"),
    ]

    with ui.row().classes("w-full flex-wrap gap-x-3 gap-y-1 items-end"):
        for field_name, label, w in fields:
            val = getattr(effective_config, field_name)

            def make_blur_handler(fname):
                def handler(e):
                    job_model.set_slurm_override(fname, e.sender.value)
                    save_handler()
                    _render_slurm_content.refresh()

                return handler

            with ui.column().classes("gap-0"):
                ui.label(label).style(f"{FONT} font-size: 9px; color: {_CLR_LABEL}; line-height: 1;")
                inp = ui.input(value=str(val))
                inp.props("dense borderless hide-bottom-space")
                inp.style(f"{_INPUT_STYLE} width: {w};")

                if is_frozen:
                    inp.props("readonly").style(f"color: {_CLR_SUBLABEL};")
                else:
                    inp.on("blur", make_blur_handler(field_name))


def _render_walltime_estimate(job_model, effective_config):
    """Show estimated total walltime for the array based on TS count and throttle."""
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

    with ui.row().classes("w-full items-center gap-1").style("margin-top: 3px;"):
        ui.icon("schedule", size="11px").style(f"color: {_CLR_SUBLABEL};")
        ui.label(
            f"{n_ts} TS \u00d7 {_format_duration(per_task_min)}/task, "
            f"throttle {throttle} \u2192 ~{_format_duration(total_min)} wall"
        ).style(f"{MONO} font-size: 10px; color: {_CLR_SUBLABEL};")


def _render_supervisor_summary():
    """Show read-only supervisor SLURM config in a bordered group."""
    try:
        from services.configs.config_service import get_config_service

        sup = get_config_service().supervisor_slurm_defaults
    except Exception:
        return

    with ui.element("div").style(
        "width: 100%; border: 1px solid #f1f5f9; border-radius: 4px; "
        "padding: 4px 6px 5px; margin-top: 4px; background: #fafbfc;"
    ):
        with ui.row().classes("w-full items-center gap-1").style("margin-bottom: 2px;"):
            ui.label("Supervisor").style(
                f"{FONT} font-size: 9px; font-weight: 600; color: {_CLR_SUBLABEL}; flex-shrink: 0;"
            )
            ui.icon("info_outline", size="11px").style(f"color: {_CLR_SUBLABEL}; cursor: help;").tooltip(
                "The supervisor dispatches array tasks and aggregates results.\n"
                "Its resources are configured globally in conf.yaml."
            )

        sup_fields = [
            ("partition", sup.partition),
            ("mem", sup.mem),
            ("time", sup.time),
            ("gres", sup.gres or "(none)"),
            ("cpus", str(sup.cpus_per_task)),
        ]
        with ui.row().classes("w-full flex-wrap gap-x-3 gap-y-0 items-end"):
            for label, val in sup_fields:
                with ui.column().classes("gap-0"):
                    ui.label(label).style(f"{FONT} font-size: 8px; color: {_CLR_SUBLABEL}; line-height: 1;")
                    ui.label(str(val)).style(
                        f"{MONO} font-size: 10px; color: {_CLR_SUBLABEL}; font-style: italic; line-height: 1.4;"
                    )
