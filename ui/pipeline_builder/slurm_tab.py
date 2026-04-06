# ui/pipeline_builder/slurm_tab.py
"""
SLURM resources: compact horizontal layout with preset pills.
"""

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


def render_slurm_tab(job_model, is_frozen: bool, save_handler: Callable):
    """Render the SLURM configuration section."""
    _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, "value") else str(raw_preset)

    # Presets inline
    with ui.row().classes("w-full items-center gap-1 mb-2"):
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

    # Fields horizontal wrap
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
