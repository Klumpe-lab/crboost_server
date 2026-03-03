# ui/pipeline_builder/slurm_tab.py
"""
SLURM resources tab: presets, per-field overrides.
"""

from typing import Callable

from nicegui import ui

from services.computing.slurm_service import SLURM_PRESET_MAP, SlurmPreset


def _ch_width(value, *, min_ch=12, max_ch=34) -> int:
    s = "" if value is None else str(value)
    if "/" in s or "\\" in s:
        min_ch, max_ch = max(min_ch, 32), max(max_ch, 90)
    elif len(s) > 32:
        min_ch, max_ch = max(min_ch, 18), max(max_ch, 60)
    return max(min_ch, min(max_ch, len(s) + 2))


def _style_compact(field_el, value, *, min_ch=12, max_ch=34):
    field_el.props("dense outlined hide-bottom-space")
    field_el.classes("text-xs font-mono")
    field_el.style(f"width: {_ch_width(value, min_ch=min_ch, max_ch=max_ch)}ch; max-width: 100%;")


def render_slurm_tab(job_model, is_frozen: bool, save_handler: Callable):
    """Render the SLURM configuration tab."""
    with ui.column().classes("w-full p-4"):
        _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, "value") else str(raw_preset)

    # Presets row
    with ui.row().classes("w-full items-center gap-2 mb-4"):
        ui.label("Presets").classes("text-[10px] font-black text-gray-400 uppercase mr-1")

        for preset in [SlurmPreset.SMALL, SlurmPreset.MEDIUM, SlurmPreset.LARGE]:
            preset_info = SLURM_PRESET_MAP[preset]
            is_active = current_preset == preset.value

            def apply_preset(p=preset):
                job_model.apply_slurm_preset(p)
                save_handler()
                _render_slurm_content.refresh()

            ui.button(preset_info["label"], on_click=apply_preset).props(
                "unelevated no-caps dense"
            ).classes(
                f"rounded-full px-3 text-xs "
                f"{'bg-blue-600 text-white' if is_active else 'bg-gray-100 text-gray-600'}"
            )

        ui.space()

        if overrides:
            def clear_and_save():
                job_model.clear_slurm_overrides()
                save_handler()
                _render_slurm_content.refresh()

            ui.button(icon="restart_alt", on_click=clear_and_save).props(
                "flat dense round"
            ).classes("text-red-400").tooltip("Clear Overrides")

    # Fields
    fields = [
        ("partition", "Partition"),
        ("constraint", "Constraint"),
        ("nodes", "Nodes"),
        ("ntasks_per_node", "Tasks/Node"),
        ("cpus_per_task", "CPUs/Task"),
        ("gres", "GRES (GPU)"),
        ("mem", "Memory"),
        ("time", "Time Limit"),
    ]
    width_hint = {
        "partition": (12, 18),
        "constraint": (18, 34),
        "nodes": (10, 12),
        "ntasks_per_node": (12, 14),
        "cpus_per_task": (12, 14),
        "gres": (14, 24),
        "mem": (12, 16),
        "time": (14, 18),
    }

    with ui.row().classes("w-full flex-wrap gap-x-5 gap-y-3 items-end"):
        for field_name, label in fields:
            val = getattr(effective_config, field_name)
            with ui.column().classes("gap-1 w-fit"):
                ui.label(label).classes(
                    "text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5"
                )

                def make_blur_handler(fname):
                    def handler(e):
                        job_model.set_slurm_override(fname, e.sender.value)
                        save_handler()
                        _render_slurm_content.refresh()
                    return handler

                inp = ui.input(value=str(val))
                mn, mx = width_hint.get(field_name, (12, 34))
                _style_compact(inp, val, min_ch=mn, max_ch=mx)

                if is_frozen:
                    inp.props("readonly bg-color=grey-1")
                else:
                    inp.on("blur", make_blur_handler(field_name))