# ui/pipeline_builder/config_tab.py
"""
Config/parameters tab content: job params, SLURM config, global params, template workbench.
"""

from pathlib import Path
from typing import Callable, List, Tuple

from nicegui import ui

from services.computing.slurm_service import SLURM_PRESET_MAP, SlurmPreset
from services.project_state import AlignmentMethod, JobStatus, JobType, get_project_state
from ui.ui_state import UIStateManager


def snake_to_title(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split("_"))


def is_job_frozen(job_type: JobType) -> bool:
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    if not job_model:
        return False
    return job_model.execution_status in [JobStatus.RUNNING, JobStatus.SUCCEEDED, JobStatus.FAILED]


# ===========================================
# SLURM Section
# ===========================================

def render_slurm_config_section(job_model, is_frozen: bool, save_handler: Callable):
    """Expansion panel wrapper -- not refreshable so it stays open."""
    with (
        ui.expansion("SLURM Resources", icon="memory")
        .classes("w-full border border-gray-200 rounded-lg mb-6 shadow-sm overflow-hidden")
        .props("dense header-class='bg-gray-50 text-gray-700 font-bold'")
    ):
        _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, "value") else str(raw_preset)

    with ui.row().classes("w-full items-center gap-2 p-3 bg-white border-b border-gray-100"):
        ui.label("Presets:").classes("text-[10px] font-black text-gray-400 uppercase mr-2")

        for preset in [SlurmPreset.SMALL, SlurmPreset.MEDIUM, SlurmPreset.LARGE]:
            preset_info = SLURM_PRESET_MAP[preset]
            is_active = current_preset == preset.value

            def apply_preset(p=preset):
                job_model.apply_slurm_preset(p)
                _render_slurm_content.refresh()

            ui.button(preset_info["label"], on_click=apply_preset).props("unelevated no-caps dense").classes(
                f"rounded-full px-3 text-xs {'bg-blue-600 text-white' if is_active else 'bg-gray-100 text-gray-600'}"
            )

        ui.space()

        if overrides:
            ui.button(
                icon="restart_alt",
                on_click=lambda: (job_model.clear_slurm_overrides(), _render_slurm_content.refresh()),
            ).props("flat dense").classes("text-red-400").tooltip("Clear Overrides")

    with ui.grid(columns=4).classes("w-full gap-x-6 gap-y-4 p-4 bg-white"):
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

        for field_name, label in fields:
            val = getattr(effective_config, field_name)

            with ui.column().classes("gap-0"):
                ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase ml-1")

                def make_blur_handler(fname):
                    return lambda e: (
                        job_model.set_slurm_override(fname, e.sender.value),
                        _render_slurm_content.refresh(),
                    )

                inp = ui.input(value=str(val)).props("outlined dense shadow-0")
                inp.classes("w-full text-xs font-mono")

                if is_frozen:
                    inp.props("readonly bg-color=grey-1")
                else:
                    inp.on("blur", make_blur_handler(field_name))


# ===========================================
# Read-only paths (for frozen jobs)
# ===========================================

def render_readonly_paths(job_model):
    paths_data = job_model.paths

    if not paths_data:
        ui.label("No paths resolved yet.").classes("text-xs text-gray-400 italic")
        return

    for i, (key, value) in enumerate(paths_data.items()):
        bg_class = "bg-white" if i % 2 == 0 else "bg-gray-50"
        with ui.row().classes(
            f"w-full p-2 {bg_class} border-b border-gray-100 "
            "last:border-0 justify-between items-start gap-4"
        ):
            ui.label(snake_to_title(key)).classes("text-xs font-semibold text-gray-500 uppercase w-32 pt-0.5")
            ui.label(str(value)).classes("text-xs font-mono text-gray-700 break-all flex-1")


# ===========================================
# Main Config Tab
# ===========================================

def render_config_tab(job_type: JobType, job_model, is_frozen: bool, ui_mgr: UIStateManager, backend, save_handler: Callable):
    """Render the full configuration/parameters tab."""

    with ui.column().classes("w-full"):
        # 1. I/O Configuration
        ui.label("I/O Configuration").classes("text-sm font-bold text-gray-900 mb-3")

        if is_frozen:
            with ui.card().classes("w-full p-3 border border-gray-200 shadow-none bg-gray-50"):
                ui.label("Job is running or completed. I/O configuration is locked.").classes(
                    "text-xs text-gray-500 italic mb-2"
                )
                render_readonly_paths(job_model)
        else:
            from ui.pipeline_builder.io_config_component import render_io_config
            render_io_config(job_type, on_change=save_handler)

        # 2. Job Parameters
        ui.label("Job Parameters").classes("text-sm font-bold text-gray-900 mb-3 mt-6")

        base_fields = {
            "execution_status", "relion_job_name", "relion_job_number",
            "paths", "additional_binds", "slurm_overrides", "source_overrides",
            "is_orphaned", "missing_inputs", "JOB_CATEGORY",
        }
        job_specific_fields = set(job_model.model_fields.keys()) - base_fields

        if not job_specific_fields:
            ui.label("This job has no configurable parameters.").classes("text-xs text-gray-500 italic mb-4")

        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            for param_name in sorted(list(job_specific_fields)):
                label = snake_to_title(param_name)
                value = getattr(job_model, param_name)

                if isinstance(value, bool):
                    checkbox = ui.checkbox(label).bind_value(job_model, param_name)
                    if not is_frozen:
                        checkbox.on_value_change(save_handler)
                    else:
                        checkbox.disable()

                elif isinstance(value, (int, float)) or value is None:
                    inp = ui.number(label, value=value, format="%.4g").bind_value(job_model, param_name)
                    inp.props("outlined dense").classes("w-full")
                    if is_frozen:
                        inp.classes("bg-gray-50 text-gray-500").props("readonly")
                    else:
                        inp.on_value_change(save_handler)

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        sel = ui.select(options=[e.value for e in AlignmentMethod], value=value, label=label)
                        sel.bind_value(job_model, param_name)
                        sel.props("outlined dense").classes("w-full")
                        if is_frozen:
                            sel.classes("bg-gray-50 text-gray-500")
                            sel.disable()
                        else:
                            sel.on_value_change(save_handler)
                    else:
                        inp = ui.input(label).bind_value(job_model, param_name)
                        inp.props("outlined dense").classes("w-full")
                        if is_frozen:
                            inp.classes("bg-gray-50 text-gray-500").props("readonly")
                        else:
                            inp.on_value_change(save_handler)

        # 3. SLURM Resources
        render_slurm_config_section(job_model, is_frozen, save_handler)

        # 4. Global Parameters (Read-Only)
        ui.label("Global Experimental Parameters (Read-Only)").classes("text-sm font-bold text-gray-900 mb-3")

        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            ui.input("Pixel Size (A)").bind_value(job_model.microscope, "pixel_size_angstrom").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")
            ui.input("Voltage (kV)").bind_value(job_model.microscope, "acceleration_voltage_kv").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")
            ui.input("Cs (mm)").bind_value(job_model.microscope, "spherical_aberration_mm").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")
            ui.input("Amplitude Contrast").bind_value(job_model.microscope, "amplitude_contrast").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")
            ui.input("Dose per Tilt").bind_value(job_model.acquisition, "dose_per_tilt").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")
            ui.input("Tilt Axis (deg)").bind_value(job_model.acquisition, "tilt_axis_degrees").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

        # 5. Template Workbench (only for Template Matching)
        if job_type == JobType.TEMPLATE_MATCH_PYTOM:
            ui.separator().classes("mb-6")
            ui.label("Template Workbench").classes("text-sm font-bold text-gray-900 mb-3")
            with ui.card().classes("w-full p-0 border border-gray-200 shadow-none bg-white"):
                from ui.template_workbench import TemplateWorkbench
                TemplateWorkbench(backend, str(ui_mgr.project_path))