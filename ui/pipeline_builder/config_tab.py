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


def _fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


def _is_pathlike_param(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ["path", "dir", "glob", "pattern", "file", "folder"])


def _ch_width(value, *, min_ch: int = 12, max_ch: int = 34) -> int:
    """
    Size inputs by content length (in 'ch' units) while keeping a sensible minimum.
    """
    s = "" if value is None else str(value)

    # If the content looks like a path, let it be wider
    if "/" in s or "\\" in s:
        min_ch = max(min_ch, 32)
        max_ch = max(max_ch, 90)
    elif len(s) > 32:
        min_ch = max(min_ch, 18)
        max_ch = max(max_ch, 60)

    return max(min_ch, min(max_ch, len(s) + 2))


def _style_compact_field(field, value, *, min_ch=12, max_ch=34) -> None:
    """Consistent compact styling for QInput-like widgets."""
    field.props("dense outlined hide-bottom-space")
    field.classes("text-xs font-mono")
    field.style(f"width: {_ch_width(value, min_ch=min_ch, max_ch=max_ch)}ch; max-width: 100%;")


def _job_string_width(param_name: str, value: str) -> tuple[int, int]:
    """
    Choose a wider default for job string fields.
    Critical: path-like fields should be wide even when empty.
    """
    if _is_pathlike_param(param_name):
        return (36, 90)
    # These often contain range-ish strings that shouldn't clip
    if any(k in param_name.lower() for k in ["range", "grid", "dimensions", "time", "constraint"]):
        return (18, 44)
    return (16, 44)


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
    """
    SLURM section as a normal card (no expansion/dropdown).
    """
    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
        # Header
        with ui.row().classes("w-full items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100"):
            with ui.row().classes("items-center gap-2"):
                ui.icon("memory", size="18px").classes("text-gray-500")
                ui.label("SLURM Resources").classes("text-sm font-bold text-gray-800")

            # Lightweight indicator if overrides exist
            if getattr(job_model, "slurm_overrides", None):
                ui.badge("overrides", color="orange").props("outline").classes("text-[10px]")

        # Body (refreshable)
        with ui.column().classes("w-full p-3"):
            _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, "value") else str(raw_preset)

    # Presets row
    with ui.row().classes("w-full items-center gap-2 mb-3"):
        ui.label("Presets").classes("text-[10px] font-black text-gray-400 uppercase mr-1")

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
            ).props("flat dense round").classes("text-red-400").tooltip("Clear Overrides")

    # Compact but slightly wider per-field widths to avoid overflow
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
        "constraint": (18, 34),  # wider: often a list like "g2|g3|..."
        "nodes": (10, 12),
        "ntasks_per_node": (12, 14),
        "cpus_per_task": (12, 14),
        "gres": (14, 24),
        "mem": (12, 16),
        "time": (14, 18),  # wider so "2:00:00" never clips
    }

    with ui.row().classes("w-full flex-wrap gap-x-5 gap-y-3 items-end"):
        for field_name, label in fields:
            val = getattr(effective_config, field_name)

            with ui.column().classes("gap-1 w-fit"):
                ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5")

                def make_blur_handler(fname):
                    return lambda e: (
                        job_model.set_slurm_override(fname, e.sender.value),
                        _render_slurm_content.refresh(),
                    )

                inp = ui.input(value=str(val))
                mn, mx = width_hint.get(field_name, (12, 34))
                _style_compact_field(inp, val, min_ch=mn, max_ch=mx)

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

    async def copy_path(p: str) -> None:
        try:
            ui.clipboard.write(p)  # no await
            ui.notify("Copied", type="positive", timeout=900)
        except Exception:
            safe = p.replace("`", "\\`")
            await ui.run_javascript(f"navigator.clipboard.writeText(`{safe}`)", respond=False)
            ui.notify("Copied", type="positive", timeout=900)

    # Tight table-like list
    with ui.column().classes("w-full border border-gray-200 rounded-md overflow-hidden"):
        for i, (key, value) in enumerate(paths_data.items()):
            bg_class = "bg-white" if i % 2 == 0 else "bg-gray-50"

            # Single-line row, no vertical padding waste
            with ui.row().classes(
                f"w-full {bg_class} border-b border-gray-100 last:border-0 items-center gap-2 px-2 py-1"
            ):
                ui.label(snake_to_title(key)).classes("text-[10px] font-bold text-gray-500 uppercase w-32 shrink-0")

                # Truncated but tooltip shows full value
                with ui.row().classes("flex-1 min-w-0 items-center gap-2"):
                    v_str = str(value)
                    ui.label(v_str).classes("text-xs font-mono text-gray-700 truncate flex-1 min-w-0").tooltip(v_str)

                    ui.button(icon="content_copy", on_click=lambda v=v_str: copy_path(v)).props(
                        "flat dense round size=sm"
                    ).classes("text-gray-500 hover:text-gray-800").tooltip("Copy path")


# ===========================================
# Main Config Tab
# ===========================================


def render_config_tab(
    job_type: JobType, job_model, is_frozen: bool, ui_mgr: UIStateManager, backend, save_handler: Callable
):
    """Render the full configuration/parameters tab."""

    with ui.column().classes("w-full"):
        # -------------------------------------------------------
        # I/O Configuration (compact dropdown)
        # -------------------------------------------------------
        with (
            ui.expansion("I/O Configuration", icon="swap_horiz", value=True)
            .classes("w-full border border-gray-200 rounded-lg shadow-sm overflow-hidden")
            .props("dense header-class='bg-gray-50 text-gray-800 font-bold' switch-toggle-side")
        ):
            with ui.column().classes("w-full p-3"):
                if is_frozen:
                    ui.label("Job is running or completed. I/O configuration is locked.").classes(
                        "text-[11px] text-gray-500 italic mb-2"
                    )
                    render_readonly_paths(job_model)
                else:
                    from ui.pipeline_builder.io_config_component import render_io_config

                    render_io_config(job_type, on_change=save_handler, active_job_types=set(ui_mgr.selected_jobs))

        ui.separator().classes("my-4")

        # Build job-specific field list (same logic)
        base_fields = {
            "execution_status",
            "relion_job_name",
            "relion_job_number",
            "paths",
            "additional_binds",
            "slurm_overrides",
            "source_overrides",
            "is_orphaned",
            "missing_inputs",
            "JOB_CATEGORY",
        }
        job_specific_fields = set(job_model.model_fields.keys()) - base_fields

        # -------------------------------------------------------
        # Parameters grid: Job / Slurm / Global
        # -------------------------------------------------------
        with ui.element("div").classes("w-full grid grid-cols-1 xl:grid-cols-3 gap-4"):
            # -------------------------------------------------------
            # Column 1: Job Parameters
            # -------------------------------------------------------
            with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
                with ui.row().classes(
                    "w-full items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100"
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("tune", size="18px").classes("text-gray-500")
                        ui.label("Job Parameters").classes("text-sm font-bold text-gray-800")

                with ui.column().classes("w-full p-3"):
                    if not job_specific_fields:
                        ui.label("This job has no configurable parameters.").classes("text-xs text-gray-500 italic")
                    else:
                        # Hand-tuned width hints for common offenders
                        job_width_hint = {
                            # Template matching / file fields
                            "template_path": (40, 90),
                            "mask_path": (40, 90),
                            "mask_fold_path": (40, 90),
                            "mdoc_pattern": (24, 60),
                            # Range/grid-ish
                            "c_defocus_min_max": (18, 28),
                            "c_range_min_max": (18, 28),
                            "m_range_min_max": (18, 28),
                            "c_grid": (16, 18),
                            "m_grid": (16, 18),
                            "tomo_dimensions": (22, 34),
                            "bandpass_filter": (18, 34),
                            "gpu_split": (16, 22),
                        }

                        # Wider default for string inputs; wrap to next line as needed
                        with ui.row().classes("w-full flex-wrap gap-x-6 gap-y-3 items-end"):
                            for param_name in sorted(list(job_specific_fields)):
                                label = snake_to_title(param_name)
                                value = getattr(job_model, param_name)

                                if isinstance(value, bool):
                                    checkbox = ui.checkbox(label).bind_value(job_model, param_name).props("dense")
                                    if not is_frozen:
                                        checkbox.on_value_change(save_handler)
                                    else:
                                        checkbox.disable()

                                elif isinstance(value, (int, float)) or value is None:
                                    with ui.column().classes("gap-1 w-fit"):
                                        ui.label(label).classes(
                                            "text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5"
                                        )
                                        inp = ui.number(value=value, format="%.4g").bind_value(job_model, param_name)
                                        # slightly wider for spinners/buttons
                                        _style_compact_field(inp, value, min_ch=10, max_ch=20)
                                        if is_frozen:
                                            inp.classes("bg-gray-50 text-gray-500").props("readonly")
                                        else:
                                            inp.on_value_change(save_handler)

                                elif isinstance(value, str):
                                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                                        with ui.column().classes("gap-1 w-fit"):
                                            ui.label(label).classes(
                                                "text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5"
                                            )
                                            sel = ui.select(
                                                options=[e.value for e in AlignmentMethod], value=value
                                            ).bind_value(job_model, param_name)
                                            sel.props("dense outlined hide-bottom-space")
                                            sel.classes("text-xs font-mono")
                                            sel.style("width: 26ch; max-width: 100%;")
                                            if is_frozen:
                                                sel.classes("bg-gray-50 text-gray-500")
                                                sel.disable()
                                            else:
                                                sel.on_value_change(save_handler)
                                    else:
                                        with ui.column().classes("gap-1 w-fit"):
                                            ui.label(label).classes(
                                                "text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5"
                                            )
                                            inp = ui.input().bind_value(job_model, param_name)

                                            # Wider strings, and path-like fields wide even when empty
                                            if param_name in job_width_hint:
                                                mn, mx = job_width_hint[param_name]
                                            else:
                                                mn, mx = _job_string_width(param_name, value)

                                            _style_compact_field(inp, value, min_ch=mn, max_ch=mx)

                                            if is_frozen:
                                                inp.classes("bg-gray-50 text-gray-500").props("readonly")
                                            else:
                                                inp.on_value_change(save_handler)

            # -------------------------------------------------------
            # Column 2: SLURM Resources (normal section)
            # -------------------------------------------------------
            render_slurm_config_section(job_model, is_frozen, save_handler)

            # -------------------------------------------------------
            # Column 3: Global Parameters (display-only)
            # -------------------------------------------------------
            with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
                with ui.row().classes(
                    "w-full items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100"
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("science", size="18px").classes("text-gray-500")
                        ui.label("Global Experimental Parameters").classes("text-sm font-bold text-gray-800")

                def _kv(label: str, obj, attr: str, tooltip: str = "Global parameter"):
                    with ui.row().classes("items-baseline gap-2 w-fit"):
                        ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase")
                        ui.label().bind_text_from(obj, attr, lambda v: _fmt(v)).classes(
                            "text-xs font-mono text-gray-800"
                        ).tooltip(tooltip)

                with ui.column().classes("w-full p-3"):
                    with ui.row().classes("w-full flex-wrap gap-x-8 gap-y-2"):
                        _kv("Pixel Size (Å)", job_model.microscope, "pixel_size_angstrom")
                        _kv("Voltage (kV)", job_model.microscope, "acceleration_voltage_kv")
                        _kv("Cs (mm)", job_model.microscope, "spherical_aberration_mm")
                        _kv("Amp Contrast", job_model.microscope, "amplitude_contrast")
                        _kv("Dose / Tilt", job_model.acquisition, "dose_per_tilt")
                        _kv("Tilt Axis (°)", job_model.acquisition, "tilt_axis_degrees")

        # -------------------------------------------------------
        # Template Workbench (only for Template Matching)
        # -------------------------------------------------------
        if job_type == JobType.TEMPLATE_MATCH_PYTOM:
            ui.separator().classes("my-6")
            ui.label("Template Workbench").classes("text-sm font-bold text-gray-900 mb-3")
            with ui.card().classes("w-full p-0 border border-gray-200 shadow-none bg-white"):
                from ui.template_workbench import TemplateWorkbench

                TemplateWorkbench(backend, str(ui_mgr.project_path))
