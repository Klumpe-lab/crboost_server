from typing import Callable
from nicegui import ui
from services.computing.slurm_service import SLURM_PRESET_MAP, SlurmPreset
from services.project_state import JobStatus, get_project_state


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


def render_slurm_tab(job_model, is_frozen: bool, save_handler: Callable) -> "ui.timer":
    live_timer = None
    with ui.column().classes("w-full p-4 gap-4"):
        _render_slurm_live_status(job_model)
        live_timer = ui.timer(3.0, lambda: _render_slurm_live_status.refresh())
        _render_slurm_content(job_model, is_frozen, save_handler)
    return live_timer


@ui.refreshable
def _render_slurm_live_status(job_model):
    state = get_project_state()
    relion_job_name = job_model.relion_job_name
    slurm_info = state.slurm_info.get(relion_job_name) if relion_job_name else None
    is_running = job_model.execution_status == JobStatus.RUNNING

    if not slurm_info and not (state.pipeline_active and is_running):
        return

    with ui.card().classes("w-full bg-gray-50 border border-gray-200 p-3"):
        ui.label("LIVE STATUS").classes("text-[10px] font-black text-gray-400 uppercase mb-2")

        if slurm_info:
            with ui.row().classes("w-full flex-wrap gap-x-6 gap-y-2 items-center"):
                _stat("SLURM ID", slurm_info.slurm_job_id)
                _stat("State", slurm_info.slurm_state)
                _stat("Elapsed", slurm_info.elapsed)
                if slurm_info.node:
                    _stat("Node", slurm_info.node)
        else:
            with ui.row().classes("items-center gap-2"):
                ui.spinner(size="sm").classes("text-blue-500")
                ui.label("Waiting for SLURM allocation...").classes("text-xs text-gray-500")


def _stat(label: str, value: str):
    with ui.column().classes("gap-0"):
        ui.label(label).classes("text-[9px] font-bold text-gray-400 uppercase leading-none")
        ui.label(value).classes("text-sm font-mono text-gray-800")


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, "value") else str(raw_preset)

    with ui.row().classes("w-full items-center gap-2 mb-4"):
        ui.label("Presets").classes("text-[10px] font-black text-gray-400 uppercase mr-1")
        for preset in [SlurmPreset.SMALL, SlurmPreset.MEDIUM, SlurmPreset.LARGE]:
            preset_info = SLURM_PRESET_MAP[preset]
            is_active = current_preset == preset.value

            def apply_preset(p=preset):
                job_model.apply_slurm_preset(p)
                save_handler()
                _render_slurm_content.refresh()

            btn = (
                ui.button(preset_info["label"], on_click=apply_preset)
                .props("unelevated no-caps dense")
                .classes(
                    f"rounded-full px-3 text-xs "
                    f"{'bg-blue-600 text-white' if is_active else 'bg-gray-100 text-gray-600'}"
                )
            )
            if is_frozen:
                btn.props("disable")

    fields = [
        ("partition",       "Partition"),
        ("constraint",      "Constraint"),
        ("nodes",           "Nodes"),
        ("ntasks_per_node", "Tasks/Node"),
        ("cpus_per_task",   "CPUs/Task"),
        ("gres",            "GRES (GPU)"),
        ("mem",             "Memory"),
        ("time",            "Time Limit"),
    ]
    width_hint = {
        "partition":       (12, 18),
        "constraint":      (18, 34),
        "nodes":           (10, 12),
        "ntasks_per_node": (12, 14),
        "cpus_per_task":   (12, 14),
        "gres":            (14, 24),
        "mem":             (12, 16),
        "time":            (14, 18),
    }

    with ui.row().classes("w-full flex-wrap gap-x-5 gap-y-3 items-end"):
        for field_name, label in fields:
            val = getattr(effective_config, field_name)
            with ui.column().classes("gap-1 w-fit"):
                ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase leading-none ml-0.5")

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
