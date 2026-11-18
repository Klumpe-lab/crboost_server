# ui/job_plugins/ts_reconstruct.py
"""
Plugin for ts_reconstruct: the default parameters with one data-fact note under Half maps, plus
the per-tilt-series task tracker tab.

The tracker reads the file-based contract written by the supervisor driver
(.task_manifest.json, .task_status/, task_*.out) and renders a live overview
of array task progress with inline log expansion.
"""

from nicegui import ui

from services.models_base import JobType
from ui.components.array_task_tracker import render_array_task_tracker
from ui.job_plugins import register_extra_tab, register_params_renderer
from ui.job_plugins._field_styles import HELPER_STYLE, NOTE_INDENT
from ui.job_plugins.default_renderer import render_default_params_card


@register_params_renderer(JobType.TS_RECONSTRUCT)
def render_reconstruct_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **ctx):
    # A project imported from SerialEM tilt stacks has one frame per tilt, so the
    # frame-series job wrote no even/odd half-averages: halfmap_frames is stamped 0
    # at init (turning it back on fails before Warp runs) and halfmap_tilts 1, so the
    # denoisers still get even/odd halves, split by tilt. The note sits under the Half maps group.
    state = getattr(job_model, "_project_state", None)
    after = {}
    if state is not None and getattr(state, "import_source_kind", "") == "stacks":
        after["Half maps"] = lambda: ui.label(
            "Single-frame stack input: no frame halves, so Frames stays 0; the even/odd halves CryoCARE and "
            "IsoNet train on are split by tilt instead (Tilts = 1)."
        ).style(f"{HELPER_STYLE} margin-left: {NOTE_INDENT}px;")
    return render_default_params_card(
        job_type, job_model, is_frozen, save_handler, ui_mgr=ui_mgr, backend=backend, after=after, **ctx
    )


@register_extra_tab(JobType.TS_RECONSTRUCT, key="tasks", label="Tasks", icon="view_list")
def render_tasks_tab(job_type, instance_id, job_model, backend, ui_mgr):
    render_array_task_tracker(instance_id, job_model, ui_mgr)
