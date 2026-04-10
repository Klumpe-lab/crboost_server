# ui/job_plugins/ts_reconstruct.py
"""
Plugin for ts_reconstruct: registers the per-tilt-series task tracker tab.

The tracker reads the file-based contract written by the supervisor driver
(.task_manifest.json, .task_status/, task_*.out) and renders a live overview
of array task progress with inline log expansion.
"""

from services.models_base import JobType
from ui.components.array_task_tracker import render_array_task_tracker
from ui.job_plugins import register_extra_tab


@register_extra_tab(JobType.TS_RECONSTRUCT, key="tasks", label="Tasks", icon="view_list")
def render_tasks_tab(job_type, job_model, backend, ui_mgr):
    render_array_task_tracker(job_model, ui_mgr)
