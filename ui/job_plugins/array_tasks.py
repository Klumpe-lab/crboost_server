# ui/job_plugins/array_tasks.py
"""
Register the per-tilt-series task tracker tab for all array-dispatching job types.

The tracker reads the file-based contract written by the supervisor driver
(.task_manifest.json, .task_status/, task_*.out) and renders a live overview
of array task progress with inline log expansion.
"""

from services.models_base import JobType
from ui.components.array_task_tracker import render_array_task_tracker
from ui.job_plugins import register_extra_tab


@register_extra_tab(JobType.FS_MOTION_CTF, key="tasks", label="Tasks", icon="view_list")
def render_fs_tasks(job_type, job_model, backend, ui_mgr):
    render_array_task_tracker(job_model, ui_mgr)


@register_extra_tab(JobType.TS_ALIGNMENT, key="tasks", label="Tasks", icon="view_list")
def render_align_tasks(job_type, job_model, backend, ui_mgr):
    render_array_task_tracker(job_model, ui_mgr)


@register_extra_tab(JobType.TS_CTF, key="tasks", label="Tasks", icon="view_list")
def render_ctf_tasks(job_type, job_model, backend, ui_mgr):
    render_array_task_tracker(job_model, ui_mgr)
