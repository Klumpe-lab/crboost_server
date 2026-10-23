# ui/pipeline_builder/disk_footprint_chip.py
"""A finished job's size on disk, in the job page header.

The figure comes from the project's disk-usage measurement (services/project_disk_usage.py), whose walk
sizes every job folder. A job that finished after the last walk asks for a new one, once per page, and
says so until it lands. Read every 15 s while the job's page is shown and the job is finished; once a
figure measured after the job finished is on screen, nothing more is read."""

import asyncio
from datetime import datetime
from pathlib import Path

from nicegui import ui

from services.project_disk_usage import DiskUsage, get_disk_usage_service
from services.project_state import JobStatus
from ui.current_project import current_project_state
from ui.job_plugins._field_styles import CLR_SUBLABEL
from ui.projects_overview import format_bytes, size_color
from ui.styles import MONO
from ui.ui_state import UIStateManager

# The project's cache file and the job's exit markers are on the shared filesystem: the disk cadence.
_POLL_S = 15.0
_FINISHED = (JobStatus.SUCCEEDED, JobStatus.FAILED)
_EXIT_MARKERS = ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE")
_WHAT = "On disk: the job folder's allocated size, as du counts it (links not followed)."


def _finished_at(job_dir: Path) -> float | None:
    """When the job's exit marker was written; None without one."""
    times = []
    for name in _EXIT_MARKERS:
        try:
            times.append((job_dir / name).stat().st_mtime)
        except FileNotFoundError:
            continue  # the other outcome's marker
    return max(times, default=None)


def _read(project_path: Path, rel: str) -> tuple[DiskUsage | None, float | None]:
    return get_disk_usage_service().cached(project_path), _finished_at(project_path / rel)


def _view(usage: DiskUsage | None, rel: str, finished_at: float | None) -> tuple[tuple | None, bool]:
    """((text, title, color) or None to hide, whether the measurement postdates the job's finish)."""
    if usage is None or usage.jobs is None:
        return ("size: measuring…", f"{_WHAT}\nThe project has not been measured since this job finished.", None), False
    current = finished_at is None or usage.measured_at >= finished_at
    when = datetime.fromtimestamp(usage.measured_at).strftime("%Y-%m-%d %H:%M")
    if usage.error:
        return ("size unknown", f"The project's disk measurement of {when} failed: {usage.error}", None), current
    size = usage.jobs.get(rel)
    if size is None:
        if current:
            return None, True  # no folder of its own on disk
        return ("size: measuring…", f"{_WHAT}\nThe project has not been measured since this job finished.", None), False
    if current:
        return (format_bytes(size), f"{_WHAT}\nMeasured {when}.", size_color(size)), True
    title = f"{_WHAT}\nMeasured {when}, before the job finished; a new measurement is on its way."
    return (f"{format_bytes(size)} · re-measuring", title, None), False


def render_disk_footprint_chip(instance_id: str, ui_mgr: UIStateManager) -> None:
    state = current_project_state()
    project_path = ui_mgr.project_path
    label = ui.label("").style(f"{MONO} font-size: 10px; white-space: nowrap; flex-shrink: 0;")
    label.set_visibility(False)
    shown: list = [None]
    # (status, folder) whose figure on screen postdates the job's finish: nothing more to read.
    settled: list = [None]
    requested = [False]

    async def tick() -> None:
        job = state.jobs.get(instance_id)
        rel = (job.relion_job_name or "").strip("/") if job is not None else ""
        key = (job.execution_status, rel) if rel and job.execution_status in _FINISHED else None
        if key is not None and key == settled[0]:
            return
        view = None
        if key is not None and project_path is not None:
            usage, finished_at = await asyncio.to_thread(_read, project_path, rel)
            view, current = _view(usage, rel, finished_at)
            service = get_disk_usage_service()
            if current:
                settled[0] = key
            elif not requested[0] and not service.is_pending(project_path):
                requested[0] = True
                service.request([str(project_path)])
        else:
            settled[0] = None
        if view == shown[0] or label.is_deleted:
            return
        shown[0] = view
        if view is not None:
            text, title, color = view
            label.set_text(text)
            label.props["title"] = title
            label.style(f"color: {color or CLR_SUBLABEL};")
        label.set_visibility(view is not None)

    scope = ui_mgr.job_scope(instance_id)
    scope.timer(0.1, tick, label, once=True)
    scope.timer(_POLL_S, tick, label)
