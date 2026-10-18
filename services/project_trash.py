"""A project's `Trash/`: the job directories removed from its pipeline.

Removing a finished job moves its directory to `Trash/<Top>_<jobNNN>`
(`pipeline_deletion_service._move_job_dir_to_trash`), so its files keep their disk space until
the trash is cleared. Nothing in ProjectState refers to a trashed job any more.

`list_trash` sizes each entry the way the project sizer does (allocated blocks, symlinks never
followed); `clear_trash` deletes named entries, or all of them. Both block: callers run them off the
event loop.
"""

from __future__ import annotations

import os
import re
import stat
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.background_tasks import ProgressCallback
from services.jobs.spec import display_name
from services.models_base import InstanceId, JobType
from services.project_delete import delete_path
from services.project_disk_usage import tree_bytes
from services.result import err, ok

TRASH_DIR = "Trash"

_INSTANCE_ID_RE = re.compile(r"--instance_id\s+(\S+)")


@dataclass(frozen=True, slots=True)
class TrashEntry:
    name: str  # `External_job012`
    size_bytes: int
    instance_id: str | None  # from the job's run_submit.script
    job_label: str  # display name of its job type; "" when unknown
    modified: float  # the entry's mtime: when the job last wrote into it
    unreadable_dirs: int  # directories the walk could not list: the size is then a lower bound


def trash_summary(project_dir: Path) -> tuple[int, float]:
    """(entries, mtime) of the project's Trash/, without sizing anything."""
    trash = project_dir / TRASH_DIR
    try:
        return len(os.listdir(trash)), trash.stat().st_mtime
    except FileNotFoundError:
        # No job was ever removed from this project.
        return 0, 0.0
    except PermissionError:
        # Another user's Trash/ this account cannot list: the project row shows without it.
        return 0, 0.0


def _instance_id(entry_dir: Path) -> str | None:
    try:
        text = (entry_dir / "run_submit.script").read_text(errors="replace")
    except (FileNotFoundError, PermissionError):
        # Never submitted, or another user's unreadable file: the entry still counts, unnamed.
        return None
    match = _INSTANCE_ID_RE.search(text)
    return match.group(1) if match else None


def _job_label(instance_id: str | None) -> str:
    if instance_id is None:
        return ""
    base, _ = InstanceId.split(instance_id)
    try:
        return display_name(JobType(base))
    except ValueError:
        # A job type this code no longer knows: the instance id alone names it.
        return ""


def list_trash(project_dir: Path) -> list[TrashEntry]:
    """Every entry of the project's Trash/ with its size, largest first."""
    try:
        with os.scandir(project_dir / TRASH_DIR) as it:
            children = list(it)
    except FileNotFoundError:
        # No job was ever removed from this project.
        return []
    entries = []
    for child in children:
        try:
            st = child.stat(follow_symlinks=False)
        except FileNotFoundError:
            # Cleared from another server between listing and stat.
            continue
        size, unreadable = tree_bytes(child.path, st)
        iid = _instance_id(Path(child.path)) if stat.S_ISDIR(st.st_mode) else None
        entries.append(TrashEntry(child.name, size, iid, _job_label(iid), st.st_mtime, unreadable))
    entries.sort(key=lambda e: -e.size_bytes)
    return entries


def clear_trash(
    project_dir: Path,
    names: list[str] | None = None,
    progress_cb: ProgressCallback | None = None,
    stop: threading.Event | None = None,
) -> dict[str, Any]:
    """Permanently delete the named entries of the project's Trash/, or all of them when `names` is
    None. Each name must be a direct entry of Trash/; a symlink is removed itself, never followed.
    Deletes what it can and names what it could not. `progress_cb` hears of each entry before it
    goes; a set `stop` ends it before the next one."""
    trash = project_dir / TRASH_DIR
    if names is None:
        try:
            names = sorted(os.listdir(trash))
        except FileNotFoundError:
            # No job was ever removed from this project.
            names = []
    removed: list[str] = []
    failed: list[str] = []
    for i, name in enumerate(names):
        if stop is not None and stop.is_set():
            return err(f"Stopped after {i} of {len(names)} trash entries", removed=removed, failed=failed)
        if progress_cb is not None:
            progress_cb(i, len(names), name)
        if not name or name in (".", "..") or Path(name).name != name:
            failed.append(f"{name!r} is not an entry of {trash}")
            continue
        # An entry already cleared from another server counts as removed.
        problems = delete_path(trash / name)
        if problems:
            more = f" (and {len(problems) - 1} more)" if len(problems) > 1 else ""
            failed.append(f"{name}: {problems[0]}{more}")
        else:
            removed.append(name)
    if failed:
        return err(
            f"Could not delete {len(failed)} of {len(names)} trash entries: " + "; ".join(failed),
            removed=removed,
            failed=failed,
        )
    return ok(removed=removed)
