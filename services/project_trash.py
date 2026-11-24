"""A project's `Trash/`: the job directories removed from its pipeline.

Removing a finished job moves its directory to `Trash/<Top>_<jobNNN>`
(`pipeline_deletion_service._move_job_dir_to_trash`), so its files keep their disk space until
the trash is cleared. Nothing in ProjectState refers to a trashed job any more: the job's settings go
into the entry as `crboost_job.json` (`write_job_record`), so Restore (`services/trash_restore.py`) can
put it back. An entry from before that record existed is found in an earlier copy of the project file
(`find_job_settings`: a schema backup, or a file-system snapshot over the project).

`list_trash` sizes each entry the way the project sizer does (allocated blocks, symlinks never
followed); `clear_trash` deletes named entries, or all of them. All of these block: callers run them
off the event loop.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import threading
from dataclasses import dataclass, field
from datetime import datetime
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


# ── Restore: what a job was, kept with its folder ─────────────────────────────

JOB_RECORD = "crboost_job.json"
STATE_FILE = "project_params.json"
# Where a file system keeps read-only snapshots of a directory: NetApp's `.snapshot/<name>/` and ZFS's
# `.zfs/snapshot/<name>/`, over the directory itself or one of its parents.
_SNAPSHOT_DIRS = (Path(".snapshot"), Path(".zfs") / "snapshot")


def entry_name(job_name: str) -> str:
    """The Trash/ entry a job folder becomes: `External/job005/` -> `External_job005`."""
    return job_name.rstrip("/").replace("/", "_")


def job_dir_of(entry: str) -> str:
    """The folder a Trash/ entry came from, relative to the project: `External_job005` -> `External/job005`."""
    top, sep, rest = entry.partition("_")
    return f"{top}/{rest}" if sep else entry


def write_job_record(
    project_dir: Path,
    job_name: str,
    instance_id: str,
    job: dict[str, Any],
    instances: dict[str, str],
    path_mapping: str | None,
) -> Path:
    """Keep a removed job's settings in its Trash/ entry: `job` as the project file holds it, and `instances`
    (every job of the project then, instance id -> folder), by which Restore finds what its inputs named."""
    path = project_dir / TRASH_DIR / entry_name(job_name) / JOB_RECORD
    record = {
        "instance_id": instance_id,
        "trashed_at": datetime.now().isoformat(timespec="seconds"),
        "job": job,
        "job_path_mapping": path_mapping,
        "instances": instances,
    }
    path.write_text(json.dumps(record, indent=1, default=str))
    return path


@dataclass(frozen=True, slots=True)
class JobSettings:
    """A trashed job's settings, as the project file held them."""

    instance_id: str
    job: dict[str, Any]
    path_mapping: str | None
    # Every job of the state they came from, instance id -> folder (`External/job005`): what the job's input
    # overrides and facets named, so Restore can name the same jobs by their instance ids now.
    instances: dict[str, str] = field(default_factory=dict)
    source: str = ""


def _state_copies(project_dir: Path) -> list[Path]:
    """Earlier copies of the project's state file, newest first: the schema backups beside it, and the file
    system's snapshots of it (`_SNAPSHOT_DIRS` over the project or one of its parents)."""
    found = list(project_dir.glob(f"{STATE_FILE}*.bak"))
    for ancestor in (project_dir, *project_dir.parents):
        rel = project_dir.relative_to(ancestor)
        for snaps in _SNAPSHOT_DIRS:
            try:
                names = os.listdir(ancestor / snaps)
            except (FileNotFoundError, NotADirectoryError, PermissionError):
                # No snapshots kept here, or none this account may list.
                continue
            for name in names:
                copy = ancestor / snaps / name / rel / STATE_FILE
                if copy.is_file():
                    found.append(copy)

    def mtime(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            # Rotated out between the listing and the stat: sorts last, and its read is skipped.
            return 0.0

    return sorted(found, key=mtime, reverse=True)


def find_job_settings(project_dir: Path, names: list[str]) -> dict[str, JobSettings | str]:
    """Each named Trash/ entry's job settings, or why there are none: its record (`write_job_record`), else the
    newest earlier copy of the project file that holds a job of its folder (`_state_copies`)."""
    out: dict[str, JobSettings | str] = {}
    wanted: dict[str, str] = {}
    for name in names:
        entry = project_dir / TRASH_DIR / name
        if not name or Path(name).name != name or not entry.is_dir():
            out[name] = f"{name!r} is not a job folder in Trash/"
            continue
        record = entry / JOB_RECORD
        if record.is_file():
            data = json.loads(record.read_text())
            out[name] = JobSettings(
                data["instance_id"],
                data["job"],
                data.get("job_path_mapping"),
                data.get("instances") or {},
                f"its record in Trash/{name}",
            )
        else:
            wanted[name] = job_dir_of(name)
    for copy in _state_copies(project_dir) if wanted else []:
        try:
            data = json.loads(copy.read_text())
        except (OSError, json.JSONDecodeError):
            # Rotated out, unreadable, or caught half-written: the next copy may hold the job.
            continue
        jobs = data.get("jobs") or {}
        instances = {iid: str(j.get("relion_job_name") or "").rstrip("/") for iid, j in jobs.items()}
        instances = {iid: d for iid, d in instances.items() if d}
        by_dir = {d: iid for iid, d in instances.items()}
        for name, job_dir in list(wanted.items()):
            iid = by_dir.get(job_dir)
            if iid is None:
                continue
            mapping = (data.get("job_path_mapping") or {}).get(iid)
            out[name] = JobSettings(iid, jobs[iid], mapping, instances, f"an earlier copy of the project file, {copy}")
            del wanted[name]
        if not wanted:
            break
    for name in wanted:
        out[name] = (
            "no record of its settings: it went to Trash before CryoBoost kept one, and no earlier copy of "
            f"{STATE_FILE} (a schema backup or a file-system snapshot) holds it"
        )
    return out


def move_back(project_dir: Path, name: str) -> None:
    """Move a Trash/ entry back to the folder it came from. Raises when that folder exists again."""
    src = project_dir / TRASH_DIR / name
    dst = project_dir / job_dir_of(name)
    if dst.exists():
        raise FileExistsError(f"{job_dir_of(name)} exists in the project again")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    # The record described the job while it was in Trash/; the project file holds it again.
    (dst / JOB_RECORD).unlink(missing_ok=True)
