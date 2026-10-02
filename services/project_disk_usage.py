"""On-disk size of a project, measured off the request path and cached next to the data.

`measure()` is a pure lstat walk that attributes every byte to a bucket
(preprocessing / particles / trash / other) by the job directory it lives in.
`DiskUsageService` measures queued projects one at a time in a worker thread and
persists each result to `<project>/.disk_usage.json`, so every server reading the
same filesystem shares one measurement and it survives restarts.

Sizes are allocated blocks (`st_blocks * 512`), the number `du` reports. Symlinks
are never followed: a project's `frames/` links into the raw-data tree, which is
not the project's to own.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import stat
import time
from collections import deque
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from services.jobs.spec import JOB_SPECS, PHASE_PREPROCESSING, display_name
from services.models_base import JobType

logger = logging.getLogger(__name__)

CACHE_NAME = ".disk_usage.json"
# A project with fresh activity (or a running job) is re-walked at most this often.
REMEASURE_AFTER_SEC = 600.0

BUCKET_PREPROCESSING = "preprocessing"
BUCKET_PARTICLES = "particles"
BUCKET_TRASH = "trash"
BUCKET_OTHER = "other"
BUCKETS = (BUCKET_PREPROCESSING, BUCKET_PARTICLES, BUCKET_TRASH, BUCKET_OTHER)

# tsImport and imported tomograms have no roster phase but are tilt-series/tomogram
# work, not particle work.
_PREPROCESSING_TYPES = frozenset(
    {s.job_type for s in JOB_SPECS if s.phase == PHASE_PREPROCESSING} | {JobType.TS_IMPORT, JobType.IMPORTED_TOMOGRAMS}
)
_JOB_DIR_RE = re.compile(r"job\d+")


class DiskUsage(BaseModel):
    measured_at: float
    total_bytes: int = 0
    # bucket -> bytes
    buckets: dict[str, int] = {}
    # bucket -> {label -> bytes}; label is a job type's display name, or a top-level entry
    parts: dict[str, dict[str, int]] = {}
    # Directories the walk could not list; the total is then a lower bound.
    unreadable_dirs: int = 0
    error: str | None = None


def _bucket_for(job_type: str) -> tuple[str, str]:
    """(bucket, label) for a raw `job_type` string from project_params.json."""
    try:
        jt = JobType(job_type)
    except ValueError:
        # A type this code no longer knows (renamed/removed): still the project's bytes.
        return BUCKET_OTHER, f"unknown job type '{job_type}'"
    bucket = BUCKET_PREPROCESSING if jt in _PREPROCESSING_TYPES else BUCKET_PARTICLES
    return bucket, display_name(jt)


def _job_dir_index(jobs: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """`External/job002` -> (bucket, label) for every job that owns a directory."""
    index: dict[str, tuple[str, str]] = {}
    for job in jobs.values():
        if not isinstance(job, dict):
            continue
        rel = (job.get("relion_job_name") or "").strip("/")
        if rel:
            index[rel] = _bucket_for(job.get("job_type") or "")
    return index


class _Walk:
    """Byte counter shared across one project's walk: hardlinks count once, as in `du`."""

    def __init__(self):
        self.seen_inodes: set[tuple[int, int]] = set()
        self.unreadable_dirs = 0

    def entry_bytes(self, st: os.stat_result) -> int:
        if st.st_nlink > 1 and not stat.S_ISDIR(st.st_mode):
            key = (st.st_dev, st.st_ino)
            if key in self.seen_inodes:
                return 0
            self.seen_inodes.add(key)
        return st.st_blocks * 512

    def tree_bytes(self, root: str, root_stat: os.stat_result) -> int:
        total = self.entry_bytes(root_stat)
        if not stat.S_ISDIR(root_stat.st_mode):
            return total
        stack = [root]
        while stack:
            current = stack.pop()
            try:
                it = os.scandir(current)
            except FileNotFoundError:
                # Removed mid-walk (a job re-run or deleted) -- nothing left to count.
                continue
            except PermissionError:
                self.unreadable_dirs += 1
                continue
            with it:
                for entry in it:
                    try:
                        st = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        # Removed between listing and stat.
                        continue
                    total += self.entry_bytes(st)
                    if stat.S_ISDIR(st.st_mode):
                        stack.append(entry.path)
        return total


def measure(project_dir: Path, jobs: dict[str, Any]) -> DiskUsage:
    """Walk `project_dir` and attribute every byte to a bucket.

    Each child of a top-level directory is looked up as `<Top>/<child>` in the jobs'
    `relion_job_name`s. A hit takes that job's bucket; an unclaimed `jobNNN` dir is
    "unlisted job dirs"; `Trash/` is trash; everything else is "other" under its
    top-level name.
    """
    index = _job_dir_index(jobs)
    walk = _Walk()
    usage = DiskUsage(measured_at=time.time())
    buckets = dict.fromkeys(BUCKETS, 0)
    parts: dict[str, dict[str, int]] = {b: {} for b in BUCKETS}

    def add(bucket: str, label: str, n: int) -> None:
        buckets[bucket] += n
        parts[bucket][label] = parts[bucket].get(label, 0) + n

    root_stat = os.lstat(project_dir)
    add(BUCKET_OTHER, "project files", walk.entry_bytes(root_stat))
    with os.scandir(project_dir) as top_entries:
        tops = list(top_entries)
    for top in tops:
        try:
            top_stat = top.stat(follow_symlinks=False)
        except FileNotFoundError:
            # Removed between listing and stat.
            continue
        if not stat.S_ISDIR(top_stat.st_mode):
            add(BUCKET_OTHER, "project files", walk.entry_bytes(top_stat))
            continue
        if top.name == "Trash":
            add(BUCKET_TRASH, "deleted jobs", walk.tree_bytes(top.path, top_stat))
            continue
        add(BUCKET_OTHER, top.name, walk.entry_bytes(top_stat))
        try:
            children = list(os.scandir(top.path))
        except FileNotFoundError:
            # Removed between listing and descent.
            continue
        except PermissionError:
            walk.unreadable_dirs += 1
            continue
        for child in children:
            try:
                child_stat = child.stat(follow_symlinks=False)
            except FileNotFoundError:
                # Removed between listing and stat.
                continue
            n = walk.tree_bytes(child.path, child_stat)
            hit = index.get(f"{top.name}/{child.name}")
            if hit is not None:
                add(hit[0], hit[1], n)
            elif _JOB_DIR_RE.fullmatch(child.name) and stat.S_ISDIR(child_stat.st_mode):
                add(BUCKET_OTHER, "unlisted job dirs", n)
            else:
                add(BUCKET_OTHER, top.name, n)

    usage.buckets = buckets
    usage.parts = {b: p for b, p in parts.items() if p}
    usage.total_bytes = sum(buckets.values())
    usage.unreadable_dirs = walk.unreadable_dirs
    return usage


def read_cache(project_dir: Path) -> DiskUsage | None:
    path = project_dir / CACHE_NAME
    try:
        raw = path.read_text()
    except (FileNotFoundError, PermissionError):
        # Never measured, or written by someone whose file this user cannot read:
        # either way this process measures it itself.
        return None
    try:
        return DiskUsage.model_validate_json(raw)
    except ValidationError:
        logger.warning("Ignoring unreadable disk-usage cache %s", path)
        return None


def is_stale(usage: DiskUsage | None, *, last_activity_ts: float, running: bool, now: float) -> bool:
    """Should this project be (re)measured? Never measured: yes. Otherwise only when
    something may have changed since -- activity after the measurement, a running job,
    or a failed measurement -- and the last one is older than REMEASURE_AFTER_SEC."""
    if usage is None:
        return True
    if now - usage.measured_at < REMEASURE_AFTER_SEC:
        return False
    return usage.error is not None or running or last_activity_ts > usage.measured_at


def _read_jobs(project_dir: Path) -> dict[str, Any]:
    with open(project_dir / "project_params.json") as f:
        data = json.load(f)
    jobs = data.get("jobs")
    return jobs if isinstance(jobs, dict) else {}


class DiskUsageService:
    """Measures queued projects one at a time, off the event loop.

    `request()` runs on the event loop; `cached()` is called from the project scan's
    worker thread and only reads. Results land in `<project>/.disk_usage.json`; a
    project whose directory is not writable keeps its result in memory instead.
    """

    def __init__(self):
        self._queue: deque[str] = deque()
        self._pending: set[str] = set()
        self._memory: dict[str, DiskUsage] = {}
        self._unwritable_logged: set[str] = set()
        self._worker: asyncio.Task | None = None

    def cached(self, project_dir: Path) -> DiskUsage | None:
        """The newest known measurement: the shared cache file, or this process's own
        result when that is newer (or the file could not be written)."""
        on_disk = read_cache(project_dir)
        in_memory = self._memory.get(str(project_dir))
        if on_disk is None or (in_memory is not None and in_memory.measured_at > on_disk.measured_at):
            return in_memory
        return on_disk

    def is_pending(self, project_dir: Path) -> bool:
        return str(project_dir) in self._pending

    def request(self, project_dirs: list[str]) -> None:
        for path in project_dirs:
            if path in self._pending:
                continue
            self._pending.add(path)
            self._queue.append(path)
        if self._queue and (self._worker is None or self._worker.done()):
            self._worker = asyncio.get_running_loop().create_task(self._drain())

    async def _drain(self) -> None:
        while self._queue:
            path = self._queue.popleft()
            try:
                await asyncio.to_thread(self._measure_and_store, Path(path))
            finally:
                self._pending.discard(path)

    def _measure_and_store(self, project_dir: Path) -> None:
        try:
            usage = measure(project_dir, _read_jobs(project_dir))
        except FileNotFoundError:
            # Deleted while queued -- nothing to measure, nowhere to store it.
            return
        except Exception as e:
            logger.exception("Disk usage measurement failed for %s", project_dir)
            usage = DiskUsage(measured_at=time.time(), error=str(e))
        self._memory[str(project_dir)] = usage
        target = project_dir / CACHE_NAME
        # Per-process temp name: two servers may measure the same project at once.
        tmp = project_dir / f"{CACHE_NAME}.{os.getpid()}.tmp"
        try:
            tmp.write_text(usage.model_dump_json())
            os.replace(tmp, target)
        except OSError as e:
            if str(project_dir) not in self._unwritable_logged:
                self._unwritable_logged.add(str(project_dir))
                logger.warning("Disk usage for %s kept in memory only (cache not writable: %s)", project_dir, e)


_instance: DiskUsageService | None = None


def get_disk_usage_service() -> DiskUsageService:
    global _instance
    if _instance is None:
        _instance = DiskUsageService()
    return _instance
