"""Array jobs' task statuses, read from disk once per tick for the whole process.

Every browser tab's roster and the pipeline table show the same per-tilt-series statuses of the
array jobs. Each of them scanning `.task_status/` and the job dir on its own timer (the roster on
the event loop) multiplied the same reads by the number of tabs. `refresh` scans, in one thread
call, the array jobs whose entry is older than `max_age_s`; every reader then takes the cached
result with `get`, which never touches disk.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

from services.array_tasks import TaskProgress, manifest_items, scan_statuses

# Readers tick every 3 s; an entry younger than this is what any of them would have read.
_MAX_AGE_S = 2.5


@dataclass(frozen=True)
class ArrayStatus:
    """One array job's directory, its manifest items and each item's status (ok / fail / skip /
    running / pending)."""

    job_dir: Path
    items: tuple[str, ...]
    statuses: dict[str, str]

    @property
    def progress(self) -> TaskProgress:
        return TaskProgress.from_statuses(self.statuses)


_cache: dict[tuple[str, str], tuple[float, ArrayStatus | None]] = {}
_lock = asyncio.Lock()


def is_array_job(job_model) -> bool:
    return "array_throttle" in getattr(job_model, "USER_PARAMS", set())


def resolve_array_job_dir(job_model, project_path: Path | None = None) -> Path | None:
    """An array job's directory: its resolved `job_dir` path, else its RELION job dir. Disk (is_dir)."""
    if not job_model:
        return None
    stored = (job_model.paths or {}).get("job_dir")
    if stored:
        p = Path(stored)
        if p.is_dir():
            return p
    if project_path:
        rjn = getattr(job_model, "relion_job_name", None)
        if rjn:
            p = project_path / rjn.rstrip("/")
            if p.is_dir():
                return p
    return None


def scan_array_job(job_model, project_path: Path | None) -> ArrayStatus | None:
    """Read one array job's status from disk; None without a job dir or a manifest. For a thread."""
    job_dir = resolve_array_job_dir(job_model, project_path)
    if job_dir is None:
        return None
    items = manifest_items(job_dir)
    if not items:
        return None
    return ArrayStatus(job_dir, tuple(items), scan_statuses(job_dir, items))


async def refresh(project_path: Path, jobs: dict, max_age_s: float = _MAX_AGE_S) -> None:
    """Re-read the array jobs among `jobs` ({instance: job model}) whose entry is older than
    `max_age_s`. One scan runs at a time; a reader that waited for it finds the entries fresh."""
    key = str(project_path)

    def _due() -> list[tuple[str, object]]:
        now = time.monotonic()
        return [
            (iid, jm)
            for iid, jm in jobs.items()
            if is_array_job(jm) and now - _cache.get((key, iid), (float("-inf"), None))[0] > max_age_s
        ]

    if not _due():
        return
    async with _lock:
        due = _due()
        if not due:
            return
        found = await asyncio.to_thread(lambda: {iid: scan_array_job(jm, project_path) for iid, jm in due})
        stamp = time.monotonic()
        for iid, status in found.items():
            _cache[(key, iid)] = (stamp, status)


def get(project_path: Path | None, instance_id: str) -> ArrayStatus | None:
    """The last status read for an array job (None before its first `refresh`, or with no manifest)."""
    if project_path is None:
        return None
    entry = _cache.get((str(project_path), instance_id))
    return entry[1] if entry is not None else None
