"""Stop a relion_refine job (Refine 3D, Class 3D) once the iteration it is on has landed (roadmap 69 G3).

The request is a marker in the job's folder, so it outlives a server restart and whichever server follows the
project acts on it: `request` writes it with the last iteration on disk, the monitor's `due` sees a newer one land,
the job is cancelled (`PipelineRunnerService.cancel_job`, which marks it Failed) and the marker becomes the record of
where it stopped (`settle`). Run again in place, the driver goes on from that checkpoint
(`drivers/relion_continue.py`), so the stop costs nothing of the work done.

Blocking (globs and stats in the job folder): callers run these in a thread.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path

from services.models_base import JobType

MARKER = ".stop_after_iteration.json"
DONE = ".stopped_after_iteration.json"
STOPPABLE = (JobType.REFINE3D, JobType.CLASS3D)
# A finished iteration's optimiser star, as drivers/relion_continue.OPTIMISER_RE reads it; a continuation names its
# files run_ct<N>_it<M>_*.
OPTIMISER_RE = re.compile(r"run(?:_ct\d+)?_it(\d{3})_optimiser\.star")
# An iteration counts as landed once none of its files has changed for this long, so the cancel never catches
# relion_refine writing one; the monitor looks every PipelineMonitor.STOP_CHECK_SEC.
SETTLE_SEC = 30.0


def landed_iteration(job_dir: Path, now: float | None = None) -> int:
    """The last iteration relion_refine finished writing in `job_dir`: its optimiser star is there and none of its
    files changed in the last SETTLE_SEC. 0 before the first."""
    now = time.time() if now is None else now
    best = 0
    for optimiser in job_dir.glob("run*_optimiser.star"):
        m = OPTIMISER_RE.fullmatch(optimiser.name)
        if not m or int(m.group(1)) <= best:
            continue
        prefix = optimiser.name.removesuffix("_optimiser.star")
        newest = max(p.stat().st_mtime for p in job_dir.glob(f"{prefix}_*"))
        if now - newest >= SETTLE_SEC:
            best = int(m.group(1))
    return best


def pending(job_dir: Path) -> dict | None:
    """The request standing on this job (`asked_at`, `after_iteration`: it stops once a later one lands), or None."""
    try:
        return json.loads((job_dir / MARKER).read_text())
    except FileNotFoundError:
        return None


def request(job_dir: Path) -> dict:
    """Ask for the stop: the job ends once an iteration later than the last one on disk has landed."""
    marker = {"asked_at": datetime.now().isoformat(timespec="seconds"), "after_iteration": landed_iteration(job_dir)}
    (job_dir / MARKER).write_text(json.dumps(marker))
    return marker


def withdraw(job_dir: Path) -> bool:
    """Take the request back; False when there was none."""
    try:
        (job_dir / MARKER).unlink()
    except FileNotFoundError:
        return False
    return True


def due(job_dir: Path) -> int | None:
    """The iteration the job may stop after now (it has landed since the request), or None."""
    marker = pending(job_dir)
    if marker is None:
        return None
    landed = landed_iteration(job_dir)
    return landed if landed > int(marker.get("after_iteration", 0)) else None


def settle(job_dir: Path, stopped_after: int) -> None:
    """The request is done: the marker becomes the record of the iteration the job stopped after."""
    marker = pending(job_dir) or {}
    marker.update(stopped_after=stopped_after, stopped_at=datetime.now().isoformat(timespec="seconds"))
    (job_dir / DONE).write_text(json.dumps(marker))
    withdraw(job_dir)
