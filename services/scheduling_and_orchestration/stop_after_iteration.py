"""Stop a relion_refine job (Refine 3D, Class 3D) once the iteration it is on has landed (roadmap 69 G3).

The request is a marker in the job's folder, so it outlives a server restart and whichever server follows the
project acts on it: `request` writes it with the last iteration on disk, the monitor's `due` sees a newer one land,
the job is cancelled (`PipelineRunnerService.cancel_job`, which marks it Failed) and the marker becomes the record of
where it stopped (`settle`). Run again in place, the driver goes on from that checkpoint
(`drivers/relion_continue.py`), so the stop costs nothing of the work done.

A Refine 3D can instead end at the last iteration that landed, with that iteration as its result (`finish`): a
stopped one, one a time limit or a cancel ended, or a running one cancelled for it. It then reads Succeeded, so a Run
starts the jobs after it instead of continuing it.

Blocking (globs and stats in the job folder): callers run these in a thread.
"""

from __future__ import annotations

import json
import re
import shutil
import time
from datetime import datetime
from pathlib import Path

import mrcfile
import numpy as np

from services.models_base import JobType

MARKER = ".stop_after_iteration.json"
DONE = ".stopped_after_iteration.json"
FINISHED = ".finished_at_iteration.json"
STOPPABLE = (JobType.REFINE3D, JobType.CLASS3D)
FINISHABLE = (JobType.REFINE3D,)
# What a finished Refine 3D hands on, under the names drivers/refine3d.py gives a converged run's (RESULT_FILES),
# and the file of an iteration each is made from. The map is the mean of the iteration's half maps.
_FROM_ITERATION = {
    "run_optimisation_set.star": "_optimisation_set.star",
    "run_half1_class001_unfil.mrc": "_half1_class001.mrc",
    "run_half2_class001_unfil.mrc": "_half2_class001.mrc",
}
_MAP = "run_class001.mrc"
_EXIT_MARKERS = ("RELION_JOB_EXIT_FAILURE", "RELION_JOB_EXIT_ABORTED")
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


def iteration_prefix(job_dir: Path, iteration: int) -> str | None:
    """The file prefix of `iteration` (run_it016, run_ct6_it016): a continuation writes the iteration it starts from
    again under its own name, so the newest optimiser star of that number wins. None when there is none."""
    found = [
        p
        for p in job_dir.glob("run*_optimiser.star")
        if (m := OPTIMISER_RE.fullmatch(p.name)) and int(m.group(1)) == iteration
    ]
    if not found:
        return None
    return max(found, key=lambda p: p.stat().st_mtime).name.removesuffix("_optimiser.star")


def finished(job_dir: Path) -> dict | None:
    """The record of a Refine 3D ended at an iteration (`finish`), or None."""
    try:
        return json.loads((job_dir / FINISHED).read_text())
    except FileNotFoundError:
        return None


def finish(job_dir: Path) -> dict:
    """End a Refine 3D at the last iteration that landed: that iteration's optimisation set and half maps become what
    the job hands on, under the names of a converged run's, with the mean of the half maps as its map; the exit
    marker says success; the record says where it ended. No run_model.star is written, so the job's Results read the
    iteration's own model and do not call it converged. Raises FileNotFoundError when no iteration has landed or one
    of its files is missing, FileExistsError when the run converged."""
    if (job_dir / "run_model.star").exists():
        raise FileExistsError(f"{job_dir.name} converged: its outputs are the converged run's")
    iteration = landed_iteration(job_dir)
    prefix = iteration_prefix(job_dir, iteration) if iteration else None
    if prefix is None:
        raise FileNotFoundError(f"no iteration of {job_dir.name} has landed")
    sources = {out: job_dir / f"{prefix}{suffix}" for out, suffix in _FROM_ITERATION.items()}
    missing = [p.name for p in sources.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"iteration {iteration} of {job_dir.name} lacks {', '.join(missing)}")

    for out, src in sources.items():
        shutil.copy2(src, job_dir / out)
    with (
        mrcfile.open(sources["run_half1_class001_unfil.mrc"], permissive=True) as h1,
        mrcfile.open(sources["run_half2_class001_unfil.mrc"], permissive=True) as h2,
    ):
        mean = (h1.data.astype(np.float32) + h2.data.astype(np.float32)) / 2
        voxel = h1.voxel_size
    with mrcfile.new(job_dir / _MAP, overwrite=True) as out:
        out.set_data(mean)
        out.voxel_size = voxel

    record = {
        "iteration": iteration,
        "from": prefix,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "map": f"{_MAP} = mean of {prefix}_half1_class001.mrc and _half2",
        "half_maps": "the iteration's half maps, as relion_refine writes them mid-run (not a final unfiltered pair)",
    }
    (job_dir / FINISHED).write_text(json.dumps(record))
    withdraw(job_dir)
    for name in _EXIT_MARKERS:
        (job_dir / name).unlink(missing_ok=True)
    (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
    return record
