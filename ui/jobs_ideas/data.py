"""What the jobs prototypes show: one real project, read and never written, as the branch map's `Project` model plus
what the roster needs on top of it (the canonical order of job types, which jobs have tasks, each task's log).

`simulate` repaints the per-tilt-series states of a few jobs so every state the prototypes draw (running, waiting,
failed) can be looked at on a project that has finished. The page says so wherever it is on.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path

from services import array_status_cache
from services.array_tasks import read_tail, ts_pretty_name
from services.jobs.spec import JOB_SPECS, PHASE_PARTICLES, PHASE_PREPROCESSING
from services.project_state import ProjectState
from ui.branch_map.model import Job, Project, project_from_state

logger = logging.getLogger(__name__)

DEFAULT_PROJECT = Path("/groups/<lab>/_crboost_data_api/<project>")

# One line per job type: what it does, for the magazine's header. In a build this belongs on JobSpec.
DESCRIPTION = {
    "importmovies": "Reads the raw movies and their .mdoc files into the project.",
    "fsMotionAndCtf": "Aligns each movie's frames and fits a CTF per tilt (WarpTools fs_motion_and_ctf).",
    "tsImport": "Groups the motion-corrected tilts into tilt series and writes Warp's settings.",
    "tiltFilter": "Scores every tilt and takes the bad ones (beam-blocked, drifted, empty) out of the series.",
    "aligntiltsWarp": "Finds each tilt series' geometry: shifts, the tilt axis, the tilt angles.",
    "missAlign": "Refines an alignment with the learned miss-alignment model.",
    "tsCtf": "Fits the defocus per tilt series, with the tilts' hand and geometry.",
    "tsReconstruct": "Back-projects each tilt series into a tomogram at the chosen pixel size.",
    "denoisetrain": "Trains a denoising model on the tomograms' even/odd halves.",
    "denoisepredict": "Denoises every tomogram with a trained model, or with a pretrained one.",
    "segment": "Segments each tomogram into membranes, filaments and other features.",
    "subset": "A cut of another job's tilt series, picked by hand or by a rule.",
    "templatematching": "Scores every orientation of a template at every voxel (PyTOM), one tomogram per GPU.",
    "tmextractcand": "Picks candidates from the score maps: peaks above a threshold, a particle apart.",
    "filamentPick": "Places points along the filaments of one segmented feature.",
    "subtomoExtraction": "Cuts the particles out of the tomograms (RELION).",
    "warpExport": "Exports the particles as 2-D stacks per tilt (WarpTools ts_export_particles).",
    "reconstructParticle": "Averages the extracted particles into a first map.",
    "class3d": "Sorts the particles into classes by 3-D classification.",
    "refine3d": "Gold-standard refinement of one class against two random halves.",
    "maskCreate": "A soft solvent mask from a map.",
    "postprocess": "Gold-standard FSC and the sharpened map from a refinement's halves.",
}

# The canonical order of the job types the roster shows, by phase. tsImport and the per-pick-list extraction have no
# phase: the roster never shows them.
PHASES: list[tuple[str, str, list[str]]] = [
    (PHASE_PREPROCESSING, "Preprocessing", [s.job_type.value for s in JOB_SPECS if s.phase == PHASE_PREPROCESSING]),
    (PHASE_PARTICLES, "Particles", [s.job_type.value for s in JOB_SPECS if s.phase == PHASE_PARTICLES]),
]
ADDABLE = {s.job_type.value for s in JOB_SPECS if s.roster_add}
DISPLAY = {s.job_type.value: s.display_name for s in JOB_SPECS}
STAGE_ORDER = [s.job_type.value for s in JOB_SPECS]


@dataclass
class Data:
    pr: Project
    path: Path
    has_tasks: set[str]  # instance ids of the array jobs: the ones a magazine opens for
    items: dict[str, list[str]] = field(default_factory=dict)  # instance id -> its tasks' items in array order
    simulated: bool = False

    def job(self, iid: str) -> Job:
        return self.pr.by_id[iid]

    def instances(self, kind: str) -> list[Job]:
        return [j for j in self.pr.jobs if j.kind == kind and j.on_roster]

    def name(self, j: Job) -> str:
        return DISPLAY.get(j.kind, j.kind)

    def tasks(self, j: Job) -> list[tuple[str, str]]:
        """(tilt series id, state) for each task of the job, in the project's tilt-series order. A job not run yet
        has a waiting task per tilt series."""
        if j.statuses is None:
            return [(t, "unrun") for t in self.pr.ts_ids]
        return [(t, s) for t, s in zip(self.pr.ts_ids, j.statuses, strict=False) if s != "none"]

    def counts(self, j: Job) -> dict[str, int]:
        out: dict[str, int] = {}
        for _, s in self.tasks(j):
            out[s] = out.get(s, 0) + 1
        return out

    def ts_short(self, ts: str) -> str:
        """Pos·Beam, the shortest name that tells a tilt series apart in this project."""
        title = self.pr.ts_title(ts)
        if title.startswith("Pos "):
            pos, _, beam = title.removeprefix("Pos ").partition(" · Beam ")
            return f"{pos}·{beam}"
        return ts_pretty_name(ts)


def grid_cols(n: int, *, max_cols: int = 16) -> int:
    """Columns for n marks laid out as square as the count allows."""
    return max(1, min(max_cols, math.ceil(math.sqrt(max(n, 1)))))


async def load(path: Path, *, simulate: bool) -> Data:
    """The project at `path`, read-only: its state, its array jobs' task states, the branch map's model of it."""
    state = await asyncio.to_thread(ProjectState.load, path / "project_params.json", read_only=True)
    await array_status_cache.refresh(path, state.jobs)
    pr = await asyncio.to_thread(project_from_state, state, path, set_parents={})
    has_tasks = {iid for iid, jm in state.jobs.items() if "array_throttle" in getattr(jm, "USER_PARAMS", set())}
    items = {}
    for iid in has_tasks:
        st = array_status_cache.get(path, iid)
        if st is not None:
            items[iid] = list(st.items)
    data = Data(pr, path, has_tasks, items)
    if simulate:
        _simulate(data)
    return data


def _simulate(data: Data) -> None:
    """A run in flight on the last array jobs: one running with a few failures and the rest waiting in the queue, one
    queued, one failed. Only the tilt series each job has are repainted."""
    pr = data.pr
    arrays = [j for j in pr.jobs if j.id in data.has_tasks and j.on_roster and j.statuses is not None]
    if not arrays:
        return

    def paint(j: Job, rule) -> None:
        own = [i for i, s in enumerate(j.statuses) if s != "none"]
        st = list(j.statuses)
        for k, i in enumerate(own):
            st[i] = rule(k, len(own))
        j.statuses, j.ok = st, st.count("ok")

    def running(k: int, n: int) -> str:
        if k in (int(n * 0.2), int(n * 0.31)):
            return "fail"
        if k < n * 0.45:
            return "ok"
        if k < n * 0.45 + 6:
            return "running"
        return "skip" if k % 17 == 5 else "pending"

    paint(arrays[-1], running)
    arrays[-1].status = "running"
    if len(arrays) > 1:
        paint(arrays[-2], lambda k, n: "pending")
        arrays[-2].status = "queued"
    if len(arrays) > 2:
        paint(arrays[-3], lambda k, n: "fail" if k % 7 == 0 else "ok")
        arrays[-3].status = "failed"
    data.simulated = True


def task_detail(data: Data, j: Job, ts: str) -> dict:
    """What the task card shows for one tilt series of one job: the task's index, its log's path and tail. Runs in a
    thread: it reads the log."""
    items = data.items.get(j.id) or []
    idx = items.index(ts) if ts in items else None
    out: dict = {"idx": idx, "log": None, "tail": "", "err": None, "err_tail": ""}
    if j.job_dir is None or idx is None:
        return out
    log = j.job_dir / f"task_{idx}.out"
    err = j.job_dir / f"task_{idx}.err"
    if log.exists():
        out["log"], out["tail"] = str(log), _last(read_tail(log, 40), 14)
    if err.exists() and err.stat().st_size:
        out["err"], out["err_tail"] = str(err), _last(read_tail(err, 40), 8)
    return out


def _last(text: str, n: int) -> str:
    return "\n".join(text.rstrip().splitlines()[-n:])
