"""Rows and lanes of the read-only pipeline table.

One row per job, in dependency order, with a narrow gutter that draws one lane per open chain: a
dot on the job's row, a line down to each consumer, a fork where a job has a second consumer.
Only each job's main lineage is drawn (`MAIN_SLOTS`: the input its per-TS rows come from); side
inputs such as the alignment settings tsReconstruct reads would cross every other line.

No UI: `pipeline_rows` reads the project, `assign_lanes` does the geometry, `run_times` reads
when each job ran from its job dir.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from pathlib import Path
from collections.abc import Mapping
from typing import TYPE_CHECKING

from services.path_resolution_service import PathResolutionService, SlotEdge

if TYPE_CHECKING:
    from services.jobs._base import AbstractJobParams
    from services.project_state import ProjectState

# The input a job's data descends through, most direct first. A job without any of these takes its
# first job-fed input.
MAIN_SLOTS = ("input_star", "input_optimisation", "input_tomograms", "input_tm_job", "input_tiltseries")

_EXIT_MARKERS = ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE")


@dataclass(frozen=True)
class TableRow:
    """One job's row. `lane` holds its dot; `arrives`: its producer's line comes down that lane into
    the dot (False: no producer in the table), solid when `explicit` (its main input was fixed at
    submit or set by hand) and dashed when chosen automatically (it can still move). `through`:
    lanes whose lines pass the row untouched. `continues`: a line leaves the dot downward in `lane`;
    `forks`: further lanes that branch off the dot here, one per extra consumer. `solid`: of the
    lines that pass or leave this row, the lanes whose line leads to an `explicit` consumer.
    `inputs`: every input of the job fed by a job in the table, the main one included."""

    instance_id: str
    lane: int
    parent: str | None
    arrives: bool
    through: tuple[int, ...]
    forks: tuple[int, ...]
    continues: bool
    explicit: bool
    solid: tuple[int, ...] = ()
    inputs: tuple[SlotEdge, ...] = ()


@dataclass(frozen=True)
class RunTimes:
    """A job's last run, from its job dir, in epoch seconds (None: not on disk). `start`: the later
    of its submit (`run_submit.script` written) and the end of the last job it reads that ended
    before it did, i.e. from when SLURM could start it; `end`: its exit marker."""

    start: float | None
    end: float | None


def pipeline_rows(state: ProjectState) -> list[TableRow]:
    """The table of the jobs on the roster (`pipeline_order`)."""
    jobs = [iid for iid in (state.pipeline_order or []) if iid in state.jobs]
    included = set(jobs)
    by_consumer: dict[str, list[SlotEdge]] = {}
    for e in PathResolutionService(state).resolve_slot_edges(jobs):
        if e.producer in included:
            by_consumer.setdefault(e.consumer, []).append(e)

    rank = {slot: i for i, slot in enumerate(MAIN_SLOTS)}
    parent: dict[str, str | None] = {}
    explicit: dict[str, bool] = {}
    for iid in jobs:
        main = min(by_consumer.get(iid, []), key=lambda e: rank.get(e.slot, len(MAIN_SLOTS)), default=None)
        parent[iid] = main.producer if main else None
        explicit[iid] = bool(main and main.explicit)

    rows = assign_lanes(_dependency_order(jobs, by_consumer), parent, explicit)
    return [replace(r, inputs=tuple(by_consumer.get(r.instance_id, ()))) for r in rows]


def _dependency_order(jobs: list[str], by_consumer: dict[str, list]) -> list[str]:
    """Every producer before its consumers (all inputs, not only the main one), otherwise in
    roster order. A loop cannot be submitted; its jobs go last, in roster order."""
    position = {iid: i for i, iid in enumerate(jobs)}
    needs = {iid: {e.producer for e in by_consumer.get(iid, []) if e.producer != iid} for iid in jobs}
    order: list[str] = []
    placed: set[str] = set()
    while len(order) < len(jobs):
        ready = [iid for iid in jobs if iid not in placed and needs[iid] <= placed]
        if not ready:
            order.extend(iid for iid in jobs if iid not in placed)
            break
        nxt = min(ready, key=position.__getitem__)
        order.append(nxt)
        placed.add(nxt)
    return order


def assign_lanes(order: list[str], parent: dict[str, str | None], explicit: dict[str, bool]) -> list[TableRow]:
    """Lanes for `order` (producers first), each job drawn under its `parent`.

    A job's first consumer in the order continues straight down its lane; each further consumer
    gets a lane forked off at the job's own row, the first free one to its right, so a branch never
    crosses the lines to its left. A job with no producer in the table starts in the leftmost free
    lane. A lane is free again once the consumer it was held for has its dot."""
    consumers: dict[str, list[str]] = {}
    placed: set[str] = set()
    for iid in order:
        p = parent.get(iid)
        # A consumer ahead of its producer (only in a loop) starts a lane of its own instead.
        if p in placed:
            consumers.setdefault(p, []).append(iid)
        placed.add(iid)

    awaiting: list[str | None] = []  # per lane: the consumer its line runs down to, or None when free
    rows: list[TableRow] = []

    def free_lane(right_of: int = -1) -> int:
        for i in range(right_of + 1, len(awaiting)):
            if awaiting[i] is None:
                return i
        awaiting.append(None)
        return len(awaiting) - 1

    for iid in order:
        arrives = iid in awaiting
        lane = awaiting.index(iid) if arrives else free_lane()
        awaiting[lane] = None
        through = tuple(i for i, c in enumerate(awaiting) if c is not None)
        kids = consumers.get(iid, [])
        if kids:
            awaiting[lane] = kids[0]
        forks: list[int] = []
        for kid in kids[1:]:
            fork = free_lane(lane)
            awaiting[fork] = kid
            forks.append(fork)
        solid = tuple(i for i, c in enumerate(awaiting) if c is not None and explicit.get(c, False))
        while awaiting and awaiting[-1] is None:
            awaiting.pop()
        rows.append(
            TableRow(
                instance_id=iid,
                lane=lane,
                parent=parent.get(iid) if arrives else None,
                arrives=arrives,
                through=through,
                forks=tuple(forks),
                continues=bool(kids),
                explicit=explicit.get(iid, False),
                solid=solid,
            )
        )
    return rows


def run_times(jobs: Mapping[str, AbstractJobParams], project_path: Path, rows: list[TableRow]) -> dict[str, RunTimes]:
    """`RunTimes` of every row's job (`jobs`: the rows' job models). Three stats per job: call it off
    the event loop."""
    submitted: dict[str, float | None] = {}
    ended: dict[str, float | None] = {}
    for row in rows:
        rel = (jobs[row.instance_id].relion_job_name or "").rstrip("/")
        job_dir = project_path / rel if rel else None
        submitted[row.instance_id] = _mtime(job_dir / "run_submit.script") if job_dir else None
        marks = [_mtime(job_dir / name) for name in _EXIT_MARKERS] if job_dir else []
        ended[row.instance_id] = max((m for m in marks if m is not None), default=None)

    now = time.time()
    times: dict[str, RunTimes] = {}
    for row in rows:
        iid, end = row.instance_id, ended[row.instance_id]
        if submitted[iid] is None:
            times[iid] = RunTimes(None, end)
            continue
        horizon = end if end is not None else now
        inputs_done = [t for e in row.inputs if (t := ended.get(e.producer)) is not None and t <= horizon]
        times[iid] = RunTimes(max([submitted[iid], *inputs_done]), end)
    return times


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        # Not written (yet): a job that was never submitted this way, or has not ended.
        return None
