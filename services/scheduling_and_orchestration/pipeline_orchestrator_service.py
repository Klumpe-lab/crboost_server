import asyncio
import logging
import pandas as pd
from pathlib import Path
from typing import Any, ClassVar

from services.array_tasks import recorded_array_job_id
from services.computing.slurm_service import SlurmConfig, normalize_slurm_ids, write_sbatch_script
from services.configs.config_service import get_config_service
from services.configs.starfile_service import StarfileService
from services.job_models import ImportMoviesParams
from services.jobs._base import SubmittedInput
from services.jobs.spec import JOB_SPEC_BY_TYPE, JOB_SPECS, driver_invocation
from services.jobs.tilt_filter import FilterMode, dl_auto_filters, review_barrier, verdict_edges
from services.path_resolution_service import PathResolutionError, PathResolutionService, find_cycle, get_context_paths
from services.models_base import InstanceId, SegmentMethod
from services.event_log import events
from services.result import err, ok
from services.project_state import AbstractJobParams, JobCategory, JobType, JobStatus, ReviewHold
from services.segmentation.search_region import search_region_problem
from services.subsets import subset_problem, write_subset_star
from services.tilt_series import get_registry_for
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)


def _toposort_submit_order(nodes: list[str], edges: list[tuple]) -> tuple:
    """Kahn's topological sort over (producer, consumer) edges restricted to ``nodes``.

    Returns ``(ordered_nodes, predecessors)`` where ``predecessors[c]`` is the set of
    producer instance_ids of ``c`` within ``nodes``. Raises ``ValueError`` on a cycle.
    Seeds the queue in ``nodes`` order so independent jobs keep their pipeline/UI order.
    """
    from collections import deque

    nodeset = set(nodes)
    indeg = {n: 0 for n in nodes}
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    preds: dict[str, set] = {n: set() for n in nodes}
    for producer, consumer in edges:
        if producer not in nodeset or consumer not in nodeset or producer == consumer:
            continue
        if producer in preds[consumer]:
            continue  # dedup (resolve_edges already dedups; defensive)
        preds[consumer].add(producer)
        succ[producer].append(consumer)
        indeg[consumer] += 1

    queue = deque([n for n in nodes if indeg[n] == 0])
    order: list[str] = []
    while queue:
        n = queue.popleft()
        order.append(n)
        for m in succ[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)

    if len(order) != len(nodes):
        stuck = nodeset - set(order)
        raise ValueError(f"cycle among {sorted(stuck)}")
    return order, preds


def _restore_allocation(state, previous: dict[str, tuple]) -> None:
    """Hand each job back the dir it held before a refused submit: `previous` maps instance_id to
    its (relion_job_name, relion_job_number, job_path_mapping entry) from before allocation."""
    for instance_id, (relion_job_name, relion_job_number, mapped) in previous.items():
        job_model = state.jobs[instance_id]
        job_model.relion_job_name = relion_job_name
        job_model.relion_job_number = relion_job_number
        if mapped is None:
            state.job_path_mapping.pop(instance_id, None)
        else:
            state.job_path_mapping[instance_id] = mapped


def _reused_dir(job_model: AbstractJobParams, project_dir: Path) -> str | None:
    """The dir a FAILED job is retried in, in place, when a run reaches it: its own, where
    `.task_status/*.ok` keeps the tilt series its last attempt finished. None for a job that
    takes a fresh dir."""
    rel = (job_model.relion_job_name or "").rstrip("/")
    if (
        job_model.execution_status == JobStatus.FAILED
        and rel
        and job_model.relion_job_number is not None
        and (project_dir / rel).is_dir()
    ):
        return rel
    return None


def _inputs_moved(job_model: AbstractJobParams, resolved: dict[str, str]) -> bool:
    """Whether `resolved` reads other input files than the job's last attempt did (its paths).
    Only input slots count; its own outputs and context paths stay with its dir."""
    before = job_model.paths or {}
    return any(before.get(s.key) != resolved.get(s.key) for s in getattr(type(job_model), "INPUT_SCHEMA", None) or [])


class PipelineOrchestratorService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.job_resolver = JobTypeResolver(self.star_handler)
        self._submit_locks: dict[Path, asyncio.Lock] = {}

    def _submit_lock(self, project_dir: Path) -> asyncio.Lock:
        """One submit at a time per project, shared by deploy and submit_parked. Deploy's guard
        reads pipeline_active, which a submit sets only after its sbatch awaits, so without the
        lock two quick Runs could both pass the guard and submit the chain twice."""
        return self._submit_locks.setdefault(project_dir.resolve(), asyncio.Lock())

    async def deploy_and_run_scheme(self, project_dir: Path, selected_instance_ids: list[str]) -> dict[str, Any]:
        async with self._submit_lock(project_dir):
            return await self._deploy_locked(project_dir, selected_instance_ids)

    async def _deploy_locked(self, project_dir: Path, selected_instance_ids: list[str]) -> dict[str, Any]:
        if not selected_instance_ids:
            return err("No jobs selected.")

        state = self.backend.state_service.state_for(project_dir)

        # A run starts what is not live, around what is: a running pipeline does not refuse it.
        # Membership and order are the roster's, which add and remove keep in pipeline_order. A run
        # only adds what it is given and pipeline_order lacks (a job the tab restored at load), so a
        # run beside live jobs never reorders or drops one.
        missing = [iid for iid in selected_instance_ids if iid not in state.pipeline_order]
        if missing:
            state.pipeline_order = [*state.pipeline_order, *missing]
            state.mark_dirty()

        instances_to_run: list[str] = []
        live: list[str] = []
        for instance_id in selected_instance_ids:
            job_model = state.jobs.get(instance_id)
            status = job_model.execution_status if job_model else None
            if status in (JobStatus.QUEUED, JobStatus.RUNNING):
                # Left as it is; a job of this run that reads it waits for its SLURM id (_submit_chain).
                live.append(instance_id)
            elif status != JobStatus.SUCCEEDED:
                # Interactive jobs are never dispatched to the cluster.
                if job_model and getattr(job_model, "IS_INTERACTIVE", False):
                    continue
                instances_to_run.append(instance_id)

        if not instances_to_run:
            if live:
                message = "Nothing to start: every job is finished, queued or running."
            else:
                message = "All selected jobs are already finished."
            return ok(already_complete=True, message=message, pid=0)

        # A run that includes a job the review parked decides afresh what waits for it; a run beside
        # the review leaves the hold to Approve.
        hold = state.review_hold
        if hold is not None and set(hold.parked) & set(instances_to_run):
            state.review_hold = None
            state.mark_dirty()

        # A DL-auto tilt filter rewrites the predictions a prediction run in flight is writing.
        for iid in dl_auto_filters(state, instances_to_run):
            if state.jobs[iid].predict_in_flight:
                return err(
                    f"A DL prediction run of the tilt filter ({iid}) is in flight; run the pipeline once it has "
                    "landed or been cancelled."
                )

        # An unapproved tilt filter in the run parks alignment, which applies its verdict, and
        # everything downstream of it; the rest runs during the review, and Approve submits the
        # parked jobs (submit_parked).
        barrier = review_barrier(state, selected_instance_ids)
        parked = self._parked_behind_review(state, instances_to_run) if barrier else []
        if parked:
            instances_to_run = [iid for iid in instances_to_run if iid not in parked]
            events.info("%s · %s wait for the %s review", project_dir.name, ", ".join(parked), barrier)
            if not instances_to_run:
                state.review_hold = ReviewHold(barrier=barrier, parked=parked)
                await self.backend.state_service.save_project(project_path=project_dir, force=True)
                # DL review scores the tilts by itself once fsMotion has run.
                await self.backend.autostart_tilt_filter_predict(project_dir)
                return ok(
                    waiting_for_review=True,
                    parked=parked,
                    message=f"Nothing to start: {len(parked)} job(s) wait for the tilt-filter review.",
                )

        events.info("%s · started %s", project_dir.name, ", ".join(instances_to_run))

        # Submit the whole remaining set (FAILED + fresh) as SLURM dependencies. _submit_chain
        # allocates External/jobNNN dirs and wires the afterok edges; edges to already-SUCCEEDED
        # producers are dropped (their outputs exist on disk).
        if parked:
            # Recorded before the submit awaits anything, so an Approve landing meanwhile finds
            # the hold; its submit_parked waits on the lock for this submit to finish.
            state.review_hold = ReviewHold(barrier=barrier, parked=parked)
        res = await self._submit_chain(project_dir=project_dir, instances_to_run=instances_to_run, state=state)
        if parked and res["success"]:
            res["parked"] = parked
        elif not res["success"] and not res.get("submitted"):
            # Nothing reached SLURM: the review holds what it held before this Run.
            state.review_hold = hold
            state.mark_dirty()
        if state.review_hold is not None:
            # DL review scores the tilts by itself: now when fsMotion has run, else on its SUCCEEDED edge.
            await self.backend.autostart_tilt_filter_predict(project_dir)
        return res

    @staticmethod
    def _parked_behind_review(state, instances_to_run: list[str]) -> list[str]:
        """What an unapproved tilt filter holds back: the run's alignment jobs and every job of
        the run downstream of them (a closure over resolve_edges), in run order. Empty when the
        run does not include alignment. The rest reads no verdict and runs during the review."""
        held = {
            iid
            for iid in instances_to_run
            if (jm := state.jobs.get(iid)) is not None and jm.job_type == JobType.TS_ALIGNMENT
        }
        if not held:
            return []
        consumers: dict[str, list[str]] = {}
        for producer, consumer in PathResolutionService(state).resolve_edges(instances_to_run):
            consumers.setdefault(producer, []).append(consumer)
        frontier = list(held)
        while frontier:
            for consumer in consumers.get(frontier.pop(), []):
                if consumer not in held:
                    held.add(consumer)
                    frontier.append(consumer)
        return [iid for iid in instances_to_run if iid in held]

    async def submit_parked(self, project_dir: Path) -> dict[str, Any]:
        """Approve's resume: submit the jobs a run parked behind the tilt filter, now approved.
        A switch of the filter to DL auto resumes the same way, with the filter's own job
        submitted ahead of them (_submit_chain's verdict edge), unless it has committed already.

        Their job dirs, paths and job.star are made now, so edits made during the review apply,
        and producers still queued or running gate them by afterok (_submit_chain). Parked jobs
        that left the pipeline, or have run or been queued since, drop out. Refused, keeping the
        hold, while the filter is unapproved in a review mode or a producer of a parked job has
        neither run nor been queued."""
        async with self._submit_lock(project_dir):
            state = self.backend.state_service.state_for(project_dir)
            hold = state.review_hold
            if hold is None:
                return ok(submitted=[], message="No jobs are waiting for a review.")
            filter_job = state.jobs.get(hold.barrier)
            auto = filter_job is not None and filter_job.mode == FilterMode.DL_AUTO
            if filter_job is None or (filter_job.execution_status != JobStatus.SUCCEEDED and not auto):
                return err(f"The tilt filter ({hold.barrier}) is not approved; the parked jobs keep waiting.")

            done_or_live = (JobStatus.SUCCEEDED, JobStatus.QUEUED, JobStatus.RUNNING)
            parked = [
                iid
                for iid in hold.parked
                if (jm := state.jobs.get(iid)) is not None and jm.execution_status not in done_or_live
            ]
            if auto and filter_job.execution_status not in done_or_live:
                parked.insert(0, hold.barrier)
            for producer, consumer in PathResolutionService(state).resolve_edges(parked):
                status = state.jobs[producer].execution_status
                if producer not in parked and status not in done_or_live:
                    return err(
                        f"{consumer} needs {producer}, which is {status.value.lower()}, so the parked jobs "
                        f"keep waiting. Run the pipeline to start {producer} and them."
                    )

            state.review_hold = None
            state.mark_dirty()
            if not parked:
                await self.backend.state_service.save_project(project_path=project_dir, force=True)
                return ok(submitted=[], message="None of the parked jobs is left to run.")
            why = "runs in DL auto" if auto else "approved"
            events.info("%s · %s %s; submitting %s", project_dir.name, hold.barrier, why, ", ".join(parked))
            res = await self._submit_chain(project_dir=project_dir, instances_to_run=parked, state=state)
            if not res["success"] and not res.get("submitted"):
                # Nothing reached SLURM: the parked jobs go on waiting for Approve.
                state.review_hold = hold
                state.mark_dirty()
            return res

    async def _live_in_reused_dirs(self, project_dir: Path, state, instances_to_run: list[str]) -> str | None:
        """Why the run must wait, or None: a FAILED job it would retry in its own dir still has work
        in SLURM there. Its supervisor can be alive while its status reads Failed; more often a
        supervisor killed at its time limit has left its array running, and a resubmit would
        dispatch those tilt series a second time while a second supervisor rebuilt the inputs they
        read. Refused rather than cancelled: the live tasks are doing the work a rerun would repeat.
        One squeue for every id; a failed squeue, or a task manifest that does not parse, refuses
        too."""
        reused = {
            iid: rel
            for iid in instances_to_run
            if (jm := state.jobs.get(iid)) is not None and (rel := _reused_dir(jm, project_dir))
        }
        if not reused:
            return None

        def read_array_ids() -> tuple[dict[str, str | None], list[str]]:
            arrays: dict[str, str | None] = {}
            unreadable: list[str] = []
            for iid, rel in reused.items():
                try:
                    arrays[iid] = recorded_array_job_id(project_dir / rel)
                except (OSError, ValueError) as e:
                    unreadable.append(f"{iid} ({Path(rel).name}): {e}, so whether its array still runs is unknown.")
            return arrays, unreadable

        arrays, unreadable = await asyncio.to_thread(read_array_ids)
        if unreadable:
            return " ".join(unreadable)
        # Per job, its supervisor first (a live one is the job itself still running), then its array.
        watched: dict[str, list[tuple[str, str]]] = {}
        for iid in reused:
            pairs = (("slurm", state.jobs[iid].slurm_job_id), ("array", arrays[iid]))
            watched[iid] = [(kind, str(jid)) for kind, jid in pairs if jid]
        ids = [jid for pairs in watched.values() for _, jid in pairs]
        live = await self.backend.slurm_service.live_task_counts(ids) if ids else {}
        if live is None:
            names = ", ".join(f"{iid} ({Path(rel).name})" for iid, rel in reused.items())
            return f"Could not ask SLURM whether {names} still run (squeue failed); try again."
        reasons: list[str] = []
        for iid, pairs in watched.items():
            name = Path(reused[iid]).name
            kind, jid = next(((kind, jid) for kind, jid in pairs if jid in live), (None, None))
            if kind == "slurm":
                reasons.append(f"{iid} ({name}) is still running as slurm {jid}; if it never ends, scancel {jid}.")
            elif kind == "array":
                n = live[jid]
                reasons.append(
                    f"{iid} ({name}) is still running as array {jid} ({n} task{'' if n == 1 else 's'}). Run again "
                    f"once it drains; the supervisor then aggregates what finished. If it never drains, scancel {jid}."
                )
        return " ".join(reasons) or None

    async def _submit_chain(self, project_dir: Path, instances_to_run: list[str], state) -> dict[str, Any]:
        """Submit the pipeline as a SLURM afterok DAG.

        Three passes, so a refused Start leaves nothing behind:

        1. Allocate. Each fresh job takes a stable External/jobNNN dir from the ProjectState
           counter (monotonic, no reuse -> no off-by-one). A FAILED job that already owns a dir is
           the one exception: it is re-submitted in place on that dir (no counter slot consumed) so
           its `.task_status/*.ok` survives and the supervisor reruns only the failed/missing
           items. Every job of the run holds its dir before anything is resolved.
        2. Resolve, from that one snapshot: every job's paths and the DAG's wires come from the
           same resolver, so they cannot disagree. All unresolved or ambiguous inputs are reported
           together, and a loop as the wires that form it; either refusal hands every job back its
           previous dir, and nothing has touched the disk.
        3. Write each job dir with its RELION-compat job.star and run_submit.script (or the inline
           output), then sbatch each supervisor in topological order with --dependency=afterok on
           its producers' supervisor job ids, including producers outside the submitted set that
           are still queued or running (an earlier run's, or the upstream of parked jobs, submit_parked).

        This method submits and persists slurm_job_id + QUEUED + pipeline_active=True. The monitor
        then ticks reconcile_afterok, which owns live status and winds pipeline_active back down on
        completion. The chain executes in SLURM regardless of the headnode process.

        Failure handling: the counter is committed once pass 3 starts, however it ends, because a
        job dir may then exist on disk and its number must never be handed out again. It reaches
        disk before the first sbatch, with every job's dir and paths, and each slurm_job_id as its
        sbatch returns, so neither a mid-chain sbatch error nor a server that dies mid-submit loses
        a handle or hands out a dir something was submitted into.
        """
        server_dir = Path(__file__).parent.parent.parent.resolve()

        # A Subset is written in pass 3 from its cut's star; whatever stops that is said now, before
        # anything is allocated.
        check = PathResolutionService(state)
        not_ready = [
            problem
            for iid in instances_to_run
            if (jm := state.jobs.get(iid)) is not None
            and jm.job_type == JobType.SUBSET
            and (problem := subset_problem(state, check, iid))
        ]
        if not_ready:
            return err("Subset not ready: " + "; ".join(not_ready))

        still_running = await self._live_in_reused_dirs(project_dir, state, instances_to_run)
        if still_running:
            return err(still_running)

        # Seed the sole job-dir allocator once from the existing RELION counter, then own it.
        if not state.job_dir_counter:
            state.job_dir_counter = max(self._get_current_relion_counter(project_dir), 1)
        # Passes 1 and 2 repeat when a FAILED job cannot be retried in place: its dir keeps the items
        # its last attempt finished, so it is reused only while its inputs are the ones that attempt
        # read. One whose inputs changed (inputs reset, or an upstream that starts over in a new dir)
        # takes a fresh dir instead, which moves its consumers' inputs in turn; each round sends at
        # least one more job to a fresh dir, so this ends.
        start_over: set[str] = set()
        while True:
            # Allocate from a local counter; it reaches state.job_dir_counter only in pass 3.
            next_job_num = state.job_dir_counter

            # 1. Allocate.
            previous: dict[str, tuple] = {}  # instance_id -> (relion_job_name, relion_job_number, job_path_mapping)
            job_dirs: dict[str, Path] = {}
            reused: list[str] = []
            for instance_id in instances_to_run:
                job_model = state.jobs.get(instance_id)
                if not job_model:
                    try:
                        job_type = InstanceId.parse(instance_id).job_type
                    except ValueError:
                        _restore_allocation(state, previous)
                        return err(f"Unknown job type for instance '{instance_id}'")
                    state.ensure_job_initialized(job_type, instance_id=instance_id)
                    job_model = state.jobs.get(instance_id)

                previous[instance_id] = (
                    job_model.relion_job_name,
                    job_model.relion_job_number,
                    state.job_path_mapping.get(instance_id),
                )
                category = JobCategory.IMPORT if job_model.job_type == JobType.IMPORT_MOVIES else JobCategory.EXTERNAL

                # Dir allocation. Fresh jobs consume a slot from the sole monotonic allocator
                # (job_dir_counter), never reused -- so crboost's numbers can't drift off-by-one.
                # The one exception: a FAILED job that already owns a dir is re-submitted in place on
                # that same dir, so the supervisor's submit_array_job sees the prior `.task_status/*.ok`
                # and reruns only the failed/missing tilt-series instead of all of them. This reuses a
                # dir crboost itself allocated and recorded (relion_job_number) and consumes no counter
                # slot. An off-by-one needs a second allocator assigning the next number while a stale
                # one is reused; there is none, so reuse here is safe.
                reuse_dir = None if instance_id in start_over else _reused_dir(job_model, project_dir)
                if reuse_dir:
                    rel = reuse_dir
                    job_num = job_model.relion_job_number
                    reused.append(instance_id)
                else:
                    job_num = next_job_num
                    next_job_num += 1
                    rel = f"{category.value}/job{job_num:03d}"
                job_model.relion_job_name = rel + "/"
                job_model.relion_job_number = job_num
                state.job_path_mapping[instance_id] = rel
                job_dirs[instance_id] = project_dir / rel

            # 2. Resolve. One resolver, built after every dir above is held, gives the paths, the
            # recorded inputs and the wires.
            resolver = PathResolutionService(state, active_instance_ids=set(instances_to_run))
            resolved: dict[str, dict[str, str]] = {}
            recorded: dict[str, dict[str, SubmittedInput]] = {}
            unresolved: list[str] = []
            for instance_id in instances_to_run:
                job_model = state.jobs[instance_id]
                job_dir = job_dirs[instance_id]
                try:
                    io_paths = resolver.resolve_all_paths(
                        job_model.job_type, job_model, job_dir, instance_id=instance_id
                    )
                except PathResolutionError as e:
                    job_model.is_orphaned = True
                    job_model.missing_inputs = [str(e)]
                    unresolved.append(f"{instance_id}: {e}")
                    continue
                job_model.is_orphaned = False
                job_model.missing_inputs = []
                context_paths = get_context_paths(job_model.job_type, job_model, job_dir)
                resolved[instance_id] = {k: str(v) for k, v in {**context_paths, **io_paths}.items() if v is not None}
                recorded[instance_id] = resolver.record_inputs(instance_id)
            if unresolved:
                _restore_allocation(state, previous)
                return err("Path resolution failed for " + "; for ".join(unresolved))

            changed = [iid for iid in reused if _inputs_moved(state.jobs[iid], resolved[iid])]
            if not changed:
                break
            _restore_allocation(state, previous)
            start_over.update(changed)
            for iid in changed:
                events.info(
                    "%s · %s: inputs differ from its last attempt; it starts over in a new job dir",
                    project_dir.name,
                    iid,
                )
        for iid in reused:
            logger.info("Reusing FAILED job dir %s for in-place retry of %s", job_dirs[iid], iid)

        # A template match restricted to a search region needs every feature it names on every tomogram
        # it will match; what lacks one is said now, not by a failed task an hour later.
        region_problems = []
        excluded: set[str] | None = None
        for iid in instances_to_run:
            job_model = state.jobs[iid]
            if not getattr(job_model, "restricts_search", False):
                continue
            if excluded is None:
                excluded = get_registry_for(project_dir).excluded_ids()
            producer = state.jobs.get(resolver.source_of(iid, "input_segmentations") or "")
            planned = (
                list(producer.features)
                if producer is not None
                and producer.job_type == JobType.SEGMENT
                and producer.method == SegmentMethod.EASYMODE
                else None
            )
            problem = await asyncio.to_thread(
                search_region_problem, job_model, resolved[iid], planned=planned, excluded=excluded
            )
            if problem:
                region_problems.append(f"{iid}: {problem}")
        if region_problems:
            _restore_allocation(state, previous)
            return err("Search region not ready — " + "; ".join(region_problems))

        # Derive the dependency DAG, restrict to the SUBMITTED set, toposort (cycle-checked).
        # Inline-completed providers (RUNS_INLINE, e.g. IMPORT) are never submitted; an edge from
        # one to a submitted consumer drops out here, so the consumer submits with no afterok dep --
        # correct, since pass 3 writes the producer's output before the chain runs.
        slot_edges = resolver.resolve_slot_edges(instances_to_run)
        # A DL-auto filter already queued or running is not in the run, yet the alignments of the run
        # read the verdict it has not committed yet: they wait for it too (live_after below).
        live_filters = [
            iid
            for iid in dl_auto_filters(state, state.jobs)
            if state.jobs[iid].execution_status in (JobStatus.QUEUED, JobStatus.RUNNING)
        ]
        edges = [(e.producer, e.consumer) for e in slot_edges] + verdict_edges(
            state, [*instances_to_run, *live_filters]
        )
        submit_ids = [iid for iid in instances_to_run if not getattr(state.jobs[iid], "RUNS_INLINE", False)]
        submit_set = set(submit_ids)
        # A producer outside the submitted set that is still queued or running gates its consumers
        # by its SLURM id (a Run beside live jobs, or Approve submitting parked jobs while their upstream runs).
        live_after: dict[str, list[str]] = {}
        for p, c in edges:
            pm = state.jobs.get(p)
            if c in submit_set and p not in submit_set and pm is not None and pm.slurm_job_id:
                if pm.execution_status in (JobStatus.QUEUED, JobStatus.RUNNING):
                    live_after.setdefault(c, []).append(str(pm.slurm_job_id))
        edges = [(p, c) for (p, c) in edges if p in submit_set and c in submit_set]
        try:
            order, preds = _toposort_submit_order(submit_ids, edges)
        except ValueError as e:
            _restore_allocation(state, previous)
            loop = find_cycle([w for w in slot_edges if w.producer in submit_set and w.consumer in submit_set])
            if not loop:
                return err(f"Pipeline DAG is not acyclic ({e}); cannot submit afterok chain.")
            return err(
                "These inputs form a loop, so the chain cannot be ordered: "
                + "; ".join(w.describe() for w in loop)
                + ". Change one of them in its job's I/O section."
            )

        # 3. Write the job dirs. Once one may exist on disk its number is spent, so the counter is
        # committed however this pass ends.
        prepared: dict[str, Path] = {}  # instance_id -> run_submit.script
        failed: str | None = None  # why this pass stopped
        try:
            for instance_id in instances_to_run:
                job_model = state.jobs[instance_id]
                job_type = job_model.job_type
                job_dir = job_dirs[instance_id]
                job_dir.mkdir(parents=True, exist_ok=True)
                # Clear stale exit sentinels so the reconciler's pass-1 can't latch a prior run's
                # terminal status. On a reused FAILED dir this is what lets the rerun proceed;
                # `.task_status/*.ok` is left intact so the supervisor (clean_status_dir
                # keep_ok=True) skips already-done items and resubmits only what failed.
                for _marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE"):
                    (job_dir / _marker).unlink(missing_ok=True)
                job_model.paths = resolved[instance_id]
                job_model.submitted_inputs = recorded[instance_id]

                # RELION-compat job.star into the real job dir.
                self._write_job_star(
                    scheme_job_dir=job_dir,
                    instance_id=instance_id,
                    job_type=job_type,
                    job_model=job_model,
                    server_dir=server_dir,
                    project_dir=project_dir,
                )

                # Inline-completed providers (RUNS_INLINE): metadata-only jobs whose output star is
                # written here in-process -- no relion binary, no supervisor, no afterok SLURM job.
                # Mark Succeeded + drop the exit sentinel so the roster/DAG show them and their
                # consumers submit with no afterok producer (the star is on disk before the chain
                # runs). ImportMovies (tilt_series.star from the registry) and Subset (the rows of
                # the star it cuts).
                if getattr(job_model, "RUNS_INLINE", False):
                    try:
                        if job_type == JobType.IMPORT_MOVIES:
                            self._write_import_stars_inline(job_model, job_dir, project_dir)
                        elif job_type == JobType.SUBSET:
                            self._write_subset_inline(job_model, job_dir, project_dir)
                        else:
                            raise ValueError(f"RUNS_INLINE set but no inline writer for {job_type}")
                    except Exception as e:
                        logger.exception("inline writer failed for %s", instance_id)
                        failed = f"Inline writer failed for {instance_id}: {e}"
                        break
                    (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
                    job_model.execution_status = JobStatus.SUCCEEDED
                    job_model.slurm_job_id = None
                else:
                    fn_exe = self._build_fn_exe(instance_id, job_type, job_model, project_dir, server_dir)
                    try:
                        script_path = self._render_supervisor_script(job_dir, job_model, fn_exe)
                    except (OSError, ValueError) as e:
                        logger.exception("sbatch script render failed for %s", instance_id)
                        failed = f"Could not write the sbatch script for {instance_id}: {e}"
                        break
                    prepared[instance_id] = script_path
        finally:
            state.job_dir_counter = next_job_num
        # Saved before the first sbatch, and when this pass stopped short: the counter has moved past
        # dirs that may exist now (a restart must not hand their numbers out again), each job holds
        # its dir, and a driver reads its paths from this file when it starts.
        state.mark_dirty()
        await self.backend.state_service.save_project(project_path=project_dir, force=True)
        if failed is not None:
            return err(failed)

        # Submit each supervisor in topo order, gating on its producers' supervisor ids. On a
        # mid-chain sbatch error, stop the loop; what already reached SLURM is saved.
        instance_to_slurm: dict[str, str] = {}
        submitted: list[dict[str, Any]] = []
        submit_error: tuple | None = None
        for instance_id in order:
            script_path = prepared[instance_id]
            after_ids = normalize_slurm_ids(
                [instance_to_slurm[p] for p in preds.get(instance_id, set()) if p in instance_to_slurm]
                + live_after.get(instance_id, [])
            )
            try:
                slurm_id = await self.backend.pipeline_runner.submit_supervisor(
                    script_path=script_path, cwd=project_dir, after_ids=after_ids or None
                )
            except Exception as e:
                logger.exception("%s · submitting %s failed", project_dir.name, instance_id)
                submit_error = (instance_id, e)
                break

            instance_to_slurm[instance_id] = slurm_id
            job_model = state.jobs[instance_id]
            job_model.slurm_job_id = slurm_id
            job_model.execution_status = JobStatus.QUEUED
            # The run is active (the monitor ticks it -> reconcile_afterok, which winds it back down
            # when every job has ended) and saved as each sbatch returns: a server that dies
            # mid-chain leaves no live supervisor unrecorded, and a restart re-observes the chain.
            state.pipeline_active = True
            submitted.append({"instance_id": instance_id, "slurm_job_id": slurm_id, "afterok": after_ids})
            after = f" after {', '.join(after_ids)}" if after_ids else ""
            events.info("%s · %s queued as SLURM %s%s", project_dir.name, instance_id, slurm_id, after)
            state.mark_dirty()
            await self.backend.state_service.save_project(project_path=project_dir, force=True)

        if submit_error is not None:
            failed_iid, exc = submit_error
            return err(
                f"sbatch failed for {failed_iid}: {exc}. {len(submitted)} earlier job(s) are queued "
                f"in SLURM and recorded; cancel them manually if needed.",
                afterok=True,
                submitted=submitted,
            )

        if submitted:
            message = f"Submitted {len(submitted)} job(s) to SLURM."
        else:
            written = [iid for iid in instances_to_run if getattr(state.jobs[iid], "RUNS_INLINE", False)]
            message = f"{', '.join(written)} written at submit; nothing to queue."
        return ok(message=message, pid=0, afterok=True, submitted=submitted)

    def _render_supervisor_script(self, job_dir: Path, job_model: AbstractJobParams, fn_exe: str) -> Path:
        """Render config/qsub.sh -> <job_dir>/run_submit.script for a supervisor submit.

        Resources come from job_model._get_queue_options() -- the per-job-type routing that also
        fills job.star (array jobs -> lightweight supervisor_slurm_defaults; single jobs ->
        get_effective_slurm_config). Unlike an array render, the RELION_JOB_EXIT marker
        block is kept: for a single supervisor job that marker is the completion signal.
        """
        opts = dict(job_model._get_queue_options())  # qsub_extra1..8 already routed per job type
        cfg = SlurmConfig(**{field: opts[key] for field, key in SlurmConfig.QSUB_EXTRA_MAPPING.items()})
        # The job's QOS belongs to its own resources; a job whose queue options are the shared
        # supervisor's (array dispatchers override _get_queue_options) keeps the default QOS here and
        # passes its QOS to the array tasks instead.
        if type(job_model)._get_queue_options is AbstractJobParams._get_queue_options:
            cfg.qos = job_model.get_effective_slurm_config().qos
        return write_sbatch_script(job_dir / "run_submit.script", cfg, fn_exe)

    def _write_job_star(
        self,
        scheme_job_dir: Path,
        instance_id: str,
        job_type: JobType,
        job_model: AbstractJobParams,
        server_dir: Path,
        project_dir: Path,
        **kwargs,
    ):
        fn_exe = self._build_fn_exe(instance_id, job_type, job_model, project_dir, server_dir)
        job_model.generate_job_star(job_dir=scheme_job_dir, fn_exe=fn_exe, star_handler=self.star_handler)

    def _build_fn_exe(
        self, instance_id: str, job_type: JobType, job_model: AbstractJobParams, project_dir: Path, server_dir: Path
    ) -> str:
        if job_type == JobType.IMPORT_MOVIES:
            return self._build_import_command(job_model)

        spec = JOB_SPEC_BY_TYPE.get(job_type)
        script = spec.driver if spec else None
        if not script:
            if getattr(job_model, "RUNS_INLINE", False):
                return "true  # written inline at submit; nothing runs"
            return "echo 'Unknown Driver'; exit 1"

        cmd = driver_invocation(
            server_dir=server_dir,
            driver_script=server_dir / "drivers" / script,
            instance_id=instance_id,
            project_path=project_dir,
        )
        # The tilt filter reaches the chain only in DL auto, where its job commits the verdict; a
        # prediction run (backend.submit_tilt_filter_predict) starts the same driver without the flag.
        return f"{cmd} --commit" if job_type == JobType.TILT_FILTER else cmd

    def _get_current_relion_counter(self, project_dir: Path) -> int:
        """
        Reads the job counter from default_pipeline.star, where a project whose earlier jobs RELION
        numbered keeps it: job_dir_counter starts from it, so no job number is handed out twice.
        Raises if the file exists but can't be parsed -- silent failures cause
        job path collisions.
        """
        pipeline_star = project_dir / "default_pipeline.star"

        if not pipeline_star.exists():
            return 0

        data = self.star_handler.read(pipeline_star)
        general = data.get("pipeline_general")

        if general is None:
            raise ValueError(f"pipeline_general block missing from {pipeline_star}")

        if isinstance(general, dict):
            counter = general.get("rlnPipeLineJobCounter")
        elif isinstance(general, pd.DataFrame) and not general.empty:
            if "rlnPipeLineJobCounter" not in general.columns:
                raise ValueError(f"rlnPipeLineJobCounter column missing from {pipeline_star}")
            counter = general["rlnPipeLineJobCounter"].values[0]
        else:
            raise ValueError(f"Unexpected pipeline_general format in {pipeline_star}: {type(general)}")

        if counter is None:
            raise ValueError(f"rlnPipeLineJobCounter not found in {pipeline_star}")

        return int(counter)

    def _write_import_stars_inline(self, job_model: ImportMoviesParams, job_dir: Path, project_dir: Path) -> None:
        """Write ``Import/jobNNN/tilt_series.star`` + per-TS ``tilt_series/<TS>.star`` from the
        TiltSeriesRegistry, reproducing what relion_python_tomo_import would emit. Runs inline in the
        server process -- pure metadata, no relion binary, no container, no SLURM job. Columns mirror a
        known-good relion import: an 8-col global block + a 6-col per-TS block,
        per-tilt rows in acquisition (tilt_index) order.
        """
        from services.tilt_series import get_registry_for
        from services.configs.mdoc_service import get_mdoc_service

        registry = get_registry_for(project_dir)
        mdoc_service = get_mdoc_service()
        rel = job_model.relion_job_name  # "Import/jobNNN/"
        hand = -1 if job_model.acquisition.invert_defocus_hand else 1

        per_ts_dir = job_dir / "tilt_series"
        per_ts_dir.mkdir(parents=True, exist_ok=True)

        global_rows: list[dict[str, Any]] = []
        for ts in sorted(registry.all_tilt_series(), key=lambda t: t.id):
            # rlnTomoNominalDefocus (mdoc TargetDefocus) is not persisted in the registry; source it
            # from the project mdoc, matched by movie basename. Non-fatal if absent (informational
            # column, not consumed by our pipeline) -> defaults to 0.0.
            target_defocus: dict[str, float] = {}
            try:
                for sec in mdoc_service.parse_mdoc_file(ts.mdoc_path).get("data", []):
                    sub = sec.get("SubFramePath", "").replace("\\", "/")
                    if sub and "TargetDefocus" in sec:
                        try:
                            target_defocus[Path(sub).name] = float(sec["TargetDefocus"])
                        except (TypeError, ValueError):
                            pass
            except Exception:
                logger.warning("import writer: could not read mdoc %s for TS %s", ts.mdoc_path, ts.id)

            frames = sorted(ts.frames, key=lambda f: f.tilt_index)
            per_ts_df = pd.DataFrame(
                {
                    "rlnMicrographMovieName": [f"frames/{f.raw_filename}" for f in frames],
                    "rlnTomoTiltMovieFrameCount": [1 for _ in frames],
                    "rlnTomoNominalStageTiltAngle": [f.nominal_tilt_angle_deg for f in frames],
                    "rlnTomoNominalTiltAxisAngle": [job_model.tilt_axis_angle for _ in frames],
                    "rlnMicrographPreExposure": [f.pre_exposure_e_per_a2 for f in frames],
                    "rlnTomoNominalDefocus": [target_defocus.get(f.raw_filename, 0.0) for f in frames],
                }
            )
            self.star_handler.write({ts.id: per_ts_df}, per_ts_dir / f"{ts.id}.star")

            global_rows.append(
                {
                    "rlnTomoName": ts.id,
                    "rlnTomoTiltSeriesStarFile": f"{rel}tilt_series/{ts.id}.star",
                    "rlnVoltage": job_model.voltage,
                    "rlnSphericalAberration": job_model.spherical_aberration,
                    "rlnAmplitudeContrast": job_model.amplitude_contrast,
                    "rlnMicrographOriginalPixelSize": job_model.pixel_size,
                    "rlnTomoHand": hand,
                    "rlnOpticsGroupName": job_model.optics_group_name,
                }
            )

        if not global_rows:
            raise RuntimeError(f"TiltSeriesRegistry for {project_dir} is empty -- cannot write import tilt_series.star")

        self.star_handler.write({"global": pd.DataFrame(global_rows)}, job_dir / "tilt_series.star")
        logger.info("import writer: wrote tilt_series.star + %d per-TS stars into %s", len(global_rows), job_dir)

    def _write_subset_inline(self, job_model: AbstractJobParams, job_dir: Path, project_dir: Path) -> None:
        """A Subset's star (services.subsets.write_subset_star) and a run.out saying what it kept."""
        src, dst = Path(job_model.paths["input_star"]), Path(job_model.paths["output_star"])
        n = write_subset_star(src, dst, job_model.ts_names, project_dir)
        lines = [f"Cut: {src}", f"Kept {n} tilt series:", *job_model.ts_names, f"Wrote: {dst}"]
        (job_dir / "run.out").write_text("\n".join(lines) + "\n")
        logger.info("Subset written: %d tilt series of %s into %s", n, src, dst)

    def _build_import_command(self, params: ImportMoviesParams) -> str:
        # IMPORT is written inline by _write_import_stars_inline. This fn_exe is never
        # executed -- the star is written inline and no import supervisor is submitted.
        # Kept as a harmless no-op so a stray execution can't run a wrong relion_import command.
        return "true  # crboost writes Import/tilt_series.star inline; relion import is not run"


class JobTypeResolver:
    # Reverse of JobSpec.driver.
    DRIVER_TO_JOBTYPE: ClassVar[dict[str, str]] = {
        s.driver: s.job_type.value for s in JOB_SPECS if s.driver is not None
    }

    def __init__(self, star_handler: StarfileService):
        self.star_handler = star_handler

    def get_job_type_from_path(self, project_dir: Path, job_path: str) -> str | None:
        if "Import/job" in job_path:
            return "importmovies"

        job_star_path = project_dir / job_path.rstrip("/") / "job.star"
        if not job_star_path.exists():
            return None

        try:
            data = self.star_handler.read(job_star_path)
            joboptions = data.get("joboptions_values")

            if joboptions is None or not isinstance(joboptions, pd.DataFrame):
                return None

            fn_exe_rows = joboptions[joboptions["rlnJobOptionVariable"] == "fn_exe"]
            if fn_exe_rows.empty:
                return None

            fn_exe = fn_exe_rows["rlnJobOptionValue"].values[0]

            for driver_name, job_type in self.DRIVER_TO_JOBTYPE.items():
                if driver_name in fn_exe:
                    return job_type

            if "relion_import" in fn_exe:
                return "importmovies"

            return None

        except Exception as e:
            logger.warning("Could not read job type from %s: %s", job_star_path, e)
            return None
