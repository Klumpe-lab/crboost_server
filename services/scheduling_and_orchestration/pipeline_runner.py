import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING

from services.array_tasks import any_task_started, manifest_array_job_id, mark_stopped_tasks_failed
from services.jobs.tilt_filter import COMMIT_RECORD, TiltFilterCommit
from services.models_base import FeatureOrigin, JobType
from services.project_state import JobStatus
from services.event_log import events
from services.result import err, ok
from services.segmentation.manifest import read_manifest as read_segmentation_manifest
from services.tilt_series import reload_registry
from services.tilt_series_service import ensure_tilt_thumbnails

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)


def _afterok_state_to_status(slurm_state: str) -> JobStatus:
    """Map a SLURM squeue/sacct state string to a JobStatus for the afterok reconciler.
    Unrecognized states -> UNKNOWN so the caller can decline to downgrade a live job."""
    s = (slurm_state or "").split()[0].upper()  # 'CANCELLED by 123' -> 'CANCELLED'
    if s == "COMPLETED":
        return JobStatus.SUCCEEDED
    if s in ("PENDING", "CONFIGURING", "REQUEUED", "RESIZING", "SUSPENDED"):
        return JobStatus.QUEUED
    if s in ("RUNNING", "COMPLETING"):
        return JobStatus.RUNNING
    if s in ("FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE", "PREEMPTED"):
        return JobStatus.FAILED
    return JobStatus.UNKNOWN


def _driver_fatal_line(job_dir: Path) -> str:
    """The driver's own FATAL line from the job's run.err, else a pointer to the log."""
    log = job_dir / "run.err"
    try:
        lines = log.read_text(errors="replace").splitlines()
    except OSError:
        return f"failed; no log at {log}"
    return next((line.strip() for line in reversed(lines) if "FATAL" in line), f"failed; see {log}")


# A tracked afterok job that has left the queue with NO exit sentinel and NO sacct terminal row
# is concluded FAILED after this many seconds absent, so pipeline_active always winds down even
# when a SIGKILL/NODE_FAIL/scancel bypassed the qsub trailer AND sacct is unavailable. Kept well
# under SLURM's MinJobAge (~300s) so the job leaves the tracked set before it could be purged.
_AFTEROK_ABSENT_GRACE_SEC = 90.0

# How much of a job log get_job_logs returns by default. The panels show the last 500 lines, so
# this only has to be comfortably more than that; the point is that a multi-megabyte run.err
# costs a fixed 256 KB per poll instead of its whole size.
LOG_TAIL_BYTES = 256 * 1024


def _read_tail(path: Path, tail_bytes: int) -> str:
    """Last ``tail_bytes`` of a log file as text, cut at a line boundary. 0 = whole file."""
    size = path.stat().st_size
    with open(path, "rb") as f:
        if tail_bytes and size > tail_bytes:
            f.seek(size - tail_bytes)
            raw = f.read()
            # Drop the partial first line the seek landed mid-way through.
            nl = raw.find(b"\n")
            raw = raw[nl + 1 :] if nl != -1 else raw
            skipped = size - len(raw)
            head = f"[... {skipped:,} earlier bytes not shown — use the copy button for the full log ...]\n"
            return head + raw.decode("utf-8", errors="replace")
        return f.read().decode("utf-8", errors="replace")


class PipelineRunnerService:
    """
    Submits supervisors, reconciles job status from SLURM and the exit markers
    (reconcile_afterok, ticked by PipelineMonitor), and stops or cancels runs.
    """

    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        # Afterok reconciler: first-absent monotonic time per supervisor slurm_id, for the
        # grace window that concludes a marker-less vanished job FAILED (see reconcile_afterok).
        self._afterok_absent_since: dict[str, float] = {}
        # The same grace window for tilt-filter prediction runs (reconcile_tilt_filter_predict).
        self._predict_absent_since: dict[str, float] = {}
        # First-seen monotonic time per instance_id that is currently QUEUED in
        # SLURM, so the UI can show how long a job has been waiting for cluster
        # nodes (a long pending time reads as a cluster wait, not a hung
        # orchestrator). Populated/pruned in get_pipeline_overview. In-memory
        # only — resets on server restart, which is fine: the queue-wait context
        # is ephemeral. Keyed "<project_path>::<instance_id>" to stay correct
        # across multiple loaded projects sharing this singleton.
        self._queued_since: dict[str, float] = {}

    # -------------------------------------------------------------------------
    # Status sync
    # -------------------------------------------------------------------------

    def _resolve_afterok_job_dir(self, job_model, project_path: Path) -> Path | None:
        """job_dir for an afterok job: the resolved paths['job_dir'] if present, else
        project_path / relion_job_name (the submit_chain-allocated External/jobNNN)."""
        jd = job_model.paths.get("job_dir") if getattr(job_model, "paths", None) else None
        if jd and Path(jd).is_dir():
            return Path(jd)
        rjn = getattr(job_model, "relion_job_name", None)
        if rjn:
            return project_path / rjn.rstrip("/")
        return None

    def _afterok_refine_running(self, job_dir: Path | None) -> JobStatus:
        """Refine a supervisor squeue reports as RUNNING: 'supervisor RUNNING' with no
        started child is still queued work (see any_task_started). A single-shot
        (no-manifest) job is genuinely RUNNING."""
        started = any_task_started(job_dir) if job_dir is not None else None
        if started is not None:
            return JobStatus.RUNNING if started else JobStatus.QUEUED
        return JobStatus.RUNNING

    def _exit_sentinels(self, tracked: dict, proj: Path) -> dict[str, str | None]:
        """{instance: "success" / "failure" / None} from each tracked job's exit sentinels; jobs with
        no job dir are left out. Disk only, for a thread."""
        out: dict[str, str | None] = {}
        for iid, jm in tracked.items():
            job_dir = self._resolve_afterok_job_dir(jm, proj)
            if job_dir is None:
                continue
            if (job_dir / "RELION_JOB_EXIT_SUCCESS").exists():
                out[iid] = "success"
            elif (job_dir / "RELION_JOB_EXIT_FAILURE").exists():
                out[iid] = "failure"
            else:
                out[iid] = None
        return out

    async def reconcile_afterok(self, project_path: str) -> dict[str, bool]:
        """Status reconciler, ticked by the monitor while a project's pipeline is active. Per
        non-terminal tracked job (submit_chain set slurm_job_id), in order of authority:
          1. disk RELION_JOB_EXIT_SUCCESS/FAILURE sentinels (written by the supervisor/qsub
             trailer; survive sacct purge),
          2. targeted ``squeue -j`` -> QUEUED-vs-RUNNING + DependencyNeverSatisfied,
          3. ``sacct`` terminal state for jobs that left the queue without a sentinel,
          4. a grace window: a job absent from squeue with no sentinel and no sacct row is
             concluded FAILED after _AFTEROK_ABSENT_GRACE_SEC, so a marker-less death (SIGKILL /
             NODE_FAIL / external scancel) -- even with sacct unavailable -- always winds down.
        Mutates execution_status in place on the bound model; drives pipeline_active off live-job
        presence; scancels DependencyNeverSatisfied dependents (stop-on-fail propagates over
        ticks). Never concludes 'done' from an errored squeue."""
        from services.computing.slurm_service import normalize_slurm_ids

        state = self.backend.state_service.state_for(Path(project_path))
        proj = Path(project_path)
        # Only NON-terminal jobs are reconciled: a SUCCEEDED/FAILED job is never re-read (honors the
        # FAILED-exclusion invariant and keeps settled jobs out of the batched SLURM query).
        tracked = {
            iid: jm
            for iid, jm in state.jobs.items()
            if getattr(jm, "slurm_job_id", None) and jm.execution_status not in (JobStatus.SUCCEEDED, JobStatus.FAILED)
        }

        changes: dict[str, bool] = {}
        pending: dict[str, list[str]] = {}  # slurm_id -> [instance_ids] lacking a terminal sentinel

        # Pass 1 -- disk sentinels (authoritative, cheapest, no SLURM call). Read in a thread: it is a
        # few stats per job on the shared filesystem, every tick.
        sentinels = await asyncio.to_thread(self._exit_sentinels, tracked, proj)
        for iid, jm in tracked.items():
            if iid not in sentinels:
                continue
            if sentinels[iid] == "success":
                if jm.execution_status != JobStatus.SUCCEEDED:
                    await self._land_job(proj, jm, JobStatus.SUCCEEDED)
                    jm.execution_status = JobStatus.SUCCEEDED
                    changes[iid] = True
            elif sentinels[iid] == "failure":
                if jm.execution_status != JobStatus.FAILED:
                    await self._land_job(proj, jm, JobStatus.FAILED)
                    jm.execution_status = JobStatus.FAILED
                    changes[iid] = True
            else:
                pending.setdefault(str(jm.slurm_job_id), []).append(iid)

        # Pass 2 -- SLURM for jobs without a terminal sentinel.
        to_scancel: list[str] = []
        if pending:
            now = time.monotonic()
            queued = await self.backend.slurm_service.query_jobs_by_ids(list(pending.keys()))
            if queued is None:
                # squeue errored. Do not downgrade any pending job; keep prior status
                # (the run is presumed live) and persist only the pass-1 sentinel changes.
                logger.warning("reconcile_afterok[%s]: squeue unavailable this tick; holding status", proj.name)
            else:
                absent = [sid for sid in pending if sid not in queued]
                terminal = {}
                if absent:
                    # None (sacct error/unavailable) and {} (sacct ran, no row) both leave an
                    # absentee without a terminal row -> the grace window below handles it.
                    terminal = await self.backend.slurm_service.query_terminal_states(absent) or {}
                for sid, iids in pending.items():
                    if sid in queued or sid in terminal:
                        self._afterok_absent_since.pop(sid, None)  # visible again -> reset grace
                    for iid in iids:
                        jm = tracked[iid]
                        if sid in queued:
                            sq_state, reason = queued[sid]
                            if reason.strip("()").casefold() == "dependencyneversatisfied":
                                # An upstream failed -> this afterok dependent can never run.
                                new = JobStatus.FAILED
                                to_scancel.append(sid)
                            else:
                                new = _afterok_state_to_status(sq_state)
                                if new == JobStatus.RUNNING:
                                    job_dir = await asyncio.to_thread(self._resolve_afterok_job_dir, jm, proj)
                                    new = await asyncio.to_thread(self._afterok_refine_running, job_dir)
                                elif new == JobStatus.UNKNOWN:
                                    new = JobStatus.QUEUED  # in the queue, just an unmapped state
                        elif sid in terminal:
                            new = _afterok_state_to_status(terminal[sid][0])
                        else:
                            # Gone from squeue, no exit sentinel, no sacct row: don't conclude
                            # immediately (Lustre/sacct lag). After the grace window, FAIL it so
                            # pipeline_active can wind down.
                            first = self._afterok_absent_since.setdefault(sid, now)
                            expired = (now - first) >= _AFTEROK_ABSENT_GRACE_SEC
                            new = JobStatus.FAILED if expired else jm.execution_status
                        if new != JobStatus.UNKNOWN and jm.execution_status != new:
                            events.info("%s · %s %s → %s", proj.name, iid, jm.execution_status.value, new.value)
                            await self._land_job(proj, jm, new)
                            jm.execution_status = new
                            changes[iid] = True
        # Drop grace timers for ids no longer pending this tick (resolved / left tracked / none pending).
        self._afterok_absent_since = {s: t for s, t in self._afterok_absent_since.items() if s in pending}

        # Pass 3 -- scancel DependencyNeverSatisfied dependents so they don't sit PENDING forever
        # (cancelling them parks THEIR dependents the same way, so stop-on-fail propagates per tick).
        if to_scancel:
            try:
                await self.backend.slurm_service.scancel_jobs(normalize_slurm_ids(to_scancel))
            except Exception:
                logger.exception("reconcile_afterok[%s]: scancel of stalled dependents failed", proj.name)

        # Pass 4 -- pipeline_active follows live-job presence; tracked excludes terminal jobs, so an
        # all-resolved tracked set means the chain is done.
        # Interactive jobs are excluded from that vote: they are never part of the chain (the
        # orchestrator refuses to dispatch them, pipeline_orchestrator_service.py) -- their
        # slurm_job_id comes from their own one-off submit. A live per-list extraction would
        # otherwise hold `pipeline_active` open, i.e. make the roster read as a live run, and one
        # that dies with the server would pin it there for good. They stay in `tracked`, so passes
        # 1-3 keep reconciling them off the disk sentinels -- but only while a real pipeline job
        # is live: once this vote clears `pipeline_active`, PipelineMonitor drops the project and
        # this reconciler stops running for it. An interactive job left non-terminal after that is
        # settled by its own owner (`backend.reconcile_pick_list_extractions` for per-list
        # extractions), not here.
        any_live = any(
            jm.execution_status in (JobStatus.QUEUED, JobStatus.RUNNING) and not getattr(jm, "IS_INTERACTIVE", False)
            for jm in tracked.values()
        )
        if any_live and not state.pipeline_active:
            state.pipeline_active = True
            changes["__pipeline_active__"] = True
        elif not any_live and state.pipeline_active:
            state.pipeline_active = False
            changes["__pipeline_active__"] = True

        if changes:
            try:
                await self.backend.state_service.save_project(project_path=proj, force=True)
            except Exception as e:
                logger.error("reconcile_afterok[%s]: failed to persist status changes: %s", proj.name, e)

        # Pass 5 -- fsMotion SUCCEEDED: render the tilt thumbnails now, rather than when someone
        # first opens a tilt-filter panel.
        for iid in changes:
            jm = tracked.get(iid)
            if jm is not None and jm.execution_status == JobStatus.SUCCEEDED and jm.job_type == JobType.FS_MOTION_CTF:
                self._kickoff_tilt_thumbnails(proj, state)
                # A tilt filter in DL review that this run waits for scores the new averages by itself.
                await self.backend.autostart_tilt_filter_predict(proj, fresh=True)
                break

        return {k: v for k, v in changes.items() if not k.startswith("__")}

    async def _land_job(self, proj: Path, jm, new: JobStatus) -> None:
        """What has to reach the server before a job's status says it ended."""
        await self._land_tilt_filter_job(proj, jm, new)
        await self._land_segment_job(proj, jm, new)

    async def _land_segment_job(self, proj: Path, jm, new: JobStatus) -> None:
        """A Segment job succeeded: its volumes are in the registry files, and the features it
        produced become the project's, each with its colour, the first time a job produces them.
        The registry is re-read first: the Tomograms view reads it on its next collection, and
        get_registry_for can miss the job's write for up to a minute (see reload_registry)."""
        if jm.job_type != JobType.SEGMENT or new != JobStatus.SUCCEEDED:
            return
        await reload_registry(proj)
        job_dir = self._resolve_afterok_job_dir(jm, proj)
        try:
            if job_dir is None:
                raise FileNotFoundError("no job directory recorded")
            manifest = await asyncio.to_thread(read_segmentation_manifest, job_dir)
        except (OSError, ValueError):
            # The job's log says why it wrote none; there is nothing to register without it.
            logger.exception("Segment job %s: no readable segmentations.json", jm.relion_job_name)
            return
        state = self.backend.state_service.state_for(proj)
        origin = FeatureOrigin.AIS if manifest.method == "ais" else FeatureOrigin.EASYMODE
        for feature in manifest.features():
            state.add_feature(feature, origin=origin)

    async def _land_tilt_filter_job(self, proj: Path, jm, new: JobStatus) -> None:
        """A DL-auto tilt filter's job ends, and its status is about to say so. Succeeded: the
        job replaced the registry files (predictions and verdict) and wrote its counts into
        COMMIT_RECORD; both reach the server first, since the job row, its page and the Tilts tab
        re-read on the status, and get_registry_for can miss the job's write for up to a minute
        (see reload_registry). Failed: its driver's FATAL line, for the row's tooltip."""
        if jm.job_type != JobType.TILT_FILTER or new not in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            return
        job_dir = self._resolve_afterok_job_dir(jm, proj)
        if new == JobStatus.FAILED:
            jm.auto_error = (
                await asyncio.to_thread(_driver_fatal_line, job_dir) if job_dir else "failed; no job directory recorded"
            )
            return
        await reload_registry(proj)
        jm.auto_error = ""
        record = job_dir / COMMIT_RECORD if job_dir else None
        try:
            text = await asyncio.to_thread(record.read_text) if record else None
            jm.last_commit = TiltFilterCommit.model_validate_json(text) if text else None
        except (OSError, ValueError):
            # The verdict is in the registry either way; the row then reads done without counts.
            logger.exception("DL-auto tilt filter: unreadable %s", record)
            jm.last_commit = None

    async def reconcile_tilt_filter_predict(self, project_path: str) -> bool:
        """Settle the tilt filter's in-flight DL prediction run (`TiltFilterParams.predict_run`).

        The run is a one-off SLURM job outside the chain, so reconcile_afterok does not see it,
        and its status must not ride on the job's execution_status (that one says whether a
        verdict is committed). Same order of authority as reconcile_afterok: the exit markers
        in the run's directory, then squeue, then sacct, then the absent-grace window. Returns
        True when a run changed; persists the change itself."""
        proj = Path(project_path)
        state = self.backend.state_service.state_for(proj)
        runs = [jm.predict_run for jm in state.jobs.values() if getattr(jm, "predict_in_flight", False)]
        if not runs:
            return False

        async def settle(run, new: JobStatus, error: str = "") -> bool:
            if new == run.status:
                return False
            if new == JobStatus.SUCCEEDED:
                # The run's predictions must be in the server's registry before its status says they
                # landed: the gallery and the job row re-read on that status, and get_registry_for can
                # miss the job's write for up to a minute (see reload_registry).
                await reload_registry(proj)
            events.info(
                "%s · tilt filter DL run %s %s → %s", proj.name, Path(run.job_dir).name, run.status.value, new.value
            )
            run.status = new
            run.error = error
            return True

        changed = False
        pending = {}  # slurm_id -> run lacking an exit marker
        for run in runs:
            run_dir = Path(run.job_dir)
            if (run_dir / "RELION_JOB_EXIT_SUCCESS").exists():
                changed |= await settle(run, JobStatus.SUCCEEDED)
            elif (run_dir / "RELION_JOB_EXIT_FAILURE").exists():
                changed |= await settle(run, JobStatus.FAILED, await asyncio.to_thread(_driver_fatal_line, run_dir))
            elif run.slurm_job_id:
                pending[str(run.slurm_job_id)] = run
            # No SLURM id yet: the submit is between persisting the run and sbatch returning.

        if pending:
            queued = await self.backend.slurm_service.query_jobs_by_ids(list(pending))
            if queued is None:
                logger.warning("reconcile_tilt_filter_predict[%s]: squeue unavailable this tick; holding", proj.name)
            else:
                absent = [sid for sid in pending if sid not in queued]
                terminal = (await self.backend.slurm_service.query_terminal_states(absent) or {}) if absent else {}
                now = time.monotonic()
                for sid, run in pending.items():
                    if sid in queued:
                        self._predict_absent_since.pop(sid, None)
                        new = _afterok_state_to_status(queued[sid][0])
                        changed |= await settle(run, JobStatus.QUEUED if new == JobStatus.UNKNOWN else new)
                    elif sid in terminal:
                        self._predict_absent_since.pop(sid, None)
                        new = _afterok_state_to_status(terminal[sid][0])
                        if new != JobStatus.UNKNOWN:
                            error = f"SLURM ended the job: {terminal[sid][0]}" if new == JobStatus.FAILED else ""
                            changed |= await settle(run, new, error)
                    elif now - self._predict_absent_since.setdefault(sid, now) >= _AFTEROK_ABSENT_GRACE_SEC:
                        self._predict_absent_since.pop(sid, None)
                        reason = "left the queue without an exit marker (killed, node failure or scancel)"
                        changed |= await settle(run, JobStatus.FAILED, reason)

        if changed:
            try:
                await self.backend.state_service.save_project(project_path=proj, force=True)
            except Exception:
                logger.exception("reconcile_tilt_filter_predict[%s]: failed to persist", proj.name)
        return changed

    def _kickoff_tilt_thumbnails(self, project_path, state) -> None:
        """fsMotionAndCtf just landed → render PNG previews of its motion-corrected
        averages in the background (the tilt-filter gallery needs them on first visit,
        and the Journey's per-tilt hover cards show them). The reconciler calls this
        on the SUCCEEDED edge; the work itself is a no-op when the PNGs already
        exist. Kickoff failures must never break a status-sync loop."""
        try:
            ensure_tilt_thumbnails(project_path, state)
        except Exception:
            logger.exception("fsMotion auto-thumbnail kickoff failed")

    # -------------------------------------------------------------------------
    # Pipeline overview / logs
    # -------------------------------------------------------------------------

    async def get_pipeline_overview(self, project_path: str):
        state = self.backend.state_service.state_for(Path(project_path))

        if not state.jobs:
            self._prune_queued_since(str(project_path), set())
            return {
                "status": "ok",
                "total": 0,
                "completed": 0,
                "running": 0,
                "failed": 0,
                "scheduled": 0,
                "queued": 0,
                "queued_jobs": [],
                "is_complete": True,
                "jobs": {},
            }

        now = time.monotonic()
        total = succeeded = running = failed = scheduled = queued = 0
        queued_jobs: list[dict[str, Any]] = []
        live_queued_keys: set = set()
        for iid, job_model in state.jobs.items():
            # IMPORT_MOVIES is a local pre-step; TS_IMPORT is a silently-injected
            # prerequisite of TS_ALIGNMENT (JobSpec.prerequisite in services/jobs/spec.py).
            # Neither appears in the user-visible roster (TS_IMPORT has phase=None), so both
            # must be excluded to keep the counter denominator aligned with the UI.
            if job_model.job_type in (JobType.IMPORT_MOVIES, JobType.TS_IMPORT):
                continue
            # Interactive jobs (e.g. TiltFilter) are never part of the chain and
            # manage their own status via the UI. If we counted them
            # here, a merely-scheduled interactive job would keep `scheduled > 0`
            # forever and the status poller would never mark the pipeline complete
            # (ui/pipeline_builder/status_poller.py:45).
            if getattr(job_model, "IS_INTERACTIVE", False):
                continue
            total += 1
            s = job_model.execution_status
            if s == JobStatus.SUCCEEDED:
                succeeded += 1
            elif s == JobStatus.RUNNING:
                running += 1
            elif s == JobStatus.FAILED:
                failed += 1
            elif s == JobStatus.SCHEDULED:
                scheduled += 1
            elif s == JobStatus.QUEUED:
                queued += 1
                key = f"{project_path}::{iid}"
                live_queued_keys.add(key)
                since = self._queued_since.setdefault(key, now)
                queued_jobs.append(
                    {
                        "instance_id": iid,
                        "slurm_job_id": getattr(job_model, "slurm_job_id", None),
                        "pending_secs": max(0, int(now - since)),
                    }
                )

        # Drop timers for jobs in this project that are no longer queued.
        self._prune_queued_since(str(project_path), live_queued_keys)

        return {
            "status": "ok",
            "total": total,
            "completed": int(succeeded),
            "running": int(running),
            "failed": int(failed),
            "scheduled": int(scheduled),
            "queued": int(queued),
            "queued_jobs": queued_jobs,
            # A pipeline with anything still queued or scheduled is NOT complete,
            # even though `running == 0` — a job waiting for a SLURM node has
            # running == 0 but is very much not done.
            "is_complete": running == 0 and queued == 0 and scheduled == 0 and total > 0,
            "jobs": {},
        }

    def _prune_queued_since(self, project_path: str, live_keys: set) -> None:
        prefix = f"{project_path}::"
        for key in [k for k in self._queued_since if k.startswith(prefix) and k not in live_keys]:
            self._queued_since.pop(key, None)

    async def get_job_logs(
        self, project_path: str, job_name: str, *, tail_bytes: int = LOG_TAIL_BYTES
    ) -> dict[str, str]:
        """run.out / run.err for a job dir, TAIL ONLY by default.

        Every consumer of this renders a tail (the logs tab shows the last 500 lines) on a 3 s
        timer, while a supervisor's run.err can reach megabytes (a driver dumping a pydantic
        error per tilt-series). Reading those in full stalls every other client for seconds per
        tick. Pass ``tail_bytes=0`` for the whole file (the logs tab's copy button, on demand).
        """
        job_path = Path(project_path) / job_name.rstrip("/")

        def _read() -> dict[str, str]:
            logs = {"stdout": "", "stderr": "", "exists": False, "path": str(job_path)}
            if not job_path.exists():
                logs["stdout"] = f"Job directory not found:\n{job_path}"
                return logs
            logs["exists"] = True
            for key, name in (("stdout", "run.out"), ("stderr", "run.err")):
                f = job_path / name
                if not f.exists():
                    logs[key] = f"{name} not found."
                    continue
                try:
                    logs[key] = _read_tail(f, tail_bytes)
                except Exception as e:
                    logs[key] = f"Error reading {name}: {e}"
            return logs

        return await asyncio.to_thread(_read)

    async def _sbatch_script(self, script_path: Path, cwd: Path, dependency_after_ids: list[str] | None = None) -> str:
        """sbatch in an env stripped of SLURM_*/SBATCH_* so the submission
        doesn't inherit any parent job context. Returns the SLURM job ID.

        When dependency_after_ids is non-empty, injects
        ``--dependency=afterok:<id>[:<id>...]`` so the job stays PENDING until every
        listed job completes successfully (the SLURM afterok DAG)."""
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}
        args = ["sbatch"]
        if dependency_after_ids:
            args.append("--dependency=afterok:" + ":".join(str(i) for i in dependency_after_ids))
        args.append(str(script_path))
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=str(cwd), env=clean_env
        )
        stdout_b, stderr_b = await process.communicate()
        stdout = stdout_b.decode()
        stderr = stderr_b.decode()
        if process.returncode != 0:
            raise RuntimeError(f"sbatch rc={process.returncode} stderr={stderr!r}")
        # "Submitted batch job 12345"
        return stdout.strip().split()[-1]

    async def submit_supervisor(self, script_path: Path, cwd: Path, after_ids: list[str] | None = None) -> str:
        """Public seam for the afterok orchestrator: sbatch one supervisor
        script, optionally gated on its producers' supervisor job ids via afterok.
        Returns the SLURM job id. Thin wrapper over _sbatch_script."""
        return await self._sbatch_script(script_path, cwd, dependency_after_ids=after_ids)

    async def _cancel_and_mark(self, project_dir: Path, jobs: dict) -> tuple[dict, list[str]]:
        """scancel each job's supervisor (`jobs`: instance_id -> job model). For every job SLURM
        accepted the cancel of, and every one without a supervisor id (nothing in SLURM to cancel),
        scancel the array it dispatched, then mark each task that was running as failed: a task
        killed mid-run writes no status marker of its own and would read running on a Failed job.
        Returns those jobs and the errors. A job whose scancel failed is left out and untouched:
        scancel exits 0 for a job that has ended, so the controller did not answer or refused."""
        from services.computing.slurm_service import normalize_slurm_ids

        slurm = self.backend.slurm_service
        sids = {iid: str(jm.slurm_job_id) for iid, jm in jobs.items() if getattr(jm, "slurm_job_id", None)}
        # One scancel per job, so a failure names its job; side by side, so a controller that does
        # not answer costs one timeout, not one per job.
        results = dict(zip(sids, await asyncio.gather(*(slurm.scancel_jobs([s]) for s in sids.values())), strict=True))
        errors: list[str] = []
        stopped = {}
        for iid, jm in jobs.items():
            res = results.get(iid)
            if res is not None and not res["success"]:
                errors.append(f"{iid}: scancel {sids[iid]} failed ({res['error']}), so it is left as it is")
            else:
                stopped[iid] = jm
        job_dirs = [d for jm in stopped.values() if (d := self._resolve_afterok_job_dir(jm, project_dir)) is not None]
        arrays = normalize_slurm_ids([a for d in job_dirs if (a := manifest_array_job_id(d))])
        if arrays:
            # A stopped supervisor cancels its array itself; this makes sure before the tasks are
            # marked. An array that has already ended makes scancel fail, which changes nothing.
            await self.backend.slurm_service.scancel_jobs(arrays)
        for d in job_dirs:
            try:
                n = await asyncio.to_thread(mark_stopped_tasks_failed, d)
            except OSError as e:
                errors.append(f"could not mark the stopped tasks in {d}: {e}")
                continue
            if n:
                logger.info("Marked %d stopped task(s) as .fail in %s", n, d)
        return stopped, errors

    async def stop_and_cleanup(self, project_dir: Path) -> dict[str, Any]:
        """Stop the run: scancel every live job by its persisted slurm_job_id (which covers a
        still-PENDING dependent with no run.out or manifest) and mark FAILED each one SLURM accepted
        the cancel of. pipeline_active clears only when every one was; a job whose scancel failed
        keeps its status, and the reconciler goes on following it."""
        state = self.backend.state_service.state_for(project_dir)
        # Interactive jobs are not part of the run being stopped -- they manage their
        # own status and were never deployed by the orchestrator (a per-list extraction
        # submitted from the Species page, a DL tilt filter). Cancelling them here would
        # scancel unrelated live work and leave it reading Failed forever.
        live = {
            iid: jm
            for iid, jm in state.jobs.items()
            if getattr(jm, "slurm_job_id", None)
            and not getattr(jm, "IS_INTERACTIVE", False)
            and jm.execution_status in (JobStatus.RUNNING, JobStatus.QUEUED, JobStatus.SCHEDULED)
        }
        stopped, errors = await self._cancel_and_mark(project_dir, live)
        for jm in stopped.values():
            jm.execution_status = JobStatus.FAILED
        state.pipeline_active = len(stopped) < len(live)
        # A stopped run has nothing left to resume: jobs parked behind a review stay as they
        # are, and Approve no longer submits them.
        state.review_hold = None
        await self.backend.state_service.save_project(project_path=project_dir, force=True)
        if errors:
            return err("; ".join(errors), cancelled_slurm_jobs=len(stopped), errors=errors)
        return ok(cancelled_slurm_jobs=len(stopped), errors=[])

    async def cancel_job(self, project_dir: Path, instance_id: str) -> dict[str, Any]:
        """Cancel one live job by its persisted slurm_job_id (which works for a still-PENDING
        dependent with no run.out or manifest) and, once SLURM accepted the cancel, mark it FAILED;
        a failed scancel leaves it as it is and says why. pipeline_active stays: cancelling one
        supervisor parks its afterok dependents (DependencyNeverSatisfied), which reconcile_afterok
        scancels, and its pass 4 winds pipeline_active down only when every job is terminal.
        Clearing it here would stop the monitor ticking and freeze the rest of the live chain."""
        state = self.backend.state_service.state_for(project_dir)
        job_model = state.jobs.get(instance_id)
        if not job_model:
            return err(f"Job '{instance_id}' not found in state")
        if job_model.execution_status not in (JobStatus.RUNNING, JobStatus.QUEUED):
            return err(f"Job is not live (status: {job_model.execution_status})")
        stopped, warnings = await self._cancel_and_mark(project_dir, {instance_id: job_model})
        if not stopped:
            return err("; ".join(warnings))
        for warning in warnings:
            logger.warning("cancel_job: %s", warning)
        job_model.execution_status = JobStatus.FAILED
        await self.backend.state_service.save_project(project_path=project_dir, force=True)
        sid = job_model.slurm_job_id
        return ok(
            cancelled_slurm_ids=[str(sid)] if sid else [],
            message=f"Cancelled {instance_id}" + (f" (SLURM {sid})" if sid else ""),
            warnings=warnings,
        )
