from __future__ import annotations
import asyncio
import getpass
import json
import logging
import os
import pwd
import re
import shutil
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from datetime import datetime

from services.templating.template_service import TemplateService
from services.templating.pdb_service import PDBService
from services.scheduling_and_orchestration.project_service import ProjectService
from services.scheduling_and_orchestration.pipeline_orchestrator_service import PipelineOrchestratorService
from services.computing.container_service import get_container_service
from services.scheduling_and_orchestration.pipeline_runner import PipelineRunnerService
from services.scheduling_and_orchestration.pipeline_monitor import PipelineMonitor
from services.array_tasks import manifest_items, progress as task_progress
from services.log_progress import read as read_log_progress
from services.jobs.spec import PIPELINE_ORDER, display_name, driver_invocation
from services.jobs.tilt_filter import FilterMode
from services.models_base import JobStatus, JobType
from services.particles import class_readout, particle_sets, set_ops
from services.particles.list_ref import extract_pick_list_instance_id
from services.particles.particle_sets import ParentRef, Rule
from services.project_state import get_state_service, remove_project_state
from services.background_tasks import ProgressCallback
from services.project_delete import delete_project_dir, is_being_deleted
from services.project_disk_usage import get_disk_usage_service, is_stale as is_disk_usage_stale
from services.project_trash import clear_trash, list_trash, trash_summary
from services.event_log import events
from services.result import ErrorCode, err, ok
from services.computing.slurm_service import SlurmService, write_sbatch_script
from services.configs.config_service import get_config_service
from services.curation.session_service import CurationSessionService
from services.curation.watcher import CurationWatcher
from services.tilt_series import TiltSeriesRegistry, get_registry_for

logger = logging.getLogger(__name__)

# The process-wide backend. main.py constructs one at startup; UI surfaces opened
# without a threaded-through reference (e.g. the tomo dashboard, which is a function
# module) reach it via get_backend(). Same idiom as get_config_service()/get_state_service().
_backend_instance: CryoBoostBackend | None = None


def get_backend() -> CryoBoostBackend | None:
    """The process-wide backend instance, or None before one is constructed."""
    return _backend_instance


def _is_pipeline_job_dict(job: Any) -> bool:
    """Does this raw ``project_params.json`` job entry belong to the pipeline's own run?

    Per-list subtomogram extractions do not: they are one-off interactive jobs whose status
    the Species page's Picks tab shows. Counting them in the project-hub scan would let one
    failed list extraction leave the whole project reading "failed" until the next submit,
    let a running one make an idle project read "running", and inflate its planned-job total.
    The scan reads json and cannot see ``IS_INTERACTIVE``, so the job type is the filter.
    ``tiltFilter`` stays counted: interactive to launch, but pipeline work."""
    return not (isinstance(job, dict) and job.get("job_type") == JobType.EXTRACT_PICK_LIST.value)


# How often a streaming project scan hands over what it has found so far.
_PARTIAL_SCAN_EVERY_S = 0.3


def _scan_order(project: dict[str, Any]) -> tuple:
    """The project scan's stable order: oldest first by created_at (or the ctime fallback). Newly
    created projects go to the end, and edits to project_params.json never shuffle positions."""
    return (project["created_timestamp"], project["name"])


def _registry_ts_count(project_dir: Path) -> int:
    """Tilt-series listed in the registry index, for the project-hub scan.

    A project created without a dataset scan records no import counts in
    ``project_params.json``; the registry is the store and still lists every series.
    Reads only ``index.json`` — the per-series sidecars are not opened."""
    index_path = project_dir / "registry" / "index.json"
    try:
        with open(index_path) as f:
            return len(json.load(f).get("tilt_series") or [])
    except FileNotFoundError:  # data-less or pre-registry project: no series to count
        return 0
    except (OSError, ValueError, AttributeError) as e:
        logger.warning("Unreadable registry index %s -- TS count unknown: %s", index_path, e)
        return 0


_RECON_PNG_APIX = re.compile(r"_(\d+(?:\.\d+)?)Apx\.png$")
_JOB_NUMBER = re.compile(r"job(\d+)")
_PIPELINE_POSITION = {jt.value: i for i, jt in enumerate(PIPELINE_ORDER)}


def _furthest_done(jobs: dict[str, Any]) -> dict[str, Any] | None:
    """The succeeded job furthest down the pipeline, for the projects roster's "where it is at"
    column: its display name, instance id and pipeline position (the sort key). None when no job has succeeded."""
    best: tuple[int, str, str] | None = None
    for iid, job in jobs.items():
        if not isinstance(job, dict) or (job.get("execution_status") or "").strip() != "Succeeded":
            continue
        position = _PIPELINE_POSITION.get(job.get("job_type"))
        if position is not None and (best is None or position > best[0]):
            best = (position, iid, job["job_type"])
    if best is None:
        return None
    return {"label": display_name(JobType(best[2])), "instance_id": best[1], "position": best[0]}


def _find_tomo_preview(project_dir: Path, jobs: dict[str, Any]) -> dict[str, Any] | None:
    """The WarpTools tomogram preview PNGs, for the projects roster's row hover.

    WarpTools writes `<job>/warp_tiltseries/reconstruction/<tomo>_<apix>Apx.png` next to
    each reconstructed volume. Takes the newest tsReconstruct job that has any, all of
    its previews by name; also counts the volumes there. None when no reconstruction
    has a preview."""
    recon_dirs = [
        job["relion_job_name"].strip("/")
        for job in jobs.values()
        if isinstance(job, dict) and job.get("job_type") == JobType.TS_RECONSTRUCT.value and job.get("relion_job_name")
    ]

    def job_number(rel: str) -> int:
        m = _JOB_NUMBER.search(rel)
        return int(m.group(1)) if m else -1

    for rel in sorted(recon_dirs, key=job_number, reverse=True):
        recon_dir = project_dir / rel / "warp_tiltseries" / "reconstruction"
        try:
            names = sorted(e.name for e in os.scandir(recon_dir))
        except FileNotFoundError:
            # Planned or deleted job: nothing reconstructed here.
            continue
        pngs = [n for n in names if n.endswith(".png")]
        if not pngs:
            continue
        m = _RECON_PNG_APIX.search(pngs[0])
        return {
            "pngs": [str(recon_dir / n) for n in pngs],
            "apix": float(m.group(1)) if m else None,
            "n_tomos": sum(1 for n in names if n.endswith(".mrc")),
        }
    return None


def _live_progress(project_dir: Path, jobs: dict[str, Any]) -> dict[str, Any] | None:
    """A live run at a glance, for the projects roster: the job at work (the running one
    furthest upstream, else the queued one), its tilt-series done out of those in scope,
    and the tilt-series the run's jobs have failed so far. None when nothing runs or queues.

    Per-TS facts come from each job's `.task_manifest.json` + `.task_status/` markers
    (services.array_tasks), read only for live projects. A failed TS is dropped from
    everything downstream, so the per-job fail counts add up to the TS the run has lost."""
    current: tuple | None = None  # (queued?, job number, label, state, job_dir, job type)
    failed: list[tuple[int, str, int]] = []  # (job number, label, failed TS)
    counts_by_dir: dict[Path, Any] = {}
    for iid, job in jobs.items():
        if not isinstance(job, dict):
            continue
        rel = (job.get("relion_job_name") or "").strip("/")
        if not rel:
            continue
        job_dir = project_dir / rel
        status = (job.get("execution_status") or "").strip()
        # Same reconcile as _derive_live_status: a job that ended unwatched leaves Running persisted.
        if status == "Running":
            if (job_dir / "RELION_JOB_EXIT_SUCCESS").exists():
                status = "Succeeded"
            elif (job_dir / "RELION_JOB_EXIT_FAILURE").exists():
                status = "Failed"
        try:
            label = display_name(JobType(job.get("job_type")))
        except ValueError:  # a job type this server no longer knows: its instance id still names it
            label = iid
        m = _JOB_NUMBER.search(rel)
        number = int(m.group(1)) if m else 0
        if status in ("Running", "Queued") and (current is None or (status == "Queued", number) < current[:2]):
            current = (status == "Queued", number, label, status.lower(), job_dir, job.get("job_type"))
        # Queued / Scheduled jobs have not run in this run; their markers are an earlier attempt's.
        if status not in ("Succeeded", "Running", "Failed"):
            continue
        items = manifest_items(job_dir)
        if not items:
            continue
        counts = counts_by_dir[job_dir] = task_progress(job_dir, items)
        if counts.n_fail:
            failed.append((number, label, counts.n_fail))
    if current is None:
        return None
    out: dict[str, Any] = {
        "stage": current[2],
        "state": current[3],
        "ok": None,
        "total": None,
        "running": None,
        "failed": sum(n for _, _, n in failed),
        "failed_by_stage": [(label, n) for _, label, n in sorted(failed)],
        "log": None,
    }
    counts = counts_by_dir.get(current[4])
    if current[3] == "running" and counts is not None:
        out["ok"] = counts.n_ok
        out["total"] = counts.total - counts.n_skip
        out["running"] = counts.n_running
    elif current[3] == "running":
        # No per-TS counts (not an array job): how far its tool says it is (services/log_progress.py).
        out["log"] = read_log_progress(current[5], current[4])
    return out


def _read_extraction_outdir(out_dir: Path) -> tuple[str, dict]:
    """What ONE per-list extraction out dir says about its job, from disk alone:

      ``("failed" | "done", result.json)`` — an exit marker is down;
      ``("running", {})``                  — SLURM created ``run.out``, so the allocation started;
      ``("pending", {})``                  — nothing there yet.

    ``running`` is only truthful because ``extract_pick_list`` unlinks the previous run's
    ``run.out`` before submitting; without that a re-extract would read RUNNING off the last
    run's leftovers. Blocking I/O — call it off the event loop. Shared by the live awaiter
    (``_await_extraction_outdirs``) and the after-the-fact reconciler
    (``reconcile_pick_list_extractions``) so both read the same disk truth."""
    rj = out_dir / "result.json"
    if (out_dir / "RELION_JOB_EXIT_FAILURE").exists():
        status = "failed"
    elif (out_dir / "RELION_JOB_EXIT_SUCCESS").exists() or rj.exists():
        status = "done"
    elif (out_dir / "run.out").exists():
        return ("running", {})
    else:
        return ("pending", {})
    try:
        return (status, json.loads(rj.read_text()) if rj.exists() else {})
    except (OSError, ValueError):
        # result.json may be mid-write or malformed; the exit marker already decides the status.
        return (status, {})


class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        global _backend_instance
        _backend_instance = self
        self.username = getpass.getuser()
        self.server_dir = server_dir
        self.config_service = get_config_service()
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()
        self.slurm_service = SlurmService(self.username)
        self.pipeline_runner = PipelineRunnerService(self)
        self.state_service = get_state_service()
        self.template_service = TemplateService(self)
        self.pdb_service = PDBService(self)
        # PipelineMonitor is the single server-side observer of pipeline
        # status across all projects. Started in main.py's FastAPI startup
        # hook; not auto-started here so unit-test / one-shot driver paths
        # that construct a backend don't spawn a background task.
        self.pipeline_monitor = PipelineMonitor(self)
        self.curation_service = CurationSessionService(
            server_dir=self.server_dir, username=self.username, slurm_service=self.slurm_service
        )
        # CurationWatcher registers ArtiaX .coords saves as `manual` pick lists server-side;
        # same lifecycle as the monitor — started/stopped in main.py.
        self.curation_watcher = CurationWatcher(self)
        # Pending debounced saves, keyed by project path — see save_project().
        self._pending_saves: dict[str, asyncio.Task] = {}
        # Projects-roster hover previews: project path -> (activity ts it was found at, preview).
        self._tomo_preview_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
        # Projects roster: base path -> its last scan, so a newly opened roster shows at once.
        self._project_scans: dict[str, list[dict[str, Any]]] = {}

    def registry_for(self, project_path: Path) -> TiltSeriesRegistry:
        """TiltSeriesRegistry for a project. Lazily loaded from sidecar JSON
        at `{project_path}/registry/`. Empty for legacy projects without a
        registry on disk — populate via project_service on import/load."""
        return get_registry_for(project_path)

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: list[str], required_paths: list[str]
    ):
        """
        Unified pipeline start method.
        selected_jobs is a list of instance_id strings (e.g. ['tsReconstruct', 'templatematching__ribosome']).
        """
        return await self.pipeline_orchestrator.deploy_and_run_scheme(
            project_dir=Path(project_path), selected_instance_ids=selected_jobs
        )

    async def delete_job(self, job_name: str, project_path: Path, instance_id: str | None = None) -> dict[str, Any]:
        return await self.project_service.delete_job(job_name, project_path=project_path, instance_id=instance_id)

    async def save_project(
        self, project_path: Path | str | None, *, force: bool = False, debounce_s: float | None = None
    ) -> None:
        """Persist a project's ProjectState to its project_params.json (explicit
        path only). `force` writes even when the state isn't marked dirty.

        With `debounce_s`, calls coalesce per project on a trailing edge: each
        call re-arms the timer and one save runs `debounce_s` after the last
        call. This is the hot-path policy (merge-card checkboxes at 0.4 s,
        config fields at 1.0 s) — a full save is ~hundreds of ms on a real
        project, too slow to run inline on every click/keystroke."""
        if not project_path:
            logger.warning("backend.save_project called without a project_path — nothing saved")
            return
        path = Path(project_path)
        if debounce_s is None:
            await self.state_service.save_project(project_path=path, force=force)
            return
        key = str(path)
        pending = self._pending_saves.get(key)
        if pending and not pending.done():
            pending.cancel()

        async def _delayed() -> None:
            try:
                await asyncio.sleep(debounce_s)
            except asyncio.CancelledError:
                # A newer save superseded this debounce -- dropping the stale one is the point.
                return
            try:
                await self.state_service.save_project(project_path=path, force=force)
            except Exception as e:
                logger.warning("Debounced save for %s failed: %s", path, e)

        self._pending_saves[key] = asyncio.create_task(_delayed())

    async def submit_tilt_filter_predict(self, project_path: Path, instance_id: str) -> dict[str, Any]:
        """Submit one DL prediction run of the tilt filter: drivers/tilt_filter.py as a
        one-off SLURM job in its own directory (TiltFilter/dl_run/NNN). The run is recorded
        in `job_model.predict_run`, which the PipelineMonitor settles from the exit markers
        and SLURM (`PipelineRunnerService.reconcile_tilt_filter_predict`), so neither a
        closed tab nor a server restart loses it. The job's execution_status says whether a
        verdict is committed and is left alone."""
        from services.jobs.tilt_filter import (
            TiltFilterPredictRun,
            calibrated_threshold,
            next_predict_run_dir,
            resolve_model,
        )
        from services.path_resolution_service import PathResolutionError, PathResolutionService

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        job_model = state.jobs.get(instance_id)
        if not job_model:
            return err(f"Job '{instance_id}' not found")
        if job_model.predict_in_flight:
            run = job_model.predict_run
            return err(f"A prediction run is already {run.status.value.lower()} (SLURM job {run.slurm_job_id}).")
        if job_model.auto_in_flight:
            return err("The DL-auto job is in flight and predicts every tilt itself; run DL once it has finished.")
        try:
            model_key, _entry = resolve_model(job_model.model)
        except ValueError as e:
            return err(str(e))

        try:
            io_paths = PathResolutionService(state).resolve_all_paths(
                job_model.job_type, job_model, project_path / "TiltFilter", instance_id=instance_id
            )
        except PathResolutionError as e:
            return err(f"Path resolution failed: {e}")
        input_star = io_paths.get("input_star")
        if input_star is None or not (project_path / input_star).exists():
            return err("The motion-correction output star is not on disk yet; run fsMotion first.")
        try:
            driver_cmd = driver_invocation(
                server_dir=self.server_dir,
                driver_script=self.server_dir / "drivers" / "tilt_filter.py",
                instance_id=instance_id,
                project_path=project_path,
            )
        except FileNotFoundError as e:
            return err(str(e))

        # Rendered before the run is recorded, so a broken qsub.sh leaves the job as it was.
        run_dir = next_predict_run_dir(project_path)
        try:
            sbatch_path = write_sbatch_script(
                run_dir / "run_tilt_filter.sh", job_model.get_effective_slurm_config(), driver_cmd
            )
        except (OSError, ValueError) as e:
            logger.exception("Tilt filter DL: sbatch script render failed")
            return err(str(e))
        job_model.paths.update({k: str(v) for k, v in io_paths.items() if v is not None})

        # The first run of a model moves the job's threshold to that model's recorded cut. Its
        # predictions replace the previous model's, which the old threshold was set against;
        # re-running the same model keeps a threshold the user has tuned.
        previous = job_model.predict_run
        cut = calibrated_threshold(model_key)
        if cut is not None and (previous is None or previous.model != model_key):
            job_model.threshold = cut

        # Recorded before the first await, so a second click finds this run in flight.
        run = TiltFilterPredictRun(job_dir=str(run_dir), model=model_key)
        job_model.predict_run = run
        # A run that starts supersedes the reason an automatic one could not be submitted.
        job_model.auto_error = ""
        state.mark_dirty()
        # The driver reads the run off project_params.json, so it must be on disk before sbatch.
        await self.state_service.save_project(project_path=project_path, force=True)

        try:
            proc = await asyncio.create_subprocess_exec(
                "sbatch",
                str(sbatch_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(run_dir),
            )
            stdout, stderr = await proc.communicate()
        except OSError as e:
            logger.exception("Tilt filter DL: sbatch could not be started")
            run.status = JobStatus.FAILED
            run.error = f"sbatch could not be started: {e}"
            await self.state_service.save_project(project_path=project_path, force=True)
            return err(run.error)
        if proc.returncode != 0:
            run.status = JobStatus.FAILED
            run.error = f"sbatch failed: {stderr.decode().strip()}"
            await self.state_service.save_project(project_path=project_path, force=True)
            return err(run.error)

        # "Submitted batch job 12345"
        output = stdout.decode().strip()
        run.slurm_job_id = output.split()[-1] if output else None
        await self.state_service.save_project(project_path=project_path, force=True)
        events.info(
            "%s · tilt filter DL run %s queued as SLURM %s", Path(project_path).name, run_dir.name, run.slurm_job_id
        )
        return ok(slurm_job_id=run.slurm_job_id, job_dir=str(run_dir))

    async def autostart_tilt_filter_predict(self, project_path: Path, *, fresh: bool = False) -> None:
        """DL review's prediction run, submitted without a click: only for a filter a Run includes
        (the run's hold names it), in DL review, uncommitted, with fsMotion succeeded and no run in
        flight. `fresh`: fsMotion has just made new averages, so a run that succeeded before does
        not count. Callers: deploy (fsMotion already done), the afterok reconciler's fsMotion
        SUCCEEDED edge, and a switch to DL review. A refused submit is recorded in the job's
        `auto_error`, which its row shows, and logged."""
        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        hold = state.review_hold
        job_model = state.jobs.get(hold.barrier) if hold is not None else None
        if (
            job_model is None
            or job_model.job_type != JobType.TILT_FILTER
            or job_model.mode != FilterMode.DL_REVIEW
            or job_model.execution_status == JobStatus.SUCCEEDED
            or job_model.predict_in_flight
        ):
            return
        if not any(
            jm.job_type == JobType.FS_MOTION_CTF and jm.execution_status == JobStatus.SUCCEEDED
            for jm in state.jobs.values()
        ):
            return
        run = job_model.predict_run
        if not fresh and run is not None and run.status == JobStatus.SUCCEEDED:
            return
        res = await self.submit_tilt_filter_predict(project_path, hold.barrier)
        if res["success"]:
            return
        job_model.auto_error = f"The automatic DL run could not be submitted: {res['error']}"
        state.mark_dirty()
        await self.state_service.save_project(project_path=project_path, force=True)
        events.warning("%s · %s", project_path.name, job_model.auto_error)

    async def cancel_tilt_filter_predict(self, project_path: Path, instance_id: str) -> dict[str, Any]:
        """Cancel the tilt filter's in-flight DL prediction run. Only scancels: the monitor
        settles the run FAILED, as it does any run SLURM ended."""
        state = self.state_service.state_for(Path(project_path))
        job_model = state.jobs.get(instance_id)
        if job_model is None or not job_model.predict_in_flight:
            return err("No DL prediction run is in flight.")
        slurm_id = job_model.predict_run.slurm_job_id
        if not slurm_id:
            return err("The run has no SLURM job id yet; try again in a moment.")
        res = await self.slurm_service.scancel_jobs([str(slurm_id)])
        if not res.get("success"):
            return err(f"scancel of SLURM job {slurm_id} failed: {res.get('error')}")
        run_name = Path(job_model.predict_run.job_dir).name
        events.info("%s · tilt filter DL run %s cancelled (SLURM %s)", Path(project_path).name, run_name, slurm_id)
        return ok()

    async def set_tilt_filter_mode(self, project_path: Path, instance_id: str, mode: FilterMode) -> dict[str, Any]:
        """Set the tilt filter's mode (its job row). Refused while a prediction run or the DL-auto
        job is in flight: both write the predictions the mode reads. Leaving DL auto after its job
        failed sets the filter back to uncommitted, which is what the review modes' status means.
        A switch to DL auto while a run's jobs wait for this filter's review submits the filter's
        job ahead of them (submit_parked), and `resume` carries that outcome."""
        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        job_model = state.jobs.get(instance_id)
        if job_model is None:
            return err(f"Job '{instance_id}' not found.")
        if mode == job_model.mode:
            return ok()
        if job_model.predict_in_flight:
            return err("A DL prediction run is in flight; switch once it has landed or been cancelled.")
        if job_model.auto_in_flight:
            return err("The DL-auto job is in flight; switch once it has finished, or stop the pipeline.")
        if job_model.mode == FilterMode.DL_AUTO and job_model.execution_status == JobStatus.FAILED:
            job_model.execution_status = JobStatus.SCHEDULED
            # Else the reconciler tracks the job again and reads its failure marker back.
            job_model.slurm_job_id = None
        job_model.mode = mode
        # The reason belonged to the mode left behind (a DL-auto job, or DL review's automatic run).
        job_model.auto_error = ""
        state.mark_dirty()
        # Not a USER_PARAMS field, so nothing marks the project dirty on its own: force the save.
        await self.state_service.save_project(project_path=project_path, force=True)
        events.info("%s · %s mode %s", Path(project_path).name, instance_id, mode.value)
        res = ok()
        hold = state.review_hold
        if mode == FilterMode.DL_AUTO and hold is not None and hold.barrier == instance_id:
            res["resume"] = await self.pipeline_orchestrator.submit_parked(project_path)
        elif mode == FilterMode.DL_REVIEW:
            await self.autostart_tilt_filter_predict(project_path)
        return res

    async def approve_tilt_filter(self, project_path: Path, instance_id: str) -> dict[str, Any]:
        """Approve the tilt filter's review: commit its verdict through the one commit path
        (`commit_verdict`) and persist the project. The gallery and the job row both call this.

        When a run parked jobs behind this filter, they are submitted too, and `resume` carries
        that outcome. The commit stands when the resume is refused; the caller reports both."""
        from services.jobs.tilt_filter import commit_verdict

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        res = await commit_verdict(state, project_path, instance_id)
        if not res["success"]:
            return res
        await self.state_service.save_project(project_path=project_path, force=True)
        events.info(
            "%s · %s approved: %d kept, %d dropped", project_path.name, instance_id, res["kept"], res["dropped"]
        )
        hold = state.review_hold
        if hold is not None and hold.barrier == instance_id:
            res["resume"] = await self.pipeline_orchestrator.submit_parked(project_path)
        return res

    async def reopen_tilt_filter(self, project_path: Path, instance_id: str) -> dict[str, Any]:
        """Undo the tilt filter's Approve (`reopen_review`), so the next Run waits for a review."""
        from services.jobs.tilt_filter import reopen_review

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        res = reopen_review(state, instance_id)
        if res["success"]:
            await self.state_service.save_project(project_path=project_path, force=True)
            events.info("%s · %s re-opened for review", project_path.name, instance_id)
        return res

    async def extract_pick_list(
        self,
        project_path: Path,
        candidate_optset: Path | None,
        list_star: Path,
        tomo_name: str,
        species_id: str,
        slug: str,
        *,
        box_size: int,
        binning: float,
        crop_size: int,
        tomograms_star: Path | None = None,
        max_dose: float = -1.0,
        min_frames: int = 1,
        do_stack2d: bool = True,
        do_float16: bool = True,
    ) -> dict[str, Any]:
        """Subtomo-extract one curation pick list: submit ``drivers/extract_pick_list.py``
        as a one-off SLURM job via ``config/qsub.sh`` (same mechanism as
        ``submit_tilt_filter_predict``). Output lands in ``<list_star dir>/<slug>/`` so lists
        extract independently. Returns the SLURM job id + the dir to watch
        (``RELION_JOB_EXIT_SUCCESS/FAILURE`` + ``result.json`` appear there; the caller
        records ``PickList.mark_extracted`` on success).

        Exactly one schema source: ``candidate_optset`` mirrors the species'
        candidate-extract schema; ``tomograms_star`` synthesizes it for a de-novo
        species with no candidate-extract job.

        ``box_size``/``binning``/``crop_size`` have no defaults: a default would cut
        wrong-but-plausible subtomograms for any species without a subtomo job. The caller
        resolves them from the species (see
        ``aggregation.extraction.extraction_params_for_species``) or asks the user."""
        project_path = Path(project_path)
        if (candidate_optset is None) == (tomograms_star is None):
            return err("extract_pick_list needs exactly one of candidate_optset / tomograms_star")
        out_dir = Path(list_star).parent / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        state = self.state_service.state_for(project_path)
        instance_id = extract_pick_list_instance_id(species_id, tomo_name, slug)

        # A re-extract over a live extraction cancels it and goes on only once SLURM no longer
        # runs it: wiping out/ under it would break both runs, and the new supervisor would find
        # the dir still owned and exit.
        prior = state.jobs.get(instance_id)
        if (
            prior is not None
            and prior.slurm_job_id
            and prior.execution_status in (JobStatus.SCHEDULED, JobStatus.QUEUED, JobStatus.RUNNING)
        ):
            ended = await self._cancel_and_await_end(str(prior.slurm_job_id), project_path.name)
            if not ended["success"]:
                return ended

        # ── job identity ────────────────────────────────────────────────────────
        # Create/update the per-list instance before sbatch: the driver bootstraps
        # off it and `get_driver_context` exits when the instance is missing. Two
        # orderings here are load-bearing:
        #   · `execution_status` is reset first because writes to USER_PARAMS fields
        #     are silently dropped once a job leaves SCHEDULED/FAILED — the reverse
        #     order would re-cut a re-curated list with the previous geometry and
        #     report nothing.
        #   · the whole instance update happens before the output below is destroyed.
        #     The Picks tab reads both from this shared in-memory state, so a render
        #     landing in between would pair the freshly-absent output
        #     (`extraction_state()` → NOT_EXTRACTED) with the previous run's
        #     Succeeded/Failed and its stale error text. Resetting first leaves only
        #     "not extracted · submitting" as the visible transient, which is true.
        # `paths["job_dir"]` is the out dir qsub already writes run.out/run.err and
        # the exit markers into. `reconcile_afterok` resolves a job by that same dir,
        # but only while the project has a real pipeline job live: this job never sets
        # `pipeline_active` itself, so once the chain ends the monitor drops the project
        # and stops looking. `reconcile_pick_list_extractions` is what covers the rest.
        state.ensure_job_initialized(JobType.EXTRACT_PICK_LIST, instance_id=instance_id)
        jm = state.jobs[instance_id]
        jm.execution_status = JobStatus.SCHEDULED
        jm.last_error = ""
        jm.species_id = species_id
        jm.tomo_name = tomo_name
        jm.list_slug = slug
        jm.candidate_optset = str(candidate_optset) if candidate_optset is not None else ""
        jm.tomograms_star = str(tomograms_star) if tomograms_star is not None else ""
        jm.list_star = str(list_star)
        jm.box_size = int(box_size)
        jm.binning = float(binning)
        jm.crop_size = int(crop_size)
        jm.max_dose = float(max_dose)
        jm.min_frames = int(min_frames)
        jm.do_stack2d = bool(do_stack2d)
        jm.do_float16 = bool(do_float16)
        jm.paths["job_dir"] = str(out_dir)
        await self.state_service.save_project(project_path=project_path, force=True)

        # The driver reads every one of those values off the instance, so the launch is the
        # standard instance form: --instance_id/--project_path, and qsub cd's into out_dir,
        # which is what the driver takes as its job dir.
        try:
            driver_cmd = driver_invocation(
                server_dir=self.server_dir,
                driver_script=self.server_dir / "drivers" / "extract_pick_list.py",
                instance_id=instance_id,
                project_path=project_path,
            )
        except FileNotFoundError as e:
            return err(str(e))

        # A user-initiated (re-)extract must re-cut from scratch: clear any prior
        # extraction output so drivers/extract_pick_list.py does NOT hit its idempotency
        # skip (out/particles.star + out/Subtomograms/) and silently reuse stale
        # subtomograms for a now-curated pick set. A genuine SLURM requeue reruns
        # run_extract.sh directly (bypassing this) and still benefits from that skip.
        # `run.out`/`run.err` go with the markers: SLURM only creates them when the
        # allocation starts, which is what `_await_extraction_outdirs` reads as "this
        # job left the queue" — a previous run's leftovers would report RUNNING while
        # the new job is still PENDING.
        # Off the loop: `out/` holds one 2D stack per particle, so on Lustre this rmtree is
        # seconds of blocking syscalls — and the batch path runs it once per pending list
        # before submitting any of them, which would stall every other client for the whole
        # prologue.
        def _clear_previous_output() -> None:
            for marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE", "result.json", "run.out", "run.err"):
                (out_dir / marker).unlink(missing_ok=True)
            shutil.rmtree(out_dir / "out", ignore_errors=True)

        await asyncio.to_thread(_clear_previous_output)

        try:
            # SLURM resources = the project's defaults (proven to run relion_tomo_subtomo;
            # over-provisioned for one tomo but consistent with the pipeline extraction).
            sbatch_path = write_sbatch_script(out_dir / "run_extract.sh", state.slurm_defaults, driver_cmd)
            proc = await asyncio.create_subprocess_exec(
                "sbatch",
                str(sbatch_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(out_dir),
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                msg = f"sbatch failed: {stderr.decode().strip()}"
                jm.execution_status = JobStatus.FAILED
                jm.last_error = msg
                await self.state_service.save_project(project_path=project_path, force=True)
                return err(msg)
            output = stdout.decode().strip()
            slurm_job_id = output.split()[-1] if output else None
            events.info(
                "%s · per-list extraction %s/%s queued as SLURM %s", project_path.name, species_id, slug, slurm_job_id
            )
            jm.slurm_job_id = slurm_job_id
            jm.execution_status = JobStatus.QUEUED
            await self.state_service.save_project(project_path=project_path, force=True)
            return ok(slurm_job_id=slurm_job_id, out_dir=str(out_dir), instance_id=instance_id)
        except Exception as e:
            jm.execution_status = JobStatus.FAILED
            jm.last_error = f"submit failed: {e}"
            await self.state_service.save_project(project_path=project_path, force=True)
            return err(str(e))

    async def extract_pick_list_and_wait(
        self,
        project_path: Path,
        candidate_optset: Path | None,
        list_star: Path,
        tomo_name: str,
        species_id: str,
        slug: str,
        *,
        timeout_s: int = 3600,
        **params: Any,
    ) -> dict[str, Any]:
        """Submit ONE per-list extraction (``extract_pick_list``), await its out dir,
        and on success record ``PickList.mark_extracted`` + persist — the single-list
        counterpart of ``extract_pending_lists``. RUN INSIDE A BackgroundTask
        (polls up to ``timeout_s``; persists by explicit path — no client context
        needed). Returns {"success", "count"} or {"success": False, "error"}."""
        project_path = Path(project_path)
        res = await self.extract_pick_list(
            project_path, candidate_optset, list_star, tomo_name, species_id, slug, **params
        )
        if not res.get("success"):
            return res
        out_dir = res["out_dir"]
        results = await self._await_extraction_outdirs(
            {out_dir: (res["instance_id"], res.get("slurm_job_id"))}, timeout_s, project_path=project_path
        )
        r = results.get(out_dir)
        if r is None:
            return err("extraction still running — check the SLURM job / tray")
        status, data = r
        if status == "failed":
            return err(data.get("error") or "extraction job failed — see run.err in the list's out dir")
        if not data.get("ok"):
            return err(data.get("error") or "extraction produced no usable result")
        state = self.state_service.state_for(project_path)
        pl = state.get_pick_list(slug, species_id, tomo_name)
        if pl is not None:
            pl.mark_extracted(data["optimisation_set"], int(data.get("count", 0)))
            state.mark_dirty()
            state.bump_registry_rev()
            await self.state_service.save_project(project_path=project_path, force=True)
        return ok(count=int(data.get("count", 0)))

    async def get_pending_extractions(self, project_path: Path, species_id: str) -> list[dict[str, Any]]:
        """Read-only: this species' pick lists whose subtomograms are missing or stale, as
        {tomo_name, slug, label, extraction_state, blocked_reason} — the preview
        ``extract_pending_lists`` acts on, so the user sees what would be submitted (and what
        cannot be) before anything is sent. Disk reads run off the event loop."""
        from dataclasses import asdict
        from services.aggregation.extraction import pending_extractions

        state = self.state_service.state_for(Path(project_path))
        rows = await asyncio.to_thread(pending_extractions, state, Path(project_path), species_id)
        return [asdict(r) for r in rows]

    # ── Tomogram import (PARTICLES-header utility; a project-level artifact, not a job) ──

    async def list_tomogram_candidates(self, directory_or_glob: str) -> list[str]:
        """Resolve a directory OR a glob to a sorted list of tomogram files (off the event
        loop). Both forms are filtered to ``TOMOGRAM_SUFFIXES`` (``.mrc`` + ``.rec``) — this
        is a tomogram importer, and the glob the directory widget composes is a bare ``*``
        precisely so a directory of etomo ``.rec`` files is not invisible here."""
        from services.tomogram_import import TOMOGRAM_SUFFIXES

        def _list() -> list[str]:
            import glob as _glob

            s = str(Path(directory_or_glob).expanduser()) if directory_or_glob else ""
            if not s:
                return []
            paths = Path(s).iterdir() if Path(s).is_dir() else (Path(p) for p in _glob.glob(s))
            return sorted(str(p) for p in paths if p.is_file() and p.suffix.lower() in TOMOGRAM_SUFFIXES)

        return await asyncio.to_thread(_list)

    async def probe_tomogram_metadata(self, paths: list[str]) -> list[dict[str, Any]]:
        """MRC header metadata (dims, voxel size, has_voxel_size) for the import preview,
        read off the event loop. Surfaces a missing voxel size so the widget can flag it
        before commit (never silently defaults apix)."""
        from services.tomogram_import import probe_mrc_metadata

        return await asyncio.to_thread(probe_mrc_metadata, [Path(p) for p in paths])

    async def preview_reference_tomograms(self, reference_star: str) -> list[dict[str, Any]]:
        """Read an existing ``tomograms.star`` (off the event loop) and project its rows
        into the same preview-dict shape ``probe_tomogram_metadata`` returns, so the import
        dialog can show a review table before referencing it. Propagates the
        FileNotFoundError / 'no rlnTomoName block' guards from ``_reference_rows`` to the UI."""
        from services.tomogram_import import preview_reference_metadata

        return await asyncio.to_thread(preview_reference_metadata, reference_star)

    async def commit_imported_tomograms(
        self,
        project_path: Path,
        *,
        mode: str = "synthesize",
        mrc_paths: list[str] | None = None,
        reference_star: str = "",
        pixel_size_angstrom: float = 0.0,
        tomogram_binning: float = 1.0,
        optics_group_name: str = "opticsGroup1",
        replace: bool = False,
    ) -> dict[str, Any]:
        """Add one import batch and rebuild ``Tomograms/tomograms.star`` from every batch the
        project holds. ``replace=True`` discards the prior batches instead, as an explicit
        user choice.

        Raises (propagated to the UI) if the batch being added cannot be read — a selected
        MRC with no voxel size and no pixel-size override, a reference star that is gone —
        because that is the thing the user is doing right now and can fix. The writer raises
        BEFORE touching the committed star, so a failed import leaves both the file and the
        recorded state exactly as they were. A PRIOR batch that has since become unreadable
        does NOT block the commit: it contributes zero rows and its error is returned under
        ``batch_errors``, since nothing in this dialog can repair a directory that moved.

        Microscope/optics values come from the project's microscope + acquisition params.
        Returns {count, star_path, added, renamed, skipped, batch_errors, batches}."""
        from services.tomogram_import import write_tomograms_star
        from services.project_state import ImportBatch, ImportedTomograms

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        out_rel = Path("Tomograms") / "tomograms.star"
        out_abs = project_path / out_rel

        rec = state.imported_tomograms
        prior = [] if (replace or rec is None) else rec.effective_batches()
        new_batch = ImportBatch(
            source_mode=mode,
            source_paths=[str(p) for p in (mrc_paths or [])],
            reference_star=reference_star,
            pixel_size_angstrom=pixel_size_angstrom,
            tomogram_binning=tomogram_binning,
            optics_group_name=optics_group_name,
        )
        all_batches = [*prior, new_batch]

        result = await asyncio.to_thread(
            write_tomograms_star,
            out_abs,
            batches=[b.model_dump() for b in all_batches],
            voltage=state.microscope.acceleration_voltage_kv,
            spherical_aberration=state.microscope.spherical_aberration_mm,
            amplitude_contrast=state.microscope.amplitude_contrast,
            invert_defocus_hand=state.acquisition.invert_defocus_hand,
            project_tag=project_path.name,
        )
        reports = result["per_batch"]
        for batch, report in zip(all_batches, reports, strict=True):
            batch.count = int(report["count"])
            batch.renamed = report["renamed"]
            batch.skipped = report["skipped"]
        state.set_imported_tomograms(
            ImportedTomograms(
                star_path=str(out_rel),
                batches=all_batches,
                source_mode=mode,
                source_paths=[str(p) for p in (mrc_paths or [])],
                reference_star=reference_star,
                pixel_size_angstrom=pixel_size_angstrom,
                tomogram_binning=tomogram_binning,
                optics_group_name=optics_group_name,
                count=int(result["count"]),
            )
        )
        await self.state_service.save_project(project_path=project_path, force=True)
        return {
            "count": int(result["count"]),
            "star_path": str(out_abs),
            "added": int(reports[-1]["count"]),
            "renamed": reports[-1]["renamed"],
            "skipped": reports[-1]["skipped"],
            "batch_errors": [
                {"batch": b.label, "error": r["error"]} for b, r in zip(all_batches, reports, strict=True) if r["error"]
            ],
            "batches": len(all_batches),
        }

    # ── Segmentation import (Tomograms view; a project-level artifact, not a job) ──

    async def list_segmentation_files(self, source: str) -> list[str]:
        """The .mrc files of a folder or glob, off the event loop."""
        from services.segmentation.ingest import list_volume_files

        return await asyncio.to_thread(list_volume_files, source)

    def segmentation_import_names(self, project_path: Path, recon_instance: str) -> dict[str, dict]:
        """The tomogram names files can be matched to (`ingest.tomogram_names`) for one reconstruct
        job. On the event loop: the cached registry is shared with every reader."""
        from services.segmentation.ingest import tomogram_names

        state = self.state_service.state_for(Path(project_path))
        return tomogram_names(state, get_registry_for(Path(project_path)).all_tilt_series(), recon_instance)

    async def probe_segmentation_volumes(
        self, paths: list[str], progress_cb: ProgressCallback | None = None
    ) -> dict[str, dict]:
        """Grid, data type and value range of each volume, read in a worker (every value is read)."""
        from services.segmentation.ingest import probe_volumes
        from services.workers import WorkKind, run_in_worker

        return await run_in_worker(WorkKind.SEGMENTATIONS, probe_volumes, list(paths), progress_cb=progress_cb)

    async def plan_segmentation_import(
        self,
        project_path: Path,
        files: list[str],
        names: dict[str, dict],
        feature: str,
        full_scale: float | None,
        probes: dict,
    ) -> list[dict]:
        """The import's rows (`ingest.plan_import`), skipping files the registry holds already; reads
        the tomograms' headers off the event loop."""
        from services.segmentation.ingest import plan_import, registered_paths

        state = self.state_service.state_for(Path(project_path))
        registered = registered_paths(state, get_registry_for(Path(project_path)).all_tilt_series())
        return await asyncio.to_thread(plan_import, files, names, feature, full_scale, probes, registered)

    async def commit_segmentation_import(
        self,
        project_path: Path,
        rows: list[dict],
        *,
        recon_instance: str,
        source: str,
        feature: str,
        full_scale: float | None,
        progress_cb: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """Register the rows a plan accepted as a new batch: their overlay masks and fractions (in a
        worker), the batch's segmentations.json, the volumes in the tilt-series registry, and the
        features and the batch in the project. The volumes are referenced where they are.
        Returns ok(batch, n_volumes, features, skipped=[{name, reason}])."""
        from services.models_base import FeatureOrigin
        from services.project_state import SegmentationImport
        from services.segmentation.ingest import IMPORTS_DIR, batch_dir, import_manifest, new_batch_id, summarize_import
        from services.segmentation.manifest import tomogram_outputs, write_manifest
        from services.workers import WorkKind, run_in_worker

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        batch = new_batch_id({b.batch for b in state.segmentation_imports})
        out_dir = batch_dir(project_path, batch)
        rows = await run_in_worker(
            WorkKind.SEGMENTATIONS, summarize_import, rows, str(out_dir / "vis"), progress_cb=progress_cb
        )
        skipped = [{"name": r["name"], "reason": r["reason"]} for r in rows if not r.get("ok")]
        manifest = import_manifest(
            batch, rows, recon_instance=recon_instance, source=source, feature=feature, full_scale=full_scale
        )
        if not manifest.entries:
            return err("Nothing was registered: no volume could be read", skipped=skipped)
        await asyncio.to_thread(out_dir.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(write_manifest, out_dir, manifest)

        registry = get_registry_for(project_path)
        for ts_id, output in tomogram_outputs(manifest, out_dir).items():
            registry.attach_tomogram_output(ts_id, output)
        await registry.save_async()

        for feature_id in manifest.features():
            state.add_feature(feature_id, origin=FeatureOrigin.IMPORT)
        state.segmentation_imports.append(
            SegmentationImport(
                batch=batch,
                dir=str(IMPORTS_DIR / batch),
                source=source,
                recon_instance=recon_instance,
                features=manifest.features(),
                n_volumes=len(manifest.entries),
            )
        )
        state.mark_dirty()
        await self.state_service.save_project(project_path=project_path, force=True)
        events.info(
            "%s · registered %d segmentation volume(s) from %s as %s",
            project_path.name,
            len(manifest.entries),
            source,
            batch,
        )
        return ok(batch=batch, n_volumes=len(manifest.entries), features=manifest.features(), skipped=skipped)

    async def remove_segmentation_import(self, project_path: Path, batch: str) -> dict[str, Any]:
        """Forget one import batch: its volumes leave the tilt-series registry and the batch's
        manifest and overlay masks are deleted. The volumes themselves are never touched."""
        from services.segmentation.ingest import producer_id

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        rec = state.get_segmentation_import(batch)
        if rec is None:
            return err(f"This project has no segmentation import {batch}")
        producer = producer_id(batch)
        registry = get_registry_for(project_path)
        for ts in list(registry.all_tilt_series()):
            if ts.tomogram is not None and producer in ts.tomogram.outputs:
                registry.detach_tomogram_output(ts.id, producer)
        await registry.save_async()
        state.segmentation_imports = [b for b in state.segmentation_imports if b.batch != batch]
        state.mark_dirty()
        await self.state_service.save_project(project_path=project_path, force=True)
        try:
            await asyncio.to_thread(shutil.rmtree, project_path / rec.dir)
        except FileNotFoundError:
            pass  # the batch's folder is already gone: nothing left to delete
        except OSError as e:
            logger.exception("Segmentation import %s: could not delete %s", batch, rec.dir)
            return ok(warning=f"Removed from the project, but {rec.dir} could not be deleted: {e}")
        return ok()

    async def _await_extraction_outdirs(
        self, targets: dict[str, tuple[str, str | None]], timeout_s: int, *, project_path: Path
    ) -> dict[str, tuple]:
        """Poll each per-list extraction out dir (RELION_JOB_EXIT_* + result.json) until all
        resolve or ``timeout_s`` elapses, moving each list's job instance as it goes.
        ``targets`` maps out dir → (the ``extractPickList__…`` instance id that cuts it, the
        SLURM id THIS awaiter submitted — the instance is shared, so that id is how a status
        write proves it belongs to the run being watched).
        Returns {out_dir: ("done"|"failed", data)} for the resolved ones (an out dir absent
        from the result = still running). Disk scans run off the event loop. Both the batch
        path (extract_pending_lists) and the per-list path (extract_pick_list_and_wait) use it.

        While it runs it is the only refresher those instances get: a one-off extraction does
        not set ``pipeline_active`` (the roster would read as a live run), and
        ``PipelineMonitor._tick_once`` iterates only active projects — so
        QUEUED → RUNNING → SUCCEEDED/FAILED happens here, and the driver's own error text
        lands on the instance so a failure outlives the dialog that launched it.
        If this awaiter dies (server restart, tray cancel, or ``timeout_s`` elapsing), nothing
        here moves the instance again and ``PickList.mark_extracted`` never lands, leaving a
        finished job's optimisation set orphaned on disk with both surfaces reading
        "not extracted" (``PickList.extraction_state()`` keys off the recorded
        ``extracted_path``). ``reconcile_pick_list_extractions`` is the recovery: it asks SLURM
        about any instance left non-terminal and settles it off this same out dir.
        Re-extracting is a recovery path too, so nothing on it may be hard-blocked."""
        pending = set(targets)
        results: dict[str, tuple] = {}
        if not pending:
            return results
        started: set[str] = set()

        def _scan(dirs: set) -> tuple[dict[str, tuple], set[str]]:
            done: dict[str, tuple] = {}
            running: set[str] = set()
            for od in dirs:
                status, data = _read_extraction_outdir(Path(od))
                if status in ("done", "failed"):
                    done[od] = (status, data)
                elif status == "running":
                    running.add(od)
            return done, running

        for _ in range(max(1, timeout_s // 10)):
            if not pending:
                break
            done, running = await asyncio.to_thread(_scan, set(pending))
            for od, r in done.items():
                results[od] = r
                pending.discard(od)
            fresh_running = running - started
            started |= running
            await self._record_extraction_progress(project_path, targets, done, fresh_running)
            if not pending:
                break
            await asyncio.sleep(10)
        return results

    async def _record_extraction_progress(
        self, project_path: Path, targets: dict[str, tuple[str, str | None]], done: dict[str, tuple], started: set[str]
    ) -> None:
        """Write one poll tick of ``_await_extraction_outdirs`` onto the per-list extraction
        instances: the ones SLURM has just started → RUNNING, the resolved
        ones → SUCCEEDED, or FAILED carrying the driver's own error text. ``last_error`` sits
        outside ``USER_PARAMS`` precisely so it stays writable on a job that has left
        SCHEDULED. One forced save per tick that changed something, by EXPLICIT path — both
        callers run inside a BackgroundTask, which has no client context to resolve one from."""
        state = self.state_service.state_for(project_path)

        def _watched(od: str):
            """The instance this awaiter is entitled to write, or None (with the reason
            logged). Re-extracting a list reuses its instance, so a user-forced second
            submit while one is in flight leaves the older awaiter watching an instance
            that now tracks a different SLURM job — it must not stamp its own outcome
            over the newer run."""
            iid, submitted_id = targets[od]
            jm = state.jobs.get(iid)
            if jm is None:
                logger.warning("per-list extraction in %s has no job instance %s", od, iid)
                return None
            if jm.slurm_job_id != submitted_id:
                logger.info(
                    "extraction watcher for %s is stale: instance tracks SLURM %s, this run was %s",
                    iid,
                    jm.slurm_job_id,
                    submitted_id,
                )
                return None
            return jm

        changed = False
        for od in started:
            jm = _watched(od)
            if jm is None:
                continue
            if jm.execution_status != JobStatus.RUNNING:
                jm.execution_status = JobStatus.RUNNING
                changed = True
        for od, (status, data) in done.items():
            jm = _watched(od)
            if jm is None:
                continue
            if status == "done" and data.get("ok"):
                jm.execution_status = JobStatus.SUCCEEDED
                jm.last_error = ""
            else:
                jm.execution_status = JobStatus.FAILED
                jm.last_error = str(data.get("error") or "").strip() or f"extraction job failed — see run.err in {od}"
            changed = True
        if changed:
            await self.state_service.save_project(project_path=project_path, force=True)

    async def _cancel_and_await_end(self, sid: str, project_name: str, *, wait_s: float = 180.0) -> dict[str, Any]:
        """scancel an earlier extraction and return once SLURM no longer runs it (a COMPLETING job
        still does: its processes may be writing). err when squeue cannot say, when the scancel
        failed on a job still live, or when it outlives ``wait_s``."""
        live = await self.slurm_service.live_task_counts([sid])
        if live is None:
            return err(f"could not ask SLURM whether the earlier extraction (SLURM {sid}) still runs")
        if not live:
            return ok()
        cancel = await self.slurm_service.scancel_jobs([sid])
        events.info("%s · re-extract: cancelling the earlier extraction, SLURM %s", project_name, sid)
        deadline = time.monotonic() + wait_s
        while True:
            await asyncio.sleep(3)
            live = await self.slurm_service.live_task_counts([sid])
            if live == {}:
                return ok()
            if live and not cancel["success"]:
                return err(f"scancel of the earlier extraction (SLURM {sid}) failed: {cancel['error']}")
            if time.monotonic() > deadline:
                return err(
                    f"the earlier extraction (SLURM {sid}) still runs {wait_s:.0f} s after its cancel; "
                    "re-extract once it has ended"
                )

    async def cancel_pick_list_extraction(
        self, project_path: Path, species_id: str, tomo_name: str, slug: str
    ) -> dict[str, Any]:
        """scancel ONE list's in-flight extraction and mark its instance FAILED.

        The delete path needs this: deleting a pick list removes the out dir a running
        extraction is writing into, the job then re-creates it, and the instance that held its
        SLURM id is popped at the same time — so nothing in the project could stop the job or
        explain the directory that reappeared. No-op ``ok(cancelled=None)`` when nothing is in
        flight."""
        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        jm = state.jobs.get(extract_pick_list_instance_id(species_id, tomo_name, slug))
        if jm is None or jm.execution_status not in (JobStatus.SCHEDULED, JobStatus.QUEUED, JobStatus.RUNNING):
            return ok(cancelled=None)
        sid = str(jm.slurm_job_id or "")
        res = await self.slurm_service.scancel_jobs([sid]) if sid else ok(cancelled=[])
        jm.execution_status = JobStatus.FAILED
        jm.last_error = f"cancelled — the pick list it was extracting was deleted (SLURM {sid or 'not submitted'})"
        state.mark_dirty()
        await self.state_service.save_project(project_path=project_path, force=True)
        if not res.get("success"):
            # Not fatal: the instance is FAILED either way and the caller is mid-delete. Report
            # it so a job that survived the scancel is not a silent surprise later.
            return err(f"scancel of SLURM {sid} failed: {res.get('error')}", cancelled=sid)
        return ok(cancelled=sid or None)

    async def reconcile_pick_list_extractions(
        self, project_path: Path, species_id: str | None = None
    ) -> dict[str, Any]:
        """Settle per-list extraction instances that no awaiter is watching any more.

        ``_await_extraction_outdirs`` is the only thing that moves these instances and it dies
        with its BackgroundTask, so a server restart, a tray Cancel or ``timeout_s`` elapsing
        strands one mid-flight — nothing else picks it up (``IS_INTERACTIVE`` keeps it out of
        every sweep, and ``PipelineMonitor`` ticks only projects with ``pipeline_active``,
        which a one-off extraction never sets). The chip then shows Queued/Running forever,
        and ``extract_pending_lists`` reports the list as "still running" and never resubmits
        it, leaving the per-row confirm as the only way out.

        So ask SLURM instead of trusting the stored status:

          · id still in the queue → genuinely live; promote QUEUED → RUNNING once the
            allocation has started (``run.out`` exists). Left alone otherwise.
          · ``query_jobs_by_ids`` returns None → squeue itself failed. Conclude nothing
            (transient infra is not evidence) and report the instance as ``unknown``.
          · id gone from the queue → the out dir decides. A ``result.json`` with ``ok`` also
            runs ``PickList.mark_extracted``, so an output whose awaiter died is recovered
            rather than re-cut; an exit-failure carries the driver's own text; and
            "left the queue without writing a result" is itself a failure.
          · no ``slurm_job_id`` recorded → ``unknown``, untouched: this is also the
            sub-second window inside ``extract_pick_list`` between resetting the instance and
            sbatch returning, and a job being submitted right now must not be marked failed.

        Cheap when there is nothing to do (no non-terminal instance ⇒ no squeue call) and
        idempotent, so poll paths may call it. Returns
        ``ok(settled=[{instance_id, status, error}], live=[iid], unknown=[iid])``."""
        from services.computing.slurm_service import normalize_slurm_ids

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        stranded = [
            (iid, jm)
            for iid, jm in list(state.jobs.items())
            if jm.job_type == JobType.EXTRACT_PICK_LIST
            and jm.execution_status in (JobStatus.SCHEDULED, JobStatus.QUEUED, JobStatus.RUNNING)
            and (species_id is None or getattr(jm, "species_id", "") == species_id)
        ]
        if not stranded:
            return ok(settled=[], live=[], unknown=[])

        ids = [str(jm.slurm_job_id) for _iid, jm in stranded if jm.slurm_job_id]
        queued = await self.slurm_service.query_jobs_by_ids(ids) if ids else {}
        if queued is None:
            logger.warning(
                "reconcile_pick_list_extractions: squeue failed — leaving %d instance(s) as they are", len(stranded)
            )
            return ok(settled=[], live=[], unknown=[iid for iid, _jm in stranded])

        dirs = {iid: jm.paths.get("job_dir", "") for iid, jm in stranded}
        obs = await asyncio.to_thread(lambda: {iid: _read_extraction_outdir(Path(d)) for iid, d in dirs.items() if d})

        settled: list[dict] = []
        live: list[str] = []
        unknown: list[str] = []
        changed = False
        for iid, jm in stranded:
            disk_status, data = obs.get(iid, ("pending", {}))
            norm = normalize_slurm_ids([str(jm.slurm_job_id)]) if jm.slurm_job_id else []
            if not norm:
                unknown.append(iid)
                continue
            if norm[0] in queued:
                live.append(iid)
                if disk_status == "running" and jm.execution_status != JobStatus.RUNNING:
                    jm.execution_status = JobStatus.RUNNING
                    changed = True
                continue

            # Left the queue with nobody watching. The out dir is the whole evidence.
            if disk_status == "done" and data.get("ok"):
                jm.execution_status = JobStatus.SUCCEEDED
                jm.last_error = ""
                pl = state.get_pick_list(jm.list_slug, jm.species_id, jm.tomo_name)
                if pl is not None and data.get("optimisation_set"):
                    # The awaiter's job, done late: without this the finished optimisation set
                    # stays orphaned on disk and both columns keep reading "not extracted".
                    pl.mark_extracted(data["optimisation_set"], int(data.get("count", 0)))
                    state.bump_registry_rev()
            else:
                jm.execution_status = JobStatus.FAILED
                jm.last_error = str(data.get("error") or "").strip() or (
                    f"SLURM job {jm.slurm_job_id} left the queue without writing a result — "
                    f"see run.out / run.err in {dirs.get(iid) or 'the list out dir'}"
                )
            settled.append({"instance_id": iid, "status": jm.execution_status.value, "error": jm.last_error})
            changed = True

        if changed:
            state.mark_dirty()
            await self.state_service.save_project(project_path=project_path, force=True)
            logger.info(
                "reconcile_pick_list_extractions[%s]: settled=%d live=%d unknown=%d",
                project_path.name,
                len(settled),
                len(live),
                len(unknown),
            )
        return ok(settled=settled, live=live, unknown=unknown)

    async def extract_pending_lists(
        self, project_path: Path, species_id: str, *, timeout_s: int = 3600
    ) -> dict[str, Any]:
        """Submit per-list subtomo extraction for every pick list of this species that is
        NOT_EXTRACTED/STALE, wait for completion, record ``mark_extracted``, and persist.

        Never touches the 'auto' candidate set (not a PickList — re-running the subtomo job is
        what refreshes it) or already-current lists; idempotent (re-running acts only on
        still-pending lists, and a list whose extraction SLURM confirms is still in flight is
        reported under ``still_running`` instead of being resubmitted — its output would be
        wiped from under the running job). Opens with ``reconcile_pick_list_extractions`` so
        that check is made against the queue rather than against a stored status no awaiter is
        maintaining. Reuses ``extract_pick_list`` (which clears ``out/`` for a clean re-cut).
        RUN INSIDE A BackgroundTask — it polls up to ``timeout_s``.

        Returns {submitted, succeeded:[{tomo,slug,count}], failed:[{tomo,slug,error}],
        blocked:[{tomo,slug,reason}], still_running:[{tomo,slug}] (already in flight, or
        submitted here and unfinished at ``timeout_s``), remaining}."""
        from services.aggregation.extraction import (
            extract_inputs_blocked_reason,
            extract_inputs_for_list,
            pending_extractions,
        )

        project_path = Path(project_path)
        # Settle before deciding: an instance whose awaiter died reads Queued/Running with
        # nothing left to move it, and the skip below would then never resubmit that list.
        # It also keeps the count right — a finished-but-unrecorded extraction gets
        # `mark_extracted` here and drops out of `pending` instead of being cut a second time.
        recon = await self.reconcile_pick_list_extractions(project_path, species_id)
        live_instances = set(recon.get("live") or [])
        state = self.state_service.state_for(project_path)
        pending = await asyncio.to_thread(pending_extractions, state, project_path, species_id)

        submitted: list[dict] = []
        failed: list[dict] = []
        blocked: list[dict] = []
        still_running: list[dict] = []
        watching: list[dict] = []  # {tomo, slug, out_dir, instance_id, slurm_job_id}
        for h in pending:
            # Never resubmit a list whose extraction is in flight: a submit wipes out/ and the
            # exit markers first, so a second job would cut into the dir the first is writing.
            # This is the common case — a list counts as "pending" the moment a re-extract
            # clears its output, so an in-flight re-extract puts the list back in this loop.
            # In flight means SLURM still has the id (`live_instances`), not a stored status:
            # the reconciler above has settled everything else. An instance with no recorded id
            # is not skipped — there is no job to clash with. The remaining race (a per-row
            # extract sbatch-ing this list at this moment) is covered by the per-list
            # SingleFlight and the task-registry dedup key.
            if extract_pick_list_instance_id(species_id, h.tomo_name, h.slug) in live_instances:
                logger.info("extract_pending_lists: %s/%s is live in SLURM — not resubmitting", h.tomo_name, h.slug)
                still_running.append({"tomo": h.tomo_name, "slug": h.slug})
                continue
            pl = state.get_pick_list(h.slug, species_id, h.tomo_name)
            inputs = extract_inputs_for_list(state, project_path, species_id, pl) if pl is not None else None
            if inputs is None:
                reason = (
                    extract_inputs_blocked_reason(state, project_path, species_id, pl)
                    if pl is not None
                    else "no pick list registered for this slug"
                )
                blocked.append({"tomo": h.tomo_name, "slug": h.slug, "reason": reason})
                continue
            cand = inputs.get("candidate_optset")
            tomo_star = inputs.get("tomograms_star")
            res = await self.extract_pick_list(
                project_path,
                Path(cand) if cand else None,
                Path(inputs["list_star"]),
                h.tomo_name,
                species_id,
                h.slug,
                tomograms_star=Path(tomo_star) if tomo_star else None,
                **inputs["params"],
            )
            if not res.get("success"):
                failed.append({"tomo": h.tomo_name, "slug": h.slug, "error": res.get("error")})
                continue
            submitted.append({"tomo": h.tomo_name, "slug": h.slug, "slurm_job_id": res.get("slurm_job_id")})
            watching.append(
                {
                    "tomo": h.tomo_name,
                    "slug": h.slug,
                    "out_dir": res["out_dir"],
                    "instance_id": res["instance_id"],
                    "slurm_job_id": res.get("slurm_job_id"),
                }
            )

        results = await self._await_extraction_outdirs(
            {w["out_dir"]: (w["instance_id"], w["slurm_job_id"]) for w in watching},
            timeout_s,
            project_path=project_path,
        )

        succeeded: list[dict] = []
        state = self.state_service.state_for(project_path)
        changed = False
        for w in watching:
            r = results.get(w["out_dir"])
            if r is None:
                still_running.append({"tomo": w["tomo"], "slug": w["slug"]})
                continue
            status, data = r
            if status == "done" and data.get("ok"):
                pl = state.get_pick_list(w["slug"], species_id, w["tomo"])
                if pl is not None:
                    pl.mark_extracted(data["optimisation_set"], int(data.get("count", 0)))
                    changed = True
                succeeded.append({"tomo": w["tomo"], "slug": w["slug"], "count": int(data.get("count", 0))})
            else:
                failed.append(
                    {"tomo": w["tomo"], "slug": w["slug"], "error": data.get("error") or "extraction job failed"}
                )
        if changed:
            state.mark_dirty()
            state.bump_registry_rev()
            await self.state_service.save_project(project_path=project_path, force=True)

        remaining = await asyncio.to_thread(pending_extractions, state, project_path, species_id)
        return {
            "submitted": len(submitted),
            "succeeded": succeeded,
            "failed": failed,
            "blocked": blocked,
            "still_running": still_running,
            "remaining": len(remaining),
        }

    # ── ChimeraX + ArtiaX curation — thin delegation; logic + docs live in
    # services/curation/session_service.py (CurationSessionService) ─────────────

    async def launch_curation_session(
        self, project_path: Path | None = None, cxc_path: Path | None = None, scope: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        res = await self.curation_service.launch_curation_session(project_path, cxc_path=cxc_path, scope=scope)
        if res.get("success") and project_path and (scope or {}).get("curation_dir"):
            # The scoped dir is where the next save lands — rescan it every tick from now,
            # not from the next full sweep (which is what made a first save wait ~30 s).
            self.curation_watcher.mark_hot(Path(project_path), Path(scope["curation_dir"]))
        return res

    async def get_curation_session_info(self, session_dir: str, slurm_job_id: str | None = None) -> dict[str, Any]:
        return await self.curation_service.get_curation_session_info(session_dir, slurm_job_id)

    async def stop_curation_session(self, slurm_job_id: str | None, project_path: Path | None = None) -> dict[str, Any]:
        return await self.curation_service.stop_curation_session(slurm_job_id, project_path=project_path)

    async def find_active_curation_session(self, project_path: Path) -> dict[str, Any] | None:
        return await self.curation_service.find_active_curation_session(project_path)

    async def find_active_curation_session_any(self) -> dict[str, Any] | None:
        return await self.curation_service.find_active_curation_session_any()

    async def send_chimerax_command(
        self, session_info: dict[str, Any], command: str, *, timeout: float = 60.0
    ) -> dict[str, Any]:
        return await self.curation_service.send_chimerax_command(session_info, command, timeout=timeout)

    # The only outbound command to a running session after launch is the confirmed scope
    # switch below, which rewrites the scope on disk in the same call.
    # See services/curation/session_service.py.

    async def switch_curation_session(
        self,
        session_info: dict[str, Any],
        *,
        open_recon: str | None,
        auto_coords: str | None,
        seed_coords: str | None,
        scope: dict[str, Any],
        features: list[dict] | None = None,
    ) -> dict[str, Any]:
        res = await self.curation_service.switch_session_scope(
            session_info,
            open_recon=open_recon,
            auto_coords=auto_coords,
            seed_coords=seed_coords,
            scope=scope,
            features=features,
        )
        if res.get("success") and scope.get("project_path") and scope.get("curation_dir"):
            self.curation_watcher.mark_hot(Path(scope["project_path"]), Path(scope["curation_dir"]))
        return res

    def curation_scope(
        self, project_path: Path, species_id: str, species_label: str, tomo_name: str, curation_dir: str = ""
    ) -> dict[str, Any]:
        return self.curation_service.curation_scope(
            project_path, species_id, species_label, tomo_name, curation_dir=curation_dir
        )

    async def assign_unattributed_coords(
        self, project_path: Path, source: Path, species_id: str, species_label: str, tomo_name: str
    ) -> dict[str, Any]:
        return await self.curation_service.assign_unattributed_coords(
            project_path, source, species_id, species_label, tomo_name
        )

    async def prepare_curation_bundle(
        self,
        project_path: Path,
        candidates_star: Path | None,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        *,
        species_id: str = "",
        source_star: Path | None = None,
        coords_label: str = "auto",
    ) -> dict[str, Any]:
        return await self.curation_service.prepare_curation_bundle(
            project_path,
            candidates_star,
            tomograms_star,
            tomo_name,
            species_label,
            species_id=species_id,
            source_star=source_star,
            coords_label=coords_label,
        )

    async def import_curation_picks(
        self,
        project_path: Path,
        tomograms_star: Path,
        tomo_name: str,
        species_label: str = "",
        species_id: str = "",
        *,
        coords_path: Path | None = None,
    ) -> dict[str, Any]:
        return await self.curation_service.import_curation_picks(
            project_path, tomograms_star, tomo_name, species_label, species_id, coords_path=coords_path
        )

    async def merge_pick_lists(
        self,
        project_path: Path,
        species_id: str,
        species_label: str,
        tomo_name: str,
        sources: list[dict[str, Any]],
        out_slug: str = "merged",
    ) -> dict[str, Any]:
        return await self.curation_service.merge_pick_lists(
            project_path, species_id, species_label, tomo_name, sources, out_slug
        )

    async def list_clash_stats(self, star_path: Path, tomo_name: str, radius_ang: float) -> dict[str, Any]:
        return await self.curation_service.list_clash_stats(star_path, tomo_name, radius_ang)

    async def deduplicate_pick_list(
        self, project_path: Path, species_id: str, tomo_name: str, slug: str, radius_ang: float
    ) -> dict[str, Any]:
        """Greedy radius-dedup one registered pick list in place: rewrite
        its star (``CurationSessionService.deduplicate_pick_list``), then update the
        ``PickList`` count, persist by explicit path and bump the registry rev — the list
        reads STALE afterwards (extracted_count ≠ count) until it is re-extracted. Returns
        the service's ``{n_total, n_removed, n_after, ...}``."""
        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        pl = state.get_pick_list(slug, species_id, tomo_name)
        if pl is None or not pl.path:
            return err(f"no registered pick list '{slug}' for {species_id}/{tomo_name}")
        res = await self.curation_service.deduplicate_pick_list(Path(pl.path), tomo_name, radius_ang)
        if not res.get("success"):
            return res
        pl.count = int(res.get("n_after", pl.count))
        state.mark_dirty()
        state.bump_registry_rev()
        # Await (force) so the dedup'd count lands on disk — a fire-and-forget
        # create_task can be GC'd before it runs.
        await self.state_service.save_project(project_path=project_path, force=True)
        return res

    # ── Particle sets (services/particles/particle_sets.py) ──────────────────────────

    async def preview_particle_set(self, parent: ParentRef, rules: list[Rule]) -> dict[str, Any]:
        """The funnel `rules` would give on `parent`; nothing is written."""
        try:
            steps = await asyncio.to_thread(set_ops.preview_rule_filter, parent, rules)
        except (ValueError, FileNotFoundError) as e:  # a rule the table cannot answer, or a parent gone from disk
            return err(str(e))
        except Exception as e:
            logger.exception("Rule-filter preview failed for %s", parent.particles_star)
            return err(f"{type(e).__name__}: {e}")
        return ok(funnel=steps)

    async def particle_overlap(
        self, a: ParentRef, cut_a: float | None, b: ParentRef, min_sigma_b: float | None, radius: float
    ) -> dict[str, Any]:
        """How many of `a`'s picks lie within `radius` Å of `b`'s (set_ops.overlap_counts)."""
        try:
            counts = await asyncio.to_thread(set_ops.overlap_counts, a, cut_a, b, min_sigma_b, radius)
        except (ValueError, FileNotFoundError) as e:
            return err(str(e))
        except Exception as e:
            logger.exception("Overlap of %s with %s failed", a.label, b.label)
            return err(f"{type(e).__name__}: {e}")
        return ok(**counts)

    async def create_particle_set(
        self, project_path: Path, *, parent: ParentRef, rules: list[Rule], name: str, species_id: str
    ) -> dict[str, Any]:
        """A new set of `parent`'s rows that pass `rules`, published under the project's
        Analytics/sets/ and logged; the parent is not touched."""
        try:
            pset = await asyncio.to_thread(
                set_ops.create_rule_filter_set,
                Path(project_path),
                parent=parent,
                rules=rules,
                name=name,
                species_id=species_id,
            )
        except (ValueError, FileExistsError, FileNotFoundError) as e:
            return err(str(e))
        except Exception as e:
            logger.exception("Creating particle set %r in %s failed", name, project_path)
            return err(f"{type(e).__name__}: {e}")
        self.state_service.state_for(Path(project_path)).bump_registry_rev()
        return ok(set=pset)

    async def particle_readout(self, project_path: Path, instance_id: str) -> dict[str, Any]:
        """The Class 3D or Reconstruct particle readout of one job (services/particles/class_readout.py);
        the first read of a job that predates its sidecar renders its images."""
        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        try:
            rec = await asyncio.to_thread(class_readout.readout_record, project_path, state, instance_id)
            decoys = {}
            if rec.provenance.kind == class_readout.CLASS3D_KIND:
                keys = await asyncio.to_thread(set_ops.decoy_keys, project_path)
                decoys = await asyncio.to_thread(set_ops.decoy_class_counts, Path(rec.data["data_star"]), keys)
        except FileNotFoundError as e:
            return err(str(e), code=ErrorCode.NOT_WRITTEN_YET)
        except Exception as e:
            logger.exception("Readout of %s failed", instance_id)
            return err(f"{type(e).__name__}: {e}")
        return ok(record=rec, decoys=decoys)

    async def create_class_subset(
        self, project_path: Path, *, class3d_instance: str, classes: list[int], name: str, species_id: str
    ) -> dict[str, Any]:
        """A new set of the chosen classes' members (set_ops.create_class_subset_set)."""
        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        try:
            pset = await asyncio.to_thread(
                set_ops.create_class_subset_set,
                project_path,
                state,
                class3d_instance=class3d_instance,
                classes=classes,
                name=name,
                species_id=species_id,
            )
        except (ValueError, FileNotFoundError, FileExistsError) as e:
            return err(str(e))
        except Exception as e:
            logger.exception("Class subset of %s failed", class3d_instance)
            return err(f"{type(e).__name__}: {e}")
        state.bump_registry_rev()
        return ok(set=pset)

    # ── Controls (services/templating/control_templates.py, services/particles/set_ops.py) ──

    async def make_control_template(
        self, project_path: Path, species_id: str, template_id: str, kind: str, *, seed: int = 1
    ) -> dict[str, Any]:
        """A decoy (phase-randomised) or mirrored copy of one of the species' templates, written beside
        its templates and registered on it (never selected). Matched by a template-match job of its
        own, it is a control for the real one."""
        from services import species_admin
        from services.templating import control_templates

        project_path = Path(project_path)
        state = self.state_service.state_for(project_path)
        sp = state.get_species(species_id)
        tpl = sp.get_template_by_id(template_id) if sp else None
        if tpl is None or not Path(tpl.template_path).exists():
            return err("that template is not on disk")
        src = Path(tpl.template_path)
        folder = project_path / "templates" / species_id
        if kind == "decoy":
            out = folder / f"{src.stem}_decoy_seed{seed}.mrc"
            source, note = f"decoy:phase_randomised:{tpl.id}:seed{seed}", "phase-randomised decoy of"
        elif kind == "mirror":
            out = folder / f"{src.stem}_mirror.mrc"
            source, note = f"mirror:{tpl.id}", "Z-flipped (other hand) copy of"
        else:
            return err(f"unknown control template kind {kind!r}")
        try:
            if kind == "decoy":
                await asyncio.to_thread(control_templates.phase_randomised, src, out, seed=seed)
            else:
                await asyncio.to_thread(control_templates.mirrored, src, out)
        except (OSError, ValueError) as e:
            logger.exception("Making the %s of %s failed", kind, src)
            return err(f"{type(e).__name__}: {e}")
        res = species_admin.register_template(
            state, species_id, str(out), polarity=tpl.polarity, source=source, notes=f"{note} {src.name}"
        )
        if not res["success"]:
            return res
        await self.state_service.save_project(project_path=project_path, force=True)
        return ok(template_id=res["template_id"], path=str(out))

    async def create_decoy_set(
        self,
        project_path: Path,
        *,
        parent: ParentRef,
        fraction: float,
        min_distance_ang: float,
        seed: int,
        name: str,
        species_id: str,
    ) -> dict[str, Any]:
        """Random in-slab decoy positions added to `parent`'s picks, as a new set."""
        try:
            pset = await asyncio.to_thread(
                set_ops.create_decoy_position_set,
                Path(project_path),
                parent=parent,
                fraction=fraction,
                min_distance_ang=min_distance_ang,
                seed=seed,
                name=name,
                species_id=species_id,
            )
        except (ValueError, FileNotFoundError, FileExistsError) as e:
            return err(str(e))
        except Exception as e:
            logger.exception("Decoy positions for %s failed", parent.label)
            return err(f"{type(e).__name__}: {e}")
        self.state_service.state_for(Path(project_path)).bump_registry_rev()
        return ok(set=pset)

    async def particle_control(self, fn, *args, **kwargs) -> dict[str, Any]:
        """Run one of set_ops' read-only controls (recall_curve, half_set_check, class_agreement,
        reference_table chains) off the event loop, as ok(result=…) / err."""
        try:
            result = await asyncio.to_thread(fn, *args, **kwargs)
        except (ValueError, FileNotFoundError) as e:
            return err(str(e))
        except Exception as e:
            logger.exception("Control %s failed", getattr(fn, "__name__", fn))
            return err(f"{type(e).__name__}: {e}")
        return ok(result=result)

    async def delete_particle_set(self, project_path: Path, set_id: str, *, reason: str = "") -> dict[str, Any]:
        """Remove a set (the operation log keeps its recipe)."""
        try:
            await asyncio.to_thread(particle_sets.delete_set, Path(project_path), set_id, reason=reason)
        except FileNotFoundError as e:
            return err(str(e))
        except OSError as e:
            logger.exception("Deleting particle set %s in %s failed", set_id, project_path)
            return err(f"{type(e).__name__}: {e}")
        self.state_service.state_for(Path(project_path)).bump_registry_rev()
        return ok()

    async def get_default_data_globs(self) -> dict[str, str]:
        """Get default glob patterns from config."""
        config_service = get_config_service()
        movies, mdocs = config_service.default_data_globs
        # Return empty strings if not set, to avoid UI errors
        return {"movies": movies if movies else "", "mdocs": mdocs if mdocs else ""}

    async def get_default_project_base(self) -> str:
        """Retrieves the default project base path from config."""
        config_service = get_config_service()
        configured_path = config_service.default_project_base
        if configured_path:
            return configured_path

        return str(Path.home())

    async def scan_for_projects(
        self, base_path: str, on_partial: Callable[[list[dict[str, Any]]], None] | None = None
    ) -> list[dict[str, Any]]:
        """Every project under `base_path`, in scan order. `on_partial`, when given, is called on the
        event loop (inside this coroutine, so in the caller's UI context) with the projects found so
        far, in the same order, while the scan runs."""
        if on_partial is None:
            projects = await asyncio.to_thread(self._scan_for_projects_sync, base_path)
        else:
            loop = asyncio.get_running_loop()
            partials: asyncio.Queue[list[dict[str, Any]] | None] = asyncio.Queue()
            scan = asyncio.ensure_future(
                asyncio.to_thread(
                    self._scan_for_projects_sync,
                    base_path,
                    lambda found: loop.call_soon_threadsafe(partials.put_nowait, found),
                )
            )
            # Runs after every partial the thread queued: those were scheduled before the result was.
            scan.add_done_callback(lambda _t: partials.put_nowait(None))
            while (found := await partials.get()) is not None:
                on_partial(found)
            projects = await scan
        self._project_scans[base_path] = projects
        # Sizes are measured in the background; a later scan picks the results up.
        get_disk_usage_service().request([p["path"] for p in projects if p["disk_usage_stale"]])
        return projects

    def last_project_scan(self, base_path: str) -> list[dict[str, Any]] | None:
        """What the last scan_for_projects of this base found; None before the first. Read-only."""
        return self._project_scans.get(base_path)

    def _scan_for_projects_sync(
        self, base_path: str, on_partial: Callable[[list[dict[str, Any]]], None] | None = None
    ) -> list[dict[str, Any]]:
        path = Path(base_path)

        logger.debug("Scanning directory: %s", path)

        if not path.exists():
            logger.info("Path does not exist: %s", path)
            return []
        if not path.is_dir():
            logger.info("Path is not a directory: %s", path)
            return []

        try:

            def scan_one(item: Path) -> dict[str, Any] | None:
                if not item.is_dir():
                    return None
                if is_being_deleted(item.name):
                    # A project mid-deletion (services/project_delete.py) is gone as far as the list goes.
                    return None
                params_file = item / "project_params.json"
                if not params_file.exists():
                    if not item.name.startswith("."):
                        logger.debug("Skipped '%s' - missing project_params.json", item.name)
                    return None
                try:
                    stats = params_file.stat()
                    mod_time = datetime.fromtimestamp(stats.st_mtime)

                    created_at = None
                    created_ts = None
                    creator = None
                    owner_raw = None
                    pipeline_active = False
                    total_jobs_planned = 0
                    # Initialized outside the try below: an unreadable project_params.json
                    # still reaches the fallback path, and _derive_live_status is called
                    # unconditionally at the bottom of this block.
                    jobs_dict: dict[str, Any] = {}
                    ts_count = 0
                    mnemonic = ""
                    # As the project recorded them; None when it recorded none (never the model defaults).
                    apix: float | None = None
                    voltage_kv: float | None = None
                    dose_per_tilt: float | None = None
                    dose_source = ""
                    source_directory = ""
                    species: list[dict[str, str]] = []
                    review_hold: dict[str, Any] | None = None
                    n_picks = 0
                    n_pick_lists = 0
                    protocol = ""
                    # What only the roster's hover card shows: the globs, and the acquisition and
                    # import facts the row has no column for. As recorded.
                    card: dict[str, Any] = {}
                    try:
                        with open(params_file) as f:
                            data = json.load(f)
                        raw_created = data.get("created_at")
                        if raw_created:
                            created_at = str(raw_created)[:16]
                            try:
                                created_ts = datetime.fromisoformat(str(raw_created)).timestamp()
                            except ValueError:
                                # Unparsable created_at string -- ctime fallback below kicks in.
                                created_ts = None
                        creator = data.get("created_by")
                        owner_raw = data.get("owner")
                        pipeline_active = bool(data.get("pipeline_active", False))
                        review_hold = data.get("review_hold")
                        jobs_dict = {
                            iid: job for iid, job in (data.get("jobs") or {}).items() if _is_pipeline_job_dict(job)
                        }
                        total_jobs_planned = len(jobs_dict)
                        ts_count = data.get("import_selected_tilt_series") or data.get("import_total_tilt_series") or 0
                        mnemonic = data.get("mnemonic") or ""
                        microscope = data.get("microscope") or {}
                        acquisition = data.get("acquisition") or {}
                        apix = microscope.get("pixel_size_angstrom")
                        voltage_kv = microscope.get("acceleration_voltage_kv")
                        dose_per_tilt = acquisition.get("dose_per_tilt")
                        dose_source = acquisition.get("dose_per_tilt_source") or ""
                        # Registered particle species, so the roster can answer "which projects
                        # have a ribosome in them". id/name/color only.
                        species = [
                            {"id": s["id"], "name": s.get("name") or s["id"], "color": s.get("color") or "#3b82f6"}
                            for s in (data.get("species_registry") or [])
                            if isinstance(s, dict) and s.get("id")
                        ]
                        # Each pick list caches its pick count for display; the roster sums them.
                        pick_lists = [pl for pl in (data.get("pick_lists") or []) if isinstance(pl, dict)]
                        n_pick_lists = len(pick_lists)
                        n_picks = sum(int(pl.get("count") or 0) for pl in pick_lists)
                        protocol = (data.get("protocol_origin") or {}).get("name") or ""
                        # Where the raw data came from. Prefer the resolved
                        # frames dir; fall back to the movies glob's parent.
                        source_directory = data.get("import_source_directory") or ""
                        if not source_directory:
                            mg = data.get("movies_glob") or ""
                            if mg:
                                source_directory = str(Path(mg).parent) if "*" in mg else mg
                        card = {
                            "movies_glob": data.get("movies_glob") or "",
                            "mdocs_glob": data.get("mdocs_glob") or "",
                            "cs_mm": microscope.get("spherical_aberration_mm"),
                            "amp_contrast": microscope.get("amplitude_contrast"),
                            "tilt_axis_deg": acquisition.get("tilt_axis_degrees"),
                            "frame_extension": data.get("import_frame_extension") or "",
                            "source_kind": data.get("import_source_kind") or "",
                            "ts_selected": data.get("import_selected_tilt_series") or 0,
                            "ts_total": data.get("import_total_tilt_series") or 0,
                        }
                    except Exception as e:
                        logger.warning("Unreadable project_params.json for %s -- using fallbacks: %s", item.name, e)

                    if not ts_count:
                        ts_count = _registry_ts_count(item)

                    # Legacy projects (no `mnemonic` persisted) get a
                    # deterministic fallback so the UI always has something
                    # to show. Seed is the resolved project dir — stable
                    # across reloads, independent of project_name renames.
                    if not mnemonic:
                        try:
                            from services.project_nickname import nickname_for

                            mnemonic = nickname_for(str(item.resolve()))
                        except Exception as e:
                            logger.warning("Nickname fallback failed for %s: %s", item.name, e)
                            mnemonic = ""

                    if creator is None:
                        try:
                            creator = pwd.getpwuid(stats.st_uid).pw_name
                        except KeyError:
                            # uid absent from the passwd db (LDAP/SSSD miss, foreign uid) -- creator stays unknown.
                            creator = None

                    if created_ts is None:
                        # Stable fallback: directory ctime (creation on most filesystems).
                        try:
                            created_ts = item.stat().st_ctime
                        except OSError:
                            # Directory vanished/unstat-able mid-scan -- fall back to the scan-time mtime.
                            created_ts = stats.st_mtime

                    derived = self._derive_live_status(item, jobs_dict, review_hold)
                    derived["pipeline_active_flag"] = pipeline_active
                    live_progress = None
                    if derived["live_status"] in ("running", "queued"):
                        try:
                            live_progress = _live_progress(item, jobs_dict)
                        except OSError:  # a job dir changing under the scan: the row shows, just without it
                            live_progress = None

                    last_activity_ts = max(stats.st_mtime, derived.get("last_activity_ts", 0.0))
                    # A directory listing, so only redone when the project shows new activity.
                    cached_preview = self._tomo_preview_cache.get(str(item))
                    if cached_preview is not None and cached_preview[0] == last_activity_ts:
                        tomo_preview = cached_preview[1]
                    else:
                        tomo_preview = _find_tomo_preview(item, jobs_dict)
                        self._tomo_preview_cache[str(item)] = (last_activity_ts, tomo_preview)
                    disk_usage = get_disk_usage_service().cached(item)
                    trash_count, trash_mtime = trash_summary(item)
                    disk_usage_stale = is_disk_usage_stale(
                        disk_usage,
                        # Clearing the trash changes nothing else in the project.
                        last_activity_ts=max(last_activity_ts, trash_mtime),
                        running=derived["live_status"] in ("running", "queued"),
                        now=time.time(),
                    )

                    try:
                        resolved_path = str(item.resolve())
                    except OSError:
                        # Unreachable mount mid-scan: the unresolved path still matches what was opened from this base.
                        resolved_path = str(item)

                    return {
                        "name": item.name,
                        "path": str(item),
                        # Compared per row on every roster render; resolving there would stat the mount on the loop.
                        "resolved_path": resolved_path,
                        "mnemonic": mnemonic,
                        "apix": apix,
                        "voltage_kv": voltage_kv,
                        "dose_per_tilt": dose_per_tilt,
                        "dose_source": dose_source,
                        "modified": mod_time.strftime("%Y-%m-%d %H:%M"),
                        "modified_timestamp": stats.st_mtime,
                        "created_at": created_at,
                        "created_timestamp": created_ts,
                        "creator": creator,
                        "owner": owner_raw,
                        "pipeline_active": pipeline_active,
                        "total_jobs_planned": total_jobs_planned,
                        "ts_count": ts_count,
                        "species": species,
                        "source_directory": source_directory,
                        "last_activity_ts": last_activity_ts,
                        "last_activity": datetime.fromtimestamp(last_activity_ts).strftime("%Y-%m-%d %H:%M"),
                        "disk_usage": disk_usage,
                        "disk_usage_stale": disk_usage_stale,
                        "trash_count": trash_count,
                        "trash_mtime": trash_mtime,
                        "tomo_preview": tomo_preview,
                        "live_progress": live_progress,
                        "furthest_done": _furthest_done(jobs_dict),
                        "n_picks": n_picks,
                        "n_pick_lists": n_pick_lists,
                        "protocol": protocol,
                        "card": card,
                        **derived,
                    }
                except Exception as e:
                    logger.info("Error reading %s: %s", item.name, e)
                    return None

            # Each project costs a dozen metadata round-trips; on a cold NFS cache, one project
            # after the other adds up to seconds for a full base.
            projects: list[dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=16) as pool:
                futures = [pool.submit(scan_one, item) for item in path.iterdir()]
                last_partial = time.monotonic()
                for future in as_completed(futures):
                    found = future.result()
                    if found is None:
                        continue
                    projects.append(found)
                    if on_partial is not None and time.monotonic() - last_partial >= _PARTIAL_SCAN_EVERY_S:
                        # A new list each time: the loop reads it while this thread goes on appending.
                        on_partial(sorted(projects, key=_scan_order))
                        last_partial = time.monotonic()

        except Exception as e:
            logger.error("Error scanning projects: %s", e)
            return []

        projects.sort(key=_scan_order)
        logger.debug("Found %d valid projects.", len(projects))
        return projects

    @staticmethod
    def _derive_live_status(
        project_dir: Path, jobs_dict: dict[str, Any], review_hold: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Derive pipeline status for the roster from project_params.json's
        `jobs` dict (canonical: includes tiltFilter and other non-RELION
        jobs that aren't in default_pipeline.star). Disk RELION_JOB_EXIT_*
        markers are only used to reconcile persisted `Running` statuses of
        jobs that ended while no server was reconciling -- otherwise the project
        state is the source of truth. A stored `review_hold` with nothing
        running reads "review": the pipeline waits for the tilt-filter review."""
        out: dict[str, Any] = {
            "executed_jobs": 0,
            "succeeded": 0,
            "running_live": 0,
            "queued": 0,
            "failed": 0,
            "scheduled": 0,
            "live_status": "idle",
            "last_activity_ts": 0.0,
            "review_parked": 0,
            "review_since": "",
        }
        pipeline_star = project_dir / "default_pipeline.star"
        if pipeline_star.exists():
            try:
                out["last_activity_ts"] = pipeline_star.stat().st_mtime
            except OSError:
                # Star deleted between exists() and stat() -- last_activity_ts stays 0.
                pass

        if not isinstance(jobs_dict, dict) or not jobs_dict:
            return out

        for _iid, job in jobs_dict.items():
            if not isinstance(job, dict) or not _is_pipeline_job_dict(job):
                continue
            status = (job.get("execution_status") or "").strip()
            relion_name = job.get("relion_job_name")
            # Reconcile Running against on-disk exit markers so a job that
            # ended while no server was watching doesn't read as "live" for good.
            if status == "Running" and relion_name:
                job_dir = project_dir / relion_name.rstrip("/")
                success_marker = job_dir / "RELION_JOB_EXIT_SUCCESS"
                failure_marker = job_dir / "RELION_JOB_EXIT_FAILURE"
                if success_marker.exists():
                    status = "Succeeded"
                    try:
                        out["last_activity_ts"] = max(out["last_activity_ts"], success_marker.stat().st_mtime)
                    except OSError:
                        # Marker deleted between exists() and stat() -- keep the current timestamp.
                        pass
                elif failure_marker.exists():
                    status = "Failed"
                    try:
                        out["last_activity_ts"] = max(out["last_activity_ts"], failure_marker.stat().st_mtime)
                    except OSError:
                        # Marker deleted between exists() and stat() -- keep the current timestamp.
                        pass

            if status == "Succeeded":
                out["succeeded"] += 1
                out["executed_jobs"] += 1
            elif status == "Running":
                out["running_live"] += 1
                out["executed_jobs"] += 1
            elif status == "Queued":
                out["queued"] += 1
            elif status == "Failed":
                out["failed"] += 1
                out["executed_jobs"] += 1
            elif status == "Scheduled":
                out["scheduled"] += 1
            # Anything else (Idle / Pending / unknown) is ignored.

        if isinstance(review_hold, dict):
            out["review_parked"] = len(review_hold.get("parked") or [])
            out["review_since"] = str(review_hold.get("held_at") or "")[11:16]

        # One job at work makes the whole project running; a run whose jobs all still wait in the SLURM
        # queue is queued -- live, not finished, but nothing computes yet.
        if out["running_live"] > 0:
            out["live_status"] = "running"
        elif out["queued"] > 0:
            out["live_status"] = "queued"
        elif out["review_parked"] > 0:
            out["live_status"] = "review"
        elif out["failed"] > 0:
            out["live_status"] = "failed"
        elif out["succeeded"] > 0 and out["scheduled"] == 0 and out["failed"] == 0 and out["running_live"] == 0:
            out["live_status"] = "done"
        else:
            out["live_status"] = "idle"

        return out

    async def create_project_and_scheme(
        self,
        project_name: str,
        project_base_path: str,
        selected_jobs: list[str],
        movies_glob: str,
        mdocs_glob: str,
        selected_mdoc_paths: list[str] | None = None,
        import_summary: dict[str, Any] | None = None,
        detected_params: dict[str, Any] | None = None,
        shared: bool = False,
        progress_cb=None,
    ):
        return await self.project_service.initialize_new_project(
            project_name=project_name,
            project_base_path=project_base_path,
            selected_jobs=selected_jobs,
            movies_glob=movies_glob,
            mdocs_glob=mdocs_glob,
            selected_mdoc_paths=selected_mdoc_paths,
            import_summary=import_summary,
            detected_params=detected_params,
            shared=shared,
            progress_cb=progress_cb,
        )

    async def delete_project(self, project_path: Path, progress_cb: ProgressCallback | None = None) -> dict[str, Any]:
        """Remove a project directory from disk and drop its in-memory state. The project leaves every
        scan the moment this starts (services/project_delete.py); `progress_cb` follows it job directory
        by job directory, and cancelling stops it after the one it is on."""
        stop = threading.Event()
        try:
            result = await asyncio.to_thread(delete_project_dir, Path(project_path), progress_cb, stop)
        except asyncio.CancelledError:
            # The thread cannot be interrupted: it stops before the next unit and puts the rest back.
            stop.set()
            raise
        if result["success"]:
            remove_project_state(project_path)
        return result

    async def list_project_trash(self, project_path: Path) -> dict[str, Any]:
        """The jobs removed from the project that still sit in its Trash/, sized afresh, largest first."""
        try:
            entries = await asyncio.to_thread(list_trash, Path(project_path))
        except OSError as e:
            logger.exception("Could not read the trash of %s", project_path)
            return err(str(e))
        return ok(entries=entries)

    async def clear_project_trash(
        self, project_path: Path, names: list[str] | None = None, progress_cb: ProgressCallback | None = None
    ) -> dict[str, Any]:
        """Permanently delete the named Trash/ entries, or all of them when `names` is None; cancelling
        stops it after the entry it is on. When that empties Trash/, the cached project size drops it
        at once."""
        project_dir = Path(project_path)
        stop = threading.Event()
        try:
            result = await asyncio.to_thread(clear_trash, project_dir, names, progress_cb, stop)
            remaining, _ = await asyncio.to_thread(trash_summary, project_dir)
        except asyncio.CancelledError:
            # The thread cannot be interrupted: it stops before the next entry.
            stop.set()
            raise
        except OSError as e:
            logger.exception("Could not clear the trash of %s", project_dir)
            return err(str(e))
        if remaining == 0:
            await asyncio.to_thread(get_disk_usage_service().trash_cleared, project_dir)
        return result

    async def transfer_project_ownership(self, project_path: Path, new_owner: str | None) -> dict[str, Any]:
        """Reassign a project's owner WITHOUT moving it on disk.

        `owner` is pure attribution/grouping metadata; the on-disk location and
        the path-keyed state registry are untouched. We resolve via state_for()
        so the live in-memory state is mutated when the project is open in this
        process (a blind JSON rewrite would be clobbered on its next save).

        new_owner: a username, SHARED_OWNER ("@lab") for the shared/lab area, or
        None to revert to the original creator. There is no access control
        anywhere (identity is the OS user of each per-user process on a shared
        filesystem), so this is honor-system attribution; cross-process
        concurrency is the same unhandled race as any other concurrent edit.
        """
        try:
            path = Path(project_path)
            state = self.state_service.state_for(path)
            state.owner = new_owner or None
            state.update_modified()
            state.mark_dirty()
            await self.state_service.save_project(project_path=path, force=True)
            return ok(owner=state.owner)
        except Exception as e:
            logger.error("Failed to transfer ownership of %s: %s", project_path, e)
            return err(str(e))

    async def autodetect_parameters(self, mdocs_glob: str) -> dict[str, Any]:
        from services.configs.mdoc_service import get_mdoc_service

        mdoc_data = await asyncio.to_thread(get_mdoc_service().get_autodetect_params, mdocs_glob)
        return {
            "microscope": {
                "pixel_size_angstrom": mdoc_data.get("pixel_spacing"),
                "acceleration_voltage_kv": mdoc_data.get("voltage"),
                "spherical_aberration_mm": None,
            },
            "acquisition": {
                "dose_per_tilt": mdoc_data.get("dose_per_tilt"),
                "tilt_axis_degrees": mdoc_data.get("tilt_axis_angle"),
            },
        }

    async def parse_dataset_overview(self, mdocs_glob: str, frames_dir: str | None = None, progress_cb=None):
        from services.configs.dataset_parsing_service import get_dataset_parsing_service

        service = get_dataset_parsing_service()
        overview = await asyncio.to_thread(service.parse_dataset, mdocs_glob, frames_dir, progress_cb)
        return overview

    async def run_shell_command(
        self,
        command: str,
        cwd: Path | None = None,
        tool_name: str | None = None,
        additional_binds: list[str] | None = None,
    ):
        """Runs a shell command, optionally using specified tool's container."""
        try:
            if tool_name:
                logger.debug("Running command with tool: %s", tool_name)
                final_command = self.container_service.wrap_command_for_tool(
                    command=command,
                    cwd=cwd or self.server_dir,
                    tool_name=tool_name,
                    additional_binds=additional_binds or [],
                )
            else:
                final_command = command
                logger.info("Running natively: %s", final_command)

            process = await asyncio.create_subprocess_shell(
                final_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd or self.server_dir,
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120.0)

                logger.debug("Process completed with return code: %s", process.returncode)
                if process.returncode == 0:
                    return ok(output=stdout.decode())
                else:
                    return err(stderr.decode(), output=stdout.decode())

            except TimeoutError:
                logger.error("Command timed out after 120 seconds: %s", final_command)
                process.terminate()
                await process.wait()
                return err("Command execution timed out", output="")

        except Exception as e:
            logger.error("Exception in run_shell_command: %s", e)
            return err(str(e), output="")

    async def get_pipeline_overview(self, project_path: str):
        """Gets a high-level overview and detailed statuses of all jobs."""
        return await self.pipeline_runner.get_pipeline_overview(project_path)

    async def get_job_logs(self, project_path: str, job_name: str, *, tail_bytes: int | None = None) -> dict[str, str]:
        """Gets logs for a specific job *path* (e.g., "External/job003/").

        Tail-only by default (see PipelineRunnerService.get_job_logs); pass tail_bytes=0 for
        the whole file."""
        if tail_bytes is None:
            return await self.pipeline_runner.get_job_logs(project_path, job_name)
        return await self.pipeline_runner.get_job_logs(project_path, job_name, tail_bytes=tail_bytes)

    async def get_eer_frames_per_tilt(self, eer_file_path: str) -> int:
        try:
            command = f"header {eer_file_path}"
            result = await self.run_shell_command(command)

            if result["success"]:
                output = result["output"]
                for line in output.split("\n"):
                    if "Number of columns, rows, sections" in line:
                        parts = line.split(".")[-1].strip().split()
                        if len(parts) >= 3:
                            return int(parts[2])
            else:
                logger.warning("header command failed for %s: %s", eer_file_path, result.get("error"))
            return None
        except Exception as e:
            logger.warning("Error getting EER frames for %s: %s", eer_file_path, e)
            return None

    async def load_existing_project(self, project_path: str) -> dict[str, Any]:
        return await self.project_service.load_project_state(project_path)

    async def debug_pipeline_status(self, project_path: str):
        """Debug method to check pipeline status directly"""
        pipeline_star = Path(project_path) / "default_pipeline.star"

        if not pipeline_star.exists():
            return err("Pipeline file not found")

        try:
            with open(pipeline_star) as f:
                content = f.read()

            return ok(content=content, exists=True)
        except Exception as e:
            return err(str(e))
