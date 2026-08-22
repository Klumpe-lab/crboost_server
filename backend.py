from __future__ import annotations
import asyncio
import getpass
import json
import logging
import pwd
import shutil
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
from services.jobs.spec import driver_invocation
from services.models_base import JobStatus, JobType
from services.particles.list_ref import extract_pick_list_instance_id
from services.project_state import get_state_service
from services.result import err, ok
from services.computing.slurm_service import SlurmService
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
    """Does this RAW ``project_params.json`` job entry belong to the pipeline's own run?

    Per-list subtomogram extractions (roadmap 07 §4) do not: they are one-off interactive jobs
    whose home is the Species page's Picks tab, which shows their status itself. Counting them
    in the project-hub scan would let ONE failed list extraction leave the whole project
    reading "failed" indefinitely (nothing clears that status until the next submit), let a
    running one make an idle project read "running", and inflate its planned-job total. That
    scan reads json, so it cannot see ``IS_INTERACTIVE``; the job type is the filter.
    ``tiltFilter`` deliberately stays counted — interactive to launch, but pipeline work."""
    return not (isinstance(job, dict) and job.get("job_type") == JobType.EXTRACT_PICK_LIST.value)


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
        # CurationWatcher registers ArtiaX .coords saves as `manual` pick lists server-side
        # (roadmap 09-S4); same lifecycle as the monitor — started/stopped in main.py.
        self.curation_watcher = CurationWatcher(self)
        # Pending debounced saves, keyed by project path — see save_project().
        self._pending_saves: dict[str, asyncio.Task] = {}

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

    async def submit_tilt_filter_dl(self, project_path: Path, instance_id: str) -> dict[str, Any]:
        """Submit the tilt filter DL driver as a standalone SLURM job."""
        from services.path_resolution_service import PathResolutionService
        from services.models_base import JobStatus

        state = self.state_service.state_for(project_path)
        job_model = state.jobs.get(instance_id)
        if not job_model:
            return err(f"Job '{instance_id}' not found")

        # Resolve input paths
        resolver = PathResolutionService(state)
        try:
            io_paths = resolver.resolve_all_paths(
                job_model.job_type, job_model, project_path / "TiltFilter", instance_id=instance_id
            )
            job_model.paths.update({k: str(v) for k, v in io_paths.items() if v is not None})
        except Exception as e:
            return err(f"Path resolution failed: {e}")

        # Create job directory and clean up stale markers from previous runs
        job_dir = project_path / "TiltFilter" / "dl_run"
        job_dir.mkdir(parents=True, exist_ok=True)
        for marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE"):
            (job_dir / marker).unlink(missing_ok=True)

        # Save state so the driver can read it
        job_model.execution_status = JobStatus.RUNNING
        state.mark_dirty()
        await self.state_service.save_project(project_path=project_path, force=True)

        # Build driver command
        python_exe = self.server_dir / "venv" / "bin" / "python3"
        if not python_exe.exists():
            python_exe = "python3"
        script_path = self.server_dir / "drivers" / "tilt_filter.py"
        driver_cmd = (
            f"export PYTHONPATH={self.server_dir}:${{PYTHONPATH}}; "
            f"{python_exe} {script_path} "
            f"--instance_id {instance_id} "
            f"--project_path {project_path}"
        )

        # Build sbatch script from template
        qsub_template = self.server_dir / "config" / "qsub.sh"
        template_text = qsub_template.read_text()

        slurm_cfg = job_model.get_effective_slurm_config()
        # Strip surrounding quotes that may be stored in config values
        constraint = slurm_cfg.constraint.strip("'\"")
        replacements = {
            "XXXextra1XXX": slurm_cfg.partition,
            "XXXextra2XXX": constraint,
            "XXXextra3XXX": str(slurm_cfg.nodes),
            "XXXextra4XXX": str(slurm_cfg.ntasks_per_node),
            "XXXextra5XXX": str(slurm_cfg.cpus_per_task),
            "XXXextra6XXX": slurm_cfg.gres,
            "XXXextra7XXX": slurm_cfg.mem,
            "XXXextra8XXX": slurm_cfg.time,
            "XXXoutfileXXX": str(job_dir / "run.out"),
            "XXXerrfileXXX": str(job_dir / "run.err"),
            "XXXcommandXXX": driver_cmd,
        }
        script = template_text
        for placeholder, value in replacements.items():
            script = script.replace(placeholder, value)

        sbatch_path = job_dir / "run_tilt_filter.sh"
        sbatch_path.write_text(script)
        sbatch_path.chmod(0o755)

        # Submit via sbatch
        try:
            proc = await asyncio.create_subprocess_exec(
                "sbatch",
                str(sbatch_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(job_dir),
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                job_model.execution_status = JobStatus.FAILED
                state.mark_dirty()
                await self.state_service.save_project(project_path=project_path, force=True)
                return err(f"sbatch failed: {stderr.decode().strip()}")

            # Parse job ID from "Submitted batch job 12345"
            output = stdout.decode().strip()
            slurm_job_id = output.split()[-1] if output else None
            job_model.slurm_job_id = slurm_job_id
            state.mark_dirty()
            await self.state_service.save_project(project_path=project_path, force=True)

            logger.info("Tilt filter DL submitted: SLURM job %s", slurm_job_id)
            return ok(slurm_job_id=slurm_job_id, job_dir=str(job_dir))

        except Exception as e:
            job_model.execution_status = JobStatus.FAILED
            state.mark_dirty()
            await self.state_service.save_project(project_path=project_path, force=True)
            return err(str(e))

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
        """Subtomo-extract ONE curation pick list (Slice C): submit
        ``drivers/extract_pick_list.py`` as a one-off SLURM job via ``config/qsub.sh``
        (same mechanism as ``submit_tilt_filter_dl``). Output lands in
        ``<list_star dir>/<slug>/`` so lists extract independently. Returns the SLURM
        job id + the dir to watch (``RELION_JOB_EXIT_SUCCESS/FAILURE`` + ``result.json``
        appear there; the caller records ``PickList.mark_extracted`` on success).

        Exactly one schema source: ``candidate_optset`` mirrors the species'
        candidate-extract schema; ``tomograms_star`` synthesizes it for a de-novo
        species with no candidate-extract job.

        ``box_size``/``binning``/``crop_size`` are REQUIRED — they used to default to
        384/1.0/224, which silently cut wrong-but-plausible subtomograms for any species
        without a subtomo job. The caller resolves them from the species (see
        ``aggregation.authoritative.extraction_params_for_species``) or asks the user."""
        project_path = Path(project_path)
        if (candidate_optset is None) == (tomograms_star is None):
            return err("extract_pick_list needs exactly one of candidate_optset / tomograms_star")
        out_dir = Path(list_star).parent / slug
        out_dir.mkdir(parents=True, exist_ok=True)

        # ── job identity (roadmap 07-S1) ────────────────────────────────────────
        # Create/update the per-list instance BEFORE sbatch: from S2 the driver
        # bootstraps off it and `get_driver_context` exits when the instance is
        # missing. Two orderings here are load-bearing:
        #   · `execution_status` is reset FIRST because writes to USER_PARAMS fields
        #     are silently dropped once a job leaves SCHEDULED/FAILED — the reverse
        #     order would re-cut a re-curated list with the PREVIOUS geometry and
        #     report nothing.
        #   · the whole instance update happens BEFORE the output below is destroyed.
        #     Both halves of the Picks tab's story live on this one shared in-memory
        #     state, so a render landing in between would pair the freshly-absent
        #     output (`extraction_state()` → NOT_EXTRACTED) with the PREVIOUS run's
        #     Succeeded/Failed and its stale error text. Reset first and the only
        #     visible transient is "not extracted · submitting", which is true.
        # `paths["job_dir"]` is the out dir qsub already writes run.out/run.err and
        # the exit markers into. `reconcile_afterok` resolves a job by that same dir,
        # but only while the project has a real pipeline job live: this job never sets
        # `pipeline_active` itself, so once the chain ends the monitor drops the project
        # and stops looking. `reconcile_pick_list_extractions` is what covers the rest.
        state = self.state_service.state_for(project_path)
        instance_id = extract_pick_list_instance_id(species_id, tomo_name, slug)
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

        # The driver reads every one of those values off the instance now (roadmap 07-S2),
        # so the launch is the standard instance form: --instance_id/--project_path, and
        # qsub cd's into out_dir, which is what the driver takes as its job dir.
        driver_cmd = driver_invocation(
            server_dir=self.server_dir,
            driver_script=self.server_dir / "drivers" / "extract_pick_list.py",
            instance_id=instance_id,
            project_path=project_path,
        )

        # SLURM resources = the project's defaults (proven to run relion_tomo_subtomo;
        # over-provisioned for one tomo but consistent with the pipeline extraction).
        slurm_cfg = self.state_service.state_for(project_path).slurm_defaults
        constraint = slurm_cfg.constraint.strip("'\"")
        template_text = (self.server_dir / "config" / "qsub.sh").read_text()
        replacements = {
            "XXXextra1XXX": slurm_cfg.partition,
            "XXXextra2XXX": constraint,
            "XXXextra3XXX": str(slurm_cfg.nodes),
            "XXXextra4XXX": str(slurm_cfg.ntasks_per_node),
            "XXXextra5XXX": str(slurm_cfg.cpus_per_task),
            "XXXextra6XXX": slurm_cfg.gres,
            "XXXextra7XXX": slurm_cfg.mem,
            "XXXextra8XXX": slurm_cfg.time,
            "XXXoutfileXXX": str(out_dir / "run.out"),
            "XXXerrfileXXX": str(out_dir / "run.err"),
            "XXXcommandXXX": driver_cmd,
        }
        script = template_text
        for placeholder, value in replacements.items():
            script = script.replace(placeholder, value)
        sbatch_path = out_dir / "run_extract.sh"
        sbatch_path.write_text(script)
        sbatch_path.chmod(0o755)

        try:
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
            logger.info("Per-list extraction submitted: SLURM job %s (%s/%s)", slurm_job_id, species_id, slug)
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
        counterpart of ``extract_authoritative_pending``. RUN INSIDE A BackgroundTask
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

    async def get_authoritative_extraction_status(self, project_path: Path, species_id: str) -> list[dict[str, Any]]:
        """Read-only: per-(species, tomo) authoritative-list extraction status — the
        aggregation gate's input (see docs/LIST_EXTRACTION_AND_AGGREGATION.md
        §8.1). Returns one dict per tomo: {species_id, tomo_name, slug, kind, extraction_state,
        optset_path, kept, total, notes}. Disk reads run off the event loop."""
        from dataclasses import asdict
        from services.aggregation.authoritative import enumerate_authoritative

        state = self.state_service.state_for(Path(project_path))
        handles = await asyncio.to_thread(enumerate_authoritative, state, Path(project_path), species_id)
        return [{**asdict(h), "extraction_state": h.extraction_state.value} for h in handles]

    async def get_authoritative_gate_report(self, project_path: Path, species_id: str) -> dict[str, Any]:
        """Read-only §8.3 gate: classify each (species, tomo) authoritative list as
        ready / pending (workbench NOT_EXTRACTED|STALE — extractable here) / blocked, so the
        UI/user sees exactly what must be extracted before a species roll-up. No side effects.
        See docs/LIST_EXTRACTION_AND_AGGREGATION.md §8.3."""
        from services.aggregation.authoritative import compute_gate_report, enumerate_authoritative

        state = self.state_service.state_for(Path(project_path))
        handles = await asyncio.to_thread(enumerate_authoritative, state, Path(project_path), species_id)
        return compute_gate_report(handles).to_dict()

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
        """Add one import BATCH and rebuild ``Tomograms/tomograms.star`` from every batch the
        project holds (de-novo S5). ``replace=True`` throws the prior batches away instead —
        the pre-S5 single-slot behaviour, kept as an explicit user choice rather than the
        silent default it used to be.

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

    async def _await_extraction_outdirs(
        self, targets: dict[str, tuple[str, str | None]], timeout_s: int, *, project_path: Path
    ) -> dict[str, tuple]:
        """Poll each per-list extraction out dir (RELION_JOB_EXIT_* + result.json) until all
        resolve or ``timeout_s`` elapses, moving each list's job instance as it goes.
        ``targets`` maps out dir → (the ``extractPickList__…`` instance id that cuts it, the
        SLURM id THIS awaiter submitted — the instance is shared, so that id is how a status
        write proves it belongs to the run being watched).
        Returns {out_dir: ("done"|"failed", data)} for the resolved ones (an out dir absent
        from the result = still running). Disk scans run off the event loop. The ONE
        extraction watcher — both the batch path (extract_authoritative_pending) and the
        per-list path (extract_pick_list_and_wait) use it.

        It is also the ONLY refresher those instances get (roadmap 07-S3): a one-off
        extraction deliberately does not set ``pipeline_active`` (the roster would read as a
        live run), and ``PipelineMonitor._tick_once`` iterates only active projects — so
        QUEUED → RUNNING → SUCCEEDED/FAILED happens here, and the driver's own error text
        lands on the instance so a failure outlives the dialog that launched it.
        It is NOT the last word, though it used to be: if this awaiter dies (server restart,
        tray cancel, or ``timeout_s`` elapsing) nothing here will ever move the instance again,
        and ``PickList.mark_extracted`` — which only these two callers run — would never land
        either, leaving a finished job's optimisation set orphaned on disk with both surfaces
        reading "not extracted" (``PickList.extraction_state()`` keys off the RECORDED
        ``extracted_path``, so it cannot discover an output nobody recorded).
        ``reconcile_pick_list_extractions`` is the recovery: it asks SLURM about any instance
        left non-terminal and settles it off this same out dir. Re-extracting stays a recovery
        path too, which is why nothing on it may be hard-blocked."""
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
        instances (roadmap 07-S3): the ones SLURM has just started → RUNNING, the resolved
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

    async def cancel_pick_list_extraction(
        self, project_path: Path, species_id: str, tomo_name: str, slug: str
    ) -> dict[str, Any]:
        """scancel ONE list's in-flight extraction and mark its instance FAILED.

        The delete path needs this (roadmap 07 review, finding H): deleting a pick list
        rmtree's the very out dir a running extraction is writing into, the job then RE-CREATES
        it, and the instance that held its SLURM id is popped in the same breath — so nothing
        in the project can stop it or explain the directory that reappeared. Cancelling first
        keeps the delete honest. No-op ``ok(cancelled=None)`` when nothing is in flight."""
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
        which a one-off extraction deliberately never sets). Neither consequence is cosmetic:
        the chip asserts Queued/Running forever, and ``extract_authoritative_pending`` reports
        the list as "still running" and refuses to resubmit it *for good* — the batch button
        becomes a permanent no-op for that list, with the per-row confirm as the only way out.

        So ask SLURM instead of trusting the stored status:

          · id still in the queue → genuinely live; promote QUEUED → RUNNING once the
            allocation has started (``run.out`` exists). Left alone otherwise.
          · ``query_jobs_by_ids`` returns None → squeue ITSELF failed. Conclude NOTHING
            (transient infra is not evidence) and report the instance as ``unknown``.
          · id gone from the queue → the out dir decides. A ``result.json`` with ``ok`` also
            runs ``PickList.mark_extracted``, which is how an output whose awaiter died gets
            RECOVERED rather than re-cut; an exit-failure carries the driver's own text; and
            "left the queue without writing a result" is itself a failure, stated as one
            instead of left spinning.
          · no ``slurm_job_id`` recorded at all → ``unknown``, untouched: that is also the
            sub-second window inside ``extract_pick_list`` between resetting the instance and
            sbatch returning, and marking a job that is *being submitted right now* as failed
            would be a worse lie than the one being fixed.

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

    async def extract_authoritative_pending(
        self, project_path: Path, species_id: str, *, only_tomos: list[str] | None = None, timeout_s: int = 3600
    ) -> dict[str, Any]:
        """§8.3 auto-extract: submit per-list subtomo extraction for every WORKBENCH
        authoritative list that is NOT_EXTRACTED/STALE (optionally limited to ``only_tomos``),
        wait for completion, record ``mark_extracted``, and persist. Never touches
        'auto'/'filtered' or already-ready lists; idempotent (re-running acts only on
        still-pending lists, and a list whose extraction SLURM confirms is still in flight is
        reported under ``still_running`` instead of being resubmitted — its output would be
        wiped from under the running job). Opens with ``reconcile_pick_list_extractions`` so
        that check is made against the queue rather than against a stored status no awaiter is
        maintaining. Reuses ``extract_pick_list`` (which clears ``out/`` for a clean re-cut).
        RUN INSIDE A BackgroundTask — it polls up to ``timeout_s``.

        Returns {submitted, succeeded:[{tomo,slug,count}], failed:[{tomo,slug,error}],
        blocked:[{tomo,slug,reason}], still_running:[{tomo,slug}] (already in flight, or
        submitted here and unfinished at ``timeout_s``), can_proceed}."""
        from services.aggregation.authoritative import (
            compute_gate_report,
            enumerate_authoritative,
            extract_inputs_blocked_reason,
            extract_inputs_for_list,
        )

        project_path = Path(project_path)
        # Settle before deciding (roadmap 07 review, finding A/B): an instance whose awaiter
        # died reads Queued/Running with nothing left to move it, and the skip below would
        # then refuse to resubmit that list forever. Running first also matters for the gate —
        # a finished-but-unrecorded extraction gets `mark_extracted` here and drops out of
        # `pending` entirely instead of being cut a second time.
        recon = await self.reconcile_pick_list_extractions(project_path, species_id)
        live_instances = set(recon.get("live") or [])
        state = self.state_service.state_for(project_path)
        handles = await asyncio.to_thread(enumerate_authoritative, state, project_path, species_id)
        pending = compute_gate_report(handles).pending
        if only_tomos is not None:
            keep = set(only_tomos)
            pending = [h for h in pending if h.tomo_name in keep]

        submitted: list[dict] = []
        failed: list[dict] = []
        blocked: list[dict] = []
        still_running: list[dict] = []
        watching: list[dict] = []  # {tomo, slug, out_dir, instance_id, slurm_job_id}
        for h in pending:
            # NEVER resubmit a list whose extraction is genuinely in flight (roadmap 07-S3):
            # a submit wipes out/ and the exit markers first, so a second job would cut into
            # the dir the first is writing. This is the common case rather than an edge — the
            # gate calls a list "pending" the moment a re-extract clears its output, so a
            # single in-flight re-extract puts the list right back in this loop's input.
            # "Genuinely" = SLURM still has the id (`live_instances`), not merely a stored
            # status: the reconciler above has already settled everything else, and an instance
            # with no recorded id is deliberately NOT skipped — there is provably no job to
            # clash with, so resubmitting is right (the sub-second cost is a duplicate submit
            # if a per-row extract is sbatch-ing this very list at this very moment, which the
            # per-list SingleFlight and the task-registry dedup key already make unlikely).
            if extract_pick_list_instance_id(species_id, h.tomo_name, h.slug) in live_instances:
                logger.info(
                    "extract_authoritative_pending: %s/%s is live in SLURM — not resubmitting", h.tomo_name, h.slug
                )
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

        handles2 = await asyncio.to_thread(enumerate_authoritative, state, project_path, species_id)
        return {
            "submitted": len(submitted),
            "succeeded": succeeded,
            "failed": failed,
            "blocked": blocked,
            "still_running": still_running,
            "can_proceed": compute_gate_report(handles2).can_proceed,
        }

    # ── ChimeraX + ArtiaX curation — thin delegation; logic + docs live in
    # services/curation/session_service.py (CurationSessionService) ─────────────

    async def launch_curation_session(
        self, project_path: Path | None = None, cxc_path: Path | None = None, scope: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self.curation_service.launch_curation_session(project_path, cxc_path=cxc_path, scope=scope)

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

    # NOTE (roadmap 10-S1): `save_session_particle_lists` / `save_curation_picks` /
    # `load_into_session` / `get_curation_loaded` used to sit here. Model B sends a curation
    # session NOTHING after launch, so they are gone — see services/curation/session_service.py.

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
        """Greedy radius-dedup ONE registered pick list in place (roadmap 11-S1): rewrite
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
        # AWAIT (force) so the dedup'd count lands on disk — a fire-and-forget
        # create_task gets GC'd before it runs (same bug as the manual-list save).
        await self.state_service.save_project(project_path=project_path, force=True)
        return res

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

    async def scan_for_projects(self, base_path: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._scan_for_projects_sync, base_path)

    async def read_project_state_detached(self, project_path: str):
        """Load a project's ProjectState from disk for read-only preview WITHOUT
        registering it in the shared in-memory registry (so the central pipeline
        observer / sync never picks up a merely-previewed project). Used by the
        project hub's left-hand params panel."""
        from services.project_state import ProjectState

        params_file = Path(project_path) / "project_params.json"
        return await asyncio.to_thread(ProjectState.load, params_file)

    def _scan_for_projects_sync(self, base_path: str) -> list[dict[str, Any]]:
        projects = []
        path = Path(base_path)

        logger.info("Scanning directory: %s", path)

        if not path.exists():
            logger.info("Path does not exist: %s", path)
            return []
        if not path.is_dir():
            logger.info("Path is not a directory: %s", path)
            return []

        try:
            for item in path.iterdir():
                if not item.is_dir():
                    continue
                params_file = item / "project_params.json"
                if not params_file.exists():
                    if not item.name.startswith("."):
                        logger.info("Skipped '%s' - missing project_params.json", item.name)
                    continue
                try:
                    stats = params_file.stat()
                    mod_time = datetime.fromtimestamp(stats.st_mtime)

                    created_at = None
                    created_ts = None
                    creator = None
                    owner_raw = None
                    pipeline_active = False
                    total_jobs_planned = 0
                    # Initialized here, not only inside the try below: an unreadable
                    # project_params.json must still reach the fallback path (which exists so
                    # "the UI always has something to show"), and _derive_live_status is called
                    # unconditionally at the bottom of this block.
                    jobs_dict: dict[str, Any] = {}
                    ts_count = 0
                    mnemonic = ""
                    source_directory = ""
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
                        jobs_dict = {
                            iid: job for iid, job in (data.get("jobs") or {}).items() if _is_pipeline_job_dict(job)
                        }
                        total_jobs_planned = len(jobs_dict)
                        ts_count = data.get("import_selected_tilt_series") or data.get("import_total_tilt_series") or 0
                        mnemonic = data.get("mnemonic") or ""
                        # Where the raw data came from. Prefer the resolved
                        # frames dir; fall back to the movies glob's parent.
                        source_directory = data.get("import_source_directory") or ""
                        if not source_directory:
                            mg = data.get("movies_glob") or ""
                            if mg:
                                source_directory = str(Path(mg).parent) if "*" in mg else mg
                    except Exception as e:
                        logger.warning("Unreadable project_params.json for %s -- using fallbacks: %s", item.name, e)

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

                    derived = self._derive_live_status(item, jobs_dict)
                    derived["pipeline_active_flag"] = pipeline_active

                    last_activity_ts = max(stats.st_mtime, derived.get("last_activity_ts", 0.0))

                    projects.append(
                        {
                            "name": item.name,
                            "path": str(item),
                            "mnemonic": mnemonic,
                            "modified": mod_time.strftime("%Y-%m-%d %H:%M"),
                            "modified_timestamp": stats.st_mtime,
                            "created_at": created_at,
                            "created_timestamp": created_ts,
                            "creator": creator,
                            "owner": owner_raw,
                            "pipeline_active": pipeline_active,
                            "total_jobs_planned": total_jobs_planned,
                            "ts_count": ts_count,
                            "source_directory": source_directory,
                            "last_activity_ts": last_activity_ts,
                            "last_activity": datetime.fromtimestamp(last_activity_ts).strftime("%Y-%m-%d %H:%M"),
                            **derived,
                        }
                    )
                except Exception as e:
                    logger.info("Error reading %s: %s", item.name, e)

        except Exception as e:
            logger.error("Error scanning projects: %s", e)
            return []

        # Stable order: oldest-first by created_at (or ctime fallback). Newly
        # created projects always go to the end -- positions never shuffle on
        # in-place edits to project_params.json.
        projects.sort(key=lambda x: (x["created_timestamp"], x["name"]))
        for idx, proj in enumerate(projects, start=1):
            proj["stable_index"] = idx
        logger.info("Found %d valid projects.", len(projects))
        return projects

    @staticmethod
    def _derive_live_status(project_dir: Path, jobs_dict: dict[str, Any]) -> dict[str, Any]:
        """Derive pipeline status for the roster from project_params.json's
        `jobs` dict (canonical: includes tiltFilter and other non-RELION
        jobs that aren't in default_pipeline.star). Disk RELION_JOB_EXIT_*
        markers are only used to reconcile in-memory `Running` statuses
        that the schemer crashed before persisting -- otherwise the project
        state is the source of truth."""
        out: dict[str, Any] = {
            "executed_jobs": 0,
            "succeeded": 0,
            "running_live": 0,
            "failed": 0,
            "scheduled": 0,
            "live_status": "idle",
            "last_activity_ts": 0.0,
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
            # Reconcile Running against on-disk exit markers so a crashed
            # schemer doesn't leave us perpetually showing a dead job as
            # "live".
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
            elif status == "Failed":
                out["failed"] += 1
                out["executed_jobs"] += 1
            elif status == "Scheduled":
                out["scheduled"] += 1
            # Anything else (Idle / Pending / unknown) is ignored.

        if out["running_live"] > 0:
            out["live_status"] = "running"
        elif out["failed"] > 0:
            out["live_status"] = "failed"
        elif out["succeeded"] > 0 and out["scheduled"] == 0 and out["failed"] == 0 and out["running_live"] == 0:
            out["live_status"] = "done"
        else:
            out["live_status"] = "idle"

        return out

    async def get_available_jobs(self) -> list[str]:
        # --- FIX: Use config root instead of cwd ---
        template_path = self.config_service.crboost_root / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

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
        )

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

    async def get_job_logs(self, project_path: str, job_name: str) -> dict[str, str]:
        """Gets logs for a specific job *path* (e.g., "External/job003/")."""
        return await self.pipeline_runner.get_job_logs(project_path, job_name)

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
