import asyncio
import logging
import os
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from typing import TYPE_CHECKING

from services.models_base import JobType
from services.project_state import JobStatus
from services.scheduling_and_orchestration.pipeline_orchestrator_service import JobTypeResolver

if TYPE_CHECKING:
    from backend import CryoBoostBackend
    from services.project_state import ProjectState

logger = logging.getLogger(__name__)


class PipelineRunnerService:
    """
    Owns the schemer process lifecycle and job status sync.

    sync_all_jobs is the single entry point for reconciling default_pipeline.star
    with in-memory job state. It is called periodically from the UI layer.
    """

    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self._active_processes: Dict[Path, asyncio.subprocess.Process] = {}
        self._stdout_log_paths: Dict[Path, Path] = {}
        self._stderr_log_paths: Dict[Path, Path] = {}
        # Retry monitors bypass the schemer but still count as pipeline activity —
        # tracked here so is_active() reports true and sync_all_jobs doesn't clear
        # pipeline_active out from under a running retry.
        self._retry_monitors: Dict[Path, asyncio.Task] = {}
        self.job_resolver = JobTypeResolver(backend_instance.pipeline_orchestrator.star_handler)

    def is_active(self, project_path: Path) -> bool:
        resolved = project_path.resolve()
        return resolved in self._active_processes or resolved in self._retry_monitors

    # -------------------------------------------------------------------------
    # Status sync
    # -------------------------------------------------------------------------

    async def sync_all_jobs(self, project_path: str) -> Dict[str, bool]:
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {}

        star_handler = self.backend.pipeline_orchestrator.star_handler
        data = star_handler.read(pipeline_star)
        processes = data.get("pipeline_processes", pd.DataFrame())

        star_patched = False
        failed_job_paths: List[str] = []
        already_failed_in_star = False
        if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
            for idx, row in processes.iterrows():
                label = row["rlnPipeLineProcessStatusLabel"]
                if label == "Failed":
                    # The relion schemer recorded this failure itself before
                    # halting the scheme — there's no Running->Failed transition
                    # left for the branch below to catch.
                    already_failed_in_star = True
                elif label == "Running":
                    job_dir = Path(project_path) / row["rlnPipeLineProcessName"].rstrip("/")
                    if (job_dir / "RELION_JOB_EXIT_SUCCESS").exists():
                        processes.at[idx, "rlnPipeLineProcessStatusLabel"] = "Succeeded"
                        star_patched = True
                        logger.info(
                            "Reconciled %s: Running -> Succeeded (RELION_JOB_EXIT_SUCCESS found on disk)",
                            row["rlnPipeLineProcessName"],
                        )
                    elif (job_dir / "RELION_JOB_EXIT_FAILURE").exists():
                        processes.at[idx, "rlnPipeLineProcessStatusLabel"] = "Failed"
                        star_patched = True
                        failed_job_paths.append(row["rlnPipeLineProcessName"])
                        logger.info(
                            "Reconciled %s: Running -> Failed (RELION_JOB_EXIT_FAILURE found on disk)",
                            row["rlnPipeLineProcessName"],
                        )
            if star_patched:
                data["pipeline_processes"] = processes
                star_handler.write(data, pipeline_star)

        changes: Dict[str, bool] = {}
        state = self.backend.state_service.state_for(Path(project_path))
        project_root = Path(project_path)

        # Stop the pipeline on any job failure. Using stop_and_cleanup (not just
        # stop_pipeline) so that any Scheduled/Pending downstream jobs are
        # patched to Failed too — otherwise the UI's all_done check in
        # status_poller.py:45-51 sees `scheduled > 0` and the spinner never
        # resolves. Also scancels any live array tasks collected from job
        # manifests. The failed job's own supervisor already exited, so this
        # is idempotent on that side.
        #
        # Two triggers:
        #   - failed_job_paths: a Running->Failed transition we just reconciled
        #     from an exit marker above.
        #   - already_failed_in_star: the relion schemer recorded the failure
        #     itself and exited. That path never populates failed_job_paths, so
        #     without this branch the downstream jobs it blocked (often never
        #     even dispatched into default_pipeline.star — e.g. a pending tsCtf)
        #     stay Scheduled forever and the UI hangs on a phantom "done". The
        #     guard makes it fire once and no-op on every subsequent poll.
        needs_stop_on_fail = bool(failed_job_paths)
        if already_failed_in_star and not needs_stop_on_fail:
            star_has_live_row = bool(
                processes["rlnPipeLineProcessStatusLabel"].isin(["Running", "Pending"]).any()
            )
            mem_has_live_job = any(
                j.execution_status in (JobStatus.RUNNING, JobStatus.QUEUED, JobStatus.SCHEDULED)
                and not getattr(j, "IS_INTERACTIVE", False)
                for j in state.jobs.values()
            )
            needs_stop_on_fail = star_has_live_row or mem_has_live_job

        if needs_stop_on_fail:
            active = self.is_active(Path(project_path))
            logger.info(
                "STOP-ON-FAIL: failure in %s — markers=%s already_in_star=%s; is_active=%s",
                project_path, failed_job_paths, already_failed_in_star, active,
            )
            try:
                result = await self.stop_and_cleanup(Path(project_path), slurm_job_ids=[])
                logger.info("STOP-ON-FAIL: stop_and_cleanup returned %s", result)
            except Exception as e:
                logger.warning("STOP-ON-FAIL: stop_and_cleanup raised: %s", e)

        # build squeue lookup once if any Running rows exist
        slurm_jobs_by_dir: dict = {}
        if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
            has_running = (processes["rlnPipeLineProcessStatusLabel"] == "Running").any()
            if has_running:
                try:
                    slurm_jobs = await self.backend.slurm_service.get_user_jobs(force_refresh=True)
                    for sj in slurm_jobs:
                        if sj.stdout_path:
                            try:
                                resolved_dir = str(Path(sj.stdout_path).resolve().parent)
                                existing = slurm_jobs_by_dir.get(resolved_dir)
                                if existing is None:
                                    slurm_jobs_by_dir[resolved_dir] = sj
                                else:
                                    # For array jobs, the supervisor (run.out) and child tasks
                                    # (task_0.out, task_1.out, ...) all resolve to the same dir.
                                    # Prefer the supervisor so the model's slurm_job_id is the
                                    # supervisor ID, which is what cancel_job needs to scancel.
                                    existing_is_task = Path(existing.stdout_path).name.startswith("task_")
                                    new_is_task = Path(sj.stdout_path).name.startswith("task_")
                                    if existing_is_task and not new_is_task:
                                        slurm_jobs_by_dir[resolved_dir] = sj
                            except Exception as e:
                                logger.info("Could not resolve stdout path for SLURM job %s: %s", sj.job_id, e)
                except Exception as e:
                    logger.info("Could not fetch squeue for QUEUED cross-reference: %s", e)

        if state.pipeline_active and not self.is_active(Path(project_path)):
            logger.info("Clearing stale pipeline_active flag (no active schemer process)")
            state.pipeline_active = False
            try:
                await self.backend.state_service.save_project(project_path=Path(project_path), force=True)
            except Exception as e:
                logger.info("Could not persist pipeline_active reset: %s", e)

        path_to_instance: Dict[str, str] = {}

        for iid, model in state.jobs.items():
            rjn = getattr(model, "relion_job_name", None)
            if rjn:
                path_to_instance[rjn.rstrip("/")] = iid

        for iid in state.jobs:
            mapped = (state.job_path_mapping or {}).get(iid)
            if mapped:
                key = mapped.rstrip("/")
                if key not in path_to_instance:
                    path_to_instance[key] = iid

        for iid, model in state.jobs.items():
            if getattr(model, "relion_job_name", None):
                continue
            job_dir_abs = (model.paths or {}).get("job_dir")
            if job_dir_abs:
                try:
                    rel = str(Path(job_dir_abs).relative_to(project_root))
                    if rel not in path_to_instance:
                        path_to_instance[rel] = iid
                except ValueError:
                    pass

        type_instance_count: Dict[str, int] = {}
        for iid in state.jobs:
            base = iid.split("__")[0]
            type_instance_count[base] = type_instance_count.get(base, 0) + 1

        found_instances: set = set()

        for _, row in processes.iterrows():
            job_path = row["rlnPipeLineProcessName"]
            job_path_clean = job_path.rstrip("/")

            instance_id = path_to_instance.get(job_path_clean)

            if instance_id is None:
                job_type_str = self.job_resolver.get_job_type_from_path(Path(project_path), job_path)
                if not job_type_str:
                    continue
                if type_instance_count.get(job_type_str, 0) == 1:
                    instance_id = job_type_str
                else:
                    continue

            if instance_id not in state.jobs:
                continue

            job_model = state.jobs[instance_id]
            old_status = job_model.execution_status

            status_str = row["rlnPipeLineProcessStatusLabel"]
            if status_str == "Pending":
                new_status = JobStatus.SCHEDULED
            elif status_str == "Running":
                job_dir_abs = str((Path(project_path) / job_path_clean).resolve())
                sj = slurm_jobs_by_dir.get(job_dir_abs)
                if sj:
                    job_model.slurm_job_id = sj.job_id
                    new_status = JobStatus.QUEUED if sj.state == "PENDING" else JobStatus.RUNNING
                else:
                    new_status = JobStatus.RUNNING

                # For array-dispatching jobs: if the supervisor wrote a task
                # manifest, the job is actively running even if the supervisor's
                # own SLURM state is still PENDING (children may already be running).
                if new_status == JobStatus.QUEUED:
                    manifest_path = Path(project_path) / job_path_clean / ".task_manifest.json"
                    if manifest_path.exists():
                        new_status = JobStatus.RUNNING
            else:
                try:
                    new_status = JobStatus(status_str)
                except ValueError:
                    new_status = JobStatus.UNKNOWN

            job_model.execution_status = new_status
            job_model.relion_job_name = job_path
            job_model.relion_job_number = self._extract_job_number(job_path)
            state.job_path_mapping[instance_id] = job_path

            if old_status != new_status:
                changes[instance_id] = True
                if (
                    new_status == JobStatus.SUCCEEDED
                    and job_model.job_type is not None
                    and job_model.job_type.value == "tsCtf"
                ):
                    self._kickoff_tilt_thumbnails(project_path, state, job_model)

            found_instances.add(instance_id)

        for instance_id, job_model in state.jobs.items():
            if instance_id in found_instances:
                continue

            # Interactive jobs manage their own status; don't reset them.
            if getattr(job_model, "IS_INTERACTIVE", False):
                continue

            old_status = job_model.execution_status

            if job_model.relion_job_name:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
                job_model.slurm_job_id = None
                state.job_path_mapping.pop(instance_id, None)
                if old_status != JobStatus.SCHEDULED:
                    changes[instance_id] = True
            elif job_model.execution_status not in (JobStatus.SCHEDULED, JobStatus.UNKNOWN, JobStatus.FAILED):
                # FAILED is excluded on purpose: a job with no relion_job_name
                # that is FAILED was deliberately marked so by stop_and_cleanup
                # (upstream failed, this one never got to run). Resetting it to
                # SCHEDULED here would resurrect it as "pending" under a
                # phantom-done pipeline — the exact stuck state this guards.
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
                job_model.slurm_job_id = None
                state.job_path_mapping.pop(instance_id, None)
                changes[instance_id] = True

        if changes:
            try:
                logger.info("Persisting %d status changes to disk.", len(changes))
                await self.backend.state_service.save_project(project_path=Path(project_path), force=True)
            except Exception as e:
                logger.error("Failed to persist status changes: %s", e)

        return changes

    def _extract_job_number(self, job_path: str) -> int:
        try:
            return int(job_path.rstrip("/").split("job")[-1])
        except Exception:
            return 0

    def _kickoff_tilt_thumbnails(self, project_path: str, state, job_model) -> None:
        """When tsCtf flips to SUCCEEDED, render PNG previews for the
        tilt-filter panel in the background so the user doesn't have to
        click 'Generate Thumbnails' on first visit. No-op if PNGs already
        exist on disk. dedup_key matches ui/tilt_filter_panel.py so a
        manual click cannot double up with this auto-trigger."""
        try:
            from services.background_tasks import get_background_task_registry
            from services.tilt_series_service import generate_tilt_thumbnails

            proj_path = Path(project_path)
            pd_str = getattr(state, "tilt_filter_png_dir", None) if state is not None else None
            png_dir = Path(pd_str) if pd_str else proj_path / "TiltFilter" / "png"

            if png_dir.exists() and any(png_dir.glob("*.png")):
                return

            star_rel = (getattr(job_model, "paths", {}) or {}).get("output_star")
            ts_ctf_star: Optional[Path] = None
            if star_rel:
                p = Path(star_rel) if Path(star_rel).is_absolute() else proj_path / star_rel
                if p.exists():
                    ts_ctf_star = p
            if ts_ctf_star is None and getattr(job_model, "relion_job_name", None):
                p = proj_path / job_model.relion_job_name / "ts_ctf_tilt_series.star"
                if p.exists():
                    ts_ctf_star = p
            if ts_ctf_star is None:
                logger.info("tsCtf auto-thumbnail: no output star found, skipping")
                return

            dedup_key = f"tilt-filter-thumbnails:{proj_path}:{png_dir}"
            registry = get_background_task_registry()
            backend = self.backend

            async def _run(progress_cb):
                n = await asyncio.to_thread(
                    generate_tilt_thumbnails, ts_ctf_star, proj_path, png_dir, progress_cb
                )
                st = backend.state_service.state_for(proj_path)
                if st is not None:
                    st.tilt_filter_png_dir = str(png_dir)
                    st.mark_dirty()
                    try:
                        await backend.state_service.save_project(project_path=proj_path)
                    except Exception as e:
                        logger.info("tsCtf auto-thumbnail: save_project failed: %s", e)
                return f"{n} thumbnails generated"

            registry.submit(
                _run,
                title="Tilt thumbnails (auto)",
                subtitle=f"Triggered by tsCtf completion · {png_dir.name}",
                project_path=str(proj_path),
                dedup_key=dedup_key,
            )
            logger.info("tsCtf auto-thumbnail: kicked off for %s", proj_path)
        except Exception:
            # Auto-trigger failures must never break the status-sync loop.
            logger.exception("tsCtf auto-thumbnail kickoff failed")

    # -------------------------------------------------------------------------
    # Pipeline overview / logs
    # -------------------------------------------------------------------------

    async def get_pipeline_overview(self, project_path: str):
        state = self.backend.state_service.state_for(Path(project_path))

        if not state.jobs:
            return {
                "status": "ok",
                "total": 0,
                "completed": 0,
                "running": 0,
                "failed": 0,
                "scheduled": 0,
                "is_complete": True,
                "jobs": {},
            }

        total = succeeded = running = failed = scheduled = 0
        for job_model in state.jobs.values():
            # IMPORT_MOVIES is a local pre-step; TS_IMPORT is a silently-injected
            # prerequisite of TS_ALIGNMENT (see pipeline_builder_panel._PREREQUISITES).
            # Neither appears in the user-visible roster (PHASE_JOBS), so both
            # must be excluded to keep the counter denominator aligned with the UI.
            if job_model.job_type in (JobType.IMPORT_MOVIES, JobType.TS_IMPORT):
                continue
            # Interactive jobs (e.g. TiltFilter) are never dispatched by the relion
            # schemer and manage their own status via the UI. If we counted them
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

        return {
            "status": "ok",
            "total": total,
            "completed": int(succeeded),
            "running": int(running),
            "failed": int(failed),
            "scheduled": int(scheduled),
            "is_complete": running == 0 and total > 0,
            "jobs": {},
        }

    async def get_job_logs(self, project_path: str, job_name: str) -> Dict[str, str]:
        job_path = Path(project_path) / job_name.rstrip("/")
        logs = {"stdout": "", "stderr": "", "exists": False, "path": str(job_path)}

        if not job_path.exists():
            logs["stdout"] = f"Job directory not found:\n{job_path}"
            return logs

        logs["exists"] = True

        out_file = job_path / "run.out"
        if out_file.exists():
            try:
                with open(out_file, "r", encoding="utf-8") as f:
                    logs["stdout"] = f.read()
            except Exception as e:
                logs["stdout"] = f"Error reading run.out: {e}"
        else:
            logs["stdout"] = "run.out not found."

        err_file = job_path / "run.err"
        if err_file.exists():
            try:
                with open(err_file, "r", encoding="utf-8") as f:
                    logs["stderr"] = f.read()
            except Exception as e:
                logs["stderr"] = f"Error reading run.err: {e}"
        else:
            logs["stderr"] = "run.err not found."

        return logs

    def get_schemer_logs(self, project_path: Path) -> Dict[str, str]:
        resolved = project_path.resolve()
        logs = {"stdout": "", "stderr": "", "running": resolved in self._active_processes}

        stdout_path = self._stdout_log_paths.get(resolved)
        stderr_path = self._stderr_log_paths.get(resolved)

        if stdout_path and stdout_path.exists():
            try:
                with open(stdout_path, "r") as f:
                    logs["stdout"] = f.read()
            except Exception as e:
                logs["stdout"] = f"Error reading log: {e}"

        if stderr_path and stderr_path.exists():
            try:
                with open(stderr_path, "r") as f:
                    logs["stderr"] = f.read()
            except Exception as e:
                logs["stderr"] = f"Error reading log: {e}"

        return logs

    def get_sbatch_errors(self, project_path: Path) -> List[str]:
        resolved = project_path.resolve()
        stderr_path = self._stderr_log_paths.get(resolved)
        if not stderr_path or not stderr_path.exists():
            return []
        errors = []
        try:
            with open(stderr_path, "r") as f:
                for line in f:
                    if "sbatch: error:" in line:
                        errors.append(line.strip())
        except Exception:
            pass
        return errors

    # -------------------------------------------------------------------------
    # Schemer process management
    # -------------------------------------------------------------------------

    async def run_generated_scheme(self, project_dir: Path, scheme_name: str, bind_paths: List[str]) -> Dict[str, Any]:
        return await self._run_relion_schemer(
            project_dir=project_dir, scheme_name=scheme_name, additional_bind_paths=bind_paths
        )

    async def _run_relion_schemer(
        self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]
    ) -> Dict[str, Any]:
        try:
            state = self.backend.state_service.state_for(project_dir)

            if state.pipeline_active:
                return {
                    "success": False,
                    "error": "Pipeline is already running. Wait for it to complete or restart the server.",
                }

            pipeline_star = project_dir / "default_pipeline.star"
            if not pipeline_star.exists():
                logger.info("default_pipeline.star not found, initializing Relion project...")
                init_command = "unset DISPLAY && relion --tomo --do_projdir ."
                init_full_command = self.backend.container_service.wrap_command_for_tool(
                    command=init_command, cwd=project_dir, tool_name="relion", additional_binds=additional_bind_paths
                )
                init_process = await asyncio.create_subprocess_shell(
                    init_full_command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
                )
                try:
                    # 30s was too tight: on a shared filesystem the apptainer
                    # cold-start + relion first-run can push past that easily.
                    stdout, stderr = await asyncio.wait_for(init_process.communicate(), timeout=180.0)
                    if init_process.returncode != 0:
                        logger.info("Relion init failed: %s", stderr.decode())
                        return {"success": False, "error": f"Failed to initialize Relion project: {stderr.decode()}"}
                    logger.info("Relion project initialized successfully")
                except asyncio.TimeoutError:
                    logger.info("Relion init timed out")
                    init_process.kill()
                    await init_process.wait()
                    return {"success": False, "error": "Relion project initialization timed out"}

            scheme_log_dir = project_dir / "Schemes" / scheme_name
            scheme_log_dir.mkdir(parents=True, exist_ok=True)

            stdout_log = scheme_log_dir / "schemer.out"
            stderr_log = scheme_log_dir / "schemer.err"

            resolved = project_dir.resolve()
            self._stdout_log_paths[resolved] = stdout_log
            self._stderr_log_paths[resolved] = stderr_log

            scheme_control_dir = f"Schemes/{scheme_name}/"
            run_command = (
                f"unset DISPLAY && relion_schemer --scheme {scheme_name} "
                f"--run --pipeline_control {scheme_control_dir} --verb 2"
            )
            full_run_command = self.backend.container_service.wrap_command_for_tool(
                command=run_command, cwd=project_dir, tool_name="relion_schemer", additional_binds=additional_bind_paths
            )

            logger.info("Starting schemer, logging to %s", scheme_log_dir)
            logger.info("Command: %s", run_command)

            stdout_handle = open(stdout_log, "w")
            stderr_handle = open(stderr_log, "w")
            try:
                process = await asyncio.create_subprocess_shell(
                    full_run_command, stdout=stdout_handle, stderr=stderr_handle, cwd=project_dir
                )
            except Exception:
                stdout_handle.close()
                stderr_handle.close()
                raise
            self._active_processes[resolved] = process

            # pipeline_active is flipped AFTER the process is registered in
            # _active_processes. Setting it earlier creates a race: the
            # PipelineMonitor's 3-s tick can fire during the apptainer/
            # subprocess-spawn await window and observe (pipeline_active=True
            # && is_active=False), which triggers the sync_all_jobs self-heal
            # at line 161 — clearing pipeline_active back to False and
            # orphaning the schemer (jobs run, but no one updates their
            # execution_status, so the orchestrator cards stay yellow).
            state.pipeline_active = True
            await self.backend.state_service.save_project(project_path=project_dir, force=True)
            logger.info("Pipeline marked active, state protected from resets")

            asyncio.create_task(
                self._monitor_schemer(
                    process=process,
                    project_dir=project_dir,
                    stdout_handle=stdout_handle,
                    stderr_handle=stderr_handle,
                    stdout_log=stdout_log,
                    stderr_log=stderr_log,
                )
            )

            return {
                "success": True,
                "message": f"Pipeline started (PID: {process.pid})",
                "pid": process.pid,
                "log_dir": str(scheme_log_dir),
            }

        except Exception as e:
            import traceback

            traceback.print_exc()
            try:
                state = self.backend.state_service.state_for(project_dir)
                state.pipeline_active = False
                await self.backend.state_service.save_project(project_path=project_dir, force=True)
            except Exception as save_err:
                logger.info("Failed to reset pipeline_active after error: %s", save_err)
            return {"success": False, "error": str(e)}

    # -------------------------------------------------------------------------
    # Retry path: re-sbatch an existing job dir's supervisor without schemer.
    #
    # When a previous run left per-task .ok files in External/jobNNN/.task_status/,
    # re-sbatching External/jobNNN/run_submit.script puts the driver back in
    # supervisor mode in the same dir. submit_array_job() (drivers/array_job_base.py)
    # diffs .ok files against ts_names and submits a sparse --array for only the
    # failed/missing tilt-series. No new jobNNN gets allocated.
    #
    # After all retries succeed, we hand off to the schemer for any downstream
    # fresh jobs (e.g. alignment/tsctf that never ran because motionctf failed).
    # -------------------------------------------------------------------------

    async def launch_retries(
        self, project_dir: Path, retry_instance_ids: List[str], on_success_fresh_ids: List[str]
    ) -> Dict[str, Any]:
        """
        Re-sbatch the supervisor script of each failed job in place. Register an
        async monitor that waits for RELION_JOB_EXIT_{SUCCESS,FAILURE} markers,
        then invokes deploy_and_run_scheme for on_success_fresh_ids.
        """
        state = self.backend.state_service.state_for(project_dir)

        if state.pipeline_active or self.is_active(project_dir):
            return {
                "success": False,
                "message": "Pipeline is already running. Wait for it to complete or cancel it first.",
            }

        prepared: List[tuple] = []  # (instance_id, job_dir, script_path)
        for iid in retry_instance_ids:
            job_model = state.jobs.get(iid)
            if not job_model:
                return {"success": False, "message": f"Retry target {iid} not in state"}
            rjn = getattr(job_model, "relion_job_name", None)
            if not rjn:
                return {"success": False, "message": f"Retry target {iid} has no relion_job_name"}
            job_dir = project_dir / rjn.rstrip("/")
            script = job_dir / "run_submit.script"
            if not script.exists():
                return {"success": False, "message": f"Cannot retry {iid}: {script} missing"}
            prepared.append((iid, job_dir, script))

        # Clean stale markers, flip pipeline.star to Running, sbatch each supervisor.
        for iid, job_dir, script in prepared:
            for marker in ("RELION_JOB_EXIT_SUCCESS", "RELION_JOB_EXIT_FAILURE"):
                (job_dir / marker).unlink(missing_ok=True)

            rjn = state.jobs[iid].relion_job_name
            self._patch_pipeline_process_status(project_dir, rjn, "Running")

            try:
                slurm_id = await self._sbatch_script(script, cwd=project_dir)
            except Exception as e:
                logger.exception("sbatch failed for retry of %s", iid)
                self._patch_pipeline_process_status(project_dir, rjn, "Failed")
                return {"success": False, "message": f"sbatch failed for {iid}: {e}"}

            job_model = state.jobs[iid]
            job_model.slurm_job_id = slurm_id
            job_model.execution_status = JobStatus.RUNNING
            logger.info("Retry sbatched: instance=%s dir=%s slurm=%s", iid, job_dir, slurm_id)

        resolved = project_dir.resolve()
        monitor_task = asyncio.create_task(
            self._monitor_retries_and_handoff(
                project_dir=project_dir,
                retry_dirs=[(iid, d) for iid, d, _ in prepared],
                on_success_fresh_ids=on_success_fresh_ids,
            )
        )
        self._retry_monitors[resolved] = monitor_task

        # Flip pipeline_active AFTER _retry_monitors registration so the
        # monitor's tick can't observe (pipeline_active=True && is_active=False)
        # during the save's await window and self-heal it back to False. Same
        # race as run_generated_scheme; same fix.
        state.pipeline_active = True
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        message = (
            f"Retrying {len(prepared)} job(s) in place; "
            f"will continue with {len(on_success_fresh_ids)} fresh job(s) on success"
        )
        return {"success": True, "message": message, "pid": 0}

    async def _monitor_retries_and_handoff(
        self,
        project_dir: Path,
        retry_dirs: List[tuple],  # [(instance_id, job_dir), ...]
        on_success_fresh_ids: List[str],
    ) -> None:
        resolved = project_dir.resolve()
        poll_interval = 5.0
        try:
            while True:
                pending = []
                failed = []
                for iid, job_dir in retry_dirs:
                    if (job_dir / "RELION_JOB_EXIT_FAILURE").exists():
                        failed.append((iid, job_dir))
                    elif (job_dir / "RELION_JOB_EXIT_SUCCESS").exists():
                        continue
                    else:
                        pending.append((iid, job_dir))

                if not pending:
                    break
                await asyncio.sleep(poll_interval)

            if failed:
                logger.info("Retry monitor: %d retry(ies) failed: %s", len(failed), [i for i, _ in failed])
                state = self.backend.state_service.state_for(project_dir)
                state.pipeline_active = False
                await self.backend.state_service.save_project(project_path=project_dir, force=True)
                return

            logger.info("Retry monitor: all %d retry(ies) succeeded", len(retry_dirs))

            # Clear pipeline_active so deploy_and_run_scheme's guard passes when
            # we hand off. The schemer branch sets it back to True.
            state = self.backend.state_service.state_for(project_dir)
            state.pipeline_active = False
            await self.backend.state_service.save_project(project_path=project_dir, force=True)

            if not on_success_fresh_ids:
                logger.info("Retry monitor: no fresh jobs to hand off to schemer")
                return

            # Remove our monitor entry BEFORE calling deploy so is_active() sees no activity.
            self._retry_monitors.pop(resolved, None)

            result = await self.backend.pipeline_orchestrator.deploy_and_run_scheme(
                project_dir=project_dir, selected_instance_ids=on_success_fresh_ids
            )
            if not result.get("success"):
                logger.info("Retry->schemer handoff failed: %s", result)

        except asyncio.CancelledError:
            logger.info("Retry monitor cancelled for %s", project_dir)
            raise
        except Exception:
            logger.exception("Retry monitor crashed for %s", project_dir)
        finally:
            self._retry_monitors.pop(resolved, None)

    def _finalize_stopped_task_statuses(self, job_dir: Path) -> int:
        """For each manifest item without .ok/.fail but with task_{idx}.out on
        disk (i.e. it was actively running when the pipeline got stopped), write
        a .fail marker atomically. Returns the count of markers written."""
        import json

        manifest_path = job_dir / ".task_manifest.json"
        if not manifest_path.exists():
            return 0
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            return 0
        items = manifest.get("items") or []
        if not items:
            return 0

        status_dir = job_dir / ".task_status"
        status_dir.mkdir(parents=True, exist_ok=True)
        existing = (
            {p.stem for p in status_dir.glob("*.ok")}
            | {p.stem for p in status_dir.glob("*.fail")}
            | {p.stem for p in status_dir.glob("*.skip")}
        )

        marked = 0
        for idx, name in enumerate(items):
            if name in existing:
                continue
            if not (job_dir / f"task_{idx}.out").exists():
                continue
            # Atomic write: temp then rename.
            tmp = status_dir / f".{name}.fail.tmp"
            target = status_dir / f"{name}.fail"
            try:
                tmp.write_text("")
                os.replace(tmp, target)
                marked += 1
            except Exception:
                tmp.unlink(missing_ok=True)
        return marked

    def _patch_pipeline_process_status(self, project_dir: Path, job_path: str, new_status: str) -> None:
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            return
        star_handler = self.backend.pipeline_orchestrator.star_handler
        try:
            data = star_handler.read(pipeline_star)
            processes = data.get("pipeline_processes", pd.DataFrame())
            if processes.empty or "rlnPipeLineProcessName" not in processes.columns:
                return
            key = job_path.rstrip("/") + "/"
            mask = processes["rlnPipeLineProcessName"] == key
            if mask.any():
                processes.loc[mask, "rlnPipeLineProcessStatusLabel"] = new_status
                data["pipeline_processes"] = processes
                star_handler.write(data, pipeline_star)
        except Exception as e:
            logger.warning("Could not patch %s status to %s: %s", job_path, new_status, e)

    async def _sbatch_script(self, script_path: Path, cwd: Path) -> str:
        """sbatch in an env stripped of SLURM_*/SBATCH_* so the submission
        doesn't inherit any parent job context. Returns the SLURM job ID."""
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}
        process = await asyncio.create_subprocess_exec(
            "sbatch",
            str(script_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(cwd),
            env=clean_env,
        )
        stdout_b, stderr_b = await process.communicate()
        stdout = stdout_b.decode()
        stderr = stderr_b.decode()
        if process.returncode != 0:
            raise RuntimeError(f"sbatch rc={process.returncode} stderr={stderr!r}")
        # "Submitted batch job 12345"
        return stdout.strip().split()[-1]

    async def _monitor_schemer(
        self,
        process: asyncio.subprocess.Process,
        project_dir: Path,
        stdout_handle,
        stderr_handle,
        stdout_log: Path,
        stderr_log: Path,
    ):
        pid = process.pid
        logger.info("Started monitoring schemer PID %s", pid)

        try:
            return_code = await process.wait()
            logger.info("Schemer PID %s exited with code: %s", pid, return_code)

            if return_code == 0:
                logger.info("Pipeline completed successfully")
            else:
                logger.info("Pipeline failed or was interrupted (code %s)", return_code)
                try:
                    if stderr_log.exists():
                        with open(stderr_log, "r") as f:
                            lines = f.readlines()
                            if lines:
                                logger.info("Last stderr lines:")
                                for line in lines[-10:]:
                                    logger.info("  %s", line.rstrip())
                except Exception as e:
                    logger.info("Could not read stderr log: %s", e)

        except asyncio.CancelledError:
            logger.info("Monitor task cancelled for PID %s", pid)
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except Exception:
                process.kill()
            raise

        except Exception as e:
            logger.info("Error monitoring PID %s: %s", pid, e)
            import traceback

            traceback.print_exc()

        finally:
            try:
                stdout_handle.close()
            except Exception:
                pass
            try:
                stderr_handle.close()
            except Exception:
                pass

            resolved = project_dir.resolve()
            if self._active_processes.get(resolved) is process:
                self._active_processes.pop(resolved, None)
                self._stdout_log_paths.pop(resolved, None)
                self._stderr_log_paths.pop(resolved, None)
                # Reconcile job statuses against the schemer's final
                # default_pipeline.star BEFORE pipeline_active goes false.
                # The schemer patches the star (e.g. a job -> Failed) and
                # exits; once pipeline_active is false the PipelineMonitor
                # stops ticking this project, so if we don't reconcile here
                # the failed job freezes at Running and the downstream jobs
                # freeze at Scheduled forever. sync_all_jobs does the
                # reconcile + stop-on-fail cleanup, and its self-heal branch
                # clears pipeline_active for us. _active_processes is already
                # popped above, so is_active() reports false and there's no
                # recursion back into a monitor.
                try:
                    await self.sync_all_jobs(str(project_dir))
                except Exception as e:
                    logger.warning("Post-schemer sync_all_jobs failed: %s", e)
                # Belt-and-braces: guarantee the flag is down even on a clean
                # finish that sync left untouched, or if sync raised above.
                try:
                    current_state = self.backend.state_service.state_for(project_dir)
                    if current_state.pipeline_active:
                        current_state.pipeline_active = False
                        await self.backend.state_service.save_project(project_path=project_dir, force=True)
                    logger.info("Pipeline marked inactive, state saved")
                except Exception as e:
                    logger.warning("Failed to persist pipeline_active=False: %s", e)
            else:
                logger.info("Old schemer PID %s cleaned up, new pipeline already running -- skipping state reset", pid)

    async def stop_pipeline(self, project_path: Path) -> Dict[str, Any]:
        resolved = project_path.resolve()
        process = self._active_processes.get(resolved)
        if not process:
            return {"success": False, "error": "No pipeline is running for this project"}

        pid = process.pid
        logger.info("Stopping schemer PID %s", pid)

        try:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=10.0)
                return {"success": True, "message": f"Pipeline stopped (PID {pid})"}
            except asyncio.TimeoutError:
                logger.info("Schemer didn't respond to SIGTERM, sending SIGKILL")
                process.kill()
                await process.wait()
                return {"success": True, "message": f"Pipeline force-killed (PID {pid})"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def stop_and_cleanup(self, project_dir: Path, slurm_job_ids: List[str]) -> Dict[str, Any]:
        """
        Full stop sequence:
        1. Terminate the schemer process
        2. scancel any live SLURM jobs (array task IDs normalized to parent IDs)
        3. Patch default_pipeline.star: Running/Pending -> Failed
        4. Update in-memory job models to FAILED
        5. Set pipeline_active=False and persist
        """
        from services.computing.slurm_service import normalize_slurm_ids

        errors = []

        resolved = project_dir.resolve()
        process = self._active_processes.get(resolved)
        if process:
            pid = process.pid
            logger.info("Stopping schemer PID %s", pid)
            try:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.info("Schemer didn't respond to SIGTERM, sending SIGKILL")
                    process.kill()
                    await process.wait()
            except Exception as e:
                errors.append(f"schemer termination: {e}")

        # Cancel any retry monitor so it doesn't try to hand off to the schemer
        # after we've cancelled the underlying SLURM jobs.
        retry_task = self._retry_monitors.pop(resolved, None)
        if retry_task and not retry_task.done():
            retry_task.cancel()

        # Also collect array_job_ids from any task manifests in running job dirs.
        state = self.backend.state_service.state_for(project_dir)
        for job_model in state.jobs.values():
            if job_model.execution_status not in (JobStatus.RUNNING, JobStatus.SCHEDULED):
                continue
            job_dir_str = (job_model.paths or {}).get("job_dir")
            if not job_dir_str:
                continue
            manifest_path = Path(job_dir_str) / ".task_manifest.json"
            if manifest_path.exists():
                try:
                    import json

                    manifest = json.loads(manifest_path.read_text())
                    array_jid = manifest.get("array_job_id")
                    if array_jid:
                        slurm_job_ids.append(str(array_jid))
                except Exception:
                    pass

        # Normalize array child IDs (28666490_1) → parent IDs (28666490) so a
        # single scancel kills entire arrays instead of individual tasks.
        normalized_ids = normalize_slurm_ids(slurm_job_ids) if slurm_job_ids else []
        logger.info("stop_and_cleanup: normalized SLURM IDs to cancel: %s", normalized_ids)

        if normalized_ids:
            result = await self.backend.slurm_service.scancel_jobs(normalized_ids)
            if not result["success"]:
                errors.append(f"scancel: {result.get('error')}")

        # After scancel: any array task that was actively running (task_{idx}.out
        # exists, no .ok/.fail) got SIGTERM'd mid-execution and won't write its
        # own status file. Mark those as .fail so the UI tracker flips Running
        # -> Failed on the next poll. Pending tasks (no task_{idx}.out) are left
        # alone — they never started and the sparse-array retry will pick them up.
        for job_model in state.jobs.values():
            if job_model.execution_status not in (JobStatus.RUNNING, JobStatus.SCHEDULED):
                continue
            job_dir_str = (job_model.paths or {}).get("job_dir")
            if not job_dir_str:
                continue
            try:
                n = self._finalize_stopped_task_statuses(Path(job_dir_str))
                if n:
                    logger.info("Marked %d stopped task(s) as .fail in %s", n, job_dir_str)
            except Exception as e:
                logger.info("Could not finalize task statuses in %s: %s", job_dir_str, e)

        pipeline_star = project_dir / "default_pipeline.star"
        if pipeline_star.exists():
            star_handler = self.backend.pipeline_orchestrator.star_handler
            try:
                data = star_handler.read(pipeline_star)
                processes = data.get("pipeline_processes", pd.DataFrame())
                if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
                    processes["rlnPipeLineProcessStatusLabel"] = processes["rlnPipeLineProcessStatusLabel"].replace(
                        {"Running": "Failed", "Pending": "Failed"}
                    )
                    data["pipeline_processes"] = processes
                    star_handler.write(data, pipeline_star)
            except Exception as e:
                errors.append(f"pipeline star patch: {e}")

        state = self.backend.state_service.state_for(project_dir)
        for job_model in state.jobs.values():
            if job_model.execution_status in (JobStatus.RUNNING, JobStatus.SCHEDULED):
                job_model.execution_status = JobStatus.FAILED

        state.pipeline_active = False
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        if errors:
            logger.info("Stop completed with non-fatal errors: %s", errors)
            return {"success": False, "errors": errors}
        return {"success": True, "cancelled_slurm_jobs": len(normalized_ids)}

    async def reset_submission_failure(self, project_dir: Path):
        """
        After sbatch rejects a submission, relion_schemer has already written
        Running into default_pipeline.star. Patch it to Failed so sync_all_jobs
        reads the correct state instead of re-asserting Running on every reload.
        """
        pipeline_star = project_dir / "default_pipeline.star"
        if pipeline_star.exists():
            star_handler = self.backend.pipeline_orchestrator.star_handler
            try:
                data = star_handler.read(pipeline_star)
                processes = data.get("pipeline_processes", pd.DataFrame())
                if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
                    processes["rlnPipeLineProcessStatusLabel"] = processes["rlnPipeLineProcessStatusLabel"].replace(
                        "Running", "Failed"
                    )
                    data["pipeline_processes"] = processes
                    star_handler.write(data, pipeline_star)
            except Exception as e:
                logger.info("Failed to patch pipeline star after sbatch error: %s", e)

        state = self.backend.state_service.state_for(project_dir)
        for job_model in state.jobs.values():
            if job_model.execution_status == JobStatus.RUNNING:
                job_model.execution_status = JobStatus.FAILED
                job_model.relion_job_name = None
                job_model.relion_job_number = None

        await self.backend.state_service.save_project(project_path=project_dir, force=True)

    async def cancel_job(self, project_dir: Path, instance_id: str) -> Dict[str, Any]:
        from services.computing.slurm_service import normalize_slurm_ids

        state = self.backend.state_service.state_for(project_dir)
        job_model = state.jobs.get(instance_id)

        if not job_model:
            return {"success": False, "error": f"Job '{instance_id}' not found in state"}

        if job_model.execution_status not in (JobStatus.RUNNING, JobStatus.SCHEDULED):
            return {"success": False, "error": f"Job is not running (status: {job_model.execution_status})"}

        relion_job_name = job_model.relion_job_name
        if not relion_job_name:
            return {"success": False, "error": "Job has no relion_job_name -- cannot locate its directory"}

        job_dir = project_dir / relion_job_name.rstrip("/")

        # ── Collect ALL related SLURM job IDs ──
        # For array jobs, the same directory can contain a supervisor (run.out) plus
        # N array tasks (task_0.out, task_1.out, ...).  We need to scancel every one
        # of them — and normalize array child IDs (28666490_1) to parent IDs (28666490)
        # so a single scancel kills the whole array.
        raw_ids: list = []

        logger.info("Looking for SLURM jobs with stdout in: %s", job_dir.resolve())
        slurm_jobs = await self.backend.slurm_service.find_all_slurm_jobs_for_directory(job_dir)
        for sj in slurm_jobs:
            raw_ids.append(sj.job_id)

        # Also check the task manifest for an explicit array_job_id written by the
        # supervisor. This is the most reliable source because the supervisor records
        # the parent ID at sbatch time.
        manifest_path = job_dir / ".task_manifest.json"
        if manifest_path.exists():
            try:
                import json

                manifest = json.loads(manifest_path.read_text())
                array_jid = manifest.get("array_job_id")
                if array_jid:
                    raw_ids.append(str(array_jid))
                    logger.info("Found array_job_id %s in task manifest", array_jid)
            except Exception as e:
                logger.info("Could not read task manifest: %s", e)

        ids_to_cancel = normalize_slurm_ids(raw_ids) if raw_ids else []
        logger.info("IDs to cancel (normalized): %s", ids_to_cancel)

        cancelled_ids: list = []
        if ids_to_cancel:
            result = await self.backend.slurm_service.scancel_jobs(ids_to_cancel)
            cancelled_ids = ids_to_cancel
            if not result["success"]:
                logger.info("scancel warning: %s", result.get("error"))
        else:
            logger.info("No SLURM jobs found for %s -- may have already finished", job_dir)

        # ── Patch RELION pipeline star ──
        pipeline_star = project_dir / "default_pipeline.star"
        if pipeline_star.exists():
            star_handler = self.backend.pipeline_orchestrator.star_handler
            try:
                data = star_handler.read(pipeline_star)
                processes = data.get("pipeline_processes", pd.DataFrame())
                if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
                    job_mask = processes["rlnPipeLineProcessName"] == (relion_job_name.rstrip("/") + "/")
                    processes.loc[job_mask, "rlnPipeLineProcessStatusLabel"] = "Failed"
                    data["pipeline_processes"] = processes
                    star_handler.write(data, pipeline_star)
            except Exception as e:
                logger.info("Failed to patch pipeline star for %s: %s", relion_job_name, e)

        job_model.execution_status = JobStatus.FAILED

        # ── Terminate schemer process ──
        resolved = project_dir.resolve()
        process = self._active_processes.get(resolved)
        if process:
            try:
                process.terminate()
                logger.info("Terminated schemer after cancelling %s", relion_job_name)
            except Exception as e:
                logger.info("Could not terminate schemer: %s", e)

        state.pipeline_active = False
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        return {
            "success": True,
            "cancelled_slurm_ids": cancelled_ids,
            "message": (
                f"Cancelled {relion_job_name}"
                + (f" (SLURM {', '.join(cancelled_ids)})" if cancelled_ids else " (no active SLURM jobs found)")
            ),
        }
