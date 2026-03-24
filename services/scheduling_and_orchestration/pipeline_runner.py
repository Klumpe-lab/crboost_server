import asyncio
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from typing import TYPE_CHECKING

from services.project_state import JobStatus
from services.scheduling_and_orchestration.pipeline_orchestrator_service import JobTypeResolver

if TYPE_CHECKING:
    from backend import CryoBoostBackend
    from services.project_state import ProjectState


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
        self.job_resolver = JobTypeResolver(backend_instance.pipeline_orchestrator.star_handler)

    def is_active(self, project_path: Path) -> bool:
        return project_path.resolve() in self._active_processes

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
        if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
            for idx, row in processes.iterrows():
                if row["rlnPipeLineProcessStatusLabel"] == "Running":
                    job_dir = Path(project_path) / row["rlnPipeLineProcessName"].rstrip("/")
                    if (job_dir / "RELION_JOB_EXIT_SUCCESS").exists():
                        processes.at[idx, "rlnPipeLineProcessStatusLabel"] = "Succeeded"
                        star_patched = True
                        print(
                            f"[SYNC] Reconciled {row['rlnPipeLineProcessName']}: "
                            f"Running -> Succeeded (RELION_JOB_EXIT_SUCCESS found on disk)"
                        )
            if star_patched:
                data["pipeline_processes"] = processes
                star_handler.write(data, pipeline_star)

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
                                slurm_jobs_by_dir[resolved_dir] = sj
                            except Exception:
                                pass
                except Exception as e:
                    print(f"[SYNC] Could not fetch squeue for QUEUED cross-reference: {e}")

        changes: Dict[str, bool] = {}
        state = self.backend.state_service.state_for(Path(project_path))
        project_root = Path(project_path)

        if state.pipeline_active and not self.is_active(Path(project_path)):
            print(f"[SYNC] Clearing stale pipeline_active flag (no active schemer process)")
            state.pipeline_active = False
            try:
                await self.backend.state_service.save_project(project_path=Path(project_path), force=True)
            except Exception as e:
                print(f"[SYNC] Could not persist pipeline_active reset: {e}")

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

            found_instances.add(instance_id)

        for instance_id, job_model in state.jobs.items():
            if instance_id in found_instances:
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
            elif job_model.execution_status not in (JobStatus.SCHEDULED, JobStatus.UNKNOWN):
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
                job_model.slurm_job_id = None
                state.job_path_mapping.pop(instance_id, None)
                changes[instance_id] = True

        if changes:
            try:
                print(f"[SYNC] Persisting {len(changes)} status changes to disk.")
                await self.backend.state_service.save_project(project_path=Path(project_path), force=True)
            except Exception as e:
                print(f"[SYNC ERROR] Failed to persist status changes: {e}")

        return changes

    def _extract_job_number(self, job_path: str) -> int:
        try:
            return int(job_path.rstrip("/").split("job")[-1])
        except Exception:
            return 0

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
                print(f"[RUNNER] default_pipeline.star not found, initializing Relion project...")
                init_command = "unset DISPLAY && relion --tomo --do_projdir ."
                init_full_command = self.backend.container_service.wrap_command_for_tool(
                    command=init_command, cwd=project_dir, tool_name="relion", additional_binds=additional_bind_paths
                )
                init_process = await asyncio.create_subprocess_shell(
                    init_full_command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
                )
                try:
                    stdout, stderr = await asyncio.wait_for(init_process.communicate(), timeout=30.0)
                    if init_process.returncode != 0:
                        print(f"[RUNNER] Relion init failed: {stderr.decode()}")
                        return {"success": False, "error": f"Failed to initialize Relion project: {stderr.decode()}"}
                    print(f"[RUNNER] Relion project initialized successfully")
                except asyncio.TimeoutError:
                    print("[RUNNER] Relion init timed out")
                    init_process.kill()
                    await init_process.wait()
                    return {"success": False, "error": "Relion project initialization timed out"}

            state.pipeline_active = True
            await self.backend.state_service.save_project(project_path=project_dir, force=True)
            print(f"[RUNNER] Pipeline marked active, state protected from resets")

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

            print(f"[RUNNER] Starting schemer, logging to {scheme_log_dir}")
            print(f"[RUNNER] Command: {run_command}")

            stdout_handle = open(stdout_log, "w")
            stderr_handle = open(stderr_log, "w")

            process = await asyncio.create_subprocess_shell(
                full_run_command, stdout=stdout_handle, stderr=stderr_handle, cwd=project_dir
            )
            self._active_processes[resolved] = process

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
            except Exception:
                pass
            return {"success": False, "error": str(e)}

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
        print(f"[MONITOR] Started monitoring schemer PID {pid}")

        try:
            return_code = await process.wait()
            print(f"[MONITOR] Schemer PID {pid} exited with code: {return_code}")

            if return_code == 0:
                print(f"[MONITOR] Pipeline completed successfully")
            else:
                print(f"[MONITOR] Pipeline failed or was interrupted (code {return_code})")
                try:
                    if stderr_log.exists():
                        with open(stderr_log, "r") as f:
                            lines = f.readlines()
                            if lines:
                                print(f"[MONITOR] Last stderr lines:")
                                for line in lines[-10:]:
                                    print(f"  {line.rstrip()}")
                except Exception as e:
                    print(f"[MONITOR] Could not read stderr log: {e}")

        except asyncio.CancelledError:
            print(f"[MONITOR] Monitor task cancelled for PID {pid}")
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except Exception:
                process.kill()
            raise

        except Exception as e:
            print(f"[MONITOR] Error monitoring PID {pid}: {e}")
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
                try:
                    current_state = self.backend.state_service.state_for(project_dir)
                    current_state.pipeline_active = False
                    await self.backend.state_service.save_project(project_path=project_dir, force=True)
                    print(f"[MONITOR] Pipeline marked inactive, state saved")
                except Exception as e:
                    print(f"[MONITOR] WARNING: Failed to persist pipeline_active=False: {e}")
            else:
                print(
                    f"[MONITOR] Old schemer PID {pid} cleaned up, new pipeline already running -- skipping state reset"
                )

    async def stop_pipeline(self, project_path: Path) -> Dict[str, Any]:
        resolved = project_path.resolve()
        process = self._active_processes.get(resolved)
        if not process:
            return {"success": False, "error": "No pipeline is running for this project"}

        pid = process.pid
        print(f"[RUNNER] Stopping schemer PID {pid}")

        try:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=10.0)
                return {"success": True, "message": f"Pipeline stopped (PID {pid})"}
            except asyncio.TimeoutError:
                print(f"[RUNNER] Schemer didn't respond to SIGTERM, sending SIGKILL")
                process.kill()
                await process.wait()
                return {"success": True, "message": f"Pipeline force-killed (PID {pid})"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def stop_and_cleanup(self, project_dir: Path, slurm_job_ids: List[str]) -> Dict[str, Any]:
        """
        Full stop sequence:
        1. Terminate the schemer process
        2. scancel any live SLURM jobs
        3. Patch default_pipeline.star: Running/Pending -> Failed
        4. Update in-memory job models to FAILED
        5. Set pipeline_active=False and persist
        """
        errors = []

        resolved = project_dir.resolve()
        process = self._active_processes.get(resolved)
        if process:
            pid = process.pid
            print(f"[RUNNER] Stopping schemer PID {pid}")
            try:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    print(f"[RUNNER] Schemer didn't respond to SIGTERM, sending SIGKILL")
                    process.kill()
                    await process.wait()
            except Exception as e:
                errors.append(f"schemer termination: {e}")

        if slurm_job_ids:
            result = await self.backend.slurm_service.scancel_jobs(slurm_job_ids)
            if not result["success"]:
                errors.append(f"scancel: {result.get('error')}")

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
            print(f"[RUNNER] Stop completed with non-fatal errors: {errors}")
            return {"success": False, "errors": errors}
        return {"success": True, "cancelled_slurm_jobs": len(slurm_job_ids)}

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
                print(f"[RUNNER] Failed to patch pipeline star after sbatch error: {e}")

        state = self.backend.state_service.state_for(project_dir)
        for job_model in state.jobs.values():
            if job_model.execution_status == JobStatus.RUNNING:
                job_model.execution_status = JobStatus.FAILED
                job_model.relion_job_name = None
                job_model.relion_job_number = None

        await self.backend.state_service.save_project(project_path=project_dir, force=True)

    async def cancel_job(self, project_dir: Path, instance_id: str) -> Dict[str, Any]:
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
        print(f"[RUNNER] Looking for SLURM job with stdout in: {job_dir.resolve()}")
        slurm_job = await self.backend.slurm_service.find_slurm_job_for_directory(job_dir)

        cancelled_id = None
        if slurm_job:
            result = await self.backend.slurm_service.scancel_jobs([slurm_job.job_id])
            cancelled_id = slurm_job.job_id
            if not result["success"]:
                print(f"[RUNNER] scancel warning for {slurm_job.job_id}: {result.get('error')}")
        else:
            print(f"[RUNNER] No SLURM job found for {job_dir} -- may have already finished")

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
                print(f"[RUNNER] Failed to patch pipeline star for {relion_job_name}: {e}")

        job_model.execution_status = JobStatus.FAILED

        resolved = project_dir.resolve()
        process = self._active_processes.get(resolved)
        if process:
            try:
                process.terminate()
                print(f"[RUNNER] Terminated schemer after cancelling {relion_job_name}")
            except Exception as e:
                print(f"[RUNNER] Could not terminate schemer: {e}")

        state.pipeline_active = False
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        return {
            "success": True,
            "cancelled_slurm_id": cancelled_id,
            "message": (
                f"Cancelled {relion_job_name}"
                + (f" (SLURM {cancelled_id})" if cancelled_id else " (no active SLURM job found)")
            ),
        }
