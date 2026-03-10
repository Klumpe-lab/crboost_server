import asyncio
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from typing import TYPE_CHECKING

from services.project_state import AbstractJobParams, JobStatus, JobType
from services.scheduling_and_orchestration.pipeline_orchestrator_service import JobTypeResolver


if TYPE_CHECKING:
    from backend import CryoBoostBackend
    from services.project_state import ProjectState


class StatusSyncService:
    """Syncs job model statuses from pipeline.star - single source of truth"""

    def __init__(self, backend):
        self.backend = backend
        self.job_resolver = JobTypeResolver(backend.pipeline_orchestrator.star_handler)

    async def sync_all_jobs(self, project_path: str) -> Dict[str, bool]:
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {}

        star_handler = self.backend.pipeline_orchestrator.star_handler
        data = star_handler.read(pipeline_star)
        processes = data.get("pipeline_processes", pd.DataFrame())

        # Reconcile filesystem reality against the star file.
        # If the schemer died after submitting a job but before updating the star
        # file, the row stays "Running" forever. Detect this by checking for
        # RELION_JOB_EXIT_SUCCESS on disk and patch the star file in place so
        # the rest of this sync -- and all future syncs -- see the correct state.
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

        changes: Dict[str, bool] = {}
        state = self.backend.state_service.state_for(Path(project_path))
        project_root = Path(project_path)

        # If pipeline_active is True but no schemer is alive in this server
        # process, we survived a restart with a stale flag. Clear it now so
        # deploy_and_run_scheme is no longer blocked.
        if state.pipeline_active and self.backend.pipeline_runner.active_schemer_process is None:
            print(f"[SYNC] Clearing stale pipeline_active flag (no active schemer process)")
            state.pipeline_active = False
            try:
                await self.backend.state_service.save_project(project_path=Path(project_path), force=True)
            except Exception as e:
                print(f"[SYNC] Could not persist pipeline_active reset: {e}")

        # Build reverse lookup: filesystem path (stripped) -> instance_id.
        # Three passes in descending priority. Lower-priority passes must NOT
        # overwrite entries already written by a higher-priority pass.

        path_to_instance: Dict[str, str] = {}

        # Pass 1 (highest): relion_job_name -- set when the job actually ran.
        # This is the only authoritative source once a job has executed.
        for iid, model in state.jobs.items():
            rjn = getattr(model, "relion_job_name", None)
            if rjn:
                path_to_instance[rjn.rstrip("/")] = iid

        # Pass 2: job_path_mapping -- written by previous sync runs,
        # consistent with relion_job_name. Don't overwrite pass 1 entries.
        for iid in state.jobs:
            mapped = (state.job_path_mapping or {}).get(iid)
            if mapped:
                key = mapped.rstrip("/")
                if key not in path_to_instance:
                    path_to_instance[key] = iid

        # Pass 3 (lowest): paths["job_dir"] -- predicted at deploy time.
        # Only use for instances that have no relion_job_name (genuinely pending).
        # Stale predicted paths from previous deploys must not overwrite
        # completed job mappings, which is why this is pass 3 and we skip
        # instances that already have a relion_job_name.
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

        # Count instances per job type -- needed for safe fallback below.
        type_instance_count: Dict[str, int] = {}
        for iid in state.jobs:
            base = iid.split("__")[0]
            type_instance_count[base] = type_instance_count.get(base, 0) + 1

        found_instances: set = set()

        for _, row in processes.iterrows():
            job_path = row["rlnPipeLineProcessName"]
            job_path_clean = job_path.rstrip("/")

            instance_id = path_to_instance.get(job_path_clean)

            # Fallback: derive instance_id from the driver script name.
            # Only safe when exactly ONE instance of that job type exists --
            # with multiple instances we cannot know which one owns this path,
            # and guessing wrong is worse than skipping.
            if instance_id is None:
                job_type_str = self.job_resolver.get_job_type_from_path(Path(project_path), job_path)
                if not job_type_str:
                    continue
                if type_instance_count.get(job_type_str, 0) == 1:
                    instance_id = job_type_str
                else:
                    # Old/zombie entry from a previous run -- no current instance owns
                    # this path anymore. Silently skip rather than warning, since this
                    # is expected when re-running jobs that previously failed.
                    continue

            if instance_id not in state.jobs:
                continue

            job_model = state.jobs[instance_id]
            old_status = job_model.execution_status

            status_str = row["rlnPipeLineProcessStatusLabel"]
            if status_str == "Pending":
                new_status = JobStatus.SCHEDULED
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

        # Jobs not found in pipeline.star: reset to Scheduled if they had a
        # stale job path (the job was deleted or superseded).
        for instance_id, job_model in state.jobs.items():
            if instance_id in found_instances:
                continue

            old_status = job_model.execution_status

            if job_model.relion_job_name:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
                state.job_path_mapping.pop(instance_id, None)
                if old_status != JobStatus.SCHEDULED:
                    changes[instance_id] = True
            elif job_model.execution_status not in (JobStatus.SCHEDULED, JobStatus.UNKNOWN):
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None
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
        except:
            return 0

    def _find_job_in_star(self, processes: pd.DataFrame, job_type: str, project_path: str):
        if processes.empty:
            return None
        for _, row in processes.iterrows():
            job_path = row["rlnPipeLineProcessName"]
            detected_type = self._extract_job_type(project_path, job_path)
            if detected_type == job_type:
                return row
        return None

    def _extract_job_type(self, project_path: str, job_path: str) -> str:
        result = self.job_resolver.get_job_type_from_path(Path(project_path), job_path)
        return result if result else "unknown"

    async def stop_and_cleanup(self, project_dir: Path, slurm_job_ids: List[str]) -> Dict[str, Any]:
        """
        Full stop sequence:
        1. Terminate the schemer process (stops new submissions)
        2. scancel any live SLURM jobs
        3. Patch default_pipeline.star: Running/Pending -> Failed
        4. Update in-memory job models to FAILED
        5. Set pipeline_active=False and persist

        Note: _monitor_schemer's finally block will also fire after the process
        exits and will redundantly save state -- that is harmless.
        """
        errors = []

        # 1. Terminate schemer
        if self.active_schemer_process:
            pid = self.active_schemer_process.pid
            print(f"[RUNNER] Stopping schemer PID {pid}")
            try:
                self.active_schemer_process.terminate()
                try:
                    await asyncio.wait_for(self.active_schemer_process.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    print(f"[RUNNER] Schemer didn't respond to SIGTERM, sending SIGKILL")
                    self.active_schemer_process.kill()
                    await self.active_schemer_process.wait()
            except Exception as e:
                errors.append(f"schemer termination: {e}")

        # 2. scancel SLURM jobs
        if slurm_job_ids:
            result = await self.backend.slurm_service.scancel_jobs(slurm_job_ids)
            if not result["success"]:
                errors.append(f"scancel: {result.get('error')}")

        # 3. Patch default_pipeline.star
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

        # 4. Update in-memory state
        state = self.backend.state_service.state_for(project_dir)
        for job_model in state.jobs.values():
            if job_model.execution_status in (JobStatus.RUNNING, JobStatus.SCHEDULED):
                job_model.execution_status = JobStatus.FAILED

        # 5. Persist
        state.pipeline_active = False
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        if errors:
            print(f"[RUNNER] Stop completed with non-fatal errors: {errors}")
            return {"success": False, "errors": errors}
        return {"success": True, "cancelled_slurm_jobs": len(slurm_job_ids)}


class PipelineRunnerService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.active_schemer_process: Optional[asyncio.subprocess.Process] = None
        self.status_sync = StatusSyncService(backend_instance)

        self._stdout_log_path: Optional[Path] = None
        self._stderr_log_path: Optional[Path] = None

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]
    ):
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}

        bind_paths = {str(Path(p).parent.resolve()) for p in required_paths if p}
        bind_paths.add(str(project_dir.parent.resolve()))

        return await self._run_relion_schemer(project_dir, scheme_name, additional_bind_paths=list(bind_paths))

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

    async def _run_relion_schemer(
        self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]
    ) -> Dict[str, Any]:
        try:
            state = self.backend.state_service.state_for(project_dir)

            # Secondary guard -- deploy_and_run_scheme now checks this first,
            # but keep it here as a safety net for any other callers.
            if state.pipeline_active:
                return {
                    "success": False,
                    "error": "Pipeline is already running. Wait for it to complete or restart the server.",
                }

            pipeline_star = project_dir / "default_pipeline.star"
            if not pipeline_star.exists():
                print(f"[RUNNER] default_pipeline.star not found, initializing Relion project...")

                init_command = "unset DISPLAY && relion --tomo --do_projdir ."
                container_service = self.backend.container_service
                init_full_command = container_service.wrap_command_for_tool(
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

            self._stdout_log_path = stdout_log
            self._stderr_log_path = stderr_log

            scheme_control_dir = f"Schemes/{scheme_name}/"
            run_command = (
                f"unset DISPLAY && relion_schemer --scheme {scheme_name} "
                f"--run --pipeline_control {scheme_control_dir} --verb 2"
            )

            container_service = self.backend.container_service
            full_run_command = container_service.wrap_command_for_tool(
                command=run_command, cwd=project_dir, tool_name="relion_schemer", additional_binds=additional_bind_paths
            )

            print(f"[RUNNER] Starting schemer, logging to {scheme_log_dir}")
            print(f"[RUNNER] Command: {run_command}")

            stdout_handle = open(stdout_log, "w")
            stderr_handle = open(stderr_log, "w")

            process = await asyncio.create_subprocess_shell(
                full_run_command, stdout=stdout_handle, stderr=stderr_handle, cwd=project_dir
            )

            self.active_schemer_process = process

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

            if self.active_schemer_process is process:
                self.active_schemer_process = None
                self._stdout_log_path = None
                self._stderr_log_path = None

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

    async def reset_submission_failure(self, project_dir: Path):
        """
        After sbatch rejects a submission, relion_schemer has already written
        Running into default_pipeline.star. Patch it to Failed so sync_all_jobs
        reads the correct state instead of re-asserting Running on every reload.
        Also resets in-memory job models and persists.
        """
        from services.project_state import JobStatus

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

    async def run_generated_scheme(self, project_dir: Path, scheme_name: str, bind_paths: List[str]) -> Dict[str, Any]:
        return await self._run_relion_schemer(
            project_dir=project_dir, scheme_name=scheme_name, additional_bind_paths=bind_paths
        )

    def get_sbatch_errors(self) -> List[str]:
        if not self._stderr_log_path or not self._stderr_log_path.exists():
            return []
        errors = []
        try:
            with open(self._stderr_log_path, "r") as f:
                for line in f:
                    if "sbatch: error:" in line:
                        errors.append(line.strip())
        except Exception:
            pass
        return errors

    def get_schemer_logs(self) -> Dict[str, str]:
        logs = {"stdout": "", "stderr": "", "running": self.active_schemer_process is not None}

        if self._stdout_log_path and self._stdout_log_path.exists():
            try:
                with open(self._stdout_log_path, "r") as f:
                    logs["stdout"] = f.read()
            except Exception as e:
                logs["stdout"] = f"Error reading log: {e}"

        if self._stderr_log_path and self._stderr_log_path.exists():
            try:
                with open(self._stderr_log_path, "r") as f:
                    logs["stderr"] = f.read()
            except Exception as e:
                logs["stderr"] = f"Error reading log: {e}"

        return logs

    async def stop_pipeline(self) -> Dict[str, Any]:
        if not self.active_schemer_process:
            return {"success": False, "error": "No pipeline is running"}

        pid = self.active_schemer_process.pid
        print(f"[RUNNER] Stopping schemer PID {pid}")

        try:
            self.active_schemer_process.terminate()

            try:
                await asyncio.wait_for(self.active_schemer_process.wait(), timeout=10.0)
                return {"success": True, "message": f"Pipeline stopped (PID {pid})"}
            except asyncio.TimeoutError:
                print(f"[RUNNER] Schemer didn't respond to SIGTERM, sending SIGKILL")
                self.active_schemer_process.kill()
                await self.active_schemer_process.wait()
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

        # 1. Terminate schemer
        if self.active_schemer_process:
            pid = self.active_schemer_process.pid
            print(f"[RUNNER] Stopping schemer PID {pid}")
            try:
                self.active_schemer_process.terminate()
                try:
                    await asyncio.wait_for(self.active_schemer_process.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    print(f"[RUNNER] Schemer didn't respond to SIGTERM, sending SIGKILL")
                    self.active_schemer_process.kill()
                    await self.active_schemer_process.wait()
            except Exception as e:
                errors.append(f"schemer termination: {e}")

        # 2. scancel SLURM jobs
        if slurm_job_ids:
            result = await self.backend.slurm_service.scancel_jobs(slurm_job_ids)
            if not result["success"]:
                errors.append(f"scancel: {result.get('error')}")

        # 3. Patch default_pipeline.star
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

        # 4. Update in-memory job models
        state = self.backend.state_service.state_for(project_dir)
        for job_model in state.jobs.values():
            if job_model.execution_status in (JobStatus.RUNNING, JobStatus.SCHEDULED):
                job_model.execution_status = JobStatus.FAILED

        # 5. Persist
        state.pipeline_active = False
        await self.backend.state_service.save_project(project_path=project_dir, force=True)

        if errors:
            print(f"[RUNNER] Stop completed with non-fatal errors: {errors}")
            return {"success": False, "errors": errors}
        return {"success": True, "cancelled_slurm_jobs": len(slurm_job_ids)}

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

        # Patch default_pipeline.star for this specific job
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

        # Terminate the schemer -- it would stall on this failed job anyway.
        # _monitor_schemer's finally block will set pipeline_active=False and save.
        # We also set it here immediately so the UI can unlock without waiting.
        if self.active_schemer_process:
            try:
                self.active_schemer_process.terminate()
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
