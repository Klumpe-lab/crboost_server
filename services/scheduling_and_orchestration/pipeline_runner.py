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
        """
        Read pipeline.star and update job models. Returns dict of what changed.
        """
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {}

        star_handler = self.backend.pipeline_orchestrator.star_handler
        data = star_handler.read(pipeline_star)
        processes = data.get("pipeline_processes", pd.DataFrame())

        changes: Dict[str, bool] = {}
        state = self.backend.state_service.state_for(Path(project_path))

        found_jobs = set()
        for _, row in processes.iterrows():
            job_path = row["rlnPipeLineProcessName"]
            job_type_str = self.job_resolver.get_job_type_from_path(Path(project_path), job_path)

            if not job_type_str:
                continue

            try:
                job_type = JobType(job_type_str)
            except ValueError:
                continue

            if job_type in state.jobs:
                job_model = state.jobs[job_type]
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

                state.job_path_mapping[job_type.value] = job_path

                if old_status != job_model.execution_status:
                    changes[job_type_str] = True

                found_jobs.add(job_type)

        for job_type, job_model in state.jobs.items():
            if job_type not in found_jobs:
                old_status = job_model.execution_status

                if job_model.relion_job_name:
                    job_model.execution_status = JobStatus.SCHEDULED
                    job_model.relion_job_name = None
                    job_model.relion_job_number = None
                    state.job_path_mapping.pop(job_type.value, None)

                    if old_status != JobStatus.SCHEDULED:
                        changes[job_type.value] = True

                elif job_model.execution_status not in [JobStatus.SCHEDULED, JobStatus.UNKNOWN]:
                    job_model.execution_status = JobStatus.SCHEDULED
                    job_model.relion_job_name = None
                    job_model.relion_job_number = None
                    state.job_path_mapping.pop(job_type.value, None)
                    changes[job_type.value] = True

        if changes:
            try:
                print(f"[SYNC] Persisting {len(changes)} status changes to disk.")
                # CHANGED: explicit project_path -- sync runs from timer callbacks
                # where tab context *usually* exists, but being explicit is safer.
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
        """Use resolver instead of hardcoded mapping."""
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

        # 2. Scancel SLURM jobs
        if slurm_job_ids:
            result = await self.backend.slurm_service.scancel_jobs(slurm_job_ids)
            if not result["success"]:
                # Non-fatal: log but continue cleanup
                errors.append(f"scancel: {result.get('error')}")

        # 3. Patch default_pipeline.star
        pipeline_star = project_dir / "default_pipeline.star"
        if pipeline_star.exists():
            star_handler = self.backend.pipeline_orchestrator.star_handler
            try:
                data = star_handler.read(pipeline_star)
                processes = data.get("pipeline_processes", pd.DataFrame())
                if not processes.empty and "rlnPipeLineProcessStatusLabel" in processes.columns:
                    processes["rlnPipeLineProcessStatusLabel"] = processes[
                        "rlnPipeLineProcessStatusLabel"
                    ].replace({"Running": "Failed", "Pending": "Failed"})
                    data["pipeline_processes"] = processes
                    star_handler.write(data, pipeline_star)
            except Exception as e:
                errors.append(f"pipeline star patch: {e}")

        # 4. Update in-memory state
        # Keep relion_job_name/number so logs remain accessible
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

        # Track log file handles for active schemer
        self._stdout_log_path: Optional[Path] = None
        self._stderr_log_path: Optional[Path] = None


    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]
    ):
        """
        Validates paths and starts the relion_schemer process.
        """
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}

        # Collect bind paths
        bind_paths = {str(Path(p).parent.resolve()) for p in required_paths if p}
        bind_paths.add(str(project_dir.parent.resolve()))

        # Call internal method to run the schemer
        return await self._run_relion_schemer(project_dir, scheme_name, additional_bind_paths=list(bind_paths))

    async def get_pipeline_job_statuses(self, project_path: str) -> Dict[str, AbstractJobParams]:
        """
        Get actual execution status for each job from default_pipeline.star
        and update the job models directly.
        """
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {}

        try:
            star_handler = self.backend.pipeline_orchestrator.star_handler
            data = star_handler.read(pipeline_star)

            processes = data.get("pipeline_processes", pd.DataFrame())

            updated_jobs = {}
            if processes.empty:
                return {}

            # CHANGED: explicit path (no tab context dependency)
            state = self.backend.state_service.state_for(Path(project_path))
            server_root = self.backend.config_service.crboost_root

            for _, process in processes.iterrows():
                job_path = process["rlnPipeLineProcessName"]
                status_str = process["rlnPipeLineProcessStatusLabel"]
                job_type_str = self.status_sync._extract_job_type(project_path, job_path)

                if job_type_str and job_type_str != "unknown":
                    try:
                        job_type = JobType(job_type_str)
                        job_model = state.jobs.get(job_type)

                        if not job_model:
                            print(f"[RUNNER] Job {job_type} not in state, initializing from template.")
                            template_base = server_root / "config" / "Schemes" / "warp_tomo_prep"
                            job_star_path = template_base / job_type.value / "job.star"

                            # CHANGED: call ensure_job_initialized directly on the
                            # state object we already have (no tab context needed)
                            state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)
                            job_model = state.jobs.get(job_type)

                            if not job_model:
                                print(f"[RUNNER] Failed to initialize job model for {job_type}, skipping")
                                continue

                        job_number_str = job_path.rstrip("/").split("job")[-1]
                        job_number = int(job_number_str)

                        if status_str == "Pending":
                            status_enum = JobStatus.SCHEDULED
                        else:
                            try:
                                status_enum = JobStatus(status_str)
                            except ValueError:
                                status_enum = JobStatus.UNKNOWN

                        job_model.execution_status = status_enum
                        job_model.relion_job_name = job_path
                        job_model.relion_job_number = job_number

                        updated_jobs[job_type.value] = job_model

                    except ValueError as e:
                        print(f"[RUNNER] Error processing job {job_type_str}: {e}")

            return updated_jobs

        except Exception as e:
            print(f"[RUNNER_SERVICE] Error reading pipeline status: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc(file=sys.stderr)
            return {}

    async def get_pipeline_overview(self, project_path: str):
        """
        Reads the default_pipeline.star file to get a high-level overview
        and detailed job statuses.
        """
        job_models = await self.get_pipeline_job_statuses(project_path)

        if not job_models:
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

        total = len(job_models)
        succeeded = 0
        running = 0
        failed = 0
        scheduled = 0

        for job_model in job_models.values():
            status = job_model.execution_status
            if status == JobStatus.SUCCEEDED:
                succeeded += 1
            elif status == JobStatus.RUNNING:
                running += 1
            elif status == JobStatus.FAILED:
                failed += 1
            elif status == JobStatus.SCHEDULED:
                scheduled += 1

        is_complete = running == 0 and total > 0

        return {
            "status": "ok",
            "total": total,
            "completed": int(succeeded),
            "running": int(running),
            "failed": int(failed),
            "scheduled": int(scheduled),
            "is_complete": is_complete,
            "jobs": {k: v.model_dump(mode="json") for k, v in job_models.items()},
        }

    async def get_job_logs(self, project_path: str, job_name: str) -> Dict[str, str]:
        """
        Get the run.out and run.err contents for a specific job path.
        (e.g., job_name = "External/job003/")
        """
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
        """
        Start relion_schemer as a background process.

        Key design decisions:
        1. Log to files instead of pipes to prevent buffer deadlock
        2. Mark pipeline_active=True BEFORE starting to protect against state resets
        3. Monitor task cleans up state on exit regardless of success/failure
        """
        try:
            # CHANGED: explicit path (this may be called from asyncio.create_task
            # where tab context is not guaranteed)
            state = self.backend.state_service.state_for(project_dir)

            # GUARD: Don't start if already running
            if state.pipeline_active:
                return {
                    "success": False,
                    "error": "Pipeline is already running. Wait for it to complete or restart the server.",
                }

            # ENSURE default_pipeline.star exists (Relion project init)
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

            # MARK PIPELINE AS ACTIVE - this protects against state resets
            state.pipeline_active = True
            # CHANGED: explicit project_path + force
            await self.backend.state_service.save_project(project_path=project_dir, force=True)
            print(f"[RUNNER] Pipeline marked active, state protected from resets")

            # Setup log files (prevents pipe buffer deadlock)
            scheme_log_dir = project_dir / "Schemes" / scheme_name
            scheme_log_dir.mkdir(parents=True, exist_ok=True)

            stdout_log = scheme_log_dir / "schemer.out"
            stderr_log = scheme_log_dir / "schemer.err"

            self._stdout_log_path = stdout_log
            self._stderr_log_path = stderr_log

            # Build and run the schemer command
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

            # Open log files and start process
            stdout_handle = open(stdout_log, "w")
            stderr_handle = open(stderr_log, "w")

            process = await asyncio.create_subprocess_shell(
                full_run_command, stdout=stdout_handle, stderr=stderr_handle, cwd=project_dir
            )

            self.active_schemer_process = process

            # Start monitor task - passes project_dir for registry lookup
            # This task is NOT tied to any NiceGUI client
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

            # On failure, ensure we don't leave pipeline_active=True
            try:
                # CHANGED: explicit path
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
        # CHANGED: removed `state` parameter -- we re-fetch from registry
        # in the finally block anyway (in case it was reloaded during execution).
        stdout_handle,
        stderr_handle,
        stdout_log: Path,
        stderr_log: Path,
    ):
        """
        Monitor the schemer process and clean up when it exits.

        This runs as a background asyncio task, completely independent of NiceGUI clients.
        It will continue running even if all browser tabs are closed.
        """
        pid = process.pid
        print(f"[MONITOR] Started monitoring schemer PID {pid}")

        try:
            # Wait for process to complete
            return_code = await process.wait()

            print(f"[MONITOR] Schemer PID {pid} exited with code: {return_code}")

            # Log final status
            if return_code == 0:
                print(f"[MONITOR] Pipeline completed successfully")
            else:
                print(f"[MONITOR] Pipeline failed or was interrupted (code {return_code})")
                # Dump last few lines of stderr for debugging
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
            # Try to terminate the process if monitor is cancelled
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

            # Only clobber shared state if we're still the active schemer.
            # If a new run started between our termination and this finally block
            # firing, leave it alone.
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
                print(f"[MONITOR] Old schemer PID {pid} cleaned up, new pipeline already running -- skipping state reset")



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
                    processes["rlnPipeLineProcessStatusLabel"] = processes[
                        "rlnPipeLineProcessStatusLabel"
                    ].replace("Running", "Failed")
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
        """
        Runs a scheme that has already been generated by the Orchestrator.
        This is the main entry point called by the orchestrator.
        """
        return await self._run_relion_schemer(
            project_dir=project_dir, scheme_name=scheme_name, additional_bind_paths=bind_paths
        )
    def get_sbatch_errors(self) -> List[str]:
        """
        Scan the active schemer's stderr log for sbatch submission errors.
        Returns list of error lines, empty if none found or no log available.
        """
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
        """
        Get current schemer log contents (for debugging/UI display).
        """
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
        """
        Attempt to stop the running schemer process gracefully.
        """
        if not self.active_schemer_process:
            return {"success": False, "error": "No pipeline is running"}

        pid = self.active_schemer_process.pid
        print(f"[RUNNER] Stopping schemer PID {pid}")

        try:
            # Try graceful termination first
            self.active_schemer_process.terminate()

            try:
                await asyncio.wait_for(self.active_schemer_process.wait(), timeout=10.0)
                return {"success": True, "message": f"Pipeline stopped (PID {pid})"}
            except asyncio.TimeoutError:
                # Force kill if it doesn't respond
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
                    processes["rlnPipeLineProcessStatusLabel"] = processes[
                        "rlnPipeLineProcessStatusLabel"
                    ].replace({"Running": "Failed", "Pending": "Failed"})
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

    async def cancel_job(self, project_dir: Path, job_type: "JobType") -> Dict[str, Any]:
        """
        Cancel a single running job:
        1. Find its SLURM job ID by matching working directory and scancel it
        2. Patch that job's row in default_pipeline.star to Failed
        3. Update in-memory job model to FAILED
        4. Terminate the schemer -- it would stall on the failed job anyway,
            and we want pipeline_active cleared so the user can requeue
        5. Persist
        """
        from services.project_state import JobStatus

        state = self.backend.state_service.state_for(project_dir)
        job_model = state.jobs.get(job_type)

        if not job_model:
            return {"success": False, "error": f"Job {job_type.value} not found in state"}

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

        # Update in-memory model
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