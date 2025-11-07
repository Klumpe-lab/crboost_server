# services/pipeline_runner.py
import asyncio
import json
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, AsyncGenerator
from typing import TYPE_CHECKING

# NEW: Import JobStatus from parameter_models
from app_state import prepare_job_params
from services.parameter_models import AbstractJobParams, JobStatus

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class PipelineRunnerService:
    """
    Handles the execution and monitoring of Relion pipelines.
    """

    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.active_schemer_process: asyncio.subprocess.Process | None = None

    async def start_pipeline(
        self,
        project_path: str,
        scheme_name: str,
        selected_jobs: List[str],
        required_paths: List[str],
    ):
        """
        Validates paths and starts the relion_schemer process.
        (Unchanged)
        """
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {
                "success": False,
                "error": f"Project path not found: {project_path}",
            }

        # Collect bind paths
        bind_paths = {str(Path(p).parent.resolve()) for p in required_paths if p}
        bind_paths.add(str(project_dir.parent.resolve()))

        # Call internal method to run the schemer
        return await self._run_relion_schemer(
            project_dir, scheme_name, additional_bind_paths=list(bind_paths)
        )

    async def get_pipeline_job_statuses(self, project_path: str) -> Dict[str, AbstractJobParams]:
        """
        Get actual execution status for each job from default_pipeline.star
        and update the job models directly.
        
        Returns: 
        { 
            "importmovies": ImportMoviesParams(execution_status=JobStatus.SUCCEEDED, ...),
            "fsMotionAndCtf": FsMotionCtfParams(execution_status=JobStatus.RUNNING, ...)
        }
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

            for _, process in processes.iterrows():
                job_path = process["rlnPipeLineProcessName"]
                status_str = process["rlnPipeLineProcessStatusLabel"]
                
                job_type = self._extract_job_type_from_path(project_path, job_path)
                
                if job_type and job_type != "unknown":
                    try:
                        # Get the existing job model from app state
                        # In the loop where we get job models:
                        job_model = self.backend.app_state.jobs.get(job_type)
                        if not job_model:
                            # Try to load the job model from project params
                            print(f"[RUNNER] Job model not in state, loading from project params: {job_type}")
                            job_model = prepare_job_params(job_type)
                            if not job_model:
                                print(f"[RUNNER] Failed to load job model for {job_type}, skipping")
                                continue

                        job_number_str = job_path.rstrip('/').split('job')[-1]
                        job_number = int(job_number_str)
                        
                        # Map Relion's "Pending" to our "Scheduled"
                        if status_str == "Pending":
                            status_enum = JobStatus.SCHEDULED
                        else:
                            try:
                                status_enum = JobStatus(status_str)
                            except ValueError:
                                status_enum = JobStatus.UNKNOWN
                        
                        # Update the job model IN-PLACE
                        job_model.execution_status = status_enum
                        job_model.relion_job_name = job_path
                        job_model.relion_job_number = job_number
                        
                        updated_jobs[job_type] = job_model
                        
                    except ValueError as e:
                        print(f"[RUNNER] Error processing job {job_type}: {e}")
            
            return updated_jobs
            
        except Exception as e:
            print(f"[RUNNER_SERVICE] Error reading pipeline status: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return {}

    def _extract_job_type_from_path(self, project_path: str, job_path: str) -> str:
        """Extract job type from job path using job_params.json."""
        
        # Handle "Import" job, which is a special case
        if "Import/job" in job_path:
            return "importmovies"
        
        # For all other jobs, read the job_params.json
        job_dir = Path(project_path) / job_path.rstrip('/')
        params_file = job_dir / "job_params.json"
        
        if params_file.exists():
            try:
                with open(params_file, 'r') as f:
                    params_data = json.load(f)
                return params_data.get("job_type", "unknown")
            except Exception:
                return "unknown" # Fallback on read error
        else:
            # This can happen if the job hasn't been created yet but is in the .star
            return "unknown" 

    async def get_pipeline_overview(self, project_path: str):
        """
        Reads the default_pipeline.star file to get a high-level overview
        and detailed job statuses.
        """
        # This should return job models from get_pipeline_job_statuses
        job_models = await self.get_pipeline_job_statuses(project_path)
        
        if not job_models:
            return {
                "status": "ok", "total": 0, "completed": 0,
                "running": 0, "failed": 0, "scheduled": 0, "is_complete": True,
                "jobs": {}
            }

        total = len(job_models)
        succeeded = 0
        running = 0
        failed = 0
        scheduled = 0

        # Count statuses from job models
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
            "jobs": job_models  # Pass the job models directly
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
    ):
        # (This method is unchanged)
        try:
            run_command = (
                f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            )
            container_service = self.backend.container_service
            full_run_command = container_service.wrap_command_for_tool(
                command=run_command,
                cwd=project_dir,
                tool_name="relion_schemer",
                additional_binds=additional_bind_paths,
            )
            process = await asyncio.create_subprocess_shell(
                full_run_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir,
            )
            self.active_schemer_process = process
            asyncio.create_task(self._monitor_schemer(process, project_dir))
            return {
                "success": True,
                "message": f"Workflow started (PID: {process.pid})",
                "pid": process.pid,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _monitor_schemer(
        self, process: asyncio.subprocess.Process, project_dir: Path
    ):
        # (This method is unchanged)
        async def read_stream(stream, callback):
            while True:
                line = await stream.readline()
                if not line:
                    break
                callback(line.decode().strip())
        def handle_stdout(line):
            print(f"[SCHEMER] {line}")
        def handle_stderr(line):
            print(f"[SCHEMER-ERR] {line}")
        await asyncio.gather(
            read_stream(process.stdout, handle_stdout),
            read_stream(process.stderr, handle_stderr),
        )
        await process.wait()
        print(
            f"[MONITOR] relion_schemer PID {process.pid} completed with return code: {process.returncode}"
        )
        self.active_schemer_process = None