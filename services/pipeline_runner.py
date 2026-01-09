# services/pipeline_runner.py

import asyncio
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from typing import TYPE_CHECKING

from services.pipeline_orchestrator_service import JobTypeResolver
from services.project_state import AbstractJobParams, JobStatus, JobType


if TYPE_CHECKING:
    from backend import CryoBoostBackend


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
        state = self.backend.state_service.state

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
                await self.backend.state_service.save_project()
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


class PipelineRunnerService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.active_schemer_process: asyncio.subprocess.Process | None = None
        self.status_sync = StatusSyncService(backend_instance)

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

            # 1. Get the single source of truth
            state = self.backend.state_service.state

            for _, process in processes.iterrows():
                job_path = process["rlnPipeLineProcessName"]
                status_str = process["rlnPipeLineProcessStatusLabel"]

                job_type_str = self.status_sync._extract_job_type(project_path, job_path)

                if job_type_str and job_type_str != "unknown":
                    try:
                        # Convert string to Enum
                        job_type = JobType(job_type_str)

                        # 2. Get the existing job model from the state
                        job_model = state.jobs.get(job_type)

                        if not job_model:
                            print(f"[RUNNER] Job {job_type} not in state, initializing from template.")
                            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
                            job_star_path = template_base / job_type.value / "job.star"

                            await self.backend.state_service.ensure_job_initialized(
                                job_type, job_star_path if job_star_path.exists() else None
                            )

                            job_model = state.jobs.get(job_type)

                            if not job_model:
                                print(f"[RUNNER] Failed to initialize job model for {job_type}, skipping")
                                continue

                        job_number_str = job_path.rstrip("/").split("job")[-1]
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

                        updated_jobs[job_type.value] = job_model  # Use string key

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
        # This should return job models from get_pipeline_job_statuses
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

    async def _run_relion_schemer(self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]):
        try:
            # ENSURE default_pipeline.star exists
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

            # Now run the schemer
            run_command = f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            container_service = self.backend.container_service
            full_run_command = container_service.wrap_command_for_tool(
                command=run_command, cwd=project_dir, tool_name="relion_schemer", additional_binds=additional_bind_paths
            )

            process = await asyncio.create_subprocess_shell(
                full_run_command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
            )

            self.active_schemer_process = process
            asyncio.create_task(self._monitor_schemer(process, project_dir))

            return {"success": True, "message": f"Workflow started (PID: {process.pid})", "pid": process.pid}

        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def _monitor_schemer(self, process: asyncio.subprocess.Process, project_dir: Path):
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

        await asyncio.gather(read_stream(process.stdout, handle_stdout), read_stream(process.stderr, handle_stderr))
        await process.wait()
        print(f"[MONITOR] relion_schemer PID {process.pid} completed with return code: {process.returncode}")
        self.active_schemer_process = None

    async def run_generated_scheme(self, project_dir: Path, scheme_name: str, bind_paths: List[str]):
        """
        Runs a scheme that has already been generated by the Orchestrator.
        """
        # 1. Initialize Relion if needed
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            init_cmd = "unset DISPLAY && relion --tomo --do_projdir ."
            await self.backend.run_shell_command(
                init_cmd, cwd=project_dir, tool_name="relion", additional_binds=bind_paths
            )

        # 2. Run Schemer
        # --pipeline_control defines where exit markers go.
        # We point it to the scheme dir to keep root clean, OR project root if you prefer.
        # DeepWiki Q6 suggests using the scheme dir or root.
        # Let's use Scheme dir to isolate logs for this specific run.
        scheme_control_dir = f"Schemes/{scheme_name}/"

        cmd = f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --pipeline_control {scheme_control_dir} --verb 2"

        full_command = self.backend.container_service.wrap_command_for_tool(
            command=cmd, cwd=project_dir, tool_name="relion_schemer", additional_binds=bind_paths
        )

        print(f"[RUNNER] Executing: {cmd}")

        process = await asyncio.create_subprocess_shell(
            full_command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
        )

        self.active_schemer_process = process
        asyncio.create_task(self._monitor_schemer(process, project_dir))

        return {"success": True, "message": f"Pipeline execution started (PID: {process.pid})", "pid": process.pid}
