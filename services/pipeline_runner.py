# services/pipeline_runner_service.py
import asyncio
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, AsyncGenerator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class PipelineRunnerService:
    """
    Handles the execution and monitoring of Relion pipelines.
    """

    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance  # Used to access container_service, etc.
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
        (Logic moved from backend.py)
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

    async def get_pipeline_progress(self, project_path: str):
        """
        Reads the default_pipeline.star file to get job statuses.
        (Logic moved from backend.py)
        """
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {"status": "not_found"}

        try:
            # Access star_handler via the backend instance
            star_handler = self.backend.pipeline_orchestrator.star_handler
            data = star_handler.read(pipeline_star)
            processes = data.get("pipeline_processes", pd.DataFrame())

            if processes.empty:
                return {
                    "status": "ok",
                    "total": 0,
                    "completed": 0,
                    "running": 0,
                    "failed": 0,
                    "is_complete": True,
                }

            total = len(processes)
            succeeded = (
                processes["rlnPipeLineProcessStatusLabel"] == "Succeeded"
            ).sum()
            running = (processes["rlnPipeLineProcessStatusLabel"] == "Running").sum()
            failed = (processes["rlnPipeLineProcessStatusLabel"] == "Failed").sum()

            is_complete = running == 0 and total > 0

            return {
                "status": "ok",
                "total": total,
                "completed": int(succeeded),
                "running": int(running),
                "failed": int(failed),
                "is_complete": is_complete,
            }
        except Exception as e:
            print(f"[RUNNER_SERVICE] Error reading pipeline progress for {project_path}: {e}")
            return {"status": "error", "message": str(e)}

    async def get_pipeline_job_logs(
        self, project_path: str, job_type: str, job_number: str
    ) -> Dict[str, str]:
        """
        Get the run.out and run.err contents for a specific pipeline job.
        (Logic moved from backend.py)
        """
        project_dir = Path(project_path)

        # TODO: This mapping should come from the parameter_models (JobCategory)
        job_dir_map = {
            "importmovies": "Import",
            "fsMotionAndCtf": "External",
            "tsAlignment": "External",
        }

        job_dir_name = job_dir_map.get(job_type, "External")
        job_path = project_dir / job_dir_name / f"job{job_number.zfill(3)}"

        logs = {"stdout": "", "stderr": "", "exists": False, "path": str(job_path)}

        if not job_path.exists():
            return logs

        logs["exists"] = True

        out_file = job_path / "run.out"
        if out_file.exists():
            try:
                with open(out_file, "r", encoding="utf-8") as f:
                    logs["stdout"] = f.read()
            except Exception as e:
                logs["stdout"] = f"Error reading run.out: {e}"

        err_file = job_path / "run.err"
        if err_file.exists():
            try:
                with open(err_file, "r", encoding="utf-8") as f:
                    logs["stderr"] = f.read()
            except Exception as e:
                logs["stderr"] = f"Error reading run.err: {e}"

        return logs

    async def monitor_pipeline_jobs(
        self, project_path: str, selected_jobs: List[str]
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """
        Monitor all pipeline jobs and yield updates.
        (Logic moved from backend.py)
        """
        while True:
            job_statuses = []
            for idx, job_type in enumerate(selected_jobs, 1):
                # Call own method
                logs = await self.get_pipeline_job_logs(
                    project_path, job_type, str(idx)
                )
                job_statuses.append(
                    {"job_type": job_type, "job_number": idx, "logs": logs}
                )
            yield job_statuses
            await asyncio.sleep(5)

    async def _run_relion_schemer(
        self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]
    ):
        """
        Run relion_schemer to execute the pipeline scheme.
        (Logic moved from backend.py)
        """
        try:
            run_command = (
                f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            )

            # Access container_service via the backend instance
            container_svc = self.backend.container_service
            full_run_command = container_svc.wrap_command_for_tool(
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
        """
        Monitor the relion_schemer process.
        (Logic moved from backend.py)
        """

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