# backend.py
from __future__ import annotations
import asyncio
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List
import pandas as pd
from services.parameter_models import JobType
from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService
from services.container_service import get_container_service
from services.pipeline_runner import PipelineRunnerService

from app_state import state as app_state, prepare_job_params, update_from_mdoc, get_ui_state_legacy

from pydantic import BaseModel
from pathlib import Path


class User(BaseModel):
    """Represents an authenticated user."""

    username: str


HARDCODED_USER = User(username="artem.kushner")


class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()

        # --- NEW: Initialize the runner service ---
        self.pipeline_runner = PipelineRunnerService(self)

        # Store a reference to global state (for services that need it)
        self.app_state = app_state
        print(f"[BACKEND] Initialized with state reference")

    async def get_job_parameters(self, job_name: str) -> Dict[str, Any]:
        """Get parameters for a specific job"""
        try:
            # Convert string to enum safely
            job_type = JobType.from_string(job_name)
            job_model = prepare_job_params(job_type)  # Updated signature

            if job_model:
                return {"success": True, "params": job_model.model_dump()}
            else:
                return {"success": False, "error": f"Unknown job type {job_name}"}
        except ValueError as e:
            return {"success": False, "error": str(e)}

    # SUS
    async def get_available_jobs(self) -> List[str]:
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    async def create_project_and_scheme(
        self, project_name: str, project_base_path: str, selected_jobs: List[str], movies_glob: str, mdocs_glob: str
    ):
        """
        Delegates project creation and initialization to the ProjectService.
        """
        return await self.project_service.initialize_new_project(
            project_name=project_name,
            project_base_path=project_base_path,
            selected_jobs=selected_jobs,
            movies_glob=movies_glob,
            mdocs_glob=mdocs_glob,
        )

    async def get_initial_parameters(self) -> Dict[str, Any]:
        """Get the default parameters to populate the UI"""
        return get_ui_state_legacy()

    async def autodetect_parameters(self, mdocs_glob: str) -> Dict[str, Any]:
        """Run mdoc autodetection and return the updated state"""
        print(f"[BACKEND] Autodetecting from {mdocs_glob}")

        update_from_mdoc(mdocs_glob)
        return get_ui_state_legacy()

    async def run_shell_command(
        self, command: str, cwd: Path = None, tool_name: str = None, additional_binds: List[str] = None
    ):
        """Runs a shell command, optionally using specified tool's container."""
        try:
            if tool_name:
                print(f"[DEBUG] Running command with tool: {tool_name}")
                final_command = self.container_service.wrap_command_for_tool(
                    command=command,
                    cwd=cwd or self.server_dir,
                    tool_name=tool_name,
                    additional_binds=additional_binds or [],
                )
            else:
                final_command = command
                print(f"[SHELL] Running natively: {final_command}")

            process = await asyncio.create_subprocess_shell(
                final_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd or self.server_dir,
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120.0)

                print(f"[DEBUG] Process completed with return code: {process.returncode}")
                if process.returncode == 0:
                    return {"success": True, "output": stdout.decode(), "error": None}
                else:
                    return {"success": False, "output": stdout.decode(), "error": stderr.decode()}

            except asyncio.TimeoutError:
                print(f"[ERROR] Command timed out after 120 seconds: {final_command}")
                process.terminate()
                await process.wait()
                return {"success": False, "output": "", "error": "Command execution timed out"}

        except Exception as e:
            print(f"[ERROR] Exception in run_shell_command: {e}")
            return {"success": False, "output": "", "error": str(e)}

    async def get_slurm_info(self):
        return await self.run_shell_command("sinfo")

    # -----------------------------------------------------------------
    # --- All pipeline methods are now delegated to PipelineRunnerService ---
    # -----------------------------------------------------------------

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]
    ):
        return await self.pipeline_runner.start_pipeline(project_path, scheme_name, selected_jobs, required_paths)

    async def get_pipeline_progress(self, project_path: str):
        return await self.pipeline_runner.get_pipeline_progress(project_path)

    async def get_pipeline_job_logs(self, project_path: str, job_type: str, job_number: str) -> Dict[str, str]:
        return await self.pipeline_runner.get_pipeline_job_logs(project_path, job_type, job_number)

    async def monitor_pipeline_jobs(self, project_path: str, selected_jobs: List[str]) -> AsyncGenerator:
        """Monitor all pipeline jobs and yield updates"""
        # We must iterate over the async generator from the service
        async for update in self.pipeline_runner.monitor_pipeline_jobs(project_path, selected_jobs):
            yield update

    # -----------------------------------------------------------------
    # --- Internal helper methods are GONE ---
    # -----------------------------------------------------------------
    # async def _run_relion_schemer(...):  <-- DELETED
    # async def _monitor_schemer(...):     <-- DELETED

    async def get_eer_frames_per_tilt(self, eer_file_path: str) -> int:
        """Extract number of frames per tilt from EER file"""
        # This is a general utility, not pipeline-specific, so it can stay here.
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
            return None
        except Exception as e:
            print(f"Error getting EER frames: {e}")
            return None
