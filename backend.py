from __future__ import annotations
import asyncio
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List
import pandas as pd
import os
from datetime import datetime

# Refactored imports
from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService
from services.container_service import get_container_service
from services.pipeline_runner import PipelineRunnerService
from services.continuation_service import ContinuationService, PipelineManipulationService, SchemeManipulationService
from services.project_state import JobType, get_state_service
from services.slurm_service import SlurmService
from services.config_service import get_config_service

HARDCODED_USER = "artem.kushner"


class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.username                  = HARDCODED_USER
        self.server_dir                = server_dir
        self.project_service           = ProjectService(self)
        self.pipeline_orchestrator     = PipelineOrchestratorService(self)
        self.container_service         = get_container_service()
        self.slurm_service             = SlurmService(HARDCODED_USER)
        self.pipeline_runner           = PipelineRunnerService(self)
        self.state_service             = get_state_service()
        self.pipeline_manipulation     = PipelineManipulationService(self)
        self.scheme_manipulation       = SchemeManipulationService(self)
        self.continuation              = ContinuationService(self)

    async def get_default_data_globs(self) -> Dict[str, str]:
            """Get default glob patterns from config."""
            config_service = get_config_service()
            movies, mdocs = config_service.default_data_globs
            # Return empty strings if not set, to avoid UI errors
            return {
                "movies": movies if movies else "", 
                "mdocs": mdocs if mdocs else ""
            }
    async def get_default_project_base(self) -> str:
        """Retrieves the default project base path from config."""
        config_service = get_config_service()
        configured_path = config_service.default_project_base
        if configured_path:
            return configured_path
        
        # Fallback to home if not configured, rather than hardcoded dev path
        return str(Path.home())

    async def scan_for_projects(self, base_path: str) -> List[Dict[str, Any]]:
        """
        Scans for subdirectories containing project_params.json.
        """
        projects = []
        path = Path(base_path)
        
        print(f"[SCANNER] Scanning directory: {path}")

        if not path.exists():
            print(f"[SCANNER] Path does not exist: {path}")
            return []
        if not path.is_dir():
            print(f"[SCANNER] Path is not a directory: {path}")
            return []

        try:
            for item in path.iterdir():
                if item.is_dir():
                    # Check for our specific state file
                    params_file = item / "project_params.json"
                    
                    if params_file.exists():
                        try:
                            stats = params_file.stat()
                            mod_time = datetime.fromtimestamp(stats.st_mtime)
                            projects.append({
                                "name": item.name,
                                "path": str(item),
                                "modified": mod_time.strftime("%Y-%m-%d %H:%M"),
                                "modified_timestamp": stats.st_mtime
                            })
                        except Exception as e:
                            print(f"[SCANNER] Error reading {item.name}: {e}")
                    else:
                        # Log why we skipped this folder (useful for debugging your older projects)
                        # We only check for hidden folders to reduce spam
                        if not item.name.startswith('.'):
                            print(f"[SCANNER] Skipped '{item.name}' - missing project_params.json")

        except Exception as e:
            print(f"[BACKEND] Error scanning projects: {e}")
            return []

        # Sort by modification time (newest first)
        projects.sort(key=lambda x: x["modified_timestamp"], reverse=True)
        print(f"[SCANNER] Found {len(projects)} valid projects.")
        return projects

    async def get_job_parameters(self, job_name: str) -> Dict[str, Any]:
        """Get parameters for a specific job, initializing if not present."""
        try:
            job_type = JobType.from_string(job_name)
            state = self.state_service.state

            job_model = state.jobs.get(job_type)
            if not job_model:
                print(f"[BACKEND] Job {job_type} not in state, initializing from template.")
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
                job_star_path = template_base / job_type.value / "job.star"

                await self.state_service.ensure_job_initialized(
                    job_type, job_star_path if job_star_path.exists() else None
                )
                job_model = state.jobs.get(job_type)  # Get it again

            if job_model:
                return {"success": True, "params": job_model.model_dump()}
            else:
                # This should not happen if ensure_job_initialized works
                return {"success": False, "error": f"Failed to initialize job {job_name}"}
        except ValueError as e:
            return {"success": False, "error": str(e)}

    async def update_job_parameters(self, job_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates parameters for a specific job and PERSISTS to project_params.json.
        This fixes the synchronization issue where UI changes weren't saved.
        """
        try:
            job_type = JobType.from_string(job_name)
            
            # 1. Update the in-memory state
            state = self.state_service.state
            job_model = state.jobs.get(job_type)
            
            if not job_model:
                return {"success": False, "error": f"Job {job_name} not initialized"}
            
            # Update fields dynamically
            # The __setattr__ hook in AbstractJobParams will block changes if the job is running/done
            for key, value in params.items():
                if hasattr(job_model, key):
                      setattr(job_model, key, value)
            
            # 2. PERSIST TO DISK (The missing link)
            await self.state_service.save_project()
            
            return {"success": True, "params": job_model.model_dump()}
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def get_available_jobs(self) -> List[str]:
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    async def create_project_and_scheme(
        self, project_name: str, project_base_path: str, selected_jobs: List[str], movies_glob: str, mdocs_glob: str
    ):
        return await self.project_service.initialize_new_project(
            project_name=project_name,
            project_base_path=project_base_path,
            selected_jobs=selected_jobs,
            movies_glob=movies_glob,
            mdocs_glob=mdocs_glob,
        )

    async def get_initial_parameters(self) -> Dict[str, Any]:
        """Returns a dump of the current project state."""
        return self.state_service.state.model_dump(mode="json", exclude={"project_path"})

    async def autodetect_parameters(self, mdocs_glob: str) -> Dict[str, Any]:
        """Runs mdoc update and returns the entire updated state."""
        await self.state_service.update_from_mdoc(mdocs_glob)
        # Ensure we save after autodetection
        await self.state_service.save_project()
        return self.state_service.state.model_dump(mode="json", exclude={"project_path"})

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

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]
    ):
        return await self.pipeline_runner.start_pipeline(project_path, scheme_name, selected_jobs, required_paths)

    async def get_pipeline_overview(self, project_path: str):
        """Gets a high-level overview and detailed statuses of all jobs."""
        return await self.pipeline_runner.get_pipeline_overview(project_path)

    async def get_job_logs(self, project_path: str, job_name: str) -> Dict[str, str]:
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
            return None
        except Exception as e:
            print(f"Error getting EER frames: {e}")
            return None

    async def load_existing_project(self, project_path: str) -> Dict[str, Any]:
        """Load an existing project for continuation"""
        return await self.project_service.load_project_state(project_path)

    async def debug_pipeline_status(self, project_path: str):
        """Debug method to check pipeline status directly"""
        pipeline_star = Path(project_path) / "default_pipeline.star"

        if not pipeline_star.exists():
            return {"error": "Pipeline file not found"}

        try:
            with open(pipeline_star, "r") as f:
                content = f.read()

            return {"success": True, "content": content, "exists": True}
        except Exception as e:
            return {"success": False, "error": str(e)}
