from __future__ import annotations
import asyncio
import getpass
import json
import logging
import pwd
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from services.templating.template_service import TemplateService
from services.templating.pdb_service import PDBService
from services.scheduling_and_orchestration.project_service import ProjectService
from services.scheduling_and_orchestration.pipeline_orchestrator_service import PipelineOrchestratorService
from services.computing.container_service import get_container_service
from services.scheduling_and_orchestration.pipeline_runner import PipelineRunnerService
from services.project_state import JobType, get_state_service
from services.computing.slurm_service import SlurmService
from services.configs.config_service import get_config_service

logger = logging.getLogger(__name__)


class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.username              = getpass.getuser()
        self.server_dir            = server_dir
        self.config_service        = get_config_service()
        self.project_service       = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service     = get_container_service()
        self.slurm_service         = SlurmService(self.username)
        self.pipeline_runner       = PipelineRunnerService(self)
        self.state_service         = get_state_service()
        self.template_service      = TemplateService(self)
        self.pdb_service           = PDBService(self)

    async def start_pipeline(
        self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]
    ):
        """
        Unified pipeline start method.
        selected_jobs is a list of instance_id strings (e.g. ['tsReconstruct', 'templatematching__ribosome']).
        """
        return await self.pipeline_orchestrator.deploy_and_run_scheme(
            project_dir=Path(project_path),
            selected_instance_ids=selected_jobs,
        )

    async def delete_job(self, job_name: str, instance_id: Optional[str] = None) -> Dict[str, Any]:
        return await self.project_service.delete_job(job_name, instance_id=instance_id)


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
        
        return str(Path.home())

    async def scan_for_projects(self, base_path: str) -> List[Dict[str, Any]]:
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
                    creator = None
                    pipeline_active = False
                    try:
                        with open(params_file) as f:
                            data = json.load(f)
                        raw_created = data.get("created_at")
                        if raw_created:
                            created_at = str(raw_created)[:16]
                        creator = data.get("created_by")
                        pipeline_active = bool(data.get("pipeline_active", False))
                    except Exception:
                        pass

                    if creator is None:
                        try:
                            creator = pwd.getpwuid(stats.st_uid).pw_name
                        except Exception:
                            creator = None

                    projects.append({
                        "name": item.name,
                        "path": str(item),
                        "modified": mod_time.strftime("%Y-%m-%d %H:%M"),
                        "modified_timestamp": stats.st_mtime,
                        "created_at": created_at,
                        "creator": creator,
                        "pipeline_active": pipeline_active,
                    })
                except Exception as e:
                    logger.info("Error reading %s: %s", item.name, e)

        except Exception as e:
            logger.error("Error scanning projects: %s", e)
            return []

        projects.sort(key=lambda x: x["modified_timestamp"], reverse=True)
        logger.info("Found %d valid projects.", len(projects))
        return projects

        


    async def get_job_parameters(self, job_name: str) -> Dict[str, Any]:
        """Get parameters for a specific job instance, initializing if not present."""
        try:
            state = self.state_service.state

            job_model = state.jobs.get(job_name)
            if not job_model:
                # instance_id not found — try to initialize as a singleton job
                try:
                    job_type = JobType.from_string(job_name)
                except ValueError:
                    return {"success": False, "error": f"Unknown job instance: {job_name}"}

                logger.info("Job %s not in state, initializing from template.", job_name)
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
                job_star_path = template_base / job_type.value / "job.star"
                state.ensure_job_initialized(
                    job_type,
                    instance_id=job_name,
                    template_path=job_star_path if job_star_path.exists() else None,
                )
                job_model = state.jobs.get(job_name)

            if job_model:
                return {"success": True, "params": job_model.model_dump()}
            else:
                return {"success": False, "error": f"Failed to initialize job {job_name}"}
        except Exception as e:
            return {"success": False, "error": str(e)}


    async def update_job_parameters(self, job_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates parameters for a specific job instance and persists to disk.
        """
        try:
            state = self.state_service.state
            job_model = state.jobs.get(job_name)

            if not job_model:
                return {"success": False, "error": f"Job {job_name} not initialized"}

            for key, value in params.items():
                if hasattr(job_model, key):
                    setattr(job_model, key, value)

            await self.state_service.save_project()

            return {"success": True, "params": job_model.model_dump()}

        except Exception as e:
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def get_available_jobs(self) -> List[str]:
        # --- FIX: Use config root instead of cwd ---
        template_path = self.config_service.crboost_root / "config" / "Schemes" / "warp_tomo_prep"
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
        from services.configs.mdoc_service import get_mdoc_service
        mdoc_data = get_mdoc_service().get_autodetect_params(mdocs_glob)
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

    async def run_shell_command(
        self, command: str, cwd: Path = None, tool_name: str = None, additional_binds: List[str] = None
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
                    return {"success": True, "output": stdout.decode(), "error": None}
                else:
                    return {"success": False, "output": stdout.decode(), "error": stderr.decode()}

            except asyncio.TimeoutError:
                logger.error("Command timed out after 120 seconds: %s", final_command)
                process.terminate()
                await process.wait()
                return {"success": False, "output": "", "error": "Command execution timed out"}

        except Exception as e:
            logger.error("Exception in run_shell_command: %s", e)
            return {"success": False, "output": "", "error": str(e)}

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
