# services/project_service.py
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
import os
import glob
import asyncio
import json
from typing import TYPE_CHECKING

from services.project_state import JobType, get_state_service, jobtype_paramclass
from services.starfile_service import StarfileService
from services.mdoc_service import get_mdoc_service

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class DataImportService:

    def __init__(self):
        self.mdoc_service = get_mdoc_service()

    async def setup_project_data(
        self, project_dir: Path, movies_glob: str, mdocs_glob: str, import_prefix: str
    ) -> Dict[str, Any]:
        """
        Orchestrates the data import process: creates dirs, symlinks movies,
        and rewrites mdocs with the specified prefix.
        """
        try:
            frames_dir = project_dir / "frames"
            mdoc_dir = project_dir / "mdoc"
            frames_dir.mkdir(exist_ok=True, parents=True)
            mdoc_dir.mkdir(exist_ok=True, parents=True)

            # Safety check if globs are empty (e.g. creating empty project)
            if not movies_glob or not mdocs_glob:
                 print("[DATA_IMPORT] Skipping data import - patterns are empty.")
                 return {"success": True, "message": "Skipped data import (empty patterns)."}

            source_movie_dir = Path(movies_glob).parent
            mdoc_files = glob.glob(mdocs_glob)

            if not mdoc_files:
                return {"success": False, "error": f"No .mdoc files found with pattern: {mdocs_glob}"}

            for mdoc_path_str in mdoc_files:
                mdoc_path = Path(mdoc_path_str)

                parsed_mdoc = self.mdoc_service.parse_mdoc_file(mdoc_path)

                for section in parsed_mdoc["data"]:
                    if "SubFramePath" not in section:
                        continue

                    original_movie_name = Path(section["SubFramePath"].replace("\\", "/")).name
                    prefixed_movie_name = f"{import_prefix}{original_movie_name}"

                    section["SubFramePath"] = prefixed_movie_name

                    source_movie_path = source_movie_dir / original_movie_name
                    link_path = frames_dir / prefixed_movie_name

                    if not source_movie_path.exists():
                        print(f"Warning: Source movie not found: {source_movie_path}")
                        continue

                    if not link_path.exists():
                        os.symlink(source_movie_path.resolve(), link_path)

                new_mdoc_path = mdoc_dir / f"{import_prefix}{mdoc_path.name}"

                self.mdoc_service.write_mdoc_file(parsed_mdoc, new_mdoc_path)

            return {"success": True, "message": f"Imported {len(mdoc_files)} tilt-series."}
        except Exception as e:
            return {"success": False, "error": str(e)}


class ProjectService:
    def __init__(self, backend_instance: "CryoBoostBackend"):

        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()
        self.project_root: Optional[Path] = None
        self.state_service = get_state_service()


    async def delete_job(self, job_name: str) -> Dict[str, Any]:
        """
        Orchestrates the full deletion: Relion files + Internal State.
        """
        try:
            job_type = JobType(job_name)
            state = self.backend.state_service.state
            project_dir = state.project_path

            if not project_dir:
                return {"success": False, "error": "Project not loaded"}

            # 1. Physical Deletion (Relion)
            if job_type.value in state.job_path_mapping:
                # --- FIX: Pass harsh=True to allow deleting failed jobs ---
                orch_result = await self.backend.pipeline_orchestrator.delete_job(
                    project_dir, 
                    job_type, 
                    harsh=True 
                )
                
                # Note: We proceed to logical deletion even if physical fails partly,
                # to ensure the UI doesn't get stuck showing a zombie job.
                if not orch_result["success"]:
                    print(f"[PROJECT_SERVICE] Warning: Physical deletion had issues: {orch_result.get('error')}")

            # 2. Logical Deletion (Memory)
            if job_type in state.jobs:
                del state.jobs[job_type]
            
            if job_type.value in state.job_path_mapping:
                del state.job_path_mapping[job_type.value]

            # 3. Persistence
            await self.backend.state_service.save_project()
            
            # 4. Sync
            await self.backend.pipeline_runner.status_sync.sync_all_jobs(str(project_dir))

            return {"success": True, "message": f"Job {job_name} deleted successfully."}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def set_project_root(self, project_dir: Path):
        """Set the project root for path resolution and update state."""
        self.project_root = project_dir.resolve()
        if self.backend and self.backend.state_service:
            self.backend.state_service.state.project_path = self.project_root

    def get_job_dir(self, job_name: str, job_number: int) -> Path:
        """
        Get absolute path to a job directory.
        Uses the job model's JOB_CATEGORY to determine location.
        """
        if not self.project_root:
            raise ValueError("Project root not set. Call set_project_root() first.")

        job_type = JobType.from_string(job_name)
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_name}")

        category = param_class.JOB_CATEGORY
        return self.project_root / category.value / f"job{job_number:03d}"

    def resolve_job_paths(self, job_name: str, job_number: int, selected_jobs: List[str]) -> Dict[str, Path]:
        if not self.project_root:
            raise ValueError("Project root not set")

        job_type = JobType.from_string(job_name)
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_name}")

        job_dir = self.get_job_dir(job_name, job_number)
        upstream_outputs = {}
        input_requirements = param_class.get_input_requirements()

        for logical_name, upstream_job_type_str in input_requirements.items():
            try:
                upstream_idx = selected_jobs.index(upstream_job_type_str)
                upstream_job_num = upstream_idx + 1
            except ValueError:
                raise ValueError(f"{job_name} requires {upstream_job_type_str} but it's not in selected jobs")

            upstream_job_type = JobType.from_string(upstream_job_type_str)
            upstream_param_class = param_classes.get(upstream_job_type)
            if not upstream_param_class:
                raise ValueError(f"Unknown upstream job type: {upstream_job_type_str}")

            upstream_job_dir = self.get_job_dir(upstream_job_type_str, upstream_job_num)
            upstream_outputs[upstream_job_type_str] = upstream_param_class.get_output_assets(upstream_job_dir)

        input_paths  = param_class.get_input_assets(job_dir, self.project_root, upstream_outputs)
        output_paths = param_class.get_output_assets(job_dir)
        all_paths    = {**input_paths, **output_paths}

        return all_paths

    async def create_project_structure(
        self, project_dir: Path, movies_glob: str, mdocs_glob: str, import_prefix: str
    ) -> Dict[str, Any]:
        """Creates the project directory structure and imports the raw data."""
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            self.set_project_root(project_dir)  # This now also updates the state

            (project_dir / "Schemes").mkdir(exist_ok=True)
            (project_dir / "Logs").mkdir(exist_ok=True)

            await self._setup_qsub_templates(project_dir)

            import_result = await self.data_importer.setup_project_data(
                project_dir, movies_glob, mdocs_glob, import_prefix
            )
            if not import_result["success"]:
                return import_result

            return {"success": True, "message": "Project directory structure created and data imported."}
        except Exception as e:
            return {"success": False, "error": f"Failed during directory setup: {str(e)}"}

    async def _setup_qsub_templates(self, project_dir: Path):
        qsub_template_path = Path.cwd() / "config" / "qsub"
        project_qsub_path = project_dir / "qsub"

        if qsub_template_path.is_dir():
            shutil.copytree(qsub_template_path, project_qsub_path, dirs_exist_ok=True)
            main_qsub_script = project_qsub_path / "qsub_cbe_warp.sh"
            if main_qsub_script.exists():
                await self._prepopulate_qsub_script(main_qsub_script)

    async def _prepopulate_qsub_script(self, qsub_script_path: Path):
        # TODO: Implement qsub script population if needed
        pass

    async def initialize_new_project(
            self, project_name: str, project_base_path: str, selected_jobs: List[str], movies_glob: str, mdocs_glob: str
        ):
            try:
                project_dir = Path(project_base_path).expanduser() / project_name
                
                # 1. Standard Setup (Dirs, Data Import)
                if project_dir.exists():
                    return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

                import_prefix = f"{project_name}_"
                
                # Update Global State Wrapper
                state = self.backend.state_service.state
                state.project_name = project_name
                state.project_path = project_dir
                state.movies_glob = movies_glob                 
                state.mdocs_glob = mdocs_glob                   

                # Create Dirs & Import Data
                structure_result = await self.create_project_structure(project_dir, movies_glob, mdocs_glob, import_prefix)
                if not structure_result["success"]:
                    return structure_result

                # 2. Load Defaults into Memory (Blueprints)
                # We DO NOT create scheme folders here. We just load the defaults from config/Schemes 
                # into our ProjectState so the user sees valid values in the UI.
                
                template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
                
                if selected_jobs:
                    print(f"[PROJECT_SERVICE] Loading default parameters for: {selected_jobs}")
                    for job_str in selected_jobs:
                        try:
                            job_type = JobType(job_str)
                            job_star_path = template_base / job_type.value / "job.star"
                            
                            # This loads the default values from disk into Python memory
                            await self.backend.state_service.ensure_job_initialized(
                                job_type, job_star_path if job_star_path.exists() else None
                            )
                        except ValueError:
                            print(f"[WARN] Skipping unknown job '{job_str}'")

                # 3. Save Project State (project_params.json)
                params_json_path = project_dir / "project_params.json"
                await self.backend.state_service.save_project(params_json_path)

                # 4. Initialize Relion (Create default_pipeline.star)
                # We initialize an EMPTY pipeline. The Orchestrator will populate it when we run jobs.
                print(f"[PROJECT_SERVICE] Initializing Relion project...")
                
                init_command = "unset DISPLAY && relion --tomo --do_projdir ."
                # Calculate binds for raw data
                binds = [str(project_dir.resolve()), str(Path(movies_glob).parent.resolve()), str(Path(mdocs_glob).parent.resolve())]
                
                container_cmd = self.backend.container_service.wrap_command_for_tool(
                    command=init_command, cwd=project_dir, tool_name="relion", additional_binds=binds
                )
                
                proc = await asyncio.create_subprocess_shell(
                    container_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
                )
                await proc.wait()

                return {
                    "success": True,
                    "message": f"Project '{project_name}' created.",
                    "project_path": str(project_dir),
                }

            except Exception as e:
                import traceback
                traceback.print_exc()
                return {"success": False, "error": str(e)}

    async def load_project_state(self, project_path: str) -> Dict[str, Any]:
        """
        Loads a project using the new StateService.
        """
        try:
            project_dir = Path(project_path)
            if not project_dir.exists():
                return {"success": False, "error": f"Project path not found: {project_path}"}

            params_file = project_dir / "project_params.json"
            if not params_file.exists():
                return {"success": False, "error": "No project_params.json found"}

            # Manually read data_sources for compatibility
            movies_glob = ""
            mdocs_glob = ""
            try:
                with open(params_file, "r") as f:
                    raw_params_data = json.load(f)

                data_sources = raw_params_data.get("data_sources", {})
                if data_sources:
                    movies_glob = data_sources.get("frames_glob", "")
                    mdocs_glob = data_sources.get("mdocs_glob", "")

                if not movies_glob and "acquisition" in raw_params_data:
                    movies_glob = raw_params_data["acquisition"].get("frames_glob", "")
                if not mdocs_glob and "acquisition" in raw_params_data:
                    mdocs_glob = raw_params_data["acquisition"].get("mdocs_glob", "")

            except Exception as e:
                print(f"[LOAD_PROJECT] Warning: could not parse raw JSON for data_sources: {e}")

            # Load via StateService
            load_success = await self.backend.state_service.load_project(params_file)

            if not load_success:
                return {"success": False, "error": f"StateService failed to load project from {params_file}"}

            state = self.backend.state_service.state
            self.set_project_root(project_dir)

            # Sync job statuses
            await self.backend.pipeline_runner.status_sync.sync_all_jobs(str(project_path))

            project_name = state.project_name
            selected_jobs = [job_type.value for job_type in state.jobs.keys()]

            return {
                "success": True,
                "project_name": project_name,
                "selected_jobs": selected_jobs,
                "movies_glob": movies_glob,
                "mdocs_glob": mdocs_glob,
            }
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}