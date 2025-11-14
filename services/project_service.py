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
from services.mdoc_service import get_mdoc_service  # This was already correct

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
        ...
        # TODO: Implement this later

    async def initialize_new_project(
        self, project_name: str, project_base_path: str, selected_jobs: List[str], movies_glob: str, mdocs_glob: str
    ):
        """
        The main orchestration logic for creating a new project.
        --- REFACTORED to use new StateService ---
        """
        try:
            project_dir = Path(project_base_path).expanduser() / project_name
            base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            scheme_name = f"scheme_{project_name}"

            if project_dir.exists():
                return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

            import_prefix = f"{project_name}_"

            # --- NEW: Get state service and update state *before* doing anything else ---
            state_service = self.backend.state_service
            state = state_service.state
            state.project_name = project_name
            state.project_path = project_dir  # This will be set again in create_project_structure, which is fine

            # This creates dirs and calls self.set_project_root()
            structure_result = await self.create_project_structure(project_dir, movies_glob, mdocs_glob, import_prefix)

            if not structure_result["success"]:
                return structure_result

            params_json_path = project_dir / "project_params.json"
            try:
                # --- NEW: Ensure all selected jobs are initialized in the state ---
                # This populates state.jobs from job.star templates
                print(f"[PROJECT_SERVICE] Initializing jobs in state: {selected_jobs}")
                for job_str in selected_jobs:
                    try:
                        job_type = JobType(job_str)
                        job_star_path = base_template_path / job_type.value / "job.star"
                        await state_service.ensure_job_initialized(
                            job_type, job_star_path if job_star_path.exists() else None
                        )
                    except ValueError:
                        print(f"[WARN] Skipping unknown job '{job_str}' during state initialization.")

                # --- NEW: Save the populated ProjectState model ---
                # This replaces the old export_for_project() and manual json.dump()
                await state_service.save_project(params_json_path)

                print(f"[PROJECT_SERVICE] Saved parameters to {params_json_path}")

                if not params_json_path.exists():
                    raise FileNotFoundError(f"Parameter file was not created at {params_json_path}")

                file_size = params_json_path.stat().st_size
                if file_size == 0:
                    raise ValueError(f"Parameter file is empty: {params_json_path}")

                print(f"[PROJECT_SERVICE] Verified parameter file: {file_size} bytes")

            except Exception as e:
                print(f"[ERROR] Failed to save project_params.json: {e}")
                import traceback

                traceback.print_exc()
                return {"success": False, "error": f"Project created but failed to save parameters: {str(e)}"}

            # Collect bind paths
            additional_bind_paths = {
                str(Path(project_base_path).expanduser().resolve()),
                str(Path(movies_glob).parent.resolve()),
                str(Path(mdocs_glob).parent.resolve()),
            }

            # Add gain ref path if it exists
            if state.acquisition.gain_reference_path:
                additional_bind_paths.add(str(Path(state.acquisition.gain_reference_path).parent.resolve()))

            # Create the scheme
            # This will now read job models from the new state service
            scheme_result = await self.backend.pipeline_orchestrator.create_custom_scheme(
                project_dir,
                scheme_name,
                base_template_path,
                selected_jobs,
                additional_bind_paths=list(additional_bind_paths),
            )

            if not scheme_result["success"]:
                return scheme_result

            # Initialize Relion project
            print(f"[PROJECT_SERVICE] Initializing Relion project in {project_dir}...")
            pipeline_star_path = project_dir / "default_pipeline.star"

            init_command = "unset DISPLAY && relion --tomo --do_projdir ."

            container_init_command = self.backend.container_service.wrap_command_for_tool(
                command=init_command, cwd=project_dir, tool_name="relion", additional_binds=list(additional_bind_paths)
            )

            process = await asyncio.create_subprocess_shell(
                container_init_command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30.0)
                if process.returncode != 0:
                    print(f"[RELION INIT ERROR] {stderr.decode()}")
            except asyncio.TimeoutError:
                print("[ERROR] Relion project initialization timed out.")
                process.kill()
                await process.wait()

            if not pipeline_star_path.exists():
                return {"success": False, "error": f"Failed to create default_pipeline.star."}

            return {
                "success": True,
                "message": f"Project '{project_name}' created and initialized successfully.",
                "project_path": str(project_dir),
                "params_file": str(params_json_path),
            }

        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "error": f"Project creation failed: {str(e)}"}

    async def load_project_state(self, project_path: str) -> Dict[str, Any]:
        """
        Loads a project using the new StateService.
        --- REFACTORED ---
        """
        try:
            project_dir = Path(project_path)
            if not project_dir.exists():
                return {"success": False, "error": f"Project path not found: {project_path}"}

            params_file = project_dir / "project_params.json"
            if not params_file.exists():
                return {"success": False, "error": "No project_params.json found"}

            # --- NEW: Bridge for missing data ---
            # Manually read data_sources, as it's not in the new ProjectState model
            movies_glob = ""
            mdocs_glob = ""
            try:
                with open(params_file, "r") as f:
                    raw_params_data = json.load(f)

                # Check for old data_sources key
                data_sources = raw_params_data.get("data_sources", {})
                if data_sources:
                    movies_glob = data_sources.get("frames_glob", "")
                    mdocs_glob = data_sources.get("mdocs_glob", "")

                # Also check for data stored in 'acquisition' model for newer saves
                if not movies_glob and "acquisition" in raw_params_data:
                    # This assumes you might store them here, adjust if not
                    movies_glob = raw_params_data["acquisition"].get("frames_glob", "")
                if not mdocs_glob and "acquisition" in raw_params_data:
                    mdocs_glob = raw_params_data["acquisition"].get("mdocs_glob", "")

            except Exception as e:
                print(f"[LOAD_PROJECT] Warning: could not parse raw JSON for data_sources: {e}")

            # --- NEW: Use the StateService to load the project ---
            # This will replace the global singleton state
            load_success = await self.backend.state_service.load_project(params_file)

            if not load_success:
                return {"success": False, "error": f"StateService failed to load project from {params_file}"}

            # Get the new state that was just loaded
            state = self.backend.state_service.state

            # Set the project root for this service instance
            self.set_project_root(project_dir)

            # Sync job statuses from the pipeline.star file
            await self.backend.pipeline_runner.status_sync.sync_all_jobs(project_path)

            # Extract data from the new state for the UI
            project_name = state.project_name
            selected_jobs = [job_type.value for job_type in state.jobs.keys()]

            return {
                "success": True,
                "project_name": project_name,
                "selected_jobs": selected_jobs,
                "movies_glob": movies_glob,  # Return the manually recovered value
                "mdocs_glob": mdocs_glob,  # Return the manually recovered value
            }
        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}