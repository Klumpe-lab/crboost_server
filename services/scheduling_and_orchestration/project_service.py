import shutil
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import os
import glob
import asyncio
import json
from typing import TYPE_CHECKING

from services.configs.mdoc_service import get_mdoc_service
from services.configs.starfile_service import StarfileService
from services.project_state import (
    JobType,
    get_state_service,
    jobtype_paramclass,
    # CHANGED: new registry functions
    set_project_state_for,
)
from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service

if TYPE_CHECKING:
    from backend import CryoBoostBackend

logger = logging.getLogger(__name__)


class DataImportService:
    def __init__(self):
        self.mdoc_service = get_mdoc_service()

    def _setup_project_data_sync(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str,
        selected_mdoc_paths: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Synchronous core of data import — runs in thread pool to avoid blocking the event loop."""
        try:
            frames_dir = project_dir / "frames"
            mdoc_dir = project_dir / "mdoc"
            frames_dir.mkdir(exist_ok=True, parents=True)
            mdoc_dir.mkdir(exist_ok=True, parents=True)

            if not movies_glob or not mdocs_glob:
                logger.info("Skipping data import - patterns are empty.")
                return {"success": True, "message": "Skipped data import (empty patterns)."}

            source_movie_dir = Path(movies_glob).parent

            # Import only selected mdocs when a selection is provided
            if selected_mdoc_paths is not None:
                mdoc_files = selected_mdoc_paths
            else:
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
                        logger.warning("Source movie not found: %s", source_movie_path)
                        continue

                    if not link_path.exists():
                        os.symlink(source_movie_path.resolve(), link_path)

                new_mdoc_path = mdoc_dir / f"{import_prefix}{mdoc_path.name}"

                self.mdoc_service.write_mdoc_file(parsed_mdoc, new_mdoc_path)

            return {"success": True, "message": f"Imported {len(mdoc_files)} tilt-series."}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def setup_project_data(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str,
        selected_mdoc_paths: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Async wrapper — offloads blocking file I/O to a thread."""
        return await asyncio.to_thread(
            self._setup_project_data_sync, project_dir, movies_glob, mdocs_glob, import_prefix, selected_mdoc_paths
        )


class ProjectService:
    def __init__(self, backend_instance: "CryoBoostBackend"):

        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()
        self.project_root: Optional[Path] = None
        self.state_service = get_state_service()

    async def delete_job(self, job_name: str, instance_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            job_type = JobType(job_name)
            state = self.backend.state_service.state
            project_dir = state.project_path

            if not project_dir:
                return {"success": False, "error": "Project not loaded"}

            deletion_service = get_deletion_service()
            job_resolver = self.backend.pipeline_orchestrator.job_resolver

            # When a specific instance is requested, only delete that one job path.
            # Otherwise fall back to deleting all instances of the type (legacy behaviour).
            if instance_id:
                job_model = state.jobs.get(instance_id)
                if job_model and job_model.relion_job_name:
                    job_paths = [job_model.relion_job_name]
                else:
                    job_paths = []
            else:
                job_paths = deletion_service.find_jobs_by_type(project_dir, job_type, job_resolver)

            if not job_paths:
                instances_to_remove = (
                    [instance_id]
                    if instance_id
                    else [
                        iid
                        for iid in list(state.jobs.keys())
                        if iid == job_type.value or iid.startswith(job_type.value + "__")
                    ]
                )
                for iid in instances_to_remove:
                    state.jobs.pop(iid, None)
                    state.job_path_mapping.pop(iid, None)
                await self.backend.state_service.save_project(project_path=project_dir, force=True)
                return {"success": True, "message": f"Job {job_name} removed from project state."}

            all_orphans = []
            deleted_count = 0
            errors = []

            for job_path in job_paths:
                result = await deletion_service.delete_job(project_dir, job_path, recursive=False)
                if result.success:
                    deleted_count += 1
                    all_orphans.extend(result.orphaned_jobs)
                else:
                    errors.append(f"{job_path}: {result.error}")

            # Remove only the affected instances from state
            if instance_id:
                instances_to_remove = [instance_id]
            else:
                instances_to_remove = [
                    iid
                    for iid in list(state.jobs.keys())
                    if iid == job_type.value or iid.startswith(job_type.value + "__")
                ]
            for iid in instances_to_remove:
                state.jobs.pop(iid, None)
                state.job_path_mapping.pop(iid, None)

            await self.backend.state_service.save_project(project_path=project_dir, force=True)
            await self.backend.pipeline_runner.sync_all_jobs(str(project_dir))

            if errors:
                return {
                    "success": False,
                    "error": f"Partial failure: {'; '.join(errors)}",
                    "deleted_count": deleted_count,
                    "orphaned_jobs": all_orphans,
                }

            return {
                "success": True,
                "message": f"Deleted {deleted_count} job instance(s)."
                + (
                    f" Warning: {len(all_orphans)} downstream job(s) now have broken inputs: {all_orphans}"
                    if all_orphans
                    else ""
                ),
                "deleted_count": deleted_count,
                "orphaned_jobs": all_orphans,
            }

        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def set_project_root(self, project_dir: Path):
        """Set the project root for path resolution and update state."""
        self.project_root = project_dir.resolve()
        # CHANGED: use registry instead of tab-context .state
        if self.backend and self.backend.state_service:
            state = self.backend.state_service.state_for(self.project_root)
            state.project_path = self.project_root

    def get_job_dir(self, job_name: str, job_number: int) -> Path:
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

        input_paths = param_class.get_input_assets(job_dir, self.project_root, upstream_outputs)
        output_paths = param_class.get_output_assets(job_dir)
        all_paths = {**input_paths, **output_paths}

        return all_paths

    async def create_project_structure(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str,
        selected_mdoc_paths: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Creates the project directory structure and imports the raw data."""
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            self.set_project_root(project_dir)

            (project_dir / "Schemes").mkdir(exist_ok=True)
            (project_dir / "Logs").mkdir(exist_ok=True)

            await self._setup_qsub_templates(project_dir)

            import_result = await self.data_importer.setup_project_data(
                project_dir, movies_glob, mdocs_glob, import_prefix, selected_mdoc_paths
            )
            if not import_result["success"]:
                return import_result

            return {"success": True, "message": "Project directory structure created and data imported."}
        except Exception as e:
            return {"success": False, "error": f"Failed during directory setup: {str(e)}"}

    async def _setup_qsub_templates(self, project_dir: Path):
        """Copy qsub.sh to project root for relion_schemer to find."""
        source_qsub = Path.cwd() / "config" / "qsub.sh"
        dest_qsub = project_dir / "qsub.sh"

        if source_qsub.exists():
            shutil.copy(source_qsub, dest_qsub)
            logger.info("Copied qsub.sh to %s", dest_qsub)
        else:
            logger.warning("qsub.sh not found at %s", source_qsub)

    async def initialize_new_project(
        self,
        project_name: str,
        project_base_path: str,
        selected_jobs: List[str],
        movies_glob: str,
        mdocs_glob: str,
        selected_mdoc_paths: Optional[List[str]] = None,
        import_summary: Optional[Dict[str, Any]] = None,
        detected_params: Optional[Dict[str, Any]] = None,
    ):
        try:
            project_dir = Path(project_base_path).expanduser() / project_name

            # 1. Standard Setup (Dirs, Data Import)
            if project_dir.exists():
                return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

            import getpass
            from services.project_state import ProjectState, ImportPositionSummary, ImportTiltSeriesSummary

            state = ProjectState()
            state.project_name = project_name
            state.project_path = project_dir
            state.movies_glob = movies_glob
            state.mdocs_glob = mdocs_glob
            state.created_by = getpass.getuser()
            set_project_state_for(project_dir, state)

            # Apply microscope/acquisition params from the already-parsed dataset
            # overview (avoids re-parsing all mdocs from scratch).
            if detected_params:
                if "pixel_size_angstrom" in detected_params:
                    state.microscope.pixel_size_angstrom = detected_params["pixel_size_angstrom"]
                if "acceleration_voltage_kv" in detected_params:
                    state.microscope.acceleration_voltage_kv = detected_params["acceleration_voltage_kv"]
                if "dose_per_tilt" in detected_params:
                    state.acquisition.dose_per_tilt = detected_params["dose_per_tilt"]
                if "tilt_axis_degrees" in detected_params:
                    state.acquisition.tilt_axis_degrees = detected_params["tilt_axis_degrees"]
                state.update_modified()
            else:
                # Fallback: re-parse mdocs (legacy path / no overview available)
                await self.backend.state_service.update_from_mdoc(mdocs_glob, project_path=project_dir)

            # Apply dataset import summary so it's saved atomically with the project
            if import_summary:
                state.import_total_positions = import_summary.get("total_positions", 0)
                state.import_selected_positions = import_summary.get("selected_positions", 0)
                state.import_total_tilt_series = import_summary.get("total_tilt_series", 0)
                state.import_selected_tilt_series = import_summary.get("selected_tilt_series", 0)
                state.import_source_directory = import_summary.get("source_directory", "")
                state.import_frame_extension = import_summary.get("frame_extension", "")
                position_details = import_summary.get("position_details", [])
                state.import_position_details = [
                    pd if isinstance(pd, ImportPositionSummary) else ImportPositionSummary(**pd)
                    for pd in position_details
                ]
                ts_details = import_summary.get("tilt_series_details", [])
                state.import_tilt_series_details = [
                    td if isinstance(td, ImportTiltSeriesSummary) else ImportTiltSeriesSummary(**td)
                    for td in ts_details
                ]
                state.tilt_metadata = import_summary.get("tilt_metadata", {})

            # Create Dirs & Import Data (runs blocking I/O in thread pool)
            import_prefix = f"{project_name}_"
            structure_result = await self.create_project_structure(
                project_dir, movies_glob, mdocs_glob, import_prefix, selected_mdoc_paths
            )
            if not structure_result["success"]:
                return structure_result

            # 2. Load Defaults into Memory (Blueprints)
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"

            if selected_jobs:
                logger.info("Loading default parameters for: %s", selected_jobs)
                for job_str in selected_jobs:
                    try:
                        job_type = JobType(job_str)
                        job_star_path = template_base / job_type.value / "job.star"
                        state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)
                    except ValueError:
                        logger.warning("Skipping unknown job '%s'", job_str)

            # 3. Save Project State (project_params.json) — includes import summary
            params_json_path = project_dir / "project_params.json"
            await self.backend.state_service.save_project(
                save_path=params_json_path, project_path=project_dir, force=True
            )

            # 4. Initialize Relion (Create default_pipeline.star)
            logger.info("Initializing Relion project...")

            init_command = "unset DISPLAY && relion --tomo --do_projdir ."
            binds = [
                str(project_dir.resolve()),
                str(Path(movies_glob).parent.resolve()),
                str(Path(mdocs_glob).parent.resolve()),
            ]

            container_cmd = self.backend.container_service.wrap_command_for_tool(
                command=init_command, cwd=project_dir, tool_name="relion", additional_binds=binds
            )

            proc = await asyncio.create_subprocess_shell(
                container_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=project_dir
            )
            await proc.wait()

            return {"success": True, "message": f"Project '{project_name}' created.", "project_path": str(project_dir)}

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
                logger.info("Warning: could not parse raw JSON for data_sources: %s", e)

            # Load via StateService (this registers into the path-keyed registry)
            load_success = await self.backend.state_service.load_project(params_file)

            if not load_success:
                return {"success": False, "error": f"StateService failed to load project from {params_file}"}

            # CHANGED: use explicit path to get the state we just loaded
            state = self.backend.state_service.state_for(project_dir)
            self.set_project_root(project_dir)

            # Sync job statuses
            await self.backend.pipeline_runner.sync_all_jobs(str(project_path))

            project_name = state.project_name
            selected_jobs = list(state.jobs.keys())

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
