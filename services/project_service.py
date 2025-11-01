# services/project_service.py
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
from services.parameter_models import JobType, jobtype_paramclass, AbstractJobParams
from services.starfile_service import StarfileService
import os
import glob
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from backend import CryoBoostBackend



class DataImportService:
    """
    Handles the core logic of preparing raw data for a CryoBoost project.
    This includes parsing mdocs, creating symlinks, and rewriting mdocs with prefixes.
    """

    def _parse_mdoc(self, mdoc_path: Path) -> Dict[str, Any]:
        header_lines = []
        data_sections = []
        current_section = {}
        in_zvalue_section = False

        with open(mdoc_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith("[ZValue"):
                    if current_section:
                        data_sections.append(current_section)
                    current_section = {"ZValue": line.split("=")[1].strip().strip("]")}
                    in_zvalue_section = True
                elif in_zvalue_section and "=" in line:
                    key, value = [x.strip() for x in line.split("=", 1)]
                    current_section[key] = value
                elif not in_zvalue_section:
                    header_lines.append(line)

        if current_section:
            data_sections.append(current_section)

        return {"header": "\n".join(header_lines), "data": data_sections}

    def _write_mdoc(self, mdoc_data: Dict[str, Any], output_path: Path):
        """
        Writes a parsed mdoc data structure back to a file.
        Lifts logic from `mdocMeta.writeMdoc`.
        """
        with open(output_path, "w") as f:
            f.write(mdoc_data["header"] + "\n")
            for section in mdoc_data["data"]:
                z_value = section.pop("ZValue", None)
                if z_value is not None:
                    f.write(f"[ZValue = {z_value}]\n")
                for key, value in section.items():
                    f.write(f"{key} = {value}\n")
                f.write("\n")

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
                parsed_mdoc = self._parse_mdoc(mdoc_path)

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
                self._write_mdoc(parsed_mdoc, new_mdoc_path)

            return {"success": True, "message": f"Imported {len(mdoc_files)} tilt-series."}
        except Exception as e:
            return {"success": False, "error": str(e)}


class ProjectService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()

        self.project_root: Optional[Path] = None

    def set_project_root(self, project_dir: Path):
        """Set the project root for path resolution"""
        self.project_root = project_dir.resolve()

    def get_job_dir(self, job_name: str, job_number: int) -> Path:
        """
        Get absolute path to a job directory.
        Uses the job model's JOB_CATEGORY to determine location.
        """
        if not self.project_root:
            raise ValueError("Project root not set. Call set_project_root() first.")

        # Get the param class for this job
        job_type = JobType.from_string(job_name)
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_name}")

        # This check is now valid because param_class is Type[AbstractJobParams]
        category = param_class.JOB_CATEGORY
        return self.project_root / category.value / f"job{job_number:03d}"

    def resolve_job_paths(self, job_name: str, job_number: int, selected_jobs: List[str]) -> Dict[str, Path]:
        """
        Resolve all paths for a job by:
        1. Getting outputs from upstream jobs
        2. Calling the job's get_input_assets() with those outputs
        3. Merging with the job's own output assets
        Returns: All paths (inputs + outputs) as absolute Paths
        """
        if not self.project_root:
            raise ValueError("Project root not set")

        job_type = JobType.from_string(job_name)
        param_classes = jobtype_paramclass()
        param_class = param_classes.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_name}")

        # Get this job's directory
        job_dir = self.get_job_dir(job_name, job_number)

        # Collect upstream outputs
        upstream_outputs = {}
        # param_class is now Type[AbstractJobParams], so this call is type-safe
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

        # Pass self.project_root to get_input_assets
        input_paths = param_class.get_input_assets(job_dir, self.project_root, upstream_outputs)
        output_paths = param_class.get_output_assets(job_dir)
        all_paths = {**input_paths, **output_paths}

        return all_paths

    async def create_project_structure(
        self, project_dir: Path, movies_glob: str, mdocs_glob: str, import_prefix: str
    ) -> Dict[str, Any]:
        """Creates the project directory structure and imports the raw data."""
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            self.set_project_root(project_dir)  # Set root for path resolution

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
        # TODO: Implement this later. Just grab values from computing and replace them in XXXparam1XXX etc.
        # replacements = self.backend.app_state.computing.get_qsub_replacements()

        # with open(qsub_script_path, 'r') as f:
        #     content = f.read()

        # for placeholder, value in replacements.items():
        #     content = content.replace(placeholder, value)

        # with open(qsub_script_path, 'w') as f:
        #     f.write(content)
