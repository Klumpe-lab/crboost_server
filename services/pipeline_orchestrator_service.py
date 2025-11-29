# services/pipeline_orchestrator_service.py
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import json
from services.config_service import get_config_service
from services.project_state import AbstractJobParams, JobCategory, JobType, get_state_service, ImportMoviesParams
from .starfile_service import StarfileService
from .project_service import ProjectService
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class BaseCommandBuilder:
    def format_paths(self, paths: Dict[str, Path]) -> Dict[str, str]:
        return {k: str(v) for k, v in paths.items()}

    def add_optional_param(self, cmd_parts: List[str], flag: str, value: Any, condition: bool = True):
        if condition and value is not None and str(value) != "None" and str(value) != "":
            cmd_parts.extend([flag, str(value)])


class ImportMoviesCommandBuilder(BaseCommandBuilder):
    """Build import movies command from params"""

    def build(self, params: ImportMoviesParams, paths: Dict[str, Path]) -> str:
        """Build the relion_import command"""
        cmd_parts = [
            "relion_import",
            "--do_movies",
            "--optics_group_name",
            params.optics_group_name,
            "--angpix",
            str(params.pixel_size),
            "--kV",
            str(params.voltage),
            "--Cs",
            str(params.spherical_aberration),
            "--Q0",
            str(params.amplitude_contrast),
            "--dose_per_tilt_image",
            str(params.acquisition.dose_per_tilt),
            "--nominal_tilt_axis_angle",
            str(params.tilt_axis_angle),
        ]

        if params.acquisition.invert_defocus_hand:
            cmd_parts.append("--invert_defocus_hand")

        if params.do_at_most > 0:
            cmd_parts.extend(["--do_at_most", str(params.do_at_most)])

        if "mdoc_dir" in paths:
            input_pattern = str(paths["mdoc_dir"]) + "/*.mdoc"
            cmd_parts.extend(["--i", input_pattern])

        if "job_dir" in paths:
            cmd_parts.extend(["--o", str(paths["job_dir"]) + "/"])

        return " ".join(cmd_parts)


class PipelineOrchestratorService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.project_service = None
        self.state_service = get_state_service()

        self.job_builders: Dict[str, BaseCommandBuilder] = {JobType.IMPORT_MOVIES.value: ImportMoviesCommandBuilder()}

    async def create_custom_scheme(
        self,
        project_dir: Path,
        new_scheme_name: str,
        base_template_path: Path,
        selected_jobs: List[str],
        additional_bind_paths: List[str],
    ):
        """
        Creates a custom Relion scheme with the selected jobs.
        Updates state with job paths instead of writing sidecar files.
        """
        # --- FIX: Guard Clause for empty jobs ---
        if not selected_jobs:
            print("[PIPELINE] No jobs selected. Skipping scheme creation.")
            return {"success": True, "message": "No jobs to schedule."}

        try:
            # Initialize project service with absolute project root
            self.project_service = ProjectService(self.backend)
            self.project_service.set_project_root(project_dir)

            # Create scheme directory
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)
            base_scheme_name = base_template_path.name

            # Get the server root dir (where this file lives)
            server_dir = Path(__file__).parent.parent.resolve()

            # Combine all bind paths
            all_binds = list(set(additional_bind_paths + [str(server_dir)]))

            # Process each selected job
            for job_index, job_name in enumerate(selected_jobs):
                job_number = job_index + 1  # 1-indexed
                job_number_str = f"job{job_number:03d}"

                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                # Read job.star template
                job_star_path = dest_job_dir / "job.star"
                if not job_star_path.exists():
                    print(f"[PIPELINE WARNING] No job.star found for {job_name}")
                    exit(f"[PIPELINE WARNING] No job.star found for {job_name}. Something went horribly wrong")

                job_data = self.star_handler.read(job_star_path)

                params_df = job_data.get("joboptions_values")
                if params_df is None:
                    print(f"[PIPELINE WARNING] No joboptions_values in job.star for {job_name}")
                    continue

                # Get job model from the StateService
                state = self.backend.state_service.state
                try:
                    job_type_enum = JobType(job_name)
                    job_model = state.jobs.get(job_type_enum)
                except ValueError:
                    print(f"[PIPELINE WARNING] Unknown job type string: {job_name}")
                    job_model = None

                if not job_model:
                    print(f"[PIPELINE WARNING] Job {job_name} not in state, skipping scheme creation for it.")
                    continue

                # --- NEW: Model-Based Path Resolution ---

                # 1. Calculate Current Job Directory location (Do NOT create it yet)
                if job_type_enum == JobType.IMPORT_MOVIES:
                    category = JobCategory.IMPORT
                else:
                    category = JobCategory.EXTERNAL

                current_job_dir = project_dir / category.value / f"job{job_number:03d}"

                # 2. Calculate Upstream Job Directory (if applicable)
                upstream_job_dir = None
                if job_type_enum != JobType.IMPORT_MOVIES:
                    try:
                        # Simple linear dependency: find the job before this one
                        if job_index > 0:
                            prev_job_name = selected_jobs[job_index - 1]
                            prev_job_type = JobType(prev_job_name)
                            upstream_job_dir = self._get_job_dir_for_type(prev_job_type, selected_jobs, project_dir)
                    except Exception as e:
                        print(f"[PIPELINE WARNING] Could not determine upstream dir for {job_name}: {e}")

                # 3. Ask the Model to Resolve its own Paths
                paths = job_model.resolve_paths(current_job_dir, upstream_job_dir)

                # --- SINGLE SOURCE OF TRUTH UPDATE ---
                job_model.paths = {k: str(v) for k, v in paths.items()}
                job_model.additional_binds = all_binds
                job_model.relion_job_name = f"{job_model.JOB_CATEGORY.value}/job{job_number:03d}"
                state.jobs[job_type_enum] = job_model

                print(f"[PIPELINE] Updated internal state for {job_name} with paths.")

                # Build command for job.star
                final_command_for_fn_exe = self._build_job_command(job_name, job_model, paths, all_binds, server_dir)
                print(f"[PIPELINE] fn_exe for {job_name}: {final_command_for_fn_exe}")

                # Update the fn_exe parameter in job.star
                params_df.loc[params_df["rlnJobOptionVariable"] == "fn_exe", "rlnJobOptionValue"] = (
                    final_command_for_fn_exe
                )

                # Clear other_args
                params_df.loc[params_df["rlnJobOptionVariable"] == "other_args", "rlnJobOptionValue"] = ""

                # Remove template parameter placeholders
                params_to_remove = [f"param{i}_{s}" for i in range(1, 11) for s in ["label", "value"]]
                cleanup_mask = ~params_df["rlnJobOptionVariable"].isin(params_to_remove)
                job_data["joboptions_values"] = params_df[cleanup_mask].reset_index(drop=True)

                # Replace scheme name references
                for block_name, block_data in job_data.items():
                    if isinstance(block_data, pd.DataFrame):
                        for col in block_data.select_dtypes(include=["object"]):
                            if block_data[col].str.contains(base_scheme_name).any():
                                block_data[col] = block_data[col].str.replace(
                                    base_scheme_name, new_scheme_name, regex=False
                                )

                # Write updated job.star
                self.star_handler.write(job_data, job_star_path)

            self._create_scheme_star(new_scheme_dir, new_scheme_name, selected_jobs)

            # --- PERSIST STATE ---
            await self.backend.state_service.save_project()

            print(f"[PIPELINE] Created complete scheme at: {new_scheme_dir}")
            return {"success": True}

        except Exception as e:
            print(f"[PIPELINE ERROR] Failed to create custom scheme: {e}")
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _build_job_command(
        self,
        job_name: str,
        job_model: AbstractJobParams,
        paths: Dict[str, Path],
        all_binds: List[str],
        server_dir: Path,
    ) -> str:
        """
        Build the fn_exe command for a job using model metadata.
        """
        try:
            job_type = JobType(job_name)
        except ValueError:
            job_type = None

        # 1. Check if the job model says it's a driver-based job
        if job_model.is_driver_job():
            if not self.project_service or not self.project_service.project_root:
                return "echo 'ERROR: Project root not set in project_service during fn_exe build'; exit 1;"
            project_root_str = str(self.project_service.project_root.resolve())

            host_python_exe = server_dir / "venv" / "bin" / "python3"
            if not host_python_exe.exists():
                print(f"[PIPELINE WARNING] VENV Python not found at {host_python_exe}, falling back to 'python3'")
                host_python_exe = "python3"

            # Environment setup for drivers
            env_setup = f"export PYTHONPATH={server_dir}:${{PYTHONPATH}};"

            # Map driver jobs to their scripts using enums
            driver_scripts = {
                JobType.FS_MOTION_CTF: "fs_motion_and_ctf.py",
                JobType.TS_ALIGNMENT: "ts_alignment.py",
                JobType.TS_CTF: "ts_ctf.py",
                JobType.TS_RECONSTRUCT: "ts_reconstruct.py",
            }

            if job_type in driver_scripts:
                driver_script_path = server_dir / "drivers" / driver_scripts[job_type]
                return (
                    f"{env_setup} {host_python_exe} {driver_script_path} "
                    f"--job_type {job_name} "
                    f"--project_path {project_root_str}"
                )
            else:
                return f"echo 'ERROR: Job type \"{job_name}\" is a driver job but has no driver script mapped in orchestrator'; exit 1;"

        else:
            # This handles non-driver jobs (like importmovies)
            builder = self.job_builders.get(job_name)
            if not builder:
                return f"echo 'ERROR: No command builder found for simple job \"{job_name}\"'; exit 1;"

            try:
                raw_command = builder.build(job_model, paths)
                tool_name = job_model.get_tool_name()
                container_svc = self.backend.container_service
                return container_svc.wrap_command_for_tool(
                    command=raw_command, cwd=paths["job_dir"], tool_name=tool_name, additional_binds=all_binds
                )
            except Exception as e:
                print(f"[PIPELINE ERROR] Failed to build command for {job_name}: {e}")
                return f"echo 'ERROR: Failed to build command for {job_name}: {e}'; exit 1;"

    def _get_job_dir_for_type(self, target_job_type: JobType, selected_jobs: List[str], project_dir: Path) -> Path:
        """Helper to determine directory for upstream jobs"""
        try:
            idx = selected_jobs.index(target_job_type.value)
            job_num = idx + 1

            if target_job_type == JobType.IMPORT_MOVIES:
                cat = JobCategory.IMPORT
            else:
                cat = JobCategory.EXTERNAL

            return project_dir / cat.value / f"job{job_num:03d}"
        except ValueError:
            return None

    def _create_scheme_star(self, scheme_dir: Path, scheme_name: str, selected_jobs: List[str]):
        """
        Create the scheme.star file that defines the pipeline flow.
        """
        # General scheme metadata
        scheme_general_df = pd.DataFrame(
            {"rlnSchemeName": [f"Schemes/{scheme_name}/"], "rlnSchemeCurrentNodeName": ["WAIT"]}
        )

        # Float variables (pipeline control parameters)
        scheme_floats_df = pd.DataFrame(
            {
                "rlnSchemeFloatVariableName": ["do_at_most", "maxtime_hr", "wait_sec"],
                "rlnSchemeFloatVariableValue": [500.0, 48.0, 180.0],
                "rlnSchemeFloatVariableResetValue": [500.0, 48.0, 180.0],
            }
        )

        # Operators (control flow nodes)
        scheme_operators_df = pd.DataFrame(
            {
                "rlnSchemeOperatorName": ["EXIT", "EXIT_maxtime", "WAIT"],
                "rlnSchemeOperatorType": ["exit", "exit_maxtime", "wait"],
                "rlnSchemeOperatorOutput": ["undefined"] * 3,
                "rlnSchemeOperatorInput1": ["undefined", "maxtime_hr", "wait_sec"],
                "rlnSchemeOperatorInput2": ["undefined"] * 3,
            }
        )

        # Jobs in the scheme
        scheme_jobs_df = pd.DataFrame(
            {
                "rlnSchemeJobNameOriginal": selected_jobs,
                "rlnSchemeJobName": selected_jobs,
                "rlnSchemeJobMode": ["continue"] * len(selected_jobs),
                "rlnSchemeJobHasStarted": [0] * len(selected_jobs),
            }
        )

        # Edges (pipeline flow)
        edges = [
            {"rlnSchemeEdgeInputNodeName": "WAIT", "rlnSchemeEdgeOutputNodeName": "EXIT_maxtime"},
            {"rlnSchemeEdgeInputNodeName": "EXIT_maxtime", "rlnSchemeEdgeOutputNodeName": selected_jobs[0]},
        ]

        # Connect jobs sequentially
        for i in range(len(selected_jobs) - 1):
            edges.append(
                {"rlnSchemeEdgeInputNodeName": selected_jobs[i], "rlnSchemeEdgeOutputNodeName": selected_jobs[i + 1]}
            )

        # Connect last job to EXIT
        edges.append({"rlnSchemeEdgeInputNodeName": selected_jobs[-1], "rlnSchemeEdgeOutputNodeName": "EXIT"})

        scheme_edges_df = pd.DataFrame(edges)

        # Add edge metadata
        scheme_edges_df["rlnSchemeEdgeIsFork"] = 0
        scheme_edges_df["rlnSchemeEdgeOutputNodeNameIfTrue"] = "undefined"
        scheme_edges_df["rlnSchemeEdgeBooleanVariable"] = "undefined"

        # Combine all blocks into scheme.star
        scheme_star_data = {
            "scheme_general": scheme_general_df,
            "scheme_floats": scheme_floats_df,
            "scheme_operators": scheme_operators_df,
            "scheme_jobs": scheme_jobs_df,
            "scheme_edges": scheme_edges_df,
        }

        scheme_star_path = scheme_dir / "scheme.star"
        self.star_handler.write(scheme_star_data, scheme_star_path)
        print(f"[PIPELINE] Created scheme.star at: {scheme_star_path}")
