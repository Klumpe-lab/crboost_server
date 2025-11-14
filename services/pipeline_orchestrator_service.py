# services/pipeline_orchestrator_service.py
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, List
import json

from services.commands_builder import ImportMoviesCommandBuilder, BaseCommandBuilder
from services.config_service import get_config_service
from services.project_state import AbstractJobParams, JobCategory, JobType
from services.state_service import get_state_service

from .starfile_service import StarfileService
from .project_service import ProjectService

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class PipelineOrchestratorService:
    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.project_service = None
        self.state_service = get_state_service()

        # Map job names to their corresponding builder class
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
        Uses standardized path resolution based on old cryoboost conventions.
        """
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

                # --- UPDATED: Use standardized path resolution ---
                paths = self._resolve_job_paths_standardized(job_name, job_number, selected_jobs, project_dir)
                
                # Create job directory and validate critical paths
                job_run_dir = paths["job_dir"]
                job_run_dir.mkdir(parents=True, exist_ok=True)
                
                # Validate that required input files exist for non-import jobs
                if job_name != "importmovies":
                    input_star = paths.get("input_star")
                    if input_star and not input_star.exists():
                        print(f"[PIPELINE WARNING] Input STAR file not found for {job_name}: {input_star}")
                        # Don't fail here, but log the warning

                params_json_path = job_run_dir / "job_params.json"

                final_command_for_fn_exe = self._build_job_command(job_name, job_model, paths, all_binds, server_dir)

                print(f"[PIPELINE] fn_exe for {job_name}: {final_command_for_fn_exe}")

                job_type = JobType.from_string(job_name)

                # Create the job parameters file with standardized paths
                data_to_serialize = {
                    "job_type": job_type.value,
                    "job_model": job_model.model_dump(),
                    "paths": {k: str(v) for k, v in paths.items()},
                    "additional_binds": all_binds,
                }

                try:
                    with open(params_json_path, "w") as f:
                        json.dump(data_to_serialize, f, indent=2)
                    print(f"[PIPELINE] Saved job params to {params_json_path}")
                    print(f"[PIPELINE] Paths for {job_name}: {list(paths.keys())}")
                except Exception as e:
                    print(f"[PIPELINE ERROR] Failed to save {params_json_path}: {e}")

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
            print(f"[PIPELINE] Created complete scheme at: {new_scheme_dir}")
            return {"success": True}

        except Exception as e:
            print(f"[PIPELINE ERROR] Failed to create custom scheme: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _resolve_job_paths_standardized(self, job_name: str, job_number: int, selected_jobs: List[str], project_dir: Path) -> Dict[str, Path]:
        """
        Standardized path resolution using JobType and JobCategory enums.
        """
        # Convert string to enum
        try:
            job_type = JobType(job_name)
        except ValueError:
            # Fallback for unknown job types
            job_type = None

        # Determine job directory based on category
        if job_type == JobType.IMPORT_MOVIES:
            job_dir = project_dir / JobCategory.IMPORT.value / f"job{job_number:03d}"
        else:
            job_dir = project_dir / JobCategory.EXTERNAL.value / f"job{job_number:03d}"
        
        base_paths = {
            "job_dir"     : job_dir,
            "project_root": project_dir,
            "frames_dir"  : project_dir / "frames",
            "mdoc_dir"    : project_dir / "mdoc",
        }
        
        # Job-specific path templates using enums
        if job_type == JobType.IMPORT_MOVIES:
            return {
                **base_paths,
                "tilt_series_dir": base_paths["job_dir"] / "tilt_series",
                "output_star"    : base_paths["job_dir"] / "tilt_series.star",
                "tomostar_dir"   : base_paths["job_dir"] / "tomostar",
            }
        
        elif job_type == JobType.FS_MOTION_CTF:
            # Find the import job that this depends on
            # import_job_num = self._find_upstream_job_number(JobType.IMPORT_MOVIES.value, selected_jobs, job_number)
            # import_job_dir = project_dir / JobCategory.IMPORT.value / f"job{import_job_num:03d}"

            import_job_dir = self._get_upstream_job_dir(JobType.IMPORT_MOVIES, selected_jobs, job_number, project_dir)
    
            
            return {
                **base_paths,
                "input_star"     : import_job_dir / "tilt_series.star",
                "output_star"    : base_paths["job_dir"] / "fs_motion_and_ctf.star",
                "warp_dir"       : base_paths["job_dir"] / "warp_frameseries",
                "warp_settings"  : base_paths["job_dir"] / "warp_frameseries.settings",
                "tilt_series_dir": base_paths["job_dir"] / "tilt_series",
            }
        
        elif job_type == JobType.TS_ALIGNMENT:
            # Find the fsMotion job that this depends on  
            # fsmotion_job_num = self._find_upstream_job_number(JobType.FS_MOTION_CTF.value, selected_jobs, job_number)
            # fsmotion_job_dir = project_dir / JobCategory.EXTERNAL.value / f"job{fsmotion_job_num:03d}"
            
            fsmotion_job_dir = self._get_upstream_job_dir(JobType.FS_MOTION_CTF, selected_jobs, job_number, project_dir)
            return {
                **base_paths,
                "input_star"     : fsmotion_job_dir / "fs_motion_and_ctf.star",
                "output_star"    : base_paths["job_dir"] / "aligned_tilt_series.star",
                "warp_dir"       : base_paths["job_dir"] / "warp_tiltseries",
                "warp_settings"  : base_paths["job_dir"] / "warp_tiltseries.settings",
                "tomostar_dir"   : base_paths["job_dir"] / "tomostar",
                "frameseries_dir": fsmotion_job_dir / "warp_frameseries",                # This is what the alignment job needs
            }
            
        # In the _resolve_job_paths_standardized method for TS_CTF:

        # In the _resolve_job_paths_standardized method for TS_CTF:

        elif job_type == JobType.TS_CTF:
            # Find the alignment job that this depends on
            # align_job_num = self._find_upstream_job_number(JobType.TS_ALIGNMENT.value, selected_jobs, job_number)
            # align_job_dir = project_dir / JobCategory.EXTERNAL.value / f"job{align_job_num:03d}"
            
            align_job_dir = self._get_upstream_job_dir(JobType.TS_ALIGNMENT, selected_jobs, job_number, project_dir)
            return {
                **base_paths,
                "input_star": align_job_dir / "aligned_tilt_series.star",
                "output_star": base_paths["job_dir"] / "ts_ctf_tilt_series.star",
                "warp_dir": base_paths["job_dir"] / "warp_tiltseries",
                "warp_settings": base_paths["job_dir"] / "warp_tiltseries.settings",  # DESTINATION
                "tomostar_dir": base_paths["job_dir"] / "tomostar",  # DESTINATION
                # UPSTREAM SOURCES:
                "upstream_warp_dir": align_job_dir / "warp_tiltseries",      # Source for XML files
                "upstream_settings": align_job_dir / "warp_tiltseries.settings",  # Source for settings
                "upstream_tomostar": align_job_dir / "tomostar",             # Source for tomostar
            }
            
        # In the _resolve_job_paths_standardized method for TS_RECONSTRUCT:

        elif job_type == JobType.TS_RECONSTRUCT:
            # Find the tsCtf job that this depends on
            # tsctf_job_num = self._find_upstream_job_number(JobType.TS_CTF.value, selected_jobs, job_number)
            # tsctf_job_dir = project_dir / JobCategory.EXTERNAL.value / f"job{tsctf_job_num:03d}"
            
            tsctf_job_dir = self._get_upstream_job_dir(JobType.TS_CTF, selected_jobs, job_number, project_dir)
            return {
                **base_paths,
                "input_star"   : tsctf_job_dir / "ts_ctf_tilt_series.star",
                "output_star"  : base_paths["job_dir"] / "tomograms.star",
                "warp_dir"     : base_paths["job_dir"] / "warp_tiltseries",
                "warp_settings": base_paths["job_dir"] / "warp_tiltseries.settings",   # DESTINATION
                "tomostar_dir" : base_paths["job_dir"] / "tomostar",                   # DESTINATION
                # UPSTREAM SOURCES:
                "upstream_warp_dir": tsctf_job_dir / "warp_tiltseries",      # Source for XML files
                "upstream_settings": tsctf_job_dir / "warp_tiltseries.settings",  # Source for settings
                "upstream_tomostar": tsctf_job_dir / "tomostar",             # Source for tomostar
            }
        
        # Default case for other job types or unknown jobs
        else:
            return base_paths

    def _find_upstream_job_number(self, upstream_job_type: str, selected_jobs: List[str], current_job_num: int) -> int:
        """
        Find the job number of an upstream job type.
        """
        try:
            upstream_idx = selected_jobs.index(upstream_job_type)
            return upstream_idx + 1  # Jobs are 1-indexed
        except ValueError:
            raise ValueError(f"Current job requires {upstream_job_type} but it's not in selected jobs")

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
        Uses JobType enums for cleaner code.
        """
        # Convert to enum for cleaner comparisons
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
                JobType.FS_MOTION_CTF : "fs_motion_and_ctf.py",
                JobType.TS_ALIGNMENT  : "ts_alignment.py",
                JobType.TS_CTF        : "ts_ctf.py",
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

    def _get_upstream_job_dir(self, upstream_job_type: JobType, selected_jobs: List[str], current_job_num: int, project_dir: Path) -> Path:
        """
        Helper method to get the directory of an upstream job.
        """
        upstream_job_num = self._find_upstream_job_number(upstream_job_type.value, selected_jobs, current_job_num)
        
        # Determine the category for the upstream job
        if upstream_job_type == JobType.IMPORT_MOVIES:
            category = JobCategory.IMPORT
        else:
            category = JobCategory.EXTERNAL
            
        return project_dir / category.value / f"job{upstream_job_num:03d}"

    def _create_scheme_star(self, scheme_dir: Path, scheme_name: str, selected_jobs: List[str]):
        """
        Create the scheme.star file that defines the pipeline flow.
        (No changes to this method)
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
