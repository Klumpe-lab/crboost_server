# services/pipeline_orchestrator_service.py

import asyncio
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
import json

from pydantic import BaseModel
from services.commands_builder import (
    ImportMoviesCommandBuilder,  # We still need this for the simple job
    BaseCommandBuilder,
)
from services.parameter_models import (
    AcquisitionParams,
    ComputingParams,
    ImportMoviesParams,
    FsMotionCtfParams,
    TsAlignmentParams,
    AlignmentMethod,
)

from .starfile_service import StarfileService
from .config_service import get_config_service
# We NO LONGER need container_service here
# from .container_service import get_container_service 

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend import CryoBoostBackend


class PipelineOrchestratorService:
    """
    Orchestrates the creation and execution of Relion pipelines.
    """

    def __init__(self, backend_instance: "CryoBoostBackend"):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        # self.container_service = get_container_service() # <-- REMOVED

        # Map job names to their corresponding builder class
        # Only simple, non-wrapper jobs remain here.
        self.job_builders: Dict[str, BaseCommandBuilder] = {
            "importmovies": ImportMoviesCommandBuilder(),
        }

    # _get_job_tool is NO LONGER needed here
    # def _get_job_tool(self, ...): ...

    def _get_job_paths(
        self,
        job_name: str,
        job_index: int,
        selected_jobs: List[str],
        acquisition_params: AcquisitionParams,
        project_dir: Path,
        job_dir: Path,
    ) -> Dict[str, Path]:
        """Construct the relative input/output paths for a job"""
        paths = {}

        def get_job_dir_by_name(name: str) -> Optional[str]:
            try:
                idx = selected_jobs.index(name)
                return f"Import/job{idx + 1:03d}" if name == "importmovies" else f"External/job{idx + 1:03d}"
            except ValueError:
                return None
        
        if job_name == "importmovies":
            paths["input_dir"] = Path(f"../../mdoc")
            paths["output_dir"] = Path(".")
            paths["pipeline_control"] = Path(".")
        
        elif job_name == "fsMotionAndCtf":
            import_dir = get_job_dir_by_name("importmovies")
            if import_dir:
                paths["input_star"] = Path(f"../{import_dir}/movies.star")
            paths["output_star"] = Path("fs_motion_and_ctf.star")
            paths["warp_output_dir"] = Path("./warp_frameseries")
            if acquisition_params.gain_reference_path:
                paths["gain_reference"] = Path(acquisition_params.gain_reference_path)
        
        elif job_name == "tsAlignment":
            motion_dir = get_job_dir_by_name("fsMotionAndCtf")
            if motion_dir:
                paths["input_star"] = Path(f"../{motion_dir}/fs_motion_and_ctf.star")
                paths["frameseries_dir"] = Path(f"../{motion_dir}/warp_frameseries")
            paths["mdoc_dir"] = Path(f"../../mdoc")
            paths["tomostar_dir"] = Path("tomostar")
            paths["processing_dir"] = Path("warp_tiltseries")
            paths["settings_file"] = Path("warp_tiltseries.settings")
            paths["output_star"] = Path("aligned_tilt_series.star")
        
        return paths

    def _build_job_command(
        self, job_name: str, job_model: BaseModel, paths: Dict[str, Path]
    ) -> str:
        """
        Dispatcher function to call the correct command builder.
        NOTE: This is now only used for simple, non-wrapper jobs.
        """
        builder = self.job_builders.get(job_name)
        if builder:
            try:
                return builder.build(job_model, paths)
            except Exception as e:
                print(f"[PIPELINE ERROR] Failed to build command for {job_name}: {e}")
                return f"echo 'ERROR: Failed to build command for {job_name}: {e}'; exit 1;"
        else:
            return f"echo 'ERROR: Job type \"{job_name}\" not implemented'; exit 1;"

    async def create_custom_scheme(
        self,
        project_dir: Path,
        new_scheme_name: str,
        base_template_path: Path,
        selected_jobs: List[str],
        additional_bind_paths: List[str],
    ):
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)
            base_scheme_name = base_template_path.name

            # Get the server root dir (assumes this file is in services/)
            server_dir = Path(__file__).parent.parent.resolve()
            
            # This is the *only* bind path we need to pass to the JSON
            # The driver script will read this and pass it to ContainerService
            all_binds = list(set(additional_bind_paths + [str(server_dir)]))
            print(f"[PIPELINE] Bind paths to be used by drivers: {all_binds}")

            for job_index, job_name in enumerate(selected_jobs):
                job_number_str = f"job{job_index + 1:03d}"

                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                job_star_path = dest_job_dir / "job.star"
                if not job_star_path.exists():
                    continue

                job_data = self.star_handler.read(job_star_path)
                params_df = job_data.get("joboptions_values")
                if params_df is None:
                    continue

                job_model = self.backend.app_state.jobs.get(job_name)
                if not job_model:
                    print(f"[PIPELINE WARNING] Job {job_name} not in state, skipping")
                    continue
                
                job_dir_name = "Import" if job_name == "importmovies" else "External"
                job_run_dir = project_dir / job_dir_name / job_number_str

                paths = self._get_job_paths(
                    job_name,
                    job_index,
                    selected_jobs,
                    self.backend.app_state.acquisition,
                    project_dir,
                    job_run_dir,
                )

                # --- THIS IS THE NEW LOGIC ---
                final_command_for_fn_exe = ""
                
                # Define the *host* python executable
                host_python_exe = server_dir / "venv" / "bin" / "python3"
                if not host_python_exe.exists():
                    print(f"[PIPELINE ERROR] VENV Python not found at {host_python_exe}")
                    host_python_exe = "python3" # Fallback
                
                # This env var setup ensures the driver can import 'services'
                env_setup = f"export PYTHONPATH={server_dir}:${{PYTHONPATH}};"

                if job_name == "importmovies":
                    # Simple job. We need to wrap it in container OURSELVES here.
                    # We need the container service for this one-off.
                    from .container_service import get_container_service
                    container_svc = get_container_service()
                    tool_name = "relion_import"
                    
                    # Build the *inner* command
                    raw_command_import = self._build_job_command(job_name, job_model, paths)
                    
                    # Build the *full container* command
                    final_command_for_fn_exe = container_svc.wrap_command_for_tool(
                        command=raw_command_import,
                        cwd=job_run_dir.resolve(),
                        tool_name=tool_name,
                        additional_binds=all_binds,
                    )

                elif job_name == "fsMotionAndCtf":
                    driver_script_path = server_dir / "drivers" / "fs_motion_and_ctf.py"
                    final_command_for_fn_exe = f"{env_setup} {host_python_exe} {driver_script_path}"
                    
                elif job_name == "tsAlignment":
                    driver_script_path = server_dir / "drivers" / "ts_alignment.py"
                    final_command_for_fn_exe = f"{env_setup} {host_python_exe} {driver_script_path}"
                
                # ... Add other elif blocks for new drivers here ...
                
                else:
                    final_command_for_fn_exe = f"echo 'ERROR: Job type \"{job_name}\" not implemented with drivers'; exit 1;"
                # --- END NEW LOGIC ---

                # Serialize parameters to JSON in the *run* directory
                job_run_dir.mkdir(parents=True, exist_ok=True)
                params_json_path = job_run_dir / "job_params.json"
                
                data_to_serialize = {
                    "job_model": job_model.model_dump(),
                    "paths": {k: str(v) for k, v in paths.items()},
                    # Pass the binds to the driver
                    "additional_binds": all_binds, 
                    # Add helper strings for optional paths
                    "gain_path_str": str(getattr(job_model, "gain_path", None) or ""),
                    "gain_operations_str": str(getattr(job_model, "gain_operations", None) or ""),
                }
                
                try:
                    with open(params_json_path, 'w') as f:
                        json.dump(data_to_serialize, f, indent=2)
                    print(f"[PIPELINE] Saved job params to {params_json_path}")
                except Exception as e:
                    print(f"[PIPELINE ERROR] Failed to save {params_json_path}: {e}")

                # --- THIS IS THE KEY ---
                # Set fn_exe to our simple python command
                params_df.loc[
                    params_df["rlnJobOptionVariable"] == "fn_exe", "rlnJobOptionValue"
                ] = final_command_for_fn_exe
                
                params_df.loc[
                    params_df["rlnJobOptionVariable"] == "other_args",
                    "rlnJobOptionValue",
                ] = "" # Clear other_args

                # ... (rest of job.star cleanup and writing) ...
                params_to_remove = [
                    f"param{i}_{s}" for i in range(1, 11) for s in ["label", "value"]
                ]
                cleanup_mask = ~params_df["rlnJobOptionVariable"].isin(params_to_remove)
                job_data["joboptions_values"] = params_df[cleanup_mask].reset_index(
                    drop=True
                )
                for block_name, block_data in job_data.items():
                    if isinstance(block_data, pd.DataFrame):
                        for col in block_data.select_dtypes(include=["object"]):
                            if block_data[col].str.contains(base_scheme_name).any():
                                block_data[col] = block_data[col].str.replace(
                                    base_scheme_name, new_scheme_name, regex=False
                                )
                self.star_handler.write(job_data, job_star_path)

            # ... (rest of the function to create scheme.star) ...
            scheme_general_df = pd.DataFrame(
                {"rlnSchemeName": [f"Schemes/{new_scheme_name}/"], "rlnSchemeCurrentNodeName": ["WAIT"]}
            )
            scheme_floats_df = pd.DataFrame(
                {
                    "rlnSchemeFloatVariableName": ["do_at_most", "maxtime_hr", "wait_sec"],
                    "rlnSchemeFloatVariableValue": [500.0, 48.0, 180.0],
                    "rlnSchemeFloatVariableResetValue": [500.0, 48.0, 180.0],
                }
            )
            scheme_operators_df = pd.DataFrame(
                {
                    "rlnSchemeOperatorName": ["EXIT", "EXIT_maxtime", "WAIT"],
                    "rlnSchemeOperatorType": ["exit", "exit_maxtime", "wait"],
                    "rlnSchemeOperatorOutput": ["undefined"] * 3,
                    "rlnSchemeOperatorInput1": ["undefined", "maxtime_hr", "wait_sec"],
                    "rlnSchemeOperatorInput2": ["undefined"] * 3,
                }
            )
            scheme_jobs_df = pd.DataFrame(
                {
                    "rlnSchemeJobNameOriginal": selected_jobs,
                    "rlnSchemeJobName": selected_jobs,
                    "rlnSchemeJobMode": ["continue"] * len(selected_jobs),
                    "rlnSchemeJobHasStarted": [0] * len(selected_jobs),
                }
            )
            edges = [
                {"rlnSchemeEdgeInputNodeName": "WAIT", "rlnSchemeEdgeOutputNodeName": "EXIT_maxtime"},
                {"rlnSchemeEdgeInputNodeName": "EXIT_maxtime", "rlnSchemeEdgeOutputNodeName": selected_jobs[0]},
            ]
            for i in range(len(selected_jobs) - 1):
                edges.append(
                    {"rlnSchemeEdgeInputNodeName": selected_jobs[i], "rlnSchemeEdgeOutputNodeName": selected_jobs[i + 1]}
                )
            edges.append(
                {"rlnSchemeEdgeInputNodeName": selected_jobs[-1], "rlnSchemeEdgeOutputNodeName": "EXIT"}
            )
            scheme_edges_df = pd.DataFrame(edges)
            for df in [scheme_edges_df]:
                df["rlnSchemeEdgeIsFork"] = 0
                df["rlnSchemeEdgeOutputNodeNameIfTrue"] = "undefined"
                df["rlnSchemeEdgeBooleanVariable"] = "undefined"
            scheme_star_data = {
                "scheme_general": scheme_general_df,
                "scheme_floats": scheme_floats_df,
                "scheme_operators": scheme_operators_df,
                "scheme_jobs": scheme_jobs_df,
                "scheme_edges": scheme_edges_df,
            }
            scheme_star_path = new_scheme_dir / "scheme.star"
            self.star_handler.write(scheme_star_data, scheme_star_path)
            print(f" Created complete scheme file at: {scheme_star_path}")

            return {"success": True}
        except Exception as e:
            print(f" ERROR creating custom scheme: {e}")
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}