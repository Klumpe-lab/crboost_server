# services/pipeline_orchestrator_service.py

import asyncio
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional

from pydantic import BaseModel
from services.commands_builder import (
    ImportMoviesCommandBuilder,
    FsMotionCtfCommandBuilder,
    TsAlignmentCommandBuilder,
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
from .container_service import get_container_service

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
        self.container_service = get_container_service()

        # Map job names to their corresponding builder class
        self.job_builders: Dict[str, BaseCommandBuilder] = {
            "importmovies": ImportMoviesCommandBuilder(),
            "fsMotionAndCtf": FsMotionCtfCommandBuilder(),
            "tsAlignment": TsAlignmentCommandBuilder(),
        }

    def _get_job_tool(self, job_name: str, job_model: BaseModel) -> str:
        """Get the correct tool name for container service"""
        if job_name == "importmovies":
            return "relion_import"

        if job_name == "fsMotionAndCtf":
            return "warptools"

        if job_name == "tsAlignment":
            if isinstance(job_model, TsAlignmentParams):
                if job_model.alignment_method == AlignmentMethod.ARETOMO:
                    # AreTomo is called via WarpTools wrapper in container
                    return "warptools"
                if job_model.alignment_method == AlignmentMethod.IMOD:
                    # IMOD also called via WarpTools wrapper
                    return "warptools"
                if job_model.alignment_method == AlignmentMethod.RELION:
                    return "relion"
            return "warptools"  # Default to warptools for alignment

        return "relion"

    def _get_job_paths(
        self,
        job_name: str,
        job_index: int,
        selected_jobs: List[str],
        acquisition_params: AcquisitionParams,
        project_dir: Path,  # This is the project root, e.g., /.../P1
        job_dir: Path,  # This is the job CWD, e.g., /.../P1/External/job003
    ) -> Dict[str, Path]:
        """Construct the relative input/output paths for a job"""
        paths = {}

        def get_job_dir_by_name(name: str) -> Optional[str]:
            try:
                idx = selected_jobs.index(name)
                if name == "importmovies":
                    return f"Import/job{idx + 1:03d}"
                else:
                    return f"External/job{idx + 1:03d}"
            except ValueError:
                return None

        if job_name == "importmovies":
            # CWD is Import/job001
            # We assume 'mdoc' is a sibling of the project dir, as per your example
            # paths['input_dir'] = Path(f"../../{project_dir.name}/mdoc")
            # Let's assume mdoc is *inside* project_dir for simplicity
            paths["input_dir"] = Path(f"../../mdoc")  # ../../ reaches project root
            paths["output_dir"] = Path(".")
            paths["pipeline_control"] = Path(".")

        elif job_name == "fsMotionAndCtf":
            # CWD is External/job002
            import_dir = get_job_dir_by_name("importmovies")
            if import_dir:
                paths["input_star"] = Path(f"../{import_dir}/movies.star")

            paths["output_star"] = Path("movies_mic.star")
            if acquisition_params.gain_reference_path:
                paths["gain_reference"] = Path(acquisition_params.gain_reference_path)

        elif job_name == "tsAlignment":
            # CWD is External/job003
            motion_dir = get_job_dir_by_name("fsMotionAndCtf")
            if motion_dir:
                # Input star from previous job
                paths["input_star"] = Path(f"../{motion_dir}/movies_mic.star")
                # Input frame series from previous job
                paths["frameseries_dir"] = Path(f"../{motion_dir}/warp_frameseries")

            # Path to project's mdoc directory
            # CWD is /.../Project/External/job003, we want /.../Project/mdoc
            paths["mdoc_dir"] = Path(f"../../mdoc")

            # Internal/output paths relative to CWD
            paths["tomostar_dir"] = Path("tomostar")
            paths["processing_dir"] = Path("warp_tiltseries")
            paths["settings_file"] = Path("warp_tiltseries.settings")
            paths["output_star"] = Path("aligned.star")  # Final output star

        return paths

    def _build_job_command(
        self, job_name: str, job_model: BaseModel, paths: Dict[str, Path]
    ) -> str:
        """Dispatcher function to call the correct command builder"""

        print(
            f"[PIPELINE] Building command for {job_name} with params: {job_model.model_dump()}"
        )

        builder = self.job_builders.get(job_name)

        if builder:
            try:
                command = builder.build(job_model, paths)
                print(
                    f"[PIPELINE] Built command: {command[:200]}..."
                )  # Log first 200 chars
                return command
            except Exception as e:
                print(f"[PIPELINE ERROR] Failed to build command for {job_name}: {e}")
                import traceback

                traceback.print_exc()
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

                # Get the job model from global state
                job_model = self.backend.app_state.jobs.get(job_name)
                if not job_model:
                    print(f"[PIPELINE WARNING] Job {job_name} not in state, skipping")
                    continue

                # Get the tool name
                tool_name = self._get_job_tool(job_name, job_model)
                if not tool_name:
                    print(
                        f"[PIPELINE WARNING] No tool mapping for job {job_name}, skipping containerization"
                    )
                    continue

                # Get the paths
                job_dir_name = "Import" if job_name == "importmovies" else "External"
                job_run_dir = project_dir / job_dir_name / job_number_str

                paths = self._get_job_paths(
                    job_name,
                    job_index,
                    selected_jobs,
                    self.backend.app_state.acquisition,
                    project_dir,  # Pass the project root
                    job_run_dir,  # Pass the job CWD
                )

                # Build the raw command
                raw_command = self._build_job_command(job_name, job_model, paths)

                # Wrap command with container
                # The CWD for the *container* should be the job run directory
                final_containerized_command = (
                    self.container_service.wrap_command_for_tool(
                        command=raw_command,
                        cwd=job_run_dir.resolve(),  # Use resolved absolute path for CWD
                        tool_name=tool_name,
                        additional_binds=additional_bind_paths,
                    )
                )

                # Update job.star
                params_df.loc[
                    params_df["rlnJobOptionVariable"] == "fn_exe", "rlnJobOptionValue"
                ] = final_containerized_command
                params_df.loc[
                    params_df["rlnJobOptionVariable"] == "other_args",
                    "rlnJobOptionValue",
                ] = ""

                # Clean up parameter fields
                params_to_remove = [
                    f"param{i}_{s}" for i in range(1, 11) for s in ["label", "value"]
                ]
                cleanup_mask = ~params_df["rlnJobOptionVariable"].isin(params_to_remove)
                job_data["joboptions_values"] = params_df[cleanup_mask].reset_index(
                    drop=True
                )

                # Update scheme name references
                print(
                    f"Updating scheme name from '{base_scheme_name}' to '{new_scheme_name}' in {job_name}/job.star"
                )
                for block_name, block_data in job_data.items():
                    if isinstance(block_data, pd.DataFrame):
                        for col in block_data.select_dtypes(include=["object"]):
                            if block_data[col].str.contains(base_scheme_name).any():
                                block_data[col] = block_data[col].str.replace(
                                    base_scheme_name, new_scheme_name, regex=False
                                )

                self.star_handler.write(job_data, job_star_path)

            # Create scheme.star file
            scheme_general_df = pd.DataFrame(
                {
                    "rlnSchemeName": [f"Schemes/{new_scheme_name}/"],
                    "rlnSchemeCurrentNodeName": ["WAIT"],
                }
            )
            scheme_floats_df = pd.DataFrame(
                {
                    "rlnSchemeFloatVariableName": [
                        "do_at_most",
                        "maxtime_hr",
                        "wait_sec",
                    ],
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
                {
                    "rlnSchemeEdgeInputNodeName": "WAIT",
                    "rlnSchemeEdgeOutputNodeName": "EXIT_maxtime",
                }
            ]
            edges.append(
                {
                    "rlnSchemeEdgeInputNodeName": "EXIT_maxtime",
                    "rlnSchemeEdgeOutputNodeName": selected_jobs[0],
                }
            )
            for i in range(len(selected_jobs) - 1):
                edges.append(
                    {
                        "rlnSchemeEdgeInputNodeName": selected_jobs[i],
                        "rlnSchemeEdgeOutputNodeName": selected_jobs[i + 1],
                    }
                )
            edges.append(
                {
                    "rlnSchemeEdgeInputNodeName": selected_jobs[-1],
                    "rlnSchemeEdgeOutputNodeName": "EXIT",
                }
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