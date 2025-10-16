# services/pipeline_orchestrator_service.py

import asyncio
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional

from services.tool_service import get_tool_service

from .starfile_service import StarfileService
from .config_service import get_config_service
from .container_service import get_container_service
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend import CryoBoostBackend

 
# services/pipeline_orchestrator_service.py

class PipelineOrchestratorService:
    """
    Orchestrates the creation and execution of Relion pipelines.
    
    This service is responsible for:
    1.  Creating custom pipeline schemes from templates.
    2.  Building raw, executable commands for tools like WarpTools.
    3.  Using the ContainerService to wrap these raw commands into full,
        containerized `apptainer` calls.
    4.  Injecting the final containerized commands into the `fn_exe` field
        of the job.star files.
    5.  Scheduling and monitoring the pipeline execution via `relion_schemer`.
    """
    
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.container_service = get_container_service()
        self.tool_service = get_tool_service()
        
        # Map job types to tools
        self.job_tools = {
            'importmovies': 'relion_import',
            'fsMotionAndCtf': 'warptools',
            'tsAlignment': 'aretomo',
            # Add more job-to-tool mappings as needed
        }

    def _build_import_movies_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """Build Relion import command for SerialEM tilt series data"""
        
        # Extract parameters - these would typically come from the job.star file
        # For now, using reasonable defaults that match your test data
        tilt_image_pattern = "./frames/*.eer"
        mdoc_pattern = "./mdoc/*.mdoc"
        output_dir = "./Import/job001/"  # This will be dynamic based on job number
        pipeline_control = "./Import/job001/"  # Same as output_dir for pipeline control
        
        # Microscope parameters - these should come from user_params or config
        nominal_tilt_axis_angle = user_params.get('nominal_tilt_axis_angle', -95.0)
        nominal_pixel_size = user_params.get('nominal_pixel_size', 2.93)  # Angstroms
        voltage = user_params.get('voltage', 300)  # keV
        spherical_aberration = user_params.get('spherical_aberration', 2.7)  # mm
        amplitude_contrast = user_params.get('amplitude_contrast', 0.1)
        dose_per_tilt_image = user_params.get('dose_per_tilt_image', 3)  # e-/Å²
        
        # Build the command according to the help output format
        command_parts = [
            "relion_python_tomo_import SerialEM",
            f"--tilt-image-movie-pattern '{tilt_image_pattern}'",
            f"--mdoc-file-pattern '{mdoc_pattern}'",
            f"--nominal-tilt-axis-angle {nominal_tilt_axis_angle}",
            f"--nominal-pixel-size {nominal_pixel_size}",
            f"--voltage {voltage}",
            f"--spherical-aberration {spherical_aberration}",
            f"--amplitude-contrast {amplitude_contrast}",
            f"--optics-group-name ''",  # Empty string for default
            f"--dose-per-tilt-image {dose_per_tilt_image}",
            f"--output-directory {output_dir}",
            f"--pipeline_control {pipeline_control}",
        ]
        
        full_command = " ".join(command_parts)
        print(f"[PIPELINE] Built import command: {full_command}")
        return full_command

    def _build_warp_fs_motion_ctf_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """Build WarpTools motion correction and CTF estimation command"""
        frame_folder = "../../frames"
        output_settings_file = "./warp_frameseries.settings"
        folder_processing = "./warp_frameseries" 

        # Extract user parameters with defaults
        angpix = user_params.get('angpix', 1.35)
        eer_fractions = params.get('eer_fractions', 32)
        voltage = user_params.get('voltage', 300)
        cs = user_params.get('cs', 2.7)
        amplitude = user_params.get('amplitude', 0.07)

        # Extract motion correction parameters with defaults
        m_min, m_max = params.get('m_range_min_max', '500:10').split(':')
        c_min, c_max = params.get('c_range_min_max', '30:4').split(':')
        defocus_min, defocus_max = params.get('c_defocus_min_max', '0.5:8').split(':')

        # Build create_settings command
        create_settings_parts = [
            "WarpTools create_settings",
            f"--folder_data {frame_folder}",
            f"--extension '*.eer'",
            f"--folder_processing {folder_processing}",
            f"--output {output_settings_file}",
            f"--angpix {angpix}",
            f"--eer_ngroups -{eer_fractions}",
        ]

        # Build main motion/CTF command
        run_main_parts = [
            "WarpTools fs_motion_and_ctf",
            f"--settings {output_settings_file}",
            f"--m_grid {params.get('m_grid', '1x1x3')}",
            f"--m_range_min {m_min}",
            f"--m_range_max {m_max}",
            f"--m_bfac {params.get('m_bfac', -500)}",
            f"--c_grid {params.get('c_grid', '2x2x1')}",
            f"--c_window {params.get('c_window', 512)}",
            f"--c_range_min {c_min}",
            f"--c_range_max {c_max}",
            f"--c_defocus_min {defocus_min}",
            f"--c_defocus_max {defocus_max}",
            f"--c_voltage {voltage}",
            f"--c_cs {cs}",
            f"--c_amplitude {amplitude}",
            f"--perdevice {params.get('perdevice', 1)}",
            "--out_averages",
        ]

        full_command = " && ".join([" ".join(create_settings_parts), " ".join(run_main_parts)])
        print(f"[PIPELINE] Built WarpTools command: {full_command}")
        return full_command

    def _build_warp_ts_alignment_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """Build WarpTools tilt series alignment command"""
        # This would be similar structure but for tilt series alignment
        return "echo 'tsAlignment job not fully implemented yet'; exit 1;"

    def _build_command_for_job(self, job_name: str, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """Dispatcher function to call the correct command builder"""
        job_builders = {
            'importmovies': self._build_import_movies_command,
            'fsMotionAndCtf': self._build_warp_fs_motion_ctf_command,
            'tsAlignment': self._build_warp_ts_alignment_command,
        }
        
        builder = job_builders.get(job_name)
        
        if builder:
            raw_command = builder(params, user_params)
            return raw_command
        else:
            return f"echo 'ERROR: Job type \"{job_name}\" not implemented'; exit 1;"

    async def create_custom_scheme(self, project_dir: Path, new_scheme_name: str, 
                                 base_template_path: Path, selected_jobs: List[str], 
                                 user_params: Dict[str, Any], additional_bind_paths: List[str]):
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)
            base_scheme_name = base_template_path.name

            for job_name in selected_jobs:
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                job_star_path = dest_job_dir / "job.star"
                if not job_star_path.exists():
                    continue

                job_data = self.star_handler.read(job_star_path)
                params_df = job_data.get('joboptions_values')
                if params_df is None:
                    continue
                
                # Extract parameters from job.star
                params_dict = pd.Series(
                    params_df.rlnJobOptionValue.values,
                    index=params_df.rlnJobOptionVariable
                ).to_dict()

                # Get the tool for this job
                tool_name = self.job_tools.get(job_name)
                if not tool_name:
                    print(f"[PIPELINE WARNING] No tool mapping for job {job_name}, skipping containerization")
                    continue
                
                # Build the raw command for this job
                raw_command = self._build_command_for_job(job_name, params_dict, user_params)
                print(f"[PIPELINE DEBUG] Raw command for {job_name}: {raw_command}")

                # Wrap command using the job's tool
                final_containerized_command = self.container_service.wrap_command_for_tool(
                    command=raw_command,
                    cwd=project_dir,
                    tool_name=tool_name,
                    additional_binds=additional_bind_paths
                )
                print(f"[PIPELINE DEBUG] Containerized command for {job_name}: {final_containerized_command}")
                
                # Update the job.star file with the containerized command
                params_df.loc[params_df['rlnJobOptionVariable'] == 'fn_exe', 'rlnJobOptionValue'] = final_containerized_command
                params_df.loc[params_df['rlnJobOptionVariable'] == 'other_args', 'rlnJobOptionValue'] = ''

                # Clean up parameter fields
                params_to_remove = [f'param{i}_{s}' for i in range(1, 11) for s in ['label', 'value']]
                cleanup_mask = ~params_df['rlnJobOptionVariable'].isin(params_to_remove)
                job_data['joboptions_values'] = params_df[cleanup_mask].reset_index(drop=True)

                # Update scheme name references
                print(f"Updating scheme name from '{base_scheme_name}' to '{new_scheme_name}' in {job_name}/job.star")
                for block_name, block_data in job_data.items():
                    if isinstance(block_data, pd.DataFrame):
                        for col in block_data.select_dtypes(include=['object']):
                            if block_data[col].str.contains(base_scheme_name).any():
                                block_data[col] = block_data[col].str.replace(base_scheme_name, new_scheme_name, regex=False)
                
                self.star_handler.write(job_data, job_star_path)

            # Create scheme.star file (existing code remains the same)
            scheme_general_df = pd.DataFrame({'rlnSchemeName': [f'Schemes/{new_scheme_name}/'], 'rlnSchemeCurrentNodeName': ['WAIT']})
            scheme_floats_df = pd.DataFrame({
                'rlnSchemeFloatVariableName': ['do_at_most', 'maxtime_hr', 'wait_sec'],
                'rlnSchemeFloatVariableValue': [500.0, 48.0, 180.0],
                'rlnSchemeFloatVariableResetValue': [500.0, 48.0, 180.0]
            })
            scheme_operators_df = pd.DataFrame({
                'rlnSchemeOperatorName': ['EXIT', 'EXIT_maxtime', 'WAIT'],
                'rlnSchemeOperatorType': ['exit', 'exit_maxtime', 'wait'],
                'rlnSchemeOperatorOutput': ['undefined'] * 3,
                'rlnSchemeOperatorInput1': ['undefined', 'maxtime_hr', 'wait_sec'],
                'rlnSchemeOperatorInput2': ['undefined'] * 3
            })
            scheme_jobs_df = pd.DataFrame({
                'rlnSchemeJobNameOriginal': selected_jobs,
                'rlnSchemeJobName': selected_jobs,
                'rlnSchemeJobMode': ['continue'] * len(selected_jobs),
                'rlnSchemeJobHasStarted': [0] * len(selected_jobs)
            })

            edges = [{'rlnSchemeEdgeInputNodeName': 'WAIT', 'rlnSchemeEdgeOutputNodeName': 'EXIT_maxtime'}]
            edges.append({'rlnSchemeEdgeInputNodeName': 'EXIT_maxtime', 'rlnSchemeEdgeOutputNodeName': selected_jobs[0]})
            for i in range(len(selected_jobs) - 1):
                edges.append({'rlnSchemeEdgeInputNodeName': selected_jobs[i], 'rlnSchemeEdgeOutputNodeName': selected_jobs[i+1]})
            edges.append({'rlnSchemeEdgeInputNodeName': selected_jobs[-1], 'rlnSchemeEdgeOutputNodeName': 'EXIT'})

            scheme_edges_df = pd.DataFrame(edges)
            for df in [scheme_edges_df]:
                df['rlnSchemeEdgeIsFork'] = 0
                df['rlnSchemeEdgeOutputNodeNameIfTrue'] = 'undefined'
                df['rlnSchemeEdgeBooleanVariable'] = 'undefined'
            
            scheme_star_data = {
                'scheme_general': scheme_general_df, 'scheme_floats': scheme_floats_df,
                'scheme_operators': scheme_operators_df, 'scheme_jobs': scheme_jobs_df,
                'scheme_edges': scheme_edges_df
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