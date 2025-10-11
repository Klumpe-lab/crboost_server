# services/pipeline_orchestrator_service.py

import asyncio
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional

from .starfile_service import StarfileService
from .config_service import get_config_service
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend import CryoBoostBackend

class PipelineOrchestratorService:
    """
    Orchestrates the creation and execution of Relion pipelines.
    This service now correctly places the entire generated command into `fn_exe`.
    """
    
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.active_schemer_process: Optional[asyncio.subprocess.Process] = None

    def _build_warp_fs_motion_ctf_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """
        Builds the direct WarpTools command string for Frame Series Motion & CTF Correction.
        """
        print("🚀 [BUILDER] Building fsMotionAndCtf command...")
        
        # Part 1: `WarpTools create_settings`
        frame_folder = "../../frames"
        frame_extension = "*.eer"
        output_settings_file = "./warp_frameseries.settings" # Relative to the job's output dir
        
        create_settings_parts = [
            "WarpTools create_settings",
            f"--folder_data {frame_folder}",
            f"--extension '{frame_extension}'",
            f"--folder_processing warp_frameseries",
            f"--output {output_settings_file}",
            f"--angpix {user_params.get('angpix', 1.35)}",
        ]
        
        if 'eer_fractions' in params:
            create_settings_parts.append(f"--eer_ngroups -{params['eer_fractions']}")

        # Part 2: `WarpTools fs_motion_and_ctf`
        voltage = user_params.get('voltage', 300)
        cs = user_params.get('cs', 2.7)
        amplitude = user_params.get('amplitude', 0.07)

        m_min, m_max = params.get('m_range_min_max', '500:10').split(':')
        c_min, c_max = params.get('c_range_min_max', '30:4').split(':')
        defocus_min, defocus_max = params.get('c_defocus_min_max', '0.5:8').split(':')

        run_main_parts = [
            "WarpTools fs_motion_and_ctf",
            f"--settings {output_settings_file}",
            f"--m_grid {params.get('m_grid', '1x1x3')}",
            f"--m_range_min {m_min}",
            f"--m_range_max {m_max}",
            f"--m_bfac {params.get('m_bfac', -500)}",
            f"--c_grid {params.get('c_grid', '1x1x1')}",
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

        if params.get('out_average_halves', False):
            run_main_parts.append("--out_average_halves")
        if params.get('c_use_sum', False):
            run_main_parts.append("--c_use_sum")

        full_command = " && ".join([" ".join(create_settings_parts), " ".join(run_main_parts)])
        print(f"✅ [BUILDER] Generated command: {full_command}")
        return full_command

    def _build_warp_ts_alignment_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        print("⚠️ [BUILDER] tsAlignment command builder is not yet implemented.")
        return "echo 'tsAlignment job not implemented yet'; exit 1;"

    def _build_command_for_job(self, job_name: str, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """Dispatcher function to call the correct command builder based on the job name."""
        job_builders = {
            'fsMotionAndCtf': self._build_warp_fs_motion_ctf_command,
            'tsAlignment': self._build_warp_ts_alignment_command,
        }
        
        builder = job_builders.get(job_name)
        
        if builder:
            return builder(params, user_params)
        else:
            # This is the message that caused the original error. It will now be correctly quoted.
            return f"echo 'ERROR: Job type \"{job_name}\" does not have a direct command builder yet.'; exit 1;"

    async def create_custom_scheme(self, project_dir: Path, new_scheme_name: str, base_template_path: Path, selected_jobs: List[str], user_params: Dict[str, Any]):
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
                
                params_dict = pd.Series(
                    params_df.rlnJobOptionValue.values,
                    index=params_df.rlnJobOptionVariable
                ).to_dict()

                for i in range(1, 11):
                    label_key = f'param{i}_label'
                    value_key = f'param{i}_value'
                    if label_key in params_dict and params_dict.get(label_key):
                        param_name = params_dict[label_key]
                        param_value = params_dict.get(value_key)
                        params_dict[param_name] = param_value
                
                direct_command = self._build_command_for_job(job_name, params_dict, user_params)

                # 🎯 FIX: Assign the entire command string to `fn_exe` and clear `other_args`.
                # The `starfile` library will automatically handle quoting the command string.
                params_df.loc[params_df['rlnJobOptionVariable'] == 'fn_exe', 'rlnJobOptionValue'] = direct_command
                params_df.loc[params_df['rlnJobOptionVariable'] == 'other_args', 'rlnJobOptionValue'] = ''

                # Clean up now-redundant `paramX` entries for clarity.
                params_to_remove = [f'param{i}_{s}' for i in range(1, 11) for s in ['label', 'value']]
                cleanup_mask = ~params_df['rlnJobOptionVariable'].isin(params_to_remove)
                job_data['joboptions_values'] = params_df[cleanup_mask].reset_index(drop=True)

                print(f"Updating scheme name from '{base_scheme_name}' to '{new_scheme_name}' in {job_name}/job.star")
                for block_name, block_data in job_data.items():
                    if isinstance(block_data, pd.DataFrame):
                        for col in block_data.select_dtypes(include=['object']):
                            if block_data[col].str.contains(base_scheme_name).any():
                                block_data[col] = block_data[col].str.replace(base_scheme_name, new_scheme_name, regex=False)
                
                self.star_handler.write(job_data, job_star_path)

            # --- Create the main scheme.star file (logic unchanged) ---
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
            print(f"✅ Created complete scheme file at: {scheme_star_path}")

            return {"success": True}
        except Exception as e:
            print(f"❌ ERROR creating custom scheme: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def schedule_and_run_manually(self, project_dir: Path, scheme_name: str, selected_jobs: List[str]):
        pipeline_star_path = project_dir / "default_pipeline.star"
        if not pipeline_star_path.exists():
            return {"success": False, "error": "Cannot start: default_pipeline.star not found."}

        print("--- Starting relion_schemer with direct-command jobs ---")
        run_command = f"relion_schemer --scheme {scheme_name} --run --verb 2"
        
        try:
            process = await asyncio.create_subprocess_shell(
                self.backend._run_containerized_relion(run_command, project_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            self.active_schemer_process = process
            print(f"Pipeline started. `relion_schemer` PID: {process.pid}")
            asyncio.create_task(self._monitor_schemer(process, project_dir))
            
            return {"success": True, "message": f"Workflow started (PID: {process.pid})", "pid": process.pid}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _monitor_schemer(self, process: asyncio.subprocess.Process, project_dir: Path):
        print(f"👀 [MONITOR] Starting to watch relion_schemer PID {process.pid}")
        
        async def read_stream(stream, callback):
            while True:
                line = await stream.readline()
                if not line:
                    break
                callback(line.decode().strip())
        
        def handle_stdout(line):
            print(f"[SCHEMER] {line}")
            
        def handle_stderr(line):
            print(f"[SCHEMER-ERR] {line}")
        
        await asyncio.gather(
            read_stream(process.stdout, handle_stdout),
            read_stream(process.stderr, handle_stderr)
        )
        
        await process.wait()
        print(f"🏁 [MONITOR] relion_schemer PID {process.pid} completed with return code: {process.returncode}")
        self.active_schemer_process = None