# In services/pipeline_orchestrator_service.py (REPLACE THE ENTIRE FILE)

import asyncio
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

from .starfile_service import StarfileService
from .config_service import get_config_service
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend import CryoBoostBackend

class PipelineOrchestratorService:
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()

    # In services/pipeline_orchestrator_service.py (add this method)

    async def _generate_run_submit_script(self, job_dir: Path, job_type: str, command: str):
        """Generate the run_submit.script with proper computing parameters"""
        from .computing_service import ComputingService
        
        computing_service = ComputingService()
        default_partition = computing_service.get_default_partition(job_type)
        comp_params = computing_service.get_computing_params(job_type, default_partition)
        
        # Read the qsub template
        qsub_template_path = Path.cwd() / "config" / "qsub" / "qsub_cbe_warp.sh"
        with open(qsub_template_path, 'r') as f:
            template_content = f.read()
        
        # Replace placeholders
        script_content = template_content.replace("XXXcommandXXX", command)
        script_content = script_content.replace("XXXthreadsXXX", str(comp_params.get("nr_threads", 8)))
        
        # Replace extra parameters
        for i in range(1, 6):
            placeholder = f"XXXextra{i}XXX"
            param_name = f"qsub_extra{i}"
            if param_name in comp_params:
                script_content = script_content.replace(placeholder, str(comp_params[param_name]))
            else:
                # Remove the line if parameter not found
                import re
                script_content = re.sub(f".*{placeholder}.*\n?", "", script_content)
        
        # Set output files
        script_content = script_content.replace("XXXoutfileXXX", f"{job_dir}/run.out")
        script_content = script_content.replace("XXXerrfileXXX", f"{job_dir}/run.err")
        
        # Write the script
        run_script_path = job_dir / "run_submit.script"
        with open(run_script_path, 'w') as f:
            f.write(script_content)
        
        return run_script_path
    async def create_custom_scheme(self, project_dir, new_scheme_name, base_template_path, selected_jobs, user_params):
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)

            base_scheme_name = base_template_path.name  # This will be "warp_tomo_prep"

            # Copy and modify job directories
            for job_name in selected_jobs:
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                job_star_path = dest_job_dir / "job.star"
                if job_star_path.exists():
                    job_data = self.star_handler.read(job_star_path)

                    # --- NEW ROBUST FIX: Find and replace all lingering references ---
                    print(f"[ORCHESTRATOR] Replacing '{base_scheme_name}' with '{new_scheme_name}' in {job_name}/job.star")
                    for block_name, block_data in job_data.items():
                        if isinstance(block_data, pd.DataFrame):
                            for col in block_data.select_dtypes(include=['object']):
                                if block_data[col].str.contains(base_scheme_name).any():
                                    block_data[col] = block_data[col].str.replace(base_scheme_name, new_scheme_name, regex=False)
                        elif isinstance(block_data, dict):
                            for key, value in block_data.items():
                                if isinstance(value, str):
                                    block_data[key] = value.replace(base_scheme_name, new_scheme_name)
                    # --- END OF FIX ---
                    
                    # Update user-defined parameters
                    if 'joboptions_values' in job_data:
                        params_df = job_data['joboptions_values']
                        for key, value in user_params.items():
                            if key in params_df['rlnJobOptionVariable'].values:
                                params_df.loc[params_df['rlnJobOptionVariable'] == key, 'rlnJobOptionValue'] = str(value)
                        job_data['joboptions_values'] = params_df
                    
                    self.star_handler.write(job_data, job_star_path)

            # Create the main scheme.star file
            scheme_general_data = {
                'rlnSchemeName': [f'Schemes/{new_scheme_name}/'],
                'rlnSchemeCurrentNodeName': ['WAIT'],
            }
            scheme_general_df = pd.DataFrame(scheme_general_data)

            scheme_floats_data = {
                'rlnSchemeFloatVariableName': ['do_at_most', 'maxtime_hr', 'wait_sec'],
                'rlnSchemeFloatVariableValue': [500.0, 48.0, 180.0],
                'rlnSchemeFloatVariableResetValue': [500.0, 48.0, 180.0]
            }
            scheme_floats_df = pd.DataFrame(scheme_floats_data)

            scheme_operators_data = {
                'rlnSchemeOperatorName': ['EXIT', 'EXIT_maxtime', 'WAIT'],
                'rlnSchemeOperatorType': ['exit', 'exit_maxtime', 'wait'],
                'rlnSchemeOperatorOutput': ['undefined', 'undefined', 'undefined'],
                'rlnSchemeOperatorInput1': ['undefined', 'maxtime_hr', 'wait_sec'],
                'rlnSchemeOperatorInput2': ['undefined', 'undefined', 'undefined']
            }
            scheme_operators_df = pd.DataFrame(scheme_operators_data)

            scheme_jobs_data = {
                'rlnSchemeJobNameOriginal': selected_jobs,
                'rlnSchemeJobName': selected_jobs,
                'rlnSchemeJobMode': ['continue'] * len(selected_jobs),
                'rlnSchemeJobHasStarted': [0] * len(selected_jobs)
            }
            scheme_jobs_df = pd.DataFrame(scheme_jobs_data)

            edges = []
            edges.append({'rlnSchemeEdgeInputNodeName': 'WAIT', 
                          'rlnSchemeEdgeOutputNodeName': 'EXIT_maxtime',
                          'rlnSchemeEdgeIsFork': 0,
                          'rlnSchemeEdgeOutputNodeNameIfTrue': 'undefined',
                          'rlnSchemeEdgeBooleanVariable': 'undefined'})
            
            edges.append({'rlnSchemeEdgeInputNodeName': 'EXIT_maxtime',
                          'rlnSchemeEdgeOutputNodeName': selected_jobs[0],
                          'rlnSchemeEdgeIsFork': 0,
                          'rlnSchemeEdgeOutputNodeNameIfTrue': 'undefined', 
                          'rlnSchemeEdgeBooleanVariable': 'undefined'})

            for i in range(len(selected_jobs) - 1):
                edges.append({'rlnSchemeEdgeInputNodeName': selected_jobs[i],
                              'rlnSchemeEdgeOutputNodeName': selected_jobs[i+1],
                              'rlnSchemeEdgeIsFork': 0,
                              'rlnSchemeEdgeOutputNodeNameIfTrue': 'undefined',
                              'rlnSchemeEdgeBooleanVariable': 'undefined'})

            edges.append({'rlnSchemeEdgeInputNodeName': selected_jobs[-1],
                          'rlnSchemeEdgeOutputNodeName': 'EXIT',
                          'rlnSchemeEdgeIsFork': 0,
                          'rlnSchemeEdgeOutputNodeNameIfTrue': 'undefined',
                          'rlnSchemeEdgeBooleanVariable': 'undefined'})

            scheme_edges_df = pd.DataFrame(edges)

            scheme_star_data = {
                'scheme_general': scheme_general_df,
                'scheme_floats': scheme_floats_df,
                'scheme_operators': scheme_operators_df,
                'scheme_jobs': scheme_jobs_df,
                'scheme_edges': scheme_edges_df
            }

            scheme_star_path = new_scheme_dir / "scheme.star"
            self.star_handler.write(scheme_star_data, scheme_star_path)
            print(f"[ORCHESTRATOR] Created complete scheme file at: {scheme_star_path}")

            return {"success": True}
        except Exception as e:
            print(f"[ORCHESTRATOR] ERROR creating custom scheme: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}


    async def schedule_and_run_manually(self, project_dir: Path, scheme_name: str, job_names: List[str]):
        """Simply run the schemer - it will handle all job creation and scheduling."""
        
        pipeline_star_path = project_dir / "default_pipeline.star"
        if not pipeline_star_path.exists():
            return {"success": False, "error": "Cannot start: default_pipeline.star not found."}

        print("--- Starting relion_schemer to manage workflow execution ---")
        scheme_folder_name = scheme_name.split('/')[-1].rstrip('/')
        run_command = f"relion_schemer --scheme {scheme_folder_name} --run --verb 2"
        
        try:
            process = await asyncio.create_subprocess_shell(
                self.backend._run_containerized_relion(run_command, project_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            self.active_schemer_process = process
            print(f"[ORCHESTRATOR] relion_schemer started with PID: {process.pid}")
            asyncio.create_task(self._monitor_schemer(process, project_dir))
            
            await self.debug_job_creation(project_dir)
            return {"success": True, "message": f"Workflow started (PID: {process.pid})", "pid": process.pid}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _monitor_schemer(self, process, project_dir):
        """Monitor the relion_schemer process and pipeline progress"""
        print(f"[MONITOR] Starting to monitor relion_schemer PID {process.pid}")
        
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
        print(f"[MONITOR] relion_schemer completed with return code: {process.returncode}")
    # services/pipeline_orchestrator_service.py (add this method for debugging)

    async def debug_job_creation(self, project_dir: Path):
        """Debug what happens when relion_schemer creates jobs"""
        print(f"[DEBUG] Checking project directory: {project_dir}")
        
        # Check if qsub templates exist
        qsub_dir = project_dir / "qsub"
        if qsub_dir.exists():
            print(f"[DEBUG] Qsub directory exists with files: {list(qsub_dir.glob('*.sh'))}")
        else:
            print("[DEBUG] Qsub directory does not exist!")
        
        # Check what happens when schemer runs
        scheme_name = "scheme_extras"  # You'll need to get this dynamically
        scheme_dir = project_dir / "Schemes" / scheme_name
        
        print(f"[DEBUG] Scheme directory: {scheme_dir}")
        if scheme_dir.exists():
            for job_dir in scheme_dir.iterdir():
                if job_dir.is_dir():
                    print(f"[DEBUG] Job template: {job_dir.name}")
                    job_star = job_dir / "job.star"
                    if job_star.exists():
                        job_data = self.star_handler.read(job_star)
                        if 'joboptions_values' in job_data:
                            params = job_data['joboptions_values']
                            qsub_script = params[params['rlnJobOptionVariable'] == 'qsubscript']['rlnJobOptionValue'].values
                            if len(qsub_script) > 0:
                                print(f"[DEBUG] Job {job_dir.name} uses qsub script: {qsub_script[0]}")