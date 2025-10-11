# services/pipeline_orchestrator_service.py (SIMPLIFIED)

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

    async def create_custom_scheme(self, project_dir, new_scheme_name, base_template_path, selected_jobs, user_params):
        """Create scheme - same as before but no computing parameter mess"""
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)

            base_scheme_name = base_template_path.name

            # Copy job directories
            for job_name in selected_jobs:
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                job_star_path = dest_job_dir / "job.star"
                if job_star_path.exists():
                    job_data = self.star_handler.read(job_star_path)

                    # Replace scheme references
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
                    
                    # Update user-defined parameters
                    if 'joboptions_values' in job_data:
                        params_df = job_data['joboptions_values']
                        for key, value in user_params.items():
                            if key in params_df['rlnJobOptionVariable'].values:
                                params_df.loc[params_df['rlnJobOptionVariable'] == key, 'rlnJobOptionValue'] = str(value)
                        job_data['joboptions_values'] = params_df
                    
                    self.star_handler.write(job_data, job_star_path)

            # Create scheme.star (same as before)
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

    async def schedule_and_run_manually(self, project_dir: Path, scheme_name: str, selected_jobs: List[str]):
        """Just run the schemer - qsub scripts are already pre-populated!"""
        
        pipeline_star_path = project_dir / "default_pipeline.star"
        if not pipeline_star_path.exists():
            return {"success": False, "error": "Cannot start: default_pipeline.star not found."}

        print("--- Starting relion_schemer with PRE-POPULATED qsub scripts ---")
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
            
            return {"success": True, "message": f"Workflow started (PID: {process.pid})", "pid": process.pid}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _monitor_schemer(self, process, project_dir):
        """Monitor the relion_schemer process"""
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