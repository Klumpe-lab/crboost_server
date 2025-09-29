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


    def _generate_scheme_data(self, scheme_name: str, selected_jobs: List[str]) -> Dict[str, pd.DataFrame]:
            """Programmatically creates the DataFrames for a new scheme.star file."""
            # General Info
            general_df = pd.DataFrame([{
                'rlnSchemeName': f'Schemes/{scheme_name}/', # Correctly sets the relative path
                'rlnSchemeCurrentNodeName': 'START',
            }])
            
            # Jobs List
            jobs_df = pd.DataFrame({
                'rlnSchemeJobName': [f'{job}/' for job in selected_jobs], # Job names need the trailing slash here
                'rlnSchemeJobNameOriginal': [f'{job}/' for job in selected_jobs],
            })
            
            # Edges
            ## THE FIX: Node names in the edges list must NOT have a trailing slash.
            nodes = ['START'] + selected_jobs + ['EXIT'] 
            edges = []
            for i in range(len(nodes) - 1):
                edges.append({
                    'rlnSchemeEdgeInputNodeName': nodes[i],
                    'rlnSchemeEdgeOutputNodeName': nodes[i+1],
                })
            edges_df = pd.DataFrame(edges)

            return {
                'scheme_general': general_df,
                'scheme_jobs': jobs_df,
                'scheme_edges': edges_df,
            }

    async def create_custom_scheme(
        self,
        project_dir: Path,
        new_scheme_name: str,
        base_template_path: Path,
        selected_jobs: List[str],
        user_params: Dict[str, Any]
    ):
        """(This function is unchanged)"""
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True)

            scheme_data = self._generate_scheme_data(new_scheme_name, selected_jobs)
            self.star_handler.write(scheme_data, new_scheme_dir / "scheme.star")

            for job_name in selected_jobs:
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                shutil.copytree(source_job_dir, dest_job_dir)
                
                job_star_path = dest_job_dir / "job.star"
                if job_star_path.exists():
                    job_data = self.star_handler.read(job_star_path)
                    if 'joboptions_values' in job_data:
                        params_df = job_data['joboptions_values']
                        for key, value in user_params.items():
                            if key in params_df['rlnJobOptionVariable'].values:
                                params_df.loc[params_df['rlnJobOptionVariable'] == key, 'rlnJobOptionValue'] = str(value)
                        job_data['joboptions_values'] = params_df
                        self.star_handler.write(job_data, job_star_path)
            
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}


    # In services/pipeline_orchestrator_service.py

    async def initialize_and_schedule_pipeline(self, project_dir: Path, scheme_dir: Path, job_names: List[str]):
        """
        Initializes a project and schedules jobs one-by-one, correctly linking their inputs and outputs.
        This is a robust alternative to the problematic --pipeline flag.
        """
        # --- 1. Initialize the Relion Project (This part is working correctly) ---
        pipeline_star_path = project_dir / "default_pipeline.star"
        if not pipeline_star_path.exists():
            print("Initializing project with Relion (async, non-blocking)...")
            init_command = "relion --tomo --do_projdir ."
            process = await asyncio.create_subprocess_shell(
                init_command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                cwd=project_dir
            )
            max_wait_seconds = 15
            waited_seconds = 0
            while not pipeline_star_path.exists() and waited_seconds < max_wait_seconds:
                await asyncio.sleep(0.5)
                waited_seconds += 0.5
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            if not pipeline_star_path.exists():
                return {"success": False, "error": f"Failed to create default_pipeline.star after {max_wait_seconds} seconds."}
            print(f"`default_pipeline.star` created in {waited_seconds:.1f} seconds.")

        # --- 2. Schedule Jobs One by One with Correct I/O Linking ---
        last_job_output_star = ""

        for i, job_name in enumerate(job_names):
            print(f"--- Scheduling Job {i+1}/{len(job_names)}: {job_name} ---")
            job_star_path = (scheme_dir / job_name / "job.star").resolve()

            command = f"relion_pipeliner --addJobFromStar {job_star_path}"

            # If this is not the first job, find its input parameter and link it to the previous output
            if i > 0 and last_job_output_star:
                job_data = self.star_handler.read(job_star_path)
                params_df = job_data.get('joboptions_values')
                if params_df is None:
                    return {"success": False, "error": f"job.star for {job_name} is missing 'joboptions_values' table."}


                # Find the correct input parameter (e.g., in_mics, in_tiltseries, etc.)
                input_param = params_df[params_df['rlnJobOptionVariable'].str.startswith('in_')]

                if not input_param.empty:
                    input_param_name = input_param.iloc[0]['rlnJobOptionVariable']
                    # Use --addJobOptions to override the input from the template file
                    command += f" --addJobOptions \"{input_param_name}='{last_job_output_star}'\""
                    print(f"Linking input '{input_param_name}' to previous output '{last_job_output_star}'")
                else:
                    print(f"WARNING: Could not find an 'in_' parameter for job {job_name} to link I/O.")

            print(f"RUNNING COMMAND: {command}")
            result = await self.backend.run_shell_command(command, cwd=project_dir)

            if not result["success"]:
                error_msg = result.get('error', 'Unknown Error')
                return {"success": False, "error": f"Failed to schedule job {job_name}: {error_msg}"}

            # After scheduling, find out what the output file will be for the next job
            await asyncio.sleep(0.5)  # Give Relion time to update the master pipeline file
            try:
                pipeline_data = self.star_handler.read(pipeline_star_path)
                last_process = pipeline_data['pipeline_processes'].iloc[-1]
                job_folder = last_process['rlnPipeLineProcessName']  # e.g., "Import/job001/"

                # Determine output filename from config or by job type convention
                output_filename = self.config_service.get_job_output_filename(job_name)
                last_job_output_star = f"{job_folder}{output_filename}"
                print(f"Registered output for {job_name} as: {last_job_output_star}")
            except Exception as e:
                return {"success": False, "error": f"Could not determine output for scheduled job {job_name}: {e}"}

        return {"success": True, "message": f"All {len(job_names)} jobs scheduled successfully."}


    ## REMOVED: All of the following methods are no longer needed with the new workflow.
    # - _get_last_scheduled_job_output (Input piping is handled by Relion automatically)
    # - schedule_pipeline_from_scheme (Replaced by `initialize_and_schedule_pipeline`)
    # - _ensure_default_pipeline_exists (Replaced by the simple headless init command)
    # - _schedule_all_jobs_from_scheme (Logic is now inside `initialize_and_schedule_pipeline`)
    # - _create_minimal_pipeline_star (No longer need to create this manually)

    async def start_pipeline(self, project_dir: Path, command: str) -> Dict[str, Any]:
        """(This function is largely unchanged but simplified for clarity)"""
        print(f"[ORCHESTRATOR] Starting pipeline execution in: {project_dir}")
        print(f"[ORCHESTRATOR] Executing command: {command}")
        
        # A quick check to ensure there's something to run
        pipeline_star = project_dir / "default_pipeline.star"
        if not pipeline_star.exists():
            error_msg = "Cannot start pipeline: default_pipeline.star not found."
            print(f"[ORCHESTRATOR] ERROR: {error_msg}")
            return {"success": False, "error": error_msg}
        
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            print(f"[ORCHESTRATOR] Pipeline process started with PID: {process.pid}")
            asyncio.create_task(self._monitor_pipeline(process, project_dir))
            
            return {
                "success": True, 
                "message": f"Pipeline started successfully (PID: {process.pid})",
                "pid": process.pid
            }
        except Exception as e:
            error_msg = f"Failed to start pipeline: {str(e)}"
            print(f"[ORCHESTRATOR] ERROR: {error_msg}")
            return {"success": False, "error": error_msg}

    async def _monitor_pipeline(self, process, project_dir: Path):
        """(This function is unchanged)"""
        print(f"[MONITOR] Starting to monitor pipeline PID {process.pid}")
        
        try:
            stdout, stderr = await process.communicate()
            
            if stdout:
                print(f"[MONITOR] Pipeline stdout:\n{stdout.decode(errors='ignore')}")
            if stderr:
                print(f"[MONITOR] Pipeline stderr:\n{stderr.decode(errors='ignore')}")
            
            if process.returncode == 0:
                print(f"[MONITOR] Pipeline completed successfully for {project_dir}")
            else:
                print(f"[MONITOR] Pipeline failed with return code {process.returncode}")
        except Exception as e:
            print(f"[MONITOR] Error monitoring pipeline: {e}")