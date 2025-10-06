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

    async def create_custom_scheme(
        self,
        project_dir: Path,
        new_scheme_name: str,
        base_template_path: Path,
        selected_jobs: List[str],
        user_params: Dict[str, Any]
    ):
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)
            

            for job_name in selected_jobs:
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
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


    async def schedule_and_run_manually(self, project_dir: Path, scheme_name: str, job_names: List[str]):
        """
        Schedules all jobs, links them, and then starts the pipeline by directly running the FIRST job.
        """
        pipeline_star_path = project_dir / "default_pipeline.star"
        if not pipeline_star_path.exists():
            return {"success": False, "error": "Cannot start: default_pipeline.star not found."}

        last_job_output_star = ""
        first_job_directory = "" 

        for i, job_name in enumerate(job_names):
            print(f"--- Scheduling Job {i+1}/{len(job_names)}: {job_name} ---")
            
            template_job_star_path = (project_dir / "Schemes" / scheme_name / job_name / "job.star").resolve()
            command_add = f"relion_pipeliner --addJobFromStar {template_job_star_path}"
            result_add = await self.backend.run_shell_command(command_add, cwd=project_dir)
            if not result_add["success"]:
                return {"success": False, "error": f"Failed to add job {job_name}: {result_add.get('error')}"}

            await asyncio.sleep(0.5)
            pipeline_data = self.star_handler.read(pipeline_star_path)
            last_process = pipeline_data['pipeline_processes'].iloc[-1]
            
            job_run_dir_name = last_process['rlnPipeLineProcessName']
            job_run_dir = project_dir / job_run_dir_name
            
            if i == 0:
                first_job_directory = job_run_dir_name

            run_job_star_path = job_run_dir / "job.star"

            if i > 0 and last_job_output_star:
                print(f"Modifying '{run_job_star_path.name}' for I/O linking...")
                job_data = self.star_handler.read(run_job_star_path)
                params_df = job_data['joboptions_values']
                input_param_name = next((p for p in ['in_mics', 'in_mic', 'in_parts', 'in_tomos'] if p in params_df['rlnJobOptionVariable'].values), None)

                if input_param_name:
                    params_df.loc[params_df['rlnJobOptionVariable'] == input_param_name, 'rlnJobOptionValue'] = last_job_output_star
                    job_data['joboptions_values'] = params_df
                    self.star_handler.write(job_data, run_job_star_path)
                    print(f"Updated '{input_param_name}' to '{last_job_output_star}'")

            output_filename = self.config_service.get_job_output_filename(job_name)
            last_job_output_star = f"{job_run_dir_name}{output_filename}"
            print(f"Registered output for {job_name} as: {last_job_output_star}")

        # --- STEP 2 IS NOW REMOVED ---
        # We NO LONGER need to mark jobs as waiting.
        
        # --- 3. Start the pipeline by running ONLY the first job ---
        if not first_job_directory:
            return {"success": False, "error": "Could not determine the first job to run."}

        # This is the new, direct, and unambiguous run command.
        run_command = f"relion_pipeliner --run_job {first_job_directory}"
        
        print(f"[ORCHESTRATOR] Starting pipeline execution in: {project_dir}")
        print(f"[ORCHESTRATOR] Executing command: {run_command}")
        
        try:
            process = await asyncio.create_subprocess_shell(
                run_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            print(f"[ORCHESTRATOR] Pipeline process started with PID: {process.pid}")
            asyncio.create_task(self._monitor_pipeline(process))
            return {"success": True, "message": f"Pipeline started (PID: {process.pid})", "pid": process.pid}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _monitor_pipeline(self, process):
        """Monitors the stdout and stderr of the running pipeline process."""
        print(f"[MONITOR] Starting to monitor pipeline PID {process.pid}")
        stdout, stderr = await process.communicate()
        if stdout:
            print(f"[MONITOR] Pipeline stdout:\n{stdout.decode(errors='ignore')}")
        if stderr:
            print(f"[MONITOR] Pipeline stderr:\n{stderr.decode(errors='ignore')}")
        
        if process.returncode == 0:
            print(f"[MONITOR] Pipeline completed successfully.")
        else:
            print(f"[MONITOR] Pipeline failed with return code {process.returncode}")