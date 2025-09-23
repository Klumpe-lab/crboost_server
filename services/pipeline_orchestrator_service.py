
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
                'rlnSchemeName': f'Schemes/{scheme_name}/',
                'rlnSchemeCurrentNodeName': 'START',
            }])
            
            # Jobs List
            jobs_df = pd.DataFrame({
                'rlnSchemeJobName': [f'Schemes/{scheme_name}/{job}/' for job in selected_jobs],
                'rlnSchemeJobNameOriginal': [f'Schemes/{scheme_name}/{job}/' for job in selected_jobs],
            })
            
            # Edges (the connections)
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
        """Creates a new scheme directory with only the selected jobs and a custom scheme.star."""
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True)

            # 1. Generate the new scheme.star data and write it
            scheme_data = self._generate_scheme_data(new_scheme_name, selected_jobs)
            self.star_handler.write(scheme_data, new_scheme_dir / "scheme.star")

            # 2. Copy only the selected job folders and apply user parameters
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
    async def _get_last_scheduled_job_output(
        self,
        project_dir: Path,
        alias: str,
        job_type: str
    ) -> str:
        """
        After scheduling a job, reads default_pipeline.star to find its output path.
        """
        pipeline_star = project_dir / "default_pipeline.star"
        await asyncio.sleep(0.5) # Give Relion a moment to write the file
        
        pipeline_data = self.star_handler.read(pipeline_star)
        processes_df = pipeline_data.get('pipeline_processes')
        
        if processes_df is None or processes_df.empty:
            raise RuntimeError("Could not read pipeline_processes from default_pipeline.star")
            
        job_row = processes_df[processes_df['rlnPipeLineProcessAlias'] == alias]
        if job_row.empty:
            raise RuntimeError(f"Could not find scheduled job with alias '{alias}'")
        
        job_folder = job_row.iloc[0]['rlnPipeLineProcessJobName'] # e.g., "Import/job001/"
        output_filename = self.config_service.get_job_output_filename(job_type)
        if not output_filename:
            raise ValueError(f"No output file defined for job type '{job_type}' in config.")

        return f"{job_folder}{output_filename}"

    async def schedule_all_jobs(self, project_dir: Path, scheme_name: str) -> Dict[str, Any]:
        """Schedules all jobs defined in a scheme, connecting inputs and outputs."""
        scheme_star_path = project_dir / "Schemes" / scheme_name / "scheme.star"
        scheme_data = self.star_handler.read(scheme_star_path)
        jobs = scheme_data['scheme_edges']['rlnSchemeEdgeOutputNodeName'][1:-1].tolist()
        
        job_outputs = {}
        
        for i, job_name in enumerate(jobs):
            alias = f"{job_name}_{i+1:03d}"
            job_star_rel_path = f"Schemes/{scheme_name}/{job_name}/job.star"
            
            command = f"relion_pipeliner --addJobFromStar {job_star_rel_path} --setJobAlias {alias}"
            
            # --- This is the core pipeline connection logic ---
            if i > 0:
                prev_job_name = jobs[i-1]
                prev_job_output = job_outputs.get(prev_job_name)
                if not prev_job_output:
                    return {"success": False, "error": f"Could not find output for previous job '{prev_job_name}'"}
                
                # For this simple pipeline, we assume the input is always 'in_mic'
                job_options = f"'in_mic == {prev_job_output}'"
                command += f" --addJobOptions {job_options}"
            
            result = await self.backend.run_shell_command(command, cwd=project_dir)
            if not result["success"]:
                return {"success": False, "error": f"Failed to schedule job {job_name}: {result['error']}"}

            # Find and store the output of the job we just scheduled
            try:
                job_type = job_name.split('_')[0] # 'fsMotionAndCtf_pick' -> 'fsMotionAndCtf'
                output_path = await self._get_last_scheduled_job_output(project_dir, alias, job_type)
                job_outputs[job_name] = output_path
            except (RuntimeError, ValueError) as e:
                return {"success": False, "error": str(e)}

        return {"success": True, "message": f"Successfully scheduled {len(jobs)} jobs."}

    async def start_pipeline(self, project_dir: Path, scheme_name: str) -> Dict[str, Any]:
        """Executes relion_schemer --run to start the pipeline in the background."""
        command = f"relion_schemer --scheme {scheme_name} --run --verb 2"
        # Run this as a background task so it doesn't block the server
        asyncio.create_task(self.backend.run_shell_command(command, cwd=project_dir))
        return {"success": True, "message": "Pipeline start command issued."}