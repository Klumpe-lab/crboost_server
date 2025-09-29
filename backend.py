# backend.py (Updated)

import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from models import Job, User
import pandas as pd

# Import the new services
from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService

movies_glob = "/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer"
mdocs_glob = "/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc"


class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs: Dict[str, Job] = {}
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)

    # --- NEW UI-FACING METHODS FOR DYNAMIC WORKFLOW ---


    async def get_available_jobs(self) -> List[str]:
        """Scans the scheme template directory to find available job types."""
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    # --- NEW, REFACTORED WORKFLOW METHOD ---
    async def create_project_and_scheme(self, user: User, project_name: str, selected_jobs: List[str]):
        """
        The new primary function for project creation.
        1. Creates the directory structure and imports data.
        2. Creates a custom scheme from the selected jobs.
        IT DOES NOT schedule or run any jobs.
        """
        # === Hardcoded paths and parameters - MODIFY THESE AS NEEDED ===
        projects_base_dir = self.server_dir / "projects"
        project_dir = projects_base_dir / user.username / project_name
        base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        scheme_name = f"scheme_{project_name}"
        user_params = {"angpix": "1.35", "dose_rate": "1.5"} # Example params
        # =============================================================

        if project_dir.exists():
            return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

        # Step 1. Create Project directory structure and Import Data
        prefix = datetime.now().strftime("%Y%m%d_%H%M_")
        create_result = await self.project_service.create_project_structure(
            project_dir, movies_glob, mdocs_glob, prefix
        )
        if not create_result["success"]:
            return create_result

        # Step 2. Create the custom scheme on disk from the selected jobs
        scheme_result = await self.pipeline_orchestrator.create_custom_scheme(
            project_dir, scheme_name, base_template_path, selected_jobs, user_params
        )
        if not scheme_result["success"]:
            return scheme_result
        
        # REMOVED: No more scheduling. The process stops here.
        # schedule_result = await self.pipeline_orchestrator.schedule_all_jobs(...)

        return {
            "success": True,
            "message": f"Project '{project_name}' created successfully on disk.",
            "project_path": str(project_dir)
        }

    async def start_scheduled_pipeline(self, project_path: str, scheme_name: str):
        """A simple method to trigger the execution of a pre-scheduled pipeline."""
        return await self.pipeline_orchestrator.start_pipeline(Path(project_path), scheme_name)

    async def run_shell_command(self, command: str, cwd: Path = None):
        """(This function is unchanged)"""
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd or self.server_dir
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                return {"success": True, "output": stdout.decode(), "error": None}
            else:
                return {"success": False, "output": stdout.decode(), "error": stderr.decode()}
        except Exception as e:
            return {"success": False, "output": "", "error": str(e)}

    
    async def create_and_run_warp_pipeline(self, user: User, project_name: str):
        """
        A single method to orchestrate project creation, setup, and pipeline launch.
        """
        # === Hardcoded paths and parameters - MODIFY THESE AS NEEDED ===
        # Define a base directory for all user projects
        projects_base_dir = self.server_dir / "projects"
        project_dir = projects_base_dir / user.username / project_name
        
        
        # Define the scheme template to use
        scheme_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        scheme_name = "warp_tomo_prep"

        # Define any user-specific parameters to override in the job.star files
        user_params = {
            "angpix": "1.35",
            "dose_rate": "1.5"
            # Add other parameters like 'gain_path' if needed
        }
        # =============================================================
        
        if project_dir.exists():
            return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

        # 1. Create Project and Import Data
        prefix = datetime.now().strftime("%Y%m%d_%H%M_")
        create_result = await self.project_service.create_new_project(
            project_dir, movies_glob, mdocs_glob, prefix
        )
        if not create_result["success"]:
            return create_result

        # 2. Apply Scheme Template with User Parameters
        apply_result = await self.project_service.apply_scheme_template(
            project_dir, scheme_template_path, user_params
        )
        if not apply_result["success"]:
            return apply_result

        # 3. Schedule the Jobs
        schedule_result = await self.pipeline_orchestrator.schedule_all_jobs(project_dir, scheme_name)
        if not schedule_result["success"]:
            return schedule_result

        # 4. Start the Pipeline
        start_result = await self.pipeline_orchestrator.start_pipeline(project_dir, scheme_name)
        if not start_result["success"]:
            return start_result
        
        return {"success": True, "message": f"Project '{project_name}' created and pipeline started successfully!"}

    # --- Existing methods for job tracking and cluster info ---
    
    async def get_slurm_info(self):
        """(This function is unchanged)"""
        return await self.run_shell_command("sinfo")

    async def submit_test_gpu_job(self, user: User):
        """(This function is unchanged)"""
        script_path = self.jobs_dir / 'test_gpu_job.sh'
        output_dir = self.server_dir / 'user_jobs' / user.username / f'test_{uuid.uuid4().hex[:8]}'
        return await self.submit_slurm_job(user, script_path, output_dir, "g", "--gpus=1")
    
    async def submit_slurm_job(self, user: User, script_path: Path, output_dir: Path, partition: str, gpus: str):
        """(This function is unchanged)"""
        if not script_path.exists():
            return {"success": False, "error": f"Script not found: {script_path}"}
        
        output_dir.mkdir(parents=True, exist_ok=True)
        log_out_path = output_dir / f"job_%j.out"
        log_err_path = output_dir / f"job_%j.err"
        command = (
            f"sbatch --partition={partition} {gpus} --output={log_out_path} "
            f"--error={log_err_path} {script_path}"
        )
        result = await self.run_shell_command(command, cwd=output_dir)
        if not result["success"]:
            return result
        
        try:
            slurm_job_id = int(result['output'].strip().split()[-1])
        except (ValueError, IndexError):
            return {"success": False, "error": f"Could not parse SLURM job ID from: {result['output']}"}
        
        job = Job(
            owner=user.username,
            slurm_id=slurm_job_id,
            log_file=output_dir / f"job_{slurm_job_id}.out",
            log_content=f"Submitted job {slurm_job_id}. Waiting for output...\n"
        )
        self.active_jobs[job.internal_id] = job
        asyncio.create_task(self.track_job_logs(job.internal_id))
        return {"success": True, "job": job}

    async def track_job_logs(self, internal_job_id: str):
        """(This function is unchanged)"""
        job = self.active_jobs.get(internal_job_id)
        if not job: return
        terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"}
        last_read_position = 0
        while True:
            status_result = await self.run_shell_command(f"squeue -j {job.slurm_id} -h -o %T")
            job.status = status_result["output"].strip() if status_result["success"] and status_result["output"].strip() else "COMPLETED"
            
            if job.log_file.exists():
                try:
                    with open(job.log_file, 'r', encoding='utf-8') as f:
                        f.seek(last_read_position)
                        new_content = f.read()
                        if new_content:
                            job.log_content += new_content
                            last_read_position = f.tell()
                except Exception as e:
                    job.log_content += f"\n--- ERROR READING LOG: {e} ---\n"
            if job.status in terminal_states:
                break
            await asyncio.sleep(5)

    def get_job_log(self, user: User, internal_job_id: str) -> Optional[Job]:
        """(This function is unchanged)"""
        job = self.active_jobs.get(internal_job_id)
        if job and job.owner == user.username:
            return job
        return None

    def get_user_jobs(self, user: User) -> List[Job]:
        """(This function is unchanged)"""
        return [job for job in self.active_jobs.values() if job.owner == user.username]

    async def schedule_pipeline(self, user: User, project_path: str, scheme_name: str):
        """
        Initializes the Relion project and schedules all jobs from the specified scheme.
        """
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}

        # The custom scheme was created inside the project directory
        scheme_dir = project_dir / "Schemes" / scheme_name
        if not scheme_dir.is_dir():
            return {"success": False, "error": f"Scheme directory not found: {scheme_dir}"}
            
        scheme_star_path = scheme_dir / "scheme.star"
        if not scheme_star_path.exists():
            return {"success": False, "error": "scheme.star not found!"}
        
        scheme_data = self.pipeline_orchestrator.star_handler.read(scheme_star_path)
        edges_df = scheme_data.get('scheme_edges')
        if edges_df is None or edges_df.empty:
            return {"success": False, "error": "Scheme file has no job edges."}
        
        # This reads the jobs in the correct, user-defined order from START to EXIT
        job_names = edges_df['rlnSchemeEdgeOutputNodeName'].iloc[0:-1].tolist()
        
        if not job_names:
            return {"success": False, "error": "No valid jobs found in the scheme file."}

        # Call the corrected orchestrator method... (the rest of the function is the same)
        return await self.pipeline_orchestrator.initialize_and_schedule_pipeline(
            project_dir, scheme_dir, job_names
        )
    
        

    async def start_pipeline(self, user: User, project_path: str, scheme_name: str):
        """
        Executes the pre-scheduled pipeline.
        """
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}
        command = "relion_pipeliner --RunJobs"
        return await self.pipeline_orchestrator.start_pipeline(project_dir, command)
        
    # In backend.py, add this method inside the CryoBoostBackend class
    async def get_pipeline_progress(self, project_path: str):
        """Reads the pipeline star file and returns the progress status."""
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {"status": "not_found"}

        try:
            # Use the star_handler from your existing orchestrator service
            data = self.pipeline_orchestrator.star_handler.read(pipeline_star)
            processes = data.get('pipeline_processes', pd.DataFrame())
            
            if processes.empty:
                return {"status": "ok", "total": 0, "completed": 0, "running": 0, "failed": 0, "is_complete": True}

            total = len(processes)
            succeeded = (processes['rlnPipeLineProcessStatusLabel'] == 'Succeeded').sum()
            running = (processes['rlnPipeLineProcessStatusLabel'] == 'Running').sum()
            failed = (processes['rlnPipeLineProcessStatusLabel'] == 'Failed').sum()
            
            # Pipeline is complete if no jobs are currently running and it's not the initial empty state
            is_complete = (running == 0 and total > 0)

            return {
                "status": "ok",
                "total": total,
                "completed": int(succeeded),
                "running": int(running),
                "failed": int(failed),
                "is_complete": is_complete,
            }
        except Exception as e:
            print(f"[BACKEND] Error reading pipeline progress for {project_path}: {e}")
            return {"status": "error", "message": str(e)}