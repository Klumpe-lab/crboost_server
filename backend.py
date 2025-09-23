# backend.py (Updated)

import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from models import Job, User

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
        # For now, we hardcode the warp_tomo_prep scheme
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        
        # Return sorted list of folder names that are jobs, excluding scheme.star
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    async def create_project_with_custom_scheme(self, user: User, project_name: str, selected_jobs: List[str]):
        """
        Creates a project with a dynamically generated scheme based on user's job selection.
        """
        # === Hardcoded paths and parameters - MODIFY THESE AS NEEDED ===
        projects_base_dir = self.server_dir / "projects"
        project_dir = projects_base_dir / user.username / project_name
        base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        # The new scheme will be named after the project
        scheme_name = f"scheme_{project_name}"
        user_params = {"angpix": "1.35", "dose_rate": "1.5"}
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

        # 2. Dynamically create a custom scheme for the selected jobs
        # This is a new method we'll add to the orchestrator service
        scheme_result = await self.pipeline_orchestrator.create_custom_scheme(
            project_dir, scheme_name, base_template_path, selected_jobs, user_params
        )
        if not scheme_result["success"]:
            return scheme_result

        # 3. Schedule the jobs from the new custom scheme
        schedule_result = await self.pipeline_orchestrator.schedule_all_jobs(project_dir, scheme_name)
        if not schedule_result["success"]:
            return schedule_result

        return {
            "success": True, 
            "message": "Project created and jobs scheduled successfully. Ready to run.",
            "project_info": {
                "path": str(project_dir),
                "scheme_name": scheme_name,
                "project_name": project_name
            }
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