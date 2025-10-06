# backend.py

import asyncio
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from models import Job, User
import pandas as pd

from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService

movies_glob = "/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer"
mdocs_glob = "/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc"

HARDCODED_USER = User(username="artem.kushner")

class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs: Dict[str, Job] = {}
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)

    async def get_available_jobs(self) -> List[str]:
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    async def create_project_and_scheme(self, project_name: str, selected_jobs: List[str]):
        projects_base_dir = self.server_dir / "projects"
        project_dir = projects_base_dir / HARDCODED_USER.username / project_name
        base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        scheme_name = f"scheme_{project_name}"
        user_params = {"angpix": "1.35", "dose_rate": "1.5"}

        if project_dir.exists():
            return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "qsub").mkdir(exist_ok=True)
        
        qsub_source_dir = Path.cwd() / "config" / "qsub"
        qsub_dest_dir = project_dir / "qsub"
        shutil.copytree(qsub_source_dir, qsub_dest_dir, dirs_exist_ok=True)
        print(f"[BACKEND] Copied submission scripts to {qsub_dest_dir}")

        scheme_result = await self.pipeline_orchestrator.create_custom_scheme(
            project_dir, scheme_name, base_template_path, selected_jobs, user_params
        )
        if not scheme_result["success"]:
            return scheme_result
        
        print(f"[BACKEND] Initializing Relion project non-blockingly in {project_dir}...")
        pipeline_star_path = project_dir / "default_pipeline.star"
        
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
            await process.wait()
            print(f"[BACKEND] Relion GUI process terminated after {waited_seconds:.1f}s.")
        except ProcessLookupError:
            pass

        if not pipeline_star_path.exists():
            return {"success": False, "error": f"Failed to create default_pipeline.star after {max_wait_seconds} seconds."}

        return {
            "success": True,
            "message": f"Project '{project_name}' created and initialized successfully.",
            "project_path": str(project_dir)
        }

    async def run_shell_command(self, command: str, cwd: Path = None):
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

    async def get_slurm_info(self):
        return await self.run_shell_command("sinfo")

    async def submit_test_gpu_job(self):
        script_path = self.jobs_dir / 'test_gpu_job.sh'
        output_dir = self.server_dir / 'user_jobs' / HARDCODED_USER.username / f'test_{uuid.uuid4().hex[:8]}'
        return await self.submit_slurm_job(script_path, output_dir, "g", "--gpus=1")

    async def submit_slurm_job(self, script_path: Path, output_dir: Path, partition: str, gpus: str):
        if not script_path.exists():
            return {"success": False, "error": f"Script not found: {script_path}"}
        
        output_dir.mkdir(parents=True, exist_ok=True)
        log_out_path = output_dir / f"job_%j.out"
        log_err_path = output_dir / f"job_%j.err"
        command = f"sbatch --partition={partition} {gpus} --output={log_out_path} --error={log_err_path} {script_path}"
        
        result = await self.run_shell_command(command, cwd=output_dir)
        if not result["success"]:
            return result
        
        try:
            slurm_job_id = int(result['output'].strip().split()[-1])
        except (ValueError, IndexError):
            return {"success": False, "error": f"Could not parse SLURM job ID from: {result['output']}"}
        
        job = Job(
            owner=HARDCODED_USER.username,
            slurm_id=slurm_job_id,
            log_file=output_dir / f"job_{slurm_job_id}.out",
            log_content=f"Submitted job {slurm_job_id}. Waiting for output...\n"
        )
        self.active_jobs[job.internal_id] = job
        asyncio.create_task(self.track_job_logs(job.internal_id))
        return {"success": True, "job": job}

    async def track_job_logs(self, internal_job_id: str):
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

    def get_job_log(self, internal_job_id: str) -> Optional[Job]:
        job = self.active_jobs.get(internal_job_id)
        if job and job.owner == HARDCODED_USER.username:
            return job
        return None

    def get_user_jobs(self) -> List[Job]:
        return [job for job in self.active_jobs.values() if job.owner == HARDCODED_USER.username]

    async def start_pipeline(self, project_path: str, scheme_name: str, selected_jobs: List[str]):
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}
            
        return await self.pipeline_orchestrator.schedule_and_run_manually(
            project_dir, scheme_name, selected_jobs
        )

    async def get_pipeline_progress(self, project_path: str):
        pipeline_star = Path(project_path) / "default_pipeline.star"
        if not pipeline_star.exists():
            return {"status": "not_found"}

        try:
            data = self.pipeline_orchestrator.star_handler.read(pipeline_star)
            processes = data.get('pipeline_processes', pd.DataFrame())
            
            if processes.empty:
                return {"status": "ok", "total": 0, "completed": 0, "running": 0, "failed": 0, "is_complete": True}

            total = len(processes)
            succeeded = (processes['rlnPipeLineProcessStatusLabel'] == 'Succeeded').sum()
            running = (processes['rlnPipeLineProcessStatusLabel'] == 'Running').sum()
            failed = (processes['rlnPipeLineProcessStatusLabel'] == 'Failed').sum()
            
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