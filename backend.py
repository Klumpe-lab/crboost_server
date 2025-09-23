import asyncio
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from models import Job, User

class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs: Dict[str, Job] = {}

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

    async def get_slurm_info(self):
        """(This function is unchanged)"""
        return await self.run_shell_command("sinfo")


    async def submit_slurm_job(self, user: User, script_path: Path, output_dir: Path, partition: str, gpus: str):
        if not script_path.exists():
            return {"success": False, "error": f"Script not found: {script_path}"}
        
        output_dir.mkdir(parents=True, exist_ok=True)
        log_out_path = output_dir / f"job_%j.out"
        log_err_path = output_dir / f"job_%j.err"

        command = (
            f"sbatch "
            f"--partition={partition} "
            f"{gpus} "
            f"--output={log_out_path} "
            f"--error={log_err_path} "
            f"{script_path}"
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
            log_content=f"Submitted job {slurm_job_id} to partition '{partition}'. Waiting for output...\n"
        )
        
        self.active_jobs[job.internal_id] = job
        
        asyncio.create_task(self.track_job_logs(job.internal_id))
        return {"success": True, "job": job}

    async def submit_test_gpu_job(self, user: User):
        """Submits a test job, now associated with a user."""
        script_path = self.jobs_dir / 'test_gpu_job.sh'
        output_dir = self.server_dir / 'user_jobs' / user.username / f'test_{uuid.uuid4().hex[:8]}'
        
        return await self.submit_slurm_job(
            user=user,
            script_path=script_path,
            output_dir=output_dir,
            partition="g",
            gpus="--gpus=1"
        )

    async def track_job_logs(self, internal_job_id: str):
        job = self.active_jobs.get(internal_job_id)
        if not job: return

        slurm_id = job.slurm_id
        log_file = job.log_file
        last_read_position = 0
        terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"}

        while True:
            squeue_cmd = f"squeue -j {slurm_id} -h -o %T"
            status_result = await self.run_shell_command(squeue_cmd)
            
            if status_result["success"] and status_result["output"].strip():
                job.status = status_result["output"].strip()
            else:
                job.status = "COMPLETED" 

            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        f.seek(last_read_position)
                        new_content = f.read()
                        if new_content:
                            job.log_content += new_content
                            last_read_position = f.tell()
                except Exception as e:
                    job.log_content += f"\n--- ERROR READING LOG: {e} ---\n"

            if job.status in terminal_states:
                break

            await asyncio.sleep(2)

    def get_job_log(self, user: User, internal_job_id: str) -> Optional[Job]:
        job = self.active_jobs.get(internal_job_id)
        if job and job.owner == user.username:
            return job
        return None 

    def get_user_jobs(self, user: User) -> List[Job]:
        return [job for job in self.active_jobs.values() if job.owner == user.username]