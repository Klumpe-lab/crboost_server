import asyncio
import uuid
from pathlib import Path

class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs = {}

    async def run_shell_command(self, command: str, cwd: Path = None):
        """Execute a shell command and return output."""
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
        """Fetches SLURM sinfo."""
        return await self.run_shell_command("sinfo")

    async def submit_slurm_job(self, script_path: Path, output_dir: Path, partition: str, gpus: str):
        """
        Submits a generic SLURM job with a specified output directory.
        """
        if not script_path.exists():
            return {"success": False, "error": f"Script not found: {script_path}"}
        
        # Create the output directory for logs
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define log file paths
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

        internal_job_id = str(uuid.uuid4())
        job_info = {
            "slurm_id": slurm_job_id,
            "status": "SUBMITTED",
            "log_file": output_dir / f"job_{slurm_job_id}.out",
            "log_content": f"Submitted job {slurm_job_id} to partition '{partition}'. Waiting for output...\n"
        }
        self.active_jobs[internal_job_id] = job_info
        
        asyncio.create_task(self.track_job_logs(internal_job_id))
        
        return {"success": True, "internal_job_id": internal_job_id, "slurm_job_id": slurm_job_id}

    async def submit_test_gpu_job(self):
        """Submits the specific test GPU job."""
        script_path = self.jobs_dir / 'test_gpu_job.sh'
        
        # Each test job gets its own directory
        output_dir = self.server_dir / 'test_job_outputs' / f'test_{uuid.uuid4().hex[:8]}'

        return await self.submit_slurm_job(
            script_path=script_path,
            output_dir=output_dir,
            partition="g",
            gpus="--gpus=1"
        )

    async def track_job_logs(self, internal_job_id: str):
        job_info = self.active_jobs.get(internal_job_id)
        if not job_info: return

        log_file = job_info["log_file"]
        
        while not log_file.exists():
            await asyncio.sleep(2)

        job_info["status"] = "RUNNING"
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                while True:
                    squeue_result = await self.run_shell_command(f"squeue -j {job_info['slurm_id']}")
                    
                    line = f.readline()
                    if not line:
                        # If no new line, check if the job is still running
                        if str(job_info['slurm_id']) not in squeue_result['output']:
                            job_info["status"] = "COMPLETED"
                            break
                        await asyncio.sleep(2) # Wait for new content
                        continue
                    
                    job_info["log_content"] += line
        except Exception as e:
            job_info["log_content"] += f"\n--- ERROR READING LOG FILE: {e} ---\n"
            job_info["status"] = "ERROR"

    def get_job_log(self, internal_job_id: str):
        return self.active_jobs.get(internal_job_id)