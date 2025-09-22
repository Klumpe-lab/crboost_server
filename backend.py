import asyncio
import uuid
from pathlib import Path

class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs = {}

    async def run_shell_command(self, command: str, cwd: Path = None):
        # ... (this function is unchanged)
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
        # ... (this function is unchanged)
        return await self.run_shell_command("sinfo")

    async def submit_slurm_job(self, script_path: Path, output_dir: Path, partition: str, gpus: str):
        # ... (this function is unchanged)
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

        internal_job_id = str(uuid.uuid4())
        job_info = {
            "slurm_id": slurm_job_id,
            "status": "PENDING", # Default status is now PENDING
            "log_file": output_dir / f"job_{slurm_job_id}.out",
            "log_content": f"Submitted job {slurm_job_id} to partition '{partition}'. Waiting for output...\n"
        }
        self.active_jobs[internal_job_id] = job_info
        
        asyncio.create_task(self.track_job_logs(internal_job_id))
        
        return {"success": True, "internal_job_id": internal_job_id, "slurm_job_id": slurm_job_id}

    async def submit_test_gpu_job(self):
        # ... (this function is unchanged)
        script_path = self.jobs_dir / 'test_gpu_job.sh'
        output_dir = self.server_dir / 'test_job_outputs' / f'test_{uuid.uuid4().hex[:8]}'
        return await self.submit_slurm_job(
            script_path=script_path,
            output_dir=output_dir,
            partition="g",
            gpus="--gpus=1"
        )

    async def track_job_logs(self, internal_job_id: str):
        """
        Periodically checks job status via squeue and reads new log output
        in a non-blocking way.
        """
        job_info = self.active_jobs.get(internal_job_id)
        if not job_info: return

        slurm_id = job_info['slurm_id']
        log_file = job_info["log_file"]
        last_read_position = 0
        terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"}

        while True:
            # 1. Get job status from squeue
            squeue_cmd = f"squeue -j {slurm_id} -h -o %T"
            status_result = await self.run_shell_command(squeue_cmd)
            
            current_status = ""
            if status_result["success"] and status_result["output"].strip():
                current_status = status_result["output"].strip()
                job_info["status"] = current_status
            else:
                # If job is no longer in squeue, it's likely finished
                job_info["status"] = "COMPLETED" 

            # 2. Read new log content without blocking
            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        f.seek(last_read_position)
                        new_content = f.read()
                        if new_content:
                            job_info["log_content"] += new_content
                            last_read_position = f.tell()
                except Exception as e:
                    job_info["log_content"] += f"\n--- ERROR READING LOG: {e} ---\n"

            # 3. Exit loop if job is in a terminal state
            if job_info["status"] in terminal_states:
                break

            await asyncio.sleep(2) # Non-blocking sleep

    def get_job_log(self, internal_job_id: str):
        return self.active_jobs.get(internal_job_id)