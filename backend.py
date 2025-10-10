# backend.py

import asyncio
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from models import Job, User
import pandas as pd

from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService

HARDCODED_USER = User(username="artem.kushner")

class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        # --- NEW: Load container path from environment variable ---
        self.relion_container_path = os.getenv("CRYOBOOST_RELION_CONTAINER")
        if not self.relion_container_path:
            raise ValueError("CRITICAL: CRYOBOOST_RELION_CONTAINER environment variable is not set.")
        if not Path(self.relion_container_path).exists():
             print(f"WARNING: Container path does not exist: {self.relion_container_path}")

        relion_bin_path = "/users/artem.kushner/dev/relion/build/bin"
        os.environ['PATH'] = f"{relion_bin_path}:{os.environ['PATH']}"
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs: Dict[str, Job] = {}
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)

    async def debug_container_environment(self, project_dir: Path):
        """Debug what environment the container is actually using"""
        test_commands = [
            "which python",
            "python --version", 
            "python -c \"import sys; print(sys.path)\"",
            "python -c \"import numpy; print(numpy.__file__)\"",
            "python -c \"import tomography_python_programs; print('SUCCESS: tomography_python_programs imported')\"",
            "echo $PATH",
            "echo $PYTHONPATH"
        ]
        
        for cmd in test_commands:
            print(f"\n=== Testing: {cmd} ===")
            result = await self.run_shell_command(cmd, cwd=project_dir, use_container=True)
            print(f"Success: {result['success']}")
            if result['success']:
                print(f"Output: {result['output']}")
            else:
                print(f"Error: {result['error']}")

    async def get_available_jobs(self) -> List[str]:
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs

    def _get_container_binds(self, cwd: Path) -> List[str]:
        """Get appropriate bind mounts for the container."""
        binds = [
            str(cwd or self.server_dir),  # Working directory
            str(Path.home()),  # Home directory
            "/tmp",  # Temp directory
            "/scratch",  # Scratch space if available
        ]
        
        # Add any project-specific paths
        projects_dir = self.server_dir / "projects"
        if projects_dir.exists():
            binds.append(str(projects_dir))
        
        # Add config directory
        config_dir = Path.cwd() / "config"
        if config_dir.exists():
            binds.append(str(config_dir))
        
        return list(set(binds))  # Remove duplicates

    def _run_containerized_relion(self, command: str, cwd: Path = None, additional_binds: List[str] = None):
        """Run a Relion command with forced clean environment."""
        # --- MODIFIED: Use the path from self.relion_container_path ---
        container_path = self.relion_container_path
        
        binds = self._get_container_binds(cwd)
        if additional_binds:
            binds.extend(additional_binds)
        
        bind_args = " ".join([f"--bind {bind}" for bind in binds])
        
        # Force clean environment and use container's conda environment
        clean_command = f"""
        unset PYTHONPATH
        unset PYTHONHOME
        export PATH="/opt/miniconda3/envs/relion-5.0/bin:/opt/miniconda3/bin:/opt/relion-5.0/build/bin:/usr/local/cuda-11.8/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        {command}
        """
        
        import shlex
        wrapped_command = f"bash -c {shlex.quote(clean_command)}"
        
        full_command = f"apptainer exec {bind_args} {container_path} {wrapped_command}"
        
        print(f"[CONTAINER CLEAN] Command: {full_command}")
        return full_command

    # --- MODIFIED: Added movies_glob and mdocs_glob parameters and integrated data import ---
    async def create_project_and_scheme(
        self, project_name: str, selected_jobs: List[str], movies_glob: str, mdocs_glob: str
    ):
        projects_base_dir = self.server_dir / "projects"
        project_dir = projects_base_dir / HARDCODED_USER.username / project_name
        base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        scheme_name = f"scheme_{project_name}"
        user_params = {"angpix": "1.35", "dose_rate": "1.5"}

        if project_dir.exists():
            return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

        # --- NEW: Delegate directory creation and data import to ProjectService ---
        import_prefix = f"{project_name}_"
        structure_result = await self.project_service.create_project_structure(
            project_dir, movies_glob, mdocs_glob, import_prefix
        )
        if not structure_result["success"]:
            return structure_result
        
        print(f"[BACKEND] Project structure and data import successful.")

        scheme_result = await self.pipeline_orchestrator.create_custom_scheme(
            project_dir, scheme_name, base_template_path, selected_jobs, user_params
        )
        if not scheme_result["success"]:
            return scheme_result
        
        print(f"[BACKEND] Initializing Relion project non-blockingly in {project_dir}...")
        pipeline_star_path = project_dir / "default_pipeline.star"

        # Use containerized relion
        init_command = "relion --tomo --do_projdir ."
        container_init_command = self._run_containerized_relion(init_command, project_dir)

        process = await asyncio.create_subprocess_shell(
            container_init_command,
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

    async def run_shell_command(self, command: str, cwd: Path = None, use_container: bool = False):
        """Runs a shell command, optionally containerized."""
        try:
            should_containerize = (
                use_container and 
                any(cmd in command for cmd in ['relion', 'relion_', 'python', 'conda'])
            )
            
            if should_containerize:
                print(f"[DEBUG] Containerizing command: {command}")
                final_command = self._run_containerized_relion(command, cwd)
            else:
                final_command = command
                print(f"[SHELL] Running natively: {final_command}")
            
            process = await asyncio.create_subprocess_shell(
                final_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd or self.server_dir
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120.0)
                
                print(f"[DEBUG] Process completed with return code: {process.returncode}")
                if process.returncode == 0:
                    return {"success": True, "output": stdout.decode(), "error": None}
                else:
                    return {"success": False, "output": stdout.decode(), "error": stderr.decode()}
                    
            except asyncio.TimeoutError:
                print(f"[ERROR] Command timed out after 120 seconds: {final_command}")
                process.terminate()
                await process.wait()
                return {"success": False, "output": "", "error": "Command execution timed out"}
                
        except Exception as e:
            print(f"[ERROR] Exception in run_shell_command: {e}")
            return {"success": False, "output": "", "error": str(e)}

    # ... (the rest of the backend.py file remains the same)
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