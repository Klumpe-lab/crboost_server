# backend.py

import asyncio
import os
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from models import Job, User
import pandas as pd

from services.config_service import get_config_service
from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService
from services.container_service import get_container_service

HARDCODED_USER = User(username="artem.kushner")

class CryoBoostBackend:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.jobs_dir = self.server_dir / 'jobs'
        self.active_jobs: Dict[str, Job] = {}
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()

    async def get_available_jobs(self) -> List[str]:
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs
    
    async def create_project_and_scheme(
        self, project_name: str, project_base_path: str, selected_jobs: List[str], movies_glob: str, mdocs_glob: str
    ):
        project_dir = Path(project_base_path).expanduser() / project_name
        base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        scheme_name = f"scheme_{project_name}"
        user_params = {"angpix": "1.35", "dose_rate": "1.5"}

        if project_dir.exists():
            return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

        import_prefix = f"{project_name}_"
        structure_result = await self.project_service.create_project_structure(
            project_dir, movies_glob, mdocs_glob, import_prefix
        )
        if not structure_result["success"]:
            return structure_result
        
        print(f"[BACKEND] Project structure and data import successful.")
        
        # Collect all unique parent directories for container binding
        additional_bind_paths = {
            str(Path(project_base_path).expanduser().resolve()),
            str(Path(movies_glob).parent.resolve()),
            str(Path(mdocs_glob).parent.resolve())
        }
        
        scheme_result = await self.pipeline_orchestrator.create_custom_scheme(
            project_dir, scheme_name, base_template_path, selected_jobs, user_params,
            additional_bind_paths=list(additional_bind_paths)
        )
        if not scheme_result["success"]:
            return scheme_result
        
        print(f"[BACKEND] Initializing Relion project in {project_dir}...")
        pipeline_star_path = project_dir / "default_pipeline.star"

        init_command = "unset DISPLAY && relion --tomo --do_projdir ."
        
        container_init_command = self.container_service.wrap_command(
            command=init_command,
            cwd=project_dir,
            additional_binds=list(additional_bind_paths)
        )

        process = await asyncio.create_subprocess_shell(
            container_init_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_dir
        )
        
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30.0)
            if process.returncode != 0:
                print(f"[RELION INIT ERROR] {stderr.decode()}")
        except asyncio.TimeoutError:
            print("[ERROR] Relion project initialization timed out.")
            process.kill()
            await process.wait()

        print(f"[BACKEND] Relion project initialization finished.")

        if not pipeline_star_path.exists():
            return {"success": False, "error": f"Failed to create default_pipeline.star."}

        return {
            "success": True,
            "message": f"Project '{project_name}' created and initialized successfully.",
            "project_path": str(project_dir)
        }

    async def run_shell_command(self, command: str, cwd: Path = None, use_container: bool = False, additional_binds: List[str] = None):
        """Runs a shell command, optionally using the centralized container service."""
        try:
            if use_container:
                print(f"[DEBUG] Containerizing command: {command}")
                final_command = self.container_service.wrap_command(
                    command=command,
                    cwd=cwd or self.server_dir,
                    additional_binds=additional_binds or []
                )
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

    async def get_slurm_info(self):
        return await self.run_shell_command("sinfo")

    async def debug_container_environment(self, project_dir: Path):
        """Debug what environment the container is actually using"""
        test_commands = [
            # Test basic container recognition
            "relion --version",
            "relion_python_tomo_import --help",
            "WarpTools --help",
            
            # Now test Python inside container
            "relion_python_tomo_import --help && which python",
            "relion_python_tomo_import --help && python -c \"import sys; print(sys.executable)\"",
            
            # Test if we can import the required modules
            "relion_python_tomo_import --help && python -c \"import mdocfile; print('mdocfile OK')\"",
            "relion_python_tomo_import --help && python -c \"import pandas; print('pandas OK')\"",
            
            # Test the actual import command that's failing
            f"cd {project_dir} && relion_python_tomo_import SerialEM --tilt-image-movie-pattern './frames/*.eer' --mdoc-file-pattern './mdoc/*.mdoc' --help"
        ]
        
        print(f"\n=== DEBUG CONTAINER ENVIRONMENT ===")
        print(f"Project dir: {project_dir}")
        
        for i, cmd in enumerate(test_commands):
            print(f"\n--- Test {i+1}: {cmd} ---")
            result = await self.run_shell_command(cmd, cwd=project_dir, use_container=True)
            print(f"Success: {result['success']}")
            if result['success']:
                print(f"Output: {result['output']}...")  # First 500 chars
            else:
                print(f"Error: {result['error']}...")  # First 500 chars
    async def start_pipeline(self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]):
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}
        
        bind_paths = {str(Path(p).parent.resolve()) for p in required_paths if p}
        bind_paths.add(str(project_dir.parent.resolve()))
            
        return await self.pipeline_orchestrator.schedule_and_run_manually(
            project_dir, scheme_name, selected_jobs, additional_bind_paths=list(bind_paths)
        )

    # def _get_container_binds(self, cwd: Path) -> List[str]:
    #         """Get appropriate bind mounts for the container, including HPC and X11."""
    #         binds = [
    #             str(cwd or self.server_dir),  # Working directory
    #             str(Path.home()),            # Home directory
    #             "/tmp",                      # Temp directory
    #             "/scratch",                  # Scratch space if available
    #         ]
            
    #         # Add project-specific and config paths
    #         projects_dir = self.server_dir / "projects"
    #         if projects_dir.exists():
    #             binds.append(str(projects_dir))
                
    #         config_dir = Path.cwd() / "config"
    #         if config_dir.exists():
    #             binds.append(str(config_dir))

    #         # HPC integration binds for Slurm
    #         hpc_binds = [
    #             "/usr/bin", "/usr/lib64/slurm", "/run/munge",
    #             "/etc/passwd", "/etc/group",
    #             "/groups", "/programs", "/software",
    #         ]

    #         x11_authority = Path.home() / ".Xauthority"
    #         x11_socket = Path("/tmp/.X11-unix")
            
    #         if x11_authority.exists():
    #             binds.append(f"{x11_authority}:{x11_authority}:ro")
    #         if x11_socket.exists():
    #             binds.append(str(x11_socket))

    #         print("[DEBUG] Checking for HPC bind paths...")
    #         for path_str in hpc_binds:
    #             path = Path(path_str)
    #             if path.exists():
    #                 if "passwd" in path_str or "group" in path_str:
    #                     binds.append(f"{path_str}:{path_str}:ro")
    #                 else:
    #                     binds.append(path_str)
                
    #         return list(set(binds))

    # def _run_containerized_relion(self, command: str, cwd: Path = None, additional_binds: List[str] = None):
    #     """
    #     Builds and returns the EXACT apptainer command string based on the user's
    #     provided working shell script template.
    #     """
    #     import os
    #     import shlex

    #     container_path = self.relion_container_path
    #     home_dir = str(Path.home())
    #     display_var = os.getenv('DISPLAY', ':0.0')

    #     SLURM_BIN_DIR = "/usr/bin" 

    #     args = [
    #         "apptainer", "exec",
    #         f"--bind {cwd}", f"--bind {home_dir}", f"--bind {self.server_dir / 'projects'}",
    #         f"--bind {Path.cwd() / 'config'}", "--bind /scratch-cbe", "--bind /programs",
    #         "--bind /groups", "--bind /software",
   
    #         # You must bind the host's /usr/bin so the container can find sbatch, squeue, etc.
    #         "--bind /usr/bin:/usr/bin",
    #         f"--env DISPLAY={display_var}",
    #         "--bind /tmp/.X11-unix/:/tmp/.X11-unix",
    #         f"--bind {home_dir}/.Xauthority:/root/.Xauthority:ro",
    #         "--bind /usr/lib64/slurm:/usr/lib64/slurm",
    #         "--bind /usr/lib64/slurm/libslurmfull.so:/usr/lib64/slurm/libslurmfull.so",
    #         "--bind /run/munge:/run/munge",
    #         "--bind /usr/bin/munge:/usr/bin/munge",
    #         "--bind /usr/bin/unmunge:/usr/bin/unmunge",
    #         "--bind /etc/passwd:/etc/passwd:ro",
    #         "--bind /etc/group:/etc/group:ro",
    #     ]

    #     args.append(container_path)

    #     inner_command = f"""
    #     unset PYTHONPATH
    #     unset PYTHONHOME
    #     export PATH="{SLURM_BIN_DIR}:/opt/miniconda3/envs/relion-5.0/bin:/opt/miniconda3/bin:/opt/relion-5.0/build/bin:/usr/local/cuda-11.8/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    #     {command}
    #     """
        
    #     wrapped_command = f"bash -c {shlex.quote(inner_command)}"
    #     args.append(wrapped_command)
    #     full_command = " ".join(args)
    #     print(f"[CONTAINER TEMPLATE] Command: {full_command}")
    #     return full_command

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