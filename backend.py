import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional
import pandas as pd
import yaml
import subprocess
import glob
import xml.etree.ElementTree as ET
from pathlib import Path

from services.config_service import get_config_service
from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService
from services.container_service import get_container_service

# --- NEW IMPORTS ---
# from services.parameters_service import get_parameter_manager # OLD
from services.parameter_manager import ParameterManagerV2  # NEW
# --- END NEW IMPORTS ---

from pydantic import BaseModel, Field
from pathlib import Path
import uuid

class User(BaseModel):
    """Represents an authenticated user."""
    username: str

HARDCODED_USER = User(username="artem.kushner")

class CryoBoostBackend:

    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()
        
        # --- PARAMETER MANAGER SWAP ---
        # self.parameter_manager = get_parameter_manager() # OLD
        self.parameter_manager = ParameterManagerV2()  # NEW
        # --- END SWAP ---

    async def get_job_parameters(self, job_name: str) -> Dict[str, Any]:
            """
            Get the parameters for a specific job, populating from
            global state and job.star defaults if not already loaded.
            """
            try:
                # This will create the job params from defaults/job.star if not exist
                job_model = self.parameter_manager.prepare_job_params(job_name)
                if job_model:
                    return {"success": True, "params": job_model.dict()}
                else:
                    return {"success": False, "error": f"Unknown job type {job_name}"}
            except Exception as e:
                print(f"[ERROR] Could not get params for job {job_name}: {e}")
                return {"success": False, "error": str(e)}

    async def get_available_jobs(self) -> List[str]:
        template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
        if not template_path.is_dir():
            return []
        jobs = sorted([p.name for p in template_path.iterdir() if p.is_dir()])
        return jobs
    
    async def create_project_and_scheme(
        self, 
        project_name: str, 
        project_base_path: str, 
        selected_jobs: List[str], 
        movies_glob: str, 
        mdocs_glob: str
    ):
        try:
            project_dir = Path(project_base_path).expanduser() / project_name
            base_template_path = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            scheme_name = f"scheme_{project_name}"
            
            # Check if project already exists
            if project_dir.exists():
                return {"success": False, "error": f"Project directory '{project_dir}' already exists."}

            # --- REMOVED LEGACY PARAMS ---
            # user_params = self.parameter_manager.get_legacy_user_params_dict() # OLD
            # print(f"[BACKEND] Using parameters: {user_params}") # OLD
            print(f"[BACKEND-V2] Using parameters from ParameterManagerV2") # NEW
            # --- END REMOVAL ---

            # Create project structure
            import_prefix = f"{project_name}_"
            structure_result = await self.project_service.create_project_structure(
                project_dir, movies_glob, mdocs_glob, import_prefix
            )

            if not structure_result["success"]:
                return structure_result
            
            params_json_path = project_dir / "project_params.json"
            try:
                # Export clean, hierarchical config (using V2 manager)
                clean_config = self.parameter_manager.export_for_project(
                    project_name=project_name,
                    movies_glob=movies_glob,
                    mdocs_glob=mdocs_glob,
                    selected_jobs=selected_jobs
                )
                
                # Save it
                with open(params_json_path, 'w') as f:
                    json.dump(clean_config, f, indent=2)
                
                print(f"[BACKEND-V2] ✓ Saved clean parameters to {params_json_path}")
                
                # Verify
                if not params_json_path.exists():
                    raise FileNotFoundError(f"Parameter file was not created at {params_json_path}")
                
                file_size = params_json_path.stat().st_size
                if file_size == 0:
                    raise ValueError(f"Parameter file is empty: {params_json_path}")
                
                print(f"[BACKEND-V2] ✓ Verified parameter file: {file_size} bytes")
                
            except Exception as e:
                print(f"[ERROR] Failed to save project_params.json: {e}")
                import traceback
                traceback.print_exc()
                return {
                    "success": False, 
                    "error": f"Project created but failed to save parameters: {str(e)}"
                }
                
            
            # Collect bind paths
            additional_bind_paths = {
                str(Path(project_base_path).expanduser().resolve()),
                str(Path(movies_glob).parent.resolve()),
                str(Path(mdocs_glob).parent.resolve())
            }
            
            # Create the scheme
            scheme_result = await self.pipeline_orchestrator.create_custom_scheme(
                project_dir, 
                scheme_name, 
                base_template_path, 
                selected_jobs, 
                # user_params, # REMOVED
                additional_bind_paths=list(additional_bind_paths)
            )
            
            if not scheme_result["success"]:
                return scheme_result
            
            # Initialize Relion project
            print(f"[BACKEND] Initializing Relion project in {project_dir}...")
            pipeline_star_path = project_dir / "default_pipeline.star"

            init_command = "unset DISPLAY && relion --tomo --do_projdir ."
            
            container_init_command = self.container_service.wrap_command_for_tool(
                command=init_command,
                cwd=project_dir,
                tool_name="relion",
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
                "project_path": str(project_dir),
                "params_file": str(params_json_path)  # Return the params file path
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": f"Project creation failed: {str(e)}"}

    async def get_initial_parameters(self) -> Dict[str, Any]:
        """Get the default parameters to populate the UI"""
        # NEW: Use get_ui_state() for backward compatibility
        return self.parameter_manager.get_ui_state()

    async def autodetect_parameters(self, mdocs_glob: str) -> Dict[str, Any]:
        """Run mdoc autodetection and return the updated state"""
        print(f"[BACKEND-V2] Autodetecting from {mdocs_glob}")
        # NEW: Use V2 method
        self.parameter_manager.update_from_mdoc(mdocs_glob)
        # NEW: Return UI-compatible state
        return self.parameter_manager.get_ui_state()
        

    async def update_parameter(self, payload: Dict[str, Any]) -> Dict[str, Any]:
            """
            Update a single parameter - NO ADAPTER BULLSHIT
            """
            try:
                param_name = payload.get("param_name")
                value = payload.get("value")
                
                if not param_name:
                    return {"success": False, "error": "Invalid payload: 'param_name' missing"}

                # JUST PASS THE FUCKING PARAMETER PATH DIRECTLY - NO MAPPING
                print(f"[BACKEND DIRECT] Updating {param_name} = {value}")
                self.parameter_manager.update_parameter(param_name, value)
                
                return {"success": True}
                
            except Exception as e:
                print(f"[ERROR] update_parameter failed: {e}")
                import traceback
                traceback.print_exc()
                return {"success": False, "error": str(e)}

    async def run_shell_command(self, command: str, cwd: Path = None, 
                                tool_name: str = None, additional_binds: List[str] = None):
        """Runs a shell command, optionally using specified tool's container."""
        try:
            if tool_name:
                print(f"[DEBUG] Running command with tool: {tool_name}")
                final_command = self.container_service.wrap_command_for_tool(
                    command=command,
                    cwd=cwd or self.server_dir,
                    tool_name=tool_name,
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

    # ... [Rest of backend.py (get_slurm_info, _run_relion_schemer, etc.) remains unchanged] ...
    async def get_slurm_info(self):
        return await self.run_shell_command("sinfo")

    async def _run_relion_schemer(self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]):
        """Run relion_schemer to execute the pipeline scheme"""
        try:
            run_command = f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            
            full_run_command = self.container_service.wrap_command_for_tool(
                command=run_command,
                cwd=project_dir,
                tool_name="relion_schemer",
                additional_binds=additional_bind_paths
            )
            
            print(f"[BACKEND] Starting pipeline with command: {full_run_command}")
            
            process = await asyncio.create_subprocess_shell(
                full_run_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            self.active_schemer_process = process
            asyncio.create_task(self._monitor_schemer(process, project_dir))
            
            return {"success": True, "message": f"Workflow started (PID: {process.pid})", "pid": process.pid}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def start_pipeline(self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]):
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}
        
        bind_paths = {str(Path(p).parent.resolve()) for p in required_paths if p}
        bind_paths.add(str(project_dir.parent.resolve()))
        
        return await self._run_relion_schemer(
            project_dir, scheme_name, additional_bind_paths=list(bind_paths)
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

    async def _monitor_schemer(self, process: asyncio.subprocess.Process, project_dir: Path):
        """Monitor the relion_schemer process"""
        async def read_stream(stream, callback):
            while True:
                line = await stream.readline()
                if not line:
                    break
                callback(line.decode().strip())
        
        def handle_stdout(line):
            print(f"[SCHEMER] {line}")
            
        def handle_stderr(line):
            print(f"[SCHEMER-ERR] {line}")
        
        await asyncio.gather(
            read_stream(process.stdout, handle_stdout),
            read_stream(process.stderr, handle_stderr)
        )
        
        await process.wait()
        print(f"[MONITOR] relion_schemer PID {process.pid} completed with return code: {process.returncode}")
        self.active_schemer_process = None

    async def get_eer_frames_per_tilt(self, eer_file_path: str) -> int:
        """Extract number of frames per tilt from EER file"""
        try:
            command = f"header {eer_file_path}"
            result = await self.run_shell_command(command)

            if result["success"]:
                output = result["output"]
                for line in output.split('\n'):
                    if "Number of columns, rows, sections" in line:
                        parts = line.split('.')[-1].strip().split()
                        if len(parts) >= 3:
                            return int(parts[2])
            return None
        except Exception as e:
            print(f"Error getting EER frames: {e}")
            return None

    async def get_pipeline_job_logs(self, project_path: str, job_type: str, job_number: str) -> Dict[str, str]:
        """Get the run.out and run.err contents for a specific pipeline job"""
        project_dir = Path(project_path)
        
        job_dir_map = {
            'importmovies': 'Import',
            'fsMotionAndCtf': 'External',
            'tsAlignment': 'External'
        }
        
        job_dir_name = job_dir_map.get(job_type, 'External')
        job_path = project_dir / job_dir_name / f"job{job_number.zfill(3)}"
        
        logs = {
            'stdout': '',
            'stderr': '',
            'exists': False,
            'path': str(job_path)
        }
        
        if not job_path.exists():
            return logs
        
        logs['exists'] = True
        
        out_file = job_path / 'run.out'
        if out_file.exists():
            try:
                with open(out_file, 'r', encoding='utf-8') as f:
                    logs['stdout'] = f.read()
            except Exception as e:
                logs['stdout'] = f"Error reading run.out: {e}"
        
        err_file = job_path / 'run.err'
        if err_file.exists():
            try:
                with open(err_file, 'r', encoding='utf-8') as f:
                    logs['stderr'] = f.read()
            except Exception as e:
                logs['stderr'] = f"Error reading run.err: {e}"
        
        return logs

    async def monitor_pipeline_jobs(self, project_path: str, selected_jobs: List[str]) -> AsyncGenerator:
        """Monitor all pipeline jobs and yield updates"""
        while True:
            job_statuses = []
            for idx, job_type in enumerate(selected_jobs, 1):
                logs = await self.get_pipeline_job_logs(project_path, job_type, str(idx))
                job_statuses.append({
                    'job_type': job_type,
                    'job_number': idx,
                    'logs': logs
                })
            yield job_statuses
            await asyncio.sleep(5)