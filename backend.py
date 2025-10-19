# backend.py

import asyncio
import os
import uuid
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional
from models import  User
import pandas as pd
# Add to CryoBoostBackend class in backend.py

import yaml
import subprocess
import glob
import xml.etree.ElementTree as ET
from pathlib import Path

from services.config_service import get_config_service
from services.project_service import ProjectService
from services.pipeline_orchestrator_service import PipelineOrchestratorService
from services.container_service import get_container_service
from services.setup_service import SetupService

HARDCODED_USER = User(username="artem.kushner")

class CryoBoostBackend:

    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()
        self.setup_service = SetupService(server_dir)  # NEW SERVICE

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
            
            container_init_command = self.container_service.wrap_command_for_tool(
                command=init_command,
                cwd=project_dir,
                tool_name="relion",  # Explicitly specify the tool
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

    async def get_slurm_info(self):
        return await self.run_shell_command("sinfo")


    async def debug_container_environment(self, project_dir: Path):
        """Debug what environment the container is actually using"""
        test_commands = [
            # Test with explicit tools
            ("relion --version", "relion"),
            ("relion_python_tomo_import --help", "relion_import"),
            ("WarpTools --help", "warptools"),
            
            # Test Python inside container using tools
            ("python -c \"import sys; print(sys.executable)\"", "relion"),
            ("python -c \"import mdocfile; print('mdocfile OK')\"", "relion_import"),
        ]
        
        print(f"\n=== DEBUG CONTAINER ENVIRONMENT ===")
        print(f"Project dir: {project_dir}")
        
        for cmd, tool in test_commands:
            print(f"\n--- Testing with tool '{tool}': {cmd} ---")
            result = await self.run_shell_command(cmd, cwd=project_dir, tool_name=tool)
            print(f"Success: {result['success']}")
            if result['success']:
                print(f"Output: {result['output'][:500]}...")
            else:
                print(f"Error: {result['error'][:500]}...")
    async def _run_relion_schemer(self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]):
        """Run relion_schemer to execute the pipeline scheme"""
        try:
            # The `unset DISPLAY` handles the non-GUI case for the schemer.
            run_command = f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            
            # Use the relion_schemer tool (you'll need to add this to your tool_service)
            full_run_command = self.container_service.wrap_command_for_tool(
                command=run_command,
                cwd=project_dir,
                tool_name="relion_schemer",  # Make sure this tool exists in tool_service
                additional_binds=additional_bind_paths
            )
            
            print(f"[BACKEND] Starting pipeline with command: {full_run_command}")
            
            process = await asyncio.create_subprocess_shell(
                full_run_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            # Store the process for monitoring
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
        
        # FIX: Use the method that actually exists - run relion_schemer directly
        # Since the orchestrator only creates schemes but doesn't run them, we need to run it ourselves
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
        print(f" [MONITOR] relion_schemer PID {process.pid} completed with return code: {process.returncode}")
        self.active_schemer_process = None

    async def get_eer_frames_per_tilt(self, eer_file_path: str) -> int:
        """Extract number of frames per tilt from EER file"""
        try:
            # Use header command to get EER metadata
            command = f"header {eer_file_path}"
            result = await self.run_shell_command(command)
            
            if result["success"]:
                output = result["output"]
                # Parse the output to find frames per tilt
                for line in output.split('\n'):
                    if "Number of columns, rows, sections" in line:
                        parts = line.split('.')[-1].strip().split()
                        if len(parts) >= 3:
                            return int(parts[2])
            return None
        except Exception as e:
            print(f"Error getting EER frames: {e}")
            return None

    async def parse_mdoc_metadata(self, mdoc_path: str) -> Dict[str, Any]:
        """Parse mdoc file for microscope and acquisition parameters"""
        try:
            metadata = {}
            mdoc_files = glob.glob(mdoc_path)
            if not mdoc_files:
                return {}
                
            with open(mdoc_files[0], 'r') as f:
                content = f.read()
                
                # Extract basic metadata
                if 'PixelSpacing = ' in content:
                    metadata['pixel_size'] = float(content.split('PixelSpacing = ')[1].split('\n')[0])
                    
                if 'ExposureDose = ' in content:
                    metadata['dose_per_tilt'] = float(content.split('ExposureDose = ')[1].split('\n')[0])
                    
                if 'ImageSize = ' in content:
                    metadata['image_size'] = content.split('ImageSize = ')[1].split('\n')[0].replace(' ', 'x')
                    
                if 'Voltage = ' in content:
                    metadata['voltage'] = float(content.split('Voltage = ')[1].split('\n')[0])
                    
                # Try to find tilt axis angle
                if 'Tilt axis angle = ' in content:
                    metadata['tilt_axis'] = float(content.split('Tilt axis angle = ')[1].split(',')[0])
                elif 'RotationAngle = ' in content:
                    # Alternative location for tilt axis
                    lines = content.split('\n')
                    for line in lines:
                        if 'RotationAngle = ' in line:
                            metadata['tilt_axis'] = abs(float(line.split('RotationAngle = ')[1]))
                            break
                            
            return metadata
        except Exception as e:
            print(f"Error parsing mdoc: {e}")
            return {}

    async def validate_setup_parameters(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the tomogram setup parameters"""
        validation_result = {
            "valid": True,
            "warnings": [],
            "errors": []
        }
        
        # Validate required parameters
        required_params = ['pixel_size', 'dose_per_tilt', 'voltage']
        for param in required_params:
            if not params.get(param):
                validation_result["valid"] = False
                validation_result["errors"].append(f"Missing required parameter: {param}")
        
        # Validate numeric ranges
        if params.get('pixel_size'):
            pix_size = float(params['pixel_size'])
            if pix_size < 0.5 or pix_size > 10:
                validation_result["warnings"].append(f"Pixel size {pix_size} seems unusual")
                
        if params.get('voltage'):
            voltage = float(params['voltage'])
            if voltage not in [200, 300]:
                validation_result["warnings"].append(f"Voltage {voltage} kV is non-standard")
        
        return validation_result

    async def apply_setup_to_project(self, project_path: str, setup_params: Dict[str, Any]) -> Dict[str, Any]:
        """Apply the setup parameters to a project"""
        try:
            # This would integrate with your existing project creation
            # but with the additional setup parameters
            
            # Store setup parameters in project configuration
            config_path = Path(project_path) / "setup_config.yaml"
            
            with open(config_path, 'w') as f:
                yaml.dump(setup_params, f)
                
            return {
                "success": True,
                "message": "Setup parameters applied successfully",
                "config_path": str(config_path)
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to apply setup: {str(e)}"
            }

    #--------- Pipeline tracking
    
    
    # Add to CryoBoostBackend class in backend.py

    async def get_pipeline_job_logs(self, project_path: str, job_type: str, job_number: str) -> Dict[str, str]:
        """Get the run.out and run.err contents for a specific pipeline job"""
        project_dir = Path(project_path)
        
        # Map job types to their directory names
        job_dir_map = {
            'importmovies': 'Import',
            'fsMotionAndCtf': 'External',
            'tsAlignment': 'External'  # Add more as needed
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
        
        # Read run.out
        out_file = job_path / 'run.out'
        if out_file.exists():
            try:
                with open(out_file, 'r', encoding='utf-8') as f:
                    logs['stdout'] = f.read()
            except Exception as e:
                logs['stdout'] = f"Error reading run.out: {e}"
        
        # Read run.err
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
            await asyncio.sleep(5)  # Poll every 5 seconds