And here is the new impl.


```
ᢹ CBE-login [dev/crboost_server] tree -L 3  -I 'projects|venv|__pycache__'\\
.
├── backend.py
├── config
│   ├── binAdapters
│   │   ├── AreTomo2
│   │   ├── cryoCARE_extract_train_data.py
│   │   ├── cryoCARE_predict.py
│   │   ├── cryoCARE_train.py
│   │   ├── pytom_extract_candidates.py
│   │   ├── pytom_match_template.py
│   │   ├── pytom_merge_stars.py
│   │   └── WarpTools
│   ├── conf.yaml
│   ├── qsub
│   │   ├── old_qsub_cbe_warp.sh
│   │   ├── qsub_cbe_warp_pre_pyinstaller.sh
│   │   ├── qsub_cbe_warp.sh
│   │   ├── qsub_pytom_cbe.sh
│   │   ├── qsub_pytom_old.sh
│   │   ├── qsub_pytom.sh
│   │   ├── qsub_relion_cbe.sh
│   │   ├── qsub_relion_hpcl89.sh
│   │   ├── qsub_warp_hpcl89.sh
│   │   └── relion5GUI_masterSubmissionScript_latest.sh
│   └── Schemes
│       ├── relion_tomo_prep
│       └── warp_tomo_prep
├── container_defs
│   ├── archive
│   │   └── __obsolete_crboost.def
│   ├── cryocare.def
│   ├── pytom.def
│   ├── relion5.0_tomo.def
│   └── warp_aretomo1.0.0_cuda11.8_glibc2.31.def
├── docs
│   ├── cryooboost_refactor_new_rough.png
│   ├── cryooboost_refactor_old.png
│   ├── docs.md
│   ├── impl_new.md
│   ├── impl_old.md
│   └── refactor_notes.md
├── LICENSE
├── local_file_picker.py
├── main.py
├── models.py
├── __pycache__
│   ├── backend.cpython-311.pyc
│   ├── local_file_picker.cpython-311.pyc
│   ├── models.cpython-311.pyc
│   └── ui.cpython-311.pyc
├── README.md
├── refactor_so_far.md
├── requirements.txt
├── services
│   ├── computing_service.py
│   ├── config_service.py
│   ├── container_service.py
│   ├── data_import_service.py
│   ├── pipeline_orchestrator_service.py
│   ├── project_service.py
│   ├── __pycache__
│   │   ├── computing_service.cpython-311.pyc
│   │   ├── config.cpython-311.pyc
│   │   ├── config_service.cpython-311.pyc
│   │   ├── container_service.cpython-311.pyc
│   │   ├── data_import.cpython-311.pyc
│   │   ├── data_import_service.cpython-310.pyc
│   │   ├── data_import_service.cpython-311.pyc
│   │   ├── hpc_config_service.cpython-311.pyc
│   │   ├── pipeline_orchestrator.cpython-311.pyc
│   │   ├── pipeline_orchestrator_service.cpython-311.pyc
│   │   ├── project.cpython-311.pyc
│   │   ├── project_service.cpython-310.pyc
│   │   ├── project_service.cpython-311.pyc
│   │   ├── simple_computing_service.cpython-311.pyc
│   │   ├── starfile.cpython-311.pyc
│   │   ├── starfile_service.cpython-310.pyc
│   │   └── starfile_service.cpython-311.pyc
│   ├── simple_computing_service.py
│   └── starfile_service.py
├── static
│   └── main.css
└── ui.py

13 directories, 69 files

```

backend.py:
```
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

    async def start_pipeline(self, project_path: str, scheme_name: str, selected_jobs: List[str], required_paths: List[str]):
        project_dir = Path(project_path)
        if not project_dir.is_dir():
            return {"success": False, "error": f"Project path not found: {project_path}"}
        
        # Derive unique parent directories from paths provided by the UI for binding
        bind_paths = {str(Path(p).parent.resolve()) for p in required_paths if p}
        # The project path itself might be inside the base, so add the base.
        bind_paths.add(str(project_dir.parent.resolve()))
            
        return await self.pipeline_orchestrator.schedule_and_run_manually(
            project_dir, scheme_name, selected_jobs, additional_bind_paths=list(bind_paths)
        )

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
```

ui.py:
```
# ui.py

import asyncio
from pathlib import Path
from nicegui import ui
from backend import CryoBoostBackend
from models import User, Job
from typing import List

from local_file_picker import local_file_picker

HARDCODED_USER = User(username="artem.kushner")
STATUS_MAP = {
    "PENDING": ("orange", "PD"),
    "RUNNING": ("green", "R"),
    "COMPLETED": ("blue", "CG"),
    "FAILED": ("red", "F"),
    "CANCELLED": ("gray", "CA"),
    "TIMEOUT": ("red", "TO"),
}


def create_ui_router(backend: CryoBoostBackend):
    @ui.page('/')
    async def main_page():
        await create_main_ui(backend, HARDCODED_USER)

async def create_main_ui(backend: CryoBoostBackend, user: User):
    with ui.header().classes('bg-white text-gray-800 shadow-sm p-4'):
        with ui.row().classes('w-full items-center justify-between'):
            ui.label('CryoBoost Server').classes('text-xl font-semibold')
            with ui.row().classes('items-center'):
                ui.label(f'User: {user.username}').classes('mr-4')

    with ui.row().classes('w-full p-8 gap-8'):
        with ui.tabs().props('vertical').classes('w-48') as tabs:
            projects_tab = ui.tab('Projects')
            jobs_tab = ui.tab('Job Status')
            info_tab = ui.tab('Cluster Info')
        
        with ui.tab_panels(tabs, value=projects_tab).classes('w-full'):
            with ui.tab_panel(projects_tab):
                build_projects_tab(backend, user)
            with ui.tab_panel(jobs_tab):
                await create_jobs_page(backend, user)
            with ui.tab_panel(info_tab):
                create_info_page(backend)

def create_info_page(backend: CryoBoostBackend):
    ui.label('SLURM Cluster Information').classes('text-lg font-medium mb-4')
    output_area = ui.log().classes('w-full h-96 border rounded-md p-2 bg-gray-50')
    async def get_info():
        output_area.push("Loading sinfo...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])
    ui.button('Get SLURM Info', on_click=get_info).classes('mt-4')

async def create_jobs_page(backend: CryoBoostBackend, user: User):
    with ui.column().classes('w-full'):
        with ui.row().classes('w-full justify-between items-center mb-4'):
            ui.label('Individual Job Management').classes('text-lg font-medium')
            ui.button('Submit Test GPU Job', on_click=lambda: submit_and_track_job(backend, user, job_tabs, job_tab_panels))
        
        with ui.tabs().classes('w-full') as job_tabs:
            pass
            
        with ui.tab_panels(job_tabs, value=None).classes('w-full mt-4 border rounded-md') as job_tab_panels:
            user_jobs = backend.get_user_jobs()
            if not user_jobs:
                with ui.tab_panel('placeholder').classes('items-center justify-center'):
                    ui.label('No jobs submitted yet.').classes('text-gray-500')
            else:
                for job in user_jobs:
                    create_job_tab(backend, user, job, job_tabs, job_tab_panels)
                job_tabs.set_value(user_jobs[-1].internal_id)

async def submit_and_track_job(backend: CryoBoostBackend, user: User, job_tabs, job_tab_panels):
    result = await backend.submit_test_gpu_job()
    if not result['success']:
        ui.notify(f"Job submission failed: {result['error']}", type='negative')
        return
    job = result['job']
    ui.notify(f"Submitted job {job.slurm_id}", type='positive')
    if 'placeholder' in job_tab_panels:
        job_tab_panels.clear()
    create_job_tab(backend, user, job, job_tabs, job_tab_panels)
    job_tabs.set_value(job.internal_id)

def create_job_tab(backend: CryoBoostBackend, user: User, job: Job, job_tabs, job_tab_panels):
    with job_tabs:
        new_tab = ui.tab(name=job.internal_id, label=f'Job {job.slurm_id}')
    
    with job_tab_panels:
        with ui.tab_panel(new_tab):
            with ui.row().classes('w-full justify-between items-center'):
                ui.label(f'Tracking logs for Job ID: {job.slurm_id}').classes('text-md font-medium')
                with ui.row().classes('items-center gap-2'):
                    color, label = STATUS_MAP.get(job.status, ("gray", job.status))
                    status_badge = ui.badge(label, color=color).props('outline')
                    refresh_button = ui.button(icon='refresh', on_click=lambda: update_log_display(True)).props('flat round dense')
            
            log_output = ui.log(max_lines=1000).classes('w-full h-screen border rounded-md bg-gray-50 p-2 mt-2')
            log_output.push(job.log_content)

    def update_log_display(manual_refresh=False):
        job_info = backend.get_job_log(job.internal_id)
        if job_info:
            log_output.clear()
            log_output.push(job_info.log_content)
            status_text = job_info.status
            color, label = STATUS_MAP.get(status_text, ("gray", status_text))
            status_badge.text = label
            status_badge.color = color
            if status_text in {"COMPLETED", "FAILED", "CANCELLED"}:
                timer.deactivate()
                refresh_button.disable()
            if manual_refresh:
                ui.notify('Logs refreshed!', type='positive', timeout=1000)
    timer = ui.timer(interval=5, callback=update_log_display, active=True)


def build_projects_tab(backend: CryoBoostBackend, user: User):
    state = {
        "selected_jobs": [],
        "current_project_path": None,
        "current_scheme_name": None,
    }

    async def _load_available_jobs():
        job_types = await backend.get_available_jobs()
        job_selector.options = job_types
        job_selector.update()

    def remove_job(job_name: str, row: ui.element):
        state["selected_jobs"].remove(job_name)
        row.delete()
        job_status_label.set_text('No jobs added yet.' if not state["selected_jobs"] else 'Current pipeline:')
        ui.notify(f"Removed '{job_name}'", type='info')

    def handle_add_job():
        job_name = job_selector.value
        if not job_name or job_name in state["selected_jobs"]:
            return
        state["selected_jobs"].append(job_name)
        if len(state["selected_jobs"]) == 1:
            job_status_label.set_text('Current pipeline:')
        with selected_jobs_container:
            with ui.row().classes('w-full items-center justify-between bg-gray-100 p-1 rounded') as row:
                ui.label(job_name)
                ui.button(icon='delete', on_click=lambda: remove_job(job_name, row)).props('flat round dense text-red-500')
        job_selector.set_value(None)

    async def handle_create_project():
        name = project_name_input.value
        movies_glob = movies_path_input.value
        mdocs_glob = mdocs_path_input.value

        if not all([name, movies_glob, mdocs_glob, state["selected_jobs"]]):
            ui.notify("Project name, data paths, and at least one job are required.", type='negative')
            return
            
        create_button.props('loading')
        
        result = await backend.create_project_and_scheme(
            name, state["selected_jobs"], movies_glob, mdocs_glob
        )
        
        create_button.props(remove='loading')
        if result.get("success"):
            state["current_project_path"] = result["project_path"]
            state["current_scheme_name"] = f"scheme_{name}"
            ui.notify(result["message"], type='positive')
            active_project_label.set_text(name)
            pipeline_status.set_text("Project created. Ready to run.")
            run_button.props(remove='disabled')

            project_name_input.disable()
            movies_path_input.disable()
            mdocs_path_input.disable()
            create_button.disable()
        else:
            ui.notify(f"Error: {result.get('error', 'Unknown')}", type='negative')

    async def _monitor_pipeline_progress():
        while state["current_project_path"] and not stop_button.props.get('disabled'):
            progress = await backend.get_pipeline_progress(state["current_project_path"])
            if not progress or progress.get('status') != 'ok':
                break
            total, completed, running, failed = progress.get('total',0), progress.get('completed',0), progress.get('running',0), progress.get('failed',0)
            if total > 0:
                progress_bar.value = completed / total
                progress_message.text = f"Progress: {completed}/{total} completed ({running} running, {failed} failed)"
            if progress.get('is_complete') and total > 0:
                msg = f"Pipeline finished with {failed} failures." if failed > 0 else "Pipeline completed successfully."
                
                pipeline_status.set_text(msg)
                if failed > 0:
                    pipeline_status.classes(add='text-red-500', remove='text-green-500')
                else:
                    pipeline_status.classes(add='text-green-500', remove='text-red-500')
                
                stop_button.props('disabled')
                run_button.props(remove='disabled')
                break
            await asyncio.sleep(5)
        print("[UI] Pipeline monitoring stopped.")

    async def handle_run_pipeline():
        pipeline_status.classes(remove='text-red-500 text-green-500')
        run_button.props('loading')
        pipeline_status.set_text("Starting pipeline...")
        progress_bar.classes(remove='hidden').value = 0
        progress_message.classes(remove='hidden').set_text("Pipeline is starting...")
        
        result = await backend.start_pipeline(
            state["current_project_path"], 
            state["current_scheme_name"],
            state["selected_jobs"]
        )
        run_button.props(remove='loading')
        if result.get("success"):
            pid = result.get('pid', 'N/A')
            ui.notify(f"Pipeline started successfully! (PID: {pid})", type="positive")
            pipeline_status.set_text(f"Pipeline running (PID: {pid})")
            run_button.props('disabled')
            stop_button.props(remove='disabled')
            asyncio.create_task(_monitor_pipeline_progress())
        else:
            pipeline_status.set_text(f"Failed to start: {result.get('error', 'Unknown')}")
            ui.notify(pipeline_status.text, type='negative')
            progress_bar.classes('hidden')
            progress_message.classes('hidden')

    async def handle_stop_pipeline():
        ui.notify("Stop functionality not fully implemented yet.", type="warning")
        pipeline_status.set_text("Pipeline stopped by user.")
        stop_button.props('disabled')
        run_button.props(remove='disabled')
        progress_bar.classes('hidden')
        progress_message.classes('hidden')

    with ui.column().classes('w-full p-4 gap-4'):
        with ui.card().classes('w-full p-4'):
            ui.label('1. Configure and Create Project').classes('text-lg font-semibold mb-2')
            project_name_input = ui.input('Project Name', placeholder='e.g., my_first_dataset').classes('w-full')
            async def choose_movie_dir():
                result = await local_file_picker('~', mode='directory', glob_pattern_annotation='*.eer')
                if result:
                    selected_dir = Path(result[0])
                    glob_pattern = '*.eer'
                    if not any(selected_dir.glob(glob_pattern)):
                        ui.notify(f"Warning: No '{glob_pattern}' files found in this directory.", type='warning')
                    movies_path_input.set_value(str(selected_dir / glob_pattern))
            
            async def choose_mdoc_dir():
                result = await local_file_picker('~', mode='directory', glob_pattern_annotation='*.mdoc')
                if result:
                    selected_dir = Path(result[0])
                    glob_pattern = '*.mdoc'
                    if not any(selected_dir.glob(glob_pattern)):
                        ui.notify(f"Warning: No '{glob_pattern}' files found in this directory.", type='warning')
                    mdocs_path_input.set_value(str(selected_dir / glob_pattern))

            with ui.row().classes('w-full items-center no-wrap'):
                movies_path_input = ui.input(
                    label='Movie Files Path/Glob', 
                    value='/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer'
                ).classes('flex-grow').props('hint="Provide a glob pattern, e.g., /path/to/frames/*.eer"')
                ui.button(icon='folder', on_click=choose_movie_dir).props('flat dense')

            with ui.row().classes('w-full items-center no-wrap'):
                mdocs_path_input = ui.input(
                    label='MDOC Files Path/Glob', 
                    value='/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc'
                ).classes('flex-grow').props('hint="Provide a glob pattern, e.g., /path/to/mdoc/*.mdoc"')
                ui.button(icon='folder', on_click=choose_mdoc_dir).props('flat dense')

            job_status_label = ui.label('No jobs added yet.').classes('text-sm text-gray-600 my-2')
            with ui.expansion('Add Job to Pipeline', icon='add').classes('w-full'):
                with ui.row().classes('w-full items-center gap-2'):
                    job_selector = ui.select(label='Select job type', options=[]).classes('flex-grow')
                    ui.button('ADD', on_click=handle_add_job).classes('bg-green-500 text-white')
            selected_jobs_container = ui.column().classes('w-full mt-2 gap-1')
            create_button = ui.button('CREATE PROJECT', on_click=handle_create_project).classes('bg-blue-500 text-white mt-4')
            
        with ui.card().classes('w-full p-4'):
            ui.label('2. Schedule and Execute Pipeline').classes('text-lg font-semibold mb-2')
            with ui.row():
                ui.label('Active Project:').classes('text-sm font-medium mr-2')
                active_project_label = ui.label('No active project').classes('text-sm font-mono')
            pipeline_status = ui.label('Create and configure a project first.').classes('text-sm text-gray-600 my-3')
            with ui.row().classes('gap-2 mb-3'):
                run_button = ui.button('RUN PIPELINE', on_click=handle_run_pipeline, icon='play_arrow').props('disabled')
                stop_button = ui.button('STOP PIPELINE', on_click=handle_stop_pipeline, icon='stop').props('disabled')

            progress_bar = ui.linear_progress(value=0, show_value=False).classes('hidden')
            progress_message = ui.label('').classes('text-sm text-gray-600 hidden')

    asyncio.create_task(_load_available_jobs())

```

pipeline_orchestration_service.py:
```
# services/pipeline_orchestrator_service.py

import asyncio
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional

from .starfile_service import StarfileService
from .config_service import get_config_service
from .container_service import get_container_service
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend import CryoBoostBackend

class PipelineOrchestratorService:
    """
    Orchestrates the creation and execution of Relion pipelines.
    
    This service is responsible for:
    1.  Creating custom pipeline schemes from templates.
    2.  Building raw, executable commands for tools like WarpTools.
    3.  Using the ContainerService to wrap these raw commands into full,
        containerized `apptainer` calls.
    4.  Injecting the final containerized commands into the `fn_exe` field
        of the job.star files.
    5.  Scheduling and monitoring the pipeline execution via `relion_schemer`.
    """
    
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.container_service = get_container_service() 
        self.active_schemer_process: Optional[asyncio.subprocess.Process] = None

    def _build_warp_fs_motion_ctf_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        frame_folder = "../../frames"
        output_settings_file = "./warp_frameseries.settings"
        folder_processing = "./warp_frameseries" 

        create_settings_parts = [
            "WarpTools create_settings",
            f"--folder_data {frame_folder}",
            f"--extension '*.eer'",
            f"--folder_processing {folder_processing}",
            f"--output {output_settings_file}",
            f"--angpix {user_params.get('angpix', 1.35)}",
            f"--eer_ngroups -{params.get('eer_fractions', 32)}",
        ]

        voltage = user_params.get('voltage', 300)
        cs = user_params.get('cs', 2.7)
        amplitude = user_params.get('amplitude', 0.07)

        m_min, m_max = params.get('m_range_min_max', '500:10').split(':')
        c_min, c_max = params.get('c_range_min_max', '30:4').split(':')
        defocus_min, defocus_max = params.get('c_defocus_min_max', '0.5:8').split(':')

        run_main_parts = [
            "WarpTools fs_motion_and_ctf",
            f"--settings {output_settings_file}",
            f"--m_grid {params.get('m_grid', '1x1x3')}",
            f"--m_range_min {m_min}",
            f"--m_range_max {m_max}",
            f"--m_bfac {params.get('m_bfac', -500)}",
            f"--c_grid {params.get('c_grid', '2x2x1')}",
            f"--c_window {params.get('c_window', 512)}",
            f"--c_range_min {c_min}",
            f"--c_range_max {c_max}",
            f"--c_defocus_min {defocus_min}",
            f"--c_defocus_max {defocus_max}",
            f"--c_voltage {user_params.get('voltage', 300)}",
            f"--c_cs {user_params.get('cs', 2.7)}",
            f"--c_amplitude {user_params.get('amplitude', 0.07)}",
            f"--perdevice {params.get('perdevice', 1)}",
            "--out_averages",
        ]

        full_command = " && ".join([" ".join(create_settings_parts), " ".join(run_main_parts)])
        
        return full_command

    def _build_warp_ts_alignment_command(self, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        return "echo 'tsAlignment job not implemented yet'; exit 1;"

    def _build_command_for_job(self, job_name: str, params: Dict[str, Any], user_params: Dict[str, Any]) -> str:
        """Dispatcher function to call the correct raw command builder"""
        job_builders = {
            'fsMotionAndCtf': self._build_warp_fs_motion_ctf_command,
            'tsAlignment': self._build_warp_ts_alignment_command,
        }
        
        builder = job_builders.get(job_name)
        
        if builder:
            raw_command = builder(params, user_params)
            return raw_command
        else:
            return f"echo 'ERROR: Job type \"{job_name}\" not implemented'; exit 1;"


    async def create_custom_scheme(self, project_dir: Path, new_scheme_name: str, base_template_path: Path, selected_jobs: List[str], user_params: Dict[str, Any], additional_bind_paths: List[str]):
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)
            base_scheme_name = base_template_path.name

            for job_name in selected_jobs:
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                job_star_path = dest_job_dir / "job.star"
                if not job_star_path.exists():
                    continue

                job_data = self.star_handler.read(job_star_path)
                params_df = job_data.get('joboptions_values')
                if params_df is None:
                    continue
                
                params_dict = pd.Series(
                    params_df.rlnJobOptionValue.values,
                    index=params_df.rlnJobOptionVariable
                ).to_dict()

                raw_command = self._build_command_for_job(job_name, {}, user_params)
                final_containerized_command = self.container_service.wrap_command(
                    command=raw_command,
                    cwd=project_dir, # Or specific job dir
                    additional_binds=additional_bind_paths
                )
                
                params_df.loc[params_df['rlnJobOptionVariable'] == 'fn_exe', 'rlnJobOptionValue'] = final_containerized_command
                params_df.loc[params_df['rlnJobOptionVariable'] == 'other_args', 'rlnJobOptionValue'] = ''

                params_to_remove = [f'param{i}_{s}' for i in range(1, 11) for s in ['label', 'value']]
                cleanup_mask = ~params_df['rlnJobOptionVariable'].isin(params_to_remove)
                job_data['joboptions_values'] = params_df[cleanup_mask].reset_index(drop=True)

                print(f"Updating scheme name from '{base_scheme_name}' to '{new_scheme_name}' in {job_name}/job.star")
                for block_name, block_data in job_data.items():
                    if isinstance(block_data, pd.DataFrame):
                        for col in block_data.select_dtypes(include=['object']):
                            if block_data[col].str.contains(base_scheme_name).any():
                                block_data[col] = block_data[col].str.replace(base_scheme_name, new_scheme_name, regex=False)
                
                self.star_handler.write(job_data, job_star_path)

            scheme_general_df = pd.DataFrame({'rlnSchemeName': [f'Schemes/{new_scheme_name}/'], 'rlnSchemeCurrentNodeName': ['WAIT']})
            scheme_floats_df = pd.DataFrame({
                'rlnSchemeFloatVariableName': ['do_at_most', 'maxtime_hr', 'wait_sec'],
                'rlnSchemeFloatVariableValue': [500.0, 48.0, 180.0],
                'rlnSchemeFloatVariableResetValue': [500.0, 48.0, 180.0]
            })
            scheme_operators_df = pd.DataFrame({
                'rlnSchemeOperatorName': ['EXIT', 'EXIT_maxtime', 'WAIT'],
                'rlnSchemeOperatorType': ['exit', 'exit_maxtime', 'wait'],
                'rlnSchemeOperatorOutput': ['undefined'] * 3,
                'rlnSchemeOperatorInput1': ['undefined', 'maxtime_hr', 'wait_sec'],
                'rlnSchemeOperatorInput2': ['undefined'] * 3
            })
            scheme_jobs_df = pd.DataFrame({
                'rlnSchemeJobNameOriginal': selected_jobs,
                'rlnSchemeJobName': selected_jobs,
                'rlnSchemeJobMode': ['continue'] * len(selected_jobs),
                'rlnSchemeJobHasStarted': [0] * len(selected_jobs)
            })

            edges = [{'rlnSchemeEdgeInputNodeName': 'WAIT', 'rlnSchemeEdgeOutputNodeName': 'EXIT_maxtime'}]
            edges.append({'rlnSchemeEdgeInputNodeName': 'EXIT_maxtime', 'rlnSchemeEdgeOutputNodeName': selected_jobs[0]})
            for i in range(len(selected_jobs) - 1):
                edges.append({'rlnSchemeEdgeInputNodeName': selected_jobs[i], 'rlnSchemeEdgeOutputNodeName': selected_jobs[i+1]})
            edges.append({'rlnSchemeEdgeInputNodeName': selected_jobs[-1], 'rlnSchemeEdgeOutputNodeName': 'EXIT'})

            scheme_edges_df = pd.DataFrame(edges)
            for df in [scheme_edges_df]:
                df['rlnSchemeEdgeIsFork'] = 0
                df['rlnSchemeEdgeOutputNodeNameIfTrue'] = 'undefined'
                df['rlnSchemeEdgeBooleanVariable'] = 'undefined'
            
            scheme_star_data = {
                'scheme_general': scheme_general_df, 'scheme_floats': scheme_floats_df,
                'scheme_operators': scheme_operators_df, 'scheme_jobs': scheme_jobs_df,
                'scheme_edges': scheme_edges_df
            }

            scheme_star_path = new_scheme_dir / "scheme.star"
            self.star_handler.write(scheme_star_data, scheme_star_path)
            print(f" Created complete scheme file at: {scheme_star_path}")

            return {"success": True}
        except Exception as e:
            print(f" ERROR creating custom scheme: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    async def schedule_and_run_manually(self, project_dir: Path, scheme_name: str, selected_jobs: List[str], additional_bind_paths: List[str]):
            pipeline_star_path = project_dir / "default_pipeline.star"
            if not pipeline_star_path.exists():
                return {"success": False, "error": "Cannot start: default_pipeline.star not found."}

            # The `unset DISPLAY` handles the non-GUI case for the schemer.
            run_command = f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            
            # Reverted: No more `enable_gui` flag.
            full_run_command = self.container_service.wrap_command(
                command=run_command,
                cwd=project_dir,
                additional_binds=additional_bind_paths
            )
            
            try:
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
    async def _monitor_schemer(self, process: asyncio.subprocess.Process, project_dir: Path):
        
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
```


container_service.py:
```
# services/container_service.py

from pathlib import Path
import shlex
import os
from typing import List, Optional
from .config_service import get_config_service

class ContainerService:
    def __init__(self):
        config_data = get_config_service().get_config()
        self.container_paths = config_data.containers or {}
        
        self.tool_to_container = {
            "WarpTools": "warp_aretomo",
            "AreTomo": "warp_aretomo",
            "cryoCARE_extract_train_data.py": "cryocare",
            "cryoCARE_train.py": "cryocare",
            "cryoCARE_predict.py": "cryocare",
            "pytom_extract_candidates.py": "pytom",
            "pytom_match_template.py": "pytom",
            "pytom_merge_stars.py": "pytom",
            "relion": "relion",
            "relion_": "relion",
        }

    def _find_container_key(self, tool_name: str) -> Optional[str]:
        if tool_name in self.tool_to_container:
            return self.tool_to_container[tool_name]
        for prefix, key in self.tool_to_container.items():
            if tool_name.startswith(prefix):
                return key
        return None

    def wrap_command(self, command: str, cwd: Path, additional_binds: List[str] = None) -> str:
        tool_name = command.split()[0]
        container_key = self._find_container_key(tool_name)
        
        if not container_key or container_key not in self.container_paths:
            return command

        container_path = self.container_paths[container_key]
        
        binds = set()
        for p in ["/tmp", "/scratch", str(Path.home())]:
            if Path(p).exists():
                binds.add(p)
        binds.add(str(cwd.resolve()))
        if additional_binds:
            for p in additional_binds:
                path = Path(p).resolve()
                if path.exists():
                    binds.add(str(path))
        
        hpc_paths = ["/usr/bin", "/usr/lib64/slurm", "/run/munge", "/etc/passwd", "/etc/group", "/groups", "/programs", "/software"]
        for p_str in hpc_paths:
            path = Path(p_str)
            if path.exists():
                binds.add(f"{p_str}:{p_str}:ro" if "passwd" in p_str or "group" in p_str else p_str)

        x11_authority = Path.home() / ".Xauthority"
        x11_socket = Path("/tmp/.X11-unix")
        if x11_authority.exists():
            binds.add(f"{str(x11_authority)}:{str(x11_authority)}:ro")
        if x11_socket.exists():
            binds.add(str(x11_socket))
        
        bind_args = [arg for path in sorted(list(binds)) for arg in ('-B', path)]
        
        args = ["apptainer", "exec", "--nv"]
        args.extend(bind_args)
        
        if container_key == "relion":
            display_var = os.getenv('DISPLAY', ':0.0')
            args.extend([f"--env", f"DISPLAY={display_var}"])
            
            # FIX: Restored the original, clean PATH. Removed the leaking os.getenv('PATH').
            # This ensures only the container's environment is used.
            inner_command = f"""
            unset PYTHONPATH && \
            unset PYTHONHOME && \
            export PATH="/opt/miniconda3/envs/relion-5.0/bin:/opt/miniconda3/bin:/opt/relion-5.0/build/bin:/usr/local/cuda-11.8/bin:/usr/sbin:/usr/bin:/sbin:/bin" && \
            {command}
            """
            wrapped_command = f"bash -c {shlex.quote(inner_command.strip())}"
            args.extend([container_path, wrapped_command])
        else:
            # The --cleanenv flag handles isolation for non-relion containers
            args.extend(["--cleanenv", container_path, "bash", "-c", shlex.quote(command)])

        full_command = " ".join(args)
        
        clean_env_vars = [
            "SINGULARITY_BIND", "APPTAINER_BIND", "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
            "SINGULARITY_NAME", "APPTAINER_NAME", "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
            "LD_PRELOAD", "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY"
        ]
        clean_env_cmd = "unset " + " ".join(clean_env_vars)
        
        final_command = f"{clean_env_cmd}; {full_command}"
        return final_command

_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
```



computing_service.py:
```

from typing import Any, Dict


class ComputingService:
    def get_computing_params(self, job_type: str, partition: str, do_node_sharing: bool = True) -> Dict[str, Any]:
        """Calculate computing parameters for a job type and partition"""
        print(f"[COMPUTING DEBUG] Getting params for job_type={job_type}, partition={partition}")
        
        conf_comp = self.config.computing
        print(f"[COMPUTING DEBUG] Available JOBTypesCompute: {conf_comp.JOBTypesCompute}")
        
        # Find job type category
        job_category = None
        for category, jobs in conf_comp.JOBTypesCompute.items():
            if job_type in jobs:
                job_category = category
                break
        
        print(f"[COMPUTING DEBUG] Found job_category: {job_category}")
        
        if not job_category:
            print(f"[COMPUTING DEBUG] No job category found for {job_type}")
            return {}

        # Get partition setup
        partition_attr = partition.replace('-', '_')
        print(f"[COMPUTING DEBUG] Looking for partition: {partition_attr}")
        partition_setup = getattr(conf_comp, partition_attr, None)
        
        if not partition_setup:
            print(f"[COMPUTING DEBUG] Partition {partition} not found in config")
            # Try to find any partition
            available_partitions = [attr for attr in dir(conf_comp) if not attr.startswith('_') and attr not in [
                'QueSize', 'NODE_Sharing', 'JOBTypesCompute', 'JOBTypesApplication', 'JOBMaxNodes', 'JOBsPerDevice'
            ]]
            print(f"[COMPUTING DEBUG] Available partitions: {available_partitions}")
            if available_partitions:
                partition_setup = getattr(conf_comp, available_partitions[0])
                print(f"[COMPUTING DEBUG] Using first available partition: {available_partitions[0]}")
        
        if not partition_setup:
            return {}

        print(f"[COMPUTING DEBUG] Partition setup: {partition_setup}")
        
        part_name_alias = self._get_alias_reverse(job_type, "PartionName") or "qsub_extra3"
        nodes_alias = self._get_alias_reverse(job_type, "NrNodes") or "qsub_extra1"
        gpu_alias = self._get_alias_reverse(job_type, "NrGPU") or "qsub_extra4"
        memory_alias = self._get_alias_reverse(job_type, "MemoryRAM") or "qsub_extra5"
        mpi_per_node_alias = self._get_alias_reverse(job_type, "MPIperNode")

        print(f"[COMPUTING DEBUG] Aliases - part_name: {part_name_alias}, nodes: {nodes_alias}, gpu: {gpu_alias}, memory: {memory_alias}")
        
        comp_params = {}
        comp_params[part_name_alias] = partition
        
        node_sharing = conf_comp.NODE_Sharing
        memory_ram = partition_setup.RAM
        if do_node_sharing and partition in node_sharing.ApplyTo:
            memory_ram = str(round(int(partition_setup.RAM[:-1]) / 2)) + "G"
        
        comp_params[memory_alias] = memory_ram

        # Calculate parameters based on job category
        if job_category == "CPU-MPI":
            comp_params[mpi_per_node_alias] = partition_setup.NrCPU
            comp_params["nr_mpi"] = partition_setup.NrCPU * 1
            comp_params[gpu_alias] = 0
            comp_params[nodes_alias] = 1
            comp_params["nr_threads"] = 1
            
        elif job_category in ["GPU-OneProcess", "GPU-OneProcessOneGPU"]:
            comp_params[mpi_per_node_alias] = 1
            comp_params["nr_mpi"] = 1
            comp_params[gpu_alias] = partition_setup.NrGPU
            comp_params[nodes_alias] = 1
            comp_params["nr_threads"] = partition_setup.NrGPU
            
            if job_category == "GPU-OneProcessOneGPU":
                comp_params[gpu_alias] = 1
                
        elif job_category == "GPU-MultProcess":
            comp_params[mpi_per_node_alias] = partition_setup.NrGPU
            comp_params[gpu_alias] = partition_setup.NrGPU
            comp_params["nr_mpi"] = partition_setup.NrGPU * 1
            comp_params["nr_threads"] = 1
            comp_params[nodes_alias] = 1

        # Add jobs per device if specified
        if job_type in conf_comp.JOBsPerDevice:
            comp_params["param10_value"] = conf_comp.JOBsPerDevice[job_type].get(partition, 1)

        print(f"[COMPUTING DEBUG] Final comp_params: {comp_params}")
        return comp_params
```

config_service.py:
```
# services/config_service.py (updated)

import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union

class SubmissionConfig(BaseModel):
    HeadNode: str
    SshCommand: str
    Environment: str
    ClusterStatus: str
    Helpssh: str
    HelpConflict: str

class LocalConfig(BaseModel):
    Environment: str

class Alias(BaseModel):
    Job: str
    Parameter: str
    Alias: str
    
class ComputingPartition(BaseModel):
    NrGPU: int
    NrCPU: int
    RAM: str
    VRAM: str

class NodeSharingConfig(BaseModel):
    CPU_PerGPU: int = Field(alias='CPU-PerGPU')
    ApplyTo: List[str]

class ComputingConfig(BaseModel):
    QueSize: Dict[str, int]
    NODE_Sharing: NodeSharingConfig = Field(alias='NODE-Sharing')
    JOBTypesCompute: Dict[str, List[str]]
    JOBTypesApplication: Dict[str, List[str]]
    JOBMaxNodes: Dict[str, List[int]]
    JOBsPerDevice: Dict[str, Dict[str, int]]
    
    # Make all partitions optional with safe defaults
    c: Optional[ComputingPartition] = None
    m: Optional[ComputingPartition] = None
    g: Optional[ComputingPartition] = None
    g_p100: Optional[ComputingPartition] = Field(None, alias='g-p100')
    g_v100: Optional[ComputingPartition] = Field(None, alias='g-v100')
    g_a100: Optional[ComputingPartition] = Field(None, alias='g-a100')

class Config(BaseModel):
    submission: List[SubmissionConfig]
    local: LocalConfig
    aliases: List[Alias]
    meta_data: Dict[str, List[Dict[str, str]]]
    microscopes: Dict[str, List[Dict[str, str]]]
    
    star_file: Optional[Dict[str, str]] = None
    
    containers: Optional[Dict[str, str]] = None 
    computing: ComputingConfig
    filepath: Dict[str, str]

    class Config:
        extra = 'ignore'  # Ignore extra fields

class ConfigService:
    def __init__(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if 'star_file' not in data:
            data['star_file'] = {}
        
        self._config = Config(**data)

    def get_config(self) -> Config:
        return self._config

    def get_job_output_filename(self, job_type: str) -> Optional[str]:
        if not self._config.star_file:
            return None
        base_job_type = job_type.split('_')[0]
        return self._config.star_file.get(base_job_type)

@lru_cache()
def get_config_service(config_path: str = "config/conf.yaml") -> ConfigService:
    path = Path.cwd() / config_path
    return ConfigService(path)
```

data_import_service.py:
```
# services/data_import_service.py

import os
import glob
import shutil
from pathlib import Path
from typing import Dict, List, Any

class DataImportService:
    """
    Handles the core logic of preparing raw data for a CryoBoost project.
    This includes parsing mdocs, creating symlinks, and rewriting mdocs with prefixes.
    """

    def _parse_mdoc(self, mdoc_path: Path) -> Dict[str, Any]:
        """
        Parses an .mdoc file into a header string and a list of data dictionaries.
        Lifts logic from `mdocMeta.readMdoc`.
        """
        header_lines = []
        data_sections = []
        current_section = {}
        in_zvalue_section = False

        with open(mdoc_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('[ZValue'):
                    if current_section:
                        data_sections.append(current_section)
                    current_section = {'ZValue': line.split('=')[1].strip().strip(']')}
                    in_zvalue_section = True
                elif in_zvalue_section and '=' in line:
                    key, value = [x.strip() for x in line.split('=', 1)]
                    current_section[key] = value
                elif not in_zvalue_section:
                    header_lines.append(line)

        if current_section:
            data_sections.append(current_section)

        return {'header': "\n".join(header_lines), 'data': data_sections}

    def _write_mdoc(self, mdoc_data: Dict[str, Any], output_path: Path):
        """
        Writes a parsed mdoc data structure back to a file.
        Lifts logic from `mdocMeta.writeMdoc`.
        """
        with open(output_path, 'w') as f:
            f.write(mdoc_data['header'] + '\n')
            for section in mdoc_data['data']:
                z_value = section.pop('ZValue', None)
                if z_value is not None:
                    f.write(f"[ZValue = {z_value}]\n")
                for key, value in section.items():
                    f.write(f"{key} = {value}\n")
                f.write("\n")

    async def setup_project_data(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str
    ) -> Dict[str, Any]:
        """
        Orchestrates the data import process: creates dirs, symlinks movies,
        and rewrites mdocs with the specified prefix.
        """
        try:
            frames_dir = project_dir / 'frames'
            mdoc_dir = project_dir / 'mdoc'
            frames_dir.mkdir(exist_ok=True, parents=True)
            mdoc_dir.mkdir(exist_ok=True, parents=True)

            source_movie_dir = Path(movies_glob).parent
            mdoc_files = glob.glob(mdocs_glob)
            
            if not mdoc_files:
                return {"success": False, "error": f"No .mdoc files found with pattern: {mdocs_glob}"}

            for mdoc_path_str in mdoc_files:
                mdoc_path = Path(mdoc_path_str)
                parsed_mdoc = self._parse_mdoc(mdoc_path)

                for section in parsed_mdoc['data']:
                    if 'SubFramePath' not in section:
                        continue
                    
                    original_movie_name = Path(section['SubFramePath'].replace('\\', '/')).name
                    prefixed_movie_name = f"{import_prefix}{original_movie_name}"
                    
                    section['SubFramePath'] = prefixed_movie_name

                    source_movie_path = source_movie_dir / original_movie_name
                    link_path = frames_dir / prefixed_movie_name
                    
                    if not source_movie_path.exists():
                        print(f"Warning: Source movie not found: {source_movie_path}")
                        continue

                    if not link_path.exists():
                        os.symlink(source_movie_path.resolve(), link_path)

                new_mdoc_path = mdoc_dir / f"{import_prefix}{mdoc_path.name}"
                self._write_mdoc(parsed_mdoc, new_mdoc_path)
            
            return {"success": True, "message": f"Imported {len(mdoc_files)} tilt-series."}
        except Exception as e:
            return {"success": False, "error": str(e)}
```

project_service.py:
```
# services/project_service.py

import shutil
from pathlib import Path
from typing import Dict, Any

from services.data_import_service import DataImportService
from services.starfile_service import StarfileService

class ProjectService:
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()

    async def create_project_structure(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str
    ) -> Dict[str, Any]:
        """
        Creates the project directory structure and imports the raw data.
        Now with PRE-POPULATED qsub scripts!
        """
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "Schemes").mkdir(exist_ok=True)
            (project_dir / "Logs").mkdir(exist_ok=True)

            # Copy AND PRE-POPULATE qsub scripts
            await self._setup_qsub_templates(project_dir)
            
            import_result = await self.data_importer.setup_project_data(
                project_dir, movies_glob, mdocs_glob, import_prefix
            )
            if not import_result["success"]:
                return import_result
            
            return {"success": True, "message": "Project directory structure created and data imported."}
        except Exception as e:
            return {"success": False, "error": f"Failed during directory setup: {str(e)}"}

    async def _setup_qsub_templates(self, project_dir: Path):
        """Copy qsub templates and replace placeholders with sensible defaults"""
        qsub_template_path = Path.cwd() / "config" / "qsub"
        project_qsub_path = project_dir / "qsub"
        
        if qsub_template_path.is_dir():
            # Copy all templates
            shutil.copytree(qsub_template_path, project_qsub_path, dirs_exist_ok=True)
            
            # Pre-populate the main qsub script we use
            main_qsub_script = project_qsub_path / "qsub_cbe_warp.sh"
            if main_qsub_script.exists():
                await self._prepopulate_qsub_script(main_qsub_script)
                
            print(f"[PROJECT] Pre-populated qsub scripts in {project_qsub_path}")

    async def _prepopulate_qsub_script(self, qsub_script_path: Path):
        """Replace XXXextraXXXX placeholders with sensible defaults"""
        with open(qsub_script_path, 'r') as f:
            content = f.read()
        
        # Replace all the extra placeholders with sensible defaults
        replacements = {
            "XXXextra1XXX": "1",      # nodes
            "XXXextra2XXX": "",       # mpi_per_node (empty = let relion handle it)
            "XXXextra3XXX": "g",      # partition (GPU)
            "XXXextra4XXX": "1",      # gpus  
            "XXXextra5XXX": "32G",    # memory
            "XXXthreadsXXX": "8",     # threads
        }
        
        for placeholder, value in replacements.items():
            content = content.replace(placeholder, value)
        
        # Write back
        with open(qsub_script_path, 'w') as f:
            f.write(content)
        
        print(f"[QSUB] Pre-populated {qsub_script_path} with defaults")
```


local_file_picker.py:

```
import platform
from pathlib import Path
from typing import Optional

from nicegui import events, ui

class local_file_picker(ui.dialog):

    def __init__(self, directory: str, *,
                 upper_limit: Optional[str] = ...,
                 mode: str = 'directory', 
                 glob_pattern_annotation: str = None) -> None:
        """Enhanced Local File Picker

        :param directory: The directory to start in.
        :param upper_limit: The directory to stop at.
        :param mode: 'directory' to select a directory, 'file' to select a file.
        :param glob_pattern_annotation: A string to display as a helpful hint (e.g., '*.eer').
        """
        super().__init__()

        self.path = Path(directory).expanduser().resolve()
        self.mode = mode
        if upper_limit is None:
            self.upper_limit = None
        else:
            self.upper_limit = Path(directory if upper_limit == ... else upper_limit).expanduser().resolve()

        with self, ui.card().classes('w-[60rem] max-w-full'):
            ui.add_head_html('<style>.ag-selection-checkbox { display: none; }</style>')

            with ui.row().classes('w-full items-center px-4 pb-2'):
                self.up_button = ui.button(icon='arrow_upward', on_click=self._go_up).props('flat round dense')
                self.path_label = ui.label(str(self.path)).classes('ml-2 text-mono')

            self.grid = ui.aggrid({
                'columnDefs': [{'field': 'name', 'headerName': 'File'}],
                'rowSelection': 'single',
            }, html_columns=[0]).classes('w-full').on('cellDoubleClicked', self.handle_double_click)

            with ui.row().classes('w-full justify-end items-center px-4 pt-2'):
                with ui.row().classes('mr-auto items-center'):
                    # NEW: Instructions change based on the mode
                    if self.mode == 'directory':
                        ui.label('Select a folder and click Ok, or navigate into a folder and click Ok.').classes('text-xs text-gray-500')
                    else:
                        ui.label('Select a file and click Ok, or double-click a file.').classes('text-xs text-gray-500')
                    
                    if glob_pattern_annotation:
                        ui.label(f'Expected: {glob_pattern_annotation}').classes('text-xs text-gray-500 ml-4 p-1 bg-gray-100 rounded')

                ui.button('Cancel', on_click=self.close).props('outline')
                ui.button('Ok', on_click=self._handle_ok)

        self.update_grid()

    def update_grid(self) -> None:
        try:
            paths = sorted(
                [p for p in self.path.glob('*') if p.name != '.DS_Store'],
                key=lambda p: (not p.is_dir(), p.name.lower())
            )
        except FileNotFoundError:
            self.path = Path('/').expanduser().resolve()
            paths = []

        self.grid.options['rowData'] = [
            {
                'name': f'📁 <strong>{p.name}</strong>' if p.is_dir() else f'📄 {p.name}',
                'path': str(p),
                'is_dir': p.is_dir(),
            }
            for p in paths
        ]
        
        self.path_label.set_text(str(self.path))
        self._update_up_button()
        self.grid.update()

    def handle_double_click(self, e: events.GenericEventArguments) -> None:
        data = e.args['data']
        path = Path(data['path'])
        
        if data['is_dir']:
            self.path = path
            self.update_grid()
        elif self.mode == 'file':  # Only submit on double-click if in file mode
            self.submit([str(path)])
        else: # In directory mode, notify that you can't double-click a file
            ui.notify('Please select a directory and click "Ok".', type='info')

    async def _handle_ok(self):
        rows = await self.grid.get_selected_rows()
        # NEW: If nothing is selected, select the current directory (in directory mode)
        if not rows:
            if self.mode == 'directory':
                self.submit([str(self.path)])
            else:
                ui.notify('Please select a file.', type='warning')
            return

        # NEW: Validate selection based on mode
        selected_path = Path(rows[0]['path'])
        is_dir = rows[0]['is_dir']

        if self.mode == 'directory':
            if is_dir:
                self.submit([str(selected_path)])
            else:
                ui.notify('You selected a file. Please select a directory.', type='negative')
        elif self.mode == 'file':
            if not is_dir:
                self.submit([str(selected_path)])
            else:
                ui.notify('You selected a directory. Please select a file.', type='negative')

    def _go_up(self) -> None:
        self.path = self.path.parent
        self.update_grid()

    def _update_up_button(self) -> None:
        if self.upper_limit is None:
            self.up_button.props(f'disable={self.path == self.path.parent}')
        else:
            self.up_button.props(f'disable={self.path == self.upper_limit or self.upper_limit in self.path.parents}')
```