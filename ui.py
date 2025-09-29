# ui.py (Updated)

import asyncio
from pathlib import Path
from nicegui import ui, app
from backend import CryoBoostBackend
from auth import AuthService
from models import User, Job
from typing import List
import pandas as pd

STATUS_MAP = {
    "PENDING": ("orange", "PD"),
    "RUNNING": ("green", "R"),
    "COMPLETED": ("blue", "CG"),
    "FAILED": ("red", "F"),
    "CANCELLED": ("gray", "CA"),
    "TIMEOUT": ("red", "TO"),
}

def create_ui_router(backend: CryoBoostBackend, auth: AuthService):
    @ui.page('/')
    async def main_page():
        if not app.storage.user.get('authenticated'):
            create_login_page(auth)
            return
        username = app.storage.user.get('username')
        user = User(username=username)
        await create_main_ui(backend, user)

def create_login_page(auth: AuthService):
    """(This function is unchanged)"""
    async def attempt_login():
        username = username_input.value
        password = password_input.value
        if auth.authenticate(username, password):
            app.storage.user.update({'authenticated': True, 'username': username})
            ui.navigate.to('/')
        else:
            ui.notify('Invalid username or password', type='negative')

    with ui.column().classes('w-full h-screen items-center justify-center gap-y-2'):
        ui.label('').classes('text-6xl')
        ui.label('CryoBoost Server').classes('text-3xl font-bold')
        ui.label('Headnode-centric implementation of cryoboost (WORK IN PROGRESS)').classes('text-gray-500')
        with ui.card().classes('w-full max-w-sm p-8 mt-4'):
            username_input = ui.input('Username').props('outlined dense').classes('w-full').on('keydown.enter', attempt_login)
            password_input = ui.input('Password', password=True, password_toggle_button=True).props('outlined dense').classes('w-full').on('keydown.enter', attempt_login)
            ui.button('Log In', on_click=attempt_login).classes('w-full primary-button mt-4')

def logout():
    app.storage.user.clear()
    ui.navigate.to('/')

async def create_main_ui(backend: CryoBoostBackend, user: User):
    with ui.header().classes('bg-white text-gray-800 shadow-sm p-4'):
        with ui.row().classes('w-full items-center justify-between'):
            ui.label(f'CryoBoost Server').classes('text-xl font-semibold')
            with ui.row().classes('items-center'):
                ui.label(f'Welcome, {user.username}!').classes('mr-4')
                ui.button('Logout', on_click=logout, icon='logout').props('flat dense')
    
    with ui.row().classes('w-full p-8 gap-8'):
        with ui.tabs().props('vertical').classes('w-48') as tabs:
            # NEW: Added a dedicated Projects tab
            projects_tab = ui.tab('Projects')
            jobs_tab = ui.tab('Job Status')
            info_tab = ui.tab('Cluster Info')
        
        with ui.tab_panels(tabs, value=projects_tab).classes('w-full'):
            with ui.tab_panel(projects_tab):
                build_projects_tab(backend, user) # NEW and CORRECT
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
    """(This function is unchanged)"""
    with ui.column().classes('w-full'):
        with ui.row().classes('w-full justify-between items-center mb-4'):
            ui.label('Individual Job Management').classes('text-lg font-medium')
            ui.button('Submit Test GPU Job', on_click=lambda: submit_and_track_job(backend, user, job_tabs, job_tab_panels))
        
        with ui.tabs().classes('w-full') as job_tabs:
            pass
            
        with ui.tab_panels(job_tabs, value=None).classes('w-full mt-4 border rounded-md') as job_tab_panels:
            user_jobs = backend.get_user_jobs(user)
            if not user_jobs:
                with ui.tab_panel('placeholder').classes('items-center justify-center'):
                    ui.label('No jobs submitted yet.').classes('text-gray-500')
            else:
                for job in user_jobs:
                    create_job_tab(backend, user, job, job_tabs, job_tab_panels)
                job_tabs.set_value(user_jobs[-1].internal_id)

async def submit_and_track_job(backend: CryoBoostBackend, user: User, job_tabs, job_tab_panels):
    """(This function is unchanged)"""
    result = await backend.submit_test_gpu_job(user)
    if not result['success']:
        ui.notify(f"Job submission failed: {result['error']}", type='negative')
        return
    job = result['job']
    ui.notify(f"Submitted job {job.slurm_id}", type='positive')
    if 'placeholder' in job_tab_panels:
        job_tab_panels.remove('placeholder')
    create_job_tab(backend, user, job, job_tabs, job_tab_panels)
    job_tabs.set_value(job.internal_id)

def create_job_tab(backend: CryoBoostBackend, user: User, job: Job, job_tabs, job_tab_panels):

    """(This function is unchanged)"""
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
        job_info = backend.get_job_log(user, job.internal_id)
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
# In backend.py, add these methods:

async def schedule_pipeline(self, user: User, project_path: str, scheme_name: str):
    """
    Schedules all jobs from the specified scheme without starting execution.
    """
    project_dir = Path(project_path)
    if not project_dir.is_dir():
        return {"success": False, "error": f"Project path not found: {project_path}"}
    
    print(f"[BACKEND] User {user.username} scheduling pipeline for project: {project_path}")
    print(f"[BACKEND] Using scheme: {scheme_name}")
    
    result = await self.pipeline_orchestrator.schedule_pipeline_from_scheme(project_dir, scheme_name)
    
    if result["success"]:
        print(f"[BACKEND] Successfully scheduled {result.get('message', 'jobs')}")
    else:
        print(f"[BACKEND] Scheduling failed: {result.get('error', 'Unknown error')}")
    
    return result

async def start_pipeline(self, user: User, project_path: str, scheme_name: str):
    """
    Executes relion_schemer --run to start a pre-scheduled pipeline.
    """
    project_dir = Path(project_path)
    if not project_dir.is_dir():
        return {"success": False, "error": f"Project path not found: {project_path}"}
    
    print(f"[BACKEND] User {user.username} starting pipeline execution for: {project_path}")
    print(f"[BACKEND] Scheme to run: {scheme_name}")
    
    return await self.pipeline_orchestrator.start_pipeline(project_dir, scheme_name)
async def _check_pipeline_status(self):
    """Periodically check if the pipeline is still running."""
    while True:
        await asyncio.sleep(10)  # Check every 10 seconds
        
        # Check if any jobs are still running
        pipeline_star = Path(self.current_project_path) / "default_pipeline.star"
        if pipeline_star.exists():
            try:
                data = self.star_handler.read(pipeline_star)
                processes = data.get('pipeline_processes', pd.DataFrame())
                
                running = processes[processes['rlnPipeLineProcessStatusLabel'] == 'Running']
                scheduled = processes[processes['rlnPipeLineProcessStatusLabel'] == 'Scheduled']
                
                if not running.empty:
                    print(f"[UI] Jobs still running: {len(running)}")
                elif not scheduled.empty:
                    print(f"[UI] Jobs still scheduled: {len(scheduled)}")
                else:
                    print("[UI] Pipeline appears to be complete")
                    self.update_status_label("Pipeline completed")
                    ui.notify("Pipeline completed!", type="positive")
                    break
            except Exception as e:
                print(f"[UI] Error checking pipeline status: {e}")
                break

# In ui.py, add this function.
def build_projects_tab(backend: CryoBoostBackend, user: User):
    """Builds the entire 'Projects' tab, encapsulating its state and handlers."""
    
    # --- 1. State Management ---
    state = {
        "selected_jobs": [],
        "current_project_path": None,
        "current_scheme_name": None,
    }

    # --- 2. Handler Functions (now nested, no 'self') ---
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
        if not name or not state["selected_jobs"]:
            ui.notify("Project name and at least one job are required.", type='negative')
            return
        create_button.props('loading')
        result = await backend.create_project_and_scheme(user, name, state["selected_jobs"])
        create_button.props(remove='loading')
        if result.get("success"):
            state["current_project_path"] = result["project_path"]
            state["current_scheme_name"] = f"scheme_{name}"
            ui.notify(result["message"], type='positive')
            active_project_label.set_text(name)
            pipeline_status.set_text("Project created. Ready to schedule jobs.")
            schedule_button.props(remove='disabled')
            project_name_input.disable()
            create_button.disable()
        else:
            ui.notify(f"Error: {result.get('error', 'Unknown')}", type='negative')

    async def _monitor_pipeline_progress():
        while not stop_button.props.get('disabled'):
            await asyncio.sleep(5)
            try:
                progress = await backend.get_pipeline_progress(state["current_project_path"])
                if not progress or progress.get('status') != 'ok':
                    break
                total, completed, running, failed = progress.get('total',0), progress.get('completed',0), progress.get('running',0), progress.get('failed',0)
                if total > 0:
                    progress_bar.value = completed / total
                    progress_message.text = f"Progress: {completed}/{total} completed ({running} running, {failed} failed)"
                if progress.get('is_complete'):
                    msg = f"Pipeline finished with {failed} failures." if failed > 0 else "Pipeline completed successfully."
                    pipeline_status.set_text(msg)
                    ui.notification(message=msg, type='warning' if failed > 0 else 'positive')
                    stop_button.props('disabled')
                    schedule_button.props(remove='disabled')
                    break
            except Exception as e:
                print(f"[UI] Error monitoring pipeline: {e}")
                break
        print("[UI] Pipeline monitoring stopped.")

    async def handle_schedule_jobs():
        schedule_button.props('loading')
        result = await backend.schedule_pipeline(user, state["current_project_path"], state["current_scheme_name"])
        schedule_button.props(remove='loading')
        if result.get("success"):
            ui.notify("Jobs scheduled successfully!", type='positive')
            pipeline_status.set_text("Jobs scheduled. Ready to run.")
            run_button.props(remove='disabled')
            schedule_button.props('disabled')
        else:
            ui.notify(f"Scheduling failed: {result.get('error', 'Unknown')}", type='negative')

    async def handle_run_pipeline():
        run_button.props('loading')
        pipeline_status.set_text("Starting pipeline...")
        progress_bar.classes(remove='hidden').value = 0
        progress_message.classes(remove='hidden')
        result = await backend.start_pipeline(user, state["current_project_path"], state["current_scheme_name"])
        run_button.props(remove='loading')
        if result.get("success"):
            pid = result.get('pid', 'N/A')
            ui.notify(f"Pipeline started successfully! (PID: {pid})", type="positive")
            pipeline_status.set_text(f"Pipeline running (PID: {pid})")
            run_button.props('disabled')
            stop_button.props(remove='disabled')
            progress_message.set_text("Pipeline is running...")
            asyncio.create_task(_monitor_pipeline_progress())
        else:
            pipeline_status.set_text(f"Failed to start: {result.get('error', 'Unknown')}")
            ui.notify(pipeline_status.text, type='negative')
            
    async def handle_stop_pipeline():
        ui.notify("Stop functionality not fully implemented yet.", type="warning")
        pipeline_status.set_text("Pipeline stopped by user.")
        stop_button.props('disabled')
        run_button.props(remove='disabled')
        progress_bar.classes('hidden')
        progress_message.classes('hidden')

    # --- 3. UI Construction ---
    with ui.column().classes('w-full p-4 gap-4'):
        with ui.card().classes('w-full p-4'):
            ui.label('1. Configure and Create Project').classes('text-lg font-semibold mb-2')
            project_name_input = ui.input('Project Name', placeholder='e.g., my_first_dataset').classes('w-full mb-2')
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
                schedule_button = ui.button('SCHEDULE JOBS', on_click=handle_schedule_jobs, icon='schedule').props('disabled')
                run_button = ui.button('RUN PIPELINE', on_click=handle_run_pipeline, icon='play_arrow').props('disabled')
                stop_button = ui.button('STOP PIPELINE', on_click=handle_stop_pipeline, icon='stop').props('disabled')
            progress_bar = ui.linear_progress(value=0, show_value=False).classes('hidden')
            progress_message = ui.label('').classes('text-sm text-gray-600 hidden')

    asyncio.create_task(_load_available_jobs())
