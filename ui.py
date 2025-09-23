# ui.py (Updated)

from nicegui import ui, app
from backend import CryoBoostBackend
from auth import AuthService
from models import User, Job
from typing import List

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

    


async def create_projects_page(backend: CryoBoostBackend, user: User):
    ui.label('Project Management').classes('text-2xl font-semibold mb-4')

    # State to hold project info after creation
    app.storage.user['current_project'] = None

    async def handle_create_and_schedule():
        name = project_name.value
        selected_jobs = job_selection.value # Use the new variable name
        if not name or not selected_jobs:
            ui.notify('Project name and at least one job must be selected.', type='negative')
            return

        spinner.visible = True
        create_button.disable()
        
        result = await backend.create_project_with_custom_scheme(user, name, selected_jobs)
        
        spinner.visible = False
        create_button.enable()
        
        if result['success']:
            ui.notify(result['message'], type='positive')
            app.storage.user['current_project'] = result['project_info']
            run_controls.refresh()
        else:
            ui.notify(f"Error: {result['error']}", type='negative', multi_line=True, close_button=True)

    async def handle_run_pipeline():
        project_info = app.storage.user.get('current_project')
        if not project_info:
            ui.notify('No project has been created and scheduled yet.', type='negative')
            return
        
        run_button.disable()
        ui.notify(f"Starting pipeline '{project_info['scheme_name']}'...", type='info')
        result = await backend.start_scheduled_pipeline(project_info['path'], project_info['scheme_name'])
        
        if result['success']:
            ui.notify('Pipeline start command issued successfully!', type='positive')
        else:
            ui.notify(f"Error starting pipeline: {result['error']}", type='negative')
            run_button.enable()

    with ui.card().classes('w-full'):
        ui.label('1. Configure and Create Project').classes('text-lg font-medium')
        ui.separator()
        
        project_name = ui.input('Project Name', placeholder='e.g., dataset_01_warp').classes('w-full')
        
        # Fetch available jobs from the backend to populate the checklist
        available_jobs = await backend.get_available_jobs()
        
        # FIX: Replaced the incorrect ui.checkbox with the correct ui.select element,
        # styled to look like checkboxes.
        job_selection = ui.select(
            available_jobs, 
            multiple=True, 
            value=available_jobs, 
            label='Select Jobs for Pipeline:'
        ).props('checkboxes')

        with ui.row().classes('w-full items-center mt-4'):
            create_button = ui.button('Create Project & Schedule Jobs', on_click=handle_create_and_schedule)
            spinner = ui.spinner('dots', size='lg', color='primary').classes('ml-4')
            spinner.set_visibility(False)

    @ui.refreshable
    def run_controls():
        project_info = app.storage.user.get('current_project')
        if project_info:
            with ui.card().classes('w-full mt-8'):
                ui.label('2. Execute Pipeline').classes('text-lg font-medium')
                ui.separator()
                ui.label(f"Project '{project_info['project_name']}' is scheduled and ready to run.")
                global run_button 
                run_button = ui.button('Run Scheduled Pipeline', on_click=handle_run_pipeline).props('icon=play_arrow color=positive')

    run_controls()
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
                await create_projects_page(backend, user) # NEW page
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