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
    """
    This function acts as a router. It displays the login page
    or the main content based on the user's authentication status.
    """
    @ui.page('/')
    async def main_page():
        if not app.storage.user.get('authenticated'):
            create_login_page(auth)
            return

        username = app.storage.user.get('username')
        user = User(username=username)
        await create_main_ui(backend, user)

def create_login_page(auth: AuthService):
    """Builds the UI for the styled login form."""
    async def attempt_login():
        username = username_input.value
        password = password_input.value
        if auth.authenticate(username, password):
            app.storage.user.update({'authenticated': True, 'username': username})
            ui.navigate.to('/')
        else:
            ui.notify('Invalid username or password', type='negative')

    with ui.column().classes('w-full h-screen items-center justify-center gap-y-2'):
        ui.label('').classes('text-6xl') # the logo willl go here
        ui.label('CryoBoost Server').classes('text-3xl font-bold')
        ui.label('Headnode-centric implementation of cryoboost (THIS IS ALL wrok in progress!)').classes('text-gray-500')

        with ui.card().classes('w-full max-w-sm p-8 mt-4'):
            username_input = ui.input('Username').props('outlined dense').classes('w-full').on('keydown.enter', attempt_login)
            password_input = ui.input('Password', password=True, password_toggle_button=True).props('outlined dense').classes('w-full').on('keydown.enter', attempt_login)
            ui.button('Log In', on_click=attempt_login).classes('w-full primary-button mt-4')

async def create_main_ui(backend: CryoBoostBackend, user: User):
    """(This function is unchanged)"""
    with ui.header().classes('bg-white text-gray-800 shadow-sm p-4'):
        with ui.row().classes('w-full items-center justify-between'):
            ui.label(f'CryoBoost Server').classes('text-xl font-semibold')
            with ui.row().classes('items-center'):
                ui.label(f'Welcome, {user.username}!').classes('mr-4')
                ui.button('Logout', on_click=logout, icon='logout').props('flat dense')
    
    with ui.row().classes('w-full p-8'):
        with ui.tabs().props('vertical').classes('w-48') as tabs:
            setup_tab = ui.tab('Setup')
            jobs_tab = ui.tab('Job Status')
        
        with ui.tab_panels(tabs, value=setup_tab).classes('w-full'):
            with ui.tab_panel(setup_tab):
                create_setup_page(backend)
            with ui.tab_panel(jobs_tab):
                await create_jobs_page(backend, user)

def logout():
    """(This function is unchanged)"""
    app.storage.user.clear()
    ui.navigate.to('/')

def create_setup_page(backend: CryoBoostBackend):
    """(This function is unchanged)"""
    ui.label('SLURM Cluster Information').classes('text-lg font-medium mb-4')
    output_area = ui.log().classes('w-full h-96 border rounded-md p-2 bg-gray-50')
    async def get_info():
        output_area.push("Loading...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])
    ui.button('Get SLURM Info', on_click=get_info).classes('mt-4')

async def create_jobs_page(backend: CryoBoostBackend, user: User):
    """(This function is unchanged)"""
    with ui.column().classes('w-full'):
        with ui.row().classes('w-full justify-between items-center mb-4'):
            ui.label('Job Management').classes('text-lg font-medium')
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
    timer = ui.timer(interval=2, callback=update_log_display, active=True)