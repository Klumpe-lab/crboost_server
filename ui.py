from nicegui import ui
from backend import CryoBoostBackend

# A dictionary to map SLURM statuses to colors and labels for our UI
STATUS_MAP = {
    "PENDING": ("orange", "PD"),
    "RUNNING": ("green", "R"),
    "COMPLETED": ("blue", "CG"),
    "FAILED": ("red", "F"),
    "CANCELLED": ("gray", "CA"),
    "TIMEOUT": ("red", "TO"),
    # Add any other statuses you want to track
}

def build_ui(backend: CryoBoostBackend):
    # ... (this function is unchanged)
    with ui.header().classes('bg-white text-gray-800 shadow-sm p-4'):
        with ui.row().classes('w-full items-center justify-between'):
            ui.label('CryoBoost Server').classes('text-xl font-semibold')
    
    with ui.row().classes('w-full p-8'):
        with ui.tabs().props('vertical').classes('w-48') as tabs:
            setup_tab = ui.tab('Setup')
            jobs_tab = ui.tab('Job Status')
        
        with ui.tab_panels(tabs, value=setup_tab).classes('w-full'):
            with ui.tab_panel(setup_tab):
                create_setup_page(backend)
            with ui.tab_panel(jobs_tab):
                create_jobs_page(backend)

def create_setup_page(backend: CryoBoostBackend):
    # ... (this function is unchanged)
    ui.label('SLURM Cluster Information').classes('text-lg font-medium mb-4')
    output_area = ui.log().classes('w-full h-96 border rounded-md p-2 bg-gray-50')

    async def get_info():
        output_area.push("Loading...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])

    ui.button('Get SLURM Info', on_click=get_info).classes('mt-4')

def create_jobs_page(backend: CryoBoostBackend):
    # ... (this function is unchanged)
    with ui.column().classes('w-full'):
        with ui.row().classes('w-full justify-between items-center mb-4'):
            ui.label('Job Management').classes('text-lg font-medium')
            ui.button('Submit Test GPU Job', on_click=lambda: submit_and_track_job(backend, job_tabs, job_tab_panels))
        
        with ui.tabs().classes('w-full') as job_tabs:
            pass 
            
        with ui.tab_panels(job_tabs, value=None).classes('w-full mt-4 border rounded-md') as job_tab_panels:
            with ui.tab_panel('placeholder').classes('items-center justify-center'):
                 ui.label('No jobs submitted yet.').classes('text-gray-500')

async def submit_and_track_job(backend: CryoBoostBackend, job_tabs, job_tab_panels):
    # ... (this part is mostly unchanged)
    result = await backend.submit_test_gpu_job()
    
    if not result['success']:
        ui.notify(f"Job submission failed: {result['error']}", type='negative')
        return

    internal_id = result['internal_job_id']
    slurm_id = result['slurm_job_id']
    ui.notify(f"Submitted job {slurm_id}", type='positive')

    if 'placeholder' in job_tab_panels:
        job_tab_panels.remove('placeholder')

    with job_tabs:
        new_tab = ui.tab(name=internal_id, label=f'Job {slurm_id}')
    
    with job_tab_panels:
        with ui.tab_panel(new_tab):
            # NEW: Header row with status badge and refresh button
            with ui.row().classes('w-full justify-between items-center'):
                ui.label(f'Tracking logs for Job ID: {slurm_id}').classes('text-md font-medium')
                with ui.row().classes('items-center gap-2'):
                    status_badge = ui.badge("PD", color="orange").props('outline')
                    refresh_button = ui.button(icon='refresh', on_click=lambda: update_log_display(True)).props('flat round dense')
            
            log_output = ui.log().classes('w-full h-screen border rounded-md bg-gray-50 p-2 mt-2')
    
    job_tabs.set_value(new_tab)

    # The timer callback now updates the status badge as well
    def update_log_display(manual_refresh=False):
        job_info = backend.get_job_log(internal_id)
        if job_info:
            log_output.clear()
            log_output.push(job_info["log_content"])
            
            # Update status badge
            status_text = job_info['status']
            color, label = STATUS_MAP.get(status_text, ("gray", status_text))
            status_badge.text = label
            status_badge.color = color

            if status_text in {"COMPLETED", "FAILED", "CANCELLED"}:
                timer.deactivate()
                refresh_button.disable() # Disable refresh when job is done
            
            if manual_refresh:
                ui.notify('Logs refreshed!', type='positive', timeout=1000)

    timer = ui.timer(interval=2, callback=update_log_display)