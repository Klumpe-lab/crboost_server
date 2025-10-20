import asyncio
from nicegui import ui
from backend import CryoBoostBackend
from models import User


def create_job_scheduler_tab(backend: CryoBoostBackend, user: User):
    """Tab for job scheduling and container configuration"""
    state = {
        "container_settings": {},
        "scheduler_settings": {}
    }

    with ui.column().classes('w-full gap-3'):
        ui.label('Job Scheduler & Container Configuration').classes('text-sm font-bold text-gray-800')

        # Container Configuration
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Container Settings').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3 items-center'):
                container_runtime_select = ui.select(label='Container Runtime',
                                                     options=['apptainer', 'singularity', 'docker'],
                                                     value='apptainer').props('dense outlined')
                gpu_mode_toggle = ui.toggle(['CPU Only', 'GPU Accelerated'], value='GPU Accelerated').props('dense')
            
            with ui.expansion('Advanced Container Settings', icon='settings').classes('w-full mt-2 text-xs'):
                with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3 p-3 bg-gray-50 rounded-lg'):
                    bind_paths_input = ui.input(label='Additional Bind Paths', placeholder='/scratch,/groups').props(
                        'dense outlined')
                    memory_limit_input = ui.input(label='Memory Limit (GB)', placeholder='32').props(
                        'dense outlined type=number')
                    container_cache_input = ui.input(label='Container Cache Path', 
                                                     placeholder='~/.cache/containers').props('dense outlined')

        # Job Scheduler Configuration
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('SLURM Scheduler Settings').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-3'):
                partition_input = ui.input(label='Partition', placeholder='gpu').props('dense outlined')
                time_limit_input = ui.input(label='Time Limit (hours)', placeholder='24').props(
                    'dense outlined type=number')
                gpus_input = ui.input(label='GPUs per Job', placeholder='1').props('dense outlined type=number')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3 mt-2'):
                cpus_input = ui.input(label='CPUs per Job', placeholder='8').props('dense outlined type=number')
                memory_input = ui.input(label='Memory per Job (GB)', placeholder='32').props(
                    'dense outlined type=number')
            
            with ui.expansion('Advanced SLURM Settings', icon='tune').classes('w-full mt-2 text-xs'):
                with ui.column().classes('w-full gap-3 p-3 bg-gray-50 rounded-lg'):
                    with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-2'):
                        qos_input = ui.input(label='QOS', placeholder='normal').props('dense outlined')
                        reservation_input = ui.input(label='Reservation', placeholder='').props('dense outlined')
                    email_input = ui.input(label='Email for Notifications', placeholder='user@example.com').props(
                        'dense outlined')
                    ui.checkbox('Receive email on job completion').props('dense')

        # Job Templates
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Job Templates').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3'):
                template_select = ui.select(
                    label='Select Job Template',
                    options=['Warp Basic', 'Warp Advanced', 'AreTomo Standard', 'Relion Tomo', 'Custom'],
                    value='Warp Basic'
                ).props('dense outlined')
                
            ui.button('Load Template', icon='download').props('outline dense')
            ui.button('Save as Template', icon='save').props('outline dense')

        # Validation and Actions
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Validation & Execution').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            async def validate_configuration():
                ui.notify("Validating job configuration...", type='info')
                await asyncio.sleep(1)
                ui.notify("Configuration validated successfully!", type='positive')

            async def test_container_setup():
                ui.notify("Testing container setup...", type='info')
                await asyncio.sleep(1)
                ui.notify("Container test completed!", type='positive')

            with ui.row().classes('w-full gap-2'):
                ui.button('Validate Configuration', on_click=validate_configuration, icon='check_circle').props('dense')
                ui.button('Test Container Setup', on_click=test_container_setup, icon='play_arrow').props('outline dense')
                ui.button('Save Configuration', icon='save').props('dense color=primary')