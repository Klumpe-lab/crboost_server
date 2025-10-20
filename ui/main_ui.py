import asyncio
from nicegui import ui

from backend import CryoBoostBackend
from models import User

from .data_parameters_tab import create_data_parameters_tab  # Changed import
from .job_scheduler_tab import create_job_scheduler_tab
from .projects_tab import build_projects_tab

HARDCODED_USER = User(username="artem.kushner")


def create_ui_router(backend: CryoBoostBackend):
    @ui.page('/')
    async def main_page():
        ui.add_head_html('''
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500&display=swap');
                
                body, .nicegui-content {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 12px !important;
                    font-weight: 400;
                }
                .q-field__native, .q-field__label, .q-select__option, .q-item__label, .q-field__hint {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 12px !important;
                }
                .q-btn, .q-tab__label {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 11px !important;
                    font-weight: 500;
                    text-transform: none !important;
                }
                .font-semibold {
                     font-weight: 500 !important;
                }
                .q-badge {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 10px !important;
                    font-weight: 500;
                }
            </style>
        ''')
        await create_main_ui(backend, HARDCODED_USER)


async def create_main_ui(backend: CryoBoostBackend, user: User):
    """Main UI nexus - handles tab structure and layout"""
    with ui.header(elevated=True).classes('bg-white text-gray-800'):
        with ui.row().classes('w-full items-center justify-between p-2'):
            ui.label('CryoBoost Server').classes('text-sm font-semibold')

            with ui.tabs().classes('w-1/2') as tabs:
                data_parameters_tab = ui.tab('Data & Parameters')  # Combined tab
                job_scheduler_tab = ui.tab('Job Scheduler')
                projects_tab = ui.tab('Projects')
                info_tab = ui.tab('Cluster Info')

            ui.label(f'User: {user.username}').classes('text-xs')

    with ui.tab_panels(tabs, value=data_parameters_tab).classes('w-full p-3'):
        with ui.tab_panel(data_parameters_tab):
            create_data_parameters_tab(backend, user)  # Use combined tab
        with ui.tab_panel(job_scheduler_tab):
            create_job_scheduler_tab(backend, user)
        with ui.tab_panel(projects_tab):
            build_projects_tab(backend, user)
        with ui.tab_panel(info_tab):
            create_info_page(backend)


def create_info_page(backend: CryoBoostBackend):
    """Simple cluster info page"""
    ui.label('SLURM Cluster Information').classes('text-sm font-medium mb-2')
    output_area = ui.log().classes('w-full h-96 border rounded-md p-2 bg-gray-50 text-xs font-mono')

    async def get_info():
        output_area.push("Loading sinfo...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])

    ui.button('Get SLURM Info', on_click=get_info).props('dense')
    asyncio.create_task(get_info())