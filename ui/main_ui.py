# ui/main_ui.py (FONT UPDATE)
from nicegui import ui, Client
from backend import CryoBoostBackend
from .projects_tab import build_projects_tab
from .state_inspector_tab import build_state_inspector_tab

HARDCODED_USER = "artem.kushner"


def create_ui_router(backend: CryoBoostBackend):
    @ui.page("/")
    async def main_page(client: Client):
        ui.add_head_html("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                
                body, .nicegui-content {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 12px !important;
                    font-weight: 400;
                }
                .q-field__native, .q-field__label, .q-select__option, .q-item__label, .q-field__hint {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 12px !important;
                }
                .q-btn, .q-tab__label {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 11px !important;
                    font-weight: 500;
                    text-transform: none !important;
                }
                .font-semibold {
                    font-weight: 500 !important;
                }
                .q-badge {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 10px !important;
                    font-weight: 500;
                }
            </style>
        """)

        with ui.header(elevated=True).classes("bg-white text-gray-800").style("box-shadow: 0 1px 3px rgba(0,0,0,0.06);"):
            with ui.row().classes("w-full items-center justify-between p-2"):
                ui.label("CryoBoost Server").classes("text-sm font-semibold")
                with ui.tabs().classes("w-1/2") as tabs:
                    projects_tab = ui.tab("Projects & Parameters")
                    state_tab = ui.tab("State Inspector")
                    info_tab = ui.tab("Cluster Info")
                ui.label(f"User: {HARDCODED_USER}").classes("text-xs")

        with ui.tab_panels(tabs, value=projects_tab).classes("w-full p-3") as panels:
            with ui.tab_panel(projects_tab):
                load_project_data_func = build_projects_tab(backend)

            with ui.tab_panel(state_tab):
                load_state_data_func = build_state_inspector_tab()

            with ui.tab_panel(info_tab):
                load_info_data_func = create_info_page(backend)

        await client.connected()

        try:
            await load_info_data_func()
            await load_project_data_func()
            await load_state_data_func()
        except Exception as e:
            print(f"--- [DEBUG] ERROR loading page data: {e} ---")
            import traceback
            traceback.print_exc()

def create_info_page(backend: CryoBoostBackend):
    ui.label("SLURM Cluster Information").classes("text-sm font-medium mb-2")
    output_area = ui.log().classes("w-full h-96 border rounded-md p-2 bg-gray-50 text-xs font-mono")

    async def get_info():
        output_area.push("Loading sinfo...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])

    ui.button("Get SLURM Info", on_click=get_info).props("dense")

    return get_info
