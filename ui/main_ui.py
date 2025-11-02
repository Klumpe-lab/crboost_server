# ui/main_ui.py
from nicegui import ui, Client
from backend import CryoBoostBackend
from .projects_tab import build_projects_tab
from .state_inspector_tab import build_state_inspector_tab  # NEW

HARDCODED_USER = "artem.kushner"


def create_ui_router(backend: CryoBoostBackend):
    @ui.page("/")
    async def main_page(client: Client):
        ui.add_head_html("""
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
        """)

        # Build UI structure synchronously
        with ui.header(elevated=True).classes("bg-white text-gray-800"):
            with ui.row().classes("w-full items-center justify-between p-2"):
                ui.label("CryoBoost Server").classes("text-sm font-semibold")
                with ui.tabs().classes("w-1/2") as tabs:
                    projects_tab = ui.tab("Projects & Parameters")
                    state_tab = ui.tab("State Inspector")  # NEW
                    info_tab = ui.tab("Cluster Info")
                ui.label(f"User: {HARDCODED_USER}").classes("text-xs")

        with ui.tab_panels(tabs, value=projects_tab).classes("w-full p-3") as panels:
            # Projects tab
            with ui.tab_panel(projects_tab):
                load_project_data_func = build_projects_tab(backend)

            # State Inspector tab (NEW)
            with ui.tab_panel(state_tab):
                load_state_data_func = build_state_inspector_tab()

            # Cluster Info tab
            with ui.tab_panel(info_tab):
                load_info_data_func = create_info_page(backend)

        # Wait for WebSocket connection to be established
        await client.connected()

        # Now load all async data
        print("--- [DEBUG] Client connected, loading page data ---")


        try:
            await load_info_data_func()
            print("--- [DEBUG] Info tab loaded ---")

            await load_project_data_func()
            print("--- [DEBUG] Project tab loaded ---")
            
            # Initialize SLURM data for data import panel
            # Note: This is called from the main page context, so it should work
            try:
                # Access the panel state through the projects tab
                # You might need to adjust this based on how you're storing panel_state
                from ui.data_import_panel import build_data_import_panel
                # Actually, the refresh should have been called at the end of build_data_import_panel
                print("--- [DEBUG] SLURM data initialization scheduled ---")
            except Exception as slurm_error:
                print(f"--- [DEBUG] SLURM initialization failed (non-critical): {slurm_error} ---")

            await load_state_data_func()
            print("--- [DEBUG] State inspector tab loaded ---")
        except Exception as e:
            print(f"--- [DEBUG] ERROR loading page data: {e} ---")
            import traceback
            traceback.print_exc()
            # Don't use ui.notify here either - might not have context
            print(f"Error loading page data: {e}")

def create_info_page(backend: CryoBoostBackend):
    """Simple cluster info page"""
    ui.label("SLURM Cluster Information").classes("text-sm font-medium mb-2")
    output_area = ui.log().classes("w-full h-96 border rounded-md p-2 bg-gray-50 text-xs font-mono")

    async def get_info():
        output_area.push("Loading sinfo...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])

    ui.button("Get SLURM Info", on_click=get_info).props("dense")

    return get_info
