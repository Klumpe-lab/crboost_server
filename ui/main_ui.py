# ui/main_ui.py (REFACTORED - NO HEADER)
from nicegui import ui, Client
from backend import CryoBoostBackend
from .data_import_panel import build_data_import_panel
from .pipeline_builder_panel import build_pipeline_builder_panel
from .state_inspector_tab import build_state_inspector_tab

HARDCODED_USER = "artem.kushner"

def create_ui_router(backend: CryoBoostBackend):
    @ui.page("/")
    async def projects_page(client: Client):
        """Main projects page with hamburger menu"""
        ui.add_head_html("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                
                body, .nicegui-content {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 12px !important;
                    font-weight: 400;
                    margin: 0 !important;
                    padding: 0 !important;
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
                .q-page {
                    padding: 0 !important;
                }
                .nicegui-content {
                    padding: 0 !important;
                }
            </style>
        """)

        with ui.button(icon="menu").props("flat dense round").classes("fixed top-2 left-2 z-50").style(
            "background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1);"
        ):
            with ui.menu() as menu:
                ui.menu_item("Projects & Parameters", on_click=lambda: ui.navigate.to("/"))
                ui.menu_item("State Inspector", on_click=lambda: ui.navigate.to("/state-inspector"))
                ui.menu_item("Cluster Info", on_click=lambda: ui.navigate.to("/cluster-info"))
                ui.separator()
                ui.label(f"User: {HARDCODED_USER}").classes("text-xs text-gray-500 px-4 py-2")

        state = {
            "selected_jobs": [],
            "current_project_path": None,
            "current_scheme_name": None,
            "auto_detected_values": {},
            "job_cards": {},
            "params_snapshot": {},
            "project_created": False,
            "pipeline_running": False,
        }

        callbacks = {}

        with ui.splitter(value=30).classes("w-full").style(
            "height: 100vh; padding: 10px 20px;"
        ) as splitter:
            with splitter.before:
                build_data_import_panel(backend, state, callbacks)
            
            with splitter.after:
                build_pipeline_builder_panel(backend, state, callbacks)

        await client.connected()

    @ui.page("/state-inspector")
    async def state_inspector_page(client: Client):
        """State inspector page"""
        ui.add_head_html("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                body, .nicegui-content {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 12px !important;
                }
            </style>
        """)

        with ui.button(icon="menu").props("flat dense round").classes("fixed top-2 left-2 z-50").style(
            "background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1);"
        ):
            with ui.menu():
                ui.menu_item("Projects & Parameters", on_click=lambda: ui.navigate.to("/"))
                ui.menu_item("State Inspector", on_click=lambda: ui.navigate.to("/state-inspector"))
                ui.menu_item("Cluster Info", on_click=lambda: ui.navigate.to("/cluster-info"))
                ui.separator()
                ui.label(f"User: {HARDCODED_USER}").classes("text-xs text-gray-500 px-4 py-2")

        with ui.column().classes("w-full p-4").style("margin-top: 50px;"):
            ui.label("State Inspector").classes("text-lg font-semibold mb-4")
            load_func = build_state_inspector_tab()

        await client.connected()
        await load_func()

    @ui.page("/cluster-info")
    async def cluster_info_page(client: Client):
        """Cluster info page"""
        ui.add_head_html("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                body, .nicegui-content {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 12px !important;
                }
            </style>
        """)

        # Hamburger menu
        with ui.button(icon="menu").props("flat dense round").classes("fixed top-2 left-2 z-50").style(
            "background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1);"
        ):
            with ui.menu():
                ui.menu_item("Projects & Parameters", on_click=lambda: ui.navigate.to("/"))
                ui.menu_item("State Inspector", on_click=lambda: ui.navigate.to("/state-inspector"))
                ui.menu_item("Cluster Info", on_click=lambda: ui.navigate.to("/cluster-info"))
                ui.separator()
                ui.label(f"User: {HARDCODED_USER}").classes("text-xs text-gray-500 px-4 py-2")

        with ui.column().classes("w-full p-4").style("margin-top: 50px;"):
            ui.label("SLURM Cluster Information").classes("text-lg font-semibold mb-4")
            output_area = ui.log().classes("w-full h-96 border rounded-md p-2 bg-gray-50 text-xs font-mono")

            async def get_info():
                output_area.push("Loading sinfo...")
                result = await backend.get_slurm_info()
                output_area.clear()
                output_area.push(result["output"] if result["success"] else result["error"])

            ui.button("Get SLURM Info", on_click=get_info).props("dense flat").style(
                "background: #f3f4f6; color: #1f2937; padding: 6px 16px; border-radius: 3px; border: 1px solid #e5e7eb;"
            )

        await client.connected()
