# ui/main_ui.py (REFACTORED - NO HEADER)
from nicegui import ui, Client
from backend import CryoBoostBackend
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from .data_import_panel import build_data_import_panel

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

        await client.connected()
