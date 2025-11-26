# ui/main_ui.py
"""
Main UI router.
Refactored to hydrate state AND restore backend state from disk on load.
"""
from nicegui import ui, Client, app

from backend import CryoBoostBackend
from ui.ui_state import get_ui_state_manager, reset_ui_state_manager
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.data_import_panel import build_data_import_panel

HARDCODED_USER = "artem.kushner"


def create_ui_router(backend: CryoBoostBackend):
    """Create the UI router with all pages."""
    
    @ui.page("/")
    async def projects_page(client: Client):
        """Main projects page with hamburger menu."""
        
        # 1. Get Manager
        ui_mgr = get_ui_state_manager()
        
        # 2. Hydrate from Browser Storage
        if 'ui_state' in app.storage.user:
            ui_mgr.load_from_storage(app.storage.user['ui_state'])
        
        # 3. CRITICAL: Resync Backend Memory from Disk
        # This prevents the "Job model not found" crash on page reload.
        if ui_mgr.is_project_created and ui_mgr.project_path:
            print(f"[MAIN_UI] Restoring backend state from {ui_mgr.project_path}")
            # This loads project_params.json into the singleton ProjectState
            await backend.load_existing_project(str(ui_mgr.project_path))
        
        # 4. Setup Auto-Persistence
        def persist_state(state):
            app.storage.user['ui_state'] = state.model_dump(mode='json')
            
        ui_mgr.subscribe(persist_state)
        
        # Add custom styles
        ui.add_head_html("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap');
                
                body, .nicegui-content {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                    font-size: 12px !important;
                    font-weight: 400;
                    margin: 0 !important;
                    padding: 0 !important;
                }
                
                .q-field__native, .q-field__label, .q-select__option, 
                .q-item__label, .q-field__hint {
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
                
                /* Status dot animations */
                .status-dot {
                    transition: background-color 0.3s ease;
                }
                
                .pulse-running {
                    animation: pulse-blue 1.5s ease-in-out infinite;
                }
                
                .pulse-success {
                    background-color: #10b981 !important;
                }
                
                .pulse-failed {
                    background-color: #ef4444 !important;
                }
                
                .pulse-scheduled {
                    background-color: #fbbf24 !important;
                }
                
                @keyframes pulse-blue {
                    0%, 100% {
                        background-color: #3b82f6;
                        opacity: 1;
                    }
                    50% {
                        background-color: #60a5fa;
                        opacity: 0.7;
                    }
                }
            </style>
        """)
        
        # Hamburger menu
        with ui.button(icon="menu").props("flat dense round").classes(
            "fixed top-2 left-2 z-50"
        ).style("background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1);"):
            with ui.menu():
                ui.menu_item(
                    "Projects & Parameters",
                    on_click=lambda: ui.navigate.to("/"),
                )
                ui.menu_item(
                    "State Inspector",
                    on_click=lambda: ui.navigate.to("/state-inspector"),
                )
                ui.menu_item(
                    "Cluster Info",
                    on_click=lambda: ui.navigate.to("/cluster-info"),
                )
                ui.separator()
                ui.label(f"User: {HARDCODED_USER}").classes(
                    "text-xs text-gray-500 px-4 py-2"
                )
        
        callbacks = {}
        
        # Main layout
        with ui.splitter(value=30).classes("w-full").style(
            "height: 100vh; padding: 10px 20px;"
        ) as splitter:
            with splitter.before:
                build_data_import_panel(backend, callbacks)
            
            with splitter.after:
                build_pipeline_builder_panel(backend, callbacks)
        
        await client.connected()
    
    @ui.page("/state-inspector")
    async def state_inspector_page(client: Client):
        """Debug page to inspect current state."""
        ui_mgr = get_ui_state_manager()
        ui.add_head_html("""
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap');
                    
                    body {
                        font-family: 'IBM Plex Sans', sans-serif !important;
                    }
                </style>
            """)
            
        with ui.column().classes("w-full p-8"):
            ui.label("State Inspector").classes("text-2xl font-bold mb-4")
            
            # Back button
            ui.button("Back to Projects", on_click=lambda: ui.navigate.to("/")).props(
                "flat no-caps"
            )
            
            ui.separator().classes("my-4")
            
            # UI State section
            ui.label("UI State").classes("text-lg font-bold mt-4 mb-2")
            ui_state_display = ui.code(
                ui_mgr.state.model_dump_json(indent=2),
                language="json",
            ).classes("w-full max-h-96 overflow-auto")
            
            # Project State section
            from services.project_state import get_project_state
            project_state = get_project_state()
            
            ui.label("Project State").classes("text-lg font-bold mt-4 mb-2")
            project_state_display = ui.code(
                project_state.model_dump_json(indent=2, exclude={"project_path"}),
                language="json",
            ).classes("w-full max-h-96 overflow-auto")
            
            # Refresh button
            def refresh_displays():
                ui_state_display.content = ui_mgr.state.model_dump_json(indent=2)
                project_state_display.content = get_project_state().model_dump_json(
                    indent=2, exclude={"project_path"}
                )
                ui.notify("State refreshed", timeout=1)
            
            ui.button("Refresh State", icon="refresh", on_click=refresh_displays).props(
                "dense no-caps"
            ).classes("mt-4")
        
        await client.connected()

    @ui.page("/cluster-info")
    async def cluster_info_page(client: Client):
        """Page showing cluster/slurm information."""
        ui.add_head_html("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
                
                body {
                    font-family: 'IBM Plex Sans', sans-serif !important;
                }
            </style>
        """)
        
        with ui.column().classes("w-full p-8"):
            ui.label("Cluster Information").classes("text-2xl font-bold mb-4")
            
            # Back button
            ui.button("Back to Projects", on_click=lambda: ui.navigate.to("/")).props(
                "flat no-caps"
            )
            
            ui.separator().classes("my-4")
            
            # Slurm queue info
            ui.label("Slurm Queue").classes("text-lg font-bold mt-4 mb-2")
            
            queue_container = ui.column().classes("w-full")
            
            async def refresh_queue():
                queue_container.clear()
                with queue_container:
                    ui.label("Loading...").classes("text-gray-500")
                
                try:
                    result = await backend.slurm_service.get_queue_status()
                    queue_container.clear()
                    with queue_container:
                        if result.get("jobs"):
                            with ui.table(columns=[
                                {"name": "job_id", "label": "Job ID", "field": "job_id"},
                                {"name": "name", "label": "Name", "field": "name"},
                                {"name": "state", "label": "State", "field": "state"},
                                {"name": "time", "label": "Time", "field": "time"},
                            ], rows=result["jobs"]).classes("w-full"):
                                pass
                        else:
                            ui.label("No jobs in queue").classes("text-gray-500")
                except Exception as e:
                    queue_container.clear()
                    with queue_container:
                        ui.label(f"Error: {e}").classes("text-red-500")
            
            ui.button("Refresh Queue", icon="refresh", on_click=refresh_queue).props(
                "dense no-caps"
            )
            
            # Initial load
            await refresh_queue()
        
        await client.connected()