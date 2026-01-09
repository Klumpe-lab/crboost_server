# ui/workspace_page.py
import asyncio
from nicegui import ui
from backend import CryoBoostBackend
from services.project_state import get_project_state
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.ui_state import get_ui_state_manager

def build_workspace_page(backend: CryoBoostBackend):
    ui_mgr = get_ui_state_manager()
    state = get_project_state()
    
    # ===================================================================
    # SAFETY CHECKS - Prevent rendering with stale/invalid state
    # ===================================================================
    if not ui_mgr.is_project_created:
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("error_outline", size="64px").classes("text-red-400")
            ui.label("No project loaded").classes("text-xl text-gray-600")
            ui.button("Return to Start", icon="home", on_click=lambda: ui.navigate.to("/"))
        return
    
    if not ui_mgr.project_path or not ui_mgr.project_path.exists():
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("folder_off", size="64px").classes("text-orange-400")
            ui.label("Project path is invalid").classes("text-xl text-gray-600")
            ui.label(f"Path: {ui_mgr.project_path}").classes("text-sm text-gray-400 font-mono")
            ui.button("Return to Start", icon="home", on_click=lambda: ui.navigate.to("/"))
        return
    
    # ===================================================================
    # FORCE SYNC FUNCTION - Nuclear option for users
    # ===================================================================
    async def force_full_sync():
        """Reload everything from disk and rebuild UI."""
        project_path = ui_mgr.project_path
        
        if not project_path:
            ui.notify("No project loaded", type="warning")
            return
        
        try:
            # 1. Reload backend state from disk
            result = await backend.load_existing_project(str(project_path))
            
            if not result.get("success"):
                ui.notify(f"Sync failed: {result.get('error')}", type="negative")
                return
            
            # 2. Force status sync
            await backend.pipeline_runner.status_sync.sync_all_jobs(str(project_path))
            
            # 3. Re-sync UI state with backend
            fresh_state = backend.state_service.state
            ui_mgr.load_from_project(
                project_path=fresh_state.project_path,
                scheme_name="default",
                jobs=list(fresh_state.jobs.keys())
            )
            
            # 4. Trigger UI rebuild
            ui_mgr.request_rebuild()
            
            ui.notify("State synced from disk", type="positive")
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            ui.notify(f"Sync error: {e}", type="negative")
    
    # ===================================================================
    # HEADER
    # ===================================================================
    with ui.header().classes("bg-white border-b border-gray-200 text-gray-800 h-auto px-4 py-2"):
        with ui.row().classes("w-full items-center justify-between"):
            # Left: Project info
            with ui.row().classes("items-center gap-6"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("layers", size="20px").classes("text-blue-600")
                    ui.label(state.project_name).classes("font-bold text-sm")
                
                # Project metadata - compact display
                with ui.row().classes("items-center gap-4 text-xs text-gray-500"):
                    if state.project_path:
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("folder", size="14px").classes("text-gray-400")
                            ui.label(str(state.project_path)).classes("font-mono")
                    
                    if state.movies_glob:
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("movie", size="14px").classes("text-gray-400")
                            ui.label(state.movies_glob).classes("font-mono")
                    
                    if state.mdocs_glob:
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("description", size="14px").classes("text-gray-400")
                            ui.label(state.mdocs_glob).classes("font-mono")
            
            # Right: Actions
            with ui.row().classes("items-center gap-2"):
                # Sync button - gives users control
                ui.button(
                    icon="sync", 
                    on_click=force_full_sync
                ).props("flat dense round").classes("text-blue-600").tooltip("Reload from disk")
                
                # Close project
                ui.button(
                    "Close Project", 
                    icon="close", 
                    on_click=lambda: ui.navigate.to("/")
                ).props("flat dense no-caps text-color=red").classes("text-xs")

    # ===================================================================
    # MAIN CONTENT
    # ===================================================================
    callbacks = {
        "force_sync": force_full_sync  # Pass to pipeline builder if needed
    }
    
    with ui.column().classes("w-full h-[calc(100vh-3rem)] p-0"):
        build_pipeline_builder_panel(backend, callbacks)