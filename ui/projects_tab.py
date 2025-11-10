# ui/projects_tab.py (updated)
import asyncio
import json
import math
from pathlib import Path
from nicegui import ui
from backend import CryoBoostBackend
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.utils import create_path_input_with_picker
from typing import Dict, Any, List

from services.state_old.app_state import state as app_state, update_from_mdoc
from typing import List, Dict, Any

# Import the new panel components
from ui.data_import_panel import build_data_import_panel

def build_projects_tab(backend: CryoBoostBackend):
    """Projects tab with split layout"""
    
    state = {
        "selected_jobs"       : [],
        "current_project_path": None,
        "current_scheme_name" : None,
        "auto_detected_values": {},
        "job_cards"           : {},
        "params_snapshot"     : {},
        "project_created"     : False,
        "pipeline_running"    : False,
        "panels_built"        : False,
    }

    callbacks = {
        "rebuild_pipeline_cards": None
    }

    # Guard against double-build
    if state.get("panels_built", False):
        print("[WARN] Panels already built!")
        async def noop():
            pass
        return noop

    # Create the split layout
    with ui.splitter(value=30).classes('w-full h-[calc(100vh-100px)]') as splitter:
        with splitter.before:
            _ = build_data_import_panel(backend, state, callbacks)
            
        with splitter.after:
            _ = build_pipeline_builder_panel(backend, state, callbacks)
    
    state["panels_built"] = True  

    # Return the combined load function
    async def load_page_data():
        pass

    return load_page_data