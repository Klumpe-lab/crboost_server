# ui/projects_tab.py (updated)
import asyncio
import json
import math
from pathlib import Path
from nicegui import ui
from backend import CryoBoostBackend
from services.parameter_models import JobType
from ui.utils import create_path_input_with_picker
from typing import Dict, Any, List

from app_state import state as app_state, update_from_mdoc
from typing import List, Dict, Any

# Import the new panel components
from ui.data_import_panel import build_data_import_panel
from ui.pipeline_builder_panel import build_pipeline_builder_panel

class JobConfig:
    """Central configuration for job pipeline ordering and metadata"""
    
    # Define job order and dependencies
    PIPELINE_ORDER = [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
    ]
    
    # Job metadata for UI display
    JOB_METADATA = {
        JobType.IMPORT_MOVIES: {
            'icon': '',
            'short_name': 'Import',
            'description': 'Import raw movies and mdocs',
        },
        JobType.FS_MOTION_CTF: {
            'icon': '',
            'short_name': 'Motion & CTF',
            'description': 'Motion correction and CTF estimation',
        },
        JobType.TS_ALIGNMENT: {
            'icon': '',
            'short_name': 'Alignment',
            'description': 'Tilt series alignment',
        },
    }
    
    @classmethod
    def get_ordered_jobs(cls) -> List[JobType]:
        """Get jobs in pipeline execution order"""
        return cls.PIPELINE_ORDER.copy()
    
    @classmethod
    def get_job_display_name(cls, job_type: JobType) -> str:
        """Get display name for a job"""
        return cls.JOB_METADATA.get(job_type, {}).get('short_name', job_type.value)
    
    @classmethod
    def get_job_icon(cls, job_type: JobType) -> str:
        """Get icon for a job"""
        return cls.JOB_METADATA.get(job_type, {}).get('icon', '📦')
    
    @classmethod
    def get_job_description(cls, job_type: JobType) -> str:
        """Get description for a job"""
        return cls.JOB_METADATA.get(job_type, {}).get('description', '')

def build_projects_tab(backend: CryoBoostBackend):
    """Projects tab with split layout"""
    
    # Shared state between panels
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

    # Callback functions that panels can use
    callbacks = {
        "rebuild_pipeline_cards": None
    }

    # Create the split layout
    with ui.splitter(value=50).classes('w-full h-[calc(100vh-100px)]') as splitter:
        with splitter.before:
            # Left panel: Data Import & Project Configuration
            data_import_state = build_data_import_panel(backend, state, callbacks)
            
        with splitter.after:
            # Right panel: Pipeline Builder
            pipeline_state = build_pipeline_builder_panel(backend, state, callbacks)

    # Return the combined load function
    async def load_page_data():
        pass

    return load_page_data