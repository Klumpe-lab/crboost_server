import asyncio
from pathlib import Path
from nicegui import ui
from ui.local_file_picker import local_file_picker
# ui/utils.py
from nicegui import ui
from typing import List, Dict, Any

from services.project_state import JobType



class JobConfig:
    """Central configuration for job pipeline ordering and metadata"""
    
    PIPELINE_ORDER = [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
        JobType.TS_CTF,
        JobType.TS_RECONSTRUCT,
        JobType.DENOISE_TRAIN,
        JobType.DENOISE_PREDICT,
        JobType.TEMPLATE_MATCH_PYTOM,
        JobType.TEMPLATE_EXTRACT_PYTOM,
        JobType.SUBTOMO_EXTRACTION,
    ]
    
    JOB_METADATA = {
        JobType.IMPORT_MOVIES: {
            'short_name': 'Import',
            'description': 'Import raw movies and mdocs',
        },
        JobType.FS_MOTION_CTF: {
            'short_name': 'Motion & CTF',
            'description': 'Motion correction and CTF estimation',
        },
        JobType.TS_ALIGNMENT: {
            'short_name': 'Alignment',
            'description': 'Tilt series alignment',
        },
        JobType.TS_CTF: {
            'short_name': 'TS CTF',
            'description': 'Tilt series CTF refinement',
        },
        JobType.TS_RECONSTRUCT: {
            'short_name': 'Reconstruct',
            'description': 'Tomogram reconstruction',
        },
        JobType.DENOISE_TRAIN: {
            'short_name': 'Denoise',
            'description': 'Train and apply denoising',
        },
        JobType.DENOISE_PREDICT: {
            'short_name': 'Denoise',
            'description': 'Train and apply denoising',
        },
        JobType.TEMPLATE_MATCH: {
            'short_name': 'Template Match',
            'description': 'Template matching for particle picking',
        },
        JobType.SUBTOMO_RECONSTRUCT: {
            'short_name': 'Subtomo Avg',
            'description': 'Subtomogram averaging',
        },
    }
    
    @classmethod
    def get_ordered_jobs(cls) -> List[JobType]:
        return cls.PIPELINE_ORDER.copy()
    
    @classmethod
    def get_job_display_name(cls, job_type: JobType) -> str:
        return cls.JOB_METADATA.get(job_type, {}).get('short_name', job_type.value)
    
    @classmethod
    def get_job_description(cls, job_type: JobType) -> str:
        return cls.JOB_METADATA.get(job_type, {}).get('description', '')

def _snake_to_title(snake_str: str) -> str:
    return " ".join(word.capitalize() for word in snake_str.split("_"))

def create_path_input_with_picker(label: str, mode: str, glob_pattern: str = '', default_value: str = '') -> ui.input:
    """A factory for creating a text input with a file/folder picker button."""

    async def _pick_path():
        start_dir = Path(path_input.value).parent if path_input.value and Path(path_input.value).exists() else '~'
        result = await local_file_picker(
            start_dir,
            mode=mode,
            glob_pattern_annotation=glob_pattern or None
        )
        if result:
            selected_path = Path(result[0])
            if mode == 'directory' and glob_pattern:
                path_input.set_value(str(selected_path / glob_pattern))
            else:
                path_input.set_value(str(selected_path))

    with ui.row().classes('w-full items-center no-wrap'):
        hint = f"Provide a path to a {mode}"
        if glob_pattern:
            hint = f"Provide a glob pattern, e.g., /path/to/files/{glob_pattern}"

        path_input = ui.input(label=label, value=default_value) \
            .classes('flex-grow') \
            .props(f'dense outlined hint="{hint}"')

        ui.button(icon='folder', on_click=_pick_path).props('flat dense')

    return path_input