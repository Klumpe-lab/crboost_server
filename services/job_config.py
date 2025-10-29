# services/job_config.py
from typing import List, Dict, Any
from enum import Enum
from services.job_types import JobType

class JobConfig:
    """Central configuration for job pipeline ordering and metadata"""
    
    # Define job order and dependencies
    PIPELINE_ORDER = [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
        # Future jobs (commented out for now):
        # JobType.TS_CTF,
        # JobType.DENOISE_TRAIN,
        # JobType.DENOISE_PREDICT,
        # JobType.TS_RECONSTRUCT,
        # JobType.TEMPLATE_MATCH,
        # JobType.EXTRACT_CANDIDATES,
        # JobType.SUBTOMO_RECONSTRUCT,
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