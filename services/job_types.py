# services/job_types.py
from enum import Enum
from typing import Type, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from services.parameter_models import BaseModel

class JobType(str, Enum):
    """Enumeration of all pipeline job types"""
    IMPORT_MOVIES = "importmovies"
    FS_MOTION_CTF = "fsMotionAndCtf"
    TS_ALIGNMENT = "tsAlignment"
    
    @classmethod
    def from_string(cls, value: str) -> 'JobType':
        """Safe conversion from string with better error message"""
        try:
            return cls(value)
        except ValueError:
            valid = [e.value for e in cls]
            raise ValueError(f"Unknown job type '{value}'. Valid types: {valid}")
    
    @property
    def display_name(self) -> str:
        """Human-readable name"""
        return self.value.replace('_', ' ').title()


# Import here to avoid circular dependency
def get_job_param_classes() -> Dict[JobType, Type['BaseModel']]:
    """Lazy import to avoid circular dependencies"""
    from services.parameter_models import (
        ImportMoviesParams, FsMotionCtfParams, TsAlignmentParams
    )
    return {
        JobType.IMPORT_MOVIES: ImportMoviesParams,
        JobType.FS_MOTION_CTF: FsMotionCtfParams,
        JobType.TS_ALIGNMENT: TsAlignmentParams,
    }