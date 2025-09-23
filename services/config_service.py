# services/config_service.py (Corrected again for forgiveness)

import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# --- Pydantic Models for your specific conf.yaml ---

class SubmissionConfig(BaseModel):
    HeadNode: str
    SshCommand: str
    Environment: str
    ClusterStatus: str
    Helpssh: str
    HelpConflict: str

class LocalConfig(BaseModel):
    Environment: str

class Alias(BaseModel):
    Job: str
    Parameter: str
    Alias: str
    
class ComputingPartition(BaseModel):
    NrGPU: int
    NrCPU: int
    RAM: str
    VRAM: str

class ComputingConfig(BaseModel):
    QueSize: Dict[str, int]
    NODE_Sharing: Dict[str, Any] = Field(alias='NODE-Sharing')
    JOBTypesCompute: Dict[str, List[str]]
    JOBTypesApplication: Dict[str, List[str]]
    JOBMaxNodes: Dict[str, List[int]]
    JOBsPerDevice: Dict[str, Dict[str, int]]
    
    c: Optional[ComputingPartition] = None
    m: Optional[ComputingPartition] = None
    g: Optional[ComputingPartition] = None
    g_p100: Optional[ComputingPartition] = Field(None, alias='g-p100')
    g_v100: Optional[ComputingPartition] = Field(None, alias='g-v100')
    g_a100: Optional[ComputingPartition] = Field(None, alias='g-a100')

class Config(BaseModel):
    """The root model for the entire conf.yaml file."""
    submission: List[SubmissionConfig]
    local: LocalConfig
    aliases: List[Alias]
    meta_data: Dict[str, List[Dict[str, str]]]
    microscopes: Dict[str, List[Dict[str, str]]]
    
    # FIX: Made the star_file field optional to prevent startup crash.
    star_file: Optional[Dict[str, str]] = Field(None, alias='star_file')
    
    computing: ComputingConfig
    filepath: Dict[str, str]

# --- Service Implementation ---

class ConfigService:
    """A singleton service to load and provide access to the application config."""
    _config: Config = None

    def __init__(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        self._config = Config(**data)

    def get_config(self) -> Config:
        return self._config

    def get_job_output_filename(self, job_type: str) -> Optional[str]:
        """Gets the standard output STAR file name for a given job type."""
        # Add a check to ensure star_file was loaded before trying to access it.
        if not self._config.star_file:
            return None
        base_job_type = job_type.split('_')[0]
        return self._config.star_file.get(base_job_type)

@lru_cache()
def get_config_service(config_path: str = "config/conf.yaml") -> ConfigService:
    """Factory function to get the single instance of the ConfigService."""
    path = Path.cwd() / config_path
    return ConfigService(path)