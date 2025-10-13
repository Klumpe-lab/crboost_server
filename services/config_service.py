# services/config_service.py (updated)

import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union

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

class NodeSharingConfig(BaseModel):
    CPU_PerGPU: int = Field(alias='CPU-PerGPU')
    ApplyTo: List[str]

class ComputingConfig(BaseModel):
    QueSize: Dict[str, int]
    NODE_Sharing: NodeSharingConfig = Field(alias='NODE-Sharing')
    JOBTypesCompute: Dict[str, List[str]]
    JOBTypesApplication: Dict[str, List[str]]
    JOBMaxNodes: Dict[str, List[int]]
    JOBsPerDevice: Dict[str, Dict[str, int]]
    
    # Make all partitions optional with safe defaults
    c: Optional[ComputingPartition] = None
    m: Optional[ComputingPartition] = None
    g: Optional[ComputingPartition] = None
    g_p100: Optional[ComputingPartition] = Field(None, alias='g-p100')
    g_v100: Optional[ComputingPartition] = Field(None, alias='g-v100')
    g_a100: Optional[ComputingPartition] = Field(None, alias='g-a100')

class Config(BaseModel):
    submission: List[SubmissionConfig]
    local: LocalConfig
    aliases: List[Alias]
    meta_data: Dict[str, List[Dict[str, str]]]
    microscopes: Dict[str, List[Dict[str, str]]]
    
    star_file: Optional[Dict[str, str]] = None
    
    containers: Optional[Dict[str, str]] = None 
    computing: ComputingConfig
    filepath: Dict[str, str]

    class Config:
        extra = 'ignore'  # Ignore extra fields

class ConfigService:
    def __init__(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if 'star_file' not in data:
            data['star_file'] = {}
        
        self._config = Config(**data)

    def get_config(self) -> Config:
        return self._config

    def get_job_output_filename(self, job_type: str) -> Optional[str]:
        if not self._config.star_file:
            return None
        base_job_type = job_type.split('_')[0]
        return self._config.star_file.get(base_job_type)

@lru_cache()
def get_config_service(config_path: str = "config/conf.yaml") -> ConfigService:
    path = Path.cwd() / config_path
    return ConfigService(path)