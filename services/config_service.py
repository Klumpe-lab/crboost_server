# services/config_service.py

import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from services.parameter_models import StrParam, IntParam

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

# Refactored to use Parameter model
class ComputingPartition(BaseModel):
    
    """Computing partition with Parameter-based fields"""
    NrGPU: IntParam
    NrCPU: IntParam
    RAM: StrParam  
    VRAM: StrParam
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ComputingPartition':
        """Create from raw dict with Parameter wrapping"""
        return cls(
            NrGPU=IntParam(value=data['NrGPU'], min_value=0, max_value=8),
            NrCPU=IntParam(value=data['NrCPU'], min_value=1, max_value=128),
            RAM=StrParam(value=data['RAM']),
            VRAM=StrParam(value=data['VRAM'])
        )

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
    
    # Partitions with Parameter-based models
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
        extra = 'ignore'

class ConfigService:
    """
    ConfigService is now SUBSERVIENT to ParameterManager.
    It loads static defaults from conf.yaml and provides them
    to the parameter manager for initialization.
    """
    
    def __init__(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if 'star_file' not in data:
            data['star_file'] = {}
        
        # Convert partition dicts to Parameter-based models
        if 'computing' in data:
            for partition_key in ['c', 'm', 'g', 'g-p100', 'g-v100', 'g-a100']:
                if partition_key in data['computing']:
                    partition_data = data['computing'][partition_key]
                    data['computing'][partition_key] = ComputingPartition.from_dict(partition_data).dict()
        
        self._config = Config(**data)

    def get_config(self) -> Config:
        return self._config
    
    def get_default_computing_params(self) -> Dict[str, Any]:
        """
        Extract default computing parameters as a dict
        for ParameterManager to consume.
        """
        # Find first available GPU partition
        for partition_key in ['g', 'g_v100', 'g_a100', 'g_p100']:
            partition = getattr(self._config.computing, partition_key, None)
            if partition:
                return {
                    'partition': partition_key.replace('_', '-'),
                    'gpu_count': partition.NrGPU.value,
                    'cpu_count': partition.NrCPU.value,
                    'memory_gb': int(partition.RAM.value.replace('G', '').replace('g', '')),
                }
        
        # Fallback defaults if no GPU partition found
        return {
            'partition': 'g',
            'gpu_count': 1,
            'cpu_count': 8,
            'memory_gb': 32,
        }

    def get_job_output_filename(self, job_type: str) -> Optional[str]:
        if not self._config.star_file:
            return None
        base_job_type = job_type.split('_')[0]
        return self._config.star_file.get(base_job_type)

@lru_cache()
def get_config_service(config_path: str = "config/conf.yaml") -> ConfigService:
    path = Path.cwd() / config_path
    return ConfigService(path)