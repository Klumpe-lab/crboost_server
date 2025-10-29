# services/config_service.py
"""
Pure configuration loader - reads conf.yaml and provides typed access.
No business logic, just data loading.
"""

import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


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
    """Simple partition definition - no Parameter wrappers"""
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
    
    # Simplified partitions
    c: Optional[ComputingPartition] = None
    m: Optional[ComputingPartition] = None
    g: Optional[ComputingPartition] = None
    g_p100: Optional[ComputingPartition] = Field(None, alias='g-p100')
    g_v100: Optional[ComputingPartition] = Field(None, alias='g-v100')
    g_a100: Optional[ComputingPartition] = Field(None, alias='g-a100')


class Config(BaseModel):
    """Root configuration model"""
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
    """Loads and provides access to static configuration"""
    
    def __init__(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        # Ensure required keys exist
        if 'star_file' not in data:
            data['star_file'] = {}
        
        self._config = Config(**data)

    @property
    def config(self) -> Config:
        return self._config
    
    @property
    def containers(self) -> Dict[str, str]:
        """Get container paths"""
        return self._config.containers or {}


    @property
    def tools(self) -> Dict[str, Dict[str, str]]:
        return {
            # Relion tools
            'relion': {'container': 'relion', 'type': 'container'},
            'relion_import': {'container': 'relion', 'type': 'container'},
            'relion_schemer': {'container': 'relion', 'type': 'container'},
            
            # Warp/AreTomo tools
            'warptools': {'container': 'warp_aretomo', 'type': 'container'},
            'aretomo': {'container': 'warp_aretomo', 'type': 'container'},
            
            # Other tools
            'cryocare': {'container': 'cryocare', 'type': 'container'},
            'pytom': {'container': 'pytom', 'type': 'container'},
        }
    
    def get_container_for_tool(self, tool_name: str) -> Optional[str]:
        tool_config = self.tools.get(tool_name)
        if not tool_config:
            return None
        container_key = tool_config.get('container')
        return self.containers.get(container_key)
    
    
    def find_gpu_partition(self) -> Optional[tuple[str, ComputingPartition]]:
        for partition_key in ['g', 'g_v100', 'g_a100', 'g_p100']:
            partition = getattr(self._config.computing, partition_key, None)
            if partition:
                return (partition_key.replace('_', '-'), partition)
        return None


_config_service_instance = None

@lru_cache()
def get_config_service(config_path: str = "config/conf.yaml") -> ConfigService:
    """Get or create the config service singleton"""
    global _config_service_instance
    if _config_service_instance is None:
        path = Path.cwd() / config_path
        _config_service_instance = ConfigService(path)
    return _config_service_instance