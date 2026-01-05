# services/config_service.py
"""
Pure configuration loader - reads conf.yaml and provides typed access.
"""

import os
import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import Dict, Optional

CRBOOST_ROOT = "/users/artem.kushner/dev/crboost_server/"
DEFAULT_CONFIG_PATH = os.path.join(CRBOOST_ROOT, "config", "conf.yaml")


class SlurmDefaultsConfig(BaseModel):
    """SLURM submission defaults from conf.yaml"""

    partition: str = "g"
    constraint: str = "g2|g3|g4"
    nodes: int = 1
    ntasks_per_node: int = 1
    cpus_per_task: int = 4
    gres: str = "gpu:4"
    mem: str = "64G"
    time: str = "3:30:00"


class LocalConfig(BaseModel):
    DefaultProjectBase: Optional[str] = None
    DefaultMoviesGlob: Optional[str] = None
    DefaultMdocsGlob: Optional[str] = None


class Config(BaseModel):
    """Root configuration model"""

    local         : LocalConfig         = Field(default_factory=LocalConfig)
    slurm_defaults: SlurmDefaultsConfig = Field(default_factory=SlurmDefaultsConfig)
    containers    : Dict[str, str]      = Field(default_factory=dict)

    class Config:
        extra = "ignore"


class ConfigService:
    """Loads and provides access to static configuration"""

    def __init__(self, config_path: Path):
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        self._config = Config(**data)

    @property
    def config(self) -> Config:
        return self._config

    @property
    def containers(self) -> Dict[str, str]:
        return self._config.containers

    @property
    def slurm_defaults(self) -> SlurmDefaultsConfig:
        return self._config.slurm_defaults

    @property
    def default_project_base(self) -> Optional[str]:
        return self._config.local.DefaultProjectBase

    @property
    def default_data_globs(self) -> tuple[Optional[str], Optional[str]]:
        return (self._config.local.DefaultMoviesGlob, self._config.local.DefaultMdocsGlob)

    @property
    def tools(self) -> Dict[str, Dict[str, str]]:
        """Tool to container mapping"""
        return {
            "relion": {"container": "relion", "type": "container"},
            "relion_import": {"container": "relion", "type": "container"},
            "relion_schemer": {"container": "relion", "type": "container"},
            "warptools": {"container": "warp_aretomo", "type": "container"},
            "aretomo": {"container": "warp_aretomo", "type": "container"},
            "cryocare": {"container": "cryocare", "type": "container"},
            "pytom": {"container": "pytom", "type": "container"},
        }

    def get_container_for_tool(self, tool_name: str) -> Optional[str]:
        tool_config = self.tools.get(tool_name)
        if not tool_config:
            return None
        container_key = tool_config.get("container")
        return self.containers.get(container_key)


_config_service_instance = None


@lru_cache()
def get_config_service() -> ConfigService:
    global _config_service_instance
    if _config_service_instance is None:
        _config_service_instance = ConfigService(Path(DEFAULT_CONFIG_PATH))
    return _config_service_instance
