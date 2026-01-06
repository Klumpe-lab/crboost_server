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

# Derive default config path from this file's location
_THIS_DIR = Path(__file__).parent
_REPO_ROOT = _THIS_DIR.parent
DEFAULT_CONFIG_PATH = _REPO_ROOT / "config" / "conf.yaml"


class SlurmDefaultsConfig(BaseModel):
    """SLURM submission defaults from conf.yaml"""

    partition: str = "g"
    constraint: str = ""
    nodes: int = 1
    ntasks_per_node: int = 1
    cpus_per_task: int = 4
    gres: str = "gpu:1"
    mem: str = "32G"
    time: str = "2:00:00"


class LocalConfig(BaseModel):
    DefaultProjectBase: Optional[str] = None
    DefaultMoviesGlob: Optional[str] = None
    DefaultMdocsGlob: Optional[str] = None


class Config(BaseModel):
    """Root configuration model"""

    crboost_root: str = Field(default_factory=lambda: str(_REPO_ROOT))
    venv_path: Optional[str] = None
    local: LocalConfig = Field(default_factory=LocalConfig)
    slurm_defaults: SlurmDefaultsConfig = Field(default_factory=SlurmDefaultsConfig)
    containers: Dict[str, str] = Field(default_factory=dict)

    class Config:
        extra = "ignore"


class ConfigService:
    """Loads and provides access to static configuration"""

    def __init__(self, config_path: Path = None):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH
            
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at: {config_path}\n"
                f"Run 'python setup.py' to create one from the template."
            )

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        self._config = Config(**data)

    @property
    def config(self) -> Config:
        return self._config

    @property
    def crboost_root(self) -> Path:
        return Path(self._config.crboost_root)

    @property
    def venv_path(self) -> Optional[Path]:
        if self._config.venv_path:
            return Path(self._config.venv_path)
        return None

    @property
    def venv_python(self) -> Optional[Path]:
        if self.venv_path:
            return self.venv_path / "bin" / "python3"
        return None

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


def get_config_service() -> ConfigService:
    global _config_service_instance
    if _config_service_instance is None:
        _config_service_instance = ConfigService()
    return _config_service_instance


def reset_config_service():
    """Reset the singleton - useful for testing or after config changes"""
    global _config_service_instance
    _config_service_instance = None
