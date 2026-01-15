# services/config_service.py
"""
Pure configuration loader - reads conf.yaml and provides typed access.
"""

import os
import yaml
from pathlib import Path
from functools import lru_cache
from pydantic import BaseModel, Field
from typing import Dict, Optional, Literal

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


class ToolConfig(BaseModel):
    """Configuration for a specific external tool"""
    exec_mode: Literal["container", "binary"] = "container"
    container_path: Optional[str] = None
    bin_path: Optional[str] = None


class Config(BaseModel):
    """Root configuration model"""

    crboost_root: str = Field(default_factory=lambda: str(_REPO_ROOT))
    venv_path: Optional[str] = None
    local: LocalConfig = Field(default_factory=LocalConfig)
    slurm_defaults: SlurmDefaultsConfig = Field(default_factory=SlurmDefaultsConfig)
    
    # Replaces the old simple dict[str, str] containers map
    tools: Dict[str, ToolConfig] = Field(default_factory=dict)
    
    # Legacy support if user hasn't migrated conf.yaml yet
    containers: Optional[Dict[str, str]] = None

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
                f"Run 'python preflight.py' to create one from the template."
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
    def slurm_defaults(self) -> SlurmDefaultsConfig:
        return self._config.slurm_defaults

    @property
    def default_project_base(self) -> Optional[str]:
        return self._config.local.DefaultProjectBase

    @property
    def default_data_globs(self) -> tuple[Optional[str], Optional[str]]:
        return (self._config.local.DefaultMoviesGlob, self._config.local.DefaultMdocsGlob)

    # --- Tool / Container Management ---

    def get_tool_config(self, tool_name: str) -> ToolConfig:
        """
        Retrieves config for a specific tool. 
        Falls back to legacy 'containers' dict if 'tools' entry is missing.
        """
        # 1. Try new 'tools' section
        if tool_name in self._config.tools:
            return self._config.tools[tool_name]
        
        # 2. Map known aliases for backward compatibility lookup
        # (This maps the key used in code to the key used in conf.yaml)
        # In the new system, we expect the key in conf.yaml to match the tool_name requested.
        # But for legacy, we need to handle the mapping.
        legacy_mapping = {
            "warptools": "warp_aretomo",
            "aretomo": "warp_aretomo",
            "relion_import": "relion",
            "relion_schemer": "relion",
        }
        lookup_name = legacy_mapping.get(tool_name, tool_name)

        if lookup_name in self._config.tools:
            return self._config.tools[lookup_name]

        # 3. Fallback to legacy 'containers' section if available
        if self._config.containers and lookup_name in self._config.containers:
            return ToolConfig(
                exec_mode="container",
                container_path=self._config.containers[lookup_name]
            )
            
        # 4. Return default (assume binary in path if nothing configured?)
        # Or return None to let caller handle it. Returning default safe-fail.
        return ToolConfig(exec_mode="binary", bin_path=tool_name)

    def get_tool_path(self, tool_name: str) -> Optional[str]:
        """
        Returns the filesystem path for the tool.
        If container: returns path to .sif
        If binary: returns path to binary
        """
        config = self.get_tool_config(tool_name)
        if config.exec_mode == "container":
            return config.container_path
        return config.bin_path


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
