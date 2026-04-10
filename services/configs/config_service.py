# services/configs/config_service.py
"""
Pure configuration loader - reads conf.yaml and provides typed access.
"""

import os
import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Dict, Optional, Literal


def find_repo_root() -> Path:
    """
    Robustly find the repository root by looking for 'config/conf.yaml'
    starting from the current directory and moving up.
    """
    current = Path.cwd()
    # Check current directory first (most likely when running main.py)
    if (current / "config" / "conf.yaml").exists():
        return current

    # Fallback: check parents (in case we are running a script from a subfolder)
    for parent in current.parents:
        if (parent / "config" / "conf.yaml").exists():
            return parent

    # Last resort fallback to the old logic but corrected for the new depth
    # config_service.py is now in services/configs/, so we need .parent.parent
    return Path(__file__).resolve().parent.parent.parent


_REPO_ROOT = find_repo_root()
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


class SupervisorSlurmConfig(BaseModel):
    """
    Resources for the lightweight supervisor sbatch used by array-dispatching jobs.
    The supervisor counts tilt-series, submits a child SLURM array, polls squeue,
    and runs pure-Python metadata aggregation — no GPU work.
    The user-facing slurm config (slurm_defaults + per-job slurm_overrides) is consumed
    by the supervisor when it submits the child array job, NOT by the supervisor's own sbatch.
    """

    partition: str = "g"
    constraint: str = "g2|g3|g4"
    nodes: int = 1
    ntasks_per_node: int = 1
    cpus_per_task: int = 1
    gres: str = "gpu:1"
    mem: str = "4G"
    time: str = "4:00:00"


# Backward compat alias
TsReconstructSupervisorSlurmConfig = SupervisorSlurmConfig


class LocalConfig(BaseModel):
    DefaultProjectBase: Optional[str] = None
    DefaultMoviesGlob: Optional[str] = None
    DefaultMdocsGlob: Optional[str] = None


class ToolConfig(BaseModel):
    """Configuration for a specific external tool"""

    exec_mode: Literal["container", "binary"] = "container"
    container_path: Optional[str] = None
    bin_path: Optional[str] = None


class ProcessingDefaultsConfig(BaseModel):
    reconstruction_binning: int = 4


class Config(BaseModel):
    """Root configuration model"""

    crboost_root: str = Field(default_factory=lambda: str(_REPO_ROOT))
    venv_path: Optional[str] = None
    local: LocalConfig = Field(default_factory=LocalConfig)
    slurm_defaults: SlurmDefaultsConfig = Field(default_factory=SlurmDefaultsConfig)
    # Accepts both new key "supervisor_slurm" and legacy "tsreconstruct_supervisor_slurm"
    supervisor_slurm: SupervisorSlurmConfig = Field(default_factory=SupervisorSlurmConfig)
    tsreconstruct_supervisor_slurm: Optional[SupervisorSlurmConfig] = None
    processing_defaults: ProcessingDefaultsConfig = Field(default_factory=ProcessingDefaultsConfig)
    tools: Dict[str, ToolConfig] = Field(default_factory=dict)
    containers: Optional[Dict[str, str]] = None

    class Config:
        extra = "ignore"


class ConfigService:
    """Loads and provides access to static configuration"""

    def __init__(self, config_path: Path = None):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH

        if not config_path.exists():
            # Diagnostic info to help debug future path shifts
            raise FileNotFoundError(
                f"Configuration file not found at: {config_path}\n"
                f"Repo Root identified as: {_REPO_ROOT}\n"
                f"Run 'python preflight.py' to create one from the template."
            )

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Migrate legacy key: tsreconstruct_supervisor_slurm → supervisor_slurm
        if "tsreconstruct_supervisor_slurm" in data and "supervisor_slurm" not in data:
            data["supervisor_slurm"] = data.pop("tsreconstruct_supervisor_slurm")

        self._config = Config(**data)

    @property
    def config(self) -> Config:
        return self._config

    @property
    def crboost_root(self) -> Path:
        return Path(self._config.crboost_root)

    @property
    def processing_defaults(self) -> ProcessingDefaultsConfig:
        return self._config.processing_defaults

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
    def supervisor_slurm_defaults(self) -> SupervisorSlurmConfig:
        return self._config.supervisor_slurm

    @property
    def tsreconstruct_supervisor_slurm_defaults(self) -> SupervisorSlurmConfig:
        """Backward compat alias."""
        return self.supervisor_slurm_defaults

    @property
    def default_project_base(self) -> Optional[str]:
        return self._config.local.DefaultProjectBase

    @property
    def default_data_globs(self) -> tuple[Optional[str], Optional[str]]:
        return (self._config.local.DefaultMoviesGlob, self._config.local.DefaultMdocsGlob)

    def get_tool_config(self, tool_name: str) -> ToolConfig:
        if tool_name in self._config.tools:
            return self._config.tools[tool_name]

        legacy_mapping = {
            "warptools": "warp_aretomo",
            "aretomo": "warp_aretomo",
            "relion_import": "relion",
            "relion_schemer": "relion",
        }
        lookup_name = legacy_mapping.get(tool_name, tool_name)

        if lookup_name in self._config.tools:
            return self._config.tools[lookup_name]

        if self._config.containers and lookup_name in self._config.containers:
            return ToolConfig(exec_mode="container", container_path=self._config.containers[lookup_name])

        return ToolConfig(exec_mode="binary", bin_path=tool_name)

    def get_tool_path(self, tool_name: str) -> Optional[str]:
        config = self.get_tool_config(tool_name)
        if config.exec_mode == "container":
            return config.container_path
        return config.bin_path


_config_service_instance: Optional[ConfigService] = None


def get_config_service() -> ConfigService:
    global _config_service_instance
    if _config_service_instance is None:
        _config_service_instance = ConfigService()
    return _config_service_instance


def reset_config_service():
    global _config_service_instance
    _config_service_instance = None
