# services/container_service.py

from pathlib import Path
import shlex
from typing import List
from .config_service import get_config_service
from .tool_service import get_tool_service
import shlex 

class ContainerService:

    def __init__(self):
        config_data = get_config_service().get_config()
        self.container_paths = config_data.containers or {}
        self.tool_service = get_tool_service()
        
        self.gui_containers = {'relion'}
        self.cli_containers = {'warp_aretomo', 'cryocare', 'pytom'}

    def wrap_command_for_tool(self, command: str, cwd: Path, tool_name: str, additional_binds: List[str] = None) -> str:
        """Simple wrapper that matches the old working code"""
        
        if not self.tool_service.is_container_tool(tool_name):
            return command
        
        container_name = self.tool_service.get_container_for_tool(tool_name)
        if not container_name or container_name not in self.container_paths:
            return command

        container_path = self.container_paths[container_name]
        if not Path(container_path).exists():
            return command
        
        # Build bind mounts (your existing logic)
        binds = set()
        essential_paths = ["/tmp", "/scratch", str(Path.home()), str(cwd.resolve())]
        for p in essential_paths:
            if Path(p).exists():
                binds.add(str(Path(p).resolve()))

        if additional_binds:
            for p in additional_binds:
                path = Path(p).resolve()
                if path.exists():
                    binds.add(str(path))

        # Add HPC paths
        hpc_paths = ["/usr/bin", "/usr/lib64/slurm", "/run/munge", "/etc/passwd", "/etc/group", "/groups", "/programs", "/software"]
        for p_str in hpc_paths:
            path = Path(p_str)
            if path.exists():
                if "passwd" in p_str or "group" in p_str:
                    binds.add(f"{p_str}:{p_str}:ro")
                else:
                    binds.add(p_str)

        bind_args = []
        for path in sorted(binds):
            bind_args.extend(['-B', path])
        
        # Simple command construction like your old working version
        inner_command_quoted = shlex.quote(command)
        apptainer_cmd_parts = [
            "apptainer", "run", "--nv", "--cleanenv",
            *bind_args,
            container_path,
            "bash", "-c", inner_command_quoted
        ]
        
        apptainer_cmd = " ".join(apptainer_cmd_parts)
        
        # Environment cleaning
        clean_env_vars = [
            "SINGULARITY_BIND", "APPTAINER_BIND", "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
            "SINGULARITY_NAME", "APPTAINER_NAME", "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER", 
            "LD_PRELOAD", "XDG_RUNTIME_DIR", "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "CONDA_PROMPT_MODIFIER"
        ]
        
        if container_name not in self.gui_containers:
            clean_env_vars.extend(["DISPLAY", "XAUTHORITY"])
            
        clean_env_cmd = "unset " + " ".join(clean_env_vars)
        final_command = f"{clean_env_cmd}; {apptainer_cmd}"
        
        return final_command

_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
