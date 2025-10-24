# services/container_service.py

from pathlib import Path
import shlex
from typing import List
from .config_service import get_config_service
from .tool_service import get_tool_service
import shlex  # Make sure this import is at the top of your file

class ContainerService:
    def __init__(self):
        config_data = get_config_service().get_config()
        self.container_paths = config_data.containers or {}
        self.tool_service = get_tool_service()
        
        self.gui_containers = {'relion'}
        self.cli_containers = {'warp_aretomo', 'cryocare', 'pytom'}

    def wrap_command_for_tool(self, command: str, cwd: Path, tool_name: str, additional_binds: List[str] = None) -> str:
        """Wrap command using the specified tool's container"""
        
        if not self.tool_service.is_container_tool(tool_name):
            print(f"[CONTAINER] Tool {tool_name} is not container-based, running natively")
            return command
        
        container_name = self.tool_service.get_container_for_tool(tool_name)
        if not container_name or container_name not in self.container_paths:
            print(f"[CONTAINER ERROR] No container found for tool {tool_name}")
            return command

        container_path = self.container_paths[container_name]
        if not Path(container_path).exists():
            print(f"[CONTAINER ERROR] Container not found: {container_path}")
            return command
        
        # Build bind mounts
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

        # HPC integration
        hpc_paths = [
            "/usr/bin", "/usr/lib64/slurm", "/run/munge", 
            "/etc/passwd", "/etc/group", "/groups", "/programs", "/software"
        ]
        for p_str in hpc_paths:
            path = Path(p_str)
            if path.exists():
                if "passwd" in p_str or "group" in p_str:
                    binds.add(f"{p_str}:{p_str}:ro")
                else:
                    binds.add(p_str)

        # X11 for GUI containers only
        if container_name in self.gui_containers:
            print(f"[CONTAINER] Adding X11 bindings for GUI container: {container_name}")
            x11_authority = Path.home() / ".Xauthority"
            x11_socket = Path("/tmp/.X11-unix")
            if x11_authority.exists():
                binds.add(f"{str(x11_authority)}:{str(x11_authority)}:ro")
            if x11_socket.exists():
                binds.add(str(x11_socket))
        
        bind_args = []
        for path in sorted(binds):
            bind_args.extend(['-B', path])
        
        # Build the apptainer command parts
        cmd_parts = ["apptainer", "run", "--nv", "--cleanenv"]
        cmd_parts.extend(bind_args)
        cmd_parts.append(container_path)

        # CRITICAL: Use the EXACT same pattern as the old working version
        # This creates a single argument: "bash -c 'command'"
        wrapped_command = f"bash -c {shlex.quote(command)}"
        cmd_parts.append(wrapped_command)

        apptainer_cmd = " ".join(cmd_parts)
        
        # Environment cleanup
        clean_env_vars = [
            "SINGULARITY_BIND", "APPTAINER_BIND", "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
            "SINGULARITY_NAME", "APPTAINER_NAME", "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
            "LD_PRELOAD", "XDG_RUNTIME_DIR", "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "CONDA_PROMPT_MODIFIER"
        ]
        
        if container_name not in self.gui_containers:
            clean_env_vars.extend(["DISPLAY", "XAUTHORITY"])
                
        clean_env_cmd = "unset " + " ".join(sorted(set(clean_env_vars)))
        final_command = f"{clean_env_cmd}; {apptainer_cmd}"
        
        print(f"[CONTAINER DEBUG] Final command structure:")
        print(f"[CONTAINER DEBUG] Parts: {cmd_parts}")
        print(f"[CONTAINER DEBUG] Full: {final_command}")
        return final_command
_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
