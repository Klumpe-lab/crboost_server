# services/container_service.py

from pathlib import Path
import shlex
import os
from typing import List, Optional
from .config_service import get_config_service
from .tool_service import get_tool_service

class ContainerService:
    def __init__(self):
        config_data = get_config_service().get_config()
        self.container_paths = config_data.containers or {}
        self.tool_service = get_tool_service()
        
        # Define which containers need X11/GUI support
        self.gui_containers = {
            'relion',  # Relion needs GUI for some operations
            # Add other GUI containers here if needed
        }
        # CLI-only containers (no X11 needed)
        self.cli_containers = {
            'warp_aretomo',  # WarpTools is CLI-only
            'cryocare',      # CryoCARE is CLI-only  
            'pytom',         # PyTom is CLI-only
        }

    def wrap_command_for_tool(self, command: str, cwd: Path, tool_name: str, additional_binds: List[str] = None) -> str:
        """Wrap command using the specified tool's container"""
        
        # Check if this is a container-based tool
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
            path = Path(p)
            if path.exists():
                binds.add(str(path.resolve()))

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

        # CRITICAL FIX: Only add X11 bindings for GUI containers
        if container_name in self.gui_containers:
            print(f"[CONTAINER] Adding X11 bindings for GUI container: {container_name}")
            x11_authority = Path.home() / ".Xauthority"
            x11_socket = Path("/tmp/.X11-unix")
            if x11_authority.exists():
                binds.add(f"{str(x11_authority)}:{str(x11_authority)}:ro")
            if x11_socket.exists():
                binds.add(str(x11_socket))
        else:
            print(f"[CONTAINER] Skipping X11 bindings for CLI container: {container_name}")
        
        bind_args = []
        for path in sorted(list(binds)):
            bind_args.extend(['-B', path])
        
        args = ["apptainer", "run", "--nv", "--cleanenv"]
        args.extend(bind_args)
        args.append(container_path)
        
        wrapped_inner_command = f"bash -c {shlex.quote(command)}"
        args.append(wrapped_inner_command)

        apptainer_cmd = " ".join(args)

        if container_name in self.gui_containers:
            display_var = os.getenv('DISPLAY', ':0.0')
            args.extend([f"--env", f"DISPLAY={display_var}"])
            
            inner_command = f"""
            unset PYTHONPATH
            unset PYTHONHOME
            unset CONDA_PREFIX
            unset CONDA_DEFAULT_ENV
            unset CONDA_PROMPT_MODIFIER
            export PATH="/opt/miniconda3/envs/relion-5.0/bin:/opt/miniconda3/bin:/opt/relion-5.0/build/bin:/usr/local/cuda-11.8/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            {command}
            """
        else:
            # CLI containers - no DISPLAY, simpler environment
            inner_command = f"""
            unset PYTHONPATH
            unset PYTHONHOME
            unset CONDA_PREFIX
            unset CONDA_DEFAULT_ENV
            unset DISPLAY  # Make sure DISPLAY is unset for CLI containers
            {command} "$@"
            """
        
        wrapped_command = f"bash -c {shlex.quote(inner_command.strip())}"
        args.append(container_path)
        args.append(wrapped_command)

        full_command = " ".join(args)
        
        # Clean host environment
        clean_env_vars = [
            "SINGULARITY_BIND", "APPTAINER_BIND", "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
            "SINGULARITY_NAME", "APPTAINER_NAME", "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
            "LD_PRELOAD", "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY",
            "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "CONDA_PROMPT_MODIFIER"
        ]
        
        args = ["apptainer", "run", "--nv", "--cleanenv"]
        args.extend(bind_args)
        args.append(container_path)

        # The raw command needs to be wrapped for bash to handle things like '&&'
        # We use shlex.quote to make it safe.
        wrapped_inner_command = f"bash -c {shlex.quote(command)}"
        args.append(wrapped_inner_command)

        # Join the main apptainer command parts
        apptainer_cmd = " ".join(args)
        clean_env_vars = [
                "SINGULARITY_BIND", "APPTAINER_BIND", "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
                "SINGULARITY_NAME", "APPTAINER_NAME", "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
                "LD_PRELOAD", "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY",
                "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "CONDA_PROMPT_MODIFIER"
            ]
        
        # For CLI containers, also clean X11-related env vars
        container_name = self.tool_service.get_container_for_tool(tool_name)
        if container_name not in self.gui_containers:
            clean_env_vars.extend(["DISPLAY", "XAUTHORITY"])
            
        clean_env_cmd = "unset " + " ".join(list(set(clean_env_vars))) # Use set to remove duplicates

        # The final command unsets host variables, THEN runs the apptainer command.
        # Relion will append its arguments after this entire string.
        final_command = f"{clean_env_cmd}; {apptainer_cmd}"
        
        print(f"[CONTAINER] Wrapping command for tool '{tool_name}'")
        return final_command

_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
