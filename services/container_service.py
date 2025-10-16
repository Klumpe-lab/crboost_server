# services/container_service.py

from pathlib import Path
import shlex
import os
from typing import List, Optional
from .config_service import get_config_service

class ContainerService:
    def __init__(self):
        config_data = get_config_service().get_config()
        self.container_paths = config_data.containers or {}
        
        self.tool_to_container = {
            "WarpTools": "warp_aretomo",
            "AreTomo": "warp_aretomo",
            "cryoCARE_extract_train_data.py": "cryocare",
            "cryoCARE_train.py": "cryocare",
            "cryoCARE_predict.py": "cryocare",
            "pytom_extract_candidates.py": "pytom",
            "pytom_match_template.py": "pytom",
            "pytom_merge_stars.py": "pytom",
            "relion": "relion",
            "relion_": "relion",
            "relion_python": "relion",  # Add this for Python-based relion commands
        }

    def _find_container_key(self, tool_name: str) -> Optional[str]:
        if tool_name in self.tool_to_container:
            return self.tool_to_container[tool_name]
        for prefix, key in self.tool_to_container.items():
            if tool_name.startswith(prefix):
                return key
        return None

    def wrap_command(self, command: str, cwd: Path, additional_binds: List[str] = None) -> str:
        tool_name = command.split()[0]
        container_key = self._find_container_key(tool_name)
        
        print(f"[CONTAINER DEBUG] Tool: {tool_name}, Container Key: {container_key}")
        
        # FOR DEBUG: Force relion container for Python commands during debug
        if "python" in command and container_key is None:
            print(f"[CONTAINER DEBUG] Forcing relion container for Python debug command")
            container_key = "relion"
        
        if not container_key or container_key not in self.container_paths:
            print(f"[CONTAINER DEBUG] No container found, using host command")
            return command

        
        print(f"[CONTAINER DEBUG] Tool: {tool_name}, Container Key: {container_key}")
        
        if not container_key or container_key not in self.container_paths:
            print(f"[CONTAINER DEBUG] No container found, using host command")
            return command

        container_path = self.container_paths[container_key]
        print(f"[CONTAINER DEBUG] Using container: {container_path}")
        
        # Check if container exists
        if not Path(container_path).exists():
            print(f"[CONTAINER ERROR] Container not found: {container_path}")
            return command
        
        binds = set()
        # Essential binds
        essential_paths = ["/tmp", "/scratch", str(Path.home()), str(cwd.resolve())]
        for p in essential_paths:
            path = Path(p)
            if path.exists():
                binds.add(str(path.resolve()))

        # Add additional binds
        if additional_binds:
            for p in additional_binds:
                path = Path(p).resolve()
                if path.exists():
                    binds.add(str(path))

        # HPC integration - CRITICAL: These must exist for Slurm
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

        # X11 for GUI
        x11_authority = Path.home() / ".Xauthority"
        x11_socket = Path("/tmp/.X11-unix")
        if x11_authority.exists():
            binds.add(f"{str(x11_authority)}:{str(x11_authority)}:ro")
        if x11_socket.exists():
            binds.add(str(x11_socket))
        
        bind_args = []
        for path in sorted(list(binds)):
            bind_args.extend(['-B', path])
        
        print(f"[CONTAINER DEBUG] Bind paths: {binds}")
        
        args = ["apptainer", "exec", "--nv", "--cleanenv"]
        args.extend(bind_args)
        
        if container_key == "relion":
            display_var = os.getenv('DISPLAY', ':0.0')
            args.extend([f"--env", f"DISPLAY={display_var}"])
            
            # CRITICAL: Use the exact command structure that worked manually
            inner_command = f"""
            unset PYTHONPATH
            unset PYTHONHOME
            # Clean any conda environment from host
            unset CONDA_PREFIX
            unset CONDA_DEFAULT_ENV
            unset CONDA_PROMPT_MODIFIER
            # Set clean PATH that only includes container binaries
            export PATH="/opt/miniconda3/envs/relion-5.0/bin:/opt/miniconda3/bin:/opt/relion-5.0/build/bin:/usr/local/cuda-11.8/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            # Execute the command
            {command}
            """
            wrapped_command = f"bash -c {shlex.quote(inner_command.strip())}"
            args.append(container_path)
            args.append(wrapped_command)
        else:
            # For non-relion containers
            inner_command = f"""
            unset PYTHONPATH
            unset PYTHONHOME
            unset CONDA_PREFIX
            unset CONDA_DEFAULT_ENV
            {command}
            """
            wrapped_command = f"bash -c {shlex.quote(inner_command.strip())}"
            args.append(container_path)
            args.append(wrapped_command)

        full_command = " ".join(args)
        
        print(f"[CONTAINER DEBUG] Full container command: {full_command}")
        
        # Clean host environment variables that might interfere
        clean_env_vars = [
            "SINGULARITY_BIND", "APPTAINER_BIND", "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
            "SINGULARITY_NAME", "APPTAINER_NAME", "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
            "LD_PRELOAD", "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY",
            "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "CONDA_PROMPT_MODIFIER"
        ]
        clean_env_cmd = "unset " + " ".join(clean_env_vars)
        
        final_command = f"{clean_env_cmd}; {full_command}"
        print(f"[CONTAINER DEBUG] Final command with env cleanup: {final_command}")
        return final_command

_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service