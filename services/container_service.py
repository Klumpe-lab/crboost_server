# services/container_service.py

from pathlib import Path
import shlex
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
        }

    # In container_service.py - replace the command building section:

    def wrap_command(self, command: str, project_dir: Path, raw_data_dir: Path) -> str:
        """
        Converts a raw command to a direct container execution with proper path handling.
        """
        parts = command.strip().split()
        if not parts:
            return command
            
        tool_name = parts[0]
        container_key = self.tool_to_container.get(tool_name)
        
        if not container_key or container_key not in self.container_paths:
            return command
            
        container_path = self.container_paths[container_key]
        project_dir_abs = str(project_dir.resolve())
        raw_data_dir_abs = str(raw_data_dir.resolve())
        
        # ===== CONTAINER-SPECIFIC BINDING STRATEGY =====
        binds = [
            f"{project_dir_abs}",
            f"{raw_data_dir_abs}:{raw_data_dir_abs}:ro"
        ]
        
        if container_key == "relion":
            binds.extend([
                "/usr/bin:/usr/bin",
                "/usr/lib64/slurm:/usr/lib64/slurm", 
                "/run/munge:/run/munge",
                "/tmp/.X11-unix:/tmp/.X11-unix",
                f"{Path.home()}/.Xauthority:/root/.Xauthority:ro"
            ])
        
        # ===== BUILD CONTAINER COMMAND =====
        # --- THIS IS THE CORRECTED LINE ---
        bind_args = [item for bind in binds for item in ('-B', bind)]
        
        quoted_inner_command = shlex.quote(command)

        container_cmd_parts = [
            "apptainer", "run",
            "--nv", 
            "--cleanenv",
            "--no-home",
            *bind_args,
            container_path,
            "bash", "-c",
            quoted_inner_command
        ]
        
        # Clean environment variables
        clean_env_vars = [
            "SINGULARITY_BIND", "APPTAINER_BIND", 
            "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
            "SINGULARITY_NAME", "APPTAINER_NAME", 
            "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
            "SINGULARITYENV_APPEND_PATH", "APPTAINERENV_APPEND_PATH", 
            "LD_PRELOAD", "SINGULARITY_TMPDIR", "APPTAINER_TMPDIR", 
            "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY"
        ]
        
        clean_env_cmd = "unset " + " ".join(clean_env_vars)
            
            # Essential environment variables
        essential_env = {
            'PATH': '/usr/bin:/bin',
            'HOME': str(Path.home()),
            'USER': 'artem.kushner', # NOTE: You might want to make this dynamic later
            'LANG': 'en_US.UTF-8',
            'LC_ALL': 'en_US.UTF-8',
            'PWD': project_dir_abs, # This tells the container where to start
        }
        
        env_vars = " ".join([f"{k}='{v}'" for k, v in essential_env.items()])
        
        full_command = f"{clean_env_cmd} && env -i {env_vars} {' '.join(container_cmd_parts)}"
        
        print(f"✅ [CONTAINER] Wrapped {tool_name} using {container_key} container")
        print(f"✅ [CONTAINER] Project dir: {project_dir_abs}")
        print(f"✅ [CONTAINER] Raw data dir bound: {raw_data_dir_abs}")
        return full_command

# Singleton instance
_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service