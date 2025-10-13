# services/container_service.py

from pathlib import Path
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

    def wrap_command(self, command: str, project_dir: Path) -> str:
        """
        Convert a raw command to a direct container execution with proper path handling
        Returns a single-line command for STAR file compatibility
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
        
        # ===== CONTAINER-SPECIFIC BINDING STRATEGY =====
        # Common binds for all containers - mount the SPECIFIC project directory
        binds = [
            f"{project_dir_abs}",
            "/users/artem.kushner/dev/001_CopiaTestSet:/users/artem.kushner/dev/001_CopiaTestSet:ro"
        ]
        
        # Container-specific additional binds
        if container_key == "relion":
            # Relion needs slurm access to submit jobs
            binds.extend([
                "/usr/bin:/usr/bin",
                "/usr/lib64/slurm:/usr/lib64/slurm", 
                "/run/munge:/run/munge",
                "/tmp/.X11-unix:/tmp/.X11-unix",
                f"{Path.home()}/.Xauthority:/root/.Xauthority:ro"
            ])
        
        # ===== BUILD CONTAINER COMMAND =====
        bind_args = []
        for bind in binds:
            bind_args.extend(["-B", bind])
        
        # Build a single-line command for STAR file compatibility
        container_cmd = [
            "apptainer", "run",
            "--cleanenv",
            "--no-home",
            *bind_args,
            container_path,
            "bash", "-c",
            f"'{command}'"  # Use single quotes for the inner command
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
            'USER': 'artem.kushner',
            'LANG': 'en_US.UTF-8',
            'LC_ALL': 'en_US.UTF-8',
            'PWD': project_dir_abs,
        }
        
        env_vars = " ".join([f"{k}='{v}'" for k, v in essential_env.items()])
        
        # Build final single-line command
        full_command = f"{clean_env_cmd} && env -i {env_vars} {' '.join(container_cmd)}"
        
        print(f"✅ [CONTAINER] Wrapped {tool_name} using {container_key} container")
        print(f"✅ [CONTAINER] Project dir: {project_dir_abs}")
        return full_command

# Singleton instance
_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service