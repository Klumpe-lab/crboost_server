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
            tool_name = command.split()[0]
            container_key = self.tool_to_container.get(tool_name)
            
            if not container_key or container_key not in self.container_paths:
                return command

            container_path = self.container_paths[container_key]
            project_dir_abs = str(project_dir.resolve())
            raw_data_dir_abs = str(raw_data_dir.resolve())

            # --- SMART MOUNT LOGIC ---
            # 1. Start with essential, universal binds
            binds = [
                f"{project_dir_abs}",
                f"{raw_data_dir_abs}:{raw_data_dir_abs}:ro"
            ]
            
            # 2. Conditionally add mounts ONLY for GUI/HPC-aware tools like Relion
            if container_key == "relion":
                binds.extend(["/usr/bin:/usr/bin", "/usr/lib64/slurm:/usr/lib64/slurm", "/run/munge:/run/munge"])
                
                # CRITICAL: Check for X11 socket before trying to mount it to prevent crash
                if Path("/tmp/.X11-unix").exists():
                    binds.append("/tmp/.X11-unix")
                
                # CRITICAL: Check for user's .Xauthority before trying to mount it
                x_authority = Path.home() / ".Xauthority"
                if x_authority.exists():
                    binds.append(f"{x_authority}:{x_authority}:ro")

            bind_args = [item for bind in binds for item in ('-B', bind)]
            inner_command_quoted = shlex.quote(command)

            # --- BUILD THE APPTAINER COMMAND ---
            apptainer_command_parts = [
                "apptainer", "run",
                "--nv",
                "--cleanenv", # The correct way to get a clean environment inside the container
                *bind_args,
                container_path,
                "bash", "-c",
                inner_command_quoted
            ]
            apptainer_command = " ".join(apptainer_command_parts)
            
            # --- BRING BACK THE UNSETS (as requested) ---
            # This cleans the HOST environment before apptainer is even called.
            clean_env_vars = [
                "SINGULARITY_BIND", "APPTAINER_BIND", 
                "SINGULARITY_BINDPATH", "APPTAINER_BINDPATH",
                "SINGULARITY_NAME", "APPTAINER_NAME", 
                "SINGULARITY_CONTAINER", "APPTAINER_CONTAINER",
                "LD_PRELOAD", "XDG_RUNTIME_DIR", "DISPLAY", "XAUTHORITY"
            ]
            clean_env_cmd = "unset " + " ".join(clean_env_vars)

            # --- COMBINE FOR THE FINAL COMMAND ---
            final_command = f"{clean_env_cmd} && {apptainer_command}"
            
            print(f"✅ [CONTAINER] Generated command for '{tool_name}': {final_command}")
            return final_command

# Singleton instance
_container_service = None

def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service