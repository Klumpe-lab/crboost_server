# services/container_service.py

from pathlib import Path
from .config_service import get_config_service

class ContainerService:
    """
    A centralized service to wrap commands for execution inside Apptainer containers.
    """
    def __init__(self):
        config_data = get_config_service().get_config()
        
        self.container_paths = config_data.containers
        if self.container_paths is None:
            print("⚠️ WARNING: 'containers' section not found in conf.yaml. Container wrapping will be disabled.")
            self.container_paths = {}

        self.command_to_container_key = {
            "WarpTools": "warp_aretomo",
            "AreTomo": "warp_aretomo",
            "cryoCARE_predict.py": "cryocare",
            "cryoCARE_train.py": "cryocare",
        }

    def create_wrapper_script(self, project_dir: Path, tool_name: str) -> Path:
        """
        Creates a wrapper script for a containerized tool.
        
        Args:
            project_dir: The project directory
            tool_name: The tool to wrap (e.g., 'WarpTools')
            
        Returns:
            Path to the created wrapper script
        """
        container_key = self.command_to_container_key.get(tool_name)
        if not container_key:
            raise ValueError(f"Unknown tool: {tool_name}")
            
        container_path = self.container_paths.get(container_key)
        if not container_path:
            raise ValueError(f"Container path for '{container_key}' not found in config")
        
        # Create wrappers directory in project
        wrappers_dir = project_dir / ".cryoboost_wrappers"
        wrappers_dir.mkdir(exist_ok=True)
        
        wrapper_path = wrappers_dir / tool_name
        project_dir_abs = str(project_dir.resolve())
        
        # Write the wrapper script
        wrapper_content = f"""#!/bin/bash
        # Auto-generated wrapper for {tool_name}

        # Clean ALL container-related environment variables
        unset LD_PRELOAD
        unset SINGULARITY_BINDPATH
        unset APPTAINER_BINDPATH
        unset SINGULARITY_BIND
        unset APPTAINER_BIND
        unset SINGULARITYENV_APPEND_PATH
        unset APPTAINERENV_APPEND_PATH

        # Configuration
        CONTAINER="{container_path}"
        PROJECT_DIR="{project_dir_abs}"
        DATA_DIR="/users/artem.kushner/dev/001_CopiaTestSet"

        # Execute in container
        apptainer run \\
            --cleanenv \\
            --no-home \\
            -B "$PROJECT_DIR" \\
            -B "$DATA_DIR":"$DATA_DIR":ro \\
            --pwd "$(pwd)" \\
            "$CONTAINER" \\
            {tool_name} "$@"
        """
        
        wrapper_path.write_text(wrapper_content)
        wrapper_path.chmod(0o755)  # Make executable
        
        print(f"✅ Created wrapper script: {wrapper_path}")
        return wrapper_path
    def wrap(self, command_string: str, project_dir: Path = None) -> str:
        """
        Returns a command that uses the wrapper script instead of direct apptainer call.
        
        Args:
            command_string: The raw command, e.g., "WarpTools create_settings ..."
            project_dir: The absolute path to the project directory
            
        Returns:
            Command string that calls the wrapper script
        """
        first_word = command_string.strip().split()[0]
        container_key = self.command_to_container_key.get(first_word)

        if not container_key:
            return command_string
        
        if project_dir is None:
            raise ValueError("project_dir must be provided for containerized commands")
        
        # Create the wrapper script
        wrapper_path = self.create_wrapper_script(project_dir, first_word)
        
        # Replace the tool name in the command with the wrapper path
        wrapped_command = command_string.replace(first_word, str(wrapper_path), 1)
        
        return wrapped_command

# Singleton instance for easy access
container_service_instance = None

def get_container_service() -> ContainerService:
    global container_service_instance
    if container_service_instance is None:
        container_service_instance = ContainerService()
    return container_service_instance