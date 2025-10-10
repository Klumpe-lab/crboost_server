# services/container_service.py
import os
import shlex
from pathlib import Path
from typing import List, Optional

class ContainerService:
    def __init__(self, container_path: Path):
        self.container_path = container_path
        self.slurm_mounts = self._get_slurm_mounts()
    
    def _get_slurm_mounts(self) -> List[str]:
        """Discover and validate SLURM-related paths that need to be mounted"""
        mounts = []
        
        # SLURM binaries
        slurm_bin_paths = [
            "/usr/bin/sbatch", "/usr/bin/squeue", "/usr/bin/scancel",
            "/usr/bin/scontrol", "/usr/bin/sinfo"
        ]
        
        for bin_path in slurm_bin_paths:
            if Path(bin_path).exists():
                mounts.append(f"--bind {bin_path}:{bin_path}")
        
        # SLURM libraries
        slurm_lib_dirs = ["/usr/lib64/slurm", "/usr/lib/slurm"]
        for lib_dir in slurm_lib_dirs:
            if Path(lib_dir).exists() and Path(lib_dir).is_dir():
                mounts.append(f"--bind {lib_dir}:{lib_dir}")
                # Also mount the main libslurm library
                libslurm_path = Path(lib_dir) / "libslurm.so"
                if libslurm_path.exists():
                    mounts.append(f"--bind {libslurm_path}:{libslurm_path}")
        
        # Munge authentication
        munge_paths = [
            "/run/munge", "/usr/bin/munge", "/usr/bin/unmunge",
            "/usr/sbin/munged", "/var/run/munge"
        ]
        
        for munge_path in munge_paths:
            if Path(munge_path).exists():
                mounts.append(f"--bind {munge_path}:{munge_path}")
        
        return list(set(mounts))  # Remove duplicates
    
    def _get_base_binds(self, cwd: Path = None) -> List[str]:
        """Get base directory binds"""
        base_binds = [
            str(cwd or Path.cwd()),
            str(Path.home()),
            "/tmp",
            "/scratch",
        ]
        
        # Add project-specific paths
        projects_dir = Path.cwd() / "projects"
        if projects_dir.exists():
            base_binds.append(str(projects_dir))
        
        # Add config directory
        config_dir = Path.cwd() / "config"
        if config_dir.exists():
            base_binds.append(str(config_dir))
        
        # Add current working directory components for nested paths
        if cwd:
            # Bind parent directories to ensure path resolution works
            current = cwd
            for _ in range(3):  # Bind up to 3 levels up
                current = current.parent
                if current.exists() and str(current) not in base_binds:
                    base_binds.append(str(current))
                if current == Path('/'):
                    break
        
        return list(set(base_binds))
    
    def build_container_command(
        self, 
        command: str, 
        cwd: Path = None, 
        additional_binds: List[str] = None,
        needs_slurm: bool = False
    ) -> str:
        """Build a complete container command with all necessary binds"""
        
        # Get base binds
        base_binds = self._get_base_binds(cwd)
        
        # Combine all binds
        all_binds = base_binds.copy()
        
        # Add SLURM mounts if needed
        if needs_slurm:
            all_binds.extend([bind.split('--bind ')[1] for bind in self.slurm_mounts])
        
        # Add any additional binds
        if additional_binds:
            all_binds.extend(additional_binds)
        
        # Remove duplicates and ensure all paths exist
        unique_binds = []
        for bind_path in set(all_binds):
            path_obj = Path(bind_path.split(':')[0]) if ':' in bind_path else Path(bind_path)
            if path_obj.exists():
                unique_binds.append(bind_path)
            else:
                print(f"[CONTAINER SERVICE] Warning: Path does not exist, skipping bind: {bind_path}")
        
        # Build bind arguments
        bind_args = " ".join([f"--bind {bind}" for bind in unique_binds])
        
        # Clean environment command
        clean_command = f"""
        unset PYTHONPATH
        unset PYTHONHOME
        export PATH="/opt/miniconda3/envs/relion-5.0/bin:/opt/miniconda3/bin:/opt/relion-5.0/build/bin:/usr/local/cuda-11.8/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        {command}
        """
        
        wrapped_command = f"bash -c {shlex.quote(clean_command)}"
        
        full_command = f"apptainer exec {bind_args} {self.container_path} {wrapped_command}"
        
        print(f"[CONTAINER SERVICE] Command: {full_command}")
        return full_command