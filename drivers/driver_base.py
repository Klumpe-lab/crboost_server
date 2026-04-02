#!/usr/bin/env python
# drivers/driver_base.py
"""
Shared bootstrap logic for all CryoBoost drivers.
Refactored for Single Source of Truth architecture.
"""

import subprocess
import sys
import os
import argparse
from pathlib import Path
from typing import Tuple, Type, TypeVar

# Add server root to path to import services
server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))

try:
    from services.project_state import ProjectState, AbstractJobParams, JobType
except ImportError as e:
    print(f"FATAL: driver_base could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def load_project_state(project_path: Path) -> ProjectState:
    """
    Loads the main project_params.json file using the ProjectState.load
    static method. This is the single source of truth for global state.
    """
    params_file = project_path / "project_params.json"
    if not params_file.exists():
        raise FileNotFoundError(f"Global project_params.json not found at {params_file}")

    print(f"[DRIVER_BASE] Loading global project state from {params_file}", flush=True)
    return ProjectState.load(params_file)


T = TypeVar("T", bound=AbstractJobParams)


def get_driver_context(expected_type: Type[T] = None) -> Tuple[ProjectState, T, dict, Path, Path, JobType]:
    """
    Primary bootstrap function for all drivers.
    Identity is now derived from --instance_id rather than --job_type,
    which supports multiple instances of the same job type per project.

    Pass the expected param class to get full type safety in the driver:
        state, params, ctx, job_dir, proj, jt = get_driver_context(FsMotionCtfParams)
    """
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "--instance_id",
        required=True,
        help="Instance ID string (e.g., 'tsReconstruct' or 'templatematching__ribosome')",
    )
    parser.add_argument("--project_path", required=True, type=Path, help="Absolute path to project root")

    args, _ = parser.parse_known_args()
    project_path = args.project_path.resolve()
    job_dir = Path.cwd().resolve()
    instance_id = args.instance_id

    # Load the single source of truth
    try:
        project_state = load_project_state(project_path)
        project_state.project_path = project_path
    except Exception as e:
        print(f"FATAL: Failed to load global project_params.json: {e}", file=sys.stderr)
        sys.exit(1)

    # Look up the job model by instance_id
    job_model = project_state.jobs.get(instance_id)
    if not job_model:
        print(
            f"FATAL: Instance '{instance_id}' not found in project_params.json. "
            f"Available: {list(project_state.jobs.keys())}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Runtime type check when a specific class is requested
    if expected_type is not None and not isinstance(job_model, expected_type):
        print(
            f"FATAL: Type mismatch for instance '{instance_id}': "
            f"expected {expected_type.__name__}, got {type(job_model).__name__}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Derive job_type from the model
    job_type = job_model.job_type
    if job_type is None:
        print(f"FATAL: job_model for instance '{instance_id}' has no job_type set.", file=sys.stderr)
        sys.exit(1)

    local_paths = job_model.paths
    if not local_paths:
        print(f"[WARN] Job instance '{instance_id}' has no paths stored in project_params.json!", flush=True)

    context_data = {
        "instance_id": instance_id,
        "job_type": job_type.value,
        "paths": local_paths,
        "additional_binds": job_model.additional_binds,
    }

    print(
        f"[DRIVER_BASE] Context loaded for instance '{instance_id}' "
        f"(type={job_type.value}, status={job_model.execution_status})",
        flush=True,
    )

    return (
        project_state,
        job_model,
        context_data,
        job_dir,
        project_path,
        job_type,
    )


def run_command(command: str, cwd: Path):
    """
    Helper to run a shell command, stream output, and check for errors.
    """
    process = subprocess.Popen(
        command, 
        shell=True, 
        cwd=cwd, 
        text=True, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,  # Merge stderr into stdout
        bufsize=1
    )
    
    print("--- CONTAINER OUTPUT ---", flush=True)
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            print(line, end="", flush=True)

    process.wait()

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command)