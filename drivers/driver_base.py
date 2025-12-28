#!/usr/bin/env python
# drivers/driver_base.py
"""
Shared bootstrap logic for all CryoBoost drivers.
Refactored for Single Source of Truth architecture.
"""

import json
import subprocess
import sys
import os
import argparse
from pathlib import Path
from typing import Tuple, Type

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


def get_driver_context() -> Tuple[ProjectState, AbstractJobParams, dict, Path, Path, JobType]:
    """
    Primary bootstrap function for all drivers.
    Identity is derived purely from CLI args and Global State.
    """
    parser = argparse.ArgumentParser(allow_abbrev=False)
    # These args are CRITICAL now. They are the only link to identity.
    parser.add_argument("--job_type", required=True, help="JobType string (e.g., fsMotionAndCtf)")
    parser.add_argument("--project_path", required=True, type=Path, help="Absolute path to project root")

    args, _ = parser.parse_known_args()
    project_path = args.project_path.resolve()
    job_dir = Path.cwd().resolve()

    # 1. Validate Job Type
    try:
        job_type = JobType.from_string(args.job_type)
    except ValueError:
        print(f"FATAL: Unknown job type {args.job_type}", file=sys.stderr)
        sys.exit(1)

    # 2. Load the Single Source of Truth
    try:
        project_state = load_project_state(project_path)
        project_state.project_path = project_path
    except Exception as e:
        print(f"FATAL: Failed to load global project_params.json: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Find Myself in the Global State
    job_model = project_state.jobs.get(job_type)

    if not job_model:
        raise ValueError(f"Job {job_type.value} requested via CLI, but not found in project_params.json.")

    # 4. Extract Paths from the Model
    local_paths = job_model.paths
    if not local_paths:
        print(f"[WARN] Job {job_type.value} has no paths stored in project_params.json!", flush=True)

    # Construct the context dictionary to mimic the old structure
    context_data = {"job_type": job_type.value, "paths": local_paths, "additional_binds": job_model.additional_binds}

    print(
        f"[DRIVER_BASE] Context loaded from Global State for {job_type.value}. Status: {job_model.execution_status}",
        flush=True,
    )

    return (
        project_state,
        job_model,  # The State Object (Params)
        context_data,  # The Context Dict (Paths/Binds)
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