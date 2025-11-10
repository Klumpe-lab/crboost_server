#!/usr/bin/env python
# drivers/driver_base.py
"""
Shared bootstrap logic for all CryoBoost drivers.
This module is responsible for loading the global project state and the
job-specific model, then attaching them to create a valid, state-aware
job parameter object.
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
    from services.project_state import (
        ProjectState,
        AbstractJobParams,
        jobtype_paramclass,
        JobType,
    )
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

    # Use the static loader from your ProjectState model
    # This correctly loads globals and initializes/attaches job models
    print(f"[DRIVER_BASE] Loading global project state from {params_file}", flush=True)
    return ProjectState.load(params_file)


def get_driver_context() -> Tuple[ProjectState, AbstractJobParams, dict, Path, Path, JobType]:
    """
    Primary bootstrap function for all drivers.

    This function is the new "single source of truth" for driver initialization.
    It:
    1. Parses CLI args (--project_path, --job_type).
    2. Loads the *entire* global ProjectState from `project_params.json`.
    3. Finds the *correct* job model (e.g., FsMotionCtfParams) inside the loaded
       ProjectState using the --job_type arg.
    4. Loads the *job-specific* `job_params.json` (for paths, binds).
    5. Returns the state-aware job model and other context.

    Returns:
     - project_state: The fully loaded global ProjectState.
     - job_model: The state-aware, instantiated job-specific model (e.g., FsMotionCtfParams)
                  that is already attached to the project_state.
     - local_params_data: The raw dict from the *job-specific* job_params.json
                          (contains 'paths', 'additional_binds', etc.).
     - job_dir: Path to the current job directory (CWD).
     - project_path: Path to the project root.
     - job_type: The JobType enum for the current job.
    """
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--job_type", required=True, help="JobType string")
    parser.add_argument("--project_path", required=True, type=Path, help="Project root")

    args, _ = parser.parse_known_args()

    project_path = args.project_path.resolve()
    job_dir = Path.cwd().resolve()
    job_type = JobType.from_string(args.job_type)
    local_params_file = job_dir / "job_params.json"

    # 1. Load the global state
    # This loads Microscope, Acquisition, *and* all job models from the JSON
    project_state = load_project_state(project_path)
    project_state.project_path = project_path  # Ensure path is set

    # 2. Get the correct, state-aware job model from the loaded state
    job_model = project_state.jobs.get(job_type)
    if not job_model:
        # This should not happen if the orchestrator is working correctly
        raise ValueError(
            f"Job type '{job_type.value}' not found in loaded ProjectState. "
            f"Available jobs: {list(project_state.jobs.keys())}"
        )

    # 3. Load the local job_params.json for paths and binds
    if not local_params_file.exists():
        # The orchestrator should *always* create this.
        # The old param_generator.py fallback was a bug.
        raise FileNotFoundError(f"Job-specific job_params.json not found at {local_params_file}")

    with open(local_params_file, "r") as f:
        local_params_data = json.load(f)

    # 4. Verify the local params match the job model from the global state
    local_job_type = local_params_data.get("job_type")
    if local_job_type != job_type.value:
        raise ValueError(
            f"Job type mismatch! CLI/Global state says '{job_type.value}' "
            f"but local {local_params_file} says '{local_job_type}'"
        )
        
    print(f"[DRIVER_BASE] Successfully loaded context for {job_type.value}", flush=True)

    return (
        project_state,
        job_model,  # This is the state-aware model
        local_params_data,  # This contains 'paths' and 'additional_binds'
        job_dir,
        project_path,
        job_type,
    )


def run_command(command: str, cwd: Path):
    """
    Helper to run a shell command, stream output, and check for errors.
    """
    # print(f"[DRIVER_BASE] Executing: {command}", flush=True) # Verbose
    process = subprocess.Popen(command, shell=True, cwd=cwd, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("--- CONTAINER STDOUT ---", flush=True)
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            print(line, end="", flush=True)

    print("--- CONTAINER STDERR ---", file=sys.stderr, flush=True)
    stderr_output = ""
    if process.stderr:
        for line in iter(process.stderr.readline, ""):
            print(line, end="", file=sys.stderr, flush=True)
            stderr_output += line

    process.wait()

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command, None, stderr_output)