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

    # Re-resolve paths at drive time rather than trusting the schedule-time snapshot
    # in job_model.paths. The orchestrator writes paths before the relion schemer has
    # allocated the real job directory (it works from a predicted job number), so the
    # cached snapshot can disagree with the actual relion_job_name if the schemer
    # skipped a number (e.g. because of an orphan row in default_pipeline.star).
    # Path.cwd() is the authoritative job dir — qsub.sh cd's into the allocated
    # directory before invoking this driver.
    from services.path_resolution_service import PathResolutionService, PathResolutionError, get_context_paths

    try:
        resolver = PathResolutionService(project_state)
        io_paths = resolver.resolve_all_paths(job_type, job_model, job_dir=job_dir, instance_id=instance_id)
        context_paths = get_context_paths(job_type, job_model, job_dir)
        fresh_paths = {**context_paths, **io_paths}
    except PathResolutionError as e:
        print(
            f"FATAL: path resolution failed for instance '{instance_id}' at drive time: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    local_paths = {k: str(v) for k, v in fresh_paths.items() if v is not None}
    # Mirror the resolver output into the model so any code path that still reads
    # job_model.paths in-process (e.g. _get_job_specific_options) sees fresh values.
    # Not persisted to disk — sync_all_jobs owns project_params.json writes.
    job_model.paths = dict(local_paths)

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


def _derive_watchdog_timeout() -> int:
    """
    Seconds of wall-clock budget for run_command's watchdog.

    Preference order:
      1. SLURM_JOB_END_TIME -- absolute epoch seconds when SLURM will kill the
         job. The most authoritative source; no parsing guesswork.
      2. SLURM_JOB_TIME_LIMIT -- minutes. SLURM documents this as integer
         minutes but at least some clusters (e.g. CBE) expose it as a
         [days-]HH:MM:SS string that fails .isdigit(), so parse both forms.
      3. Hard fallback of 8 hours. The watchdog exists to kill orphaned
         container processes holding a SLURM slot after a tool crash; SLURM
         itself will kill the job at --time, so the fallback only needs to
         exceed the longest tool we'd realistically run. 25 min was too short
         (post-loop pytom/Warp finalization alone can take 15+ min).

    In all cases we apply a 90% safety margin so the watchdog fires before
    SLURM's own SIGTERM, letting us emit a clean failure marker.
    """
    import time

    end_time = os.environ.get("SLURM_JOB_END_TIME")
    if end_time and end_time.isdigit():
        remaining = int(end_time) - int(time.time())
        if remaining > 60:
            return int(remaining * 0.9)

    slurm_limit = os.environ.get("SLURM_JOB_TIME_LIMIT", "").strip()
    if slurm_limit and slurm_limit.upper() not in ("UNLIMITED", "INFINITE", ""):
        minutes = None
        if slurm_limit.isdigit():
            minutes = int(slurm_limit)
        else:
            # Accept [DD-]HH:MM:SS or HH:MM:SS or MM:SS
            days, _, hms = slurm_limit.partition("-")
            parts = (hms or days).split(":")
            try:
                nums = [int(p) for p in parts]
                if hms:
                    d = int(days)
                else:
                    d = 0
                if len(nums) == 3:
                    h, m, s = nums
                elif len(nums) == 2:
                    h, m, s = 0, nums[0], nums[1]
                else:
                    h = m = s = 0
                minutes = d * 24 * 60 + h * 60 + m + (1 if s else 0)
            except ValueError:
                minutes = None
        if minutes and minutes > 0:
            return int(minutes * 60 * 0.9)

    return 8 * 60 * 60  # 8h fallback


def run_command(command: str, cwd: Path, timeout: int = None):
    """
    Run a shell command, stream output, and check for errors.

    If timeout is set (seconds), the process tree is killed after that many
    seconds of wall-clock time. When unset, the watchdog budget is derived
    from SLURM walltime env vars (see _derive_watchdog_timeout), falling back
    to 8 hours. The watchdog exists to kill orphaned container processes that
    hold a SLURM slot after a tool crash -- SLURM itself enforces --time, so
    the fallback only needs to exceed the longest legitimate tool runtime.
    """
    import signal
    import threading

    if timeout is None:
        timeout = _derive_watchdog_timeout()

    process = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        start_new_session=True,
    )

    # Watchdog: kill the entire process group if it exceeds the timeout.
    # This handles the case where a tool crashes but orphaned child processes
    # keep the stdout pipe open, blocking the readline loop forever.
    done = threading.Event()

    def _watchdog():
        if done.wait(timeout):
            return  # process finished before timeout
        # Timeout expired — kill the process group
        if process.poll() is None:
            print(f"\n[run_command] TIMEOUT after {timeout}s — killing process group", flush=True)
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                pass

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    print("--- CONTAINER OUTPUT ---", flush=True)
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            print(line, end="", flush=True)

    process.wait()
    done.set()  # signal the watchdog to stop

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command)