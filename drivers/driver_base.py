#!/usr/bin/env python
# drivers/driver_base.py
"""
Shared bootstrap logic for all CryoBoost drivers.
Refactored for Single Source of Truth architecture.
"""

import subprocess
import sys
import os
import shlex
import time
import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

# Add server root to path to import services
server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))

try:
    from services.project_state import ProjectState, AbstractJobParams, JobType
    from services.computing.container_service import get_container_service
except ImportError as e:
    print("FATAL: driver_base could not import services. Check PYTHONPATH.", file=sys.stderr)
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


def get_driver_context(expected_type: type[T] | None = None) -> tuple[ProjectState, T, dict, Path, Path, JobType]:
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
        print(f"FATAL: path resolution failed for instance '{instance_id}' at drive time: {e}", file=sys.stderr)
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

    return (project_state, job_model, context_data, job_dir, project_path, job_type)


@dataclass(frozen=True)
class DriverContext(Generic[T]):
    """One driver's bootstrap result, as a single frozen object.

    Replaces `get_driver_context()`'s 6-positional tuple plus bare `context_data`
    dict — an unpack that appeared under three different local names across 17
    sites, each re-doing the same two conversions (`paths` to `Path`, binds to a
    mutable list). Those conversions happen once, here.

    `params` keeps its concrete type: `DriverContext.load(TsReconstructParams)`
    returns a `DriverContext[TsReconstructParams]`.
    """

    state: ProjectState
    params: T
    job_dir: Path
    project_path: Path
    job_type: JobType
    instance_id: str
    paths: dict[str, Path]
    additional_binds: list[str]

    @classmethod
    def load(cls, expected_type: type[T]) -> "DriverContext[T]":
        state, params, data, job_dir, project_path, job_type = get_driver_context(expected_type)
        return cls(
            state=state,
            params=params,
            job_dir=job_dir,
            project_path=project_path,
            job_type=job_type,
            instance_id=data["instance_id"],
            paths={k: Path(v) for k, v in data["paths"].items()},
            additional_binds=list(data["additional_binds"]),
        )


# Seconds of complete silence from a tool before run_command treats it as hung.
# Generous on purpose: WarpTools/pytom go quiet during finalization, and killing a
# working-but-quiet tool is far worse than waiting out a real hang a bit longer.
IDLE_TIMEOUT_DEFAULT = 45 * 60


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


PRINT_CMD_ENV = "CRBOOST_PRINT_CMD"


def print_cmd_only() -> bool:
    """True when the driver should print every tool command instead of running it.

    Snapshot mode for command-parity work: set ``CRBOOST_PRINT_CMD=1`` and a driver
    emits its usual ``[run_command] $ ...`` lines — fully container-wrapped, byte-for
    -byte what would have executed — without launching anything. The gate lives in
    ``run_command`` rather than ``run_tool`` on purpose: every driver funnels through
    it, migrated or not, so a snapshot taken before a command-builder refactor is
    directly diffable against one taken after.

    Read at call time, not import time, so a caller can flip it per command.

    Two caveats when snapshotting: the driver's Python-side work (staging dirs,
    manifest/star writes) still happens, so run it against a scratch copy of a
    project; and a driver that validates a tool's output right after the call will
    abort there, giving a partial — but deterministic, hence still diffable —
    transcript.
    """
    return os.environ.get(PRINT_CMD_ENV, "").strip() not in ("", "0")


def run_command(command: str, cwd: Path, timeout: int | None = None, idle_timeout: int = IDLE_TIMEOUT_DEFAULT):
    """
    Run a shell command, stream output, and check for errors.

    Two independent watchdogs, because "hung" and "slow" are different failures
    and only one of them used to be caught:

      timeout (total wall-clock) -- derived from SLURM walltime at 90% (see
        _derive_watchdog_timeout) when unset. This does NOT save the job: a run
        that trips it was going to exceed --time anyway. What it buys is that WE
        kill it rather than SLURM, so the driver still gets to write a .fail
        marker and log a reason instead of vanishing mid-line.

      idle_timeout (seconds with ZERO output) -- the one that actually targets
        the documented hazard: a tool crashes, an orphaned child keeps the stdout
        pipe open, and the readline loop below blocks forever on a pipe that will
        never EOF. Total-runtime alone is a poor detector for that, because it
        scales with the allocation: under the 14-day g_long QOS a process hung in
        its first minute would sit on a GPU for ~12 days before the 90% mark.
        Inactivity catches it in `idle_timeout` regardless of allocation size.

    Set idle_timeout=0 to disable inactivity checking for a legitimately silent
    tool. It is capped at the total budget, since outliving that is meaningless.
    """
    import signal
    import threading

    if timeout is None:
        timeout = _derive_watchdog_timeout()
    idle_timeout = min(idle_timeout, timeout) if idle_timeout else 0

    # Echo the exact command into the job log: the one reliable record of what
    # was actually executed (container wrap included) — reproducibility + the
    # ground truth for command-builder refactors.
    print(f"[run_command] $ {command}", flush=True)

    if print_cmd_only():
        print(f"[run_command] {PRINT_CMD_ENV} set — printed only, not executed", flush=True)
        return

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

    # Watchdog: kill the entire process group on total-time or inactivity breach.
    done = threading.Event()
    started = time.monotonic()
    last_output = [started]  # list so the reader loop below can rebind it

    def _kill(reason: str):
        if process.poll() is None:
            print(f"\n[run_command] {reason} — killing process group", flush=True)
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                pass

    def _watchdog():
        poll = min(30, idle_timeout) if idle_timeout else timeout
        while not done.wait(poll):
            now = time.monotonic()
            if now - started > timeout:
                return _kill(f"TIMEOUT after {timeout}s")
            idle = now - last_output[0]
            if idle_timeout and idle > idle_timeout:
                return _kill(f"NO OUTPUT for {int(idle)}s (idle limit {idle_timeout}s)")

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    print("--- CONTAINER OUTPUT ---", flush=True)
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            last_output[0] = time.monotonic()
            print(line, end="", flush=True)

    process.wait()
    done.set()  # signal the watchdog to stop

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command)


def run_command_with_retries(
    command: str,
    cwd: Path,
    attempts: int = 3,
    retry_delay: int = 10,
    label: str = "command",
    timeout: int | None = None,
):
    """
    Run `command` via run_command(), retrying on a non-zero exit up to `attempts`
    total tries. Bounded by design -- a small fixed cap and a short fixed delay (no
    exponential backoff) -- so a transient tool/worker crash (e.g. WarpTools' GPU
    worker self-terminating on a missed heartbeat) self-heals without the retries
    idling the SLURM allocation for long. Retries are also implicitly capped by the
    job's SLURM --time, so a genuinely-stuck tool can never loop "for ages".
    Re-raises the final CalledProcessError once all attempts are exhausted.
    """
    import time

    for attempt in range(1, attempts + 1):
        try:
            run_command(command, cwd=cwd, timeout=timeout)
            if attempt > 1:
                print(f"[retry] {label}: succeeded on attempt {attempt}/{attempts}", flush=True)
            return
        except subprocess.CalledProcessError as e:
            if attempt >= attempts:
                print(
                    f"[retry] {label}: failed after {attempts} attempts (exit {e.returncode}); giving up",
                    file=sys.stderr,
                    flush=True,
                )
                raise
            print(
                f"[retry] {label}: attempt {attempt}/{attempts} failed (exit {e.returncode}); "
                f"retrying in {retry_delay}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(retry_delay)


class ToolCommand:
    """Ordered accumulator for one tool invocation, rendered as a flat string.

    Deliberately NOT a quoting engine. Every tool execution ends up as a single
    ``bash -c '<string>'`` argument (the container wrapper quotes the whole command),
    so compound shell survives — and the per-site quoting policy differs across
    drivers and is part of the bytes we must preserve. Hence ``opt_path`` takes a
    mandatory ``quote=`` that transcribes the call site's existing policy; the
    ``quote=False`` sites double as the greppable inventory of unquoted-path
    injection hazards.

    Rendering is ``" ".join(parts)`` in insertion order: no reordering, no dedup, no
    validation, no implicit quoting. Value formatting quirks (``round(...)``,
    ``f"{x}x{y}"``, sign prefixes) stay at the call site — the builder never
    re-formats what it is handed.
    """

    def __init__(self, exe: str):
        # exe may fuse a subcommand: "WarpTools ts_reconstruct".
        self.parts: list[str] = [exe]

    def flag(self, flag: str) -> "ToolCommand":
        """A valueless switch: ``--dont_invert``."""
        self.parts.append(flag)
        return self

    def opt(self, flag: str, value) -> "ToolCommand":
        """A flag with an unquoted scalar value — ``str(value)``, as an f-string would."""
        self.parts.extend([flag, str(value)])
        return self

    def opt_path(self, flag: str, path, *, quote: bool) -> "ToolCommand":
        """A flag with a path value. ``quote=True`` → ``shlex.quote``; ``False`` → verbatim."""
        rendered = str(path)
        self.parts.extend([flag, shlex.quote(rendered) if quote else rendered])
        return self

    def raw(self, fragment: str) -> "ToolCommand":
        """Append a pre-rendered fragment verbatim.

        The escape hatch for shapes that are not flag/value pairs: env prefixes,
        fused glob tokens (``--extension '*.eer'``), multi-value splats (``-g 0 1 2``),
        and user passthrough args.
        """
        self.parts.append(fragment)
        return self

    def render(self) -> str:
        return " ".join(self.parts)

    def __str__(self) -> str:
        return self.render()


def run_tool(
    command: str | ToolCommand,
    *,
    tool_name: str,
    cwd: Path,
    binds: Iterable[str | Path] = (),
    attempts: int = 1,
    label: str = "",
    timeout: int | None = None,
) -> None:
    """Wrap a tool command for its execution mode and run it.

    Consolidates the wrap-and-run tail every driver repeats. ``binds`` are extra
    paths to bind into the container: the wrapper resolves, dedups and sorts them
    itself, so caller-side ordering and dedup idiom cannot affect the emitted bytes.
    ``attempts > 1`` routes through ``run_command_with_retries``.

    Accepts a plain ``str`` as well as a ``ToolCommand`` — compound shell (guard
    chains, ``$()`` capture) is composed by the driver into a string and passed
    through whole.
    """
    rendered = command.render() if isinstance(command, ToolCommand) else command
    wrapped = get_container_service().wrap_command_for_tool(
        command=rendered, cwd=cwd, tool_name=tool_name, additional_binds=[str(b) for b in binds]
    )
    if attempts > 1:
        run_command_with_retries(wrapped, cwd=cwd, attempts=attempts, label=label or tool_name, timeout=timeout)
    else:
        run_command(wrapped, cwd=cwd, timeout=timeout)


def diagnose_stale_producer(path: Path) -> str:
    """Build a diagnostic hint when a resolved upstream input is missing/empty.

    Scans sibling ``External/job*/<name>`` entries for a populated copy of the
    same output and names it. The classic trigger is a STALE JOB-NUMBER MAPPING
    after an aborted+redeployed scheme run: crboost predicts each job's
    External/jobNNN dir at deploy time, and after a scheme aborts partway the
    prediction can drift one behind RELION's actual assignment — so a consumer
    (e.g. alignment) ends up pointed at an empty stub dir (job003) instead of
    the real producer output (job004). Returns "" when nothing useful is found.

    See docs/ORCHESTRATOR_ROADMAP.md.
    """
    try:
        name = path.name
        job_dir = path.parent
        external_dir = job_dir.parent
        if external_dir.name != "External":
            return ""
        found = []
        for sib in sorted(external_dir.glob(f"job*/{name}")):
            try:
                if sib.resolve() == path.resolve():
                    continue
                if sib.is_dir() and any(sib.iterdir()):
                    found.append(sib)
                elif sib.is_file() and sib.stat().st_size > 0:
                    found.append(sib)
            except OSError:
                continue
        if not found:
            return ""
        listed = "\n        ".join(str(p) for p in found)
        if job_dir.name.startswith("pending_"):
            producer = job_dir.name.removeprefix("pending_")
            cause = (
                f"\n  ↳ LIKELY CAUSE: producer '{producer}' was never deployed — a pending_* placeholder"
                f" dir is only materialized when the producer is dispatched. For an interactive job"
                f" (e.g. tilt filter) this means its committed output path was not recorded on the job:"
                f" open its panel, re-commit, and requeue the downstream jobs."
            )
        else:
            cause = (
                f"\n  ↳ LIKELY CAUSE: stale job-number mapping after an aborted+redeployed scheme run —"
                f" the predicted External/jobNNN drifted behind RELION's actual assignment, so this job"
                f" points at an empty stub ({job_dir.name}) instead of the real producer output."
                f"\n     See docs/ORCHESTRATOR_ROADMAP.md (job-number off-by-one)."
            )
        return f"\n  ↳ A populated '{name}' exists elsewhere:\n        {listed}" + cause
    except Exception:
        return ""


def require_producer_input(path: Path, label: str, *, require_nonempty: bool = True) -> None:
    """Validate a resolved upstream input path; raise an actionable error when it
    is missing (or, by default, present-but-empty).

    Unlike a bare ``path.exists()`` check, the raised message runs
    ``diagnose_stale_producer`` so a stale-job-number failure reads as exactly
    that — pointing at the real producer dir — instead of a cryptic
    "not found". Use for upstream inputs resolved from another job's output
    (tomostar dirs, settings files, input star/processing dirs).
    """
    exists = path.exists()
    empty = False
    if exists and require_nonempty:
        try:
            empty = (path.is_dir() and not any(path.iterdir())) or (path.is_file() and path.stat().st_size == 0)
        except OSError:
            empty = False
    if exists and not empty:
        return
    state = "is empty" if (exists and empty) else "not found"
    raise FileNotFoundError(f"{label} {state}: {path}{diagnose_stale_producer(path)}")
