#!/usr/bin/env python3
"""
Reusable supervisor / SLURM-array-task infrastructure for per-tilt-series parallelism.

Any driver that processes tilt series independently can use this module:

  Supervisor side:
    1. Enumerate tilt-series (job-specific logic)
    2. Call write_manifest() + submit_array_job() to dispatch
    3. Call collect_task_results() to check outcomes
    4. Run job-specific metadata aggregation

  Task side:
    1. Call read_manifest() to find this task's tilt-series
    2. Call stage_per_ts_environment() to isolate one TS
    3. Run job-specific WarpTools command
    4. Call write_status_atomic() to report outcome

Extracted from drivers/ts_reconstruct.py to support parallelization of
fs_motion_and_ctf, ts_alignment, ts_ctf, and future per-TS jobs.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from services.computing.slurm_service import SlurmConfig


# ----------------------------------------------------------------------
# Manifest helpers
# ----------------------------------------------------------------------

MANIFEST_FILENAME = ".task_manifest.json"
STATUS_DIR_NAME = ".task_status"


def write_manifest(
    job_dir: Path, ts_names: List[str], *, ts_metadata: Optional[Dict[str, dict]] = None, extra: Optional[dict] = None
) -> Path:
    """
    Write the task manifest that maps array indices → tilt-series names.

    ts_metadata: optional per-TS metadata dict (stage_position, beam_position, etc.)
    extra: arbitrary additional keys merged into the manifest root.
    """
    manifest = {"items": ts_names, "ts_names": ts_names, "item_count": len(ts_names), "item_label": "Tilt Series"}
    if ts_metadata:
        manifest["ts_metadata"] = ts_metadata
    if extra:
        manifest.update(extra)

    path = job_dir / MANIFEST_FILENAME
    path.write_text(json.dumps(manifest, indent=2))
    return path


def read_manifest(job_dir: Path) -> dict:
    """Read the task manifest. Raises FileNotFoundError if missing."""
    path = job_dir / MANIFEST_FILENAME
    return json.loads(path.read_text())


def update_manifest(job_dir: Path, updates: dict) -> None:
    """Merge keys into an existing manifest (e.g. to add array_job_id after submission)."""
    path = job_dir / MANIFEST_FILENAME
    manifest = json.loads(path.read_text())
    manifest.update(updates)
    path.write_text(json.dumps(manifest, indent=2))


# ----------------------------------------------------------------------
# Status file helpers
# ----------------------------------------------------------------------


def write_status_atomic(status_dir: Path, item_name: str, ok: bool) -> None:
    """Atomically write a per-item status file (.ok or .fail)."""
    status_dir.mkdir(parents=True, exist_ok=True)
    suffix = "ok" if ok else "fail"
    target = status_dir / f"{item_name}.{suffix}"
    tmp = status_dir / f".{item_name}.{suffix}.tmp"
    tmp.write_text("")
    os.replace(tmp, target)


def clean_status_dir(job_dir: Path) -> Path:
    """Remove stale .ok/.fail files from a previous run. Returns the status dir."""
    status_dir = job_dir / STATUS_DIR_NAME
    if status_dir.exists():
        for f in status_dir.glob("*.ok"):
            f.unlink()
        for f in status_dir.glob("*.fail"):
            f.unlink()
    status_dir.mkdir(parents=True, exist_ok=True)
    return status_dir


@dataclass
class ArrayResults:
    """Outcome of a completed SLURM array job."""

    ok: List[str]
    failed: List[str]
    missing: List[str]
    all_succeeded: bool

    @property
    def summary(self) -> str:
        return f"{len(self.ok)} ok, {len(self.failed)} fail, {len(self.missing)} missing"


def collect_task_results(job_dir: Path, ts_names: List[str]) -> ArrayResults:
    """Read .task_status/ directory to tally per-TS outcomes."""
    status_dir = job_dir / STATUS_DIR_NAME
    ok_files = sorted(p.stem for p in status_dir.glob("*.ok"))
    fail_files = sorted(p.stem for p in status_dir.glob("*.fail"))
    missing = sorted(set(ts_names) - set(ok_files) - set(fail_files))
    all_ok = len(ok_files) == len(ts_names) and not fail_files and not missing
    return ArrayResults(ok=ok_files, failed=fail_files, missing=missing, all_succeeded=all_ok)


# ----------------------------------------------------------------------
# Tomostar path handling
# ----------------------------------------------------------------------


def copy_tomostar_with_absolute_paths(src: Path, dst: Path, original_dir: Path) -> None:
    """
    Copy a .tomostar file, converting relative _wrpMovieName paths to absolute.

    Tomostar is a RELION STAR file where the first column of the data block is
    _wrpMovieName (a relative path like ../../job002/warp_frameseries/xxx.eer).
    WarpTools resolves these relative to the tomostar file's location, so copying
    the file to a different directory would break them. We resolve each path
    against the ORIGINAL directory and write absolute paths.
    """
    lines = src.read_text().splitlines(keepends=True)
    out_lines = []
    in_data_block = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("data_") or stripped.startswith("loop_") or stripped.startswith("_"):
            if stripped.startswith("data_"):
                in_data_block = True
            out_lines.append(line)
            continue

        # Data rows: first whitespace-delimited token is the MoviePath
        if in_data_block and stripped and not stripped.startswith("#"):
            tokens = stripped.split()
            if tokens:
                movie_path = tokens[0]
                if not Path(movie_path).is_absolute():
                    abs_path = str((original_dir / movie_path).resolve())
                    line = line.replace(movie_path, abs_path, 1)

        out_lines.append(line)

    dst.write_text("".join(out_lines))


# ----------------------------------------------------------------------
# Per-TS staging
# ----------------------------------------------------------------------


def stage_per_ts_environment(
    job_dir: Path, ts_name: str, input_processing: Path, settings_file: Path
) -> Tuple[Path, Path]:
    """
    Build a per-TS staging directory so WarpTools only sees ONE tilt-series.

    WarpTools enumerates items from the settings file's DataFolder (tomostar dir),
    NOT from --input_processing.  So we must stage:

        .staging/task_{ts_name}/
        ├── warp_tiltseries.settings    # copy of original
        ├── tomostar/
        │   └── {ts_name}.tomostar      # copy with absolute movie paths
        └── warp_tiltseries/
            └── {ts_name}.xml           # symlink to input_processing

    The settings file uses relative paths (DataFolder="tomostar",
    ProcessingFolder="warp_tiltseries"), so placing the copy inside the staging
    root makes them resolve to the per-TS dirs we created.

    Returns (staged_settings_file, staged_input_processing).
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    stage_root.mkdir(parents=True, exist_ok=True)

    # 1. Copy the settings file (it's small XML).
    staged_settings = stage_root / settings_file.name
    shutil.copy2(str(settings_file), str(staged_settings))

    # 2. Stage the tomostar — one TS only, with ABSOLUTE MoviePaths.
    original_tomostar_dir = settings_file.parent / "tomostar"
    staged_tomostar_dir = stage_root / "tomostar"
    staged_tomostar_dir.mkdir(parents=True, exist_ok=True)

    src_tomostar = original_tomostar_dir / f"{ts_name}.tomostar"
    if not src_tomostar.exists():
        raise FileNotFoundError(f"Tomostar not found: {src_tomostar}")

    dst_tomostar = staged_tomostar_dir / f"{ts_name}.tomostar"
    copy_tomostar_with_absolute_paths(src_tomostar, dst_tomostar, original_tomostar_dir)

    # 3. Stage the warp_tiltseries dir (input_processing) — one XML only.
    staged_processing = stage_root / "warp_tiltseries"
    staged_processing.mkdir(parents=True, exist_ok=True)

    src_xml = input_processing / f"{ts_name}.xml"
    if not src_xml.exists():
        raise FileNotFoundError(f"Per-TS XML not found in input_processing: {src_xml}")

    dst_xml = staged_processing / f"{ts_name}.xml"
    if dst_xml.exists() or dst_xml.is_symlink():
        dst_xml.unlink()
    dst_xml.symlink_to(src_xml.resolve())

    return staged_settings, staged_processing


# ----------------------------------------------------------------------
# SLURM array job submission
# ----------------------------------------------------------------------


def build_array_sbatch_script(
    template_path: Path,
    job_dir: Path,
    project_path: Path,
    instance_id: str,
    per_task_cfg: SlurmConfig,
    array_spec: str,
    driver_script: Path,
) -> Path:
    """
    Read config/qsub.sh, inject `#SBATCH --array=...`, substitute the standard
    XXXextraNXXX placeholders with PER-TASK SLURM resources, and XXXcommandXXX
    with the driver re-invocation. The driver detects task mode via
    SLURM_ARRAY_TASK_ID set by SLURM in the array env.
    """
    template = template_path.read_text()

    # Inject the array directive BEFORE substitution so we can match a stable anchor.
    array_line = f"#SBATCH --array={array_spec}\n"
    template = template.replace("#SBATCH --output=XXXoutfileXXX", f"{array_line}#SBATCH --output=XXXoutfileXXX")

    constraint = per_task_cfg.constraint.strip("'\"")

    python_exe = server_dir / "venv" / "bin" / "python3"
    if not python_exe.exists():
        python_exe = Path("python3")

    driver_cmd = (
        f"export PYTHONPATH={server_dir}:${{PYTHONPATH}}; "
        f"{python_exe} {driver_script} "
        f"--instance_id {instance_id} "
        f"--project_path {project_path}"
    )

    array_outfile = job_dir / "task_%a.out"
    array_errfile = job_dir / "task_%a.err"

    replacements = {
        "XXXextra1XXX": per_task_cfg.partition,
        "XXXextra2XXX": constraint,
        "XXXextra3XXX": str(per_task_cfg.nodes),
        "XXXextra4XXX": str(per_task_cfg.ntasks_per_node),
        "XXXextra5XXX": str(per_task_cfg.cpus_per_task),
        "XXXextra6XXX": per_task_cfg.gres,
        "XXXextra7XXX": per_task_cfg.mem,
        "XXXextra8XXX": per_task_cfg.time,
        "XXXoutfileXXX": str(array_outfile),
        "XXXerrfileXXX": str(array_errfile),
        "XXXcommandXXX": driver_cmd,
    }
    script = template
    for placeholder, value in replacements.items():
        script = script.replace(placeholder, value)

    # Strip the RELION marker-writing block from the array script.
    # Only the SUPERVISOR should write RELION_JOB_EXIT_{SUCCESS,FAILURE} —
    # if a child task writes the marker first, relion_schemer sees it and
    # thinks the entire job is done, halting the pipeline while other tasks
    # are still queued.
    script = script.replace(
        "if [ $EXIT_CODE -eq 0 ]; then\n"
        '    echo "Creating RELION_JOB_EXIT_SUCCESS"\n'
        '    touch "./RELION_JOB_EXIT_SUCCESS"\n'
        "else\n"
        '    echo "Creating RELION_JOB_EXIT_FAILURE"\n'
        '    touch "./RELION_JOB_EXIT_FAILURE"\n'
        "fi",
        "# [array task] RELION markers suppressed — supervisor writes them after all tasks finish",
    )

    out_path = job_dir / "run_array.sh"
    out_path.write_text(script)
    out_path.chmod(0o755)
    return out_path


def submit_array_sbatch(script_path: Path, cwd: Path) -> str:
    """
    sbatch the script in a clean environment stripped of inherited SLURM_/SBATCH_
    vars so the child job doesn't inherit the supervisor's job context.
    Returns the SLURM job ID.
    """
    clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}
    proc = subprocess.run(["sbatch", str(script_path)], cwd=str(cwd), env=clean_env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"sbatch failed: rc={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}")
    out = proc.stdout.strip()
    print(f"[ARRAY] sbatch output: {out}", flush=True)
    # "Submitted batch job 12345"
    return out.split()[-1]


def wait_for_array_completion(array_job_id: str, poll_secs: int = 30) -> None:
    """
    Poll squeue until no array tasks remain.  squeue exits non-zero (or returns
    empty) once the array is no longer in the queue.
    """
    print(f"[ARRAY] Polling squeue for array job {array_job_id}...", flush=True)
    while True:
        proc = subprocess.run(["squeue", "-j", str(array_job_id), "--noheader", "-h"], capture_output=True, text=True)
        if proc.returncode != 0 or not proc.stdout.strip():
            print(f"[ARRAY] Array job {array_job_id} no longer in queue", flush=True)
            return
        n_remaining = sum(1 for line in proc.stdout.splitlines() if line.strip())
        print(f"[ARRAY] {n_remaining} task(s) still in queue", flush=True)
        time.sleep(poll_secs)


# ----------------------------------------------------------------------
# High-level supervisor lifecycle
# ----------------------------------------------------------------------


def submit_array_job(
    job_dir: Path,
    project_path: Path,
    instance_id: str,
    ts_names: List[str],
    per_task_cfg: SlurmConfig,
    array_throttle: int,
    driver_script: Path,
    *,
    ts_metadata: Optional[Dict[str, dict]] = None,
    manifest_extra: Optional[dict] = None,
) -> str:
    """
    Complete supervisor dispatch: write manifest, clean status dir, build + submit array.

    Returns the SLURM array job ID.
    """
    n_tasks = len(ts_names)

    # 1. Write manifest
    write_manifest(job_dir, ts_names, ts_metadata=ts_metadata, extra=manifest_extra)

    # 2. Clean stale status from previous run
    clean_status_dir(job_dir)

    # 3. Compute array spec with throttle
    throttle = max(1, min(array_throttle, n_tasks))
    array_spec = f"0-{n_tasks - 1}%{throttle}"
    print(
        f"[SUPERVISOR] Per-task SLURM: mem={per_task_cfg.mem} time={per_task_cfg.time} "
        f"gres={per_task_cfg.gres} cpus={per_task_cfg.cpus_per_task} | --array={array_spec}",
        flush=True,
    )

    # 4. Build and submit the array sbatch
    run_array_path = build_array_sbatch_script(
        template_path=server_dir / "config" / "qsub.sh",
        job_dir=job_dir,
        project_path=project_path,
        instance_id=instance_id,
        per_task_cfg=per_task_cfg,
        array_spec=array_spec,
        driver_script=driver_script,
    )

    print(f"[SUPERVISOR] Submitting array sbatch: {run_array_path}", flush=True)
    array_job_id = submit_array_sbatch(run_array_path, cwd=job_dir)
    print(f"[SUPERVISOR] Array job id: {array_job_id}", flush=True)

    # 5. Store the array job ID in the manifest for UI cross-reference
    update_manifest(job_dir, {"array_job_id": array_job_id})

    return array_job_id


def install_cancel_handler(array_job_id: str, job_dir: Path) -> None:
    """Install SIGTERM/SIGINT handlers that scancel the array job on supervisor kill."""

    def _cancel(signum, frame):
        print(f"[SUPERVISOR] Caught signal {signum}; scancelling array job {array_job_id}", flush=True)
        try:
            subprocess.run(["scancel", str(array_job_id)], check=False)
        finally:
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            sys.exit(130)

    signal.signal(signal.SIGTERM, _cancel)
    signal.signal(signal.SIGINT, _cancel)
