#!/usr/bin/env python
# drivers/fs_motion_and_ctf.py
"""
fs_motion_and_ctf driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Reads the import STAR to group frames by tilt-series,
          writes a manifest with the TS→frame mapping, submits a SLURM array
          (one task per TS), polls until completion, then aggregates per-frame
          XML metadata into the output STAR.

- Set:    TASK mode. Creates a staging directory with only this TS's frames
          (symlinked), runs WarpTools create_settings + fs_motion_and_ctf on
          that subset, then copies resulting XMLs and averages into the shared
          warp_frameseries/ output directory.
"""

import os
import shlex
import shutil
import sys
import traceback
from pathlib import Path
from typing import Dict, List

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    collect_task_results,
    install_cancel_handler,
    read_manifest,
    submit_array_job,
    wait_for_array_completion,
    write_status_atomic,
    STATUS_DIR_NAME,
)
from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service
from services.configs.metadata_service import MetadataTranslator
from services.configs.starfile_service import StarfileService
from services.jobs.fs_motion_ctf import FsMotionCtfParams


DRIVER_SCRIPT = Path(__file__).resolve()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def read_ts_frame_mapping(input_star: Path, project_root: Path) -> Dict[str, List[str]]:
    """
    Parse the import STAR to build a mapping: ts_name → [frame_filename, ...].

    The import STAR has a `global` block with one row per TS, each pointing to
    a per-TS star file. The per-TS star file lists frames via rlnMicrographMovieName.

    rlnTomoTiltSeriesStarFile paths are relative to the project root
    (e.g. "Import/job001/tilt_series/xxx.star").
    """
    star_svc = StarfileService()
    star_data = star_svc.read(input_star)
    global_df = star_data.get("global")
    if global_df is None or len(global_df) == 0:
        return {}

    input_star_dir = input_star.parent
    mapping: Dict[str, List[str]] = {}

    for _, row in global_df.iterrows():
        ts_name = str(row["rlnTomoName"])
        ts_star_rel = row["rlnTomoTiltSeriesStarFile"]

        # Try project root first (RELION convention), then input_star_dir as fallback
        ts_star_path = None
        for base in [project_root, input_star_dir]:
            candidate = (base / ts_star_rel).resolve()
            if candidate.exists():
                ts_star_path = candidate
                break

        if ts_star_path is None:
            print(
                f"[WARN] Per-TS star not found: tried {project_root / ts_star_rel} and {input_star_dir / ts_star_rel}",
                flush=True,
            )
            continue

        ts_data = star_svc.read(ts_star_path)
        # The per-TS star has one data block named after the TS
        ts_df = next(iter(ts_data.values()))
        frames = ts_df["rlnMicrographMovieName"].astype(str).tolist()
        # These are relative paths like "frames/xxx.eer" — extract just filenames
        mapping[ts_name] = [Path(f).name for f in frames]

    return mapping


def build_warp_commands(params: FsMotionCtfParams, frames_rel: str) -> str:
    """Build WarpTools create_settings + fs_motion_and_ctf command for a staged frame dir."""
    gain_path_str = ""
    if params.gain_path and params.gain_path != "None":
        gain_path_str = shlex.quote(params.gain_path)
    gain_ops_str = params.gain_operations if params.gain_operations else ""

    # Detect frame extension from the frames_rel dir name context
    # We'll detect at runtime inside the staged dir
    create_settings_parts = [
        "WarpTools create_settings",
        "--folder_data",
        frames_rel,
        "--extension",
        "'*.eer'",
        "--folder_processing",
        "warp_frameseries",
        "--output",
        "warp_frameseries.settings",
        "--angpix",
        str(params.pixel_size),
        "--eer_ngroups",
        str(params.eer_ngroups),
    ]
    if gain_path_str:
        create_settings_parts.extend(["--gain_reference", gain_path_str])
        if gain_ops_str:
            create_settings_parts.extend(["--gain_operations", gain_ops_str])

    run_main_parts = [
        "WarpTools fs_motion_and_ctf",
        "--settings",
        "warp_frameseries.settings",
        "--m_grid",
        params.m_grid,
        "--m_range_min",
        str(params.m_range_min),
        "--m_range_max",
        str(params.m_range_max),
        "--m_bfac",
        str(params.m_bfac),
        "--c_grid",
        params.c_grid,
        "--c_window",
        str(params.c_window),
        "--c_range_min",
        str(params.c_range_min),
        "--c_range_max",
        str(params.c_range_max),
        "--c_defocus_min",
        str(params.defocus_min_microns),
        "--c_defocus_max",
        str(params.defocus_max_microns),
        "--c_voltage",
        str(round(float(params.voltage))),
        "--c_cs",
        str(params.spherical_aberration),
        "--c_amplitude",
        str(params.amplitude_contrast),
        "--perdevice",
        str(params.perdevice),
        "--out_averages",
        "--out_skip_first",
        str(params.out_skip_first),
        "--out_skip_last",
        str(params.out_skip_last),
    ]
    if params.out_average_halves:
        run_main_parts.append("--out_average_halves")
    if params.c_use_sum:
        run_main_parts.append("--c_use_sum")
    if params.do_phase:
        run_main_parts.append("--c_fit_phase")

    create_cmd = " ".join(create_settings_parts)
    run_cmd = " ".join(run_main_parts)
    return f"test -f warp_frameseries.settings || ({create_cmd}) && {run_cmd}"


def detect_frame_extension(frames_dir: Path) -> str:
    """Detect the frame file extension from a directory."""
    for ext in [".eer", ".tiff", ".tif", ".mrc"]:
        if any(frames_dir.glob(f"*{ext}")):
            return f"*{ext}"
    return "*.eer"


def stage_fs_environment(job_dir: Path, ts_name: str, frame_filenames: List[str], project_frames_dir: Path) -> Path:
    """
    Build a per-TS staging directory with only this TS's frames symlinked.

    .staging/task_{ts_name}/
    ├── frames/              ← symlinks to this TS's frame files only
    ├── warp_frameseries.settings  ← created by WarpTools
    └── warp_frameseries/    ← WarpTools output dir
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    stage_root.mkdir(parents=True, exist_ok=True)

    staged_frames = stage_root / "frames"
    staged_frames.mkdir(parents=True, exist_ok=True)

    for fname in frame_filenames:
        src = project_frames_dir / fname
        dst = staged_frames / fname
        if not dst.exists() and not dst.is_symlink():
            if src.exists():
                dst.symlink_to(src.resolve())
            else:
                print(f"  [WARN] Frame not found: {src}", flush=True)

    return stage_root


def collect_fs_outputs(job_dir: Path, ts_name: str) -> None:
    """
    Copy per-TS warp_frameseries outputs into the shared job-level output dir.

    Each frame produces a unique XML and unique average MRCs (named by frame stem),
    so there are no collisions between TS tasks.
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    staged_warp = stage_root / "warp_frameseries"
    shared_warp = job_dir / "warp_frameseries"
    shared_warp.mkdir(parents=True, exist_ok=True)

    if not staged_warp.exists():
        return

    # Copy XMLs (per-frame metadata)
    for xml_file in staged_warp.glob("*.xml"):
        shutil.copy2(str(xml_file), str(shared_warp / xml_file.name))

    # Copy subdirectories (average/, powerspectrum/, etc.)
    for subdir in staged_warp.iterdir():
        if subdir.is_dir():
            shared_subdir = shared_warp / subdir.name
            shared_subdir.mkdir(parents=True, exist_ok=True)
            for f in subdir.iterdir():
                if f.is_file():
                    shutil.copy2(str(f), str(shared_subdir / f.name))
                elif f.is_dir():
                    # Nested subdirs like average/even/, average/odd/
                    shared_nested = shared_subdir / f.name
                    shared_nested.mkdir(parents=True, exist_ok=True)
                    for nf in f.iterdir():
                        if nf.is_file():
                            shutil.copy2(str(nf), str(shared_nested / nf.name))


# ----------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------


def main():
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- fs_motion_and_ctf: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- fs_motion_and_ctf: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(
            FsMotionCtfParams
        )
    except Exception as e:
        fail_dir = Path.cwd()
        (fail_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[SUPERVISOR] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(f"[SUPERVISOR] CWD (job dir): {job_dir}", flush=True)

    try:
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        instance_id = local_params_data["instance_id"]

        input_star_path = paths.get("input_star")
        if not input_star_path or not input_star_path.exists():
            raise FileNotFoundError(f"Required input STAR file not found: {input_star_path}")

        # Parse input STAR to build TS→frames mapping
        ts_frame_map = read_ts_frame_mapping(input_star_path, project_path)
        if not ts_frame_map:
            raise ValueError(f"No tilt-series/frames found in input STAR: {input_star_path}")

        ts_names = sorted(ts_frame_map.keys())
        n_tasks = len(ts_names)
        total_frames = sum(len(frames) for frames in ts_frame_map.values())
        print(f"[SUPERVISOR] Found {n_tasks} tilt-series with {total_frames} total frames", flush=True)
        for ts in ts_names:
            print(f"  {ts}: {len(ts_frame_map[ts])} frames", flush=True)

        # Write the manifest with the frame mapping embedded
        per_task_cfg = params.get_effective_slurm_config()

        array_job_id = submit_array_job(
            job_dir=job_dir,
            project_path=project_path,
            instance_id=instance_id,
            ts_names=ts_names,
            per_task_cfg=per_task_cfg,
            array_throttle=params.array_throttle,
            driver_script=DRIVER_SCRIPT,
            manifest_extra={"ts_frames": ts_frame_map},
        )

        install_cancel_handler(array_job_id, job_dir)
        wait_for_array_completion(array_job_id, poll_secs=30)

        results = collect_task_results(job_dir, ts_names)
        print(f"[SUPERVISOR] Status: {results.summary}", flush=True)
        if results.failed:
            print(f"[SUPERVISOR] FAILED tilt-series: {results.failed}", flush=True)
        if results.missing:
            print(f"[SUPERVISOR] MISSING tilt-series: {results.missing}", flush=True)

        if not results.all_succeeded:
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            print("[SUPERVISOR] Marking job as FAILED (some tilt-series did not succeed)", flush=True)
            sys.exit(1)

        # Aggregate metadata
        print("[SUPERVISOR] All tasks succeeded; aggregating metadata...", flush=True)

        # Ensure output processing dir exists
        output_processing_dir = paths.get("output_processing", job_dir / "warp_frameseries")
        output_processing_dir.mkdir(parents=True, exist_ok=True)

        translator = MetadataTranslator(StarfileService())
        result = translator.update_fs_motion_and_ctf_metadata(
            job_dir=job_dir,
            input_star_path=input_star_path,
            output_star_path=paths["output_star"],
            project_root=project_path,
            warp_folder="warp_frameseries",
        )
        if not result["success"]:
            raise Exception(f"Metadata aggregation failed: {result['error']}")

        print("[SUPERVISOR] Metadata processing successful.", flush=True)

        (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
        print("[SUPERVISOR] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[SUPERVISOR] FATAL ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        sys.exit(1)


# ----------------------------------------------------------------------
# Task mode
# ----------------------------------------------------------------------


def run_task_mode(array_idx: int):
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(
            FsMotionCtfParams
        )
    except Exception as e:
        print(f"[TASK {array_idx}] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    status_dir = job_dir / STATUS_DIR_NAME
    ts_name = None
    try:
        manifest = read_manifest(job_dir)
        ts_names = manifest["ts_names"]
        ts_frames = manifest["ts_frames"]

        if array_idx >= len(ts_names):
            raise IndexError(f"SLURM_ARRAY_TASK_ID {array_idx} out of range (manifest has {len(ts_names)})")
        ts_name = ts_names[array_idx]
        frame_filenames = ts_frames[ts_name]
        print(f"[TASK {array_idx}] ts_name={ts_name}, {len(frame_filenames)} frames", flush=True)

        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]
        project_frames_dir = paths.get("frames_dir", project_path / "frames")

        # Stage per-TS environment with only this TS's frames
        stage_root = stage_fs_environment(job_dir, ts_name, frame_filenames, project_frames_dir)
        print(f"[TASK {array_idx}] Staged at: {stage_root}", flush=True)

        # Detect frame extension from staged frames
        staged_frames_dir = stage_root / "frames"
        ext = detect_frame_extension(staged_frames_dir)

        # Build the WarpTools command — runs inside stage_root with frames in ./frames/
        warp_command = build_warp_commands(params, "frames")

        # Patch the extension in the create_settings command if not .eer
        if ext != "*.eer":
            warp_command = warp_command.replace("'*.eer'", f"'{ext}'")

        print(f"[TASK {array_idx}] Command: {warp_command[:300]}...", flush=True)

        container_svc = get_container_service()
        apptainer_command = container_svc.wrap_command_for_tool(
            command=warp_command, cwd=stage_root, tool_name="warptools", additional_binds=additional_binds
        )

        run_command(apptainer_command, cwd=stage_root)

        # Collect outputs into shared warp_frameseries/ dir
        print(f"[TASK {array_idx}] Collecting outputs...", flush=True)
        collect_fs_outputs(job_dir, ts_name)

        write_status_atomic(status_dir, ts_name, ok=True)
        print(f"[TASK {array_idx}] {ts_name} done", flush=True)
        sys.exit(0)

    except Exception as e:
        label = ts_name or f"_unknown_idx{array_idx}"
        print(f"[TASK {array_idx}] FATAL ERROR for ts={label}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        try:
            write_status_atomic(status_dir, label, ok=False)
        except Exception as inner:
            print(f"[TASK {array_idx}] Could not write fail status: {inner}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
