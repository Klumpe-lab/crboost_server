#!/usr/bin/env python
# drivers/ts_alignment.py
"""
ts_alignment driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Enumerates tomostar files from the tsImport job,
          writes task manifest, submits a SLURM array with one task per TS,
          polls until completion, then aggregates alignment metadata.

- Set:    TASK mode. Stages a per-TS environment (single tomostar + settings),
          runs ts_aretomo or ts_etomo_patches for one tilt-series.
"""

import os
import shutil
import sys
import traceback
from pathlib import Path
from typing import List

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    collect_task_results,
    copy_tomostar_with_absolute_paths,
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
from services.jobs.ts_alignment import TsAlignmentParams
from services.models_base import AlignmentMethod


DRIVER_SCRIPT = Path(__file__).resolve()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def enumerate_tomostar_names(tomostar_dir: Path) -> List[str]:
    """Get sorted TS names from the tomostar directory."""
    files = sorted(tomostar_dir.glob("*.tomostar"))
    return [f.stem for f in files]


def stage_alignment_environment(job_dir: Path, ts_name: str, source_tomostar_dir: Path, source_settings: Path) -> Path:
    """
    Build a per-TS staging directory for alignment.

    .staging/task_{ts_name}/
    ├── warp_tiltseries.settings    # copy of original
    ├── tomostar/
    │   └── {ts_name}.tomostar      # copy with absolute movie paths
    └── warp_tiltseries/             # empty — alignment writes here

    Returns the staging root directory.
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    stage_root.mkdir(parents=True, exist_ok=True)

    # 1. Copy settings file
    staged_settings = stage_root / source_settings.name
    shutil.copy2(str(source_settings), str(staged_settings))

    # 2. Stage the tomostar with absolute movie paths
    staged_tomostar_dir = stage_root / "tomostar"
    staged_tomostar_dir.mkdir(parents=True, exist_ok=True)

    src_tomostar = source_tomostar_dir / f"{ts_name}.tomostar"
    if not src_tomostar.exists():
        raise FileNotFoundError(f"Tomostar not found: {src_tomostar}")

    dst_tomostar = staged_tomostar_dir / f"{ts_name}.tomostar"
    copy_tomostar_with_absolute_paths(src_tomostar, dst_tomostar, source_tomostar_dir)

    # 3. Create empty warp_tiltseries dir for output
    (stage_root / "warp_tiltseries").mkdir(parents=True, exist_ok=True)

    return stage_root


def build_alignment_command(params: TsAlignmentParams, stage_root: Path) -> str:
    """Build the alignment command to run inside the staged environment."""
    if params.alignment_method == AlignmentMethod.ARETOMO:
        cmd_parts = [
            "WarpTools ts_aretomo",
            "--settings",
            "warp_tiltseries.settings",
            "--output_processing",
            "warp_tiltseries",
            "--angpix",
            str(params.rescale_angpixs),
            "--alignz",
            str(int(params.sample_thickness_nm * 10)),
            "--perdevice",
            str(params.perdevice),
        ]
        if params.patch_x > 0 and params.patch_y > 0:
            cmd_parts.extend(["--patches", f"{params.patch_x}x{params.patch_y}"])
        if params.axis_iter > 0:
            cmd_parts.extend(["--axis_iter", str(params.axis_iter)])
            cmd_parts.extend(["--axis_batch", str(min(params.axis_batch, 1))])

    elif params.alignment_method == AlignmentMethod.IMOD:
        cmd_parts = [
            "WarpTools ts_etomo_patches",
            "--settings",
            "warp_tiltseries.settings",
            "--output_processing",
            "warp_tiltseries",
            "--angpix",
            str(params.rescale_angpixs),
            "--patch_size",
            str(int(params.imod_patch_size * 10)),
        ]
    else:
        return f"echo 'ERROR: Alignment method {params.alignment_method} not implemented'; exit 1;"

    return " ".join(cmd_parts)


def collect_per_ts_outputs(job_dir: Path, ts_name: str) -> None:
    """
    Copy alignment outputs from the per-TS staging dir into the shared job dir.
    - warp_tiltseries/{ts_name}.xml → job_dir/warp_tiltseries/{ts_name}.xml
    - warp_tiltseries/tiltstack/{ts_name}/ → job_dir/warp_tiltseries/tiltstack/{ts_name}/
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    staged_warp = stage_root / "warp_tiltseries"

    shared_warp = job_dir / "warp_tiltseries"
    shared_warp.mkdir(parents=True, exist_ok=True)

    # Copy XML
    src_xml = staged_warp / f"{ts_name}.xml"
    if src_xml.exists():
        shutil.copy2(str(src_xml), str(shared_warp / f"{ts_name}.xml"))

    # Copy tiltstack directory
    src_tiltstack = staged_warp / "tiltstack" / ts_name
    if src_tiltstack.exists():
        dst_tiltstack = shared_warp / "tiltstack" / ts_name
        if dst_tiltstack.exists():
            shutil.rmtree(str(dst_tiltstack))
        shutil.copytree(str(src_tiltstack), str(dst_tiltstack))


# ----------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------


def main():
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- ts_alignment: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- ts_alignment: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(
            TsAlignmentParams
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

        tomostar_dir = paths["tomostar_dir"]
        if not tomostar_dir.exists():
            raise FileNotFoundError(f"Tomostar directory not found: {tomostar_dir}")

        settings_file = paths["warp_tiltseries_settings"]
        if not settings_file.exists():
            raise FileNotFoundError(f"Settings file not found: {settings_file}")

        # Copy the tomostar dir and settings into the job dir so staged environments
        # can reference them with stable paths
        local_tomostar_dir = job_dir / "tomostar"
        if not local_tomostar_dir.exists():
            shutil.copytree(str(tomostar_dir), str(local_tomostar_dir))
        local_settings = job_dir / settings_file.name
        if not local_settings.exists():
            shutil.copy2(str(settings_file), str(local_settings))

        ts_names = enumerate_tomostar_names(local_tomostar_dir)
        if not ts_names:
            raise ValueError(f"No tomostar files found in {local_tomostar_dir}")

        n_tasks = len(ts_names)
        print(f"[SUPERVISOR] Found {n_tasks} tilt-series to align", flush=True)

        per_task_cfg = params.get_effective_slurm_config()

        array_job_id = submit_array_job(
            job_dir=job_dir,
            project_path=project_path,
            instance_id=instance_id,
            ts_names=ts_names,
            per_task_cfg=per_task_cfg,
            array_throttle=params.array_throttle,
            driver_script=DRIVER_SCRIPT,
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

        # We need the input star for metadata translation — get it from fsMotionAndCtf
        # via the project state's path resolution. The alignment metadata service needs
        # the upstream STAR to build the output STAR.
        input_star_path = paths.get("input_star")
        output_star_path = paths.get("output_star", job_dir / "aligned_tilt_series.star")

        if input_star_path and Path(input_star_path).exists():
            translator = MetadataTranslator(StarfileService())
            result = translator.update_ts_alignment_metadata(
                job_dir=job_dir,
                input_star_path=Path(input_star_path),
                output_star_path=Path(output_star_path),
                project_root=project_path,
                tomo_dimensions=params.tomo_dimensions if hasattr(params, "tomo_dimensions") else "4096x4096x2048",
                alignment_method=params.alignment_method.value,
                alignment_angpix=params.rescale_angpixs,
            )
            if not result["success"]:
                raise Exception(f"Metadata aggregation failed: {result['error']}")
            print("[SUPERVISOR] Metadata aggregation successful.", flush=True)
        else:
            print("[SUPERVISOR] WARN: No input_star path available; skipping metadata aggregation.", flush=True)

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
            TsAlignmentParams
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
        if array_idx >= len(ts_names):
            raise IndexError(f"SLURM_ARRAY_TASK_ID {array_idx} out of range (manifest has {len(ts_names)})")
        ts_name = ts_names[array_idx]
        print(f"[TASK {array_idx}] ts_name={ts_name}", flush=True)

        additional_binds = local_params_data["additional_binds"]

        local_tomostar_dir = job_dir / "tomostar"
        local_settings = job_dir / "warp_tiltseries.settings"

        # Idempotency: skip if this TS's XML already exists in the shared output
        shared_xml = job_dir / "warp_tiltseries" / f"{ts_name}.xml"
        if shared_xml.exists():
            print(f"[TASK {array_idx}] Alignment output already exists, skipping: {shared_xml}", flush=True)
            write_status_atomic(status_dir, ts_name, ok=True)
            sys.exit(0)

        # Stage per-TS environment
        stage_root = stage_alignment_environment(job_dir, ts_name, local_tomostar_dir, local_settings)
        print(f"[TASK {array_idx}] Staged at: {stage_root}", flush=True)

        # Build and run alignment command
        cmd = build_alignment_command(params, stage_root)
        print(f"[TASK {array_idx}] Command: {cmd}", flush=True)

        wrapped = get_container_service().wrap_command_for_tool(
            command=cmd, cwd=stage_root, tool_name=params.get_tool_name(), additional_binds=additional_binds
        )

        run_command(wrapped, cwd=stage_root)

        # Collect outputs into shared job dir
        collect_per_ts_outputs(job_dir, ts_name)

        # Verify XML was produced
        if not shared_xml.exists():
            raise FileNotFoundError(f"Alignment produced no XML for {ts_name} (expected {shared_xml})")

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
