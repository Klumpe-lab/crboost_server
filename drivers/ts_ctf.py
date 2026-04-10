#!/usr/bin/env python3
# drivers/ts_ctf.py
"""
ts_ctf driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Copies alignment XMLs into the output dir, runs
          ts_defocus_hand globally (needs all TS for handedness decision),
          then dispatches per-TS ts_ctf tasks via SLURM array.

- Set:    TASK mode. Stages a per-TS environment, runs ts_ctf for one TS,
          copies result XML back to the shared output dir.
"""

import os
import shlex
import shutil
import sys
import traceback
from pathlib import Path

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
from services.job_models import TsCtfParams


DRIVER_SCRIPT = Path(__file__).resolve()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def run_defocus_hand_globally(
    params: TsCtfParams, settings_file: Path, output_processing: Path, job_dir: Path, additional_binds: list
) -> None:
    """
    Run ts_defocus_hand on ALL tilt-series at once. This is a global step because
    the handedness decision needs statistics across multiple TS.
    """
    settings_str = shlex.quote(str(settings_file))
    output_str = shlex.quote(str(output_processing))

    check_cmd = f"WarpTools ts_defocus_hand --settings {settings_str} --output_processing {output_str} --check"

    set_flip_cmd = f"WarpTools ts_defocus_hand --settings {settings_str} --output_processing {output_str} --set_flip"
    set_noflip_cmd = (
        f"WarpTools ts_defocus_hand --settings {settings_str} --output_processing {output_str} --set_noflip"
    )

    if params.defocus_hand == "auto":
        hand_cmd = (
            f"hand_output=$({check_cmd} 2>&1); "
            f'echo "$hand_output"; '
            f'if echo "$hand_output" | grep -q "should be set to \'flip\'"; then '
            f"  {set_flip_cmd}; "
            f"else "
            f"  {set_noflip_cmd}; "
            f"fi"
        )
    elif params.defocus_hand == "set_flip":
        hand_cmd = " && ".join([check_cmd, set_flip_cmd])
    else:
        hand_cmd = " && ".join([check_cmd, set_noflip_cmd])

    container_svc = get_container_service()
    wrapped = container_svc.wrap_command_for_tool(
        command=hand_cmd, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
    )
    run_command(wrapped, cwd=job_dir)


def stage_ctf_environment(
    job_dir: Path, ts_name: str, output_processing: Path, settings_file: Path, tomostar_dir: Path
) -> Path:
    """
    Build a per-TS staging directory for CTF estimation.

    .staging/task_{ts_name}/
    ├── warp_tiltseries.settings
    ├── tomostar/
    │   └── {ts_name}.tomostar
    └── warp_tiltseries/
        └── {ts_name}.xml       # symlink to the defocus-hand-updated XML
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    stage_root.mkdir(parents=True, exist_ok=True)

    # 1. Copy settings
    staged_settings = stage_root / settings_file.name
    shutil.copy2(str(settings_file), str(staged_settings))

    # 2. Stage tomostar
    staged_tomostar_dir = stage_root / "tomostar"
    staged_tomostar_dir.mkdir(parents=True, exist_ok=True)
    src_tomostar = tomostar_dir / f"{ts_name}.tomostar"
    if src_tomostar.exists():
        dst_tomostar = staged_tomostar_dir / f"{ts_name}.tomostar"
        copy_tomostar_with_absolute_paths(src_tomostar, dst_tomostar, tomostar_dir)

    # 3. Stage the XML (symlink to the defocus-hand-updated copy in output_processing)
    staged_processing = stage_root / "warp_tiltseries"
    staged_processing.mkdir(parents=True, exist_ok=True)
    src_xml = output_processing / f"{ts_name}.xml"
    if not src_xml.exists():
        raise FileNotFoundError(f"Per-TS XML not found: {src_xml}")
    dst_xml = staged_processing / f"{ts_name}.xml"
    if dst_xml.exists() or dst_xml.is_symlink():
        dst_xml.unlink()
    dst_xml.symlink_to(src_xml.resolve())

    return stage_root


def build_ctf_command(params: TsCtfParams) -> str:
    """Build the ts_ctf command to run inside a staged environment."""
    cmd = (
        f"WarpTools ts_ctf "
        f"--settings warp_tiltseries.settings "
        f"--input_processing warp_tiltseries "
        f"--output_processing warp_tiltseries "
        f"--window {params.window} "
        f"--range_low {params.range_min} "
        f"--range_high {params.range_max} "
        f"--defocus_min {params.defocus_min} "
        f"--defocus_max {params.defocus_max} "
        f"--voltage {int(round(params.voltage))} "
        f"--cs {params.spherical_aberration} "
        f"--amplitude {params.amplitude_contrast} "
        f"--perdevice {params.perdevice}"
    )
    if params.do_phase:
        cmd += " --fit_phase"
    return cmd


# ----------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------


def main():
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- ts_ctf: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- ts_ctf: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(TsCtfParams)
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
        additional_binds = local_params_data["additional_binds"]

        input_processing = paths["input_processing"]
        settings_file = paths["warp_tiltseries_settings"]
        output_processing = paths.get("output_processing", job_dir / "warp_tiltseries")

        if not input_processing.exists():
            raise FileNotFoundError(f"Input processing dir not found: {input_processing}")

        # Step 1: Copy alignment XMLs into our output dir
        output_processing.mkdir(parents=True, exist_ok=True)
        print(f"[SUPERVISOR] Copying XMLs from {input_processing} to {output_processing}", flush=True)
        for xml_file in input_processing.glob("*.xml"):
            shutil.copy2(str(xml_file), str(output_processing / xml_file.name))

        # Copy settings into job dir for staging
        local_settings = job_dir / settings_file.name
        if not local_settings.exists():
            shutil.copy2(str(settings_file), str(local_settings))

        # Find tomostar dir (from the alignment job or tsImport)
        tomostar_dir = settings_file.parent / "tomostar"
        local_tomostar = job_dir / "tomostar"
        if not local_tomostar.exists() and tomostar_dir.exists():
            shutil.copytree(str(tomostar_dir), str(local_tomostar))

        # Enumerate TS from the XMLs we just copied
        ts_names = sorted(p.stem for p in output_processing.glob("*.xml"))
        if not ts_names:
            raise ValueError(f"No XML files found in {output_processing}")

        n_tasks = len(ts_names)
        print(f"[SUPERVISOR] Found {n_tasks} tilt-series for CTF estimation", flush=True)

        # Step 2: Run defocus hand detection GLOBALLY (needs all TS)
        print("[SUPERVISOR] Running ts_defocus_hand globally...", flush=True)
        run_defocus_hand_globally(params, local_settings, output_processing, job_dir, additional_binds)
        print("[SUPERVISOR] Defocus hand detection complete.", flush=True)

        # Step 3: Dispatch per-TS CTF estimation
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

        # Step 4: Aggregate metadata
        print("[SUPERVISOR] All tasks succeeded; aggregating metadata...", flush=True)
        translator = MetadataTranslator(StarfileService())
        result = translator.update_ts_ctf_metadata(
            job_dir=job_dir,
            input_star_path=paths["input_star"],
            output_star_path=paths["output_star"],
            project_root=project_path,
            warp_folder="warp_tiltseries",
        )
        if not result["success"]:
            raise Exception(f"Metadata aggregation failed: {result['error']}")

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
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(TsCtfParams)
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
        output_processing = job_dir / "warp_tiltseries"
        local_settings = job_dir / "warp_tiltseries.settings"
        local_tomostar = job_dir / "tomostar"

        # Stage per-TS environment
        stage_root = stage_ctf_environment(job_dir, ts_name, output_processing, local_settings, local_tomostar)
        print(f"[TASK {array_idx}] Staged at: {stage_root}", flush=True)

        # Build and run CTF command
        cmd = build_ctf_command(params)
        print(f"[TASK {array_idx}] Command: {cmd}", flush=True)

        wrapped = get_container_service().wrap_command_for_tool(
            command=cmd, cwd=stage_root, tool_name=params.get_tool_name(), additional_binds=additional_binds
        )
        run_command(wrapped, cwd=stage_root)

        # Copy the updated XML back to the shared output dir
        staged_xml = stage_root / "warp_tiltseries" / f"{ts_name}.xml"
        if staged_xml.exists():
            shutil.copy2(str(staged_xml), str(output_processing / f"{ts_name}.xml"))

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
