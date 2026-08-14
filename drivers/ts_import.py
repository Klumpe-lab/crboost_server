#!/usr/bin/env python
# drivers/ts_import.py
"""
ts_import driver — lightweight metadata assembly job.

Runs WarpTools ts_import + create_settings to convert mdocs and processed
frame-series into tomostar files and a warp_tiltseries.settings file.
No GPU work — purely metadata assembly. Runs as a single SLURM job (not an array).
"""

import os
import sys
import traceback
from pathlib import Path

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.driver_base import ToolCommand, get_driver_context, run_tool
from services.jobs.ts_import import TsImportParams


def build_ts_import_commands(params: TsImportParams, paths: dict, job_dir: Path) -> str:
    """
    Build the WarpTools ts_import + create_settings command chain.

    After this runs, job_dir will contain:
        tomostar/               <- one .tomostar per tilt-series
        warp_tiltseries.settings <- settings for downstream TS jobs
    """
    # frameseries dir relative to job_dir — WarpTools stores _wrpMovieName
    # in the tomostar relative to the tomostar file's location.
    frameseries_rel = os.path.relpath(str(paths["input_processing"]), str(job_dir))

    gain_path = params.gain_path if params.gain_path and params.gain_path != "None" else ""
    gain_ops_str = params.gain_operations if hasattr(params, "gain_operations") and params.gain_operations else ""

    # === Step 1: ts_import ===
    ts_import = (
        ToolCommand("WarpTools ts_import")
        .opt_path("--mdocs", paths["mdoc_dir"], quote=True)
        .opt_path("--pattern", params.mdoc_pattern, quote=True)
        .opt_path("--frameseries", frameseries_rel, quote=True)
        .opt("--output", "tomostar")
        .opt("--tilt_exposure", params.dose_per_tilt)
        .opt("--override_axis", params.tilt_axis_angle)
        .opt("--min_intensity", params.min_intensity)
    )
    if not params.invert_tilt_angles:
        ts_import.flag("--dont_invert")
    if params.do_at_most > 0:
        ts_import.opt("--do_at_most", params.do_at_most)

    # === Step 2: create_settings ===
    create_settings = (
        ToolCommand("WarpTools create_settings")
        .opt("--folder_data", "tomostar")
        .raw("--extension '*.tomostar'")
        .opt("--folder_processing", "warp_tiltseries")
        .opt("--output", "warp_tiltseries.settings")
        .opt("--angpix", params.pixel_size)
        .opt("--exposure", params.dose_per_tilt)
        .opt("--tomo_dimensions", params.tomo_dimensions)
    )
    if gain_path:
        create_settings.opt_path("--gain_reference", gain_path, quote=True)
        if gain_ops_str:
            create_settings.opt("--gain_operations", gain_ops_str)

    return " && ".join(
        [
            f"test -d tomostar && ls tomostar/*.tomostar >/dev/null 2>&1 || ({ts_import.render()})",
            f"test -f warp_tiltseries.settings || ({create_settings.render()})",
        ]
    )


def main():
    print("[DRIVER] ts_import driver started.", flush=True)

    try:
        (_project_state, params, local_params_data, job_dir, _project_path, job_type) = get_driver_context(
            TsImportParams
        )
    except Exception as e:
        job_dir = Path.cwd()
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        print(f"[DRIVER] Params loaded for job type {job_type} in {job_dir}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        # Build and run the WarpTools commands
        command_str = build_ts_import_commands(params, paths, job_dir)
        print(f"[DRIVER] Command: {command_str}", flush=True)

        print("[DRIVER] Executing container...", flush=True)
        run_tool(command_str, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

        # Verify outputs exist
        tomostar_dir = job_dir / "tomostar"
        settings_file = job_dir / "warp_tiltseries.settings"
        tomostar_files = list(tomostar_dir.glob("*.tomostar")) if tomostar_dir.exists() else []

        if not tomostar_files:
            raise FileNotFoundError(f"ts_import produced no tomostar files in {tomostar_dir}")
        if not settings_file.exists():
            raise FileNotFoundError(f"create_settings did not produce {settings_file}")

        print(f"[DRIVER] Created {len(tomostar_files)} tomostar files", flush=True)

        success_file.touch()
        print("[DRIVER] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(str(e), file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("[DRIVER] Job failed.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
