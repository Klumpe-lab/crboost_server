#!/usr/bin/env python3
import json
import shlex
import sys
import os
import argparse
import subprocess
from pathlib import Path
from typing import Dict
import traceback
import shutil

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    # --- NEW: Import shared driver logic ---
    from drivers.driver_base import get_driver_context, run_command
    from services.project_state import TsCtfParams
    from services.metadata_service import MetadataTranslator
    from services.starfile_service import StarfileService
    from services.container_service import get_container_service
except ImportError as e:
    print(f"FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def main():
    """Main driver function for tsCTF job"""
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    try:
        # --- NEW BOOTSTRAP CALL ---
        (
            project_state,
            params,  # This is now the state-aware TsCtfParams object
            local_params_data,
            job_dir,
            project_path,
            job_type,
        ) = get_driver_context()
        
    except Exception as e:
        job_dir = Path.cwd()
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(
        f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}",
        flush=True,
    )
    print(f"CWD (Job Directory): {job_dir}", flush=True)
    print("[DRIVER] tsCTF driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths and binds
        print(f"[DRIVER] Params loaded for {job_type}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        if not paths["input_star"].exists():
            raise FileNotFoundError(f"Input STAR file not found: {paths['input_star']}")

        # 2. Copy settings and tomostar from previous job
        print("[DRIVER] Copying settings and metadata from alignment job...", flush=True)
        copy_previous_metadata(paths, paths)

        # 3. Build and execute WarpTools commands
        print("[DRIVER] Building WarpTools commands...", flush=True)
        container_service = get_container_service()

        check_hand_command = build_check_defocus_hand_command(params, paths)
        set_hand_command = build_set_defocus_hand_command(params, paths)
        ctf_command = build_ctf_command(params, paths)

        # 4. Execute commands in container
        print("[DRIVER] Executing container commands...", flush=True)
        for i, command in enumerate([check_hand_command, set_hand_command, ctf_command], 1):
            print(f"[DRIVER] Executing command {i}/3...", flush=True)
            wrapped_command = container_service.wrap_command_for_tool(
                command=command, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
            )
            run_command(wrapped_command, cwd=job_dir)
            print(f"[DRIVER] Command {i} completed successfully", flush=True)

        # 5. Metadata processing
        print("[DRIVER] Computation finished. Starting metadata processing.", flush=True)
        metadata_service = MetadataTranslator(StarfileService())
        result = metadata_service.update_ts_ctf_metadata(
            job_dir=job_dir,
            input_star_path=paths["input_star"],
            output_star_path=paths["output_star"],
            project_root=project_path,
            warp_folder="warp_tiltseries",
        )

        if not result["success"]:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.", flush=True)
        print("[DRIVER] Job finished successfully.", flush=True)

        # 6. Create success sentinel
        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)


def copy_previous_metadata(input_assets: Dict[str, Path], output_assets: Dict[str, Path]):
    """Copy settings and tomostar from alignment job"""
    prev_settings = input_assets["warp_settings_in"]
    prev_tomostar = input_assets["tomostar_dir_in"]
    prev_warp_dir = input_assets["frameseries_dir"]

    if prev_settings.exists():
        shutil.copy2(prev_settings, output_assets["warp_settings"])
    else:
        print(f"[DRIVER] WARNING: Previous settings not found at {prev_settings}", flush=True)

    if prev_tomostar.exists():
        if output_assets["tomostar_dir"].exists():
            shutil.rmtree(output_assets["tomostar_dir"])
        shutil.copytree(prev_tomostar, output_assets["tomostar_dir"])
    else:
        print(f"[DRIVER] WARNING: Previous tomostar not found at {prev_tomostar}", flush=True)

    if prev_warp_dir.exists():
        if output_assets["warp_dir"].exists():
            shutil.rmtree(output_assets["warp_dir"])
        shutil.copytree(prev_warp_dir, output_assets["warp_dir"])
    else:
        print(f"[DRIVER] WARNING: Previous warp dir not found at {prev_warp_dir}", flush=True)


def build_check_defocus_hand_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    settings_file = shlex.quote(str(input_assets["warp_settings"]))
    return f"WarpTools ts_defocus_hand --settings {settings_file} --check"


def build_set_defocus_hand_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    settings_file = shlex.quote(str(input_assets["warp_settings"]))
    if params.defocus_hand == "set_flip":
        return f"WarpTools ts_defocus_hand --settings {settings_file} --set_flip"
    else:
        return f"WarpTools ts_defocus_hand --settings {settings_file} --set_noflip"


def build_ctf_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    settings_file = shlex.quote(str(input_assets["warp_settings"]))
    return (
        f"WarpTools ts_ctf "
        f"--settings {settings_file} "
        f"--window {params.window} "
        f"--range_low {params.range_min} "
        f"--range_high {params.range_max} "
        f"--defocus_min {params.defocus_min} "
        f"--defocus_max {params.defocus_max} "
        f"--voltage {int(round(params.voltage))} "          # Property access
        f"--cs {params.spherical_aberration} "         # Property access
        f"--amplitude {params.amplitude_contrast} "    # Property access
        f"--perdevice {params.perdevice}"
    )


if __name__ == "__main__":
    main()
