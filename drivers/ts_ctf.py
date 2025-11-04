#!/usr/bin/env python3

import json
import sys
from pathlib import Path
from typing import Dict

# Add the project root to Python path to import services
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.metadata_service import MetadataTranslator
from services.container_service import get_container_service
from services.parameter_models import TsCtfParams


def main():
    """Main driver function for tsCTF job"""
    print("Python", sys.version)
    print("--- SLURM JOB START ---")

    # Get job directory from command line argument
    if len(sys.argv) < 2:
        print("ERROR: Job directory argument required")
        sys.exit(1)

    job_dir = Path(sys.argv[1])
    print(f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}")
    print(f"Original CWD: {Path.cwd()}")
    print(f"Target Job Directory: {job_dir}")

    # Change to job directory
    job_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(job_dir)
    print(f"New CWD: {Path.cwd()}")

    print("[DRIVER] tsCTF driver started.")

    try:
        # Load job parameters
        params_path = job_dir / "job_params.json"
        print(f"[DRIVER] Loading params from {params_path}")

        if not params_path.exists():
            raise FileNotFoundError(f"Job parameters not found at {params_path}")

        with open(params_path, "r") as f:
            params_data = json.load(f)

        params = TsCtfParams(**params_data)
        print("[DRIVER] Parameters loaded successfully")

        # Get input/output assets
        project_root = job_dir.parent.parent
        upstream_outputs = {
            "aligntiltsWarp": {
                "output_star": project_root / "External" / "job003" / "aligned_tilt_series.star",
                "warp_dir": project_root / "External" / "job003" / "warp_tiltseries",
            }
        }

        input_assets = params.get_input_assets(job_dir, project_root, upstream_outputs)
        output_assets = params.get_output_assets(job_dir)

        print("[DRIVER] Received paths:")
        for key, path in input_assets.items():
            print(f"  {key}: {path}")

        # Validate input files
        if not input_assets["input_star"].exists():
            raise FileNotFoundError(f"Input STAR file not found: {input_assets['input_star']}")

        # Copy settings and tomostar from previous job
        print("[DRIVER] Copying settings and metadata from alignment job...")
        copy_previous_metadata(input_assets, output_assets)

        # Build and execute WarpTools commands
        print("[DRIVER] Building WarpTools commands...")
        container_service = get_container_service()

        # Command 1: Check defocus handness
        check_hand_command = build_check_defocus_hand_command(params, input_assets)
        print(f"[DRIVER] Command 1: {check_hand_command}")

        # Command 2: Set defocus handness
        set_hand_command = build_set_defocus_hand_command(params, input_assets)
        print(f"[DRIVER] Command 2: {set_hand_command}")

        # Command 3: Run CTF determination
        ctf_command = build_ctf_command(params, input_assets)
        print(f"[DRIVER] Command 3: {ctf_command}")

        # Execute commands in container - USE THE SAME PATTERN AS OTHER DRIVERS
        print("[DRIVER] Executing container commands...")

        for i, command in enumerate([check_hand_command, set_hand_command, ctf_command], 1):
            print(f"[DRIVER] Executing command {i}/3...")

            # Use container service to wrap the command (same pattern as other drivers)
            wrapped_command = container_service.wrap_command_for_tool(
                command=command, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=[str(project_root)]
            )

            # Execute using subprocess (same pattern as other drivers)
            import subprocess

            result = subprocess.run(wrapped_command, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                print(f"[ERROR] Command {i} failed with return code {result.returncode}")
                print(f"[ERROR] stdout: {result.stdout}")
                print(f"[ERROR] stderr: {result.stderr}")
                raise Exception(f"Command {i} failed: {result.stderr}")

            print(f"[DRIVER] Command {i} completed successfully")

        print("[DRIVER] Computation finished. Starting metadata processing.")

        # Update metadata
        metadata_service = MetadataTranslator()
        result = metadata_service.update_ts_ctf_metadata(
            job_dir=job_dir,
            input_star_path=input_assets["input_star"],
            output_star_path=output_assets["output_star"],
            warp_folder="warp_tiltseries",
        )

        if not result["success"]:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.")
        print("[DRIVER] Job finished successfully.")

        # Create success sentinel
        success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---")
        print("Creating RELION_JOB_EXIT_SUCCESS")

    except Exception as e:
        print(f"[DRIVER] FATAL ERROR: Job failed.")
        print(f"{type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()

        # Create failure sentinel
        failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---")
        print("Creating RELION_JOB_EXIT_FAILURE")
        sys.exit(1)


def copy_previous_metadata(input_assets: Dict[str, Path], output_assets: Dict[str, Path]):
    """Copy settings and tomostar from alignment job"""
    import shutil

    # Copy warp_tiltseries settings
    prev_settings = input_assets["frameseries_dir"].parent / "warp_tiltseries.settings"
    if prev_settings.exists():
        shutil.copy2(prev_settings, output_assets["warp_settings"])
        print(f"[DRIVER] Copied settings: {prev_settings} -> {output_assets['warp_settings']}")

    # Copy tomostar directory
    prev_tomostar = input_assets["frameseries_dir"].parent / "tomostar"
    if prev_tomostar.exists():
        if output_assets["tomostar_dir"].exists():
            shutil.rmtree(output_assets["tomostar_dir"])
        shutil.copytree(prev_tomostar, output_assets["tomostar_dir"])
        print(f"[DRIVER] Copied tomostar: {prev_tomostar} -> {output_assets['tomostar_dir']}")

    # Copy existing warp_tiltseries XML files
    if input_assets["frameseries_dir"].exists():
        if output_assets["warp_dir"].exists():
            shutil.rmtree(output_assets["warp_dir"])
        shutil.copytree(input_assets["frameseries_dir"], output_assets["warp_dir"])
        print(f"[DRIVER] Copied warp directory: {input_assets['frameseries_dir']} -> {output_assets['warp_dir']}")


def build_check_defocus_hand_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    """Build command to check defocus handness"""
    return f"WarpTools ts_defocus_hand --settings {input_assets['warp_dir']}.settings --check"


def build_set_defocus_hand_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    """Build command to set defocus handness"""
    return f"WarpTools ts_defocus_hand --settings {input_assets['warp_dir']}.settings --{params.defocus_hand}"


def build_ctf_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    """Build command for CTF determination"""
    return (
        f"WarpTools ts_ctf "
        f"--settings {input_assets['warp_dir']}.settings "
        f"--window {params.window} "
        f"--range_low {params.range_min} "
        f"--range_high {params.range_max} "
        f"--defocus_min {params.defocus_min} "
        f"--defocus_max {params.defocus_max} "
        f"--voltage {params.voltage} "
        f"--cs {params.cs} "
        f"--amplitude {params.amplitude} "
        f"--perdevice {params.perdevice}"
    )


if __name__ == "__main__":
    import os

    main()
