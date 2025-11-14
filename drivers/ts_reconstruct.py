#!/usr/bin/env python3
import shlex
import sys
import os
from pathlib import Path
import traceback

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.project_state import TsReconstructParams
from services.metadata_service import MetadataTranslator
from services.starfile_service import StarfileService
from services.container_service import get_container_service


def build_reconstruct_command(params: TsReconstructParams, paths: dict) -> str:
    """Build reconstruction command using input_processing."""
    
    settings_file     = shlex.quote(str(paths['warp_tiltseries_settings']))
    input_processing  = shlex.quote(str(paths["input_processing"]))          # From CTF job
    output_processing = shlex.quote(str(paths["output_processing"]))         # From CTF job


    return (
        f"WarpTools ts_reconstruct "
        f"--settings {settings_file} "
        f"--input_processing {input_processing} "  # Read from CTF job
        f"--output_processing {output_processing} "  # CRITICAL: Write to current job
        f"--angpix {params.rescale_angpixs} "
        f"--halfmap_frames {params.halfmap_frames} "
        f"--deconv {params.deconv} "
        f"--perdevice {params.perdevice} "
        f"--dont_invert"
    )


def main():
    """Main driver function for tsReconstruct job"""
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    try:
        (
            project_state,
            params,  
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
    print("[DRIVER] tsReconstruct driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths and binds
        print(f"[DRIVER] Params loaded for {job_type}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        if not paths["input_star"].exists():
            raise FileNotFoundError(f"Input STAR file not found: {paths['input_star']}")

        # 2. NO MORE COPYING - WarpTools handles this via input_processing
        print("[DRIVER] Using WarpTools input_processing for data flow", flush=True)

        # 3. Build and execute reconstruction command
        print("[DRIVER] Building WarpTools command...", flush=True)
        reconstruct_command = build_reconstruct_command(params, paths)
        print(f"[DRIVER] Command: {reconstruct_command}", flush=True)

        container_service = get_container_service()
        wrapped_command = container_service.wrap_command_for_tool(
            command=reconstruct_command,
            cwd=job_dir,
            tool_name=params.get_tool_name(),
            additional_binds=additional_binds,
        )

        print("[DRIVER] Executing container command...", flush=True)
        run_command(wrapped_command, cwd=job_dir)

        # 4. Update metadata
        print("[DRIVER] Computation finished. Starting metadata processing.", flush=True)
        metadata_service = MetadataTranslator(StarfileService())
        result = metadata_service.update_ts_reconstruct_metadata(
            job_dir=job_dir,
            input_star_path=paths["input_star"],
            output_star_path=paths["output_star"],
            warp_folder="warp_tiltseries",
            rescale_angpixs=params.rescale_angpixs,
            frame_pixel_size=params.pixel_size,
        )

        if not result["success"]:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.", flush=True)
        print("[DRIVER] Job finished successfully.", flush=True)

        # 5. Create success file
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


if __name__ == "__main__":
    main()
