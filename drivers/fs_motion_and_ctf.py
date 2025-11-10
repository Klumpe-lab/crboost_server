#!/usr/bin/env python
# drivers/fs_motion_and_ctf.py
import json
import subprocess
import sys
import os
import shlex
from pathlib import Path
import traceback

# Add the server root to PYTHONPATH
server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))

try:
    # --- NEW: Import shared driver logic ---
    from drivers.driver_base import get_driver_context, run_command
    from services.project_state import FsMotionCtfParams
    from services.metadata_service import MetadataTranslator
    from services.starfile_service import StarfileService
    from services.container_service import get_container_service
except ImportError as e:
    print(f"FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


# This function is job-specific, so it stays here
def build_warp_commands(params: FsMotionCtfParams, paths: dict[str, Path]) -> str:
    """
    Builds the multi-step WarpTools command string using absolute paths.
    """
    
    # Get absolute paths from the 'paths' dict
    frames_dir_abs = shlex.quote(str(paths["frames_dir"]))
    warp_dir_abs = shlex.quote(str(paths["warp_dir"]))
    settings_file_abs = shlex.quote(str(paths["warp_settings"]))

    # --- THIS IS THE CALL THAT WAS FAILING ---
    # It will now work because 'params' is state-aware.
    gain_path_str = ""
    if params.gain_path and params.gain_path != "None":
        gain_path_str = shlex.quote(params.gain_path)

    gain_ops_str = params.gain_operations if params.gain_operations else ""

    eer_groups_val = str(params.eer_ngroups)
    
    # Try to find a frame file to check its extension
    frame_files = list(paths["frames_dir"].glob(f"*.eer"))
    if not frame_files:
         frame_files = list(paths["frames_dir"].glob(f"*.mrc")) # Add other types if needed
    
    frame_ext = ".eer" # Default
    if frame_files:
        frame_ext = frame_files[0].suffix
        
    if frame_ext.lower() == ".eer":
        eer_groups_val = f"-{params.eer_ngroups}"
        print(f"[DRIVER] Detected EER files, using eer_ngroups: {eer_groups_val}", flush=True)

    create_settings_parts = [
        "WarpTools create_settings",
        f"--folder_data {frames_dir_abs}",
        f"--extension '*{frame_ext}'",  # Use detected extension
        f"--folder_processing {warp_dir_abs}",
        f"--output {settings_file_abs}",
        "--angpix",
        str(params.pixel_size),
        "--eer_ngroups",
        eer_groups_val,
    ]

    if gain_path_str:
        create_settings_parts.extend(["--gain_reference", gain_path_str])
        if gain_ops_str:
            create_settings_parts.extend(["--gain_operations", gain_ops_str])

    run_main_parts = [
        "WarpTools fs_motion_and_ctf",
        f"--settings {settings_file_abs}",
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
        str(params.spherical_aberration), # Use property
        "--c_amplitude",
        str(params.amplitude_contrast), # Use property
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

    if params.do_at_most > 0:
        run_main_parts.extend(["--do_at_most", str(params.do_at_most)])

    return " && ".join([" ".join(create_settings_parts), " ".join(run_main_parts)])


def main():
    print("[DRIVER] fs_motion_and_ctf driver started.", flush=True)

    try:
        # --- NEW BOOTSTRAP CALL ---
        # This replaces the old get_driver_context()
        # 'params' is now the fully instantiated, state-aware FsMotionCtfParams object
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

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths and binds from the local params file
        print(f"[DRIVER] Params loaded for job type {job_type} in {job_dir}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        # 2. Build the *inner* WarpTools command
        # We pass the state-aware 'params' object
        warp_command = build_warp_commands(params, paths)
        print(f"[DRIVER] Built inner command: {warp_command[:200]}...", flush=True)

        # 3. Get container service to build the *full apptainer* command
        container_svc = get_container_service()
        apptainer_command = container_svc.wrap_command_for_tool(
            command=warp_command, cwd=job_dir, tool_name="warptools", additional_binds=additional_binds
        )

        # 4. Run the containerized computation (using shared function)
        print(f"[DRIVER] Executing container...", flush=True)
        run_command(apptainer_command, cwd=job_dir)

        # 5. Run the metadata processing step
        print("[DRIVER] Computation finished. Starting metadata processing.", flush=True)
        translator = MetadataTranslator(StarfileService())

        result = translator.update_fs_motion_and_ctf_metadata(
            job_dir=job_dir,
            input_star_path=paths["input_star"],
            output_star_path=paths["output_star"],
            project_root=project_path,
            warp_folder="warp_frameseries",
        )

        if not result["success"]:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.", flush=True)

        # 6. Create success file
        success_file.touch()
        print("[DRIVER] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(str(e), file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("[DRIVER] Job failed.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
