#!/usr/bin/env python
# drivers/fs_motion_and_ctf.py
# This script is executed directly on the compute node by Relion

import json
import subprocess
import sys
import os
import shlex
from pathlib import Path

# Add the server root to PYTHONPATH (set by fn_exe command)
# This allows us to import 'services'
server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))

try:
    from services.parameter_models import FsMotionCtfParams
    from services.metadata_service import MetadataTranslator
    from services.starfile_service import StarfileService
    from services.container_service import get_container_service
except ImportError as e:
    print(f"FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def run_command(command: str, cwd: Path):
    """
    Helper to run a shell command, stream output, and check for errors.
    This will run the main apptainer command.
    """
    # print(f"[DRIVER] Executing: {command}", flush=True) # <-- REMOVED THIS LINE
    process = subprocess.Popen(command, shell=True, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("--- CONTAINER STDOUT ---", flush=True)
    if process.stdout:
        for line in iter(process.stdout.readline, ""):
            print(line, end="", flush=True)

    print("--- CONTAINER STDERR ---", file=sys.stderr, flush=True)
    stderr_output = ""
    if process.stderr:
        for line in iter(process.stderr.readline, ""):
            print(line, end="", file=sys.stderr, flush=True)
            stderr_output += line

    process.wait()

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command, None, stderr_output)


def build_warp_commands(params: FsMotionCtfParams, paths: dict[str, Path]) -> str:
    """
    Builds the multi-step WarpTools command string using absolute paths.
    """
    
    # Get absolute paths from the 'paths' dict
    frames_dir_abs = shlex.quote(str(paths["frames_dir"]))
    warp_dir_abs = shlex.quote(str(paths["warp_dir"]))
    settings_file_abs = shlex.quote(str(paths["warp_settings"]))

    gain_path_str = ""
    if params.gain_path and params.gain_path != "None":
        gain_path_str = shlex.quote(params.gain_path)

    gain_ops_str = params.gain_operations if params.gain_operations else ""

    create_settings_parts = [
        "WarpTools create_settings",
        f"--folder_data {frames_dir_abs}",
        "--extension '*.eer'",
        f"--folder_processing {warp_dir_abs}",
        f"--output {settings_file_abs}",
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
         # WarpTools fs_motion_and_ctf expects microns
        "--c_defocus_min",
        str(params.defocus_min_microns),
        "--c_defocus_max",
        str(params.defocus_max_microns),
        "--c_voltage",
        str(round(float(params.voltage))),
        "--c_cs",
        str(params.cs),
        "--c_amplitude",
        str(params.amplitude),
        "--perdevice",
        str(params.perdevice),
        "--out_averages",
    ]

    if params.do_at_most > 0:
        run_main_parts.extend(["--do_at_most", str(params.do_at_most)])

    # These commands are run *inside* the container
    return " && ".join([" ".join(create_settings_parts), " ".join(run_main_parts)])


def main():
    print("[DRIVER] fs_motion_and_ctf driver started.", flush=True)

    # Relion sets the CWD to the job directory
    job_dir = Path.cwd()
    params_file = job_dir / "job_params.json"
    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load parameters from JSON
        print(f"[DRIVER] Loading params from {params_file}", flush=True)
        with open(params_file, "r") as f:
            params_data = json.load(f)

        params = FsMotionCtfParams(**params_data["job_model"])

        # Paths are ALL ABSOLUTE from orchestrator
        paths = {k: Path(v) for k, v in params_data["paths"].items()}
        additional_binds = params_data["additional_binds"]

        print(f"[DRIVER] Job directory: {job_dir}", flush=True)
        print(f"[DRIVER] Received paths:", flush=True)
        for key, path in paths.items():
            print(f"  {key}: {path}", flush=True)

        # 2. Build the *inner* WarpTools command (now using absolute paths)
        warp_command = build_warp_commands(params, paths)
        print(f"[DRIVER] Built inner command: {warp_command[:200]}...", flush=True)

        # 3. Get container service to build the *full apptainer* command
        container_svc = get_container_service()
        apptainer_command = container_svc.wrap_command_for_tool(
            command=warp_command, cwd=job_dir, tool_name="warptools", additional_binds=additional_binds
        )

        # 4. Run the containerized computation
        print(f"[DRIVER] Executing container...", flush=True)
        run_command(apptainer_command, cwd=job_dir)

        # 5. Run the metadata processing step
        print("[DRIVER] Computation finished. Starting metadata processing.", flush=True)
        translator = MetadataTranslator(StarfileService())

        # All paths are ABSOLUTE - use them directly
        input_star_abs = paths["input_star"]
        output_star_abs = paths["output_star"]

        print(f"[DRIVER] Input STAR (absolute): {input_star_abs}", flush=True)
        print(f"[DRIVER] Output STAR (absolute): {output_star_abs}", flush=True)
        print(f"[DRIVER] Input STAR exists: {input_star_abs.exists()}", flush=True)

        result = translator.update_fs_motion_and_ctf_metadata(
            job_dir=job_dir,
            input_star_path=input_star_abs,  # Already absolute
            output_star_path=output_star_abs,  # Already absolute
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
        import traceback

        traceback.print_exc(file=sys.stderr)

        # Create failure file
        failure_file.touch()
        print("[DRIVER] Job failed.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
