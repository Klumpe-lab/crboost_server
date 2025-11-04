#!/usr/bin/env python
# drivers/ts_alignment.py
import json
import subprocess
import sys
import os
import shlex
from pathlib import Path
import traceback

server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))
# ----------------------------------------

try:
    from services.parameter_models import TsAlignmentParams, AlignmentMethod
    from services.metadata_service import MetadataTranslator
    from services.starfile_service import StarfileService
    from services.container_service import get_container_service # Import for consistency
except ImportError as e:
    print(f"FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

def run_command(command: str, cwd: Path):
    """
    Helper to run a shell command, stream output, and check for errors.
    """
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

def build_alignment_commands(params: TsAlignmentParams, paths: dict[str, Path]) -> str:
    """Builds the multi-step WarpTools alignment command string."""
    
    # Get gain path from params object
    gain_path_str = ""
    if params.gain_path and params.gain_path != "None":
        gain_path_str = shlex.quote(params.gain_path)
    
    gain_ops_str = params.gain_operations if params.gain_operations else ""
    
    # Paths from the JSON (are all absolute)
    mdoc_dir        = shlex.quote(str(paths['mdoc_dir']))
    frameseries_dir = shlex.quote(str(paths['frameseries_dir']))
    tomostar_dir    = shlex.quote(str(paths['tomostar_dir']))
    processing_dir  = shlex.quote(str(paths['warp_dir']))         # Use 'warp_dir' for processing
    settings_file   = shlex.quote(str(paths['warp_settings']))

    mkdir_cmds = [
        f"mkdir -p {tomostar_dir}",
        f"mkdir -p {processing_dir}",
    ]

    # === Step 1: ts_import ===
    cmd_parts_import = [
        "WarpTools ts_import",
        "--mdocs", mdoc_dir,
        "--pattern '*.mdoc'",
        "--frameseries", frameseries_dir,
        "--output", tomostar_dir,
        "--tilt_exposure", str(params.dose_per_tilt),
        "--override_axis", str(params.tilt_axis_angle),
    ]
    if not params.invert_tilt_angles:
        cmd_parts_import.append("--dont_invert")
    if params.do_at_most > 0:
        cmd_parts_import.extend(["--do_at_most", str(params.do_at_most)])

    # === Step 2: create_settings ===
    cmd_parts_settings = [
        "WarpTools create_settings",
        "--folder_data", tomostar_dir,
        "--extension '*.tomostar'",
        "--folder_processing", processing_dir,
        "--output", settings_file,
        "--angpix", str(params.pixel_size),
        "--exposure", str(params.dose_per_tilt),
        "--tomo_dimensions", params.tomo_dimensions,
    ]
    if gain_path_str:
        cmd_parts_settings.extend(["--gain_reference", gain_path_str])
        if gain_ops_str:
            cmd_parts_settings.extend(["--gain_operations", gain_ops_str])

    # === Step 3: Alignment ===
    cmd_parts_align = []
    if params.alignment_method == AlignmentMethod.ARETOMO:
        cmd_parts_align = [
            "WarpTools ts_aretomo",
            "--settings", settings_file,
            "--angpix", str(params.rescale_angpixs),
            "--alignz", str(int(params.thickness_nm * 10)),
            "--perdevice", str(params.perdevice),
            "--patches", f"{params.patch_x}x{params.patch_y}",
        ]
        if params.axis_iter > 0:
            cmd_parts_align.extend([
                "--axis_iter", str(params.axis_iter),
                "--axis_batch", str(params.axis_batch),
            ])
    elif params.alignment_method == AlignmentMethod.IMOD:
        cmd_parts_align = [
            "WarpTools ts_etomo_patches",
            "--settings", settings_file,
            "--angpix", str(params.rescale_angpixs),
            "--patch_size", str(int(params.imod_patch_size * 10)),
        ]
    else:
        return f"echo 'ERROR: Alignment method {params.alignment_method} not implemented'; exit 1;"

    if params.do_at_most > 0:
        cmd_parts_align.extend(["--do_at_most", str(params.do_at_most)])
    
    # === Combine all commands ===
    return " && ".join([
        " ".join(mkdir_cmds),
        " ".join(cmd_parts_import),
        " ".join(cmd_parts_settings),
        " ".join(cmd_parts_align),
    ])

def main():
    print("[DRIVER] tsAlignment driver started.", flush=True)
    job_dir = Path.cwd()
    params_file = job_dir / "job_params.json"
    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load parameters
        print(f"[DRIVER] Loading params from {params_file}", flush=True)
        with open(params_file, 'r') as f:
            params_data = json.load(f)
        
        params = TsAlignmentParams(**params_data['job_model'])
        # Paths are Dict[str, str] of absolute paths
        paths_str_dict = params_data['paths']
        # Convert to Path objects for local use
        paths = {k: Path(v) for k, v in paths_str_dict.items()}

        
        # Get I/O STAR files (already absolute)
        input_star_abs = paths['input_star']
        output_star_abs = paths['output_star']
        
        # 2. Build and run alignment commands
        # We pass the Path dict for building
        align_command_str = build_alignment_commands(params, paths)
        
        # 3. Get container service to build the *full apptainer* command
        container_svc = get_container_service()
        apptainer_command = container_svc.wrap_command_for_tool(
            command          = align_command_str,
            cwd              = job_dir,
            tool_name        = "warptools",                     
            additional_binds = params_data["additional_binds"]
        )
        
        # 4. Run the containerized computation
        print(f"[DRIVER] Executing container...", flush=True)
        run_command(apptainer_command, cwd=job_dir)
        
        # 5. Run metadata processing
        print("[DRIVER] Alignment finished. Starting metadata processing.", flush=True)
        translator = MetadataTranslator(StarfileService())
        
        result = translator.update_ts_alignment_metadata(
            job_dir=job_dir,
            input_star_path=input_star_abs,
            output_star_path=output_star_abs,
            tomo_dimensions=params.tomo_dimensions,
            alignment_method=params.alignment_method.value
        )

        if not result['success']:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.", flush=True)
        
        # 6. Create success file
        success_file.touch()
        print("[DRIVER] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(str(e), file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        
        failure_file.touch()
        print("[DRIVER] Job failed.", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()