#!/usr/bin/env python
import json
import subprocess
import sys
import os
from pathlib import Path

# --- Add server root to Python path ---
CRBOOST_SERVER_DIR = os.environ.get("CRBOOST_SERVER_DIR")
if not CRBOOST_SERVER_DIR:
    print("FATAL: CRBOOST_SERVER_DIR environment variable not set.", file=sys.stderr)
    sys.exit(1)
sys.path.append(CRBOOST_SERVER_DIR)
# ----------------------------------------

try:
    from services.parameter_models import TsAlignmentParams, AlignmentMethod
    from services.metadata_service import MetadataTranslator
    from services.starfile_service import StarfileService
except ImportError as e:
    print(f"FATAL: Could not import services. Check CRBOOST_SERVER_DIR.", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

# (Include the same run_command helper function from fs_motion_ctf_wrapper.py)
def run_command(command: str, cwd: Path):
    """Helper to run a shell command and check for errors."""
    print(f"[WRAPPER] Executing: {command}", flush=True)
    result = subprocess.run(
        command, shell=True, cwd=cwd, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    print("--- STDOUT ---", flush=True)
    print(result.stdout, flush=True)
    print("--- STDERR ---", flush=True)
    print(result.stderr, file=sys.stderr, flush=True)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, command, result.stdout, result.stderr
        )
    return result

def build_alignment_commands(params: TsAlignmentParams, paths: dict) -> str:
    """Builds the multi-step WarpTools alignment command string."""
    
    gain_path_str = paths.get('gain_path_str', '')
    gain_ops_str = paths.get('gain_operations_str', '')
    
    # Paths from the JSON
    mdoc_dir = paths['mdoc_dir']
    frameseries_dir = paths['frameseries_dir']
    tomostar_dir = paths['tomostar_dir']
    processing_dir = paths['processing_dir']
    settings_file = paths['settings_file']

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
            "--out_imod", str(params.out_imod),
            "--tilt_cor", str(params.tilt_cor),
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
    print("[WRAPPER] tsAlignment wrapper started.", flush=True)
    job_dir = Path.cwd()
    params_file = job_dir / "job_params.json"
    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load parameters
        print(f"[WRAPPER] Loading params from {params_file}", flush=True)
        with open(params_file, 'r') as f:
            params_data = json.load(f)
        
        params = TsAlignmentParams(**params_data['job_model'])
        paths = params_data['paths']
        
        # Get I/O STAR files
        input_star_rel = paths['input_star']
        output_star_rel = paths['output_star']
        
        # Metadata service needs absolute path to input star
        abs_input_star = (job_dir / input_star_rel).resolve()
        
        # 2. Build and run alignment commands
        align_command = build_alignment_commands(params, paths)
        run_command(align_command, cwd=job_dir)
        
        # 3. Run metadata processing
        print("[WRAPPER] Alignment finished. Starting metadata processing.", flush=True)
        translator = MetadataTranslator(StarfileService())
        
        result = translator.update_ts_alignment_metadata(
            job_dir=job_dir,
            input_star_path=abs_input_star,
            output_star_path=Path(output_star_rel),
            tomo_dimensions=params.tomo_dimensions,
            alignment_program=params.alignment_method.value # Pass "Aretomo" or "Imod"
        )

        if not result['success']:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[WRAPPER] Metadata processing successful.", flush=True)
        
        # 4. Create success file
        success_file.touch()
        print("[WRAPPER] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[WRAPPER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(str(e), file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
        
        failure_file.touch()
        print("[WRAPPER] Job failed.", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()