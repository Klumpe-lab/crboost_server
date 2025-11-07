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

# Add the project root to Python path to import services
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from services.metadata_service import MetadataTranslator
    from services.parameter_models import TsCtfParams
    from services.starfile_service import StarfileService # Added for translator
    from services.container_service import get_container_service
except ImportError as e:
    print(f"FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


# --- DRIVER BOOTSTRAP ---
def get_driver_context():
    """
    Parses args, finds paths, and ensures job_params.json exists.
    Returns:
        - params_data (dict): The full, raw loaded JSON data.
        - job_dir (Path): The current job directory.
        - project_path (Path): The root project directory.
        - job_type (str): The job_type string.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_type", required=True, help="JobType string (e.g., tsAlignment)")
    parser.add_argument("--project_path", required=True, type=Path, help="Absolute path to the project root")
    
    args, unknown = parser.parse_known_args()
    
    job_type = args.job_type
    project_path = args.project_path.resolve()
    job_dir = Path.cwd().resolve() # Relion sets CWD to the job dir
    params_file = job_dir / "job_params.json"
    
    params_data = None

    if not params_file.exists():
        print(f"job_params.json not found. Generating for new job...")
        try:
            # 1. Get job number from CWD (e.g., "job007" -> 7)
            job_number = int(job_dir.name.replace('job', ''))
            
            # 2. Find server root and param_generator.py
            # sys.argv[0] is this script
            current_server_root = Path(sys.argv[0]).parent.parent.resolve()
            param_generator_script = current_server_root / "services" / "param_generator.py"
            
            # 3. Find python executable (use the one running this script)
            python_exe = sys.executable 
            
            # 4. Build command to call the generator
            cmd = [
                str(python_exe),
                str(param_generator_script),
                "--job_type", job_type,
                "--project_path", str(project_path),
                "--job_number", str(job_number)
            ]
            
            # 5. Run command. Must inherit PYTHONPATH from fn_exe setup
            env = os.environ.copy()
            if str(current_server_root) not in env.get("PYTHONPATH", ""):
                 env["PYTHONPATH"] = f"{current_server_root}{os.pathsep}{env.get('PYTHONPATH', '')}"

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=job_dir, env=env)

            if result.returncode != 0:
                print(f"--- Param Generator STDOUT ---\n{result.stdout}", file=sys.stderr)
                print(f"--- Param Generator STDERR ---\n{result.stderr}", file=sys.stderr)
                raise Exception(f"param_generator.py failed. See stderr.")
            
            # 6. Save the generator's stdout to file
            params_json_str = result.stdout
            if not params_json_str:
                 raise Exception("param_generator.py gave no output.")
            
            with open(params_file, 'w') as f:
                f.write(params_json_str)
            
            params_data = json.loads(params_json_str)
            print(f"Successfully generated and saved {params_file}")

        except Exception as e:
            print(f"FATAL: Could not generate job_params.json: {e}", file=sys.stderr)
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            sys.exit(1) # Exit with failure
            
    else:
        # File already exists (standard run)
        with open(params_file, 'r') as f:
            params_data = json.load(f)

    return params_data, job_dir, project_path, job_type
# --- END DRIVER BOOTSTRAP ---

# --- Re-usable run_command from other drivers ---
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


def main():
    """Main driver function for tsCTF job"""
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    # --- NEW BOOTSTRAP CALL ---
    try:
        # This function gets CWD=job_dir and ensures params exist
        params_data, job_dir, project_path, job_type = get_driver_context()
    except Exception as e:
        # Bootstrap failed, write failure file and exit
        job_dir = Path.cwd() # Try to get CWD for failure file
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    # --- END BOOTSTRAP CALL ---

    print(f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}", flush=True)
    print(f"CWD (Job Directory): {job_dir}", flush=True)
    print("[DRIVER] tsCTF driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # Load job parameters (already loaded by bootstrap)
        print(f"[DRIVER] Params loaded for {job_type}", flush=True)

        # Load model, paths, and binds from the params_data dict
        params = TsCtfParams(**params_data["job_model"])
        paths = {k: Path(v) for k, v in params_data["paths"].items()}
        additional_binds = params_data["additional_binds"]
        
        # Use 'paths' dict for all assets
        input_assets = paths
        output_assets = paths

        print("[DRIVER] Parameters loaded successfully", flush=True)
        print("[DRIVER] Received paths:", flush=True)
        for key, path in paths.items():
            print(f"  {key}: {path}", flush=True)

        # Validate input files
        if not input_assets["input_star"].exists():
            raise FileNotFoundError(f"Input STAR file not found: {input_assets['input_star']}")

        # Copy settings and tomostar from previous job
        print("[DRIVER] Copying settings and metadata from alignment job...", flush=True)
        copy_previous_metadata(input_assets, output_assets)

        # Build and execute WarpTools commands
        print("[DRIVER] Building WarpTools commands...", flush=True)
        container_service = get_container_service()

        # Command 1: Check defocus handness
        check_hand_command = build_check_defocus_hand_command(params, input_assets)
        print(f"[DRIVER] Command 1: {check_hand_command}", flush=True)

        # Command 2: Set defocus handness
        set_hand_command = build_set_defocus_hand_command(params, input_assets)
        print(f"[DRIVER] Command 2: {set_hand_command}", flush=True)

        # Command 3: Run CTF determination
        ctf_command = build_ctf_command(params, input_assets)
        print(f"[DRIVER] Command 3: {ctf_command}", flush=True)

        # Execute commands in container
        print("[DRIVER] Executing container commands...", flush=True)

        for i, command in enumerate([check_hand_command, set_hand_command, ctf_command], 1):
            print(f"[DRIVER] Executing command {i}/3...", flush=True)

            wrapped_command = container_service.wrap_command_for_tool(
                command=command, 
                cwd=job_dir, 
                tool_name=params.get_tool_name(), 
                additional_binds=additional_binds
            )
            
            # Use the robust run_command function
            run_command(wrapped_command, cwd=job_dir)
            
            print(f"[DRIVER] Command {i} completed successfully", flush=True)

        print("[DRIVER] Computation finished. Starting metadata processing.", flush=True)

        # Update metadata
        metadata_service = MetadataTranslator(StarfileService())
        result = metadata_service.update_ts_ctf_metadata(
            job_dir=job_dir,
            input_star_path=input_assets["input_star"],
            output_star_path=output_assets["output_star"],
            warp_folder="warp_tiltseries",
        )

        if not result["success"]:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.", flush=True)
        print("[DRIVER] Job finished successfully.", flush=True)

        # Create success sentinel
        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
        print("Creating RELION_JOB_EXIT_SUCCESS", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)

        # Create failure sentinel
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        print("Creating RELION_JOB_EXIT_FAILURE", file=sys.stderr, flush=True)
        sys.exit(1)


def copy_previous_metadata(input_assets: Dict[str, Path], output_assets: Dict[str, Path]):
    """Copy settings and tomostar from alignment job"""
    import shutil

    # This logic relies on 'input_assets' having the paths from the *previous* job.
    # The 'param_generator' and 'resolve_job_paths' must provide this correctly.
    
    # 'frameseries_dir' is the output 'warp_dir' of the *previous* job
    # 'warp_settings' is the output 'warp_settings' of the *previous* job
    
    prev_settings = input_assets["warp_settings_in"]
    prev_tomostar = input_assets["tomostar_dir_in"]
    prev_warp_dir = input_assets["frameseries_dir"] # This is the input warp_dir

    # Copy warp_tiltseries settings
    if prev_settings.exists():
        shutil.copy2(prev_settings, output_assets["warp_settings"])
        print(f"[DRIVER] Copied settings: {prev_settings} -> {output_assets['warp_settings']}", flush=True)
    else:
        print(f"[DRIVER] WARNING: Previous settings not found at {prev_settings}", flush=True)


    # Copy tomostar directory
    if prev_tomostar.exists():
        if output_assets["tomostar_dir"].exists():
            shutil.rmtree(output_assets["tomostar_dir"])
        shutil.copytree(prev_tomostar, output_assets["tomostar_dir"])
        print(f"[DRIVER] Copied tomostar: {prev_tomostar} -> {output_assets['tomostar_dir']}", flush=True)
    else:
        print(f"[DRIVER] WARNING: Previous tomostar not found at {prev_tomostar}", flush=True)


    # Copy existing warp_tiltseries XML files
    if prev_warp_dir.exists():
        if output_assets["warp_dir"].exists():
            shutil.rmtree(output_assets["warp_dir"])
        shutil.copytree(prev_warp_dir, output_assets["warp_dir"])
        print(f"[DRIVER] Copied warp directory: {prev_warp_dir} -> {output_assets['warp_dir']}", flush=True)
    else:
        print(f"[DRIVER] WARNING: Previous warp dir not found at {prev_warp_dir}", flush=True)


def build_check_defocus_hand_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    """Build command to check defocus handness"""
    # Use the *output* settings file path for this job
    settings_file = shlex.quote(str(input_assets['warp_settings']))
    return f"WarpTools ts_defocus_hand --settings {settings_file} --check"


def build_set_defocus_hand_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    """Build command to set defocus handness"""
    settings_file = shlex.quote(str(input_assets['warp_settings']))
    return f"WarpTools ts_defocus_hand --settings {settings_file} --{params.defocus_hand}"


def build_ctf_command(params: TsCtfParams, input_assets: Dict[str, Path]) -> str:
    """Build command for CTF determination"""
    settings_file = shlex.quote(str(input_assets['warp_settings']))
    return (
        f"WarpTools ts_ctf "
        f"--settings {settings_file} "
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
    main()
