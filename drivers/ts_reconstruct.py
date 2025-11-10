#!/usr/bin/env python3
import json
import shlex
import sys
import os
import argparse
import subprocess
import shutil
from pathlib import Path
import traceback

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from services.metadata_service import MetadataTranslator
    from services.parameter_models import TsReconstructParams
    from services.starfile_service import StarfileService
    from services.container_service import get_container_service
except ImportError as e:
    print(f"FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def get_driver_context():
    """
    Parses args, finds paths, and ensures job_params.json exists.
    """
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--job_type", required=True, help="JobType string")
    parser.add_argument("--project_path", required=True, type=Path, help="Project root")
    
    args, unknown = parser.parse_known_args()
    
    job_type = args.job_type
    project_path = args.project_path.resolve()
    job_dir = Path.cwd().resolve()
    params_file = job_dir / "job_params.json"
    
    if not params_file.exists():
        print(f"[DRIVER] Generating job_params.json...", file=sys.stderr)
        try:
            job_number = int(job_dir.name.replace('job', ''))
            current_server_root = Path(sys.argv[0]).parent.parent.resolve()
            param_generator_script = current_server_root / "services" / "param_generator.py"
            python_exe = sys.executable
            
            cmd = [
                str(python_exe),
                str(param_generator_script),
                "--job_type", job_type,
                "--project_path", str(project_path),
                "--job_number", str(job_number),
                "--output_file", str(params_file)
            ]
            
            env = os.environ.copy()
            if str(current_server_root) not in env.get("PYTHONPATH", ""):
                env["PYTHONPATH"] = f"{current_server_root}{os.pathsep}{env.get('PYTHONPATH', '')}"

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=job_dir, env=env)

            if result.returncode != 0:
                print(f"[DRIVER] Param generator failed:", file=sys.stderr)
                print(result.stderr, file=sys.stderr)
                raise Exception(f"param_generator.py failed with exit code {result.returncode}")
            
            if not params_file.exists():
                raise Exception(f"param_generator.py didn't create {params_file}")
            
            print(f"[DRIVER] Generated {params_file}", file=sys.stderr)

        except Exception as e:
            print(f"[DRIVER] FATAL: Could not generate job_params.json: {e}", file=sys.stderr)
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            sys.exit(1)
    
    with open(params_file, 'r') as f:
        params_data = json.load(f)

    return params_data, job_dir, project_path, job_type


def run_command(command: str, cwd: Path):
    """Helper to run a shell command, stream output, and check for errors."""
    process = subprocess.Popen(command, shell=True, cwd=cwd, text=True, 
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
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


def copy_previous_metadata(input_assets: dict, output_assets: dict):
    """Copy settings and metadata from CTF job"""
    
    prev_settings = input_assets["warp_settings_in"]
    prev_tomostar = input_assets["tomostar_dir_in"]
    prev_warp_dir = input_assets["warp_dir_in"]

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


def build_reconstruct_command(params: TsReconstructParams, paths: dict) -> str:
    """Build WarpTools ts_reconstruct command"""
    settings_file = shlex.quote(str(paths['warp_settings']))
    
    return (
        f"WarpTools ts_reconstruct "
        f"--settings {settings_file} "
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
        params_data, job_dir, project_path, job_type = get_driver_context()
    except Exception as e:
        job_dir = Path.cwd()
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    print(f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}", flush=True)
    print(f"CWD (Job Directory): {job_dir}", flush=True)
    print("[DRIVER] tsReconstruct driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        print(f"[DRIVER] Params loaded for {job_type}", flush=True)

        # Load parameters
        params = TsReconstructParams(**params_data["job_model"])
        paths = {k: Path(v) for k, v in params_data["paths"].items()}
        additional_binds = params_data["additional_binds"]
        
        print("[DRIVER] Parameters loaded successfully", flush=True)
        print("[DRIVER] Received paths:", flush=True)
        for key, path in paths.items():
            print(f"  {key}: {path}", flush=True)

        # Validate input
        if not paths["input_star"].exists():
            raise FileNotFoundError(f"Input STAR file not found: {paths['input_star']}")

        # Copy metadata from previous job
        print("[DRIVER] Copying metadata from CTF job...", flush=True)
        copy_previous_metadata(paths, paths)

        # Build and execute reconstruction command
        print("[DRIVER] Building WarpTools command...", flush=True)
        reconstruct_command = build_reconstruct_command(params, paths)
        print(f"[DRIVER] Command: {reconstruct_command}", flush=True)

        container_service = get_container_service()
        wrapped_command = container_service.wrap_command_for_tool(
            command=reconstruct_command,
            cwd=job_dir,
            tool_name=params.get_tool_name(),
            additional_binds=additional_binds
        )
        
        print("[DRIVER] Executing container command...", flush=True)
        run_command(wrapped_command, cwd=job_dir)

        # Update metadata
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