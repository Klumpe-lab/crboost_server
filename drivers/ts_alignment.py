#!/usr/bin/env python
# drivers/ts_alignment.py
from services.metadata_service import MetadataTranslator
from services.starfile_service import StarfileService
from services.container_service import get_container_service  # Import for consistency
import json
import subprocess
import sys
import os
import shlex
import pandas as pd
import argparse
from pathlib import Path
import traceback

from services.project_state import AlignmentMethod, TsAlignmentParams

server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))


def get_driver_context():
    """
    Parses args, finds paths, and ensures job_params.json exists.
    Returns:
        - params_data (dict): The full, raw loaded JSON data.
        - job_dir (Path): The current job directory.
        - project_path (Path): The root project directory.
        - job_type (str): The job_type string.
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
        print("[DRIVER] Generating job_params.json...", file=sys.stderr)
        try:
            job_number = int(job_dir.name.replace("job", ""))
            current_server_root = Path(sys.argv[0]).parent.parent.resolve()
            param_generator_script = current_server_root / "services" / "param_generator.py"
            python_exe = sys.executable

            cmd = [
                str(python_exe),
                str(param_generator_script),
                "--job_type",
                job_type,
                "--project_path",
                str(project_path),
                "--job_number",
                str(job_number),
                "--output_file",
                str(params_file),  # NEW: Tell it where to write
            ]

            env = os.environ.copy()
            if str(current_server_root) not in env.get("PYTHONPATH", ""):
                env["PYTHONPATH"] = f"{current_server_root}{os.pathsep}{env.get('PYTHONPATH', '')}"

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=job_dir, env=env)

            if result.returncode != 0:
                print("[DRIVER] Param generator failed:", file=sys.stderr)
                print(result.stderr, file=sys.stderr)
                raise Exception(f"param_generator.py failed with exit code {result.returncode}")

            # Check that file was created
            if not params_file.exists():
                raise Exception(f"param_generator.py didn't create {params_file}")

            print(f"[DRIVER] Generated {params_file}", file=sys.stderr)

        except Exception as e:
            print(f"[DRIVER] FATAL: Could not generate job_params.json: {e}", file=sys.stderr)
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            sys.exit(1)

    # Read the file (whether it existed or was just created)
    with open(params_file, "r") as f:
        params_data = json.load(f)

    return params_data, job_dir, project_path, job_type


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


def build_alignment_commands(params: TsAlignmentParams, paths: dict[str, Path], num_tomograms: int) -> str:
    """Builds the multi-step WarpTools alignment command string matching old logic."""

    # Get gain path from params object
    gain_path_str = ""
    if params.gain_path and params.gain_path != "None":
        gain_path_str = shlex.quote(params.gain_path)

    gain_ops_str = params.gain_operations if params.gain_operations else ""

    # Paths from the JSON (are all absolute)
    mdoc_dir = shlex.quote(str(paths["mdoc_dir"]))
    frameseries_dir = shlex.quote(str(paths["frameseries_dir"]))
    tomostar_dir = shlex.quote(str(paths["tomostar_dir"]))
    processing_dir = shlex.quote(str(paths["warp_dir"]))
    settings_file = shlex.quote(str(paths["warp_settings"]))

    mkdir_cmds = [f"mkdir -p {tomostar_dir}", f"mkdir -p {processing_dir}"]

    # === Step 1: ts_import ===
    # OLD LOGIC: Use configurable mdoc pattern and tilt angle inversion
    cmd_parts_import = [
        "WarpTools ts_import",
        "--mdocs",
        mdoc_dir,
        "--pattern",
        shlex.quote(params.mdoc_pattern),  # Use configurable pattern
        "--frameseries",
        frameseries_dir,
        "--output",
        tomostar_dir,
        "--tilt_exposure",
        str(params.dose_per_tilt),
        "--override_axis",
        str(params.tilt_axis_angle),
    ]

    # OLD LOGIC: Inversion logic - old code seemed to always use --dont_invert
    # but let's use the parameter properly
    if not params.invert_tilt_angles:
        cmd_parts_import.append("--dont_invert")

    if params.do_at_most > 0:
        cmd_parts_import.extend(["--do_at_most", str(params.do_at_most)])

    # === Step 2: create_settings ===
    cmd_parts_settings = [
        "WarpTools create_settings",
        "--folder_data",
        tomostar_dir,
        "--extension '*.tomostar'",
        "--folder_processing",
        processing_dir,
        "--output",
        settings_file,
        "--angpix",
        str(params.pixel_size),
        "--exposure",
        str(params.dose_per_tilt),
        "--tomo_dimensions",
        params.tomo_dimensions,
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
            "--settings",
            settings_file,
            "--angpix",
            str(params.rescale_angpixs),
            "--alignz",
            str(int(params.thickness_nm * 10)),  # Convert nm to Å
            "--perdevice",
            str(params.perdevice),
        ]

        # OLD LOGIC: Only set patches if not zero
        if params.patch_x > 0 and params.patch_y > 0:
            cmd_parts_align.extend(["--patches", f"{params.patch_x}x{params.patch_y}"])

        # OLD LOGIC: Axis refinement with safety check
        if params.axis_iter > 0:
            # Safety check: batch size shouldn't exceed available tomograms
            batch_size = min(params.axis_batch, num_tomograms)
            cmd_parts_align.extend(["--axis_iter", str(params.axis_iter)])
            cmd_parts_align.extend(["--axis_batch", str(batch_size)])

    elif params.alignment_method == AlignmentMethod.IMOD:
        cmd_parts_align = [
            "WarpTools ts_etomo_patches",
            "--settings",
            settings_file,
            "--angpix",
            str(params.rescale_angpixs),
            "--patch_size",
            str(int(params.imod_patch_size * 10)),  # Convert nm to Å
        ]
    else:
        return f"echo 'ERROR: Alignment method {params.alignment_method} not implemented'; exit 1;"

    if params.do_at_most > 0:
        cmd_parts_align.extend(["--do_at_most", str(params.do_at_most)])

    # === Combine all commands ===
    return " && ".join(
        [" ".join(mkdir_cmds), " ".join(cmd_parts_import), " ".join(cmd_parts_settings), " ".join(cmd_parts_align)]
    )


def main():
    print("[DRIVER] tsAlignment driver started.", flush=True)

    # --- NEW BOOTSTRAP CALL ---
    try:
        # This function gets CWD=job_dir and ensures params exist
        params_data, job_dir, project_path, job_type = get_driver_context()
    except Exception as e:
        # Bootstrap failed, write failure file and exit
        job_dir = Path.cwd()  # Try to get CWD for failure file
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    # --- END BOOTSTRAP CALL ---

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load parameters (already done by bootstrap)
        print(f"[DRIVER] Params loaded for job type {job_type} in {job_dir}", flush=True)

        params = TsAlignmentParams(**params_data["job_model"])
        paths = {k: Path(v) for k, v in params_data["paths"].items()}
        additional_binds = params_data["additional_binds"]

        # Get I/O STAR files (already absolute)
        input_star_abs = paths["input_star"]
        output_star_abs = paths["output_star"]

        star_data = StarfileService().read(input_star_abs)
        tilt_series_df = star_data.get("global", pd.DataFrame())
        num_tomograms = len(tilt_series_df)
        print(f"[DRIVER] Found {num_tomograms} tomograms in input STAR file", flush=True)

        # 2. Build and run alignment commands with tomogram count
        align_command_str = build_alignment_commands(params, paths, num_tomograms)

        # 3. Get container service to build the *full apptainer* command
        container_svc = get_container_service()
        apptainer_command = container_svc.wrap_command_for_tool(
            command=align_command_str, cwd=job_dir, tool_name="warptools", additional_binds=additional_binds
        )

        # 4. Run the containerized computation
        print("[DRIVER] Executing container...", flush=True)
        run_command(apptainer_command, cwd=job_dir)

        # 5. Run metadata processing
        print("[DRIVER] Alignment finished. Starting metadata processing.", flush=True)
        translator = MetadataTranslator(StarfileService())

        result = translator.update_ts_alignment_metadata(
            job_dir=job_dir,
            input_star_path=input_star_abs,
            output_star_path=output_star_abs,
            tomo_dimensions=params.tomo_dimensions,
            project_root=project_path,
            alignment_method=params.alignment_method.value,
        )

        if not result["success"]:
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
