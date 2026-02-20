#!/usr/bin/env python3
import shlex
import sys
import os
from pathlib import Path
from typing import Dict
import traceback

from services.computing.container_service import get_container_service
from services.configs.metadata_service import MetadataTranslator
from services.configs.starfile_service import StarfileService

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context, run_command
    from services.project_state import TsCtfParams
except ImportError as e:
    print("FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def build_ctf_commands(params: TsCtfParams, paths: dict[str, Path]) -> str:
    settings_file = shlex.quote(str(paths["warp_tiltseries_settings"]))
    input_processing = shlex.quote(str(paths["input_processing"]))
    output_processing = shlex.quote(str(paths["output_processing"]))

    # Step 1: copy job003 XMLs into job004 so the flip is written to the right place
    # and ts_ctf reads the already-flipped XMLs from job004.
    #
    # Sequence:
    #   copy job003/*.xml → job004/
    #   ts_defocus_hand --output_processing job004   (no --input_processing: reads from
    #                                                 settings ProcessingFolder=job003,
    #                                                 writes AreAnglesInverted to job004)
    #   ts_ctf --input_processing job004             (reads flipped XMLs from job004,
    #          --output_processing job004             writes CTF results to job004)
    #
    # This matches the GT workflow. Without the copy, ts_defocus_hand has nothing to
    # write a flip into at job004, and ts_ctf reads unflipped XMLs from job003.

    copy_step = f"mkdir -p {output_processing} && cp {input_processing}/*.xml {output_processing}/"

    check_hand_command = (
        f"WarpTools ts_defocus_hand "
        f"--settings {settings_file} "
        f"--output_processing {output_processing} "
        f"--check"
    )
    set_flip_cmd = (
        f"WarpTools ts_defocus_hand "
        f"--settings {settings_file} "
        f"--output_processing {output_processing} "
        f"--set_flip"
    )
    set_noflip_cmd = (
        f"WarpTools ts_defocus_hand "
        f"--settings {settings_file} "
        f"--output_processing {output_processing} "
        f"--set_noflip"
    )
    ctf_command = (
        f"WarpTools ts_ctf "
        f"--settings {settings_file} "
        f"--input_processing {output_processing} "
        f"--output_processing {output_processing} "
        f"--window {params.window} "
        f"--range_low {params.range_min} "
        f"--range_high {params.range_max} "
        f"--defocus_min {params.defocus_min} "
        f"--defocus_max {params.defocus_max} "
        f"--voltage {int(round(params.voltage))} "
        f"--cs {params.spherical_aberration} "
        f"--amplitude {params.amplitude_contrast} "
        f"--perdevice {params.perdevice}"
    )

    if params.defocus_hand == "auto":
        hand_step = (
            f"hand_output=$({check_hand_command} 2>&1); "
            f'echo "$hand_output"; '
            f"if echo \"$hand_output\" | grep -q \"should be set to 'flip'\"; then "
            f"  {set_flip_cmd}; "
            f"else "
            f"  {set_noflip_cmd}; "
            f"fi"
        )
    elif params.defocus_hand == "set_flip":
        hand_step = " && ".join([check_hand_command, set_flip_cmd])
    else:
        hand_step = " && ".join([check_hand_command, set_noflip_cmd])

    return " && ".join([copy_step, hand_step, ctf_command])


def main():
    """Main driver function for tsCTF job"""
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context()

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
        print(f"[DRIVER] Params loaded for {job_type}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        if not paths["input_star"].exists():
            raise FileNotFoundError(f"Input STAR file not found: {paths['input_star']}")

        print("[DRIVER] Using WarpTools input_processing/output_processing for data flow", flush=True)

        print("[DRIVER] Building WarpTools commands...", flush=True)
        ctf_command_str = build_ctf_commands(params, paths)

        print("[DRIVER] Executing container command...", flush=True)
        container_service = get_container_service()
        wrapped_command = container_service.wrap_command_for_tool(
            command=ctf_command_str, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
        )
        run_command(wrapped_command, cwd=job_dir)
        print("[DRIVER] CTF processing completed successfully", flush=True)

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

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
