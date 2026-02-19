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
    # -------------------------------------------------------------------------
    # DEFOCUS HANDEDNESS: WHY THIS DEFAULTS TO set_flip
    #
    # Defocus hand describes the sign of the relationship between tilt angle and
    # defocus: for the Krios at CBE, set_flip is correct (rlnTomoHand = -1).
    #
    # We debugged this against a ground-truth run (klumpe/run12) on identical
    # data. GT used set_flip and got a decisive check correlation of -0.958.
    # Our auto-detection gave weakly positive correlations (0.394, then 0.111
    # after fixing an upstream alignment bug), causing it to incorrectly select
    # set_noflip, which produced scattered defocus estimates and wrong hand in
    # the output STAR. The upstream bug was --patches 2x2 --axis_iter 1 in
    # ts_aretomo causing AreAnglesInverted="True" in the alignment XML, which
    # corrupted the check. Even after fixing alignment, the check remained
    # unreliable (weak positive signal), so we hardcode set_flip for now.
    #
    # IDEAL BEHAVIOR: defocus_hand should default to "auto" in TsCtfParams,
    # with the UI exposing set_flip / set_noflip / auto as a dropdown. Auto
    # works correctly when upstream alignment is clean and the check correlation
    # is decisive (|r| > ~0.7). The current hardcode should be revisited once
    # we have multiple tomograms and reliable alignment to validate against.
    # -------------------------------------------------------------------------

    settings_file = shlex.quote(str(paths["warp_tiltseries_settings"]))
    input_processing = shlex.quote(str(paths["input_processing"]))
    output_processing = shlex.quote(str(paths["output_processing"]))

    check_hand_command = (
        f"WarpTools ts_defocus_hand --settings {settings_file} --input_processing {input_processing} --check"
    )

    set_flip_cmd = (
        f"WarpTools ts_defocus_hand --settings {settings_file} --input_processing {input_processing} --set_flip"
    )

    set_noflip_cmd = (
        f"WarpTools ts_defocus_hand --settings {settings_file} --input_processing {input_processing} --set_noflip"
    )

    ctf_command = (
        f"WarpTools ts_ctf "
        f"--settings {settings_file} "
        f"--input_processing {input_processing} "
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
        auto_hand_script = (
            f"hand_output=$({check_hand_command} 2>&1); "
            f'echo "$hand_output"; '
            f'if echo "$hand_output" | grep -q "should be set to \'flip\'"; then '
            f"  {set_flip_cmd}; "
            f"else "
            f"  {set_noflip_cmd}; "
            f"fi"
        )
        return f"{auto_hand_script} && {ctf_command}"
    elif params.defocus_hand == "set_flip":
        return " && ".join([check_hand_command, set_flip_cmd, ctf_command])
    else:
        return " && ".join([check_hand_command, set_noflip_cmd, ctf_command])


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
