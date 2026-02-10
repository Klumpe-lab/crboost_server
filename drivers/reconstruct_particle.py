#!/usr/bin/env python3
"""
Driver for RELION subtomogram reconstruction (relion_tomo_reconstruct_particle).

Takes an optimisation_set.star (from extraction) and produces:
  - merged.mrc  (initial average from all particles)
  - half1.mrc, half2.mrc  (independent half-maps for FSC)

This is the first STA step after extraction: it creates the reference
volume that Class3D / Refine3D use downstream.
"""

import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service


def main():
    print("--- SLURM JOB START (Reconstruct Particle) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = list(context.get("additional_binds", []))

        input_optimisation = paths["input_optimisation"]
        if not input_optimisation.exists():
            raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")

        print(f"[DRIVER] Input optimisation_set: {input_optimisation}", flush=True)
        print(f"[DRIVER] Job dir: {job_dir}", flush=True)

        # Build command
        cmd_parts = [
            "relion_tomo_reconstruct_particle",
            "--i", str(input_optimisation),
            "--o", str(job_dir) + "/",
            "--b", str(params.box_size),
            "--sym", str(params.symmetry),
            "--j", str(params.threads),
            "--j_in", str(params.threads_in),
            "--j_out", str(params.threads_out),
        ]

        if params.crop_size > 0:
            cmd_parts.extend(["--crop", str(params.crop_size)])

        if params.binning > 1:
            cmd_parts.extend(["--bin", str(params.binning)])

        if params.whiten:
            cmd_parts.append("--whiten")

        if params.no_ctf:
            cmd_parts.append("--no_ctf")

        cmd_str = " ".join(cmd_parts)
        print(f"[DRIVER] Command: {cmd_str}", flush=True)

        container_service = get_container_service()
        additional_binds.append(str(input_optimisation.parent.resolve()))
        additional_binds = list(set(additional_binds))

        wrapped_cmd = container_service.wrap_command_for_tool(
            cmd_str, cwd=job_dir, tool_name="relion", additional_binds=additional_binds
        )

        run_command(wrapped_cmd, cwd=job_dir)

        # Verify primary output exists
        expected_merged = job_dir / "merged.mrc"
        if not expected_merged.exists():
            raise RuntimeError(
                f"Expected output merged.mrc not found in {job_dir}. "
                f"Check run.out for relion_tomo_reconstruct_particle errors."
            )

        print(f"[DRIVER] Output merged.mrc: {expected_merged}", flush=True)

        # Log half-map presence (not fatal if missing, but good to know)
        for hm in ["half1.mrc", "half2.mrc"]:
            hm_path = job_dir / hm
            if hm_path.exists():
                print(f"[DRIVER] Output {hm}: {hm_path}", flush=True)
            else:
                print(f"[DRIVER] WARN: {hm} not found (may use different naming)", flush=True)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()