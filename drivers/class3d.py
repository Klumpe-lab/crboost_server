#!/usr/bin/env python3
"""
Driver for RELION 3D Classification (relion_refine without --auto_refine).

Uses --ios to read tomo optimisation_set and --ref for the reference map.
Produces run_optimisation_set.star and per-iteration class volumes.

Two typical use cases:
1. K=1 single-class alignment -> clean reference for mask creation
2. K>1 multi-class sorting -> particle selection
"""

import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service


def main():
    print("--- SLURM JOB START (Class3D) ---", flush=True)

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
        input_reference = paths["input_reference"]

        if not input_optimisation.exists():
            raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")
        if not input_reference.exists():
            raise FileNotFoundError(f"Input reference map not found: {input_reference}")

        print(f"[DRIVER] Input optimisation_set: {input_optimisation}", flush=True)
        print(f"[DRIVER] Input reference: {input_reference}", flush=True)
        print(f"[DRIVER] Job dir: {job_dir}", flush=True)

        # Output rootname: all RELION outputs will be prefixed with "run_"
        output_root = str(job_dir / "run")

        cmd_parts = [
            "relion_refine",
            "--ios", str(input_optimisation),
            "--ref", str(input_reference),
            "--o", output_root,
            "--K", str(params.n_classes),
            "--iter", str(params.n_iterations),
            "--healpix_order", str(params.healpix_order),
            "--offset_range", str(params.offset_range),
            "--offset_step", str(params.offset_step),
            "--sym", str(params.symmetry),
            "--j", str(params.threads),
        ]

        cmd_parts.append("--trust_ref_size")
        if params.ini_high > 0:
            cmd_parts.extend(["--ini_high", str(params.ini_high)])

        if params.particle_diameter > 0:
            cmd_parts.extend(["--particle_diameter", str(params.particle_diameter)])

        if params.tau_fudge > 0:
            cmd_parts.extend(["--tau2_fudge", str(params.tau_fudge)])

        if params.sigma_ang > 0:
            cmd_parts.extend(["--sigma_ang", str(params.sigma_ang)])

        if params.flatten_solvent:
            cmd_parts.append("--flatten_solvent")

        if params.firstiter_cc:
            cmd_parts.append("--firstiter_cc")

        if params.preread_images:
            cmd_parts.append("--preread_images")

        # Optional mask
        solvent_mask = getattr(params, "solvent_mask_path", "")
        if solvent_mask and solvent_mask.strip():
            mask_path = Path(solvent_mask)
            if not mask_path.exists():
                raise FileNotFoundError(f"Solvent mask not found: {mask_path}")
            cmd_parts.extend(["--solvent_mask", str(mask_path)])
            additional_binds.append(str(mask_path.parent.resolve()))

        # GPU
        if params.use_gpu:
            cmd_parts.append("--gpu")

        cmd_str = " ".join(cmd_parts)
        print(f"[DRIVER] Command: {cmd_str}", flush=True)

        container_service = get_container_service()
        additional_binds.append(str(input_optimisation.parent.resolve()))
        additional_binds.append(str(input_reference.parent.resolve()))
        additional_binds = list(set(additional_binds))

        wrapped_cmd = container_service.wrap_command_for_tool(
            cmd_str, cwd=job_dir, tool_name="relion", additional_binds=additional_binds
        )

        run_command(wrapped_cmd, cwd=job_dir)

        # Verify output
        expected_optset = job_dir / "run_optimisation_set.star"
        if not expected_optset.exists():
            raise RuntimeError(
                f"Expected output run_optimisation_set.star not found in {job_dir}. "
                f"Check run.out for relion_refine errors."
            )
        print(f"[DRIVER] Output optimisation_set: {expected_optset}", flush=True)

        # Log class volumes found
        class_maps = sorted(job_dir.glob("run_it*_class*.mrc"))
        if class_maps:
            last_iter_maps = [m for m in class_maps if m.stem.startswith(f"run_it{params.n_iterations:03d}")]
            if not last_iter_maps:
                last_iter_maps = class_maps[-params.n_classes:]
            for cm in last_iter_maps:
                print(f"[DRIVER] Class volume: {cm}", flush=True)
        else:
            print("[DRIVER] WARN: No class volumes found", flush=True)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()