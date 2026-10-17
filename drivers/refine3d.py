#!/usr/bin/env python3
"""
Driver for RELION gold-standard 3D refinement (relion_refine_mpi --auto_refine --split_random_halves).
"""

import sys
import traceback
from pathlib import Path

from drivers.driver_base import ToolCommand, get_driver_context, run_tool
from services.jobs.refine3d import Refine3DParams


def main():
    print("--- SLURM JOB START (Refine3D) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(Refine3DParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

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

        output_root = str(job_dir / "run")

        # Split random halves needs MPI: a leader plus one follower per half-set. The container runs with
        # --cleanenv and SLURM_* unset, so mpirun cannot see the allocation and counts physical cores as
        # slots; --oversubscribe lets three ranks start on a hyperthreaded 4-CPU allocation, --bind-to none
        # keeps them off a shared core.
        cmd = (
            ToolCommand("mpirun")
            .flag("--oversubscribe")
            .opt("--bind-to", "none")
            .opt("-n", params.mpi_ranks)
            .raw("relion_refine_mpi")
            .opt_path("--ios", input_optimisation, quote=False)
            .opt_path("--ref", input_reference, quote=False)
            .opt_path("--o", output_root, quote=False)
            .flag("--auto_refine")
            .flag("--split_random_halves")
            .opt("--healpix_order", params.healpix_order)
            .opt("--auto_local_healpix_order", params.auto_local_healpix_order)
            .opt("--offset_range", params.offset_range)
            .opt("--offset_step", params.offset_step)
            .opt("--oversampling", 1)
            .opt("--sym", params.symmetry.value if hasattr(params.symmetry, "value") else params.symmetry)
            .opt("--low_resol_join_halves", params.low_resol_join_halves)
            .opt("--j", params.threads)
            .opt("--pool", params.pool)
            .opt("--pad", params.pad)
            .flag("--trust_ref_size")
        )

        if params.ini_high > 0:
            cmd.opt("--ini_high", params.ini_high)
        if params.particle_diameter > 0:
            cmd.opt("--particle_diameter", params.particle_diameter)
        if params.random_seed >= 0:
            cmd.opt("--random_seed", params.random_seed)

        if params.do_ctf:
            cmd.flag("--ctf")
        if params.do_norm:
            cmd.flag("--norm")
        if params.do_scale:
            cmd.flag("--scale")
        if params.zero_mask:
            cmd.flag("--zero_mask")
        if params.dont_combine_weights_via_disc:
            cmd.flag("--dont_combine_weights_via_disc")
        if params.flatten_solvent:
            cmd.flag("--flatten_solvent")
        if params.firstiter_cc:
            cmd.flag("--firstiter_cc")
        if params.preread_images:
            cmd.flag("--preread_images")

        solvent_mask = params.solvent_mask_path.strip()
        if solvent_mask:
            mask_path = Path(solvent_mask)
            if not mask_path.exists():
                raise FileNotFoundError(f"Solvent mask not found: {mask_path}")
            cmd.opt_path("--solvent_mask", mask_path, quote=False)
            if params.solvent_correct_fsc:
                cmd.flag("--solvent_correct_fsc")
            additional_binds.append(str(mask_path.parent.resolve()))

        if params.use_gpu:
            cmd.flag("--gpu")

        print(f"[DRIVER] Command: {cmd}", flush=True)

        additional_binds.append(str(input_optimisation.parent.resolve()))
        additional_binds.append(str(input_reference.parent.resolve()))

        # RELION's progress bar ticks only every 1/60 of an expectation step, which runs for hours on tens of
        # thousands of tilt-series particles: the idle watchdog would kill a working refine.
        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds, idle_timeout=0)

        # Auto-refine writes its converged result without an iteration tag (RELION's MlOptimiser::write
        # with fn_root = fn_out); these are the OUTPUT_SCHEMA files plus the two half-maps.
        expected = [
            job_dir / "run_optimisation_set.star",
            job_dir / "run_class001.mrc",
            job_dir / "run_half1_class001_unfil.mrc",
            job_dir / "run_half2_class001_unfil.mrc",
        ]
        missing = [p.name for p in expected if not p.exists()]
        if missing:
            raise RuntimeError(
                f"relion_refine_mpi finished without the converged outputs {missing} in {job_dir}. "
                "Check run.out for relion_refine errors."
            )
        for p in expected:
            print(f"[DRIVER] Output: {p}", flush=True)

        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
