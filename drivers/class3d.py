#!/usr/bin/env python3
"""
Driver for RELION 3D Classification (relion_refine without --auto_refine).
"""

import shutil
import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import ToolCommand, get_driver_context, run_tool
from services.job_models import Class3DParams


def main():
    print("--- SLURM JOB START (Class3D) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(Class3DParams)
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

        output_root = str(job_dir / "run")

        cmd = (
            ToolCommand("relion_refine")
            .opt_path("--ios", input_optimisation, quote=False)
            .opt_path("--ref", input_reference, quote=False)
            .opt_path("--o", output_root, quote=False)
            .opt("--K", params.n_classes)
            .opt("--iter", params.n_iterations)
            .opt("--healpix_order", params.healpix_order)
            .opt("--offset_range", params.offset_range)
            .opt("--offset_step", params.offset_step)
            .opt("--oversampling", params.oversampling)
            .opt("--sym", params.symmetry.value if hasattr(params.symmetry, "value") else params.symmetry)
            .opt("--j", params.threads)
            .opt("--pool", params.pool)
            .opt("--pad", params.pad)
            .flag("--trust_ref_size")
        )

        if params.ini_high > 0:
            cmd.opt("--ini_high", params.ini_high)
        if params.particle_diameter > 0:
            cmd.opt("--particle_diameter", params.particle_diameter)
        if params.tau_fudge > 0:
            cmd.opt("--tau2_fudge", params.tau_fudge)
        if params.sigma_ang > 0:
            cmd.opt("--sigma_ang", params.sigma_ang)

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

        solvent_mask = getattr(params, "solvent_mask_path", "")
        if solvent_mask and solvent_mask.strip():
            mask_path = Path(solvent_mask)
            if not mask_path.exists():
                raise FileNotFoundError(f"Solvent mask not found: {mask_path}")
            cmd.opt_path("--solvent_mask", mask_path, quote=False)
            additional_binds.append(str(mask_path.parent.resolve()))

        if params.use_gpu:
            cmd.flag("--gpu")

        print(f"[DRIVER] Command: {cmd}", flush=True)

        additional_binds.append(str(input_optimisation.parent.resolve()))
        additional_binds.append(str(input_reference.parent.resolve()))

        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

        # relion_refine for Class3D (fixed --iter N, no --auto_refine) writes its
        # outputs with the iteration number: run_it{N}_optimisation_set.star -- there
        # is NO bare run_optimisation_set.star (that name is an auto-refine/Refine3D
        # convention). The numbered file is the real completion signal; checking only
        # the bare name false-failed every Class3D run at the very end despite all
        # iterations completing. Check the numbered file, then mirror it to the
        # un-numbered name the OUTPUT_SCHEMA path_template + downstream resolver expect.
        final_optset = job_dir / f"run_it{params.n_iterations:03d}_optimisation_set.star"
        expected_optset = job_dir / "run_optimisation_set.star"
        if not final_optset.exists():
            raise RuntimeError(
                f"Expected output {final_optset.name} not found in {job_dir}. "
                f"Check run.out for relion_refine errors."
            )
        if not expected_optset.exists():
            shutil.copy2(final_optset, expected_optset)
        print(f"[DRIVER] Output optimisation_set: {expected_optset}", flush=True)

        class_maps = sorted(job_dir.glob("run_it*_class*.mrc"))
        if class_maps:
            last_iter_maps = [m for m in class_maps if m.stem.startswith(f"run_it{params.n_iterations:03d}")]
            if not last_iter_maps:
                last_iter_maps = class_maps[-params.n_classes :]
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
