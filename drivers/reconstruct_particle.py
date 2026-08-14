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

from drivers.driver_base import ToolCommand, get_driver_context, run_tool
from services.job_models import ReconstructParticleParams


def main():
    print("--- SLURM JOB START (Reconstruct Particle) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(ReconstructParticleParams)
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
        cmd = (
            ToolCommand("relion_tomo_reconstruct_particle")
            .opt_path("--i", input_optimisation, quote=False)
            .opt_path("--o", f"{job_dir}/", quote=False)
            .opt("--b", params.box_size)
            .opt("--sym", params.symmetry.value if hasattr(params.symmetry, 'value') else params.symmetry)
            .opt("--j", params.threads)
            .opt("--j_in", params.threads_in)
            .opt("--j_out", params.threads_out)
        )

        if params.crop_size > 0:
            cmd.opt("--crop", params.crop_size)

        if params.binning > 1:
            cmd.opt("--bin", params.binning)

        if params.whiten:
            cmd.flag("--whiten")

        if params.no_ctf:
            cmd.flag("--no_ctf")

        # SNR override (0 = let RELION auto-determine).
        if getattr(params, "snr", 0.0) and params.snr > 0:
            cmd.opt("--snr", params.snr)

        # Backend theme: RELION 5 default vs. legacy RELION 4 ("classic").
        theme_val = getattr(params, "theme", None)
        if theme_val is not None:
            theme_str = theme_val.value if hasattr(theme_val, "value") else str(theme_val)
            if theme_str and theme_str != "default":
                cmd.opt("--theme", theme_str)

        # Helical params: only emitted when explicitly enabled.
        if getattr(params, "do_helix", False):
            cmd.flag("--helix")
            if params.helical_twist != -1.0:
                cmd.opt("--helical_twist", params.helical_twist)
            if params.helical_rise:
                cmd.opt("--helical_rise", params.helical_rise)
            if params.helical_z_percentage:
                cmd.opt("--helical_z_percentage", params.helical_z_percentage)
            if params.helical_tube_outer_diameter > 0:
                cmd.opt("--helical_tube_outer_diameter", params.helical_tube_outer_diameter)
            if params.helical_nr_asu and params.helical_nr_asu > 1:
                cmd.opt("--helical_nr_asu", params.helical_nr_asu)

        # Free-form passthrough (appended verbatim, not shell-escaped).
        extra = (getattr(params, "other_args", "") or "").strip()
        if extra:
            cmd.raw(extra)

        print(f"[DRIVER] Command: {cmd}", flush=True)

        additional_binds.append(str(input_optimisation.parent.resolve()))

        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

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