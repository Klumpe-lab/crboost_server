#!/usr/bin/env python3
"""
Driver for RELION subtomogram extraction (relion_tomo_subtomo).
Creates pseudo-subtomograms from tilt series for downstream STA.

Supports a merge_only mode: skips extraction and just merges
this job's existing outputs with additional_sources.
"""

import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from drivers.subtomo_merge import merge_optimisation_sets_into_jobdir
from services.computing.container_service import get_container_service


def run_extraction(params, context, job_dir):
    """Run relion_tomo_subtomo."""
    paths = {k: Path(v) for k, v in context["paths"].items()}
    additional_binds = context["additional_binds"]

    input_optimisation = paths["input_optimisation"]
    if not input_optimisation.exists():
        raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")

    print(f"[DRIVER] Input: {input_optimisation}", flush=True)
    print(f"[DRIVER] Output dir: {job_dir}", flush=True)

    cmd_parts = [
        "relion_tomo_subtomo",
        "--o",
        str(job_dir) + "/",
        "--i",
        str(input_optimisation),
        "--b",
        str(params.box_size),
        "--bin",
        str(int(params.binning)),
    ]

    if params.crop_size > 0:
        cmd_parts.extend(["--crop", str(params.crop_size)])
    if params.max_dose > 0:
        cmd_parts.extend(["--max_dose", str(params.max_dose)])
    if params.min_frames > 1:
        cmd_parts.extend(["--min_frames", str(params.min_frames)])
    if params.do_stack2d:
        cmd_parts.append("--stack2d")
    if params.do_float16:
        cmd_parts.append("--float16")

    cmd_str = " ".join(cmd_parts)
    print(f"[DRIVER] Command: {cmd_str}", flush=True)

    container_service = get_container_service()
    additional_binds.append(str(input_optimisation.parent.resolve()))
    additional_binds = list(set(additional_binds))

    wrapped_cmd = container_service.wrap_command_for_tool(
        cmd_str, cwd=job_dir, tool_name="relion", additional_binds=additional_binds
    )
    run_command(wrapped_cmd, cwd=job_dir)

    # Verify RELION produced outputs
    output_particles = job_dir / "particles.star"
    if not output_particles.exists():
        raise RuntimeError(f"Expected output not created: {output_particles}")

    output_optset = job_dir / "optimisation_set.star"
    if not output_optset.exists():
        raise RuntimeError(f"Expected optimisation_set.star not created: {output_optset}")

    print(f"[DRIVER] Extraction complete. particles.star and optimisation_set.star present.", flush=True)


def run_merge(params, job_dir):
    """Merge this job's outputs with additional sources."""
    additional_sources = params.additional_sources

    if not additional_sources:
        print("[DRIVER] No additional_sources specified, nothing to merge.", flush=True)
        return

    print(f"[DRIVER] Merging {len(additional_sources)} additional source(s)...", flush=True)
    for i, src in enumerate(additional_sources):
        print(f"  [{i + 1}] {src}", flush=True)

    summary = merge_optimisation_sets_into_jobdir(job_dir=job_dir, additional_sources=additional_sources)

    print(
        f"[DRIVER] Merge complete: {summary['totals']['n_particles']} particles, "
        f"{summary['totals']['n_tomograms']} tomograms",
        flush=True,
    )


def main():
    print("--- SLURM JOB START (Subtomogram Extraction) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        if params.merge_only:
            print("[DRIVER] merge_only=True, skipping extraction.", flush=True)
            # In merge_only mode, job_dir must already have outputs from a prior extraction run.
            if not (job_dir / "optimisation_set.star").exists():
                raise RuntimeError("merge_only=True but no optimisation_set.star in job dir. Run extraction first.")
            run_merge(params, job_dir)
        else:
            run_extraction(params, context, job_dir)
            # After extraction, optionally merge if sources were provided
            if params.additional_sources:
                run_merge(params, job_dir)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()
