#!/usr/bin/env python3
import sys
import os
import glob
import starfile
import pandas as pd
import traceback
import shutil
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.container_service import get_container_service

def main():
    print("--- SLURM JOB START (Extract Candidates) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = context["additional_binds"]

        # Upstream Inputs
        input_tm_job = paths["input_tm_job"]
        upstream_results = input_tm_job / "tmResults"

        if not upstream_results.exists():
            raise FileNotFoundError(f"Upstream tmResults not found at {upstream_results}")

        # 1. Setup Local Environment
        # PyTOM extraction works best when the json/mrc files are local or explicitly pointed to.
        # We will symlink the upstream results into a local 'tmResults' folder to keep the job self-contained.
        local_tm_results = job_dir / "tmResults"
        local_tm_results.mkdir(exist_ok=True)

        print("[DRIVER] Symlinking upstream score maps...", flush=True)
        linked_count = 0
        for ext in ["*.json", "*.mrc"]:
            for f in upstream_results.glob(ext):
                target = local_tm_results / f.name
                if not target.exists():
                    os.symlink(f, target)
                    linked_count += 1
        
        if linked_count == 0:
            print("[WARN] No JSON/MRC files found to link. Did the matching job produce output?")

        # 2. Determine Score Map Pixel Size (for Radius Calc)
        # In `project_state.py`, if `apix_score_map` is "auto", we need to guess.
        # Usually it's: TS_PixelSize * TS_Binning. 
        # For simplicity in this driver, we assume the user provides it or we fallback to state.pixel_size * 10 (binning)
        apix = 10.0 
        if params.apix_score_map != "auto":
            apix = float(params.apix_score_map)
        else:
            # Try to infer from global state (often ~1.35 * 8 binning ~ 10.8)
            # This is a heuristic. Ideally, read the mrc header of one score map.
            apix = state.microscope.pixel_size_angstrom * 8.0 

        # Radius in pixels = (Diameter / 2) / AngstromPerPixel
        radius_pix = int((params.particle_diameter_ang / 2.0) / apix)
        if radius_pix < 1: radius_pix = 1

        print(f"[DRIVER] Calculated extraction radius: {radius_pix} px (based on {apix} A/px)", flush=True)

        # 3. Build Extraction Command
        base_cmd = [
            "pytom_extract_candidates.py",
            "-n", str(params.max_num_particles),
            "-r", str(radius_pix),
            "--relion5-compat",
            "--log", "debug"
        ]

        if params.cutoff_method == "NumberOfFalsePositives":
            base_cmd.extend(["--number-of-false-positives", str(params.cutoff_value)])
        elif params.cutoff_method == "ManualCutOff":
            base_cmd.extend(["-c", str(params.cutoff_value)])

        if params.score_filter_method == "tophat":
            base_cmd.append("--tophat-filter")
            if params.score_filter_value != "None":
                # Expecting format "connectivity:bins" e.g., "1:10"
                conn, bins = params.score_filter_value.split(":")
                base_cmd.extend(["--tophat-connectivity", conn, "--tophat-bins", bins])

        # 4. Run Extraction Loop
        container_service = get_container_service()
        job_files = list(local_tm_results.glob("*_job.json"))

        print(f"[DRIVER] Processing {len(job_files)} score maps...", flush=True)
        for job_json in job_files:
            cmd = base_cmd.copy()
            cmd.extend(["-j", str(job_json)])
            
            wrapped = container_service.wrap_command_for_tool(
                " ".join(cmd), 
                cwd=job_dir, 
                tool_name="pytom", 
                additional_binds=additional_binds
            )
            run_command(wrapped, cwd=job_dir)

        # 5. Merge Results into candidates.star
        print("[DRIVER] Merging results...", flush=True)
        merge_cmd = [
            "pytom_merge_stars.py",
            "-i", str(local_tm_results),
            "-o", str(job_dir / "candidates.star")
        ]
        wrapped_merge = container_service.wrap_command_for_tool(
            " ".join(merge_cmd),
            cwd=job_dir,
            tool_name="pytom",
            additional_binds=additional_binds
        )
        run_command(wrapped_merge, cwd=job_dir)

        # 6. Generate Optimisation Set (Relion Compatibility)
        candidates_star = job_dir / "candidates.star"
        if candidates_star.exists():
            # We also need the tomograms star file path
            tomo_star = paths["input_tomograms"]
            
            opt_df = pd.DataFrame({
                'rlnTomoParticlesFile': [str(candidates_star)],
                'rlnTomoTomogramsFile': [str(tomo_star)]
            })
            starfile.write(opt_df, job_dir / "optimisation_set.star")
            print("[DRIVER] optimisation_set.star created.", flush=True)
        else:
            print("[WARN] candidates.star was not created.")

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)

if __name__ == "__main__":
    main()