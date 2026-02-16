#!/usr/bin/env python3
"""
Driver for PyTOM candidate extraction from template matching results.
"""

import sys
import os
import json
import shutil
from drivers.subtomo_merge import write_optimisation_set
import starfile
import pandas as pd
import numpy as np
import traceback
import subprocess
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service


# Columns in star files that contain file paths
PATH_COLUMNS = [
    "rlnTomoTiltSeriesStarFile",
    "rlnTomoReconstructedTomogram",
    "rlnTomoReconstructedTomogramHalf1",
    "rlnTomoReconstructedTomogramHalf2",
    "rlnTomoReconstructedTomogramDenoised",
    "rlnTomoParticlesFile",
    "rlnTomoTomogramsFile",
]



def get_pixel_size_from_star(tomograms_star: Path) -> float:
    """
    Extract pixel size from tomograms.star metadata.
    """
    try:
        data = starfile.read(tomograms_star)
        if isinstance(data, dict):
            df = list(data.values())[0]
        else:
            df = data

        ts_pixs = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0])
        binning = float(df.get("rlnTomoTomogramBinning", pd.Series([1])).iloc[0])

        return ts_pixs * binning
    except Exception as e:
        print(f"[WARN] Could not read pixel size from {tomograms_star}: {e}")
        return None


def cleanup_tomo_names(candidates_star: Path, apix_fallback: float) -> int:
    """
    Remove the pixel size suffix from rlnTomoName.
    Reads pixel size from the star file itself if available, falls back to provided value.
    """
    try:
        data = starfile.read(candidates_star, always_dict=True)
        # Find the particles block
        df = None
        for key, val in data.items():
            if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
                df = val
                break
        if df is None:
            return 0

        # Determine pixel size: prefer what's in the file (matches pytom's naming)
        if "rlnTomoTiltSeriesPixelSize" in df.columns and "rlnTomoTomogramBinning" in df.columns:
            apix = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0]) * float(df["rlnTomoTomogramBinning"].iloc[0])
        elif "rlnTomoTiltSeriesPixelSize" in df.columns:
            apix = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0])
        else:
            apix = apix_fallback

        suffix = f"_{apix:.2f}Apx"
        df["rlnTomoName"] = df["rlnTomoName"].str.replace(suffix, "", regex=False)
        starfile.write(data, candidates_star, overwrite=True)
        print(f"[DRIVER] Cleaned rlnTomoName suffix '{suffix}' from {len(df)} particles")
        return len(df)
    except Exception as e:
        print(f"[WARN] Could not clean tomo names: {e}")
        return 0


def main():
    os.environ["TQDM_DISABLE"] = "1"
    print("--- SLURM JOB START (Extract Candidates) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = context["additional_binds"]

        # --- 1. Locate Upstream Template Matching Results ---
        upstream_results = paths["input_tm_job"]  # This IS the tmResults dir

        if not upstream_results.exists():
            raise FileNotFoundError(f"Upstream tmResults not found at {upstream_results}")

        # --- 2. Find Tomograms Star ---
        input_tomograms = paths.get("input_tomograms")
        if not input_tomograms or not input_tomograms.exists():
            raise FileNotFoundError(
                f"Input tomograms.star not found at {input_tomograms}. "
                "Check that template matching completed successfully."
            )

        # --- 3. Determine Pixel Size ---
        apix = None
        if params.apix_score_map != "auto":
            apix = float(params.apix_score_map)
        else:
            apix = get_pixel_size_from_star(input_tomograms)

        if apix is None:
            # Try to compute from reconstruction parameters
            rec_job = state.jobs.get(JobType.TS_RECONSTRUCT)
            if rec_job and hasattr(rec_job, 'rescale_angpixs'):
                apix = rec_job.rescale_angpixs
                print(f"[WARN] Using pixel size from TsReconstruct params: {apix} A/px")
            else:
                raise RuntimeError(
                    "Could not determine score map pixel size. "
                    "Set apix_score_map explicitly or check tomograms.star."
                )

        print(f"[DRIVER] Score map pixel size: {apix:.2f} A/px", flush=True)
        print(f"[DRIVER] Particle diameter: {params.particle_diameter_ang} A", flush=True)

        # --- 4. Setup Local tmResults ---
        local_tm_results = job_dir / "tmResults"
        local_tm_results.mkdir(exist_ok=True)

        print("[DRIVER] Preparing upstream score maps...", flush=True)
        linked_count = 0
        for f in upstream_results.iterdir():
            target = local_tm_results / f.name
            if target.exists():
                continue

            if f.suffix == ".json":
                with open(f, "r") as src:
                    data = json.load(src)
                data["output_dir"] = str(local_tm_results)
                with open(target, "w") as dst:
                    json.dump(data, dst, indent=4)
                print(f"  [COPY+PATCH] {f.name}")
            else:
                os.symlink(f.resolve(), target)

            linked_count += 1

        print(f"[DRIVER] Prepared {linked_count} files from upstream", flush=True)

        if linked_count == 0:
            raise RuntimeError("No files found in upstream tmResults")

        # --- 5. Build Extraction Command ---
        base_cmd = [
            "pytom_extract_candidates.py",
            "-n", str(params.max_num_particles),
            "--particle-diameter", str(params.particle_diameter_ang),
            "--relion5-compat",
            "--log", "debug",
        ]

        if params.cutoff_method == "NumberOfFalsePositives":
            base_cmd.extend(["--number-of-false-positives", str(params.cutoff_value)])
        elif params.cutoff_method == "ManualCutOff":
            base_cmd.extend(["-c", str(params.cutoff_value)])

        if params.score_filter_method == "tophat":
            base_cmd.append("--tophat-filter")
            if params.score_filter_value != "None" and ":" in params.score_filter_value:
                conn, bins = params.score_filter_value.split(":")
                base_cmd.extend(["--tophat-connectivity", conn, "--tophat-bins", bins])

        # --- 6. Run Extraction Loop ---
        container_service = get_container_service()
        job_files = list(local_tm_results.glob("*_job.json"))

        if not job_files:
            raise RuntimeError("No *_job.json files found")

        print(f"[DRIVER] Processing {len(job_files)} score maps...", flush=True)

        for i, job_json in enumerate(job_files, 1):
            print(f"  --> [{i}/{len(job_files)}] {job_json.name}", flush=True)

            cmd = base_cmd.copy()
            cmd.extend(["-j", str(job_json)])

            wrapped = container_service.wrap_command_for_tool(
                " ".join(cmd), cwd=job_dir, tool_name="pytom", additional_binds=additional_binds
            )
            run_command(wrapped, cwd=job_dir)

        # --- 7. Collect Results ---
        print("[DRIVER] Collecting particle lists...", flush=True)

        candidates_star = job_dir / "candidates.star"
        star_files = list(local_tm_results.glob("*_particles.star"))

        if len(star_files) == 0:
            raise RuntimeError("No *_particles.star files found")
        elif len(star_files) == 1:
            print(f"[DRIVER] Single tomogram - copying {star_files[0].name}")
            shutil.copy(star_files[0], candidates_star)
        else:
            print(f"[DRIVER] Merging {len(star_files)} particle lists...")
            merge_cmd = ["pytom_merge_stars.py", "-i"]
            merge_cmd.extend([str(f) for f in star_files])
            merge_cmd.extend(["-o", str(candidates_star), "--relion5-compat"])

            wrapped_merge = container_service.wrap_command_for_tool(
                " ".join(merge_cmd), cwd=job_dir, tool_name="pytom", additional_binds=additional_binds
            )
            run_command(wrapped_merge, cwd=job_dir)

        # --- 8. Post-Processing ---
        if not candidates_star.exists():
            raise RuntimeError("candidates.star was not created")

        n_particles = cleanup_tomo_names(candidates_star, apix)
        print(f"[DRIVER] Extracted {n_particles} particles", flush=True)

        # Copy AND ABSOLUTIZE tomograms.star
        output_tomograms = job_dir / "tomograms.star"
        shutil.copy2(input_tomograms, output_tomograms)

        # Create optimisation_set.star with ABSOLUTE paths
        # opt_df = pd.DataFrame({
        #     "rlnTomoParticlesFile": [str(candidates_star.resolve())],
        #     "rlnTomoTomogramsFile": [str(output_tomograms.resolve())]
        # })
        # starfile.write(opt_df, job_dir / "optimisation_set.star", overwrite=True)
        write_optimisation_set(
            job_dir / "optimisation_set.star",
            particles_star=candidates_star,
            tomograms_star=output_tomograms,
        )
        print("[DRIVER] Created optimisation_set.star with absolute paths", flush=True)
        print("[DRIVER] Created optimisation_set.star with absolute paths", flush=True)

        # --- 9. Success ---
        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()
