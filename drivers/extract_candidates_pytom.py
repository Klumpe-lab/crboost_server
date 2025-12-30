#!/usr/bin/env python3
"""
Driver for PyTOM candidate extraction from template matching results.
"""

import sys
import os
import json
import shutil
import starfile
import pandas as pd
import numpy as np
import traceback
import subprocess
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.container_service import get_container_service


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


def absolutize_star_paths(input_star: Path, output_star: Path, base_path: Path):
    """
    Reads a star file and rewrites it with absolute paths.
    Paths are resolved relative to base_path (usually project root).
    """
    print(f"[DRIVER] Absolutizing paths in {input_star.name}...", flush=True)
    
    data = starfile.read(input_star, always_dict=True)
    
    for block_name, block_data in data.items():
        if not isinstance(block_data, pd.DataFrame):
            continue
            
        for col in PATH_COLUMNS:
            if col not in block_data.columns:
                continue
                
            def make_absolute(p):
                if pd.isna(p) or p == "" or p is None:
                    return p
                path = Path(p)
                if path.is_absolute():
                    return str(path)
                # Resolve relative to base_path
                abs_path = (base_path / path).resolve()
                return str(abs_path)
            
            block_data[col] = block_data[col].apply(make_absolute)
            print(f"  Absolutized column: {col}", flush=True)
    
    starfile.write(data, output_star, overwrite=True)
    print(f"[DRIVER] Wrote {output_star}", flush=True)


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


def cleanup_tomo_names(candidates_star: Path, apix: float):
    """
    Remove the pixel size suffix from rlnTomoName (e.g., '_12.00Apx' -> '').
    """
    try:
        data = starfile.read(candidates_star)
        if isinstance(data, dict):
            df = None
            for key, val in data.items():
                if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
                    df = val
                    break
            if df is None:
                return 0
        else:
            df = data

        suffix = f"_{apix:.2f}Apx"

        if "rlnTomoName" in df.columns:
            df["rlnTomoName"] = df["rlnTomoName"].str.replace(suffix, "", regex=False)

            if isinstance(data, dict):
                starfile.write(data, candidates_star, overwrite=True)
            else:
                starfile.write(df, candidates_star, overwrite=True)

            print(f"[DRIVER] Cleaned rlnTomoName suffix '{suffix}'")
            return len(df)
        return 0
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
        input_tm_job = paths["input_tm_job"]
        upstream_results = input_tm_job / "tmResults"

        if not upstream_results.exists():
            raise FileNotFoundError(f"Upstream tmResults not found at {upstream_results}")

        # --- 2. Find Tomograms Star ---
        input_tomograms = paths.get("input_tomograms")

        if not input_tomograms or not input_tomograms.exists():
            tm_tomos = input_tm_job / "tomograms.star"
            if tm_tomos.exists():
                input_tomograms = tm_tomos
            else:
                tm_job_star = input_tm_job / "job.star"
                if tm_job_star.exists():
                    tm_data = starfile.read(tm_job_star)
                    jobopts = tm_data.get("joboptions_values")
                    if jobopts is not None:
                        in_mic_row = jobopts[jobopts["rlnJobOptionVariable"] == "in_mic"]
                        if not in_mic_row.empty:
                            in_mic_path = in_mic_row["rlnJobOptionValue"].values[0]
                            if in_mic_path and (project_path / in_mic_path).exists():
                                input_tomograms = project_path / in_mic_path

        if not input_tomograms or not input_tomograms.exists():
            raise FileNotFoundError("Could not locate input tomograms.star")

        print(f"[DRIVER] Input tomograms: {input_tomograms}", flush=True)

        # --- 3. Determine Pixel Size ---
        apix = None
        if params.apix_score_map != "auto":
            apix = float(params.apix_score_map)
        else:
            apix = get_pixel_size_from_star(input_tomograms)

        if apix is None:
            apix = state.microscope.pixel_size_angstrom * 8.0
            print(f"[WARN] Using fallback pixel size: {apix} A/px")

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
        absolutize_star_paths(input_tomograms, output_tomograms, project_path)

        # Create optimisation_set.star with ABSOLUTE paths
        opt_df = pd.DataFrame({
            "rlnTomoParticlesFile": [str(candidates_star.resolve())],
            "rlnTomoTomogramsFile": [str(output_tomograms.resolve())]
        })
        starfile.write(opt_df, job_dir / "optimisation_set.star", overwrite=True)
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
