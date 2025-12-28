#!/usr/bin/env python3
"""
Driver for PyTOM candidate extraction from template matching results.

Reads score maps and job files from upstream template matching,
extracts particle candidates, and produces RELION-compatible output.
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


def get_pixel_size_from_star(tomograms_star: Path) -> float:
    """
    Extract pixel size from tomograms.star metadata.
    Returns the binned pixel size (rlnTomoTiltSeriesPixelSize * rlnTomoTomogramBinning).
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
    This matches CryoBoost's behavior in pytomExtractCandidates.updateMetaData().
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

        # Build the suffix pattern
        suffix = f"_{apix:.2f}Apx"

        if "rlnTomoName" in df.columns:
            df["rlnTomoName"] = df["rlnTomoName"].str.replace(suffix, "", regex=False)

            # Write back
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


def write_imod_models(
    candidates_star: Path, output_dir: Path, diameter_ang: float, apix: float, tomo_size: tuple = None
):
    """
    Generate IMOD .mod files for visualization (optional, requires point2model).
    """
    try:
        data = starfile.read(candidates_star)
        if isinstance(data, dict):
            df = list(data.values())[0]
        else:
            df = data

        if df.empty:
            return

        vis_dir = output_dir / "vis" / "imodCenter"
        vis_dir.mkdir(parents=True, exist_ok=True)

        radius_pix = int(diameter_ang / (2.0 * apix))

        for tomo_name in df["rlnTomoName"].unique():
            tomo_df = df[df["rlnTomoName"] == tomo_name]

            # Convert centered Angstrom coords to IMOD pixel coords
            if "rlnCenteredCoordinateXAngst" in tomo_df.columns and tomo_size:
                coords_ang = tomo_df[
                    ["rlnCenteredCoordinateXAngst", "rlnCenteredCoordinateYAngst", "rlnCenteredCoordinateZAngst"]
                ].values
                # Convert: pixel = (angstrom / apix) + (tomo_size / 2)
                coords = coords_ang / apix
                coords[:, 0] += tomo_size[0] / 2
                coords[:, 1] += tomo_size[1] / 2
                coords[:, 2] += tomo_size[2] / 2
            elif "rlnCoordinateX" in tomo_df.columns:
                coords = tomo_df[["rlnCoordinateX", "rlnCoordinateY", "rlnCoordinateZ"]].values
            else:
                print(f"[WARN] No usable coordinates for IMOD model: {tomo_name}")
                continue

            txt_file = vis_dir / f"coords_{tomo_name}.txt"
            mod_file = vis_dir / f"coords_{tomo_name}.mod"

            np.savetxt(txt_file, coords, delimiter="\t", fmt="%.0f")

            # Try to create IMOD model (may not be available)
            try:
                cmd = f"point2model {txt_file} {mod_file} -sphere {radius_pix} -scat -color 0,255,0"
                subprocess.run(cmd, shell=True, check=False, capture_output=True)
            except Exception:
                pass

    except Exception as e:
        print(f"[WARN] Could not generate IMOD models: {e}")


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

        # --- 2. Find/Copy Tomograms Star ---
        input_tomograms = paths.get("input_tomograms")

        if not input_tomograms or not input_tomograms.exists():
            # Check if TM job has it
            tm_tomos = input_tm_job / "tomograms.star"
            if tm_tomos.exists():
                input_tomograms = tm_tomos
            else:
                # Try to find from job.star in TM job
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

        # --- 4. Setup Local tmResults (copy JSON with patched output_dir, symlink rest) ---
        local_tm_results = job_dir / "tmResults"
        local_tm_results.mkdir(exist_ok=True)

        print("[DRIVER] Preparing upstream score maps...", flush=True)
        linked_count = 0
        for f in upstream_results.iterdir():
            target = local_tm_results / f.name
            if target.exists():
                continue

            if f.suffix == ".json":
                # COPY and patch output_dir (like original CryoBoost)
                with open(f, "r") as src:
                    data = json.load(src)
                data["output_dir"] = str(local_tm_results)
                with open(target, "w") as dst:
                    json.dump(data, dst, indent=4)
                print(f"  [COPY+PATCH] {f.name}")
            else:
                # Symlink .mrc, .npy files
                os.symlink(f.resolve(), target)

            linked_count += 1

        print(f"[DRIVER] Prepared {linked_count} files from upstream", flush=True)

        if linked_count == 0:
            raise RuntimeError("No files found in upstream tmResults - did template matching complete?")

        # --- 5. Build Extraction Command ---
        base_cmd = [
            "pytom_extract_candidates.py",
            "-n",
            str(params.max_num_particles),
            "--particle-diameter",
            str(params.particle_diameter_ang),
            "--relion5-compat",
            "--log",
            "debug",
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
            raise RuntimeError("No *_job.json files found - cannot run extraction")

        print(f"[DRIVER] Processing {len(job_files)} score maps...", flush=True)

        for i, job_json in enumerate(job_files, 1):
            print(f"  --> [{i}/{len(job_files)}] {job_json.name}", flush=True)

            cmd = base_cmd.copy()
            cmd.extend(["-j", str(job_json)])

            wrapped = container_service.wrap_command_for_tool(
                " ".join(cmd), cwd=job_dir, tool_name="pytom", additional_binds=additional_binds
            )
            run_command(wrapped, cwd=job_dir)

        # --- 7. Collect Results (copy if single, merge if multiple) ---
        print("[DRIVER] Collecting particle lists...", flush=True)

        candidates_star = job_dir / "candidates.star"
        star_files = list(local_tm_results.glob("*_particles.star"))

        if len(star_files) == 0:
            raise RuntimeError("No *_particles.star files found - extraction may have failed")
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
            raise RuntimeError("candidates.star was not created - extraction failed")

        # Clean up rlnTomoName (remove pixel size suffix) and get particle count
        n_particles = cleanup_tomo_names(candidates_star, apix)
        print(f"[DRIVER] Extracted {n_particles} particles", flush=True)

        # Copy tomograms.star to job output
        output_tomograms = job_dir / "tomograms.star"
        if not output_tomograms.exists():
            shutil.copy(input_tomograms, output_tomograms)

        # Create optimisation_set.star with RELATIVE paths (RELION compatibility)
        candidates_rel = candidates_star.relative_to(project_path)
        tomograms_rel = output_tomograms.relative_to(project_path)

        opt_df = pd.DataFrame(
            {"rlnTomoParticlesFile": [str(candidates_rel)], "rlnTomoTomogramsFile": [str(tomograms_rel)]}
        )
        starfile.write(opt_df, job_dir / "optimisation_set.star", overwrite=True)
        print("[DRIVER] Created optimisation_set.star", flush=True)

        # Optional: Generate IMOD visualization models (skip if no tomo size info)
        # write_imod_models(candidates_star, job_dir, params.particle_diameter_ang, apix)

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
