#!/usr/bin/env python3
import sys
import os
import shutil
import pandas as pd
import starfile
import traceback
import subprocess
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.container_service import get_container_service

def generate_aux_files(job_dir: Path, tilt_series_star: Path, tomo_name: str) -> bool:
    """
    Generates .tlt (angles), defocus.txt, and dose.txt for PyTOM.
    Reads metadata from the upstream tilt_series.star.
    """
    aux_dirs = {
        "tilt": job_dir / "tiltAngleFiles",
        "defocus": job_dir / "defocusFiles",
        "dose": job_dir / "doseFiles"
    }
    for d in aux_dirs.values():
        d.mkdir(exist_ok=True)

    try:
        # We read the star file to find rows belonging to this tomogram
        # Note: In a highly optimized version, we'd read this ONCE outside the loop, 
        # but reading per tomo is safer for memory if the star file is massive.
        ts_df = starfile.read(tilt_series_star)
        if isinstance(ts_df, dict):
            # Handle RELION split blocks, usually we want the data block
            ts_df = list(ts_df.values())[0]
            
        # Filter for the specific tomogram
        subset = ts_df[ts_df["rlnTomoName"] == tomo_name]
        
        if subset.empty:
            print(f"[WARN] No tilt series metadata found for {tomo_name}")
            return False

        # 1. Tilt Angles (.tlt) - PyTOM needs just the numbers, new line separated
        col_tilt = "rlnTomoYTilt" if "rlnTomoYTilt" in subset.columns else "rlnTomoNominalStageTiltAngle"
        subset[col_tilt].to_csv(aux_dirs["tilt"] / f"{tomo_name}.tlt", index=False, header=False)

        # 2. Defocus (.txt) - Legacy code divided by 10000.0 (Angstrom -> microns/10?)
        # PyTOM usually expects microns. Check if your old code divided by 10000.
        if "rlnDefocusU" in subset.columns:
            defocus_vals = subset["rlnDefocusU"] / 10000.0
            defocus_vals.to_csv(aux_dirs["defocus"] / f"{tomo_name}.txt", index=False, header=False)

        # 3. Dose (.txt)
        if "rlnMicrographPreExposure" in subset.columns:
            subset["rlnMicrographPreExposure"].to_csv(aux_dirs["dose"] / f"{tomo_name}.txt", index=False, header=False)
            
        return True

    except Exception as e:
        print(f"[ERROR] Aux file generation failed for {tomo_name}: {e}")
        return False

def get_gpu_split(requested_split: str) -> list:
    """Parses the '4:4:2' string into list arguments for PyTOM."""
    if requested_split in ["auto", "None", ""]:
        # Default fallback if auto (assuming 24GB+ VRAM cards for modern setups)
        return ['2', '2', '1']
    return requested_split.split(":")

def main():
    print("--- SLURM JOB START (Template Matching) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Unpack Resolved Paths
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = context["additional_binds"]

        input_star_tomos = paths["input_tomograms"]  # List of tomograms (from Denoise/Reconstruct)
        input_star_ts = paths["input_tiltseries"]    # Metadata (from CtfFind)
        
        # THESE ARE THE PATHS FROM YOUR UI FILE PICKER
        template_file = paths["template_path"]
        mask_file = paths["mask_path"]

        # 2. Validation
        if not input_star_tomos.exists():
            raise FileNotFoundError(f"Input tomograms STAR missing: {input_star_tomos}")
        if not template_file.exists():
            raise FileNotFoundError(f"Template file missing: {template_file}")

        # 3. Setup Output
        tm_results_dir = job_dir / "tmResults"
        tm_results_dir.mkdir(exist_ok=True)

        # 4. Construct Base Command
        # We determine GPU ID from Slurm environment or default to 0
        gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")
        
        base_cmd = [
            "pytom_match_template.py",
            "-t", str(template_file),
            "-d", str(tm_results_dir),
            "-m", str(mask_file),
            "--angular-search", str(params.angular_search),
            "--voltage", str(params.voltage),
            "--spherical-aberration", str(params.spherical_aberration),
            "--amplitude-contrast", str(params.amplitude_contrast),
            "--tomogram-ctf-model", "phase-flip",
            "--per-tilt-weighting",
            "--log", "debug",
            "-g"
        ] + gpu_ids

        # Optional flags
        if params.gpu_split != "None":
            base_cmd.extend(["-s"] + get_gpu_split(params.gpu_split))
        
        if params.spectral_whitening: base_cmd.append("--spectral-whitening")
        if params.random_phase_correction: base_cmd.append("--random-phase-correction")
        if params.non_spherical_mask: base_cmd.append("--non-spherical-mask")
        
        if params.bandpass_filter != "None" and ":" in params.bandpass_filter:
            low, high = params.bandpass_filter.split(":")
            base_cmd.extend(["--low-pass", low, "--high-pass", high])

        if params.symmetry != "C1":
            if params.symmetry.startswith("C"):
                base_cmd.extend(["--z-axis-rotational-symmetry", params.symmetry[1:]])

        # 5. Iteration
        tomo_df = starfile.read(input_star_tomos)
        if isinstance(tomo_df, dict): tomo_df = list(tomo_df.values())[0]

        container_service = get_container_service()

        # ### HANDLING EXTERNAL TEMPLATES ###
        # The template might be in /data/templates/ref.mrc, while project is in /data/project.
        # We must explicitly bind the parent folders of the template and mask.
        additional_binds.append(str(template_file.parent.resolve()))
        if mask_file.exists():
            additional_binds.append(str(mask_file.parent.resolve()))
        # Deduplicate binds
        additional_binds = list(set(additional_binds))

        for _, row in tomo_df.iterrows():
            tomo_path = Path(row["rlnTomoReconstructedTomogram"])
            tomo_name = row["rlnTomoName"]

            print(f"[DRIVER] Processing {tomo_name}...", flush=True)

            # Generate local aux files (angles, defocus, dose)
            files_ok = generate_aux_files(job_dir, input_star_ts, tomo_name)
            if not files_ok:
                print(f"[SKIP] Metadata generation failed for {tomo_name}")
                continue

            # Complete the command for this specific tomogram
            cmd = base_cmd.copy()
            cmd.extend(["-v", str(tomo_path)])
            cmd.extend(["--tilt-angles", str(job_dir / "tiltAngleFiles" / f"{tomo_name}.tlt")])

            if params.defocus_weight:
                cmd.extend(["--defocus", str(job_dir / "defocusFiles" / f"{tomo_name}.txt")])
            if params.dose_weight:
                cmd.extend(["--dose-accumulation", str(job_dir / "doseFiles" / f"{tomo_name}.txt")])

            # Wrap for container
            cmd_str = " ".join(cmd)
            wrapped_cmd = container_service.wrap_command_for_tool(
                cmd_str, 
                cwd=job_dir, 
                tool_name="pytom", 
                additional_binds=additional_binds
            )
            
            run_command(wrapped_cmd, cwd=job_dir)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)

if __name__ == "__main__":
    main()