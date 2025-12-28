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
    Reads metadata from the per-tilt-series star file (referenced from main star).
    """
    aux_dirs = {
        "tilt": job_dir / "tiltAngleFiles",
        "defocus": job_dir / "defocusFiles",
        "dose": job_dir / "doseFiles"
    }
    for d in aux_dirs.values():
        d.mkdir(exist_ok=True)

    try:
        # 1. Read the MAIN star file to find the per-tilt-series star file path
        main_data = starfile.read(tilt_series_star)
        
        # Handle different starfile return formats
        if isinstance(main_data, dict):
            # Find the data block (could be 'global', '', or named)
            main_df = None
            for key in ['global', '', 'data_']:
                if key in main_data:
                    main_df = main_data[key]
                    break
            if main_df is None:
                main_df = list(main_data.values())[0]
        else:
            main_df = main_data
        
        # Find the row for this tomogram
        tomo_row = main_df[main_df["rlnTomoName"] == tomo_name]
        if tomo_row.empty:
            print(f"[WARN] Tomogram {tomo_name} not found in {tilt_series_star}")
            return False
        
        # Get the per-tilt-series star file path (relative to main star's directory)
        per_ts_star_rel = tomo_row["rlnTomoTiltSeriesStarFile"].values[0]
        per_ts_star = tilt_series_star.parent / per_ts_star_rel
        
        if not per_ts_star.exists():
            print(f"[WARN] Per-tilt-series star file not found: {per_ts_star}")
            return False

        # 2. Read the PER-TILT-SERIES star file
        per_tilt_data = starfile.read(per_ts_star)
        
        if isinstance(per_tilt_data, dict):
            # The block name is usually the tomo name itself
            per_tilt_df = None
            for key, val in per_tilt_data.items():
                if isinstance(val, pd.DataFrame) and not val.empty:
                    per_tilt_df = val
                    break
            if per_tilt_df is None:
                print(f"[WARN] No data block found in {per_ts_star}")
                return False
        else:
            per_tilt_df = per_tilt_data

        # 3. Write aux files from per-tilt data
        
        # Tilt Angles - prefer rlnTomoYTilt (refined), fallback to rlnTomoNominalStageTiltAngle
        if "rlnTomoYTilt" in per_tilt_df.columns:
            tilt_col = "rlnTomoYTilt"
        elif "rlnTomoNominalStageTiltAngle" in per_tilt_df.columns:
            tilt_col = "rlnTomoNominalStageTiltAngle"
        else:
            print(f"[WARN] No tilt angle column found in {per_ts_star}")
            return False
        
        per_tilt_df[tilt_col].to_csv(
            aux_dirs["tilt"] / f"{tomo_name}.tlt", 
            index=False, header=False
        )

        # Defocus (convert from Angstrom to microns / 10 for PyTOM)
        if "rlnDefocusU" in per_tilt_df.columns:
            defocus_vals = per_tilt_df["rlnDefocusU"] / 10000.0
            defocus_vals.to_csv(
                aux_dirs["defocus"] / f"{tomo_name}.txt", 
                index=False, header=False
            )

        # Dose accumulation
        if "rlnMicrographPreExposure" in per_tilt_df.columns:
            per_tilt_df["rlnMicrographPreExposure"].to_csv(
                aux_dirs["dose"] / f"{tomo_name}.txt", 
                index=False, header=False
            )
            
        print(f"[DRIVER] Generated aux files for {tomo_name} ({len(per_tilt_df)} tilts)")
        return True

    except Exception as e:
        import traceback
        print(f"[ERROR] Aux file generation failed for {tomo_name}: {e}")
        traceback.print_exc()
        return False

def get_gpu_split(requested_split: str) -> list:
    """Parses the '4:4:2' string into list arguments for PyTOM."""
    if requested_split in ["auto", "None", ""]:
        # Default fallback if auto (assuming 24GB+ VRAM cards for modern setups)
        return ['2', '2', '1']
    return requested_split.split(":")

def main():
    import os  
    os.environ['TQDM_DISABLE'] = '1'
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
            if not tomo_path.is_absolute():
                tomo_path = project_path / tomo_path
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