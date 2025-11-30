#!/usr/bin/env python3
import shlex
import sys
import os
from pathlib import Path
from typing import Dict
import traceback
import starfile
import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context, run_command
    from services.project_state import DenoisePredictParams
    from services.container_service import get_container_service
except ImportError as e:
    print("FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def main():
    """Main driver function for Denoise Predict job"""
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    try:
        (
            project_state,
            params,  # DenoisePredictParams
            local_params_data,
            job_dir,
            project_path,
            job_type,
        ) = get_driver_context()
        
    except Exception as e:
        job_dir = Path.cwd()
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(
        f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}",
        flush=True,
    )
    print(f"CWD (Job Directory): {job_dir}", flush=True)
    print("[DRIVER] Denoise Predict driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths and binds
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        # 2. Check inputs
        if not paths["model_path"].exists():
             raise FileNotFoundError(f"Denoising model not found at: {paths['model_path']}")
             
        # Resolve Input Star (Tomograms) if not correct in paths
        input_star = paths.get("input_star")
        if not input_star or not input_star.exists():
             # Try fallback
             fallback_star = project_path / "External" / "tsReconstruct" / "tomograms.star"
             if fallback_star.exists():
                 input_star = fallback_star
             else:
                 raise FileNotFoundError(f"Input Tomograms STAR not found. Checked {paths.get('input_star')} and {fallback_star}")

        # 3. Untar model
        print("[DRIVER] Extracting model...", flush=True)
        run_command(f"tar -xzf {paths['model_path']}", cwd=job_dir)
        model_dir_name = "denoising_model" # Assumed from tar content in Train driver

        # 4. Prepare Configuration
        # Need to read tomograms list again
        tomo_df = starfile.read(input_star)
        if isinstance(tomo_df, dict):
            tomo_df = list(tomo_df.values())[0]

        # Filter if specific tomograms requested (though Params defaults to empty=all)
        # Note: Params logic in project_state might be strict on 'denoising_tomo_name'
        target_tomos = []
        output_dir = job_dir / "denoised"
        output_dir.mkdir(exist_ok=True)
        
        filter_str = params.denoising_tomo_name
        
        # Prepare list for output star file
        output_star_data = []

        for idx, row in tomo_df.iterrows():
            tomo_path = row["rlnTomoReconstructedTomogram"]
            if filter_str and filter_str not in str(tomo_path):
                continue
            
            full_path = project_path / tomo_path if not Path(tomo_path).is_absolute() else Path(tomo_path)
            tomo_name = Path(tomo_path).name
            denoised_path = output_dir / tomo_name
            
            # Create config for THIS tomogram (CryoCARE predict usually takes one config per run or a list)
            # To handle them one by one allows for better error handling per tomo, or we can batch.
            # Let's batch if possible, or loop. cryoCARE_predict takes a config with a path.
            
            # Config structure for prediction
            predict_config = {
                "path": str(model_dir_name),
                "even": str(full_path), # Predict on the full tomogram (or even/odd if merging later)
                # Note: If we want to denoise the 'final' tomo, we pass it here.
                # If inputs were split, we might need logic for that. Assuming standard single-file input.
                "n_tiles": [params.ntiles_z, params.ntiles_y, params.ntiles_x],
                "output": str(denoised_path),
                "gpu_id": 0
            }
            
            config_name = f"predict_config_{idx}.json"
            import json
            with open(job_dir / config_name, 'w') as f:
                json.dump(predict_config, f, indent=4)
                
            print(f"[DRIVER] Denoising {tomo_name}...", flush=True)
            
            container_service = get_container_service()
            predict_cmd = f"cryoCARE_predict.py --conf {config_name}"
            
            wrapped_predict = container_service.wrap_command_for_tool(
                command=predict_cmd,
                cwd=job_dir,
                tool_name="cryocare",
                additional_binds=additional_binds
            )
            run_command(wrapped_predict, cwd=job_dir)
            
            # Add to output star data
            new_row = row.copy()
            # Update the path to point to the new denoised tomogram
            # Relion expects paths relative to project dir usually
            rel_path = denoised_path.relative_to(project_path)
            new_row["rlnTomoReconstructedTomogram"] = str(rel_path)
            output_star_data.append(new_row)

        # 5. Write Output STAR
        print("[DRIVER] Writing output STAR file...", flush=True)
        if output_star_data:
            out_df = pd.DataFrame(output_star_data)
            starfile.write(out_df, job_dir / "tomograms.star")
        else:
            print("[WARN] No tomograms processed, output STAR is empty.")

        print("[DRIVER] Job finished successfully.", flush=True)

        # 6. Create success sentinel
        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()