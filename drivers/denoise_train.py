#!/usr/bin/env python3
import sys
import os
from pathlib import Path
import traceback
import json
import starfile  # Requires pip install starfile

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context, run_command
    from services.container_service import get_container_service
except ImportError as e:
    print("FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def resolve_tomo_path(project_root: Path, star_path: Path, relative_path: str) -> Path:
    """
    Resolves a tomogram path found in a STAR file.
    1. Checks relative to Project Root.
    2. Checks relative to the STAR file's directory (common in chained jobs).
    """
    path_obj = Path(relative_path)
    if path_obj.is_absolute():
        return path_obj

    # 1. Try relative to Project Root (Standard Relion behavior)
    p1 = project_root / relative_path
    if p1.exists():
        return p1

    # 2. Try relative to the STAR file location (Specific Job behavior)
    p2 = star_path.parent / relative_path
    if p2.exists():
        return p2

    # Return p1 as default for error reporting if neither exists
    return p1


def derive_half_map_path(base_path: Path, half: str) -> Path:
    """
    Derives the path to 'even' or 'odd' half maps based on Warp directory structure.
    Warp Structure: .../reconstruction/tomoname.mrc
    Halves:         .../reconstruction/even/tomoname.mrc
    """
    # Check if we are in a 'reconstruction' folder
    if base_path.parent.name == "reconstruction":
        # Standard Warp: parent/even/file.mrc
        half_path = base_path.parent / half / base_path.name
        return half_path
    
    # Fallback: simple insertion (might fail if structure differs)
    return base_path.parent / half / base_path.name


def main():
    """Main driver function for Denoise Train job"""
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    try:
        (
            project_state,
            params,  # DenoiseTrainParams
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

    print(f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}", flush=True)
    print(f"CWD (Job Directory): {job_dir}", flush=True)
    print("[DRIVER] Denoise Train driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths and binds
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        input_star = paths["input_star"]

        if not input_star.exists():
            # Fallback logic handled in project_state, but double check here
            print(f"[WARN] Input STAR {input_star} not found. Searching...", flush=True)
            # Try finding it via global state mapping if available, or error out
            raise FileNotFoundError(f"Input STAR file not found: {input_star}")

        print(f"[DRIVER] Reading inputs from: {input_star}", flush=True)

        # 2. Parse Input STAR
        tomo_df = starfile.read(input_star)
        if isinstance(tomo_df, dict):
            # Handle Relion 3.1+ split dataframes, usually in 'tomograms' or default key
            tomo_df = list(tomo_df.values())[0]
            
        # 3. Filter Tomograms
        target_even = []
        target_odd = []
        
        filter_str = params.tomograms_for_training
        print(f"[DRIVER] Filtering tomograms with string: '{filter_str}'", flush=True)

        # Column name check (Relion 3 vs 4 vs Warp)
        col_name = "rlnTomoReconstructedTomogram"
        if col_name not in tomo_df.columns:
            # Fallback for Warp/Other namings
            possible_cols = [c for c in tomo_df.columns if "Name" in c or "Tomogram" in c]
            if possible_cols:
                col_name = possible_cols[0]
            else:
                raise ValueError("Could not determine tomogram path column in STAR file")

        found_count = 0
        
        for raw_path in tomo_df[col_name]:
            if filter_str in str(raw_path):
                # A. Resolve the base path (Full/Deconv map)
                full_path_resolved = resolve_tomo_path(project_path, input_star, str(raw_path))
                
                # B. Derive Even/Odd paths
                even_path = derive_half_map_path(full_path_resolved, "even")
                odd_path = derive_half_map_path(full_path_resolved, "odd")

                # C. Validate Existence
                if not even_path.exists() or not odd_path.exists():
                    print(f"[WARN] Half maps missing for {full_path_resolved.name}. Skipping.", flush=True)
                    print(f"       Expected: {even_path}", flush=True)
                    continue

                target_even.append(str(even_path))
                target_odd.append(str(odd_path))
                found_count += 1
        
        print(f"[DRIVER] Found {found_count} valid tomograms for training.", flush=True)
        
        if found_count == 0:
            raise ValueError(f"No valid even/odd tomogram pairs found matching '{filter_str}'")

        # 4. Construct JSON for cryoCARE
        config_json_path = job_dir / "train_config.json"
        
        # --- FIX: Reduced Learning Rate to prevent NaN ---
        safe_learning_rate = 0.0001  # Reduced from 0.0004
        
        train_config = {
            "even": target_even,
            "odd": target_odd,
            "patch_shape": [params.subvolume_dimensions] * 3,
            "num_slices": params.number_training_subvolumes,
            "split": 0.9,
            "tilt_axis": "Y", 
            "n_normalization_samples": 500,
            "path": str(job_dir / "train_data"),
            "train_data": str(job_dir / "train_data"), # Required for training step
            "model_name": "denoising_model",
            "epochs": 100,
            "steps_per_epoch": 200,
            "batch_size": 16,
            "unet_kern_size": 3,
            "unet_n_depth": 3,
            "unet_n_first": 16,
            "learning_rate": safe_learning_rate, 
            "gpu_id": 0
        }
        
        with open(config_json_path, 'w') as f:
            json.dump(train_config, f, indent=4)

        container_service = get_container_service()

        # 5. Execute Extraction
        # Only run if train_data folder is empty, to save time on restarts (Optional optimization)
        # But for safety, we run it.
        print("[DRIVER] Extracting training data...", flush=True)
        extract_cmd = f"cryoCARE_extract_train_data.py --conf {config_json_path.name}"
        
        wrapped_extract = container_service.wrap_command_for_tool(
            command=extract_cmd,
            cwd=job_dir,
            tool_name="cryocare",
            additional_binds=additional_binds
        )
        run_command(wrapped_extract, cwd=job_dir)

        # 6. Execute Training
        print("[DRIVER] Training model...", flush=True)
        train_cmd = f"cryoCARE_train.py --conf {config_json_path.name}"
        
        wrapped_train = container_service.wrap_command_for_tool(
            command=train_cmd,
            cwd=job_dir,
            tool_name="cryocare", 
            additional_binds=additional_binds
        )
        run_command(wrapped_train, cwd=job_dir)
        
        # 7. Archive Model
        print("[DRIVER] Archiving model...", flush=True)
        model_dir = job_dir / "denoising_model"
        
        # --- FIX: Fail if model was not created ---
        if model_dir.exists():
            run_command(f"tar -czf denoising_model.tar.gz denoising_model", cwd=job_dir)
        else:
            print("[FATAL] Model directory not found! Training likely failed.", file=sys.stderr, flush=True)
            raise RuntimeError("Training failed to produce a model directory.")

        print("[DRIVER] Job finished successfully.", flush=True)

        # 8. Create success sentinel
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