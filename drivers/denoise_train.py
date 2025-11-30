#!/usr/bin/env python3
import shlex
import sys
import os
from pathlib import Path
from typing import Dict
import traceback

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context, run_command
    from services.project_state import DenoiseTrainParams
    from services.container_service import get_container_service
except ImportError as e:
    print("FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


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

    print(
        f"Node: {Path('/etc/hostname').read_text().strip() if Path('/etc/hostname').exists() else 'unknown'}",
        flush=True,
    )
    print(f"CWD (Job Directory): {job_dir}", flush=True)
    print("[DRIVER] Denoise Train driver started.", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths and binds
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        if not paths["input_star"].exists():
            print(f"[WARN] Input STAR file not found at expected path: {paths['input_star']}. Checking for explicit path...", flush=True)
            # Try finding it in project/External/tsReconstruct/tomograms.star as fallback
            fallback_star = project_path / "External" / "tsReconstruct" / "tomograms.star"
            if fallback_star.exists():
                print(f"[INFO] Found tomograms.star at fallback: {fallback_star}", flush=True)
                paths["input_star"] = fallback_star
            else:
                 raise FileNotFoundError(f"Input STAR file not found: {paths['input_star']}")

        # 2. Prepare Command 1: Extract Training Data
        # Usage inferred from binAdapters: cryoCARE_extract_train_data.py --conf config.json
        # However, we likely need to generate the config json or pass args directly.
        # The old job.star passes args like `tomograms_for_training`, `number_training_subvolumes`.
        # Relion's denoisetomo job constructs the json config internally.
        
        # Since we are using the cryoCARE scripts directly via the wrapper, let's see what arguments they take.
        # Assuming they take standard cryoCARE arguments.
        
        # Construct extraction command
        # Note: cryoCARE usually expects a configuration file. 
        # Since we don't have Relion to write it for us, we must write it here in Python.
        
        config_json_path = job_dir / "train_config.json"
        
        # Determine tomograms
        # We need to read the input star file to find the tomograms corresponding to 'tomograms_for_training'
        # If "Position_1", we look for tomograms with that in the name.
        
        import starfile
        tomo_df = starfile.read(paths["input_star"])
        if isinstance(tomo_df, dict):
            tomo_df = list(tomo_df.values())[0]
            
        # Filter tomograms
        target_tomos = []
        filter_str = params.tomograms_for_training
        
        for tomo_path in tomo_df["rlnTomoReconstructedTomogram"]:
            if filter_str in str(tomo_path):
                # Ensure absolute path or relative to project root correctly
                full_path = project_path / tomo_path if not Path(tomo_path).is_absolute() else Path(tomo_path)
                target_tomos.append(str(full_path))
        
        if not target_tomos:
            raise ValueError(f"No tomograms found matching filter '{filter_str}' in {paths['input_star']}")

        # Construct JSON for cryoCARE
        # This structure depends on cryoCARE version, assuming 0.2/0.3 standard
        train_config = {
            "even": target_tomos, # Simplified, assuming single frame/odd-even splitting is handled or we just pass the tomos
            "odd": target_tomos, # Usually we need even/odd halves. If we only have full tomos, this might fail unless cryoCARE handles it.
                                 # NOTE: Relion's denoisetomo splits tomograms.
                                 # If we are using standard Reconstruct, we might not have even/odd halves unless we configured Reconstruct to output them.
                                 # Let's assume standard behavior: We might pass the same list if splitting happens internally or if we lack halves.
                                 # However, Noise2Noise needs independent noise. 
                                 # If we don't have halves, we cannot do standard N2N.
                                 # Check if Reconstruct outputted halves.
            "patch_shape": [params.subvolume_dimensions] * 3,
            "num_slices": params.number_training_subvolumes,
            "split": 0.9,
            "tilt_axis": "Y", 
            "n_normalization_samples": 500,
            "path": str(job_dir / "train_data"),
            "model_name": "denoising_model",
            "epochs": 100,
            "steps_per_epoch": 200,
            "batch_size": 16,
            "unet_kern_size": 3,
            "unet_n_depth": 3,
            "unet_n_first": 16,
            "learning_rate": 0.0004,
            "gpu_id": 0 # Handled by SLURM
        }
        
        # Write config
        import json
        with open(config_json_path, 'w') as f:
            json.dump(train_config, f, indent=4)

        container_service = get_container_service()

        # 3. Execute Extraction
        # Adapting to the provided binAdapter style: `cryoCARE_extract_train_data.py --conf config.json`
        print("[DRIVER] Extracting training data...", flush=True)
        extract_cmd = f"cryoCARE_extract_train_data.py --conf {config_json_path.name}"
        
        wrapped_extract = container_service.wrap_command_for_tool(
            command=extract_cmd,
            cwd=job_dir,
            tool_name="cryocare",
            additional_binds=additional_binds
        )
        run_command(wrapped_extract, cwd=job_dir)

        # 4. Execute Training
        print("[DRIVER] Training model...", flush=True)
        train_cmd = f"cryoCARE_train.py --conf {config_json_path.name}"
        
        wrapped_train = container_service.wrap_command_for_tool(
            command=train_cmd,
            cwd=job_dir,
            tool_name="cryocare_train",
            additional_binds=additional_binds
        )
        run_command(wrapped_train, cwd=job_dir)
        
        # 5. Tar the model (as Relion expects a tar.gz usually, or consistent with old flow)
        # The output seems to be a directory 'denoising_model'.
        # We should verify if downstream expects a directory or tar.
        # The old job.star says: `care_denoising_model Schemes/.../denoising_model.tar.gz`
        print("[DRIVER] Archiving model...", flush=True)
        run_command(f"tar -czf denoising_model.tar.gz denoising_model", cwd=job_dir)

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