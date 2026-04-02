#!/usr/bin/env python3
import sys
import traceback
from pathlib import Path
import starfile
import pandas as pd
import json
import numpy as np
import mrcfile

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service
from services.job_models import DenoisePredictParams

def calculate_memory_aware_tiles(tomogram_path: Path, base_tiles=(4, 4, 4), max_tiles=(8, 8, 8)) -> tuple:
    """
    Calculate optimal tiling based on tomogram dimensions.
    Returns (n_tiles_z, n_tiles_y, n_tiles_x)
    """
    try:
        with mrcfile.open(tomogram_path, "r") as mrc:
            dims = mrc.data.shape  # (z, y, x) for tomograms

        print(f"[DRIVER] Tomogram dimensions: {dims}")

        # Simple heuristic: if any dimension > 1000, increase tiling
        tiles = list(base_tiles)
        for i, dim in enumerate(dims):
            if dim > 1000:
                tiles[i] = min(tiles[i] * 2, max_tiles[i])
            elif dim > 2000:
                tiles[i] = min(tiles[i] * 3, max_tiles[i])

        return tuple(tiles)

    except Exception as e:
        print(f"[WARN] Could not read tomogram for tiling calculation: {e}")
        return base_tiles

def main():
    print("--- SLURM JOB START ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(DenoisePredictParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[DRIVER] Denoise Predict (cryoCARE 0.2+ mode)", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = context["additional_binds"]

        model_tar_path  = paths["model_path"]
        input_star      = paths["input_star"]
        reconstruct_base = paths["reconstruct_base"]
        output_dir      = paths["output_dir"]

        if not model_tar_path.exists():
            raise FileNotFoundError(f"Model archive missing: {model_tar_path}")
        if not input_star.exists():
            raise FileNotFoundError(f"Input STAR missing: {input_star}")

        tomo_df = starfile.read(input_star)
        if isinstance(tomo_df, dict):
            tomo_df = list(tomo_df.values())[0]

        output_dir.mkdir(exist_ok=True)
        container_service = get_container_service()
        output_rows = []

        col_name = "rlnTomoReconstructedTomogram"

        for i, row in tomo_df.iterrows():
            tomo_name = Path(row[col_name]).name

            if params.denoising_tomo_name and params.denoising_tomo_name not in tomo_name:
                continue

            even_path = reconstruct_base / "reconstruction" / "even" / tomo_name
            odd_path  = reconstruct_base / "reconstruction" / "odd"  / tomo_name

            if not even_path.exists() or not odd_path.exists():
                print(f"[WARN] Halves missing for {tomo_name}, skipping.")
                continue

            base_tiles = (params.ntiles_z, params.ntiles_y, params.ntiles_x)
            n_tiles_z, n_tiles_y, n_tiles_x = calculate_memory_aware_tiles(
                even_path, base_tiles=base_tiles, max_tiles=(8, 8, 8)
            )
            print(f"[DRIVER] Tiles: z={n_tiles_z} y={n_tiles_y} x={n_tiles_x}")

            # cryoCARE_predict.py treats "output" as a directory and writes
            # output_dir/basename(even) inside it. Pass output_dir here, not
            # a per-tomogram path -- otherwise it creates output_dir/tomo_name/
            # as a directory and writes the MRC one level too deep.
            cfg = {
                "path"      : str(model_tar_path),
                "even"      : str(even_path),
                "odd"       : str(odd_path),
                "n_tiles"   : [n_tiles_z, n_tiles_y, n_tiles_x],
                "output"    : str(output_dir),
                "gpu_id"    : 0,
                "overwrite" : True,
            }

            cfg_name = f"predict_{i}.json"
            with open(job_dir / cfg_name, "w") as f:
                json.dump(cfg, f, indent=4)

            print(f"[DRIVER] Processing {tomo_name}...", flush=True)

            cmd = (
                f"TF_FORCE_GPU_ALLOW_GROWTH=true "
                f"TF_GPU_ALLOCATOR=cuda_malloc_async "
                f"cryoCARE_predict.py --conf {cfg_name}"
            )
            wrapped_cmd = container_service.wrap_command_for_tool(
                cmd, cwd=job_dir, tool_name="cryocare", additional_binds=additional_binds
            )
            run_command(wrapped_cmd, cwd=job_dir)

            # cryoCARE writes output_dir/basename(even) = output_dir/tomo_name
            actual_output = output_dir / tomo_name
            if not actual_output.exists():
                print(f"[WARN] Expected output not found: {actual_output}")
                continue

            new_row = row.copy()
            try:
                new_row[col_name] = str(actual_output.relative_to(project_path))
            except ValueError:
                new_row[col_name] = str(actual_output)
            output_rows.append(new_row)

        if output_rows:
            starfile.write(pd.DataFrame(output_rows), job_dir / "tomograms.star")
        else:
            print("[WARN] No outputs produced.")

        success_file.touch()
        sys.exit(0)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)

if __name__ == "__main__":
    main()
