#!/usr/bin/env python3
import sys
import traceback
from pathlib import Path
import starfile
import pandas as pd
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.container_service import get_container_service


def main():
    print("--- SLURM JOB START ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[DRIVER] Denoise Predict (Native 0.2+ Mode)", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Unpack Resolved Paths
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = context["additional_binds"]

        model_tar_path = paths["model_path"]  # The .tar.gz file
        input_star = paths["input_star"]
        reconstruct_base = paths["reconstruct_base"]
        output_dir = paths["output_dir"]

        # 2. Validation
        if not model_tar_path.exists():
            raise FileNotFoundError(f"Model archive missing at: {model_tar_path}")
        if not input_star.exists():
            raise FileNotFoundError(f"Input STAR missing at: {input_star}")

        # 3. Iterate Tomograms
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
            odd_path = reconstruct_base / "reconstruction" / "odd" / tomo_name

            if not even_path.exists() or not odd_path.exists():
                print(f"[WARN] Halves missing for {tomo_name}. Skipping.")
                continue

            out_path = output_dir / tomo_name

            # 4. Write Config
            # CRITICAL CHANGE: 'path' must point to the .tar.gz file, NOT a directory.
            # We use absolute paths because we know the container binds /users/artem.kushner
            cfg = {
                "path": str(model_tar_path),
                "even": str(even_path),
                "odd": str(odd_path),
                "n_tiles": [params.ntiles_z, params.ntiles_y, params.ntiles_x],
                "output": str(out_path),
                "gpu_id": 0,
                "overwrite": True,
            }

            cfg_name = f"predict_{i}.json"
            with open(job_dir / cfg_name, "w") as f:
                json.dump(cfg, f, indent=4)

            print(f"[DRIVER] Processing {tomo_name}...", flush=True)

            cmd = container_service.wrap_command_for_tool(
                f"cryoCARE_predict.py --conf {cfg_name}",
                cwd=job_dir,
                tool_name="cryocare",
                additional_binds=additional_binds,
            )
            run_command(cmd, cwd=job_dir)
            actual_output = out_path / tomo_name if out_path.is_dir() else out_path
            if actual_output.exists():
                new_row = row.copy()
                try:
                    new_row[col_name] = str(actual_output.relative_to(project_path))
                except ValueError:
                    new_row[col_name] = str(actual_output)
                output_rows.append(new_row)
            else:
                print(f"[WARN] Expected output not found: {actual_output}")

        if output_rows:
            starfile.write(pd.DataFrame(output_rows), job_dir / "tomograms.star")
        else:
            print("[WARN] No outputs created.")

        success_file.touch()
        sys.exit(0)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()
