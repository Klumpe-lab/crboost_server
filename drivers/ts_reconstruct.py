#!/usr/bin/env python3
"""
ts_reconstruct driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Submitted by relion_schemer via the standard qsub.sh.
          Reads the input STAR, persists a task manifest, builds run_array.sh from
          config/qsub.sh with `#SBATCH --array=...` injected and per-task SLURM
          resources from job_model.get_effective_slurm_config(), sbatches it, polls
          squeue until the array is empty, then runs metadata aggregation. Exit code
          0 only if every tilt-series has a `.ok` status file.

- Set:    TASK mode. One tilt-series per array index. Reads the manifest, picks its
          TS, idempotently skips if the reconstruction MRC already exists, otherwise
          stages a per-TS input_processing dir (symlinking only this TS's XML),
          runs `WarpTools ts_reconstruct`, performs the f16->f32 conversion just for
          this TS, and atomically writes `.task_status/{ts_name}.{ok|fail}`.
"""

import os
import shlex
import sys
import traceback
from pathlib import Path
from typing import List

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    collect_task_results,
    install_cancel_handler,
    read_manifest,
    stage_per_ts_environment,
    submit_array_job,
    wait_for_array_completion,
    write_status_atomic,
    STATUS_DIR_NAME,
)
from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service
from services.configs.metadata_service import MetadataTranslator
from services.configs.starfile_service import StarfileService
from services.job_models import TsReconstructParams


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

DRIVER_SCRIPT = Path(__file__).resolve()


def read_tilt_series_names_from_input_star(input_star: Path) -> List[str]:
    """Sorted list of TS names from the input STAR's `global` block."""
    star_data = StarfileService().read(input_star)
    df = star_data.get("global")
    if df is None or len(df) == 0:
        return []
    return sorted(df["rlnTomoName"].astype(str).tolist())


def reconstruction_mrc_path(job_dir: Path, ts_name: str, rescale_angpixs: float) -> Path:
    """Mirror the convention in metadata_service.update_ts_reconstruct_metadata()."""
    rec_res = f"{rescale_angpixs:.2f}"
    return job_dir / "warp_tiltseries" / "reconstruction" / f"{ts_name}_{rec_res}Apx.mrc"


def build_reconstruct_command(
    params: TsReconstructParams, settings_file: Path, input_processing: Path, output_processing: Path
) -> str:
    return (
        f"WarpTools ts_reconstruct "
        f"--settings {shlex.quote(str(settings_file))} "
        f"--input_processing {shlex.quote(str(input_processing))} "
        f"--output_processing {shlex.quote(str(output_processing))} "
        f"--angpix {params.rescale_angpixs} "
        f"--halfmap_frames {params.halfmap_frames} "
        f"--deconv {params.deconv} "
        f"--perdevice {params.perdevice} "
        f"--dont_invert"
    )


def convert_recon_to_f32_for_ts(recon_dir: Path, ts_name: str, rescale_angpixs: float) -> bool:
    """
    Convert the main per-TS reconstruction MRC from float16 to float32 (sibling file
    with `_f32` suffix), matching the legacy single-job driver's IMOD compatibility step.
    Idempotent: returns False if the f32 file already exists or the source is not float16.
    """
    import mrcfile
    import numpy as np

    rec_res = f"{rescale_angpixs:.2f}"
    src = recon_dir / f"{ts_name}_{rec_res}Apx.mrc"
    if not src.is_file():
        return False
    f32 = src.with_name(src.stem + "_f32.mrc")
    if f32.exists():
        return False
    with mrcfile.open(str(src), mode="r") as mrc:
        if mrc.data.dtype != np.float16:
            return False
        with mrcfile.new(str(f32), overwrite=True) as out:
            out.set_data(mrc.data.astype(np.float32))
            out.voxel_size = mrc.voxel_size
    print(f"  [F32] {src.name} -> {f32.name}", flush=True)
    return True


# ----------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------


def main():
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- ts_reconstruct: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- ts_reconstruct: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(
            TsReconstructParams
        )
    except Exception as e:
        fail_dir = Path.cwd()
        (fail_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[SUPERVISOR] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(f"[SUPERVISOR] CWD (job dir): {job_dir}", flush=True)

    try:
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        instance_id = local_params_data["instance_id"]

        if not paths["input_star"].exists():
            raise FileNotFoundError(f"Input STAR not found: {paths['input_star']}")

        ts_names = read_tilt_series_names_from_input_star(paths["input_star"])
        if not ts_names:
            raise ValueError(f"No tilt-series found in input STAR: {paths['input_star']}")

        n_tasks = len(ts_names)
        print(f"[SUPERVISOR] Found {n_tasks} tilt-series in input STAR", flush=True)

        per_task_cfg = params.get_effective_slurm_config()

        array_job_id = submit_array_job(
            job_dir=job_dir,
            project_path=project_path,
            instance_id=instance_id,
            ts_names=ts_names,
            per_task_cfg=per_task_cfg,
            array_throttle=params.array_throttle,
            driver_script=DRIVER_SCRIPT,
        )

        install_cancel_handler(array_job_id, job_dir)
        wait_for_array_completion(array_job_id, poll_secs=30)

        results = collect_task_results(job_dir, ts_names)
        print(f"[SUPERVISOR] Status: {results.summary}", flush=True)
        if results.failed:
            print(f"[SUPERVISOR] FAILED tilt-series: {results.failed}", flush=True)
        if results.missing:
            print(f"[SUPERVISOR] MISSING tilt-series: {results.missing}", flush=True)

        if not results.all_succeeded:
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            print("[SUPERVISOR] Marking job as FAILED (some tilt-series did not succeed)", flush=True)
            sys.exit(1)

        print("[SUPERVISOR] All tasks succeeded; aggregating metadata...", flush=True)
        translator = MetadataTranslator(StarfileService())
        result = translator.update_ts_reconstruct_metadata(
            job_dir=job_dir,
            input_star_path=paths["input_star"],
            output_star_path=paths["output_star"],
            warp_folder="warp_tiltseries",
            rescale_angpixs=params.rescale_angpixs,
            frame_pixel_size=params.pixel_size,
        )
        if not result["success"]:
            raise Exception(f"Metadata aggregation failed: {result['error']}")

        (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
        print("[SUPERVISOR] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[SUPERVISOR] FATAL ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        sys.exit(1)


# ----------------------------------------------------------------------
# Task mode
# ----------------------------------------------------------------------


def run_task_mode(array_idx: int):
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(
            TsReconstructParams
        )
    except Exception as e:
        print(f"[TASK {array_idx}] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    status_dir = job_dir / STATUS_DIR_NAME
    ts_name = None
    try:
        manifest = read_manifest(job_dir)
        ts_names = manifest["ts_names"]
        if array_idx >= len(ts_names):
            raise IndexError(f"SLURM_ARRAY_TASK_ID {array_idx} out of range (manifest has {len(ts_names)})")
        ts_name = ts_names[array_idx]
        print(f"[TASK {array_idx}] ts_name={ts_name}", flush=True)

        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        out_mrc = reconstruction_mrc_path(job_dir, ts_name, params.rescale_angpixs)

        # Idempotency: skip TS whose reconstruction MRC already exists.
        # Run the f16->f32 conversion just in case it was missed on a previous attempt.
        if out_mrc.exists() and out_mrc.stat().st_size > 0:
            print(f"[TASK {array_idx}] Reconstruction already exists, skipping: {out_mrc}", flush=True)
            convert_recon_to_f32_for_ts(job_dir / "warp_tiltseries" / "reconstruction", ts_name, params.rescale_angpixs)
            write_status_atomic(status_dir, ts_name, ok=True)
            sys.exit(0)

        staged_settings, staged_processing = stage_per_ts_environment(
            job_dir, ts_name, paths["input_processing"], paths["warp_tiltseries_settings"]
        )
        print(f"[TASK {array_idx}] Staged settings: {staged_settings}", flush=True)

        cmd = build_reconstruct_command(
            params=params,
            settings_file=staged_settings,
            input_processing=staged_processing,
            output_processing=paths["output_processing"],
        )
        print(f"[TASK {array_idx}] Command: {cmd}", flush=True)

        wrapped = get_container_service().wrap_command_for_tool(
            command=cmd, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
        )

        run_command(wrapped, cwd=job_dir)

        convert_recon_to_f32_for_ts(job_dir / "warp_tiltseries" / "reconstruction", ts_name, params.rescale_angpixs)

        if not out_mrc.exists():
            raise FileNotFoundError(f"WarpTools reported success but expected output MRC missing: {out_mrc}")

        write_status_atomic(status_dir, ts_name, ok=True)
        print(f"[TASK {array_idx}] {ts_name} done", flush=True)
        sys.exit(0)

    except Exception as e:
        label = ts_name or f"_unknown_idx{array_idx}"
        print(f"[TASK {array_idx}] FATAL ERROR for ts={label}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        try:
            write_status_atomic(status_dir, label, ok=False)
        except Exception as inner:
            print(f"[TASK {array_idx}] Could not write fail status: {inner}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
