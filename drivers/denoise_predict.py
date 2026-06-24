#!/usr/bin/env python3
"""
denoise_predict driver — supervisor + per-tomogram SLURM array task.

Symmetric with the other per-TS jobs (ts_reconstruct, ts_ctf, ...): one driver script
that dispatches on the SLURM_ARRAY_TASK_ID env var.

- Unset:  SUPERVISOR mode. Submitted by the orchestrator via the lightweight CPU-only
          supervisor sbatch. Enumerates tomograms from the reconstruct tomograms.star,
          pre-skips those filtered out by denoising_tomo_name, submits a child SLURM array
          (one tomogram per task) with PER-TASK GPU resources, polls squeue until the array
          drains, then aggregates the denoised tomograms.star. Writes
          RELION_JOB_EXIT_{SUCCESS,FAILURE} (the array tasks never do).

- Set:    TASK mode. One tomogram per array index. Reads the manifest, resolves this
          tomogram's even/odd halves, idempotently skips if the denoised MRC already exists,
          otherwise runs cryoCARE_predict.py (or isonet.py predict), and atomically writes
          .task_status/{ts}.{ok|fail}.

Denoise method (cryoCARE | IsoNet) is selected per-job via params.denoise_method and only
affects the per-task command built in TASK mode.
"""

import json
import os
import shlex
import shutil
import sys
import tarfile
import traceback
from pathlib import Path
from typing import Dict, List, Tuple

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

import starfile

from drivers.array_job_base import (
    collect_task_results,
    install_cancel_handler,
    read_manifest,
    submit_array_job,
    wait_for_array_completion,
    write_skip_status,
    write_status_atomic,
    STATUS_DIR_NAME,
)
from drivers.driver_base import get_driver_context, run_command, require_producer_input
from services.computing.container_service import get_container_service
from services.job_models import DenoisePredictParams
from services.models_base import DenoiseMethod

DRIVER_SCRIPT = Path(__file__).resolve()

# Column in the reconstruct tomograms.star that holds the full reconstructed tomogram path.
TOMO_COL = "rlnTomoReconstructedTomogram"
# Stale-after-denoising half-map columns; dropped from the denoised output STAR.
HALF_COLS = ("rlnTomoReconstructedTomogramHalf1", "rlnTomoReconstructedTomogramHalf2")


# ----------------------------------------------------------------------
# Shared helpers
# ----------------------------------------------------------------------


def _global_block(data):
    """starfile.read returns a DataFrame (single unnamed block) or a dict of blocks.
    Return (block_name_or_None, df) preferring the RELION 'global' block."""
    if isinstance(data, dict):
        if "global" in data:
            return "global", data["global"]
        name = next(iter(data))
        return name, data[name]
    return None, data


def read_tomo_map(input_star: Path) -> Tuple[List[str], Dict[str, str]]:
    """Return (sorted ts_names, {ts_name: reconstructed-tomogram basename}) from the
    reconstruct tomograms.star. The array maps index N -> ts_names[N]; the basename map
    lets each task resolve its even/odd halves without re-parsing the whole STAR."""
    _, df = _global_block(starfile.read(input_star))
    ts_names: List[str] = []
    tomo_basenames: Dict[str, str] = {}
    for _, row in df.iterrows():
        ts = str(row["rlnTomoName"])
        tomo_basenames[ts] = Path(str(row[TOMO_COL])).name
        ts_names.append(ts)
    return sorted(ts_names), tomo_basenames


def calculate_memory_aware_tiles(tomogram_path: Path, base_tiles=(4, 4, 4), max_tiles=(8, 8, 8)) -> tuple:
    """Calculate optimal tiling based on tomogram dimensions. Returns (n_tiles_z, n_tiles_y, n_tiles_x)."""
    try:
        import mrcfile

        with mrcfile.open(tomogram_path, "r") as mrc:
            dims = mrc.data.shape  # (z, y, x) for tomograms
        print(f"[DRIVER] Tomogram dimensions: {dims}")
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


# ----------------------------------------------------------------------
# Per-task command construction (method-specific)
# ----------------------------------------------------------------------


def build_cryocare_predict_command(
    params: DenoisePredictParams,
    job_dir: Path,
    model_tar: Path,
    even_path: Path,
    odd_path: Path,
    output_dir: Path,
    idx: int,
) -> str:
    """cryoCARE_predict.py treats `output` as a directory and writes output_dir/basename(even)
    inside it -- which equals output_dir/<tomo_basename>."""
    base_tiles = (params.ntiles_z, params.ntiles_y, params.ntiles_x)
    n_tiles_z, n_tiles_y, n_tiles_x = calculate_memory_aware_tiles(
        even_path, base_tiles=base_tiles, max_tiles=(8, 8, 8)
    )
    print(f"[TASK {idx}] Tiles: z={n_tiles_z} y={n_tiles_y} x={n_tiles_x}", flush=True)
    cfg = {
        "path": str(model_tar),
        "even": str(even_path),
        "odd": str(odd_path),
        "n_tiles": [n_tiles_z, n_tiles_y, n_tiles_x],
        "output": str(output_dir),
        "gpu_id": 0,
        "overwrite": True,
    }
    cfg_name = f"predict_{idx}.json"
    with open(job_dir / cfg_name, "w") as f:
        json.dump(cfg, f, indent=4)
    return f"TF_FORCE_GPU_ALLOW_GROWTH=true TF_GPU_ALLOCATOR=cuda_malloc_async cryoCARE_predict.py --conf {cfg_name}"


def prepare_isonet_model(job_dir: Path, model_tar: Path) -> Path:
    """Untar the IsoNet model archive (once, in the supervisor) and return the newest .pt.

    Also enforces the method contract: an IsoNet model tar has an inner 'isonet_maps/' dir; a
    cryoCARE tar has 'denoising_model/'. A mismatch here means denoise_method=IsoNet was pointed at
    a cryoCARE-trained model (or vice-versa) -- fail loud rather than feed the wrong tool."""
    dest = job_dir / ".isonet_model"
    dest.mkdir(exist_ok=True)
    with tarfile.open(model_tar, "r:gz") as t:
        t.extractall(dest)
    maps = dest / "isonet_maps"
    if not maps.exists():
        raise RuntimeError(
            f"Model archive {model_tar} is not an IsoNet model (no isonet_maps/ inside; found "
            f"{[p.name for p in dest.iterdir()]}). denoise_method=IsoNet needs an IsoNet-trained model."
        )
    pts = sorted(maps.glob("**/*.pt"), key=lambda p: p.stat().st_mtime)
    if not pts:
        raise RuntimeError(f"No .pt checkpoint found under {maps}")
    return pts[-1]


def run_isonet_predict_task(
    params: DenoisePredictParams,
    job_dir: Path,
    additional_binds: list,
    model_pt: Path,
    full_path: Path,
    output_dir: Path,
    out_mrc: Path,
    ts_name: str,
    idx: int,
) -> None:
    """Per-tomogram IsoNet prediction: stage this tomo's FULL reconstruction into a 1-file dir
    (prepare_star --full populates rlnTomoName; predict denoises the full volume), [deconv], then
    predict into a per-task dir and move the single corrected MRC to the canonical denoised path
    (out_mrc). Predicting into an isolated per-task dir sidesteps IsoNet's output-filename
    convention -- ISONET-ASSUMPTION: predict writes exactly one full-size .mrc. See
    ISONET_INTEGRATION_PLAN.md."""
    container = get_container_service()
    stage = job_dir / ".staging" / f"task_{ts_name}"
    full_dir, corrected = stage / "full", stage / "corrected"
    for d in (full_dir, corrected):
        d.mkdir(parents=True, exist_ok=True)
    link = full_dir / full_path.name
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(full_path.resolve())

    prep = stage / "isonet_prep.star"

    def isonet(cmd: str):
        wrapped = container.wrap_command_for_tool(
            command=cmd, cwd=job_dir, tool_name="isonet", additional_binds=additional_binds
        )
        run_command(wrapped, cwd=job_dir)

    isonet(
        f"isonet.py prepare_star --full {shlex.quote(str(full_dir))} "
        f"--star_name {shlex.quote(str(prep))} --pixel_size auto"
    )
    input_col = "rlnTomoName"
    if params.isonet_deconv:
        isonet(
            f"isonet.py deconv --star_file {shlex.quote(str(prep))} --output_dir {shlex.quote(str(stage / 'deconv'))}"
        )
        input_col = "rlnDeconvTomoName"
    isonet(
        f"isonet.py predict --star_file {shlex.quote(str(prep))} --model {shlex.quote(str(model_pt))} "
        f"--input_column {input_col} --output_dir {shlex.quote(str(corrected))}"
    )

    mrcs = list(corrected.glob("*.mrc"))
    if not mrcs:
        raise FileNotFoundError(f"IsoNet predict produced no MRC in {corrected}")
    # predict may also drop small preview-slice MRCs (save_slices defaults True); the corrected
    # tomogram is by far the largest volume in the per-task dir.
    produced = max(mrcs, key=lambda p: p.stat().st_size)
    out_mrc.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(produced), str(out_mrc))
    print(f"[TASK {idx}] IsoNet corrected -> {out_mrc}", flush=True)


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def aggregate_output_star(
    input_star: Path,
    job_dir: Path,
    output_dir: Path,
    project_path: Path,
    ok_ts_names: List[str],
    tomo_basenames: Dict[str, str],
) -> None:
    """Build job_dir/tomograms.star from the reconstruct STAR, keeping only rows whose tomogram
    was denoised (.ok), with TOMO_COL repointed at output_dir/<basename> and the now-stale
    half-map columns dropped. Downstream (template matching) reads TOMO_COL from this STAR."""
    import pandas as pd

    block_name, df = _global_block(starfile.read(input_star))
    ok_set = set(ok_ts_names)
    keep_rows = []
    for _, row in df.iterrows():
        ts = str(row["rlnTomoName"])
        if ts not in ok_set:
            continue
        denoised = output_dir / tomo_basenames[ts]
        new_row = row.copy()
        try:
            new_row[TOMO_COL] = str(denoised.relative_to(project_path))
        except ValueError:
            new_row[TOMO_COL] = str(denoised)
        keep_rows.append(new_row)

    out_df = pd.DataFrame(keep_rows)
    for col in HALF_COLS:
        if col in out_df.columns:
            out_df = out_df.drop(columns=col)

    out_path = job_dir / "tomograms.star"
    starfile.write({block_name: out_df} if block_name else out_df, out_path, overwrite=True)
    print(f"[SUPERVISOR] Wrote denoised STAR with {len(out_df)} tomogram(s): {out_path}", flush=True)


def run_supervisor_mode():
    try:
        (project_state, params, local_params_data, job_dir, project_path, job_type) = get_driver_context(
            DenoisePredictParams
        )
    except Exception as e:
        (Path.cwd() / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[SUPERVISOR] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(f"[SUPERVISOR] CWD (job dir): {job_dir}", flush=True)
    try:
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        instance_id = local_params_data["instance_id"]

        require_producer_input(paths["model_path"], "Denoise model archive")
        require_producer_input(paths["input_star"], "Input tomograms STAR")

        ts_names, tomo_basenames = read_tomo_map(paths["input_star"])
        if not ts_names:
            raise ValueError(f"No tomograms found in input STAR: {paths['input_star']}")
        print(
            f"[SUPERVISOR] {len(ts_names)} tomogram(s) in input STAR (method={params.denoise_method.value})", flush=True
        )

        status_dir = job_dir / STATUS_DIR_NAME
        # Pre-skip tomograms filtered out by denoising_tomo_name so they never spawn a GPU task.
        flt = params.denoising_tomo_name
        if flt:
            skipped = [ts for ts in ts_names if flt not in tomo_basenames[ts]]
            for ts in skipped:
                write_skip_status(status_dir, ts, reason=f"filtered out by denoising_tomo_name={flt!r}")
            print(
                f"[SUPERVISOR] {len(skipped)} tomogram(s) pre-skipped by filter; {len(ts_names) - len(skipped)} to run",
                flush=True,
            )

        manifest_extra = {"tomo_basenames": tomo_basenames}
        # For IsoNet, untar the model ONCE here (shared NFS) and stash the .pt for the tasks.
        # cryoCARE tasks read the tar directly (cryoCARE_predict accepts the .tar.gz as "path").
        if params.denoise_method == DenoiseMethod.ISONET:
            model_pt = prepare_isonet_model(job_dir, paths["model_path"])
            manifest_extra["isonet_model_pt"] = str(model_pt)
            print(f"[SUPERVISOR] Staged IsoNet model: {model_pt}", flush=True)

        per_task_cfg = params.get_effective_slurm_config()
        array_job_id = submit_array_job(
            job_dir=job_dir,
            project_path=project_path,
            instance_id=instance_id,
            ts_names=ts_names,
            per_task_cfg=per_task_cfg,
            array_throttle=params.array_throttle,
            driver_script=DRIVER_SCRIPT,
            manifest_extra=manifest_extra,
        )

        if array_job_id is not None:
            install_cancel_handler(array_job_id, job_dir)
            wait_for_array_completion(array_job_id, poll_secs=30)
        else:
            print("[SUPERVISOR] No array submitted (all tomograms previously settled)", flush=True)

        results = collect_task_results(job_dir, ts_names)
        print(f"[SUPERVISOR] Status: {results.summary}", flush=True)
        if results.failed:
            print(f"[SUPERVISOR] FAILED tomograms: {results.failed}", flush=True)
        if results.missing:
            print(f"[SUPERVISOR] MISSING tomograms: {results.missing}", flush=True)

        if not results.all_succeeded:
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            print("[SUPERVISOR] Marking job FAILED (some tomograms did not succeed)", flush=True)
            sys.exit(1)

        if not results.ok:
            print("[SUPERVISOR] WARN: no tomograms denoised (all filtered out?); writing empty STAR", flush=True)
        aggregate_output_star(
            paths["input_star"], job_dir, paths["output_dir"], project_path, results.ok, tomo_basenames
        )

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
            DenoisePredictParams
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
        tomo_basenames = manifest.get("tomo_basenames", {})
        if array_idx >= len(ts_names):
            raise IndexError(f"SLURM_ARRAY_TASK_ID {array_idx} out of range (manifest has {len(ts_names)})")
        ts_name = ts_names[array_idx]
        tomo_basename = tomo_basenames.get(ts_name)
        if not tomo_basename:
            raise ValueError(f"No tomogram basename in manifest for ts={ts_name}")
        print(f"[TASK {array_idx}] ts={ts_name} tomo={tomo_basename}", flush=True)

        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]
        reconstruct_base = paths["reconstruct_base"]
        output_dir = paths["output_dir"]
        model_tar = paths["model_path"]
        output_dir.mkdir(parents=True, exist_ok=True)

        even_path = reconstruct_base / "reconstruction" / "even" / tomo_basename
        odd_path = reconstruct_base / "reconstruction" / "odd" / tomo_basename
        full_path = reconstruct_base / "reconstruction" / tomo_basename

        out_mrc = output_dir / tomo_basename
        # Idempotency: skip tomograms already denoised on a previous attempt.
        if out_mrc.exists() and out_mrc.stat().st_size > 0:
            print(f"[TASK {array_idx}] Denoised output already exists, skipping: {out_mrc}", flush=True)
            write_status_atomic(status_dir, ts_name, ok=True)
            sys.exit(0)

        if params.denoise_method == DenoiseMethod.ISONET:
            # IsoNet denoises the full reconstruction (rlnTomoName); even/odd were for training.
            if not full_path.exists():
                raise FileNotFoundError(f"Missing full reconstruction for {tomo_basename}: {full_path}")
            model_pt = manifest.get("isonet_model_pt")
            if not model_pt:
                raise RuntimeError("Manifest missing isonet_model_pt (supervisor did not stage the IsoNet model)")
            run_isonet_predict_task(
                params, job_dir, additional_binds, Path(model_pt), full_path, output_dir, out_mrc, ts_name, array_idx
            )
        else:
            if not even_path.exists() or not odd_path.exists():
                raise FileNotFoundError(f"Missing even/odd halves for {tomo_basename}: {even_path} / {odd_path}")
            cmd = build_cryocare_predict_command(params, job_dir, model_tar, even_path, odd_path, output_dir, array_idx)
            print(f"[TASK {array_idx}] Command: {cmd}", flush=True)
            wrapped = get_container_service().wrap_command_for_tool(
                command=cmd, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
            )
            run_command(wrapped, cwd=job_dir)

        if not out_mrc.exists():
            raise FileNotFoundError(f"Prediction reported success but output missing: {out_mrc}")

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


def main():
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- denoise_predict: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- denoise_predict: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


if __name__ == "__main__":
    main()
