#!/usr/bin/env python3
"""
SLURM driver for one DL prediction run of the tilt filter.

Runs on a GPU node in the run's own directory (TiltFilter/dl_run/NNN). Classifies every
tilt listed in the fs-motion star and writes P(bad) per tilt into the TiltSeries registry.
It writes no verdict (Frame.is_filtered_out) and does not save project_params.json: the
server settles the run from this directory's exit markers.
"""

import os
import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context
    from services.jobs.tilt_filter import LIVENESS_MIN_STD, TiltFilterParams, prediction_liveness, resolve_model
except ImportError as e:
    print("FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def _named(keys: list[str]) -> str:
    return ", ".join(keys[:5]) + (f" (+{len(keys) - 5} more)" if len(keys) > 5 else "")


def _is_model_input(png: Path, size: int) -> bool:
    """True when `png` is an 8-bit grayscale size x size image: a gallery thumbnail the model can read."""
    from PIL import Image

    try:
        with Image.open(png) as img:
            return img.mode == "L" and img.size == (size, size)
    except OSError:  # missing or not an image: the tilt is converted from its MRC instead
        return False


def predict(state, job_model: TiltFilterParams, job_dir: Path, project_path: Path) -> None:
    from filterTilts.deepLearning.model_loader import GALLERY_PNG_SIZE, ModelLoader
    from filterTilts.image_processor import ImageProcessor
    from services.tilt_series import get_registry_for
    from services.tilt_series_service import get_tilt_image_paths, load_tilt_series

    run = job_model.predict_run
    if run is None:
        raise RuntimeError("project_params.json records no prediction run for this job")
    model_key, entry = resolve_model(run.model)
    workers = len(os.sched_getaffinity(0))

    # The model first, so a missing weights file or GPU fails the run before any conversion.
    loader = ModelLoader(entry.weights_path, entry.arch, entry.normalisation, gpu=0, num_workers=workers)
    loader.load_model()
    size = GALLERY_PNG_SIZE
    tta = ", averaged over 8 rotations/flips" if loader.tta else ""
    print(
        f"[DRIVER] Model {model_key}: {entry.arch} at {loader.input_size} px, '{entry.normalisation}' "
        f"normalisation, temperature {loader.temperature:.3f}{tta}, {entry.weights_path}, on {loader.device_name}",
        flush=True,
    )

    star_rel = job_model.paths.get("input_star", "")
    if not star_rel:
        raise RuntimeError("No input star file resolved")
    input_star = Path(star_rel) if Path(star_rel).is_absolute() else project_path / star_rel
    if not input_star.exists():
        raise RuntimeError(f"Input star file does not exist: {input_star}")
    ts_data = load_tilt_series(str(input_star), str(project_path))
    df = ts_data.all_tilts_df
    if df.empty:
        raise RuntimeError(f"{input_star} lists no tilts")
    tilt_keys = [str(k) for k in df["cryoBoostKey"]]
    print(f"[DRIVER] {len(df)} tilts in {ts_data.num_tomograms} tilt series from {input_star}", flush=True)

    # Every model reads the gallery thumbnails (ImageProcessor at GALLERY_PNG_SIZE, named after
    # the MRC); one trained at another size resizes them (ModelLoader). A tilt without a usable
    # thumbnail is converted from its MRC the same way, into this run's directory.
    png_dir = Path(state.tilt_filter_png_dir or project_path / "TiltFilter" / "png")
    mrc_paths = get_tilt_image_paths(ts_data, project_path)
    inputs = [png_dir / f"{Path(m).stem}.png" for m in mrc_paths]
    todo = [i for i, png in enumerate(inputs) if not _is_model_input(png, size)]
    if todo:
        conv_dir = job_dir / "png"
        print(
            f"[DRIVER] {len(todo)} of {len(inputs)} tilts have no usable PNG in {png_dir}; "
            f"converting them into {conv_dir}",
            flush=True,
        )
        processor = ImageProcessor(target_size=size, max_workers=workers)
        processor.batch_convert([mrc_paths[i] for i in todo], len(todo), str(conv_dir), show_progress=False)
        # batch_convert skips a tilt it cannot read without saying which, so the files decide.
        for i in todo:
            inputs[i] = conv_dir / inputs[i].name
        failed = [tilt_keys[i] for i in todo if not _is_model_input(inputs[i], size)]
        if failed:
            raise RuntimeError(
                f"{len(failed)} tilts could not be made into a {size}x{size} model input: {_named(failed)}"
            )
    else:
        print(f"[DRIVER] Model inputs: the gallery PNGs in {png_dir}", flush=True)

    p_bad = loader.predict_p_bad([str(p) for p in inputs], job_model.dl_batch_size)

    registry = get_registry_for(project_path)
    if not registry.tilt_series_ids():
        raise RuntimeError(
            f"The TiltSeries registry of {project_path} is empty. Reload the project in the UI to backfill it "
            "from the mdocs, then run DL again."
        )
    unknown = []
    for key, p in zip(tilt_keys, p_bad, strict=True):
        try:
            registry.set_frame_prediction(key, p)
        except KeyError:
            unknown.append(key)
    if unknown:
        # Nothing is saved: a partial set of predictions would read as a complete run.
        raise RuntimeError(f"{len(unknown)} tilts are not in the TiltSeries registry: {_named(unknown)}")
    registry.save()
    print(
        f"[DRIVER] Wrote P(bad) for {len(p_bad)} tilts to the registry (min {min(p_bad):.3f}, max {max(p_bad):.3f})",
        flush=True,
    )

    by_series: dict[str, list[float]] = {}
    for ts_name, p in zip(df["rlnTomoName"], p_bad, strict=True):
        by_series.setdefault(str(ts_name), []).append(p)
    liveness = prediction_liveness(by_series)
    if liveness is None:
        print("[DRIVER] Liveness not assessed: no tilt series has two predictions", flush=True)
    elif liveness[0]:
        print(f"[DRIVER] Liveness: P(bad) varies within tilt series (mean {liveness[1]:.3f})", flush=True)
    else:
        print(
            f"[DRIVER] WARNING: the model gives every tilt P(bad) ~ {liveness[1]:.3f} (spread below "
            f"{LIVENESS_MIN_STD} in every tilt series); its verdicts are meaningless",
            flush=True,
        )


def main():
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START (tilt_filter) ---", flush=True)

    try:
        state, job_model, _context_data, job_dir, project_path, _job_type = get_driver_context(TiltFilterParams)
    except Exception as e:
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Node: {os.uname().nodename}", flush=True)
    print(f"CWD: {job_dir}", flush=True)

    try:
        predict(state, job_model, job_dir, project_path)
    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)

    (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
    print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)


if __name__ == "__main__":
    main()
