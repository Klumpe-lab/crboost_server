#!/usr/bin/env python3
"""
SLURM driver of the tilt filter's DL model, in two roles.

A prediction run (no flag): a one-off job in its own directory (TiltFilter/dl_run/NNN) that
classifies every tilt listed in the fs-motion star and writes P(bad) per tilt into the
TiltSeries registry. It writes no verdict (Frame.is_filtered_out).

The DL-auto chain job (--commit): the same prediction in the job's External/jobNNN, with the
job's model, then the verdict: every tilt's effective label at the job's threshold is stamped
into the registry and the counts go to commit.json beside the log. A model that gives every
tilt the same P(bad) fails the job and commits nothing.

Neither role saves project_params.json: the server settles both from the exit markers.

The network runs in the tilt_filter container (filterTilts/deepLearning/predict.py): this driver, like the
server's environment, has no torch. It makes or finds the PNGs, hands the list to the container, and reads the
P(bad) values back from a JSON file.
"""

import argparse
import asyncio
import json
import os
import shlex
import sys
import traceback
from pathlib import Path

try:
    from drivers.driver_base import get_driver_context, run_tool, start_supervisor
    from services.configs.config_service import REPO_ROOT
    from services.jobs.tilt_filter import (
        COMMIT_RECORD,
        LIVENESS_MIN_STD,
        TILT_FILTER_TOOL,
        FilterMode,
        TiltFilterCommit,
        TiltFilterParams,
        prediction_liveness,
        resolve_model,
        stamp_verdict,
        verdict_labels,
    )
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


def _classify(pngs: list[Path], entry, batch_size: int, job_dir: Path, binds: list[str]) -> tuple[list[float], dict]:
    """P(bad) for each PNG, in order, from the classifier running in the tilt_filter container, and the
    facts it reports about the model. The files are the whole interface: a list in, a JSON out."""
    list_file = job_dir / "tilt_pngs.txt"
    result_file = job_dir / "predictions.json"
    list_file.write_text("".join(f"{p}\n" for p in pngs))
    result_file.unlink(missing_ok=True)  # an earlier attempt's result must not pass for this run's
    command = (
        f"cd {shlex.quote(str(job_dir))} && export PYTHONPATH={shlex.quote(str(REPO_ROOT))} && "
        f"python -m filterTilts.deepLearning.predict --weights {shlex.quote(str(entry.weights_path))} "
        f"--arch {shlex.quote(entry.arch)} --normalisation {shlex.quote(entry.normalisation)} "
        f"--pngs {shlex.quote(str(list_file))} --out {shlex.quote(str(result_file))} --batch-size {batch_size}"
    )
    # The checkout is not among the default binds: the code that runs is its filterTilts/ directory.
    png_dirs = {p.parent for p in pngs}
    run_tool(
        command,
        tool_name=TILT_FILTER_TOOL,
        cwd=job_dir,
        binds=[*binds, REPO_ROOT / "filterTilts", entry.weights_path.parent, *png_dirs],
    )
    result = json.loads(result_file.read_text())
    p_bad, facts = result["p_bad"], result["model"]
    if len(p_bad) != len(pngs):
        raise RuntimeError(f"The classifier returned {len(p_bad)} predictions for {len(pngs)} tilts")
    return p_bad, facts


def predict(state, job_model: TiltFilterParams, job_dir: Path, project_path: Path, model: str | None, binds: list[str]):
    """Predict P(bad) for every tilt of the fs-motion star with `model` (None: the default) and
    save it into the registry. `binds` are the project's container binds. Returns the model's key
    and the predictions' liveness (prediction_liveness)."""
    from filterTilts.gallery import GALLERY_PNG_SIZE
    from filterTilts.image_processor import ImageProcessor
    from services.tilt_series import get_registry_for
    from services.tilt_series_service import get_tilt_image_paths, load_tilt_series

    # The registry entry first, so a missing weights file or tool entry fails the run before any
    # conversion. A missing GPU shows at the container call.
    model_key, entry = resolve_model(model)
    workers = len(os.sched_getaffinity(0))
    size = GALLERY_PNG_SIZE

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

    p_bad, facts = _classify(inputs, entry, job_model.dl_batch_size, job_dir, binds)
    tta = ", averaged over 8 rotations/flips" if facts["tta"] else ""
    print(
        f"[DRIVER] Model {model_key}: {facts['arch']} at {facts['input_size']} px, '{facts['normalisation']}' "
        f"normalisation, temperature {facts['temperature']:.3f}{tta}, {entry.weights_path}, on {facts['device']} "
        f"(torch {facts['torch']})",
        flush=True,
    )

    registry = get_registry_for(project_path)
    registry.raise_if_unreadable()
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
        f"[DRIVER] Wrote the confidence score of {len(p_bad)} tilts to the registry "
        f"(min {min(p_bad):.3f}, max {max(p_bad):.3f})",
        flush=True,
    )

    by_series: dict[str, list[float]] = {}
    for ts_name, p in zip(df["rlnTomoName"], p_bad, strict=True):
        by_series.setdefault(str(ts_name), []).append(p)
    liveness = prediction_liveness(by_series)
    if liveness is None:
        print("[DRIVER] Liveness not assessed: no tilt series has two predictions", flush=True)
    elif liveness[0]:
        print(f"[DRIVER] Liveness: the confidence score varies within tilt series (mean {liveness[1]:.3f})", flush=True)
    else:
        print(
            f"[DRIVER] WARNING: the model gives every tilt the same confidence score, {liveness[1]:.3f} (spread "
            f"below {LIVENESS_MIN_STD:g} in every tilt series); its verdicts are meaningless",
            flush=True,
        )
    return model_key, liveness


def commit(job_model: TiltFilterParams, job_dir: Path, project_path: Path, model_key: str) -> None:
    """Stamp the verdict the predictions just written give at the job's threshold (a human's
    label still wins), and record its counts in COMMIT_RECORD for the server."""
    from services.tilt_series import get_registry_for

    registry = get_registry_for(project_path)
    registry.raise_if_unreadable()
    res = asyncio.run(stamp_verdict(registry, verdict_labels(registry, job_model)))
    if not res["success"]:
        raise RuntimeError(res["error"])
    record = TiltFilterCommit(
        mode=FilterMode.DL_AUTO,
        kept=res["kept"],
        dropped=res["dropped"],
        model=model_key,
        threshold=job_model.threshold,
    )
    (job_dir / COMMIT_RECORD).write_text(record.model_dump_json(indent=2))
    print(
        f"[DRIVER] Verdict committed at confidence score >= {job_model.threshold:.2f}: {record.kept} tilts kept, "
        f"{record.dropped} excluded",
        flush=True,
    )


def main():
    start_supervisor()
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START (tilt_filter) ---", flush=True)

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--commit", action="store_true", help="DL auto: commit the verdict after predicting")
    args, _ = parser.parse_known_args()

    try:
        state, job_model, context_data, job_dir, project_path, _job_type = get_driver_context(TiltFilterParams)
    except Exception as e:
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Node: {os.uname().nodename}", flush=True)
    print(f"CWD: {job_dir}", flush=True)

    binds = list(context_data["additional_binds"])
    try:
        if args.commit:
            model_key, liveness = predict(state, job_model, job_dir, project_path, job_model.model, binds)
            if liveness is not None and not liveness[0]:
                raise RuntimeError(
                    f"The model gives every tilt the same confidence score, {liveness[1]:.3f}: its verdicts are "
                    "meaningless. No verdict was committed."
                )
            commit(job_model, job_dir, project_path, model_key)
        else:
            run = job_model.predict_run
            if run is None:
                raise RuntimeError("project_params.json records no prediction run for this job")
            predict(state, job_model, job_dir, project_path, run.model, binds)
    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)

    print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)


if __name__ == "__main__":
    main()
