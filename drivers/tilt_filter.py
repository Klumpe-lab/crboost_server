#!/usr/bin/env python3
"""
SLURM driver for tilt series filtering (DL pass).

Runs on a GPU compute node. Converts MRC tilt images to PNG,
runs the DL classifier, writes labeled + filtered star files.
"""

import os
import sys
import traceback
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context
    from services.jobs.tilt_filter import TiltFilterParams
except ImportError as e:
    print("FATAL: Could not import services. Check PYTHONPATH.", file=sys.stderr)
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


def main():
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START (tilt_filter) ---", flush=True)

    try:
        _project_state, job_model, _context_data, job_dir, project_path, _job_type = get_driver_context(
            TiltFilterParams
        )
    except Exception as e:
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Node: {os.uname().nodename}", flush=True)
    print(f"CWD: {job_dir}", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    input_star = job_model.paths.get("input_star", "")
    if not input_star:
        print("[DRIVER] ERROR: No input star file resolved", file=sys.stderr, flush=True)
        failure_file.touch()
        sys.exit(1)

    input_star_abs = Path(input_star)
    if not input_star_abs.is_absolute():
        input_star_abs = project_path / input_star

    if not input_star_abs.exists():
        print(f"[DRIVER] ERROR: Input star file does not exist: {input_star_abs}", file=sys.stderr, flush=True)
        failure_file.touch()
        sys.exit(1)

    try:
        from services.tilt_series_service import (
            load_tilt_series,
            get_tilt_image_paths,
            apply_labels,
            filter_good_tilts,
            write_tilt_series,
            drop_tilts_from_tomostar,
        )
        from filterTilts.image_processor import ImageProcessor
        from filterTilts.deepLearning.model_loader import ModelLoader
        from filterTilts.deepLearning.statistics_calculator import PredictionThresholder

        output_dir = job_dir / "filtered"
        output_dir.mkdir(parents=True, exist_ok=True)
        png_dir = output_dir / "png"

        # Step 1: Load tilt series
        print(f"[DRIVER] Loading tilt series from {input_star_abs}", flush=True)
        ts_data = load_tilt_series(str(input_star_abs), str(project_path))
        mrc_paths = get_tilt_image_paths(ts_data, project_path)
        print(f"[DRIVER] Loaded {ts_data.num_tilts} tilts from {ts_data.num_tomograms} tilt series", flush=True)

        # Step 2: Convert MRC to PNG
        print("[DRIVER] Converting MRC images to PNG...", flush=True)
        processor = ImageProcessor(target_size=job_model.image_size, max_workers=min(16, max(1, len(mrc_paths))))
        pil_images = processor.batch_convert(mrc_paths, len(mrc_paths), str(png_dir), show_progress=True)
        print(f"[DRIVER] Converted {len(pil_images)} images", flush=True)

        # Step 3: Run DL inference
        print(f"[DRIVER] Running DL inference with model: {job_model.model_name}", flush=True)
        model_loader = ModelLoader(job_model.model_name, gpu=0)
        model_loader.load_model()
        pred_labels, pred_probs = model_loader.predict_batch(pil_images, job_model.dl_batch_size)
        print(f"[DRIVER] Inference complete: {len(pred_labels)} predictions", flush=True)

        # Step 4: Apply threshold
        thresholder = PredictionThresholder(prob_threshold=job_model.prob_threshold, prob_action=job_model.prob_action)
        pred_labels, pred_probs = thresholder.apply_threshold(pred_labels, pred_probs)

        # Step 5: Apply predictions to tilt data
        ts_data.all_tilts_df["cryoBoostDlLabel"] = pred_labels
        ts_data.all_tilts_df["cryoBoostDlProbability"] = pred_probs

        # Step 6: Apply any existing manual overrides
        if job_model.tilt_labels:
            apply_labels(ts_data, job_model.tilt_labels)

        # Step 7: Write output files
        labeled_path = output_dir / "tiltseries_labeled.star"
        write_tilt_series(ts_data, labeled_path, "tilt_series_labeled")
        print(f"[DRIVER] Wrote labeled star: {labeled_path}", flush=True)

        good_data = filter_good_tilts(ts_data)
        filtered_path = output_dir / "tiltseries_filtered.star"
        write_tilt_series(good_data, filtered_path, "tilt_series_filtered")
        print(f"[DRIVER] Wrote filtered star: {filtered_path} ({good_data.num_tilts} good tilts)", flush=True)

        # Step 8: Apply the cut to the tomostar — the functional output. Alignment
        # reads the tomostar (not the star), so trimming its rows here is what makes
        # the filter actually filter alignment/CTF/reconstruct. The labeled/filtered
        # stars above remain for the dashboard's keep/drop panel.
        df = ts_data.all_tilts_df
        bad_stems = set(df.loc[df["cryoBoostDlLabel"] != "good", "cryoBoostKey"].tolist())

        src_tomostar = job_model.paths.get("input_tomostar", "")
        if not src_tomostar:
            raise ValueError("No input tomostar directory resolved (input_tomostar)")
        src_tomostar_abs = Path(src_tomostar)
        if not src_tomostar_abs.is_absolute():
            src_tomostar_abs = project_path / src_tomostar
        if not src_tomostar_abs.is_dir():
            raise FileNotFoundError(f"Input tomostar directory does not exist: {src_tomostar_abs}")

        out_tomostar = job_model.paths.get("output_tomostar", "") or str(job_dir / "tomostar")
        out_tomostar_abs = Path(out_tomostar)
        if not out_tomostar_abs.is_absolute():
            out_tomostar_abs = project_path / out_tomostar

        kept, dropped = drop_tilts_from_tomostar(src_tomostar_abs, out_tomostar_abs, bad_stems)
        print(f"[DRIVER] Trimmed tomostar -> {out_tomostar_abs} (kept {kept}, dropped {dropped})", flush=True)

        # Step 9 (Stage 2): record the per-tilt verdict in the registry so it stays the
        # authoritative source of truth (the tomostar trim above is the functional cut;
        # dashboard/stars still carry it too until Stage 3 migrates consumers). Frame.id
        # is the raw-movie stem == cryoBoostKey, so we can stamp by key.
        #
        # Fail-loud (maintainer decision 2026-08-14): the registry is the single source
        # of truth for downstream reads, so a missed verdict stamp is stale-data
        # corruption, not a cosmetic miss. A stamp failure fails the job; the trimmed
        # tomostar stays on disk and a re-run is cheap.
        from services.tilt_series import get_registry_for

        registry = get_registry_for(project_path)
        if not registry.tilt_series_ids():
            raise RuntimeError(
                f"TiltSeries registry is empty for project {project_path}. "
                f"Reload the project in the UI to backfill the registry from mdocs, then restart this job."
            )
        verdicts = zip(
            df["cryoBoostKey"], (df["cryoBoostDlLabel"] != "good"), df["cryoBoostDlProbability"], strict=False
        )
        stamped = 0
        for stem, is_filt, prob in verdicts:
            # An unknown stem means the registry and the star disagree on frame
            # identity — drift that must surface, not be skipped over.
            registry.set_frame_filtered(
                str(stem),
                bool(is_filt),
                reason="DL tilt-filter" if is_filt else None,
                probability=float(prob) if prob is not None else None,
            )
            stamped += 1
        registry.save()
        print(f"[DRIVER] Stamped tilt-filter verdict on {stamped} registry frames", flush=True)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
