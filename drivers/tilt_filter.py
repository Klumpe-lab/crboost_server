#!/usr/bin/env python3
"""
SLURM driver for tilt series filtering (DL pass).

Runs on a GPU compute node. Converts MRC tilt images to PNG,
runs the DL classifier, writes labeled + filtered star files.
"""

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
    project_state, job_model, context_data, job_dir, project_path, job_type = get_driver_context(TiltFilterParams)

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

        # Step 8: Symlink warp_tiltseries dir from upstream
        input_processing = job_model.paths.get("input_processing", "")
        if input_processing:
            warp_ts_link = job_dir / "warp_tiltseries"
            if not warp_ts_link.exists():
                source = Path(input_processing)
                if not source.is_absolute():
                    source = project_path / input_processing
                if source.exists():
                    warp_ts_link.symlink_to(source)
                    print(f"[DRIVER] Symlinked warp_tiltseries -> {source}", flush=True)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()
