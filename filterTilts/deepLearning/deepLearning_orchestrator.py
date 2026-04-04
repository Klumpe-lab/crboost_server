#import os
from pathlib import Path
from ..star_handler import add_predictions_to_ts
from ..image_processor import ImageProcessor
from .model_loader import ModelLoader
from .statistics_calculator import PredictionThresholder


class TiltSeriesPredictor:
    """
    Deep learning predictor for tilt series classification.
    Works with pre-loaded tiltSeriesMeta objects.
    """
    
    def __init__(self, model_path, output_folder, sz=384, batch_size=50, 
                 gpu=0, prob_thr=0.1, prob_action="assignToGood", 
                 threads=20, num_dataloader_workers=4, save_pngs=True):
        """Initialize the prediction pipeline."""
        self.model_path = model_path
        self.output_folder = Path(output_folder)
        self.sz = sz
        self.batch_size = batch_size
        self.gpu = gpu
        self.prob_thr = prob_thr
        self.prob_action = prob_action
        self.threads = threads
        self.num_dataloader_workers = num_dataloader_workers
        self.save_pngs = save_pngs
        
        # Initialize components
        self.image_processor = ImageProcessor(target_size=sz, max_workers=threads)
        self.model_loader = ModelLoader(model_path, gpu=gpu, num_workers=num_dataloader_workers)
        self.threshold_statistics = PredictionThresholder(prob_threshold=prob_thr, prob_action=prob_action)
        
        # Create output folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
    
    def predict(self, ts):
        """
        Run predictions on a tiltSeriesMeta object.
        
        Parameters:
        - ts: tiltSeriesMeta object (already loaded)
        
        Returns:
        - Updated tiltSeriesMeta object with predictions
        """
        print(f"\n{'='*60}")
        print("Deep Learning Prediction")
        print(f"{'='*60}")
        print(f"Processing {len(ts.all_tilts_df)} tilts from {ts.nrTomo} tomograms")
        
        # Step 1: Get image paths
        image_paths = ts.getMicrographMovieNameFull()
        
        # Step 2: Convert MRC to PIL
        print("\nStep 1: Converting MRC images to PIL format...")
        png_folder = self.output_folder / "png" if self.save_pngs else None
        if png_folder:
            print(f"Saving PNGs to: {png_folder}")
        
        pil_images = self.image_processor.batch_convert(
            image_paths,
            len(ts.all_tilts_df),
            png_output_folder=png_folder,
            show_progress=True
        )
        
        print(f"Successfully converted {len(pil_images)} images")
        
        # Step 3: Load model and predict
        print("\nStep 2: Loading model and running predictions...")
        print(f"Model: {self.model_path}")
        print(f"Device: {'GPU ' + str(self.gpu) if self.gpu >= 0 else 'CPU'}")
        
        import time
        start_time = time.time()
        
        self.model_loader.load_model()
        pred_labels, pred_probs = self.model_loader.predict_batch(pil_images, self.batch_size)
        
        elapsed = time.time() - start_time
        print(f"Model loading and prediction time: {elapsed:.2f} seconds ({len(pred_labels)/elapsed:.1f} tilts/sec)")
        
        # Step 4: Apply threshold
        print("\nStep 3: Applying probability threshold...")
        print(f"Threshold: {self.prob_thr}, Action: {self.prob_action}")
        
        pred_labels, pred_probs = self.threshold_statistics.apply_threshold(pred_labels, pred_probs)
        
        # Step 5: Add predictions to ts
        print("\nStep 4: Adding predictions to metadata...")
        ts = add_predictions_to_ts(ts, pred_labels, pred_probs)
        
        # Step 6: Write output
        output_star = self.output_folder / "tiltseries_labeled.star"
        print(f"\nStep 5: Writing output to {output_star}...")
        ts.writeTiltSeries(str(output_star))
        
        print(f"\n{'='*60}")
        print("Prediction Complete!")
        print(f"{'='*60}\n")
        
        return ts
    


        """
    def predict_from_star(self, star_path, relion_proj=''):
        Run complete prediction pipeline from star file.
        
        Parameters:
        - star_path: Path to tiltseries star file
        - relion_proj: Path to RELION project folder
        
        Returns:
        - Updated tiltSeriesMeta object with predictions

        print(f"\n{'='*60}")
        print("Starting Tilt Series Prediction Pipeline")
        print(f"{'='*60}\n")
        
        # Step 1: Load star file and get image paths
        print("Step 1: Loading tilt series from star file...")
        self.star_handler = StarFileHandler(star_path, relion_proj)
        ts = self.star_handler.load_tilt_series()
        image_paths = self.star_handler.get_image_paths()
        
        print(f"Found {len(image_paths)} tilts from {self.star_handler.get_tomogram_count()} tomograms")
        
        # Step 2: Convert MRC images to PIL
        print("\nStep 2: Converting MRC images to PIL format...")
        png_folder = self.output_folder / "png" if self.save_pngs else None
        if png_folder:
            print(f"Saving PNGs to: {png_folder}")
        
        pil_images = self.image_processor.batch_convert(
            image_paths, 
            len(ts.all_tilts_df),
            png_output_folder=png_folder,
            show_progress=True
        )
        
        print(f"Successfully converted {len(pil_images)} images")
        
        # Step 3: Load model and run predictions
        print("\nStep 3: Loading model and running predictions...")
        print(f"Using model: {self.model_path}")
        print(f"Device: {'GPU ' + str(self.gpu) if self.gpu >= 0 else 'CPU'}")
        
        import time
        start_time = time.time()

        self.model_loader.load_model()
        pred_labels, pred_probs = self.model_loader.predict_batch(pil_images, self.batch_size)
        
        end_time = time.time()
        print(f"Prediction time: {end_time - start_time:.2f} seconds")

        print(f"Predictions complete: {len(pred_labels)} tilts classified")
        
        # Step 4: Apply threshold policy
        print("\nStep 4: Applying probability threshold policy...")
        print(f"Threshold: {self.prob_thr}, Action: {self.prob_action}")
        
        pred_labels, pred_probs = self.threshold_statistics.apply_threshold(pred_labels, pred_probs)
        
        # Step 5: Add predictions to star file
        print("\nStep 5: Adding predictions to tilt series metadata...")
        ts = add_predictions_to_ts(ts, pred_labels, pred_probs)
        
        # Step 6: Merge with existing predictions if available
        existing_star = self.output_folder / "tiltseries_labeled.star"
        if existing_star.exists():
            print(f"\nStep 6: Found existing predictions, merging...")
            # Create a handler to use merge functionality
            handler = StarFileHandler.__new__(StarFileHandler)
            handler.ts = ts
            handler.star_path = None
            handler.relion_proj = Path(relion_proj) if relion_proj else Path.cwd()
            handler.merge_with_existing(existing_star)
            ts = handler.ts
        
        # Step 7: Write output star file
        output_star = self.output_folder / "tiltseries_labeled.star"
        print(f"\nStep 7: Writing output star file to {output_star}...")
        ts.writeTiltSeries(str(output_star))
        
        print(f"\n{'='*60}")
        print("Pipeline Complete!")
        print(f"{'='*60}\n")
        print(f"Output star file: {output_star}")
        if png_folder:
            print(f"PNG images: {png_folder}")
        
        return ts
    """