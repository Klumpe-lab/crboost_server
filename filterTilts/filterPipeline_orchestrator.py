import os
from pathlib import Path
from .deepLearning.deepLearning_orchestrator import TiltSeriesPredictor
from .deepLearning.statistics_calculator import FilterStatistics
from .plotter import FilterPlotter
from .warpProjectHandler import WarpProjectHandler
from src.rw.librw import tiltSeriesMeta


class TiltSeriesFilter:
    """
    TiltSeriesFilter with deep learning integration.
    Handles complete filtering pipeline, 
    rule-based filtering (not yet implemented), and deep learning filtering.
    """
    
    def __init__(self, tiltseries_star, relion_proj='', output_folder=None, 
                 threads=24, prob_thr=0.1, prob_action="assignToGood"):
        """
        Initialize tilt series filter.
        
        Parameters:
        - tiltseries_star: Path to tiltseries star file
        - relion_proj: Path to RELION project folder
        - output_folder: Output directory for results
        - threads: Number of parallel workers
        - prob_thr: Probability threshold for DL filtering
        - prob_action: Action for low-probability predictions
        """
        #self.tiltseries_star = Path(tiltseries_star)
        #self.relion_proj = Path(relion_proj) if relion_proj else Path.cwd() #does not work with Path obj in tiltSeriesMeta (str used throughout code)
        self.tiltseries_star = tiltseries_star
        self.relion_proj = relion_proj
        self.output_folder = Path(output_folder) if output_folder else self.tiltseries_star.parent / "filtered"
        self.threads = int(threads)
        self.prob_thr = prob_thr
        self.prob_action = prob_action
        
        self.ts = None
        self.len_unfiltered = 0
        self.predictor = None
        self.statistics = None
        
        # Create output folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
    def load_tilt_series(self):
        """Load tilt series from star file."""
        print(f"\n{'='*70}")
        print("Loading Tilt Series")
        print(f"{'='*70}")
        print(f"Star file: {self.tiltseries_star}")
        print(f"RELION project: {self.relion_proj}")
        
        self.ts=tiltSeriesMeta(self.tiltseries_star,self.relion_proj) #relion_proj is not being used, remove it?

        self.len_unfiltered = len(self.ts.all_tilts_df)
        
        print(f"Loaded {self.len_unfiltered} tilts from {self.ts.nrTomo} tomograms")
        print(f"{'='*70}\n")
        
        return self.ts
    
    def apply_rule_filter(self, param_rule_filter, plot=None):
        #TODO: implement rule-based filtering logic based on param_rule_filter?
        """
        Apply rule-based filtering.
        
        Parameters:
        - param_rule_filter: Dictionary of filter parameters
        - plot: Plotting option
        """
        if param_rule_filter is None:
            print("No rule-based filter parameters provided, skipping...")
            return
        
        print(f"\n{'='*70}")
        print("Applying Rule-Based Filters")
        print(f"{'='*70}")
        print(f"Filter parameters: {param_rule_filter}")
        print(f"Rule-based filtering complete")
        print(f"{'='*70}\n")

    def apply_dl_filter(self, model_path='default', batch_size=60, gpu=0, 
                       sz=384, save_pngs=True, plot=None):
        """
        Apply deep learning filtering.
        
        Parameters:
        - model_path: Path to model file, or model name ('default', 'binary', 'oneclass')
        - batch_size: Batch size for inference
        - gpu: GPU device number
        - sz: Target image size
        - save_pngs: Whether to save PNG files
        - plot: Plotting option
        
        Returns:
        - Filtered tiltSeriesMeta object
        """
        if self.ts is None:
            raise ValueError("Tilt series not loaded. Call load_tilt_series() first.")
        
        print(f"\n{'='*70}")
        print("Deep Learning Filter")
        print(f"{'='*70}")
        print(f"Model: {model_path}")
        print(f"Output Folder: {self.output_folder}")
        print(f"Probability Threshold: {self.prob_thr}")
        print(f"Probability Action: {self.prob_action}")
        print(f"{'='*70}\n")
        
        # Create predictor instance
        self.predictor = TiltSeriesPredictor(
            model_path=model_path,
            output_folder=str(self.output_folder),
            sz=sz,
            batch_size=batch_size,
            gpu=gpu,
            prob_thr=self.prob_thr,
            prob_action=self.prob_action,
            threads=self.threads,
            num_dataloader_workers=4,
            save_pngs=save_pngs
        )
        
        # Run prediction
        self.ts = self.predictor.predict(self.ts)
        
        # Plot results if requested
        if plot:
            try:
                print("\nGenerating filter results plots...")
                plotter = FilterPlotter(str(self.output_folder), threads=self.threads)
                plotter.plot_filter_results(
                    self.ts,
                    class_label_name="cryoBoostDlLabel",
                    pred_score_label_name="cryoBoostDlProbability",
                    tilt_name_label="cryoBoostKey",
                    plot=plot
                )
                print("Plots generated successfully")
            except Exception as e:
                print(f"Error plotting filter results: {e}")
        
        print(f"\n{'='*70}")
        print("Deep Learning Filter - Complete")
        print(f"{'='*70}\n")
        
        return self.ts
    
    def merge_with_existing(self):
        """Merge with existing labeled star file if available."""
        existing_star = self.output_folder / "tiltseries_labeled.star"
        
        if not existing_star.exists():
            print("No existing labeled star file found, skipping merge...")
            return
        
        print(f"\n{'='*70}")
        print("Merging with Existing Labels")
        print(f"{'='*70}")
        print(f"Existing file: {existing_star}")
        
        # Load existing predictions
        existing_ts = tiltSeriesMeta(str(existing_star), str(self.relion_proj))
        print(f"Existing predictions loaded: {len(existing_ts.all_tilts_df)} tilts")
        print(f"{70*'/' } new ts tilts {type(self.ts)}: tilts")
        print(f"new ts tilts: {len(self.ts.all_tilts_df)} tilts")
        # Merge logic: keep existing predictions where available
        for col in ['cryoBoostDlLabel', 'cryoBoostDlProbability']:
            if col in existing_ts.all_tilts_df.columns:
                # Find rows that need updating (where existing has NaN)
                print(f"Type of things: {type(self.ts)}")
                mask = existing_ts.all_tilts_df[col].isna()
                if col in self.ts.all_tilts_df.columns:
                    existing_ts.all_tilts_df.loc[mask, col] = self.ts.all_tilts_df.loc[mask, col]
                    n_updated = mask.sum()
                    print(f"Updated {n_updated} entries in column '{col}'")
        
        self.ts = existing_ts
        print(f"Merge complete")
        print(f"{'='*70}\n")
    
    def calculate_statistics(self):
        """Calculate and display filter statistics."""
        if self.ts is None:
            raise ValueError("Tilt series not loaded.")
        
        # Create statistics object
        self.statistics = FilterStatistics(
            self.ts, 
            self.len_unfiltered, 
            prob_threshold=self.prob_thr
        )
        
        # Evaluate distribution if DL predictions exist
        if 'cryoBoostDlProbability' in self.ts.all_tilts_df.columns:
            self.statistics.evaluate_distribution()
        
        # Print summary
        self.statistics.print_summary()
        
        print("Statistics calculation complete")
        
        return self.statistics
    
    
    def write_output_files(self):
        """Write all output files (both star files and distribution marker)."""
        if self.ts is None:
            raise ValueError("Tilt series not loaded.")
        
        print(f"\n{'='*70}")
        print("Writing Output Files")
        print(f"{'='*70}")
        
        # 1. Write labeled star file (all tilts with predictions)
        output_labeled = self.output_folder / "tiltseries_labeled.star"
        self.ts.writeTiltSeries(tiltseriesStarFile=str(output_labeled), tiltSeriesStarFolder='tiltseries_labeled')
        print(f"✓ Labeled star file: {output_labeled}")

        #print(f"row of position 1 {self.ts.tilt_series_df.loc[self.ts.tilt_series_df['rlnTomoTiltSeriesStarFile'] == '2026-02-19-14-48-50_Position_1']}")
        print(f"\n {'n'*50} first row of initial unfiltered ts tilts: {self.ts.all_tilts_df.iloc[0]}")

        # 2. Write filtered star file (only good tilts)
        if 'cryoBoostDlLabel' in self.ts.all_tilts_df.columns:
            good_tilts = self.ts.all_tilts_df[self.ts.all_tilts_df['cryoBoostDlLabel'] == 'good']
            
            if len(good_tilts) > 0:
                filtered_ts = self.ts
                filtered_ts.filterTilts({'cryoBoostDlLabel': 'good'})
                print(f"Filtered star file written with {len(filtered_ts.all_tilts_df)} good tilts")
                output_filtered = self.output_folder / "tiltseries_filtered.star"
                print(f"Tilt series filtered: {output_filtered}")
                filtered_ts.writeTiltSeries(tiltseriesStarFile=str(output_filtered), tiltSeriesStarFolder='tiltseries_filtered')

                n_good = len(good_tilts)
                n_total = len(self.ts.all_tilts_df)
                pct = (n_good / n_total * 100) if n_total > 0 else 0
                print(f"  Kept {n_good}/{n_total} tilts ({pct:.1f}%)")
            else:
                print("⚠ No good tilts found, skipping filtered star file")

        # 3. Write distribution marker (statistics summary and whether filtering should be trusted)
        if self.statistics is not None:
            marker_file = self.output_folder / "distribution_check.txt"
            is_ood = self.statistics.is_out_of_distribution
            
            with open(marker_file, 'w') as f:
                # Header
                f.write(f"Distribution Health: {'WARNING' if is_ood else 'PASS'}\n")
                f.write(f"{'='*70}\n\n")
                
                # Warning details if OOD
                if is_ood:
                    f.write("⚠️  WARNING: Data appears out of distribution!\n")
                    f.write("Consider manual sorting or retraining the model.\n\n")
                    f.write("Issues detected:\n")
                    
                    if self.statistics.mean_prob < 0.95:
                        f.write(f"  - Low mean probability: {self.statistics.mean_prob:.3f} (expected > 0.95)\n")
                    if self.statistics.mean_ang_good > (self.statistics.mean_ang_bad - 2):
                        f.write(f"  - Good tilts at high angles: {self.statistics.mean_ang_good:.1f}° vs bad {self.statistics.mean_ang_bad:.1f}°\n")
                    if self.statistics.bad_fraction > 25:
                        f.write(f"  - High bad fraction: {self.statistics.bad_fraction}% (expected < 25%)\n")
                else:
                    f.write("✅ Removal of bad tilts successful\n")
                    f.write("Data appears to be in distribution.\n")
                
                # Full statistics summary
                f.write(f"\n{self.statistics.print_summary()}\n")
            
            print(f"✓ Distribution check: {marker_file}")
        else:
            print("⚠ No statistics available, skipping distribution marker")
        
        print(f"{'='*70}\n")


    def execute(self, param_rule_filter=None, model=None, plot=None, 
                mdoc_wk=None, batch_size=60, gpu=0, sz=384, save_pngs=True):
        """
        Execute complete filtering pipeline.
        
        Parameters:
        - param_rule_filter: Rule-based filter parameters
        - model: Path to DL model (or 'default')
        - plot: Plotting option
        - mdoc_wk: Warp project MDOC settings
        - batch_size: Batch size for DL inference
        - gpu: GPU device number
        - sz: Target image size
        - save_pngs: Whether to save PNG files
        """
        print(f"\n{'#'*70}")
        print("TILT SERIES FILTERING PIPELINE")
        print(f"{'#'*70}\n")
        import time
        start_time = time.time()
        
        # Step 1: Load tilt series
        self.load_tilt_series()
        
        # Step 2: Apply rule-based filters
        if param_rule_filter:
            self.apply_rule_filter(param_rule_filter, plot)
        
        # Step 3: Apply deep learning filter
        if model:
            self.apply_dl_filter(
                model_path=model,
                batch_size=batch_size,
                gpu=gpu,
                sz=sz,
                save_pngs=save_pngs,
                plot=plot
            )
        
        # Step 4: Merge with existing predictions
        self.merge_with_existing()

        print(f"\n{'#'*70} merge complete, now calculating statistics and writing outputs {('#'*70)}\n")
        # Step 5: Calculate statistics
        self.calculate_statistics()
        
        # Step 6: Write output files (two star files and statistics distribution marker of tilt-removal)
        self.write_output_files()
        
        # Step 8: Handle Warp project if needed
        if mdoc_wk:
            #print(f'\n{"#"*70} warp project now')
            warp_handler = WarpProjectHandler(
                Path(self.tiltseries_star).parent,
                #self.tiltseries_star.parent,
                self.output_folder,
                mdoc_wk
            )
            output_star = self.output_folder / "tiltseries_filtered.star"
            warp_handler.filter_mdocs(self.ts, str(output_star))
        
        print('got all the way here, now plotting')
        # Step 9: Plot results
        if plot:
            plotter = FilterPlotter(str(self.output_folder), threads=self.threads)
            plotter.plot_tilt_statistics(self.ts, plot)
        
        print(f"\n{'#'*70}")
        print("PIPELINE COMPLETE")
        end_time = time.time()
        duration = end_time - start_time
        print(f"Total duration: {duration:.2f} seconds")
        print(f"{'#'*70}\n")
        
# %%
