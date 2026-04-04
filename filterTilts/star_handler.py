from pathlib import Path
from src.rw.librw import tiltSeriesMeta


def add_predictions_to_ts(ts, pred_labels, pred_probs, 
                         label_column='cryoBoostDlLabel', 
                         prob_column='cryoBoostDlProbability'):
    """
    Add prediction results to tilt series metadata.
    
    Parameters:
    - ts: tiltSeriesMeta object
    - pred_labels: List of predicted labels
    - pred_probs: List of prediction probabilities
    - label_column: Column name for labels
    - prob_column: Column name for probabilities
    
    Returns:
    - Updated tiltSeriesMeta object (same object, modified in place)
    """
    ts.all_tilts_df[label_column] = pred_labels
    ts.all_tilts_df[prob_column] = pred_probs
    return ts

class StarFileHandler:
    """Handles reading and writing RELION star files for tilt series.
    Specifically for the deep learning part, to keep it cleaner."""
    
    def __init__(self, star_path, relion_proj=''):
        """
        Initialize star file handler.
        
        Parameters:
        - star_path: Path to the tiltseries star file
        - relion_proj: Path to RELION project folder
        """
        self.star_path = Path(star_path)
        self.relion_proj = Path(relion_proj) if relion_proj else Path.cwd()
        self.ts = None
        
    def load_tilt_series(self):
        """Load tilt series metadata from star file."""
        self.ts = tiltSeriesMeta(str(self.star_path), str(self.relion_proj))
        return self.ts
    
    def get_image_paths(self):
        """
        Get full paths to all tilt images.
        
        Returns:
        - List of paths to tilt images
        """
        if self.ts is None:
            self.load_tilt_series()
        
        return self.ts.getMicrographMovieNameFull()
    
    def get_tilt_count(self):
        """Get total number of tilts."""
        if self.ts is None:
            self.load_tilt_series()
        return len(self.ts.all_tilts_df)
    
    def get_tomogram_count(self):
        """Get number of tomograms."""
        if self.ts is None:
            self.load_tilt_series()
        return self.ts.nrTomo

    def write_star_file(self, output_path):
        """
        Write tilt series with predictions to new star file.
        
        Parameters:
        - output_path: Path for output star file
        """
        if self.ts is None:
            raise ValueError("Tilt series not loaded.")
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.ts.writeTiltSeries(str(output_path))
        
    def merge_with_existing(self, existing_star_path):
        """
        Merge predictions with existing labeled star file.
        
        Parameters:
        - existing_star_path: Path to existing labeled star file
        """
        if self.ts is None:
            raise ValueError("Tilt series not loaded.")
        
        existing_ts = tiltSeriesMeta(str(existing_star_path), str(self.relion_proj))
        
        # Merge logic: keep existing predictions, add new ones
        for col in ['cryoBoostDlLabel', 'cryoBoostDlProbability']:
            if col in existing_ts.all_tilts_df.columns:
                # Create mask for rows that need updating
                mask = existing_ts.all_tilts_df[col].isna()
                if col in self.ts.all_tilts_df.columns:
                    existing_ts.all_tilts_df.loc[mask, col] = self.ts.all_tilts_df.loc[mask, col]
        
        self.ts = existing_ts