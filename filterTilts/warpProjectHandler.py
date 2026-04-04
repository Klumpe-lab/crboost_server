"""Warp project integration utilities."""

import shutil
from pathlib import Path
from src.rw.librw import mdocMeta


class WarpProjectHandler:
    """Handle Warp project specific operations."""
    
    def __init__(self, pre_exp_folder, output_folder, mdoc_wk=None):
        """
        Initialize Warp project handler.
        
        Parameters:
        - pre_exp_folder: Source folder (contains warp data)
        - output_folder: Output folder for filtered data
        - mdoc_wk: Wildcard pattern for mdoc files
        """
        self.pre_exp_folder = Path(pre_exp_folder)
        self.output_folder = Path(output_folder)
        self.mdoc_wk = mdoc_wk
        self.is_warp_project = False
        
    def detect_warp_project(self):
        """Check if this is a Warp project."""
        self.is_warp_project = (
            self.pre_exp_folder / "warp_frameseries.settings"
        ).exists()
        return self.is_warp_project
        
    def setup_warp_data(self):
        """Set up symlinks and copy settings for Warp projects."""
        if not self.is_warp_project:
            return
            
        print(f"\nWarp frame alignment detected")
        print(f"Getting data from: {self.pre_exp_folder}")
        
        self._create_symlinks()
        self._copy_settings()
        
    def _create_symlinks(self):
        """Create symlinks to Warp frameseries data."""
        fs_source = (self.pre_exp_folder / "warp_frameseries").resolve()
        fs_target = (self.output_folder / "warp_frameseries").resolve()
        
        if fs_target.exists():
            print(f"Symlink already exists: {fs_target}")
        else:
            print(f"Creating symlink:")
            print(f"  ln -s {fs_source} {fs_target}")
            fs_target.symlink_to(fs_source)
            
    def _copy_settings(self):
        """Copy Warp settings file."""
        settings_source = self.pre_exp_folder / "warp_frameseries.settings"
        settings_target = self.output_folder / "warp_frameseries.settings"
        
        print(f"Copying settings file:")
        print(f"  cp {settings_source} {settings_target}")
        shutil.copyfile(settings_source, settings_target)
        
    def filter_mdocs(self, ts, filtered_star_file):
        """
        Filter mdoc files based on filtered tilt series.
        
        Parameters:
        - ts: tiltSeriesMeta object with filtered data
        - filtered_star_file: Path to filtered star file
        
        Returns:
        - True if successful, False otherwise
        """
        if not self.is_warp_project or len(ts.tilt_series_df) == 0:
            return False
            
        mdoc_output = self.output_folder / "mdoc"
        mdoc_output.mkdir(parents=True, exist_ok=True)
        
        print(f"\nFiltering mdoc files: {self.mdoc_wk}")
        
        mdoc_pattern = str(Path(self.mdoc_wk).parent / "*.mdoc")
        mdoc = mdocMeta(mdoc_pattern)
        mdoc.filterByTiltSeriesStarFile(str(filtered_star_file))
        
        print(f"Filtered mdoc has {len(mdoc.all_df)} tilts")
        mdoc.writeAllMdoc(str(mdoc_output))
        
        return True