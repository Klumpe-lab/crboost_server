# services/metadata_service.py
"""
Service for translating WarpTools metadata to Relion STAR format.
Bridges the gap between fsMotionAndCtf output and downstream jobs.
"""

import glob
import os
from pathlib import Path
from typing import Dict, Optional
import xml.etree.ElementTree as ET
import pandas as pd
from services.starfile_service import StarfileService


class WarpXmlParser:
    """Parses WarpTools XML files to extract CTF and processing metadata"""
    
    def __init__(self, xml_pattern: str):
        """
        Args:
            xml_pattern: Glob pattern for XML files (e.g., "warp_frameseries/*.xml")
        """
        self.data_df = pd.DataFrame()
        self._parse_xml_files(xml_pattern)
    
    def _parse_xml_files(self, pattern: str):
        """Parse all XML files matching the pattern"""
        xml_files = glob.glob(pattern)
        if not xml_files:
            raise FileNotFoundError(f"No XML files found matching: {pattern}")
        
        for xml_path in xml_files:
            file_type = self._check_xml_type(xml_path)
            if file_type == 'fs':  # Frame series
                df = self._parse_frame_series_xml(xml_path)
            else:  # Tilt series
                df = self._parse_tilt_series_xml(xml_path)
            
            self.data_df = pd.concat([self.data_df, df], ignore_index=True)
    
    @staticmethod
    def _check_xml_type(xml_path: str) -> str:
        """Check if XML is frame series or tilt series"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        return 'fs' if root.find('MoviePath') is None else 'ts'
    
    def _parse_frame_series_xml(self, xml_path: str) -> pd.DataFrame:
        """Parse frame series XML to extract CTF parameters"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        ctf = root.find(".//CTF")
        
        if ctf is None:
            raise ValueError(f"No CTF data found in {xml_path}")
        
        data = {
            "cryoBoostKey": Path(xml_path).stem,  # Filename without .xml
            "folder": str(Path(xml_path).parent),
            "defocus_value": float(ctf.find(".//Param[@Name='Defocus']").get('Value')),
            "defocus_angle": float(ctf.find(".//Param[@Name='DefocusAngle']").get('Value')),
            "defocus_delta": float(ctf.find(".//Param[@Name='DefocusDelta']").get('Value')),
        }
        
        return pd.DataFrame([data])
    
    def _parse_tilt_series_xml(self, xml_path: str) -> pd.DataFrame:
        """Parse tilt series XML (for future use)"""
        # Implement if needed for tilt series alignment
        raise NotImplementedError("Tilt series XML parsing not yet implemented")


class MetadataTranslator:
    """Translates WarpTools metadata to Relion STAR format"""
    
    def __init__(self, starfile_service: Optional[StarfileService] = None):
        self.starfile_service = starfile_service or StarfileService()
    
    def update_fs_motion_and_ctf_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        warp_folder: str = "warp_frameseries"
    ) -> Dict:
        """
        Main entry point: Update tilt series STAR file with WarpTools results
        
        Args:
            job_dir: Job directory (e.g., External/job002)
            input_star_path: Input tilt series STAR (from importmovies)
            output_star_path: Output STAR path (fs_motion_and_ctf.star)
            warp_folder: Name of WarpTools output folder
            
        Returns:
            Dict with success status and message
        """
        try:
            # 1. Parse WarpTools XML files
            xml_pattern = str(job_dir / warp_folder / "*.xml")
            warp_data = WarpXmlParser(xml_pattern)
            print(f"[METADATA] Parsed {len(warp_data.data_df)} XML files")
            
            # 2. Read input tilt series STAR file
            star_data = self.starfile_service.read(input_star_path)
            tilt_series_df = star_data.get('global', pd.DataFrame())
            
            if tilt_series_df.empty:
                raise ValueError(f"No tilt series data in {input_star_path}")
            
            # 3. Read individual tilt series STAR files
            all_tilts_df = self._load_all_tilt_series(
                input_star_path.parent, tilt_series_df
            )
            
            # 4. Update metadata
            updated_df = self._merge_warp_metadata(
                all_tilts_df, warp_data.data_df, job_dir / warp_folder
            )
            
            # 5. Write output STAR file
            self._write_updated_star(
                updated_df, tilt_series_df, output_star_path
            )
            
            return {
                "success": True,
                "message": f"Updated {len(updated_df)} tilts with WarpTools metadata",
                "output_path": str(output_star_path)
            }
            
        except Exception as e:
            print(f"[ERROR] Metadata update failed: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def _load_all_tilt_series(
        self, 
        base_dir: Path, 
        tilt_series_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Load all individual tilt series STAR files into one DataFrame"""
        all_tilts = []
        
        for ts_file in tilt_series_df['rlnTomoTiltSeriesStarFile']:
            ts_path = base_dir / ts_file
            if not ts_path.exists():
                print(f"[WARN] Tilt series file not found: {ts_path}")
                continue
            
            ts_data = self.starfile_service.read(ts_path)
            # Get the first (and usually only) data block
            ts_df = next(iter(ts_data.values()))
            
            # Create a lookup key from the movie name
            ts_df['cryoBoostKey'] = ts_df['rlnMicrographMovieName'].apply(
                lambda x: Path(x).stem  # Remove extension
            )
            
            all_tilts.append(ts_df)
        
        return pd.concat(all_tilts, ignore_index=True)
    
    def _merge_warp_metadata(
        self,
        tilts_df: pd.DataFrame,
        warp_df: pd.DataFrame,
        warp_folder: Path
    ) -> pd.DataFrame:
        """
        Merge WarpTools XML data into tilt series DataFrame
        
        Key transformations:
        - Update paths to point to motion-corrected averages
        - Convert CTF values from microns to Angstroms
        - Add CTF quality metrics
        """
        updated_df = tilts_df.copy()
        
        for index, row in updated_df.iterrows():
            key = row['cryoBoostKey']
            
            # Find matching WarpTools data
            matches = warp_df[warp_df['cryoBoostKey'] == key]
            
            if matches.empty:
                print(f"[WARN] No WarpTools data for {key}")
                continue
            
            warp_row = matches.iloc[0]
            base_name = key.replace(".eer", "").replace(".tif", "")
            
            # Update paths to motion-corrected outputs
            updated_df.at[index, 'rlnMicrographName'] = \
                f"{warp_row['folder']}/average/{base_name}.mrc"
            updated_df.at[index, 'rlnMicrographNameEven'] = \
                f"{warp_row['folder']}/average/even/{base_name}.mrc"
            updated_df.at[index, 'rlnMicrographNameOdd'] = \
                f"{warp_row['folder']}/average/odd/{base_name}.mrc"
            
            # Update CTF parameters (convert microns to Angstroms)
            defocus_angstroms = warp_row['defocus_value'] * 10000.0
            delta_angstroms = warp_row['defocus_delta'] * 10000.0
            
            updated_df.at[index, 'rlnDefocusU'] = defocus_angstroms
            updated_df.at[index, 'rlnDefocusV'] = defocus_angstroms
            updated_df.at[index, 'rlnCtfAstigmatism'] = delta_angstroms
            updated_df.at[index, 'rlnDefocusAngle'] = warp_row['defocus_angle']
            
            # Add CTF diagnostic outputs
            updated_df.at[index, 'rlnCtfImage'] = \
                f"{warp_row['folder']}/powerspectrum/{base_name}.mrc"
            
            # Placeholder values (WarpTools doesn't provide these directly)
            updated_df.at[index, 'rlnAccumMotionTotal'] = 0.000001
            updated_df.at[index, 'rlnAccumMotionEarly'] = 0.000001
            updated_df.at[index, 'rlnAccumMotionLate'] = 0.000001
            updated_df.at[index, 'rlnCtfMaxResolution'] = 0.000001
            updated_df.at[index, 'rlnMicrographMetadata'] = "None"
            updated_df.at[index, 'rlnCtfFigureOfMerit'] = "None"
        
        return updated_df
    
    def _write_updated_star(
        self,
        tilts_df: pd.DataFrame,
        tilt_series_df: pd.DataFrame,
        output_path: Path
    ):
        """Write updated metadata to STAR file"""
        # Create output directory structure
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tilt_series_dir = output_path.parent / "tilt_series"
        tilt_series_dir.mkdir(exist_ok=True)
        
        # Update tilt series paths
        updated_ts_df = tilt_series_df.copy()
        updated_ts_df['rlnTomoTiltSeriesStarFile'] = \
            updated_ts_df['rlnTomoTiltSeriesStarFile'].apply(
                lambda x: f"tilt_series/{Path(x).name}"
            )
        
        # Write main tilt series STAR file
        self.starfile_service.write(
            {'global': updated_ts_df},
            output_path
        )
        
        # Write individual tilt series files
        for ts_name in updated_ts_df['rlnTomoName']:
            ts_tilts = tilts_df[tilts_df['rlnTomoName'] == ts_name].copy()
            
            # Remove the cryoBoostKey helper column
            if 'cryoBoostKey' in ts_tilts.columns:
                ts_tilts = ts_tilts.drop('cryoBoostKey', axis=1)
            
            ts_file = tilt_series_dir / f"{ts_name}.star"
            self.starfile_service.write(
                {ts_name: ts_tilts},
                ts_file
            )
        
        print(f"[METADATA] Wrote updated STAR files to {output_path}")


# Convenience function for easy import
def update_fs_motion_ctf_metadata(
    job_dir: Path,
    input_star: Path,
    output_star: Path
) -> Dict:
    """
    Convenience function to update fsMotionAndCtf metadata.
    Call this after the WarpTools job completes.
    """
    translator = MetadataTranslator()
    return translator.update_fs_motion_and_ctf_metadata(
        job_dir, input_star, output_star
    )