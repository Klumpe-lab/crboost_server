# services/metadata_service.py
"""
Service for translating WarpTools metadata to Relion STAR format.
Bridges the gap between fsMotionAndCtf output and downstream jobs.
"""

import glob
import os
from pathlib import Path
import sys
from typing import Dict, Optional
import xml.etree.ElementTree as ET
import pandas as pd
from services.starfile_service import StarfileService
import numpy as np


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

    def update_ts_alignment_metadata(
        self,
        job_dir: Path,
        input_star_path: Path, # Absolute path to *original* tiltseries.star
        output_star_path: Path, # Path for new aligned_tilt_series.star
        tomo_dimensions: str, # e.g., "4096x4096x2048"
        alignment_program: str, # "Aretomo" or "Imod"
    ) -> Dict:
        """
        Updates STAR files with alignment data from AreTomo/Imod.
        Ported from old tsAlignment.py::updateMetaData
        """
        try:
            print(f"[METADATA] Starting tsAlignment update for {input_star_path}")
            
            # 1. Read the *input* tilt series STAR (e.g., from fsMotionAndCtf)
            # We need the absolute path to resolve the individual tilt_series files
            input_star_dir = input_star_path.parent
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get('global')
            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")

            # 2. Prepare output paths
            output_star_path.parent.mkdir(parents=True, exist_ok=True)
            output_tilts_dir = output_star_path.parent / "tilt_series"
            output_tilts_dir.mkdir(exist_ok=True)

            pixS = float(in_ts_df["rlnTomoTiltSeriesPixelSize"].iloc[0])
            ts_id_failed = []
            
            # This will hold the individual star files
            updated_tilt_dfs = {} 
            all_tilts_list = [] # This will hold all tilts for the new .all_tilts_df

            # 3. Loop over each tilt series defined in the input global star
            for _, ts_row in in_ts_df.iterrows():
                ts_star_file_rel = ts_row["rlnTomoTiltSeriesStarFile"]
                ts_id = Path(ts_star_file_rel).stem # e.g., "Position_1"
                
                # Read the individual tilt star file (e.g., .../job002/tilt_series/Position_1.star)
                ts_star_path_abs = (input_star_dir / ts_star_file_rel).resolve()
                if not ts_star_path_abs.exists():
                    print(f"[WARN] Missing tilt star: {ts_star_path_abs}, skipping {ts_id}")
                    ts_id_failed.append(ts_id)
                    continue

                ts_data_in = self.starfile_service.read(ts_star_path_abs)
                ts_tilts_df = next(iter(ts_data_in.values())).copy() # Get first data block

                # 4. Find and parse alignment file for this tilt series
                aln_data = None
                if alignment_program == "Aretomo":
                    aln_file = job_dir / f"warp_tiltseries/tiltstack/{ts_id}/{ts_id}.st.aln"
                    aln_data = self._read_aretomo_aln_file(aln_file)
                elif alignment_program == "Imod":
                    xf_file = job_dir / f"warp_tiltseries/tiltstack/{ts_id}/{ts_id}.xf"
                    tlt_file = job_dir / f"warp_tiltseries/tiltstack/{ts_id}/{ts_id}.tlt"
                    aln_data = self._read_imod_xf_tlt_files(xf_file, tlt_file)
                else:
                    raise ValueError(f"Unknown alignment program: {alignment_program}")

                if aln_data is None:
                    print(f"[WARN] Alignment failed/file missing for {ts_id}, skipping.")
                    ts_id_failed.append(ts_id)
                    continue
                
                # 5. Apply alignment data to the tilt DataFrame
                # Ported from old logic
                aln_data = aln_data[aln_data[:, 0].argsort()] # Sort by index
                keys_rel = [Path(p).name for p in ts_tilts_df["rlnMicrographMovieName"]]

                # We need the .tomostar file to map indices
                tomostar_file = job_dir / f"tomostar/{ts_id}.tomostar"
                if not tomostar_file.exists():
                    print(f"[WARN] Missing {tomostar_file} for {ts_id}, skipping.")
                    ts_id_failed.append(ts_id)
                    continue
                
                tomostar_df = self.starfile_service.read(tomostar_file)['global']

                for index, tomo_row in tomostar_df.iterrows():
                    key_tomo = Path(tomo_row["wrpMovieName"]).name
                    try:
                        position = keys_rel.index(key_tomo)
                    except ValueError:
                        print(f"[WARN] Movie {key_tomo} from {tomostar_file} not found in {ts_star_path_abs}, skipping tilt.")
                        continue

                    ts_tilts_df.at[position, "rlnTomoXTilt"] = 0
                    ts_tilts_df.at[position, "rlnTomoYTilt"] = -1.0 * aln_data[index, 9] # multTiltAngle = -1
                    ts_tilts_df.at[position, "rlnTomoZRot"] = aln_data[index, 1]
                    ts_tilts_df.at[position, "rlnTomoXShiftAngst"] = aln_data[index, 3] * pixS
                    ts_tilts_df.at[position, "rlnTomoYShiftAngst"] = aln_data[index, 4] * pixS
                
                # Store for writing
                updated_tilt_dfs[ts_id] = ts_tilts_df
                # Add ts_row data to all individual tilts for merging
                ts_row_df = pd.concat([pd.DataFrame(ts_row).T] * len(ts_tilts_df), ignore_index=True)
                ts_row_df.index = ts_tilts_df.index
                all_tilts_list.append(pd.concat([ts_row_df, ts_tilts_df], axis=1))

            # 6. Check for total failure
            if len(ts_id_failed) == len(in_ts_df):
                raise Exception("Alignment failed for all tilt series. Check job logs.")

            # 7. Write new individual STAR files
            for ts_id, tilts_df in updated_tilt_dfs.items():
                out_ts_star = output_tilts_dir / f"{ts_id}.star"
                self.starfile_service.write({ts_id: tilts_df}, out_ts_star)

            # 8. Create and write new main STAR file
            # Filter out failed tilt series
            out_ts_df = in_ts_df[~in_ts_df["rlnTomoName"].isin(ts_id_failed)].copy()
            
            # Update paths to point to new tilt_series directory
            out_ts_df["rlnTomoTiltSeriesStarFile"] = out_ts_df["rlnTomoName"].apply(
                lambda x: f"tilt_series/{x}.star"
            )
            
            # Add tomo dimensions
            dims = tomo_dimensions.split('x')
            out_ts_df["rlnTomoSizeX"] = int(dims[0])
            out_ts_df["rlnTomoSizeY"] = int(dims[1])
            out_ts_df["rlnTomoSizeZ"] = int(dims[2])
            
            self.starfile_service.write({'global': out_ts_df}, output_star_path)

            # 9. (Optional) Write .all_tilts_df if needed
            if all_tilts_list:
                all_tilts_df = pd.concat(all_tilts_list, ignore_index=True)
                self.starfile_service.write(
                    {'all_tilts': all_tilts_df},
                    output_star_path.parent / "all_tilts.star"
                )

            print(f"[METADATA] tsAlignment update complete. Wrote to {output_star_path}")
            return {"success": True, "message": f"Wrote {len(out_ts_df)} aligned tilt series."}

        except Exception as e:
            print(f"[ERROR] tsAlignment metadata update failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return {"success": False, "error": str(e)}

    def _read_aretomo_aln_file(self, aln_file: Path) -> Optional[np.ndarray]:
        """Parses AreTomo .aln file."""
        if not aln_file.exists():
            print(f"Warning: {aln_file} not found", file=sys.stderr)
            return None
        data = []
        with open(aln_file, "r") as f:
            for line in f:
                if line.startswith("# Local Alignment"):
                    break
                if not line.startswith("#"):
                    try:
                        numbers = [float(x) for x in line.split()]
                        if numbers:
                            data.append(numbers)
                    except ValueError:
                        continue
        return np.array(data) if data else None

    def _read_imod_xf_tlt_files(self, xf_file: Path, tlt_file: Path) -> Optional[np.ndarray]:
        """Parses IMOD .xf and .tlt files."""
        if not xf_file.exists() or not tlt_file.exists():
            print(f"Warning: {xf_file} or {tlt_file} not found", file=sys.stderr)
            return None
        
        df1 = pd.read_csv(
            xf_file, delim_whitespace=True, header=None,
            names=["m1", "m2", "m3", "m4", "tx", "ty"],
        )
        df2 = pd.read_csv(
            tlt_file, delim_whitespace=True, header=None, names=["tilt_angle"]
        )
        combined = pd.concat([df1, df2], axis=1)

        results_x, results_y, titlAng = [], [], []
        for index, row in combined.iterrows():
            M = np.array([[row["m1"], row["m2"]], [row["m3"], row["m4"]]])
            M = np.linalg.inv(M)
            v = np.array([row["tx"], row["ty"]]) * -1
            result = np.dot(M, v)
            angle = np.degrees(np.arctan2(M[1, 0], M[0, 0]))
            results_x.append(result[0])
            results_y.append(result[1])
            titlAng.append(angle)

        data_np = np.zeros((len(combined), 10))
        data_np[:, 0] = np.arange(0, len(combined)) # Index
        data_np[:, 1] = titlAng # ZRot
        data_np[:, 3] = results_x # XShift
        data_np[:, 4] = results_y # YShift
        data_np[:, 9] = combined["tilt_angle"] # TiltAngle
        return data_np


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