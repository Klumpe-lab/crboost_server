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
import numpy as np
import pandas as pd
from services.configs.starfile_service import StarfileService
from services.project_state import AlignmentMethod

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
        """Parse tilt series XML to extract per-tilt CTF parameters"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Parse GridCTF (Defocus) - this determines how many valid entries we have
        grid_ctf = root.find('GridCTF')
        defocus_values = []
        z_values = []
        for node in grid_ctf.findall('Node'):
            value = float(node.get('Value'))
            z = int(node.get('Z'))
            defocus_values.append(value)
            z_values.append(z)
        
        num_entries = len(defocus_values)  # This is the authoritative count
        
        # Parse GridCTFDefocusDelta
        grid_delta = root.find('GridCTFDefocusDelta')
        delta_values = []
        for node in grid_delta.findall('Node'):
            value = float(node.get('Value'))
            delta_values.append(value)
            
        # Parse GridCTFDefocusAngle
        grid_angle = root.find('GridCTFDefocusAngle')
        angle_values = []
        for node in grid_angle.findall('Node'):
            value = float(node.get('Value'))
            angle_values.append(value)
            
        # Parse MoviePath - get ALL movie names, then slice to match grid length
        movie_paths_all = []
        for path in root.find('MoviePath').text.split('\n'):
            if path.strip():  # Skip empty lines
                # Get basename and remove extensions
                movie_name = os.path.basename(path).replace('_EER.eer', '')
                movie_name = movie_name.replace(".tif", "")
                movie_name = movie_name.replace(".eer", "")
                movie_paths_all.append(movie_name)
        
        # CRITICAL: Only use the first num_entries movies (grids might be shorter if some failed)
        movie_paths = movie_paths_all[:num_entries]
        
        # Verify all arrays have the same length
        if not (len(defocus_values) == len(delta_values) == len(angle_values) == len(movie_paths)):
            print(f"[WARN] Array length mismatch in {xml_path}:")
            print(f"  defocus: {len(defocus_values)}, delta: {len(delta_values)}, "
                f"angle: {len(angle_values)}, movies: {len(movie_paths)}")
            # Truncate all to the shortest length
            min_len = min(len(defocus_values), len(delta_values), len(angle_values), len(movie_paths))
            defocus_values = defocus_values[:min_len]
            delta_values = delta_values[:min_len]
            angle_values = angle_values[:min_len]
            movie_paths = movie_paths[:min_len]
            z_values = z_values[:min_len]
        
        # Create DataFrame
        df = pd.DataFrame({
            'Z'            : z_values,
            'defocus_value': defocus_values,
            'defocus_delta': delta_values,
            'defocus_angle': angle_values,
            'cryoBoostKey' : movie_paths
        })
        
        return df


class MetadataTranslator:
    """Translates WarpTools metadata to Relion STAR format"""
    
    def __init__(self, starfile_service: Optional[StarfileService] = None):
        self.starfile_service = starfile_service or StarfileService()

    def _read_aretomo_aln_file(self, aln_file: Path) -> Optional[np.ndarray]:
        """Parses AreTomo .aln file."""
        if not aln_file.exists():
            print(f"Warning: {aln_file} not found")
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
        
        if not data:
            print(f"Warning: No alignment data found in {aln_file}")
            return None
            
        return np.array(data)

    def _read_imod_xf_tlt_files(self, xf_file: Path, tlt_file: Path) -> Optional[np.ndarray]:
        """Parses IMOD .xf and .tlt files."""
        if not xf_file.exists() or not tlt_file.exists():
            print(f"Warning: {xf_file} or {tlt_file} not found")
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
        data_np[:, 0] = np.arange(0, len(combined))  # Index
        data_np[:, 1] = titlAng  # ZRot
        data_np[:, 3] = results_x  # XShift
        data_np[:, 4] = results_y  # YShift
        data_np[:, 9] = combined["tilt_angle"]  # TiltAngle
        return data_np
    
    def update_fs_motion_and_ctf_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        project_root: Path,  # NEW: Required parameter
        warp_folder: str = "warp_frameseries"
    ) -> Dict:
        try:
            xml_pattern = str(job_dir / warp_folder / "*.xml")
            warp_data = WarpXmlParser(xml_pattern)
            print(f"[METADATA] Parsed {len(warp_data.data_df)} XML files")
            
            star_data = self.starfile_service.read(input_star_path)
            tilt_series_df = star_data.get('global', pd.DataFrame())
            
            if tilt_series_df.empty:
                raise ValueError(f"No tilt series data in {input_star_path}")
            
            # ALWAYS use project root for path resolution
            all_tilts_df = self._load_all_tilt_series(project_root, input_star_path, tilt_series_df)
            
            updated_df = self._merge_warp_metadata(
                all_tilts_df, warp_data.data_df, job_dir / warp_folder
            )
            
            self._write_updated_star(
                updated_df, tilt_series_df, output_star_path
            )
            
            return {"success": True, "message": f"Updated {len(updated_df)} tilts", "output_path": str(output_star_path)}
            
        except Exception as e:
            print(f"[ERROR] Metadata update failed: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _load_all_tilt_series(
        self, 
        project_root: Path,
        input_star_path: Path,  # NEW: Pass the input STAR file path
        tilt_series_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Load all individual tilt series STAR files into one merged DataFrame.
        Resolves paths relative to the input STAR file's directory.
        """
        all_tilts = []
        
        input_star_dir = input_star_path.parent
        print(f"[METADATA] Loading tilt series relative to: {input_star_dir}")
        
        for i, (_, ts_row) in enumerate(tilt_series_df.iterrows()):
            ts_file = ts_row["rlnTomoTiltSeriesStarFile"]
            
            # Try multiple path resolution strategies in order of likelihood:
            paths_to_try = [
                input_star_dir / ts_file,  # 1. Relative to input STAR file (most likely)
                project_root / ts_file,    # 2. Relative to project root
            ]
            
            ts_path = None
            for path in paths_to_try:
                print(f"[DEBUG] Trying path: {path}")
                print(f"[DEBUG]   Path exists: {path.exists()}")
                if path.exists():
                    ts_path = path
                    print(f"[DEBUG]   ✓ Using this path")
                    break
                else:
                    print(f"[DEBUG]   ✗ Path does not exist")
            
            if ts_path is None:
                print(f"[WARN] Tilt series file not found: {ts_file}")
                print(f"[WARN] Tried the following locations:")
                for path in paths_to_try:
                    print(f"[WARN]   - {path}")
                continue
            
            print(f"[METADATA] Loading tilt series from: {ts_path}")
            
            try:
                ts_data = self.starfile_service.read(ts_path)
                ts_df = next(iter(ts_data.values()))
                
                # Create a lookup key from the movie name
                ts_df['cryoBoostKey'] = ts_df['rlnMicrographMovieName'].apply(
                    lambda x: Path(x).stem
                )
                
                # Repeat the tilt_series row for each tilt
                ts_row_repeated = pd.concat(
                    [pd.DataFrame(ts_row).T] * len(ts_df), 
                    ignore_index=True
                )
                
                # Merge horizontally
                merged = pd.concat([ts_row_repeated.reset_index(drop=True), ts_df.reset_index(drop=True)], axis=1)
                all_tilts.append(merged)
                
            except Exception as e:
                print(f"[ERROR] Failed to load tilt series file {ts_path}: {e}")
                continue
        
        if not all_tilts:
            raise ValueError(f"No tilt series files could be loaded. Checked relative to: {input_star_dir}")
        
        all_tilts_df = pd.concat(all_tilts, ignore_index=True)
        
        # Move cryoBoostKey to the end
        key_values = all_tilts_df['cryoBoostKey']
        all_tilts_df = all_tilts_df.drop('cryoBoostKey', axis=1)
        all_tilts_df['cryoBoostKey'] = key_values
        
        print(f"[METADATA] Loaded {len(all_tilts_df)} individual tilts from {len(tilt_series_df)} tilt series")
        return all_tilts_df

    def _write_updated_star(
        self,
        tilts_df: pd.DataFrame,
        tilt_series_df: pd.DataFrame,
        output_path: Path
    ):
        """
        Write updated metadata to STAR file.
        Replicates old tiltSeriesMeta.writeTiltSeries() behavior.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tilt_series_dir = output_path.parent / "tilt_series"
        tilt_series_dir.mkdir(exist_ok=True)
        
        # Extract tilt series info from the merged dataframe
        # The first N columns are the tilt series columns
        num_ts_cols = len(tilt_series_df.columns)
        ts_df = tilts_df.iloc[:, :num_ts_cols].copy()
        ts_df = ts_df.drop('cryoBoostKey', axis=1, errors='ignore')
        ts_df = ts_df.drop_duplicates().reset_index(drop=True)
        
        # Update paths to point to new tilt_series directory
        ts_df['rlnTomoTiltSeriesStarFile'] = ts_df['rlnTomoTiltSeriesStarFile'].apply(
            lambda x: f"tilt_series/{Path(x).name}"
        )
        
        # Write main tilt series STAR file
        self.starfile_service.write({'global': ts_df}, output_path)
        
        # Write individual tilt series files
        for ts_name in ts_df['rlnTomoName']:
            # Get all tilts for this tilt series
            ts_tilts = tilts_df[tilts_df['rlnTomoName'] == ts_name].copy()
            
            # Extract only the per-tilt columns (after the tilt series columns)
            ts_tilts_only = ts_tilts.iloc[:, num_ts_cols:].copy()
            
            # Remove the cryoBoostKey helper column
            if 'cryoBoostKey' in ts_tilts_only.columns:
                ts_tilts_only = ts_tilts_only.drop('cryoBoostKey', axis=1)
            
            ts_file = tilt_series_dir / f"{ts_name}.star"
            self.starfile_service.write({ts_name: ts_tilts_only}, ts_file)
        
        print(f"[METADATA] Wrote updated STAR files to {output_path}")
        
    def _merge_warp_metadata(
        self,
        tilts_df: pd.DataFrame,
        warp_df: pd.DataFrame,
        warp_folder: Path
    ) -> pd.DataFrame:
        """
        Merge WarpTools XML data into tilt series DataFrame
        EXACTLY matches old CryoBoost behavior
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
            
            # OLD LOGIC: Update paths to motion-corrected outputs
            updated_df.at[index, 'rlnMicrographName'] = \
                f"{warp_row['folder']}/average/{base_name}.mrc"
            updated_df.at[index, 'rlnMicrographNameEven'] = \
                f"{warp_row['folder']}/average/even/{base_name}.mrc"
            updated_df.at[index, 'rlnMicrographNameOdd'] = \
                f"{warp_row['folder']}/average/odd/{base_name}.mrc"
            
            # OLD LOGIC: Update CTF parameters (convert microns to Angstroms)
            defocus_angstroms = warp_row['defocus_value'] * 10000.0
            delta_angstroms = warp_row['defocus_delta'] * 10000.0
            
            updated_df.at[index, 'rlnDefocusU'] = defocus_angstroms
            updated_df.at[index, 'rlnDefocusV'] = defocus_angstroms  # Same as DefocusU in old code
            updated_df.at[index, 'rlnCtfAstigmatism'] = delta_angstroms
            updated_df.at[index, 'rlnDefocusAngle'] = warp_row['defocus_angle']
            
            # OLD LOGIC: Add all the placeholder values exactly as in old code
            updated_df.at[index, 'rlnCtfImage'] = \
                f"{warp_row['folder']}/powerspectrum/{base_name}.mrc"
            
            # These exact placeholder values from old code
            updated_df.at[index, 'rlnAccumMotionTotal']   = 0.000001
            updated_df.at[index, 'rlnAccumMotionEarly']   = 0.000001
            updated_df.at[index, 'rlnAccumMotionLate']    = 0.000001
            updated_df.at[index, 'rlnCtfMaxResolution']   = 0.000001
            updated_df.at[index, 'rlnMicrographMetadata'] = "None"
            updated_df.at[index, 'rlnCtfFigureOfMerit']   = "None"
        
        return updated_df

    def update_ts_alignment_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        project_root: Path,
        tomo_dimensions: str,
        alignment_method: str,
    ) -> Dict:
        try:
            print(f"[METADATA] Starting tsAlignment update for {input_star_path}")
            alignment_method_enum = AlignmentMethod(alignment_method)  
            input_star_dir = input_star_path.parent
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get('global')
            
            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")

            # Resolve Pixel Size
            pixel_size_col = next((c for c in ['rlnMicrographOriginalPixelSize', 'rlnTomoTiltSeriesPixelSize', 'rlnMicrographPixelSize'] 
                                 if c in in_ts_df.columns), None)
            
            if pixel_size_col:
                pixS = float(in_ts_df[pixel_size_col].iloc[0])
            else:
                pixS = 1.35 # Fallback
            
            print(f"[METADATA] Using pixel size: {pixS} Å")

            output_star_path.parent.mkdir(parents=True, exist_ok=True)
            output_tilts_dir = output_star_path.parent / "tilt_series"
            output_tilts_dir.mkdir(exist_ok=True)

            ts_id_failed = []
            updated_tilt_dfs = {}
            all_tilts_list = []

            for _, ts_row in in_ts_df.iterrows():
                ts_star_file_rel = ts_row["rlnTomoTiltSeriesStarFile"]
                
                # REFACTOR: ts_id is just the search pattern base. 
                # We need the actual folder name from the filesystem.
                ts_id_base = Path(ts_star_file_rel).stem 
                
                ts_star_path_abs = (input_star_dir / ts_star_file_rel).resolve()
                if not ts_star_path_abs.exists():
                    ts_id_failed.append(ts_id_base)
                    continue

                ts_data_in = self.starfile_service.read(ts_star_path_abs)
                ts_tilts_df = next(iter(ts_data_in.values())).copy()

                # --- NEW ROBUST PATH RESOLUTION ---
                # Search for the folder in tiltstack that starts with our ID
                tiltstack_root = job_dir / "warp_tiltseries" / "tiltstack"
                # Search for directory like "new_stage3*"
                matching_dirs = list(tiltstack_root.glob(f"{ts_id_base}*"))
                
                if not matching_dirs or not matching_dirs[0].is_dir():
                    print(f"[WARN] No output folder found in {tiltstack_root} for {ts_id_base}")
                    ts_id_failed.append(ts_id_base)
                    continue
                
                actual_ts_dir = matching_dirs[0]
                actual_ts_id = actual_ts_dir.name # e.g. "new_stage3.5_project_Position_1"
                
                aln_data = None
                if alignment_method_enum == AlignmentMethod.ARETOMO:
                    # Look for the .aln file inside that specific folder
                    # Using glob here handles case where filename has extra dots or suffixes
                    aln_files = list(actual_ts_dir.glob("*.st.aln"))
                    if aln_files:
                        print(f"[DEBUG] Found AreTomo alignment: {aln_files[0]}")
                        aln_data = self._read_aretomo_aln_file(aln_files[0])
                
                elif alignment_method_enum == AlignmentMethod.IMOD:
                    xf_files = list(actual_ts_dir.glob("*.xf"))
                    tlt_files = list(actual_ts_dir.glob("*.tlt"))
                    if xf_files and tlt_files:
                        aln_data = self._read_imod_xf_tlt_files(xf_files[0], tlt_files[0])

                if aln_data is None:
                    print(f"[WARN] Alignment data missing in {actual_ts_dir}")
                    ts_id_failed.append(ts_id_base)
                    continue
                
                # Sort by tilt index
                aln_data = aln_data[aln_data[:, 0].argsort()]
                keys_rel = [Path(p).name for p in ts_tilts_df["rlnMicrographMovieName"]]

                # Match tomostar using the same base-name logic
                tomostar_dir = project_root / "tomostar"
                # The tomostar file name usually matches the folder name Warp created
                tomostar_path = tomostar_dir / f"{actual_ts_id}.tomostar"
                
                if not tomostar_path.exists():
                    # Fallback: try the base id
                    tomostar_path = tomostar_dir / f"{ts_id_base}.tomostar"
                
                if not tomostar_path.exists():
                    print(f"[WARN] Tomostar not found at {tomostar_path}")
                    ts_id_failed.append(ts_id_base)
                    continue
                
                tomostar_data = self.starfile_service.read(tomostar_path)
                tomostar_df = next(iter(tomostar_data.values()))

                # Apply alignment
                applied_count = 0
                for index, tomo_row in tomostar_df.iterrows():
                    if 'wrpMovieName' not in tomo_row: continue
                    
                    movie_path = tomo_row['wrpMovieName']
                    key_base = Path(movie_path).stem
                    
                    # Match by stem to be extension-agnostic
                    matching_positions = [i for i, k in enumerate(keys_rel) if Path(k).stem == key_base]
                    
                    if matching_positions:
                        pos = matching_positions[0]
                        ts_tilts_df.at[pos, "rlnTomoXTilt"] = 0
                        ts_tilts_df.at[pos, "rlnTomoYTilt"] = -1.0 * aln_data[index, 9]
                        ts_tilts_df.at[pos, "rlnTomoZRot"] = aln_data[index, 1]
                        ts_tilts_df.at[pos, "rlnTomoXShiftAngst"] = aln_data[index, 3] * pixS
                        ts_tilts_df.at[pos, "rlnTomoYShiftAngst"] = aln_data[index, 4] * pixS
                        applied_count += 1

                if applied_count == 0:
                    ts_id_failed.append(ts_id_base)
                    continue

                # Use actual_ts_id for the output filenames to maintain consistency with disk
                updated_tilt_dfs[actual_ts_id] = ts_tilts_df
                ts_row_df = pd.concat([pd.DataFrame(ts_row).T] * len(ts_tilts_df), ignore_index=True)
                ts_row_df.index = ts_tilts_df.index
                all_tilts_list.append(pd.concat([ts_row_df, ts_tilts_df], axis=1))

            if not updated_tilt_dfs:
                raise Exception("Alignment metadata update failed for all series. Check naming conventions.")

            # Write outputs
            for tid, tdf in updated_tilt_dfs.items():
                self.starfile_service.write({tid: tdf}, output_tilts_dir / f"{tid}.star")

            out_ts_df = in_ts_df[~in_ts_df["rlnTomoName"].isin(ts_id_failed)].copy()
            out_ts_df["rlnTomoTiltSeriesStarFile"] = out_ts_df["rlnTomoName"].apply(lambda x: f"tilt_series/{x}.star")
            
            dims = tomo_dimensions.split('x')
            out_ts_df["rlnTomoSizeX"], out_ts_df["rlnTomoSizeY"], out_ts_df["rlnTomoSizeZ"] = map(int, dims)
            out_ts_df["rlnTomoTiltSeriesPixelSize"] = pixS
            
            self.starfile_service.write({'global': out_ts_df}, output_star_path)
            
            if all_tilts_list:
                all_tilts_df = pd.concat(all_tilts_list, ignore_index=True)
                self.starfile_service.write({'all_tilts': all_tilts_df}, output_star_path.parent / "all_tilts.star")

            return {"success": True, "message": "Metadata updated using robust path resolution."}

        except Exception as e:
            print(f"[ERROR] tsAlignment metadata update failed: {e}")
            import traceback; traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _merge_ctf_metadata(
        self,
        tilts_df: pd.DataFrame,
        warp_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge CTF parameters from WarpTools into tilt series DataFrame."""
        updated_df = tilts_df.copy()
        
        for index, row in updated_df.iterrows():
            key = row['cryoBoostKey']
            
            # More comprehensive key cleaning to match WarpTools format
            clean_key = key.replace("_EER.eer.mrc", "").replace("_EER.mrc", "").replace(".mrc", "")
            clean_key = clean_key.replace("_EER", "").replace(".eer", "")
            
            # Also try matching by the base filename without extensions
            base_key = Path(key).stem
            base_key = base_key.replace("_EER", "")
            
            # Find matching WarpTools data - try multiple key formats
            matches = warp_df[warp_df['cryoBoostKey'] == clean_key]
            if matches.empty:
                matches = warp_df[warp_df['cryoBoostKey'] == base_key]
            if matches.empty:
                # Try partial matching for cases like "001[10.00]" vs "001_10.00"
                clean_key_alt = clean_key.replace("[", "_").replace("]", "")
                matches = warp_df[warp_df['cryoBoostKey'].str.contains(clean_key_alt, na=False)]
            
            if matches.empty:
                print(f"[WARN] No CTF data for {key} (tried: {clean_key}, {base_key})")
                continue
            
            warp_row = matches.iloc[0]
            
            # Calculate defocus values (convert microns to Angstroms)
            defocus_u = (float(warp_row['defocus_value']) + float(warp_row['defocus_delta'])) * 10000
            defocus_v = (float(warp_row['defocus_value']) - float(warp_row['defocus_delta'])) * 10000
            defocus_angle = float(warp_row['defocus_angle'])
            astigmatism = defocus_u - defocus_v
            
            # ONLY update CTF parameters (like the old code)
            updated_df.at[index, 'rlnDefocusU'] = defocus_u
            updated_df.at[index, 'rlnDefocusV'] = defocus_v
            updated_df.at[index, 'rlnDefocusAngle'] = defocus_angle
            updated_df.at[index, 'rlnCtfAstigmatism'] = astigmatism
            
        return updated_df

    def _write_updated_ctf_star(
        self,
        tilts_df: pd.DataFrame,
        tilt_series_df: pd.DataFrame,
        output_path: Path
    ):
        """
        Write updated CTF metadata to STAR files.
        Replicates old tiltSeriesMeta.writeTiltSeries() behavior.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tilt_series_dir = output_path.parent / "tilt_series"
        tilt_series_dir.mkdir(exist_ok=True)
        
        # Extract tilt series info
        num_ts_cols = len(tilt_series_df.columns)
        ts_df = tilts_df.iloc[:, :num_ts_cols].copy()
        ts_df = ts_df.drop('cryoBoostKey', axis=1, errors='ignore')
        ts_df = ts_df.drop_duplicates().reset_index(drop=True)
        
        # Update paths to point to new tilt_series directory
        ts_df['rlnTomoTiltSeriesStarFile'] = ts_df['rlnTomoTiltSeriesStarFile'].apply(
            lambda x: f"tilt_series/{Path(x).name}"
        )
        
        # Write main tilt series STAR file
        self.starfile_service.write({'global': ts_df}, output_path)
        
        # Write individual tilt series files
        for ts_name in ts_df['rlnTomoName']:
            # Get all tilts for this tilt series
            ts_tilts = tilts_df[tilts_df['rlnTomoName'] == ts_name].copy()
            
            # Extract only the per-tilt columns
            ts_tilts_only = ts_tilts.iloc[:, num_ts_cols:].copy()
            
            # Remove the cryoBoostKey helper column
            if 'cryoBoostKey' in ts_tilts_only.columns:
                ts_tilts_only = ts_tilts_only.drop('cryoBoostKey', axis=1)
            
            ts_file = tilt_series_dir / f"{ts_name}.star"
            self.starfile_service.write({ts_name: ts_tilts_only}, ts_file)
        
        print(f"[METADATA] Wrote updated CTF STAR files to {output_path}")

    def update_ts_ctf_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        project_root: Path,
        warp_folder: str = "warp_tiltseries"
    ) -> Dict:
        try:
            print(f"[METADATA] Starting tsCTF update for {input_star_path}")
            print(f"[METADATA] Using project root: {project_root}")
            
            # Parse WarpTools XML files from job directory
            xml_pattern = str(job_dir / warp_folder / "*.xml")
            warp_data = WarpXmlParser(xml_pattern)
            print(f"[METADATA] Parsed {len(warp_data.data_df)} XML files")
            
            # Read input tilt series data
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get('global')
            
            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")
            
            # Load all tilt series data - pass input_star_path for proper path resolution
            all_tilts_df = self._load_all_tilt_series(project_root, input_star_path, in_ts_df)
            
            # Update with CTF parameters
            updated_df = self._merge_ctf_metadata(all_tilts_df, warp_data.data_df)
            
            # Write updated STAR files
            self._write_updated_ctf_star(updated_df, in_ts_df, output_star_path)
            
            return {
                "success": True,
                "message": f"Updated {len(updated_df)} tilts with CTF metadata",
                "output_path": str(output_star_path)
            }
            
        except Exception as e:
            print(f"[ERROR] tsCTF metadata update failed: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def update_ts_reconstruct_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        warp_folder: str,
        rescale_angpixs: float,
        frame_pixel_size: float,
    ) -> Dict:
        """
        Updates STAR files with tomogram reconstruction paths.
        Ported from old tsReconstruct.py::updateMetaData
        """
        try:
            print(f"[METADATA] Starting tsReconstruct update for {input_star_path}")
            
            # Read input tilt series data
            input_star_dir = input_star_path.parent
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get('global')
            
            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")
            
            # Create output dataframe
            out_ts_df = in_ts_df.copy()
            
            # Format resolution string
            rec_res = f"{rescale_angpixs:.2f}"
            
            # Calculate binning factor
            binning = rescale_angpixs / frame_pixel_size
            
            # Update paths for each tilt series - use job_dir for reconstruction outputs
            for index, row in out_ts_df.iterrows():
                ts_name = row["rlnTomoName"]
                
                # Build reconstruction paths relative to job directory
                rec_name = f"{warp_folder}/reconstruction/{ts_name}_{rec_res}Apx.mrc"
                rec_half1 = f"{warp_folder}/reconstruction/even/{ts_name}_{rec_res}Apx.mrc"
                rec_half2 = f"{warp_folder}/reconstruction/odd/{ts_name}_{rec_res}Apx.mrc"
                
                # Update dataframe
                out_ts_df.at[index, "rlnTomoReconstructedTomogram"] = rec_name
                out_ts_df.at[index, "rlnTomoReconstructedTomogramHalf1"] = rec_half1
                out_ts_df.at[index, "rlnTomoReconstructedTomogramHalf2"] = rec_half2
                out_ts_df.at[index, "rlnTomoTiltSeriesPixelSize"] = frame_pixel_size
                out_ts_df.at[index, "rlnTomoTomogramBinning"] = binning
            
            # Write output STAR file
            output_star_path.parent.mkdir(parents=True, exist_ok=True)
            self.starfile_service.write({'global': out_ts_df}, output_star_path)
            
            print(f"[METADATA] Wrote tomograms.star to {output_star_path}")
            
            return {
                "success": True,
                "message": f"Updated {len(out_ts_df)} tomograms with reconstruction paths",
                "output_path": str(output_star_path)
            }
            
        except Exception as e:
            print(f"[ERROR] tsReconstruct metadata update failed: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}