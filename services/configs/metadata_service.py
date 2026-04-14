# services/metadata_service.py
"""
Service for translating WarpTools metadata to Relion STAR format.
Bridges the gap between fsMotionAndCtf output and downstream jobs.
"""

import glob
import logging
import os
from pathlib import Path
from typing import Dict, Optional
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
from services.configs.starfile_service import StarfileService
from services.project_state import AlignmentMethod

logger = logging.getLogger(__name__)


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
            if file_type == "fs":  # Frame series
                df = self._parse_frame_series_xml(xml_path)
            else:  # Tilt series
                df = self._parse_tilt_series_xml(xml_path)

            self.data_df = pd.concat([self.data_df, df], ignore_index=True)

    @staticmethod
    def _check_xml_type(xml_path: str) -> str:
        """Check if XML is frame series or tilt series"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        return "fs" if root.find("MoviePath") is None else "ts"

    def _parse_frame_series_xml(self, xml_path: str) -> pd.DataFrame:
        """Parse frame series XML to extract CTF parameters"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        ctf  = root.find(".//CTF")

        if ctf is None:
            raise ValueError(f"No CTF data found in {xml_path}")

        data = {
            "cryoBoostKey": Path(xml_path).stem,  # Filename without .xml
            "folder": str(Path(xml_path).parent),
            "defocus_value": float(ctf.find(".//Param[@Name='Defocus']").get("Value")),
            "defocus_angle": float(ctf.find(".//Param[@Name='DefocusAngle']").get("Value")),
            "defocus_delta": float(ctf.find(".//Param[@Name='DefocusDelta']").get("Value")),
        }

        return pd.DataFrame([data])

    def _parse_tilt_series_xml(self, xml_path: str) -> pd.DataFrame:
        """Parse tilt series XML to extract per-tilt CTF parameters.

        <MoviePath> is the authoritative ordered list of tilts in the TS.
        Each <GridCTF>/<GridCTFDefocusDelta>/<GridCTFDefocusAngle> <Node Z="k">
        addresses into MoviePath by Z. Never slice [:num_entries] — when WarpTools
        writes fewer grid nodes than MoviePath entries (CTF fit skipped/failed on
        some tilts), that slice silently drops the tail of MoviePath and routes
        results to the wrong tilts.
        """
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Read handedness flag from root element.
        # ts_defocus_hand --set_flip writes AreAnglesInverted="True" here.
        # This maps to rlnTomoHand = -1 in the output STAR.
        are_angles_inverted = root.get("AreAnglesInverted", "False").strip() == "True"

        # MoviePath is authoritative for tilt ordering and identity.
        movie_paths_all = []
        for path in root.find("MoviePath").text.split("\n"):
            if path.strip():
                movie_name = os.path.basename(path).replace("_EER.eer", "")
                movie_name = movie_name.replace(".tif", "")
                movie_name = movie_name.replace(".eer", "")
                movie_paths_all.append(movie_name)

        def _read_grid(grid_name: str) -> dict:
            grid = root.find(grid_name)
            if grid is None:
                raise ValueError(f"No <{grid_name}> element in {xml_path}")
            return {int(n.get("Z")): float(n.get("Value")) for n in grid.findall("Node")}

        ctf_by_z = _read_grid("GridCTF")
        delta_by_z = _read_grid("GridCTFDefocusDelta")
        angle_by_z = _read_grid("GridCTFDefocusAngle")

        # The three grids MUST share an identical Z-set: they are parallel arrays
        # keyed by Z. Divergence means per-tilt values would get joined across
        # different tilts — the exact silent-corruption failure we forbid.
        ctf_z = set(ctf_by_z)
        if ctf_z != set(delta_by_z) or ctf_z != set(angle_by_z):
            raise ValueError(
                f"Grid Z-sets diverge in {xml_path}: "
                f"GridCTF={sorted(ctf_z)}, Delta={sorted(delta_by_z)}, Angle={sorted(angle_by_z)}"
            )

        # Every Z must be a valid index into MoviePath. If WarpTools skipped CTF
        # fitting on some tilts, those Z values are simply absent from the grids;
        # the tilts_df merge will then report them as unresolved — that's correct,
        # a missing CTF IS a problem and must be visible, not silently dropped.
        out_of_range = sorted(z for z in ctf_z if z < 0 or z >= len(movie_paths_all))
        if out_of_range:
            raise ValueError(
                f"GridCTF Z indices {out_of_range} out of range for "
                f"{len(movie_paths_all)} MoviePath entries in {xml_path}"
            )

        missing = sorted(set(range(len(movie_paths_all))) - ctf_z)
        if missing:
            logger.warning(
                "%s: %d of %d tilts have no CTF fit (Z=%s); those tilts will be reported "
                "as unresolved at merge time.",
                xml_path, len(missing), len(movie_paths_all), missing,
            )

        rows = [
            {
                "Z": z,
                "defocus_value": ctf_by_z[z],
                "defocus_delta": delta_by_z[z],
                "defocus_angle": angle_by_z[z],
                "cryoBoostKey": movie_paths_all[z],
                "are_angles_inverted": are_angles_inverted,
            }
            for z in sorted(ctf_z)
        ]
        return pd.DataFrame(rows)


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

        df1 = pd.read_csv(xf_file, delim_whitespace=True, header=None, names=["m1", "m2", "m3", "m4", "tx", "ty"])
        df2 = pd.read_csv(tlt_file, delim_whitespace=True, header=None, names=["tilt_angle"])
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
        warp_folder: str = "warp_frameseries",
    ) -> Dict:
        try:
            xml_pattern = str(job_dir / warp_folder / "*.xml")
            warp_data = WarpXmlParser(xml_pattern)
            logger.info("Parsed %d XML files", len(warp_data.data_df))

            star_data = self.starfile_service.read(input_star_path)
            tilt_series_df = star_data.get("global", pd.DataFrame())

            if tilt_series_df.empty:
                raise ValueError(f"No tilt series data in {input_star_path}")

            # ALWAYS use project root for path resolution
            all_tilts_df = self._load_all_tilt_series(project_root, input_star_path, tilt_series_df)

            updated_df = self._merge_warp_metadata(all_tilts_df, warp_data.data_df, job_dir / warp_folder)

            self._write_updated_star(updated_df, tilt_series_df, output_star_path)

            return {
                "success": True,
                "message": f"Updated {len(updated_df)} tilts",
                "output_path": str(output_star_path),
            }

        except Exception as e:
            logger.error("Metadata update failed: %s", e)
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _load_all_tilt_series(
        self,
        project_root: Path,
        input_star_path: Path,  # NEW: Pass the input STAR file path
        tilt_series_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Load all individual tilt series STAR files into one merged DataFrame.
        Resolves paths relative to the input STAR file's directory.
        """
        all_tilts = []

        input_star_dir = input_star_path.parent
        logger.info("Loading tilt series relative to: %s", input_star_dir)

        for i, (_, ts_row) in enumerate(tilt_series_df.iterrows()):
            ts_file = ts_row["rlnTomoTiltSeriesStarFile"]

            # Try multiple path resolution strategies in order of likelihood:
            paths_to_try = [
                input_star_dir / ts_file,  # 1. Relative to input STAR file (most likely)
                project_root / ts_file,  # 2. Relative to project root
            ]

            ts_path = None
            for path in paths_to_try:
                logger.debug("Trying path: %s", path)
                logger.debug("  Path exists: %s", path.exists())
                if path.exists():
                    ts_path = path
                    logger.debug("  Using this path")
                    break
                else:
                    logger.debug("  Path does not exist")

            if ts_path is None:
                logger.warning("Tilt series file not found: %s", ts_file)
                logger.warning("Tried the following locations:")
                for path in paths_to_try:
                    logger.warning("  - %s", path)
                continue

            logger.info("Loading tilt series from: %s", ts_path)

            try:
                ts_data = self.starfile_service.read(ts_path)
                ts_df = next(iter(ts_data.values()))

                ts_df["cryoBoostKey"] = ts_df["rlnMicrographMovieName"].apply(lambda x: Path(x).stem)
                ts_row_repeated = pd.concat([pd.DataFrame(ts_row).T] * len(ts_df), ignore_index=True)
                merged = pd.concat([ts_row_repeated.reset_index(drop=True), ts_df.reset_index(drop=True)], axis=1)
                all_tilts.append(merged)

            except Exception as e:
                logger.error("Failed to load tilt series file %s: %s", ts_path, e)
                continue

        if not all_tilts:
            raise ValueError(f"No tilt series files could be loaded. Checked relative to: {input_star_dir}")

        all_tilts_df = pd.concat(all_tilts, ignore_index=True)

        # Move cryoBoostKey to the end
        key_values = all_tilts_df["cryoBoostKey"]
        all_tilts_df = all_tilts_df.drop("cryoBoostKey", axis=1)
        all_tilts_df["cryoBoostKey"] = key_values

        logger.info("Loaded %d individual tilts from %d tilt series", len(all_tilts_df), len(tilt_series_df))
        return all_tilts_df

    def _write_updated_star(self, tilts_df: pd.DataFrame, tilt_series_df: pd.DataFrame, output_path: Path):
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
        ts_df = ts_df.drop("cryoBoostKey", axis=1, errors="ignore")
        ts_df = ts_df.drop_duplicates().reset_index(drop=True)

        # Update paths to point to new tilt_series directory
        ts_df["rlnTomoTiltSeriesStarFile"] = ts_df["rlnTomoTiltSeriesStarFile"].apply(
            lambda x: f"tilt_series/{Path(x).name}"
        )

        # Write main tilt series STAR file
        self.starfile_service.write({"global": ts_df}, output_path)

        # Write individual tilt series files
        for ts_name in ts_df["rlnTomoName"]:
            # Get all tilts for this tilt series
            ts_tilts = tilts_df[tilts_df["rlnTomoName"] == ts_name].copy()

            # Extract only the per-tilt columns (after the tilt series columns)
            ts_tilts_only = ts_tilts.iloc[:, num_ts_cols:].copy()

            # Remove the cryoBoostKey helper column
            if "cryoBoostKey" in ts_tilts_only.columns:
                ts_tilts_only = ts_tilts_only.drop("cryoBoostKey", axis=1)

            ts_file = tilt_series_dir / f"{ts_name}.star"
            self.starfile_service.write({ts_name: ts_tilts_only}, ts_file)

        logger.info("Wrote updated STAR files to %s", output_path)

    def _merge_warp_metadata(self, tilts_df: pd.DataFrame, warp_df: pd.DataFrame, warp_folder: Path) -> pd.DataFrame:
        """
        Merge WarpTools XML data into tilt series DataFrame
        EXACTLY matches old CryoBoost behavior
        """
        updated_df = tilts_df.copy()

        for index, row in updated_df.iterrows():
            key = row["cryoBoostKey"]

            # Find matching WarpTools data
            matches = warp_df[warp_df["cryoBoostKey"] == key]

            if matches.empty:
                logger.warning("No WarpTools data for %s", key)
                continue

            warp_row = matches.iloc[0]
            base_name = key.replace(".eer", "").replace(".tif", "")

            # OLD LOGIC: Update paths to motion-corrected outputs
            updated_df.at[index, "rlnMicrographName"] = f"{warp_row['folder']}/average/{base_name}.mrc"
            updated_df.at[index, "rlnMicrographNameEven"] = f"{warp_row['folder']}/average/even/{base_name}.mrc"
            updated_df.at[index, "rlnMicrographNameOdd"] = f"{warp_row['folder']}/average/odd/{base_name}.mrc"

            # OLD LOGIC: Update CTF parameters (convert microns to Angstroms)
            defocus_angstroms = warp_row["defocus_value"] * 10000.0
            delta_angstroms = warp_row["defocus_delta"] * 10000.0

            updated_df.at[index, "rlnDefocusU"] = defocus_angstroms
            updated_df.at[index, "rlnDefocusV"] = defocus_angstroms  # Same as DefocusU in old code
            updated_df.at[index, "rlnCtfAstigmatism"] = delta_angstroms
            updated_df.at[index, "rlnDefocusAngle"] = warp_row["defocus_angle"]

            # OLD LOGIC: Add all the placeholder values exactly as in old code
            updated_df.at[index, "rlnCtfImage"] = f"{warp_row['folder']}/powerspectrum/{base_name}.mrc"

            # These exact placeholder values from old code
            updated_df.at[index, "rlnAccumMotionTotal"] = 0.000001
            updated_df.at[index, "rlnAccumMotionEarly"] = 0.000001
            updated_df.at[index, "rlnAccumMotionLate"] = 0.000001
            updated_df.at[index, "rlnCtfMaxResolution"] = 0.000001
            updated_df.at[index, "rlnMicrographMetadata"] = "None"
            updated_df.at[index, "rlnCtfFigureOfMerit"] = "None"

        return updated_df

    def _infer_alignment_angpix(self, job_dir: Path) -> float:
        """
        Infer the pixel size of the binned tilt stack that AreTomo/IMOD
        aligned against, by reading the MRC header of the .st file.
        """
        tiltstack_root = job_dir / "warp_tiltseries" / "tiltstack"
        if not tiltstack_root.exists():
            raise FileNotFoundError(
                f"No tiltstack directory at {tiltstack_root}. "
                f"Cannot determine alignment pixel size for shift conversion."
            )

        st_files = list(tiltstack_root.glob("*/*.st"))
        if not st_files:
            raise FileNotFoundError(
                f"No .st files found under {tiltstack_root}. "
                f"Cannot determine alignment pixel size for shift conversion."
            )

        import mrcfile
        with mrcfile.open(st_files[0], header_only=True, mode='r') as mrc:
            voxel_x = float(mrc.voxel_size.x)
            if voxel_x <= 0:
                raise ValueError(
                    f"Invalid pixel size {voxel_x} in {st_files[0]}. "
                    f"Cannot determine alignment pixel size for shift conversion."
                )
            logger.info("Read pixel size %.4f A from %s", voxel_x, st_files[0].name)
            return voxel_x

    def _assert_ts_identity_consistency(self, job_dir: Path, expected_ts_names: set) -> None:
        """Verify the single-source-of-truth invariant for TS identity in an array job.

        For a completed per-TS array job, THREE independent sets of filenames must agree
        on the TS identity:
          * tomostar files          → {job_dir}/tomostar/{ts}.tomostar
          * per-TS XMLs             → {job_dir}/warp_tiltseries/{ts}.xml
          * per-TS tiltstack dirs   → {job_dir}/warp_tiltseries/tiltstack/{ts}/

        Any drift between these three sets means an upstream bug and MUST fail loud,
        otherwise the aggregator would silently route one TS's results through another
        TS's folder. Also enforces that the input-STAR TS names are a subset of all three.
        """
        tomostar_dir = job_dir / "tomostar"
        warp_dir = job_dir / "warp_tiltseries"
        tiltstack_dir = warp_dir / "tiltstack"

        tomostar_stems = {p.stem for p in tomostar_dir.glob("*.tomostar")} if tomostar_dir.is_dir() else set()
        xml_stems = {p.stem for p in warp_dir.glob("*.xml")} if warp_dir.is_dir() else set()
        tiltstack_stems = {p.name for p in tiltstack_dir.iterdir() if p.is_dir()} if tiltstack_dir.is_dir() else set()

        mismatches = []
        if tomostar_stems != tiltstack_stems:
            mismatches.append(
                f"tomostar vs tiltstack: only-tomostar={sorted(tomostar_stems - tiltstack_stems)}, "
                f"only-tiltstack={sorted(tiltstack_stems - tomostar_stems)}"
            )
        if tomostar_stems != xml_stems:
            mismatches.append(
                f"tomostar vs xml: only-tomostar={sorted(tomostar_stems - xml_stems)}, "
                f"only-xml={sorted(xml_stems - tomostar_stems)}"
            )
        input_missing = expected_ts_names - tomostar_stems
        if input_missing:
            mismatches.append(f"input STAR refers to TS with no tomostar: {sorted(input_missing)}")

        if mismatches:
            raise RuntimeError(
                "TS identity consistency violated in "
                f"{job_dir}; refusing to aggregate to avoid silent cross-TS contamination.\n  - "
                + "\n  - ".join(mismatches)
            )

    def update_ts_alignment_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        project_root: Path,
        tomo_dimensions: str,
        alignment_method: str,
        alignment_angpix: float = 0,
    ) -> Dict:
        try:
            logger.info("Starting tsAlignment update for %s", input_star_path)
            alignment_method_enum = AlignmentMethod(alignment_method)
            input_star_dir = input_star_path.parent
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get("global")

            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")

            # Strict TS identity invariant — fail loud if anything has drifted.
            expected_ts = {Path(p).stem for p in in_ts_df["rlnTomoTiltSeriesStarFile"]}
            self._assert_ts_identity_consistency(job_dir, expected_ts)

            # Resolve frame pixel size (used for non-shift metadata)
            pixel_size_col = next(
                (
                    c
                    for c in ["rlnMicrographOriginalPixelSize", "rlnTomoTiltSeriesPixelSize", "rlnMicrographPixelSize"]
                    if c in in_ts_df.columns
                ),
                None,
            )

            if pixel_size_col:
                pixS = float(in_ts_df[pixel_size_col].iloc[0])
            else:
                pixS = 1.35  # Fallback

            logger.info("Frame pixel size: %s A", pixS)

            # Pixel size for converting alignment shifts from pixels to Angstroms.
            # AreTomo/IMOD operate on the binned tilt stack, so shifts are in
            # binned pixels (rescale_angpixs), NOT raw frame pixels.
            if alignment_angpix > 0:
                shift_angpix = alignment_angpix
                logger.info("Using explicit alignment pixel size for shifts: %s A/px", shift_angpix)
            else:
                shift_angpix = self._infer_alignment_angpix(job_dir)
                logger.info("Auto-inferred alignment pixel size for shifts: %s A/px", shift_angpix)

            output_star_path.parent.mkdir(parents=True, exist_ok=True)
            output_tilts_dir = output_star_path.parent / "tilt_series"
            output_tilts_dir.mkdir(exist_ok=True)

            # Collect per-TS failures with a structured reason so a partial aggregation
            # failure surfaces ALL missing series, not just the first one.
            ts_failures: Dict[str, str] = {}
            updated_tilt_dfs = {}
            all_tilts_list = []

            tiltstack_root = job_dir / "warp_tiltseries" / "tiltstack"
            tomostar_dir = job_dir / "tomostar"

            for _, ts_row in in_ts_df.iterrows():
                ts_star_file_rel = ts_row["rlnTomoTiltSeriesStarFile"]
                ts_id = Path(ts_star_file_rel).stem
                # Strict identity: the rlnTomoName MUST equal the tilt_series_star filename stem.
                # Per-TS STARs are created one-to-one, indexed by name. Divergence would mean
                # upstream data has already been corrupted.
                if str(ts_row["rlnTomoName"]) != ts_id:
                    ts_failures[ts_id] = (
                        f"rlnTomoName={ts_row['rlnTomoName']!r} does not equal "
                        f"tilt_series filename stem={ts_id!r}"
                    )
                    continue

                ts_star_path_abs = (input_star_dir / ts_star_file_rel).resolve()
                if not ts_star_path_abs.exists():
                    ts_failures[ts_id] = f"input tilt_series STAR not found: {ts_star_path_abs}"
                    continue

                ts_data_in = self.starfile_service.read(ts_star_path_abs)
                ts_tilts_df = next(iter(ts_data_in.values())).copy()

                # Strict exact-name match. No glob, no prefix — tiltstack dir MUST exist with
                # exactly this name or aggregation fails loud.
                ts_tiltstack_dir = tiltstack_root / ts_id
                if not ts_tiltstack_dir.is_dir():
                    ts_failures[ts_id] = f"no tiltstack dir at {ts_tiltstack_dir}"
                    continue

                aln_data = None
                if alignment_method_enum == AlignmentMethod.ARETOMO:
                    aln_files = sorted(ts_tiltstack_dir.glob("*.st.aln"))
                    if len(aln_files) > 1:
                        ts_failures[ts_id] = f"expected 1 .st.aln file, found {len(aln_files)}: {aln_files}"
                        continue
                    if aln_files:
                        aln_data = self._read_aretomo_aln_file(aln_files[0])
                elif alignment_method_enum == AlignmentMethod.IMOD:
                    xf_files = sorted(ts_tiltstack_dir.glob("*.xf"))
                    tlt_files = sorted(ts_tiltstack_dir.glob("*.tlt"))
                    if len(xf_files) > 1 or len(tlt_files) > 1:
                        ts_failures[ts_id] = (
                            f"expected 1 .xf and 1 .tlt, found {len(xf_files)} .xf / {len(tlt_files)} .tlt"
                        )
                        continue
                    if xf_files and tlt_files:
                        aln_data = self._read_imod_xf_tlt_files(xf_files[0], tlt_files[0])

                if aln_data is None:
                    ts_failures[ts_id] = f"alignment output files missing in {ts_tiltstack_dir}"
                    continue

                # Sort by tilt index
                aln_data = aln_data[aln_data[:, 0].argsort()]
                keys_rel = [Path(p).name for p in ts_tilts_df["rlnMicrographMovieName"]]

                # Strict exact-name tomostar lookup — no id_base fallback.
                tomostar_path = tomostar_dir / f"{ts_id}.tomostar"
                if not tomostar_path.exists():
                    ts_failures[ts_id] = f"tomostar not found at {tomostar_path}"
                    continue

                tomostar_data = self.starfile_service.read(tomostar_path)
                tomostar_df = next(iter(tomostar_data.values()))

                # Apply alignment
                applied_count = 0
                for index, tomo_row in tomostar_df.iterrows():
                    if "wrpMovieName" not in tomo_row:
                        continue

                    movie_path = tomo_row["wrpMovieName"]
                    key_base = Path(movie_path).stem

                    matching_positions = [i for i, k in enumerate(keys_rel) if Path(k).stem == key_base]

                    if matching_positions:
                        pos = matching_positions[0]
                        ts_tilts_df.at[pos, "rlnTomoXTilt"] = 0
                        ts_tilts_df.at[pos, "rlnTomoYTilt"] = -1.0 * aln_data[index, 9]
                        ts_tilts_df.at[pos, "rlnTomoZRot"] = aln_data[index, 1]
                        ts_tilts_df.at[pos, "rlnTomoXShiftAngst"] = aln_data[index, 3] * shift_angpix
                        ts_tilts_df.at[pos, "rlnTomoYShiftAngst"] = aln_data[index, 4] * shift_angpix
                        applied_count += 1

                if applied_count == 0:
                    ts_failures[ts_id] = (
                        "no movie names in the tomostar matched the per-TS input STAR — "
                        "likely a cross-TS contamination of the staging dir"
                    )
                    continue

                updated_tilt_dfs[ts_id] = ts_tilts_df
                ts_row_df = pd.concat([pd.DataFrame(ts_row).T] * len(ts_tilts_df), ignore_index=True)
                ts_row_df.index = ts_tilts_df.index
                all_tilts_list.append(pd.concat([ts_row_df, ts_tilts_df], axis=1))

            # Fail loud on any per-TS problem. Silently dropping rows from the output STAR
            # is the exact "tomogram produced from the wrong tilt-series" risk we are
            # explicitly guarding against.
            if ts_failures:
                detail = "\n  - ".join(f"{tid}: {reason}" for tid, reason in sorted(ts_failures.items()))
                raise RuntimeError(
                    f"tsAlignment metadata aggregation failed for {len(ts_failures)} tilt-series:\n  - {detail}"
                )

            # Write outputs
            for tid, tdf in updated_tilt_dfs.items():
                self.starfile_service.write({tid: tdf}, output_tilts_dir / f"{tid}.star")

            # No more name_mapping: rlnTomoName already equals the canonical TS id (asserted above).
            out_ts_df = in_ts_df.copy()
            out_ts_df["rlnTomoTiltSeriesStarFile"] = out_ts_df["rlnTomoName"].apply(lambda x: f"tilt_series/{x}.star")

            if out_ts_df["rlnTomoName"].duplicated().any():
                dups = out_ts_df.loc[out_ts_df["rlnTomoName"].duplicated(keep=False), "rlnTomoName"].tolist()
                raise RuntimeError(f"duplicate rlnTomoName values in alignment output STAR: {sorted(set(dups))}")

            dims = tomo_dimensions.split("x")
            out_ts_df["rlnTomoSizeX"], out_ts_df["rlnTomoSizeY"], out_ts_df["rlnTomoSizeZ"] = map(int, dims)
            out_ts_df["rlnTomoTiltSeriesPixelSize"] = pixS

            self.starfile_service.write({"global": out_ts_df}, output_star_path)

            if all_tilts_list:
                all_tilts_df = pd.concat(all_tilts_list, ignore_index=True)
                self.starfile_service.write({"all_tilts": all_tilts_df}, output_star_path.parent / "all_tilts.star")

            return {"success": True, "message": "Metadata updated using robust path resolution."}

        except Exception as e:
            logger.error("tsAlignment metadata update failed: %s", e)
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _merge_ctf_metadata(self, tilts_df: pd.DataFrame, warp_df: pd.DataFrame) -> pd.DataFrame:
        """Merge CTF parameters from WarpTools into tilt series DataFrame.

        Strict key matching only. We try a small set of canonical normalizations of
        the movie-stem key (stripping the _EER / .eer / .mrc wrappers that different
        WarpTools versions strip differently), and for each candidate key we require
        EXACTLY ONE matching row in warp_df. Any ambiguity (>1 match) or miss (0 match
        after all candidates) is a hard failure, because silently picking .iloc[0]
        or falling back to a substring match can route CTF values from one tilt to
        another without the user ever seeing it.
        """
        updated_df = tilts_df.copy()
        unresolved: list[str] = []
        ambiguous: list[str] = []

        for index, row in updated_df.iterrows():
            key = row["cryoBoostKey"]

            # Only the raw key + the EER/MRC-stripping cascade. Do NOT use
            # Path(key).stem as a third candidate — tilt filenames embed the
            # tilt angle (e.g. "_52.00_") and Path treats everything after the
            # last "." as the suffix, corrupting the key to "..._52" and either
            # matching nothing or, worse, matching the wrong tilt by prefix.
            clean_key = key.replace("_EER.eer.mrc", "").replace("_EER.mrc", "").replace(".mrc", "")
            clean_key = clean_key.replace("_EER", "").replace(".eer", "")

            candidates = [key] if clean_key == key else [key, clean_key]

            warp_row = None
            hit_ambiguous = False
            for cand in candidates:
                matches = warp_df[warp_df["cryoBoostKey"] == cand]
                if len(matches) == 1:
                    warp_row = matches.iloc[0]
                    break
                if len(matches) > 1:
                    ambiguous.append(f"{key!r} (candidate {cand!r} matched {len(matches)} rows)")
                    hit_ambiguous = True
                    break

            if hit_ambiguous:
                continue
            if warp_row is None:
                unresolved.append(f"{key!r} (tried {candidates})")
                continue

            # Calculate defocus values (convert microns to Angstroms)
            defocus_u = (float(warp_row["defocus_value"]) + float(warp_row["defocus_delta"])) * 10000
            defocus_v = (float(warp_row["defocus_value"]) - float(warp_row["defocus_delta"])) * 10000
            defocus_angle = float(warp_row["defocus_angle"])
            astigmatism = defocus_u - defocus_v

            updated_df.at[index, "rlnDefocusU"] = defocus_u
            updated_df.at[index, "rlnDefocusV"] = defocus_v
            updated_df.at[index, "rlnDefocusAngle"] = defocus_angle
            updated_df.at[index, "rlnCtfAstigmatism"] = astigmatism

            # Update handedness from AreAnglesInverted.
            # AreAnglesInverted="True"  -> rlnTomoHand = -1  (flip)
            # AreAnglesInverted="False" -> rlnTomoHand =  1  (no flip)
            if "are_angles_inverted" in warp_row.index:
                updated_df.at[index, "rlnTomoHand"] = -1 if bool(warp_row["are_angles_inverted"]) else 1

        # Fail loud on any unresolved or ambiguous keys. Silently leaving a tilt with
        # un-updated CTF values (or worse — pulling CTF from the wrong tilt via a
        # substring match) is exactly the silent-corruption failure mode we forbid.
        problems = []
        if ambiguous:
            problems.append(f"{len(ambiguous)} ambiguous key(s): " + "; ".join(ambiguous))
        if unresolved:
            problems.append(f"{len(unresolved)} unresolved key(s): " + "; ".join(unresolved))
        if problems:
            raise RuntimeError("CTF metadata merge has identity conflicts — " + " | ".join(problems))

        return updated_df

    def _write_updated_ctf_star(self, tilts_df: pd.DataFrame, tilt_series_df: pd.DataFrame, output_path: Path):
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
        ts_df = ts_df.drop("cryoBoostKey", axis=1, errors="ignore")
        ts_df = ts_df.drop_duplicates().reset_index(drop=True)

        # Update paths to point to new tilt_series directory
        ts_df["rlnTomoTiltSeriesStarFile"] = ts_df["rlnTomoTiltSeriesStarFile"].apply(
            lambda x: f"tilt_series/{Path(x).name}"
        )

        # Write main tilt series STAR file
        self.starfile_service.write({"global": ts_df}, output_path)

        # Write individual tilt series files
        for ts_name in ts_df["rlnTomoName"]:
            # Get all tilts for this tilt series
            ts_tilts = tilts_df[tilts_df["rlnTomoName"] == ts_name].copy()

            # Extract only the per-tilt columns
            ts_tilts_only = ts_tilts.iloc[:, num_ts_cols:].copy()

            # Remove the cryoBoostKey helper column
            if "cryoBoostKey" in ts_tilts_only.columns:
                ts_tilts_only = ts_tilts_only.drop("cryoBoostKey", axis=1)

            ts_file = tilt_series_dir / f"{ts_name}.star"
            self.starfile_service.write({ts_name: ts_tilts_only}, ts_file)

        logger.info("Wrote updated CTF STAR files to %s", output_path)

    def update_ts_ctf_metadata(
        self,
        job_dir: Path,
        input_star_path: Path,
        output_star_path: Path,
        project_root: Path,
        warp_folder: str = "warp_tiltseries",
    ) -> Dict:
        try:
            logger.info("Starting tsCTF update for %s", input_star_path)
            logger.info("Using project root: %s", project_root)

            # Parse WarpTools XML files from job directory
            xml_pattern = str(job_dir / warp_folder / "*.xml")
            warp_data = WarpXmlParser(xml_pattern)
            logger.info("Parsed %d XML files", len(warp_data.data_df))

            # Read input tilt series data
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get("global")

            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")

            # Identity invariant: every TS in the input STAR MUST have a matching per-TS
            # XML file in the output processing dir. Drift would mean the CTF merge is
            # pulling values across TS boundaries. Fail loud rather than produce silent
            # cross-contamination.
            xml_stems = {p.stem for p in (job_dir / warp_folder).glob("*.xml")}
            input_ts_names = {str(n) for n in in_ts_df["rlnTomoName"]}
            missing_xml = input_ts_names - xml_stems
            if missing_xml:
                raise RuntimeError(
                    f"tsCtf aggregation aborted: {len(missing_xml)} tilt-series from the input STAR "
                    f"have no XML in {job_dir / warp_folder}: {sorted(missing_xml)}"
                )

            # Load all tilt series data - pass input_star_path for proper path resolution
            all_tilts_df = self._load_all_tilt_series(project_root, input_star_path, in_ts_df)

            # Update with CTF parameters
            updated_df = self._merge_ctf_metadata(all_tilts_df, warp_data.data_df)

            # Write updated STAR files
            self._write_updated_ctf_star(updated_df, in_ts_df, output_star_path)

            return {
                "success": True,
                "message": f"Updated {len(updated_df)} tilts with CTF metadata",
                "output_path": str(output_star_path),
            }

        except Exception as e:
            logger.error("tsCTF metadata update failed: %s", e)
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
        All paths written as ABSOLUTE to avoid resolution ambiguity downstream.
        """
        try:
            logger.info("Starting tsReconstruct update for %s", input_star_path)

            input_star_dir = input_star_path.parent
            in_star_data = self.starfile_service.read(input_star_path)
            in_ts_df = in_star_data.get("global")

            if in_ts_df is None:
                raise ValueError(f"No 'global' block in {input_star_path}")

            out_ts_df = in_ts_df.copy()
            rec_res = f"{rescale_angpixs:.2f}"
            binning = rescale_angpixs / frame_pixel_size

            for index, row in out_ts_df.iterrows():
                ts_name = row["rlnTomoName"]

                # Reconstruction paths as ABSOLUTE
                rec_base = (job_dir / warp_folder / "reconstruction").resolve()
                rec_name = str(rec_base / f"{ts_name}_{rec_res}Apx.mrc")
                rec_half1 = str(rec_base / "even" / f"{ts_name}_{rec_res}Apx.mrc")
                rec_half2 = str(rec_base / "odd" / f"{ts_name}_{rec_res}Apx.mrc")

                out_ts_df.at[index, "rlnTomoReconstructedTomogram"] = rec_name
                out_ts_df.at[index, "rlnTomoReconstructedTomogramHalf1"] = rec_half1
                out_ts_df.at[index, "rlnTomoReconstructedTomogramHalf2"] = rec_half2
                out_ts_df.at[index, "rlnTomoTiltSeriesPixelSize"] = frame_pixel_size
                out_ts_df.at[index, "rlnTomoTomogramBinning"] = binning

                # Resolve tilt series star path to ABSOLUTE
                ts_star_rel = row["rlnTomoTiltSeriesStarFile"]
                ts_star_abs = (input_star_dir / ts_star_rel).resolve()
                if not ts_star_abs.exists():
                    logger.warning("Per-tilt star not found at %s", ts_star_abs)
                out_ts_df.at[index, "rlnTomoTiltSeriesStarFile"] = str(ts_star_abs)

            output_star_path.parent.mkdir(parents=True, exist_ok=True)
            self.starfile_service.write({"global": out_ts_df}, output_star_path)

            logger.info("Wrote tomograms.star to %s", output_star_path)

            return {
                "success": True,
                "message": f"Updated {len(out_ts_df)} tomograms with reconstruction paths",
                "output_path": str(output_star_path),
            }

        except Exception as e:
            logger.error("tsReconstruct metadata update failed: %s", e)
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}
