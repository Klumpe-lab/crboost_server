"""Warp XML parsing.

Only `WarpXmlParser` lives here. The old `MetadataTranslator` (WarpTools → RELION STAR
translation) was superseded by `services/tilt_series/adapters/` and deleted 2026-08-10
(see docs/roadmaps/00-deletions-and-lint.md). Eventual home for this parser is
`services/formats/warp_xml.py` (Roadmap 01).
"""

import glob
import logging
import os
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd

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
