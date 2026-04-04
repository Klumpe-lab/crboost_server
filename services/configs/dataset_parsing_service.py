# services/configs/dataset_parsing_service.py
"""
Parses cryo-ET dataset directories into a structured position/tilt-series hierarchy.

Uses mdoc filenames as the authoritative source for stage/beam position disambiguation.
Each mdoc's ZValue sections provide the definitive frame-to-tilt-series association.
"""

import glob
import logging
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from services.configs.mdoc_service import get_mdoc_service
from services.dataset_models import AcquisitionSummary, DatasetOverview, StagePositionInfo, TiltInfo, TiltSeriesInfo

logger = logging.getLogger(__name__)

MDOC_FILENAME_RE = re.compile(r"^Position_(\d+)(?:_(\d+))?\.mdoc$")


class DatasetParsingService:
    """Parses cryo-ET dataset directories into structured position/tilt-series hierarchy."""

    def __init__(self):
        self.mdoc_service = get_mdoc_service()

    def parse_dataset(self, mdocs_glob: str, frames_dir: Optional[str] = None) -> DatasetOverview:
        """
        Parse all mdoc files matching the glob and associate with frame files.

        Args:
            mdocs_glob: Glob pattern for mdoc files (e.g., "/data/frames/*.mdoc")
            frames_dir: Directory containing frame files. If None, inferred from
                        mdoc SubFramePath entries or from the mdoc directory itself.
        """
        mdoc_files = sorted(glob.glob(mdocs_glob))
        mdoc_paths = [Path(p) for p in mdoc_files if os.path.isfile(p) and p.endswith(".mdoc")]

        if not mdoc_paths:
            return DatasetOverview(
                source_directory=str(Path(mdocs_glob).parent), parse_warnings=["No .mdoc files found"]
            )

        resolved_frames_dir = self._resolve_frames_directory(mdoc_paths, frames_dir)
        frame_ext = self._detect_frame_extension(resolved_frames_dir)

        warnings: List[str] = []
        tilt_series_list: List[TiltSeriesInfo] = []

        for mdoc_path in mdoc_paths:
            parsed = self._parse_mdoc_filename(mdoc_path.name)
            if parsed is None:
                warnings.append(f"Skipped mdoc with unrecognized name: {mdoc_path.name}")
                continue

            stage_pos, beam_pos = parsed

            try:
                mdoc_data = self.mdoc_service.parse_mdoc_file(mdoc_path)
            except Exception as e:
                warnings.append(f"Failed to parse {mdoc_path.name}: {e}")
                continue

            tilts: List[TiltInfo] = []
            for section in mdoc_data["data"]:
                tilt = self._build_tilt_info(section, resolved_frames_dir)
                if tilt is not None:
                    tilts.append(tilt)

            acq = self._extract_acquisition_params(mdoc_data)

            ts = TiltSeriesInfo(
                stage_position=stage_pos,
                beam_position=beam_pos,
                mdoc_filename=mdoc_path.name,
                mdoc_path=mdoc_path,
                tilts=tilts,
                pixel_size=acq.get("pixel_size"),
                voltage=acq.get("voltage"),
                dose_per_tilt=acq.get("dose_per_tilt"),
                tilt_axis=acq.get("tilt_axis"),
            )
            tilt_series_list.append(ts)

            missing = ts.missing_frames
            if missing > 0:
                warnings.append(f"{ts.ts_label}: {missing}/{ts.tilt_count} frames not found")

        positions = self._aggregate_to_positions(tilt_series_list)
        acq_summary = self._build_acquisition_summary(tilt_series_list)

        return DatasetOverview(
            source_directory=str(resolved_frames_dir) if resolved_frames_dir else str(Path(mdocs_glob).parent),
            frame_extension=frame_ext,
            positions=positions,
            parse_warnings=warnings,
            acquisition_summary=acq_summary,
        )

    def _parse_mdoc_filename(self, mdoc_name: str) -> Optional[Tuple[int, int]]:
        """
        Extract (stage_position, beam_position) from mdoc filename.

        'Position_10.mdoc'   -> (10, 1)
        'Position_10_2.mdoc' -> (10, 2)
        'Position_10_3.mdoc' -> (10, 3)

        Returns None if the filename doesn't match the expected pattern.
        """
        m = MDOC_FILENAME_RE.match(mdoc_name)
        if not m:
            return None
        stage = int(m.group(1))
        beam = int(m.group(2)) if m.group(2) else 1
        return (stage, beam)

    def _resolve_frames_directory(self, mdoc_files: List[Path], frames_dir: Optional[str]) -> Optional[Path]:
        """
        Determine where frame files are located.

        Strategy:
        1. If frames_dir is explicitly provided, use it
        2. Parse SubFramePath from first mdoc — if it's an absolute Unix path, use its parent
        3. Otherwise assume frames are in same directory as mdocs
        """
        if frames_dir:
            p = Path(frames_dir)
            if p.exists():
                return p
            # If the glob pattern was passed, extract the directory
            if "*" in frames_dir:
                return Path(frames_dir).parent
            return p

        # Try to infer from first mdoc's SubFramePath
        if mdoc_files:
            try:
                mdoc_data = self.mdoc_service.parse_mdoc_file(mdoc_files[0])
                for section in mdoc_data["data"]:
                    sub = section.get("SubFramePath", "")
                    if not sub:
                        continue
                    # Windows-style paths: just use same dir as mdoc
                    if "\\" in sub:
                        return mdoc_files[0].parent
                    sub_path = Path(sub)
                    if sub_path.is_absolute() and sub_path.parent.exists():
                        return sub_path.parent
                    # Relative path — assume same dir as mdoc
                    return mdoc_files[0].parent
            except Exception:
                pass

            return mdoc_files[0].parent

        return None

    def _detect_frame_extension(self, frames_dir: Optional[Path]) -> str:
        if not frames_dir or not frames_dir.exists():
            return ""
        for ext in [".eer", ".tiff", ".tif", ".mrc"]:
            if any(frames_dir.glob(f"*{ext}")):
                return ext
        return ""

    def _build_tilt_info(self, section: Dict, frames_dir: Optional[Path]) -> Optional[TiltInfo]:
        """Build a TiltInfo from a parsed mdoc ZValue section."""
        z_value_str = section.get("ZValue")
        if z_value_str is None:
            return None

        try:
            z_value = int(z_value_str)
        except (ValueError, TypeError):
            return None

        tilt_angle = 0.0
        if "TiltAngle" in section:
            try:
                tilt_angle = float(section["TiltAngle"])
            except (ValueError, TypeError):
                pass

        sub_frame_path = section.get("SubFramePath", "")
        if not sub_frame_path:
            return None

        # Extract bare filename, handling Windows-style paths
        frame_filename = Path(sub_frame_path.replace("\\", "/")).name

        # Resolve the actual file path
        frame_path = None
        if frames_dir:
            candidate = frames_dir / frame_filename
            if candidate.exists():
                frame_path = candidate.resolve()

        return TiltInfo(z_value=z_value, tilt_angle=tilt_angle, frame_filename=frame_filename, frame_path=frame_path)

    def _extract_acquisition_params(self, mdoc_data: Dict) -> Dict:
        """Extract acquisition parameters from an mdoc's header and first ZValue section."""
        result: Dict = {}
        header_text = mdoc_data.get("header", "")
        sections = mdoc_data.get("data", [])
        first = sections[0] if sections else {}

        # Parse header key=value lines
        header_kv: Dict[str, str] = {}
        for line in header_text.split("\n"):
            if "=" in line:
                k, v = line.split("=", 1)
                header_kv[k.strip()] = v.strip()

        # Pixel size
        for src in [header_kv, first]:
            if "PixelSpacing" in src:
                try:
                    result["pixel_size"] = float(src["PixelSpacing"])
                    break
                except (ValueError, TypeError):
                    pass

        # Voltage
        for src in [header_kv, first]:
            if "Voltage" in src:
                try:
                    result["voltage"] = float(src["Voltage"])
                    break
                except (ValueError, TypeError):
                    pass

        # Dose per tilt (from ExposureDose in first section)
        if "ExposureDose" in first:
            try:
                result["dose_per_tilt"] = round(float(first["ExposureDose"]) * 1.5, 2)
            except (ValueError, TypeError):
                pass

        # Tilt axis
        if "Tilt axis angle" in header_kv:
            try:
                result["tilt_axis"] = float(header_kv["Tilt axis angle"])
            except (ValueError, TypeError):
                pass
        elif "RotationAngle" in first:
            try:
                result["tilt_axis"] = abs(float(first["RotationAngle"]))
            except (ValueError, TypeError):
                pass

        return result

    def _build_acquisition_summary(self, tilt_series_list: List[TiltSeriesInfo]) -> AcquisitionSummary:
        """Collect unique acquisition parameter values across all tilt-series."""
        pxs: set = set()
        vs: set = set()
        ds: set = set()
        tas: set = set()
        tcs: set = set()
        ars: set = set()
        for ts in tilt_series_list:
            if ts.pixel_size is not None:
                pxs.add(round(ts.pixel_size, 3))
            if ts.voltage is not None:
                vs.add(round(ts.voltage, 0))
            if ts.dose_per_tilt is not None:
                ds.add(round(ts.dose_per_tilt, 1))
            if ts.tilt_axis is not None:
                tas.add(round(ts.tilt_axis, 1))
            tcs.add(ts.tilt_count)
            lo, hi = ts.angle_range
            ars.add((round(lo, 0), round(hi, 0)))
        return AcquisitionSummary(
            pixel_sizes=sorted(pxs),
            voltages=sorted(vs),
            doses=sorted(ds),
            tilt_axes=sorted(tas),
            tilt_counts=sorted(tcs),
            angle_ranges=sorted(ars),
        )

    def _aggregate_to_positions(self, tilt_series_list: List[TiltSeriesInfo]) -> List[StagePositionInfo]:
        """Group tilt-series by stage_position, sort by position number."""
        groups: Dict[int, List[TiltSeriesInfo]] = defaultdict(list)
        for ts in tilt_series_list:
            groups[ts.stage_position].append(ts)

        positions = []
        for stage_pos in sorted(groups.keys()):
            series = sorted(groups[stage_pos], key=lambda ts: ts.beam_position)
            positions.append(StagePositionInfo(stage_position=stage_pos, tilt_series=series))
        return positions


_dataset_parsing_service: Optional[DatasetParsingService] = None


def get_dataset_parsing_service() -> DatasetParsingService:
    global _dataset_parsing_service
    if _dataset_parsing_service is None:
        _dataset_parsing_service = DatasetParsingService()
    return _dataset_parsing_service
