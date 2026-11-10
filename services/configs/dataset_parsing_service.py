# services/configs/dataset_parsing_service.py
"""
Parses cryo-ET dataset directories into a structured position/tilt-series hierarchy.

One mdoc = one tilt-series, named after the mdoc file. Each mdoc's ZValue sections
provide the definitive frame-to-tilt-series association; the source layer of each
series (per-tilt movies vs a SerialEM stack beside the mdoc) is inferred per mdoc
and never chosen by the user.
"""

import glob
import logging
import os
import re
from pathlib import Path
from collections.abc import Callable

from services.configs.mdoc_service import (
    acquisition_from_mdoc,
    classify_mdoc,
    describe_ignored_mdocs,
    get_mdoc_service,
    ts_name_from_mdoc,
)
from services.stack_import import find_beside_or_above, read_frame_facts, resolve_stack, same_image_data
from services.tilt_series.preimport import (
    AcquisitionSummary,
    DatasetOverview,
    TiltInfo,
    TiltSeriesInfo,
    aggregate_to_positions,
)
from services.tilt_series.build import parse_position

logger = logging.getLogger(__name__)

_MOVIE_EXTENSIONS = (".eer", ".tiff", ".tif", ".mrc")

# PACEtomo names a series `<navigator area>_ts_<target>` (`area_5-A_ts_002`).
_PACETOMO_NAME_RE = re.compile(r"^(.+)_ts_(\d+)$")


class DatasetParsingService:
    """Parses cryo-ET dataset directories into structured position/tilt-series hierarchy."""

    def __init__(self):
        self.mdoc_service = get_mdoc_service()

    def parse_dataset(
        self, mdocs_glob: str, frames_dir: str | None = None, progress_cb: Callable[[int, int], None] | None = None
    ) -> DatasetOverview:
        """
        Parse all mdoc files matching the glob and associate with frame files.

        Args:
            mdocs_glob: Glob pattern for mdoc files (e.g., "/data/frames/*.mdoc")
            frames_dir: Directory containing frame files. If None, inferred from
                        mdoc SubFramePath entries or from the mdoc directory itself.
                        Frames are also looked for beside each mdoc.
            progress_cb: Optional (current, total) callback invoked as each mdoc
                        is parsed. Runs on the calling (worker) thread.
        """
        mdoc_files = sorted(glob.glob(mdocs_glob))
        mdoc_paths = [Path(p) for p in mdoc_files if os.path.isfile(p) and p.endswith(".mdoc")]

        if not mdoc_paths:
            return DatasetOverview(
                source_directory=str(Path(mdocs_glob).parent), parse_warnings=["No .mdoc files found"]
            )

        resolved_frames_dir = self._resolve_frames_directory(mdoc_paths, frames_dir)

        warnings: list[str] = []
        tilt_series_list: list[TiltSeriesInfo] = []
        ignored: dict[str, int] = {}
        scan_root = Path(mdocs_glob.split("*", 1)[0]) if "*" in mdocs_glob else Path(mdocs_glob).parent

        total_mdocs = len(mdoc_paths)
        if progress_cb:
            progress_cb(0, total_mdocs)

        for mdoc_idx, mdoc_path in enumerate(mdoc_paths):
            try:
                mdoc_data = self.mdoc_service.parse_mdoc_file(mdoc_path)
            except Exception as e:
                warnings.append(f"Failed to parse {mdoc_path.name}: {e}")
                continue

            kind = classify_mdoc(mdoc_data)
            if kind != "tilt_series":
                # Per-movie mdocs, Navigator maps and single images sit beside the series in a
                # SerialEM delivery; they are counted, not offered.
                ignored[kind] = ignored.get(kind, 0) + 1
                if progress_cb:
                    progress_cb(mdoc_idx + 1, total_mdocs)
                continue

            tilts: list[TiltInfo] = []
            for section in mdoc_data["data"]:
                tilt = self._build_tilt_info(section, resolved_frames_dir, mdoc_path.parent)
                if tilt is not None:
                    tilts.append(tilt)

            facts = acquisition_from_mdoc(mdoc_data)
            ts_name = ts_name_from_mdoc(mdoc_path.name)
            parsed_pos = parse_position(ts_name)
            stage_pos, beam_pos = (parsed_pos[0], parsed_pos[1] or 1) if parsed_pos else (None, None)
            stack = resolve_stack(mdoc_path, facts.image_file, facts.n_sections)
            first_movie = next((t.frame_path for t in tilts if t.frame_path is not None), None)
            frame_facts = read_frame_facts(first_movie) if first_movie is not None else None
            gain_name = frame_facts.gain_reference if frame_facts else ""
            pace = _PACETOMO_NAME_RE.match(ts_name)

            ts = TiltSeriesInfo(
                ts_name=ts_name,
                stage_position=stage_pos,
                beam_position=beam_pos,
                mdoc_filename=mdoc_path.name,
                mdoc_path=mdoc_path,
                tilts=tilts,
                stack_path=stack[0] if stack else None,
                stack_nz=stack[1] if stack else None,
                movies_complete=bool(tilts) and all(t.frame_path is not None for t in tilts),
                gain_reference_name=gain_name,
                gain_reference=find_beside_or_above(gain_name, mdoc_path.parent, scan_root),
                frames_rotation_flip=frame_facts.rotation_flip if frame_facts else None,
                frames_binning=frame_facts.binning if frame_facts else None,
                area_label=pace.group(1) if pace and stage_pos is None else None,
                target_label=pace.group(2) if pace and stage_pos is None else None,
                pixel_size=facts.pixel_size,
                voltage=facts.voltage,
                dose_per_tilt=facts.dose_per_tilt,
                dose_estimate=facts.dose_estimate,
                tilt_axis=facts.tilt_axis,
                mdoc_tilt_axis=facts.tilt_axis,
                acquisition_software=facts.software,
                software_version=facts.software_version,
                detector_dimensions=facts.detector_dimensions,
            )
            tilt_series_list.append(ts)

            if stack is None and not ts.movies_complete:
                found = sum(1 for t in tilts if t.frame_path is not None)
                warnings.append(f"{ts.ts_label}: no stack, and {found}/{ts.tilt_count} movies found")

            if progress_cb:
                progress_cb(mdoc_idx + 1, total_mdocs)

        if ignored:
            warnings.append(describe_ignored_mdocs(ignored))

        positions = aggregate_to_positions(tilt_series_list)
        first_with_software = next((ts for ts in tilt_series_list if ts.acquisition_software), None)
        first_with_dims = next((ts for ts in tilt_series_list if ts.detector_dimensions), None)

        overview = DatasetOverview(
            source_directory=str(resolved_frames_dir) if resolved_frames_dir else str(Path(mdocs_glob).parent),
            frame_extension=self._detect_frame_extension(resolved_frames_dir) if tilt_series_list else "",
            positions=positions,
            parse_warnings=warnings,
            acquisition_summary=AcquisitionSummary.from_tilt_series(tilt_series_list),
            acquisition_software=first_with_software.acquisition_software if first_with_software else "",
            software_version=first_with_software.software_version if first_with_software else "",
            detector_dimensions=first_with_dims.detector_dimensions if first_with_dims else None,
            ignored_mdocs=ignored,
        )
        gains = sorted({ts.gain_reference for ts in tilt_series_list if ts.gain_reference is not None})
        if len(gains) > 1:
            overview.gain_references_differ = not same_image_data(gains)
        overview.apply_layer(overview.default_layer())
        return overview

    def _resolve_frames_directory(self, mdoc_files: list[Path], frames_dir: str | None) -> Path | None:
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
            except Exception as e:
                # Inference heuristic only — fall back to the mdoc's own directory below.
                logger.warning("Could not infer frames dir from %s: %s", mdoc_files[0].name, e)

            return mdoc_files[0].parent

        return None

    def _detect_frame_extension(self, frames_dir: Path | None) -> str:
        if not frames_dir or not frames_dir.exists():
            return ""
        for ext in _MOVIE_EXTENSIONS:
            if any(frames_dir.glob(f"*{ext}")):
                return ext
        return ""

    def _build_tilt_info(self, section: dict, frames_dir: Path | None, mdoc_dir: Path) -> TiltInfo | None:
        """Build a TiltInfo from a parsed mdoc ZValue section. The movie is looked for
        in `frames_dir`, then beside the mdoc (one-folder-per-series deliveries)."""
        z_value_str = section.get("ZValue")
        if z_value_str is None:
            return None

        try:
            z_value = int(z_value_str)
        except (ValueError, TypeError):
            # Malformed ZValue means the section can't be ordered — drop this tilt.
            logger.warning("Skipping mdoc section with malformed ZValue %r", z_value_str)
            return None

        tilt_angle = 0.0
        if "TiltAngle" in section:
            try:
                tilt_angle = float(section["TiltAngle"])
            except (ValueError, TypeError):
                # Malformed TiltAngle — keep the 0.0 placeholder rather than drop the tilt.
                logger.warning("Malformed TiltAngle %r in mdoc section ZValue=%s", section["TiltAngle"], z_value_str)

        sub_frame_path = section.get("SubFramePath", "")
        if not sub_frame_path:
            return None

        # Extract bare filename, handling Windows-style paths
        frame_filename = Path(sub_frame_path.replace("\\", "/")).name

        # Resolve the actual file path
        frame_path = None
        for directory in (frames_dir, mdoc_dir):
            if directory is None:
                continue
            candidate = directory / frame_filename
            if candidate.exists():
                frame_path = candidate.resolve()
                break

        # Extract numeric MDOC stats for per-tilt metadata registry
        mdoc_stats: dict[str, float] = {}
        mmm = section.get("MinMaxMean", "")
        if mmm:
            parts = mmm.split()
            if len(parts) >= 3:
                try:
                    mdoc_stats["min_intensity"] = float(parts[0])
                    mdoc_stats["max_intensity"] = float(parts[1])
                    mdoc_stats["mean_intensity"] = float(parts[2])
                except (ValueError, TypeError):
                    # Malformed MinMaxMean — optional per-tilt stats, skip them.
                    pass
        for mdoc_key, stat_key in [
            ("ExposureDose", "exposure_dose"),
            ("PriorRecordDose", "prior_dose"),
            ("DoseRate", "dose_rate"),
            ("Defocus", "defocus"),
            ("ExposureTime", "exposure_time"),
        ]:
            val = section.get(mdoc_key)
            if val is not None:
                try:
                    mdoc_stats[stat_key] = float(val)
                except (ValueError, TypeError):
                    # Non-numeric mdoc value — optional per-tilt stat, skip it.
                    pass
        ish = section.get("ImageShift", "")
        if ish:
            parts = ish.split()
            if len(parts) >= 2:
                try:
                    mdoc_stats["image_shift_x"] = float(parts[0])
                    mdoc_stats["image_shift_y"] = float(parts[1])
                except (ValueError, TypeError):
                    # Malformed ImageShift pair — optional per-tilt stat, skip it.
                    pass

        return TiltInfo(
            z_value=z_value,
            tilt_angle=tilt_angle,
            frame_filename=frame_filename,
            frame_path=frame_path,
            mdoc_stats=mdoc_stats,
            date_time=section.get("DateTime"),
        )


_dataset_parsing_service: DatasetParsingService | None = None


def get_dataset_parsing_service() -> DatasetParsingService:
    global _dataset_parsing_service
    if _dataset_parsing_service is None:
        _dataset_parsing_service = DatasetParsingService()
    return _dataset_parsing_service
