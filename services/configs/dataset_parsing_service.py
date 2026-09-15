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
from collections import Counter, defaultdict
from pathlib import Path
from collections.abc import Callable

from services.configs.mdoc_service import MdocFacts, acquisition_from_mdoc, get_mdoc_service, ts_name_from_mdoc
from services.stack_import import choose_source_layer, resolve_stack
from services.tilt_series.preimport import (
    AcquisitionSummary,
    DatasetOverview,
    SourceKind,
    StagePositionInfo,
    TiltInfo,
    TiltSeriesInfo,
)
from services.tilt_series.build import parse_position

logger = logging.getLogger(__name__)

_MOVIE_EXTENSIONS = (".eer", ".tiff", ".tif", ".mrc")


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
        skipped_no_sections = 0

        total_mdocs = len(mdoc_paths)
        if progress_cb:
            progress_cb(0, total_mdocs)

        for mdoc_idx, mdoc_path in enumerate(mdoc_paths):
            try:
                mdoc_data = self.mdoc_service.parse_mdoc_file(mdoc_path)
            except Exception as e:
                warnings.append(f"Failed to parse {mdoc_path.name}: {e}")
                continue

            if not mdoc_data["data"]:
                # A raw SerialEM delivery holds one per-movie mdoc per tilt (no [ZValue]
                # section) next to the stack mdoc — those are not tilt-series.
                skipped_no_sections += 1
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
            source_kind, stack = self._classify_source(mdoc_path, tilts, facts)
            if source_kind == "stack":
                for tilt in tilts:
                    tilt.frame_filename = Path(tilt.frame_filename).with_suffix(".mrc").name

            ts = TiltSeriesInfo(
                ts_name=ts_name,
                stage_position=stage_pos,
                beam_position=beam_pos,
                mdoc_filename=mdoc_path.name,
                mdoc_path=mdoc_path,
                tilts=tilts,
                source_kind=source_kind,
                stack_path=stack[0] if stack else None,
                stack_nz=stack[1] if stack else None,
                pixel_size=facts.pixel_size,
                voltage=facts.voltage,
                dose_per_tilt=facts.dose_per_tilt,
                dose_estimate=facts.dose_estimate,
                tilt_axis=facts.tilt_axis,
                acquisition_software=facts.software,
                software_version=facts.software_version,
                detector_dimensions=facts.detector_dimensions,
            )
            tilt_series_list.append(ts)

            if source_kind == "missing":
                warnings.append(f"{ts.ts_label}: no movies and no stack found for {ts.tilt_count} tilts")
            elif ts.missing_frames > 0:
                warnings.append(f"{ts.ts_label}: {ts.missing_frames}/{ts.tilt_count} frames not found")

            if progress_cb:
                progress_cb(mdoc_idx + 1, total_mdocs)

        if skipped_no_sections:
            warnings.append(f"Skipped {skipped_no_sections} mdocs without [ZValue] sections (per-movie mdocs)")

        frame_ext = self._frame_extension(tilt_series_list, resolved_frames_dir, warnings)
        positions = self._aggregate_to_positions(tilt_series_list)
        first_with_software = next((ts for ts in tilt_series_list if ts.acquisition_software), None)
        first_with_dims = next((ts for ts in tilt_series_list if ts.detector_dimensions), None)

        return DatasetOverview(
            source_directory=str(resolved_frames_dir) if resolved_frames_dir else str(Path(mdocs_glob).parent),
            frame_extension=frame_ext,
            positions=positions,
            parse_warnings=warnings,
            acquisition_summary=AcquisitionSummary.from_tilt_series(tilt_series_list),
            acquisition_software=first_with_software.acquisition_software if first_with_software else "",
            software_version=first_with_software.software_version if first_with_software else "",
            detector_dimensions=first_with_dims.detector_dimensions if first_with_dims else None,
        )

    def _classify_source(
        self, mdoc_path: Path, tilts: list[TiltInfo], facts: MdocFacts
    ) -> tuple[SourceKind, tuple[Path, int] | None]:
        """Movies when every SubFramePath resolved; stack when
        `<mdoc dir>/<ImageFile>` is an MRC with nz == sections; both complete →
        SerialEM prefers the (frame-aligned) stack, Tomo5 the movies."""
        movies_complete = bool(tilts) and all(t.frame_path is not None for t in tilts)
        movies_partial = any(t.frame_path is not None for t in tilts)
        stack = resolve_stack(mdoc_path, facts.image_file, facts.n_sections)
        kind = choose_source_layer(facts.software, movies_complete, movies_partial, stack is not None)
        return kind, (stack if kind == "stack" else None)

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

    def _frame_extension(
        self, tilt_series_list: list[TiltSeriesInfo], frames_dir: Path | None, warnings: list[str]
    ) -> str:
        """The extension the import will record: `.mrc` for stack series (the split
        writes MRC slices), the resolved movies' suffix otherwise; a directory sniff
        remains the fallback when nothing resolved at all."""
        kinds = Counter(ts.source_kind for ts in tilt_series_list if ts.source_kind != "missing")
        if len(kinds) > 1:
            warnings.append(
                "Mixed source layers: "
                + ", ".join(f"{n} series from {'stacks' if k == 'stack' else k}" for k, n in kinds.most_common())
            )
        if not kinds:
            return self._detect_frame_extension(frames_dir)
        if kinds.most_common(1)[0][0] == "stack":
            return ".mrc"
        for ts in tilt_series_list:
            if ts.source_kind != "movies":
                continue
            for tilt in ts.tilts:
                if tilt.frame_path is not None:
                    return tilt.frame_path.suffix.lower()
        return self._detect_frame_extension(frames_dir)

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

    def _aggregate_to_positions(self, tilt_series_list: list[TiltSeriesInfo]) -> list[StagePositionInfo]:
        """Group tilt-series by stage position when every series has one (Tomo5 names);
        otherwise a single flat group, `stage_position=None`, sorted by name."""
        if not all(ts.stage_position is not None for ts in tilt_series_list):
            flat = sorted(tilt_series_list, key=lambda ts: ts.ts_name)
            return [StagePositionInfo(stage_position=None, tilt_series=flat)] if flat else []

        groups: dict[int, list[TiltSeriesInfo]] = defaultdict(list)
        for ts in tilt_series_list:
            groups[ts.stage_position].append(ts)

        positions = []
        for stage_pos in sorted(groups.keys()):
            series = sorted(groups[stage_pos], key=lambda ts: ts.beam_position or 1)
            positions.append(StagePositionInfo(stage_position=stage_pos, tilt_series=series))
        return positions


_dataset_parsing_service: DatasetParsingService | None = None


def get_dataset_parsing_service() -> DatasetParsingService:
    global _dataset_parsing_service
    if _dataset_parsing_service is None:
        _dataset_parsing_service = DatasetParsingService()
    return _dataset_parsing_service
