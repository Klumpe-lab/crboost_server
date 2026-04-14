"""Registry construction from import-time sources.

Two entry points:

- `build_from_dataset_overview(overview)` — preferred path. Takes the already-
  parsed `DatasetOverview` from `data_import_panel` and returns populated
  TiltSeries/Frame entities. No file reads.

- `build_from_mdocs(mdocs_glob, frames_dir)` — fallback for legacy projects
  that predate the registry. Re-parses mdocs via `MdocService`.

Both return a list of `TiltSeries`; callers are responsible for attaching
them to a `TiltSeriesRegistry`.
"""

from __future__ import annotations

import glob
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from services.dataset_models import DatasetOverview, TiltSeriesInfo
from services.tilt_series.models import Frame, TiltSeries

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# From DatasetOverview (preferred)
# ─────────────────────────────────────────────────────────────────────────────


def build_from_dataset_overview(
    overview: DatasetOverview, *, project_prefix: str = ""
) -> List[TiltSeries]:
    """Construct TS entities from an already-parsed DatasetOverview.

    `project_prefix` is prepended to TS labels to match how WarpTools names the
    per-TS XMLs/tomostar files on disk (which are typically
    `{project_name}_{ts_label}` after `ts_import`). Pass `""` to use the bare
    labels.
    """
    out: List[TiltSeries] = []
    for pos in overview.positions:
        for ts_info in pos.tilt_series:
            if not ts_info.selected:
                continue
            ts = _build_one_ts(ts_info, project_prefix=project_prefix)
            out.append(ts)
    return out


def _build_one_ts(ts_info: TiltSeriesInfo, *, project_prefix: str) -> TiltSeries:
    ts_label = ts_info.ts_label  # "Position_10" or "Position_10_2"
    ts_id = f"{project_prefix}{ts_label}" if project_prefix else ts_label

    # Frames: sort by z_value ascending to match acquisition order / Warp's Z.
    sorted_tilts = sorted(ts_info.tilts, key=lambda t: t.z_value)

    # Compute cumulative pre-exposure if not already present in the mdoc stats.
    # Dose is delivered per tilt; pre-exposure for tilt k = sum(dose) for j < k
    # in acquisition order.
    cumulative = 0.0
    dose_per_tilt = ts_info.dose_per_tilt or 0.0

    frames: List[Frame] = []
    for i, tilt in enumerate(sorted_tilts):
        frame_path = tilt.frame_path or Path(tilt.frame_filename)
        frame_id = Path(tilt.frame_filename).stem

        # Prefer mdoc-reported pre-exposure if present; else accumulate.
        pre_exposure = _coerce_float(tilt.mdoc_stats.get("PriorRecordDose"))
        if pre_exposure is None:
            pre_exposure = cumulative

        acq_time = _parse_mdoc_datetime(tilt.mdoc_stats.get("DateTime"))

        frames.append(
            Frame(
                id=frame_id,
                tilt_series_id=ts_id,
                raw_path=frame_path if frame_path.is_absolute() else Path(str(frame_path)),
                raw_filename=tilt.frame_filename,
                tilt_index=i,
                nominal_tilt_angle_deg=tilt.tilt_angle,
                pre_exposure_e_per_a2=pre_exposure,
                acquisition_time=acq_time,
            )
        )
        cumulative += dose_per_tilt

    return TiltSeries(
        id=ts_id,
        mdoc_path=ts_info.mdoc_path,
        mdoc_filename=ts_info.mdoc_filename,
        stage_position=ts_info.stage_position,
        beam_position=ts_info.beam_position,
        frames=frames,
        is_selected=ts_info.selected,
    )


# ─────────────────────────────────────────────────────────────────────────────
# From raw mdocs (fallback for legacy projects)
# ─────────────────────────────────────────────────────────────────────────────


def build_from_mdocs(
    mdocs_glob: str,
    *,
    frames_dir: Optional[Path] = None,
    project_prefix: str = "",
) -> List[TiltSeries]:
    """Parse mdocs directly when no DatasetOverview is available.

    `frames_dir` is optional; if provided, each frame's `raw_path` is resolved
    to an absolute path within it. Otherwise `raw_path` is left as the bare
    filename from the mdoc's `SubFramePath`.
    """
    from services.configs.mdoc_service import get_mdoc_service

    svc = get_mdoc_service()
    mdoc_paths = sorted(Path(p) for p in glob.glob(mdocs_glob))
    if not mdoc_paths:
        logger.warning("build_from_mdocs: no mdocs matched %s", mdocs_glob)
        return []

    out: List[TiltSeries] = []
    for mdoc_path in mdoc_paths:
        try:
            parsed = svc.parse_mdoc_file(mdoc_path)
        except Exception as e:
            logger.warning("Failed to parse mdoc %s: %s", mdoc_path, e)
            continue

        sections = parsed.get("data", [])
        if not sections:
            logger.warning("Empty mdoc (no ZValue sections): %s", mdoc_path)
            continue

        ts_label = mdoc_path.stem
        ts_id = f"{project_prefix}{ts_label}" if project_prefix else ts_label
        stage_position, beam_position = _infer_position(ts_label)

        # Sort by ZValue to nail down acquisition order.
        sorted_sections = sorted(sections, key=lambda s: int(s.get("ZValue", 0)))

        cumulative = 0.0
        frames: List[Frame] = []
        dose_per_tilt = _coerce_float(sorted_sections[0].get("ExposureDose")) or 0.0

        for i, sec in enumerate(sorted_sections):
            subframe_path = sec.get("SubFramePath", "").replace("\\", "/")
            frame_filename = os.path.basename(subframe_path) if subframe_path else f"{ts_id}_{i:03d}.eer"
            frame_id = Path(frame_filename).stem

            if frames_dir:
                raw_path = Path(frames_dir) / frame_filename
            else:
                raw_path = Path(frame_filename)

            pre_exposure = _coerce_float(sec.get("PriorRecordDose"))
            if pre_exposure is None:
                pre_exposure = cumulative

            frames.append(
                Frame(
                    id=frame_id,
                    tilt_series_id=ts_id,
                    raw_path=raw_path,
                    raw_filename=frame_filename,
                    tilt_index=i,
                    nominal_tilt_angle_deg=_coerce_float(sec.get("TiltAngle")) or 0.0,
                    pre_exposure_e_per_a2=pre_exposure,
                    acquisition_time=_parse_mdoc_datetime(sec.get("DateTime")),
                )
            )
            cumulative += dose_per_tilt

        out.append(
            TiltSeries(
                id=ts_id,
                mdoc_path=mdoc_path,
                mdoc_filename=mdoc_path.name,
                stage_position=stage_position,
                beam_position=beam_position,
                frames=frames,
            )
        )

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _coerce_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_mdoc_datetime(raw) -> Optional[datetime]:
    if not raw:
        return None
    # mdoc DateTime format is typically like "05-Feb-2026  17:15:24"
    for fmt in ("%d-%b-%Y  %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(raw).strip(), fmt)
        except ValueError:
            continue
    return None


def _infer_position(ts_label: str) -> tuple[int, int]:
    """Infer stage/beam from a ts_label like 'Position_10' or 'Position_10_2'.

    Returns (stage, beam). Beam defaults to 1 when not explicitly present.
    Returns (0, 1) for labels that don't match the expected pattern — this
    only affects the cosmetic display fields, not identity.
    """
    parts = ts_label.rsplit("_", 2)
    # Try: "..._Position_10_2" → stage=10, beam=2
    # Or:  "..._Position_10"    → stage=10, beam=1
    try:
        if len(parts) >= 3 and parts[-3].endswith("Position"):
            return int(parts[-2]), int(parts[-1])
        if len(parts) >= 2 and parts[-2].endswith("Position"):
            return int(parts[-1]), 1
    except ValueError:
        pass
    return 0, 1
