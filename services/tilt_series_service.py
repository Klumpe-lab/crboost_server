# services/tilt_series_service.py
"""
Service for reading, writing, and filtering tilt series STAR files.

Replaces the old CryoBoost's tiltSeriesMeta class for tilt-filtering workflows,
built on the patterns already established in MetadataTranslator.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from services.configs.starfile_service import StarfileService

logger = logging.getLogger(__name__)

_starfile_svc: Optional[StarfileService] = None


def _get_starfile_service() -> StarfileService:
    global _starfile_svc
    if _starfile_svc is None:
        _starfile_svc = StarfileService()
    return _starfile_svc


class TiltSeriesData:
    """
    In-memory representation of a hierarchical tilt series STAR file.

    Holds a merged DataFrame of all tilts (with per-tilt + per-series columns)
    and knows how to split it back for writing.
    """

    def __init__(self, all_tilts_df: pd.DataFrame, tilt_series_df: pd.DataFrame, num_ts_cols: int):
        self.all_tilts_df = all_tilts_df
        self.tilt_series_df = tilt_series_df
        self.num_ts_cols = num_ts_cols

    @property
    def num_tomograms(self) -> int:
        return self.tilt_series_df["rlnTomoName"].nunique()

    @property
    def num_tilts(self) -> int:
        return len(self.all_tilts_df)

    @property
    def tilt_series_names(self) -> List[str]:
        return sorted(self.all_tilts_df["rlnTomoName"].unique().tolist())


def load_tilt_series(star_path: str | Path, project_root: str | Path) -> TiltSeriesData:
    """
    Load a hierarchical tilt series STAR file into a merged DataFrame.

    Mirrors MetadataTranslator._load_all_tilt_series() but returns a
    standalone TiltSeriesData object.
    """
    star_path = Path(star_path)
    project_root = Path(project_root)
    svc = _get_starfile_service()

    data = svc.read(star_path)
    tilt_series_df = next(iter(data.values()))
    num_ts_cols = len(tilt_series_df.columns)

    all_tilts: list[pd.DataFrame] = []
    input_star_dir = star_path.parent

    for _, ts_row in tilt_series_df.iterrows():
        ts_file = ts_row["rlnTomoTiltSeriesStarFile"]

        # Resolve path: relative to star dir first, then project root
        ts_path = None
        for base in (input_star_dir, project_root):
            candidate = base / ts_file
            if candidate.exists():
                ts_path = candidate
                break

        if ts_path is None:
            logger.warning(
                "Tilt series file not found: %s (tried relative to %s and %s)", ts_file, input_star_dir, project_root
            )
            continue

        try:
            ts_data = svc.read(ts_path)
            ts_df = next(iter(ts_data.values()))

            # Generate cryoBoostKey from micrograph name
            name_col = "rlnMicrographMovieName" if "rlnMicrographMovieName" in ts_df.columns else "rlnMicrographName"
            ts_df["cryoBoostKey"] = ts_df[name_col].apply(lambda x: Path(x).stem)

            # Repeat series-level row to match number of tilts
            ts_row_repeated = pd.concat([pd.DataFrame(ts_row).T] * len(ts_df), ignore_index=True)
            merged = pd.concat([ts_row_repeated.reset_index(drop=True), ts_df.reset_index(drop=True)], axis=1)
            all_tilts.append(merged)
        except Exception as e:
            logger.error("Failed to load tilt series file %s: %s", ts_path, e)
            continue

    if not all_tilts:
        raise ValueError(f"No tilt series files could be loaded from {star_path}")

    all_tilts_df = pd.concat(all_tilts, ignore_index=True)

    # Move cryoBoostKey to end
    key_values = all_tilts_df["cryoBoostKey"]
    all_tilts_df = all_tilts_df.drop("cryoBoostKey", axis=1)
    all_tilts_df["cryoBoostKey"] = key_values

    logger.info("Loaded %d tilts from %d tilt series (%s)", len(all_tilts_df), len(tilt_series_df), star_path.name)
    return TiltSeriesData(all_tilts_df, tilt_series_df, num_ts_cols)


def get_tilt_image_paths(ts_data: TiltSeriesData, project_root: str | Path) -> List[str]:
    """Return absolute paths to all tilt MRC images."""
    project_root = str(project_root).rstrip("/") + "/"
    col = "rlnMicrographName" if "rlnMicrographName" in ts_data.all_tilts_df.columns else "rlnMicrographMovieName"
    paths = []
    for p in ts_data.all_tilts_df[col].tolist():
        if Path(p).is_absolute():
            paths.append(p)
        else:
            paths.append(project_root + p)
    return paths


def apply_labels(ts_data: TiltSeriesData, labels: Dict[str, str]) -> TiltSeriesData:
    """
    Apply good/bad labels to tilts. Labels dict is keyed by cryoBoostKey.

    If no existing label column, creates one defaulting to "good".
    """
    df = ts_data.all_tilts_df
    if "cryoBoostDlLabel" not in df.columns:
        df["cryoBoostDlLabel"] = "good"
    if "cryoBoostDlProbability" not in df.columns:
        df["cryoBoostDlProbability"] = 1.0

    for key, label in labels.items():
        mask = df["cryoBoostKey"] == key
        df.loc[mask, "cryoBoostDlLabel"] = label

    return ts_data


def filter_good_tilts(ts_data: TiltSeriesData) -> TiltSeriesData:
    """Return a new TiltSeriesData containing only 'good' tilts."""
    df = ts_data.all_tilts_df
    if "cryoBoostDlLabel" not in df.columns:
        return ts_data

    good_df = df[df["cryoBoostDlLabel"] == "good"].copy().reset_index(drop=True)

    # Also filter tilt_series_df to only include series that still have tilts
    remaining_names = set(good_df["rlnTomoName"].unique())
    filtered_ts_df = (
        ts_data.tilt_series_df[ts_data.tilt_series_df["rlnTomoName"].isin(remaining_names)]
        .copy()
        .reset_index(drop=True)
    )

    return TiltSeriesData(good_df, filtered_ts_df, ts_data.num_ts_cols)


def write_tilt_series(ts_data: TiltSeriesData, output_path: str | Path, subfolder: str = "tilt_series"):
    """
    Write tilt series data back to hierarchical STAR format.

    Mirrors MetadataTranslator._write_updated_star().
    """
    output_path = Path(output_path)
    svc = _get_starfile_service()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tilt_series_dir = output_path.parent / subfolder
    tilt_series_dir.mkdir(exist_ok=True)

    df = ts_data.all_tilts_df
    num_ts_cols = ts_data.num_ts_cols

    # Extract and deduplicate series-level columns
    ts_df = df.iloc[:, :num_ts_cols].copy()
    ts_df = ts_df.drop("cryoBoostKey", axis=1, errors="ignore")
    ts_df = ts_df.drop_duplicates().reset_index(drop=True)

    # Update paths to point to subfolder
    ts_df["rlnTomoTiltSeriesStarFile"] = ts_df["rlnTomoTiltSeriesStarFile"].apply(
        lambda x: f"{subfolder}/{Path(x).name}"
    )

    # Write main star file
    svc.write({"global": ts_df}, output_path)

    # Write individual tilt series files
    for ts_name in ts_df["rlnTomoName"]:
        ts_tilts = df[df["rlnTomoName"] == ts_name].copy()
        ts_tilts_only = ts_tilts.iloc[:, num_ts_cols:].copy()
        ts_tilts_only = ts_tilts_only.drop("cryoBoostKey", axis=1, errors="ignore")
        # Drop DL label/probability columns from per-tilt star files (keep in main only)
        for col in ("cryoBoostDlLabel", "cryoBoostDlProbability"):
            ts_tilts_only = ts_tilts_only.drop(col, axis=1, errors="ignore")

        ts_file = tilt_series_dir / f"{ts_name}.star"
        svc.write({ts_name: ts_tilts_only}, ts_file)

    logger.info("Wrote tilt series to %s (%d tilts, %d series)", output_path, len(df), len(ts_df))


def get_label_summary(ts_data: TiltSeriesData) -> Dict[str, int]:
    """Return counts of good/bad/unlabeled tilts."""
    df = ts_data.all_tilts_df
    total = len(df)
    if "cryoBoostDlLabel" not in df.columns:
        return {"total": total, "good": total, "bad": 0, "unlabeled": 0}

    good = int((df["cryoBoostDlLabel"] == "good").sum())
    bad = int((df["cryoBoostDlLabel"] == "bad").sum())
    unlabeled = total - good - bad
    return {"total": total, "good": good, "bad": bad, "unlabeled": unlabeled}
