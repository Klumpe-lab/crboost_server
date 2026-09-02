# services/tilt_series_service.py
"""
Service for reading, writing, and filtering tilt series STAR files.

Replaces the old CryoBoost's tiltSeriesMeta class for tilt-filtering workflows,
built on the patterns already established in MetadataTranslator.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pandas as pd

from services.configs.starfile_service import StarfileService

logger = logging.getLogger(__name__)

_starfile_svc: StarfileService | None = None


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
    def tilt_series_names(self) -> list[str]:
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

    # Deduplicate tilt series by name (upstream STAR files can list the same
    # tilt series twice, e.g. with different rlnTomoHand values).
    n_before = len(tilt_series_df)
    tilt_series_df = tilt_series_df.drop_duplicates(subset=["rlnTomoName"], keep="first").reset_index(drop=True)
    if len(tilt_series_df) < n_before:
        logger.warning(
            "Deduplicated %d → %d tilt series by rlnTomoName in %s",
            n_before, len(tilt_series_df), star_path.name,
        )

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


def get_tilt_image_paths(ts_data: TiltSeriesData, project_root: str | Path) -> list[str]:
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


def apply_labels(ts_data: TiltSeriesData, labels: dict[str, str]) -> TiltSeriesData:
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


def drop_tilts_from_tomostar(src_dir: str | Path, out_dir: str | Path, bad_movie_stems: set[str]) -> tuple[int, int]:
    """Copy every ``*.tomostar`` from ``src_dir`` to ``out_dir``, dropping the
    loop rows whose ``_wrpMovieName`` basename stem is in ``bad_movie_stems``.

    This is the mechanism that lets the tilt-filter run *before* alignment: the
    WarpTools tomostar (one file per tilt-series, one row per tilt) is what
    ts_aretomo/ts_ctf/ts_reconstruct actually read, so trimming rows here
    propagates the cut through the whole downstream chain natively.

    Kept rows are copied verbatim (original whitespace + the same relative
    ``_wrpMovieName`` paths), so alignment's absolute-path staging resolves them
    exactly as it does for the tsImport tomostar. Match is by ``Path(name).stem``
    so a ``foo_EER.eer`` movie matches the ``foo_EER`` key the DL emits (the
    fs-motion star's ``rlnMicrographMovieName`` stem). Returns (kept, dropped).
    """
    src_dir = Path(src_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_kept = total_dropped = 0
    for star in sorted(src_dir.glob("*.tomostar")):
        raw = star.read_text()
        lines = raw.splitlines()

        col_names: list[str] = []
        data_start: int | None = None
        for i, line in enumerate(lines):
            s = line.strip()
            if s.startswith("_"):
                col_names.append(s.split()[0])
            elif s and not s.startswith(("data_", "loop_", "#")) and col_names:
                data_start = i
                break

        # Unexpected format (no loop / no movie column) -> copy verbatim rather
        # than silently mangle it. The filter is best-effort; alignment still
        # reads the untrimmed tomostar in that case.
        if data_start is None or "_wrpMovieName" not in col_names:
            (out_dir / star.name).write_text(raw)
            continue

        movie_i = col_names.index("_wrpMovieName")
        header = lines[:data_start]
        kept_rows: list[str] = []
        for line in lines[data_start:]:
            toks = line.split()
            if not toks:
                continue
            if len(toks) <= movie_i:
                kept_rows.append(line)  # malformed row -> keep, don't guess
                continue
            if Path(toks[movie_i]).stem in bad_movie_stems:
                total_dropped += 1
                continue
            # Absolutize the movie path so the trimmed tomostar resolves no matter
            # where it is written — the driver puts it under External/jobNNN/ (same
            # depth as tsImport), but the interactive filter writes to TiltFilter/ at
            # a different depth, which would otherwise break the `../../` movie paths.
            # Alignment's tomostar copy leaves already-absolute paths untouched.
            mv = toks[movie_i]
            if not Path(mv).is_absolute():
                toks[movie_i] = str((src_dir / mv).resolve())
            kept_rows.append("  ".join(toks))
            total_kept += 1

        (out_dir / star.name).write_text("\n".join(header + kept_rows) + "\n")

    logger.info("Tomostar trim: kept %d, dropped %d tilts across %s", total_kept, total_dropped, out_dir)
    return total_kept, total_dropped


def get_label_summary(ts_data: TiltSeriesData) -> dict[str, int]:
    """Return counts of good/bad/unlabeled tilts."""
    df = ts_data.all_tilts_df
    total = len(df)
    if "cryoBoostDlLabel" not in df.columns:
        return {"total": total, "good": total, "bad": 0, "unlabeled": 0}

    good = int((df["cryoBoostDlLabel"] == "good").sum())
    bad = int((df["cryoBoostDlLabel"] == "bad").sum())
    unlabeled = total - good - bad
    return {"total": total, "good": good, "bad": bad, "unlabeled": unlabeled}


def generate_tilt_thumbnails(
    ts_ctf_star: str | Path, project_path: str | Path, png_dir: str | Path, progress_cb=None, target_size: int = 384
) -> int:
    """Synchronously generate PNG thumbnails for every tilt referenced by a
    ts_ctf star file. Returns the number of source MRC images processed.

    Designed to be wrapped in `asyncio.to_thread`; the CPU-bound work uses
    a ProcessPoolExecutor internally. `progress_cb` matches the
    BackgroundTaskRegistry signature `(done, total, message)`.
    """
    # Lazy-import the heavy stack (scipy/PIL/mrcfile) to keep
    # tilt_series_service light when only the star-IO helpers are used.
    from filterTilts.image_processor import ImageProcessor

    ts_data = load_tilt_series(str(ts_ctf_star), str(project_path))
    paths = get_tilt_image_paths(ts_data, project_path)
    n = len(paths)
    if n == 0:
        return 0
    Path(png_dir).mkdir(parents=True, exist_ok=True)
    proc = ImageProcessor(target_size=target_size, max_workers=min(16, max(1, n)))
    proc.batch_convert(paths, n, str(png_dir), False, progress_cb)
    return n


# One kickoff attempt per (project, png_dir) per process: the background registry's
# dedup_key only covers a task that is still RUNNING, so without this a failed pass
# would be resubmitted on every Journey render.
_thumb_kickoffs: set[str] = set()


def _fs_motion_output_star(project_path: Path, job_model) -> Path | None:
    """The fsMotionAndCtf output star (`paths['output_star']`, else the job dir's
    fs_motion_and_ctf.star), or None when neither is on disk."""
    star_rel = (getattr(job_model, "paths", {}) or {}).get("output_star")
    if star_rel:
        p = Path(star_rel) if Path(star_rel).is_absolute() else project_path / star_rel
        if p.exists():
            return p
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn:
        p = project_path / rjn / "fs_motion_and_ctf.star"
        if p.exists():
            return p
    return None


def ensure_tilt_thumbnails(project_path: str | Path, state) -> bool:
    """Render the PNG previews of fsMotion's motion-corrected averages in the
    background if this project has none yet. Returns True when a task was submitted.

    The PNGs back BOTH the tilt-filter gallery and the Journey's per-tilt hover
    cards, so every project that ran fsMotionAndCtf needs them — with or without a
    tilt-filter job in the pipeline. Callers: both status reconcilers, on the
    fsMotion -> SUCCEEDED edge, and the Journey, which self-heals a run whose edge
    no server was around to observe. No-op when the PNGs already exist, when
    fsMotion hasn't succeeded, or when its output star isn't on disk. dedup_key
    matches ui/tilt_filter_panel.py so a manual click cannot double up with this.
    """
    from services.background_tasks import get_background_task_registry
    from services.models_base import JobStatus, JobType
    from services.project_state import get_state_service

    proj = Path(project_path)
    pd_str = getattr(state, "tilt_filter_png_dir", None) if state is not None else None
    png_dir = Path(pd_str) if pd_str else proj / "TiltFilter" / "png"
    if png_dir.exists() and any(png_dir.glob("*.png")):
        return False

    guard = f"{proj}:{png_dir}"
    if guard in _thumb_kickoffs:
        return False

    job_model = next(
        (
            jm
            for jm in (getattr(state, "jobs", None) or {}).values()
            if getattr(jm, "job_type", None) == JobType.FS_MOTION_CTF
        ),
        None,
    )
    if job_model is None or getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False

    source_star = _fs_motion_output_star(proj, job_model)
    if source_star is None:
        logger.info("Tilt thumbnails: fsMotion succeeded but no output star under %s — skipping", proj)
        return False

    _thumb_kickoffs.add(guard)

    async def _run(progress_cb):
        n = await asyncio.to_thread(generate_tilt_thumbnails, source_star, proj, png_dir, progress_cb)
        # Resolve by explicit path: this runs with no client/tab context, where a bare
        # current_project_state() would hand back a blank throwaway and the assignment
        # would silently no-op.
        st = get_state_service().state_for(proj)
        if st is not None:
            st.tilt_filter_png_dir = str(png_dir)
            st.mark_dirty()
            await get_state_service().save_project(project_path=proj, force=True)
        return f"{n} thumbnails generated"

    get_background_task_registry().submit(
        _run,
        title="Tilt thumbnails (auto)",
        subtitle=f"Motion-corrected tilt previews · {png_dir.name}",
        project_path=str(proj),
        dedup_key=f"tilt-filter-thumbnails:{proj}:{png_dir}",
    )
    logger.info("Tilt thumbnails: kicked off for %s (source %s)", proj, source_star)
    return True
