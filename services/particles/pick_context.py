"""What a project's segmentations say around each pick, as two columns per feature, for picks from either source:

- `cbP_<feature>`: the mean p of the feature over the voxels within ½ Ø of the pick, i.e. inside the particle;
- `cbDistAngst_<feature>`: the signed distance from the pick to the feature's surface (p ≥ 0.5, the wall's fill),
  negative inside: to the centre of the nearest voxel of the feature's boundary, on the volume as stored, so precise
  to one of its voxels.

A feature the tomogram has no volume of leaves both empty (NaN); so does the distance when the volume holds none of
the feature, or nothing else. The columns are computed on the manifest's grid, which is the tomogram's; context is
never read from segmentations of another reconstruction (`context_frame_problem`).
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import mrcfile
import numpy as np
import pandas as pd
from scipy import ndimage
from scipy.spatial import cKDTree

from services.job_lineage import recon_of
from services.models_base import JobType
from services.particles.coords import CENTERED_COLS, centered_angst_to_voxel
from services.particles.seg_picks import mean_p_within
from services.segmentation.manifest import SegmentationEntry
from services.segmentation.search_region import read_segmentations
from services.segmentation.volumes import DISPLAY_THRESHOLD, entry_dtype

P_COL = "cbP_{}"
DIST_COL = "cbDistAngst_{}"
# The mean p is taken within this fraction of the diameter: the particle's own volume.
P_RADIUS = 0.5


def context_columns(features) -> list[str]:
    return [c for f in sorted(features) for c in (P_COL.format(f), DIST_COL.format(f))]


def feature_context(
    stored: np.ndarray, centres_zyx: np.ndarray, *, full_scale: float, voxel_angstrom: float, diameter_ang: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per pick (`centres_zyx` in voxels of the volume): the mean p within ½ Ø, the signed distance to the
    feature's surface in Å, and whether the pick's voxel is the feature."""
    centres = np.asarray(centres_zyx, dtype=float).reshape(-1, 3)
    p = mean_p_within(stored, centres, P_RADIUS * diameter_ang / voxel_angstrom, full_scale)
    mask = np.asarray(stored) >= DISPLAY_THRESHOLD * full_scale
    idx = np.clip(np.rint(centres).astype(int), 0, np.asarray(mask.shape) - 1)
    inside = mask[idx[:, 0], idx[:, 1], idx[:, 2]]
    # The volume's faces are no surface: the feature may go on beyond them.
    boundary = np.argwhere(mask & ~ndimage.binary_erosion(mask, border_value=1))
    if len(boundary) == 0 or len(centres) == 0:
        return p, np.full(len(centres), np.nan), inside
    distance, _ = cKDTree(boundary).query(centres)
    return p, np.where(inside, -distance, distance) * voxel_angstrom, inside


def tomogram_context(
    rows: pd.DataFrame, entries: Mapping[str, SegmentationEntry], features, diameter_ang: float
) -> tuple[pd.DataFrame, dict[str, dict | None]]:
    """One tomogram's picks (`rows`, centred Å) with the context columns of `features`, read from `entries` (the
    tomogram's volumes by feature, on its grid), and per feature how many picks sit inside it and their median p;
    None for a feature the tomogram has no volume of."""
    out = rows.copy()
    summary: dict[str, dict | None] = {}
    coords = out[CENTERED_COLS].to_numpy(dtype=float) if len(out) else np.empty((0, 3))
    for feature in sorted(features):
        p_col, dist_col = P_COL.format(feature), DIST_COL.format(feature)
        entry = entries.get(feature)
        if entry is None:
            out[p_col], out[dist_col] = np.nan, np.nan
            summary[feature] = None
            continue
        entry_dtype(entry)
        size_xyz = np.asarray(entry.shape_zyx[::-1], dtype=float)
        centres = centered_angst_to_voxel(coords, size_xyz, entry.voxel_angstrom)[:, ::-1]
        with mrcfile.mmap(entry.path, mode="r", permissive=True) as m:
            p, distance, inside = feature_context(
                m.data,
                centres,
                full_scale=entry.full_scale,
                voxel_angstrom=entry.voxel_angstrom,
                diameter_ang=diameter_ang,
            )
        out[p_col], out[dist_col] = p, distance
        summary[feature] = {"inside": int(inside.sum()), "median_p": float(np.median(p)) if len(p) else None}
    return out, summary


def _segmentations_recons(
    state, source: Path, producer: str | None, paths: Mapping[str, Mapping[str, str]]
) -> set[str]:
    """The reconstruct jobs segmentations were computed on: those of the job about to make them in this Run, else
    those their manifest records, else their producer's. Empty when none can be told."""
    if producer and producer in state.jobs and (producer in paths or not source.is_file()):
        recon = recon_of(state, producer, paths=paths)
        return {recon} if recon else set()
    if source.is_file():
        manifest = read_segmentations(source)
        found = {e.recon_instance for e in manifest.entries if e.recon_instance}
        named = manifest.parameters.get("recon_instance")
        return found or ({str(named)} if named else set())
    return set()


def context_frame_problem(
    state, iid: str, *, producer: str | None = None, paths: Mapping[str, Mapping[str, str]] | None = None
) -> str | None:
    """Why a Pick candidates job from template matching cannot take context from the segmentations it reads: they
    were computed on another reconstruction than its picks. None when it reads none, they agree, or either side
    cannot be told; the driver then checks each volume against its tomogram's grid. `paths`: input paths a Run has
    just resolved, by instance; `producer`: the job that makes the segmentations. Reads their manifest once it is
    written."""
    jm = state.jobs.get(iid)
    if jm is None or jm.job_type != JobType.TEMPLATE_EXTRACT_PYTOM or jm.from_segmentation or not jm.context:
        return None
    paths = paths or {}
    source = (paths.get(iid) or jm.paths or {}).get("input_segmentations")
    if not source:
        return None
    picks = recon_of(state, iid, paths=paths)
    if picks is None:
        return None
    seg = _segmentations_recons(state, Path(source), producer, paths)
    if not seg or seg == {picks}:
        return None
    named = producer or Path(source).parent.name
    return (
        f"its context segmentations ({named}) were computed on {', '.join(sorted(seg))}, its picks on the tomograms "
        f"of {picks}; choose segmentations of {picks} in its I/O section, or set Context to no"
    )
