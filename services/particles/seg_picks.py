"""Ais's blob picks of one feature's segmentation, as candidate rows: one per pick, with its score, the piece it
sits in, its starting angles and where they came from, and a mark when it lies near the volume's edge.

Pure (arrays and DataFrames in, a DataFrame out); the Pick candidates driver does the file I/O.

Ais (`ais pick`, blob mode) mean-bins the stored values, keeps the voxels above its threshold, labels the pieces
they make (face-connected), leaves out pieces smaller than its size, and places picks: the peaks of the distance to
a piece's edge, at least its spacing apart, or one at each piece's centroid (`-centroid`). It writes binned voxel
indices times the binning (x y z, 0-based, corner origin) and, in centroid mode with `-orient`, the tilt and psi
that lay the reference's Z along each piece's axis, with rot 0. The pieces are labelled again here with Ais's own
arithmetic, so that each pick knows its piece.
"""

from __future__ import annotations

import zlib
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import ndimage
from scipy.spatial import cKDTree

from services.models_base import PickOrientation
from services.particles.coords import CENTERED_COLS, voxel_to_centered_angst

AIS_COORD_COLS = ["rlnCoordinateX", "rlnCoordinateY", "rlnCoordinateZ"]
ANGLE_COLS = ["rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi"]

SCORE_COL = "rlnAutopickFigureOfMerit"
ANGLE_SOURCE_COL = "cbAngleSource"
PIECE_VOLUME_COL = "cbPieceVolumeNm3"
PIECE_PICKS_COL = "cbPiecePicks"
MARK_COL = "cbMarkReason"
NOT_MARKED = "none"
EDGE = "edge"

# Where a pick's angles came from (`cbAngleSource`).
ANGLES_RANDOM = "random"
ANGLE_SOURCE = {PickOrientation.LONG_AXIS: "long_axis", PickOrientation.SHORT_AXIS: "short_axis"}
# Ais's `-orient` for each axis: `normal` is a piece's smallest principal axis.
AIS_ORIENT = {PickOrientation.LONG_AXIS: "long-axis", PickOrientation.SHORT_AXIS: "normal"}

# The score is the mean p within this fraction of the diameter of a pick; a pick closer than half the diameter to
# the volume's faces has part of its particle outside the volume.
SCORE_RADIUS = 0.25
EDGE_DISTANCE = 0.5

# A centroid Ais wrote and the centroid of the piece labelled here agree to the precision of the star.
CENTROID_TOLERANCE = 0.01  # binned voxels

OUTPUT_COLUMNS = [
    "rlnTomoName",
    *CENTERED_COLS,
    *ANGLE_COLS,
    ANGLE_SOURCE_COL,
    SCORE_COL,
    PIECE_VOLUME_COL,
    PIECE_PICKS_COL,
    MARK_COL,
]


@dataclass(frozen=True)
class Detection:
    """What the job asked for, in the units of its parameters."""

    p_threshold: float
    min_piece: float  # of a sphere of the diameter
    one_per_piece: bool
    binning: int
    orientation: PickOrientation  # the effective one: random unless one per piece
    diameter_ang: float


# ---------------------------------------------------------------- Ais's arithmetic


def ais_threshold_divisor(dtype) -> float:
    """What Ais divides its threshold by before it compares it with the values of a volume of this data type: its
    threshold is on a 0–255 scale, so 255 for float32; 2 for int8; 1 for anything else."""
    dt = np.dtype(dtype)
    if dt == np.float32:
        return 255.0
    if dt == np.int8:
        return 2.0
    return 1.0


def stored_cut(p_threshold: float, dtype, full_scale: float) -> float:
    """The stored value a voxel must exceed to count as the feature at p ≥ `p_threshold`, Ais's compare being
    strict (`>`). On integer storage the cut lies half a step below the first stored value at or above
    p × full scale, so the compare means p ≥ the threshold exactly at binning 1, and a mean of binned blocks is cut
    at the same level. On floats it is p × full scale, where equality has no weight."""
    target = p_threshold * full_scale
    if np.dtype(dtype).kind in "iu":
        return float(np.ceil(target - 1e-9)) - 0.5
    return float(target)


def ais_threshold(p_threshold: float, dtype, full_scale: float) -> float:
    """Ais's `-threshold` for p ≥ `p_threshold` on a volume of this data type and full scale (the stored value of
    p = 1, which the manifest states)."""
    return stored_cut(p_threshold, dtype, full_scale) * ais_threshold_divisor(dtype)


def min_piece_voxels(min_piece: float, diameter_ang: float, voxel_angstrom: float, binning: int) -> int:
    """The fewest binned voxels a piece may have and stay: `min_piece` of a sphere of the diameter, at least one."""
    sphere = np.pi / 6.0 * diameter_ang**3
    return max(1, int(np.ceil(min_piece * sphere / (voxel_angstrom * binning) ** 3 - 1e-9)))


def ais_size_px(min_voxels: int, binning: int) -> float:
    """Ais's `-size-px` (voxels of the volume as stored) for pieces of at least `min_voxels` binned voxels. Ais
    leaves out a piece of fewer than size-px ÷ binning³ voxels; half a voxel below the count keeps a piece of
    exactly that count whatever the rounding."""
    return (min_voxels - 0.5) * binning**3


def detection_grid(stored: np.ndarray, binning: int) -> np.ndarray:
    """The volume as Ais detects on it: float32, cut to whole blocks and mean-binned in Ais's order of means, so a
    voxel on the threshold falls the same way here as in Ais."""
    data = np.asarray(stored).astype(np.float32)
    b = int(binning)
    if b > 1:
        z, y, x = data.shape
        data = data[: z // b * b, : y // b * b, : x // b * b]
        data = (
            data.reshape((z // b, b, y // b, b, x // b, b))
            .mean(5, dtype=np.float32)
            .mean(3, dtype=np.float32)
            .mean(1, dtype=np.float32)
        )
    return data


def pieces(grid: np.ndarray, threshold: float, dtype, min_voxels: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Ais's pieces on the detection grid (`detection_grid` of a volume of data type `dtype`) at its `-threshold`:
    the labels, with the pieces smaller than `min_voxels` set to 0; the voxel count of every label; and the labels
    of the pieces that stay."""
    labels, _ = ndimage.label(grid > threshold / ais_threshold_divisor(dtype))
    sizes = np.bincount(labels.ravel())
    small = sizes < min_voxels - 0.5
    small[0] = False
    labels[small[labels]] = 0
    kept = np.flatnonzero(~small)
    return labels, sizes, kept[kept > 0]


# ---------------------------------------------------------------- per pick


def random_angles(n: int, rng: np.random.Generator) -> np.ndarray:
    """n RELION (rot, tilt, psi) uniform on SO(3), in degrees: rot and psi uniform, cos(tilt) uniform."""
    rot = rng.uniform(-180.0, 180.0, n)
    psi = rng.uniform(-180.0, 180.0, n)
    tilt = np.degrees(np.arccos(rng.uniform(-1.0, 1.0, n)))
    return np.stack([rot, tilt, psi], axis=1)


def job_seed(instance_id: str) -> int:
    """The seed of a job's random angles: the same job draws the same angles when it runs again."""
    return zlib.crc32(instance_id.encode())


def tomogram_rng(seed: int, tomo: str) -> np.random.Generator:
    """The job's random stream for one tomogram, independent of the order its tasks run in."""
    return np.random.default_rng([seed, zlib.crc32(tomo.encode())])


def mean_p_within(stored: np.ndarray, centres_zyx: np.ndarray, radius_vox: float, full_scale: float) -> np.ndarray:
    """The mean p over the voxels within `radius_vox` of each centre's voxel (the volume's own grid), counting only
    those inside the volume."""
    r = max(float(radius_vox), 0.0)
    reach = int(np.floor(r))
    span = np.arange(-reach, reach + 1)
    dz, dy, dx = np.meshgrid(span, span, span, indexing="ij")
    keep = dz**2 + dy**2 + dx**2 <= r * r
    offsets = np.stack([dz[keep], dy[keep], dx[keep]], axis=1)
    shape = np.asarray(stored.shape)
    centres = np.rint(np.asarray(centres_zyx, dtype=float)).astype(int)
    out = np.empty(len(centres))
    for start in range(0, len(centres), 256):
        block = centres[start : start + 256]
        pts = block[:, None, :] + offsets[None, :, :]
        inside = ((pts >= 0) & (pts < shape)).all(axis=2)
        clipped = np.clip(pts, 0, shape - 1)
        vals = stored[clipped[..., 0], clipped[..., 1], clipped[..., 2]].astype(np.float64)
        out[start : start + 256] = (vals * inside).sum(axis=1) / inside.sum(axis=1)
    return out / full_scale


def edge_distance_vox(voxel_xyz: np.ndarray, size_xyz: np.ndarray) -> np.ndarray:
    """How far each position (voxel indices) lies from the nearest face of the volume, in voxels: the faces are
    half a voxel beyond the first and the last voxel centres."""
    v = np.asarray(voxel_xyz, dtype=float)
    return np.minimum(v + 0.5, np.asarray(size_xyz, dtype=float) - 0.5 - v).min(axis=1)


def _pick_labels(labels: np.ndarray, ids: np.ndarray, binned_zyx: np.ndarray, centroid: bool, tomo: str) -> np.ndarray:
    """The piece of each pick: the label under a distance-transform peak, or the piece (of those that stay, `ids`)
    whose centroid Ais wrote."""
    if len(binned_zyx) == 0:
        return np.zeros(0, dtype=int)
    if not centroid:
        idx = np.rint(binned_zyx).astype(int)
        found = labels[idx[:, 0], idx[:, 1], idx[:, 2]]
        if (found == 0).any():
            raise ValueError(
                f"{tomo}: {int((found == 0).sum())} of Ais's picks lie in no piece at its threshold here: the volume "
                "or the threshold is not the one Ais read"
            )
        return found
    if len(ids) == 0:
        raise ValueError(f"{tomo}: Ais wrote {len(binned_zyx)} centroids, but no piece stays at its threshold here")
    centroids = np.asarray(ndimage.center_of_mass(labels > 0, labels, ids), dtype=float).reshape(-1, 3)
    dist, nearest = cKDTree(centroids).query(binned_zyx)
    if (dist > CENTROID_TOLERANCE).any():
        raise ValueError(
            f"{tomo}: {int((dist > CENTROID_TOLERANCE).sum())} of Ais's centroids are no piece's centroid here "
            f"(off by up to {dist.max():.3g} binned voxels): the volume or the threshold is not the one Ais read"
        )
    return ids[nearest]


def pick_rows(
    ais: pd.DataFrame,
    stored: np.ndarray,
    *,
    tomo: str,
    voxel_angstrom: float,
    full_scale: float,
    det: Detection,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, dict]:
    """One tomogram's Ais picks as candidate rows, and its summary. `stored` is the feature's volume as stored (on
    the manifest's grid, z y x), `voxel_angstrom` and `full_scale` the manifest's; the angles drawn come from
    `rng`."""
    if ais.empty:
        ais = pd.DataFrame(columns=AIS_COORD_COLS)
    missing = [c for c in AIS_COORD_COLS if c not in ais.columns]
    axis = det.orientation != PickOrientation.RANDOM
    if axis:
        missing += [c for c in ANGLE_COLS if c not in ais.columns]
    if missing:
        raise ValueError(f"{tomo}: Ais's picks lack {missing} (columns: {list(ais.columns)})")

    b = int(det.binning)
    shape_zyx = np.asarray(stored.shape)
    size_xyz = shape_zyx[::-1].astype(float)
    ais_xyz = ais[AIS_COORD_COLS].to_numpy(dtype=float)
    # A binned voxel j covers voxels j*b … j*b + b − 1; Ais writes j*b (a centroid: the mean of such j, times b).
    voxel_xyz = ais_xyz + (b - 1) / 2.0
    outside = ((voxel_xyz < 0) | (voxel_xyz > size_xyz - 1)).any(axis=1)
    if outside.any():
        raise ValueError(
            f"{tomo}: {int(outside.sum())} of Ais's picks lie outside the {tuple(int(v) for v in size_xyz)} grid "
            "(x y z) the manifest names"
        )

    min_voxels = min_piece_voxels(det.min_piece, det.diameter_ang, voxel_angstrom, b)
    threshold = ais_threshold(det.p_threshold, stored.dtype, full_scale)
    labels, sizes, kept = pieces(detection_grid(stored, b), threshold, stored.dtype, min_voxels)
    picked = _pick_labels(labels, kept, ais_xyz[:, ::-1] / b, det.one_per_piece, tomo)
    per_piece = np.bincount(picked, minlength=len(sizes))

    n = len(ais)
    angles = random_angles(n, rng)
    source = np.full(n, ANGLES_RANDOM, dtype=object)
    if axis and n:
        given = ais[ANGLE_COLS].to_numpy(dtype=float)
        # Ais writes 0, 0, 0 for a piece too round for the axis asked for.
        defined = ~(given == 0.0).all(axis=1)
        angles[defined, 1:] = given[defined, 1:]
        source[defined] = ANGLE_SOURCE[det.orientation]

    edge = edge_distance_vox(voxel_xyz, size_xyz) * voxel_angstrom < EDGE_DISTANCE * det.diameter_ang
    rows = pd.DataFrame(voxel_to_centered_angst(voxel_xyz, size_xyz, voxel_angstrom), columns=CENTERED_COLS)
    rows.insert(0, "rlnTomoName", tomo)
    rows[ANGLE_COLS] = angles
    rows[ANGLE_SOURCE_COL] = source
    rows[SCORE_COL] = mean_p_within(
        stored, voxel_xyz[:, ::-1], SCORE_RADIUS * det.diameter_ang / voxel_angstrom, full_scale
    )
    rows[PIECE_VOLUME_COL] = sizes[picked] * (voxel_angstrom * b) ** 3 / 1000.0
    rows[PIECE_PICKS_COL] = per_piece[picked]
    rows[MARK_COL] = np.where(edge, EDGE, NOT_MARKED)
    rows = rows[OUTPUT_COLUMNS]

    summary = {
        "pieces": len(kept),
        "picks": n,
        "median_score": float(np.median(rows[SCORE_COL])) if n else None,
        "edge": int(edge.sum()),
        "angle_sources": {str(k): int(v) for k, v in pd.Series(source).value_counts().sort_index().items()},
        "min_piece_voxels": min_voxels,
        "ais_threshold": threshold,
    }
    return rows, summary
