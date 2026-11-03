"""Ais filament traces → a candidate table: one row per traced point, oriented along its filament.

Pure (DataFrames in, DataFrames out); the Filament pick driver does the file I/O.

Ais (`ais pick -filament`) writes, per tomogram, points resampled along each traced filament in order,
as voxel indices of the input grid multiplied back by its binning, with an `aisFilamentID` per filament.
Only the positions, their order and the filament ids are taken from it. The direction at each point is
computed here from the neighbouring points, so the angle convention is this module's own: the reference's
Z axis lies along the filament. Ais's own tilt/psi are compared with it, per point (`cbAisTangentDiffDeg`).

Rules mark, they never drop: every point stays in the full table with `cbMarkReason`; a filament with a
reason is left out of the candidates.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from services.particles.coords import CENTERED_COLS, voxel_to_centered_angst

AIS_COORD_COLS = ["rlnCoordinateX", "rlnCoordinateY", "rlnCoordinateZ"]
AIS_REQUIRED = [*AIS_COORD_COLS, "rlnAngleTilt", "rlnAnglePsi", "aisFilamentID"]

TUBE_COL = "rlnHelicalTubeID"
MARK_COL = "cbMarkReason"
NOT_MARKED = "none"

# Reasons a filament is marked.
SHORT = "short"
TIGHT_BEND = "tight_bend"
ALONG_Z = "along_z"
UNEVEN_STEPS = "uneven_steps"

# A straight stretch has no finite radius; written as this many Å.
RADIUS_CAP_ANG = 1.0e6

OUTPUT_COLUMNS = [
    "rlnTomoName",
    *CENTERED_COLS,
    "rlnAngleRot",
    "rlnAngleTilt",
    "rlnAnglePsi",
    "rlnAngleTiltPrior",
    "rlnAnglePsiPrior",
    "rlnAnglePsiFlipRatio",
    TUBE_COL,
    "rlnHelicalTrackLengthAngst",
    "rlnRandomSubset",
    "cbFilamentLengthAngst",
    "cbCurvatureRadiusAngst",
    "cbAisTangentDiffDeg",
    "aisFilamentConnectivity",
    MARK_COL,
]


@dataclass(frozen=True)
class MarkRules:
    spacing_ang: float
    min_length_ang: float
    min_curvature_radius_ang: float
    min_angle_from_z_deg: float


def angles_from_tangents(t: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """RELION tilt and psi (degrees) that lay a reference's Z axis along each unit tangent (N × 3, x y z).
    RELION's Euler matrix maps the reference into the tomogram; its third column, the image of Z, is
    (−cos psi sin tilt, sin psi sin tilt, cos tilt) for any rot."""
    t = np.asarray(t, dtype=float)
    tilt = np.degrees(np.arccos(np.clip(t[:, 2], -1.0, 1.0)))
    psi = np.degrees(np.arctan2(t[:, 1], -t[:, 0]))
    return tilt, psi


def tangents_from_angles(tilt_deg: np.ndarray, psi_deg: np.ndarray) -> np.ndarray:
    """The unit vector (x y z) a reference's Z axis points along at RELION tilt and psi."""
    tilt, psi = np.radians(tilt_deg), np.radians(psi_deg)
    return np.stack([-np.cos(psi) * np.sin(tilt), np.sin(psi) * np.sin(tilt), np.cos(tilt)], axis=1)


def _unit_tangents(points: np.ndarray) -> np.ndarray:
    d = np.gradient(points, axis=0)
    norm = np.linalg.norm(d, axis=1, keepdims=True)
    return d / np.where(norm > 0, norm, 1.0)


def _curvature_radii(points: np.ndarray) -> np.ndarray:
    """The radius of the circle through each interior point and its two neighbours (Å); the cap at the ends
    and where three points are collinear."""
    radii = np.full(len(points), RADIUS_CAP_ANG)
    if len(points) < 3:
        return radii
    a, b, c = points[:-2], points[1:-1], points[2:]
    ab, bc, ca = (np.linalg.norm(u, axis=1) for u in (b - a, c - b, a - c))
    twice_area = np.linalg.norm(np.cross(b - a, c - a), axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        r = ab * bc * ca / (2.0 * twice_area)
    radii[1:-1] = np.where(np.isfinite(r), np.minimum(r, RADIUS_CAP_ANG), RADIUS_CAP_ANG)
    return radii


def filament_marks(points: np.ndarray, tangents: np.ndarray, radii: np.ndarray, rules: MarkRules) -> list[str]:
    """Why one filament (its points in order, Å) is left out of the candidates; empty when it is not."""
    reasons = []
    steps = np.linalg.norm(np.diff(points, axis=0), axis=1)
    if steps.sum() < rules.min_length_ang:
        reasons.append(SHORT)
    if len(points) >= 3 and np.median(radii[1:-1]) < rules.min_curvature_radius_ang:
        reasons.append(TIGHT_BEND)
    from_z = np.degrees(np.arccos(np.clip(np.abs(tangents[:, 2]), 0.0, 1.0)))
    if np.median(from_z) < rules.min_angle_from_z_deg:
        reasons.append(ALONG_Z)
    if len(steps) and ((steps < 0.5 * rules.spacing_ang) | (steps > 1.5 * rules.spacing_ang)).any():
        reasons.append(UNEVEN_STEPS)
    return reasons


def filament_points(
    ais: pd.DataFrame,
    *,
    tomo: str,
    shape_zyx: tuple[int, int, int],
    voxel_angstrom: float,
    binning: int,
    rules: MarkRules,
) -> pd.DataFrame:
    """One tomogram's Ais trace as candidate rows (without rot and half-set, which `finish_table` assigns
    over the whole job). `shape_zyx` and `voxel_angstrom` are the segmented grid's, from the Segment
    manifest, never from the star."""
    if ais.empty:
        return pd.DataFrame(columns=[c for c in OUTPUT_COLUMNS if c not in ("rlnAngleRot", "rlnRandomSubset")])
    missing = [c for c in AIS_REQUIRED if c not in ais.columns]
    if missing:
        raise ValueError(f"{tomo}: the Ais trace lacks {missing} (columns: {list(ais.columns)})")

    # A binned voxel j covers voxels j*b … j*b + b − 1; Ais writes j*b.
    voxel = ais[AIS_COORD_COLS].to_numpy(dtype=float) + (binning - 1) / 2.0
    size_xyz = np.asarray(shape_zyx[::-1], dtype=float)
    # The spline through the skeleton may overshoot the last voxel slightly; a binned voxel is the slack.
    outside = ((voxel < -binning) | (voxel > size_xyz - 1 + binning)).any(axis=1)
    if outside.any():
        raise ValueError(
            f"{tomo}: {int(outside.sum())} traced points lie outside the {shape_zyx[::-1]} grid (x y z) the "
            "Segment manifest names"
        )
    centred = voxel_to_centered_angst(voxel, size_xyz, voxel_angstrom)
    ids = ais["aisFilamentID"].to_numpy()
    connectivity = (
        ais["aisFilamentConnectivity"].to_numpy() if "aisFilamentConnectivity" in ais.columns else np.zeros(len(ais))
    )
    ais_t = tangents_from_angles(ais["rlnAngleTilt"].to_numpy(dtype=float), ais["rlnAnglePsi"].to_numpy(dtype=float))

    blocks = []
    for fid in pd.unique(ids):
        rows = np.flatnonzero(ids == fid)
        pts = centred[rows]
        steps = np.linalg.norm(np.diff(pts, axis=0), axis=1)
        t = _unit_tangents(pts) if len(pts) > 1 else ais_t[rows]
        tilt, psi = angles_from_tangents(t)
        radii = _curvature_radii(pts)
        reasons = filament_marks(pts, t, radii, rules)
        diff = np.degrees(np.arccos(np.clip(np.abs((t * ais_t[rows]).sum(axis=1)), 0.0, 1.0)))
        block = pd.DataFrame(pts, columns=CENTERED_COLS)
        block.insert(0, "rlnTomoName", tomo)
        block["rlnAngleTilt"] = tilt
        block["rlnAnglePsi"] = psi
        block["rlnAngleTiltPrior"] = tilt
        block["rlnAnglePsiPrior"] = psi
        block["rlnAnglePsiFlipRatio"] = 0.5
        block[TUBE_COL] = int(fid) + 1
        block["rlnHelicalTrackLengthAngst"] = np.concatenate([[0.0], np.cumsum(steps)])
        block["cbFilamentLengthAngst"] = float(steps.sum())
        block["cbCurvatureRadiusAngst"] = radii
        block["cbAisTangentDiffDeg"] = diff
        block["aisFilamentConnectivity"] = connectivity[rows]
        block[MARK_COL] = ",".join(reasons) or NOT_MARKED
        blocks.append(block)
    return pd.concat(blocks, ignore_index=True)


def finish_table(points: pd.DataFrame, *, seed: int = 0) -> pd.DataFrame:
    """The whole job's points with a random rot (searched globally) and a half-set per tube. Boxes along a
    tube overlap, so a tube never straddles the halves: tubes go, the longest first and unmarked ones before
    marked ones, to the half that has fewer points so far."""
    df = points.reset_index(drop=True).copy()
    rng = np.random.default_rng(seed)
    df["rlnAngleRot"] = rng.uniform(-180.0, 180.0, len(df))
    df["rlnRandomSubset"] = 0
    if df.empty:
        return df[OUTPUT_COLUMNS]
    keys = df["rlnTomoName"].astype(str) + "\t" + df[TUBE_COL].astype(str)
    tubes = (
        pd.DataFrame({"key": keys, "marked": df[MARK_COL] != NOT_MARKED})
        .groupby("key")
        .agg(n=("marked", "size"), marked=("marked", "first"))
        .reset_index()
        .sort_values(["marked", "n", "key"], ascending=[True, False, True])
    )
    counts = {1: 0, 2: 0}
    half_of = {}
    for key, n in zip(tubes["key"], tubes["n"], strict=True):
        half = 1 if counts[1] <= counts[2] else 2
        half_of[key] = half
        counts[half] += int(n)
    df["rlnRandomSubset"] = keys.map(half_of).astype(int)
    return df[OUTPUT_COLUMNS]


def candidates_of(table: pd.DataFrame) -> pd.DataFrame:
    return table[table[MARK_COL] == NOT_MARKED].reset_index(drop=True)


def tomogram_summary(table: pd.DataFrame) -> dict[str, dict]:
    """Per tomogram: filaments, points, unmarked points, traced length (Å) and marked filaments by reason."""
    out: dict[str, dict] = {}
    for tomo, grp in table.groupby("rlnTomoName", sort=True):
        tubes = grp.groupby(TUBE_COL).agg(
            length=("cbFilamentLengthAngst", "first"), reason=(MARK_COL, "first"), n=(MARK_COL, "size")
        )
        marked: dict[str, int] = {}
        for reason in tubes["reason"]:
            for r in reason.split(","):
                if r != NOT_MARKED:
                    marked[r] = marked.get(r, 0) + 1
        out[str(tomo)] = {
            "filaments": len(tubes),
            "points": len(grp),
            "candidate_points": int((grp[MARK_COL] == NOT_MARKED).sum()),
            "length_ang": float(tubes["length"].sum()),
            "marked_filaments": marked,
        }
    return out
