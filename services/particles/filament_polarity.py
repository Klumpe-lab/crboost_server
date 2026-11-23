"""Which way each filament points, by the vote of its segments after a helical Refine 3D.

A trace has no direction: Pick candidates lays a filament's segments along it one way or the other, and the export
lets RELION try both (`rlnAnglePsiFlipRatio` 0.5). RELION decides each segment on its own, so one filament can
come back pointing both ways. The vote takes each filament's majority, turns the minority round to it, and leaves
out filaments without a clear majority.

What a segment is compared with is the direction it went in with: the prior columns of the set the refinement
read (RELION's own copy of it, ``run_it000_data.star``). The refined star's prior columns are not that any more:
without ``--helical_sigma_distance`` RELION writes each iteration's psi into them. A reversal is of the whole axis
(tilt to 180° − tilt and psi + 180°), so axes are compared, not psi.

Whether the vote means anything is the map's question: a map without polarity fits both directions equally, and
the segments split at random. `pieces_agreement` measures it: two pieces of one filament more than a box apart
share no density, so they choose the same direction only if the map can tell the ends apart.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from services.particles.filament_picks import tangents_from_angles
from services.particles.poses import euler_angles, euler_matrices

TOMO_COL = "rlnTomoName"
TUBE_COL = "rlnHelicalTubeID"
NAME_COL = "rlnTomoParticleName"
TRACK_COL = "rlnHelicalTrackLengthAngst"
FLIP_RATIO_COL = "rlnAnglePsiFlipRatio"
ANGLE_COLS = ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")
PRIOR_COLS = ("rlnAngleTiltPrior", "rlnAnglePsiPrior")

# A filament is decided when at least this share of its segments point one way.
MIN_MAJORITY = 2.0 / 3.0
# A piece's majority counts in `pieces_agreement` only when it is this far from an even split.
_PIECE_MARGIN = 0.1
_PIECE_MIN_SEGMENTS = 5

KEPT, REVERSED, UNDECIDED = "kept", "reversed", "undecided"


def reverse_poses(rot: np.ndarray, tilt: np.ndarray, psi: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """The same poses turned end for end: the reference rotated 180° about its own X axis first, so its Z axis
    points the other way along the filament and the lattice stays where it was."""
    m = euler_matrices(rot, tilt, psi) @ np.diag([1.0, -1.0, -1.0])
    return euler_angles(m)


def reverse_axis(tilt: np.ndarray, psi: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Tilt and psi of the opposite direction along the same line."""
    return 180.0 - np.asarray(tilt, dtype=float), _wrap(np.asarray(psi, dtype=float) + 180.0)


def _wrap(deg: np.ndarray) -> np.ndarray:
    return (deg + 180.0) % 360.0 - 180.0


# ── The vote ─────────────────────────────────────────────────────────────────────────────────────


@dataclass
class Vote:
    """One vote over a refinement's segments. `segments` is the refined table in its own order with three
    columns added: ``started_tilt``/``started_psi`` (the direction it went in with) and ``reversed`` (whether
    the refinement turned it round). `filaments` has one row per filament."""

    segments: pd.DataFrame
    filaments: pd.DataFrame
    min_majority: float
    pieces: dict = field(default_factory=dict)

    @property
    def counts(self) -> dict[str, int]:
        v = self.filaments["verdict"]
        seg = self.filaments.groupby("verdict")["segments"].sum()
        return {
            "filaments": len(v),
            "segments": int(self.filaments["segments"].sum()),
            KEPT: int((v == KEPT).sum()),
            REVERSED: int((v == REVERSED).sum()),
            UNDECIDED: int((v == UNDECIDED).sum()),
            "segments_undecided": int(seg.get(UNDECIDED, 0)),
            "segments_turned": int(self.filaments["minority"].where(v != UNDECIDED, 0).sum()),
        }


def _require(df: pd.DataFrame, cols, what: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{what} has no {', '.join(missing)} column{'s' if len(missing) > 1 else ''}")


def started_directions(refined: pd.DataFrame, started: pd.DataFrame) -> pd.DataFrame:
    """Each refined segment's direction as it went in (tilt, psi), matched by particle name: the started set's
    priors where it has them, else its angles."""
    _require(refined, (NAME_COL,), "The refined star")
    _require(started, (NAME_COL,), "The star the refinement read")
    cols = PRIOR_COLS if all(c in started.columns for c in PRIOR_COLS) else ANGLE_COLS[1:]
    s = started.drop_duplicates(NAME_COL).set_index(NAME_COL)[list(cols)]
    s.columns = ["started_tilt", "started_psi"]
    joined = s.reindex(refined[NAME_COL].to_numpy())
    lost = int(joined["started_tilt"].isna().sum())
    if lost:
        raise ValueError(f"{lost} refined segment(s) are not in the star the refinement read")
    return joined.reset_index(drop=True)


def vote(refined: pd.DataFrame, started: pd.DataFrame, *, min_majority: float = MIN_MAJORITY, box_ang=None) -> Vote:
    """The vote of a refinement's segments (`refined`, its data star's rows) per filament, against the
    directions they went in with (`started`, the star it read). `box_ang` (the particle box in Å) turns on
    `pieces_agreement`."""
    if not 0.5 < min_majority <= 1.0:
        raise ValueError(f"a majority is more than half and at most all; {min_majority:g} is not")
    _require(refined, (TOMO_COL, TUBE_COL, *ANGLE_COLS[1:]), "The refined star")
    seg = refined.reset_index(drop=True).copy()
    seg[["started_tilt", "started_psi"]] = started_directions(seg, started).to_numpy()
    now = tangents_from_angles(seg["rlnAngleTilt"].to_numpy(float), seg["rlnAnglePsi"].to_numpy(float))
    then = tangents_from_angles(seg["started_tilt"].to_numpy(float), seg["started_psi"].to_numpy(float))
    seg["reversed"] = (now * then).sum(axis=1) < 0
    g = seg.groupby([TOMO_COL, TUBE_COL], sort=True)["reversed"]
    fil = pd.DataFrame({"segments": g.size(), "n_reversed": g.sum().astype(int)}).reset_index()
    share_rev = fil["n_reversed"] / fil["segments"]
    fil["majority"] = np.maximum(share_rev, 1.0 - share_rev)
    fil["minority"] = np.minimum(fil["n_reversed"], fil["segments"] - fil["n_reversed"]).astype(int)
    decided = fil["majority"] >= min_majority - 1e-9
    fil["verdict"] = np.where(~decided, UNDECIDED, np.where(share_rev > 0.5, REVERSED, KEPT))
    out = Vote(segments=seg, filaments=fil, min_majority=float(min_majority))
    if box_ang:
        out.pieces = pieces_agreement(seg, float(box_ang))
    return out


def pieces_agreement(segments: pd.DataFrame, box_ang: float) -> dict:
    """For each filament at least three boxes long, the majorities of its two ends (each piece a half-box short
    of the middle, so they share no density): how often they agree. About half by chance; near all when the map
    tells the ends of a filament apart."""
    if TRACK_COL not in segments.columns:
        return {"tested": 0, "decided": 0, "agree": 0, "note": f"no {TRACK_COL} column"}
    tested = decided = agree = 0
    for _, g in segments.groupby([TOMO_COL, TUBE_COL]):
        track = g[TRACK_COL].to_numpy(float)
        lo, hi = float(np.nanmin(track)), float(np.nanmax(track))
        if hi - lo < 3 * box_ang:
            continue
        mid = (lo + hi) / 2
        a = g["reversed"].to_numpy()[track < mid - box_ang / 2]
        b = g["reversed"].to_numpy()[track > mid + box_ang / 2]
        if len(a) < _PIECE_MIN_SEGMENTS or len(b) < _PIECE_MIN_SEGMENTS:
            continue
        tested += 1
        ma, mb = a.mean(), b.mean()
        if abs(ma - 0.5) < _PIECE_MARGIN or abs(mb - 0.5) < _PIECE_MARGIN:
            continue
        decided += 1
        agree += int((ma > 0.5) == (mb > 0.5))
    return {"tested": tested, "decided": decided, "agree": agree}


def decided_rows(v: Vote, *, keep_undecided: bool = False) -> pd.DataFrame:
    """The refined rows with the vote applied, in the refined star's columns: a decided filament's minority
    turned round to its majority (poses `reverse_poses`), its priors the direction it went in with, turned
    when the filament was, and the flip ratio 0, so the next refinement keeps the direction. An undecided
    filament is left out, or with `keep_undecided` kept as it went in: its priors as started, flip ratio 0.5."""
    seg = v.segments
    verdict = seg.set_index([TOMO_COL, TUBE_COL]).index.map(
        v.filaments.set_index([TOMO_COL, TUBE_COL])["verdict"].to_dict()
    )
    verdict = np.asarray(verdict, dtype=object)
    undecided = verdict == UNDECIDED
    fil_reversed = verdict == REVERSED
    rows = seg.drop(columns=["started_tilt", "started_psi", "reversed"]).copy()
    turn = ~undecided & (seg["reversed"].to_numpy() != fil_reversed)
    if turn.any():
        r, t, p = reverse_poses(*(rows.loc[turn, c].to_numpy(float) for c in ANGLE_COLS))
        rows.loc[turn, "rlnAngleRot"], rows.loc[turn, "rlnAngleTilt"], rows.loc[turn, "rlnAnglePsi"] = r, t, p
    tilt0, psi0 = seg["started_tilt"].to_numpy(float), seg["started_psi"].to_numpy(float)
    rt, rp = reverse_axis(tilt0, psi0)
    rows["rlnAngleTiltPrior"] = np.where(fil_reversed, rt, tilt0)
    rows["rlnAnglePsiPrior"] = np.where(fil_reversed, rp, psi0)
    rows[FLIP_RATIO_COL] = np.where(undecided, 0.5, 0.0)
    return rows[~undecided] if not keep_undecided else rows


# ── A refinement's files ─────────────────────────────────────────────────────────────────────────

_IT_RE = re.compile(r"run_it(\d{3})_data\.star$")


def refine3d_files(job_dir: Path) -> dict:
    """The stars of a Refine 3D the vote reads: the data and optimisation set it ended with (``run_data.star``
    once converged, else its last iteration), the star it started from (``run_it000_data.star``) and its
    optimiser star. Raises FileNotFoundError naming what is missing."""
    job_dir = Path(job_dir)
    final = job_dir / "run_data.star"
    if final.exists():
        it = None
        data, optset, optimiser = final, job_dir / "run_optimisation_set.star", job_dir / "run_optimiser.star"
    else:
        its = sorted(int(m.group(1)) for p in job_dir.glob("run_it*_data.star") if (m := _IT_RE.search(p.name)))
        its = [i for i in its if i > 0]
        if not its:
            raise FileNotFoundError("The refinement has written no iteration yet")
        it = its[-1]
        data = job_dir / f"run_it{it:03d}_data.star"
        optset = job_dir / f"run_it{it:03d}_optimisation_set.star"
        optimiser = job_dir / f"run_it{it:03d}_optimiser.star"
    started = job_dir / "run_it000_data.star"
    for p in (data, started):
        if not p.exists():
            raise FileNotFoundError(f"{p.name} is missing from {job_dir.name}")
    return {"data": data, "started": started, "optset": optset, "optimiser": optimiser, "iteration": it}


def particle_box_ang(blocks: dict) -> float | None:
    """The particle box in Å from a data star's optics block."""
    optics = blocks.get("optics")
    if optics is None or not len(optics):
        return None
    for size, apix in (("rlnImageSize", "rlnImagePixelSize"), ("rlnImageSize", "rlnTomoTiltSeriesPixelSize")):
        if size in optics.columns and apix in optics.columns:
            return float(optics[size].iloc[0]) * float(optics[apix].iloc[0])
    return None


__all__ = [
    "KEPT",
    "MIN_MAJORITY",
    "REVERSED",
    "UNDECIDED",
    "Vote",
    "decided_rows",
    "euler_angles",
    "euler_matrices",
    "particle_box_ang",
    "pieces_agreement",
    "refine3d_files",
    "reverse_axis",
    "reverse_poses",
    "started_directions",
    "vote",
]
