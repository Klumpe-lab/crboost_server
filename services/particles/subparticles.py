"""Sub-particles: the copies of a unit that each aligned particle is built from (roadmap 71).

`place` applies a site (`services/particles/sites.py`) to a table of refined parents: each copy's pose and centre
are its parent's followed by the site's transform (`poses.compose`), and each row says where it came from. Pure:
tables in, a table out; `set_ops.create_subparticle_set` publishes it as a set.

What holds whatever the species:

- **The assembly** is the physical object the copies belong to: the parent itself for a symmetric particle, the
  filament for a segment. Every copy of one assembly is in one half-set, or the FSC would measure an assembly
  against itself; doubles are looked for inside an assembly only.
- **A copy is a coordinate**: its centre in the centred coordinates with zero origins, its pose in the three
  angles. What named the parent's images (and what its refinement measured of it) is not carried, so the images are
  cut anew in a small box. The parent's score and context columns are.
- **A copy is a valid parent**: it has a centre, a pose, a half, a uid and an assembly, so a set of copies can be
  expanded again.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from services.particles import sites
from services.particles.coords import CENTERED_COLS
from services.particles.pick_merge import _greedy_drop_mask
from services.particles.pick_uids import HALF_COL, UID_COL, parse_cell, uid_cell
from services.particles.poses import compose, euler_angles, euler_matrices

TOMO_COL = "rlnTomoName"
TUBE_COL = "rlnHelicalTubeID"
NAME_COL = "rlnTomoParticleName"
ANGLE_COLS = ["rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi"]
ORIGIN_COLS = ["rlnOriginXAngst", "rlnOriginYAngst", "rlnOriginZAngst"]
ORIGIN_PIXEL_COLS = ["rlnOriginX", "rlnOriginY", "rlnOriginZ"]
SUBTOMO_ANGLE_COLS = ["rlnTomoSubtomogramRot", "rlnTomoSubtomogramTilt", "rlnTomoSubtomogramPsi"]
# The parent's template-matching score: a later rule can cut copies by it.
SCORE_COLS = ["rlnLCCmax", "rlnCutOff", "rlnSearchStd"]

PARENT_COL = "cbParentUid"  # the parent particle's uid
ASSEMBLY_COL = "cbAssembly"  # the capsid, or the filament, the copy belongs to
INDEX_COL = "cbSubIndex"  # which of the site's copies in its parent

# Doubles are copies of one assembly closer than this share of the site's own spacing.
DOUBLES_SHARE = 1.0 / 3.0


@dataclass
class Placement:
    """What `place` made: the rows, and the counts a funnel states."""

    rows: pd.DataFrame
    site: sites.Site
    n_parents: int
    n_placed: int  # parents × the site's copies
    n_outside: int = 0  # dropped: outside the tomogram, or within the margin of a face
    n_doubles: int = 0  # dropped: the same unit from a neighbouring segment
    n_close: int = 0  # kept, although nearer another copy of their filament than half the site's spacing
    margin_ang: float = 0.0
    doubles_radius_ang: float | None = None
    half_window_ang: float | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def per_parent(self) -> int:
        return self.site.n


def _hash12(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def _check_parents(parents: pd.DataFrame, helix: bool) -> None:
    missing = [c for c in [TOMO_COL, *CENTERED_COLS, *ANGLE_COLS] if c not in parents.columns]
    if missing:
        raise ValueError(f"the parents have no {', '.join(missing)}: sub-particles need each parent's centre and pose")
    subtomo = [c for c in SUBTOMO_ANGLE_COLS if c in parents.columns]
    if subtomo:
        raise ValueError(
            f"the parents carry {', '.join(subtomo)} (a RELION extraction's rotation, which their origins and angles "
            "are relative to): sub-particles are made from particles cut by Warp Export or Reconstruct Particle"
        )
    if any(c in parents.columns for c in ORIGIN_PIXEL_COLS):
        raise ValueError("the parents' origins are in pixels; RELION 5 writes them in Å")
    origins = [c for c in ORIGIN_COLS if c in parents.columns]
    if origins and len(origins) != 3:
        raise ValueError(f"the parents have only some of the origin columns ({', '.join(origins)})")
    if helix and TUBE_COL not in parents.columns:
        raise ValueError(f"the parents carry no {TUBE_COL}: they are not segments of filaments")
    numeric = parents[[*CENTERED_COLS, *ANGLE_COLS, *origins]].apply(pd.to_numeric, errors="coerce")
    bad = int(numeric.isna().any(axis=1).sum())
    if bad:
        raise ValueError(f"{bad:,} parent(s) have a centre, origin or angle that is not a number")


def segment_spacing(xyz: np.ndarray, tomo: np.ndarray, tube: np.ndarray) -> float | None:
    """The usual distance between neighbouring segments of one filament (the median over every segment's nearest
    neighbour on its own filament), or None when no filament has two segments."""
    from scipy.spatial import cKDTree

    nearest: list[np.ndarray] = []
    keys = pd.MultiIndex.from_arrays([tomo, tube])
    for _, idx in pd.Series(np.arange(len(xyz))).groupby(keys, sort=False):
        p = xyz[idx.to_numpy()]
        if len(p) >= 2:
            d, _ = cKDTree(p).query(p, k=2)
            nearest.append(d[:, 1])
    if not nearest:
        return None
    return float(np.median(np.concatenate(nearest)))


def _close_copies(xyz: np.ndarray, tomo: np.ndarray, assembly: np.ndarray, radius: float) -> int:
    """How many copies have another copy of their own assembly within `radius`."""
    from scipy.spatial import cKDTree

    close = np.zeros(len(xyz), dtype=bool)
    for t in np.unique(tomo):
        idx = np.flatnonzero(tomo == t)
        if len(idx) < 2:
            continue
        pairs = cKDTree(xyz[idx]).query_pairs(radius, output_type="ndarray")
        if len(pairs):
            same = assembly[idx[pairs[:, 0]]] == assembly[idx[pairs[:, 1]]]
            close[idx[pairs[same].ravel()]] = True
    return int(close.sum())


def _site_for(description: dict, xyz: np.ndarray, tomo: np.ndarray, tube) -> tuple[sites.Site, float | None]:
    if (description or {}).get("kind") != sites.HELIX:
        return sites.site_of(description), None
    spacing = segment_spacing(xyz, tomo, tube)
    if spacing is None:
        raise ValueError("no filament has two segments, so the length of filament one segment stands for is unknown")
    # Half the way to the next segment, and half a unit more so neighbours overlap: a unit near the middle is
    # made by both and one is removed, instead of by neither.
    unit = description.get("axial_repeat_ang") or abs(float(description.get("rise_ang") or 0.0)) * int(
        description.get("every") or 1
    )
    half = spacing / 2.0 + float(unit) / 2.0
    return sites.site_of(description, half_window_ang=half), half


def _halves(parents: pd.DataFrame, assembly: np.ndarray, notes: list[str]) -> np.ndarray:
    """Each parent's half-set, one per assembly: the parents' own where every parent of an assembly agrees, the
    assembly's majority where they do not (said), a hash of the assembly where the parents carry none (said)."""
    if HALF_COL not in parents.columns:
        notes.append("the parents carry no half-set: each assembly's half is drawn from its name")
        return np.array([1 + (hashlib.sha1(f"half|{a}".encode()).digest()[0] & 1) for a in assembly])
    half = pd.to_numeric(parents[HALF_COL], errors="coerce")
    if half.isna().any() or not half.isin([1, 2]).all():
        raise ValueError(f"the parents' {HALF_COL} is not 1 or 2 on every row")
    half = half.astype(int)
    share2 = (half == 2).groupby(assembly).mean()
    mixed = int(((share2 > 0) & (share2 < 1)).sum())
    if mixed:
        notes.append(
            f"{mixed:,} filament(s) had segments in both half-sets: their copies take the filament's majority half"
        )
    by_assembly = (1 + (share2 > 0.5).astype(int)).to_dict()
    return np.array([by_assembly[a] for a in assembly])


def place(
    parents: pd.DataFrame, description: dict, *, extents: dict[str, np.ndarray] | None = None, margin_ang: float = 0.0
) -> Placement:
    """The copies of the site `description` names in every row of `parents` (a refined particle table).

    With `extents` ({tomogram: its size in Å}) a copy is dropped when its centre is outside its tomogram or
    within `margin_ang` of a face. A helix site's copies from neighbouring segments of one filament that land on
    the same unit are doubles: the one nearer its own segment's centre is kept."""
    helix = (description or {}).get("kind") == sites.HELIX
    _check_parents(parents, helix)
    parents = parents.reset_index(drop=True)
    n = len(parents)
    if n == 0:
        raise ValueError("there is no parent to make sub-particles of")
    tomo = parents[TOMO_COL].astype(str).to_numpy()
    xyz = parents[CENTERED_COLS].to_numpy(dtype=float)
    if all(c in parents.columns for c in ORIGIN_COLS):
        xyz = xyz - parents[ORIGIN_COLS].to_numpy(dtype=float)
    tube = parents[TUBE_COL].to_numpy() if helix else None
    site, half_window = _site_for(description, xyz, tomo, tube)
    notes: list[str] = []

    # Who each parent is: its uid when it has one to trust, else its name or place in its tomogram.
    uids = [parse_cell(v) for v in parents[UID_COL]] if UID_COL in parents.columns else [None] * n
    if NAME_COL in parents.columns:
        names = parents[NAME_COL].astype(str).tolist()
    else:
        names = [f"{t}|{x:.2f}|{y:.2f}|{z:.2f}" for t, (x, y, z) in zip(tomo, xyz, strict=True)]
    parent_key = [u if u is not None else _hash12(f"parent|{nm}") for u, nm in zip(uids, names, strict=True)]
    if len(set(parent_key)) != n:
        raise ValueError("two parents are the same particle (one uid or name twice): the set would hold doubles")
    if helix:
        assembly = np.array([f"f{_hash12(f'{t}|{tb}')}" for t, tb in zip(tomo, tube, strict=True)], dtype=object)
    else:
        assembly = np.array([uid_cell(k) for k in parent_key], dtype=object)
    half = _halves(parents, assembly, notes)

    a = euler_matrices(*(parents[c].to_numpy(dtype=float) for c in ANGLE_COLS))
    poses, centres = compose(a, xyz, site.rotations, site.offsets)
    rot, tilt, psi = euler_angles(poses)
    k = site.n
    of_parent = np.repeat(np.arange(n), k)
    index = np.tile(np.arange(k), n)

    carried = [c for c in parents.columns if c in SCORE_COLS or (c.startswith("cb") and c not in (UID_COL, PARENT_COL))]
    carried = [c for c in carried if c not in (ASSEMBLY_COL, INDEX_COL)]
    rows = pd.DataFrame({TOMO_COL: tomo[of_parent]})
    for i, c in enumerate(CENTERED_COLS):
        rows[c] = centres[:, i]
    rows["rlnAngleRot"], rows["rlnAngleTilt"], rows["rlnAnglePsi"] = rot, tilt, psi
    for c in ORIGIN_COLS:
        rows[c] = 0.0
    for c in carried:
        rows[c] = parents[c].to_numpy()[of_parent]
    digest = site.digest
    rows[UID_COL] = [
        uid_cell(_hash12(f"sub|{parent_key[p]}|{digest}|{i}")) for p, i in zip(of_parent, index, strict=True)
    ]
    rows[PARENT_COL] = [uid_cell(parent_key[p]) for p in of_parent]
    rows[ASSEMBLY_COL] = assembly[of_parent]
    rows[INDEX_COL] = index
    rows[HALF_COL] = half[of_parent]

    out = Placement(rows=rows, site=site, n_parents=n, n_placed=len(rows), half_window_ang=half_window, notes=notes)
    keep = np.ones(len(rows), dtype=bool)
    row_tomo = tomo[of_parent]

    if extents is not None:
        out.margin_ang = float(margin_ang)
        unknown = sorted(set(np.unique(row_tomo)) - set(extents))
        for t, ext in extents.items():
            sel = row_tomo == t
            if sel.any():
                room = (np.asarray(ext, dtype=float) / 2.0)[None, :] - np.abs(centres[sel])
                keep[np.flatnonzero(sel)[room.min(axis=1) < margin_ang]] = False
        out.n_outside = int((~keep).sum())
        if unknown:
            notes.append(f"{len(unknown)} tomogram(s) are missing from the tomograms star: their copies were all kept")

    if helix and site.spacing_ang:
        radius = site.spacing_ang * DOUBLES_SHARE
        out.doubles_radius_ang = radius
        along = np.abs(np.tile(site.along_ang, n))
        # Numbers, as the doubles walk groups by them.
        row_assembly = pd.factorize(rows[ASSEMBLY_COL])[0]
        before = int(keep.sum())
        for t in np.unique(row_tomo):
            idx = np.flatnonzero(keep & (row_tomo == t))
            if len(idx) < 2:
                continue
            order = idx[np.argsort(along[idx], kind="stable")]
            dropped = _greedy_drop_mask(centres[order], radius, row_assembly[order])
            keep[order[dropped]] = False
        out.n_doubles = before - int(keep.sum())
        out.n_close = _close_copies(centres[keep], row_tomo[keep], row_assembly[keep], site.spacing_ang / 2.0)
        if out.n_close:
            notes.append(
                f"{out.n_close:,} copies sit closer than half the lattice's spacing ({site.spacing_ang / 2.0:.0f} Å) "
                "to another copy of their filament: neighbouring segments that disagree about where the lattice "
                "is. They are kept"
            )

    out.rows = rows[keep].reset_index(drop=True)
    return out


__all__ = ["ASSEMBLY_COL", "DOUBLES_SHARE", "INDEX_COL", "PARENT_COL", "Placement", "place", "segment_spacing"]
