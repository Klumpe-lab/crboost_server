"""Correct symmetry-reduced angle lists for pytom, checked against the template with pytom's own rotation.

    python3 -B tm_angle_lists.py <template.mrc> <mask.mrc> <old list> <out dir> <increment deg> [...]

Runs inside the pytom container on a GPU (probe_tm_angles.sh calls it). An icosahedral list is only right if it
uses the template's own symmetry axes, composed on the side pytom applies a listed rotation, under the Euler
convention pytom reads. So, before writing a list, it rotates the template the way pytom's search does (a voltools
texture, filtered B-spline, `rzxz`, radians) and measures:

1. Which axes: for scipy's icosahedral group as it is and turned 90° about z (the two 2-2-2 settings, RELION's I1
   and I2), and for Euler angles read as scipy's intrinsic `ZXZ` or extrinsic `zxz`, the masked correlation between
   the template and the template rotated by each of the 59 non-identity elements. The right pair gives about 1 for
   all of them; 59 random rotations give the baseline.
2. Which side: for random R and a 5-fold S of that group, whether rot(R·S) or rot(S·R) equals rot(R).

Then for each increment it takes pytom's own sampling (`pytom_tm.angles.angle_to_angle_list`: HEALPix view
directions times in-plane steps, what `--angular-search <degrees>` searches without symmetry) and keeps each rotation
that is the nearest to the identity among its symmetry images: one per orbit, about a 60th of the list, with the
unreduced list's coverage. Every list, the old one included, gets its coverage: for 20,000 random orientations, the
angle to the nearest symmetry image of a listed one. Writes `angles_old.txt`, `angles_I_<inc>.txt`, `check.txt` and
`coverage.tsv` into the out dir; exits 3 when the template does not say clearly which axes it has.
"""

import shutil
import sys
from pathlib import Path

import cupy as cp
import numpy as np
import voltools as vt
from pytom_tm.angles import angle_to_angle_list
from pytom_tm.io import read_mrc
from scipy.spatial import cKDTree
from scipy.spatial.transform import Rotation as R


def keys(rots: R) -> np.ndarray:
    """Integer quaternion keys, one per rotation, with q and -q made the same: w > 0, or the first non-zero of x, y, z
    positive when w is 0."""
    q = rots.as_quat()
    lead = np.where(
        np.abs(q[:, 3]) > 1e-9,
        q[:, 3],
        np.where(np.abs(q[:, 0]) > 1e-9, q[:, 0], np.where(np.abs(q[:, 1]) > 1e-9, q[:, 1], q[:, 2])),
    )
    return np.round(q * np.where(lead < 0, -1.0, 1.0)[:, None] * 1e6).astype(np.int64)


def nearer(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Where key a is nearer the identity than key b (larger w, a smaller rotation); ties go to the smaller x, y, z."""
    won, equal = a[:, 3] > b[:, 3], a[:, 3] == b[:, 3]
    for c in (0, 1, 2):
        won |= equal & (a[:, c] < b[:, c])
        equal &= a[:, c] == b[:, c]
    return won


def images(rots: R, group: R, side: str, k: int) -> R:
    return rots * group[k] if side == "right" else group[k] * rots


def fundamental_region(euler: np.ndarray, convention: str, group: R, side: str) -> np.ndarray:
    rots = R.from_euler(convention, euler)
    own = keys(rots)
    best = own.copy()
    for k in range(len(group)):
        cand = keys(images(rots, group, side, k))
        swap = nearer(cand, best)
        best[swap] = cand[swap]
    return euler[np.all(best == own, axis=1)]


def coverage(euler: np.ndarray, convention: str, group: R, side: str) -> tuple[float, float, float]:
    rots = R.from_euler(convention, euler)
    pts = np.vstack([images(rots, group, side, k).as_quat() for k in range(len(group))])
    tree = cKDTree(np.vstack([pts, -pts]))
    d, _ = tree.query(R.random(20000, random_state=1).as_quat())
    ang = np.degrees(4 * np.arcsin(np.clip(d / 2, 0, 1)))
    return float(np.median(ang)), float(np.percentile(ang, 95)), float(ang.max())


def main() -> None:
    template, mask, old, out = (Path(a) for a in sys.argv[1:5])
    increments = [float(a) for a in sys.argv[5:]]
    out.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(old, out / "angles_old.txt")
    lines = []

    volume = cp.asarray(read_mrc(template), dtype=cp.float32, order="C")
    tex = vt.StaticVolume(volume, interpolation="filt_bspline", device="gpu:0")
    weight = cp.asarray(read_mrc(mask), dtype=cp.float32)
    weight /= weight.sum()
    buf = cp.zeros_like(volume)

    def rotated(euler) -> cp.ndarray:
        tex.transform(rotation=tuple(float(x) for x in euler), rotation_order="rzxz", rotation_units="rad", output=buf)
        return buf.copy()

    def corr(a: cp.ndarray, b: cp.ndarray) -> float:
        da, db = a - (a * weight).sum(), b - (b * weight).sum()
        return float((da * db * weight).sum() / cp.sqrt((da * da * weight).sum() * (db * db * weight).sum()))

    base = rotated((0.0, 0.0, 0.0))
    g0 = R.create_group("I")
    turn = R.from_euler("z", 90, degrees=True)
    groups = {"I as scipy has it": g0, "I turned 90 deg about z": turn * g0 * turn.inv()}
    rand = sorted(corr(base, rotated(r)) for r in R.random(59, random_state=2).as_euler("ZXZ"))
    lines.append(f"Template against itself under 59 random rotations: median {np.median(rand):.3f}, max {rand[-1]:.3f}")
    results = []
    for gname, group in groups.items():
        nontrivial = [k for k in range(len(group)) if group[k].magnitude() > 1e-6]
        for convention in ("ZXZ", "zxz"):
            c = [corr(base, rotated(group[k].as_euler(convention))) for k in nontrivial]
            results.append((min(c), float(np.median(c)), gname, convention))
            lines.append(
                f"{gname}, Euler read as {convention}: the 59 symmetry rotations give median {np.median(c):.3f}, "
                f"min {min(c):.3f}"
            )
    results.sort(reverse=True)
    best_min, _, gname, convention = results[0]
    other_median = max((r[1] for r in results if r[2] != gname), default=-1.0)
    clear = best_min > rand[-1] and best_min > other_median
    group = groups[gname]
    lines.append(
        f"Chosen: {gname}, Euler read as {convention} "
        f"({'clear' if clear else 'NOT CLEAR: the lists below may be wrong'})"
    )
    if len({(round(r[0], 6), round(r[1], 6)) for r in results if r[2] == gname}) == 1:
        lines.append(
            "The two Euler readings agree for this group: they differ by an inversion and a 2-fold about y, both in "
            "the group, so either gives the same list."
        )

    five = [k for k in range(len(group)) if abs(np.degrees(group[k].magnitude()) - 72) < 1]
    right, left = [], []
    for r in R.random(4, random_state=3):
        ref = rotated(r.as_euler(convention))
        for k in five[:3]:
            right.append(corr(ref, rotated((r * group[k]).as_euler(convention))))
            left.append(corr(ref, rotated((group[k] * r).as_euler(convention))))
    side = "right" if np.median(right) > np.median(left) else "left"
    lines.append(
        f"Side: rot(R*S) vs rot(R) median {np.median(right):.3f}, rot(S*R) vs rot(R) median "
        f"{np.median(left):.3f}: the symmetry composes on the {side}"
    )

    rows = []
    old_euler = np.loadtxt(out / "angles_old.txt", ndmin=2)
    rows.append(("old", len(old_euler), *coverage(old_euler, convention, group, side)))
    for inc in increments:
        full = np.array(angle_to_angle_list(inc))
        kept = fundamental_region(full, convention, group, side)
        np.savetxt(out / f"angles_I_{inc:g}.txt", kept, fmt="%.8f")
        rows.append((f"{inc:g}", len(kept), *coverage(kept, convention, group, side)))
        c1 = coverage(full, convention, R.identity(1), side)
        lines.append(
            f"{inc:g} deg: pytom's list {len(full)} rotations, {len(kept)} kept (/ 60 = {len(full) / 60:.0f}); "
            f"unreduced it covers median {c1[0]:.1f}, 95th pct {c1[1]:.1f}, max {c1[2]:.1f} deg"
        )
    with open(out / "coverage.tsv", "w") as f:
        f.write("list\trotations\tmedian_deg\tp95_deg\tmax_deg\n")
        for row in rows:
            f.write("\t".join([row[0], str(row[1])] + [f"{v:.2f}" for v in row[2:]]) + "\n")
    lines += [
        f"{row[0]}: {row[1]} rotations, nearest symmetry image median {row[2]:.1f}, 95th pct {row[3]:.1f}, "
        f"max {row[4]:.1f} deg"
        for row in rows
    ]
    (out / "check.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    sys.exit(0 if clear else 3)


if __name__ == "__main__":
    main()
