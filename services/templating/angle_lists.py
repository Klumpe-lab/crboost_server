"""Symmetry-reduced angle lists for PyTOM template matching (HOTFIXES H7, roadmap 45 D9).

Where the lists come from. A search tries every rotation in its list against the whole tomogram, so its time is
the list's length. For a bare increment pytom builds the list itself (`pytom_tm.angles.angle_to_angle_list`:
HEALPix view directions times in-plane steps, 9,000 rotations at 12°, 124,416 at 5°). For Cn crboost passes
`--z-axis-rotational-symmetry n` and pytom cuts the in-plane steps to 360/n. For D, T, O and I pytom has no option,
so this module writes the list: pytom's own list for the increment, cut to one rotation per symmetry orbit.

Why that is exact. The template is invariant under its group G, so rotating it by R·S (S in G, applied first)
gives the same picture as R: the |G| rotations R·G score alike, and one of them is enough. Keeping, per orbit, the
rotation nearest the identity leaves about 1/|G| of pytom's list (148 of 9,000 for I at 12°) with the unreduced
list's coverage, so the increment means the same with symmetry as without. The increment and the group are
independent: the increment sets how finely orientations are sampled, the group how much of rotation space is
needed.

What must match the template. The group's axes: I1 is scipy's icosahedral setting, I2 the same turned 90° about z
(RELION's two settings); D, T and O are scipy's (n-fold on z). The side: symmetry composes on the right, R·S, which
P9 measured with pytom's own rotation on the 412 template. Euler angles are pytom's anti-clockwise ZXZ in radians,
scipy's intrinsic "ZXZ".

The list depends only on (group, increment); each job writes its own copy, which records what it searched.
P9 (roadmap 45 §11) checked these lists against pytom's on the 412 data: 148, 578 and 2,124 rotations at 12°, 8°
and 5° for I.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np

# Order of each point group: the reduction factor against pytom's list for the increment.
POINT_GROUP_ORDER = {
    "C1": 1, "C2": 2, "C3": 3, "C4": 4, "C5": 5, "C6": 6,
    "D2": 4, "D3": 6, "D4": 8, "D5": 10, "D6": 12,
    "T": 12, "O": 24, "I1": 60, "I2": 60,
}  # fmt: skip

# Cn goes through pytom's --z-axis-rotational-symmetry instead.
POINT_GROUPS_NEEDING_ANGLE_LIST = {"D2", "D3", "D4", "D5", "D6", "T", "O", "I1", "I2"}


def needs_angle_list(point_group: str) -> bool:
    """True iff PyTOM needs a custom --angular-search file for this group."""
    return point_group in POINT_GROUPS_NEEDING_ANGLE_LIST


def parse_increment(angular_search: str | float) -> float:
    """The job's angular_search as degrees; a value that is not a positive number is refused, never replaced."""
    try:
        inc = float(angular_search)
    except (TypeError, ValueError):
        raise ValueError(f"angular_search {angular_search!r} is not an increment in degrees") from None
    if not inc > 0:
        raise ValueError(f"angular_search {angular_search!r} must be a positive increment in degrees")
    return inc


def _healpix_ring_pix2ang(nside: int) -> tuple[np.ndarray, np.ndarray]:
    """(theta, phi) of every HEALPix pixel centre in the RING scheme, as healpix's pix2ang gives them."""
    npix = 12 * nside * nside
    ncap = 2 * nside * (nside - 1)
    p = np.arange(npix, dtype=np.int64)
    z = np.empty(npix)
    phi = np.empty(npix)

    north = p < ncap
    iring = (1 + np.sqrt(1 + 2 * p[north]).astype(np.int64)) >> 1
    iphi = p[north] + 1 - 2 * iring * (iring - 1)
    z[north] = 1 - iring * iring * 4.0 / npix
    phi[north] = (iphi - 0.5) * (math.pi / 2) / iring

    belt = (p >= ncap) & (p < npix - ncap)
    ip = p[belt] - ncap
    iring = ip // (4 * nside) + nside
    iphi = ip % (4 * nside) + 1
    fodd = np.where((iring + nside) & 1, 1.0, 0.5)
    z[belt] = (2 * nside - iring) * 2.0 / (3 * nside)
    phi[belt] = (iphi - fodd) * (math.pi / 2) / nside

    south = p >= npix - ncap
    ip = npix - p[south]
    iring = (1 + np.sqrt(2 * ip - 1).astype(np.int64)) >> 1
    iphi = 4 * iring + 1 - (ip - 2 * iring * (iring - 1))
    z[south] = -1 + iring * iring * 4.0 / npix
    phi[south] = (iphi - 0.5) * (math.pi / 2) / iring
    return np.arccos(z), phi


def _healpix_nside(increment_deg: float) -> int:
    """The HEALPix resolution pytom takes for the increment: the coarsest whose pixels are no larger than it."""
    npix = 4 * math.pi / math.radians(increment_deg) ** 2
    nside = 0
    while 12 * nside * nside < npix:
        nside += 1
    return nside


def pytom_angle_count(increment_deg: float) -> int:
    """The length of pytom's list for the increment, without building it: view directions times in-plane steps."""
    return 12 * _healpix_nside(increment_deg) ** 2 * math.ceil(360 / increment_deg)


def pytom_angle_list(increment_deg: float) -> np.ndarray:
    """pytom's `angle_to_angle_list(increment)` (pytom-match-pick 0.14), as ZXZ Euler triples in radians:
    what `--angular-search <degrees>` searches without symmetry."""
    theta, phi = _healpix_ring_pix2ang(_healpix_nside(increment_deg))
    phi %= 2 * math.pi
    theta %= math.pi
    psi = np.linspace(0, 2 * math.pi, math.ceil(360 / increment_deg), endpoint=False)
    out = np.empty((len(phi) * len(psi), 3))
    out[:, 0] = np.repeat(phi, len(psi))
    out[:, 1] = np.repeat(theta, len(psi))
    out[:, 2] = np.tile(psi, len(phi))
    return out[np.lexsort((out[:, 2], out[:, 1], out[:, 0]))]


def symmetry_group(point_group: str):
    """The group's rotations on the template's axes: scipy's setting, I2 turned 90° about z from I1."""
    from scipy.spatial.transform import Rotation as R

    if point_group == "I2":
        turn = R.from_euler("z", 90, degrees=True)
        return turn * R.create_group("I") * turn.inv()
    return R.create_group("I" if point_group == "I1" else point_group)


def _keys(rots) -> np.ndarray:
    """Integer quaternion keys, q and -q made the same (w > 0, or the first non-zero of x, y, z when w is 0)."""
    q = rots.as_quat()
    lead = np.where(
        np.abs(q[:, 3]) > 1e-9,
        q[:, 3],
        np.where(np.abs(q[:, 0]) > 1e-9, q[:, 0], np.where(np.abs(q[:, 1]) > 1e-9, q[:, 1], q[:, 2])),
    )
    return np.round(q * np.where(lead < 0, -1.0, 1.0)[:, None] * 1e6).astype(np.int64)


def _nearer(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Where key a is nearer the identity than key b (larger w); ties go to the smaller x, y, z."""
    won, equal = a[:, 3] > b[:, 3], a[:, 3] == b[:, 3]
    for c in (0, 1, 2):
        won |= equal & (a[:, c] < b[:, c])
        equal &= a[:, c] == b[:, c]
    return won


def generate_asymmetric_unit_angles(point_group: str, angle_increment_deg: float) -> np.ndarray:
    """ZXZ Euler triples (radians): pytom's list for the increment, one rotation per orbit of the group (the one
    nearest the identity). C1 returns pytom's whole list."""
    from scipy.spatial.transform import Rotation as R

    if point_group not in POINT_GROUP_ORDER:
        raise ValueError(f"Unknown point group: {point_group}")
    euler = pytom_angle_list(angle_increment_deg)
    if point_group == "C1":
        return euler
    group = symmetry_group(point_group)
    rots = R.from_euler("ZXZ", euler)
    own = _keys(rots)
    best = own.copy()
    for k in range(len(group)):
        cand = _keys(rots * group[k])
        swap = _nearer(cand, best)
        best[swap] = cand[swap]
    kept = euler[np.all(best == own, axis=1)]
    # An increment coarser than the region nearest the identity (90° in I or O) leaves none of pytom's rotations
    # in it; the identity then stands for the whole region, which lies within the increment of it.
    return kept if len(kept) else np.zeros((1, 3))


def write_angle_list_file(angles_rad: np.ndarray, out_path: Path) -> Path:
    """ZXZ Euler triples (radians), space-separated, one per line: what pytom's load_angle_list reads."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for angle in angles_rad:
            f.write(f"{float(angle[0]):.8f} {float(angle[1]):.8f} {float(angle[2]):.8f}\n")
    return out_path


def expected_angle_count(point_group: str, angle_increment_deg: float) -> int:
    """pytom's list length for the increment over the group's order: what the written list should come close to."""
    return max(1, round(pytom_angle_count(angle_increment_deg) / POINT_GROUP_ORDER[point_group]))
