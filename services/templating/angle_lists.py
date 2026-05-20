"""Asymmetric-unit angle list generation for PyTOM template-matching.

PyTOM's `pytom_match_template.py` has only one symmetry-named flag —
`--z-axis-rotational-symmetry N` — which restricts the angular search to
1/N of SO(3) for Cn rotational symmetry around the z-axis. For *any other*
point group (D, T, O, I), the only way to exploit symmetry during the
search is to pass a custom angle list via `--angular-search <file>`. From
the pytom-match-pick help text verbatim:

  "a .txt file can be provided with three Euler angles (in radians) per
   line that define the angular search. Angle format is ZXZ anti-clockwise
   (see https://www.ccpem.ac.uk/user_help/rotation_conventions.php)."

That file IS the symmetry: instead of evaluating cross-correlation at every
orientation in SO(3) and discovering that |G| of them give the same score
(because the template is invariant under the point group G), we pre-pick one
representative per equivalence class. Result: |G|× faster search with
identical scoring on a symmetric template.

The convention question (which way scipy orients the icosahedral axes vs.
which way RELION does) is moot for *correctness*: any orientation of the
group in 3D yields a valid asymmetric unit of SO(3). The 60 cosets tile
SO(3); we pick one representative per coset; PyTOM's correlation score at
that representative equals the score at any of the other 59 equivalents
(because the template is G-invariant). Convention only matters if you want
the rep angles to "look natural" relative to the molecule, which doesn't
affect the picker.

The user IS responsible for the template being symmetric under the point
group they declare. A RELION reconstruction with `--sym I1` is already I1-
symmetric; nothing further required. For a non-symmetrized volume, the
search would be wrong — but that's the user's mistake, not ours.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# Order of each point group — used to estimate output count and as the
# expected reduction factor relative to full SO(3) sampling.
POINT_GROUP_ORDER = {
    "C1": 1, "C2": 2, "C3": 3, "C4": 4, "C5": 5, "C6": 6,
    "D2": 4, "D3": 6, "D4": 8, "D5": 10, "D6": 12,
    "T": 12, "O": 24, "I1": 60, "I2": 60,
}

# Subset where PyTOM has no built-in flag → we MUST supply an angle list.
# Cn (n>=2) goes through --z-axis-rotational-symmetry instead.
POINT_GROUPS_NEEDING_ANGLE_LIST = {"D2", "D3", "D4", "D5", "D6", "T", "O", "I1", "I2"}


def needs_angle_list(point_group: str) -> bool:
    """True iff PyTOM needs a custom --angular-search file for this group.
    Cn goes through the dedicated flag; C1 doesn't need symmetry handling."""
    return point_group in POINT_GROUPS_NEEDING_ANGLE_LIST


def _scipy_group_name(point_group: str) -> str:
    """scipy.spatial.transform.Rotation.create_group treats both icosahedral
    conventions as the same group "I" (the 60 rotations are the same set in
    any embedding). The two enum values I1/I2 exist on our side because
    RELION uses them to distinguish axis conventions for the *template*,
    not for the *group*."""
    if point_group in ("I1", "I2"):
        return "I"
    return point_group  # "D2".."D6", "T", "O" map 1:1


def _uniform_so3_grid(angle_increment_deg: float) -> np.ndarray:
    """Approximately-uniform SO(3) sampling as ZXZ Euler triples (radians).

    cos(beta) is sampled at midpoints to absorb the sin(beta) spherical
    Jacobian — without that, the resulting grid over-samples the poles and
    the post-reduction angle count drifts well below |full|/|G|.

    Output shape (n_alpha * n_beta * n_gamma, 3). For a 12° increment that's
    30 × 15 × 30 = 13500 angles full-SO(3); ~225 after I1 reduction.
    """
    inc_rad = np.deg2rad(angle_increment_deg)
    n_alpha = max(1, int(round(2 * np.pi / inc_rad)))
    n_beta = max(1, int(round(np.pi / inc_rad)))
    n_gamma = max(1, int(round(2 * np.pi / inc_rad)))

    alphas = np.linspace(0.0, 2 * np.pi, n_alpha, endpoint=False)
    step = 2.0 / n_beta
    cos_betas = -1.0 + step * (np.arange(n_beta) + 0.5)
    betas = np.arccos(np.clip(cos_betas, -1.0, 1.0))
    gammas = np.linspace(0.0, 2 * np.pi, n_gamma, endpoint=False)

    A, B, G = np.meshgrid(alphas, betas, gammas, indexing="ij")
    return np.stack([A.ravel(), B.ravel(), G.ravel()], axis=1)


def _reduce_to_asymmetric_unit(angles_rad: np.ndarray, point_group: str) -> np.ndarray:
    """For each input orientation R, compute all |G| equivalents (S·R for S in
    group), canonicalize to the lex-smallest quaternion (with w≥0 sign-flip
    so q and -q collapse), and keep one input per canonical key. Output
    contains the original ZXZ angles of the surviving reps."""
    from scipy.spatial.transform import Rotation as R

    sym_group = R.create_group(_scipy_group_name(point_group))
    rots = R.from_euler("ZXZ", angles_rad)

    canonical_keys: set[tuple] = set()
    keep_indices: list[int] = []
    for i in range(len(rots)):
        rot = rots[i]
        equivs = sym_group * rot  # shape (|G|,)
        quats = equivs.as_quat()  # (|G|, 4), scipy order [x, y, z, w]
        # Enforce w >= 0 so the two equivalent quaternions (q, -q) for one
        # rotation collapse to a single canonical form. Without this we'd
        # double-count via sign.
        signs = np.where(quats[:, 3:4] < 0, -1.0, 1.0)
        quats = quats * signs
        # Pick lex-smallest as the canonical fingerprint — stable across runs.
        rep_key = min(tuple(np.round(q, 6)) for q in quats)
        if rep_key not in canonical_keys:
            canonical_keys.add(rep_key)
            keep_indices.append(i)

    if not keep_indices:
        return np.zeros((0, 3))
    return angles_rad[keep_indices]


def generate_asymmetric_unit_angles(point_group: str, angle_increment_deg: float = 12.0) -> np.ndarray:
    """ZXZ Euler triples (radians), one representative per SO(3)/G coset.

    For C1 returns full SO(3) sampling — caller would normally use the float
    --angular-search increment instead of writing a file, but this is the
    sensible fallback if a downstream caller wants a file unconditionally.
    """
    if point_group not in POINT_GROUP_ORDER:
        raise ValueError(f"Unknown point group: {point_group}")
    grid = _uniform_so3_grid(angle_increment_deg)
    if point_group == "C1":
        return grid
    return _reduce_to_asymmetric_unit(grid, point_group)


def write_angle_list_file(angles_rad: np.ndarray, out_path: Path) -> Path:
    """Write ZXZ Euler triples (radians) as space-separated triples, one per
    line — the format pytom_tm.angles.load_angle_list() expects from
    --angular-search <file>."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for angle in angles_rad:
            f.write(f"{float(angle[0]):.8f} {float(angle[1]):.8f} {float(angle[2]):.8f}\n")
    return out_path


def expected_angle_count(point_group: str, angle_increment_deg: float) -> int:
    """Rough estimate of the output count, before actually running the
    reduction. Used for sanity-check logging at supervisor time."""
    inc_rad = np.deg2rad(angle_increment_deg)
    n_alpha = max(1, int(round(2 * np.pi / inc_rad)))
    n_beta = max(1, int(round(np.pi / inc_rad)))
    n_gamma = max(1, int(round(2 * np.pi / inc_rad)))
    full_n = n_alpha * n_beta * n_gamma
    return max(1, full_n // POINT_GROUP_ORDER[point_group])
