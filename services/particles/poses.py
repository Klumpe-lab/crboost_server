"""A particle's pose as RELION writes it, and how a pose inside a particle becomes one in the tomogram.

RELION's Euler matrix A of a row's rlnAngleRot/Tilt/Psi takes the reference's coordinates to the tomogram's: a
point u of the reference sits at centre + A·u, where centre is the row's centred coordinate minus its origin.
Both halves of that were read from RELION itself (roadmap 71 §10): `relion_star_handler --center` on a point v
writes origin − A·v with this A, and the third column of A is the direction a filament's axis (the reference's
Z) has in the tomogram.

A unit that sits at a fixed transform (Q, v) in the reference, rotated by Q with its centre at v, therefore sits
in the tomogram at A·Q, centre + A·v (`compose`). Rows that carry rlnTomoSubtomogramRot/Tilt/Psi (a RELION
extraction's pre-rotation) are not handled here: their origins live in the rotated frame, and the callers refuse
them.
"""

from __future__ import annotations

import numpy as np


def euler_matrices(rot: np.ndarray, tilt: np.ndarray, psi: np.ndarray) -> np.ndarray:
    """RELION's Euler_angles2matrix for each row (degrees): N × 3 × 3. Its third column is the reference's Z axis
    in the tomogram (`tangents_from_angles`)."""
    a, b, g = (np.radians(np.atleast_1d(np.asarray(x, dtype=float))) for x in (rot, tilt, psi))
    ca, sa, cb, sb, cg, sg = np.cos(a), np.sin(a), np.cos(b), np.sin(b), np.cos(g), np.sin(g)
    cc, cs, sc, ss = cb * ca, cb * sa, sb * ca, sb * sa
    m = np.empty((len(a), 3, 3))
    m[:, 0, 0], m[:, 0, 1], m[:, 0, 2] = cg * cc - sg * sa, cg * cs + sg * ca, -cg * sb
    m[:, 1, 0], m[:, 1, 1], m[:, 1, 2] = -sg * cc - cg * sa, -sg * cs + cg * ca, sg * sb
    m[:, 2, 0], m[:, 2, 1], m[:, 2, 2] = sc, ss, cb
    return m


def euler_angles(m: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """RELION's Euler_matrix2angles for each N × 3 × 3 matrix: rot, tilt, psi in degrees."""
    eps = np.finfo(np.float32).eps
    abs_sb = np.hypot(m[:, 0, 2], m[:, 1, 2])
    general = abs_sb > 16 * eps
    psi = np.where(general, np.arctan2(m[:, 1, 2], -m[:, 0, 2]), 0.0)
    rot = np.where(general, np.arctan2(m[:, 2, 1], m[:, 2, 0]), 0.0)
    sin_g, cos_g = np.sin(psi), np.cos(psi)
    with np.errstate(divide="ignore", invalid="ignore"):
        by_cos = np.sign(-m[:, 0, 2] / cos_g)
    sign_sb = np.where(np.abs(sin_g) < eps, by_cos, np.where(sin_g > 0, np.sign(m[:, 1, 2]), -np.sign(m[:, 1, 2])))
    tilt = np.where(general, np.arctan2(sign_sb * abs_sb, m[:, 2, 2]), 0.0)
    # Along Z the matrix fixes only rot + psi: RELION puts it all in psi.
    up = m[:, 2, 2] > 0
    psi = np.where(general, psi, np.where(up, np.arctan2(-m[:, 1, 0], m[:, 0, 0]), np.arctan2(m[:, 1, 0], -m[:, 0, 0])))
    tilt = np.where(general, tilt, np.where(up, 0.0, np.pi))
    return np.degrees(rot), np.degrees(tilt), np.degrees(psi)


def rot_z(deg: float) -> np.ndarray:
    """A turn about Z by `deg`, counter-clockwise seen from +Z: what moves a point of the reference."""
    a = np.radians(float(deg))
    c, s = np.cos(a), np.sin(a)
    return np.array([[c, -s, 0.0], [s, c, 0.0], [0.0, 0.0, 1.0]])


def frame_on(direction) -> np.ndarray:
    """A rotation whose Z axis is `direction`: the frame of a unit that sits on that line with its own axis along
    it. The turn about the line is fixed by the rule (about Z first, then down from Z), nothing more."""
    d = np.asarray(direction, dtype=float)
    n = float(np.linalg.norm(d))
    if not n > 0:
        raise ValueError("a direction of length zero points nowhere")
    d = d / n
    theta = np.arccos(np.clip(d[2], -1.0, 1.0))
    phi = np.arctan2(d[1], d[0]) if np.hypot(d[0], d[1]) > 1e-12 else 0.0
    ct, st = np.cos(theta), np.sin(theta)
    about_y = np.array([[ct, 0.0, st], [0.0, 1.0, 0.0], [-st, 0.0, ct]])
    return rot_z(np.degrees(phi)) @ about_y


def compose(a: np.ndarray, centre: np.ndarray, q: np.ndarray, v: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Every transform (q[k], v[k]) in the reference of every parent (a[n], centre[n]): the poses A·Q as
    (N·K) × 3 × 3 and the centres centre + A·v as (N·K) × 3, a parent's K copies together and in order."""
    a, centre = np.asarray(a, dtype=float), np.asarray(centre, dtype=float)
    q, v = np.asarray(q, dtype=float), np.asarray(v, dtype=float)
    poses = np.einsum("nij,kjl->nkil", a, q).reshape(-1, 3, 3)
    centres = (centre[:, None, :] + np.einsum("nij,kj->nki", a, v)).reshape(-1, 3)
    return poses, centres


def recentred_origin(origin: np.ndarray, a: np.ndarray, v) -> np.ndarray:
    """The origins `relion_star_handler --center` writes for the point `v` of the reference (Å): origin − A·v."""
    return np.asarray(origin, dtype=float) - np.einsum("nij,j->ni", np.asarray(a, dtype=float), np.asarray(v, float))


__all__ = ["compose", "euler_angles", "euler_matrices", "frame_on", "recentred_origin", "rot_z"]
