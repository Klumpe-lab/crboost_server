"""Where a repeating unit sits inside an aligned particle: a short description that expands into transforms.

A site is what a person can read off a map: for a symmetric particle a point group, an axis (or any direction)
and a radius; for a filament a helix. `site_of` turns the description into the list of transforms (Q, v), one per
copy of the unit in one parent, each in the parent's reference frame: the unit's own frame rotated by Q, its
centre at v. Placing them on real particles is `services/particles/subparticles.py`; nothing here reads a star.

Point groups are RELION's own operators (`data/relion_symmetry.json`, read from RELION by
`scripts/relion_symmetry_operators.py`), so an axis is where RELION put it in the map, whatever the setting
(I1 … I4). A site gives the distinct positions only: the 60 operators of I1 put a point on a 5-fold in 12 places,
five operators each, and one operator per place is kept (the first in RELION's order). The operators that leave
the place where it is are the unit's own symmetry there (C5).

A helix (twist, rise) is the one RELION imposes: a point of the lattice has its mates a turn of k·twist about Z
(counter-clockwise from +Z) and k·rise along it. A lattice with several starts has a second step, straight along
the axis (`axial_repeat_ang`: 13 steps of a 13-protofilament microtubule's helix come back three monomers up, so
its helix alone visits every third monomer of a protofilament, and the monomer repeat fills in the rest).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np

from services.particles.poses import euler_matrices, frame_on, rot_z

DATA = Path(__file__).parent / "data" / "relion_symmetry.json"
READ_SCRIPT = "scripts/relion_symmetry_operators.py"

POINT_GROUP = "point_group"
HELIX = "helix"
DIRECTION = "direction"  # an axis choice: any direction, typed

# Two positions closer than this are one: no unit sits 3 Å from the next.
_SAME_POINT_ANG = 3.0
_TOL = 1e-6


# ── RELION's point groups ────────────────────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _operator_angles() -> dict[str, list[list[float]]]:
    return json.loads(DATA.read_text())["groups"]


def group_name(symmetry: str) -> str:
    return str(symmetry or "").strip().upper()


def known_group(symmetry: str) -> bool:
    return group_name(symmetry) in _operator_angles()


@lru_cache(maxsize=32)
def operators(symmetry: str) -> np.ndarray:
    """The rotations of a point group, M × 3 × 3, in RELION's order: each takes the reference onto itself."""
    name = group_name(symmetry)
    angles = _operator_angles().get(name)
    if angles is None:
        raise ValueError(
            f"the operators of {name or 'this symmetry'} have not been read from RELION (they are read up to C40 "
            f"and D40, with T, O and I1 to I4: {READ_SCRIPT})"
        )
    a = np.asarray(angles, dtype=float)
    ops = euler_matrices(a[:, 0], a[:, 1], a[:, 2])
    ops.setflags(write=False)
    return ops


def _axis_of(op: np.ndarray) -> tuple[np.ndarray, float] | None:
    """The axis (unit vector) and angle (radians, 0 < angle ≤ π) of a rotation, or None for the identity."""
    cos = np.clip((np.trace(op) - 1.0) / 2.0, -1.0, 1.0)
    angle = float(np.arccos(cos))
    if angle < 1e-6:
        return None
    if np.pi - angle < 1e-6:
        # A half turn: (op + 1) / 2 projects onto the axis.
        proj = (op + np.eye(3)) / 2.0
        axis = proj[:, int(np.argmax(np.linalg.norm(proj, axis=0)))]
    else:
        axis = np.array([op[2, 1] - op[1, 2], op[0, 2] - op[2, 0], op[1, 0] - op[0, 1]])
    return axis / np.linalg.norm(axis), angle


def _distinct(points: np.ndarray, tol: float) -> list[int]:
    """Indices of the first of each group of points closer than `tol`, in order."""
    kept: list[int] = []
    for i, p in enumerate(points):
        if not any(np.linalg.norm(p - points[j]) < tol for j in kept):
            kept.append(i)
    return kept


def _rank(d: np.ndarray) -> tuple[float, float, float]:
    """The order that names one direction of several equivalent ones: highest along Z, then X, then Y."""
    r = np.round(d, 6) + 0.0
    return (float(r[2]), float(r[0]), float(r[1]))


@dataclass(frozen=True)
class AxisClass:
    """The directions along a point group's axes that the group carries onto one another: one choice of where a
    unit can sit on an axis. `direction` is the one named (`_rank`), `copies` how many there are."""

    label: str
    order: int
    direction: tuple[float, float, float]
    copies: int


@lru_cache(maxsize=32)
def axis_classes(symmetry: str) -> tuple[AxisClass, ...]:
    """A point group's axes as the choices a site has, highest order first. Directions the group does not carry
    onto one another are separate choices of the same order (the two ends of a Cn's axis, the two kinds of 2-fold
    of a Dn with n odd), lettered in `_rank` order."""
    ops = operators(symmetry)
    lines: list[tuple[np.ndarray, int]] = []
    for op in ops:
        found = _axis_of(op)
        if found is None:
            continue
        axis = found[0]
        for i, (line, n) in enumerate(lines):
            if abs(abs(float(line @ axis)) - 1.0) < _TOL:
                lines[i] = (line, n + 1)
                break
        else:
            lines.append((axis, 1))
    found_classes: list[tuple[int, np.ndarray, int]] = []
    seen: list[np.ndarray] = []
    for line, n_turns in lines:
        for d in (line, -line):
            if any(np.linalg.norm(d - s) < _TOL for s in seen):
                continue
            orbit = np.einsum("mij,j->mi", ops, d)
            orbit = orbit[_distinct(orbit, _TOL)]
            seen.extend(orbit)
            named = max(orbit, key=_rank)
            found_classes.append((n_turns + 1, named, len(orbit)))
    found_classes.sort(key=lambda c: (-c[0], tuple(-x for x in _rank(c[1]))))
    out = []
    for order, named, copies in found_classes:
        same = [c for c in found_classes if c[0] == order]
        letter = f" {'abcdefgh'[next(i for i, c in enumerate(same) if c[1] is named)]}" if len(same) > 1 else ""
        out.append(AxisClass(f"{order}-fold{letter}", order, tuple(float(x) for x in np.round(named, 9) + 0.0), copies))
    return tuple(out)


# ── Sites ────────────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True, eq=False)
class Site:
    """One kind of unit in one parent: `rotations[k]` and `offsets[k]` (Å) are copy k's transform in the parent's
    reference frame. `description` is what was asked, complete enough to make the same site again."""

    description: dict[str, Any]
    rotations: np.ndarray
    offsets: np.ndarray
    own_symmetry: str = ""  # a point-group site: the symmetry the unit has where it sits ("C5")
    spacing_ang: float | None = None  # the least distance between two copies of one parent
    along_ang: np.ndarray | None = None  # a helix: how far along the axis each copy sits from its parent's centre

    @property
    def kind(self) -> str:
        return str(self.description["kind"])

    @property
    def n(self) -> int:
        return len(self.offsets)

    @property
    def digest(self) -> str:
        """What tells one site from another in a uid: the description, whatever its parents."""
        return hashlib.sha1(json.dumps(self.description, sort_keys=True).encode()).hexdigest()[:10]

    def text(self) -> str:
        d = self.description
        if self.kind == POINT_GROUP:
            where = (
                d["axis"] if d["axis"] != DIRECTION else "direction ({:.3g}, {:.3g}, {:.3g})".format(*d["direction"])
            )
            return f"{where} of {d['group']} at {d['radius_ang']:g} Å"
        every = f", every {d['every']}" if d["every"] != 1 else ""
        repeat = f", repeat {d['axial_repeat_ang']:g} Å along the axis" if d.get("axial_repeat_ang") else ""
        return f"helix {d['twist_deg']:g}° / {d['rise_ang']:g} Å at {d['radius_ang']:g} Å{every}{repeat}"


def _spacing(points: np.ndarray) -> float | None:
    if len(points) < 2:
        return None
    d = np.linalg.norm(points[:, None, :] - points[None, :, :], axis=2)
    return float(d[~np.eye(len(points), dtype=bool)].min())


def _positive(value, what: str) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{what} is not given") from None
    if not np.isfinite(v) or v <= 0:
        raise ValueError(f"{what} must be more than 0 (got {value!r})")
    return v


def point_group_site(symmetry: str, *, axis: str = "", direction=None, radius_ang) -> Site:
    """A unit on `axis` (an `axis_classes` label) or along `direction` (three numbers in the map's frame, any
    length) of a particle with point group `symmetry`, `radius_ang` from the particle's centre. A `direction`
    wins over `axis`, which then only names it: a site made again from its description sits where it sat."""
    name = group_name(symmetry)
    ops = operators(name)
    radius = _positive(radius_ang, "the radius")
    label = str(axis or "").strip()
    if direction is not None:
        d = np.asarray(direction, dtype=float).reshape(-1)
        if d.shape != (3,) or not np.isfinite(d).all() or not np.linalg.norm(d) > 0:
            raise ValueError("a direction is three numbers that are not all zero")
        # A direction of length 1 already (one a description kept) is taken as it is, so the site is the same.
        length = float(np.linalg.norm(d))
        d = d if abs(length - 1.0) < 1e-8 else d / length
        label = label or DIRECTION
    else:
        classes = {c.label: c for c in axis_classes(name)}
        if label not in classes:
            raise ValueError(f"{name} has no axis {label!r}; its axes: {', '.join(classes) or 'none'}, or a direction")
        d = np.asarray(classes[label].direction, dtype=float)
    # The site is built from the numbers its description keeps, so made again from it, it is the same to the digit.
    d = np.round(d, 9) + 0.0
    v0 = radius * d
    q0 = frame_on(d)
    positions = np.einsum("mij,j->mi", ops, v0)
    keep = _distinct(positions, min(_SAME_POINT_ANG, radius * 1e-3))
    if len(ops) % len(keep):
        raise ValueError(f"{len(ops)} operators of {name} put the unit in {len(keep)} places: not a point group's")
    own = len(ops) // len(keep)
    return Site(
        description={
            "kind": POINT_GROUP,
            "group": name,
            "axis": label,
            "direction": [float(x) for x in d],
            "radius_ang": radius,
        },
        rotations=ops[keep] @ q0,
        offsets=positions[keep],
        own_symmetry=f"C{own}",
        spacing_ang=_spacing(positions[keep]),
    )


def helix_site(
    *,
    twist_deg,
    rise_ang,
    radius_ang,
    half_window_ang: float,
    axial_repeat_ang=None,
    every: int = 1,
    azimuth_deg: float = 0.0,
    axial_ang: float = 0.0,
) -> Site:
    """The units of a helical lattice within `half_window_ang` along the axis of a segment's centre: the copies
    one segment gives. A unit sits at `radius_ang` from the axis, `azimuth_deg` about it from the map's X and
    `axial_ang` along it from the box centre; its mates are every `every`-th step of the helix and, with
    `axial_repeat_ang`, the same again each repeat along the axis. Each copy's frame keeps Z along the filament and
    has its X pointing away from the axis."""
    try:
        twist, rise = float(twist_deg), float(rise_ang)
    except (TypeError, ValueError):
        raise ValueError("the twist and the rise are not given") from None
    radius = _positive(radius_ang, "the radius")
    every = int(every)
    if every < 1:
        raise ValueError("every n-th step of the helix: n is 1 or more")
    repeat = (
        None if axial_repeat_ang in (None, "", 0, 0.0) else _positive(axial_repeat_ang, "the repeat along the axis")
    )
    step_twist, step_rise = twist * every, rise * every
    if repeat is None and abs(step_rise) < 1e-6:
        raise ValueError("a helix with no rise and no repeat along the axis has no second unit")
    half = _positive(half_window_ang, "the length of filament a segment stands for")
    azimuth, axial = float(azimuth_deg or 0.0), float(axial_ang or 0.0)
    steps: list[tuple[int, int]] = []
    if repeat is None:
        k_max = int(np.floor((half + abs(axial)) / abs(step_rise))) + 1
        steps = [(k, 0) for k in range(-k_max, k_max + 1)]
    else:
        # One turn of the helix, each of its units repeated along the axis.
        k_max = int(np.floor(180.0 / abs(step_twist))) if abs(step_twist) > 1e-9 else 0
        for k in range(-k_max, k_max + 1):
            z0 = axial + k * step_rise
            m_lo, m_hi = int(np.ceil((-half - z0) / repeat)), int(np.floor((half - z0) / repeat))
            steps.extend((k, m) for m in range(m_lo, m_hi + 1))
    rotations, offsets = [], []
    for k, m in steps:
        phi = azimuth + k * step_twist
        z = axial + k * step_rise + m * (repeat or 0.0)
        if abs(z) > half:
            continue
        turn = rot_z(phi)
        rotations.append(turn)
        offsets.append(turn @ np.array([radius, 0.0, 0.0]) + np.array([0.0, 0.0, z]))
    if not offsets:
        raise ValueError("no unit of this helix falls within a segment's length")
    offsets_a, rotations_a = np.asarray(offsets), np.asarray(rotations)
    keep = _distinct(offsets_a, _SAME_POINT_ANG)
    offsets_a, rotations_a = offsets_a[keep], rotations_a[keep]
    return Site(
        description={
            "kind": HELIX,
            "twist_deg": twist,
            "rise_ang": rise,
            "radius_ang": radius,
            "axial_repeat_ang": repeat,
            "every": every,
            "azimuth_deg": azimuth,
            "axial_ang": axial,
        },
        rotations=rotations_a,
        offsets=offsets_a,
        spacing_ang=_spacing(offsets_a),
        along_ang=offsets_a[:, 2].copy(),
    )


# ── Expansions: the poses a particle's symmetry, or its lattice's, makes equal ───────────────────

LATTICE = "lattice"
# A helix comes back onto itself when its steps turn whole turns within this and rise whole repeats within that.
_CLOSE_DEG = 0.5
_CLOSE_ANG = 1.0
_MAX_STEPS = 60


@dataclass(frozen=True, eq=False)
class Expansion:
    """The poses of one particle that a refinement could not tell apart: copy k is the particle's pose followed by
    (`rotations[k]`, `offsets[k]`), labelled `labels[k]`. A point group's are its operators (the particle sits on
    its own centre in each). A lattice's are its registrations: the poses one helical step, or one unit along the
    axis, apart, as many as the true repeat tells apart, each by its smallest move. The images stay the particle's;
    only the pose and the origin change."""

    description: dict[str, Any]
    rotations: np.ndarray
    offsets: np.ndarray
    labels: np.ndarray
    turns_deg: np.ndarray
    shifts_ang: np.ndarray
    by_steps: bool = False  # a lattice whose labels count the helix's own steps (every registration is one)

    @property
    def kind(self) -> str:
        return str(self.description["kind"])

    @property
    def n(self) -> int:
        return len(self.offsets)

    @property
    def digest(self) -> str:
        return hashlib.sha1(json.dumps(self.description, sort_keys=True).encode()).hexdigest()[:10]

    @property
    def max_shift_ang(self) -> float:
        return float(np.abs(self.shifts_ang).max()) if self.n else 0.0

    def text(self) -> str:
        d = self.description
        if self.kind == POINT_GROUP:
            return f"the {self.n} operators of {d['group']}"
        return (
            f"{self.n} registrations of a helix {d['twist_deg']:g}° / {d['rise_ang']:g} Å with "
            f"{d['axial_repeat_ang']:g} Å along the axis, {d['units_per_repeat']} units to a repeat"
        )

    def table(self) -> list[dict[str, float]]:
        """One row per copy: its label, its turn about the axis and its move along it."""
        return [
            {"label": int(k), "turn_deg": round(float(t), 3), "shift_ang": round(float(s), 3)}
            for k, t, s in zip(self.labels, self.turns_deg, self.shifts_ang, strict=True)
        ]


def _wrap_deg(deg: float) -> float:
    return float((deg + 180.0) % 360.0 - 180.0)


def point_group_expansion(symmetry: str) -> Expansion:
    """Every operator of `symmetry`, in RELION's order: the copies RELION's symmetry expansion makes."""
    name = group_name(symmetry)
    if name in ("", "C1"):
        raise ValueError("C1 has one operator: there is nothing to expand")
    ops = operators(name)
    n = len(ops)
    return Expansion(
        description={"kind": POINT_GROUP, "group": name},
        rotations=np.array(ops),
        offsets=np.zeros((n, 3)),
        labels=np.arange(n),
        turns_deg=np.array([np.degrees(a[1]) if (a := _axis_of(op)) else 0.0 for op in ops]),
        shifts_ang=np.zeros(n),
    )


def lattice_expansion(*, twist_deg, rise_ang, axial_repeat_ang, units_per_repeat) -> Expansion:
    """The registrations of a helical lattice with several starts whose units a refinement could not tell apart,
    when the true repeat along the axis is `units_per_repeat` units (2 for a microtubule's alpha/beta dimers).

    The lattice the refinement saw is carried onto itself by a helical step (`twist_deg`, `rise_ang`) and by one
    unit straight along the axis (`axial_repeat_ang`); after n steps the helix is back on its own protofilament m
    units up (13 and 3 for a 13_3 microtubule). Two of those poses that differ by whole true repeats are one
    registration, so there are n × `units_per_repeat` (26). Each is taken by its smallest move along the axis, so a
    copy's reference moves at most half a true repeat from the particle's own (41 Å for a microtubule), not the
    124 Å of 26 helical steps in a row. Labels count the helix's steps (k = 0 … 25, k steps of the helix) when
    every registration is some number of steps; otherwise turns + n × units."""
    try:
        twist, rise = float(twist_deg), float(rise_ang)
    except (TypeError, ValueError):
        raise ValueError("the twist and the rise are not given") from None
    if not np.isfinite(twist) or abs(twist) < 1e-6:
        raise ValueError("a lattice's registrations need its twist (not 0)")
    repeat = _positive(axial_repeat_ang, "the repeat along the axis")
    try:
        u = int(units_per_repeat)
    except (TypeError, ValueError):
        raise ValueError("how many units make the true repeat is not given") from None
    if u < 2:
        raise ValueError("with one unit to a repeat every registration is the same: units per repeat is 2 or more")
    found = None
    for steps in range(1, _MAX_STEPS + 1):
        turns, ups = steps * twist / 360.0, steps * rise / repeat
        if round(turns) != 0 and abs(turns - round(turns)) * 360.0 < _CLOSE_DEG:
            if abs(ups - round(ups)) * repeat < _CLOSE_ANG:
                found = (steps, round(ups))
                break
    if found is None:
        raise ValueError(
            f"{_MAX_STEPS} steps of a helix of {twist:g}° and {rise:g} Å never come back onto the same "
            f"protofilament a whole number of {repeat:g} Å repeats up: not a lattice of several starts"
        )
    n, m = found
    true_repeat = u * repeat
    rotations, offsets, labels, turns_out, shifts = [], [], [], [], []
    by_steps = int(np.gcd(m, u)) == 1
    for a0 in range(n):
        for b0 in range(u):
            # (a0, b0) is a0 steps and b0 units; n steps are m units, u units a true repeat.
            candidates = []
            for j in (-1, 0, 1):
                a = a0 + j * n
                z0 = a * rise + (b0 - j * m) * repeat
                i = round(-z0 / true_repeat)
                candidates += [(a, z0 + ii * true_repeat) for ii in (i - 1, i, i + 1)]
            # n steps less m units moves by n·rise − m·repeat, a hundredth of an Å: moves equal within a tenth
            # are one, and the fewest steps take it.
            least = min(abs(z) for _, z in candidates)
            a, z = min(((a, z) for a, z in candidates if abs(z) <= least + 0.1), key=lambda c: (abs(c[0]), -c[1]))
            if by_steps:
                # The step count k whose k steps land in this registration: k ≡ a0 (mod n), (k // n)·m ≡ b0 (mod u).
                label = next(k for k in range(n * u) if k % n == a0 and ((k // n) * m - b0) % u == 0)
            else:
                label = a0 + n * b0
            turn = _wrap_deg(a * twist)
            rotations.append(rot_z(turn))
            offsets.append([0.0, 0.0, z])
            labels.append(label)
            turns_out.append(turn)
            shifts.append(z)
    order = np.argsort(labels, kind="stable")
    return Expansion(
        description={
            "kind": LATTICE,
            "twist_deg": twist,
            "rise_ang": rise,
            "axial_repeat_ang": repeat,
            "units_per_repeat": u,
        },
        rotations=np.asarray(rotations)[order],
        offsets=np.asarray(offsets)[order],
        labels=np.asarray(labels)[order],
        turns_deg=np.asarray(turns_out)[order],
        shifts_ang=np.asarray(shifts)[order],
        by_steps=by_steps,
    )


def expansion_of(description: dict[str, Any]) -> Expansion:
    """The expansion a description names."""
    kind = (description or {}).get("kind")
    if kind == POINT_GROUP:
        return point_group_expansion(description.get("group", ""))
    if kind == LATTICE:
        return lattice_expansion(
            twist_deg=description.get("twist_deg"),
            rise_ang=description.get("rise_ang"),
            axial_repeat_ang=description.get("axial_repeat_ang"),
            units_per_repeat=description.get("units_per_repeat"),
        )
    raise ValueError(f"not a kind of expansion: {kind!r}")


def site_of(description: dict[str, Any], *, half_window_ang: float | None = None) -> Site:
    """The site a description names. A helix's copies depend on how much filament a segment stands for
    (`half_window_ang`), which the segments say, not the description."""
    kind = (description or {}).get("kind")
    if kind == POINT_GROUP:
        return point_group_site(
            description.get("group", ""),
            axis=description.get("axis", ""),
            direction=description.get("direction"),
            radius_ang=description.get("radius_ang"),
        )
    if kind == HELIX:
        if half_window_ang is None:
            raise ValueError("a helix's copies need the length of filament a segment stands for")
        return helix_site(
            twist_deg=description.get("twist_deg"),
            rise_ang=description.get("rise_ang"),
            radius_ang=description.get("radius_ang"),
            half_window_ang=half_window_ang,
            axial_repeat_ang=description.get("axial_repeat_ang"),
            every=description.get("every") or 1,
            azimuth_deg=description.get("azimuth_deg") or 0.0,
            axial_ang=description.get("axial_ang") or 0.0,
        )
    raise ValueError(f"not a kind of site: {kind!r}")


__all__ = [
    "DIRECTION",
    "HELIX",
    "LATTICE",
    "POINT_GROUP",
    "AxisClass",
    "Expansion",
    "Site",
    "axis_classes",
    "expansion_of",
    "group_name",
    "helix_site",
    "known_group",
    "lattice_expansion",
    "operators",
    "point_group_expansion",
    "point_group_site",
    "site_of",
]
