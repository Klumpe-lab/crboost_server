"""What a refinement changed in a tilt-series alignment, per tilt.

Warp projects a volume point p through Rz(-a)·Ry(θ) (a = the tilt-axis angle), so in the image the tilt
axis runs along (-sin a, cos a) and the direction across it is (cos a, sin a). A rigid translation
(tx, ty, tz) of the whole volume therefore moves tilt θ by tx·cos θ - tz·sin θ across the axis and by ty
along it — three numbers for the whole series. A refiner that re-centres a series (miss-alignment holds
the zero tilt fixed and lets the volume slide along Z) produces exactly that, and it changes only where
the reconstruction sits in its box. `rigid_residuals` fits and removes that part by least squares; what is
left — including any constant shift across the axis, which moves the axis itself — reshapes the
reconstruction.

`compare_alignments` reads two Warp tilt-series XMLs of one series (before, after) and reports both parts
plus the size of any local image-warp grid the refinement wrote.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass(frozen=True, slots=True)
class RigidFit:
    """Per-tilt shift changes (Å) split into the part a rigid volume translation explains (`gauge_*`)
    and the rest (`residual_*`)."""

    gauge_x: np.ndarray
    gauge_y: np.ndarray
    residual_x: np.ndarray
    residual_y: np.ndarray

    @property
    def residual(self) -> np.ndarray:
        return np.hypot(self.residual_x, self.residual_y)

    @property
    def residual_rms(self) -> float:
        return float(np.sqrt(np.mean(self.residual**2)))

    @property
    def gauge_rms(self) -> float:
        return float(np.sqrt(np.mean(self.gauge_x**2 + self.gauge_y**2)))


def rigid_residuals(angles_deg, axis_angle_deg, dx, dy) -> RigidFit:
    """Remove the best-fitting rigid volume translation from per-tilt image-shift changes (Å, the frame of
    AxisOffsetX/Y): across the tilt axis fit tx·cos θ - tz·sin θ, along it a constant ty."""
    theta = np.deg2rad(np.asarray(angles_deg, dtype=float))
    a = np.deg2rad(np.asarray(axis_angle_deg, dtype=float))
    dx = np.asarray(dx, dtype=float)
    dy = np.asarray(dy, dtype=float)
    if theta.ndim != 1 or not (theta.shape == a.shape == dx.shape == dy.shape):
        raise ValueError(
            f"angles, axis angles, dx, dy must be 1-D and equal length, got {theta.shape}, {a.shape}, "
            f"{dx.shape}, {dy.shape}"
        )
    if theta.size < 4:
        raise ValueError(f"need at least 4 tilts to separate a rigid shift from the rest, got {theta.size}")
    across = dx * np.cos(a) + dy * np.sin(a)
    along = -dx * np.sin(a) + dy * np.cos(a)
    basis = np.column_stack([np.cos(theta), np.sin(theta)])
    fit_across = basis @ np.linalg.lstsq(basis, across, rcond=None)[0]
    fit_along = np.full_like(along, along.mean())
    gauge_x = fit_across * np.cos(a) - fit_along * np.sin(a)
    gauge_y = fit_across * np.sin(a) + fit_along * np.cos(a)
    return RigidFit(gauge_x=gauge_x, gauge_y=gauge_y, residual_x=dx - gauge_x, residual_y=dy - gauge_y)


@dataclass(frozen=True, slots=True)
class XmlAlignment:
    """The rigid per-tilt geometry of a Warp tilt-series XML, in its row order."""

    movies: list[str]  # movie basenames
    angles: np.ndarray  # <Angles>, degrees
    axis_angle: np.ndarray  # <AxisAngle>, degrees (in-plane angle of the tilt axis)
    offset_x: np.ndarray  # <AxisOffsetX>, Å (image frame)
    offset_y: np.ndarray  # <AxisOffsetY>, Å (image frame)
    use_tilt: np.ndarray  # <UseTilt>, bool


def _lines(root: ET.Element, name: str) -> list[str]:
    el = root.find(name)
    if el is None or not el.text:
        return []
    return [s.strip() for s in el.text.strip().splitlines() if s.strip()]


def read_alignment(xml_path: Path) -> XmlAlignment:
    root = ET.parse(xml_path).getroot()
    movies = [Path(m).name for m in _lines(root, "MoviePath")]
    n = len(movies)
    if n == 0:
        raise ValueError(f"{xml_path}: no <MoviePath> entries")
    columns = {name: _lines(root, name) for name in ("Angles", "AxisAngle", "AxisOffsetX", "AxisOffsetY")}
    uneven = {name: len(v) for name, v in columns.items() if len(v) != n}
    if uneven:
        raise ValueError(f"{xml_path}: per-tilt lists disagree with {n} <MoviePath> entries: {uneven}")
    use = _lines(root, "UseTilt")
    return XmlAlignment(
        movies=movies,
        angles=np.array(columns["Angles"], dtype=float),
        axis_angle=np.array(columns["AxisAngle"], dtype=float),
        offset_x=np.array(columns["AxisOffsetX"], dtype=float),
        offset_y=np.array(columns["AxisOffsetY"], dtype=float),
        use_tilt=np.array([v == "True" for v in use], dtype=bool) if len(use) == n else np.ones(n, dtype=bool),
    )


@dataclass(frozen=True, slots=True)
class GridStats:
    dims: str  # "WxHxD" (D = tilts)
    rms_angstrom: float  # over nodes, of the (x, y) node vector
    max_angstrom: float


def movement_grid_stats(xml_path: Path) -> GridStats | None:
    """Size of the per-tilt image-warp grid (<GridMovementX/Y>, Å); None when the grid has a single node
    per tilt or every node is zero."""
    root = ET.parse(xml_path).getroot()
    gx, gy = root.find("GridMovementX"), root.find("GridMovementY")
    if gx is None or gy is None:
        return None
    dims = tuple(int(gx.get(k, "1")) for k in ("Width", "Height", "Depth"))
    if dims[0] * dims[1] <= 1:
        return None
    vx = np.array([float(node.get("Value", "0")) for node in gx.findall("Node")])
    vy = np.array([float(node.get("Value", "0")) for node in gy.findall("Node")])
    if vx.shape != vy.shape:
        raise ValueError(f"{xml_path}: GridMovementX has {vx.size} nodes, GridMovementY {vy.size}")
    magnitude = np.hypot(vx, vy)
    if magnitude.size == 0 or not magnitude.any():
        return None
    return GridStats(
        dims="x".join(str(d) for d in dims),
        rms_angstrom=float(np.sqrt(np.mean(magnitude**2))),
        max_angstrom=float(magnitude.max()),
    )


def compare_alignments(before_xml: Path, after_xml: Path, *, threshold_angstrom: float) -> dict:
    """JSON-ready report of what changed from `before_xml` to `after_xml` (the same series).

    Tilts are matched by movie name; only tilts in use after the refinement count. `threshold_angstrom`
    selects the tilts listed under `tilts_over_threshold` (by rigid-removed change)."""
    before, after = read_alignment(before_xml), read_alignment(after_xml)
    index_before = {movie: i for i, movie in enumerate(before.movies)}
    pairs = [(i, index_before[m]) for i, m in enumerate(after.movies) if m in index_before and after.use_tilt[i]]
    if len(pairs) < 4:
        raise ValueError(f"{after_xml.name}: only {len(pairs)} used tilts match {before_xml}")
    ia = np.array([a for a, _ in pairs])
    ib = np.array([b for _, b in pairs])
    angles = after.angles[ia]
    dx = after.offset_x[ia] - before.offset_x[ib]
    dy = after.offset_y[ia] - before.offset_y[ib]
    fit = rigid_residuals(angles, after.axis_angle[ia], dx, dy)
    residual = fit.residual
    order = np.argsort(angles)
    per_tilt = [
        {
            "movie": after.movies[ia[k]],
            "angle_deg": round(float(angles[k]), 3),
            "dx_angstrom": round(float(dx[k]), 3),
            "dy_angstrom": round(float(dy[k]), 3),
            "residual_angstrom": round(float(residual[k]), 3),
        }
        for k in order
    ]
    grid = movement_grid_stats(after_xml)
    return {
        "n_tilts": len(pairs),
        "residual_rms_angstrom": round(fit.residual_rms, 3),
        "residual_max_angstrom": round(float(residual.max()), 3),
        "gauge_rms_angstrom": round(fit.gauge_rms, 3),
        "raw_rms_angstrom": round(float(np.sqrt(np.mean(dx**2 + dy**2))), 3),
        "threshold_angstrom": threshold_angstrom,
        "tilts_over_threshold": [row for row in per_tilt if row["residual_angstrom"] > threshold_angstrom],
        "local_warp": None
        if grid is None
        else {
            "dims": grid.dims,
            "rms_angstrom": round(grid.rms_angstrom, 3),
            "max_angstrom": round(grid.max_angstrom, 3),
        },
        "per_tilt": per_tilt,
    }
