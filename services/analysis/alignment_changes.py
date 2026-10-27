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
plus the size of any local image-warp grid the refinement wrote. `refinement_changes` gives the same per tilt
for drawing: the whole-tilt shift with the grid's mean in it, and each tilt's warp node by node.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import lru_cache
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


@dataclass(frozen=True, slots=True)
class MovementGrid:
    """A Warp <GridMovementX/Y>: one W×H lattice of image shifts (Å) per tilt, in the XML's tilt order.
    Node i of an axis of N nodes sits at margin + i·(1 - 2·margin)/(N - 1) of the image (warpylib
    CubicGrid), Y running along the image rows. Warp subtracts the interpolated movement from where a
    volume point projects (warpylib `get_position_in_all_tilts`), so content moves by minus the grid."""

    width: int
    height: int
    margin_x: float
    margin_y: float
    x: np.ndarray  # (tilts, H, W), Å
    y: np.ndarray

    @property
    def mean_x(self) -> np.ndarray:
        return self.x.mean(axis=(1, 2))

    @property
    def mean_y(self) -> np.ndarray:
        return self.y.mean(axis=(1, 2))

    @property
    def deform_x(self) -> np.ndarray:
        """The nodes with each tilt's mean removed: the warp proper."""
        return self.x - self.mean_x[:, None, None]

    @property
    def deform_y(self) -> np.ndarray:
        return self.y - self.mean_y[:, None, None]


def _grid_values(
    el: ET.Element, n_tilts: int, xml_path: Path
) -> tuple[np.ndarray, tuple[int, int], tuple[float, float]]:
    """(values (tilts, H, W), (W, H), (margin X, margin Y)) of one grid element, its nodes placed by their
    X/Y/Z attributes; a Depth-1 grid holds for every tilt."""
    w, h, d = (int(el.get(k, "1")) for k in ("Width", "Height", "Depth"))
    values = np.zeros((d, h, w))
    for node in el.findall("Node"):
        values[int(node.get("Z", "0")), int(node.get("Y", "0")), int(node.get("X", "0"))] = float(
            node.get("Value", "0")
        )
    if d == 1:
        values = np.repeat(values, n_tilts, axis=0)
    elif d != n_tilts:
        raise ValueError(f"{xml_path}: <{el.tag}> has Depth {d}, the series {n_tilts} tilts")
    return values, (w, h), (float(el.get("MarginX", "0")), float(el.get("MarginY", "0")))


def movement_grid(xml_path: Path) -> MovementGrid:
    """The per-tilt image-warp grid of a Warp tilt-series XML; a 1×1 grid (zero unless a refinement
    moved whole tilts through it) when the XML has none."""
    root = ET.parse(xml_path).getroot()
    n = len(_lines(root, "MoviePath"))
    gx, gy = root.find("GridMovementX"), root.find("GridMovementY")
    if gx is None or gy is None:
        zeros = np.zeros((n, 1, 1))
        return MovementGrid(width=1, height=1, margin_x=0.0, margin_y=0.0, x=zeros, y=zeros.copy())
    x, dims_x, margins = _grid_values(gx, n, xml_path)
    y, dims_y, _ = _grid_values(gy, n, xml_path)
    if dims_x != dims_y:
        raise ValueError(f"{xml_path}: GridMovementX is {dims_x}, GridMovementY {dims_y}")
    return MovementGrid(width=dims_x[0], height=dims_x[1], margin_x=margins[0], margin_y=margins[1], x=x, y=y)


def image_size_angstrom(xml_path: Path) -> tuple[float, float]:
    """<TiltSeries ImageDimensionsAngstrom="X, Y">."""
    text = ET.parse(xml_path).getroot().get("ImageDimensionsAngstrom", "")
    parts = [float(v) for v in text.split(",") if v.strip()]
    if len(parts) != 2 or min(parts) <= 0:
        raise ValueError(f"{xml_path}: no usable ImageDimensionsAngstrom ({text!r})")
    return parts[0], parts[1]


@lru_cache(maxsize=256)
def _geometry(path: str, _mtime_ns: int) -> tuple[XmlAlignment, MovementGrid, tuple[float, float]]:
    p = Path(path)
    return read_alignment(p), movement_grid(p), image_size_angstrom(p)


def tilt_geometry(xml_path: Path) -> tuple[XmlAlignment, MovementGrid, tuple[float, float]]:
    """The rigid geometry, the movement grid and the image size of an XML, parsed once per mtime."""
    return _geometry(str(xml_path), xml_path.stat().st_mtime_ns)


@dataclass(frozen=True, slots=True)
class TiltChange:
    """What a refinement did to one tilt, as image-content displacements (Å, image axes, Y along the
    rows). `shift_*` is the whole-tilt move, ΔAxisOffset minus the change of the grid's mean, with the
    rigid volume shift of the series removed; `warp_*` the change of the grid's deformation, (H, W)."""

    movie: str
    angle: float
    shift_x: float
    shift_y: float
    warp_x: np.ndarray
    warp_y: np.ndarray

    @property
    def shift(self) -> float:
        return float(np.hypot(self.shift_x, self.shift_y))

    @property
    def warp_rms(self) -> float:
        return float(np.sqrt(np.mean(self.warp_x**2 + self.warp_y**2)))

    @property
    def warp_max(self) -> float:
        return float(np.hypot(self.warp_x, self.warp_y).max())


@dataclass(frozen=True, slots=True)
class RefinementChanges:
    """Per used tilt in angle order, plus the series' rigid part (`gauge_rms`, Å) and grid shape."""

    tilts: list[TiltChange]
    gauge_rms: float
    width: int
    height: int
    margin_x: float
    margin_y: float
    image_angstrom: tuple[float, float]

    @property
    def has_warp(self) -> bool:
        return self.width * self.height > 1 and any(t.warp_max > 0 for t in self.tilts)


def refinement_changes(before_xml: Path, after_xml: Path) -> RefinementChanges:
    """What changed from `before_xml` to `after_xml` (one series), tilts matched by movie name, only the
    tilts in use after the refinement. Unlike `compare_alignments` the whole-tilt shift includes the
    grid's mean, where `[N,N]` rounds put any shift of a whole tilt."""
    before, grid_b, _ = tilt_geometry(before_xml)
    after, grid_a, image = tilt_geometry(after_xml)
    index_before = {movie: i for i, movie in enumerate(before.movies)}
    pairs = [(i, index_before[m]) for i, m in enumerate(after.movies) if m in index_before and after.use_tilt[i]]
    if len(pairs) < 4:
        raise ValueError(f"{after_xml.name}: only {len(pairs)} used tilts match {before_xml}")
    ia = np.array([a for a, _ in pairs])
    ib = np.array([b for _, b in pairs])
    if (grid_b.width, grid_b.height) not in ((1, 1), (grid_a.width, grid_a.height)):
        raise ValueError(
            f"{after_xml.name}: grid {grid_a.width}x{grid_a.height} after, {grid_b.width}x{grid_b.height} before"
        )
    dx = (after.offset_x[ia] - before.offset_x[ib]) - (grid_a.mean_x[ia] - grid_b.mean_x[ib])
    dy = (after.offset_y[ia] - before.offset_y[ib]) - (grid_a.mean_y[ia] - grid_b.mean_y[ib])
    angles = after.angles[ia]
    fit = rigid_residuals(angles, after.axis_angle[ia], dx, dy)
    warp_x = -(grid_a.deform_x[ia] - grid_b.deform_x[ib])
    warp_y = -(grid_a.deform_y[ia] - grid_b.deform_y[ib])
    tilts = [
        TiltChange(
            movie=after.movies[ia[k]],
            angle=float(angles[k]),
            shift_x=float(fit.residual_x[k]),
            shift_y=float(fit.residual_y[k]),
            warp_x=warp_x[k],
            warp_y=warp_y[k],
        )
        for k in np.argsort(angles)
    ]
    return RefinementChanges(
        tilts=tilts,
        gauge_rms=fit.gauge_rms,
        width=grid_a.width,
        height=grid_a.height,
        margin_x=grid_a.margin_x,
        margin_y=grid_a.margin_y,
        image_angstrom=image,
    )


def node_fractions(n: int, margin: float) -> np.ndarray:
    """Where the nodes of a grid axis of `n` nodes sit, as fractions of the image (warpylib CubicGrid)."""
    if n == 1:
        return np.array([0.5])
    return margin + np.arange(n) * (1 - 2 * margin) / (n - 1)


def interpolated_field(
    warp_x: np.ndarray, warp_y: np.ndarray, margin_x: float, margin_y: float, samples: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """A warp's field on a `samples`² lattice over the image: (fx, fy, u, v), the sample positions as
    fractions of the image and the displacement there (Å). Natural cubic splines through the nodes,
    one axis then the other; Warp's own is an interpolating cubic B-spline whose ends may differ."""
    from scipy.interpolate import CubicSpline

    h, w = warp_x.shape
    nx, ny = node_fractions(w, margin_x), node_fractions(h, margin_y)
    fx = (np.arange(samples) + 0.5) / samples
    fy = fx.copy()

    def _resample(values: np.ndarray) -> np.ndarray:
        rows = CubicSpline(nx, values, axis=1, bc_type="natural")(fx) if w > 1 else np.repeat(values, samples, 1)
        return CubicSpline(ny, rows, axis=0, bc_type="natural")(fy) if h > 1 else np.repeat(rows, samples, 0)

    return fx, fy, _resample(warp_x), _resample(warp_y)
