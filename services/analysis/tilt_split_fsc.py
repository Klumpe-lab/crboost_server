"""Tilt-split FSC: how self-consistent a tilt series' alignment is, measured on its reconstructions.

Warp's `ts_reconstruct --halfmap_tilts` reconstructs two half-tomograms from alternate tilts (by angle).
Every per-tilt alignment error lands in one half only, so the halves disagree where the alignment is
inconsistent; frame-split halves share their tilts' geometry and cannot see it. The FSC between the
tilt halves, within the lamella, compared between two alignments of the same series, is the particle-free
verdict on a refinement.

Two readouts per XY tile of the lamella:
- shells: FSC over full |k| shells. Above k_c ≈ 1/(thickness · tilt step) the halves' tilt planes only
  meet near the tilt axis, so shifts across the axis are testable only below about k_c.
- axial: FSC inside a cylinder |k⊥| < radius around the tilt axis, binned along the axis. Every tilt
  plane contains the axis, so both halves have support there up to Nyquist: this readout tests shifts
  along the axis at high resolution and is blind to shifts across it.

Tomograms are (Z, Y, X) arrays with the tilt axis along Y — the frame Warp reconstructs in.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from services.analysis.lamella_slab import fit_slab

# Width of the raised-cosine edge of the slab mask, in voxels.
MASK_EDGE_VOXELS = 8.0
# The axial cylinder holds few Fourier voxels per shell width; its bins are this many shells wide.
AXIAL_BIN_SHELLS = 4


@dataclass(frozen=True, slots=True)
class Slab:
    """A lamella as an inclined slab, in voxels of the full tomogram: its mid-plane is
    z = z0 + slope_x·x + slope_y·y."""

    z0: float
    slope_x: float
    slope_y: float
    half_thickness: float

    def center_z(self, x, y):
        return self.z0 + self.slope_x * x + self.slope_y * y


def bin_volume(volume: np.ndarray, factor: int) -> np.ndarray:
    """Block-average a (Z, Y, X) volume by `factor` along every axis, a few slices at a time so a
    memory-mapped tomogram is never loaded whole."""
    nz, ny, nx = (s // factor for s in volume.shape)
    out = np.empty((nz, ny, nx), dtype=np.float32)
    for i in range(nz):
        block = np.asarray(volume[i * factor : (i + 1) * factor, : ny * factor, : nx * factor], dtype=np.float32)
        out[i] = block.reshape(factor, ny, factor, nx, factor).mean(axis=(0, 2, 4))
    return out


def _unbinned(index: float, factor: int) -> float:
    """Full-resolution position of a (fractional) index into a `factor`-binned axis: binned index i
    averages slices i·factor .. i·factor + factor - 1."""
    return index * factor + (factor - 1) / 2


def fit_inclined_slab(volume: np.ndarray, *, grid: int = 4, bin_factor: int = 4, fov: float = 0.8) -> Slab | None:
    """The lamella in a full (Z, Y, X) tomogram as an inclined slab: the slab in the Z profile of each cell
    of a grid x grid XY grid over the central `fov` (lamella_slab.fit_slab, on a `bin_factor`-binned copy),
    a plane through the centres of the cells' slab extents, the median of those extents. None when fewer
    than three cells show a slab."""
    b = bin_factor
    binned = bin_volume(volume, b)
    bz, by, bx = binned.shape
    margin = (1 - fov) / 2
    y_edges = np.linspace(margin * by, (1 - margin) * by, grid + 1).round().astype(int)
    x_edges = np.linspace(margin * bx, (1 - margin) * bx, grid + 1).round().astype(int)
    xs, ys, zs, thickness = [], [], [], []
    for iy in range(grid):
        for ix in range(grid):
            cell = binned[:, y_edges[iy] : y_edges[iy + 1], x_edges[ix] : x_edges[ix + 1]]
            fit = fit_slab(cell.reshape(bz, -1).std(axis=1), 1.0)  # voxel = 1: offsets in binned voxels
            if not fit.found:
                continue
            xs.append(_unbinned((x_edges[ix] + x_edges[ix + 1] - 1) / 2, b))
            ys.append(_unbinned((y_edges[iy] + y_edges[iy + 1] - 1) / 2, b))
            zs.append(_unbinned(bz / 2 + (fit.lower_angstrom + fit.upper_angstrom) / 2, b))
            thickness.append((fit.upper_angstrom - fit.lower_angstrom) * b)
    if len(zs) < 3:
        return None
    design = np.column_stack([np.ones(len(zs)), xs, ys])
    z0, slope_x, slope_y = np.linalg.lstsq(design, np.asarray(zs), rcond=None)[0]
    return Slab(float(z0), float(slope_x), float(slope_y), float(np.median(thickness)) / 2)


def slab_mask(slab: Slab, z_range: tuple[int, int], y_range: tuple[int, int], x_range: tuple[int, int]) -> np.ndarray:
    """Soft mask over a window of the tomogram: 1 inside the slab, a raised cosine over
    MASK_EDGE_VOXELS outside it, 0 beyond."""
    z = np.arange(*z_range, dtype=np.float32)[:, None, None]
    y = np.arange(*y_range, dtype=np.float32)[None, :, None]
    x = np.arange(*x_range, dtype=np.float32)[None, None, :]
    outside = np.abs(z - slab.center_z(x, y)) - slab.half_thickness
    edge = np.clip(outside / MASK_EDGE_VOXELS, 0.0, 1.0)
    return (0.5 * (1 + np.cos(np.pi * edge))).astype(np.float32)


@dataclass(frozen=True, slots=True)
class FscCurves:
    """FSC per frequency bin of every tile of one reconstruction pair. Arrays are (tiles, bins)."""

    tiles: np.ndarray  # (tiles, 2): y, x of each tile's corner, voxels
    shell_freq: np.ndarray  # (bins,) 1/Å, bin centres of |k|
    shell_fsc: np.ndarray
    shell_n: np.ndarray  # Fourier voxels per bin
    axial_freq: np.ndarray  # (bins,) 1/Å, bin centres of |k along the tilt axis|
    axial_fsc: np.ndarray
    axial_n: np.ndarray
    axial_radius: float  # 1/Å, radius of the cylinder around the axis


def _binned_fsc(f1: np.ndarray, f2: np.ndarray, key: np.ndarray, n_bins: int) -> tuple[np.ndarray, np.ndarray]:
    """FSC per bin of `key` (values >= n_bins are left out) and the Fourier voxels per bin."""
    inside = key < n_bins
    k = key[inside]
    a, b = f1[inside], f2[inside]
    cross = np.bincount(k, (a * b.conj()).real, n_bins)
    p1 = np.bincount(k, np.abs(a) ** 2, n_bins)
    p2 = np.bincount(k, np.abs(b) ** 2, n_bins)
    n = np.bincount(k, minlength=n_bins)
    with np.errstate(invalid="ignore", divide="ignore"):
        fsc = cross / np.sqrt(p1 * p2)
    return fsc, n


def tile_fsc(
    even: np.ndarray,
    odd: np.ndarray,
    slab: Slab,
    voxel: float,
    *,
    tile: int = 256,
    fov: float = 0.8,
    axial_radius: float,
) -> FscCurves:
    """Shell and axial FSC between two half-tomograms (same shape, (Z, Y, X), tilt axis along Y) in square
    XY tiles of `tile` voxels over the central `fov`, each within the slab's soft mask over a Z window
    that follows the slab. `axial_radius` (1/Å) is the cylinder radius around the tilt axis."""
    if even.shape != odd.shape:
        raise ValueError(f"half-tomograms differ in shape: {even.shape} vs {odd.shape}")
    nz, ny, nx = even.shape
    dk = 1.0 / (tile * voxel)
    n_bins = int(0.5 / voxel / dk) + 1
    n_axial = int(0.5 / voxel / (AXIAL_BIN_SHELLS * dk)) + 1
    half_window = int(np.ceil(slab.half_thickness + 2 * MASK_EDGE_VOXELS))
    starts = []
    for n in (ny, nx):
        count = int(fov * n // tile)  # an axis shorter than a tile yields no tiles
        first = (n - count * tile) // 2
        starts.append([first + i * tile for i in range(count)])
    tiles, shells, shell_counts, axial, axial_counts = [], [], [], [], []
    for y0 in starts[0]:
        for x0 in starts[1]:
            zc = slab.center_z(x0 + tile / 2, y0 + tile / 2)
            z_mid = int(np.rint(zc))
            z_lo, z_hi = max(0, z_mid - half_window), min(nz, z_mid + half_window)
            if z_hi - z_lo < 2 * MASK_EDGE_VOXELS:
                continue  # the slab lies outside the volume here
            mask = slab_mask(slab, (z_lo, z_hi), (y0, y0 + tile), (x0, x0 + tile))
            weight = mask.sum()
            if weight <= 0:
                continue
            spectra = []
            for half in (even, odd):
                block = np.asarray(half[z_lo:z_hi, y0 : y0 + tile, x0 : x0 + tile], dtype=np.float32)
                block = (block - (block * mask).sum() / weight) * mask
                spectra.append(np.fft.rfftn(block))
            kz = np.fft.fftfreq(z_hi - z_lo, d=voxel)[:, None, None]
            ky = np.fft.fftfreq(tile, d=voxel)[None, :, None]
            kx = np.fft.rfftfreq(tile, d=voxel)[None, None, :]
            shell_key = (np.sqrt(kz**2 + ky**2 + kx**2) / dk).astype(np.int64)
            fsc, n = _binned_fsc(spectra[0], spectra[1], shell_key, n_bins)
            shells.append(fsc)
            shell_counts.append(n)
            along = np.broadcast_to(np.abs(ky) / (AXIAL_BIN_SHELLS * dk), shell_key.shape).astype(np.int64)
            off_axis = np.broadcast_to(np.sqrt(kz**2 + kx**2) >= axial_radius, shell_key.shape)
            along = np.where(off_axis, n_axial, along)
            fsc, n = _binned_fsc(spectra[0], spectra[1], along, n_axial)
            axial.append(fsc)
            axial_counts.append(n)
            tiles.append((y0, x0))
    return FscCurves(
        tiles=np.asarray(tiles, dtype=np.int64).reshape(-1, 2),
        shell_freq=(np.arange(n_bins) + 0.5) * dk,
        shell_fsc=np.asarray(shells).reshape(-1, n_bins),
        shell_n=np.asarray(shell_counts).reshape(-1, n_bins),
        axial_freq=(np.arange(n_axial) + 0.5) * AXIAL_BIN_SHELLS * dk,
        axial_fsc=np.asarray(axial).reshape(-1, n_axial),
        axial_n=np.asarray(axial_counts).reshape(-1, n_axial),
        axial_radius=float(axial_radius),
    )


def crossover_frequency(thickness_angstrom: float, tilt_step_deg: float) -> float:
    """k_c ≈ 1/(D·Δθ) (1/Å): above it, neighbouring tilt planes of a slab of thickness D no longer overlap."""
    return 1.0 / (thickness_angstrom * np.deg2rad(tilt_step_deg))


def band_log_ssnr_ratio(
    freq: np.ndarray,
    fsc_ref: np.ndarray,
    n_ref: np.ndarray,
    fsc_test: np.ndarray,
    n_test: np.ndarray,
    band: tuple[float, float],
) -> float:
    """Mean over the bins in `band` (1/Å) of ln(SSNR_test / SSNR_ref), SSNR = FSC / (1 - FSC), over the
    bins where both FSCs clear 3/sqrt(n) (their noise level) and stay below 1. NaN when no bin qualifies.
    Positive = the test alignment is more self-consistent."""
    lo, hi = band
    with np.errstate(invalid="ignore", divide="ignore"):
        floor_ref = 3 / np.sqrt(np.maximum(n_ref, 1))
        floor_test = 3 / np.sqrt(np.maximum(n_test, 1))
        ok = (
            (freq >= lo)
            & (freq <= hi)
            & (n_ref > 0)
            & (n_test > 0)
            & (fsc_ref > floor_ref)
            & (fsc_test > floor_test)
            & (fsc_ref < 1)
            & (fsc_test < 1)
        )
        if not ok.any():
            return float("nan")
        ssnr_ref = fsc_ref[ok] / (1 - fsc_ref[ok])
        ssnr_test = fsc_test[ok] / (1 - fsc_test[ok])
        return float(np.mean(np.log(ssnr_test / ssnr_ref)))


def paired_tile_scores(ref: FscCurves, test: FscCurves, *, shell_band, axial_band) -> tuple[np.ndarray, np.ndarray]:
    """Per tile present in both (matched by position): the shell and the axial band score of `test`
    against `ref`."""
    index = {tuple(t): i for i, t in enumerate(ref.tiles.tolist())}
    shell, axial = [], []
    for j, t in enumerate(test.tiles.tolist()):
        i = index.get(tuple(t))
        if i is None:
            continue
        shell.append(
            band_log_ssnr_ratio(
                ref.shell_freq, ref.shell_fsc[i], ref.shell_n[i], test.shell_fsc[j], test.shell_n[j], shell_band
            )
        )
        axial.append(
            band_log_ssnr_ratio(
                ref.axial_freq, ref.axial_fsc[i], ref.axial_n[i], test.axial_fsc[j], test.axial_n[j], axial_band
            )
        )
    return np.asarray(shell), np.asarray(axial)
