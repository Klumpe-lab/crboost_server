"""The grid easymode's networks segment on, and the way back to the tomogram's.

easymode's networks were trained on Warp reconstructions at 10 Å. Its own 3D networks resample a
tomogram to that in X, Y and Z themselves. The Ais-engine ones (every .scnm model) resample X/Y only
and take the Z planes as they come, so on a finer tomogram they see less depth than in training. Both
normalize by the spread of the raw voxels, which is wider on a finer grid. The Segment job therefore
gives the tools the tomogram as a 10 Å reconstruction holds it (`write_resampled`) and puts each
result back on the tomogram's grid (`finish_volume`), so every volume keeps the tomogram's shape.

A network that decides each Z plane on its own sees X/Y at its own, coarser pixel (50 Å for cytoplasm
and nucleus); `finish_volume` can smooth its result along Z to that resolution.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import numpy as np

from services.segmentation.volumes import full_scale_of

# The voxel size of the reconstructions easymode's networks were trained on.
TRAINING_VOXEL_ANGSTROM = 10.0
# easymode and Ais leave a volume within 5 % of their network's voxel size as it is.
_TOLERANCE = 0.05
_FWHM_PER_SIGMA = 2.0 * np.sqrt(2.0 * np.log(2.0))


def processing_shape(shape_zyx, voxel: float, target: float = TRAINING_VOXEL_ANGSTROM) -> tuple[int, ...] | None:
    """The tomogram's shape (z, y, x) at `target` Å, or None when it is segmented as it is: within
    5 % of `target` (the tools would not resample it either) or coarser."""
    if voxel >= target * (1.0 - _TOLERANCE):
        return None
    return tuple(max(1, round(int(n) * voxel / target)) for n in shape_zyx)


def write_resampled(src: Path, dst: Path, shape_zyx, *, tmp_dir: Path) -> None:
    """Write the tomogram `src` onto the smaller grid `shape_zyx` as the float32 MRC `dst`, by
    cropping its spectrum: what a reconstruction at the coarser voxel size holds. Sample 0 stays where
    it was and the box keeps its length (`map_back` inverts this), so the voxel size written is the
    box's length over the new count, per axis."""
    import mrcfile

    with mrcfile.mmap(src, mode="r", permissive=True) as m:
        volume = np.array(m.data, dtype=np.float32)
        voxel = float(m.voxel_size.x)
    old = volume.shape
    out = fourier_crop(volume, shape_zyx)
    del volume
    vz, vy, vx = (voxel * n_old / n_new for n_old, n_new in zip(old, shape_zyx, strict=True))
    _write_mrc(dst, out, (vx, vy, vz), tmp_dir)


def fourier_crop(volume: np.ndarray, shape_zyx) -> np.ndarray:
    """`volume` band-limited and sampled onto the smaller `shape_zyx` by keeping the lowest
    frequencies of its spectrum, its mean kept."""
    from scipy import fft

    workers = len(os.sched_getaffinity(0))
    nz, ny, nx = (int(n) for n in shape_zyx)
    spectrum = fft.rfftn(volume, workers=workers)
    kept = spectrum[np.ix_(_lowest(volume.shape[0], nz), _lowest(volume.shape[1], ny), np.arange(nx // 2 + 1))]
    del spectrum
    out = fft.irfftn(kept, s=(nz, ny, nx), workers=workers)
    out *= (nz * ny * nx) / volume.size
    return out.astype(np.float32, copy=False)


def _lowest(n: int, keep: int) -> np.ndarray:
    """The `keep` lowest frequencies of a full FFT axis of length `n`, in FFT order."""
    return np.r_[0 : (keep + 1) // 2, n - keep // 2 : n]


def map_back(p: np.ndarray, shape_zyx) -> np.ndarray:
    """`p`, on a grid `write_resampled` made, linearly interpolated onto the tomogram's `shape_zyx`:
    tomogram voxel i lies at i × n / N on it (sample 0 on sample 0, the box's length kept)."""
    from scipy.ndimage import affine_transform

    scale = [n / big for n, big in zip(p.shape, shape_zyx, strict=True)]
    return affine_transform(p, scale, output_shape=tuple(int(n) for n in shape_zyx), order=1, mode="nearest")


def smooth_z(p: np.ndarray, fwhm_planes: float) -> np.ndarray:
    """`p` smoothed along Z (axis 0) by a Gaussian of FWHM `fwhm_planes` planes."""
    from scipy.ndimage import gaussian_filter1d

    return gaussian_filter1d(p, fwhm_planes / _FWHM_PER_SIGMA, axis=0, mode="nearest")


def finish_volume(
    produced: Path, final: Path, *, shape_zyx, voxel: float, smooth_fwhm: float | None, tmp_dir: Path
) -> str:
    """Make the tool's volume `produced` (on the grid it segmented) the job's volume `final`, on the
    tomogram's grid `shape_zyx` at `voxel` Å and in the tool's encoding: smoothed along Z first when
    `smooth_fwhm` (Å) is given, then mapped back when the grids differ. A volume needing neither is
    moved. Returns what was done, for the log."""
    import mrcfile

    shape = tuple(int(n) for n in shape_zyx)
    with mrcfile.mmap(produced, mode="r", permissive=True) as m:
        grid = tuple(int(n) for n in m.data.shape)
        dtype = m.data.dtype
        p = None if grid == shape and not smooth_fwhm else np.array(m.data, dtype=np.float32)
    if p is None:
        os.replace(produced, final)
        return "kept as the tool wrote it"
    p /= full_scale_of(dtype)
    done = []
    if smooth_fwhm:
        # This grid's Z spacing: the tomogram's depth over its planes.
        z_voxel = voxel * shape[0] / grid[0]
        p = smooth_z(p, smooth_fwhm / z_voxel)
        done.append(f"smoothed along Z (FWHM {smooth_fwhm:g} Å)")
    if grid != shape:
        p = map_back(p, shape)
        done.append(f"mapped back onto {shape[2]} × {shape[1]} × {shape[0]}")
    _write_mrc(final, _encode(p, dtype), (voxel, voxel, voxel), tmp_dir)
    produced.unlink()
    return ", ".join(done)


def _encode(p: np.ndarray, dtype) -> np.ndarray:
    """p (float32, changed in place) stored as `dtype` stores it (`full_scale_of`)."""
    dt = np.dtype(dtype)
    np.clip(p, 0.0, 1.0, out=p)
    if dt.kind == "f":
        return p.astype(dt, copy=False)
    p *= full_scale_of(dt)
    np.rint(p, out=p)
    return p.astype(dt)


def _write_mrc(path: Path, data: np.ndarray, voxel_xyz, tmp_dir: Path) -> None:
    """`data` as the MRC `path`, written under a temporary name in `tmp_dir` (on the same file system)
    and renamed into place, so nothing ever reads half a volume."""
    import mrcfile

    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(tmp_dir), prefix=".tmp_", suffix=".mrc")
    os.close(fd)
    try:
        with mrcfile.new(tmp, overwrite=True) as m:
            m.set_data(data)
            m.voxel_size = voxel_xyz
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
