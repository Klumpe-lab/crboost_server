"""Put several reconstructions of one tilt-series on the same physical plane.

Two alignments of the same tilt-series reconstruct the specimen at different heights in
the volume (a per-tilt shift change that grows with sin(tilt) is a Z translation), so the
same slice index cuts different planes. The translation between two volumes is found by
cross-correlating block-binned copies; every version's X/Y slab is then rendered on the
reference's plane and shifted in X/Y so the same features sit at the same pixels.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import mrcfile
import numpy as np
from scipy import fft
from scipy.signal.windows import tukey

from services.visualization.preview_render import render_xy_slab_preview

BIN = 4
# Cosine edge taper per axis: damps the volume-frame content (field-of-view fringe, empty
# Z margins) that sits at the same voxels in both volumes and would pull the peak to zero.
_TAPER = 0.5


def _binned(mrc_path: Path, b: int) -> tuple[np.ndarray, tuple[int, int, int], float]:
    """Block-mean binned copy (z, y, x), the full-size shape, and the voxel size in Å.
    Reads b Z-slices at a time, so memory is one binned volume plus one chunk."""
    with mrcfile.mmap(str(mrc_path), mode="r") as m:
        data = m.data
        if data.ndim != 3:
            raise ValueError(f"{mrc_path} is not a 3D volume (shape {data.shape})")
        nz, ny, nx = data.shape
        voxel = float(m.voxel_size.x)
        zb, yb, xb = nz // b, ny // b, nx // b
        out = np.empty((zb, yb, xb), dtype=np.float32)
        for k in range(zb):
            chunk = np.asarray(data[k * b : (k + 1) * b, : yb * b, : xb * b], dtype=np.float32)
            out[k] = chunk.reshape(b, yb, b, xb, b).mean(axis=(0, 2, 4))
    return out, (nz, ny, nx), voxel


def _prepared(vol: np.ndarray) -> np.ndarray:
    """Tapered, zero-mean, unit-variance copy."""
    v = vol - vol.mean()
    for axis, n in enumerate(v.shape):
        shape = [1] * v.ndim
        shape[axis] = n
        v *= tukey(n, _TAPER).astype(np.float32).reshape(shape)
    v -= v.mean()
    sd = float(v.std())
    if sd == 0.0:
        raise ValueError("volume is flat after binning; nothing to register")
    return v / sd


def _translation(ref: np.ndarray, mov: np.ndarray) -> tuple[np.ndarray, float]:
    """Shift d (binned voxels, z/y/x) with mov[p + d] ≈ ref[p], and the correlation there."""
    a, b = _prepared(ref), _prepared(mov)
    cc = fft.irfftn(fft.rfftn(b) * np.conj(fft.rfftn(a)), s=a.shape, workers=-1)
    peak = np.unravel_index(int(np.argmax(cc)), cc.shape)
    d = np.array(peak, dtype=float)
    for ax, n in enumerate(cc.shape):
        lo, hi = list(peak), list(peak)
        lo[ax], hi[ax] = (peak[ax] - 1) % n, (peak[ax] + 1) % n
        cm, c0, cp = cc[tuple(lo)], cc[peak], cc[tuple(hi)]
        den = cm - 2 * c0 + cp
        if den < 0:  # parabolic sub-voxel refinement around the maximum
            d[ax] += 0.5 * (cm - cp) / den
        if d[ax] > n / 2:
            d[ax] -= n
    return d, float(cc[peak] / a.size)


def render_matched_slabs(ref: tuple[Path, Path, Path], others: list[tuple[Path, Path, Path]]) -> None:
    """Each entry is (volume, slab PNG, plane JSON). The reference gets its central X/Y slab;
    every other volume is registered against it and gets the slab on the same plane, with
    the offset (full-size voxels) and the correlation it was found at in its JSON."""
    ref_mrc, ref_png, ref_json = ref
    ref_vol, shape, voxel = _binned(ref_mrc, BIN)
    z_ref = shape[0] // 2
    if render_xy_slab_preview(ref_mrc, ref_png) is None:
        raise RuntimeError(f"X/Y slab renderer wrote nothing for {ref_mrc}; see the server log")
    ref_json.write_text(json.dumps({"z": z_ref, "reference": str(ref_mrc)}))

    for mrc, png, meta in others:
        vol, mov_shape, mov_voxel = _binned(mrc, BIN)
        if mov_shape != shape or not math.isclose(mov_voxel, voxel, rel_tol=1e-3):
            raise ValueError(
                f"{mrc.name} is {mov_shape} voxels at {mov_voxel:.2f} Å but the reference {ref_mrc.name} is "
                f"{shape} at {voxel:.2f} Å; planes can only be matched between volumes of the same geometry"
            )
        d, r = _translation(ref_vol, vol)
        dz, dy, dx = (float(v) * BIN for v in d)
        z = z_ref + round(dz)
        if not 0 <= z < shape[0]:
            raise ValueError(f"{mrc.name}: matched plane z={z} (offset {dz:+.1f}) is outside the volume")
        if render_xy_slab_preview(mrc, png, z_center=z, shift_yx=(round(dy), round(dx))) is None:
            raise RuntimeError(f"X/Y slab renderer wrote nothing for {mrc}; see the server log")
        meta.write_text(
            json.dumps({"z": z, "dz": dz, "dy": dy, "dx": dx, "r": r, "bin": BIN, "reference": str(ref_mrc)})
        )
