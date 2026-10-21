"""The probability volumes easymode and Ais write: their encoding, whether a file is a finished
volume, and what the views show of each (two overlay masks and the fraction of the volume).

The Segment job runs `summarize_volume` on the compute node right after a tomogram is segmented,
while the volume is still in the page cache; an import of volumes made elsewhere runs the same
function in a worker.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from services.visualization.preview_render import central_slab

# The p at which a voxel counts as the feature on the overlays: the convention of easymode, Pom and
# MemBrain.
DISPLAY_THRESHOLD = 0.5

# easymode and Ais claim a tomogram by writing a 10³ float32 volume (5 kB with its header) under the
# output name before they start on it, so an interrupted run leaves one behind.
PLACEHOLDER_MAX_BYTES = 8192


def full_scale_of(dtype) -> float:
    """The stored value of p = 1 in a volume easymode or Ais wrote, from its data type: easymode's
    int8 holds p × 127; Ais's uint8 (which MRC stores as mode 6 and reads back as uint16) and
    easymode's uint16 hold p × 255; float32 holds p."""
    dt = np.dtype(dtype)
    if dt == np.int8:
        return 127.0
    if dt in (np.uint8, np.uint16):
        return 255.0
    if dt.kind == "f":
        return 1.0
    raise ValueError(f"no known probability encoding for data type {dt}")


def volume_problem(path: Path, shape_zyx) -> str | None:
    """Why `path` is not a finished probability volume on the grid `shape_zyx` (z, y, x), or None
    when it is one."""
    import mrcfile

    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return "missing"
    if size <= PLACEHOLDER_MAX_BYTES:
        return "a placeholder left by an interrupted run"
    try:
        with mrcfile.open(path, header_only=True, permissive=True) as m:
            h = m.header
            shape = (int(h.nz), int(h.ny), int(h.nx))
            dtype = mrcfile.utils.data_dtype_from_header(h)
            data_start = 1024 + int(h.nsymbt)
    except (OSError, ValueError) as e:
        return f"unreadable MRC ({e})"
    if shape != tuple(shape_zyx):
        return f"shape {shape} (z, y, x) is not the tomogram's {tuple(shape_zyx)}"
    try:
        full_scale_of(dtype)
    except ValueError as e:
        return str(e)
    if size < data_start + int(np.prod(shape)) * dtype.itemsize:
        return "truncated (the run stopped while writing it)"
    return None


def summarize_volume(
    volume: Path, *, slab_shape_zyx, slab_itemsize: int, xy_png: Path, xz_png: Path, max_dim: int = 1024
) -> dict:
    """Write `volume`'s two overlay masks and measure it: ``{full_scale, fraction_pct, overlay_xy,
    overlay_xz}``.

    An overlay is p ≥ DISPLAY_THRESHOLD over the planes the tomogram's own slab preview averages
    (`central_slab`, from `slab_shape_zyx` and `slab_itemsize`, the tomogram's shape and data
    type), max-projected so a membrane thinner than the slab still shows, flipped and scaled as the
    slab preview is. It is white with the mask as alpha: the page gives it its colour. The X/Y mask
    covers the central Z planes, the X/Z mask the central Y planes. The fraction is the mean p over
    the whole volume, in percent."""
    import mrcfile

    nz, ny, nx = (int(v) for v in slab_shape_zyx)
    with mrcfile.mmap(volume, mode="r", permissive=True) as m:
        data = m.data
        full_scale = full_scale_of(data.dtype)
        threshold = DISPLAY_THRESHOLD * full_scale
        z_lo, z_hi = central_slab(nz, ny * nx * slab_itemsize)
        y_lo, y_hi = central_slab(ny, nz * nx * slab_itemsize)
        xy = data[z_lo:z_hi].max(axis=0) >= threshold
        xz = data[:, y_lo:y_hi, :].max(axis=1) >= threshold
        fraction = float(np.mean(data, dtype=np.float64)) / full_scale * 100.0
    _save_mask(xy, Path(xy_png), max_dim)
    _save_mask(xz, Path(xz_png), max_dim)
    return {"full_scale": full_scale, "fraction_pct": fraction, "overlay_xy": xy_png, "overlay_xz": xz_png}


def _save_mask(mask: np.ndarray, out_png: Path, max_dim: int) -> None:
    """A 2D mask as a grey-alpha PNG, row 0 at the top as the slab previews have it (Y and Z up)."""
    from PIL import Image

    alpha = Image.fromarray(np.flipud(mask).astype(np.uint8) * 255)
    w, h = alpha.size
    if max(w, h) > max_dim:
        scale = max_dim / float(max(w, h))
        alpha = alpha.resize((max(1, round(w * scale)), max(1, round(h * scale))), Image.LANCZOS)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    Image.merge("LA", (Image.new("L", alpha.size, 255), alpha)).save(out_png, format="PNG", optimize=True)
