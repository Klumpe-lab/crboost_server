"""The probability volumes easymode and Ais write: their encoding, whether a file is a finished
volume, and what the views show of each (two overlay masks and the fraction of the volume).

The Segment job runs `summarize_volume` on the compute node right after a tomogram is segmented,
while the volume is still in the page cache; an import of volumes made elsewhere runs the same
function in a worker, with the encoding `infer_full_scale` read off the data (or the import named).
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


def infer_full_scale(dtype, vmin: float, vmax: float, *, mask_if_binary: bool = True) -> tuple[float | None, str]:
    """The stored value of p = 1 in a volume made elsewhere, from its data type and the range of
    its values, with how it was read: integers up to 1 are a mask (MemBrain's segmentation; with
    `mask_if_binary` False, a volume of easymode's or Ais's with next to nothing in it), larger int8
    is easymode's p × 127, unsigned integers up to 255 Ais's p × 255, floats within 0 … 1 are p.
    Anything else is (None, why): a label map with more labels, scores or logits have no p to show,
    and only the import can say what they are."""
    dt = np.dtype(dtype)
    if dt.kind in "iu":
        if vmin < 0:
            return None, f"{dt} with negative values ({vmin:g})"
        if vmax <= 1 and mask_if_binary:
            return 1.0, "mask (0/1)"
        if vmax <= 1:
            fs = 127.0 if dt == np.int8 else 255.0
            return fs, f"p × {fs:g}, only 0 and 1 (choose a mask's encoding if it is one)"
        if dt == np.int8:
            return 127.0, "p × 127"
        if dt.kind == "u" and vmax <= 255:
            return 255.0, "p × 255"
        return None, f"{dt} with values up to {vmax:g}"
    if dt.kind == "f":
        if vmin >= 0 and vmax <= 1:
            return 1.0, "p (0–1)"
        return None, f"{dt} from {vmin:.3g} to {vmax:.3g}, not probabilities"
    return None, f"data type {dt}"


def probe_volume(path: Path) -> dict:
    """What an import needs to know about a volume made elsewhere: its grid (z, y, x), data type,
    header voxel size, and the range of its values, which takes a full read. Raises OSError or
    ValueError for a file that is not a complete 3D MRC."""
    import mrcfile

    with mrcfile.mmap(path, mode="r", permissive=True) as m:
        data = m.data
        if data is None or data.ndim != 3:
            raise ValueError("not a complete 3D MRC volume")
        return {
            "shape_zyx": [int(v) for v in data.shape],
            "dtype": str(data.dtype),
            "voxel": float(m.voxel_size.x),
            "min": float(data.min()),
            "max": float(data.max()),
        }


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
    volume: Path,
    *,
    slab_shape_zyx,
    slab_itemsize: int,
    xy_png: Path,
    xz_png: Path,
    max_dim: int = 1024,
    full_scale: float | None = None,
) -> dict:
    """Write `volume`'s two overlay masks and measure it: ``{full_scale, fraction_pct, overlay_xy,
    overlay_xz}``. `full_scale` is the stored value of p = 1; by default the one easymode and Ais
    use for the volume's data type (`full_scale_of`).

    An overlay is p ≥ DISPLAY_THRESHOLD over the planes the tomogram's own slab preview averages
    (`central_slab`, from `slab_shape_zyx` and `slab_itemsize`, the tomogram's shape and data
    type), max-projected so a membrane thinner than the slab still shows, flipped and scaled as the
    slab preview is. It is white with the mask as alpha: the page gives it its colour. The X/Y mask
    covers the central Z planes, the X/Z mask the central Y planes. The fraction is the mean p over
    the whole volume, in percent, with values beyond 0 … full scale counted as 0 and 1."""
    import mrcfile

    nz, ny, nx = (int(v) for v in slab_shape_zyx)
    with mrcfile.mmap(volume, mode="r", permissive=True) as m:
        data = m.data
        if full_scale is None:
            full_scale = full_scale_of(data.dtype)
        threshold = DISPLAY_THRESHOLD * full_scale
        z_lo, z_hi = central_slab(nz, ny * nx * slab_itemsize)
        y_lo, y_hi = central_slab(ny, nz * nx * slab_itemsize)
        xy = data[z_lo:z_hi].max(axis=0) >= threshold
        xz = data[:, y_lo:y_hi, :].max(axis=1) >= threshold
        hi = min(full_scale, np.iinfo(data.dtype).max) if data.dtype.kind in "iu" else full_scale
        clipped = np.clip(data, data.dtype.type(0), data.dtype.type(hi))
        fraction = float(np.mean(clipped, dtype=np.float64)) / full_scale * 100.0
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
