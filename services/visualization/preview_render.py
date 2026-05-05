"""
Render a single tomogram MIP with candidate-circle overlays as a PNG.

Pure function module: takes paths and arrays in, writes one PNG, returns metadata.
No state, no project knowledge — that lives in preview_orchestrator.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger(__name__)


def _autocontrast(img: np.ndarray, lo_pct: float = 2.0, hi_pct: float = 99.5) -> np.ndarray:
    """Percentile clip + linear stretch to [0, 1]. Robust against outlier voxels."""
    finite = img[np.isfinite(img)]
    if finite.size == 0:
        return np.zeros_like(img, dtype=np.float32)
    lo, hi = np.percentile(finite, [lo_pct, hi_pct])
    if hi <= lo:
        hi = lo + 1.0
    out = (img.astype(np.float32) - lo) / (hi - lo)
    return np.clip(out, 0.0, 1.0)


def _downscale(img: np.ndarray, max_edge: int) -> Tuple[np.ndarray, float]:
    """Decimate (block-mean) to fit max_edge. Returns (img, scale) where scale = out / in."""
    h, w = img.shape
    longest = max(h, w)
    if longest <= max_edge:
        return img, 1.0
    factor = int(np.ceil(longest / max_edge))
    new_h = (h // factor) * factor
    new_w = (w // factor) * factor
    cropped = img[:new_h, :new_w]
    block = cropped.reshape(new_h // factor, factor, new_w // factor, factor).mean(axis=(1, 3))
    return block, 1.0 / factor


def _slab_indices(z_dim: int, mode: str) -> Tuple[int, int]:
    """Return (z_lo, z_hi) for the requested slab mode (half-open).

    Defaults are tuned for thick cryo-ET tomograms where a full Z-MIP washes
    out particles in noise. A central slab keeps SNR high while still letting
    you see most picks.
    """
    if mode == "mip":
        return 0, z_dim
    if mode == "central":
        # Middle 1/3 of Z. For a 512-slice tomo this is the central ~170 slices.
        third = max(z_dim // 3, 1)
        lo = (z_dim - third) // 2
        return lo, lo + third
    if mode == "thin":
        # ±15 slices around center, capped to volume bounds.
        half = min(15, z_dim // 2)
        mid = z_dim // 2
        return max(mid - half, 0), min(mid + half, z_dim)
    raise ValueError(f"unknown slab mode: {mode!r}")


def _project_slab(vol: np.ndarray, z_lo: int, z_hi: int) -> np.ndarray:
    """MIP across [z_lo, z_hi). mrcfile delivers volumes as (Z, Y, X)."""
    return np.max(vol[z_lo:z_hi], axis=0)


def render_candidate_preview(
    tomo_path: Path,
    coords_xyz_px: np.ndarray,
    radius_px: float,
    out_png: Path,
    scores: Optional[Sequence[float]] = None,
    max_edge_px: int = 1400,
    title: Optional[str] = None,
    slab_mode: str = "central",
) -> dict:
    """Render a Z-projection of the tomogram with viridis-colored candidate circles.

    Args:
        tomo_path: Path to a tomogram MRC. Memory-mapped — full volume not loaded.
        coords_xyz_px: (N, 3) candidate coords in tomogram pixel space (XYZ order,
            origin at top-left, matching IMOD coords as produced by imod_vis).
        radius_px: Circle radius in tomogram pixels (unscaled — preview-side scaling
            is applied internally to match the downscaled image).
        out_png: Destination PNG path. Parent dir is created.
        scores: Optional (N,) per-candidate score. If given, circles are colored by
            viridis(score normalized 0–1). If None, all circles are uniform yellow.
        max_edge_px: Longest output edge in pixels.
        title: Optional caption rendered in the corner.
        slab_mode: "central" (middle third of Z, default — best SNR/visibility),
            "thin" (±15 slices around center) or "mip" (full Z, washes particles).

    Returns:
        dict with keys: png_path (str), n_candidates (int), score_range (tuple|None),
        dims (tuple of int, the rendered image dims as (h, w)), slab_mode (str),
        slab_z_range (tuple of int).
    """
    # Thread-safe OO API: avoid pyplot's global figure registry so concurrent
    # renders from run.io_bound's thread pool don't trample each other.
    import mrcfile
    import matplotlib
    matplotlib.use("Agg")
    from matplotlib.figure import Figure
    from matplotlib.patches import Circle
    from matplotlib import cm

    tomo_path = Path(tomo_path)
    out_png = Path(out_png)
    out_png.parent.mkdir(parents=True, exist_ok=True)

    with mrcfile.mmap(str(tomo_path), mode="r", permissive=True) as m:
        # mrcfile orders axes (z, y, x). Coerce to that — most cryo-ET tomograms
        # come out this way but some legacy IMOD files have permuted headers.
        vol = m.data
        if vol.ndim != 3:
            raise ValueError(f"{tomo_path} is not a 3D volume (ndim={vol.ndim})")
        z, y, x = vol.shape
        z_lo, z_hi = _slab_indices(z, slab_mode)
        mip = _project_slab(np.asarray(vol), z_lo, z_hi)

    img = _autocontrast(mip)
    img_small, scale = _downscale(img, max_edge_px)
    h_out, w_out = img_small.shape

    if coords_xyz_px.size:
        xs = coords_xyz_px[:, 0].astype(float) * scale
        ys = coords_xyz_px[:, 1].astype(float) * scale
        rad_out = max(radius_px * scale, 1.5)
    else:
        xs = ys = np.array([])
        rad_out = max(radius_px * scale, 1.5)

    if scores is not None and len(scores) > 0:
        s = np.asarray(scores, dtype=float)
        s_min, s_max = float(np.nanmin(s)), float(np.nanmax(s))
        if s_max > s_min:
            s_norm = (s - s_min) / (s_max - s_min)
        else:
            s_norm = np.full_like(s, 0.5)
        score_range: Optional[tuple] = (s_min, s_max)
        colors = cm.viridis(s_norm)
    else:
        score_range = None
        colors = [(1.0, 0.85, 0.0, 0.9)] * len(xs)

    dpi = 100
    fig_w = w_out / dpi
    fig_h = h_out / dpi
    fig = Figure(figsize=(fig_w, fig_h), dpi=dpi)
    ax = fig.add_subplot(1, 1, 1)
    ax.imshow(img_small, cmap="gray", origin="upper", interpolation="nearest")
    for cx, cy, color in zip(xs, ys, colors):
        if not (0 <= cx < w_out and 0 <= cy < h_out):
            continue
        ax.add_patch(Circle((cx, cy), rad_out, fill=False, edgecolor=color, linewidth=1.0))

    ax.set_xlim(0, w_out)
    ax.set_ylim(h_out, 0)
    ax.set_axis_off()

    if title:
        ax.text(
            0.01, 0.99, title, transform=ax.transAxes, ha="left", va="top",
            fontsize=9, color="white",
            bbox=dict(facecolor="black", alpha=0.6, pad=3, edgecolor="none"),
        )

    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    fig.savefig(out_png, dpi=dpi, bbox_inches=None, pad_inches=0)

    return {
        "png_path": str(out_png),
        "n_candidates": int(coords_xyz_px.shape[0]) if coords_xyz_px.size else 0,
        "score_range": score_range,
        "dims": (h_out, w_out),
        "slab_mode": slab_mode,
        "slab_z_range": (z_lo, z_hi),
    }


def is_preview_stale(png: Path, sources: Sequence[Path]) -> bool:
    """True if PNG missing or older than any source file."""
    if not png.exists():
        return True
    png_mtime = png.stat().st_mtime
    for src in sources:
        if src.exists() and src.stat().st_mtime > png_mtime:
            return True
    return False


# Allow `python -m services.visualization.preview_render <tomo> <out.png>` for ad-hoc test
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("usage: preview_render.py <tomo.mrc> <out.png> [radius_px]", file=sys.stderr)
        sys.exit(2)
    radius = float(sys.argv[3]) if len(sys.argv) > 3 else 20.0
    info = render_candidate_preview(
        tomo_path=Path(sys.argv[1]),
        coords_xyz_px=np.empty((0, 3)),
        radius_px=radius,
        out_png=Path(sys.argv[2]),
    )
    print(info)
