"""
Per-tomogram pick data writer for the candidate-preview UI.

The actual plotting now happens client-side in Plotly (responsive layout, hover
tooltips, image overlays) — this module just emits the per-tomogram JSON that
the UI fetches when a tomogram is selected. Pure metadata, no volume reads,
no matplotlib.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def write_picks_data(
    pick_coords_xyz: np.ndarray,
    scores: Optional[np.ndarray],
    score_field: Optional[str],
    tomo_dims_xyz: tuple,
    out_path: Path,
) -> dict:
    """Write per-tomogram picks.json: [{i, x, y, z, score?, z_pct?, nn_px?}, ...].

    Sorted score-descending so the UI can take "best K" / "worst K" slices
    without re-sorting client-side. Per-pick z-percentile and nearest-neighbor
    distance are precomputed here so the hover-details panel can read them
    cheaply on every mouse-move.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if pick_coords_xyz.size and scores is not None and len(scores):
        order = np.argsort(-np.asarray(scores, dtype=float))
        pick_coords_xyz = pick_coords_xyz[order]
        scores = np.asarray(scores, dtype=float)[order]

    n = len(pick_coords_xyz)
    z_pct = _z_percentile(pick_coords_xyz)
    nn_px = _nearest_neighbor_distances(pick_coords_xyz.astype(float))

    picks = []
    for i in range(n):
        pt = pick_coords_xyz[i]
        entry = {"i": int(i), "x": int(pt[0]), "y": int(pt[1]), "z": int(pt[2])}
        if scores is not None and i < len(scores):
            entry["score"] = float(scores[i])
        if z_pct is not None:
            entry["z_pct"] = float(z_pct[i])
        if nn_px is not None and np.isfinite(nn_px[i]):
            entry["nn_px"] = float(nn_px[i])
        picks.append(entry)

    payload = {
        "score_field": score_field,
        "tomo_dims_xyz_px": [int(v) for v in tomo_dims_xyz],
        "n": len(picks),
        "picks": picks,
    }
    out_path.write_text(json.dumps(payload))
    return {"json_path": str(out_path), "n": len(picks)}


def _z_percentile(coords: np.ndarray) -> Optional[np.ndarray]:
    n = len(coords)
    if n <= 1:
        return None
    zs = coords[:, 2].astype(float)
    ranks = np.argsort(np.argsort(zs))
    return (ranks.astype(float) / (n - 1)) * 100.0


def _nearest_neighbor_distances(coords: np.ndarray, chunk: int = 256) -> Optional[np.ndarray]:
    """Per-pick distance to nearest other pick (3D, in pixels).

    O(N^2) but chunked so peak memory stays bounded — for typical hundreds of
    picks per tomo this is sub-millisecond; handles ten-thousand-pick edge
    cases without blowing memory.
    """
    n = len(coords)
    if n <= 1:
        return None
    out = np.empty(n, dtype=float)
    for start in range(0, n, chunk):
        end = min(start + chunk, n)
        block = coords[start:end]
        diff = block[:, None, :] - coords[None, :, :]
        d = np.sqrt(np.einsum("ijk,ijk->ij", diff, diff))
        for j, i in enumerate(range(start, end)):
            d[j, i] = np.inf  # mask self-pair
        out[start:end] = d.min(axis=1)
    return out


def is_output_stale(target: Path, sources) -> bool:
    """True if `target` is missing or older than any source file."""
    if not target.exists():
        return True
    t_mtime = target.stat().st_mtime
    for src in sources:
        if src.exists() and src.stat().st_mtime > t_mtime:
            return True
    return False


# ---------------------------------------------------------------------------
# X/Z slab preview — server-side rendered once per tomogram, cached as PNG.
# This is NOT a Z-MIP / per-pick rendering (which §3.1 forbids); it's the
# X/Z analogue of the WarpTools-emitted top-down PNG, generated once with a
# bounded read budget. Volume bytes touched per tomogram are capped so we
# don't read 2 GB MRCs end-to-end on Lustre.
# ---------------------------------------------------------------------------


def render_xz_slab_preview(
    mrc_path: Path, out_path: Path, *, max_dim: int = 1024, slab_byte_budget: int = 50 * 1024 * 1024
) -> Optional[Path]:
    """Render an X/Z preview PNG by averaging a central Y-slab of the tomogram.

    Reads only the central few Y-slices via mmap so the byte cost is bounded
    regardless of tomogram size. Slab thickness adapts: ~2% of Y, capped so
    that the (slices * Z * X) byte read stays under `slab_byte_budget`.
    Robust-percentile normalized for visibility. Returns the written path,
    or None if the read fails.
    """
    try:
        import mrcfile
        from PIL import Image
    except ImportError as e:
        logger.warning("X/Z preview deps unavailable: %s", e)
        return None

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with mrcfile.mmap(str(mrc_path), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                logger.warning("Skipping X/Z preview, MRC is not 3D: %s", mrc_path)
                return None
            nz, ny, nx = data.shape
            slab_per_slice_bytes = nz * nx * data.dtype.itemsize
            if slab_per_slice_bytes <= 0:
                return None
            max_slices_by_budget = max(1, slab_byte_budget // slab_per_slice_bytes)
            target_slices = max(3, ny // 50)
            n_slices = int(min(max_slices_by_budget, target_slices, ny))
            half = n_slices // 2
            y_lo = max(0, (ny // 2) - half)
            y_hi = min(ny, y_lo + n_slices)
            # Always copy out — see §3.5 mmap view trap. Cast to float32 inside
            # the with-block so the mmap is still valid when np.array is called.
            slab = np.array(data[:, y_lo:y_hi, :], dtype=np.float32, copy=True)
    except Exception as e:
        logger.warning("X/Z slab read failed for %s: %s", mrc_path, e)
        return None

    img2d = slab.mean(axis=1)  # shape (Z, X)
    # Tomograms have a tiny dynamic range with rare extreme outliers — fixed
    # min/max gives a flat washed-out result. 1-99 percentile clip mimics the
    # WarpTools preview's contrast.
    lo = float(np.percentile(img2d, 1.0))
    hi = float(np.percentile(img2d, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    norm = np.clip((img2d - lo) / (hi - lo), 0.0, 1.0)
    u8 = (norm * 255.0).astype(np.uint8)

    # Output orientation: image row 0 is at the *top* of the plot when the UI
    # anchors at y=z_dim. With shape (Z, X), row 0 == z=0; we want z=0 at the
    # bottom of the X/Z scatter (matches the IMOD-up convention). Flip.
    u8 = np.flipud(u8)

    img = Image.fromarray(u8, mode="L")
    h, w = u8.shape
    if max(h, w) > max_dim:
        scale = max_dim / float(max(h, w))
        new_w = max(1, int(round(w * scale)))
        new_h = max(1, int(round(h * scale)))
        img = img.resize((new_w, new_h), Image.LANCZOS)
    img.save(str(out_path), format="PNG", optimize=True)
    return out_path


def render_xy_slab_preview(
    mrc_path: Path, out_path: Path, *, max_dim: int = 1024, slab_byte_budget: int = 50 * 1024 * 1024
) -> Optional[Path]:
    """Render an X/Y top-down preview PNG by averaging a central Z-slab.

    Bytewise mirror of `render_xz_slab_preview` — same percentile clip, same
    byte budget, same uint8 conversion — so template, X/Y slab, X/Z slab, and
    subtomo cutouts all share polarity (low density → dark, high → bright).
    Replaces the WarpTools-rendered tomogram PNG in the dashboard, which used
    WarpTools' own convention and so could be inverted relative to the other
    three renderings. The dashboard's invert toggle then flips all four at
    once via CSS.

    The slab is centered on Z (since the picker's per-pick z is rarely at the
    z-midplane, but the central Z-slab covers the cellular layer for plunge-
    frozen samples). Adapts thickness so the total bytes read stays under
    `slab_byte_budget`.
    """
    try:
        import mrcfile
        from PIL import Image
    except ImportError as e:
        logger.warning("X/Y preview deps unavailable: %s", e)
        return None

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with mrcfile.mmap(str(mrc_path), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                logger.warning("Skipping X/Y preview, MRC is not 3D: %s", mrc_path)
                return None
            nz, ny, nx = data.shape
            slab_per_slice_bytes = ny * nx * data.dtype.itemsize
            if slab_per_slice_bytes <= 0:
                return None
            max_slices_by_budget = max(1, slab_byte_budget // slab_per_slice_bytes)
            target_slices = max(3, nz // 50)
            n_slices = int(min(max_slices_by_budget, target_slices, nz))
            half = n_slices // 2
            z_lo = max(0, (nz // 2) - half)
            z_hi = min(nz, z_lo + n_slices)
            slab = np.array(data[z_lo:z_hi, :, :], dtype=np.float32, copy=True)
    except Exception as e:
        logger.warning("X/Y slab read failed for %s: %s", mrc_path, e)
        return None

    img2d = slab.mean(axis=0)  # shape (Y, X)
    lo = float(np.percentile(img2d, 1.0))
    hi = float(np.percentile(img2d, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    norm = np.clip((img2d - lo) / (hi - lo), 0.0, 1.0)
    u8 = (norm * 255.0).astype(np.uint8)

    # IMOD-up convention: Y=0 at the *bottom* of the displayed image (the
    # dashboard's CSS positions overlay dots with `top: (1.0 - y/y_dim)`).
    # Array row 0 currently == y=0; flipud so row 0 == y=Y_max == top of PNG.
    u8 = np.flipud(u8)

    img = Image.fromarray(u8, mode="L")
    h, w = u8.shape
    if max(h, w) > max_dim:
        scale = max_dim / float(max(h, w))
        new_w = max(1, int(round(w * scale)))
        new_h = max(1, int(round(h * scale)))
        img = img.resize((new_w, new_h), Image.LANCZOS)
    img.save(str(out_path), format="PNG", optimize=True)
    return out_path


# ---------------------------------------------------------------------------
# Subtomo cutout sprite-atlas — turns each pick's per-particle .mrcs (the
# 2D tilt-stack from `relion_tomo_subtomo`) into a small thumbnail; packs
# all thumbnails for one tomogram into a single sprite-sheet PNG that the
# UI displays as a CSS-background-image grid. Atlas-as-sprite-sheet is the
# correct application of the v2 stamp pattern (see ROADMAP §5.2): the
# cutouts come from already-2D extracted data, not from re-projecting the
# tomogram volume — so missing-wedge streaks aren't an issue at this scale.
# ---------------------------------------------------------------------------


def render_pick_cutouts_atlas(
    pick_to_mrcs: list, out_atlas_path: Path, out_index_path: Path, *, tile_px: int = 192, cols: int = 8
) -> Optional[dict]:
    """Build a sprite-atlas PNG of per-pick subtomo thumbnails plus an index JSON.

    `pick_to_mrcs` is a list aligned to the pick order in picks.json; each
    element is either None (no matching subtomo) or a dict with keys
      - mrcs: Path to the per-particle .mrcs
      - visible_frames: list[int] | None  (1 = use that frame for thumbnail)

    Each thumbnail is the per-frame mean (across visible frames only) of the
    .mrcs, normalized to 8-bit greyscale and resized to `tile_px`.

    Normalization is **tomogram-wide**: we sample up to 12 picks, pool their
    mean-frame pixels, and compute a single 1-99 percentile clip applied to
    every tile. The previous per-tile percentile stretched pure-noise tiles
    to fill 0-255 too, making them visually indistinguishable from low-SNR
    particle tiles. With a shared clip, noise tiles stay uniformly grey and
    real-density tiles pop. The bounds are exported in the index JSON so
    sibling renders (template ref tile, noise baseline tiles) can match.

    Returns a metadata dict on success or None if every pick failed. The
    `failures` list inside the metadata names exactly which picks failed and
    why, so the UI can surface "92/96 — 4 failures: <reasons>" instead of
    silently dropping tiles.
    """
    try:
        from PIL import Image
    except ImportError as e:
        logger.warning("Subtomo atlas deps unavailable: %s", e)
        return None

    out_atlas_path = Path(out_atlas_path)
    out_index_path = Path(out_index_path)
    out_atlas_path.parent.mkdir(parents=True, exist_ok=True)

    n = len(pick_to_mrcs)
    if n == 0:
        return None

    norm_bounds = _sample_global_norm_bounds(pick_to_mrcs)

    rows = (n + cols - 1) // cols
    atlas_w = cols * tile_px
    atlas_h = rows * tile_px
    atlas = np.zeros((atlas_h, atlas_w), dtype=np.uint8)
    index_entries: dict[str, list[int]] = {}
    failures: list[dict] = []
    n_ok = 0

    for i, info in enumerate(pick_to_mrcs):
        if info is None:
            failures.append({"i": i, "reason": "no subtomo match"})
            continue
        thumb, err = _render_one_subtomo_thumb(info["mrcs"], info.get("visible_frames"), tile_px, norm_bounds)
        if thumb is None:
            failures.append({"i": i, "reason": err or "render failed", "mrcs": str(info["mrcs"])})
            continue
        r, c = divmod(i, cols)
        y0 = r * tile_px
        x0 = c * tile_px
        atlas[y0 : y0 + tile_px, x0 : x0 + tile_px] = thumb
        index_entries[str(i)] = [r, c]
        n_ok += 1

    if n_ok == 0:
        # Still emit the index JSON so the caller can see why every pick failed.
        out_index_path.write_text(
            json.dumps({"tile_px": tile_px, "cols": cols, "rows": rows, "n_picks": n, "n_ok": 0, "failures": failures})
        )
        return None

    Image.fromarray(atlas, mode="L").save(str(out_atlas_path), format="PNG", optimize=True)

    payload = {
        "tile_px": tile_px,
        "cols": cols,
        "rows": rows,
        "n_picks": n,
        "n_ok": n_ok,
        "atlas_w": atlas_w,
        "atlas_h": atlas_h,
        "index": index_entries,
        "failures": failures,
        "norm_lo": float(norm_bounds[0]) if norm_bounds else None,
        "norm_hi": float(norm_bounds[1]) if norm_bounds else None,
    }
    out_index_path.write_text(json.dumps(payload))
    return {
        "atlas_path": str(out_atlas_path),
        "index_path": str(out_index_path),
        "n_ok": n_ok,
        "n_total": n,
        "failures": failures,
        "norm_bounds": norm_bounds,
    }


def _load_subtomo_mean_frame(
    mrcs_path: Path, visible_frames: Optional[list]
) -> tuple[Optional[np.ndarray], Optional[str]]:
    """Read one .mrcs and return the mean-frame float32 array (pre-normalization).

    Shared by the per-tile thumbnail renderer and the global-bounds sampler so
    both consume the same underlying pixel values.
    """
    try:
        import mrcfile
    except ImportError as e:
        return None, f"deps unavailable: {e}"
    if not Path(mrcs_path).exists():
        return None, "mrcs not on disk"
    try:
        with mrcfile.mmap(str(mrcs_path), mode="r") as m:
            data = m.data
            if data.ndim == 2:
                arr = np.array(data, dtype=np.float32, copy=True)
            elif data.ndim == 3:
                n_frames = data.shape[0]
                if visible_frames and len(visible_frames) >= n_frames:
                    mask = np.array(visible_frames[:n_frames], dtype=bool)
                else:
                    mask = np.ones(n_frames, dtype=bool)
                if not mask.any():
                    # All-zero visible-frames means RELION decided this pick has
                    # no usable tilts. Falling back to all-frames average gives
                    # the user *something* — better than a blank tile.
                    mask = np.ones(n_frames, dtype=bool)
                stack = np.array(data, dtype=np.float32, copy=True)
                arr = stack[mask].mean(axis=0)
            else:
                return None, f"unexpected ndim {data.ndim}"
    except Exception as e:
        logger.warning("Subtomo .mrcs read failed for %s: %s", mrcs_path, e)
        return None, f"mrc read error: {e}"
    if not np.isfinite(arr).any():
        return None, "all-NaN frame mean"
    return arr, None


def _sample_global_norm_bounds(
    pick_to_mrcs: list, sample_n: int = 12, percentile_lo: float = 1.0, percentile_hi: float = 99.0
) -> Optional[tuple[float, float]]:
    """Pool pixels from a stratified sample of picks and return shared (lo, hi).

    Stratification: take picks at evenly spaced indices through the score-sorted
    list. Index 0 is the best pick, last index is the worst — sampling across
    the whole range keeps the bounds from being biased toward only-particles
    (top of list) or only-noise (bottom). Up to `sample_n` valid samples are
    pooled; if none read cleanly, returns None and the per-tile fallback
    re-engages (caller handles).
    """
    if not pick_to_mrcs:
        return None
    valid = [(i, info) for i, info in enumerate(pick_to_mrcs) if info is not None]
    if not valid:
        return None
    if len(valid) <= sample_n:
        chosen = valid
    else:
        step = len(valid) / sample_n
        chosen = [valid[int(i * step)] for i in range(sample_n)]
    pooled: list[np.ndarray] = []
    for _, info in chosen:
        arr, _ = _load_subtomo_mean_frame(info["mrcs"], info.get("visible_frames"))
        if arr is not None:
            pooled.append(arr.ravel())
    if not pooled:
        return None
    flat = np.concatenate(pooled)
    lo = float(np.percentile(flat, percentile_lo))
    hi = float(np.percentile(flat, percentile_hi))
    if hi <= lo:
        hi = lo + 1.0
    return (lo, hi)


def _render_one_subtomo_thumb(
    mrcs_path: Path, visible_frames: Optional[list], tile_px: int, norm_bounds: Optional[tuple[float, float]] = None
) -> tuple[Optional[np.ndarray], Optional[str]]:
    """Mean across visible frames of one .mrcs, normalized + resized to tile_px×tile_px.

    If `norm_bounds=(lo, hi)` is supplied the tile uses that shared clip; the
    per-tile percentile fallback only applies when the global sample failed
    (e.g. every sampled .mrcs unreadable). Returns (thumbnail, None) on
    success or (None, reason) on failure.
    """
    try:
        from PIL import Image
    except ImportError as e:
        return None, f"deps unavailable: {e}"
    arr, err = _load_subtomo_mean_frame(mrcs_path, visible_frames)
    if arr is None:
        return None, err

    if norm_bounds is not None:
        lo, hi = norm_bounds
    else:
        lo = float(np.percentile(arr, 1.0))
        hi = float(np.percentile(arr, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    norm = np.clip((arr - lo) / (hi - lo), 0.0, 1.0)
    u8 = (norm * 255.0).astype(np.uint8)

    img = Image.fromarray(u8, mode="L")
    img = img.resize((tile_px, tile_px), Image.LANCZOS)
    return np.array(img, dtype=np.uint8), None


def extract_pick_subvolume(
    tomo_mrc_path: Path, x_px: int, y_px: int, z_px: int, half_box_px: int, out_path: Path
) -> Optional[Path]:
    """Extract a small 3D cube around a pick from a reconstructed tomogram → MRC.

    Used by the gallery's "open in 3dmod" handoff: rather than the user
    scrolling through a 2 GB tomogram volume in 3dmod to find one pick,
    we slice out a `2*half_box_px` cube centered on (x, y, z) and hand
    that small volume off. 3dmod's slicer then gives the X/Y/X/Z/Y/Z
    triptych directly on the particle.

    Bounds are clamped — picks near the tomogram edge get a smaller box
    rather than failing. The output MRC carries the same voxel_size
    header as the source so 3dmod's measurement tools stay calibrated.

    Returns the written path or None on read failure. Cheap: ~1 MB read
    for a 96-cube box at uint16, mmap so only the requested window is
    touched.
    """
    try:
        import mrcfile
    except ImportError as e:
        logger.warning("Subvolume extract deps unavailable: %s", e)
        return None
    if not Path(tomo_mrc_path).exists():
        return None
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with mrcfile.mmap(str(tomo_mrc_path), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                logger.warning("Source MRC not 3D: %s", tomo_mrc_path)
                return None
            nz, ny, nx = data.shape
            x0 = max(0, int(x_px) - half_box_px)
            x1 = min(nx, int(x_px) + half_box_px)
            y0 = max(0, int(y_px) - half_box_px)
            y1 = min(ny, int(y_px) + half_box_px)
            z0 = max(0, int(z_px) - half_box_px)
            z1 = min(nz, int(z_px) + half_box_px)
            if x1 <= x0 or y1 <= y0 or z1 <= z0:
                return None
            sub = np.array(data[z0:z1, y0:y1, x0:x1], dtype=np.float32, copy=True)
            vx = float(getattr(m.voxel_size, "x", 0.0) or 0.0)
    except Exception as e:
        logger.warning("Subvolume read failed for %s: %s", tomo_mrc_path, e)
        return None
    try:
        with mrcfile.new(str(out_path), overwrite=True) as out:
            out.set_data(sub)
            if vx > 0:
                # voxel_size accepts a scalar broadcast → x=y=z=vx.
                out.voxel_size = vx
    except Exception as e:
        logger.warning("Subvolume write failed for %s: %s", out_path, e)
        return None
    return out_path


def render_template_thumb(template_path: Path, out_path: Path, *, tile_px: int = 192) -> Optional[Path]:
    """Render a central X/Y slice of the template volume as a single thumbnail PNG.

    The template lives in its own intensity regime — `relion_tomo_subtomo`'s
    tilt-mean tiles are conditioned on tomogram density (often µ≈0 σ≈1 after
    CTF-correction), while a templating reference is whatever the user
    generated (PDB-derived synthetic, class-averaged map, etc.). So template
    normalization is self-contained — 1-99 percentile of its own pixels.
    The point is to give the eye a calibration of "this is the shape we're
    looking for"; matching contrast scales would require a forward model of
    the tomogram CTF/dose envelope, which is out of scope.

    Central X/Y slice is the default view because for most templates the
    z-direction is the missing-wedge axis and identifying features (rings,
    knobs, channels) are most legible perpendicular to it.

    Returns the written path or None if the read failed.
    """
    try:
        import mrcfile
        from PIL import Image
    except ImportError as e:
        logger.warning("Template thumb deps unavailable: %s", e)
        return None
    if not Path(template_path).exists():
        return None
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with mrcfile.mmap(str(template_path), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                logger.warning("Template not 3D, skipping thumb: %s", template_path)
                return None
            nz = data.shape[0]
            arr = np.array(data[nz // 2], dtype=np.float32, copy=True)
    except Exception as e:
        logger.warning("Template read failed for %s: %s", template_path, e)
        return None
    if not np.isfinite(arr).any():
        return None
    lo = float(np.percentile(arr, 1.0))
    hi = float(np.percentile(arr, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    norm = np.clip((arr - lo) / (hi - lo), 0.0, 1.0)
    u8 = (norm * 255.0).astype(np.uint8)
    img = Image.fromarray(u8, mode="L").resize((tile_px, tile_px), Image.LANCZOS)
    img.save(str(out_path), format="PNG", optimize=True)
    return out_path
