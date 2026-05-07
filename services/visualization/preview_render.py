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
    .mrcs, percentile-normalized to 8-bit greyscale and resized to `tile_px`.
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
        thumb, err = _render_one_subtomo_thumb(info["mrcs"], info.get("visible_frames"), tile_px)
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
    }
    out_index_path.write_text(json.dumps(payload))
    return {
        "atlas_path": str(out_atlas_path),
        "index_path": str(out_index_path),
        "n_ok": n_ok,
        "n_total": n,
        "failures": failures,
    }


def _render_one_subtomo_thumb(
    mrcs_path: Path, visible_frames: Optional[list], tile_px: int
) -> tuple[Optional[np.ndarray], Optional[str]]:
    """Mean across visible frames of one .mrcs, normalized + resized to tile_px×tile_px.

    Returns (thumbnail, None) on success or (None, reason) on failure. Reasons
    we surface up to the manifest so the dialog can explain why a tile is
    missing rather than silently dropping it.
    """
    try:
        import mrcfile
        from PIL import Image
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

    lo = float(np.percentile(arr, 1.0))
    hi = float(np.percentile(arr, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    norm = np.clip((arr - lo) / (hi - lo), 0.0, 1.0)
    u8 = (norm * 255.0).astype(np.uint8)

    img = Image.fromarray(u8, mode="L")
    img = img.resize((tile_px, tile_px), Image.LANCZOS)
    return np.array(img, dtype=np.uint8), None
