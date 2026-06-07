"""Recon-sourced pick cutouts for the curation workbench.

The subtomo gallery's cutout atlas (`preview_render.render_pick_cutouts_atlas`)
is built from EXTRACTED subtomos — so a pick list that was never subtomo-
extracted (a manual list placed in ArtiaX, an imported `.coords`, the raw auto
candidates before extraction) has no cutouts to show. This module builds the
SAME sprite-atlas PNG + index JSON directly from the binned reconstruction MRC:
one central-Z-slab crop per pick. The index schema is byte-identical to
`render_pick_cutouts_atlas`, so `_read_atlas_index` / `_render_gallery_body`
(and the dot↔cutout hover bridge) consume it unchanged for ANY list.

Coordinates are picks.json VOXEL coords (x, y, z) in the binned tomogram space
— the same space the recon slab PNGs are rendered from, so a tile and its dot
agree. Normalization is tomogram-wide (one 1-99 percentile clip for every tile),
matching the subtomo atlas so pure-noise tiles stay uniformly grey instead of
being stretched to look like low-SNR particles.

numpy is a hard dep (top-level, like `preview_render`); mrcfile/PIL are imported
lazily and degrade to None if absent — so this runs in the module-loaded app
env, not Claude's bare venv.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def _crop_slab(data, x: int, y: int, z: int, half_box: int, half_slab: int) -> Optional[np.ndarray]:
    """Mean over a Z slab of a box centered at voxel (x, y, z) → 2D float32 (y, x).

    Edge-clamped exactly like `extract_pick_subvolume`: picks near a border get a
    smaller window rather than failing. Returns None if the clamp leaves an empty
    box or the slab is all-NaN."""
    nz, ny, nx = data.shape
    x0 = max(0, int(x) - half_box)
    x1 = min(nx, int(x) + half_box)
    y0 = max(0, int(y) - half_box)
    y1 = min(ny, int(y) + half_box)
    z0 = max(0, int(z) - half_slab)
    z1 = min(nz, int(z) + half_slab + 1)
    if x1 <= x0 or y1 <= y0 or z1 <= z0:
        return None
    sub = np.array(data[z0:z1, y0:y1, x0:x1], dtype=np.float32, copy=True)
    slab = sub.mean(axis=0)  # (y, x)
    if not np.isfinite(slab).any():
        return None
    return slab


def _sample_norm_bounds(data, picks: list, half_box: int, half_slab: int, sample_n: int = 12) -> Optional[tuple]:
    """Pool pixels from a stratified pick sample → shared (lo, hi) 1-99 clip.

    Stratified across the (score-sorted) pick order so the bounds aren't biased
    toward only-particles or only-noise — same discipline as the subtomo atlas's
    `_sample_global_norm_bounds`. Returns None if nothing read cleanly."""
    if not picks:
        return None
    if len(picks) <= sample_n:
        chosen = picks
    else:
        step = len(picks) / sample_n
        chosen = [picks[int(i * step)] for i in range(sample_n)]
    pooled: list = []
    for p in chosen:
        slab = _crop_slab(data, p.get("x", 0), p.get("y", 0), p.get("z", 0), half_box, half_slab)
        if slab is not None:
            pooled.append(slab.ravel())
    if not pooled:
        return None
    flat = np.concatenate(pooled)
    lo = float(np.percentile(flat, 1.0))
    hi = float(np.percentile(flat, 99.0))
    if hi <= lo:
        hi = lo + 1.0
    return (lo, hi)


def render_recon_cutouts_atlas(
    recon_mrc: Path,
    picks: list,
    out_atlas_path: Path,
    out_index_path: Path,
    *,
    box_px: int = 48,
    slab_px: int = 5,
    tile_px: int = 192,
    cols: int = 8,
) -> Optional[dict]:
    """Build a sprite-atlas PNG + index JSON of per-pick recon cutouts.

    `picks` is a list of voxel-coord dicts `{x, y, z, ...}` in picks.json order.
    Each tile is the mean over a `slab_px`-thick Z slab of a `box_px` box
    centered on the pick, normalized tomogram-wide and resized to `tile_px`.

    Tiles are keyed by ENUMERATE position (= picks.json order = the dots'
    `data-pick-idx`), identical to `render_pick_cutouts_atlas`, so the gallery's
    tile grid and the canvas dots cross-link with no change.

    Returns a metadata dict (atlas/index paths, n_ok, failures, norm_bounds) or
    None if the recon can't be read or every pick failed (an index JSON is still
    written in the all-fail case so the caller can see why)."""
    try:
        import mrcfile
        from PIL import Image
    except ImportError as e:
        logger.warning("Recon cutout deps unavailable: %s", e)
        return None

    recon_mrc = Path(recon_mrc)
    out_atlas_path = Path(out_atlas_path)
    out_index_path = Path(out_index_path)
    out_atlas_path.parent.mkdir(parents=True, exist_ok=True)

    n = len(picks)
    if n == 0 or not recon_mrc.exists():
        return None

    half_box = max(1, box_px // 2)
    half_slab = max(0, slab_px // 2)
    rows = (n + cols - 1) // cols
    atlas_w = cols * tile_px
    atlas_h = rows * tile_px
    index_entries: dict[str, list] = {}
    failures: list[dict] = []
    n_ok = 0

    try:
        with mrcfile.mmap(str(recon_mrc), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                logger.warning("Recon MRC not 3D: %s", recon_mrc)
                return None
            norm = _sample_norm_bounds(data, picks, half_box, half_slab)
            lo, hi = norm if norm else (0.0, 1.0)

            atlas = np.zeros((atlas_h, atlas_w), dtype=np.uint8)
            for i, p in enumerate(picks):
                slab = _crop_slab(data, p.get("x", 0), p.get("y", 0), p.get("z", 0), half_box, half_slab)
                if slab is None:
                    failures.append({"i": i, "reason": "crop out of bounds"})
                    continue
                clipped = np.clip((slab - lo) / (hi - lo), 0.0, 1.0)
                u8 = (clipped * 255.0).astype(np.uint8)
                img = Image.fromarray(u8, mode="L").resize((tile_px, tile_px), Image.LANCZOS)
                r, c = divmod(i, cols)
                atlas[r * tile_px : (r + 1) * tile_px, c * tile_px : (c + 1) * tile_px] = np.array(img, dtype=np.uint8)
                index_entries[str(i)] = [r, c]
                n_ok += 1
    except Exception as e:
        logger.warning("Recon cutout atlas failed for %s: %s", recon_mrc, e)
        return None

    if n_ok == 0:
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
        "norm_lo": float(lo),
        "norm_hi": float(hi),
        "source": "recon",
    }
    out_index_path.write_text(json.dumps(payload))
    return {
        "atlas_path": str(out_atlas_path),
        "index_path": str(out_index_path),
        "n_ok": n_ok,
        "n_total": n,
        "failures": failures,
        "norm_bounds": (lo, hi),
    }
