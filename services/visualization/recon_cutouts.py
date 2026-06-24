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

import logging
from pathlib import Path
from typing import Optional

import numpy as np

from services.visualization.cutout_filters import emit_filtered_atlases

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
    filters: Optional[list] = None,
    apix_hint: Optional[float] = None,
) -> Optional[dict]:
    """Build sprite-atlas PNG(s) + index JSON(s) of per-pick recon cutouts — one
    per display-filter preset (see services/visualization/cutout_filters.py).

    `picks` is a list of voxel-coord dicts `{x, y, z, ...}` in picks.json order.
    Each tile is the mean over a `slab_px`-thick Z slab of a `box_px` box centered
    on the pick. The `raw` variant keeps the base filenames (back-compat); other
    presets get `__<key>` siblings. apix is read from the recon MRC header (the
    reconstruction bin), falling back to `apix_hint`.

    Tiles are keyed by ENUMERATE position (= picks.json order = the dots'
    `data-pick-idx`), identical to `render_pick_cutouts_atlas`, so the gallery's
    tile grid and the canvas dots cross-link with no change.

    Returns a metadata dict (raw atlas/index paths, n_ok, failures, norm_bounds,
    apix, apix_source, `variants` map) or None if the recon can't be read or every
    pick failed (a raw index JSON is still written in the all-fail case)."""
    try:
        import mrcfile
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

    # Read the recon once: preload one 2D float slab per pick (None where the crop
    # falls out of bounds), plus the header pixel size for Å→px conversion.
    frames: list = []
    fail_info: list = []
    apix_header: Optional[float] = None
    try:
        with mrcfile.mmap(str(recon_mrc), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                logger.warning("Recon MRC not 3D: %s", recon_mrc)
                return None
            try:
                apix_header = float(m.voxel_size.x)
            except Exception:
                apix_header = None
            for p in picks:
                slab = _crop_slab(data, p.get("x", 0), p.get("y", 0), p.get("z", 0), half_box, half_slab)
                frames.append(slab)
                fail_info.append(None if slab is not None else {"reason": "crop out of bounds"})
    except Exception as e:
        logger.warning("Recon cutout read failed for %s: %s", recon_mrc, e)
        return None

    return emit_filtered_atlases(
        frames,
        fail_info,
        out_atlas_path,
        out_index_path,
        apix_header=apix_header,
        apix_hint=apix_hint,
        filters=filters,
        tile_px=tile_px,
        cols=cols,
        source="recon",
    )
