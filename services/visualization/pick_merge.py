"""Merge + radius-dedup for the curation workbench's pick lists.

Two SEPARATE operations (the user drives both; nothing dedups automatically):

- **merge** — a pure UNION of 2+ lists' centered-Å coordinates into one `merged`
  list. Sources are concatenated in PRIORITY order (curated/human lists before
  machine `auto`), and that row order is the *only* thing encoding "manual wins":
  the greedy dedup below keeps earlier rows, so a manual pick survives over an
  `auto` pick it clashes with. No merge-time dedup — the union can (and is meant
  to) contain clashers, surfaced to the user afterwards.

- **dedup** — greedy radius dedup the user triggers on a list (typically a merged
  one) at a CHOSEN radius, after seeing how many picks clash at that radius.
  Keep-first walk: a pick is dropped iff it lies within `radius` Å of an
  already-kept (earlier = higher-priority) pick.

numpy/pandas/starfile are hard deps (top-level, like the rest of
services/visualization) — this is compile-checked in Claude's bare venv and
runtime-tested in the module-loaded app.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd
import starfile

from services.visualization.coords import CENTERED_COLS, centered_angst_dataframe

logger = logging.getLogger(__name__)

# Lower number = higher priority = earlier in the merged star = wins a clash on
# dedup. Human/curated placements beat machine candidates. Unknown types sort last.
_TYPE_PRIORITY = {"merged": 0, "manual": 1, "imported": 2, "filtered": 3, "auto": 4}


def type_priority(list_type: str) -> int:
    """Dedup priority for a list type (lower wins). `manual` beats `auto`."""
    return _TYPE_PRIORITY.get(str(list_type), 9)


def _particles_block(star_path: Path) -> Optional[tuple[str, pd.DataFrame]]:
    """(block_key, DataFrame) of the first block carrying the centered-Å columns."""
    data = starfile.read(Path(star_path), always_dict=True)
    for k, v in data.items():
        if isinstance(v, pd.DataFrame) and all(c in v.columns for c in CENTERED_COLS):
            return k, v
    return None


def read_centered_coords(star_path: Path, tomo_name: str) -> np.ndarray:
    """N×3 centered-Å coords for one tomogram from any star with the centered
    columns (+ optional rlnTomoName filter). Empty (0×3) if none/unreadable."""
    try:
        block = _particles_block(Path(star_path))
    except Exception as e:
        logger.warning("Could not read coords from %s: %s", star_path, e)
        return np.empty((0, 3), dtype=float)
    if block is None:
        return np.empty((0, 3), dtype=float)
    df = block[1]
    if "rlnTomoName" in df.columns:
        df = df[df["rlnTomoName"] == tomo_name]
    if df.empty:
        return np.empty((0, 3), dtype=float)
    return df[CENTERED_COLS].to_numpy(dtype=float)


def merge_lists_to_star(sources: Sequence[dict], tomo_name: str, out_star: Path) -> dict:
    """Union 2+ lists into one centered-Å merged star (NO dedup).

    `sources` = ``[{"path": <star>, "priority": <int>}, ...]`` — each list's
    centered-Å coords for `tomo_name` are read and concatenated in ascending
    `priority` (so the highest-priority list's rows come first and win later
    dedup). Writes ``rlnTomoName`` + the three centered columns. Returns
    ``{count, out_star, n_sources}``.
    """
    ordered = sorted(sources, key=lambda s: s.get("priority", 9))
    arrs = []
    for s in ordered:
        c = read_centered_coords(Path(s["path"]), tomo_name)
        if len(c):
            arrs.append(c)
    merged = np.concatenate(arrs) if arrs else np.empty((0, 3), dtype=float)
    df = centered_angst_dataframe(merged)
    df.insert(0, "rlnTomoName", tomo_name)
    out_star = Path(out_star)
    out_star.parent.mkdir(parents=True, exist_ok=True)
    starfile.write({"particles": df}, out_star, overwrite=True)
    logger.info("Merged %d lists -> %s (%d picks) for %s", len(arrs), out_star, len(df), tomo_name)
    return {"count": int(len(df)), "out_star": str(out_star), "n_sources": int(len(arrs))}


def _greedy_drop_mask(coords: np.ndarray, radius: float) -> np.ndarray:
    """Boolean N: True for picks dropped by greedy keep-first dedup — a pick is
    dropped iff within `radius` of an EARLIER kept pick (so row order = priority).
    O(n²) worst case but n is small (tens–hundreds of picks per tomogram)."""
    coords = np.asarray(coords, dtype=float).reshape(-1, 3)
    n = len(coords)
    dropped = np.zeros(n, dtype=bool)
    if n < 2 or radius <= 0:
        return dropped
    r2 = float(radius) * float(radius)
    for i in range(n):
        if dropped[i]:
            continue
        d = coords[i + 1 :] - coords[i]
        within = (d * d).sum(axis=1) <= r2
        dropped[i + 1 :] |= within
    return dropped


def clash_stats_coords(coords: np.ndarray, radius: float) -> dict:
    """Clash overview at a chosen radius: ``{n_total, n_clashing, n_removed,
    n_after}``. `n_clashing` = picks with ≥1 neighbor within `radius` (the
    "how many clash" the user sees); `n_removed` = what a greedy dedup would
    drop; `n_after` = survivors."""
    coords = np.asarray(coords, dtype=float).reshape(-1, 3)
    n = len(coords)
    if n < 2 or radius <= 0:
        return {"n_total": int(n), "n_clashing": 0, "n_removed": 0, "n_after": int(n)}
    r2 = float(radius) * float(radius)
    # Symmetric "involved in any clash" count. Full pairwise is O(n²) memory;
    # cap it and fall back to the greedy drop count as a proxy for very large n.
    if n <= 2500:
        diff = coords[:, None, :] - coords[None, :, :]
        d2 = (diff * diff).sum(axis=-1)
        np.fill_diagonal(d2, np.inf)
        n_clashing = int((d2 <= r2).any(axis=1).sum())
    else:
        n_clashing = -1  # unknown at this scale; UI shows "≥ n_removed"
    dropped = _greedy_drop_mask(coords, radius)
    n_removed = int(dropped.sum())
    return {
        "n_total": int(n),
        "n_clashing": n_clashing if n_clashing >= 0 else n_removed,
        "n_removed": n_removed,
        "n_after": int(n - n_removed),
    }


def clash_stats_star(star_path: Path, tomo_name: str, radius: float) -> dict:
    """Clash overview for a list's star at `radius` Å (see clash_stats_coords)."""
    return clash_stats_coords(read_centered_coords(Path(star_path), tomo_name), radius)


def deduplicate_star(star_path: Path, tomo_name: str, radius: float, out_star: Optional[Path] = None) -> dict:
    """Greedy radius dedup a centered-Å star in place (or to `out_star`), keeping
    earlier (higher-priority) rows. Preserves all columns + non-particle blocks.
    Returns ``{n_before, n_removed, n_after, out_star}``."""
    block = _particles_block(Path(star_path))
    if block is None:
        raise ValueError(f"No centered-coordinate table in {star_path}")
    key, df = block
    # Dedup only this tomogram's rows; leave any other rlnTomoName rows untouched.
    if "rlnTomoName" in df.columns:
        this_mask = df["rlnTomoName"] == tomo_name
    else:
        this_mask = pd.Series(True, index=df.index)
    this = df[this_mask]
    coords = this[CENTERED_COLS].to_numpy(dtype=float)
    dropped = _greedy_drop_mask(coords, radius)
    keep_idx = this.index[~dropped]
    kept = df.loc[df.index.isin(keep_idx) | (~this_mask)].reset_index(drop=True)

    data = starfile.read(Path(star_path), always_dict=True)
    data[key] = kept
    target = Path(out_star) if out_star else Path(star_path)
    starfile.write(data, target, overwrite=True)
    n_removed = int(dropped.sum())
    logger.info("Dedup %s at %.1f A: removed %d, kept %d", star_path, radius, n_removed, len(kept))
    return {
        "n_before": int(len(this)),
        "n_removed": n_removed,
        "n_after": int(len(this) - n_removed),
        "out_star": str(target),
    }
