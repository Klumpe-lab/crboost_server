"""D6: the curation a member already carries becomes the pool's verdicts, once.

A template-match member's curation is each of its subtomo extractions' ``particles_filtered.star``:
an extracted particle missing from it was dropped there. A pick list's is ``<stem>_filtered.star``
beside the list: a pick missing from it was dropped. Matched by tomogram and centred Å rounded to
0.1 Å, as the extraction join is. Blocking (star reads): call in a thread.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from services.sta_kitchen.pool import PoolTable
from services.sta_kitchen.table import COORD_DECIMALS, _particle_block
from services.particles import analytics
from services.particles.coords import CENTERED_COLS


def _key_list(tomos, x, y, z) -> list[tuple]:
    """(tomogram, x, y, z in 0.1 Å) per row: how a pick is named across files."""
    s = 10**COORD_DECIMALS
    coords = (np.round(np.asarray(v, dtype=float) * s).astype(np.int64).tolist() for v in (x, y, z))
    return list(zip((str(t) for t in tomos), *coords, strict=True))


def seed_drops(table: PoolTable, member_key: str) -> dict[str, list[str]]:
    """{curation file: uids it dropped} for one member of `table`."""
    meta = table.member_meta[member_key]
    sl = table.member_slices[member_key]
    rows = np.arange(sl.start, sl.stop)
    tomo_names = np.asarray(table.categories["tomo"], dtype=object)[table.cat["tomo"][rows]]
    row_keys = _key_list(tomo_names, *(table.num[c][rows] for c in ("x_a", "y_a", "z_a")))
    out: dict[str, list[str]] = {}
    extracted = table.categories["extracted"]
    if meta["source"] == "tm":
        for job in meta.get("subtomo") or []:
            filtered = Path(job["star"]).with_name("particles_filtered.star")
            if not filtered.exists():
                continue
            kept = _star_keys(filtered)
            code = extracted.index(job["label"]) if job["label"] in extracted else -1
            on_job = table.cat["extracted"][rows] == code
            out[str(filtered)] = [
                table.uid_hex(int(r)) for r, k, e in zip(rows, row_keys, on_job, strict=True) if e and k not in kept
            ]
    else:
        star = Path(meta["star"])
        filtered = star.with_name(f"{star.stem}_filtered.star")
        if filtered.exists():
            kept = _star_keys(filtered)
            out[str(filtered)] = [table.uid_hex(int(r)) for r, k in zip(rows, row_keys, strict=True) if k not in kept]
    return out


def _star_keys(path: Path) -> set[tuple]:
    df = _particle_block(path)
    return set(_key_list(df[analytics.TOMO_COL], *(df[c] for c in CENTERED_COLS)))
