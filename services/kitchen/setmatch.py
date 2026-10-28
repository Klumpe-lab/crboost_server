"""Particle sets of the home project as filters of a pool (D9): which pool rows a set holds.

A set the kitchen made names its picks (``kitchen_uids.txt``). Any other set is matched by position:
each of its particles to the pick of the home project's members on the same tomogram within MATCH_A Å.
A set's particles that match no pick of the pool are counted and said, never guessed at.

Blocking (star reads, stats): call in a thread.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

import numpy as np

from services.kitchen.export import UIDS_FILE
from services.kitchen.pool import PoolTable
from services.particles.coords import CENTERED_COLS
from services.particles.particle_sets import get_set, list_sets, set_dir
from services.particles.records import file_mtime

# A set's particle and a pool pick are one when this close (Å): positions are written to 0.1 Å or finer,
# and two picks of one source are never this close.
MATCH_A = 0.5


def pool_sets(home: Path, species_id: str) -> list[dict[str, Any]]:
    """The home project's sets of the species, for "+ filter": id, name, particles, level."""
    return [
        {"id": s.id, "name": s.name, "n": s.n_particles, "level": s.level.value}
        for s in list_sets(home)
        if s.species_id == species_id
    ]


def set_stamp(home: Path, set_id: str) -> tuple:
    """What a set's match is computed from: its record and its uid list, by mtime."""
    d = set_dir(home, set_id)
    return file_mtime(d / "set.json"), file_mtime(d / UIDS_FILE)


def set_rows(table: PoolTable, home: Path, set_id: str, home_members: list[str]) -> tuple[np.ndarray, dict[str, Any]]:
    """(mask over the table's rows, {name, n_set, n_matched, how}). `home_members`: the keys of the members
    from the home project, the only picks a set without a uid list can name."""
    from scipy.spatial import cKDTree

    from services.kitchen.table import _particle_block

    pset = get_set(home, set_id)
    if pset is None:
        raise FileNotFoundError(f"no particle set {set_id} in {home.name}")
    info: dict[str, Any] = {"name": pset.name, "n_set": int(pset.n_particles)}
    uids = set_dir(home, set_id) / UIDS_FILE
    if uids.exists():
        wanted = np.array([int(u, 16) for u in uids.read_text().split()], dtype=np.float64)
        mask = np.isin(table.uid, wanted)
        return mask, {**info, "n_set": len(wanted), "n_matched": int(mask.sum()), "how": "uids"}
    star = Path(pset.particles_star)
    df = _particle_block(star if star.is_absolute() else home / star)
    mask = np.zeros(table.n, dtype=bool)
    rows = np.concatenate(
        [np.arange(table.n)[table.member_slices[k]] for k in home_members if k in table.member_slices]
        or [np.zeros(0, dtype=np.int64)]
    )
    names = np.array(table.categories["tomo"], dtype=object)[table.cat["tomo"][rows]] if len(rows) else np.zeros(0)
    pool_xyz = np.column_stack([table.num[c][rows] for c in ("x_a", "y_a", "z_a")]).astype(np.float64)
    set_tomo = df["rlnTomoName"].astype(str).to_numpy()
    set_xyz = df[CENTERED_COLS].to_numpy(dtype=np.float64)
    matched = 0
    for tomo in np.unique(set_tomo):
        here = rows[names == tomo]
        if not len(here):
            continue
        d, j = cKDTree(pool_xyz[names == tomo]).query(set_xyz[set_tomo == tomo], k=1, distance_upper_bound=MATCH_A)
        hit = np.isfinite(d)
        mask[here[j[hit]]] = True
        matched += int(hit.sum())
    return mask, {**info, "n_set": len(df), "n_matched": matched, "how": "positions"}


def mask_payload(mask: np.ndarray) -> str:
    """A row mask for the browser: bits, the first row in the lowest bit of the first byte, base64."""
    return base64.b64encode(np.packbits(mask.astype(bool), bitorder="little").tobytes()).decode()
