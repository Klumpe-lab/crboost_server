"""Extractions whose rows name the pool's picks: the home project's Subtomo extraction and Warp export jobs that
read a particle set of the pool's species, joined to the pool by the uid each row carries (``cbPickUid``).

RELION and Warp write these stars, so the uid column is checked as it is read (`pick_uids.read_uid_column`): rows
without a uid, with a malformed one or sharing one with another row are left out and counted, never matched some
other way. Uids no pick of the pool holds are counted where the join is applied (`pool.apply_uid_joins`).

Blocking: `uid_extractions` stats files (call it in a thread); `read_join` parses a star (call it in a worker).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np

from services.sta_kitchen.snapshots import ProjectView


def uid_extractions(view: ProjectView, species_id: str) -> list[dict[str, Any]]:
    """The jobs of the project `view` shows whose output may name the pool's picks: Subtomo extraction and Warp
    export jobs reading a particle set of `species_id` whose particles.star declares the uid column, oldest job
    first. Each: {job, label, star, set_id, mtime}."""
    from services.models_base import PARTICLE_EXTRACTION_TYPES
    from services.particles.analytics_store import job_dir_of
    from services.particles.particle_sets import list_sets, set_read_by
    from services.particles.pick_uids import UID_COL
    from services.particles.star_columns import star_labels

    out = []
    sets = None
    for iid, jm in view.state.jobs.items():
        if getattr(jm, "job_type", None) not in PARTICLE_EXTRACTION_TYPES:
            continue
        sid = set_read_by(jm)
        if sid is None:
            continue
        if sets is None:
            sets = {s.id: s for s in list_sets(view.path)}
        if sid not in sets or sets[sid].species_id != species_id:
            continue
        jd = job_dir_of(view.state, view.path, iid)
        star = jd / "particles.star" if jd is not None else None
        if star is None or UID_COL not in star_labels(star):
            continue
        out.append(
            {
                "job": iid,
                "label": f"{jd.name} · {sid.split('_', 1)[0]}",
                "star": str(star),
                "set_id": sid,
                "mtime": star.stat().st_mtime_ns,
            }
        )
    return sorted(out, key=lambda e: _job_number(Path(e["star"]).parent.name))


def _job_number(name: str) -> int:
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else -1


def read_join(star: str) -> dict[str, Any]:
    """An extraction's rows as the pool reads them: per row the uid it carries (None where `read_uid_column`
    trusts none), its visible tilts, and what the column check found. Worker-safe."""
    from services.sta_kitchen.table import _particle_block, _visible_count
    from services.particles.pick_uids import UID_COL, read_uid_column

    df = _particle_block(Path(star))
    col = read_uid_column(df)
    if col is None:
        return {"uids": [], "vis": None, "n_rows": len(df), "problems": [f"its rows carry no {UID_COL} any more"]}
    vis = None
    if "rlnTomoVisibleFrames" in df.columns:
        vis = df["rlnTomoVisibleFrames"].astype(str).map(_visible_count).to_numpy(dtype=np.float32)
    return {"uids": col.uids, "vis": vis, "n_rows": len(df), "problems": col.problems()}
