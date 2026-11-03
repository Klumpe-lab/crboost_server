"""Each pick's extracted subtomogram, for the kitchen's "extracted subtomograms" pictures.

The join is the Journey's own (`services/particles/subtomo_link.py`: every Subtomo Extraction and Warp
Export job serving the member's candidate extraction, matched by tomogram and centred Å), so a pick shows
the same subtomogram in both. Pick lists are not joined yet: their picks say so.

Blocking (star reads, stats): call in a thread.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from services.sta_kitchen.models import MemberKind, PoolMember
from services.sta_kitchen.snapshots import ProjectView
from services.particles.analytics_store import job_dir_of
from services.particles.coords import CENTERED_COLS
from services.particles.records import file_mtime
from services.particles.subtomo_link import build_pick_to_mrcs_index, coord_keys, subtomo_particles_stars

NOT_EXTRACTED = "not extracted: no subtomogram of this pick in the extraction jobs of its candidates"
LIST_NOT_JOINED = "a pick list's extraction is not shown here yet"


def pictures_stamp(view: ProjectView, member: PoolMember) -> tuple:
    """What a member's pictures are joined from: the extraction stars serving it, by mtime."""
    if member.kind != MemberKind.TM:
        return ()
    jd = job_dir_of(view.state, view.path, member.ref)
    if jd is None:
        return ()
    return tuple((str(s), file_mtime(s)) for s in subtomo_particles_stars(view.state, view.path, jd))


def member_pictures(view: ProjectView, member: PoolMember, star: str) -> dict[int, tuple[str, str, tuple | None]]:
    """{row of the member's star: (the extraction's particles.star, the subtomogram, its visible tilts)}."""
    from services.sta_kitchen.table import _particle_block

    if member.kind != MemberKind.TM:
        return {}
    jd = job_dir_of(view.state, view.path, member.ref)
    index = build_pick_to_mrcs_index(jd, view.state, view.path) if jd is not None else {}
    if not index:
        return {}
    df = _particle_block(Path(star))
    tomos = df["rlnTomoName"].astype(str).to_numpy()
    xyz = df[CENTERED_COLS].to_numpy(dtype=float)
    rows = np.flatnonzero(np.isfinite(xyz).all(axis=1))
    out: dict[int, tuple[str, str, tuple | None]] = {}
    for i, key in zip(rows.tolist(), coord_keys(xyz[rows]).tolist(), strict=True):
        hit = index.get((tomos[i], tuple(key)))
        if hit is not None:
            visible = tuple(hit["visible_frames"]) if hit.get("visible_frames") else None
            out[i] = (str(Path(hit["src_job_dir"]) / "particles.star"), str(hit["mrcs"]), visible)
    return out
