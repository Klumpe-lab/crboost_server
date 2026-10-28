"""The Doctor: what in a pool needs a human look, as markers. A marker states its rule, the value it
measured, and that it drops nothing; the rule texts are one table (`RULES`).

Blocking (it reads the members' template-match settings from their projects): call in a thread.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from services.kitchen.pool import UNKNOWN_ACQ, PoolTable, Selection

DROPS_NOTHING = "Markers point a human at something; nothing is dropped by them."
# The template-match settings that change what a σ means; throughput settings are left out.
TM_FIELDS = (
    "template_path",
    "mask_path",
    "angular_search",
    "symmetry",
    "defocus_weight",
    "dose_weight",
    "spectral_whitening",
    "random_phase_correction",
    "non_spherical_mask",
    "bandpass_filter",
)

RULES: dict[str, str] = {
    "frames_conflict": (
        "One acquisition reconstructed in frames that cannot share picks (another alignment, geometry or hand). "
        "A set takes one frame per acquisition: Make set waits until one is chosen here."
    ),
    "frames_pooled": (
        "One acquisition in several frames of one alignment reconstructed alike: their picks are pooled at Make set "
        "and clashes within Ø/2 deduplicated, a hand pick winning."
    ),
    "acq_unknown": (
        "No raw filename or mdoc time on record for these tilt series, so a duplicate of them in another frame "
        "cannot be recognised."
    ),
    "apix_unknown": "The reconstruction's pixel size is unknown: these picks have no voxel position, dot or cutout.",
    "no_cutout": "Picks without a geometry or outside their volume: no cutout can be cut for them.",
    "unscored": "Rows without σ (hand-picked or imported): a σ range keeps them unless its coverage line says drop.",
    "mixed_levels": (
        "The kept rows mix coordinates and extracted particles: Make set offers a coordinates set (extract later) "
        "or only the extracted rows."
    ),
    "tm_settings": (
        "Members were template-matched with different settings, so one σ cut does not mean the same on both."
    ),
    "member_error": "A member's table could not be built: its rows are missing from the pool.",
    "orphans": "Verdicts on particles no member has any more (its source was re-run): kept, applied to nothing.",
}


@dataclass
class Marker:
    key: str
    value: str  # what was measured, in words
    severity: str = "warn"  # block | warn | info
    items: list[dict[str, Any]] = field(default_factory=list)  # per-acquisition detail, for actions

    @property
    def text(self) -> str:
        return f"{self.value}. {RULES[self.key]} {DROPS_NOTHING}"


def _tm_settings(member) -> dict[str, str] | None:
    from services.kitchen.snapshots import project_view
    from services.particles.analytics_store import tm_instance_for_ce

    if member.kind.value != "tm":
        return None
    state = project_view(Path(member.project_path)).state
    tm = tm_instance_for_ce(state, member.ref)
    jm = state.jobs.get(tm) if tm else None
    if jm is None:
        return None
    out = {}
    for f in TM_FIELDS:
        v = getattr(jm, f, None)
        v = getattr(v, "value", v)
        out[f] = Path(str(v)).name if f.endswith("_path") and v else str(v)
    return out


def diagnose(lp, sel: Selection | None) -> list[Marker]:
    table: PoolTable | None = lp.table
    out: list[Marker] = []
    for key, st in lp.members.items():
        if st.state == "error":
            m = lp.pool.member(key)
            out.append(Marker("member_error", f"{m.project_label if m else key}: {st.error}", "block"))
    if table is None:
        return out
    frames = table.frames
    for acq, groups in table.conflicts.items():
        chosen = lp.pool.frame_choice.get(acq)
        items = [
            {
                "acq": acq,
                "frame": frames[g[0]]["key"],
                "tomo": frames[g[0]]["tomo"],
                "project": frames[g[0]]["project"],
                "rows": int(np.isin(table.cat["frame"], g).sum()),
                "chosen": chosen == frames[g[0]]["key"],
            }
            for g in groups
        ]
        out.append(
            Marker(
                "frames_conflict",
                f"{frames[groups[0][0]]['tomo']}: {len(groups)} frames" + (" (one chosen)" if chosen else ""),
                "info" if chosen else "block",
                items,
            )
        )
    from services.kitchen.export import pooled_groups

    heads = pooled_groups(table)
    pooled = {}
    for i, h in enumerate(heads):
        if h != i:
            pooled.setdefault(int(h), []).append(i)
    if pooled:
        out.append(Marker("frames_pooled", f"{len(pooled)} acquisition(s) in poolable frames", "info"))
    unknown = [i for i, fr in enumerate(frames) if fr.get("acq") == UNKNOWN_ACQ]
    if unknown:
        out.append(Marker("acq_unknown", f"{len(unknown)} of {len(frames)} tomograms", "warn"))
    no_apix = [fr["tomo"] for fr in frames if not fr.get("apix")]
    if no_apix:
        out.append(Marker("apix_unknown", f"{len(no_apix)} tomogram(s), e.g. {no_apix[0]}", "block"))
    face = table.num["face_a"]
    outside = int(((table.cat["frame"] < 0) | np.isnan(table.num["x_px"]) | (face < 0)).sum())
    if outside:
        out.append(Marker("no_cutout", f"{outside:,} picks", "warn"))
    unscored = int(np.isnan(table.num["sigma"]).sum())
    if unscored:
        out.append(Marker("unscored", f"{unscored:,} of {table.n:,} rows", "info"))
    if sel is not None:
        kept = sel.final
        ext = table.cat["extracted"] > 0
        n_ext, n_kept = int((kept & ext).sum()), int(kept.sum())
        if 0 < n_ext < n_kept:
            out.append(Marker("mixed_levels", f"{n_ext:,} of {n_kept:,} kept rows extracted", "warn"))
    settings = {m.key: _tm_settings(m) for m in lp.pool.members}
    settings = {k: v for k, v in settings.items() if v}
    if len(settings) > 1:
        differ = [f for f in TM_FIELDS if len({v[f] for v in settings.values()}) > 1]
        if differ:
            out.append(Marker("tm_settings", "differ in " + ", ".join(differ), "warn"))
    orphans = sum(1 for u in lp.verdicts if table.row_of(u) is None)
    if orphans:
        out.append(Marker("orphans", f"{orphans:,} verdict(s)", "info"))
    return out
