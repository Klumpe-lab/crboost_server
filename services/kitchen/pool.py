"""The pool table and the one authoritative reading of a recipe on it.

The table is the members' column chunks (`services/kitchen/table.py`) put end to end, with the
categorical columns recoded to pool-wide category lists and three identity columns added (member,
project, source). Verdicts and tags are not in it: they change with every key press and travel
apart, so the table's version moves only when a member's files do.

`select` is §4.5 of the kitchen's roadmap; ``ui/kitchen/kitchen.js`` implements the same recipe for
the drag frames, and its count after a release must equal this one's.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from services.kitchen.members import TABLE_SCHEMA
from services.kitchen.models import CategoryFilter, Draft, KitchenPool, RangeFilter, SetFilter
from services.particles.set_ops import greedy_keep
from services.particles.tomo_identity import check_transferable

UNKNOWN_ACQ = "unknown"


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    label: str
    unit: str = ""
    group: str = ""
    # A score-like column: a row without a value passes a range filter unless the user says otherwise.
    keep_missing: bool = True


# Every column a filter can name, in the order "+ filter" lists them.
NUM_COLUMNS: dict[str, ColumnInfo] = {
    "sigma": ColumnInfo("σ", "σ", "Score"),
    "margin": ColumnInfo("σ above the tomogram's FP=1 cutoff", "σ", "Score"),
    "lcc": ColumnInfo("LCC", "", "Score"),
    "rank": ColumnInfo("rank in its tomogram", "", "Score"),
    "depth_a": ColumnInfo("depth from the pick plane", "Å", "Geometry"),
    "face_a": ColumnInfo("distance to a volume face", "Å", "Geometry"),
    "nn_a": ColumnInfo("nearest pick of its source", "Å", "Geometry"),
    "z_pct": ColumnInfo("height in the volume", "%", "Geometry"),
    "slab_a": ColumnInfo("thickness of the tomogram's pick slab", "Å", "Tomogram"),
    "ctf_res": ColumnInfo("CTF fit of the tilt series", "Å", "Tomogram"),
    "n_tilts": ColumnInfo("tilts in the reconstruction", "", "Tomogram"),
    "n_blank": ColumnInfo("blank tilts", "", "Tomogram"),
    "vis_tilts": ColumnInfo("visible tilts of the extracted particle", "", "Extraction"),
    "x_a": ColumnInfo("X (centred)", "Å", "Position"),
    "y_a": ColumnInfo("Y (centred)", "Å", "Position"),
    "z_a": ColumnInfo("Z (centred)", "Å", "Position"),
    "x_px": ColumnInfo("X (voxels)", "px", "Position"),
    "y_px": ColumnInfo("Y (voxels)", "px", "Position"),
    "z_px": ColumnInfo("Z (voxels)", "px", "Position"),
}
CAT_COLUMNS: dict[str, ColumnInfo] = {
    "source": ColumnInfo("source", group="Identity"),
    "project": ColumnInfo("project", group="Identity"),
    "member": ColumnInfo("member", group="Identity"),
    "tomo": ColumnInfo("tomogram", group="Identity"),
    "frame": ColumnInfo("reconstruction", group="Identity"),
    "acq": ColumnInfo("acquisition", group="Identity"),
    "extracted": ColumnInfo("extracted by", group="Extraction"),
    "tm_marker": ColumnInfo("template-match marker", group="Tomogram"),
}
_CHUNK_CATS = ("tomo", "acq", "extracted", "tm_marker")


@dataclass
class PoolTable:
    species_id: str
    n: int
    num: dict[str, np.ndarray]
    cat: dict[str, np.ndarray]
    categories: dict[str, list[str]]
    uid: np.ndarray  # float64, 48-bit ids
    frames: list[dict[str, Any]]  # index = the "frame" code
    members: list[str]
    member_slices: dict[str, slice]
    member_meta: dict[str, dict[str, Any]]
    src_row: np.ndarray  # row in the member's own star
    ext_row: np.ndarray  # row in its extraction's star, -1 when not extracted
    version: str
    # acquisition label -> frame groups (lists of frame codes, each group poolable), for acquisitions
    # seen in more than one group: Make set takes one group per such acquisition.
    conflicts: dict[str, list[list[int]]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)  # member key -> why it has no rows
    _uid_rows: dict[str, int] | None = None

    def uid_hex(self, row: int) -> str:
        return f"{int(self.uid[row]):012x}"

    def row_of(self, uid: str) -> int | None:
        if self._uid_rows is None:
            self._uid_rows = {f"{int(u):012x}": i for i, u in enumerate(self.uid)}
        return self._uid_rows.get(uid)


def read_chunk(npz: Path) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    meta = json.loads(npz.with_suffix(".json").read_text())
    with np.load(npz) as z:
        arrays = {k: z[k] for k in z.files}
    return meta, arrays


def assemble(
    pool: KitchenPool, chunks: list[tuple[str, dict[str, Any], dict[str, np.ndarray]]], *, labels
) -> PoolTable:
    """The pool table from its members' chunks, in member order. `labels(member_key)` gives the
    member's (project label, member label)."""
    num: dict[str, list[np.ndarray]] = {k: [] for k in NUM_COLUMNS}
    cat_names: dict[str, list[str]] = {k: [] for k in CAT_COLUMNS}
    cat: dict[str, list[np.ndarray]] = {k: [] for k in CAT_COLUMNS}
    uid, src_row, ext_row = [], [], []
    frames: list[dict[str, Any]] = []
    frame_index: dict[str, int] = {}
    members, slices, metas = [], {}, {}
    start = 0

    def code_of(col: str, name: str) -> int:
        names = cat_names[col]
        if name not in names:
            names.append(name)
        return names.index(name)

    for key, meta, arr in chunks:
        n = int(meta["n"])
        project_label, member_label = labels(key)
        members.append(key)
        metas[key] = meta
        slices[key] = slice(start, start + n)
        start += n
        for k in NUM_COLUMNS:
            a = arr.get(f"num_{k}")
            num[k].append(a if a is not None else np.full(n, np.nan, dtype=np.float32))
        for k in _CHUNK_CATS:
            local = meta["categories"][k]
            if k == "acq":
                local = [a or UNKNOWN_ACQ for a in local]
            remap = np.array([code_of(k, name) for name in local] or [0], dtype=np.int32)
            cat[k].append(remap[arr[f"cat_{k}"]] if n else np.zeros(0, np.int32))
        acq_local = [a or UNKNOWN_ACQ for a in meta["categories"]["acq"]]
        fcodes = np.full(n, -1, dtype=np.int32)
        local_frames = arr["cat_frame"]
        for fi, fr in enumerate(meta["frames"]):
            fkey = fr["key"] or f"{key}#{fr['tomo']}"
            if fkey not in frame_index:
                frame_index[fkey] = len(frames)
                rows = np.flatnonzero(local_frames == fi)
                acq = acq_local[arr["cat_acq"][rows[0]]] if len(rows) else UNKNOWN_ACQ
                frames.append({**fr, "key": fkey, "project": project_label, "members": [], "acq": acq})
            frames[frame_index[fkey]]["members"].append(key)
            fcodes[local_frames == fi] = frame_index[fkey]
        cat["frame"].append(fcodes)
        for k, value in (("source", meta["source"]), ("project", project_label), ("member", member_label)):
            cat[k].append(np.full(n, code_of(k, value), dtype=np.int32))
        uid.append(arr["uid"])
        src_row.append(arr["src_row"])
        ext_row.append(arr["ext_row"])
    cat_names["frame"] = [f["key"] for f in frames]
    total = start
    table = PoolTable(
        species_id=pool.species_id,
        n=total,
        num={k: _cat_or_empty(v, np.float32) for k, v in num.items()},
        cat={k: _cat_or_empty(v, np.int32) for k, v in cat.items()},
        categories=cat_names,
        uid=_cat_or_empty(uid, np.float64),
        frames=frames,
        members=members,
        member_slices=slices,
        member_meta=metas,
        src_row=_cat_or_empty(src_row, np.int32),
        ext_row=_cat_or_empty(ext_row, np.int32),
        version=pool_version(pool.species_id, [m["fingerprint"] for _k, m, _a in chunks]),
    )
    table.conflicts = frame_conflicts(frames)
    return table


def _cat_or_empty(parts: list[np.ndarray], dtype) -> np.ndarray:
    return np.concatenate(parts).astype(dtype, copy=False) if parts else np.zeros(0, dtype=dtype)


def pool_version(species_id: str, fingerprints: list[str]) -> str:
    raw = f"{TABLE_SCHEMA}|{species_id}|" + "|".join(fingerprints)
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


def frame_conflicts(frames: list[dict[str, Any]]) -> dict[str, list[list[int]]]:
    """Acquisitions whose frames fall into more than one poolable group (`_poolable`: the same alignment, then the
    geometry gate of `check_transferable`, `_poolable`). An unknown acquisition is never equated with anything."""
    by_acq: dict[str, list[int]] = {}
    for i, fr in enumerate(frames):
        if fr.get("acq") and fr["acq"] != UNKNOWN_ACQ:
            by_acq.setdefault(fr["acq"], []).append(i)
    out: dict[str, list[list[int]]] = {}
    for acq, idx in by_acq.items():
        groups: list[list[int]] = []
        for i in idx:
            home = next((g for g in groups if _poolable(frames[g[0]], frames[i])), None)
            if home is None:
                groups.append([i])
            else:
                home.append(i)
        if len(groups) > 1:
            out[acq] = groups
    return out


def _poolable(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Frames of one acquisition share picks only when they are one alignment reconstructed alike: the
    same tilt-series star, geometry and handedness. Any other frame is another frame of reference, and
    its picks never move into this one."""
    if a["key"] == b["key"]:
        return True
    if not a.get("ts_star") or a.get("ts_star") != b.get("ts_star"):
        return False
    report = check_transferable(
        {**a["geometry"], "rlnTomoName": a["tomo"]},
        {**b["geometry"], "rlnTomoName": b["tomo"]},
        hand_a=a.get("hand"),
        hand_b=b.get("hand"),
    )
    return report.ok


@dataclass
class Selection:
    passes: np.ndarray  # every filter and the frame choice
    after_post: np.ndarray  # and the post-ops
    final: np.ndarray  # and the hand verdicts
    removes: list[int]  # per filter: rows passing every other filter and failing this one
    coverage: list[tuple[int, int]]  # per range filter: (rows with a value, rows); (n, n) otherwise
    kept_by_hand: int  # final only because of a keep
    dropped_by_hand: int  # out only because of a drop
    set_aside: int  # on a frame of an acquisition whose other frame was chosen
    notes: list[str] = field(default_factory=list)

    @property
    def n(self) -> int:
        return int(self.final.sum())


VERDICT_CODES = {"keep": 1, "drop": 2}


def verdict_codes(table: PoolTable, verdicts: dict[str, str]) -> np.ndarray:
    out = np.zeros(table.n, dtype=np.int8)
    for uid, v in verdicts.items():
        row = table.row_of(uid)
        if row is not None and v in VERDICT_CODES:
            out[row] = VERDICT_CODES[v]
    return out


def frame_choice_mask(table: PoolTable, choice: dict[str, str]) -> np.ndarray:
    """False on rows of a conflicted acquisition's frames outside the chosen group (named by its
    first frame's key)."""
    ok = np.ones(table.n, dtype=bool)
    fcode = table.cat["frame"]
    for acq, groups in table.conflicts.items():
        chosen = choice.get(acq)
        if not chosen:
            continue
        keep = next((g for g in groups if table.frames[g[0]]["key"] == chosen), None)
        if keep is None:
            continue
        for g in groups:
            if g is not keep:
                ok &= ~np.isin(fcode, g)
    return ok


def filter_mask(table: PoolTable, f) -> tuple[np.ndarray, tuple[int, int], str]:
    n = table.n
    if isinstance(f, RangeFilter):
        v = table.num.get(f.col)
        if v is None:
            return np.ones(n, dtype=bool), (n, n), f"no column {f.col}: the filter passes everything"
        with np.errstate(invalid="ignore"):
            ok = np.ones(n, dtype=bool)
            if f.lo is not None:
                ok &= v >= f.lo
            if f.hi is not None:
                ok &= v <= f.hi
        missing = np.isnan(v)
        ok = np.where(missing, bool(f.keep_missing), ok)
        return ok, (int(n - missing.sum()), n), ""
    if isinstance(f, CategoryFilter):
        codes = table.cat.get(f.col)
        if codes is None:
            return np.ones(n, dtype=bool), (n, n), f"no column {f.col}: the filter passes everything"
        names = table.categories.get(f.col) or []
        allowed = [i for i, name in enumerate(names) if name in set(f.allowed)]
        return np.isin(codes, allowed), (n, n), ""
    if isinstance(f, SetFilter):
        return np.ones(n, dtype=bool), (n, n), f"set filter {f.set_id} is not applied yet"
    return np.ones(n, dtype=bool), (n, n), f"unknown filter {f!r}"


def select(
    table: PoolTable, draft: Draft, verdicts: np.ndarray, frame_choice: dict[str, str] | None = None
) -> Selection:
    """§4.5: filters (a conjunction), the frame choice, then the post-ops per frame on what passes
    (best σ first, NaN last), then the hand verdicts: kept = (passes and not dropped) or kept by hand."""
    masks, coverage, notes = [], [], []
    for f in draft.filters:
        m, cov, note = filter_mask(table, f)
        masks.append(m)
        coverage.append(cov)
        if note:
            notes.append(note)
    choice = frame_choice_mask(table, frame_choice or {})
    passes = choice.copy()
    for m in masks:
        passes &= m
    removes = []
    for i, m in enumerate(masks):
        others = choice.copy()
        for j, o in enumerate(masks):
            if j != i:
                others &= o
        removes.append(int((others & ~m).sum()))
    after = apply_post_ops(table, passes, draft.post.nn_a, draft.post.top_n)
    keep_v, drop_v = verdicts == VERDICT_CODES["keep"], verdicts == VERDICT_CODES["drop"]
    final = (after & ~drop_v) | keep_v
    return Selection(
        passes=passes,
        after_post=after,
        final=final,
        removes=removes,
        coverage=coverage,
        kept_by_hand=int((keep_v & ~after).sum()),
        dropped_by_hand=int((drop_v & after).sum()),
        set_aside=int((~choice).sum()),
        notes=notes,
    )


def apply_post_ops(table: PoolTable, passes: np.ndarray, nn_a: float | None, top_n: int | None) -> np.ndarray:
    if not nn_a and not top_n:
        return passes.copy()
    out = np.zeros(table.n, dtype=bool)
    rows = np.flatnonzero(passes)
    if not len(rows):
        return out
    groups = _post_groups(table)[rows]
    sigma = table.num["sigma"][rows]
    xyz = np.column_stack([table.num["x_a"][rows], table.num["y_a"][rows], table.num["z_a"][rows]]).astype(float)
    # Best σ first, rows without one last in table order.
    order_all = np.lexsort((np.arange(len(rows)), np.where(np.isnan(sigma), np.inf, -sigma), groups))
    bounds = np.flatnonzero(np.diff(groups[order_all])) + 1
    for part in np.split(order_all, bounds):
        keep_local = np.ones(len(part), dtype=bool)
        if nn_a:
            keep_local = greedy_keep(xyz[part], np.arange(len(part)), float(nn_a))
        kept = part[keep_local]
        if top_n:
            kept = kept[: int(top_n)]
        out[rows[kept]] = True
    return out


def _post_groups(table: PoolTable) -> np.ndarray:
    """One group per frame; a row with no frame is grouped by its member and tomogram."""
    f = table.cat["frame"].astype(np.int64)
    alt = -1 - (table.cat["member"].astype(np.int64) * 100_000 + table.cat["tomo"].astype(np.int64))
    return np.where(f >= 0, f, alt)


def counts(table: PoolTable, mask: np.ndarray) -> dict[str, Any]:
    """The count line: rows, frames (tomograms), projects, and rows per project."""
    rows = np.flatnonzero(mask)
    pcode = table.cat["project"][rows]
    projects = table.categories["project"]
    per_project = {projects[int(c)]: int(k) for c, k in zip(*np.unique(pcode, return_counts=True), strict=True)}
    return {
        "n": len(rows),
        "n_frames": len(np.unique(table.cat["frame"][rows])),
        "n_projects": len(per_project),
        "per_project": per_project,
    }


def column_header(table: PoolTable) -> list[dict[str, Any]]:
    """What the browser is told about each column: name, kind, label, unit, group, coverage."""
    out = []
    for k, info in NUM_COLUMNS.items():
        v = table.num[k]
        out.append(
            {
                "name": k,
                "kind": "num",
                "label": info.label,
                "unit": info.unit,
                "group": info.group,
                "coverage": int(np.count_nonzero(~np.isnan(v))),
                "keep_missing": info.keep_missing,
            }
        )
    for k, info in CAT_COLUMNS.items():
        out.append(
            {
                "name": k,
                "kind": "cat",
                "label": info.label,
                "group": info.group,
                "categories": table.categories[k],
                "coverage": table.n,
            }
        )
    return out
