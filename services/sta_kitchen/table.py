"""One member's columns, built in a worker from files only (`services/workers.py`).

`build_member(spec, out_npz)` reads the member's star, its ``tomograms.star``, the template match's
per-tomogram facts and its extractions, and writes one row per pick: identity, position (centred Å
and binned voxels of its reconstruction), score, geometry, extraction, the tomogram's facts. Numeric
columns are float32 with NaN for "no value"; categorical columns are int32 codes into lists kept in
the JSON beside the arrays, with the frames (reconstructions) the rows sit on.

`spec` comes from `services/sta_kitchen/members.member_spec`: paths and plain values, nothing from a
project state.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import starfile

from services.particles import analytics
from services.particles.coords import CENTERED_COLS

logger = logging.getLogger(__name__)

MARKER_KEYS = ("none", "sigma_outlier", "no_peak", "top_at_edge", "runtime_outlier", "cap_bound")
NO_EXTRACTION = "none"
# Centred Å are rounded to this many decimals wherever a pick is named by its position: a uid, a join.
COORD_DECIMALS = 1


def uid_of(project: str, frame_key: str, member_key: str, xyz: np.ndarray) -> np.ndarray:
    """48-bit ids (exact in float64) from sha1 of where the pick is: project, frame, member and its
    centred Å rounded to 0.1 Å. Stable across rebuilds; a re-run of the source makes new picks and
    new ids."""
    prefix = f"{project}|{frame_key}|{member_key}|"
    out = np.empty(len(xyz), dtype=np.float64)
    for i, (x, y, z) in enumerate(np.round(xyz, COORD_DECIMALS)):
        digest = hashlib.sha1(f"{prefix}{x:.1f},{y:.1f},{z:.1f}".encode()).hexdigest()[:12]
        out[i] = int(digest, 16)
    return out


def uid_hex(uid: float) -> str:
    return f"{int(uid):012x}"


def _particle_block(path: Path) -> pd.DataFrame:
    data = starfile.read(path, always_dict=True)
    block = next((v for v in data.values() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None)
    if block is None:
        raise ValueError(f"{path} has no particle table (no rlnTomoName)")
    missing = [c for c in CENTERED_COLS if c not in block.columns]
    if missing:
        raise ValueError(f"{path} has no centred coordinates ({', '.join(missing)})")
    return block.reset_index(drop=True)


def _tomo_table(path: Path) -> pd.DataFrame:
    data = starfile.read(path, always_dict=True)
    block = next((v for v in data.values() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None)
    if block is None:
        raise ValueError(f"{path} has no tomogram table")
    return block


def _abs(value: Any, project: Path) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    p = Path(value)
    return p if p.is_absolute() else project / p


def _header(mrc: Path) -> tuple[tuple[int, int, int], float] | None:
    import mrcfile

    try:
        with mrcfile.open(str(mrc), header_only=True, permissive=True) as m:
            h = m.header
            return (int(h.nx), int(h.ny), int(h.nz)), float(m.voxel_size.x)
    except (OSError, ValueError) as e:
        logger.warning("Kitchen: unreadable reconstruction header %s: %s", mrc, e)
        return None


def _frame_of(row: pd.Series, project: Path) -> dict[str, Any]:
    """The reconstruction a tomograms.star row names: its key (resolved path), binned voxel size and
    dimensions with where each came from. Unknown stays None, never a stand-in."""
    recon = _abs(row.get("rlnTomoReconstructedTomogram"), project)
    key = str(recon.resolve()) if recon is not None else ""
    apix, apix_src = None, "missing"
    try:
        ts_px = float(row["rlnTomoTiltSeriesPixelSize"])
        binning = float(row.get("rlnTomoTomogramBinning", 1.0))
        if ts_px > 0 and binning > 0:
            apix, apix_src = ts_px * binning, "star"
    except (KeyError, TypeError, ValueError):  # no pixel size stated: the header may carry one
        pass
    dims, dims_src = None, "missing"
    head = _header(recon) if recon is not None and recon.exists() else None
    if head is not None:
        dims, dims_src = head[0], "mrc_header"
        if apix is None and head[1] > 0:
            apix, apix_src = head[1], "mrc_header"
    elif apix is not None:
        try:
            binning = float(row.get("rlnTomoTomogramBinning", 1.0))
            dims = tuple(round(float(row[c]) / binning) for c in ("rlnTomoSizeX", "rlnTomoSizeY", "rlnTomoSizeZ"))
            dims_src = "star"
        except (KeyError, TypeError, ValueError):  # no sizes stated: the frame stays without dimensions
            dims = None
    hand = row.get("rlnTomoHand")
    return {
        "key": key,
        "recon": str(recon or ""),
        "recon_exists": head is not None,
        "tomo": str(row[analytics.TOMO_COL]),
        "apix": apix,
        "apix_src": apix_src,
        "dims": list(dims) if dims else None,
        "dims_src": dims_src,
        "hand": int(hand) if hand is not None and not (isinstance(hand, float) and math.isnan(hand)) else None,
        "ts_star": str(_abs(row.get("rlnTomoTiltSeriesStarFile"), project) or ""),
        "geometry": {c: _plain(row.get(c)) for c in analytics_transfer_cols()},
    }


def analytics_transfer_cols() -> tuple[str, ...]:
    from services.particles.tomo_identity import TRANSFERABLE_COLS

    return TRANSFERABLE_COLS


def _plain(v: Any) -> Any:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return v.item() if hasattr(v, "item") else v


def _read_record_data(path: Path) -> dict[str, Any] | None:
    try:
        return (json.loads(path.read_text()) or {}).get("data") or None
    except (OSError, ValueError):  # absent or being rewritten: the job.json backfill answers instead
        return None


def _tm_rows(tm_dir: str, tomos: list[str]) -> dict[str, dict[str, Any]]:
    """Per tomogram, the template match's facts: the driver's sidecar, else PyTOM's job.json."""
    out: dict[str, dict[str, Any]] = {}
    if not tm_dir:
        return out
    base = Path(tm_dir)
    for tomo in tomos:
        row = _read_record_data(base / "analytics" / "tm_tomo" / f"{tomo}.json")
        if row is None:
            try:
                row = analytics.tm_tomogram_data(json.loads((base / "tmResults" / f"{tomo}_job.json").read_text()))
            except (OSError, ValueError):  # no job.json for this tomogram: no TM facts, stated by NaN
                continue
        row["tomo"] = tomo
        out[tomo] = row
    return out


def _subtomo_join(df: pd.DataFrame, subtomo: list[dict[str, str]]) -> tuple[np.ndarray, np.ndarray, np.ndarray, list]:
    """(extraction code, row in that extraction's star, visible tilts) per row of `df`, matching by
    tomogram and centred Å rounded to 0.1 Å; the newest extraction wins a pick both carry."""
    n = len(df)
    code = np.zeros(n, dtype=np.int32)
    ext_row = np.full(n, -1, dtype=np.int32)
    vis = np.full(n, np.nan, dtype=np.float32)
    labels = [NO_EXTRACTION]
    if not subtomo:
        return code, ext_row, vis, labels
    key = _coord_keys(df)
    for job in subtomo:
        try:
            part = _particle_block(Path(job["star"]))
        except (OSError, ValueError) as e:
            logger.warning("Kitchen: extraction %s unreadable: %s", job["star"], e)
            continue
        labels.append(job["label"])
        k = _coord_keys(part)
        lookup = pd.Series(np.arange(len(part)), index=pd.MultiIndex.from_frame(k))
        lookup = lookup[~lookup.index.duplicated(keep="first")]
        hit = lookup.reindex(pd.MultiIndex.from_frame(key)).to_numpy()
        found = ~np.isnan(hit) & (code == 0)
        idx = hit[found].astype(int)
        code[found] = len(labels) - 1
        ext_row[found] = idx
        if "rlnTomoVisibleFrames" in part.columns:
            counts = part["rlnTomoVisibleFrames"].astype(str).map(_visible_count).to_numpy(dtype=np.float32)
            vis[found] = counts[idx]
    return code, ext_row, vis, labels


def _visible_count(raw: str) -> float:
    digits = [c for c in raw if c in "01"]
    return float(digits.count("1")) if digits else np.nan


def _coord_keys(df: pd.DataFrame) -> pd.DataFrame:
    scale = 10**COORD_DECIMALS
    out = pd.DataFrame({"t": df[analytics.TOMO_COL].astype(str).to_numpy()})
    for i, c in enumerate(CENTERED_COLS):
        out[f"c{i}"] = np.round(df[c].astype(float).to_numpy() * scale).astype(np.int64)
    return out


def _nn_within(xyz: np.ndarray) -> np.ndarray:
    """Each pick's distance to its nearest other pick (inf when alone)."""
    from scipy.spatial import cKDTree

    if len(xyz) < 2:
        return np.full(len(xyz), np.inf)
    return cKDTree(xyz).query(xyz, k=2)[0][:, 1]


def build_member(spec: dict[str, Any], out_npz: str) -> dict[str, Any]:
    """Build one member's columns into `out_npz` (+ its ``.json``) and return the meta. Worker-safe."""
    t0 = time.time()
    project = Path(spec["project"])
    df = _particle_block(Path(spec["star"]))
    src_rows = np.arange(len(df), dtype=np.int32)
    if spec.get("tomos"):
        taken = df[analytics.TOMO_COL].astype(str).isin(spec["tomos"]).to_numpy()
        src_rows = src_rows[taken]
        df = df[taken].reset_index(drop=True)
    n = len(df)
    notes: list[str] = []
    tomo_names = df[analytics.TOMO_COL].astype(str).to_numpy()
    tomos = sorted(set(tomo_names))
    tomo_code = np.searchsorted(np.array(tomos), tomo_names).astype(np.int32) if n else np.zeros(0, np.int32)

    tdf = _tomo_table(Path(spec["tomograms_star"]))
    rows_by_tomo = {str(r[analytics.TOMO_COL]): r for _, r in tdf.iterrows()}
    facts = spec.get("facts") or {}
    frames: list[dict[str, Any]] = []
    frame_of_tomo: dict[str, int] = {}
    for t in tomos:
        row = rows_by_tomo.get(t)
        if row is None:
            notes.append(f"{t}: no row in {spec['tomograms_star']}, so its picks have no voxel position")
            continue
        frame_of_tomo[t] = len(frames)
        # Excluded from processing in the member's own project: said in the kitchen, applied to nothing.
        frames.append({**_frame_of(row, project), "excluded": bool((facts.get(t) or {}).get("excluded"))})

    xyz = df[CENTERED_COLS].to_numpy(dtype=np.float64)
    cols: dict[str, np.ndarray] = {}
    nan = np.full(n, np.nan, dtype=np.float32)
    for i, c in enumerate(("x_a", "y_a", "z_a")):
        cols[c] = xyz[:, i].astype(np.float32)
    vox = np.full((n, 3), np.nan)
    frame_code = np.full(n, -1, dtype=np.int32)
    face = nan.copy()
    z_pct = nan.copy()
    for ti, t in enumerate(tomos):
        fi = frame_of_tomo.get(t)
        if fi is None:
            continue
        m = tomo_code == ti
        frame_code[m] = fi
        fr = frames[fi]
        if fr["apix"] and fr["dims"]:
            dims = np.asarray(fr["dims"], dtype=float)
            vox[m] = xyz[m] / fr["apix"] + dims / 2.0
            extent = dims * fr["apix"]
            face[m] = (extent / 2.0 - np.abs(xyz[m])).min(axis=1)
            z_pct[m] = vox[m, 2] / dims[2] * 100.0
    for i, c in enumerate(("x_px", "y_px", "z_px")):
        cols[c] = vox[:, i].astype(np.float32)

    sigma = analytics.particle_sigma_scores(df)
    # Rounded as the analytics records store it, so a cut counts here what it counts on the Analytics tab.
    cols["sigma"] = np.round(sigma, analytics.SCORE_DECIMALS).astype(np.float32) if sigma is not None else nan.copy()
    cols["lcc"] = (
        df[analytics.SCORE_COL].astype(float).to_numpy(dtype=np.float32)
        if analytics.SCORE_COL in df.columns
        else nan.copy()
    )
    tm_rows = _tm_rows(spec.get("tm_dir", ""), tomos)
    rank, margin, depth, nn = nan.copy(), nan.copy(), nan.copy(), nan.copy()
    slab, ctf, n_tilts, n_blank = nan.copy(), nan.copy(), nan.copy(), nan.copy()
    marker_code = np.zeros(n, dtype=np.int32)
    acq_names = [""]
    acq_code = np.zeros(n, dtype=np.int32)
    cand_rows: list[dict[str, Any]] = []
    cap = spec.get("cap")
    for ti, t in enumerate(tomos):
        m = np.flatnonzero(tomo_code == ti)
        p = xyz[m]
        s = cols["sigma"][m]
        if np.isfinite(s).any():
            order = np.argsort(-np.nan_to_num(s, nan=-np.inf), kind="stable")
            r = np.empty(len(m), dtype=np.float32)
            r[order] = np.arange(1, len(m) + 1)
            r[~np.isfinite(s)] = np.nan
            rank[m] = r
        fp1 = (tm_rows.get(t) or {}).get("fp1_sigma")
        if fp1 is not None:
            margin[m] = s - float(fp1)
        coef = analytics.fit_pick_plane(p)
        if coef is not None:
            d = analytics.plane_depth(p, coef)
            depth[m] = d
            lo, hi = np.quantile(d, [0.05, 0.95])
            slab[m] = hi - lo
        if len(m) > 1:
            nn[m] = _nn_within(p)
        f = facts.get(t) or {}
        for arr, k in ((ctf, "ctf_res"), (n_tilts, "n_tilts"), (n_blank, "n_blank")):
            if f.get(k) is not None:
                arr[m] = float(f[k])
        acq = f.get("acq") or ""
        if acq not in acq_names:
            acq_names.append(acq)
        acq_code[m] = acq_names.index(acq)
        if np.isfinite(s).any():
            fin = np.sort(s[np.isfinite(s)])[::-1]
            sig = df.loc[m, analytics.SIGMA_COL].astype(float) if analytics.SIGMA_COL in df.columns else None
            cand_rows.append(
                {
                    "tomo": t,
                    "n": len(m),
                    "sigma": float(np.median(sig)) if sig is not None else None,
                    "top_sigma": float(fin[0]),
                    "last_sigma": float(fin[-1]),
                    "cap_bound": bool(cap) and len(m) >= int(cap),
                }
            )
    if tm_rows or cand_rows:
        rows = analytics.tomogram_rows({"tomograms": list(tm_rows.values())}, {"tomograms": cand_rows})
        by_tomo = {}
        for mk in analytics.tomogram_markers(rows):
            by_tomo.setdefault(mk.tomo, mk.key)
        for ti, t in enumerate(tomos):
            if t in by_tomo:
                marker_code[tomo_code == ti] = MARKER_KEYS.index(by_tomo[t])
    cols.update(
        rank=rank,
        margin=margin,
        depth_a=depth,
        face_a=face,
        nn_a=np.where(np.isinf(nn), np.nan, nn).astype(np.float32),
        z_pct=z_pct,
        slab_a=slab,
        ctf_res=ctf,
        n_tilts=n_tilts,
        n_blank=n_blank,
    )
    ext_code, ext_row, vis, ext_labels = _subtomo_join(df, spec.get("subtomo") or [])
    cols["vis_tilts"] = vis

    uids = np.zeros(n, dtype=np.float64)
    for fi, fr in enumerate(frames):
        m = frame_code == fi
        uids[m] = uid_of(str(project), fr["key"], spec["key"], xyz[m])
    m = frame_code < 0
    if m.any():
        uids[m] = uid_of(str(project), "", spec["key"], xyz[m])

    arrays = {f"num_{k}": v.astype(np.float32) for k, v in cols.items()}
    arrays.update(
        uid=uids,
        cat_tomo=tomo_code,
        cat_frame=frame_code,
        cat_acq=acq_code,
        cat_extracted=ext_code,
        cat_tm_marker=marker_code,
        src_row=src_rows,
        ext_row=ext_row,
    )
    if len(np.unique(uids)) != n:
        notes.append(f"{n - len(np.unique(uids))} picks share a position with another in the same frame")
    out = Path(out_npz)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.stem}.tmp.npz")
    np.savez(tmp, **arrays)
    tmp.replace(out)
    meta = {
        "key": spec["key"],
        "fingerprint": spec["fingerprint"],
        "n": n,
        "source": spec["source"],
        "categories": {"tomo": tomos, "acq": acq_names, "extracted": ext_labels, "tm_marker": list(MARKER_KEYS)},
        "frames": frames,
        "subtomo": spec.get("subtomo") or [],
        "star": spec["star"],
        "tomograms_star": spec["tomograms_star"],
        "notes": notes,
        "build_s": round(time.time() - t0, 2),
    }
    meta_path = out.with_suffix(".json")
    tmp_meta = meta_path.with_name(f".{meta_path.name}.tmp")
    tmp_meta.write_text(json.dumps(meta, default=_plain))
    tmp_meta.replace(meta_path)
    return meta
