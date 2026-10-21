"""Particle-stage statistics: pure functions over PyTOM job.json dicts, score maps and star tables.

Two kinds of number live here:

- **Fixed facts of a finished job** — σ, search space, the FP=1 cutoff, score-map peaks, per-tomogram
  ladders. The job's own driver computes them once into a sidecar (`tm_tomogram_data`,
  `scan_score_map`, `candidates_data`); `services/particles/analytics_store.py` backfills the same
  data blocks for jobs that finished before the sidecars existed, from the files they left.
- **Choice-dependent numbers** — anything that depends on a cut the user is looking at
  (`counts_at_cut`, `capped_above_cut`). Computed on demand from the records, never stored.

Markers are rules that mark a tomogram for a human to look at; they never drop anything, and each
carries the rule, the measured value and the threshold in its text (`MARKER_RULES`).
"""

from __future__ import annotations

import bisect
import math
import re
import statistics
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from services.particles.coords import CENTERED_COLS
from services.particles.sigma import LADDER_SIGMAS, expected_noise_count, fp_cutoff_sigma

TM_TOMOGRAM_KIND = "tm_tomogram"
TM_TOMOGRAM_SCHEMA = 1
CANDIDATES_KIND = "candidates"
CANDIDATES_SCHEMA = 2
SUBTOMO_KIND = "subtomo"
SUBTOMO_SCHEMA = 1

SCORE_COL = "rlnLCCmax"
SIGMA_COL = "rlnSearchStd"
CUTOFF_COL = "rlnCutOff"
TOMO_COL = "rlnTomoName"

# A score-map maximum this close to a face is an edge artefact candidate: the volume's border is
# where reconstruction artefacts and the missing context of the template sit.
EDGE_XY_PX = 32
EDGE_Z_PX = 16
# How many of the highest distinct peaks a scan keeps, in σ.
SCAN_TOP_PEAKS = 20
# Scores are stored to this many decimals (σ units): 0.001σ is far below any cut anyone makes.
SCORE_DECIMALS = 3


# ── TM: one tomogram ─────────────────────────────────────────────────────────────────────────────


def tm_tomogram_data(job: dict[str, Any]) -> dict[str, Any]:
    """The facts of one tomogram's template match, from its PyTOM ``job.json``. ``sigma`` is None
    while PyTOM has not written its statistics (the match is still running)."""
    stats = job.get("job_stats") or {}
    sigma = _float_or_none(stats.get("std"))
    search_space = _float_or_none(stats.get("search_space"))
    tilts = [float(t) for t in job.get("tilt_angles") or []]
    ctf = [c for c in job.get("ctf_data") or [] if isinstance(c, dict)]
    defocus_um = [float(c["defocus"]) * 1e6 for c in ctf if c.get("defocus") is not None]
    dose = [float(d) for d in job.get("dose_accumulation") or []]
    return {
        "tomo": str(job.get("tomo_id") or ""),
        "sigma": sigma,
        "search_space": search_space,
        "fp1_sigma": fp_cutoff_sigma(search_space) if search_space and search_space > 0 else None,
        "n_rotations": _int_or_none(job.get("n_rotations")),
        "voxel": _float_or_none(job.get("voxel_size")),
        "tomo_shape": [int(v) for v in job.get("tomo_shape") or []],
        "n_tilts": len(tilts),
        "tilt_min": min(tilts) if tilts else None,
        "tilt_max": max(tilts) if tilts else None,
        "defocus_um_min": min(defocus_um) if defocus_um else None,
        "defocus_um_max": max(defocus_um) if defocus_um else None,
        "dose_max": max(dose) if dose else None,
        "phase_flip": bool(ctf[0].get("flip_phase")) if ctf else None,
        "random_phase": bool(job.get("random_phase_correction")),
        "low_pass": _float_or_none(job.get("low_pass")),
        "high_pass": _float_or_none(job.get("high_pass")),
        "pytom_version": str(job.get("pytom_tm_version_number") or ""),
    }


def exclusion_radius_px(diameter_ang: float | None, voxel: float | None) -> int | None:
    """PyTOM's extraction exclusion radius for a particle diameter: int(Ø / 2 / voxel)."""
    if not diameter_ang or not voxel or diameter_ang <= 0 or voxel <= 0:
        return None
    return int(diameter_ang / 2.0 / voxel)


def scan_score_map(
    scores_mrc: Path,
    *,
    sigma: float,
    cutoff_sigma: float,
    radius_px: int | None,
    chunk: int = 16,
    mask_mrc: Path | None = None,
) -> dict[str, Any]:
    """Peaks of one PyTOM score map, in σ: the global maximum and where it sits, the local maxima
    (26-neighbourhood) above `cutoff_sigma`, and how many of them survive PyTOM's greedy exclusion
    at `radius_px` — what extraction would return with that cutoff and no cap (extraction can add
    shoulder picks, so read the count as a floor). Streams the map in Z chunks of `chunk` slices
    with a one-slice halo, so memory stays at a few chunks whatever the volume size. With the job's
    search mask (`mask_mrc`), scores outside it count as 0, as extraction sets them."""
    from contextlib import nullcontext

    import mrcfile

    t0 = time.monotonic()
    cutoff_lcc = cutoff_sigma * sigma
    values: list[np.ndarray] = []
    positions: list[np.ndarray] = []
    top_val, top_zyx = -math.inf, (0, 0, 0)
    with (
        mrcfile.mmap(str(scores_mrc), mode="r", permissive=True) as m,
        mrcfile.mmap(str(mask_mrc), mode="r", permissive=True) if mask_mrc is not None else nullcontext() as mm,
    ):
        data = m.data
        if data is None or data.ndim != 3:
            raise ValueError(f"{scores_mrc} is not a 3D volume")
        nz, ny, nx = data.shape
        mask = mm.data if mm is not None else None
        if mask is not None and mask.shape != data.shape:
            raise ValueError(f"search mask {mask_mrc} is {mask.shape}, the score map {data.shape}")
        for z0 in range(0, nz, chunk):
            z1 = min(nz, z0 + chunk)
            lo, hi = max(0, z0 - 1), min(nz, z1 + 1)
            block = np.asarray(data[lo:hi], dtype=np.float32)
            if mask is not None:
                # A new array: `block` can be a read-only view of the mapped score map.
                block = np.where(np.asarray(mask[lo:hi]) > 0, block, np.float32(0.0))
            core = block[z0 - lo : z0 - lo + (z1 - z0)]
            k = int(np.argmax(core))
            v = float(core.flat[k])
            if v > top_val:
                top_val = v
                top_zyx = (z0 + k // (ny * nx), (k // nx) % ny, k % nx)
            zz, yy, xx = np.nonzero(core > cutoff_lcc)
            if zz.size == 0:
                continue
            zb = zz + (z0 - lo)
            vals = core[zz, yy, xx]
            is_max = np.ones(zz.size, dtype=bool)
            for dz in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for dx in (-1, 0, 1):
                        if dz == dy == dx == 0:
                            continue
                        z2, y2, x2 = zb + dz, yy + dy, xx + dx
                        ok = (z2 >= 0) & (z2 < block.shape[0]) & (y2 >= 0) & (y2 < ny) & (x2 >= 0) & (x2 < nx)
                        neighbour = np.full(zz.size, -np.inf, dtype=np.float32)
                        neighbour[ok] = block[z2[ok], y2[ok], x2[ok]]
                        is_max &= ~(neighbour > vals)
            values.append(vals[is_max])
            positions.append(np.column_stack([zz[is_max] + z0, yy[is_max], xx[is_max]]))
    vals = np.concatenate(values) if values else np.empty(0, dtype=np.float32)
    pos = np.concatenate(positions) if positions else np.empty((0, 3), dtype=int)
    kept = _greedy_exclusion(vals, pos, radius_px) if radius_px else None
    tz, ty, tx = top_zyx
    out: dict[str, Any] = {
        "top_sigma": round(top_val / sigma, SCORE_DECIMALS),
        "top_xyz": [int(tx), int(ty), int(tz)],
        "top_edge": bool(
            tx < EDGE_XY_PX
            or tx >= nx - EDGE_XY_PX
            or ty < EDGE_XY_PX
            or ty >= ny - EDGE_XY_PX
            or tz < EDGE_Z_PX
            or tz >= nz - EDGE_Z_PX
        ),
        "shape_xyz": [int(nx), int(ny), int(nz)],
        "cutoff_sigma": round(cutoff_sigma, SCORE_DECIMALS),
        "n_local_max": int(vals.size),
        "radius_px": radius_px,
        "n_peaks": None,
        "peaks_sigma": [],
        "seconds": round(time.monotonic() - t0, 1),
    }
    if kept is not None:
        out["n_peaks"] = int(kept.size)
        out["peaks_sigma"] = [round(float(v) / sigma, SCORE_DECIMALS) for v in vals[kept][:SCAN_TOP_PEAKS]]
    return out


def mask_fraction(mask_mrc: Path) -> float:
    """The share of a volume a mask (> 0 inside) covers, 0 … 1."""
    import mrcfile

    with mrcfile.mmap(str(mask_mrc), mode="r", permissive=True) as m:
        if m.data is None:
            raise ValueError(f"{mask_mrc} holds no data")
        return float(np.count_nonzero(np.asarray(m.data) > 0)) / float(m.data.size)


def _greedy_exclusion(vals: np.ndarray, pos: np.ndarray, radius: float) -> np.ndarray:
    """Indices of the peaks PyTOM's extraction would keep: highest first, each one blanking a
    sphere of `radius` around it. Returned in descending score order. A grid of `radius`-sized
    cells limits each check to the 27 neighbouring cells."""
    order = np.argsort(-vals, kind="stable")
    cells: dict[tuple[int, int, int], list[int]] = {}
    kept: list[int] = []
    r2 = float(radius) * float(radius)
    size = max(float(radius), 1.0)
    for i in order:
        p = pos[i]
        c = (int(p[0] // size), int(p[1] // size), int(p[2] // size))
        clash = False
        for dz in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dx in (-1, 0, 1):
                    for j in cells.get((c[0] + dz, c[1] + dy, c[2] + dx), ()):
                        d = pos[j] - p
                        if float(d[0] * d[0] + d[1] * d[1] + d[2] * d[2]) <= r2:
                            clash = True
                            break
                    if clash:
                        break
                if clash:
                    break
            if clash:
                break
        if not clash:
            kept.append(int(i))
            cells.setdefault(c, []).append(int(i))
    return np.asarray(kept, dtype=int)


# ── Candidates: one extraction job ───────────────────────────────────────────────────────────────


def candidates_data(
    particles,
    job_stats_by_tomo: dict[str, dict[str, Any]],
    *,
    cap: int | None,
    cutoff_method: str = "",
    false_positives: float | None = None,
    cc_threshold: float | None = None,
    diameter_ang: float | None = None,
    extents: dict[str, np.ndarray] | None = None,
) -> dict[str, Any]:
    """The candidate set of one extraction job, per tomogram, in σ, with where its picks sit
    (`pick_geometry`). `particles` is the candidates table (pandas); `job_stats_by_tomo` maps each
    tomogram TM processed to its `tm_tomogram_data` (tomograms with no picks appear with n = 0);
    `extents` are the volumes' sizes in Å (`tomogram_extents`), without which edge picks are not
    counted."""
    missing = [c for c in (TOMO_COL, SCORE_COL, SIGMA_COL) if c not in particles.columns]
    if missing:
        raise ValueError(f"candidates table has no {', '.join(missing)}; σ needs PyTOM's score and noise columns")
    rows: list[dict[str, Any]] = []
    grouped = {str(t): g for t, g in particles.groupby(TOMO_COL, sort=True)}
    for tomo in sorted(set(grouped) | set(job_stats_by_tomo)):
        tm = job_stats_by_tomo.get(tomo) or {}
        g = grouped.get(tomo)
        rows.append(_candidate_row(tomo, g, tm, cap, (extents or {}).get(tomo), diameter_ang))
    n_total = sum(r["n"] for r in rows)
    return {
        "cap": cap,
        "cutoff_method": cutoff_method,
        "false_positives": false_positives,
        "cc_threshold": cc_threshold,
        "diameter_ang": diameter_ang,
        "n_total": n_total,
        "n_tomograms": len(rows),
        "n_with_picks": sum(1 for r in rows if r["n"]),
        "ladder_sigmas": list(LADDER_SIGMAS),
        "has_extents": extents is not None,
        "edge_ang": EDGE_ANG,
        "nn_close_ang": NN_CLOSE_FRACTION * diameter_ang if diameter_ang else None,
        "tomograms": rows,
    }


def _candidate_row(tomo: str, g, tm: dict[str, Any], cap: int | None, extent, diameter_ang) -> dict[str, Any]:
    search_space = tm.get("search_space")
    fp1 = tm.get("fp1_sigma")
    if g is None or len(g) == 0:
        return {
            "tomo": tomo,
            "n": 0,
            "sigma": tm.get("sigma"),
            "sigma_spread": 0.0,
            "cutoff_lcc": None,
            "cutoff_sigma": None,
            "search_space": search_space,
            "fp1_sigma": fp1,
            "top_sigma": None,
            "last_sigma": None,
            "cap_bound": False,
            "ladder": [0] * len(LADDER_SIGMAS),
            "scores": [],
            "geometry": None,
        }
    sigmas = g[SIGMA_COL].astype(float).to_numpy()
    sigma = float(np.median(sigmas))
    lcc = g[SCORE_COL].astype(float).to_numpy()
    order = np.argsort(-(lcc / sigmas), kind="stable")
    scores = (lcc / sigmas)[order]
    geometry = None
    if all(c in g.columns for c in CENTERED_COLS):
        geometry = pick_geometry(g[CENTERED_COLS].to_numpy(dtype=float)[order], extent, diameter_ang)
    cutoff_lcc = float(np.median(g[CUTOFF_COL].astype(float))) if CUTOFF_COL in g.columns else None
    n = len(g)
    return {
        "tomo": tomo,
        "n": n,
        "sigma": sigma,
        "sigma_spread": float(sigmas.max() - sigmas.min()),
        "cutoff_lcc": cutoff_lcc,
        "cutoff_sigma": round(cutoff_lcc / sigma, SCORE_DECIMALS) if cutoff_lcc is not None and sigma > 0 else None,
        "search_space": search_space,
        "fp1_sigma": fp1,
        "top_sigma": round(float(scores[0]), SCORE_DECIMALS),
        "last_sigma": round(float(scores[-1]), SCORE_DECIMALS),
        "cap_bound": bool(cap) and n >= int(cap),
        "ladder": [int(np.count_nonzero(scores >= z)) for z in LADDER_SIGMAS],
        "scores": [round(float(s), SCORE_DECIMALS) for s in scores],
        "geometry": geometry,
    }


def subtomo_data(particles, upstream) -> dict[str, Any]:
    """One subtomo extraction, per tomogram: the candidates it was given, the particles it made (the
    rest fell outside the tilt images when the stacks were cut) and how many tilts each particle is
    visible in (`rlnTomoVisibleFrames`), pooled as a histogram. `upstream` may be None."""
    out_counts = particles[TOMO_COL].astype(str).value_counts()
    in_counts = upstream[TOMO_COL].astype(str).value_counts() if upstream is not None else None
    visible = None
    if "rlnTomoVisibleFrames" in particles.columns:
        visible = particles["rlnTomoVisibleFrames"].astype(str).str.count("1").to_numpy()
    rows = []
    tomos = sorted(set(out_counts.index) | (set(in_counts.index) if in_counts is not None else set()))
    tomo_col = particles[TOMO_COL].astype(str).to_numpy()
    for t in tomos:
        n_out = int(out_counts.get(t, 0))
        n_in = int(in_counts.get(t, 0)) if in_counts is not None else None
        vis = visible[tomo_col == t] if visible is not None else None
        rows.append(
            {
                "tomo": t,
                "n_in": n_in,
                "n_out": n_out,
                "lost": None if n_in is None else n_in - n_out,
                "visible_median": float(np.median(vis)) if vis is not None and vis.size else None,
            }
        )
    hist: dict[str, int] = {}
    if visible is not None:
        for k, n in zip(*np.unique(visible, return_counts=True), strict=True):
            hist[str(int(k))] = int(n)
    return {
        "n_in": sum(r["n_in"] or 0 for r in rows) if in_counts is not None else None,
        "n_out": len(particles),
        "visible_hist": hist,
        "tomograms": rows,
    }


def particle_sigma_scores(particles) -> np.ndarray | None:
    """LCC ÷ σ for every row of a particles or candidates table (rows in table order), or None
    when the table does not carry PyTOM's score and noise columns."""
    if SCORE_COL not in particles.columns or SIGMA_COL not in particles.columns:
        return None
    sig = particles[SIGMA_COL].astype(float).to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        out = particles[SCORE_COL].astype(float).to_numpy() / sig
    out[~(sig > 0)] = np.nan
    return out


# ── Choice-dependent numbers (on demand, never stored) ───────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class CutCounts:
    cut: float
    n_picks: int
    n_tomograms: int
    per_tomo: dict[str, int]


def counts_at_cut(rows: Sequence[dict[str, Any]], cut: float) -> CutCounts:
    """Picks at or above `cut` σ, per tomogram and pooled, from the candidate rows' sorted scores."""
    per: dict[str, int] = {}
    for r in rows:
        desc = r.get("scores") or []
        # Scores are stored descending; bisect on the negated, ascending list.
        k = bisect.bisect_right([-s for s in desc], -cut)
        if k:
            per[r["tomo"]] = k
    return CutCounts(cut, sum(per.values()), len(per), per)


def capped_above_cut(rows: Sequence[dict[str, Any]], cut: float) -> list[dict[str, Any]]:
    """Tomograms where the extraction cap bound and every capped pick sits at or above `cut`:
    either a dense field (the cap then cut real particles) or a structured artefact."""
    return [r for r in rows if r.get("cap_bound") and r.get("last_sigma") is not None and r["last_sigma"] >= cut]


def pooled_scores(rows: Iterable[dict[str, Any]]) -> np.ndarray:
    parts = [np.asarray(r.get("scores") or [], dtype=float) for r in rows]
    return np.concatenate(parts) if parts else np.empty(0)


def sigma_histogram(scores: np.ndarray, *, width: float = 0.25) -> tuple[list[float], list[int]]:
    """Counts per `width`-σ bin, as (left edges, counts); empty when there are no scores."""
    if scores.size == 0:
        return [], []
    lo = math.floor(float(scores.min()) / width) * width
    hi = math.floor(float(scores.max()) / width) * width + width
    n_bins = max(1, round((hi - lo) / width))
    counts, edges = np.histogram(scores, bins=n_bins, range=(lo, lo + n_bins * width))
    return [round(float(e), 4) for e in edges[:-1]], [int(c) for c in counts]


def survival_curves(rows: Sequence[dict[str, Any]], *, step: float = 0.1) -> dict[str, list]:
    """Observed picks at or above z, against z, and the Gaussian null Σ N·Q(z) over the tomograms
    whose search space is known. The observed curve is truncated below the extraction cutoff (no
    candidate exists there) and flattened by the cap where it bound."""
    scores = pooled_scores(rows)
    if scores.size == 0:
        return {"z": [], "observed": [], "null": [], "null_tomograms": 0}
    z_lo = math.floor(float(scores.min()) / step) * step
    z_hi = math.ceil(float(scores.max()) / step) * step
    z = [round(z_lo + i * step, 4) for i in range(round((z_hi - z_lo) / step) + 1)]
    desc = np.sort(scores)[::-1]
    neg = -desc
    observed = [int(np.searchsorted(neg, -zz, side="right")) for zz in z]
    spaces = [float(r["search_space"]) for r in rows if r.get("search_space")]
    null = [expected_noise_count(zz, spaces) for zz in z] if spaces else []
    return {"z": z, "observed": observed, "null": null, "null_tomograms": len(spaces)}


def ladder_totals(rows: Sequence[dict[str, Any]]) -> list[tuple[float, int, int]]:
    """(σ level, picks at or above it, tomograms with any) for each ladder level."""
    out = []
    for i, z in enumerate(LADDER_SIGMAS):
        counts = [int((r.get("ladder") or [0] * len(LADDER_SIGMAS))[i]) for r in rows]
        out.append((z, sum(counts), sum(1 for c in counts if c)))
    return out


# ── Geometry of picks ─────────────────────────────────────────────────────────────────────────

# A plane through fewer picks than this says nothing about the lamella.
PLANE_MIN_PICKS = 20
# Each refit keeps this fraction of picks closest to the previous plane.
PLANE_KEEP_FRACTION = 0.8
PLANE_ITERATIONS = 5


def tomogram_extents(tomograms_star: str | Path) -> dict[str, np.ndarray]:
    """{tomogram: (X, Y, Z) extent in Å} from a tomograms star: the unbinned size times the
    tilt-series pixel size, which is the reconstruction's extent whatever its binning."""
    import pandas as pd
    import starfile

    data = starfile.read(tomograms_star, always_dict=True)
    df = next((v for v in data.values() if isinstance(v, pd.DataFrame) and TOMO_COL in v.columns), None)
    cols = ["rlnTomoSizeX", "rlnTomoSizeY", "rlnTomoSizeZ", "rlnTomoTiltSeriesPixelSize"]
    if df is None or any(c not in df.columns for c in cols):
        raise ValueError(f"{tomograms_star} has no tomogram sizes ({', '.join(cols)})")
    out = {}
    for _, r in df.iterrows():
        px = float(r["rlnTomoTiltSeriesPixelSize"])
        out[str(r[TOMO_COL])] = np.array([float(r[c]) * px for c in cols[:3]])
    return out


def fit_pick_plane(xyz: np.ndarray) -> np.ndarray | None:
    """(a, b, c) of z = a·x + b·y + c through a tomogram's picks (centred Å), refit each round on
    the PLANE_KEEP_FRACTION closest to the previous plane so outliers do not tilt it. None when there
    are fewer than PLANE_MIN_PICKS picks."""
    xyz = np.asarray(xyz, dtype=float).reshape(-1, 3)
    if len(xyz) < PLANE_MIN_PICKS:
        return None
    use = xyz
    coef = np.zeros(3)
    for _ in range(PLANE_ITERATIONS):
        design = np.column_stack([use[:, 0], use[:, 1], np.ones(len(use))])
        coef = np.linalg.lstsq(design, use[:, 2], rcond=None)[0]
        residual = np.abs(xyz[:, 2] - (coef[0] * xyz[:, 0] + coef[1] * xyz[:, 1] + coef[2]))
        use = xyz[residual <= np.quantile(residual, PLANE_KEEP_FRACTION)]
        if len(use) < 10:
            break
    return coef


def plane_depth(xyz: np.ndarray, coef: np.ndarray) -> np.ndarray:
    """Signed distance along Z from the pick plane, in Å (positive = above it)."""
    return xyz[:, 2] - (coef[0] * xyz[:, 0] + coef[1] * xyz[:, 1] + coef[2])


# Depth through the pick slab is reported in this many equal bins between the slab's 5th and 95th
# percentile; picks beyond those fall in the outer bins, which together are the "surface" share.
DEPTH_BINS = 10
# Picks closer than this to a face of the volume.
EDGE_ANG = 100.0
# Two picks closer than this fraction of the particle diameter overlap: one particle picked twice,
# or a diameter that does not fit the particle.
NN_CLOSE_FRACTION = 0.65


def pick_geometry(xyz: np.ndarray, extent: np.ndarray | None, diameter_ang: float | None) -> dict[str, Any]:
    """Where one tomogram's picks sit (centred Å, rows in the caller's order): the plane through
    them (the lamella, as the picks see it) — its tilt, the slab's 5th–95th percentile thickness, the
    share in the outer bins and each pick's depth bin; picks near the faces; nearest-neighbour
    distances."""
    out: dict[str, Any] = {"plane": None, "depth_bins": [], "edge_n": None, "nn_median_ang": None, "nn_close_n": None}
    coef = fit_pick_plane(xyz)
    if coef is not None:
        depth = plane_depth(xyz, coef)
        lo, hi = (float(v) for v in np.quantile(depth, [0.05, 0.95]))
        width = hi - lo
        bins = (
            np.clip(np.floor((depth - lo) / width * DEPTH_BINS), 0, DEPTH_BINS - 1).astype(int)
            if width > 0
            else np.full(len(xyz), DEPTH_BINS // 2)
        )
        out["plane"] = {
            "tilt_deg": round(math.degrees(math.atan(math.hypot(coef[0], coef[1]))), 1),
            "thickness_ang": round(width, 1),
            "offset_ang": round(float(coef[2]), 1),
            "surface_share": round(float(np.mean((bins == 0) | (bins == DEPTH_BINS - 1))), 3),
        }
        out["depth_bins"] = bins.tolist()
    if extent is not None:
        margin = (np.asarray(extent, dtype=float) / 2.0)[None, :] - np.abs(xyz)
        out["edge_n"] = int(np.count_nonzero(margin.min(axis=1) < EDGE_ANG))
    if len(xyz) > 1:
        nn = np.full(len(xyz), np.inf)
        for i in range(0, len(xyz), 512):
            d = xyz[i : i + 512, None, :] - xyz[None, :, :]
            dist = np.sqrt((d * d).sum(axis=2))
            dist[np.arange(dist.shape[0]), np.arange(i, i + dist.shape[0])] = np.inf
            nn[i : i + 512] = dist.min(axis=1)
        out["nn_median_ang"] = round(float(np.median(nn)), 1)
        if diameter_ang:
            out["nn_close_n"] = int(np.count_nonzero(nn < NN_CLOSE_FRACTION * diameter_ang))
    return out


def depth_profile(rows: Sequence[dict[str, Any]], cut: float) -> dict[str, list]:
    """Pooled over tomograms with a pick plane: picks per depth bin and how many of them score at or
    above `cut` — real particles peak mid-slab, surface artefacts in the outer bins."""
    n = [0] * DEPTH_BINS
    at_cut = [0] * DEPTH_BINS
    for r in rows:
        geo = r.get("geometry") or {}
        bins = geo.get("depth_bins") or []
        for b, s in zip(bins, r.get("scores") or [], strict=False):
            n[b] += 1
            if s >= cut:
                at_cut[b] += 1
    return {"n": n, "at_cut": at_cut}


def min_distances(a: np.ndarray, b: np.ndarray, chunk: int = 2048) -> np.ndarray:
    """For each row of `a`, the distance to its nearest row of `b` (inf when `b` is empty)."""
    out = np.full(len(a), np.inf)
    if len(a) == 0 or len(b) == 0:
        return out
    for i in range(0, len(a), chunk):
        d = a[i : i + chunk, None, :] - b[None, :, :]
        out[i : i + chunk] = np.sqrt((d * d).sum(axis=2)).min(axis=1)
    return out


# ── Markers ──────────────────────────────────────────────────────────────────────────────────────

# Robust z of σ against the job's median (MAD scaled to a Gaussian σ) beyond which a tomogram's
# noise level is unusual for its job: a thickness or contrast outlier.
SIGMA_OUTLIER_Z = 3.0
# Top peak within this many σ of the FP=1 cutoff: the match found nothing that stands out.
NO_PEAK_MARGIN = 1.0
# Runtime more than this times the median of its node family (with at least RUNTIME_MIN_FAMILY
# tomograms there to compare against).
RUNTIME_OUTLIER_FACTOR = 2.0
RUNTIME_MIN_FAMILY = 3

MARKER_RULES: dict[str, str] = {
    "sigma_outlier": (
        f"σ is more than {SIGMA_OUTLIER_Z:g} robust standard deviations (1.4826 × MAD) from the job's median σ: "
        "this tomogram's noise level is unusual — a thickness or contrast outlier."
    ),
    "no_peak": (
        f"The highest peak is within {NO_PEAK_MARGIN:g}σ of the FP=1 cutoff: template matching found nothing that "
        "stands out of the background here."
    ),
    "runtime_outlier": (
        f"Matching took more than {RUNTIME_OUTLIER_FACTOR:g}× the median of tomograms on the same node family "
        f"(compared when {RUNTIME_MIN_FAMILY} or more ran there)."
    ),
    "top_at_edge": (
        f"The score map's global maximum is within {EDGE_XY_PX} px (X/Y) or {EDGE_Z_PX} px (Z) of the volume's "
        "faces: likely an edge artefact rather than a particle."
    ),
    "cap_bound": "Extraction stopped at its cap (max particles): peaks below the last pick were never extracted.",
    "cap_above_cut": (
        "The cap bound and every capped pick is at or above the cut: a dense field (the cap cut real particles) "
        "or a structured artefact. Look at these first."
    ),
}
MARKERS_DROP_NOTHING = "Markers point a human at a tomogram; nothing is dropped or excluded by them."


@dataclass(frozen=True, slots=True)
class Marker:
    key: str
    tomo: str
    value: str  # the measured value against the rule, in words

    @property
    def text(self) -> str:
        return f"{self.value}. {MARKER_RULES[self.key]} {MARKERS_DROP_NOTHING}"


def node_family(node: str | None) -> str:
    """'clip-g3-5.cbe.vbc.ac.at' → 'clip-g3': the hostname without domain and trailing node number."""
    if not node:
        return ""
    short = node.split(".", 1)[0]
    return re.sub(r"[-_]?\d+$", "", short) or short


def tomogram_rows(
    tm_data: dict[str, Any] | None, cand_data: dict[str, Any] | None, subtomo_data: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """One row per tomogram joining a template-match record, the extraction made from it and the
    subtomo extraction made from that (any may be None). TM facts win where both carry a value; the
    candidate and subtomo rows ride along under "cand" and "sub". Rows come in position order."""
    from services.array_tasks import ts_position_sort_key

    tm = {r["tomo"]: r for r in (tm_data or {}).get("tomograms") or [] if r.get("tomo")}
    cand = {r["tomo"]: r for r in (cand_data or {}).get("tomograms") or [] if r.get("tomo")}
    sub = {r["tomo"]: r for r in (subtomo_data or {}).get("tomograms") or [] if r.get("tomo")}
    rows: list[dict[str, Any]] = []
    for tomo in sorted(set(tm) | set(cand), key=ts_position_sort_key):
        t, c = tm.get(tomo) or {}, cand.get(tomo)
        scan = t.get("scan") or None
        rows.append(
            {
                "tomo": tomo,
                "sigma": _first(t.get("sigma"), (c or {}).get("sigma")),
                "search_space": _first(t.get("search_space"), (c or {}).get("search_space")),
                "fp1_sigma": _first(t.get("fp1_sigma"), (c or {}).get("fp1_sigma")),
                "n_rotations": t.get("n_rotations"),
                "n_tilts": t.get("n_tilts"),
                "tilt_min": t.get("tilt_min"),
                "tilt_max": t.get("tilt_max"),
                "defocus_um_min": t.get("defocus_um_min"),
                "defocus_um_max": t.get("defocus_um_max"),
                "dose_max": t.get("dose_max"),
                "phase_flip": t.get("phase_flip"),
                "random_phase": t.get("random_phase"),
                "runtime_min": t.get("runtime_min"),
                "node": t.get("node"),
                "done": bool(t.get("done")) if t else c is not None,
                "tm_source": t.get("source", ""),
                "error": t.get("error", ""),
                "scan": scan,
                "top_sigma": top_sigma_of(t, c),
                "n_peaks": (scan or {}).get("n_peaks"),
                "cand": c,
                "sub": sub.get(tomo),
            }
        )
    return rows


def _first(*values):
    return next((v for v in values if v is not None), None)


def tomogram_markers(rows: Sequence[dict[str, Any]]) -> list[Marker]:
    """The cut-independent markers over one job's tomograms (rows of `tomogram_rows`)."""
    out: list[Marker] = []
    sigmas = [float(r["sigma"]) for r in rows if r.get("sigma")]
    if len(sigmas) >= 5:
        med = statistics.median(sigmas)
        mad = statistics.median(abs(s - med) for s in sigmas) * 1.4826
        if mad > 0:
            for r in rows:
                if r.get("sigma"):
                    z = (float(r["sigma"]) - med) / mad
                    if abs(z) > SIGMA_OUTLIER_Z:
                        out.append(
                            Marker("sigma_outlier", r["tomo"], f"σ {r['sigma']:.4g} vs median {med:.4g} ({z:+.1f})")
                        )
    for r in rows:
        top, fp1 = r.get("top_sigma"), r.get("fp1_sigma")
        if top is not None and fp1 is not None and top - fp1 < NO_PEAK_MARGIN:
            out.append(Marker("no_peak", r["tomo"], f"top peak {top:.1f}σ, FP=1 cutoff {fp1:.2f}σ"))
        scan = r.get("scan") or {}
        if scan.get("top_edge"):
            x, y, z = scan.get("top_xyz") or (0, 0, 0)
            out.append(Marker("top_at_edge", r["tomo"], f"maximum {scan.get('top_sigma')}σ at x {x}, y {y}, z {z}"))
    families: dict[str, list[float]] = {}
    for r in rows:
        if r.get("runtime_min") and r.get("node"):
            families.setdefault(node_family(r["node"]), []).append(float(r["runtime_min"]))
    for r in rows:
        fam = node_family(r.get("node"))
        times = families.get(fam) or []
        if r.get("runtime_min") and len(times) >= RUNTIME_MIN_FAMILY:
            med = statistics.median(times)
            if med > 0 and float(r["runtime_min"]) > RUNTIME_OUTLIER_FACTOR * med:
                out.append(
                    Marker(
                        "runtime_outlier",
                        r["tomo"],
                        f"{r['runtime_min']:.0f} min on {fam} (median there {med:.0f} min)",
                    )
                )
    for r in rows:
        c = r.get("cand") or {}
        if c.get("cap_bound"):
            out.append(Marker("cap_bound", r["tomo"], f"{c['n']} picks, the last at {c.get('last_sigma')}σ"))
    return out


def top_sigma_of(tm_row: dict[str, Any], candidate_row: dict[str, Any] | None) -> float | None:
    """The tomogram's highest peak in σ: the score-map scan's when the TM driver wrote one, else
    the extraction's first pick (the same peak, unless it sat inside the edge PyTOM masks)."""
    scan = tm_row.get("scan") or {}
    if scan.get("top_sigma") is not None:
        return float(scan["top_sigma"])
    if candidate_row and candidate_row.get("top_sigma") is not None:
        return float(candidate_row["top_sigma"])
    return None


def _float_or_none(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _int_or_none(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ── Controls: a decoy or mirror template run against the real one ─────────────────────────────────


def _common(target_rows: Sequence[dict[str, Any]], control_rows: Sequence[dict[str, Any]]):
    """(target rows, control rows) of the tomograms the control ran on that the target also has."""
    t = {r["tomo"]: r for r in target_rows}
    c = {r["tomo"]: r for r in control_rows}
    common = sorted(set(t) & set(c))
    return [t[k] for k in common], [c[k] for k in common]


def fdr_curve(
    target_rows: Sequence[dict[str, Any]], decoy_rows: Sequence[dict[str, Any]], *, step: float = 0.1
) -> dict[str, Any]:
    """On the tomograms both ran on: picks at or above z for the real template and for the decoy,
    and their ratio — the share of the real picks at or above z that background alone produces, an
    estimate of the false discovery rate at a cut of z."""
    t, d = _common(target_rows, decoy_rows)
    ts, ds = pooled_scores(t), pooled_scores(d)
    if ts.size == 0:
        return {"z": [], "target": [], "decoy": [], "fdr": [], "n_common": len(t)}
    lo = math.floor(min(float(ts.min()), float(ds.min()) if ds.size else float(ts.min())) / step) * step
    hi = math.ceil(float(ts.max()) / step) * step
    z = [round(lo + i * step, 4) for i in range(round((hi - lo) / step) + 1)]
    t_neg, d_neg = -np.sort(ts)[::-1], -np.sort(ds)[::-1]
    tc = [int(np.searchsorted(t_neg, -zz, side="right")) for zz in z]
    dc = [int(np.searchsorted(d_neg, -zz, side="right")) for zz in z]
    fdr = [round(b / a, 4) if a else None for a, b in zip(tc, dc, strict=True)]
    return {"z": z, "target": tc, "decoy": dc, "fdr": fdr, "n_common": len(t)}


def fdr_at(target_rows, decoy_rows, cut: float) -> tuple[int, int, float | None, int]:
    """(decoy picks ≥ cut, real picks ≥ cut, their ratio, tomograms compared)."""
    t, d = _common(target_rows, decoy_rows)
    nt = counts_at_cut(t, cut).n_picks
    nd = counts_at_cut(d, cut).n_picks
    return nd, nt, (nd / nt if nt else None), len(t)


def hand_comparison(target_rows: Sequence[dict[str, Any]], mirror_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """On the tomograms both ran on, the highest peak of the real template against its mirror's: the
    right hand scores higher in nearly every tomogram."""
    t, m = _common(target_rows, mirror_rows)
    pairs = [
        (a["top_sigma"], b["top_sigma"]) for a, b in zip(t, m, strict=True) if a.get("top_sigma") and b.get("top_sigma")
    ]
    diffs = [a - b for a, b in pairs]
    return {
        "n_common": len(pairs),
        "n_template_higher": sum(1 for d in diffs if d > 0),
        "median_difference": round(statistics.median(diffs), 2) if diffs else None,
    }
