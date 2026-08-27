"""Per-stage metric extractors (roadmap 14 §checks).

Each extractor returns a flat, JSON-able dict. A metric that cannot be read is None — a
band that expects it then FAILS ("metric missing"), never a silent default — and the reason
is appended to the notes. Readers reuse the registry / star / MRC-header paths the
dashboard already trusts; the net-new ones are the tomostar tilt set, the TM job json, the
pick overlap and the map correlation. Never asserts on `rlnCtfMaxResolution` /
`rlnAccumMotion*` (WarpTools star placeholders).
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from services.configs.starfile_service import StarfileService
from services.models_base import JobType
from services.particles.coords import picks_centered_angst
from services.tilt_series import get_registry_for

logger = logging.getLogger(__name__)

_star = StarfileService()

# Blessed reference artifacts (copied by `record`, promoted by `bless`), by file name.
BLESSED_CANDIDATES = "candidates.star"
BLESSED_MERGED = "merged.mrc"
BLESSED_CLASS = "class3d.mrc"
NO_BLESSED = "no blessed reference yet (run `crboost_regress.py bless`)"


def job_dir_of(project_dir: Path, jm) -> Path | None:
    rel = (getattr(jm, "relion_job_name", None) or "").rstrip("/")
    return project_dir / rel if rel else None


def collect_stage_metrics(project_dir: Path, iid: str, jm, *, blessed_dir: Path | None) -> tuple[dict, list[str]]:
    job_dir = job_dir_of(project_dir, jm)
    if job_dir is None or not job_dir.is_dir():
        return {}, ["no job directory recorded for this stage"]
    notes: list[str] = []
    try:
        match jm.job_type:
            case JobType.IMPORT_MOVIES:
                metrics = _import_movies(job_dir)
            case JobType.FS_MOTION_CTF:
                metrics = _fs_motion_ctf(project_dir, iid)
            case JobType.TS_IMPORT:
                metrics = _ts_import(job_dir)
            case JobType.TS_ALIGNMENT:
                metrics = _ts_alignment(project_dir, iid)
            case JobType.TS_CTF:
                metrics = _ts_ctf(project_dir, iid, notes)
            case JobType.TS_RECONSTRUCT:
                metrics = _ts_reconstruct(project_dir, iid, notes)
            case JobType.DENOISE_TRAIN:
                metrics = _denoise_train(job_dir)
            case JobType.DENOISE_PREDICT:
                metrics = _denoise_predict(project_dir, iid, notes)
            case JobType.TEMPLATE_MATCH_PYTOM:
                metrics = _template_match(job_dir, notes)
            case JobType.TEMPLATE_EXTRACT_PYTOM:
                metrics = _candidate_extract(job_dir, jm, blessed_dir, notes)
            case JobType.SUBTOMO_EXTRACTION:
                metrics = _subtomo(job_dir)
            case JobType.RECONSTRUCT_PARTICLE:
                metrics = _reconstruct_particle(job_dir, blessed_dir, notes)
            case JobType.CLASS3D:
                metrics = _class3d(job_dir, jm, blessed_dir, notes)
            case _:
                metrics = {}
                notes.append("no metric extractor for this job type")
    except Exception as e:
        logger.exception("metric extraction failed for %s", iid)
        return {}, [f"metric extraction raised: {e}"]
    return metrics, notes


# ── readers ───────────────────────────────────────────────────────────────────


def _table(path: Path, prefer: str = "particles") -> pd.DataFrame | None:
    if not path.exists():
        return None
    data = _star.read(path)
    df = data.get(prefer)
    if isinstance(df, pd.DataFrame):
        return df
    return next((v for v in data.values() if isinstance(v, pd.DataFrame)), None)


def _header_stats(mrc: Path) -> tuple[float | None, float | None]:
    """(mean, rms) from the MRC header — cheap, no volume read. None when the writer left
    the statistics at zero (a genuinely all-zero volume has rms 0 too; the caller notes it)."""
    import mrcfile

    with mrcfile.open(str(mrc), header_only=True, permissive=True, mode="r") as m:
        mean, rms = float(m.header.dmean), float(m.header.rms)
    if rms == 0.0:
        return None, None
    return _r(mean), _r(rms)


def _map_cc(a: Path, b: Path) -> float | None:
    """Pearson correlation of two same-shape volumes (blessed vs run)."""
    import mrcfile

    with mrcfile.open(str(a), permissive=True, mode="r") as ma, mrcfile.open(str(b), permissive=True, mode="r") as mb:
        x = np.asarray(ma.data, dtype=np.float64).ravel()
        y = np.asarray(mb.data, dtype=np.float64).ravel()
    if x.shape != y.shape:
        return None
    x = x - x.mean()
    y = y - y.mean()
    denom = float(np.linalg.norm(x) * np.linalg.norm(y))
    return _r(float(x @ y / denom)) if denom > 0 else None


def _pick_overlap(run_star: Path, blessed_star: Path, radius_ang: float) -> float | None:
    """Fraction of blessed picks with a run pick within `radius_ang`, matched per tomogram."""
    run = _table(run_star)
    blessed = _table(blessed_star)
    if run is None or blessed is None or len(blessed) == 0:
        return None
    r2 = radius_ang * radius_ang
    hits = 0
    for tomo, group in blessed.groupby("rlnTomoName"):
        b = picks_centered_angst(group)
        a_rows = run[run["rlnTomoName"] == tomo]
        if len(a_rows) == 0:
            continue
        a = picks_centered_angst(a_rows)
        d2 = ((b[:, None, :] - a[None, :, :]) ** 2).sum(axis=-1)
        hits += int((d2.min(axis=1) <= r2).sum())
    return _r(hits / len(blessed))


# ── per-stage ─────────────────────────────────────────────────────────────────


def _import_movies(job_dir: Path) -> dict[str, Any]:
    g = _table(job_dir / "tilt_series.star", prefer="global")
    n_tilts = 0
    for p in sorted((job_dir / "tilt_series").glob("*.star")):
        df = _table(p)
        n_tilts += 0 if df is None else len(df)
    return {"n_tilt_series": 0 if g is None else len(g), "n_tilts": n_tilts}


def _fs_motion_ctf(project_dir: Path, iid: str) -> dict[str, Any]:
    reg = get_registry_for(project_dir)
    defocus: list[float] = []
    res: list[float] = []
    motion: list[float] = []
    for ts in reg.all_tilt_series():
        for f in ts.frames:
            out = f.outputs.get(iid)
            if out is None or getattr(out, "output_type", "") != "fs_motion_ctf":
                continue
            defocus.append((out.defocus_u_angstrom + out.defocus_v_angstrom) / 2e4)  # µm
            if out.ctf_resolution is not None:
                res.append(out.ctf_resolution)
            if out.mean_frame_movement is not None:
                motion.append(out.mean_frame_movement)
    return {
        "n_frames": len(defocus),
        "defocus_um_mean": _mean(defocus),
        "defocus_um_min": _r(min(defocus)) if defocus else None,
        "defocus_um_max": _r(max(defocus)) if defocus else None,
        "ctf_res_ang_median": _median(res),
        "ctf_res_ang_max": _r(max(res)) if res else None,
        "motion_median": _median(motion),
    }


def _ts_import(job_dir: Path) -> dict[str, Any]:
    ids: list[str] = []
    files = sorted((job_dir / "tomostar").glob("*.tomostar"))
    for p in files:
        df = _table(p)
        if df is None or "wrpMovieName" not in df.columns:
            continue
        ids += [Path(str(m)).stem for m in df["wrpMovieName"]]
    return {"n_tomostars": len(files), "n_tilts": len(ids), "tilt_ids": sorted(ids)}


def _ts_alignment(project_dir: Path, iid: str) -> dict[str, Any]:
    reg = get_registry_for(project_dir)
    shifts: list[float] = []
    tilt_y: list[float] = []
    z_rot: list[float] = []
    n_ts = 0
    for ts in reg.all_tilt_series():
        out = ts.outputs.get(iid)
        if out is None or getattr(out, "output_type", "") != "ts_alignment":
            continue
        n_ts += 1
        for e in out.per_frame:
            shifts.append(math.hypot(e.x_shift_angstrom, e.y_shift_angstrom))
            tilt_y.append(e.tilt_y_deg)
            z_rot.append(e.z_rot_deg)
    return {
        "n_tilt_series": n_ts,
        "n_tilts": len(shifts),
        "shift_ang_max": _r(max(shifts)) if shifts else None,
        "shift_ang_median": _median(shifts),
        "tilt_y_span_deg": _r(max(tilt_y) - min(tilt_y)) if tilt_y else None,
        "tilt_axis_deg_mean": _mean(z_rot),
    }


def _ts_ctf(project_dir: Path, iid: str, notes: list[str]) -> dict[str, Any]:
    reg = get_registry_for(project_dir)
    defocus: list[float] = []
    inverted: set[bool] = set()
    n_ts = 0
    for ts in reg.all_tilt_series():
        out = ts.outputs.get(iid)
        if out is None or getattr(out, "output_type", "") != "ts_ctf":
            continue
        n_ts += 1
        inverted.add(bool(out.are_angles_inverted))
        for e in out.per_frame:
            defocus.append((e.defocus_u_angstrom + e.defocus_v_angstrom) / 2e4)
    if len(inverted) > 1:
        notes.append("are_angles_inverted differs between tilt-series")
    return {
        "n_tilt_series": n_ts,
        "n_tilts": len(defocus),
        "defocus_um_mean": _mean(defocus),
        "defocus_um_min": _r(min(defocus)) if defocus else None,
        "defocus_um_max": _r(max(defocus)) if defocus else None,
        "are_angles_inverted": next(iter(inverted)) if len(inverted) == 1 else None,
    }


def _ts_reconstruct(project_dir: Path, iid: str, notes: list[str]) -> dict[str, Any]:
    reg = get_registry_for(project_dir)
    outs = [
        ts.tomogram.outputs.get(iid)
        for ts in reg.all_tilt_series()
        if ts.tomogram is not None and ts.tomogram.outputs.get(iid) is not None
    ]
    outs = [o for o in outs if getattr(o, "output_type", "") == "ts_reconstruct"]
    if not outs:
        return {"n_tomograms": 0}
    first = outs[0]
    mean, rms = (None, None)
    if Path(first.reconstructed_mrc).exists():
        mean, rms = _header_stats(Path(first.reconstructed_mrc))
        if rms is None:
            notes.append("reconstruction MRC header carries no statistics (rms 0)")
    else:
        notes.append(f"reconstructed volume missing: {first.reconstructed_mrc}")
    return {
        "n_tomograms": len(outs),
        "size_x": int(first.size_x),
        "size_y": int(first.size_y),
        "size_z": int(first.size_z),
        "pixel_size_ang": _r(first.tomogram_pixel_size_angstrom),
        "intensity_mean": mean,
        "intensity_rms": rms,
    }


def _denoise_train(job_dir: Path) -> dict[str, Any]:
    model = job_dir / "denoising_model.tar.gz"
    return {"model_exists": model.exists(), "model_bytes": model.stat().st_size if model.exists() else None}


def _denoise_predict(project_dir: Path, iid: str, notes: list[str]) -> dict[str, Any]:
    reg = get_registry_for(project_dir)
    paths = [
        Path(ts.tomogram.outputs[iid].denoised_mrc)
        for ts in reg.all_tilt_series()
        if ts.tomogram is not None
        and iid in ts.tomogram.outputs
        and getattr(ts.tomogram.outputs[iid], "output_type", "") == "denoise_predict"
    ]
    present = [p for p in paths if p.exists()]
    mean = rms = None
    if present:
        mean, rms = _header_stats(present[0])
        if rms is None:
            notes.append("denoised MRC header carries no statistics (rms 0)")
    elif paths:
        notes.append(f"denoised volume missing: {paths[0]}")
    return {"n_denoised": len(present), "denoised_mean": mean, "denoised_rms": rms}


def _template_match(job_dir: Path, notes: list[str]) -> dict[str, Any]:
    tm_dir = job_dir / "tmResults"
    jsons = sorted(tm_dir.glob("*_job.json"))
    rotations: set[int] = set()
    score_max: list[float] = []
    score_rms: list[float] = []
    for p in jsons:
        with open(p) as f:
            data = json.load(f)
        if data.get("n_rotations") is not None:
            rotations.add(int(data["n_rotations"]))
        scores = tm_dir / f"{p.name[: -len('_job.json')]}_scores.mrc"
        if scores.exists():
            import mrcfile

            with mrcfile.open(str(scores), header_only=True, permissive=True, mode="r") as m:
                if float(m.header.rms) > 0:
                    score_max.append(float(m.header.dmax))
                    score_rms.append(float(m.header.rms))
    if jsons and not score_max:
        notes.append("score maps carry no header statistics")
    if len(rotations) > 1:
        notes.append(f"n_rotations differs between tomograms: {sorted(rotations)}")
    return {
        "n_tomograms": len(jsons),
        "n_rotations": next(iter(rotations)) if len(rotations) == 1 else None,
        "score_max": _r(max(score_max)) if score_max else None,
        "score_rms_mean": _mean(score_rms),
    }


def _candidate_extract(job_dir: Path, jm, blessed_dir: Path | None, notes: list[str]) -> dict[str, Any]:
    star = job_dir / "candidates.star"
    df = _table(star)
    n = 0 if df is None else len(df)
    lcc = df["rlnLCCmax"].astype(float).tolist() if df is not None and "rlnLCCmax" in df.columns else []
    overlap = None
    blessed = blessed_dir / BLESSED_CANDIDATES if blessed_dir else None
    if blessed is not None and blessed.exists() and df is not None:
        radius = 0.5 * float(getattr(jm, "particle_diameter_ang", 0.0) or 0.0)
        overlap = _pick_overlap(star, blessed, radius) if radius > 0 else None
        if radius <= 0:
            notes.append("particle_diameter_ang is 0 — overlap radius undefined")
    else:
        notes.append(NO_BLESSED)
    return {
        "n_candidates": n,
        "lcc_max": _r(max(lcc)) if lcc else None,
        "lcc_median": _median(lcc),
        "overlap_frac": overlap,
    }


def _subtomo(job_dir: Path) -> dict[str, Any]:
    df = _table(job_dir / "particles.star")
    return {"n_particles": 0 if df is None else len(df)}


def _reconstruct_particle(job_dir: Path, blessed_dir: Path | None, notes: list[str]) -> dict[str, Any]:
    merged = job_dir / "merged.mrc"
    cc = None
    blessed = blessed_dir / BLESSED_MERGED if blessed_dir else None
    if merged.exists() and blessed is not None and blessed.exists():
        cc = _map_cc(blessed, merged)
        if cc is None:
            notes.append("merged.mrc shape differs from the blessed map")
    elif merged.exists():
        notes.append(NO_BLESSED)
    return {"merged_exists": merged.exists(), "cc_vs_blessed": cc}


def _class3d(job_dir: Path, jm, blessed_dir: Path | None, notes: list[str]) -> dict[str, Any]:
    n_iter = int(getattr(jm, "n_iterations", 0) or 0)
    model = job_dir / f"run_it{n_iter:03d}_model.star"
    data_star = job_dir / f"run_it{n_iter:03d}_data.star"
    out: dict[str, Any] = {
        "completed": model.exists(),
        "resolution_ang": None,
        "accuracy_rot_deg": None,
        "accuracy_trans_ang": None,
        "n_particles": None,
        "cc_vs_blessed": None,
    }
    if not model.exists():
        notes.append(f"final model star missing: {model.name}")
        return out
    data = _star.read(model)
    general = data.get("model_general") or {}
    if isinstance(general, dict) and "rlnCurrentResolution" in general:
        out["resolution_ang"] = _r(float(general["rlnCurrentResolution"]))
    classes = data.get("model_classes")
    if isinstance(classes, pd.DataFrame) and len(classes):
        if "rlnAccuracyRotations" in classes.columns:
            out["accuracy_rot_deg"] = _r(float(classes["rlnAccuracyRotations"].iloc[0]))
        if "rlnAccuracyTranslationsAngst" in classes.columns:
            out["accuracy_trans_ang"] = _r(float(classes["rlnAccuracyTranslationsAngst"].iloc[0]))
    parts = _table(data_star)
    if parts is not None:
        out["n_particles"] = len(parts)
    class_map = job_dir / f"run_it{n_iter:03d}_class001.mrc"
    blessed = blessed_dir / BLESSED_CLASS if blessed_dir else None
    if class_map.exists() and blessed is not None and blessed.exists():
        out["cc_vs_blessed"] = _map_cc(blessed, class_map)
        if out["cc_vs_blessed"] is None:
            notes.append("class map shape differs from the blessed map")
    elif class_map.exists():
        notes.append(NO_BLESSED)
    return out


# ── arithmetic ────────────────────────────────────────────────────────────────


def _r(v: float | None) -> float | None:
    return None if v is None else round(float(v), 4)


def _mean(values: list[float]) -> float | None:
    return _r(sum(values) / len(values)) if values else None


def _median(values: list[float]) -> float | None:
    return _r(float(np.median(values))) if values else None
