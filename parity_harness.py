#!/usr/bin/env python3
"""Roadmap 02 stage 0 — read-only star-vs-registry parity harness.

For one project, compares every fact the Journey dashboard currently derives by
re-reading emitted STARs / Warp XMLs / filename surgery against the value the
TiltSeriesRegistry stores. Every divergence is a triage item BEFORE any consumer
is switched to registry reads:

    star-only  → registry backfill gap (feeds the consolidation backfill)
    DIFF       → adapter bug (registry wrong) OR star wrong — decide per fact
    reg-only   → registry richer than stars (evidence for the switch; record it)

Deliberately imports the DASHBOARD'S OWN reader functions (ui.tomo_dashboard_dialog
helpers, services.dashboard_data, frameseries_quality) rather than re-implementing
them — parity is only meaningful against the real read paths. Read-only: never
writes to the project.

Usage:
    venv/bin/python parity_harness.py projects/try2_after_pixShift
    venv/bin/python parity_harness.py projects/pos9_10_after_pixShift --verbose
"""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from services.dashboard_data import find_job_by_type, job_dir_for, read_tomograms_table
from services.models_base import JobType
from services.project_state import get_project_state_for
from services.tilt_series.build import infer_position
from services.tilt_series.frameseries_quality import WARP_FRAMESERIES_DIR, read_frame_quality
from services.tilt_series.registry import TiltSeriesRegistry
from ui.tomo_dashboard_dialog import (
    _per_tilt_star_path,
    _read_per_tilt_df,
    _read_per_tilt_kept_dropped,
    _read_tomohand_from_import_star,
    _resolve_denoised_mrc_for_job,
    _resolve_tilt_filter_dir,
)

# (fact, ts_id, item, star_value, registry_value, status)
Row = tuple[str, str, str, str, str, str]

DIFF_CAP = 25  # non-verbose print cap per fact; total counts are always shown


def _close(a, b, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        return abs(float(a) - float(b)) <= tol
    except (TypeError, ValueError):
        return str(a) == str(b)


def _fmt(v) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)


def _row(rows: list[Row], fact: str, ts: str, item: str, star, reg, tol: float | None = None) -> None:
    matches = _close(star, reg, tol) if tol is not None else _fmt(star) == _fmt(reg)
    if matches:
        status = "ok"
    else:
        status = "star-only" if reg is None else "reg-only" if star is None else "DIFF"
    rows.append((fact, ts, item, _fmt(star), _fmt(reg), status))


def _movie_stem_map(df) -> dict[int, str] | None:
    """Row position → frame stem, when the star carries movie names."""
    if df is None or "rlnMicrographMovieName" not in df.columns:
        return None
    return {i: Path(str(v)).stem for i, v in enumerate(df["rlnMicrographMovieName"].tolist())}


# ── Facts ────────────────────────────────────────────────────────────────────


def fact_position(reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """A: Position_* regex (dashboard) vs TiltSeries.stage_position/beam_position."""
    for ts in reg.all_tilt_series():
        try:
            stage, beam = infer_position(ts.id)
        except Exception:
            stage = beam = None
        _row(rows, "position", ts.id, "stage", stage, ts.stage_position, tol=0)
        _row(rows, "position", ts.id, "beam", beam, ts.beam_position, tol=0)


def fact_fsm_defocus(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """B: fsMotion per-tilt star defocus vs FsMotionCtfFrameOutput."""
    found = find_job_by_type(state, JobType.FS_MOTION_CTF)
    if not found:
        return
    iid, jm = found
    jd = job_dir_for(state, iid, jm, project_path)
    if jd is None:
        return
    for ts in reg.all_tilt_series():
        df = _read_per_tilt_df(_per_tilt_star_path(jd, ts.id))
        frames = {f.id: f for f in ts.frames}
        if df is None:
            for f in ts.frames:
                if iid in f.outputs:
                    _row(rows, "fsm_defocus", ts.id, f"{f.id} (no star)", None, f.outputs[iid].defocus_u_angstrom)
            continue
        stems = _movie_stem_map(df)
        for i in range(len(df)):
            stem = stems.get(i) if stems else None
            label = stem or f"row{i}"
            frame = frames.get(stem) if stem else None
            out = frame.outputs.get(iid) if frame else None
            if frame is None:
                _row(rows, "fsm_defocus", ts.id, f"{label} (frame not in registry)", "present", None)
                continue
            du = df.iloc[i].get("rlnDefocusU")
            dv = df.iloc[i].get("rlnDefocusV")
            _row(rows, "fsm_defocus", ts.id, f"{label}:defU", du, out.defocus_u_angstrom if out else None, tol=1.0)
            _row(rows, "fsm_defocus", ts.id, f"{label}:defV", dv, out.defocus_v_angstrom if out else None, tol=1.0)


def fact_fsm_quality(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """C: Warp per-movie XML CTF-res/motion (dashboard XML read) vs registry QC fields."""
    found = find_job_by_type(state, JobType.FS_MOTION_CTF)
    if not found:
        return
    iid, jm = found
    jd = job_dir_for(state, iid, jm, project_path)
    if jd is None:
        return
    warp_dir = jd / WARP_FRAMESERIES_DIR
    for ts in reg.all_tilt_series():
        for f in ts.frames:
            q = read_frame_quality(warp_dir, f.id)
            out = f.outputs.get(iid)
            _row(
                rows,
                "fsm_quality",
                ts.id,
                f"{f.id}:ctfres",
                q.ctf_resolution if q else None,
                out.ctf_resolution if out else None,
                tol=0.01,
            )
            _row(
                rows,
                "fsm_quality",
                ts.id,
                f"{f.id}:motion",
                q.mean_frame_movement if q else None,
                out.mean_frame_movement if out else None,
                tol=0.01,
            )


def _per_frame_star_vs_reg(
    df, per_frame_by_id, per_frame_by_z, cols: dict[str, str], fact: str, ts_id: str, rows: list[Row], tol: float
) -> None:
    """Shared D/E matcher: star rows → registry per_frame entries by movie stem, else Z index."""
    stems = _movie_stem_map(df)
    for i in range(len(df)):
        stem = stems.get(i) if stems else None
        entry = per_frame_by_id.get(stem) if stem else per_frame_by_z.get(i)
        label = stem or f"z{i}"
        if entry is None:
            _row(rows, fact, ts_id, f"{label} (no registry per-frame)", "present", None)
            continue
        for star_col, reg_attr in cols.items():
            if star_col not in df.columns:
                continue
            _row(rows, fact, ts_id, f"{label}:{reg_attr}", df.iloc[i].get(star_col), getattr(entry, reg_attr), tol=tol)


def fact_tsctf(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """D: tsCtf per-tilt star defocus vs TsCtfTiltSeriesOutput.per_frame."""
    found = find_job_by_type(state, JobType.TS_CTF)
    if not found:
        return
    iid, jm = found
    jd = job_dir_for(state, iid, jm, project_path)
    if jd is None:
        return
    for ts in reg.all_tilt_series():
        df = _read_per_tilt_df(_per_tilt_star_path(jd, ts.id))
        out = ts.outputs.get(iid)
        if df is None:
            if out is not None and out.per_frame:
                _row(rows, "tsctf_defocus", ts.id, "(no star, registry has per_frame)", None, len(out.per_frame))
            continue
        if out is None or getattr(out, "output_type", "") != "ts_ctf":
            _row(rows, "tsctf_defocus", ts.id, "(star present, no registry output)", "present", None)
            continue
        by_id = {e.frame_id: e for e in out.per_frame}
        by_z = {e.z_index: e for e in out.per_frame}
        _per_frame_star_vs_reg(
            df,
            by_id,
            by_z,
            {"rlnDefocusU": "defocus_u_angstrom", "rlnDefocusV": "defocus_v_angstrom"},
            "tsctf_defocus",
            ts.id,
            rows,
            tol=1.0,
        )


def fact_alignment(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """E: alignment per-tilt star vs TsAlignmentTiltSeriesOutput.per_frame."""
    found = find_job_by_type(state, JobType.TS_ALIGNMENT)
    if not found:
        return
    iid, jm = found
    jd = job_dir_for(state, iid, jm, project_path)
    if jd is None:
        return
    for ts in reg.all_tilt_series():
        df = _read_per_tilt_df(_per_tilt_star_path(jd, ts.id))
        out = ts.outputs.get(iid)
        if df is None:
            if out is not None and getattr(out, "per_frame", None):
                _row(rows, "alignment", ts.id, "(no star, registry has per_frame)", None, len(out.per_frame))
            continue
        if out is None or getattr(out, "output_type", "") != "ts_alignment":
            _row(rows, "alignment", ts.id, "(star present, no registry output)", "present", None)
            continue
        by_id = {e.frame_id: e for e in out.per_frame}
        by_z = {e.z_index: e for e in out.per_frame}
        _per_frame_star_vs_reg(
            df,
            by_id,
            by_z,
            {
                "rlnTomoXTilt": "tilt_x_deg",
                "rlnTomoYTilt": "tilt_y_deg",
                "rlnTomoZRot": "z_rot_deg",
                "rlnTomoXShiftAngst": "x_shift_angstrom",
                "rlnTomoYShiftAngst": "y_shift_angstrom",
            },
            "alignment",
            ts.id,
            rows,
            tol=0.05,
        )


def fact_filter(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """F: tilt-filter labeled/filtered stars vs Frame.is_filtered_out/filter_probability."""
    filter_dir = _resolve_tilt_filter_dir(state, project_path)
    if filter_dir is None:
        return
    for ts in reg.all_tilt_series():
        kd = _read_per_tilt_kept_dropped(filter_dir, ts.id)
        if kd is None:
            for f in ts.frames:
                if f.is_filtered_out:
                    _row(rows, "filter", ts.id, f"{f.id} (no labeled star)", None, "filtered_out")
            continue
        lab = kd["labeled_df"]
        dropped_frames = {d["frame"] for d in kd["dropped"]}
        frames = {f.id: f for f in ts.frames}
        for i in range(len(lab)):
            movie = str(lab.iloc[i].get("rlnMicrographMovieName", ""))
            stem = Path(movie).stem
            frame = frames.get(stem)
            star_dropped = Path(movie).name in dropped_frames
            if frame is None:
                _row(rows, "filter", ts.id, f"{stem} (frame not in registry)", "present", None)
                continue
            _row(rows, "filter", ts.id, f"{stem}:dropped", star_dropped, frame.is_filtered_out, tol=0)
            if "cryoBoostDlProbability" in lab.columns:
                try:
                    p = float(lab.iloc[i]["cryoBoostDlProbability"])
                except (TypeError, ValueError):
                    p = None
                _row(rows, "filter", ts.id, f"{stem}:prob", p, frame.filter_probability, tol=0.005)


def fact_tomohand(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """G: import-star rlnTomoHand vs TsCtfTiltSeriesOutput.are_angles_inverted.
    These are different authorities (import intention vs Warp's ts_defocus_hand
    decision) — divergence here is a finding, not automatically a bug."""
    imp = find_job_by_type(state, JobType.IMPORT_MOVIES)
    star_hand = None
    if imp:
        imp_dir = job_dir_for(state, imp[0], imp[1], project_path)
        if imp_dir:
            star_hand = _read_tomohand_from_import_star(imp_dir / "tilt_series.star")
    tsctf = find_job_by_type(state, JobType.TS_CTF)
    for ts in reg.all_tilt_series():
        reg_hand = None
        if tsctf:
            out = ts.outputs.get(tsctf[0])
            if out is not None and getattr(out, "output_type", "") == "ts_ctf":
                reg_hand = -1 if out.are_angles_inverted else 1
        _row(rows, "tomohand", ts.id, "hand", star_hand, reg_hand, tol=0)


def fact_denoise(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """H: denoised MRC via star + stem surgery vs DenoisePredictTomogramOutput.denoised_mrc."""
    for iid, jm in (state.jobs or {}).items():
        is_dn = getattr(jm, "job_type", None) == JobType.DENOISE_PREDICT
        if not is_dn and iid.split("__")[0] != JobType.DENOISE_PREDICT.value:
            continue
        jd = job_dir_for(state, iid, jm, project_path)
        if not jd:
            continue
        for ts in reg.all_tilt_series():
            star_mrc = _resolve_denoised_mrc_for_job(jd, project_path, ts.id)
            out = ts.tomogram.outputs.get(iid) if ts.tomogram else None
            is_dn_out = out is not None and getattr(out, "output_type", "") == "denoise_predict"
            reg_mrc = out.denoised_mrc if is_dn_out else None
            if star_mrc is None and reg_mrc is None:
                continue
            s = str(star_mrc.resolve()) if star_mrc else None
            r = str(Path(reg_mrc).resolve()) if reg_mrc else None
            _row(rows, "denoise_path", ts.id, iid, s, r)


def fact_recon(state, project_path: Path, reg: TiltSeriesRegistry, rows: list[Row]) -> None:
    """I: tomograms.star reconstructed path vs TsReconstructTomogramOutput.reconstructed_mrc."""
    found = find_job_by_type(state, JobType.TS_RECONSTRUCT)
    if not found:
        return
    iid, jm = found
    jd = job_dir_for(state, iid, jm, project_path)
    if jd is None:
        return
    df = read_tomograms_table(jd / "tomograms.star")
    star_by_ts: dict[str, str] = {}
    if df is not None and {"rlnTomoName", "rlnTomoReconstructedTomogram"}.issubset(df.columns):
        for _, r in df.iterrows():
            star_by_ts[str(r["rlnTomoName"])] = str(r["rlnTomoReconstructedTomogram"])
    for ts in reg.all_tilt_series():
        out = ts.tomogram.outputs.get(iid) if ts.tomogram else None
        is_rec_out = out is not None and getattr(out, "output_type", "") == "ts_reconstruct"
        reg_mrc = out.reconstructed_mrc if is_rec_out else None
        star_rel = star_by_ts.get(ts.id)
        if star_rel is None and reg_mrc is None:
            continue
        s = str((project_path / star_rel).resolve()) if star_rel else None
        r = str(Path(reg_mrc).resolve()) if reg_mrc else None
        _row(rows, "recon_path", ts.id, iid, s, r)


# ── Inventory + report ───────────────────────────────────────────────────────


def star_only_ts(state, project_path: Path, reg: TiltSeriesRegistry) -> list[str]:
    """TS present in the fsMotion job's per-tilt stars but absent from the registry."""
    found = find_job_by_type(state, JobType.FS_MOTION_CTF)
    if not found:
        return []
    jd = job_dir_for(state, found[0], found[1], project_path)
    if jd is None:
        return []
    star_ids = {p.stem for p in (jd / "tilt_series").glob("*.star")}
    return sorted(star_ids - set(reg.tilt_series_ids()))


FACTS = [
    ("position", fact_position, "registry-only"),
    ("fsm_defocus", fact_fsm_defocus, "state"),
    ("fsm_quality", fact_fsm_quality, "state"),
    ("tsctf_defocus", fact_tsctf, "state"),
    ("alignment", fact_alignment, "state"),
    ("filter", fact_filter, "state"),
    ("tomohand", fact_tomohand, "state"),
    ("denoise_path", fact_denoise, "state"),
    ("recon_path", fact_recon, "state"),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("project", type=Path)
    ap.add_argument("--verbose", action="store_true", help="print every divergent row (no cap)")
    args = ap.parse_args()

    project_path = args.project.resolve()
    if not (project_path / "project_params.json").exists():
        print(f"ERROR: {project_path} has no project_params.json")
        return 2

    print(f"== PARITY {project_path.name} ==")
    reg = TiltSeriesRegistry(project_path)
    reg.load()
    ts_ids = reg.tilt_series_ids()
    if not ts_ids:
        print("NO REGISTRY DATA (pre-registry project) — every dashboard fact is star-only here.")
        print("Triage: this project class needs the stage-0 fallback decision (backfill-on-open vs star fallback).")
        return 0
    print(f"registry: {len(ts_ids)} TS, {reg.frame_count()} frames")

    problems = reg.sanity_check()
    for p in problems:
        print(f"  SANITY: {p}")

    state = get_project_state_for(project_path)

    orphans = star_only_ts(state, project_path, reg)
    for o in orphans:
        print(f"  STAR-ONLY TS (not in registry): {o}")

    rows: list[Row] = []
    for fact_name, fn, kind in FACTS:
        try:
            if kind == "registry-only":
                fn(reg, rows)
            else:
                fn(state, project_path, reg, rows)
        except Exception:
            print(f"  ERROR in fact {fact_name}:")
            traceback.print_exc()

    print(f"\n{'fact':<14} {'ok':>6} {'DIFF':>6} {'star-only':>10} {'reg-only':>9}")
    by_fact: dict[str, list[Row]] = {}
    for r in rows:
        by_fact.setdefault(r[0], []).append(r)
    for fact_name, _fn, _k in FACTS:
        frows = by_fact.get(fact_name, [])
        if not frows:
            print(f"{fact_name:<14} {'—':>6}  (job absent or nothing to compare)")
            continue
        n = {"ok": 0, "DIFF": 0, "star-only": 0, "reg-only": 0}
        for r in frows:
            n[r[5]] += 1
        print(f"{fact_name:<14} {n['ok']:>6} {n['DIFF']:>6} {n['star-only']:>10} {n['reg-only']:>9}")

    bad = [r for r in rows if r[5] != "ok"]
    if bad:
        print(f"\nDIVERGENCES ({len(bad)}):")
        shown_per_fact: dict[str, int] = {}
        for fact_name, ts, item, star, regv, status in bad:
            shown_per_fact[fact_name] = shown_per_fact.get(fact_name, 0) + 1
            if not args.verbose and shown_per_fact[fact_name] > DIFF_CAP:
                continue
            print(f"  [{status:<9}] {fact_name:<13} {ts:<28} {item:<40} star={star}  reg={regv}")
        if not args.verbose:
            for fact_name, count in shown_per_fact.items():
                if count > DIFF_CAP:
                    print(f"  ... {fact_name}: {count - DIFF_CAP} more rows suppressed (--verbose for all)")
    else:
        print("\nFULL PARITY — no divergences.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
