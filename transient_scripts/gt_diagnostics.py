#!/usr/bin/env python3
"""
crboost_diagnostics.py
Compare a crboost_server project against the ground truth project.

Usage:
    python crboost_diagnostics.py --project auto_flip_cusesum
    python crboost_diagnostics.py --project all_together --gt-dir /groups/klumpe/software/Setup/Testing/test1/run12
    python crboost_diagnostics.py --project all_together --sections scores templates masks defocus jobs picks tomo xmlangles ctfingested
"""

import argparse
import glob
import json
import os
import statistics
import xml.etree.ElementTree as ET
from pathlib import Path

try:
    import mrcfile
    import numpy as np
    HAS_MRC = True
except ImportError:
    HAS_MRC = False
    print("[WARN] mrcfile/numpy not available -- score/template/mask/tomo sections will be skipped\n")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
PROJECTS_ROOT = "/users/artem.kushner/dev/crboost_server/projects"
GT_DIR        = "/groups/klumpe/software/Setup/Testing/test1/run12"
GT_TM_JOB     = "External/job006"
GT_EXT_JOB    = "External/job007"
GT_SCORE_MAP  = "tmResults/Position_1_11.80Apx_scores.mrc"
GT_JOB_JSON   = "tmResults/Position_1_11.80Apx_job.json"
GT_TEMPLATES  = {
    "black": "templates/ellipsoid_550_550_550_apix11.8_black.mrc",
    "mask":  "templates/ellipsoid_550_550_550_apix11.8_mask.mrc",
    "white": "templates/ellipsoid_550_550_550_apix11.8.mrc",
}
GT_TOMO_RECON = "External/job005/warp_tiltseries/reconstruction/Position_1_11.80Apx.mrc"
GT_TILT_SERIES_XML = "External/job003/warp_tiltseries/Position_1.xml"
GT_TS_CTF_XML      = "External/job004/warp_tiltseries/Position_1.xml"

# ---------------------------------------------------------------------------

def section_header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def find_in_project(project_dir, glob_pattern, require_sibling=None):
    """Glob within project_dir, optionally requiring a sibling file."""
    hits = sorted(glob.glob(os.path.join(project_dir, glob_pattern), recursive=True))
    if require_sibling:
        hits = [h for h in hits if os.path.exists(h.replace(*require_sibling))]
    return hits


def mrc_stats(path):
    with mrcfile.open(path, permissive=True) as m:
        d = m.data.copy()
        apix = float(m.voxel_size.x)
    return d, apix


def print_mrc_stats(label, path, d, apix, percentiles=(95, 97, 99, 99.5)):
    nz = int(np.count_nonzero(d))
    print(f"\n  {label}: {path}")
    print(f"    shape={d.shape}  apix={apix:.3f}  dtype={d.dtype}")
    print(f"    min={d.min():.6f}  max={d.max():.6f}  mean={d.mean():.6f}  std={d.std():.6f}")
    print(f"    nonzero={nz}/{d.size} ({100*nz/d.size:.2f}%)")
    for pct in percentiles:
        print(f"    p{pct:.1f} = {np.percentile(d, pct):.6f}")


# ---------------------------------------------------------------------------
# 1. Score map comparison
# ---------------------------------------------------------------------------
def compare_scores(project_dir, gt_dir):
    if not HAS_MRC:
        return
    section_header("SCORE MAP STATISTICS")

    gt_path = os.path.join(gt_dir, GT_TM_JOB, GT_SCORE_MAP)
    cur_hits = find_in_project(project_dir, "External/job0**/tmResults/*_scores.mrc",
                                require_sibling=("_scores.mrc", "_job.json"))
    cur_path = cur_hits[0] if cur_hits else None

    results = {}
    for label, p in [("GT", gt_path), ("CURRENT", cur_path)]:
        if not p or not os.path.exists(p):
            print(f"  [MISS] {label}: {p}")
            continue
        d, apix = mrc_stats(p)
        print_mrc_stats(label, p, d, apix)
        results[label] = d

    if "GT" in results and "CURRENT" in results:
        ratio = results["CURRENT"].std() / results["GT"].std()
        flag = "  <<< SUSPICIOUS (>30% deviation)" if not 0.7 < ratio < 1.3 else ""
        print(f"\n  std ratio (current/GT): {ratio:.3f}{flag}")

        max_ratio = results["CURRENT"].max() / results["GT"].max()
        print(f"  max ratio (current/GT): {max_ratio:.3f}")

        mean_ratio = results["CURRENT"].mean() / results["GT"].mean()
        flag_mean = "  <<< elevated background" if mean_ratio > 3 else ""
        print(f"  mean ratio (current/GT): {mean_ratio:.3f}{flag_mean}")


# ---------------------------------------------------------------------------
# 2. Template comparison
# ---------------------------------------------------------------------------
def compare_templates(project_dir, gt_dir):
    if not HAS_MRC:
        return
    section_header("TEMPLATE COMPARISON")

    cur_by_key = {
        "black": find_in_project(project_dir, "templates/*black*.mrc"),
        "mask":  find_in_project(project_dir, "templates/*mask*.mrc"),
        "white": find_in_project(project_dir, "templates/*white*.mrc"),
    }

    for key in ("black", "mask", "white"):
        gt_p  = os.path.join(gt_dir, GT_TEMPLATES[key]) if key in GT_TEMPLATES else None
        cur_p = cur_by_key[key][0] if cur_by_key[key] else None
        print(f"\n-- {key} --")

        results = {}
        for label, p in [("GT", gt_p), ("CURRENT", cur_p)]:
            if not p or not os.path.exists(p):
                print(f"  [MISS] {label}: {p}")
                continue
            d, apix = mrc_stats(p)
            print_mrc_stats(label, p, d, apix, percentiles=())
            results[label] = (d, apix)

        if "GT" in results and "CURRENT" in results:
            gd, ga = results["GT"]
            cd, ca = results["CURRENT"]
            shape_ok = gd.shape == cd.shape
            apix_ok  = abs(ga - ca) < 0.05
            print(f"\n  shape match: {'OK' if shape_ok else f'<<< DIFF  GT={gd.shape} CURRENT={cd.shape}'}")
            print(f"  apix  match: {'OK' if apix_ok  else f'<<< DIFF  GT={ga:.3f} CURRENT={ca:.3f}'}")
            if shape_ok:
                mae  = float(np.mean(np.abs(gd - cd)))
                rmse = float(np.sqrt(np.mean((gd - cd)**2)))
                corr = float(np.corrcoef(gd.ravel(), cd.ravel())[0, 1])
                print(f"  MAE={mae:.6f}  RMSE={rmse:.6f}  correlation={corr:.6f}")
                if corr < 0.99:
                    print(f"  <<< WARNING: templates differ meaningfully (corr < 0.99)")


# ---------------------------------------------------------------------------
# 3. Mask comparison
# ---------------------------------------------------------------------------
def compare_masks(project_dir, gt_dir):
    if not HAS_MRC:
        return
    section_header("MASK COMPARISON")

    gt_p  = os.path.join(gt_dir, GT_TEMPLATES["mask"])
    cur_hits = find_in_project(project_dir, "templates/*mask*.mrc")
    cur_p = cur_hits[0] if cur_hits else None

    results = {}
    for label, p in [("GT", gt_p), ("CURRENT", cur_p)]:
        if not p or not os.path.exists(p):
            print(f"  [MISS] {label}: {p}")
            continue
        d, apix = mrc_stats(p)
        nz = int(np.count_nonzero(d))
        vals = sorted(np.unique(d))
        print(f"\n  {label}: {p}")
        print(f"    shape={d.shape}  apix={apix:.3f}")
        print(f"    nonzero={nz}/{d.size} ({100*nz/d.size:.2f}%)")
        print(f"    unique values: {vals[:10]}{'...' if len(vals) > 10 else ''}")
        print(f"    sum={d.sum():.2f}  mean={d.mean():.6f}")
        results[label] = (d, apix)

    if "GT" in results and "CURRENT" in results:
        gd, ga = results["GT"]
        cd, ca = results["CURRENT"]
        print(f"\n  shape match: {'OK' if gd.shape == cd.shape else f'<<< DIFF  GT={gd.shape} CURRENT={cd.shape}'}")
        print(f"  apix  match: {'OK' if abs(ga-ca)<0.05 else f'<<< DIFF  GT={ga:.3f} CURRENT={ca:.3f}'}")
        if gd.shape == cd.shape:
            # for masks the key question is: are the nonzero regions equivalent?
            g_binary = (gd > 0).astype(np.float32)
            c_binary = (cd > 0).astype(np.float32)
            overlap = float(np.logical_and(g_binary, c_binary).sum())
            union   = float(np.logical_or(g_binary, c_binary).sum())
            iou     = overlap / union if union > 0 else 0.0
            print(f"  binary IoU: {iou:.4f}  {'OK' if iou > 0.95 else '<<< DIFF'}")
            # are they soft or binary?
            g_is_binary = set(np.unique(gd)).issubset({0.0, 1.0})
            c_is_binary = set(np.unique(cd)).issubset({0.0, 1.0})
            print(f"  GT is binary: {g_is_binary}  CURRENT is binary: {c_is_binary}")
            if not g_is_binary or not c_is_binary:
                print(f"  [NOTE] soft mask detected -- IoU is computed on thresholded (>0) version")


# ---------------------------------------------------------------------------
# 4. Defocus from frameseries XMLs
# ---------------------------------------------------------------------------
def compare_defocus(project_dir, gt_dir):
    section_header("DEFOCUS VALUES (frameseries XMLs, job002)")

    def read_defoci(xml_glob):
        xmls = sorted(glob.glob(xml_glob))
        vals = []
        for f in xmls:
            root = ET.parse(f).getroot()
            ctf = root.find('.//Param[@Name="Defocus"]')
            if ctf is not None:
                vals.append((os.path.basename(f), float(ctf.attrib["Value"])))
        return vals

    gt_glob  = os.path.join(gt_dir, "External/job002/warp_frameseries/*.xml")
    cur_glob = os.path.join(project_dir, "External/job002/warp_frameseries/*.xml")

    for label, pattern in [("GT", gt_glob), ("CURRENT", cur_glob)]:
        vals = read_defoci(pattern)
        print(f"\n{label} (n={len(vals)}):")
        if not vals:
            print("  [MISS] no XMLs found at", pattern)
            continue
        vs = [v for _, v in vals]
        mean = statistics.mean(vs)
        std  = statistics.stdev(vs) if len(vs) > 1 else 0.0
        print(f"  mean={mean:.3f}  std={std:.4f}  min={min(vs):.3f}  max={max(vs):.3f}")
        outliers = [(n, v) for n, v in vals if abs(v - mean) > 3 * std]
        if outliers:
            print(f"  OUTLIERS (>3 sigma from mean):")
            for n, v in outliers:
                print(f"    {n}: {v:.4f}  (delta={v-mean:+.4f})")
        else:
            print("  no outliers")


# ---------------------------------------------------------------------------
# 5. CTF values actually ingested by pytom (from job.json)
# ---------------------------------------------------------------------------
def compare_ctf_ingested(project_dir, gt_dir):
    section_header("CTF VALUES INGESTED BY PYTOM (job.json ctf_data)")

    gt_path  = os.path.join(gt_dir, GT_TM_JOB, GT_JOB_JSON)
    cur_hits = find_in_project(project_dir, "External/job0**/tmResults/*_job.json",
                                require_sibling=("_job.json", "_scores.mrc"))
    cur_path = cur_hits[0] if cur_hits else None

    def extract_defoci(jpath):
        if not jpath or not os.path.exists(jpath):
            return None, None
        with open(jpath) as f:
            j = json.load(f)
        # v0.10: flat ctf_data list; v0.12: inside ts_metadata
        ctf = j.get("ctf_data") or j.get("ts_metadata", {}).get("ctf_data", [])
        defoci = [c["defocus"] * 1e6 for c in ctf]   # convert m to um
        handedness = j.get("defocus_handedness") or j.get("ts_metadata", {}).get("defocus_handedness")
        return defoci, handedness

    gt_defoci, gt_hand   = extract_defoci(gt_path)
    cur_defoci, cur_hand = extract_defoci(cur_path)

    for label, defoci, hand in [("GT", gt_defoci, gt_hand), ("CURRENT", cur_defoci, cur_hand)]:
        if defoci is None:
            print(f"\n  [MISS] {label}")
            continue
        print(f"\n{label}  (defocus_handedness={hand}):")
        print(f"  n={len(defoci)}  mean={statistics.mean(defoci):.3f}  "
              f"std={statistics.stdev(defoci):.4f}  min={min(defoci):.3f}  max={max(defoci):.3f}")
        mean = statistics.mean(defoci)
        std  = statistics.stdev(defoci) if len(defoci) > 1 else 0.0
        outliers = [(i, v) for i, v in enumerate(defoci) if abs(v - mean) > 3 * std]
        if outliers:
            print(f"  OUTLIERS (>3 sigma):")
            for i, v in outliers:
                print(f"    tilt index {i}: {v:.4f} um  (delta={v-mean:+.4f})")

    if gt_defoci and cur_defoci and len(gt_defoci) == len(cur_defoci):
        diffs = [abs(g - c) for g, c in zip(gt_defoci, cur_defoci)]
        print(f"\n  per-tilt defocus delta (GT vs CURRENT):")
        print(f"  mean_abs_diff={statistics.mean(diffs):.4f} um  max_diff={max(diffs):.4f} um")
        if max(diffs) > 0.2:
            worst = max(range(len(diffs)), key=lambda i: diffs[i])
            print(f"  <<< WARNING: max diff >0.2 um at tilt index {worst} "
                  f"(GT={gt_defoci[worst]:.3f}, CURRENT={cur_defoci[worst]:.3f})")

    if gt_hand != cur_hand:
        print(f"\n  <<< WARNING: defocus_handedness differs: GT={gt_hand}, CURRENT={cur_hand}")


# ---------------------------------------------------------------------------
# 6. Tilt series XML: angles and CTF grid
# ---------------------------------------------------------------------------
def compare_tilt_series_xml(project_dir, gt_dir):
    section_header("TILT SERIES XML (job003 alignment)")

    gt_p  = os.path.join(gt_dir, GT_TILT_SERIES_XML)
    cur_hits = find_in_project(project_dir, "External/job003/warp_tiltseries/*.xml")
    cur_p = cur_hits[0] if cur_hits else None

    keys_to_check = ["AreAnglesInverted", "PlaneNormal", "GridMovement"]

    for label, p in [("GT", gt_p), ("CURRENT", cur_p)]:
        if not p or not os.path.exists(p):
            print(f"  [MISS] {label}: {p}")
            continue
        print(f"\n{label}: {p}")
        root = ET.parse(p).getroot()
        for key in keys_to_check:
            nodes = root.findall(f'.//{key}')
            if not nodes:
                # try as attribute
                hits = root.findall(f'.//*[@{key}]')
                val = hits[0].attrib[key] if hits else "NOT FOUND"
            else:
                val = nodes[0].text or ET.tostring(nodes[0], encoding="unicode")[:120]
            print(f"  {key}: {val}")
        # GridCTF summary
        ctf_grids = root.findall('.//GridCTF')
        print(f"  GridCTF entries: {len(ctf_grids)}")


# ---------------------------------------------------------------------------
# 7. Reconstruction tomogram stats
# ---------------------------------------------------------------------------
def compare_tomo(project_dir, gt_dir):
    if not HAS_MRC:
        return
    section_header("RECONSTRUCTION TOMOGRAM STATS (job005)")

    gt_p  = os.path.join(gt_dir, GT_TOMO_RECON)
    cur_hits = find_in_project(project_dir,
        "External/job005/warp_tiltseries/reconstruction/*_11.80Apx.mrc")
    # exclude ctf/deconv/even/odd subdirs
    cur_hits = [h for h in cur_hits if os.path.basename(os.path.dirname(h)) == "reconstruction"]
    cur_p = cur_hits[0] if cur_hits else None

    results = {}
    for label, p in [("GT", gt_p), ("CURRENT", cur_p)]:
        if not p or not os.path.exists(p):
            print(f"  [MISS] {label}: {p}")
            continue
        d, apix = mrc_stats(p)
        print_mrc_stats(label, p, d, apix, percentiles=(1, 5, 95, 99))
        results[label] = d

    if "GT" in results and "CURRENT" in results:
        ratio = results["CURRENT"].std() / results["GT"].std()
        flag = "  <<< significant intensity scale difference" if not 0.5 < ratio < 2.0 else ""
        print(f"\n  std ratio (current/GT): {ratio:.3f}{flag}")


# ---------------------------------------------------------------------------
# 8. Job JSON parameter diff
# ---------------------------------------------------------------------------
SCALAR_KEYS = [
    "voxel_size", "rotational_symmetry", "rotation_file", "n_rotations",
    "whiten_spectrum", "random_phase_correction", "defocus_handedness",
    "low_pass", "high_pass",
]

def compare_job_jsons(project_dir, gt_dir):
    section_header("JOB.JSON PARAMETER DIFF (template matching)")

    gt_path  = os.path.join(gt_dir, GT_TM_JOB, GT_JOB_JSON)
    cur_hits = find_in_project(project_dir, "External/job0**/tmResults/*_job.json",
                                require_sibling=("_job.json", "_scores.mrc"))
    cur_path = cur_hits[0] if cur_hits else None

    if not os.path.exists(gt_path):
        print(f"  [MISS] GT: {gt_path}"); return
    if not cur_path:
        print("  [MISS] current TM job.json not found"); return

    with open(gt_path)  as f: gt  = json.load(f)
    with open(cur_path) as f: cur = json.load(f)

    print(f"\n  GT:      {gt_path}")
    print(f"  CURRENT: {cur_path}")
    print(f"\n  {'PARAMETER':<32} {'GT':>16}  {'CURRENT':>16}  STATUS")
    print(f"  {'-'*76}")

    def get_val(d, key):
        if key in d:
            return d[key]
        return d.get("ts_metadata", {}).get(key, "MISSING")

    for key in SCALAR_KEYS:
        gv = get_val(gt, key)
        cv = get_val(cur, key)
        status = "OK" if gv == cv else "<<< DIFF"
        print(f"  {key:<32} {str(gv):>16}  {str(cv):>16}  {status}")

    # version and stats
    print(f"\n  {'pytom_version':<32} {gt.get('pytom_tm_version_number','?'):>16}  "
          f"{cur.get('pytom_tm_version_number','?'):>16}")

    for stat in ("std", "variance"):
        gv = gt.get("job_stats", {}).get(stat, float("nan"))
        cv = cur.get("job_stats", {}).get(stat, float("nan"))
        ratio = cv/gv if gv else float("nan")
        flag = f"  <<< ratio={ratio:.2f}" if not 0.7 < ratio < 1.3 else f"  ratio={ratio:.2f}"
        print(f"  {'job_stats.' + stat:<32} {gv:>16.8f}  {cv:>16.8f}{flag}")

    gs = gt.get("tomo_shape"); cs = cur.get("tomo_shape")
    print(f"\n  {'tomo_shape':<32} {str(gs):>16}  {str(cs):>16}  "
          f"{'OK' if gs == cs else '<<< DIFF'}")
    ts = gt.get("template_shape"); tc = cur.get("template_shape")
    print(f"  {'template_shape':<32} {str(ts):>16}  {str(tc):>16}  "
          f"{'OK' if ts == tc else '<<< DIFF'}")


# ---------------------------------------------------------------------------
# 9. Pick counts
# ---------------------------------------------------------------------------
def compare_pick_counts(project_dir, gt_dir):
    section_header("PICK COUNTS")

    def grep_count(run_out_path):
        if not os.path.exists(run_out_path):
            return None, None
        cutoff = count = None
        with open(run_out_path) as f:
            for line in f:
                if "cut off for particle extraction" in line:
                    try: cutoff = float(line.strip().split(":")[-1])
                    except ValueError: pass
                if "Model created" in line and "points" in line:
                    try: count = int(line.strip().split(",")[-1].strip().split()[0])
                    except (ValueError, IndexError): pass
                if "Extracted" in line and "particles" in line:
                    try: count = int(line.strip().split("Extracted")[1].strip().split()[0])
                    except (ValueError, IndexError): pass
        return cutoff, count

    gt_cutoff, gt_count = grep_count(os.path.join(gt_dir, GT_EXT_JOB, "run.out"))
    print(f"\n  GT:  cutoff={gt_cutoff}  picks={gt_count}")

    for p in sorted(find_in_project(project_dir, "External/job0**/run.out")):
        cutoff, count = grep_count(p)
        if count is not None:
            label = os.path.relpath(p, project_dir)
            delta = f"  (delta vs GT: {count - gt_count:+d})" if gt_count else ""
            print(f"  {label}: cutoff={cutoff}  picks={count}{delta}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
SECTIONS = ["scores", "templates", "masks", "defocus", "ctfingested",
            "xmlangles", "tomo", "jobs", "picks"]

def main():
    parser = argparse.ArgumentParser(
        description="crboost GT vs current project diagnostics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project", required=True,
                        help="Project name under PROJECTS_ROOT")
    parser.add_argument("--gt-dir", default=GT_DIR)
    parser.add_argument("--projects-root", default=PROJECTS_ROOT)
    parser.add_argument("--sections", nargs="+", choices=SECTIONS, default=SECTIONS,
                        help="Sections to run (default: all)")
    args = parser.parse_args()

    project_dir = os.path.join(args.projects_root, args.project)
    if not os.path.isdir(project_dir):
        print(f"[ERROR] project directory not found: {project_dir}"); return 1

    print(f"\nGT:      {args.gt_dir}")
    print(f"CURRENT: {project_dir}")

    dispatch = {
        "scores":      compare_scores,
        "templates":   compare_templates,
        "masks":       compare_masks,
        "defocus":     compare_defocus,
        "ctfingested": compare_ctf_ingested,
        "xmlangles":   compare_tilt_series_xml,
        "tomo":        compare_tomo,
        "jobs":        compare_job_jsons,
        "picks":       compare_pick_counts,
    }
    for sec in args.sections:
        dispatch[sec](project_dir, args.gt_dir)

    print("\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())