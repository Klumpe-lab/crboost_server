"""Roadmap-43 S1: compare every benchmark case against the reference case (b200_s111_fp32_g1).

Per case: wall time, peak GPU memory, the largest score difference from the reference, the share of the
reference's top-N peaks found again (within a quarter particle diameter) and, of those, the share with
the same best rotation; plus the candidate count pytom_extract_candidates gives with the production
settings (written by extract.sh beside each case as <ts>_particles.star). Writes results.md and results.json.

Usage: python compare.py <bench root> <report dir>
"""

import json
import sys
from pathlib import Path

import mrcfile
import numpy as np
import starfile
from scipy.ndimage import maximum_filter
from scipy.spatial import cKDTree

REFERENCE = "b200_s111_fp32_g1"
# Particle diameter (Å), score-map pixel (Å), and how many top peaks to compare.
TOMOS = {"copia": (550.0, 11.8, 1500), "412": (575.0, 6.2, 300)}


def read(path: Path) -> np.ndarray:
    with mrcfile.open(path, permissive=True) as m:
        return np.asarray(m.data, dtype=np.float32)


def top_peaks(scores: np.ndarray, radius_px: int, n: int) -> np.ndarray:
    """The n highest local maxima at least `radius_px` apart (pytom keeps peaks diameter/2 apart)."""
    is_max = (scores == maximum_filter(scores, size=2 * radius_px + 1)) & (scores > 0)
    idx = np.argwhere(is_max)
    order = np.argsort(scores[tuple(idx.T)])[::-1][:n]
    return idx[order]


def case_files(case_dir: Path) -> tuple[Path, Path] | None:
    scores = sorted(case_dir.glob("*_scores.mrc"))
    angles = sorted(case_dir.glob("*_angles.mrc"))
    return (scores[0], angles[0]) if scores and angles else None


def n_candidates(case_dir: Path) -> int | None:
    stars = sorted(case_dir.glob("*_particles.star"))
    if not stars:
        return None
    data = starfile.read(stars[0], always_dict=True)
    return len(next(iter(data.values())))


def main() -> None:
    root, report = Path(sys.argv[1]), Path(sys.argv[2])
    rows = []
    for tomo, (diam, apix, n_top) in TOMOS.items():
        radius = round(diam / apix / 2)
        match_px = diam / apix / 4
        cases = sorted(d for d in (root / tomo).iterdir() if d.is_dir())
        ref_dir = root / tomo / f"{tomo}_{REFERENCE}"
        ref_files = case_files(ref_dir)
        if ref_files is None:
            print(f"{tomo}: reference {ref_dir.name} has no maps; skipped", flush=True)
            continue
        ref_scores, ref_angles = read(ref_files[0]), read(ref_files[1])
        ref_peaks = top_peaks(ref_scores, radius, n_top)
        for d in cases:
            row = {"tomo": tomo, "case": d.name.removeprefix(f"{tomo}_")}
            res = d / "result.txt"
            if res.exists():
                fields = dict(kv.split("=") for kv in res.read_text().split()[1:])
                row["wall_s"] = int(fields["wall_s"])
                row["peak_mib"] = None if fields["peak_mib"] == "NA" else int(fields["peak_mib"])
            files = case_files(d)
            if files is None:
                row["status"] = "no maps (failed or not run)"
                rows.append(row)
                continue
            scores, angles = read(files[0]), read(files[1])
            row["max_abs_score_diff"] = float(np.abs(scores - ref_scores).max())
            row["ref_max_score"] = float(ref_scores.max())
            peaks = top_peaks(scores, radius, n_top)
            dist, j = cKDTree(peaks).query(ref_peaks, distance_upper_bound=match_px)
            found = np.isfinite(dist)
            row["top_n"] = n_top
            row["top_n_found"] = float(found.mean())
            same = [
                angles[tuple(peaks[k])] == ref_angles[tuple(p)]
                for p, k, f in zip(ref_peaks, j, found, strict=True)
                if f
            ]
            row["same_rotation"] = float(np.mean(same)) if same else None
            row["candidates"] = n_candidates(d)
            rows.append(row)
            print(json.dumps(row), flush=True)
    report.mkdir(parents=True, exist_ok=True)
    (report / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
    lines = [
        "| tomo | case | wall (s) | peak GPU MiB | max score diff | top-N found | same rotation | candidates |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        if "status" in r:
            lines.append(f"| {r['tomo']} | {r['case']} | {r.get('wall_s', '–')} | – | {r['status']} | | | |")
            continue
        rot = "–" if r["same_rotation"] is None else f"{r['same_rotation']:.3f}"
        cands = "–" if r["candidates"] is None else r["candidates"]
        lines.append(
            f"| {r['tomo']} | {r['case']} | {r.get('wall_s', '–')} | {r.get('peak_mib') or '–'} | "
            f"{r['max_abs_score_diff']:.2e} (max {r['ref_max_score']:.3f}) | "
            f"{r['top_n_found']:.3f} of {r['top_n']} | {rot} | {cands} |"
        )
    (report / "results.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
