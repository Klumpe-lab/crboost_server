"""Roadmap-43 S1 follow-up: does a case pick what the reference (b200_s111_fp32_g1) picks, away from the edges?

compare.py compares whole score maps, edges included, where a split's pieces and the whole tomogram wrap around
differently. This compares what extraction keeps (pytom_extract_candidates zeroes a particle radius at every face):
per case, the share of its candidates the reference also picked and of the reference's it picked (within a
quarter particle diameter), and the largest score difference inside that region. Also each map's noise level (std
inside the region) against job_stats' std, which sets the cut-off: pytom 0.14 writes the same job_stats with and
without --random-phase-correction.

Usage: python candidate_check.py <bench root> <report dir>   (writes candidates.md and candidates.json there)
"""

import json
import sys
from pathlib import Path

import mrcfile
import numpy as np
import starfile
from scipy.spatial import cKDTree

REFERENCE = "b200_s111_fp32_g1"
TOMOS = {"copia": 550.0, "412": 575.0}  # particle diameter (Å), as extracted


def candidates(case: Path) -> np.ndarray | None:
    """Centred x, y, z (Å: --relion5-compat) of each candidate, None without a star file."""
    stars = sorted(case.glob("*_particles.star"))
    if not stars:
        return None
    df = next(iter(starfile.read(stars[0], always_dict=True).values()))
    return df[[f"rlnCenteredCoordinate{a}Angst" for a in "XYZ"]].to_numpy(dtype=float)


def interior(case: Path, edge_px: int) -> np.ndarray:
    with mrcfile.open(next(case.glob("*_scores.mrc")), permissive=True) as m:
        return np.array(m.data[edge_px:-edge_px, edge_px:-edge_px, edge_px:-edge_px], dtype=np.float32)


def shared(a: np.ndarray, b: np.ndarray, within: float) -> float:
    """Share of the points in `a` with a point of `b` within `within`."""
    dist, _ = cKDTree(b).query(a, distance_upper_bound=within)
    return float(np.isfinite(dist).mean())


def main() -> None:
    root, report = Path(sys.argv[1]), Path(sys.argv[2])
    rows = []
    for tomo, diameter in TOMOS.items():
        ref = root / tomo / f"{tomo}_{REFERENCE}"
        job = json.loads(next(ref.glob("*_job.json")).read_text())
        edge = int(diameter / 2 / job["voxel_size"])
        ref_scores, ref_cands = interior(ref, edge), candidates(ref)
        for case in sorted(d for d in (root / tomo).iterdir() if d.is_dir() and any(d.glob("*_scores.mrc"))):
            scores, cands = interior(case, edge), candidates(case)
            jobs = sorted(case.glob("*_job.json"))
            row = {
                "tomo": tomo,
                "case": case.name.removeprefix(f"{tomo}_"),
                "candidates": None if cands is None else len(cands),
                "interior_max_abs_diff": float(np.abs(scores - ref_scores).max()),
                "interior_std": float(scores.std(dtype=np.float64)),
                "job_stats_std": json.loads(jobs[0].read_text())["job_stats"]["std"] if jobs else None,
            }
            if cands is not None and len(cands) and len(ref_cands):
                row["picked_by_reference"] = shared(cands, ref_cands, diameter / 4)
                row["reference_picked"] = shared(ref_cands, cands, diameter / 4)
            rows.append(row)
            print(json.dumps(row), flush=True)
    (report / "candidates.json").write_text(json.dumps(rows, indent=2) + "\n")
    lines = [
        "| tomo | case | candidates | of them picked by the reference | of the reference's picked | "
        "max score diff inside | noise std inside | job_stats std |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        picked, ref_picked = (f"{r[k]:.1%}" if k in r else "–" for k in ("picked_by_reference", "reference_picked"))
        stats = "–" if r["job_stats_std"] is None else f"{r['job_stats_std']:.5f}"
        lines.append(
            f"| {r['tomo']} | {r['case']} | {r['candidates'] if r['candidates'] is not None else '–'} | "
            f"{picked} | {ref_picked} | {r['interior_max_abs_diff']:.2e} | {r['interior_std']:.5f} | {stats} |"
        )
    (report / "candidates.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
