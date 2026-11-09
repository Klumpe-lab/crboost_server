"""Summarise probe_tm_angles.sh (P9): per angle list, its size, coverage and run time, and what its candidates do
against the old list's on the same tomograms.

    python3 -B summarise_tm_angles.py /scratch/users/artem.kushner/gpu_probe/tm_angles_<jobid>

A candidate matches one of the old list's when their centres lie within a quarter of the particle diameter (the
extraction's --particle-diameter). "Found" is the share of the old list's candidates a list also finds, "new" the
ones it finds that the old list did not, "score" the median ratio of matched candidates' LCCmax to the old list's,
and "cut-off" the median of the extraction's own, which follows each search's score statistics and so changes with
the number of rotations. The last table checks the old arm against the pilot's own candidates for the same
tomograms: the same command, so it should agree.
"""

import os
import statistics
import sys
from pathlib import Path


def read_star(path: Path) -> list[dict]:
    cols, rows = [], []
    for line in path.read_text().splitlines():
        s = line.strip()
        if s.startswith("_rln"):
            cols.append(s.split()[0][4:])
        elif cols and s and s[0] in "-0123456789.":
            rows.append(dict(zip(cols, s.split(), strict=True)))
    return rows


def xyz(row: dict) -> tuple[float, ...]:
    return tuple(float(row[f"CenteredCoordinate{a}Angst"]) for a in "XYZ")


def match(a: list[dict], b: list[dict], radius: float) -> list[tuple[int, int]]:
    """Nearest pairs first, each candidate used once, within `radius` Å."""
    dist = sorted(
        (sum((p - q) ** 2 for p, q in zip(xyz(ra), xyz(rb), strict=True)) ** 0.5, i, j)
        for i, ra in enumerate(a)
        for j, rb in enumerate(b)
    )
    pairs, used_a, used_b = [], set(), set()
    for d, i, j in dist:
        if d > radius:
            break
        if i not in used_a and j not in used_b:
            used_a.add(i)
            used_b.add(j)
            pairs.append((i, j))
    return pairs


def med(values: list[float], fmt: str = "{:.0f}") -> str:
    return fmt.format(statistics.median(values)) if values else "-"


def main() -> None:
    res = Path(sys.argv[1])
    print(f"{res.name}\n")
    for name in ("lists/check.txt", "failed.txt"):
        if (res / name).exists():
            print(f"{name}:\n{(res / name).read_text()}")
    runs = [ln.split("\t") for ln in (res / "runs.tsv").read_text().splitlines()[1:]]
    cover = {}
    if (res / "lists/coverage.tsv").exists():
        for ln in (res / "lists/coverage.tsv").read_text().splitlines()[1:]:
            name, n, m, p95, mx = ln.split("\t")
            cover["old" if name == "old" else name] = (n, m, p95, mx)
    arms = list(dict.fromkeys(r[0] for r in runs))
    tomos = list(dict.fromkeys(r[1] for r in runs))
    radius = float((res / "diameter.txt").read_text().strip() or "nan") / 4

    print(
        "| List | Rotations | Coverage median / 95th pct / max, deg | Runs | Median wall s | Median start to scores s |"
    )
    print("|---|---:|---|---:|---:|---:|")
    for arm in arms:
        ok = [r for r in runs if r[0] == arm and r[5].strip() == "0"]
        walls = [float(r[4]) - float(r[3]) for r in ok]
        to_scores = []
        for r in ok:
            scores = res / "out" / arm / f"{r[1]}_scores.mrc"
            if scores.exists():
                to_scores.append(os.stat(scores).st_mtime - float(r[3]))
        cov = " / ".join(cover[arm][1:]) if arm in cover else "-"
        rot = ok[0][2] if ok else (cover[arm][0] if arm in cover else "-")
        print(f"| {arm} | {rot} | {cov} | {len(ok)} | {med(walls)} | {med(to_scores)} |")

    cands = {
        (arm, t): read_star(res / "out" / arm / f"{t}_particles.star")
        for arm in arms
        for t in tomos
        if (res / "out" / arm / f"{t}_particles.star").exists()
    }
    if "old" in arms:
        print(f"\nCandidates against the old list's, matched within {radius:.0f} Å, over {len(tomos)} tomograms:\n")
        print("| List | Candidates | Found of the old list's | New | Score against old | Cut-off |")
        print("|---|---:|---:|---:|---:|---:|")
        for arm in arms:
            total = found = new = old_total = 0
            ratios, cutoffs = [], []
            for t in tomos:
                mine, old = cands.get((arm, t)), cands.get(("old", t))
                if mine is None or old is None:
                    continue
                pairs = match(mine, old, radius)
                total, old_total, found, new = (
                    total + len(mine),
                    old_total + len(old),
                    found + len(pairs),
                    new + len(mine) - len(pairs),
                )
                ratios += [float(mine[i]["LCCmax"]) / float(old[j]["LCCmax"]) for i, j in pairs]
                cutoffs += [float(r["CutOff"]) for r in mine[:1]]
            share = f"{found}/{old_total}" if old_total else "-"
            print(f"| {arm} | {total} | {share} | {new} | {med(ratios, '{:.3f}')} | {med(cutoffs, '{:.4f}')} |")

        print("\nThe old list here against the pilot's own candidates (same command):\n")
        print("| Tomogram | Here | Pilot | Matched | Median score difference |")
        print("|---|---:|---:|---:|---:|")
        for t in tomos:
            mine, pilot_star = cands.get(("old", t)), res / "pilot" / f"{t}_particles.star"
            if mine is None or not pilot_star.exists():
                continue
            pilot = read_star(pilot_star)
            pairs = match(mine, pilot, radius)
            diffs = [abs(float(mine[i]["LCCmax"]) - float(pilot[j]["LCCmax"])) for i, j in pairs]
            print(f"| {t} | {len(mine)} | {len(pilot)} | {len(pairs)} | {med(diffs, '{:.5f}')} |")


if __name__ == "__main__":
    main()
