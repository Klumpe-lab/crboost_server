"""Summarise probe_warp_chunk.sh (P10): Warp reconstruct one call per tilt series against N tilt series in one call
with 1, 2 or 4 workers on the card.

    python3 -B summarise_warp_chunk.py /scratch/users/artem.kushner/gpu_probe/warp_chunk_<jobid>

One row per arm: its wall time, the time per tilt series, that against one call each ("one", the sum of its calls),
and on a B200 the mean SM % over the arm from nvidia-smi dmon (a slice reports none). Then the timed read of every
average the tilt series use (what a cold read from /groups costs), and whether each arm's tomograms equal one call
each's (centre crop: identical, or the lowest correlation and the largest difference).
"""

import datetime as dt
import statistics
import sys
from pathlib import Path


def load_dmon(path: Path, day: dt.date) -> list[tuple[float, int]]:
    """(epoch, SM %) per second; the clock column has no date, so a jump back is the next day."""
    rows, last, col = [], None, None
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            names = line.lstrip("#").split()
            col = names.index("sm") if "sm" in names else col
            continue
        cols = line.split()
        if not cols or col is None or not cols[col].isdigit():
            continue
        h, m, s = map(int, cols[0].split(":"))
        t = dt.datetime.combine(day, dt.time(h, m, s)).timestamp()
        if last is not None and t < last - 12 * 3600:
            day += dt.timedelta(days=1)
            t += 24 * 3600
        last = t
        rows.append((t, int(cols[col])))
    return rows


def main() -> None:
    res = Path(sys.argv[1])
    print(f"{res.name}: {(res / 'gpu.txt').read_text().strip()}\n")
    runs = [ln.split("\t") for ln in (res / "runs.tsv").read_text().splitlines()[1:]]
    n_ts = len((res / "tilt_series.txt").read_text().split())
    day = dt.datetime.fromtimestamp(float(runs[0][2])).date()
    dmon = load_dmon(res / "dmon.log", day) if (res / "dmon.log").exists() else []

    def sm(t0: float, t1: float) -> str:
        window = [v for t, v in dmon if t0 <= t <= t1]
        return f"{sum(window) / len(window):.0f}" if window else "-"

    one = [r for r in runs if r[0] == "one"]
    one_total = sum(float(r[3]) - float(r[2]) for r in one)
    print("| Arm | Tilt series | Wall s | Per tilt series s | Against one call each | Mean SM % | Exit |")
    print("|---|---:|---:|---:|---:|---:|---|")
    if one:
        per = [float(r[3]) - float(r[2]) for r in one]
        exits = ",".join(sorted({r[4] for r in one}))
        print(
            f"| one call each, 1 worker | {len(one)} | {one_total:.0f} | {statistics.median(per):.0f} (median) "
            f"| 1.00 | {sm(float(one[0][2]), float(one[-1][3]))} | {exits} |"
        )
    for r in runs:
        if not r[0].startswith("p"):
            continue
        wall = float(r[3]) - float(r[2])
        vs = f"{one_total / wall:.2f}" if one and len(one) == n_ts else "-"
        print(
            f"| one call, {r[0][1:]} workers | {n_ts} | {wall:.0f} | {wall / n_ts:.0f} | {vs} "
            f"| {sm(float(r[2]), float(r[3]))} | {r[4]} |"
        )
    for r in runs:
        if r[0] == "warmup":
            print(f"\nWarm-up (one tilt series, Warp's first start in the job): {float(r[3]) - float(r[2]):.0f} s.")
        if r[0] == "read":
            secs = float(r[3]) - float(r[2])
            files, nbytes = r[1].split(", ")[0], float(r[1].split(", ")[1].split()[0])
            print(
                f"Reading every average and half the {n_ts} tilt series use ({files}, {nbytes / 1e9:.1f} GB) cold from "
                f"/groups: {secs:.0f} s, {nbytes / 1e6 / secs:.0f} MB/s, {secs / n_ts:.1f} s a tilt series."
            )
    eq = res / "equivalence.tsv"
    if eq.exists():
        print("\nTomograms against one call each (centre 128 x 256 x 256 voxels):\n")
        print("| Arm | Compared | Identical | Lowest correlation | Largest difference |")
        print("|---|---:|---:|---:|---:|")
        rows = [ln.split("\t") for ln in eq.read_text().splitlines() if ln.strip()]
        for arm in dict.fromkeys(r[0] for r in rows):
            mine = [r for r in rows if r[0] == arm and r[2] != "missing"]
            corr = [float(r[3]) for r in mine if r[3]]
            diff = [float(r[4]) for r in mine if r[4]]
            same = sum(r[2] == "identical" for r in mine)
            missing = sum(r[0] == arm and r[2] == "missing" for r in rows)
            low = f"{min(corr):.6f}" if corr else "-"
            high = f"{max(diff):.3g}" if diff else "-"
            print(f"| {arm} | {len(mine)}{f' ({missing} missing)' if missing else ''} | {same} | {low} | {high} |")


if __name__ == "__main__":
    main()
