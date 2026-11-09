"""Summarise probe_tm_lanes.sh: template-matching throughput on one B200 with 1 to 4 lanes, with and without MPS.

    python3 -B summarise_tm_lanes.py /scratch/users/artem.kushner/gpu_probe/tm_lanes_1476700

The first table has one row per arm (runs.tsv): its tomograms and lanes, wall time from the first start to the last
end, tomograms a minute and that against the one-lane baseline (seq), SM-busy seconds, mean SM % and peak GPU memory
from nvidia-smi dmon, and the mean DCGM SM active, SM occupancy, tensor and DRAM activity if dcgmi ran. The second
has the medians over an arm's tomograms from their phase logs (tm_timed.py): the CPU set-up before the search
(reading, the wedge filters, the CPU FFTs, the GPU plan), the search, the writes, and a tomogram's wall time.
"""

import datetime as dt
import statistics
import sys
from pathlib import Path


def load_dmon(path: Path, day: dt.date) -> list[tuple[float, int, int]]:
    """(epoch, SM %, framebuffer MiB) per second; the clock column has no date, so a jump back is the next day."""
    rows, last = [], None
    for line in path.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        h, m, s = map(int, cols[0].split(":"))
        t = dt.datetime.combine(day, dt.time(h, m, s)).timestamp()
        if last is not None and t < last - 12 * 3600:
            day += dt.timedelta(days=1)
            t += 24 * 3600
        last = t
        rows.append((t, int(cols[2]), int(cols[8])))
    return rows


def load_dcgm(folder: Path) -> list[tuple[float, float, float, float, float]]:
    """Our GPU's dcgmi dmon rows as (epoch, SM active, SM occupancy, tensor active, DRAM active), one a second."""
    log, start, gpu = folder / "dcgm.log", folder / "dcgm_start.txt", folder / "dcgm_gpu.txt"
    if not log.exists() or not start.exists() or not gpu.exists():
        return []
    t_start, wanted = float(start.read_text()), gpu.read_text().strip()
    if wanted == "all":  # the GPU could not be identified: the log holds all 8, none of them known to be ours
        return []
    rows, seen = [], 0
    for line in log.read_text().splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[0] == "GPU" and cols[1] == wanted:
            seen += 1  # N/A rows while the counters start still take their second
            try:
                values = [float(c) for c in cols[2:6]]
            except ValueError:
                continue
            rows.append((t_start + seen, *values))
    return rows


def phase_seconds(path: Path) -> dict[str, float]:
    """Seconds per phase name of one tomogram's phase log, summed over its calls."""
    out: dict[str, float] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            name, _detail, t0, t1 = line.split("\t")
            out[name] = out.get(name, 0.0) + float(t1) - float(t0)
    return out


def median(values: list[float]) -> str:
    return f"{statistics.median(values):.1f}" if values else "-"


def main() -> None:
    folder = Path(sys.argv[1])
    arms = [
        (label, int(n), int(lanes), float(a), float(b), load)
        for label, n, lanes, a, b, load in (ln.rstrip("\n").split("\t") for ln in (folder / "runs.tsv").open())
    ]
    print(f"{folder.name}\n")
    for name in ("failed.txt", "identity.txt"):
        if (folder / name).exists():
            print(f"{name}:\n{(folder / name).read_text()}")
    day = dt.datetime.fromtimestamp(arms[0][3]).date()
    dmon = load_dmon(folder / "dmon.log", day) if (folder / "dmon.log").exists() else []
    dcgm = load_dcgm(folder)
    base = next((n / (b - a) for label, n, _lanes, a, b, _load in arms if label == "seq"), None)
    print(
        "| arm | tomograms | lanes | MPS | wall s | tomograms/min | vs seq | SM>0 s | mean SM % | peak GPU GB "
        "| DCGM SM act / occ / tensor / DRAM | load 1 5 15 |"
    )
    print("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|---|")
    for label, n, lanes, t0, t1, load in arms:
        wall = t1 - t0
        window = [r for r in dmon if t0 <= r[0] <= t1]
        busy = sum(1 for r in window if r[1] > 0)
        mean_sm = f"{sum(r[1] for r in window) / len(window):.0f}" if window else "-"
        peak = f"{max(r[2] for r in window) / 1024:.0f}" if window else "-"
        dw = [r for r in dcgm if t0 <= r[0] <= t1]
        dtxt = " / ".join(f"{sum(r[i] for r in dw) / len(dw):.2f}" for i in (1, 2, 3, 4)) if dw else "-"
        vs = f"{(n / wall) / base:.2f}" if base else "-"
        mps = "yes" if label.endswith("_mps") else "no"
        print(
            f"| {label} | {n} | {lanes} | {mps} | {wall:.0f} | {60 * n / wall:.2f} "
            f"| {vs} | {busy if window else '-'} | {mean_sm} | {peak} | {dtxt} | {load} |"
        )

    procs: dict[str, tuple[float, float]] = {}
    for line in (folder / "procs.tsv").read_text().splitlines():
        label, _cpus, t0, t1 = line.split("\t")
        procs[label] = (float(t0), float(t1))
    print("\nMedians over each arm's tomograms, seconds:\n")
    print("| arm | set-up | search | writes | tomogram wall |")
    print("|---|---:|---:|---:|---:|")
    setup_phases = ("read_mrc", "create_wedge", "rfftn", "irfftn", "gpu_plan")
    for label, *_ in arms:
        mine = [k for k in procs if k.startswith(f"{label}_l")]
        phases = [phase_seconds(folder / f"{k}.phases.tsv") for k in mine]
        setup = [sum(p.get(s, 0.0) for s in setup_phases) for p in phases if p]
        search = [p["gpu_search"] for p in phases if "gpu_search" in p]
        writes = [p.get("write_mrc", 0.0) + p.get("write_json", 0.0) for p in phases if p]
        walls = [procs[k][1] - procs[k][0] for k in mine]
        print(f"| {label} | {median(setup)} | {median(search)} | {median(writes)} | {median(walls)} |")
    if (folder / "failed.txt").exists():
        print("\nSome tomograms failed: their rows count them in the wall time; see failed.txt.")


if __name__ == "__main__":
    main()
