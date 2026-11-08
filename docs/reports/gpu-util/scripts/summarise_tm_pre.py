"""Summarise probe_tm_prestart.sh: what the template-matching process does before the card starts to work.

    python3 summarise_tm_pre.py <folder holding runs.tsv, ps.log and dmon.log>

The window of a run is from its start to the first second in which the GPU's SMs were busy (dmon.log); without a
dmon.log it is the whole run. In the window: cores busy = CPU seconds the busiest python process used over the seconds
sampled; read = bytes the process read (rchar, which counts reads served from the page cache too). The last column is
cores busy per 10 s of the run.
"""

import datetime as dt
import sys
from pathlib import Path

TICKS_PER_SECOND = 100


def load_ps(path: Path) -> list[tuple[float, int, int, int, int, int]]:
    rows = []
    for line in path.read_text().splitlines():
        t, pid, ticks, threads, rss_kb, rchar = line.split()
        rows.append((float(t), int(pid), int(ticks), int(threads), int(rss_kb), int(rchar)))
    return rows


def first_busy(dmon: Path, t0: float, t1: float) -> float | None:
    """Epoch of the first dmon sample with sm > 0 inside [t0, t1], or None."""
    day = dt.datetime.fromtimestamp(t0).date()
    for line in dmon.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        h, m, s = map(int, cols[0].split(":"))
        t = dt.datetime.combine(day, dt.time(h, m, s)).timestamp()
        if t0 <= t <= t1 + 0.5 and int(cols[2]) > 0:  # a sample stamped before the start holds the previous run's tail
            return t
    return None


def main() -> None:
    folder = Path(sys.argv[1])
    ps_path, dmon_path = folder / "ps.log", folder / "dmon.log"
    ps = load_ps(ps_path) if ps_path.exists() else []
    if not ps:
        print("ps.log is empty: the sampler saw no python process")
    print(f"{folder.name}  {(folder / 'cpus.txt').read_text().strip() if (folder / 'cpus.txt').exists() else ''}\n")
    print(
        "| run | cpus | wall s | to first SM s | cores busy before | peak threads | peak RSS GB | read GB "
        "| cores per 10 s |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    by_cpus: dict[str, list[float]] = {}
    for line in (folder / "runs.tsv").read_text().splitlines():
        label, cpus, t0s, t1s = line.split("\t")
        t0, t1 = float(t0s), float(t1s)
        sm = first_busy(dmon_path, t0, t1) if dmon_path.exists() else None
        end = sm if sm is not None else t1
        window = [r for r in ps if t0 <= r[0] <= end]
        cpu = seen = read = 0.0
        buckets: dict[int, list[float]] = {}
        for a, b in zip(window, window[1:]):
            if a[1] != b[1] or b[0] <= a[0]:  # another process: no delta
                continue
            span = b[0] - a[0]
            used = (b[2] - a[2]) / TICKS_PER_SECOND
            cpu += used
            seen += span
            read += b[5] - a[5]
            slot = buckets.setdefault(int((a[0] - t0) // 10), [0.0, 0.0])
            slot[0] += used
            slot[1] += span
        cores = f"{cpu / seen:.1f}" if seen else "-"
        timeline = " ".join(f"{u / s:.1f}" for _, (u, s) in sorted(buckets.items()) if s)
        threads = max((r[3] for r in window), default=0)
        rss = max((r[4] for r in window), default=0) / 1048576
        to_sm = f"{end - t0:.0f}" if sm is not None else "-"
        print(
            f"| {label} | {len(cpus.split(','))} | {t1 - t0:.0f} | {to_sm} | {cores} | {threads} | {rss:.1f} "
            f"| {read / 1e9:.2f} | {timeline} |"
        )
        if sm is not None and label != "prime":
            by_cpus.setdefault(str(len(cpus.split(","))), []).append(end - t0)
    if by_cpus:
        print("\nMean seconds to the first busy SM (prime run left out):")
        for n, v in sorted(by_cpus.items(), key=lambda kv: int(kv[0])):
            print(f"  {n} cpus: {sum(v) / len(v):.1f} s over {len(v)} runs")


if __name__ == "__main__":
    main()
