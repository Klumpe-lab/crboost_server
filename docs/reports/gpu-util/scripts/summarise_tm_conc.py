"""Summarise probe_tm_concurrency.sh: what N template-matching processes at once get out of one B200.

    python3 summarise_tm_conc.py /scratch/users/artem.kushner/gpu_probe/tm_conc_1475000

runs.tsv has one line per batch (N, start, end); the first batch of one process is the reference. Throughput is
tomograms per minute; speed-up is that over the first one-process batch. GPU columns come from nvidia-smi dmon
(seconds with SM > 0, mean SM % over the batch, peak GPU memory) and, if dcgmi worked in the job, from dcgm.log
(mean SM active, tensor active and DRAM active over the batch, as fractions).
"""

import datetime as dt
import sys
from pathlib import Path


def load_dmon(path: Path, day: dt.date) -> list[tuple[float, int, int]]:
    rows = []
    for line in path.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        h, m, s = map(int, cols[0].split(":"))
        rows.append((dt.datetime.combine(day, dt.time(h, m, s)).timestamp(), int(cols[2]), int(cols[8])))
    return rows


def load_dcgm(folder: Path) -> list[tuple[float, float, float, float]]:
    log, start = folder / "dcgm.log", folder / "dcgm_start.txt"
    if not log.exists() or not start.exists():
        return []
    t_start = float(start.read_text())
    rows = []
    for line in log.read_text().splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[0] == "GPU":
            try:
                values = [float(c) for c in cols[2:6]]
            except ValueError:  # N/A while the counters start
                continue
            rows.append((t_start + len(rows) + 1.0, values[0], values[2], values[3]))
    return rows


def main() -> None:
    folder = Path(sys.argv[1])
    batches = [(int(n), float(a), float(b)) for n, a, b in (ln.split("\t") for ln in (folder / "runs.tsv").open())]
    day = dt.datetime.fromtimestamp(batches[0][1]).date()
    dmon = load_dmon(folder / "dmon.log", day) if (folder / "dmon.log").exists() else []
    dcgm = load_dcgm(folder)
    if (folder / "failed.txt").exists():
        print((folder / "failed.txt").read_text())
    reference = batches[0][2] - batches[0][1]
    print(f"{folder.name}\n")
    print(
        "| N at once | wall s | tomograms/min | speed-up | SM>0 s | mean SM % | peak GPU GB "
        "| dcgm SM / tensor / DRAM |"
    )
    print("|---:|---:|---:|---:|---:|---:|---:|---|")
    for n, t0, t1 in batches:
        wall = t1 - t0
        window = [r for r in dmon if t0 <= r[0] <= t1]
        busy = sum(1 for r in window if r[1] > 0)
        mean_sm = sum(r[1] for r in window) / len(window) if window else float("nan")
        peak = max((r[2] for r in window), default=0) / 1024
        dw = [r for r in dcgm if t0 <= r[0] <= t1]
        dtxt = "-"
        if dw:
            dtxt = " / ".join(f"{sum(r[i] for r in dw) / len(dw):.2f}" for i in (1, 2, 3))
        speed = n * reference / wall
        print(
            f"| {n} | {wall:.0f} | {60 * n / wall:.2f} | {speed:.2f} | {busy} | {mean_sm:.0f} | {peak:.0f} | {dtxt} |"
        )
    discovery = folder / "dcgm_discovery.txt"
    if discovery.exists():
        print("\ndcgmi discovery:", " ".join(discovery.read_text().split())[:200])


if __name__ == "__main__":
    main()
