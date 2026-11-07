"""Summarise probe_tm_ab.sh: for each run, the seconds before the GPU first works, the seconds it works, and the idle
seconds between and after, then the effect of the kernel cache and of the output folder.

A second counts as busy when nvidia-smi dmon's `sm` column is above 0 (the same coarse measure as SLURM's gpuutil,
but sampled every second from before the first run).

    python3 summarise_tm_ab.py $SCRATCHDIR/gpu_probe/tm_ab_<jobid>
"""

import statistics
import sys
from datetime import datetime
from pathlib import Path


def read_dmon(path: Path, first_epoch: float) -> list[tuple[float, float]]:
    """(epoch, sm %) per sample. `dmon -o T` prints the time of day; the date comes from the first run."""
    midnight = datetime.fromtimestamp(first_epoch).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    names: list[str] = []
    samples: list[tuple[float, float]] = []
    previous, offset = -1, 0
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            if not names and "sm" in line.split():
                names = line.lstrip("#").split()
            continue
        fields = line.split()
        if not names or not fields:
            continue
        try:
            clock = datetime.strptime(fields[0], "%H:%M:%S")
        except ValueError:
            continue
        second = clock.hour * 3600 + clock.minute * 60 + clock.second
        if second < previous:
            offset += 86400  # past midnight
        previous = second
        sm = dict(zip(names, fields, strict=False)).get("sm", "-")
        samples.append((midnight + offset + second, float(sm) if sm.replace(".", "").isdigit() else 0.0))
    return samples


def main() -> None:
    res = Path(sys.argv[1])
    runs = [line.split("\t") for line in (res / "runs.tsv").read_text().splitlines() if line]
    runs = [(label, float(t0), float(t1), int(kernels)) for label, t0, t1, kernels in runs]
    samples = read_dmon(res / "dmon.log", runs[0][1])
    print(f"{res}\n")
    for name in ("container_check.txt", "failed.txt"):
        if (res / name).exists():
            print((res / name).read_text())

    print("| run | wall s | before first busy s | busy s | idle between s | idle after s | cached kernels |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    wall: dict[str, list[float]] = {}
    for label, t0, t1, kernels in runs:
        busy = [t for t, sm in samples if t0 - 1 <= t <= t1 + 1 and sm > 0]
        if busy:
            lead, tail = max(0.0, busy[0] - t0), max(0.0, t1 - busy[-1])
            between = (busy[-1] - busy[0] + 1) - len(busy)
            cells = [f"{lead:.0f}", str(len(busy)), f"{between:.0f}", f"{tail:.0f}"]
        else:
            cells = ["no busy second seen"] + ["-"] * 3
        print(f"| {label} | {t1 - t0:.0f} | " + " | ".join(cells) + f" | {kernels} |")
        wall.setdefault(label, []).append(t1 - t0)

    def mean(label: str) -> float:
        return statistics.mean(wall[label]) if label in wall else float("nan")

    print("\nMean wall seconds per cell (short angular search; each cell run twice):")
    for label in ("cold_nfs", "warm_nfs", "cold_tmp", "warm_tmp"):
        print(f"  {label}: {mean(label):.1f}")
    print("\nWhat the kernel cache costs (cold - warm):")
    print(f"  with outputs on /groups:  {mean('cold_nfs') - mean('warm_nfs'):+.1f} s")
    print(f"  with outputs in /tmp:     {mean('cold_tmp') - mean('warm_tmp'):+.1f} s")
    print("What outputs on /groups cost over /tmp (nfs - tmp):")
    print(f"  with a cold cache:        {mean('cold_nfs') - mean('cold_tmp'):+.1f} s")
    print(f"  with a warm cache:        {mean('warm_nfs') - mean('warm_tmp'):+.1f} s")


if __name__ == "__main__":
    main()
