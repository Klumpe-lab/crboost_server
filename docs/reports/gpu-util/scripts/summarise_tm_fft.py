"""Summarise probe_tm_fft_mps.sh: where a template-matching process spends its time, with 1 and 4 FFT workers, and
what four processes at once get out of one B200 with and without MPS.

    python3 -B summarise_tm_fft.py /scratch/users/artem.kushner/gpu_probe/tm_fft_1475262

The first table has one row per batch (runs.tsv): wall time, throughput against the first 1-worker run, seconds from
the batch's start to its first second with SM > 0, SM-busy seconds, mean SM % and peak GPU memory from nvidia-smi
dmon, and the mean DCGM SM active, SM occupancy, tensor and DRAM activity if dcgmi ran. The second has one row per
process from its phase log (tm_timed.py): start-up to the end of pytom's imports, TMJob set-up, reading the volumes,
the full-size wedge, the CPU FFTs, the GPU plan, the search, the writes, and what is left of the process's wall time.
"""

import datetime as dt
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


def load_phases(path: Path) -> dict[str, float]:
    """Seconds per phase name, summed over its calls, plus the epoch the imports ended and the process ended."""
    out: dict[str, float] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        name, _detail, t0, t1 = line.split("\t")
        out[name] = out.get(name, 0.0) + float(t1) - float(t0)
        if name in ("imports", "total"):
            out[f"{name}_end"] = float(t1)
    return out


def fmt(value: float | None, digits: int = 0) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def batch_table(folder: Path, batches: list[tuple[str, int, int, float, float, str]]) -> None:
    day = dt.datetime.fromtimestamp(batches[0][3]).date()
    dmon = load_dmon(folder / "dmon.log", day) if (folder / "dmon.log").exists() else []
    dcgm = load_dcgm(folder)
    reference = next((b[4] - b[3] for b in batches if b[0] == "w1_a"), batches[0][4] - batches[0][3])
    print(
        "| batch | processes | FFT workers | wall s | tomograms/min | vs w1_a | first SM s | SM>0 s | mean SM % "
        "| peak GPU GB | DCGM SM act / occ / tensor / DRAM | load 1 5 15 |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|")
    for label, n, workers, t0, t1, load in batches:
        wall = t1 - t0
        window = [r for r in dmon if t0 <= r[0] <= t1]
        busy = [r for r in window if r[1] > 0]
        first = busy[0][0] - t0 if busy else None
        mean_sm = sum(r[1] for r in window) / len(window) if window else None
        peak = max((r[2] for r in window), default=None)
        dw = [r for r in dcgm if t0 <= r[0] <= t1]
        dtxt = " / ".join(f"{sum(r[i] for r in dw) / len(dw):.2f}" for i in (1, 2, 3, 4)) if dw else "-"
        print(
            f"| {label} | {n} | {workers} | {wall:.0f} | {60 * n / wall:.2f} | {n * reference / wall:.2f} "
            f"| {fmt(first)} | {len(busy) if window else '-'} | {fmt(mean_sm)} "
            f"| {fmt(peak / 1024 if peak is not None else None)} | {dtxt} | {load} |"
        )


def process_table(folder: Path) -> None:
    columns = [
        ("TMJob", ("tmjob_init",)),
        ("read", ("read_mrc",)),
        ("wedge", ("create_wedge",)),
        ("CPU FFT", ("rfftn", "irfftn")),
        ("GPU plan", ("gpu_plan",)),
        ("search", ("gpu_search",)),
        ("writes", ("write_mrc", "write_json")),
    ]
    print(
        "\n| process | FFT workers | start-up s | " + " | ".join(f"{c} s" for c, _ in columns) + " | rest s | wall s |"
    )
    print("|---|---:|" + "---:|" * (len(columns) + 3))
    for line in (folder / "procs.tsv").read_text().splitlines():
        label, workers, _cpus, t0, t1 = line.split("\t")
        if label == "prime":
            continue
        t0, t1 = float(t0), float(t1)
        ph = load_phases(folder / f"{label}.phases.tsv")
        if "imports_end" not in ph:
            print(f"| {label} | {workers} | no phase log |" + " |" * (len(columns) + 2))
            continue
        startup = ph["imports_end"] - t0
        cells = [sum(ph.get(k, 0.0) for k in keys) for _, keys in columns]
        end = ph.get("total_end", t1)
        rest = (end - ph["imports_end"]) - sum(cells) + (t1 - end)
        print(
            f"| {label} | {workers} | {startup:.1f} | "
            + " | ".join(f"{c:.1f}" for c in cells)
            + f" | {rest:.1f} | {t1 - t0:.0f} |"
        )


def main() -> None:
    folder = Path(sys.argv[1])
    batches = [
        (label, int(n), int(w), float(a), float(b), load)
        for label, n, w, a, b, load in (ln.rstrip("\n").split("\t") for ln in (folder / "runs.tsv").open())
    ]
    print(f"{folder.name}\n")
    for name in ("failed.txt", "identity.txt", "cpus.txt"):
        if (folder / name).exists():
            print(f"{name}:\n{(folder / name).read_text()}")
    batch_table(folder, batches)
    process_table(folder)
    check = folder / "mps_check.txt"
    if check.exists():
        print(f"\nmps_check.txt:\n{check.read_text()}")


if __name__ == "__main__":
    main()
