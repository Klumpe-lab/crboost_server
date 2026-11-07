"""How busy crboost's GPU jobs were, read from SLURM accounting and grouped by driver.

Read-only: it only calls sacct. It prints aggregates and no user names. What the numbers are:

- `gres/gpuutil` is nvidia-smi's utilization.gpu, the share of time some kernel was running (not how much of the
  GPU it used), sampled every JobAcctGatherFrequency seconds (30 s on CBE.next) and summed over a job's GPUs.
  The report divides by the GPU count and weights by GPU-seconds.
- A MIG slice always reports 0, so slice jobs are counted in GPU-hours and left out of the busy column.
- TensorFlow tools reserve the whole card, so their `gres/gpumem` says nothing about need.
- sacct hides other users' batch scripts, so their jobs are grouped under "?".
- The scheduler is shared and rate-limits heavy users, so a run makes two accounting queries plus one stored-script
  lookup per job directory it has not seen before; those answers are kept in ~/.cache/crboost/ (--no-cache skips it).
- Read and write GB are SLURM's disk counters per job. What they count (cached reads, NFS) has not been checked
  against the bytes that crossed the network, so read them as upper bounds; tilt_filter's are implausibly large.

    python3 sacct_gpu_report.py --since 2026-11-01 --scope all --completed-only
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

UNITS_MB = {"": 1 / 1024, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
DRIVER_CACHE = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "crboost" / "sacct_driver_cache.json"


@dataclass
class Job:
    driver: str
    on_slice: bool
    user: str
    state: str
    seconds: float
    gpus: int
    busy: float  # per-GPU gpuutil, percent
    gpu_mem_mb: float
    cpus: int
    cpu_eff: float
    max_rss_mb: float
    req_mem_mb: float
    read_mb: float
    write_mb: float


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


def sacct(fields: list[str], args: argparse.Namespace, *extra: str) -> list[dict[str, str]]:
    who = ["-a"] if args.scope == "all" else ["-u", run(["id", "-un"]).strip()]
    cmd = ["sacct", "-P", "-n", "-r", args.partition, "-S", args.since, "--format", ",".join(fields), *who, *extra]
    return [dict(zip(fields, line.split("|"), strict=True)) for line in run(cmd).splitlines() if line]


def seconds(text: str) -> float:
    """[D-]HH:MM:SS[.fff] or MM:SS.fff as seconds."""
    if not text:
        return 0.0
    days = 0
    if "-" in text:
        head, text = text.split("-")
        days = int(head)
    parts = [float(p) for p in text.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    return days * 86400 + parts[0] * 3600 + parts[1] * 60 + parts[2]


def tres(text: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in text.split(",") if "=" in item)


def megabytes(text: str) -> float:
    m = re.match(r"([\d.]+)\s*([KMGT]?)", text or "")
    return float(m.group(1)) * UNITS_MB[m.group(2)] if m else 0.0


def driver_of(job_id: str) -> str:
    script = run(["sacct", "-j", job_id, "--batch-script"])
    m = re.search(r"drivers/([A-Za-z0-9_]+)\.py", script)
    return m.group(1) if m else "?"


def load_driver_cache() -> dict[str, str]:
    try:
        return json.loads(DRIVER_CACHE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        # No cache yet, or a half-written one: every directory is looked up again.
        return {}


def save_driver_cache(cache: dict[str, str]) -> None:
    DRIVER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    DRIVER_CACHE.write_text(json.dumps(cache, indent=0, sort_keys=True))


def load_jobs(args: argparse.Namespace) -> list[Job]:
    allocations = sacct(
        ["JobID", "User", "JobName", "State", "Elapsed", "AllocCPUS", "AllocTRES", "WorkDir", "StdOut"], args, "-X"
    )
    allocations = [a for a in allocations if a["JobName"] == args.job_name and "gres/gpu" in a["AllocTRES"]]
    step_fields = ["JobID", "TRESUsageInAve", "TRESUsageInMax", "TotalCPU", "MaxRSS", "AveDiskRead", "AveDiskWrite"]
    steps = sacct(step_fields, args)
    batch = {s["JobID"].removesuffix(".batch"): s for s in steps if s["JobID"].endswith(".batch")}

    # A single job's WorkDir is the project; its log (StdOut) sits in the job's own directory.
    def job_dir_of(a: dict[str, str]) -> str:
        return str(PurePosixPath(a["StdOut"]).parent) if a["StdOut"] else a["WorkDir"]

    first_job_in: dict[str, str] = {}
    for a in allocations:
        first_job_in.setdefault(job_dir_of(a), a["JobID"])
    cache = {} if args.no_cache else load_driver_cache()
    new = [job_dir for job_dir in first_job_in if job_dir not in cache]
    for job_dir in new:
        cache[job_dir] = driver_of(first_job_in[job_dir])
        time.sleep(0.2)  # one query per directory on a shared scheduler: no tight loop
    if new and not args.no_cache:
        save_driver_cache(cache)
    print(f"stored-script lookups: {len(new)} new, {len(first_job_in) - len(new)} cached", file=sys.stderr)
    driver_in = {job_dir: cache[job_dir] for job_dir in first_job_in}

    jobs = []
    for a in allocations:
        step = batch.get(a["JobID"])
        allocated, average = tres(a["AllocTRES"]), tres(step["TRESUsageInAve"]) if step else {}
        elapsed, cpus = seconds(a["Elapsed"]), int(a["AllocCPUS"])
        if step is None or elapsed <= 0 or "gres/gpuutil" not in average:
            continue
        gpus = int(allocated["gres/gpu"])
        jobs.append(
            Job(
                driver=driver_in[job_dir_of(a)],
                on_slice=any("1g.23gb" in key for key in allocated),
                user=a["User"],
                state=a["State"].split()[0],
                seconds=elapsed,
                gpus=gpus,
                busy=float(average["gres/gpuutil"]) / gpus,
                gpu_mem_mb=megabytes(tres(step["TRESUsageInMax"]).get("gres/gpumem", "0")),
                cpus=cpus,
                cpu_eff=seconds(step["TotalCPU"]) / (elapsed * cpus),
                max_rss_mb=megabytes(step["MaxRSS"]),
                req_mem_mb=megabytes(allocated.get("mem", "")),
                read_mb=megabytes(step["AveDiskRead"]),
                write_mb=megabytes(step["AveDiskWrite"]),
            )
        )
    return jobs


def weighted(jobs: list[Job], attr: str) -> float:
    total = sum(j.seconds * j.gpus for j in jobs)
    return sum(getattr(j, attr) * j.seconds * j.gpus for j in jobs) / total if total else 0.0


def gpu_hours(jobs: list[Job]) -> float:
    return sum(j.seconds * j.gpus for j in jobs) / 3600


def report(jobs: list[Job]) -> None:
    groups: dict[tuple[str, bool], list[Job]] = defaultdict(list)
    for j in jobs:
        groups[(j.driver, j.on_slice)].append(j)
    print(f"{len(jobs)} jobs, {len({j.user for j in jobs})} user(s)\n")
    columns = "driver GPU jobs GPU-h busy% busy<10% noGPUmem CPUeff CPUs memEff median-s read-GB write-GB".split()
    print("| " + " | ".join(columns) + " |")
    print("|---|---|" + "---:|" * (len(columns) - 2))
    for (driver, on_slice), rs in sorted(groups.items(), key=lambda kv: -gpu_hours(kv[1])):
        busy = "n/a" if on_slice else f"{weighted(rs, 'busy'):.1f}"
        mem_eff = sum(j.max_rss_mb for j in rs) / max(sum(j.req_mem_mb for j in rs), 1)
        cells = [
            driver,
            "slice" if on_slice else "B200",
            len(rs),
            f"{gpu_hours(rs):.1f}",
            busy,
            sum(j.busy < 10 for j in rs),
            sum(j.gpu_mem_mb == 0 for j in rs),
            f"{weighted(rs, 'cpu_eff'):.2f}",
            f"{statistics.median(j.cpus for j in rs):.0f}",
            f"{mem_eff:.2f}",
            f"{statistics.median(j.seconds for j in rs):.0f}",
            f"{statistics.mean(j.read_mb for j in rs) / 1024:.2f}",
            f"{statistics.mean(j.write_mb for j in rs) / 1024:.2f}",
        ]
        print("| " + " | ".join(str(c) for c in cells) + " |")
    full = [j for j in jobs if not j.on_slice]
    print(f"\nFull-B200 GPU-hours {gpu_hours(full):.1f}, busy {weighted(full, 'busy'):.1f} % (GPU-seconds weighted).")
    print(f"MIG-slice GPU-hours {gpu_hours([j for j in jobs if j.on_slice]):.1f} (busy is not reported on a slice).")
    if full:
        short = [j for j in full if j.seconds < 120]
        used = [j for j in full if j.gpu_mem_mb > 0]
        mean_all, median_all = statistics.mean(j.busy for j in full), statistics.median(j.busy for j in full)
        print(f"\nCounted per job instead (full B200, {len(full)} jobs):")
        print(f"mean {mean_all:.1f} %, median {median_all:.1f} %.")
        print(
            f"{len(short)} jobs ran under 2 min: {100 * len(short) / len(full):.0f} % of the jobs, "
            f"{100 * gpu_hours(short) / gpu_hours(full):.1f} % of the GPU-hours."
        )
        if used:
            print(
                f"The {len(used)} that allocated GPU memory: mean {statistics.mean(j.busy for j in used):.1f} %, "
                f"GPU-seconds weighted {weighted(used, 'busy'):.1f} %."
            )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--since", required=True, help="sacct -S date, e.g. 2026-11-01")
    ap.add_argument("--scope", choices=["me", "all"], default="me")
    ap.add_argument("--partition", default="g")
    ap.add_argument("--job-name", default="CryoBoost")
    ap.add_argument("--completed-only", action="store_true", help="leave failed and cancelled jobs out")
    ap.add_argument("--no-cache", action="store_true", help="look every job directory up again")
    args = ap.parse_args()

    jobs = load_jobs(args)
    if args.completed_only:
        held = gpu_hours([j for j in jobs if j.state != "COMPLETED"])
        jobs = [j for j in jobs if j.state == "COMPLETED"]
        print(f"Completed jobs only; the others held {held:.1f} GPU-h.\n")
    report(jobs)


if __name__ == "__main__":
    main()
