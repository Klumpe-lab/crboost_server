"""Summarise probe_io.sh: per phase, what each task got and what the sixteen got together.

    python3 summarise_io.py <folder holding the probe_io-*.out files>

Per task MB/s is the file size over the task's own time. Together is the bytes of all tasks over the time from the
first task's start to the last task's end; "start spread" says how far apart the tasks began, so a spread of more
than a few seconds means they did not hit the storage together.
"""

import statistics
import sys
from collections import defaultdict
from pathlib import Path

ORDER = [
    "write_groups_shared",
    "write_groups_own",
    "write_tmp",
    "write_scratch",
    "read_groups_shared",
    "read_groups_own",
    "read_tmp",
    "read_scratch",
    "read_real_maps_one_folder",
]


def main() -> None:
    folder = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    phases: dict[str, list[tuple[float, float, float, str]]] = defaultdict(list)
    for out in sorted(folder.glob("probe_io-*.out")):
        for line in out.read_text().splitlines():
            if line.startswith("RESULT\t"):
                _, phase, _task, host, t0, t1, rate = line.split("\t")
                phases[phase].append((float(t0), float(t1), float(rate), host))
        for line in out.read_text().splitlines():
            if line.startswith(("FAILED", "VOID")):
                print(f"{out.name}: {line}")
    print("| phase | tasks | nodes | per task MB/s median (min-max) | together MB/s | start spread s |")
    print("|---|---:|---:|---|---:|---:|")
    for phase in ORDER:
        rows = phases.get(phase)
        if not rows:
            continue
        rates = [r[2] for r in rows]
        # Each task moved the same number of MB, which is rate x its own time.
        megabytes = sum(r[2] * (r[1] - r[0]) for r in rows)
        span = max(r[1] for r in rows) - min(r[0] for r in rows)
        spread = max(r[0] for r in rows) - min(r[0] for r in rows)
        print(
            f"| {phase} | {len(rows)} | {len({r[3] for r in rows})} | {statistics.median(rates):.0f} "
            f"({min(rates):.0f}-{max(rates):.0f}) | {megabytes / span:.0f} | {spread:.1f} |"
        )


if __name__ == "__main__":
    main()
