"""Replay a finished pipeline's task times through other pipeline shapes: how long would it have taken if each tilt
series went on to its next stage as soon as its own task ended, instead of waiting for the whole stage?

    sacct -u artem.kushner -S 2026-11-08T16:50 -E 2026-11-08T18:40 -P -n \
        -o JobID,JobName,Partition,AllocTRES,Elapsed,State,TotalCPU,AllocCPUS,TRESUsageInTot,StdOut,Start,End \
        > /scratch/users/artem.kushner/gpu_probe/pilot_412_sacct.txt
    python3 -B simulate_item_flow.py /scratch/users/artem.kushner/gpu_probe/pilot_412_sacct.txt \
        /groups/klumpe/crboost_data/slurmtest_412_FCIsolation

Reads only: the accounting dump, and each array job's `run_array.sh` (its throttle) and `task_<i>.out` (which tilt
series task i ran). The first table is what happened, stage by stage. The second replays it: every task keeps its
measured duration and its stage keeps its throttle, and only the moment a task may start changes. "Barriers" waits
for a whole stage as the afterok chain does, with the measured gaps between stages; it should land near the real
wall time and so checks the replay. "Barriers, no gaps" removes only the time between one stage's last task and
the next stage's first. "Per item" starts a tilt series' next task when its own previous one ends, with tsImport
either still one job for all (barrier) or split per tilt series (its time divided evenly, no throttle). A task waits
the median dispatch delay measured in the run (a task's start after the last task end before it) for a free slot,
and the given delay per dependency once its item is ready (the measured one, and 60 s). The replay sets no limit on
GPUs across stages: the last two columns say how many tasks it ran at once, all of them and all but template
matching's.
"""

import re
import statistics
import sys
from datetime import datetime
from pathlib import Path

STAGES = [  # (job name, what it is), in pipeline order; tsImport is one job for all tilt series
    ("cb-fsMotionAndCtf", "Warp motion + CTF"),
    ("cb-tsImport-sup", "tsImport"),
    ("cb-aligntiltsWarp", "AreTomo alignment"),
    ("cb-tsCtf", "Warp CTF"),
    ("cb-tsReconstruct", "Warp reconstruct"),
    ("cb-templatematching", "Template matching"),
    ("cb-tmextractcand", "Candidate extraction"),
    ("cb-warpExport", "Warp export"),
]
IMPORT = "cb-tsImport-sup"


def ts(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()


def clock(t: float) -> str:
    return datetime.fromtimestamp(t).strftime("%H:%M:%S")


def hms(s: float) -> str:
    s = round(s)
    return f"{s // 3600}:{s % 3600 // 60:02d}:{s % 60:02d}"


def load(dump: Path, project: str):
    """Per stage: {item: (start, end)} for its array tasks, its supervisor's (start, end), its job dir."""
    rows = [ln.rstrip("\n").split("|") for ln in dump.open()]
    mine = [r for r in rows if len(r) >= 12 and "." not in r[0] and project in r[9] and r[10][:1].isdigit()]
    stages = {}
    for name, _ in STAGES:
        tasks = [r for r in mine if r[1] == name and "_" in r[0]]
        sups = [r for r in mine if r[1] in (name, f"{name}-sup") and "_" not in r[0]]
        job_dir = Path(re.sub(r"/[^/]*$", "", (tasks or sups)[0][9])) if (tasks or sups) else None
        items = {}
        for r in tasks:
            idx = r[0].split("_")[1]
            log = job_dir / f"task_{idx}.out"
            m = re.search(r"ts_name=(\S+)", log.read_text(errors="replace")) if log.exists() else None
            if m and r[11][:1].isdigit():
                items[m.group(1)] = (ts(r[10]), ts(r[11]))  # a rerun of the same item keeps the later one
        sup = (ts(sups[-1][10]), ts(sups[-1][11])) if sups and sups[-1][11][:1].isdigit() else None
        stages[name] = dict(items=items, sup=sup, job_dir=job_dir)
    return stages


def throttle(job_dir: Path | None) -> int | None:
    script = job_dir / "run_array.sh" if job_dir else None
    m = re.search(r"--array=\S*%(\d+)", script.read_text()) if script and script.exists() else None
    return int(m.group(1)) if m else None


def dispatch_delays(items: dict) -> list[float]:
    """For each task after the first wave: its start minus the last task end before it."""
    ends = sorted(e for _, e in items.values())
    out = []
    for s, _ in sorted(items.values()):
        before = [e for e in ends if e <= s]
        if before:
            out.append(s - before[-1])
    return out


def run_stage(ready: dict, durations: dict, slots: int | None, delay: float, dep_delay: float, spans: list) -> dict:
    """List scheduling: items in order of readiness, each on the first free slot; returns {item: end} and appends
    each task's (start, end) to `spans`. A task starts `dep_delay` after its item is ready and `delay` after its slot
    frees."""
    free = [0.0] * (slots or len(ready) or 1)
    out = {}
    for item, r in sorted(ready.items(), key=lambda kv: kv[1]):
        if item not in durations:
            continue
        i = min(range(len(free)), key=free.__getitem__)
        start = max(r + dep_delay, free[i] + delay)
        free[i] = out[item] = start + durations[item]
        spans.append((start, out[item]))
    return out


def peak(spans: list) -> int:
    events = sorted([(s, 1) for s, _ in spans] + [(e, -1) for _, e in spans])
    level = top = 0
    for _, step in events:
        level += step
        top = max(top, level)
    return top


def replay(stages, slots, gaps, delay, shape: str, dep_delay: float) -> tuple[float, int, int]:
    """Wall time, and the most tasks running at once: all stages, and all but template matching."""
    names = [n for n, _ in STAGES if stages[n]["items"] or n == IMPORT]
    first = stages[names[0]]
    t0 = min(s for s, _ in first["items"].values())
    ready = {item: t0 - delay for item in first["items"]}
    imp = stages[IMPORT]["sup"]
    import_secs = imp[1] - imp[0] if imp else 0.0
    end = t0
    spans: dict[str, list] = {}
    for k, name in enumerate(names):
        st = stages[name]
        if name == IMPORT:
            if shape == "per item, split import":
                per = import_secs / max(1, len(ready))
                ready = {item: r + per for item, r in ready.items()}
            else:
                done = max(ready.values()) + (gaps.get(name, 0.0) if shape == "barriers" else delay)
                ready = dict.fromkeys(ready, done + import_secs)
            continue
        durations = {i: e - s for i, (s, e) in st["items"].items()}
        if shape in ("barriers", "barriers, no gaps") and k > 0:
            done = max(ready.values()) + (gaps.get(name, 0.0) if shape == "barriers" else 0.0)
            ready = dict.fromkeys(ready, done)
        ends = run_stage(ready, durations, slots.get(name), delay, dep_delay, spans.setdefault(name, []))
        ready = ends
        end = max(ends.values())
    others = [sp for n, v in spans.items() if n != "cb-templatematching" for sp in v]
    return end - t0, peak([sp for v in spans.values() for sp in v]), peak(others)


def main() -> None:
    dump, project = Path(sys.argv[1]), sys.argv[2].rstrip("/")
    stages = load(dump, project)
    slots = {n: throttle(stages[n]["job_dir"]) for n, _ in STAGES}
    delays = [d for n, _ in STAGES for d in dispatch_delays(stages[n]["items"])]
    delay = statistics.median(delays) if delays else 0.0

    print(f"{Path(project).name}: what happened\n")
    print("| Stage | Tasks | Throttle | Median task s | First task start | Last task end | Gap before it s |")
    print("|---|---:|---:|---:|---|---|---:|")
    gaps, prev_end, t_first, t_last = {}, None, None, None
    for name, label in STAGES:
        st = stages[name]
        spans = list(st["items"].values()) or ([st["sup"]] if st["sup"] else [])
        if not spans:
            continue
        start, end = min(s for s, _ in spans), max(e for _, e in spans)
        gaps[name] = start - prev_end if prev_end is not None else 0.0
        t_first = start if t_first is None else t_first
        med = statistics.median(e - s for s, e in spans)
        print(
            f"| {label} | {len(st['items']) or 1} | {slots[name] or '-'} | {med:.0f} | {clock(start)} | {clock(end)} "
            f"| {gaps[name]:.0f} |"
        )
        prev_end = t_last = end
    print(
        f"\nWall time from the first task to the last: {hms(t_last - t_first)}. Gaps between stages: "
        f"{hms(sum(gaps.values()))}. Median dispatch delay inside an array: {delay:.0f} s "
        f"({len(delays)} task starts).\n"
    )

    print("Replayed with each task's own duration and each stage's throttle:\n")
    print("| Shape | Delay per dependency | Wall time | Against the real run | Most tasks at once | Without TM |")
    print("|---|---:|---:|---:|---:|---:|")
    real = t_last - t_first
    for shape, dep in (
        ("barriers", delay),
        ("barriers, no gaps", delay),
        ("per item, import as one job", delay),
        ("per item, import as one job", 60.0),
        ("per item, split import", delay),
        ("per item, split import", 60.0),
    ):
        wall, top, top_others = replay(stages, slots, gaps, delay, shape, dep)
        print(f"| {shape} | {dep:.0f} s | {hms(wall)} | {100 * (wall - real) / real:+.0f} % | {top} | {top_others} |")


if __name__ == "__main__":
    main()
