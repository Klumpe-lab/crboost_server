#!/usr/bin/env python3
"""Delete the `.staging/` copies that Motion & CTF and Alignment jobs left behind.

Until their collect step became a move, both jobs copied each tilt-series' outputs out of
`<job>/.staging/task_<ts>/` into the job dir and kept the staged copies, which nothing reads.
This deletes those copies, for finished tilt-series only:

    - the job's instance id (run_submit.script) is fsMotionAndCtf or aligntiltsWarp;
    - the tilt-series has a `.task_status/<ts>.ok` marker (its collect ran to the end);
    - the job dir holds a regular file at the same place with the same size (for an alignment
      that ran under the AreTomo alias, under the series' real name).

Anything else in `.staging/` (frame symlinks, settings, tomostars, logs that were never
collected) stays. Dry run by default; `--delete` deletes.

    clear_staging_duplicates.py [--delete] PATH [PATH ...]

PATH is a job dir (`<project>/External/job002`), a project dir, or a directory of projects.
Standard library only: runs with any python3 on the headnode, server stopped or not.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Mirrors the drivers: services/array_tasks.py STATUS_DIR_NAME, drivers/ts_alignment.py ARETOMO_ALIAS.
STATUS_DIR_NAME = ".task_status"
ARETOMO_ALIAS = "crbalias"

# Job type -> the staged folder its collect copied out of.
COLLECTED_FOLDER = {"fsMotionAndCtf": "warp_frameseries", "aligntiltsWarp": "warp_tiltseries"}

_INSTANCE_ID_RE = re.compile(r"--instance_id\s+(\S+)")


@dataclass
class JobReport:
    job_dir: Path
    job_type: str
    files: list[Path] = field(default_factory=list)
    size_bytes: int = 0
    ts_done: int = 0
    ts_not_ok: list[str] = field(default_factory=list)
    unmatched: int = 0
    errors: list[str] = field(default_factory=list)


def job_type(job_dir: Path) -> str | None:
    try:
        text = (job_dir / "run_submit.script").read_text(errors="replace")
    except FileNotFoundError:
        # Never submitted: no staging to clear.
        return None
    match = _INSTANCE_ID_RE.search(text)
    return match.group(1).split("__", 1)[0] if match else None


def real_name(part: str, ts_name: str) -> str:
    """A staged path component under its collected name (see rename_alias_files)."""
    if part == ARETOMO_ALIAS:
        return ts_name
    if part.startswith(ARETOMO_ALIAS) and part[len(ARETOMO_ALIAS) : len(ARETOMO_ALIAS) + 1] in (".", "_"):
        return ts_name + part[len(ARETOMO_ALIAS) :]
    return part


def collected_copy(job_dir: Path, ts_name: str, rel: Path) -> Path:
    return job_dir.joinpath(*(real_name(p, ts_name) for p in rel.parts))


def is_duplicate(staged: Path, copy: Path, staging_root: Path, size: int) -> bool:
    try:
        st = copy.lstat()
    except FileNotFoundError:
        return False
    if not os.path.isfile(copy) or os.path.islink(copy) or st.st_size != size:
        return False
    # A copy reached through a symlinked dir that points back into staging is the staged file itself.
    resolved = copy.resolve()
    return resolved != staged.resolve() and staging_root.resolve() not in resolved.parents


def scan_job(job_dir: Path, jtype: str) -> JobReport:
    report = JobReport(job_dir, jtype)
    staging_root = job_dir / ".staging"
    if not staging_root.is_dir():
        return report
    status_dir = job_dir / STATUS_DIR_NAME
    ok = {p.name[: -len(".ok")] for p in status_dir.glob("*.ok")} if status_dir.is_dir() else set()
    folder = COLLECTED_FOLDER[jtype]

    for task_dir in sorted(staging_root.glob("task_*")):
        ts_name = task_dir.name[len("task_") :]
        if ts_name not in ok:
            report.ts_not_ok.append(ts_name)
            continue
        report.ts_done += 1
        for dirpath, _dirnames, filenames in os.walk(task_dir / folder):
            for name in filenames:
                staged = Path(dirpath) / name
                st = staged.lstat()
                if not os.path.isfile(staged) or os.path.islink(staged):
                    continue
                copy = collected_copy(job_dir, ts_name, staged.relative_to(task_dir))
                if is_duplicate(staged, copy, staging_root, st.st_size):
                    report.files.append(staged)
                    report.size_bytes += st.st_blocks * 512
                else:
                    report.unmatched += 1
    return report


def delete(report: JobReport) -> None:
    for path in report.files:
        try:
            path.unlink()
        except OSError as e:
            report.errors.append(f"{path}: {e.strerror}")
    # Prune the directories the deletions emptied, deepest first; a non-empty one stays.
    staging_root = report.job_dir / ".staging"
    emptied = {p.parent for p in report.files}
    emptied |= {a for d in emptied for a in d.parents if staging_root in a.parents}
    for d in sorted(emptied, reverse=True):
        try:
            d.rmdir()
        except OSError:
            # Still holds what was not deleted (or is not ours to remove): it stays.
            pass


def job_dirs(path: Path) -> list[Path]:
    if (path / "run_submit.script").exists():
        return [path]
    if (path / "External").is_dir():
        return sorted((path / "External").glob("job*"))
    return sorted(j for project in path.iterdir() if (project / "External").is_dir() for j in job_dirs(project))


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--delete", action="store_true", help="delete (default: only report)")
    args = parser.parse_args()

    total_bytes = total_files = 0
    failed = False
    for root in args.paths:
        for jdir in job_dirs(root.resolve()):
            jtype = job_type(jdir)
            if jtype not in COLLECTED_FOLDER:
                continue
            report = scan_job(jdir, jtype)
            if not report.files and not report.ts_not_ok:
                continue
            if args.delete:
                delete(report)
            size = human(report.size_bytes)
            line = f"{size:>10}  {len(report.files):>7} files  {report.ts_done:>3} TS  {jtype:<15} {jdir}"
            if report.ts_not_ok:
                line += f"  [kept: {len(report.ts_not_ok)} TS without .ok]"
            if report.unmatched:
                line += f"  [kept: {report.unmatched} files with no identical collected copy]"
            print(line, flush=True)
            for e in report.errors:
                print(f"    could not delete {e}", file=sys.stderr)
            failed |= bool(report.errors)
            total_bytes += report.size_bytes
            total_files += len(report.files)

    verb = "deleted" if args.delete else "would delete (dry run; --delete to delete)"
    print(f"{human(total_bytes)} in {total_files} files {verb}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
