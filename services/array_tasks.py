"""Per-tilt-series array-job task tracking: manifest/status readers + tallies.

Shared by the UI (array_task_tracker, pipeline roster, Journey dashboard
collectors) and the control plane (pipeline_runner) — plus the tilt-series
display-name helpers. Owns the on-disk protocol names (`MANIFEST_FILENAME`,
`STATUS_DIR_NAME`); the task-side writers live in `drivers/array_job_base.py`,
which re-imports them from here so the two ends can't drift. The one
control-plane writer, `mark_stopped_tasks_failed`, lives here because only
`pipeline_runner`'s Stop and Cancel may call it (post-scancel).
"""

import json
import os
import re
from collections import Counter
from collections.abc import Collection
from pathlib import Path
from typing import NamedTuple

from services.tilt_series.build import parse_position

MANIFEST_FILENAME = ".task_manifest.json"
STATUS_DIR_NAME = ".task_status"


def ts_display_name(raw_name: str) -> str:
    """Derive a human-readable display name from a raw tilt-series name.

    Keeps the Position_{stage}_{beam} suffix from tomostar-derived names:
      'agg5_20251113_412_Position_11'   -> 'Position_11'
      'agg5_20251113_412_Position_11_2' -> 'Position_11_2'

    Falls back to the raw name if the pattern doesn't match.
    """
    parsed = parse_position(raw_name)
    if parsed is None:
        return raw_name
    stage, beam = parsed
    return f"Position_{stage}_{beam}" if beam else f"Position_{stage}"


def shorten_ts_names(items: list[str]) -> dict[str, str]:
    """Map raw TS names to display names using Position_{stage}_{beam} scheme."""
    return {name: ts_display_name(name) for name in items}


def ts_pretty_name(raw_name: str) -> str:
    """Human-friendly tomogram label: 'agg_..._Position_22_2' -> 'Pos 22 · Beam 2'.
    Falls back to the raw name when there's no Position_{stage}[_{beam}] suffix."""
    parsed = parse_position(raw_name)
    if parsed is None:
        return raw_name
    stage, beam = parsed
    return f"Pos {stage} · Beam {beam}" if beam else f"Pos {stage}"


def ts_position_sort_key(raw_name: str):
    """Numeric (stage, beam) key for sorting TS names ascending.

    Names without a Position_{stage}[_{beam}] suffix sort after parseable ones
    and then alphabetically among themselves so the order is still stable.
    """
    parsed = parse_position(raw_name)
    if parsed is None:
        return (1, float("inf"), 0, raw_name)
    stage, beam = parsed
    return (0, stage, beam or 0, raw_name)


def sort_ts_by_position(items: list[str]) -> list[str]:
    """Return items ordered by (stage, beam) ascending."""
    return sorted(items, key=ts_position_sort_key)


def read_tail(path: Path, max_lines: int = 200) -> str:
    """Read the tail of a log file, truncating if needed."""
    if not path.exists():
        return ""
    try:
        text = path.read_text(errors="replace")
        lines = text.splitlines()
        if len(lines) > max_lines:
            return f"[... truncated {len(lines) - max_lines} lines ...]\n" + "\n".join(lines[-max_lines:])
        return text
    except OSError:
        # Log may vanish mid-read (job reset/cleanup); the tail is best-effort display.
        return ""


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def read_manifest(job_dir: Path) -> dict | None:
    """Read .task_manifest.json from a job directory. None means "no manifest"."""
    manifest_path = job_dir / MANIFEST_FILENAME
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text())
    except (OSError, ValueError):
        # Unreadable or mid-write manifest == "no manifest yet"; callers poll again.
        return None


def manifest_items(job_dir: Path) -> list[str]:
    """The manifest's item list (raw TS names in array-index order), or []."""
    manifest = read_manifest(job_dir)
    return (manifest.get("items") or []) if manifest else []


def manifest_array_job_id(job_dir: Path) -> str | None:
    """The child-array SLURM job id the supervisor wrote back after sbatch, or None."""
    manifest = read_manifest(job_dir)
    jid = manifest.get("array_job_id") if manifest else None
    return str(jid) if jid else None


def recorded_array_job_id(job_dir: Path) -> str | None:
    """manifest_array_job_id for a caller deciding whether the job still runs: a manifest that does
    not parse (mid-write or damaged) raises ValueError naming the file, since reading it as "no
    array" would let that caller go ahead over a live one."""
    path = job_dir / MANIFEST_FILENAME
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    try:
        jid = json.loads(text).get("array_job_id")
    except (ValueError, AttributeError) as e:  # AttributeError: valid JSON that is not an object
        raise ValueError(f"{path} does not parse ({e})") from None
    return str(jid) if jid else None


def any_task_started(job_dir: Path) -> bool | None:
    """QUEUED-vs-RUNNING refinement for array-dispatching jobs; None = no manifest.

    The supervisor writes .task_manifest.json BEFORE it sbatches the child
    array, and its own SLURM state can read RUNNING while every child task is
    still PENDING in the queue — so neither manifest existence nor supervisor
    state is a truthful "work in flight" signal. SLURM creates task_<idx>.out
    the moment a child STARTS, so that is the real signal. A no-manifest
    (single-shot) job returns None: the caller falls back to supervisor state.
    """
    if not (job_dir / MANIFEST_FILENAME).exists():
        return None
    return any(job_dir.glob("task_*.out"))


def scan_statuses(job_dir: Path, items: list[str]) -> dict[str, str]:
    """Scan .task_status/ dir and return {item_name: status_string}.

    Uses task_{idx}.out existence to distinguish running vs pending:
      - .task_status/{name}.ok   → "ok"
      - .task_status/{name}.fail → "fail"
      - .task_status/{name}.skip → "skip" (supervisor pre-marked; never dispatched)
      - task_{idx}.out exists (no status file) → "running" (SLURM started it)
      - task_{idx}.out missing (no status file) → "pending" (still queued)
    """
    status_dir = job_dir / STATUS_DIR_NAME
    ok_set: set = set()
    fail_set: set = set()
    skip_set: set = set()
    if status_dir.is_dir():
        for p in status_dir.iterdir():
            if p.suffix == ".ok":
                ok_set.add(p.stem)
            elif p.suffix == ".fail":
                fail_set.add(p.stem)
            elif p.suffix == ".skip":
                skip_set.add(p.stem)

    # One directory listing rather than one stat() per item: this runs on a 5 s UI timer over
    # the shared filesystem, where a 114-tilt-series job would cost 114 round-trips a tick.
    started: set = set()
    if job_dir.is_dir():
        started = {e.name for e in os.scandir(job_dir) if e.name.startswith("task_") and e.name.endswith(".out")}

    statuses: dict[str, str] = {}
    for idx, name in enumerate(items):
        if name in ok_set:
            statuses[name] = "ok"
        elif name in fail_set:
            statuses[name] = "fail"
        elif name in skip_set:
            statuses[name] = "skip"
        elif f"task_{idx}.out" in started:
            statuses[name] = "running"
        else:
            statuses[name] = "pending"
    return statuses


# A task log's first line (services/computing/slurm_service.py SLURM_ID_LINE), and the line a chunk item's own
# log starts with instead (drivers/array_job_base.py chunk_item_header), naming the task the item runs in.
_SLURM_ID_LINE = re.compile(r"^\[SLURM\] job (\d+_\d+) = ")
_CHUNK_LINE = re.compile(r"^\[CHUNK\] .* runs in array task (\d+) ")
_LOG_HEAD_BYTES = 2048
_LOG_HEAD_LINES = 8


def _log_head(path: Path) -> tuple[str | None, int | None]:
    """(SLURM id, chunk task index) named by the first lines of a task log; (None, None) when it names neither."""
    try:
        with open(path, "rb") as f:
            head = f.read(_LOG_HEAD_BYTES).decode("utf-8", errors="replace")
    except FileNotFoundError:  # removed between the listing and the read
        return None, None
    for line in head.splitlines()[:_LOG_HEAD_LINES]:
        if m := _SLURM_ID_LINE.match(line):
            return m.group(1), None
        if m := _CHUNK_LINE.match(line):
            return None, int(m.group(1))
    return None, None


def task_slurm_ids(job_dir: Path, manifest: dict, only: Collection[str] | None = None) -> dict[str, str]:
    """{item: "<array job>_<task index>"}, the SLURM id each item runs (or ran) under, for the items in `only`
    (every item when None). An item of the current submission's chunks runs in its chunk's task, which the
    manifest names; any other item's id is on the first line of its task_<idx>.out, or of the chunk task's log
    it points to. Items whose log has not started, or names no id, are left out."""
    items = manifest.get("items") or []
    array_id = manifest.get("array_job_id")
    chunk_task = {int(i): int(task) for task, indices in (manifest.get("chunks") or {}).items() for i in indices}
    started: set[str] = set()
    if job_dir.is_dir():
        started = {e.name for e in os.scandir(job_dir) if e.name.startswith("task_") and e.name.endswith(".out")}
    heads: dict[int, tuple[str | None, int | None]] = {}

    def head(idx: int) -> tuple[str | None, int | None]:
        if idx not in heads:
            name = f"task_{idx}.out"
            heads[idx] = _log_head(job_dir / name) if name in started else (None, None)
        return heads[idx]

    ids: dict[str, str] = {}
    for idx, name in enumerate(items):
        if only is not None and name not in only:
            continue
        if array_id and idx in chunk_task:
            ids[name] = f"{array_id}_{chunk_task[idx]}"
            continue
        sid, task = head(idx)
        if sid is None and task is not None:
            sid = head(task)[0]
        if sid:
            ids[name] = sid
    return ids


class TaskProgress(NamedTuple):
    """Job-level tally over per-item statuses; the single settledness arithmetic.

    Every consumer (roster chip, tracker summary/progress-bar, dashboards)
    derives its counts from here so they can't disagree on what "settled"
    means. Skipped items ARE settled: they were deliberately never dispatched
    and will not run, so a job with only oks + skips left is complete.
    """

    n_ok: int
    n_fail: int
    n_skip: int
    n_running: int
    n_pending: int
    total: int

    @property
    def n_settled(self) -> int:
        """Items that will never run again in this submission: ok + fail + skip."""
        return self.n_ok + self.n_fail + self.n_skip

    @classmethod
    def from_statuses(cls, statuses: dict[str, str]) -> "TaskProgress":
        c = Counter(statuses.values())
        return cls(c["ok"], c["fail"], c["skip"], c["running"], c["pending"], len(statuses))


def progress(job_dir: Path, items: list[str]) -> TaskProgress:
    """One-stop per-item scan + tally for the given manifest items."""
    return TaskProgress.from_statuses(scan_statuses(job_dir, items))


def eta_s(job_dir: Path, p: TaskProgress) -> float | None:
    """Seconds until the rest of the items settle, at the pace this submission's .ok markers have kept
    from the first to the latest. Markers older than the manifest are an earlier submission's and do not
    count. None before two have landed, or when nothing is left."""
    remaining = p.total - p.n_settled
    if remaining <= 0:
        return None
    try:
        since = (job_dir / MANIFEST_FILENAME).stat().st_mtime
        marks = [e.stat().st_mtime for e in os.scandir(job_dir / STATUS_DIR_NAME) if e.name.endswith(".ok")]
    except FileNotFoundError:  # no manifest or no markers yet: nothing to time
        return None
    times = sorted(t for t in marks if t >= since)
    if len(times) < 2 or times[-1] <= times[0]:
        return None
    return remaining * (times[-1] - times[0]) / (len(times) - 1)


def mark_stopped_tasks_failed(job_dir: Path) -> int:
    """For each manifest item without a status marker but with task_{idx}.out on
    disk (i.e. it was actively running when the pipeline got stopped), write a
    .fail marker atomically. Returns the count of markers written.

    Control-plane writer: call only AFTER the owning array has been scancel'd.
    Nothing but that convention guarantees the task is dead and won't write its
    own marker.
    """
    items = manifest_items(job_dir)
    if not items:
        return 0

    status_dir = job_dir / STATUS_DIR_NAME
    status_dir.mkdir(parents=True, exist_ok=True)
    existing = {p.stem for p in status_dir.iterdir() if p.suffix in (".ok", ".fail", ".skip")}

    marked = 0
    for idx, name in enumerate(items):
        if name in existing:
            continue
        if not (job_dir / f"task_{idx}.out").exists():
            continue
        # Atomic write: temp then rename (same dot-tmp convention as the
        # task-side writers in drivers/array_job_base.py).
        tmp = status_dir / f".{name}.fail.tmp"
        try:
            tmp.write_text("")
            os.replace(tmp, status_dir / f"{name}.fail")
            marked += 1
        except OSError:
            # Best-effort post-stop sweep: an unwritable marker just leaves this
            # task looking "running"; don't abort marking the rest.
            tmp.unlink(missing_ok=True)
    return marked


def resolve_job_dir(job_model, project_path: Path | None = None) -> Path | None:
    """Resolve the on-disk job directory from a job model."""
    stored = (job_model.paths or {}).get("job_dir")
    if stored:
        p = Path(stored)
        if p.is_dir():
            return p
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn and project_path:
        p = project_path / rjn.rstrip("/")
        if p.is_dir():
            return p
    return None
