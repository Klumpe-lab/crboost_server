"""Read-side utilities for per-tilt-series array-job task tracking.

Pure state+disk readers shared by the UI (array_task_tracker, pipeline roster,
Journey dashboard collectors) — plus the tilt-series display-name helpers.
Owns the on-disk protocol names (`MANIFEST_FILENAME`, `STATUS_DIR_NAME`); the
writer side lives in `drivers/array_job_base.py`, which re-imports them from
here so the two ends can't drift.
"""

import json
import re
from pathlib import Path

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


def ts_anchor_id(instance_id: str, raw_name: str) -> str:
    """Stable DOM id for a per-TS row; used for deep-link scroll-to."""
    safe_iid = re.sub(r"[^A-Za-z0-9_-]", "_", instance_id)
    safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", raw_name)
    return f"ts-row-{safe_iid}-{safe_name}"


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
    except Exception:
        return ""


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def read_manifest(job_dir: Path) -> dict | None:
    """Read .task_manifest.json from a job directory."""
    manifest_path = job_dir / MANIFEST_FILENAME
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text())
    except Exception:
        return None


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

    statuses: dict[str, str] = {}
    for idx, name in enumerate(items):
        if name in ok_set:
            statuses[name] = "ok"
        elif name in fail_set:
            statuses[name] = "fail"
        elif name in skip_set:
            statuses[name] = "skip"
        elif (job_dir / f"task_{idx}.out").exists():
            statuses[name] = "running"
        else:
            statuses[name] = "pending"
    return statuses


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
