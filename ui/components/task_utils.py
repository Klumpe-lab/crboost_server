# ui/components/task_utils.py
"""
Shared utilities for per-tilt-series task tracking across array jobs.
Used by both array_task_tracker.py and ts_journey_view.py.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional

_POSITION_RE = re.compile(r"Position_(\d+)(?:_(\d+))?$")


def ts_display_name(raw_name: str) -> str:
    """Derive a human-readable display name from a raw tilt-series name.

    Parses the Position_{stage}_{beam} suffix from tomostar-derived names:
      'agg5_20251113_412_Position_11'   -> 'Position_11'
      'agg5_20251113_412_Position_11_2' -> 'Position_11_2'

    Falls back to the raw name if the pattern doesn't match.
    """
    m = _POSITION_RE.search(raw_name)
    if m:
        stage = m.group(1)
        beam = m.group(2)
        return f"Position_{stage}_{beam}" if beam else f"Position_{stage}"
    return raw_name


def shorten_ts_names(items: List[str]) -> Dict[str, str]:
    """Map raw TS names to display names using Position_{stage}_{beam} scheme."""
    return {name: ts_display_name(name) for name in items}


def ts_position_sort_key(raw_name: str):
    """Numeric (stage, beam) key for sorting TS names ascending.

    Names without a Position_{stage}[_{beam}] suffix sort after parseable ones
    and then alphabetically among themselves so the order is still stable.
    """
    m = _POSITION_RE.search(raw_name)
    if not m:
        return (1, float("inf"), 0, raw_name)
    stage = int(m.group(1))
    beam = int(m.group(2)) if m.group(2) else 0
    return (0, stage, beam, raw_name)


def sort_ts_by_position(items: List[str]) -> List[str]:
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


def read_manifest(job_dir: Path) -> Optional[dict]:
    """Read .task_manifest.json from a job directory."""
    manifest_path = job_dir / ".task_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text())
    except Exception:
        return None


def scan_statuses(job_dir: Path, items: List[str]) -> Dict[str, str]:
    """Scan .task_status/ dir and return {item_name: status_string}.

    Uses task_{idx}.out existence to distinguish running vs pending:
      - .task_status/{name}.ok  → "ok"
      - .task_status/{name}.fail → "fail"
      - task_{idx}.out exists (no status file) → "running" (SLURM started it)
      - task_{idx}.out missing (no status file) → "pending" (still queued)
    """
    status_dir = job_dir / ".task_status"
    ok_set: set = set()
    fail_set: set = set()
    if status_dir.is_dir():
        for p in status_dir.iterdir():
            if p.suffix == ".ok":
                ok_set.add(p.stem)
            elif p.suffix == ".fail":
                fail_set.add(p.stem)

    statuses: Dict[str, str] = {}
    for idx, name in enumerate(items):
        if name in ok_set:
            statuses[name] = "ok"
        elif name in fail_set:
            statuses[name] = "fail"
        elif (job_dir / f"task_{idx}.out").exists():
            statuses[name] = "running"
        else:
            statuses[name] = "pending"
    return statuses


def resolve_job_dir(job_model, project_path: Optional[Path] = None) -> Optional[Path]:
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
