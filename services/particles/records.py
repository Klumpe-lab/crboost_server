"""Analytics records: a computed number never travels without where it came from.

Every analytic the app shows is a record — ``{"provenance": …, "data": …}`` — whose provenance names
the source files (path + mtime), the job and instance that produced them, the code that computed it
(module + schema version), how (a job's driver or a backfill from files a finished job left) and
when. Views render records and nothing else, so a view can be redesigned or dropped without
re-wiring or recomputing anything.

Where records live:

- **Driver sidecars**, written by a job's own driver on the compute node, inside the job dir:
  ``<job>/analytics/…`` (see each driver). Heavy numbers (score-map scans) exist only there.
- **Project records**, ``<project>/Analytics/records/<kind>/<key>.json``: what the server assembled
  or backfilled from a job's files. Each carries a fingerprint of its sources and is recomputed
  when the fingerprint moves.
- **Particle sets**, ``<project>/Analytics/sets/`` (``services/particles/particle_sets.py``).

Standard library only: the drivers import this on compute nodes.
"""

from __future__ import annotations

import getpass
import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

ANALYTICS_DIR = "Analytics"
RECORDS_SUBDIR = "records"
JOB_SIDECAR_DIR = "analytics"


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def current_user() -> str:
    try:
        return getpass.getuser()
    except (KeyError, OSError):  # no passwd entry for the uid (containers): the uid still names the writer
        return str(os.getuid())


def file_mtime(path: str | Path) -> float | None:
    """mtime of `path`, or None when it does not exist."""
    try:
        return os.stat(path).st_mtime
    except FileNotFoundError:
        return None


@dataclass(frozen=True, slots=True)
class SourceStamp:
    """One file a record was computed from, as it was then."""

    path: str
    mtime: float | None  # None: absent when the record was computed

    @classmethod
    def of(cls, path: str | Path) -> SourceStamp:
        return cls(str(path), file_mtime(path))


@dataclass(frozen=True, slots=True)
class Producer:
    """The job whose files a record describes."""

    job_type: str = ""
    instance_id: str = ""
    job_dir: str = ""


@dataclass(frozen=True, slots=True)
class Provenance:
    kind: str  # what the data block is, e.g. "tm_tomogram", "tm_health", "candidates"
    schema: int  # version of that data block's layout
    module: str  # the code that computed it
    computed_by: str  # "driver" | "backfill" | "server"
    computed_at: str = field(default_factory=now_iso)
    producer: Producer = field(default_factory=Producer)
    sources: tuple[SourceStamp, ...] = ()
    # What the sources looked like as a whole when this was computed (directory mtimes, a star's
    # size); `fingerprint_of` recomputes it to decide whether a stored record still holds.
    fingerprint: str = ""
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Provenance:
        producer = d.get("producer") or {}
        return cls(
            kind=str(d.get("kind", "")),
            schema=int(d.get("schema", 0)),
            module=str(d.get("module", "")),
            computed_by=str(d.get("computed_by", "")),
            computed_at=str(d.get("computed_at", "")),
            producer=Producer(**{k: str(v) for k, v in producer.items() if k in Producer.__dataclass_fields__}),
            sources=tuple(
                SourceStamp(str(s.get("path", "")), s.get("mtime"))
                for s in d.get("sources") or []
                if isinstance(s, dict)
            ),
            fingerprint=str(d.get("fingerprint", "")),
            note=str(d.get("note", "")),
        )


@dataclass(frozen=True, slots=True)
class Record:
    provenance: Provenance
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"provenance": self.provenance.to_dict(), "data": self.data}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Record:
        return cls(Provenance.from_dict(d.get("provenance") or {}), dict(d.get("data") or {}))


def fingerprint_of(paths: list[str | Path]) -> str:
    """Cheap identity of a set of files or directories: each one's mtime (ns) and size. A
    directory's mtime moves when an entry is added, removed or renamed into it, which is how
    drivers publish (write a temp file, rename it)."""
    parts = []
    for p in paths:
        try:
            st = os.stat(p)
            parts.append(f"{p}:{st.st_mtime_ns}:{st.st_size}")
        except FileNotFoundError:
            parts.append(f"{p}:-")
    return "|".join(parts)


def write_json_atomic(path: Path, payload: Any) -> None:
    """Write JSON through a uniquely named temp file and a rename, so a reader never sees half a
    file and two writers never share a temp name."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp{uuid.uuid4().hex[:8]}")
    tmp.write_text(json.dumps(payload, indent=1, default=_json_default))
    tmp.replace(path)


def _json_default(o: Any) -> Any:
    # numpy scalars and arrays reach records from the statistics code.
    if hasattr(o, "item") and callable(o.item):
        try:
            return o.item()
        except (TypeError, ValueError):  # a multi-element array: fall through to tolist
            pass
    if hasattr(o, "tolist"):
        return o.tolist()
    if isinstance(o, Path):
        return str(o)
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


def write_record(path: Path, record: Record) -> None:
    write_json_atomic(path, record.to_dict())


def read_record(path: Path) -> Record | None:
    """The record at `path`, or None when there is none. A file that exists but does not parse
    raises: a corrupt record is reported, never read as "no record"."""
    try:
        text = Path(path).read_text()
    except FileNotFoundError:
        return None
    return Record.from_dict(json.loads(text))


def analytics_root(project_path: Path) -> Path:
    return Path(project_path) / ANALYTICS_DIR


def project_record_path(project_path: Path, kind: str, key: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in key) or "_"
    return analytics_root(project_path) / RECORDS_SUBDIR / kind / f"{safe}.json"


def job_sidecar_dir(job_dir: Path) -> Path:
    return Path(job_dir) / JOB_SIDECAR_DIR
