"""Where a kitchen pool keeps its files, inside the home project only::

    Analytics/kitchen/<species>/pool.json          the KitchenPool (atomic writes)
    Analytics/kitchen/<species>/verdicts.jsonl     hand verdicts, append-only
    Analytics/kitchen/<species>/review.json        the review sample and its good / bad labels
    Analytics/kitchen/<species>/table/             one column table per member, by fingerprint
    Analytics/kitchen/<species>/tiles/<frame>/     cutouts rendered on demand
    Analytics/kitchen/<species>/slabs/<frame>/     the slabs' display copies

Only the server writes here. Verdict events are ``{at, user, uid, op, value, source}``: ``op`` "verdict",
value keep / drop / clear; the latest event per uid wins.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

from services.kitchen.models import KitchenPool, Verdict
from services.particles.records import analytics_root, current_user, now_iso, write_json_atomic

logger = logging.getLogger(__name__)

KITCHEN_SUBDIR = "kitchen"
POOL_FILE = "pool.json"
VERDICTS_FILE = "verdicts.jsonl"
# The review sample and its good / bad labels (roadmap 40 S6.6): the reviewer's, never verdicts.
REVIEW_FILE = "review.json"
# The log is rewritten to one line per live entry once it is this many times longer.
COMPACT_FACTOR = 4
COMPACT_MIN_LINES = 2000


def safe_name(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_") or "_"


def kitchen_dir(home: Path, species_id: str) -> Path:
    return analytics_root(home) / KITCHEN_SUBDIR / safe_name(species_id)


def table_dir(home: Path, species_id: str) -> Path:
    return kitchen_dir(home, species_id) / "table"


def frame_token(frame_key: str) -> str:
    """A frame's directory name under tiles/ and slabs/: the volume's name and a hash of its path."""
    stem = safe_name(Path(frame_key).stem)[:60]
    return f"{stem}-{hashlib.sha1(frame_key.encode()).hexdigest()[:8]}"


def tiles_dir(home: Path, species_id: str, frame_key: str) -> Path:
    return kitchen_dir(home, species_id) / "tiles" / frame_token(frame_key)


def slabs_dir(home: Path, species_id: str, frame_key: str) -> Path:
    return kitchen_dir(home, species_id) / "slabs" / frame_token(frame_key)


def load_pool(home: Path, species_id: str) -> KitchenPool:
    """The pool, or a new empty one when there is none. A damaged file raises: it holds the members
    and the draft, which nothing else can rebuild."""
    path = kitchen_dir(home, species_id) / POOL_FILE
    try:
        text = path.read_text()
    except FileNotFoundError:
        return KitchenPool(species_id=species_id)
    return KitchenPool.model_validate_json(text)


def save_pool(home: Path, pool: KitchenPool) -> None:
    write_json_atomic(kitchen_dir(home, pool.species_id) / POOL_FILE, json.loads(pool.model_dump_json()))


def read_review(home: Path, species_id: str) -> dict[str, Any] | None:
    """The last review sample and its labels, or None. A damaged file is logged and read as none: the
    labels are an estimate's working notes, and a new sample can be drawn."""
    path = kitchen_dir(home, species_id) / REVIEW_FILE
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except ValueError:
        logger.warning("Unreadable review sample %s: a new one can be drawn", path)
        return None


def save_review(home: Path, species_id: str, review: dict[str, Any]) -> None:
    write_json_atomic(kitchen_dir(home, species_id) / REVIEW_FILE, review)


def append_events(home: Path, species_id: str, events: list[dict[str, Any]]) -> None:
    """Append verdict events, stamped with time and user. Blocking: call it in a thread."""
    if not events:
        return
    path = kitchen_dir(home, species_id) / VERDICTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = {"at": now_iso(), "user": current_user()}
    with open(path, "a") as f:
        for e in events:
            f.write(json.dumps({**stamp, **e}) + "\n")


def verdict_event(uid: str, verdict: Verdict, source: str = "hand") -> dict[str, Any]:
    return {"uid": uid, "op": "verdict", "value": verdict.value, "source": source}


def read_verdicts(home: Path, species_id: str) -> tuple[dict[str, str], int]:
    """({uid: keep | drop}, lines read). A damaged line is logged and skipped: every other line still
    states its event."""
    path = kitchen_dir(home, species_id) / VERDICTS_FILE
    verdicts: dict[str, str] = {}
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return verdicts, 0
    for line in lines:
        try:
            e = json.loads(line)
            uid = str(e["uid"])
        except (ValueError, KeyError, TypeError):
            logger.warning("Skipping a damaged line in %s", path)
            continue
        if e.get("op") == "verdict":
            if e.get("value") in (Verdict.KEEP, Verdict.DROP):
                verdicts[uid] = str(e["value"])
            else:
                verdicts.pop(uid, None)
    return verdicts, len(lines)


def compact_verdicts(home: Path, species_id: str) -> bool:
    """Rewrite the log as one event per live verdict once it has grown COMPACT_FACTOR times longer than
    that. True when it was rewritten. Blocking."""
    verdicts, n_lines = read_verdicts(home, species_id)
    live = len(verdicts)
    if n_lines < COMPACT_MIN_LINES or n_lines < COMPACT_FACTOR * max(live, 1):
        return False
    path = kitchen_dir(home, species_id) / VERDICTS_FILE
    tmp = path.with_name(f".{path.name}.compact")
    stamp = {"at": now_iso(), "user": current_user(), "source": "compaction"}
    with open(tmp, "w") as f:
        for uid, v in verdicts.items():
            f.write(json.dumps({**stamp, "uid": uid, "op": "verdict", "value": v}) + "\n")
    tmp.replace(path)
    logger.info("Compacted %s: %d lines → %d", path, n_lines, live)
    return True
