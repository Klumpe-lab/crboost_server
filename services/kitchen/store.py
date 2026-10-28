"""Where a kitchen pool keeps its files, inside the home project only::

    Analytics/kitchen/<species>/pool.json          the KitchenPool (atomic writes)
    Analytics/kitchen/<species>/verdicts.jsonl     hand verdicts and tag events, append-only
    Analytics/kitchen/<species>/table/             one column table per member, by fingerprint
    Analytics/kitchen/<species>/tiles/<frame>/     cutouts rendered on demand
    Analytics/kitchen/<species>/slabs/<frame>/     slabs of other projects' volumes that have none

Only the server writes here. Verdict events are ``{at, user, uid, op, value, source}``: ``op`` "verdict"
(value keep / drop / clear) or "tag" (``tag`` the bit, value true / false); the latest event per uid
and key wins.
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


def append_events(home: Path, species_id: str, events: list[dict[str, Any]]) -> None:
    """Append verdict and tag events, stamped with time and user. Blocking: call it in a thread."""
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


def tag_event(uid: str, tag: int, on: bool) -> dict[str, Any]:
    return {"uid": uid, "op": "tag", "tag": int(tag), "value": bool(on)}


def read_verdicts(home: Path, species_id: str) -> tuple[dict[str, str], dict[str, int], int]:
    """({uid: keep | drop}, {uid: tag bits}, lines read). A damaged line is logged and skipped: every
    other line still states its event."""
    path = kitchen_dir(home, species_id) / VERDICTS_FILE
    verdicts: dict[str, str] = {}
    tags: dict[str, int] = {}
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return verdicts, tags, 0
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
        elif e.get("op") == "tag":
            bit = 1 << int(e.get("tag", 0))
            bits = (tags.get(uid, 0) | bit) if e.get("value") else (tags.get(uid, 0) & ~bit)
            if bits:
                tags[uid] = bits
            else:
                tags.pop(uid, None)
    return verdicts, tags, len(lines)


def compact_verdicts(home: Path, species_id: str) -> bool:
    """Rewrite the log as one event per live verdict and tag once it has grown COMPACT_FACTOR times
    longer than that. True when it was rewritten. Blocking."""
    verdicts, tags, n_lines = read_verdicts(home, species_id)
    live = len(verdicts) + sum(bin(b).count("1") for b in tags.values())
    if n_lines < COMPACT_MIN_LINES or n_lines < COMPACT_FACTOR * max(live, 1):
        return False
    path = kitchen_dir(home, species_id) / VERDICTS_FILE
    tmp = path.with_name(f".{path.name}.compact")
    stamp = {"at": now_iso(), "user": current_user(), "source": "compaction"}
    with open(tmp, "w") as f:
        for uid, v in verdicts.items():
            f.write(json.dumps({**stamp, "uid": uid, "op": "verdict", "value": v}) + "\n")
        for uid, bits in tags.items():
            for bit in range(32):
                if bits & (1 << bit):
                    f.write(json.dumps({**stamp, "uid": uid, "op": "tag", "tag": bit, "value": True}) + "\n")
    tmp.replace(path)
    logger.info("Compacted %s: %d lines → %d", path, n_lines, live)
    return True
