"""The column labels a star file declares, read from its header without parsing its rows.

Cheap enough for a menu or a disk tick: the header of a particles star is a few kilobytes, and the reading
stops at the first row of the particle table. Kept per file and mtime.

Blocking (a stat and a short read): call it in a thread where it may meet a cold file on NFS.
"""

from __future__ import annotations

import os
from pathlib import Path

# A header longer than this is not looked through further: its labels past it are not seen.
_HEADER_LIMIT = 2 << 20
_LABELS: dict[str, tuple[int, frozenset[str]]] = {}


def star_labels(path: str | Path) -> frozenset[str]:
    """Every label the star declares (loop columns and key-value lines, every block up to the particle
    table's first row); empty when the file is missing."""
    p = str(path)
    try:
        mtime = os.stat(p).st_mtime_ns
    except FileNotFoundError:
        return frozenset()
    hit = _LABELS.get(p)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    labels: set[str] = set()
    block: list[str] = []
    read = 0
    with open(p, "rb") as f:
        for raw in f:
            read += len(raw)
            if read > _HEADER_LIMIT:
                break
            line = raw.strip()
            if not line or line.startswith(b"#"):
                continue
            if line.startswith((b"data_", b"loop_")):
                block = []
                continue
            if line.startswith(b"_"):
                name = line.split()[0][1:].decode(errors="replace")
                labels.add(name)
                block.append(name)
                continue
            if "rlnTomoName" in block:
                break  # the particle table's first row: nothing declared after it
    out = frozenset(labels)
    _LABELS[p] = (mtime, out)
    return out
