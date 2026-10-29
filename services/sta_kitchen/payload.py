"""The pool table as one binary response for the browser.

Layout: ``CBK1``, the header's length (uint32 LE), the header (UTF-8 JSON) padded to 8 bytes, then the
columns as little-endian arrays, each starting on an 8-byte boundary. The header lists every array with
its dtype, offset and length, the columns' labels, units, categories and coverage, and the frames.
"""

from __future__ import annotations

import json
import struct
from typing import Any

import numpy as np

from services.sta_kitchen.pool import PoolTable, column_header

MAGIC = b"CBK1"


def frame_header(table: PoolTable) -> list[dict[str, Any]]:
    keys = ("key", "tomo", "project", "dims", "apix", "apix_src", "acq", "members", "recon_exists")
    return [{k: fr.get(k) for k in keys} for fr in table.frames]


def encode(table: PoolTable, extra: dict[str, Any] | None = None) -> bytes:
    arrays: list[tuple[str, np.ndarray]] = [("uid", table.uid.astype("<f8"))]
    arrays += [(f"num:{k}", v.astype("<f4")) for k, v in table.num.items()]
    arrays += [(f"cat:{k}", v.astype("<i4")) for k, v in table.cat.items()]
    listing, offset = [], 0
    for name, a in arrays:
        listing.append({"name": name, "dtype": a.dtype.str, "offset": offset, "length": int(a.size)})
        offset += _pad8(a.nbytes)
    header = {
        "version": table.version,
        "species": table.species_id,
        "n": table.n,
        "columns": column_header(table),
        "frames": frame_header(table),
        "conflicts": table.conflicts,
        "source_conflicts": table.source_conflicts,
        "members": [
            {"key": k, "start": table.member_slices[k].start, "stop": table.member_slices[k].stop}
            for k in table.members
        ],
        "arrays": listing,
        **(extra or {}),
    }
    head = json.dumps(header, separators=(",", ":")).encode()
    head += b" " * (_pad8(len(head) + 8) - len(head) - 8)
    out = bytearray(MAGIC + struct.pack("<I", len(head)) + head)
    for _name, a in arrays:
        out += a.tobytes()
        out += b"\0" * (_pad8(a.nbytes) - a.nbytes)
    return bytes(out)


def _pad8(n: int) -> int:
    return (n + 7) // 8 * 8
