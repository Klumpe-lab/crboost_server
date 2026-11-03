"""A pick's uid as a star column, and how every star that should carry it is read.

Every particle set the STA kitchen makes writes its picks' pool uids as ``cbPickUid``, and a half from a hash of
the uid as ``rlnRandomSubset``, so a particle keeps its half in every set it is ever in (relion_refine keeps the
halves it is given when every row has one, and refuses a star where only some rows do). RELION's extraction
copies whole rows and Warp's export copies unknown columns row for row, so the jobs after a set name their picks
too.

A cell holds ``u`` and the 12 hex digits of the uid: with the letter first no reader takes it for a number
(a uid of digits only, or one like ``12e4…``, would come back as a float and lose its leading zeros).

Stars written by RELION or Warp are untrusted: the column is checked on every read (present, one per row,
unique), and a row that fails is reported and left out, never matched some other way. Whether its uids are
known is the reader's question.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

import pandas as pd

UID_COL = "cbPickUid"
HALF_COL = "rlnRandomSubset"
_CELL_RE = re.compile(r"^u([0-9a-f]{12})$")


def uid_cell(uid: str) -> str:
    return f"u{uid}"


def parse_cell(value) -> str | None:
    """The uid a cell holds, or None when it holds none (blank, NaN, or not the cell's form)."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    m = _CELL_RE.match(str(value).strip())
    return m.group(1) if m else None


def half_of(uid: str) -> int:
    """1 or 2, the same for a uid whatever set it is in."""
    return 1 + (hashlib.sha1(f"half|{uid}".encode()).digest()[0] & 1)


@dataclass(frozen=True)
class UidColumn:
    """One star's uid column as read: per row its uid, None where the row has none to trust (blank,
    malformed, or carried by another row too)."""

    uids: list[str | None]
    n_missing: int
    n_malformed: int
    n_duplicated: int  # rows whose uid another row carries as well

    @property
    def n_valid(self) -> int:
        return sum(u is not None for u in self.uids)

    def problems(self) -> list[str]:
        out = []
        if self.n_missing:
            out.append(f"{self.n_missing:,} rows have no {UID_COL}")
        if self.n_malformed:
            out.append(f"{self.n_malformed:,} rows hold a {UID_COL} that is not one")
        if self.n_duplicated:
            out.append(f"{self.n_duplicated:,} rows share a {UID_COL} with another row")
        return out


def read_uid_column(df: pd.DataFrame) -> UidColumn | None:
    """The uid column of a particle table, checked; None when the table has no such column."""
    if UID_COL not in df.columns:
        return None
    raw = df[UID_COL].tolist()
    uids: list[str | None] = []
    n_missing = n_malformed = 0
    for v in raw:
        u = parse_cell(v)
        if u is None:
            blank = v is None or (isinstance(v, float) and v != v) or not str(v).strip()
            n_missing += blank
            n_malformed += not blank
        uids.append(u)
    seen: dict[str, int] = {}
    for u in uids:
        if u is not None:
            seen[u] = seen.get(u, 0) + 1
    dup = {u for u, k in seen.items() if k > 1}
    if dup:
        uids = [None if u in dup else u for u in uids]
    return UidColumn(uids, n_missing, n_malformed, sum(seen[u] for u in dup))
