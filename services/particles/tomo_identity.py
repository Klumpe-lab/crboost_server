"""Tomogram identity across projects, and whether two tomograms' coordinates are
interchangeable (roadmap `picking_ui/12-S1`).

Two DIFFERENT questions need two DIFFERENT keys, and conflating them is the whole
difficulty of cross-project aggregation:

``distinguishing_key`` — *"are these two different things?"*
    The absolute ``rlnTomoReconstructedTomogram`` path. Two reconstructions are never
    the same file, so this can never wrongly equate. It also cannot RECOGNISE the same
    tilt series reprocessed in another project, because each project writes its own
    reconstruction under its own job dir.

``acquisition_key`` — *"are these the same acquisition?"*
    The mdoc's ``SubFramePath`` + ``DateTime`` of the first tilt, which the registry
    already stores as ``TiltSeries.frames[0].raw_filename`` / ``.acquisition_time``.
    Invariant across reprocessing, so it survives the trip between projects. Returns
    None when either half is absent -- a partial key is not a key, and guessing one
    would silently equate unrelated series.

A matching acquisition key is NECESSARY BUT NOT SUFFICIENT for merging coordinates.
``rlnCenteredCoordinate*Angst`` is measured in Angstrom from the tomogram CENTRE, which
makes it pleasantly binning-independent -- but two projects can reconstruct one tilt
series with a different alignment, handedness or Z-height, and then the centre is a
different physical point. Identical numbers would name different places.
``check_transferable`` is that second gate.

The report separates BLOCKING from UNVERIFIED on purpose. A stated disagreement is
fatal. A fact neither side states (handedness, typically -- see ``read_tomo_hand``) is
NOT silently assumed equal and NOT silently assumed unequal: it rides back as an
``unverified`` line for the caller to put in front of the user, per CLAUDE.md's
"surfacing uncertainty" policy.

Compile-checked in Claude's bare venv; runtime-exercised by the aggregation flow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import starfile

if TYPE_CHECKING:
    from services.tilt_series.models import TiltSeries

logger = logging.getLogger(__name__)


# Columns of a tomograms.star row that must agree before two tomograms' centred-Angstrom
# coordinates may be pooled. Pixel size and binning are here not because centred-Angstrom
# depends on them -- it does not -- but because a disagreement means the two volumes were
# reconstructed differently, which is exactly when the centre stops being the same place.
TRANSFERABLE_COLS = (
    "rlnTomoTiltSeriesPixelSize",
    "rlnTomoTomogramBinning",
    "rlnTomoSizeX",
    "rlnTomoSizeY",
    "rlnTomoSizeZ",
)


@dataclass
class TransferReport:
    """Outcome of the transferability gate for ONE pair of tomograms.

    ``blocking`` — stated facts that disagree. Merging is wrong; the caller raises.
    ``unverified`` — facts one or both sides do not state. Merging may be right; the
    caller must SHOW these rather than resolve them.
    """

    tomo_a: str
    tomo_b: str
    blocking: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.blocking

    def message(self) -> str:
        lines = [f"{self.tomo_a} ↔ {self.tomo_b}"]
        lines += [f"  BLOCKING: {b}" for b in self.blocking]
        lines += [f"  unverified: {u}" for u in self.unverified]
        return "\n".join(lines)


def distinguishing_key(tomo_row: pd.Series | dict[str, Any]) -> str:
    """Absolute reconstruction path — identity of one RECONSTRUCTION.

    Never equates two different volumes. Also never recognises the same tilt series
    reprocessed elsewhere; use `acquisition_key` for that.
    """
    raw = tomo_row.get("rlnTomoReconstructedTomogram")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        raise KeyError(f"tomograms.star row has no rlnTomoReconstructedTomogram: {dict(tomo_row)!r}")
    return str(Path(str(raw)).resolve())


def acquisition_key(ts: TiltSeries) -> tuple[str, str] | None:
    """``(first frame's raw filename, its mdoc DateTime)`` — identity of the ACQUISITION,
    stable across projects and across reprocessing.

    None when either half is missing (legacy registries and non-SerialEM imports may lack
    `acquisition_time`). A partial key is not a key: returning half of one would let two
    unrelated series match on a filename alone.
    """
    frames = sorted(getattr(ts, "frames", []) or [], key=lambda f: f.tilt_index)
    if not frames:
        return None
    first = frames[0]
    name = getattr(first, "raw_filename", "") or ""
    when = getattr(first, "acquisition_time", None)
    if not name or when is None:
        return None
    return (str(name), when.isoformat())


def read_tomo_hand(tilt_series_star: str | Path) -> int | None:
    """``rlnTomoHand`` (+1 / -1) from a per-TS tilt star, or None when it is not stated.

    Handedness is the one transferability fact that does NOT live in tomograms.star -- it
    is written into the Import job's tilt_series.star and carried per tilt series. A
    project whose star predates that column has no handedness on record, and this returns
    None so the caller can say so rather than assume agreement. A handedness flip mirrors
    Z: every imported pick lands on the wrong side of the section, silently. That is the
    412 chirality cascade.
    """
    path = Path(tilt_series_star)
    if not path.exists():
        return None
    try:
        data = starfile.read(path, always_dict=True)
    except Exception:
        # Expected-and-ignorable: an unreadable or non-star tilt file is "handedness not
        # stated", which the report already models. Logged, not swallowed.
        logger.warning("Could not read handedness from %s", path, exc_info=True)
        return None
    for block in data.values():
        if isinstance(block, pd.DataFrame) and "rlnTomoHand" in block.columns:
            vals = pd.to_numeric(block["rlnTomoHand"], errors="coerce").dropna().unique().tolist()
            if len(vals) == 1:
                return int(vals[0])
            if len(vals) > 1:
                logger.warning("Mixed rlnTomoHand %s in %s — treating as not stated", vals, path)
            return None
    return None


def check_transferable(
    row_a: pd.Series | dict[str, Any],
    row_b: pd.Series | dict[str, Any],
    *,
    hand_a: int | None = None,
    hand_b: int | None = None,
) -> TransferReport:
    """Second gate: may these two tomograms' centred-Angstrom coordinates be pooled?

    Call it only for pairs whose `acquisition_key` already matched — this answers
    "same reconstruction geometry", not "same tilt series". Pass handedness from
    `read_tomo_hand` on each side; None means "not stated" and rides back as unverified.
    """
    name_a = str(row_a.get("rlnTomoName", "?"))
    name_b = str(row_b.get("rlnTomoName", "?"))
    report = TransferReport(tomo_a=name_a, tomo_b=name_b)

    for col in TRANSFERABLE_COLS:
        va, vb = row_a.get(col), row_b.get(col)
        missing = [c for c, v in ((name_a, va), (name_b, vb)) if v is None or pd.isna(v)]
        if missing:
            report.unverified.append(f"{col} not stated by {', '.join(missing)}")
            continue
        if str(va) != str(vb):
            report.blocking.append(f"{col} differs: {name_a}={va} vs {name_b}={vb}")

    if hand_a is None or hand_b is None:
        which = [n for n, h in ((name_a, hand_a), (name_b, hand_b)) if h is None]
        report.unverified.append(
            f"handedness (rlnTomoHand) not stated by {', '.join(which)} — "
            "a flip mirrors Z and puts every pick on the wrong side of the section"
        )
    elif hand_a != hand_b:
        report.blocking.append(f"handedness differs: {name_a}={hand_a:+d} vs {name_b}={hand_b:+d} — Z is mirrored")

    return report


def assert_transferable(
    row_a: pd.Series | dict[str, Any],
    row_b: pd.Series | dict[str, Any],
    *,
    hand_a: int | None = None,
    hand_b: int | None = None,
) -> TransferReport:
    """`check_transferable`, raising on anything blocking. Returns the report so the
    caller can still surface `unverified` lines on the success path."""
    report = check_transferable(row_a, row_b, hand_a=hand_a, hand_b=hand_b)
    if not report.ok:
        raise ValueError(
            "These tomograms are the same acquisition but were reconstructed differently, "
            "so their coordinates are not interchangeable:\n" + report.message()
        )
    return report
