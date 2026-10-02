"""What each tilt is, for every surface that shows one.

A Tilts card, a cell of a tomogram's mosaic, a tick on a tilt series' strip and a tomogram's
caption read this one derivation, so a tilt reads the same wherever it appears:

- ``in_tomogram``: the tilt is in alignment's per-frame output, the set CTF and
  reconstruction use. None until alignment has recorded one for the series.
- ``drop``: the committed tilt-filter verdict's reason, when the verdict drops the tilt.
- ``review``: what the uncommitted review says, by the rule Approve commits with: a human's
  label, else in DL review the model's call at the job's threshold.
- ``p_bad``: the model's P(bad), in DL review only, as the filter panel shows it.
- ``exposure``: the tilt's mdoc mean counts over its series' median, and its class.

Dark exposures are marked, never dropped here; dropping stays with the review and Approve.
Pure: no I/O.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from services.jobs.tilt_filter import FilterMode, effective_label
from services.tilt_series.models import TiltSeries

# A tilt is dark when its mdoc mean counts fall under a fraction of its series' median. Raw
# counts, not normalised by exposure time: a longer high-tilt exposure raises them, which can
# hide a dim tilt but never invents one.
# Blank: the beam was blocked (a grid bar, the lamella edge). Blank exposures sit near 1e-5 of
# their series' median and the dimmest exposures that still carry an image near 0.1, so 1 %
# clears both by an order of magnitude or more.
BLANK_EXPOSURE_FRACTION = 0.01
# Dim: under a tenth of the median. Reviewers label nearly every such tilt bad.
DIM_EXPOSURE_FRACTION = 0.10

Exposure = Literal["blank", "dim", "normal"]
Review = Literal["human_bad", "human_good", "model_bad"]


@dataclass(frozen=True)
class TiltState:
    frame_id: str
    number: int  # tilt_index + 1: SerialEM's file counter, i.e. the acquisition (dose) order
    angle: float  # nominal stage tilt, degrees
    in_tomogram: bool | None
    drop: str | None
    review: Review | None
    p_bad: float | None
    exposure_ratio: float | None
    exposure: Exposure | None

    @property
    def is_dark(self) -> bool:
        return self.exposure in ("blank", "dim")

    @property
    def flagged(self) -> bool:
        """The uncommitted review marks the tilt bad, so Approve would drop it."""
        return self.review in ("human_bad", "model_bad")

    @property
    def kept(self) -> bool:
        """Nothing has taken the tilt out: no verdict drops it and alignment did not leave it
        out. It is in the tomogram, or will be."""
        return self.drop is None and self.in_tomogram is not False


@dataclass(frozen=True)
class SeriesSummary:
    total: int
    used: int | None  # tilts in alignment's output; None until it has recorded one
    kept_range: tuple[float, float] | None  # stage tilts of the used tilts, min and max
    dark_in_use: tuple[TiltState, ...]  # dark exposures in the tomogram
    dark_unflagged: tuple[TiltState, ...]  # dark exposures nothing drops and the review does not flag
    flagged: tuple[TiltState, ...]  # the uncommitted review marks bad
    dropped: tuple[TiltState, ...]  # the committed verdict drops
    left_out: tuple[TiltState, ...]  # not in alignment's output, and no verdict drops them


def exposure_ratios(ts: TiltSeries) -> dict[str, float]:
    """Each frame's mdoc mean counts over its series' median, by frame id. Empty when the series
    records no counts or their median is 0; a frame without counts has no entry."""
    means = {f.id: f.mean_intensity for f in ts.frames if f.mean_intensity is not None}
    if not means:
        return {}
    median = statistics.median(means.values())
    if median <= 0:
        return {}
    return {fid: m / median for fid, m in means.items()}


def exposure_class(ratio: float | None) -> Exposure | None:
    if ratio is None:
        return None
    if ratio < BLANK_EXPOSURE_FRACTION:
        return "blank"
    if ratio < DIM_EXPOSURE_FRACTION:
        return "dim"
    return "normal"


def tomogram_frame_ids(ts: TiltSeries, alignment_instance: str | None) -> set[str] | None:
    """The frames alignment's output holds for this series, or None when it holds no per-frame
    list (alignment has not run for it, or ran before the registry recorded one)."""
    out = ts.outputs.get(alignment_instance) if alignment_instance else None
    if out is None or out.output_type != "ts_alignment" or not out.per_frame:
        return None
    return {e.frame_id for e in out.per_frame}


def tilt_states(
    ts: TiltSeries,
    *,
    alignment_instance: str | None,
    committed: bool,
    labels: Mapping[str, str],
    threshold: float | None,
    mode: FilterMode | None,
) -> list[TiltState]:
    """Every frame's state, in the registry's (acquisition) order. `mode` is None when the
    project has no tilt filter; `committed`, `labels` and `threshold` are its job's."""
    in_tomo = tomogram_frame_ids(ts, alignment_instance)
    ratios = exposure_ratios(ts)
    dl = mode == FilterMode.DL_REVIEW
    states = []
    for f in ts.frames:
        p_bad = f.p_bad if dl else None
        review: Review | None = None
        if mode is not None and not committed:
            human = labels.get(f.id)
            if human:
                # Approve drops every tilt whose label is not "good".
                review = "human_good" if human == "good" else "human_bad"
            elif threshold is not None and effective_label(f.id, p_bad, {}, threshold, mode) == "bad":
                review = "model_bad"
        ratio = ratios.get(f.id)
        states.append(
            TiltState(
                frame_id=f.id,
                number=f.tilt_index + 1,
                angle=f.nominal_tilt_angle_deg,
                in_tomogram=None if in_tomo is None else f.id in in_tomo,
                drop=(f.filter_reason or "tilt-filter") if f.is_filtered_out else None,
                review=review,
                p_bad=p_bad,
                exposure_ratio=ratio,
                exposure=exposure_class(ratio),
            )
        )
    return states


def series_summary(states: Sequence[TiltState]) -> SeriesSummary:
    known = [s for s in states if s.in_tomogram is not None]
    used = [s for s in states if s.in_tomogram]
    return SeriesSummary(
        total=len(states),
        used=len(used) if known else None,
        kept_range=(min(s.angle for s in used), max(s.angle for s in used)) if used else None,
        dark_in_use=tuple(s for s in used if s.is_dark),
        dark_unflagged=tuple(s for s in states if s.is_dark and s.kept and not s.flagged),
        flagged=tuple(s for s in states if s.flagged),
        dropped=tuple(s for s in states if s.drop is not None),
        left_out=tuple(s for s in states if s.in_tomogram is False and s.drop is None),
    )
