"""How bright a tilt's exposure was against the rest of its series, from the mdoc's counts.

Its own module so both the tilt views (tilt_state) and the tilt filter's commit (keep_rule) read
one definition of a blank exposure. Pure: no I/O.
"""

from __future__ import annotations

import statistics
from typing import Literal

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
