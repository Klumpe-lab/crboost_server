"""The tilts a tilt-filter verdict keeps although it calls them bad, so each series stays alignable.

AreTomo2 (alignment, in the Warp dev41 image) crashes on a tilt series with a hole in it. On the
AGG412 reruns (783 series, 2026-11-16), by the largest gap between the tilts the filter kept, it
crashed on 8 of 559 series at ≤ 3.5° (5 of them series the filter had emptied), 7 of 127 at
3.5–9.5°, 15 of 33 at 9.5–15.5° and 32 of 33 beyond. The holes were mostly tilts the model was
unsure of, and a crashed series is lost whole. So a commit keeps a bad-called tilt where excluding
it would cost the series:

- a gap wider than ``max_gap_deg`` between kept tilts is closed with the tilts inside it;
- a series the verdict would leave with fewer than ``min_tilts`` keeps every tilt it can;
- when no kept tilt lies within ``max_gap_deg`` of 0°, the tilt nearest 0° is kept, and the gap
  out to the kept tilts closed as above.

Never put back: a tilt a manual label calls bad (a person decided) and a blank exposure (the beam
was blocked; it holds no image). Where those leave a gap wider than ``max_gap_deg``, the series is
cut there instead: the kept tilts beyond the gap, away from 0°, are excluded too.

Bad tilts past the outermost kept tilt at either end stay excluded: they leave no gap. Pure: no I/O.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from itertools import pairwise

from services.tilt_series.exposure import exposure_class, exposure_ratios
from services.tilt_series.models import TiltSeries


@dataclass(frozen=True)
class KeepRule:
    max_gap_deg: float
    min_tilts: int


@dataclass
class Adjustment:
    """What the rule changes in a verdict, by frame id: `keep` the bad-called tilts it keeps,
    `cut` the good-called tilts it excludes, each with the reason the views show."""

    keep: dict[str, str] = field(default_factory=dict)
    cut: dict[str, str] = field(default_factory=dict)


def adjust(
    tilt_series: Iterable[TiltSeries], labels: Mapping[str, str], human: Mapping[str, str], rule: KeepRule
) -> Adjustment:
    """The rule over every series. `labels` is each frame's verdict label ("good" keeps it,
    anything else excludes it), `human` the manual labels behind them."""
    out = Adjustment()
    for ts in tilt_series:
        one = adjust_series(ts, labels, human, rule)
        out.keep.update(one.keep)
        out.cut.update(one.cut)
    return out


def adjust_series(ts: TiltSeries, labels: Mapping[str, str], human: Mapping[str, str], rule: KeepRule) -> Adjustment:
    frames = sorted(ts.frames, key=lambda f: f.nominal_tilt_angle_deg)
    out = Adjustment()
    if not frames:
        return out
    angle = [f.nominal_tilt_angle_deg for f in frames]
    ratios = exposure_ratios(ts)
    blank = [exposure_class(ratios.get(f.id)) == "blank" for f in frames]
    bad = [labels.get(f.id, "good") != "good" for f in frames]
    # A bad call the rule may overturn: the model's, on an exposure that holds an image.
    restorable = [b and human.get(f.id) is None and not bl for f, b, bl in zip(frames, bad, blank, strict=True)]
    kept = {i for i, b in enumerate(bad) if not b}

    n_good = len(kept)
    if n_good < rule.min_tilts:
        why = f"the filter would leave {n_good} of {len(frames)} tilts, under the floor of {rule.min_tilts}"
        for i in range(len(frames)):
            if restorable[i]:
                kept.add(i)
                out.keep[frames[i].id] = why

    nearest = min(range(len(frames)), key=lambda i: abs(angle[i]))
    if not any(abs(angle[i]) <= rule.max_gap_deg for i in kept) and restorable[nearest]:
        kept.add(nearest)
        out.keep[frames[nearest].id] = "it is the tilt nearest 0°, and no kept tilt was near it"
    if not kept:
        return out

    anchor = min(kept, key=lambda i: abs(angle[i]))
    for step in (1, -1):
        _walk(frames, angle, restorable, kept, anchor, step, rule.max_gap_deg, out)
    return out


def _walk(frames, angle, restorable, kept: set[int], anchor: int, step: int, max_gap: float, out: Adjustment) -> None:
    """From the anchor outward in one direction: close each gap wider than `max_gap` between
    kept tilts with the restorable tilts inside it, or cut the series at the first gap that
    cannot be closed."""
    last = anchor
    i = anchor + step
    while 0 <= i < len(frames):
        if i not in kept:
            i += step
            continue
        if abs(angle[i] - angle[last]) > max_gap:
            between = list(range(last + step, i, step))
            fill = [j for j in between if restorable[j]]
            points = [last, *fill, i]
            gap = abs(angle[i] - angle[last])
            if max(abs(angle[b] - angle[a]) for a, b in pairwise(points)) <= max_gap:
                for j in fill:
                    kept.add(j)
                    out.keep[frames[j].id] = f"excluding it would leave a {gap:.0f}° gap in the series"
            else:
                # Blank or manually excluded tilts hold the gap open: the series ends at `last`. A partial
                # fill would only leave bad-called tilts at the new edge, so none is kept, and the kept
                # tilts beyond the gap are cut.
                cut_why = f"cut past a {gap:.0f}° gap that blank or manually excluded tilts hold open"
                for j in [k for k in kept if (k - last) * step > 0]:
                    kept.discard(j)
                    if out.keep.pop(frames[j].id, None) is None:
                        out.cut[frames[j].id] = cut_why
                return
        last = i
        i += step
