"""The card's measurements and the map's layout: every size the cables depend on, known before anything is
drawn, so a cable lands on its port whatever the card holds."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import pairwise
from math import ceil

CARD_W, LARGE_W = 236, 480
HEAD_H, SLOT_H, OUT_H = 26, 17, 22
SIDE = 12  # the card's horizontal padding
TEXT_X = 28  # the text column: the status light sits left of it, every label and value starts here
STRIP_GAP = 7  # between the magazine and the card below it
PORT = 10  # the port: a small square straddling the card's edge, where a cable lands or leaves
LABEL_W = 70  # the input row's label column
GUTTER = 44  # between columns
GAP = 14  # between siblings
PAD = 20


@dataclass(frozen=True)
class StripSpec:
    """The tilt-series marks of one magazine: mark size, gaps, how many per row, how many rows."""

    bw: float
    bh: float
    gap: float
    per_row: int
    rows: int
    row_gap: float

    @property
    def height(self) -> float:
        return self.rows * self.bh + (self.rows - 1) * self.row_gap

    def width(self, total: int) -> float:
        return min(total, self.per_row) * (self.bw + self.gap) - self.gap


def strip_spec(total: int, inner_w: int) -> StripSpec:
    """Squares sized to the project: one row while they stay 6 px or wider, two rows while that keeps them 6 px,
    then 2 px slivers (1 px past 300) in as many rows as it takes."""
    total = max(1, total)
    gap = 2
    for rows in (1, 2):
        per = ceil(total / rows)
        side = min(10, (inner_w + gap) // per - gap)
        if side >= 6:
            return StripSpec(side, side, gap, per, rows, 2)
    bw = 1 if total > 300 else 2
    per_row = max(1, (inner_w + 1) // (bw + 1))
    return StripSpec(bw, bw, 1, per_row, ceil(total / per_row), 1)


def card_height(n_rows: int, extra: float = 0) -> float:
    return HEAD_H + n_rows * SLOT_H + extra + OUT_H


def port_y(i: int) -> float:
    """The vertical centre of input port `i` on a card's left edge."""
    return HEAD_H + i * SLOT_H + SLOT_H / 2


def out_y_of(height: float) -> float:
    """The vertical centre of the output row, and of the output port, of a card this tall."""
    return height - OUT_H / 2


@dataclass(frozen=True)
class Box:
    """A card's measurements: width, height, the height of its middle block (parameters, results, the tilt
    series), the band above it where its magazine hovers."""

    w: int
    h: float
    extra: float = 0
    band: float = 0

    @property
    def inner_w(self) -> int:
        return self.w - 2 * SIDE


def path_d(x0: float, y0: float, x1: float, y1: float) -> str:
    """A cable's path, an S-curve from port to port; mirrored by the browser's `pathD` for redraws while a card
    is dragged."""
    mx = (x0 + x1) / 2
    return f"M {x0} {y0} C {mx} {y0}, {mx} {y1}, {x1} {y1}"


def tidy(roots: list[str], kids_of, height_of) -> dict[str, float]:
    """Top of every node: leaves stacked in depth-first order, a parent centred on its children."""
    top: dict[str, float] = {}
    cursor = [0.0]

    def place(nid: str) -> None:
        kids = kids_of(nid)
        if not kids:
            top[nid] = cursor[0]
            cursor[0] += height_of(nid) + GAP
            return
        for k in kids:
            place(k)
        first, last = kids[0], kids[-1]
        centre = ((top[first] + height_of(first) / 2) + (top[last] + height_of(last) / 2)) / 2
        top[nid] = centre - height_of(nid) / 2

    for r in roots:
        place(r)
    return top


def de_overlap(top: dict[str, float], col: dict[str, int], outer: dict[str, float], ncols: int) -> None:
    """Boxes of one column must not overlap: push down in order of their tops."""
    for c in range(ncols):
        column = sorted((nid for nid in top if col[nid] == c), key=top.__getitem__)
        for a, b in pairwise(column):
            if top[b] < top[a] + outer[a] + GAP:
                top[b] = top[a] + outer[a] + GAP
