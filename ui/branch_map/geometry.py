"""The card's measurements. The browser lays the cards out (a grid, ui/branch_map/view.py) and draws the cables
after layout, so the server knows a card's widths only: what its pictures, plots and magazine are sized to."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil

CARD_W = 236  # a small card: one column of the grid
GRID_GAP = 18  # between the grid's columns
LARGE_W = 2 * CARD_W + GRID_GAP  # a large card spans two columns
HEAD_H, SLOT_H = 26, 17
SIDE = 12  # the card's horizontal padding
TEXT_X = 28  # the text column: the status light sits left of it, every label and value starts here
LABEL_W = 70  # the input row's label column


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
