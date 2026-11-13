"""The large card's plots as small SVGs in the Journey's chart theme (`JOURNEY_THEME`, ui/dashboard/figures.py):
one value per tilt series over a baseline, bars, a histogram, a curve, and the per-tilt plot of one tilt series
over the stage angle with dropped tilts hollow and a dashed threshold."""

from __future__ import annotations

from ui.branch_map.model import Plot, TiltPlot
from ui.branch_map.style import TH

AXIS_W = 30  # the per-tilt plot's y labels


def svg(w: float, h: float, parts: list[str], *, extra: str = "") -> str:
    body = "".join(parts)
    head = f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display:block;overflow:visible"{extra}>'
    return f"{head}{body}</svg>"


def _dot_mark(x: float, y: float, dropped: bool) -> str:
    """One point the Journey's way: a disc in the first series' colour; a dropped or flagged one a red ring."""
    if dropped:
        return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.3" fill="#fff" stroke="{TH.drop_color}" stroke-width="1.2"/>'
    return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.2" fill="{TH.series[0]}"/>'


def _ref_line(x0: float, y0: float, x1: float, y1: float) -> str:
    """A reference or threshold line, the Journey's dashed grey."""
    return (
        f'<line x1="{x0:.1f}" y1="{y0:.1f}" x2="{x1:.1f}" y2="{y1:.1f}" stroke="{TH.name_color}" stroke-width="1" '
        'stroke-dasharray="3 2"/>'
    )


def svg_dots(
    vals: list[float], w: int, h: int, *, lo: float, hi: float, flag: set[int] = frozenset(), cut: float | None = None
) -> str:
    """One dot per tilt series over a baseline: a defocus, a resolution; `cut` draws a threshold line."""
    n = len(vals)
    parts = [f'<line x1="0" y1="{h - 0.5}" x2="{w}" y2="{h - 0.5}" stroke="{TH.x_line}"/>']

    def yof(v: float) -> float:
        return h - 3 - (v - lo) / (hi - lo or 1) * (h - 8)

    if cut is not None:
        parts.append(_ref_line(0, yof(cut), w, yof(cut)))
    for i, v in enumerate(vals):
        x = 3 + i * (w - 6) / max(1, n - 1)
        parts.append(_dot_mark(x, yof(v), i in flag))
    return svg(w, h, parts)


def svg_bars(vals: list[float], w: int, h: int, *, flag: set[int] = frozenset(), color: str = TH.series[0]) -> str:
    """One bar per tilt series, the flagged ones in the Journey's dropped-tilt red."""
    n = max(1, len(vals))
    bw = (w - (n - 1)) / n
    hi = max(vals, default=1) or 1
    parts = [f'<line x1="0" y1="{h - 0.5}" x2="{w}" y2="{h - 0.5}" stroke="{TH.x_line}"/>']
    for i, v in enumerate(vals):
        bh = max(1.0, v / hi * (h - 2))
        parts.append(
            f'<rect x="{i * (bw + 1):.1f}" y="{h - bh:.1f}" width="{bw:.1f}" height="{bh:.1f}" '
            f'fill="{TH.drop_color if i in flag else color}"/>'
        )
    return svg(w, h, parts)


def svg_hist(counts: list[float], w: int, h: int, *, cut_at: float | None = None) -> str:
    """A histogram in the Journey's score-histogram blue with an optional dashed cut-off line, `cut_at` in bins."""
    n = max(1, len(counts))
    bw = (w - (n - 1)) / n
    hi = max(counts, default=1) or 1
    parts = [f'<line x1="0" y1="{h - 0.5}" x2="{w}" y2="{h - 0.5}" stroke="{TH.x_line}"/>']
    for i, c in enumerate(counts):
        bh = c / hi * (h - 2)
        parts.append(
            f'<rect x="{i * (bw + 1):.1f}" y="{h - bh:.1f}" width="{bw:.1f}" height="{bh:.1f}" fill="{TH.hist_color}"/>'
        )
    if cut_at is not None:
        x = cut_at * (bw + 1) - 0.5
        parts.append(_ref_line(x, 0, x, h))
    return svg(w, h, parts)


def svg_curve(vals: list[float], w: int, h: int) -> str:
    if len(vals) < 2:
        return svg(w, h, [])
    lo, hi = min(vals), max(vals)
    pts = " ".join(
        f"{i * w / (len(vals) - 1):.1f},{h - 2 - (v - lo) / (hi - lo or 1) * (h - 4):.1f}" for i, v in enumerate(vals)
    )
    return svg(w, h, [f'<polyline points="{pts}" fill="none" stroke="{TH.series[0]}" stroke-width="1.5"/>'])


def svg_stack(kept: int, out: int, w: int, h: int) -> str:
    """Two parts of one bar: tilts kept and tilts taken out."""
    kw = w * kept / (kept + out or 1)
    return svg(
        w,
        h,
        [
            f'<rect x="0" y="0" width="{kw:.1f}" height="{h}" rx="2" fill="{TH.series[0]}"/>',
            f'<rect x="{kw:.1f}" y="0" width="{w - kw:.1f}" height="{h}" rx="2" fill="{TH.drop_color}"/>',
        ],
    )


def svg_plot(p: Plot, w: int, h: int) -> str:
    if p.kind == "dots":
        lo = p.lo if p.lo is not None else min(p.values)
        hi = p.hi if p.hi is not None else max(p.values)
        return svg_dots(p.values, w, h, lo=lo, hi=hi, flag=p.flags, cut=p.cut)
    if p.kind == "bars":
        return svg_bars(p.values, w, h, flag=p.flags)
    if p.kind == "curve":
        return svg_curve(p.values, w, h)
    if p.kind == "hist":
        return svg_hist(p.values, w, h, cut_at=p.cut)
    return svg_stack(p.kept, p.out, w, h)


def svg_tilt(tp: TiltPlot, w: int, h: int) -> str:
    """One per-tilt plot the Journey's way: a value per tilt over the stage angle, discs in the first series'
    colour (bars in the histogram blue), dropped tilts as red rings, a dashed threshold, the y grid, the extremes
    of both axes as tick labels in the tick face."""
    if not tp.values:
        return svg(w, h, [])
    vals = tp.values
    lo = tp.lo if tp.lo is not None else min(vals)
    hi = tp.hi if tp.hi is not None else max(vals)
    if tp.cut is not None:
        hi = max(hi, tp.cut * 1.1)
        lo = min(lo, tp.cut)
    if hi <= lo:
        hi = lo + 1
    x0, x1 = AXIS_W, w - 2
    y0, y1 = 3, h - 11
    t_lo, t_hi = min(tp.tilts), max(tp.tilts)

    def xof(t: float) -> float:
        return x0 + (t - t_lo) / ((t_hi - t_lo) or 1) * (x1 - x0)

    def yof(v: float) -> float:
        return y1 - (min(max(v, lo), hi) - lo) / (hi - lo) * (y1 - y0)

    lab = f"font-family: {TH.tick_font}; font-size: 7.5px; fill: {TH.tick_color};"
    ym = (y0 + y1) / 2 + 0.5
    parts = [
        f'<line x1="{x0}" y1="{y0 + 0.5}" x2="{x1}" y2="{y0 + 0.5}" stroke="{TH.y_grid}"/>',
        f'<line x1="{x0}" y1="{ym:.1f}" x2="{x1}" y2="{ym:.1f}" stroke="{TH.y_grid}"/>',
        f'<line x1="{x0}" y1="{y1 + 0.5}" x2="{x1}" y2="{y1 + 0.5}" stroke="{TH.x_line}"/>',
        f'<text x="{x0 - 3}" y="{y0 + 5}" text-anchor="end" style="{lab}">{hi:g}{tp.unit}</text>',
        f'<text x="{x0 - 3}" y="{y1 - 1}" text-anchor="end" style="{lab}">{lo:g}</text>',
        f'<text x="{x0}" y="{h - 2}" style="{lab}">{t_lo:+.0f}°</text>',
        f'<text x="{x1}" y="{h - 2}" text-anchor="end" style="{lab}">{t_hi:+.0f}°</text>',
    ]
    if tp.cut is not None:
        parts.append(_ref_line(x0, yof(tp.cut), x1, yof(tp.cut)))
    n = len(vals)
    if tp.kind == "bars":
        bw = max(1.0, (x1 - x0) / n - 1)
        for t, v in zip(tp.tilts, vals, strict=True):
            x = xof(t) - bw / 2
            over = tp.over_is_bad and tp.cut is not None and v > tp.cut
            parts.append(
                f'<rect x="{x:.1f}" y="{yof(v):.1f}" width="{bw:.1f}" height="{y1 - yof(v):.1f}" '
                f'fill="{TH.drop_color if over else TH.hist_color}"/>'
            )
    else:
        for i, (t, v) in enumerate(zip(tp.tilts, vals, strict=True)):
            parts.append(_dot_mark(xof(t), yof(v), i in tp.dropped))
    return svg(w, h, parts)
