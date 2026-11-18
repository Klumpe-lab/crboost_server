"""A plot that reads, drawn as one SVG: a y grid at labelled ticks, both axes titled with their units, named reference
lines, vertical marks, and a readout of every series at the x under the pointer (CSS only, `.cb-plot-*` in
ui/dashboard/css.py). Numbers are set in the mono face, axis titles in the label voice, so a plot reads like the
tables beside it.

`fsc_plot` and `shares_plot` are the two the particle jobs' Results tabs draw: a Fourier shell correlation on a
1/Å axis named in Å, and the classes' shares at every iteration of a classification.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from nicegui import ui

from services.particles.fsc_readout import crossing


GRID = "var(--cb-plot-grid, #eef2f6)"
AXIS = "var(--cb-plot-axis, #c5d3e2)"
TICK = "var(--cb-ink-3, #5b7089)"
TITLE = "var(--cb-ink-4, #94a3b8)"
INK = "var(--cb-ink-1, #12324f)"
READ_BG = "var(--cb-plot-read, #12324f)"
READ_INK = "var(--cb-plot-read-ink, #f2f6fb)"
SURFACE = "var(--cb-surface, #ffffff)"
REF = "var(--cb-ink-4, #94a3b8)"
# Series in order: the slate ink, the accent, a muted ochre, a muted verdigris.
SERIES = (
    "var(--cb-plot-s1, #4a8fd4)",
    "var(--cb-plot-s2, #5b7089)",
    "var(--cb-plot-s3, #a07a2c)",
    "var(--cb-plot-s4, #3f7a6e)",
)
# The faces are classes (ui/dashboard/css.py): numbers in the mono face with even figures, titles in the label voice.
_NUM = 'class="cb-plot-num" style="'
_WORD = 'class="cb-plot-word" style="'


@dataclass
class Axis:
    lo: float
    hi: float
    ticks: list[tuple[float, str]]
    title: str
    fmt: Callable[[float], str] = lambda v: f"{v:g}"  # the readout's words for a value


@dataclass
class Series:
    xs: list[float]
    ys: list[float]
    color: str
    name: str
    width: float = 1.6
    dash: str = ""


@dataclass
class Plot:
    series: list[Series]
    x: Axis
    y: Axis
    refs: list[tuple[float, str]] = field(default_factory=list)  # horizontal reference lines (y, words)
    marks: list[tuple[float, str]] = field(default_factory=list)  # vertical marks (x, words)
    hover_xs: list[float] | None = None  # where the readout stops; the first series' xs by default
    stacked: bool = False  # areas stacked to the top instead of lines


ML, MR, MT, MB = 36, 10, 18, 28  # the margins: y labels, a little room, the y title, x labels and the x title


def plot_svg(p: Plot, w: int, h: int) -> str:
    """The plot as SVG, `w` × `h` in its own units (it scales with the box it is put in)."""
    iw, ih = w - ML - MR, h - MT - MB

    def X(v: float) -> float:
        return ML + (v - p.x.lo) / ((p.x.hi - p.x.lo) or 1) * iw

    def Y(v: float) -> float:
        return MT + ih - (min(max(v, p.y.lo), p.y.hi) - p.y.lo) / ((p.y.hi - p.y.lo) or 1) * ih

    out = []
    for v, words in p.y.ticks:
        out.append(f'<line x1="{ML}" x2="{ML + iw}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" style="stroke:{GRID}"/>')
        out.append(
            f'<text x="{ML - 5}" y="{Y(v) + 3:.1f}" text-anchor="end" {_NUM}fill:{TICK};font-size:9px">{words}</text>'
        )
    out.append(f'<line x1="{ML}" x2="{ML + iw}" y1="{MT + ih}" y2="{MT + ih}" style="stroke:{AXIS}"/>')
    for v, words in p.x.ticks:
        out.append(f'<line x1="{X(v):.1f}" x2="{X(v):.1f}" y1="{MT + ih}" y2="{MT + ih + 3}" style="stroke:{AXIS}"/>')
        out.append(
            f'<text x="{X(v):.1f}" y="{MT + ih + 13}" text-anchor="middle" {_NUM}fill:{TICK};font-size:9px">'
            f"{words}</text>"
        )
    out.append(
        f'<text x="{ML + iw}" y="{h - 3}" text-anchor="end" {_WORD}fill:{TITLE};font-size:8.5px">{p.x.title}</text>'
    )
    out.append(
        f'<text x="{ML}" y="{MT - 7}" text-anchor="start" {_WORD}fill:{TITLE};font-size:8.5px">{p.y.title}</text>'
    )
    for v, words in p.refs:
        y = Y(v)
        out.append(
            f'<line x1="{ML}" x2="{ML + iw}" y1="{y:.1f}" y2="{y:.1f}" '
            f'style="stroke:{REF};stroke-dasharray:3 3;stroke-width:1"/>'
            f'<text x="{ML + iw - 2}" y="{y - 3:.1f}" text-anchor="end" '
            f'{_NUM}fill:{TICK};font-size:8.5px">{words}</text>'
        )
    for v, words in p.marks:
        x = X(v)
        right = x < ML + iw * 0.7
        out.append(
            f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{MT}" y2="{MT + ih}" style="stroke:{INK};stroke-width:1"/>'
            f'<text x="{x + (4 if right else -4):.1f}" y="{MT + 9}" text-anchor="{"start" if right else "end"}" '
            f'{_NUM}fill:{INK};font-size:9px;font-weight:600">{words}</text>'
        )
    if p.stacked:
        base = [0.0] * len(p.series[0].xs) if p.series else []
        for s in p.series:
            top = [b + y for b, y in zip(base, s.ys, strict=True)]
            pts = [f"{X(x):.1f},{Y(t):.1f}" for x, t in zip(s.xs, top, strict=True)]
            pts += [f"{X(x):.1f},{Y(b):.1f}" for x, b in reversed(list(zip(s.xs, base, strict=True)))]
            out.append(
                f'<polygon points="{" ".join(pts)}" style="fill:{s.color};fill-opacity:.8;stroke:{SURFACE};'
                'stroke-width:.6"/>'
            )
            base = top
    else:
        for s in p.series:
            pts = " ".join(f"{X(x):.1f},{Y(y):.1f}" for x, y in zip(s.xs, s.ys, strict=True))
            dash = f";stroke-dasharray:{s.dash}" if s.dash else ""
            out.append(
                f'<polyline points="{pts}" style="fill:none;stroke:{s.color};stroke-width:{s.width}'
                f';stroke-linejoin:round{dash}"/>'
            )
    out.append(_readouts(p, X, Y, iw, ih))
    return (
        f'<svg viewBox="0 0 {w} {h}" width="100%" style="display:block;overflow:visible;max-width:{w}px">'
        f"{''.join(out)}</svg>"
    )


def _readouts(p: Plot, X, Y, iw: float, ih: float) -> str:
    """One band per x the readout stops at: under the pointer, a cursor line, a dot on every series and a box with
    the x and every series' value."""
    xs = p.hover_xs or (p.series[0].xs if p.series else [])
    if not xs:
        return ""
    parts = []
    for i, xv in enumerate(xs):
        left = ML if i == 0 else (X(xs[i - 1]) + X(xv)) / 2
        right = ML + iw if i == len(xs) - 1 else (X(xv) + X(xs[i + 1])) / 2
        x = X(xv)
        lines = [p.x.fmt(xv)]
        dots = []
        base = 0.0
        for s in p.series:
            j = min(range(len(s.xs)), key=lambda k: abs(s.xs[k] - xv))
            yv = s.ys[j]
            ydot = base + yv if p.stacked else yv
            base = ydot if p.stacked else base
            dots.append(f'<circle cx="{x:.1f}" cy="{Y(ydot):.1f}" r="2.6" style="fill:{s.color};stroke:{SURFACE}"/>')
            lines.append(f"{s.name}  {p.y.fmt(yv)}")
        bw = max(len(t) for t in lines) * 5.6 + 12
        bh = len(lines) * 12 + 6
        bx = x + 8 if x + 8 + bw < ML + iw + MR else x - 8 - bw
        txt = "".join(
            f'<text x="{bx + 6:.1f}" y="{MT + 14 + k * 12:.1f}" {_NUM}fill:{READ_INK};font-size:9px;'
            f'{"font-weight:600;" if k == 0 else ""}">{t}</text>'
            for k, t in enumerate(lines)
        )
        parts.append(
            f'<g class="cb-plot-hit"><rect x="{left:.1f}" y="{MT}" width="{max(1.0, right - left):.1f}" '
            f'height="{ih}" style="fill:transparent"/><g class="cb-plot-read" style="pointer-events:none">'
            f'<line class="cb-plot-cur" x1="{x:.1f}" x2="{x:.1f}" y1="{MT}" y2="{MT + ih}" style="stroke:{AXIS}"/>'
            f"{''.join(dots)}"
            f'<rect x="{bx:.1f}" y="{MT + 3}" width="{bw:.1f}" height="{bh}" rx="1" '
            f'style="fill:{READ_BG};opacity:.94"/>'
            f"{txt}</g></g>"
        )
    return "".join(parts)


def plot(p: Plot, w: int, h: int) -> ui.element:
    """The plot as an element; it shrinks with its box below `w` and keeps its proportions."""
    return ui.html(plot_svg(p, w, h), sanitize=False).style(f"line-height: 0; width: 100%; max-width: {w}px;")


# ── the two the Results tabs draw ────────────────────────────────────────────


def angstrom_ticks(f_max: float, inner_w: float) -> list[tuple[float, str]]:
    """Ticks on a 1/Å axis named in Å, at the resolutions people quote, no two closer than ~34 px."""
    out: list[tuple[float, str]] = []
    last = -1e9
    for a in (100, 60, 40, 30, 25, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3.5, 3, 2.5, 2):
        f = 1 / a
        if f > f_max * 1.0001:
            break
        x = f / f_max * inner_w
        if x - last >= 34:
            out.append((f, f"{a:g}"))
            last = x
    return out


def fsc_plot(curves: dict[str, dict], w: int = 560, h: int = 190, *, mark: str | None = None) -> Plot:
    """Fourier shell correlation curves ({name: {"freq": 1/Å, "fsc": …}}) on a 1/Å axis named in Å, with the 0.143
    and 0.5 lines. `mark`: the curve whose 0.143 crossing is marked (the first by default); a curve that never
    crosses is marked at its last shell and says so, since that number is the sampling limit, not a resolution."""
    names = list(curves)
    f_max = max((max(c["freq"]) for c in curves.values() if c["freq"]), default=0.1) or 0.1
    series = [
        Series(list(c["freq"]), list(c["fsc"]), SERIES[i % len(SERIES)], name, width=1.8 if i == 0 else 1.3)
        for i, (name, c) in enumerate(curves.items())
    ]
    marks = []
    key = mark if mark in curves else (names[0] if names else None)
    if key is not None and curves[key]["freq"]:
        c = curves[key]
        f = crossing(c["freq"], c["fsc"], 0.143)
        marks = [(f, f"{1 / f:.1f} Å") if f else (c["freq"][-1], f"> {1 / c['freq'][-1]:.1f} Å (Nyquist)")]
    return Plot(
        series,
        Axis(
            0, f_max, angstrom_ticks(f_max, w - ML - MR), "resolution, Å", fmt=lambda f: f"{1 / f:.1f} Å" if f else "∞"
        ),
        Axis(-0.05, 1.0, [(0, "0"), (0.5, "0.5"), (1.0, "1")], "FSC", fmt=lambda v: f"{v:.3f}"),
        refs=[(0.143, "0.143"), (0.5, "0.5")],
        marks=marks,
    )


def shares_plot(
    iterations: Sequence[int], per_class: dict[int, Sequence[float]], color_of: Callable[[int], str]
) -> Plot:
    """Each class's share of the particles at every iteration, stacked to 100 %: the classification has settled
    where the bands stop moving."""
    its = [float(i) for i in iterations]
    series = [Series(its, [v * 100 for v in per_class[k]], color_of(k), f"class {k}") for k in sorted(per_class)]
    step = max(1, round(len(its) / 8)) if its else 1
    return Plot(
        series,
        Axis(its[0] if its else 0, its[-1] if its else 1, [(x, f"{x:g}") for x in its[::step]], "iteration"),
        Axis(0, 100, [(0, "0"), (50, "50"), (100, "100")], "% of particles", fmt=lambda v: f"{v:.1f} %"),
        stacked=True,
    )


__all__ = ["Axis", "Plot", "Series", "angstrom_ticks", "crossing", "fsc_plot", "plot", "plot_svg", "shares_plot"]
