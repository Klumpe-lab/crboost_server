"""Species glyphs, round 2: each molecule is a small coarse-grained model (beads where its domains are, on a 24-unit
grid), drawn two ways in one ink: `beads`, the model itself, and `density`, iso-contours of the model's density,
the way a map is read. Membranes, DNA and the like are drawn as lines over either.

The contours are computed once per glyph (`skimage.measure.find_contours` on the sum of one Gaussian per bead).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from math import cos, pi, radians, sin

import numpy as np

Bead = tuple[float, float, float]  # x, y, radius on the 24 grid


Rod = tuple[float, float, float, float, float]  # x1, y1, x2, y2, radius: a helix or a fibre


@dataclass(frozen=True)
class Model:
    title: str
    beads: tuple[Bead, ...]
    lines: str = ""  # extra SVG drawn over either style, in currentColor
    rods: tuple[Rod, ...] = ()
    core: float | None = None  # the density style's inner contour, as a share of the smoothed map's peak


def _ring(n: int, radius: float, r: float, *, phase: float = -90.0, cx: float = 12, cy: float = 12) -> list[Bead]:
    return [
        (cx + radius * cos(radians(phase + 360 * i / n)), cy + radius * sin(radians(phase + 360 * i / n)), r)
        for i in range(n)
    ]


def _rotate(beads: list[Bead], deg: float, cx: float = 12, cy: float = 12) -> list[Bead]:
    a = radians(deg)
    return [
        (cx + (x - cx) * cos(a) - (y - cy) * sin(a), cy + (x - cx) * sin(a) + (y - cy) * cos(a), r) for x, y, r in beads
    ]


_LINE = 'stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"'


def _trimer() -> list[Bead]:
    out: list[Bead] = []
    for ang in (-90, 30, 150):
        cx, cy = 12 + 5.0 * cos(radians(ang)), 12.6 + 5.0 * sin(radians(ang))
        out.append((cx, cy, 2.9))
        for side in (-38, 38):
            out.append((cx + 2.9 * cos(radians(ang + side)), cy + 2.9 * sin(radians(ang + side)), 1.9))
    return out


def _capsid() -> list[Bead]:
    out: list[Bead] = []
    for i in range(6):  # pentamers at the vertices of the projected icosahedron, capsomers between them
        a = radians(-90 + 60 * i)
        out.append((12 + 9.0 * cos(a), 12 + 9.0 * sin(a), 2.15))
        b = radians(-60 + 60 * i)
        out.append((12 + 7.8 * cos(b), 12 + 7.8 * sin(b), 1.85))
    out += _ring(6, 4.4, 1.75, phase=-60)
    return out


def _actin() -> list[Bead]:
    out: list[Bead] = []
    for i in range(6):
        x = 1.6 + 4.15 * i
        out.append((x, 10.0 + 0.9 * sin(i * 1.1), 2.15))
        out.append((x + 2.07, 14.0 - 0.9 * sin(i * 1.1 + 0.6), 2.15))
    return _rotate(out, -38)


def _superhelix() -> str:
    """DNA's 1.65 turns around the octamer, seen down its axis: a spiral widening by its own width per turn, with
    the linker DNA running out at both ends."""
    turns, start = 1.65, 125.0
    pts = []
    for k in range(0, 121):
        t = k / 120
        a = radians(start + 360 * turns * t)
        r = 7.0 + 2.0 * t
        pts.append((12 + r * cos(a), 12 + r * sin(a)))
    head = f"M{12 + 7.0 * cos(radians(start)) - 3.2:.2f} {12 + 7.0 * sin(radians(start)) + 3.6:.2f} "
    body = "L" + " L".join(f"{x:.2f} {y:.2f}" for x, y in pts)
    end = pts[-1]
    a_end = radians(start + 360 * turns)
    tail = f" L{end[0] + 3.0 * -sin(a_end):.2f} {end[1] + 3.0 * cos(a_end):.2f}"
    return head + body + tail


def _membrane_heads(y: float, skip: tuple[float, float]) -> str:
    xs = [1.4 + 2.2 * i for i in range(11)]
    return "".join(
        f'<circle cx="{x:.2f}" cy="{y}" r=".85" fill="currentColor"/>' for x in xs if not (skip[0] <= x <= skip[1])
    )


MODELS: dict[str, Model] = {
    "globule": Model(
        "Globule",
        (
            (9.0, 8.2, 3.4),
            (14.8, 7.6, 3.0),
            (17.6, 12.6, 3.2),
            (13.6, 17.0, 3.5),
            (7.8, 15.4, 3.1),
            (12.2, 12.2, 3.0),
            (4.6, 10.6, 2.0),
            (18.8, 17.9, 1.8),
        ),
        core=0.75,
    ),
    "trimer": Model("Trimer", tuple(_trimer()), core=0.75),
    "ribosome": Model(
        "Ribosome",
        (
            # small subunit: head, body, foot, platform
            (5.4, 6.6, 2.7),
            (4.9, 12.0, 3.1),
            (5.8, 17.6, 2.8),
            (8.1, 9.6, 1.4),
            # large subunit: core, central protuberance, the stalks
            (15.8, 13.6, 5.2),
            (14.2, 7.0, 2.9),
            (19.8, 7.8, 2.3),
            (12.8, 19.2, 2.7),
            (18.8, 19.0, 2.8),
            (21.4, 13.8, 1.9),
        ),
        core=0.75,
    ),
    "capsid": Model("Icosahedral capsid", tuple(_capsid())),
    "enveloped": Model(
        "Enveloped virus",
        tuple(
            [(12 + 10.0 * cos(radians(a)), 12 + 10.0 * sin(radians(a)), 1.35) for a in range(-90, 270, 40)]
            + [(10.2, 10.6, 1.05), (13.6, 9.6, 1.05), (14.2, 13.4, 1.05), (10.6, 14.2, 1.05)]
        ),
        lines=f'<circle cx="12" cy="12" r="6.4" {_LINE} stroke-width=".9"/>'
        f'<circle cx="12" cy="12" r="7.7" {_LINE} stroke-width=".9"/>'
        + "".join(
            f'<line x1="{12 + 7.7 * cos(radians(a)):.2f}" y1="{12 + 7.7 * sin(radians(a)):.2f}" '
            f'x2="{12 + 8.8 * cos(radians(a)):.2f}" y2="{12 + 8.8 * sin(radians(a)):.2f}" {_LINE} stroke-width=".9"/>'
            for a in range(-90, 270, 40)
        ),
    ),
    "spike": Model(
        "Glycoprotein spike",
        ((9.3, 7.6, 2.7), (14.7, 7.6, 2.7), (12.0, 3.9, 2.6), (12.0, 11.6, 1.6), (12.0, 14.4, 1.25)),
        lines=f'<path d="M1 18.4 H23 M1 21.6 H23" {_LINE} stroke-width=".9"/>'
        f'<path d="M12 15.6 V21.6" {_LINE} stroke-width="1.1"/>',
    ),
    "membrane": Model(
        "Membrane protein",
        ((12.0, 3.0, 2.2), (12.0, 21.0, 1.9)),
        lines=_membrane_heads(6.6, (5.6, 18.4)) + _membrane_heads(17.4, (5.6, 18.4)),
        rods=tuple(
            (x, 4.6 + dy, x + 0.6, 19.4 - dy, 1.25) for x, dy in ((7.6, 1.0), (10.6, 0.0), (13.4, 0.0), (16.2, 1.0))
        ),
    ),
    "actin": Model("Actin filament", tuple(_actin())),
    "microtubule": Model(
        "Microtubule (end-on)",
        tuple(_ring(13, 8.1, 1.8)),
        lines=f'<circle cx="12" cy="12" r="3.2" {_LINE} stroke-width=".6" stroke-dasharray=".8 1"/>',
    ),
    "proteasome": Model(
        "Proteasome (side)",
        tuple(
            (x, y, r)
            for y, r in ((4.8, 1.85), (9.6, 2.15), (14.4, 2.15), (19.2, 1.85))
            for x in (6.3, 10.1, 13.9, 17.7)
        ),
    ),
    "pore": Model(
        "Nuclear pore",
        tuple(_ring(8, 8.7, 1.75, phase=-90) + _ring(8, 5.9, 1.4, phase=-90)),
        lines='<circle cx="12" cy="12" r="1" fill="currentColor" opacity=".55"/>',
    ),
    "nucleosome": Model(
        "Nucleosome",
        tuple([(12, 12, 2.2), *_ring(6, 3.5, 2.1, phase=-60)]),
        lines=f'<path d="{_superhelix()}" {_LINE} stroke-width="1.05"/>',
    ),
    "lattice": Model(
        "Hexameric lattice",
        tuple(
            b
            for cx, cy in [
                (12.0, 12.0),
                *[(12 + 7.8 * cos(radians(a)), 12 + 7.8 * sin(radians(a))) for a in range(-90, 270, 60)],
            ]
            for b in _ring(6, 2.0, 1.0, phase=0, cx=cx, cy=cy)
        ),
    ),
}

DEFAULT_KEYS = ("globule", "trimer")
KIND_KEYS = tuple(k for k in MODELS if k not in DEFAULT_KEYS)


# ── drawing ──────────────────────────────────────────────────────────────────


def _samples(m: Model) -> list[Bead]:
    """The model's beads, and its rods as beads along them, for the density."""
    out = list(m.beads)
    for x1, y1, x2, y2, r in m.rods:
        n = max(2, int(np.hypot(x2 - x1, y2 - y1) / (r * 0.6)))
        out += [(x1 + (x2 - x1) * t, y1 + (y2 - y1) * t, r) for t in np.linspace(0, 1, n)]
    return out


@cache
def _contours(key: str) -> tuple[str, str]:
    """The density style's outline (the model's union, a low threshold of a sharp map) and its core (a high
    threshold of the same map smoothed), "" when the model has none."""
    from skimage import measure

    m = MODELS[key]
    step = 0.1
    ax = np.arange(-1.0, 25.0 + step, step)
    xx, yy = np.meshgrid(ax, ax)
    sharp = np.zeros_like(xx)
    smooth = np.zeros_like(xx)
    for x, y, r in _samples(m):
        d2 = (xx - x) ** 2 + (yy - y) ** 2
        sharp += np.exp(-2.0 * d2 / r**2)
        smooth += np.exp(-d2 / (2 * r**2))

    def path(field, level) -> str:
        return " ".join(
            "M"
            + " L".join(f"{ax[0] + p[1] * step:.2f} {ax[0] + p[0] * step:.2f}" for p in c[:: max(1, len(c) // 90)])
            + " Z"
            for c in measure.find_contours(field, level)
        )

    core = path(smooth, m.core * smooth.max()) if m.core else ""
    return path(sharp, 0.16), core


def inner(key: str, style: str) -> str:
    m = MODELS[key]
    if style == "beads":
        body = "".join(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{r * 0.86:.2f}" fill="currentColor"/>' for x, y, r in m.beads
        )
        body += "".join(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="currentColor" stroke-width="{2 * r * 0.86:.2f}" '
            'stroke-linecap="round"/>'
            for x1, y1, x2, y2, r in m.rods
        )
    else:
        outer, core = _contours(key)
        body = f'<path d="{outer}" {_LINE} stroke-width="1.5"/>'
        if core:
            body += f'<path d="{core}" {_LINE} stroke-width=".85" opacity=".75"/>'
        body += "".join(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" {_LINE} stroke-width=".8" opacity=".75"/>'
            for x1, y1, x2, y2, _r in m.rods
        )
    return body + m.lines


def svg(key: str, style: str, size: int, color: str) -> str:
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" '
        f'style="color: {color}; display: block; flex-shrink: 0;">{inner(key, style)}</svg>'
    )


def title(key: str) -> str:
    return MODELS[key].title


_ = pi  # math names used by the models above
