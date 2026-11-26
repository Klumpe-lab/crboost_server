"""Moving grounds: slow backgrounds the browser draws, for the desk, an empty view or a page that waits.

Ideas for now: `/dev/ideas` shows each one under every theme, and nothing else in the app uses them yet (roadmap 75
§7). A `Backdrop` is a few layers (plain divs) inside a `.cb-bd` element that fills its parent under everything else
in it, plus the CSS that draws and moves them. Every one keeps the same rules, so any of them can go anywhere:

- Only `transform`, `translate`, `rotate`, `scale` and `opacity` move. The browser's compositor runs those without
  repainting, and the server sends nothing (CLAUDE.md: animations are CSS, never server ticks).
- Slow and faint: a cycle takes a minute or more, and the ink is the theme's muted ink at a fraction (`--cb-bd-ink`),
  so each follows the theme it sits in.
- Shapes are SVG drawings used as masks over that ink, so one drawing serves every theme.
- `prefers-reduced-motion` stops them; `--cb-bd-speed` (1 by default) divides every duration, for a preview.
"""

from __future__ import annotations

import functools
import math
import random
from collections import defaultdict
from dataclasses import dataclass

from nicegui import ui


@dataclass(frozen=True)
class Backdrop:
    name: str
    label: str
    note: str  # what it shows, and where it comes from
    layers: tuple[str, ...]  # the layer divs' classes, back to front
    css: str


def _svg(body: str, w: float, h: float) -> str:
    """An SVG drawing as a CSS `url()`: black on transparent, read as a mask."""
    svg = f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' viewBox='0 0 {w} {h}'>{body}</svg>"
    return 'url("data:image/svg+xml;utf8,' + svg.replace("#", "%23") + '")'


def _mask(image: str, size: str, repeat: str = "repeat", at: str = "0 0") -> str:
    value = f"{image} {at} / {size} {repeat}"
    return f"-webkit-mask: {value}; mask: {value};"


def _t(seconds: float) -> str:
    return f"calc({seconds}s / var(--cb-bd-speed, 1))"


_BASE = """
.cb-bd { position: absolute; inset: 0; overflow: hidden; pointer-events: none; z-index: 0; container-type: size;
    --cb-bd-ink: color-mix(in oklab, var(--cb-ink-3) 30%, transparent);
    --cb-bd-faint: color-mix(in oklab, var(--cb-ink-3) 17%, transparent);
    --cb-bd-glow: color-mix(in oklab, var(--cb-surface) 60%, transparent);
    --cb-bd-wash: color-mix(in oklab, var(--cb-surface) 30%, transparent); }
.cb-bd > * { position: absolute; will-change: transform; }
@media (prefers-reduced-motion: reduce) { .cb-bd, .cb-bd * { animation: none !important; } }
"""


# ── Sea: wave lines drifting past each other ─────────────────────────────────────────────────────────────────
def _wave(period: float, amp: float, y0: float, shift: float, width: float = 1.0) -> str:
    """One sine-like line across a tile `period` wide (cubic halves, smooth where the tiles meet)."""
    h = amp / 0.75  # a cubic's middle reaches 3/4 of its control points' height
    x = shift % period - 2 * period
    d, sign = f"M{x:.1f} {y0:.1f}", -1
    while x < 2 * period:
        y = y0 + sign * h
        d += f" C{x + period / 6:.1f} {y:.1f} {x + period / 3:.1f} {y:.1f} {x + period / 2:.1f} {y0:.1f}"
        x, sign = x + period / 2, -sign
    return f"<path d='{d}' fill='none' stroke='black' stroke-width='{width}'/>"


def _sea() -> Backdrop:
    swells = (  # tile w, h; rows (amplitude, y, phase); ink; seconds across one tile; drift direction; bob seconds
        (240, 84, ((4, 14, 0), (5, 42, 90), (3.5, 70, 170)), "ink", 110, -1, 27),
        (360, 64, ((6, 20, 0), (4.5, 50, 200)), "faint", 170, 1, 37),
        (600, 120, ((10, 60, 0),), "faint", 260, -1, 53),
    )
    css = [
        ".cb-bd-sea { -webkit-mask: linear-gradient(to top, black 30%, transparent 92%); "
        "mask: linear-gradient(to top, black 30%, transparent 92%); }"
    ]
    for i, (w, h, rows, ink, secs, way, bob) in enumerate(swells):
        tile = _svg("".join(_wave(w, a, y, ph) for a, y, ph in rows), w, h)
        css.append(
            f".cb-bd-sea-{i} {{ inset: -24px -640px; background: var(--cb-bd-{ink}); {_mask(tile, f'{w}px {h}px')} "
            f"animation: cb-bd-sea-{i} {_t(secs)} linear infinite, cb-bd-bob-{i} {_t(bob)} ease-in-out infinite "
            f"alternate; }}\n@keyframes cb-bd-sea-{i} {{ to {{ transform: translateX({way * w}px); }} }}\n"
            f"@keyframes cb-bd-bob-{i} {{ from {{ translate: 0 -5px; }} to {{ translate: 0 5px; }} }}"
        )
    return Backdrop(
        "sea",
        "Slow sea",
        "Three swells of wave lines drifting past each other, under a horizon.",
        tuple(f"cb-bd-sea-{i}" for i in range(len(swells))),
        "\n".join(css),
    )


# ── Particles in ice: capsids, ribosomes, gold beads, drifting at three depths ───────────────────────────────
_GLYPHS = {
    # The species mark's capsid (static/icons/species/capsid.svg), centred.
    "capsid": "<g transform='translate(-8 -8)'><path d='M8 1.6 L13.55 4.8 V11.2 L8 14.4 L2.45 11.2 V4.8 Z'/>"
    "<path d='M8 5.6 L10.6 10.1 H5.4 Z M8 1.6 V5.6 M13.55 11.2 L10.6 10.1 M2.45 11.2 L5.4 10.1' "
    "stroke-width='.7'/></g>",
    "ribosome": "<circle r='3.6'/><circle cx='4.1' cy='-1.6' r='2.4'/>",
    "gold": "<circle r='2.4' fill='black'/>",  # a fiducial: solid
    "vesicle": "<circle r='5.5'/>",
    "dot": "<circle r='0.9' fill='black'/>",
}


def _ice_tile(rng: random.Random, tile: int, n: int, scale: tuple[float, float]) -> str:
    kinds = ("capsid", "capsid", "ribosome", "ribosome", "gold", "vesicle", "dot", "dot")
    out = []
    for _ in range(n):
        x, y = rng.uniform(14, tile - 14), rng.uniform(14, tile - 14)
        k, r = rng.uniform(*scale), rng.uniform(0, 360)
        out.append(
            f"<g transform='translate({x:.1f} {y:.1f}) rotate({r:.0f}) scale({k:.2f})'>{_GLYPHS[rng.choice(kinds)]}</g>"
        )
    body = "".join(out)
    return _svg(f"<g fill='none' stroke='black' stroke-width='1' stroke-linejoin='round'>{body}</g>", tile, tile)


def _ice() -> Backdrop:
    rng = random.Random(75)
    depths = (  # tile, glyphs, scale range, ink, drift in tiles (x, y), seconds
        (230, 12, (0.5, 0.65), "faint", (-2, -1), 420),
        (310, 8, (0.75, 0.95), "ink", (-1, -1), 300),
        (410, 5, (1.05, 1.3), "ink", (-1, -2), 420),
    )
    css = []
    for i, (tile, n, scale, ink, (dx, dy), secs) in enumerate(depths):
        css.append(
            f".cb-bd-ice-{i} {{ inset: -{2 * tile}px; background: var(--cb-bd-{ink}); "
            f"{_mask(_ice_tile(rng, tile, n, scale), f'{tile}px {tile}px')} "
            f"animation: cb-bd-ice-{i} {_t(secs)} linear infinite; }}\n"
            f"@keyframes cb-bd-ice-{i} {{ to {{ transform: translate({dx * tile}px, {dy * tile}px); }} }}"
        )
    return Backdrop(
        "ice",
        "Particles in ice",
        "Capsids, ribosomes, gold beads and vesicles adrift at three depths, the near ones faster.",
        tuple(f"cb-bd-ice-{i}" for i in range(len(depths))),
        "\n".join(css),
    )


# ── Thon rings: a CTF's rings, breathing with defocus, the astigmatism turning ───────────────────────────────
def _thon_rings() -> str:
    rings, n, big = [], 24, 570.0
    for k in range(1, n + 1):
        r, r_next = big * math.sqrt(k / n), big * math.sqrt((k + 1) / n)  # zeros of the CTF go as sqrt(k)
        fade = 0.05 + 0.95 * math.exp(-k / 12)  # the envelope
        rings.append(
            f"<ellipse cx='600' cy='600' rx='{r:.1f}' ry='{r * 0.94:.1f}' "
            f"stroke-width='{0.36 * (r_next - r):.2f}' stroke-opacity='{fade:.2f}'/>"
        )
    return _svg(f"<g fill='none' stroke='black'>{''.join(rings)}</g>", 1200, 1200)


def _thon() -> Backdrop:
    css = (
        ".cb-bd-thon-0 { width: 160cqmax; height: 160cqmax; left: calc(76cqw - 80cqmax); "
        f"top: calc(90cqh - 80cqmax); background: var(--cb-bd-faint); {_mask(_thon_rings(), '100% 100%', 'no-repeat')} "
        f"animation: cb-bd-thon-df {_t(52)} ease-in-out infinite alternate, "
        f"cb-bd-thon-ast {_t(720)} linear infinite; }}\n"
        "@keyframes cb-bd-thon-df { from { scale: 1; } to { scale: 1.16; } }\n"
        "@keyframes cb-bd-thon-ast { to { rotate: 360deg; } }"
    )
    return Backdrop(
        "thon",
        "Thon rings",
        "A power spectrum's rings from a corner: they breathe as the defocus "
        "changes, and the astigmatism turns once in twelve minutes.",
        ("cb-bd-thon-0",),
        css,
    )


# ── Tilt series: the rays rock ±60°, the missing wedge never lit ─────────────────────────────────────────────
def _tilt() -> Backdrop:
    spot = (
        "-webkit-mask: radial-gradient(circle closest-side, black 25%, transparent 100%); "
        "mask: radial-gradient(circle closest-side, black 25%, transparent 100%);"
    )
    place = "width: 90cqmax; height: 90cqmax; left: calc(32cqw - 45cqmax); top: calc(55cqh - 45cqmax);"
    css = (
        f".cb-bd-tilt-0 {{ {place} {spot} background: conic-gradient(from -60deg, var(--cb-bd-wash) 0 120deg, "
        "transparent 120deg 180deg, var(--cb-bd-wash) 180deg 300deg, transparent 300deg); }\n"
        f".cb-bd-tilt-1 {{ {place} {spot} background: repeating-linear-gradient(90deg, var(--cb-bd-ink) 0 1px, "
        f"transparent 1px 9px); animation: cb-bd-tilt {_t(84)} ease-in-out infinite alternate; }}\n"
        "@keyframes cb-bd-tilt { from { rotate: -60deg; } to { rotate: 60deg; } }"
    )
    return Backdrop(
        "tilt",
        "Tilt series",
        "A round beam whose rays rock from −60° to +60° and back; the double "
        "wedge they sweep is faintly lit, the missing wedge never.",
        ("cb-bd-tilt-0", "cb-bd-tilt-1"),
        css,
    )


# ── Moiré: a second dot grid turning over the desk's ─────────────────────────────────────────────────────────
def _moire() -> Backdrop:
    css = (
        ".cb-bd-moire-0 { inset: -14cqmax; background: radial-gradient(circle, var(--cb-bd-ink) 0.9px, "
        f"transparent 1.25px) 0 0 / 14px 14px; animation: cb-bd-moire-turn {_t(300)} ease-in-out infinite alternate, "
        f"cb-bd-moire-zoom {_t(130)} ease-in-out infinite alternate; }}\n"
        "@keyframes cb-bd-moire-turn { from { rotate: 0.7deg; } to { rotate: 4.5deg; } }\n"
        "@keyframes cb-bd-moire-zoom { from { scale: 1; } to { scale: 1.025; } }"
    )
    return Backdrop(
        "moire",
        "Moiré",
        "A second dot grid over the desk's, turning a few degrees and back: its "
        "rosettes swell, shrink and wander. The desk's own grid, alive.",
        ("cb-bd-moire-0",),
        css,
    )


# ── Back-projection: rays through one point in the dose-symmetric order, a star forming ──────────────────────
_BP_ANGLES = (0, 10, -10, 20, -20, 30, -30, 40, -40, 50, -50, 60, -60)


def _backprojection() -> Backdrop:
    cycle = 48
    css = [
        ".cb-bd-bp-l { width: 150cqmax; height: 1px; left: calc(34cqw - 75cqmax); top: 52cqh; opacity: 0; "
        "background: linear-gradient(90deg, transparent, var(--cb-bd-ink) 35% 65%, transparent); }",
        ".cb-bd-bp-c { width: 7px; height: 7px; border-radius: 50%; left: calc(34cqw - 3.5px); top: calc(52cqh - 3px);"
        f" background: var(--cb-bd-ink); opacity: 0; animation: cb-bd-bp-c {_t(cycle)} ease-in infinite; }}",
        "@keyframes cb-bd-bp-c { 0%, 4% { opacity: 0; } 62%, 76% { opacity: 1; } 90%, 100% { opacity: 0; } }",
    ]
    for i, angle in enumerate(_BP_ANGLES):
        on = 3 + 4.5 * i
        css.append(
            f".cb-bd-bp-l:nth-child({i + 1}) {{ rotate: {90 + angle}deg; animation: cb-bd-bp-{i} {_t(cycle)} linear "
            f"infinite; }}\n@keyframes cb-bd-bp-{i} {{ 0%, {on:.1f}% {{ opacity: 0; }} {on + 3:.1f}%, 76% "
            "{ opacity: 1; } 90%, 100% { opacity: 0; } }"
        )
    return Backdrop(
        "backprojection",
        "Back-projection",
        "Rays through one point, laid down tilt by tilt in the "
        "dose-symmetric order (0, +10, −10, …, ±60): the star and the point form, hold, and fade.",
        ("cb-bd-bp-l",) * len(_BP_ANGLES) + ("cb-bd-bp-c",),
        "\n".join(css),
    )


# ── Filament: two-start helices of beads, turning as they pass ───────────────────────────────────────────────
def _helix_tile(pitch: float, height: float, amp: float, r0: float, dr: float, n: int) -> str:
    beads = []
    for strand, phase in ((1, 0.0), (-1, 0.5)):
        for i in range(-1, n + 2):
            x = (i + phase) * pitch / n
            th = 2 * math.pi * x / pitch
            depth = strand * math.cos(th)  # +1 in front
            beads.append(
                f"<circle cx='{x:.2f}' cy='{height / 2 + strand * amp * math.sin(th):.2f}' "
                f"r='{r0 + dr * depth:.2f}' fill-opacity='{0.72 + 0.28 * depth:.2f}'/>"
            )
    return _svg(f"<g fill='black'>{''.join(beads)}</g>", pitch, height)


def _helix() -> Backdrop:
    strands = (  # pitch, height, amplitude, bead radius, its depth swing, beads per turn; place; ink; seconds; way
        (84, 28, 8, 3.2, 0.9, 7, "38cqh", -14, "ink", 46, -1),
        (56, 20, 5.5, 2.2, 0.6, 7, "72cqh", 9, "faint", 70, 1),
    )
    css = []
    for i, (p, h, amp, r0, dr, n, top, angle, ink, secs, way) in enumerate(strands):
        css.append(
            f".cb-bd-helix-{i} {{ width: 300cqmax; height: {h}px; left: calc(50cqw - 150cqmax); "
            f"top: calc({top} - {h / 2}px); rotate: {angle}deg; background: var(--cb-bd-{ink}); "
            f"{_mask(_helix_tile(p, h, amp, r0, dr, n), f'{p}px {h}px', 'repeat-x')} "
            f"animation: cb-bd-helix-{i} {_t(secs)} linear infinite; }}\n"
            f"@keyframes cb-bd-helix-{i} {{ to {{ transform: translateX({way * p}px); }} }}"
        )
    return Backdrop(
        "helix",
        "Filament",
        "Two double helices of beads crossing the desk, each turning as it slides along its axis, one turn in 46 s.",
        ("cb-bd-helix-0", "cb-bd-helix-1"),
        "\n".join(css),
    )


# ── Contours: a density map's level lines, traced here, two sheets drifting over each other ──────────────────
def _contour_tile(seed: int, tile: int, levels: tuple[float, ...], cells: int = 64) -> str:
    """Level lines (in standard deviations) of a smooth field that repeats every `tile` px, traced by marching
    squares: the field is a sum of waves whole numbers of times across the tile, so the lines meet at its edges."""
    rng = random.Random(seed)
    waves = []
    for _ in range(6):
        m, n = rng.randint(-3, 3), rng.randint(0, 3)
        if m or n:
            waves.append((m, n, 1.0 / math.hypot(m, n), rng.uniform(0, 2 * math.pi)))
    grid = [
        [
            sum(a * math.cos(2 * math.pi * (m * i + n * j) / cells + ph) for m, n, a, ph in waves)
            for i in range(cells + 1)
        ]
        for j in range(cells + 1)
    ]
    sd = math.sqrt(sum(v * v for row in grid for v in row) / (cells + 1) ** 2)
    step, segs = tile / cells, []
    for level in (lv * sd for lv in levels):
        for j in range(cells):
            for i in range(cells):
                v = (grid[j][i], grid[j][i + 1], grid[j + 1][i + 1], grid[j + 1][i])
                corners = ((i, j), (i + 1, j), (i + 1, j + 1), (i, j + 1))
                cuts = []
                for e in range(4):  # where the level crosses each edge of the cell
                    a, b = v[e], v[(e + 1) % 4]
                    if (a < level) != (b < level):
                        t = (level - a) / (b - a)
                        (x0, y0), (x1, y1) = corners[e], corners[(e + 1) % 4]
                        cuts.append(((x0 + t * (x1 - x0)) * step, (y0 + t * (y1 - y0)) * step))
                for k in range(0, len(cuts) - 1, 2):  # 2 crossings: one segment; 4 (a saddle): two
                    segs.append(tuple((round(x, 1), round(y, 1)) for x, y in cuts[k : k + 2]))
    d = "".join(f"M{line[0][0]} {line[0][1]}L" + " ".join(f"{x} {y}" for x, y in line[1:]) for line in _chains(segs))
    return _svg(f"<path d='{d}' fill='none' stroke='black' stroke-width='1' stroke-linecap='round'/>", tile, tile)


def _chains(segs: list) -> list[list]:
    """Segments that share ends joined into polylines (a path a third the size)."""
    ends = defaultdict(list)
    for k, (a, b) in enumerate(segs):
        ends[a].append(k)
        ends[b].append(k)
    used, lines = [False] * len(segs), []
    for k, seg in enumerate(segs):
        if used[k]:
            continue
        used[k] = True
        line = list(seg)
        for forward in (True, False):
            while True:
                tip = line[-1] if forward else line[0]
                nxt = next((q for q in ends[tip] if not used[q]), None)
                if nxt is None:
                    break
                used[nxt] = True
                a, b = segs[nxt]
                if forward:
                    line.append(b if a == tip else a)
                else:
                    line.insert(0, b if a == tip else a)
        lines.append(line)
    return lines


def _drifting(name: str, sheets: tuple) -> str:
    # Each sheet drifts by whole tiles, so its loop has no seam.
    css = []
    for i, (image, tile, ink, (dx, dy), secs) in enumerate(sheets):
        css.append(
            f".cb-bd-{name}-{i} {{ inset: -{2 * tile}px; background: var(--cb-bd-{ink}); "
            f"{_mask(image, f'{tile}px {tile}px')} animation: cb-bd-{name}-{i} {_t(secs)} linear infinite; }}\n"
            f"@keyframes cb-bd-{name}-{i} {{ to {{ transform: translate({dx * tile}px, {dy * tile}px); }} }}"
        )
    return "\n".join(css)


def _contours() -> Backdrop:
    sheets = (
        (_contour_tile(4, 480, (-1.2, -0.4, 0.4, 1.2)), 480, "ink", (-2, -1), 600),
        (_contour_tile(11, 560, (-0.6, 0.6)), 560, "faint", (1, -1), 420),
    )
    return Backdrop(
        "contours",
        "Contours",
        "A density map's level lines, two sheets drifting across each other so the map slowly reshapes.",
        ("cb-bd-contours-0", "cb-bd-contours-1"),
        _drifting("contours", sheets),
    )


# ── Caustics: bright webs where a smooth noise crosses its middle, sliding over each other ───────────────────
def _web(seed: int, freq: float, tile: int) -> str:
    """A soft web of lines where a fractal noise crosses its middle (the browser draws the noise once)."""
    table = " ".join("1" if k == 20 else "0" for k in range(41))
    body = (
        f"<filter id='f' x='0' y='0' width='{tile}' height='{tile}' filterUnits='userSpaceOnUse' "
        "color-interpolation-filters='sRGB'>"
        f"<feTurbulence type='fractalNoise' baseFrequency='{freq}' numOctaves='2' seed='{seed}' stitchTiles='stitch'/>"
        "<feColorMatrix type='matrix' values='0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  1 0 0 0 0'/>"
        f"<feComponentTransfer><feFuncA type='table' tableValues='{table}'/></feComponentTransfer></filter>"
        f"<rect width='{tile}' height='{tile}' filter='url(#f)'/>"
    )
    return _svg(body, tile, tile)


def _caustics() -> Backdrop:
    sheets = ((_web(2, 0.011, 360), 360, "glow", (-2, -1), 280), (_web(8, 0.009, 440), 440, "glow", (1, -2), 400))
    return Backdrop(
        "caustics",
        "Caustics",
        "Light at the bottom of a pool, or through a condenser: two webs of "
        "bright lines sliding over each other. Lighter than the desk, not darker.",
        ("cb-bd-caustics-0", "cb-bd-caustics-1"),
        _drifting("caustics", sheets),
    )


@functools.cache
def backdrops() -> dict[str, Backdrop]:
    """Every backdrop, drawn on first use (tracing the contours takes a moment; a server that never shows one never
    pays it)."""
    made = (_sea(), _ice(), _thon(), _tilt(), _moire(), _backprojection(), _helix(), _contours(), _caustics())
    return {b.name: b for b in made}


def backdrop_css() -> str:
    """Every backdrop's CSS, for a page that shows them (sent once, with the page's other head HTML)."""
    return _BASE + "\n".join(b.css for b in backdrops().values())


def backdrop(name: str) -> ui.element:
    """A backdrop filling the current container (which must be positioned): its layers under the container's other
    children, which need `position: relative; z-index: 1` or similar to sit above it."""
    b = backdrops()[name]
    with ui.element("div").classes(f"cb-bd cb-bd-{name}") as root:
        for cls in b.layers:
            ui.element("div").classes(cls)
    return root
