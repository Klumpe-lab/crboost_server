"""Species glyphs for the registry prototypes: candidates for the default mark, and a set a species can pick from
to say what kind of particle it is, each drawn in one ink (`currentColor`) on a 16-unit grid like `static/icons`.

A species' glyph is chrome: grey by default, in the theme's ink, or in one of a few muted hues the user picks. Its
overlay colour stays on the canvases.
"""

from __future__ import annotations

_S = 'stroke="currentColor" stroke-width="1.15" stroke-linecap="round" stroke-linejoin="round" fill="none"'
_F = 'fill="currentColor"'
_THIN = 'stroke="currentColor" stroke-width=".85" stroke-linecap="round" stroke-linejoin="round" fill="none"'


def _ring_of(n: int, r: float, dot: float, cx: float = 8, cy: float = 8) -> str:
    from math import cos, pi, sin

    return "".join(
        f'<circle cx="{cx + r * cos(2 * pi * i / n - pi / 2):.2f}" cy="{cy + r * sin(2 * pi * i / n - pi / 2):.2f}" '
        f'r="{dot}" {_F}/>'
        for i in range(n)
    )


def _spikes(n: int, r0: float, r1: float) -> str:
    from math import cos, pi, sin

    return "".join(
        f'<line x1="{8 + r0 * cos(2 * pi * i / n):.2f}" y1="{8 + r0 * sin(2 * pi * i / n):.2f}" '
        f'x2="{8 + r1 * cos(2 * pi * i / n):.2f}" y2="{8 + r1 * sin(2 * pi * i / n):.2f}" {_S}/>'
        + f'<circle cx="{8 + (r1 + 0.6) * cos(2 * pi * i / n):.2f}" cy="{8 + (r1 + 0.6) * sin(2 * pi * i / n):.2f}" '
        f'r=".85" {_F}/>'
        for i in range(n)
    )


# Candidates for the default: what every species shows until someone picks another.
DEFAULTS: dict[str, tuple[str, str, str]] = {
    "hex": (
        "Today",
        "the hexagon with three dots (static/icons/particle.svg)",
        f'<path d="M8 1.5 L13.63 4.75 V11.25 L8 14.5 L2.37 11.25 V4.75 Z" {_S}/>'
        f'<circle cx="8" cy="5.35" r="1.2" {_F}/><circle cx="5.7" cy="9.35" r="1.2" {_F}/>'
        f'<circle cx="10.3" cy="9.35" r="1.2" {_F}/>',
    ),
    "trimer": (
        "Trimer",
        "three subunits around a centre: a molecule, not a hexagon",
        f'<circle cx="8" cy="5" r="2.75" {_S}/><circle cx="5.2" cy="10" r="2.75" {_S}/>'
        f'<circle cx="10.8" cy="10" r="2.75" {_S}/>',
    ),
    "blob": (
        "Blob",
        "a density's lumpy outline with its centre: what a pick is",
        '<path d="M6.6 2.2 C8.3 1.6 9.6 2.9 10.8 3.2 C12.6 3.6 14.1 5.0 13.6 7.0 C13.2 8.4 14.2 9.8 13.2 11.4 '
        "C12.2 13.0 10.4 12.8 9.0 13.6 C7.4 14.5 5.6 13.8 4.6 12.6 C3.6 11.4 1.9 10.9 2.1 8.9 C2.3 7.3 1.8 5.6 "
        f'3.0 4.3 C4.0 3.2 5.2 2.7 6.6 2.2 Z" {_S}/><circle cx="7.9" cy="8.1" r="1.45" {_F}/>',
    ),
    "target": (
        "Target",
        "a ring and its centre: the pick as the viewers draw it",
        f'<circle cx="8" cy="8" r="5.7" {_S}/><circle cx="8" cy="8" r="1.7" {_F}/>',
    ),
}

# What a species may pick to say what kind of particle it is.
KINDS: dict[str, tuple[str, str]] = {
    "ribosome": (
        "Ribosome",
        f'<ellipse cx="6.3" cy="4.3" rx="3.6" ry="2.2" transform="rotate(-18 6.3 4.3)" {_S}/>'
        f'<path d="M4.3 8.4 C4.3 6.9 5.6 6.6 7.0 7.0 C8.4 7.4 9.4 6.0 10.7 5.9 C12.6 5.8 13.9 7.6 13.8 9.7 '
        f'C13.7 12.3 11.6 14.1 9.0 14.1 C6.3 14.1 4.3 11.6 4.3 8.4 Z" {_S}/>',
    ),
    "capsid": (
        "Icosahedral capsid",
        f'<path d="M8 1.6 L13.55 4.8 V11.2 L8 14.4 L2.45 11.2 V4.8 Z" {_S}/>'
        f'<path d="M8 5.6 L10.6 10.1 H5.4 Z M8 1.6 V5.6 M13.55 11.2 L10.6 10.1 M2.45 11.2 L5.4 10.1" {_THIN}/>',
    ),
    "enveloped": ("Enveloped virus", f'<circle cx="8" cy="8" r="3.9" {_S}/>' + _spikes(8, 3.9, 5.7)),
    "spike": (
        "Spike / trimer on a stalk",
        f'<path d="M8 14.3 V8.6" {_S}/><path d="M3.2 14.3 H12.8" {_S}/>'
        f'<circle cx="8" cy="3.6" r="2.05" {_S}/><circle cx="5.55" cy="6.7" r="2.05" {_S}/>'
        f'<circle cx="10.45" cy="6.7" r="2.05" {_S}/>',
    ),
    "membrane": (
        "Membrane protein",
        f'<path d="M1.2 6.6 H5.2 M10.8 6.6 H14.8 M1.2 9.4 H5.2 M10.8 9.4 H14.8" {_S}/>'
        f'<rect x="5.6" y="2.3" width="4.8" height="11.4" rx="2" {_S}/>',
    ),
    "filament": (
        "Filament (actin)",
        "".join(
            f'<circle cx="{x}" cy="{y}" r="1.75" {_S}/>'
            for x, y in ((3.2, 12.6), (5.9, 11.2), (6.6, 8.4), (9.4, 7.6), (10.1, 4.8), (12.8, 3.4))
        ),
    ),
    "tube": (
        "Tube (microtubule)",
        f'<ellipse cx="3.4" cy="8" rx="1.7" ry="3.6" {_S}/>'
        f'<path d="M3.4 4.4 H12.6 C13.9 4.4 14.6 6.0 14.6 8 C14.6 10.0 13.9 11.6 12.6 11.6 H3.4" {_S}/>'
        f'<path d="M5.6 6.4 H12.4 M5.6 9.6 H12.4" {_THIN} opacity=".7"/>',
    ),
    "barrel": (
        "Barrel (proteasome)",
        "".join(f'<rect x="3.8" y="{y}" width="8.4" height="2.5" rx="1.25" {_S}/>' for y in (1.9, 4.95, 8.0, 11.05)),
    ),
    "pore": ("Ring / pore", _ring_of(8, 5.2, 1.3) + f'<circle cx="8" cy="8" r="2.1" {_S}/>'),
    "lattice": (
        "Lattice",
        "".join(
            f'<circle cx="{x}" cy="{y}" r="1.5" {_F}/>'
            for y, xs in ((3.4, (3.4, 8, 12.6)), (8, (5.7, 10.3)), (12.6, (3.4, 8, 12.6)))
            for x in xs
        ),
    ),
}

# The glyph's ink: the theme's grey by default, or one of roadmap 58's muted hues.
HUES: dict[str, tuple[str, str]] = {
    "grey": ("grey", "#94a3b8"),
    "slate": ("slate", "#5f6f86"),
    "steel": ("steel blue", "#4f7aa8"),
    "verdigris": ("verdigris", "#3f8a78"),
    "olive": ("olive", "#6f7f3a"),
    "ochre": ("ochre", "#b08a2e"),
    "sand": ("sand", "#a8916a"),
    "rust": ("rust", "#b0643a"),
    "oxide": ("oxide red", "#a14d4a"),
}


def inner(name: str) -> str:
    if name in DEFAULTS:
        return DEFAULTS[name][2]
    return KINDS[name][1]


def svg(name: str, size: int, color: str) -> str:
    """One glyph, `size` px square, in `color`."""
    return (
        f'<svg width="{size}" height="{size}" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg" '
        f'style="color: {color}; display: block; flex-shrink: 0;">{inner(name)}</svg>'
    )


# ── round 2: the molecules, and which set a page draws ───────────────────────

STYLES = (("density", "density"), ("beads", "beads"), ("line", "line, round 1"))
_WORDS2 = {
    "globule": "a globular protein, coarse-grained: its domains as beads",
    "trimer": "three subunits around an axis",
}


def catalogue(style: str) -> tuple[list[tuple[str, str, str]], list[tuple[str, str]]]:
    """(default candidates as (key, title, words), kinds as (key, title)) for a style. Today's hexagon is always a
    candidate, for comparison."""
    from ui.registry_ideas import molecules

    if style == "line":
        return [(k, t, w) for k, (t, w, _s) in DEFAULTS.items()], [(k, t) for k, (t, _s) in KINDS.items()]
    defaults = [("hex", DEFAULTS["hex"][0], DEFAULTS["hex"][1])]
    defaults += [(k, molecules.title(k), _WORDS2[k]) for k in molecules.DEFAULT_KEYS]
    return defaults, [(k, molecules.title(k)) for k in molecules.KIND_KEYS]


def draw(key: str, size: int, color: str, style: str) -> str:
    """A glyph of the page's set: the molecules for `density` and `beads`, round 1's line drawings otherwise (and
    always for today's hexagon)."""
    from ui.registry_ideas import molecules

    if style != "line" and key in molecules.MODELS:
        return molecules.svg(key, style, size, color)
    return svg(key, size, color)


def title(key: str, style: str) -> str:
    defaults, kinds = catalogue(style)
    return next((t for k, t, *_w in [*defaults, *kinds] if k == key), key)
