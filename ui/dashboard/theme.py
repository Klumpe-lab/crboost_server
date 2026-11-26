"""The look's values: the palette, the tokens and the themes, as CSS custom properties.

Three layers, each read by the one above it:

1. **The palette** (`PALETTE`). Every colour a component still spells is a palette variable named after it:
   `var(--cb-slate-400, #94a3b8)` inline, `var(--cb-slate-400)` in `ui/dashboard/css.py`. The light theme
   defines each name as exactly the colour it is named after, so the light look is the literal one; a theme
   redefines the names. Tailwind's colours are named as Tailwind names them, the house's own `x-<hex>`.
   Colours that are the same in every theme stay literal: light text (it sits on a dark well or a coloured
   fill), dark grounds and edges (log wells, image panes, the navy action).
2. **The tokens** (`_LIGHT_TOKENS`): what a colour, a line, a radius or a voice is for (`--cb-page`,
   `--cb-ink-3`, `--cb-label-*`, `--cb-ok-*`, ...). New UI names a token or a voice, never a palette colour.
3. **The themes** (`THEMES`): a theme is a set of values for the palette and the tokens, under
   `[data-cb-theme="<name>"]` on the page's root (or on any element, which themes only what is inside it:
   the boards of `/dev/ideas`). Light is the default and sets no attribute. Dark also turns on Quasar's dark
   mode for the Quasar components that draw their own grounds. The papers (Mist, Sage, Linen) are light themes
   on softer grounds than white. Every theme's desk wears the same dot grid (`--cb-desk-art`), in its own ink.

The theme a page opens in is the user's (`UserSettings.theme`), or `?theme=<name>` in its address for that
page only. `theme_head()` puts it on the page before its first paint; `apply_theme_js()` switches it live.
"""

from __future__ import annotations

import asyncio
import functools
import itertools
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, field

from nicegui import Client, context, ui

from services.configs.user_settings import load_user_settings

# ── 1. The palette, light values ─────────────────────────────────────────────────────────────────────────────
PALETTE: dict[str, str] = {
    "amber-100": "#fef3c7", "amber-200": "#fde68a", "amber-300": "#fcd34d", "amber-50": "#fffbeb",
    "amber-500": "#f59e0b", "amber-600": "#d97706", "amber-700": "#b45309", "black": "#000000",
    "blue-100": "#dbeafe", "blue-200": "#bfdbfe", "blue-300": "#93c5fd", "blue-400": "#60a5fa",
    "blue-50": "#eff6ff", "blue-500": "#3b82f6", "blue-600": "#2563eb", "blue-700": "#1d4ed8",
    "blue-800": "#1e40af", "cyan-400": "#22d3ee", "emerald-200": "#a7f3d0", "emerald-50": "#ecfdf5",
    "emerald-500": "#10b981", "emerald-600": "#059669", "emerald-700": "#047857", "gray-100": "#f3f4f6",
    "gray-200": "#e5e7eb", "gray-300": "#d1d5db", "gray-400": "#9ca3af", "gray-500": "#6b7280",
    "gray-700": "#374151", "gray-800": "#1f2937", "green-200": "#bbf7d0", "green-50": "#f0fdf4",
    "green-600": "#16a34a", "green-700": "#15803d", "green-800": "#166534", "indigo-200": "#c7d2fe",
    "indigo-400": "#818cf8", "indigo-50": "#eef2ff", "indigo-500": "#6366f1", "indigo-600": "#4f46e5",
    "orange-50": "#fff7ed", "orange-500": "#f97316", "orange-700": "#c2410c", "red-100": "#fee2e2",
    "red-200": "#fecaca", "red-300": "#fca5a5", "red-400": "#f87171", "red-50": "#fef2f2", "red-500": "#ef4444",
    "red-600": "#dc2626", "red-700": "#b91c1c", "sky-50": "#f0f9ff", "sky-700": "#0369a1", "slate-100": "#f1f5f9",
    "slate-200": "#e2e8f0", "slate-300": "#cbd5e1", "slate-400": "#94a3b8", "slate-50": "#f8fafc",
    "slate-500": "#64748b", "slate-600": "#475569", "slate-700": "#334155", "slate-800": "#1e293b",
    "slate-900": "#0f172a", "violet-700": "#6d28d9", "white": "#ffffff", "x-12324f": "#12324f",
    "x-1f4e79": "#1f4e79", "x-24405e": "#24405e", "x-2c4a6b": "#2c4a6b", "x-33506e": "#33506e",
    "x-4a6584": "#4a6584", "x-4a8fd4": "#4a8fd4", "x-4e79a7": "#4e79a7", "x-4f6782": "#4f6782",
    "x-5b6f8a": "#5b6f8a", "x-5b7089": "#5b7089", "x-6b829b": "#6b829b", "x-7d93ab": "#7d93ab",
    "x-8a5a00": "#8a5a00", "x-8aa0b8": "#8aa0b8", "x-9fb2c8": "#9fb2c8", "x-9fb6cf": "#9fb6cf",
    "x-a61b1b": "#a61b1b", "x-a9b8c9": "#a9b8c9", "x-a9c6e4": "#a9c6e4", "x-b3c0cf": "#b3c0cf",
    "x-b6c0cf": "#b6c0cf", "x-b8c7d8": "#b8c7d8", "x-b8cce0": "#b8cce0", "x-c3cfdc": "#c3cfdc",
    "x-c5d3e2": "#c5d3e2", "x-c9d1dc": "#c9d1dc", "x-c9d6e3": "#c9d6e3", "x-c9d6e5": "#c9d6e5",
    "x-c9dbee": "#c9dbee", "x-d3dde8": "#d3dde8", "x-dbe3ec": "#dbe3ec", "x-dbe7f3": "#dbe7f3",
    "x-e2eaf3": "#e2eaf3", "x-e3e8f0": "#e3e8f0", "x-e3eaf2": "#e3eaf2", "x-e4eaf1": "#e4eaf1",
    "x-e8eef6": "#e8eef6", "x-e9a3a3": "#e9a3a3", "x-e9f0f7": "#e9f0f7", "x-eceff3": "#eceff3",
    "x-edf4fb": "#edf4fb", "x-eef1f6": "#eef1f6", "x-eef2f6": "#eef2f6", "x-eef2f7": "#eef2f7",
    "x-eef3f8": "#eef3f8", "x-eef4fb": "#eef4fb", "x-f1f1f1": "#f1f1f1", "x-f2f6fb": "#f2f6fb",
    "x-f3d9a4": "#f3d9a4", "x-f4f8fc": "#f4f8fc", "x-fafbfc": "#fafbfc", "x-fafcfe": "#fafcfe",
    "x-fbf4f4": "#fbf4f4", "x-fefafa": "#fefafa", "x-fff7e6": "#fff7e6", "x-fffafa": "#fffafa",
    "zinc-50": "#fafafa", "yellow-50": "#fefce8", "x-4a3aa7": "#4a3aa7", "x-be4343": "#be4343",
    "x-c0cad4": "#c0cad4",
}  # fmt: skip

# ── 2. The tokens, light values ──────────────────────────────────────────────────────────────────────────────
# Harbor's palette and faces in Oxide's voice and geometry (Harbor Mono, roadmap 51): labels, controls, badges and
# column heads in mono capitals; 2 px corners, no shadows; square pips; one box deep.
_LIGHT_TOKENS = """
    /* Grounds and cards. */
    --cb-page: #ffffff; --cb-desk: #e9eef4; --cb-surface: #ffffff; --cb-rail: #eef3f8; --cb-panel: #ffffff;
    --cb-panel-line: #d6e0ea; --cb-panel-shadow: none; --cb-radius: 2px; --cb-pad-y: 9px; --cb-pad-x: 11px;
    --cb-edge: #c9d6e3; --cb-edge-w: 1px; --cb-edge-shadow: none; --cb-hover: #eaf1f8; --cb-card-line: #d3dde8;
    --cb-card-radius: 3px; --cb-cap: #f4f8fc; --cb-card-shadow: none; --cb-pop-bg: #ffffff; --cb-pop-line: #c9d6e3;
    --cb-pop-shadow: 0 6px 18px rgba(16, 42, 67, 0.10); --cb-pop-radius: 2px;
    /* The desk's dot grid, the same in every theme; each theme sets only the dots' ink. */
    --cb-desk-dot: #cbd3dd; --cb-desk-art: radial-gradient(circle, var(--cb-desk-dot) 0.9px, transparent 1.25px);
    --cb-desk-art-size: 14px 14px;
    /* Title stripes and view heads. */
    --cb-head: #f2f6fb; --cb-head-line: #dbe3ec; --cb-head-ink: #12324f; --cb-head-size: 12px; --cb-head-track: 0;
    --cb-head-pad: 5px; --cb-accent: #4a8fd4; --cb-accent-w: 2px; --cb-head-glint: transparent;
    --cb-view-head: #ffffff; --cb-view-head-line: #dbe3ec; --cb-view-head-line-w: 1px; --cb-view-head-ink: #12324f;
    --cb-group-head: #f2f6fb; --cb-group-head-line: #dbe3ec; --cb-box-line: #dbe3ec; --cb-head-rule: #d6e0ea;
    --cb-head-accent: #2f6fb3; --cb-head-accent-soft: #a9c6e4;
    /* Fields and buttons. */
    --cb-field-line: #d3dde8; --cb-field-line-hover: #a9c6e4; --cb-field-focus: #4a8fd4;
    --cb-field-ring: 0 0 0 2px rgba(74, 143, 212, 0.16); --cb-field-radius: 2px; --cb-field-bg: #fff;
    --cb-btn-line: #c5d3e2; --cb-btn-accent: #1f4e79; --cb-btn-accent-hover: #163a5c; --cb-btn-ink: #24405e;
    --cb-btn-minw: 0px;
    /* Faces and voices: a label, a control (buttons, tabs), a badge, a column head, a stat, a tag. */
    --cb-sans: 'IBM Plex Sans', sans-serif; --cb-mono: 'IBM Plex Mono', monospace; --cb-label-font: var(--cb-mono);
    --cb-label-size: 8.5px; --cb-label-weight: 400; --cb-label-case: uppercase; --cb-label-track: .07em;
    --cb-label-ink: #5b7089; --cb-label-w: 104px; --cb-ctl-font: var(--cb-mono); --cb-ctl-size: 9px;
    --cb-ctl-weight: 500; --cb-ctl-case: uppercase; --cb-ctl-track: .07em; --cb-ctl-radius: 2px;
    --cb-seg-size: 8.5px; --cb-seg-weight: 500; --cb-seg-weight-on: 500; --cb-badge-font: var(--cb-mono);
    --cb-badge-size: 8.5px; --cb-badge-weight: 500; --cb-badge-case: uppercase; --cb-badge-track: .05em;
    --cb-badge-radius: 2px; --cb-badge-pad: 1px 5px; --cb-th-font: var(--cb-mono); --cb-th-size: 8.5px;
    --cb-th-weight: 500; --cb-th-case: uppercase; --cb-th-track: .07em; --cb-th-ink: #4f6782;
    --cb-stat-font: var(--cb-mono); --cb-stat-size: 8.5px; --cb-stat-weight: 400; --cb-stat-case: uppercase;
    --cb-stat-track: .07em; --cb-stat-ink: #5b7089; --cb-unit-ink: #94a3b8; --cb-tag: #f1f5f9;
    --cb-tag-line: #dbe3ec; --cb-tag-ink: #4f6782; --cb-tag-font: var(--cb-mono); --cb-tag-size: 8px;
    --cb-tag-weight: 500; --cb-tag-case: uppercase; --cb-tag-track: .06em; --cb-tag-radius: 2px;
    /* The ink scale, a rule, what is chosen. */
    --cb-ink-1: #12324f; --cb-ink-2: #1e293b; --cb-ink-3: #5b7089; --cb-ink-4: #94a3b8; --cb-line: #e4eaf1;
    --cb-pick: #4a8fd4;
    /* States: the light, its ink, a badge's fill and edge; a pip's corners and motion. */
    --cb-ok-dot: #10b981; --cb-ok-ink: #0f6b4c; --cb-ok-bg: transparent; --cb-ok-line: #b5e3cd;
    --cb-run-dot: #3b82f6; --cb-run-ink: #1d4fa0; --cb-run-bg: #e8f1fb; --cb-run-line: #bcd6f3;
    --cb-que-dot: #60a5fa; --cb-que-ink: #3a78bf; --cb-que-bg: transparent; --cb-que-line: #a9c9ec;
    --cb-bad-dot: #ef4444; --cb-bad-ink: #a61b1b; --cb-bad-bg: #fdeeee; --cb-bad-line: #f5c2c2;
    --cb-warn-dot: #fbbf24; --cb-warn-ink: #8a4b06; --cb-warn-bg: #fdf3e3; --cb-warn-line: #f1d29a;
    --cb-off-dot: #c5d3e2; --cb-off-ink: #4f6782; --cb-off-bg: transparent; --cb-off-line: #c5d3e2;
    --cb-pip-radius: 1px; --cb-wait-dot: #dbe3ec; --cb-pip-halo: none; --cb-run-halo: none;
    --cb-run-blink: cb-blink 1.6s ease-in-out infinite;
    /* One box deep: a section's ring, fill and shadow; a fold's box; a group's; the species rail. */
    --cb-section-ring: transparent; --cb-section-fill: transparent; --cb-section-shadow: none;
    --cb-fold-line: transparent; --cb-group-ring: transparent; --cb-group-fill: transparent;
    --cb-srail-line: var(--cb-panel-line); --cb-srail-radius: 0; --cb-srail-margin: 0;
    /* The rail. */
    --cb-rail-line: #d3dde8; --cb-rail-on: #dbe7f3; --cb-rail-tick: none; --cb-avatar-radius: 2px;
    --cb-rail-btn-radius: 2px; --cb-rail-n-radius: 0; --cb-rail-n-bg: transparent; --cb-rail-n-ring: none;
    --cb-rail-n-ink: #5b7089; --cb-rail-n-font: var(--cb-mono); --cb-rail-n-weight: 500;
    /* The branch map. */
    --cb-bm-card-line: #d3dde8; --cb-bm-card-radius: 3px; --cb-bm-card-shadow: none;
    --cb-bm-card-hover: 0 0 0 1px #a9c6e4; --cb-bm-band: #f4f7fb; --cb-bm-hair: #e4eaf1; --cb-bm-port: #5b7089;
    --cb-bm-port-radius: 1px; --cb-bm-paper: #f6f8fb; --cb-bm-paper-dot: #d3dde8; --cb-bm-chosen: #edf4fb;
    --cb-bm-hover: #eaf1f8; --cb-bm-chip-radius: 2px; --cb-bm-chip-on: #edf4fb; --cb-bm-chip-on-line: #4a8fd4;
    --cb-bm-chip-on-ink: #12324f; --cb-bm-chip-on-soft: #5b7089; --cb-bm-menu-radius: 2px;
    --cb-bm-menu-shadow: 0 6px 18px rgba(16, 42, 67, 0.10); --cb-bm-cable: #a9b8c9; --cb-bm-cable-fan: #a9c6e4;
    --cb-bm-box-radius: 2px; --cb-bm-paper-radius: 2px;
    /* Corners. */
    --cb-r-lg: 3px; --cb-r-md: 2px; --cb-r-sm: 2px; --cb-r-pill: 2px; --cb-r-tile: 2px; --cb-img-radius: 1px;
    /* The ground under pictures (tomograms, slabs, cutouts): dark in every theme. */
    --cb-pane: #0f172a;
    /* Drawn things. */
    --cb-drawn: block; --cb-undrawn: none; --cb-prompt: #4a8fd4; --cb-ascii: #a9b8c9; --cb-ascii-hi: #4a8fd4;
    --cb-prompt-glyph: "~> "; --cb-log-note-style: normal; --cb-fig-n: inline-block;
    --cb-wait-dots: radial-gradient(circle, #94a3b8 0.75px, transparent 1.05px);
    --cb-wait-anim: cb-dither 2.4s steps(6) infinite;
"""


def _light_tokens() -> dict[str, str]:
    body = "\n".join(ln for ln in _LIGHT_TOKENS.splitlines() if not ln.strip().startswith("/*"))
    return {k.strip(): v.strip() for k, v in (d.split(":", 1) for d in body.split(";") if ":" in d)}


LIGHT_TOKENS = _light_tokens()


# ── Colour arithmetic (OKLCH), for the themes derived from the light values ──────────────────────────────────
def _lin(c: float) -> float:
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def _gam(c: float) -> float:
    return 12.92 * c if c <= 0.0031308 else 1.055 * c ** (1 / 2.4) - 0.055


def oklch(hex_colour: str) -> tuple[float, float, float]:
    """(lightness 0–1, chroma, hue °) of a `#rgb` / `#rrggbb` colour."""
    h = hex_colour.lstrip("#")[:6]
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r, g, b = (_lin(int(h[i : i + 2], 16) / 255) for i in (0, 2, 4))
    l_ = (0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b) ** (1 / 3)
    m_ = (0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b) ** (1 / 3)
    s_ = (0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b) ** (1 / 3)
    lightness = 0.2104542553 * l_ + 0.7936177850 * m_ - 0.0040720468 * s_
    a = 1.9779984951 * l_ - 2.4285922050 * m_ + 0.4505937099 * s_
    bb = 0.0259040371 * l_ + 0.7827717662 * m_ - 0.8086757660 * s_
    return lightness, math.hypot(a, bb), math.degrees(math.atan2(bb, a)) % 360


def _srgb_linear(lightness: float, chroma: float, hue: float) -> tuple[float, float, float]:
    a, b = chroma * math.cos(math.radians(hue)), chroma * math.sin(math.radians(hue))
    l_ = (lightness + 0.3963377774 * a + 0.2158037573 * b) ** 3
    m_ = (lightness - 0.1055613458 * a - 0.0638541728 * b) ** 3
    s_ = (lightness - 0.0894841775 * a - 1.2914855480 * b) ** 3
    return (
        4.0767416621 * l_ - 3.3077115913 * m_ + 0.2309699292 * s_,
        -1.2684380046 * l_ + 2.6097574011 * m_ - 0.3413193965 * s_,
        -0.0041960863 * l_ - 0.7034186147 * m_ + 1.7076147010 * s_,
    )


def from_oklch(lightness: float, chroma: float, hue: float) -> str:
    """The `#rrggbb` nearest an OKLCH colour, its chroma reduced until it is in sRGB."""
    lightness = min(max(lightness, 0.0), 1.0)
    for _ in range(40):
        if all(-1e-4 <= c <= 1 + 1e-4 for c in _srgb_linear(lightness, chroma, hue)):
            break
        chroma *= 0.93
    rgb = (min(max(_gam(min(max(c, 0.0), 1.0)), 0.0), 1.0) for c in _srgb_linear(lightness, chroma, hue))
    return "#" + "".join(f"{round(c * 255):02x}" for c in rgb)


def _is_hex(v: str) -> bool:
    return v.startswith("#") and len(v) in (4, 7) and all(c in "0123456789abcdefABCDEF" for c in v[1:])


# ── 3a. Dark ─────────────────────────────────────────────────────────────────────────────────────────────────
# A grey's place on the light ladder -> its place on the dark one: white grounds become the card, the darkest ink
# the lightest text, and the order between them kept (OKLCH lightness, light -> dark).
_DARK_LADDER = (
    (0.000, 0.950), (0.208, 0.905), (0.279, 0.860), (0.372, 0.795), (0.446, 0.735), (0.554, 0.655),
    (0.711, 0.525), (0.869, 0.370), (0.929, 0.305), (0.968, 0.262), (0.985, 0.240), (1.000, 0.222),
)  # fmt: skip


def _ladder(lightness: float) -> float:
    for (x0, y0), (x1, y1) in itertools.pairwise(_DARK_LADDER):
        if x0 <= lightness <= x1:
            return y0 + (y1 - y0) * (lightness - x0) / (x1 - x0)
    return _DARK_LADDER[-1][1]


def dark_of(hex_colour: str) -> str:
    """A light theme's colour in the dark theme, by what such a colour is used for."""
    lightness, chroma, hue = oklch(hex_colour)
    if chroma < 0.07:  # a grey (Harbor's are faintly blue): along the ladder, in the house's cool hue
        return from_oklch(_ladder(lightness), min(max(chroma * 0.8, 0.012), 0.032), hue if chroma > 0.008 else 255.0)
    if lightness >= 0.86:  # a pale tint (a badge's or a warning's ground): a dim tint of the same hue
        return from_oklch(0.255 + (1 - lightness) * 0.75, min(chroma * 0.55 + 0.012, 0.06), hue)
    if lightness <= 0.58:  # a deep ink (text in a hue): the light end of the same hue
        return from_oklch(0.80 + (0.58 - lightness) * 0.2, min(chroma, 0.15), hue)
    return from_oklch(max(lightness, 0.68), chroma, hue)  # a signal colour (a dot, a bar): itself, lifted a little


# Tailwind's colours, for the utility classes older code still uses (`text-gray-500`, `bg-white`): Tailwind 4
# reads them from `--color-<family>-<step>`, so a theme redefines those.
_TW_STEPS = ("50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950")
_TW = {
    "slate": "f8fafc f1f5f9 e2e8f0 cbd5e1 94a3b8 64748b 475569 334155 1e293b 0f172a 020617",
    "gray": "f9fafb f3f4f6 e5e7eb d1d5db 9ca3af 6b7280 4b5563 374151 1f2937 111827 030712",
    "red": "fef2f2 fee2e2 fecaca fca5a5 f87171 ef4444 dc2626 b91c1c 991b1b 7f1d1d 450a0a",
    "orange": "fff7ed ffedd5 fed7aa fdba74 fb923c f97316 ea580c c2410c 9a3412 7c2d12 431407",
    "amber": "fffbeb fef3c7 fde68a fcd34d fbbf24 f59e0b d97706 b45309 92400e 78350f 451a03",
    "green": "f0fdf4 dcfce7 bbf7d0 86efac 4ade80 22c55e 16a34a 15803d 166534 14532d 052e16",
    "emerald": "ecfdf5 d1fae5 a7f3d0 6ee7b7 34d399 10b981 059669 047857 065f46 064e3b 022c22",
    "blue": "eff6ff dbeafe bfdbfe 93c5fd 60a5fa 3b82f6 2563eb 1d4ed8 1e40af 1e3a8a 172554",
    "indigo": "eef2ff e0e7ff c7d2fe a5b4fc 818cf8 6366f1 4f46e5 4338ca 3730a3 312e81 1e1b4b",
}
TAILWIND = {
    f"--color-{fam}-{step}": f"#{h}" for fam, row in _TW.items() for step, h in zip(_TW_STEPS, row.split(), strict=True)
} | {"--color-white": "#ffffff", "--color-black": "#000000"}

# What the ladder gets wrong, or what has no light value to derive from.
_DARK_SET = {
    # Grounds: the desk darkest, the cards a step up, a pop-up a step further; the rail with the desk.
    "--cb-desk": "#0e1116", "--cb-desk-dot": "#232931", "--cb-page": "#161a20", "--cb-surface": "#161a20",
    "--cb-panel": "#161a20",
    "--cb-view-head": "#161a20", "--cb-rail": "#11151a", "--cb-rail-line": "#232a33", "--cb-rail-on": "#222b36",
    "--cb-panel-line": "#262d36", "--cb-card-line": "#2c343e", "--cb-edge": "#2c343e", "--cb-hover": "#1e252e",
    "--cb-cap": "#1b2027", "--cb-pop-bg": "#1d232b", "--cb-pop-line": "#333c47",
    "--cb-pop-shadow": "0 8px 22px rgba(0, 0, 0, 0.45)", "--cb-bm-menu-shadow": "0 8px 22px rgba(0, 0, 0, 0.45)",
    "--cb-head": "#1a1f26", "--cb-head-line": "#2a313b", "--cb-group-head": "#1a1f26", "--cb-head-ink": "#d9e3f0",
    "--cb-view-head-ink": "#d9e3f0", "--cb-view-head-line": "#262d36", "--cb-box-line": "#2a313b",
    "--cb-accent": "#5b9be0", "--cb-head-accent": "#6aa6e6", "--cb-pick": "#6aa6e6", "--cb-prompt": "#6aa6e6",
    "--cb-ascii-hi": "#6aa6e6", "--cb-bm-chip-on-line": "#5b9be0",
    # Fields and buttons: a field a step below its card; the one action a lighter navy.
    "--cb-field-bg": "#11151a", "--cb-field-line": "#323b46", "--cb-field-line-hover": "#4d5e72",
    "--cb-field-focus": "#5b9be0", "--cb-field-ring": "0 0 0 2px rgba(91, 155, 224, 0.22)",
    "--cb-btn-line": "#3a4451", "--cb-btn-ink": "#c9d5e4", "--cb-btn-accent": "#2b62a0",
    "--cb-btn-accent-hover": "#3572b5",
    # Ink: the house's navy becomes a pale blue-grey; muted ink stays readable on the dark card.
    "--cb-ink-1": "#dde6f2", "--cb-ink-2": "#c9d3e0", "--cb-ink-3": "#8d9bae", "--cb-ink-4": "#677488",
    "--cb-line": "#252c35", "--cb-label-ink": "#8796a9", "--cb-stat-ink": "#8796a9", "--cb-th-ink": "#8a99ad",
    "--cb-unit-ink": "#677488", "--cb-tag": "#1e242c", "--cb-tag-line": "#2f3741", "--cb-tag-ink": "#97a5b8",
    # States: the lights as they are, a little lifted; inks pale; grounds dim tints.
    "--cb-ok-dot": "#22c08b", "--cb-ok-ink": "#7fd8b4", "--cb-ok-line": "#24563f",
    "--cb-run-dot": "#4f93f8", "--cb-run-ink": "#a8c8fb", "--cb-run-bg": "#1a2a40", "--cb-run-line": "#2b4a72",
    "--cb-que-dot": "#6aabf9", "--cb-que-ink": "#93bff0", "--cb-que-line": "#2f4e74",
    "--cb-bad-dot": "#f05252", "--cb-bad-ink": "#f6a7a7", "--cb-bad-bg": "#3a1f22", "--cb-bad-line": "#6b2c31",
    "--cb-warn-dot": "#f5bd3a", "--cb-warn-ink": "#f0c487", "--cb-warn-bg": "#33270f", "--cb-warn-line": "#6a5021",
    "--cb-off-dot": "#4a5563", "--cb-off-ink": "#97a5b8", "--cb-off-line": "#3a4451", "--cb-wait-dot": "#2c343e",
    # The branch map's paper, a step below its cards.
    "--cb-bm-paper": "#12161b", "--cb-bm-paper-dot": "#262d36", "--cb-bm-band": "#181d24",
    "--cb-bm-card-line": "#2c343e", "--cb-bm-card-hover": "0 0 0 1px #4d5e72", "--cb-bm-hair": "#252c35",
    "--cb-bm-chosen": "#1c2633", "--cb-bm-chip-on": "#1c2633", "--cb-bm-hover": "#1e252e",
    "--cb-bm-cable": "#4b5866", "--cb-bm-cable-fan": "#4d6a8c", "--cb-bm-chip-on-ink": "#dde6f2",
    "--cb-wait-dots": "radial-gradient(circle, #5b6676 0.75px, transparent 1.05px)",
    # Hooks a component reads with its light value as the fallback, set here only.
    "--cb-well": "#11151a", "--cb-plot-grid": "#232a33", "--cb-plot-axis": "#3a4451", "--cb-rule": "#323b46",
    "--cb-pill-line": "#323b46", "--cb-gallery-bar": "#2c343e", "--cb-mag-skip": "#3a4451",
    "--cb-glyph-on": "#1c2633", "--cb-bm-open": "#2e2712", "--cb-check-on": "#9fb3cc", "--cb-check-on-ink": "#0e1116",
    "--cb-check-ink": "#c9d3e0", "--cb-readout-head": "#c9d3e0", "--cb-readout-soft": "#8d9bae",
    "--cb-plan-on": "#1c2633", "--cb-plan-off-bg": "#1a1f26", "--cb-plan-ok-bg": "#16281f",
    "--cb-plan-bad-bg": "#3a1f22",
    "--cb-plan-ok": "#7fd8b4", "--cb-plan-bad": "#f6a7a7", "--cb-plan-warn": "#f0c487", "--cb-plan-dot": "#4a5563",
    "--cb-plot-read": "#c9d3e0", "--cb-plot-read-ink": "#0e1116",
    # The same in every theme: the ground under pictures.
    "--cb-pane": "#0f172a",
    # Quasar's own dark grounds (its cards, menus and fields in dark mode).
    "--q-dark": "#161a20", "--q-dark-page": "#0e1116",
}  # fmt: skip


def _derived(values: dict[str, str], light: dict[str, str], fn) -> dict[str, str]:
    return {k: fn(v) for k, v in light.items() if _is_hex(v) and k not in values}


def _dark_values() -> dict[str, str]:
    out = {f"--cb-{k}": dark_of(v) for k, v in PALETTE.items()}
    out |= {k: dark_of(v) for k, v in LIGHT_TOKENS.items() if _is_hex(v)}
    out |= {k: dark_of(v) for k, v in TAILWIND.items()}
    return out | _DARK_SET


# ── 3b. Papers: light themes on a softer ground ──────────────────────────────────────────────────────────────
def papered(hex_colour: str, card: tuple[float, float, float], desk_l: float | None = None) -> str | None:
    """A light-theme grey moved onto a paper (`card`, OKLCH): the near-whites and the lines keep their distance
    from white, measured from the paper, in its hue. Hues and inks stay as they are (None)."""
    lightness, chroma, _ = oklch(hex_colour)
    if chroma >= 0.07 or lightness < 0.74:
        return None
    c_l, c_c, c_h = card
    return from_oklch(c_l - (1.0 - lightness) * 1.05, c_c + min(chroma, 0.02) * 0.4, c_h)


@dataclass(frozen=True)
class Paper:
    card: tuple[float, float, float]  # OKLCH of the cards (the views, the roster, dialogs)
    desk: tuple[float, float, float]  # OKLCH of the desk between them


def _paper_values(p: Paper) -> dict[str, str]:
    card = from_oklch(*p.card)
    desk = from_oklch(*p.desk)
    out = {}
    for k, v in PALETTE.items():
        if (moved := papered(v, p.card)) is not None:
            out[f"--cb-{k}"] = moved
    for k, v in (LIGHT_TOKENS | TAILWIND).items():
        if _is_hex(v) and (moved := papered(v, p.card)) is not None:
            out[k] = moved
    rail = from_oklch((p.card[0] + p.desk[0]) / 2, p.desk[1], p.desk[2])
    return out | {
        "--cb-page": card, "--cb-surface": card, "--cb-panel": card, "--cb-view-head": card, "--cb-pop-bg": card,
        "--cb-field-bg": from_oklch(min(p.card[0] + 0.012, 1.0), p.card[1] * 0.6, p.card[2]),
        "--cb-desk": desk, "--cb-rail": rail, "--cb-bm-paper": from_oklch(p.card[0] - 0.02, p.card[1], p.card[2]),
        "--cb-white": card, "--color-white": card,
    }  # fmt: skip


# ── The themes ───────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Theme:
    name: str
    label: str
    note: str
    dark: bool = False
    values: dict[str, str] = field(default_factory=dict)


def _paper(name: str, label: str, note: str, paper: Paper) -> Theme:
    return Theme(name, label, note, values=_paper_values(paper))


THEMES: dict[str, Theme] = {
    t.name: t
    for t in (
        Theme("light", "Light", "White cards on a pale blue-grey desk."),
        Theme("dark", "Dark", "Charcoal cards on a near-black desk, pale ink, the same states.", True, _dark_values()),
        _paper("mist", "Mist", "Cool grey cards, a deeper blue-grey desk: Harbor, a notch dimmer.",
               Paper(card=(0.955, 0.006, 250.0), desk=(0.885, 0.012, 250.0))),
        _paper("sage", "Sage", "A faint green-grey: calm, low contrast, far from white.",
               Paper(card=(0.962, 0.012, 145.0), desk=(0.902, 0.016, 150.0))),
        _paper("linen", "Linen", "Warm off-white cards on a sand desk; the inks stay slate.",
               Paper(card=(0.972, 0.010, 85.0), desk=(0.915, 0.016, 80.0))),
    )
}  # fmt: skip
# The papers, listed under Light and Dark wherever a theme is picked.
PAPERS = tuple(n for n, t in THEMES.items() if n != "light" and not t.dark)
DEFAULT_THEME = "light"


def theme_of(name: str | None) -> Theme:
    return THEMES.get(name or DEFAULT_THEME, THEMES[DEFAULT_THEME])


# ── The stylesheet ───────────────────────────────────────────────────────────────────────────────────────────
def _decls(values: dict[str, str]) -> str:
    return "".join(f"{k}: {v}; " for k, v in values.items())


# Tokens whose light value names another token are computed where they are declared, so a theme on an element
# below the root declares them again to take its own values.
_REFERRING = {k: v for k, v in LIGHT_TOKENS.items() if "var(" in v}

# What every theme other than light needs besides values: Quasar's light components draw white grounds of their
# own.
_THEMED_RULES = """
[data-cb-theme]:not([data-cb-theme="light"]) :is(.q-card, .q-menu, .q-table__container, .q-table__card,
    .q-list--bordered, .q-expansion-item__content, .q-dialog .q-card) { background: var(--cb-surface); }
[data-cb-theme]:not([data-cb-theme="light"]) :is(.q-card, .q-menu, .q-item, .q-table, .q-field__native,
    .q-field__input, .q-select__dropdown-icon) { color: var(--cb-ink-2); }
[data-cb-theme]:not([data-cb-theme="light"]) .q-separator { background: var(--cb-line); }
"""

# Dark only: the charts (Plotly draws its chrome with inline fills; a stylesheet's !important wins over those),
# the native controls and scrollbars, and pictures, which keep their own colours.
_DARK_RULES = """
[data-cb-theme="dark"] { color-scheme: dark; }
[data-cb-theme="dark"] .js-plotly-plot .main-svg { background: transparent !important; }
[data-cb-theme="dark"] .js-plotly-plot .bg { fill: transparent !important; }
[data-cb-theme="dark"] .js-plotly-plot :is(.xtick, .ytick, .g-xtitle, .g-ytitle, .gtitle, .legendtext,
    .annotation-text, .colorbar .cbtitle, .cbaxis) text { fill: var(--cb-ink-3) !important; }
[data-cb-theme="dark"] .js-plotly-plot :is(.gridlayer, .zerolinelayer) path { stroke: var(--cb-plot-grid) !important; }
[data-cb-theme="dark"] .js-plotly-plot :is(.xlines-above, .ylines-above, .xlines-below, .ylines-below) {
    stroke: var(--cb-plot-axis) !important; }
[data-cb-theme="dark"] .js-plotly-plot .legend .bg { fill: transparent !important; }
[data-cb-theme="dark"] ::selection { background: rgba(91, 155, 224, 0.35); }
"""


@functools.cache
def _block(name: str) -> str:
    t = THEMES[name]
    if name == DEFAULT_THEME:
        light = {f"--cb-{k}": v for k, v in PALETTE.items()}
        return f':root, [data-cb-theme="light"] {{ {_decls(light)}\n{_LIGHT_TOKENS}}}\n'
    return f':root[data-cb-theme="{name}"], [data-cb-theme="{name}"] {{ {_decls(_REFERRING | t.values)}}}\n'


def theme_css(names: Iterable[str] = ()) -> str:
    """The light values on the root, the named themes' under their attribute (all of them when none are named),
    and the rules the other themes need."""
    wanted = [n for n in (names or THEMES) if n in THEMES and n != DEFAULT_THEME]
    return _block(DEFAULT_THEME) + "".join(_block(n) for n in wanted) + _THEMED_RULES + _DARK_RULES


def theme_block(name: str) -> str:
    """One theme's values alone, for a page that switches to a theme it was not opened in."""
    return _block(theme_of(name).name)


# ── Putting a theme on a page ────────────────────────────────────────────────────────────────────────────────
def page_theme(client: Client, setting: str | None) -> str:
    """The theme a page opens in: `?theme=<name>` in its address, else the user's setting."""
    asked = client.request.query_params.get("theme") if client.request is not None else None
    return theme_of(asked if asked in THEMES else setting).name


async def theme_head() -> str:
    """Put the page's theme on it before its first paint, and return its name. Call it in a page function before
    `client.connected()`, while the page's HTML is still being written: the attribute and the theme's grounds go
    into its head, the full stylesheet follows with `ensure_assets_loaded()` once it connects."""
    client = context.client
    settings = await asyncio.to_thread(load_user_settings)
    name = page_theme(client, settings.theme)
    client._cb_theme = name  # read by ensure_assets_loaded
    if name == DEFAULT_THEME:
        return name
    t = THEMES[name]
    ground = t.values.get("--cb-desk") or t.values.get("--cb-page", "")
    scheme = "color-scheme: dark; " if t.dark else ""
    ui.add_head_html(
        f"<script>document.documentElement.setAttribute('data-cb-theme', {json.dumps(name)});</script>"
        f"<style>:root[data-cb-theme], :root[data-cb-theme] body {{ {scheme}background: {ground}; }}</style>"
    )
    return name


def apply_theme_js(name: str | None) -> str:
    """JavaScript that puts a theme on the open page (Quasar's dark mode with it). Its values must be on the page
    already (`theme_css`, `theme_block`)."""
    t = theme_of(name)
    return (
        "(() => { const el = document.documentElement;"
        f" const t = {json.dumps(t.name)};"
        " if (t === 'light') el.removeAttribute('data-cb-theme'); else el.setAttribute('data-cb-theme', t);"
        f" if (window.setDark) setDark({'true' if t.dark else 'false'}); }})();"
    )


def switch_theme(name: str) -> None:
    """Switch the current page to a theme now, its values sent first if the page has not got them."""
    client = context.client
    sent = getattr(client, "_cb_themes_sent", set())
    t = theme_of(name).name
    if t not in sent:
        ui.add_head_html(f"<style>{theme_block(t)}</style>")
        client._cb_themes_sent = sent | {t}
    client._cb_theme = t
    client.run_javascript(apply_theme_js(t))
