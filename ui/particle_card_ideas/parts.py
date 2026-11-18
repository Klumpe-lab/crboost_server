"""The particle-card prototypes' pieces: the type rule (words, numbers, labels), the look the page is drawn in (the
particle accent and the class palette, both switchable), the particle card's skeleton (the tilt-series card's header,
ports and edges, with a slight accent) and the bars that say where particles came from.

Every colour is a `--cb-*` variable with today's value as its fallback; the page sets the accent it shows as
`--pci-acc*` on its root, so every piece below reads one accent whichever candidate is picked.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from nicegui import ui

from ui.branch_map.style import (
    BAND,
    BODY,
    CARD_BORDER,
    FONT,
    HAIR,
    INK,
    INNER_R,
    MONO_FONT,
    MUTED,
    SOFT,
    STATUS,
    hrow,
    label,
)
from ui.dashboard.figures import class_color as today_class_color
from ui.job_plugins._field_styles import voice

# The particle accent, read by every piece; the page sets it from the candidate picked (`ACCENTS`).
PART = "var(--pci-acc)"
PART_TINT = "var(--pci-acc-tint)"
PART_LINE = "var(--pci-acc-line)"
# The candidates: a muted complement of the theme's slate blue, each its accent, the tint behind a chip, a hairline.
ACCENTS: dict[str, tuple[str, str, str]] = {
    "ochre": (
        "var(--cb-particle-ochre, #9a7631)",
        "var(--cb-particle-ochre-tint, #f6f1e6)",
        "var(--cb-particle-ochre-line, #e1d3b2)",
    ),
    "verdigris": (
        "var(--cb-particle-verdigris, #3f7a6e)",
        "var(--cb-particle-verdigris-tint, #eef4f2)",
        "var(--cb-particle-verdigris-line, #c6dad4)",
    ),
    "graphite": (
        "var(--cb-particle-graphite, #46566a)",
        "var(--cb-particle-graphite-tint, #f1f3f6)",
        "var(--cb-particle-graphite-line, #cfd6df)",
    ),
}
# A muted, industrial class palette (steel blue, rust, verdigris, ochre, olive, oxide red, slate, sand), proposed in
# place of today's saturated one (ui/dashboard/figures.py CLASS_PALETTE).
MUTED_CLASSES = (
    "var(--cb-class-1, #4f7aa8)",
    "var(--cb-class-2, #b0643a)",
    "var(--cb-class-3, #3f8a78)",
    "var(--cb-class-4, #b08a2e)",
    "var(--cb-class-5, #6f7f3a)",
    "var(--cb-class-6, #a14d4a)",
    "var(--cb-class-7, #5f6f86)",
    "var(--cb-class-8, #a8916a)",
)
GRID = "var(--cb-plot-grid, #eef2f6)"
WHITE = "var(--cb-surface, #ffffff)"
PIC_BG = "var(--cb-bm-pic, #12324f)"
WARN = "var(--cb-warn-ink, #8a4b06)"
HEAD_H = 26
SMALL_W = 236


@dataclass(frozen=True)
class Look:
    """What the page was asked to draw in: the accent's name and the class palette's."""

    accent: str = "ochre"
    classes: str = "muted"

    def class_color(self, k: int) -> str:
        if self.classes == "today":
            return today_class_color(k)
        return MUTED_CLASSES[(int(k) - 1) % len(MUTED_CLASSES)]

    def root_style(self) -> str:
        acc, tint, line = ACCENTS.get(self.accent, ACCENTS["ochre"])
        return f"--pci-acc: {acc}; --pci-acc-tint: {tint}; --pci-acc-line: {line};"


# Page-scoped rules: hover and selection as hairline rings, the picture swap.
CSS = f"""
.pci-tile {{ cursor: pointer; transition: box-shadow .1s; }}
.pci-tile:hover {{ box-shadow: 0 0 0 1px {SOFT}; }}
.pci-tile.pci-sel {{ box-shadow: 0 0 0 2px var(--pci-k); }}
.pci-tile .pci-alt {{ opacity: 0; transition: opacity .1s; }}
.pci-tile:hover .pci-alt {{ opacity: 1; }}
.pci-tile:hover .pci-main-word {{ visibility: hidden; }}
.pci-tile:hover .pci-alt-word {{ visibility: visible; }}
.pci-alt-word {{ visibility: hidden; }}
.pci-row {{ cursor: pointer; }}
.pci-row:hover {{ background: var(--cb-hover, #eaf1f8); }}
.pci-row.pci-sel {{ background: var(--cb-bm-chosen, #edf4fb); box-shadow: inset 2px 0 0 var(--pci-k); }}
.pci-mini {{ cursor: pointer; }}
.pci-mini:hover {{ outline: 1px solid {SOFT}; }}
.pci-col:hover {{ fill: {INK}; }}
"""


# ── the type rule ────────────────────────────────────────────────────────────
# Words in the sans face (≥ 10 px, captions 9.5), numbers and ids in the mono face with even figures, labels in the
# label voice (the theme's: mono capitals in Harbor Mono).


def words(content: str, size: float = 10.5, ink: str = BODY, *, bold: bool = False, extra: str = "") -> ui.label:
    """A name, a caption, a sentence: the sans face."""
    return ui.label(content).style(
        f"{FONT} font-size: {max(size, 9.5)}px; color: {ink}; font-weight: {600 if bold else 400}; "
        f"line-height: 1.35; {extra}"
    )


def num(content: str, size: float = 10.5, ink: str = INK, *, bold: bool = False, extra: str = "") -> ui.label:
    """A number, an id, a count, a value with its unit: the mono face, even figures."""
    return ui.label(content).style(
        f"{MONO_FONT} font-variant-numeric: tabular-nums; font-size: {max(size, 9)}px; color: {ink}; "
        f"font-weight: {600 if bold else 400}; line-height: 1.3; white-space: nowrap; {extra}"
    )


def lab(content: str, extra: str = "") -> ui.label:
    """A field's, a column's or a block's name: the label voice."""
    return label(content, 8.5, SOFT, extra=extra)


# ── bars ─────────────────────────────────────────────────────────────────────


def project_shade(i: int) -> str:
    """The i-th project's grey: a ramp of the slate ink, so projects never read as classes."""
    pct = [74, 56, 42, 31, 23, 17, 12, 8][i % 8]
    return f"color-mix(in srgb, var(--cb-ink-1, #12324f) {pct}%, {WHITE})"


def project_ink(i: int) -> str:
    """Text that reads on `project_shade(i)`."""
    return WHITE if i % 8 < 3 else INK


def project_bar(projects: dict[str, int], order: list[str], w: int, h: int = 6, *, caption: bool = False) -> None:
    """Where particles came from, one segment per project in the page's project order, named on hover; `caption`
    writes the two largest under it."""
    total = sum(projects.values()) or 1
    with ui.element("div").style(f"width: {w}px;"):
        with ui.element("div").style(
            f"display: flex; width: {w}px; height: {h}px; overflow: hidden; background: {GRID};"
        ):
            for name in order:
                n = projects.get(name, 0)
                if not n:
                    continue
                seg = ui.element("div").style(
                    f"width: {n / total * 100:.2f}%; height: 100%; background: {project_shade(order.index(name))}; "
                    f"box-shadow: inset -1px 0 0 {WHITE};"
                )
                seg.props(f'title="{name}: {n:,} ({n / total:.0%})"')
        if caption:
            top = sorted(projects.items(), key=lambda kv: -kv[1])[:2]
            words(
                " · ".join(f"{k} {v / total:.0%}" for k, v in top),
                9.5,
                SOFT,
                extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-top: 2px;",
            )


def share_bar(share: float, color: str, w: int, h: int = 3) -> None:
    with ui.element("div").style(f"width: {w}px; height: {h}px; background: {GRID};"):
        ui.element("div").style(f"width: {share * 100:.1f}%; height: 100%; background: {color};")


# ── the card ─────────────────────────────────────────────────────────────────


def light(status: str, size: int = 7) -> None:
    ui.element("div").style(
        f"width: {size}px; height: {size}px; border-radius: var(--cb-pip-radius, 1px); flex-shrink: 0; "
        f"background: {STATUS.get(status, MUTED)};"
    )


def card(w: int, *, particle: bool) -> ui.element:
    """The card's box, flat as the theme draws cards: a particle job has the accent down its left edge."""
    edge = f"box-shadow: inset 2px 0 0 {PART};" if particle else ""
    return ui.element("div").style(
        f"position: relative; width: {w}px; box-sizing: border-box; border: 1px solid {CARD_BORDER}; "
        f"border-radius: var(--cb-bm-card-radius, 3px); background: {WHITE}; display: flex; flex-direction: column; "
        f"{edge} flex-shrink: 0;"
    )


def head(title: str, number: str, status: str, method: str, *, particle: bool) -> None:
    """The header band at the tilt-series card's height and in its band colour: the light, the name, the tool, the
    number; a particle job's number carries the accent."""
    with hrow(
        7,
        f"height: {HEAD_H}px; padding: 0 10px 0 14px; background: {BAND}; border-bottom: 1px solid {HAIR}; "
        f"border-radius: {INNER_R} {INNER_R} 0 0; box-sizing: border-box;",
    ):
        light(status)
        words(title, 12, INK, bold=True, extra="white-space: nowrap; letter-spacing: -0.01em;")
        if method:
            ui.label(method).style(
                voice("badge", font="'IBM Plex Sans', sans-serif", size="9px", weight=500, ink=SOFT)
                + f" border: 1px solid {HAIR}; border-radius: var(--cb-badge-radius, 2px); padding: 0 4px; "
                f"line-height: 13px; background: {WHITE}; white-space: nowrap;"
            )
        ui.element("div").style("flex: 1 1 0;")
        num(number, 10, PART if particle else MUTED)


def input_row(lbl: str, value: str, *, particle_port: bool, y: float) -> None:
    """An input row and its port on the card's left edge: a square for tilt series, tomograms and maps, a disc in
    the accent for a particle set. The value is words; an id inside it stays in the sentence."""
    with hrow(6, "height: 17px; padding: 0 10px 0 28px;"):
        lab(lbl, "width: 64px; flex-shrink: 0;")
        words(value, 10, BODY, extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1 1 0;")
    radius = "50%" if particle_port else "1px"
    ring = PART if particle_port else "var(--cb-bm-port, #5b7089)"
    ui.element("div").style(
        f"position: absolute; left: -5px; top: {y - 5}px; width: 10px; height: 10px; box-sizing: border-box; "
        f"border-radius: {radius}; background: {ring}; border: 1.5px solid {WHITE}; box-shadow: 0 0 0 1px {ring};"
    )


def out_port(h_center: float, w: int, *, particle: bool) -> None:
    ring = PART if particle else "var(--cb-bm-port, #5b7089)"
    ui.element("div").style(
        f"position: absolute; left: {w - 5}px; top: {h_center - 5}px; width: 10px; height: 10px; "
        f"box-sizing: border-box; border-radius: {'50%' if particle else '1px'}; background: {WHITE}; "
        f"border: 1.5px solid {ring};"
    )


def magazine(n: int, w: int, *, done: int) -> None:
    """Today's escort of a tilt-series job, for comparison: one square per tilt series."""
    side = max(2, min(8, int((w + 2) / max(1, n)) - 2))
    per = max(1, (w + 2) // (side + 2))
    rows = (n + per - 1) // per
    parts = []
    for i in range(n):
        x, y = (i % per) * (side + 2), (i // per) * (side + 2)
        fill = STATUS["done"] if i < done else "var(--cb-wait-dot, #dbe3ec)"
        parts.append(f'<rect x="{x}" y="{y}" width="{side}" height="{side}" rx="1" style="fill:{fill}"/>')
    ui.html(
        f'<svg width="{w}" height="{rows * (side + 2)}" style="display:block">{"".join(parts)}</svg>', sanitize=False
    ).style("line-height: 0; margin-bottom: 6px;")


def section(title: str, w: int) -> ui.element:
    """A titled block inside a large card: its stripe and its content, one box deep."""
    box = ui.element("div").style(
        f"width: {w}px; padding: 8px 14px 10px; box-sizing: border-box; border-top: 1px solid {HAIR};"
    )
    with box:
        lab(title, "margin-bottom: 6px;")
    return box


def img(src: str | None, px: int, *, cls: str = "", extra: str = "") -> ui.element:
    el = ui.element("img").style(f"width: {px}px; height: {px}px; display: block; background: {PIC_BG}; {extra}")
    if cls:
        el.classes(cls)
    if src:
        el.props(f'src="{src}"')
    return el


ColorOf = Callable[[int], str]
