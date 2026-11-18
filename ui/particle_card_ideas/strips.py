"""The set strip: what sits above a particle job's card in place of the tilt-series magazine. Six ways to say which set
the job reads, how many particles it holds and where they came from.

S1 Ledger: one line (id, name, particles), a project bar with the projects named inside their segments.
S2 Tomograms: one thin column per tomogram, height its particles, grouped by project: the magazine's analogue.
S3 Chain: the steps back to the picks, each with the particles it held.
S4 Ruler: this set as a share of what it was cut from.
S5 Status line: one terminal-like line, every number in a column of its own.
S6 Tally: particles per project as a small table.

Every strip takes the set as `ui.particle_card_ideas.data.set_info` gives it (or None: the job reads no set), the
page's project order and the width it has.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nicegui import ui

from ui.branch_map.style import BAND, BODY, HAIR, INK, MUTED, SOFT, hrow
from ui.particle_card_ideas.data import project_of, short_project
from ui.particle_card_ideas.parts import (
    GRID,
    PART,
    PART_LINE,
    PART_TINT,
    WHITE,
    lab,
    num,
    project_ink,
    project_shade,
    words,
)

Set = dict[str, Any] | None
LARGE = 400  # from this width a strip says more


def _none(w: int) -> bool:
    words("reads no particle set", 9.5, MUTED, extra=f"width: {w}px;")
    return True


def _ledger(s: dict[str, Any], w: int, *, tomos: bool = False) -> None:
    """The line every strip but S5 starts with: the set's id, its name, its particles (and tomograms)."""
    with hrow(6, f"height: 16px; width: {w}px; align-items: baseline;"):
        num(s["short"], 10, PART, bold=True)
        words(
            s["name"], 10, BODY, extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0;"
        )
        ui.element("div").style("flex: 1 1 0;")
        num(f"{s['n']:,}", 10.5, INK, bold=True)
        lab("ptcl")
        if tomos:
            num(f"{s['n_tomos']}", 10.5, INK, extra="margin-left: 4px;")
            lab("tomo")


# ── S1 · ledger ──────────────────────────────────────────────────────────────


def _named_bar(s: dict[str, Any], order: list[str], w: int) -> None:
    """The projects as one bar, each segment in its project's grey and named inside where the name fits."""
    total = s["n"] or 1
    with ui.element("div").style(f"display: flex; width: {w}px; height: 14px; overflow: hidden; background: {GRID};"):
        for name in order:
            n = s["projects"].get(name, 0)
            if not n:
                continue
            i = order.index(name)
            seg = ui.element("div").style(
                f"width: {n / total * 100:.2f}%; height: 100%; background: {project_shade(i)}; "
                f"box-shadow: inset -1px 0 0 {WHITE}; overflow: hidden; display: flex; align-items: center;"
            )
            seg.props(f'title="{name}: {n:,} ({n / total:.0%})"')
            if n / total * w >= 6.2 * len(name) + 8:
                with seg:
                    words(name, 9.5, project_ink(i), extra="padding-left: 4px; white-space: nowrap;")


def s1_ledger(s: Set, order: list[str], w: int) -> None:
    if s is None and _none(w):
        return
    with ui.element("div").style(f"width: {w}px; display: flex; flex-direction: column; gap: 3px;"):
        _ledger(s, w)
        _named_bar(s, order, w)


# ── S2 · tomograms ───────────────────────────────────────────────────────────


def s2_tomograms(s: Set, order: list[str], w: int) -> None:
    """One column per tomogram, its height the square root of its particles (one crowded tomogram would flatten
    the rest), grouped by project (largest first inside one), a gap between projects; hover a column for its
    tomogram. The tilt-series magazine's analogue: a mark per thing the job
    ran over."""
    if s is None and _none(w):
        return
    h = 26 if w >= LARGE else 18
    groups: list[tuple[str, list[tuple[str, int]]]] = []
    by_project: dict[str, list[tuple[str, int]]] = {}
    for t, n in s["per_tomo"].items():
        by_project.setdefault(short_project(project_of(t)), []).append((t, n))
    for name in order:
        if name in by_project:
            groups.append((name, sorted(by_project[name], key=lambda tn: -tn[1])))
    gap = 3
    n_cols = sum(len(g) for _, g in groups)
    col_w = max(0.5, (w - gap * (len(groups) - 1)) / max(1, n_cols))
    hi = max(s["per_tomo"].values()) if s["per_tomo"] else 1
    parts, labels = [], []
    x = 0.0
    for name, tomos in groups:
        x0 = x
        for t, n in tomos:
            ch = max(1.0, (n / hi) ** 0.5 * h)
            cw = max(0.5, col_w - (0.4 if col_w > 2 else 0))
            parts.append(
                f'<rect class="pci-col" x="{x:.2f}" y="{h - ch:.2f}" width="{cw:.2f}" '
                f'height="{ch:.2f}" style="fill:{SOFT}"><title>{t} · {n:,}</title></rect>'
            )
            x += col_w
        parts.append(
            f'<line x1="{x0:.1f}" x2="{x:.1f}" y1="{h + 1.5}" y2="{h + 1.5}" style="stroke:{PART};stroke-width:1.5"/>'
        )
        labels.append((name, x0, x - x0, sum(n for _, n in tomos), len(tomos)))
        x += gap
    with ui.element("div").style(f"width: {w}px; display: flex; flex-direction: column; gap: 3px;"):
        _ledger(s, w, tomos=True)
        ui.html(
            f'<svg width="{w}" height="{h + 3}" style="display:block">{"".join(parts)}</svg>', sanitize=False
        ).style("line-height: 0;")
        if w >= LARGE:
            with ui.element("div").style(f"position: relative; width: {w}px; height: 14px;"):
                for name, x0, gw, n, nt in labels:
                    if gw >= 6 * len(name):
                        words(
                            name,
                            9.5,
                            SOFT,
                            extra=f"position: absolute; left: {x0:.1f}px; width: {gw:.1f}px; white-space: nowrap; "
                            "overflow: hidden;",
                        ).props(f'title="{name}: {n:,} particles in {nt} tomograms"')


# ── S3 · chain ───────────────────────────────────────────────────────────────


def _step(st: dict[str, Any], *, last: bool) -> None:
    with hrow(4, "align-items: baseline; flex-shrink: 0;"):
        if st["kind"] == "source":
            words(st["words"], 10, SOFT, extra="white-space: nowrap;")
            return
        num(st["id"], 10, PART if st["kind"] == "set" else INK, bold=last)
        if st["kind"] == "job":
            words(st["words"].split(" of ")[0], 10, SOFT, extra="white-space: nowrap;")
        if st["n"] is not None:
            num(f"{st['n']:,}", 10, INK if last else BODY, bold=last)


def s3_chain(s: Set, order: list[str], w: int) -> None:
    """From the picks to this set, left to right, each step with the particles it held; a narrow strip keeps the
    last two steps and the source behind an ellipsis."""
    if s is None and _none(w):
        return
    steps = list(reversed(s["chain"]))
    shown = steps if w >= LARGE or len(steps) <= 2 else steps[-2:]
    with ui.element("div").style(f"width: {w}px; display: flex; flex-direction: column; gap: 3px;"):
        with hrow(5, f"height: 16px; width: {w}px; overflow: hidden; align-items: baseline;"):
            if len(shown) < len(steps):
                words("…", 10, MUTED).props(
                    'title="' + " → ".join(x["id"] or x["words"] for x in steps[: len(steps) - len(shown)]) + '"'
                )
                words("→", 10, MUTED)
            for i, st in enumerate(shown):
                if i:
                    words("→", 10, MUTED)
                _step(st, last=i == len(shown) - 1)
        words(
            s["name"], 9.5, SOFT, extra=f"width: {w}px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
        )


# ── S4 · ruler ───────────────────────────────────────────────────────────────


def s4_ruler(s: Set, order: list[str], w: int) -> None:
    """This set as a share of what it was cut from (a classification's particles, a set); a pool, cut from pick
    lists the strip does not count, shows its projects instead and says so."""
    if s is None and _none(w):
        return
    parent = next((st for st in s["chain"][1:] if st["n"]), None)
    with ui.element("div").style(f"width: {w}px; display: flex; flex-direction: column; gap: 3px;"):
        _ledger(s, w)
        if parent is None:
            _named_bar(s, order, w)
            words(f"a pool of {s['chain'][-1]['words']}", 9.5, SOFT, extra="white-space: nowrap; overflow: hidden;")
            return
        frac = s["n"] / max(1, parent["n"])
        with ui.element("div").style(
            f"width: {w}px; height: 8px; background: {PART_TINT}; box-shadow: inset 0 0 0 1px {PART_LINE};"
        ):
            ui.element("div").style(f"width: {frac * 100:.1f}%; height: 100%; background: {PART};")
        with hrow(4, f"width: {w}px; align-items: baseline;"):
            num(f"{frac:.0%}", 10, INK, bold=True)
            words("of", 9.5, SOFT)
            num(f"{parent['n']:,}", 10, BODY)
            words(
                f"in {parent['id']}" + (f", {parent['words']}" if parent["kind"] == "job" else ""),
                9.5,
                SOFT,
                extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0;",
            )


# ── S5 · status line ─────────────────────────────────────────────────────────

_BLOCKS = " ▏▎▍▌▋▊▉█"


def _block_bar(frac: float, cells: int) -> str:
    eighths = round(max(0.0, min(1.0, frac)) * cells * 8)
    full, part = divmod(eighths, 8)
    return ("█" * full + (_BLOCKS[part] if part else "")).ljust(cells)


def s5_status(s: Set, order: list[str], w: int) -> None:
    """A status line in the terminal's manner: fields in fixed columns split by hairlines, the share of the parent
    (or of the largest project, for a pool) as block characters."""
    if s is None and _none(w):
        return
    parent = next((st for st in s["chain"][1:] if st["n"]), None)
    frac = s["n"] / parent["n"] if parent else max(s["projects"].values()) / max(1, s["n"])
    what = f"of {parent['id']}" if parent else f"in {next(iter(s['projects']))}"
    cells = 12 if w >= LARGE else 6
    fields: list[tuple[str, str, str]] = [
        (s["short"], "", PART),
        (f"{s['n']:,}", "ptcl", INK),
        (f"{s['n_tomos']}", "tomo", BODY),
    ]
    if w >= LARGE:
        fields.append((f"{len(s['projects'])}", "proj", BODY))
    with hrow(
        0,
        f"width: {w}px; height: 18px; background: {BAND}; box-shadow: inset 0 0 0 1px {HAIR}; box-sizing: border-box; "
        "overflow: hidden;",
    ):
        for value, unit, ink in fields:
            with hrow(3, f"padding: 0 6px; height: 100%; box-shadow: inset -1px 0 0 {HAIR}; align-items: center;"):
                num(value, 10, ink, bold=ink != BODY)
                if unit:
                    lab(unit)
        with hrow(4, "padding: 0 6px; min-width: 0; flex: 1 1 0;"):
            num(f"▕{_block_bar(frac, cells)}▏", 10, PART, extra="letter-spacing: 0;")
            num(f"{frac:.0%}", 10, INK)
            if w >= LARGE:
                words(what, 9.5, SOFT, extra="white-space: nowrap; overflow: hidden;")


# ── S6 · tally ───────────────────────────────────────────────────────────────


def s6_tally(s: Set, order: list[str], w: int) -> None:
    """Particles per project as a small table: name, a bar to the largest, the count. A narrow strip lists the three
    largest and how many more."""
    if s is None and _none(w):
        return
    rows = list(s["projects"].items())
    shown = rows if w >= LARGE else rows[:3]
    hi = rows[0][1] if rows else 1
    cols = 2 if w >= LARGE and len(shown) > 4 else 1
    cw = (w - 14 * (cols - 1)) // cols
    with ui.element("div").style(f"width: {w}px; display: flex; flex-direction: column; gap: 2px;"):
        _ledger(s, w)
        with ui.element("div").style(f"display: grid; grid-template-columns: repeat({cols}, {cw}px); gap: 1px 14px;"):
            for name, n in shown:
                with hrow(6, f"height: 14px; width: {cw}px;"):
                    words(
                        name,
                        9.5,
                        BODY,
                        extra=f"width: {int(cw * 0.42)}px; white-space: nowrap; overflow: hidden; "
                        "text-overflow: ellipsis; flex-shrink: 0;",
                    )
                    with ui.element("div").style(f"flex: 1 1 0; height: 4px; background: {GRID};"):
                        ui.element("div").style(
                            f"width: {n / hi * 100:.1f}%; height: 100%; background: "
                            f"{project_shade(order.index(name) if name in order else 0)};"
                        )
                    num(f"{n:,}", 10, INK, extra="width: 40px; text-align: right;")
        if len(shown) < len(rows):
            rest = rows[len(shown) :]
            words(f"+ {len(rest)} more · {sum(n for _, n in rest):,}", 9.5, MUTED).props(
                'title="' + " · ".join(f"{k} {v:,}" for k, v in rest) + '"'
            )


STRIPS: list[tuple[str, str, str, Callable[[Set, list[str], int], None]]] = [
    (
        "S1",
        "Ledger",
        "One line: the set's id, its name, its particles; under it the projects as a bar, named inside "
        "their segments where they fit.",
        s1_ledger,
    ),
    (
        "S2",
        "Tomograms",
        "One column per tomogram, its height the square root of its particles (one crowded tomogram would "
        "flatten the rest), grouped by project with the accent under each group: the magazine's analogue, a mark "
        "per thing the job runs over. Hover a column for its tomogram.",
        s2_tomograms,
    ),
    (
        "S3",
        "Chain",
        "The steps back to the picks, each with the particles it held: what this set was cut from, at a "
        "glance. A narrow strip keeps the last two steps; the rest is behind the ellipsis.",
        s3_chain,
    ),
    (
        "S4",
        "Ruler",
        "The set as a share of what it was cut from, in the accent; a pool, cut from pick lists, shows S1's "
        "named project bar instead. Recommended: the one number a downstream job needs, what was kept.",
        s4_ruler,
    ),
    (
        "S5",
        "Status line",
        "A terminal's status line: fixed fields split by hairlines, the share of the parent as block characters.",
        s5_status,
    ),
    (
        "S6",
        "Tally",
        "Particles per project as a small table; a narrow strip lists the three largest and how many more.",
        s6_tally,
    ),
]
