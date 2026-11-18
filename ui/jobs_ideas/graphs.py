"""Four ways to draw the pipeline as a graph, each laid out by rule, never by hand: where a job sits follows from
its type (the canonical stage order) and from what it reads (its main input's job). A click on a job opens its
magazine, the same popup the roster opens.

- `subway`   stages left to right as columns, each branch a line of its own; stations carry a light and a strip
- `gitlog`   one row per job in job-number order, the branches drawn in a narrow gutter, as `git log --graph`
- `icicle`   stages as columns, each job a block as tall as the branches below it: the tree as a table
- `tree`     an indented listing; a chain stays on one level, only a fork indents; columns for the counts
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field

from nicegui import ui

from ui.branch_map.model import Job
from ui.jobs_ideas.data import DISPLAY, STAGE_ORDER, Data
from ui.jobs_ideas.glyphs import glyph
from ui.jobs_ideas.look import (
    BODY,
    GLYPH,
    HEAD,
    INK,
    LIGHT,
    LINE,
    MARK,
    MONO,
    MUTED,
    SOFT,
    light,
    micro_magazine,
    tally,
)
from ui.jobs_ideas.popups import Popups

# A click anywhere in a graph: the job under the pointer, and its rectangle.
_NODE_JS = (
    "(e) => { const t = e.target.closest('[data-job]'); if (!t) return; const r = t.getBoundingClientRect(); "
    "emit({iid: t.dataset.job, x: r.right, y: r.top, l: r.left, vw: window.innerWidth, vh: window.innerHeight}); }"
)
_LINE_INK = "#a9b8c9"


def _h(s) -> str:
    return html.escape(str(s))


@dataclass
class Node:
    j: Job
    children: list[Node] = field(default_factory=list)
    parent: Node | None = None
    lane: int = 0
    col: int = 0
    leaf0: int = 0
    leaves: int = 1


class Tree:
    """The jobs as a forest by main input; lanes (a branch's line), stage columns and leaf spans worked out once."""

    def __init__(self, d: Data) -> None:
        self.d = d
        jobs = [j for j in d.pr.jobs if j.on_roster]
        self.nodes = {j.id: Node(j) for j in jobs}
        self.roots: list[Node] = []
        for n in self.nodes.values():
            p = self.nodes.get(n.j.parent) if n.j.parent else None
            if p is not None and p is not n:
                n.parent = p
                p.children.append(n)
            else:
                self.roots.append(n)

        def key(n: Node) -> tuple[str, str]:
            return (n.j.number or "999", n.j.id)

        for n in self.nodes.values():
            n.children.sort(key=key)
        self.roots.sort(key=key)
        used = sorted({n.j.kind for n in self.nodes.values()}, key=STAGE_ORDER.index)
        self.cols = used
        for n in self.nodes.values():
            n.col = used.index(n.j.kind)
        self.lanes = 0
        self.order: list[Node] = []
        for r in self.roots:
            self._walk(r, self._new_lane())
        leaf = 0
        for n in self.order:
            if not n.children:
                n.leaf0, leaf = leaf, leaf + 1
        for n in reversed(self.order):
            if n.children:
                n.leaf0 = min(c.leaf0 for c in n.children)
                n.leaves = sum(c.leaves for c in n.children)
        self.n_leaves = leaf

    def _new_lane(self) -> int:
        self.lanes += 1
        return self.lanes - 1

    def _walk(self, n: Node, lane: int) -> None:
        n.lane = lane
        self.order.append(n)
        for i, c in enumerate(n.children):
            self._walk(c, lane if i == 0 else self._new_lane())


def _mag_rects(states: list[str], x: float, y: float, w: float, h: float) -> str:
    n = len(states)
    if not n:
        return ""
    bw = w / n
    out = []
    for i, s in enumerate(states):
        cls = ' class="cbji-run"' if s == "running" else ""
        out.append(
            f'<rect x="{x + i * bw:.2f}" y="{y}" width="{max(bw - 0.4, 0.5):.2f}" height="{h}" '
            f'fill="{MARK.get(s, MARK["unrun"])[0]}"{cls}/>'
        )
    return "".join(out)


def _glyph_at(kind: str, x: float, y: float, ink: str = GLYPH, icons: str = "drawn") -> str:
    if icons == "material":
        return ""
    return glyph(kind, ink=ink).replace("<svg ", f'<svg x="{x}" y="{y}" ', 1)


class Graphs:
    def __init__(self, data: Data, popups: Popups, icons) -> None:
        self.d = data
        self.pop = popups
        self.icons = icons
        self.t = Tree(data)

    def render(self, variant: str) -> None:
        getattr(self, f"_{variant}")()

    def _clickable(self, el: ui.element) -> ui.element:
        def _open(e):
            iid = e.args.get("iid")
            if iid in self.d.has_tasks:
                self.pop.open(iid, e.args)
            else:
                j = self.d.job(iid)
                ui.notify(f"Opens {self.d.name(j)} {j.number}'s page (no tasks: no magazine)")

        return el.on("click", _open, js_handler=_NODE_JS)

    def _states(self, j: Job) -> list[str]:
        return [s for _, s in self.d.tasks(j)] if j.id in self.d.has_tasks else []

    # ── subway ──

    def _subway(self) -> None:
        t, d = self.t, self.d
        cw, lh, x0, y0 = 96, 58, 40, 46
        w = x0 + len(t.cols) * cw + 20
        h = y0 + t.lanes * lh + 10
        pos = {iid: (x0 + n.col * cw, y0 + n.lane * lh) for iid, n in t.nodes.items()}
        parts = []
        for i, kind in enumerate(t.cols):
            x = x0 + i * cw
            parts.append(_glyph_at(kind, x - 6, 6))
            parts.append(
                f'<text x="{x}" y="30" text-anchor="middle" style="font: 400 7.5px var(--cb-mono, monospace); '
                f'letter-spacing:.06em; text-transform: uppercase;" fill="{SOFT}">{_h(DISPLAY.get(kind, kind))}</text>'
            )
            parts.append(f'<line x1="{x}" y1="36" x2="{x}" y2="{h}" stroke="{LINE}" stroke-dasharray="1 3"/>')
        for iid, n in t.nodes.items():
            if n.parent is None:
                continue
            (px, py), (cx, cy) = pos[n.parent.j.id], pos[iid]
            dy = abs(cy - py)
            if dy == 0:
                path = f"M{px} {py} H{cx}"
            else:
                # Metro style: leave the parent's line at once, drop to the branch's lane, run along it.
                path = f"M{px} {py} H{px + 12} L{px + 12 + cw * 0.45} {cy} H{cx}"
            parts.append(f'<path d="{path}" fill="none" stroke="#5b7089" stroke-width="2.2" stroke-linejoin="round"/>')
        for iid, n in t.nodes.items():
            for s in n.j.side:
                if s in pos and s != (n.parent.j.id if n.parent else None):
                    (sx, sy), (cx, cy) = pos[s], pos[iid]
                    parts.append(
                        f'<path d="M{sx} {sy} C{sx + 30} {sy}, {cx - 40} {cy - 14}, {cx - 6} {cy - 5}" fill="none" '
                        f'stroke="{_LINE_INK}" stroke-width="1" stroke-dasharray="2 2"/>'
                    )
        for iid, n in t.nodes.items():
            x, y = pos[iid]
            j = n.j
            color = LIGHT.get(j.status, LIGHT["scheduled"])
            cls = ' class="cbji-run"' if j.status == "running" else ""
            states = self._states(j)
            parts.append(f'<g data-job="{_h(iid)}" class="cbji-node">')
            parts.append(f'<rect x="{x - 26}" y="{y - 9}" width="52" height="40" fill="transparent"/>')
            parts.append(
                f'<rect x="{x - 6}" y="{y - 6}" width="12" height="12" fill="#fff" stroke="#12324f" '
                'stroke-width="1.6"/>'
            )
            parts.append(f'<rect x="{x - 3}" y="{y - 3}" width="6" height="6" fill="{color}"{cls}/>')
            label = (j.number or "new") + (f" {d.pr.species_names.get(j.species, '')[:8]}" if j.species else "")
            parts.append(
                f'<text x="{x}" y="{y + 18}" text-anchor="middle" style="font: 500 8px var(--cb-mono, monospace);" '
                f'fill="{BODY}">{_h(label)}</text>'
            )
            if states:
                parts.append(_mag_rects(states, x - 22, y + 23, 44, 4))
            parts.append(f"<title>{_h(d.name(j))} {j.number} · {_h(j.tool)} · {_h(d.pr.tally_words(j))}</title></g>")
        svg = f'<svg width="{w}" height="{h}" style="display:block;">{"".join(parts)}</svg>'
        with ui.element("div").style("overflow-x: auto; background: #fff;"):
            self._clickable(ui.html(svg, sanitize=False))

    # ── git log ──

    def _gitlog(self) -> None:
        t, d = self.t, self.d
        rows = sorted(t.nodes.values(), key=lambda n: (n.j.number or "999", n.j.id))
        rh, lw = 26, 13
        gw = 12 + t.lanes * lw
        index = {n.j.id: i for i, n in enumerate(rows)}
        parts = []
        for n in rows:
            if n.parent is None:
                continue
            x1, y1 = 8 + n.parent.lane * lw, index[n.parent.j.id] * rh + rh / 2
            x2, y2 = 8 + n.lane * lw, index[n.j.id] * rh + rh / 2
            if x1 == x2:
                path = f"M{x1} {y1} V{y2}"
            else:
                path = f"M{x1} {y1} C{x1} {y1 + rh * 0.6}, {x2} {y1 + rh * 0.4}, {x2} {y1 + rh} V{y2}"
            parts.append(f'<path d="{path}" fill="none" stroke="#7d93ab" stroke-width="1.4"/>')
        for i, n in enumerate(rows):
            x, y = 8 + n.lane * lw, i * rh + rh / 2
            color = LIGHT.get(n.j.status, LIGHT["scheduled"])
            cls = ' class="cbji-run"' if n.j.status == "running" else ""
            parts.append(
                f'<rect x="{x - 4}" y="{y - 4}" width="8" height="8" fill="#fff" stroke="#12324f" stroke-width="1.3"/>'
            )
            parts.append(f'<rect x="{x - 2}" y="{y - 2}" width="4" height="4" fill="{color}"{cls}/>')
        gutter = (
            f'<svg width="{gw}" height="{len(rows) * rh}" style="display:block;flex-shrink:0;">{"".join(parts)}</svg>'
        )
        with ui.element("div").style("display: flex; background: #fff;") as box:
            ui.html(gutter, sanitize=False)
            with ui.element("div").style("flex: 1 1 auto; min-width: 0;"):
                for n in rows:
                    j = n.j
                    with (
                        ui.element("div")
                        .classes("cbji-row")
                        .props(f'data-job="{j.id}"')
                        .style(
                            "display: grid; grid-template-columns: 14px 150px 30px 92px 1fr 52px 132px; "
                            "align-items: center; "
                            f"gap: 6px; height: {rh}px; padding: 0 8px; border-bottom: 1px solid {LINE};"
                        )
                    ):
                        ui.html(glyph(j.kind, ink=GLYPH, icons=self.icons()), sanitize=False).style("display: flex;")
                        ui.label(d.name(j)).style(f"{MONO} font-size: 10px; color: {BODY}; white-space: nowrap;")
                        ui.label(j.number).style(f"{MONO} font-size: 8.5px; color: {MUTED};")
                        ui.label(j.tool).style(f"{MONO} font-size: 8.5px; color: {SOFT}; white-space: nowrap;")
                        tag = d.pr.species_names.get(j.species, "") if j.species else j.method
                        ui.label(tag or "").style(f"{MONO} font-size: 8.5px; color: {SOFT}; white-space: nowrap;")
                        if j.id in d.has_tasks:
                            text, ink = tally(d.counts(j), j.status)
                            ui.label(text).style(f"{MONO} font-size: 8.5px; color: {ink}; text-align: right;")
                            ui.html(micro_magazine(self._states(j), width=132, height=8), sanitize=False)
                        else:
                            ui.label("")
                            ui.label("")
        self._clickable(box)

    # ── icicle ──

    def _icicle(self) -> None:
        t, d = self.t, self.d
        cw, rh, head = 108, 34, 34
        w, h = len(t.cols) * cw, head + t.n_leaves * rh
        with ui.element("div").style("overflow-x: auto; background: #fff;"):
            with ui.element("div").style(f"position: relative; width: {w}px; height: {h}px;") as box:
                for i, kind in enumerate(t.cols):
                    with ui.element("div").style(
                        f"position: absolute; left: {i * cw}px; top: 0; width: {cw - 2}px; height: {head - 4}px; "
                        f"background: {HEAD}; display: flex; flex-direction: column; align-items: center; "
                        "justify-content: center; gap: 2px;"
                    ):
                        ui.html(glyph(kind, ink=GLYPH, icons=self.icons()), sanitize=False)
                        ui.label(DISPLAY.get(kind, kind)).style(
                            f"{MONO} font-size: 7.5px; text-transform: uppercase; letter-spacing: .06em; color: "
                            f"{SOFT}; "
                            "white-space: nowrap;"
                        )
                for n in t.nodes.values():
                    j = n.j
                    top = head + n.leaf0 * rh
                    height = n.leaves * rh - 2
                    left = n.col * cw
                    # A stage this branch skips: a thin bridge from the parent's block.
                    if n.parent is not None and n.col > n.parent.col + 1:
                        bl = (n.parent.col + 1) * cw
                        ui.element("div").style(
                            f"position: absolute; left: {bl - 2}px; top: {top + rh / 2 - 1}px; "
                            f"width: {left - bl + 2}px; "
                            "height: 1px; background: #c5d3e2;"
                        )
                    color = LIGHT.get(j.status, LIGHT["scheduled"])
                    with (
                        ui.element("div")
                        .classes("cbji-node")
                        .props(f'data-job="{j.id}"')
                        .style(
                            f"position: absolute; left: {left}px; top: {top}px; width: {cw - 2}px; height: {height}px; "
                            f"background: #f6f8fb; border-left: 3px solid {color}; box-sizing: border-box; "
                            "padding: 3px 5px; display: flex; flex-direction: column; gap: 1px; overflow: hidden;"
                        )
                    ):
                        with ui.element("div").style("display: flex; gap: 4px; align-items: baseline;"):
                            ui.label(j.number or "new").style(f"{MONO} font-size: 9px; color: {INK}; font-weight: 500;")
                            tag = d.pr.species_names.get(j.species, "") if j.species else (j.method or j.tool)
                            ui.label(tag).style(
                                f"{MONO} font-size: 8px; color: {SOFT}; white-space: nowrap; overflow: hidden; "
                                "text-overflow: ellipsis;"
                            )
                        if j.id in d.has_tasks:
                            text, ink = tally(d.counts(j), j.status)
                            ui.label(text).style(f"{MONO} font-size: 8px; color: {ink};")
                            ui.html(micro_magazine(self._states(j), width=cw - 14, height=5), sanitize=False)
                self._clickable(box)

    # ── tree listing ──

    def _tree(self) -> None:
        t, d = self.t, self.d
        lines: list[tuple[str, Node]] = []

        def walk(n: Node, prefix: str, lead: str) -> None:
            lines.append((prefix + lead, n))
            kids = n.children
            if len(kids) == 1:
                # A chain stays on its level.
                walk(kids[0], prefix + ("   " if lead.startswith("└") else ("│  " if lead else "")), "")
                return
            base = prefix + ("   " if lead.startswith("└") else ("│  " if lead else ""))
            for i, c in enumerate(kids):
                walk(c, base, "└─ " if i == len(kids) - 1 else "├─ ")

        for r in t.roots:
            walk(r, "", "")
        pre_w = 8 + 7 * max((len(p) for p, _ in lines), default=0)
        with ui.element("div").style("background: #fff;") as box:
            for pre, n in lines:
                j = n.j
                with (
                    ui.element("div")
                    .classes("cbji-row")
                    .props(f'data-job="{j.id}"')
                    .style(
                        f"display: grid; grid-template-columns: {pre_w}px 10px 14px 140px 30px 1fr 52px 132px; "
                        "align-items: center; "
                        f"gap: 6px; height: 22px; padding: 0 10px; border-bottom: 1px solid {LINE};"
                    )
                ):
                    ui.label(pre).style(f"{MONO} font-size: 11px; color: #a9b8c9; white-space: pre; letter-spacing: 0;")
                    ui.html(light(j.status), sanitize=False).style("display: flex;")
                    ui.html(glyph(j.kind, ink=GLYPH, icons=self.icons()), sanitize=False).style("display: flex;")
                    ui.label(d.name(j)).style(f"{MONO} font-size: 10px; color: {BODY}; white-space: nowrap;")
                    ui.label(j.number).style(f"{MONO} font-size: 8.5px; color: {MUTED};")
                    tag = d.pr.species_names.get(j.species, "") if j.species else (j.method or j.tool)
                    ui.label(tag).style(f"{MONO} font-size: 8.5px; color: {SOFT}; white-space: nowrap;")
                    if j.id in d.has_tasks:
                        text, ink = tally(d.counts(j), j.status)
                        ui.label(text).style(f"{MONO} font-size: 8.5px; color: {ink}; text-align: right;")
                        ui.html(micro_magazine(self._states(j), width=132, height=8), sanitize=False)
                    else:
                        ui.label("")
                        ui.label("")
        self._clickable(box)
