"""The four top-down arrangements of /dev/map-ideas. Each builds a skeleton of plain divs and asks `node(nid)` for
every card; the browser lays it out (flex, grid) and the cables are drawn after, so a card that grows moves the
others without the server.

tree    each chain runs down its own column, a parent centred over what reads it; the kids that end there are stacked
        into one column instead of spreading sideways
lanes   the tree pinned left: the deepest chain runs straight down under its parent, every other branch steps right,
        and the jobs that end at a job hang beside it
layers  a row per generation: a job sits one row below the deepest job it reads; rows wrap at the view's width
grid    every card in the order the jobs were made, wrapping at the view's width; cables only for the lineage
"""

from __future__ import annotations

from collections.abc import Callable
from html import escape

from nicegui import ui

from ui.map_ideas.data import MapData

Node = Callable[[str], None]
Item = str | tuple[str, list[str]]  # a node id, or ("stack", node ids)


def _band_head(d: MapData, species: str, n: int) -> None:
    title = d.pr.species_names.get(species, species) if species else "every species"
    ui.html(
        f'<span class="t">{escape(title)}</span><span class="r"></span><span class="m">{n} jobs</span>', sanitize=False
    ).classes("cbmi-band")


def _bands(d: MapData, nids: list[str], by_species: bool) -> list[tuple[str | None, list[str]]]:
    if not by_species:
        return [(None, nids)]
    groups: dict[str, list[str]] = {}
    for n in nids:
        groups.setdefault(d.species_of(n), []).append(n)
    order = sorted(groups, key=lambda s: (s != "", min(d.order(n) for n in groups[s])))
    return [(s, groups[s]) for s in order]


def _count_jobs(d: MapData, nids: list[str], under) -> int:
    seen: set[str] = set()
    todo = list(nids)
    while todo:
        n = todo.pop()
        if n in seen:
            continue
        seen.add(n)
        todo.extend(under(n))
    return sum(1 for n in seen if n not in d.sources)


def _stacked(d: MapData, nids: list[str], leaf) -> list[Item]:
    """Siblings in order, with the leaves of one job type and species among them (two or more) as one stack in the
    first one's place."""
    groups: dict[tuple, list[str]] = {}
    for n in nids:
        if n in d.sources or not leaf(n):
            continue
        j = d.job(n)
        groups.setdefault((j.kind, j.species), []).append(n)
    first = {v[0]: v for v in groups.values() if len(v) > 1}
    member = {n for v in first.values() for n in v}
    out: list[Item] = []
    for n in nids:
        if n in first:
            out.append(("stack", first[n]))
        elif n not in member:
            out.append(n)
    return out


# ── tree and lanes ───────────────────────────────────────────────────────────


def _kids_items(d: MapData, kids: list[str], *, lanes: bool, depth) -> list[Item]:
    """What hangs under a node: the kids that go on, each a branch of its own, then every kid that ends there in one
    stack. In lanes the deepest branch comes first, straight under its parent."""
    branches = [k for k in kids if k in d.sources or d.kids(k)]
    ends = [k for k in kids if k not in branches]
    if lanes:
        branches.sort(key=lambda k: (-depth(k), d.order(k)))
    ends.sort(key=lambda k: (d.pr.stage_index(d.job(k).kind), d.order(k)))
    out: list[Item] = list(branches)
    if len(ends) > 1:
        out.append(("stack", ends))
    else:
        out.extend(ends)
    return out


def _forest(d: MapData, node: Node, *, by_species: bool, lanes: bool) -> None:
    main_in = {e.b for e in d.edges if e.kind == "main"}
    roots = [s.id for s in d.sources.values() if s.shared] + [j.id for j in d.pr.jobs if j.id not in main_in]
    roots.sort(key=d.order)
    reached: set[str] = set()
    todo = list(roots)
    while todo:
        n = todo.pop()
        if n not in reached:
            reached.add(n)
            todo.extend(d.kids(n))
    # A job in a loop of recorded inputs is reached by no root: it starts a tree of its own.
    roots += [j.id for j in d.pr.jobs if j.id not in reached]
    placed: set[str] = set()
    memo: dict[str, int] = {}

    def depth(n: str, path: frozenset = frozenset()) -> int:
        if n not in memo:
            memo[n] = 1 + max((depth(k, path | {n}) for k in d.kids(n) if k not in path), default=0)
        return memo[n]

    def item(it: Item) -> None:
        if isinstance(it, tuple):
            with ui.element("div").classes("cbmi-stack"):
                for n in it[1]:
                    placed.add(n)
                    node(n)
            return
        placed.add(it)
        kids = [k for k in d.kids(it) if k not in placed]
        placed.update(kids)
        ends = [k for k in kids if k not in d.sources and not d.kids(k)] if lanes else []
        below = [k for k in kids if k not in ends]
        with ui.element("div").classes("cbmi-sub"):
            if ends:
                # Lanes: what ends at this job hangs beside it, so the chain under it stays one column.
                ends.sort(key=lambda k: (d.pr.stage_index(d.job(k).kind), d.order(k)))
                with ui.element("div").classes("cbmi-headrow"):
                    node(it)
                    with ui.element("div").classes("cbmi-stack cbmi-beside"):
                        for n in ends:
                            node(n)
            else:
                node(it)
            if below:
                with ui.element("div").classes("cbmi-kids"):
                    for k in _kids_items(d, below, lanes=lanes, depth=depth):
                        item(k)

    for species, rs in _bands(d, roots, by_species):
        if species is not None:
            _band_head(d, species, _count_jobs(d, rs, d.kids))
        with ui.element("div").classes("cbmi-forest" + (" cbmi-lanes" if lanes else "")):
            for it in _stacked(d, rs, lambda n: not d.kids(n)):
                item(it)


def tree(d: MapData, node: Node, *, by_species: bool) -> None:
    _forest(d, node, by_species=by_species, lanes=False)


def lanes(d: MapData, node: Node, *, by_species: bool) -> None:
    _forest(d, node, by_species=by_species, lanes=True)


# ── layers ───────────────────────────────────────────────────────────────────


def layers(d: MapData, node: Node, *, by_species: bool) -> None:
    nodes = [s.id for s in d.sources.values() if s.shared] + [j.id for j in d.pr.jobs]
    preds: dict[str, list[str]] = {n: [] for n in nodes}
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    for e in d.edges:
        if e.a in preds and e.b in preds:
            preds[e.b].append(e.a)
            succ[e.a].append(e.b)
    depth: dict[str, int] = {}

    def dep(n: str, path: frozenset = frozenset()) -> int:
        if n in depth:
            return depth[n]
        if n in path:
            return 0  # a loop in the recorded inputs
        depth[n] = 1 + max((dep(p, path | {n}) for p in preds[n]), default=-1)
        return depth[n]

    for n in nodes:
        dep(n)
    for species, ns in _bands(d, sorted(nodes, key=d.order), by_species):
        if species is not None:
            _band_head(d, species, sum(1 for n in ns if n not in d.sources))
        rows: dict[int, list[str]] = {}
        for n in ns:
            rows.setdefault(depth[n], []).append(n)
        pos: dict[str, float] = {}
        with ui.element("div").classes("cbmi-layers"):
            for level in sorted(rows):
                row = sorted(rows[level], key=lambda n, pos=pos: (_bary(preds[n], pos), d.order(n)))
                items = _stacked_layer(d, row, preds, succ)
                with ui.element("div").classes("cbmi-layer"):
                    for i, it in enumerate(items):
                        for n in it[1] if isinstance(it, tuple) else [it]:
                            pos[n] = (i + 0.5) / len(items)
                        if isinstance(it, tuple):
                            with ui.element("div").classes("cbmi-stack"):
                                for n in it[1]:
                                    node(n)
                        else:
                            node(it)


def _bary(ps: list[str], pos: dict[str, float]) -> float:
    """Where a node's inputs sit across the rows above, 0 to 1; a node reading nothing on the map goes first."""
    xs = [pos[p] for p in ps if p in pos]
    return sum(xs) / len(xs) if xs else -1.0


def _stacked_layer(d: MapData, row: list[str], preds: dict, succ: dict) -> list[Item]:
    """A row's leaves of one job type and species that read the same jobs, stacked."""
    groups: dict[tuple, list[str]] = {}
    for n in row:
        if n in d.sources or succ[n]:
            continue
        j = d.job(n)
        groups.setdefault((j.kind, j.species, tuple(sorted(preds[n]))), []).append(n)
    first = {v[0]: v for v in groups.values() if len(v) > 1}
    member = {n for v in first.values() for n in v}
    out: list[Item] = []
    for n in row:
        if n in first:
            out.append(("stack", first[n]))
        elif n not in member:
            out.append(n)
    return out


# ── grid ─────────────────────────────────────────────────────────────────────


def grid(d: MapData, node: Node, *, by_species: bool) -> None:
    jobs = sorted((j.id for j in d.pr.jobs), key=d.order)
    for species, ns in _bands(d, jobs, by_species):
        if species is not None:
            _band_head(d, species, len(ns))
        with ui.element("div").classes("cbmi-grid"):
            for n in ns:
                node(n)


LAYOUTS: dict[str, Callable[..., None]] = {"tree": tree, "lanes": lanes, "layers": layers, "grid": grid}
