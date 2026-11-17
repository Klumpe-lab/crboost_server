"""The particle map's cards, in the Jobs map's visual language (`ui/branch_map/card.render_card`), without the
tilt-series magazine.

A job card: its input rows and footer as on the Jobs map. A Class 3D card also has one row per class of its last
iteration just above the footer ("class 2 · 41,203 · 38 % · 11.2 Å (Class 3D estimate)"), each with its own port on
the right edge, where a set made of that class takes its cable; the footer keeps the whole job's port. A set card
is slim: its name, its number, one "from" row per parent and its count in the footer.

Large: PARAMETERS and RESULTS as the Jobs map draws them (`ui/branch_map/blocks.py`, without the tilt-series
tally), and PARTICLES: the particles per project as bars and per tomogram as a histogram. A set's large card shows
its RECIPE instead of the job blocks.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from nicegui import ui

from services.particles.lineage import ClassRow, Composition, JobParticles
from ui.branch_map.blocks import LINE_H, PLOT_H, Section, draw_middle, middle_height, params_parts, results_parts
from ui.branch_map.card import CardSpec, SocketSpec, port, render_card
from ui.branch_map.facts import FactsStore
from ui.branch_map.geometry import CARD_W, HEAD_H, LARGE_W, SIDE, SLOT_H, Box, card_height
from ui.branch_map.plots import svg_hist
from ui.branch_map.state import MapState
from ui.branch_map.style import AMBER, BLUE, BODY, INK, LINE, MUTED, RED, SOFT, hrow, text
from ui.particle_map.model import Node, ParticleMap, set_slot

SET_W = 220
CLASS_H = SLOT_H
BAR_ROW_H = 14
MAX_PROJECT_ROWS = 8
HIST_BINS = 24
SHARE_TIP = (
    "share: rlnClassDistribution of the model star, RELION's probability-weighted share of the particles in this "
    "class; it can differ from the members counted by rlnClassNumber"
)
LEVEL_WORDS = {"extracted": "extracted", "coordinates": "positions"}


@dataclass
class Drawn:
    """A node's card as measured before drawing: its spec, box, middle and where its class ports sit."""

    spec: CardSpec
    box: Box
    secs: list[Section]
    classes: tuple[ClassRow, ...]
    class_y0: float  # the top of the first class row, from the card's top


# ── specs ────────────────────────────────────────────────────────────────────


def _sockets(node: Node) -> list[SocketSpec]:
    return [SocketSpec(set_slot(r), r.state, r.value, r.tip) for r in node.rows]


def _count(n: int) -> str:
    return f"{n:,}"


def spec_of(pm: ParticleMap, node: Node) -> CardSpec:
    pr = pm.pr
    if node.set is not None:
        s = node.set
        return CardSpec(
            "set",
            s.name,
            s.number,
            "done",
            LEVEL_WORDS.get(s.level, s.level),
            _sockets(node),
            f"{_count(s.n)} particles · {s.n_tomos} tomograms",
        )
    j = node.job
    makes = pr.makes.get(j.kind, "")
    jp = node.particles
    if jp is not None and jp.classes is not None:
        makes = f"particles, every class · it {jp.classes.iteration:03d}"
    elif jp is not None and jp.composition is not None and jp.role == "makes":
        makes = f"{makes} · {_count(jp.composition.total)}"
    return CardSpec(
        j.kind, pr.names.get(j.kind, j.kind), j.number, j.status, pr.method_of(j), _sockets(node), makes, job=j
    )


# ── the blocks of a large card ───────────────────────────────────────────────


def _cap(content: str, color: str = SOFT) -> None:
    text(content, 9, color, extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;")


def _bars(rows: list[tuple[str, int]], inner_w: int) -> None:
    top = max((n for _, n in rows), default=1) or 1
    bar_w = inner_w - 150 - 60
    with ui.element("div").style("display: flex; flex-direction: column;"):
        for name, n in rows:
            with hrow(8, f"height: {BAR_ROW_H}px;"):
                text(
                    name,
                    9,
                    BODY,
                    extra="width: 150px; flex-shrink: 0; white-space: nowrap; overflow: hidden; "
                    "text-overflow: ellipsis;",
                ).tooltip(name)
                text(_count(n), 9, INK, mono=True, extra="width: 52px; text-align: right; flex-shrink: 0;")
                with ui.element("div").style(f"width: {bar_w}px; height: 6px; background: {LINE}; border-radius: 2px;"):
                    ui.element("div").style(
                        f"width: {max(1.0, bar_w * n / top):.1f}px; height: 6px; background: {BLUE}; "
                        "border-radius: 2px;"
                    )


def _hist(counts: list[int]) -> tuple[list[float], str]:
    lo, hi = min(counts), max(counts)
    if lo == hi:
        return [float(len(counts))], f"{len(counts)} tomograms, {lo:,} particles each"
    bins = [0.0] * HIST_BINS
    for c in counts:
        bins[min(HIST_BINS - 1, int((c - lo) / (hi - lo) * HIST_BINS))] += 1
    return (
        bins,
        f"particles per tomogram, {len(counts)} tomograms · {lo:,} – {hi:,} · "
        f"median {sorted(counts)[len(counts) // 2]:,}",
    )


def particles_parts(jp: JobParticles | None, inner_w: int) -> list:
    """The particles a node holds: per project as bars, per tomogram as a histogram, what the reads flagged."""
    if jp is None:
        return [(LINE_H, lambda: _cap("reading the particles…"))]
    comp: Composition | None = jp.composition
    if comp is None:
        return [(LINE_H, lambda: _cap(jp.problem or "no particles", RED if jp.role else SOFT))]
    parts: list = []
    read = " read" if jp.role == "reads" else ""
    n_proj = len(comp.by_project)
    head = (
        f"{_count(comp.total)} particles{read} · {n_proj} project{'' if n_proj == 1 else 's'} · "
        f"{len(comp.by_tomo)} tomograms"
    )
    parts.append((LINE_H, lambda: _cap(head)))
    shown = list(comp.by_project[:MAX_PROJECT_ROWS])
    if shown:
        parts.append((BAR_ROW_H * len(shown), lambda: _bars(shown, inner_w)))
    if len(comp.by_project) > MAX_PROJECT_ROWS:
        rest = comp.by_project[MAX_PROJECT_ROWS:]
        more = f"and {len(rest)} more projects, {_count(sum(n for _, n in rest))} particles"
        parts.append((LINE_H, lambda: _cap(more, MUTED)))
    counts = [n for _t, _p, n in comp.by_tomo]
    if len(counts) >= 3:
        bins, caption = _hist(counts)
        parts.append((PLOT_H, lambda: ui.html(svg_hist(bins, inner_w, PLOT_H), sanitize=False)))
        parts.append((LINE_H, lambda: _cap(caption)))
    for p in comp.problems:
        parts.append((LINE_H, lambda p=p: _cap(p, AMBER)))
    return parts


def _recipe_parts(node: Node) -> list:
    s = node.set
    lines = [s.how, f"{s.op.replace('_', ' ')} · {LEVEL_WORDS.get(s.level, s.level)} · {s.id}"]
    stale = [p.label for p in s.parents if p.stale]
    parts = [(LINE_H, lambda line=line: _cap(line)) for line in lines]
    if stale:
        parts.append((LINE_H, lambda: _cap("changed since the set was made: " + ", ".join(stale), AMBER)))
    for p in s.problems:
        parts.append((LINE_H, lambda p=p: _cap(p, AMBER)))
    return parts


def sections(pm: ParticleMap, node: Node, facts: FactsStore, inner_w: int) -> list[Section]:
    if node.set is not None:
        return [
            ("RECIPE", _recipe_parts(node)),
            ("PARTICLES", particles_parts(JobParticles("makes", node.set.composition), inner_w)),
        ]
    j = node.job
    when = "not run yet" if j.status in ("scheduled", "queued") else "as run"
    secs: list[Section] = [
        (
            f"PARAMETERS · {when}",
            params_parts(pm.pr.param_fields.get(j.kind, ()), j.param_values, inner_w, editable=False),
        )
    ]
    if j.status in ("done", "failed", "running"):
        secs.append(("RESULTS", results_parts(pm.pr, j, inner_w, facts, tally=False)))
        secs.append(("PARTICLES", particles_parts(node.particles, inner_w)))
    return secs


# ── measuring and drawing ────────────────────────────────────────────────────


def measure(pm: ParticleMap, node: Node, facts: FactsStore, *, large: bool) -> Drawn:
    spec = spec_of(pm, node)
    w = LARGE_W if large else (SET_W if node.is_set else CARD_W)
    secs = sections(pm, node, facts, w - 2 * SIDE) if large else []
    jp = node.particles
    classes = jp.classes.classes if jp is not None and jp.classes is not None else ()
    sec_h = middle_height(secs)
    extra = sec_h + len(classes) * CLASS_H
    box = Box(w, card_height(len(spec.sockets), extra), extra, 0)
    return Drawn(spec, box, secs, classes, HEAD_H + len(spec.sockets) * SLOT_H + sec_h)


def class_port_y(d: Drawn, number: int) -> float | None:
    """Where the cable of class `number` leaves the card, from its top."""
    for k, c in enumerate(d.classes):
        if c.number == number:
            return d.class_y0 + k * CLASS_H + CLASS_H / 2
    return None


def _class_words(c: ClassRow) -> str:
    share = f"{c.share:.0%}".replace("%", " %") if c.share is not None else "share ?"
    est = f"{c.estimate_ang:.1f} Å (Class 3D estimate)" if c.estimate_ang else "no estimate"
    return f"class {c.number} · {_count(c.n)} · {share} · {est}"


def _class_rows(classes: tuple[ClassRow, ...]) -> None:
    for c in classes:
        row = hrow(6, f"height: {CLASS_H}px; padding-right: 4px; box-sizing: border-box;").classes("cb-bm-row")
        with row:
            text(_class_words(c), 10, INK, extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;")
        row.tooltip(f"{_class_words(c)} · {SHARE_TIP} · map {c.map_path.rsplit('/', 1)[-1]}")


def render(
    pm: ParticleMap,
    node: Node,
    d: Drawn,
    st: MapState,
    *,
    x: float,
    y: float,
    on_open: Callable | None,
    on_size: Callable | None,
    picked: bool,
) -> ui.element:
    secs, classes = d.secs, d.classes

    def middle() -> None:
        draw_middle(secs)
        _class_rows(classes)

    card = render_card(
        pm.pr,
        d.spec,
        d.box,
        st,
        x=x,
        y=y,
        middle=middle if d.box.extra else None,
        on_open=on_open,
        on_size=on_size,
        chosen=picked,
        size_tip="The recipe and the particles on the card"
        if node.is_set
        else "Parameters, results and the particles on the card",
    )
    with card:
        for c in classes:
            port(d.box.w, class_port_y(d, c.number), "output", tip=f"class {c.number}: a set made of it hangs here")
    return card
