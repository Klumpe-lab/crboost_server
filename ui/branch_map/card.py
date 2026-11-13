"""One job card as the visual language of every job type. Small: the magazine of tilt-series squares hovering
above it, a header band with the status light, the type's name, the tool or method as a badge and the job number;
one labelled row per input saying what it reads and from which job, each with a port on the card's left edge
where its cable arrives; a footer naming what the job makes, its port on the right edge where cables leave, and
the one action a card offers: add a job that reads this output. Large: the same card holding the blocks of
`ui/branch_map/blocks.py` between the input rows and the footer. A job being added is a large card with the ✕
that discards it and the ✓ that adds it in its header."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from nicegui import ui

from ui.branch_map.geometry import (
    CARD_W,
    HEAD_H,
    LABEL_W,
    LARGE_W,
    OUT_H,
    PORT,
    SIDE,
    SLOT_H,
    STRIP_GAP,
    TEXT_X,
    Box,
    StripSpec,
    card_height,
    out_y_of,
    port_y,
    strip_spec,
)
from ui.branch_map.model import Job, Project, Slot
from ui.branch_map.plots import svg
from ui.branch_map.state import MapState
from ui.branch_map.style import (
    AMBER,
    BAND,
    BLUE,
    BODY,
    BOX_COLORS,
    CARD_BORDER,
    CARD_SHADOW,
    GREEN,
    HAIR,
    INK,
    INNER_R,
    MUTED,
    RED,
    SLATE,
    SOFT,
    STATUS,
    hrow,
    label,
    text,
    tok,
    voice,
)

PORT_FILL = {
    "wired": SLATE,
    "named": "#fff",
    "explicit": BLUE,
    "automatic": SLATE,
    "ambiguous": AMBER,
    "missing": "#fff",
    "optional": "#fff",
    "pending": "#fff",
    "output": "#fff",
}
PORT_RING = {
    "wired": SLATE,
    "named": SLATE,
    "explicit": BLUE,
    "automatic": SLATE,
    "ambiguous": AMBER,
    "missing": RED,
    "optional": tok("off-dot", "#cbd5e1"),
    "pending": tok("off-dot", "#cbd5e1"),
    "output": SLATE,
}
VALUE_COLOR = {
    "wired": INK,
    "named": INK,
    "explicit": BLUE,
    "automatic": BODY,
    "ambiguous": AMBER,
    "missing": RED,
    "optional": MUTED,
    "pending": MUTED,
}


# ── the new job and how its inputs resolve ───────────────────────────────────


@dataclass
class Resolved:
    """One input of the new job: `state` is explicit / automatic / ambiguous / missing / optional / pending (the
    main input is not chosen yet), `source` the job it reads, `candidates` every job it could read, `note` why."""

    slot: Slot
    state: str
    source: str | None
    candidates: list[Job]
    note: str


def resolve(pr: Project, st: MapState) -> list[Resolved]:
    """The new job's inputs. The first slot is the main one; a bundled slot rides it; any other slot is filled by
    itself when exactly one candidate sits on the main choice's chain (the train job of this reconstruction, the
    CTF these tomograms came from), asked when several do, flagged when none does."""
    if st.adding is None:
        return []
    out: list[Resolved] = []
    main_src: str | None = None
    for i, slot in enumerate(pr.slots_of(st.adding, st.method)):
        cands = pr.candidates_for(slot)
        picked = st.picks.get(slot.key)
        if picked is not None and picked in pr.by_id:
            out.append(Resolved(slot, "explicit", picked, cands, "set by hand"))
            if i == 0:
                main_src = picked
            continue
        if i == 0:
            out.append(Resolved(slot, "ambiguous", None, cands, f"choose one of {len(cands)}"))
            continue
        if slot.bundle:
            if main_src is not None and pr.produces.get(pr.by_id[main_src].kind) in slot.accepts:
                out.append(Resolved(slot, "automatic", main_src, cands, f"comes with the {out[0].slot.label}"))
            else:
                out.append(Resolved(slot, "pending", None, cands, f"follows the {out[0].slot.label}"))
            continue
        if main_src is None:
            out.append(Resolved(slot, "pending", None, cands, f"once the {out[0].slot.label} are chosen"))
            continue
        chain = {a.id for a in pr.path_of(pr.by_id[main_src])}
        # On the chain: an ancestor of the main choice, a job reading it, or a job descending from it.
        near = [c for c in cands if c.id in chain or c.parent == main_src or main_src in {a.id for a in pr.path_of(c)}]
        if len(near) == 1:
            out.append(Resolved(slot, "automatic", near[0].id, cands, "the one on this chain"))
        elif near:
            out.append(Resolved(slot, "ambiguous", None, near, f"{len(near)} on this chain: choose one"))
        elif not slot.required:
            out.append(Resolved(slot, "optional", None, cands, "optional, none on this chain"))
        else:
            first = pr.names.get(cands[0].kind, "job") if cands else "job"
            out.append(Resolved(slot, "missing", None, cands, f"none yet: add a {first} first"))
    return out


def unsettled(resolved: list[Resolved]) -> list[Resolved]:
    """What still blocks the ✓: an input to choose, or one nothing provides. A row that follows the main choice
    (the warp dir that comes with the tomograms) is settled with it."""
    return [r for r in resolved if r.state in ("ambiguous", "missing")]


# ── the card's content ───────────────────────────────────────────────────────


@dataclass
class SocketSpec:
    slot: Slot
    state: str
    value: str
    tip: str
    bracket: bool = False  # reads the same job as the main row, bracketed to its port instead of a cable of its own


@dataclass
class CardSpec:
    """Everything a card shows; built from a Job or from the new job."""

    kind: str
    title: str
    number: str
    status: str  # done / running / failed / queued / scheduled / new
    method: str
    sockets: list[SocketSpec]
    makes: str
    job: Job | None = None
    ghost: bool = False


def spec_of(pr: Project, j: Job) -> CardSpec:
    sockets = []
    rows = pr.rows_of(j)
    main_src = rows[0].source if rows else None
    for i, r in enumerate(rows):
        slot = r.slot
        gloss = pr.slot_gloss.get(slot.label, "")
        if r.source is not None and r.source in pr.by_id:
            s = pr.by_id[r.source]
            value = f"{pr.names.get(s.kind, s.kind)} {s.number or s.id}"
            tip = f"{slot.label} ← {pr.ident(s)} · {r.how} · {gloss}"
            bracket = i > 0 and slot.bundle and r.source == main_src
            sockets.append(SocketSpec(slot, "explicit" if r.how == "set by hand" else "wired", value, tip, bracket))
        elif r.value:
            sockets.append(SocketSpec(slot, "named", r.value, f"{slot.label}: {r.value} · {r.how} · {gloss}"))
        elif r.how == "to choose":
            sockets.append(SocketSpec(slot, "ambiguous", "to choose", f"{slot.label}: not chosen yet · {gloss}"))
        elif slot.required:
            sockets.append(SocketSpec(slot, "missing", "nothing", f"{slot.label}: no job provides this · {gloss}"))
        else:
            sockets.append(SocketSpec(slot, "optional", "optional, none", f"{slot.label}: optional, not set · {gloss}"))
    return CardSpec(
        j.kind,
        pr.names.get(j.kind, j.kind),
        j.number,
        j.status,
        pr.method_of(j),
        sockets,
        pr.makes.get(j.kind, ""),
        job=j,
    )


def spec_of_new(pr: Project, st: MapState, resolved: list[Resolved]) -> CardSpec:
    sockets = []
    main_src = resolved[0].source if resolved else None
    for i, r in enumerate(resolved):
        if r.source is not None:
            s = pr.by_id[r.source]
            value = f"{pr.names.get(s.kind, s.kind)} {s.number or s.id}"
        elif r.state == "optional":
            value = "optional, none"
        elif r.state in ("missing", "pending"):
            value = r.note
        else:
            value = "choose a source"
        tip = f"{r.slot.label}: {r.note} · {pr.slot_gloss.get(r.slot.label, '')}"
        bracket = i > 0 and r.slot.bundle and r.state == "automatic" and r.source == main_src
        sockets.append(SocketSpec(r.slot, r.state, value, tip, bracket=bracket))
    method = pr.method_label(st.method) if st.method else pr.tool.get(st.adding, "")
    return CardSpec(
        st.adding,
        pr.names.get(st.adding, st.adding),
        "new",
        "new",
        method,
        sockets,
        pr.makes.get(st.adding, ""),
        ghost=True,
    )


# ── pieces ───────────────────────────────────────────────────────────────────


def light(status: str, size: int = 8) -> ui.element:
    """The status light: a filled dot, pulsing when running, hollow for a job not yet added."""
    base = (
        f"width: {size}px; height: {size}px; border-radius: var(--cb-pip-radius, 50%); flex-shrink: 0; "
        "box-sizing: border-box;"
    )
    if status == "new":
        return ui.element("div").style(f"{base} border: 1.5px solid {SLATE};")
    el = ui.element("div").style(f"{base} background: {STATUS.get(status, '#9ca3af')};")
    if status == "running":
        el.classes("cb-bm-pulse")
    return el


def port(cx: float, cy: float, state: str, *, tip: str | None = None, on_click=None, lit: bool = False) -> ui.element:
    dashed = " border-style: dashed;" if state == "missing" else ""
    halo = f" box-shadow: 0 0 0 3px color-mix(in srgb, {AMBER} 33%, transparent);" if lit else ""
    el = (
        ui.element("div")
        .classes("cb-bm-port")
        .style(
            f"position: absolute; left: {cx - PORT / 2}px; top: {cy - PORT / 2}px; width: {PORT}px; height: {PORT}px; "
            f"border-radius: var(--cb-bm-port-radius, 3px); box-sizing: border-box; background: {PORT_FILL[state]}; "
            f"border: 2px solid {PORT_RING[state]}; z-index: 3; cursor: {'pointer' if on_click else 'default'};"
            f"{dashed}{halo}"
        )
    )
    if tip:
        el.tooltip(tip)
    if on_click:
        el.on("click.stop", on_click)
    return el


def _bracket_el(y0: float, y1: float, color: str) -> None:
    """Joins an input port to the one above it on the card's edge: the lower one reads the same job, riding its
    cable. Drawn inside the card so it moves with it."""
    w = PORT / 2 + 6
    h = y1 - y0 + 4
    d = f"M {w} 2 L 3 2 Q 0 2 0 5 L 0 {h - 5} Q 0 {h - 2} 3 {h - 2} L {w} {h - 2}"
    ui.html(
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display:block">'
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.5"/></svg>',
        sanitize=False,
    ).style(f"position: absolute; left: {-w}px; top: {y0 - 2}px; width: {w}px; height: {h}px; z-index: 2;")


def badge(content: str, *, tip: str | None = None) -> ui.element:
    """The tool or method, explicit: a small grey badge, its gloss on hover."""
    el = hrow(
        0,
        "height: 14px; padding: 0 5px; border-radius: var(--cb-tag-radius, 3px); "
        "border: 1px solid var(--cb-tag-line, #cfd8e3); background: var(--cb-tag, #f1f5f9); flex-shrink: 0;",
    )
    with el:
        ui.label(content).style(f"{voice('tag', 9, BODY, track='0.01em')} line-height: 1.3; white-space: nowrap;")
    if tip:
        el.tooltip(tip)
    return el


def _card_box(
    box: Box, *, x: float | None, y: float | None, border: str, background: str, dim: bool, halo: str = ""
) -> ui.element:
    place = f"position: absolute; left: {x}px; top: {y}px;" if x is not None else "position: relative;"
    return (
        ui.element("div")
        .classes("cb-bm-card")
        .style(
            f"{place} width: {box.w}px; height: {box.h}px; box-sizing: border-box; border: {border}; "
            f"border-radius: var(--cb-bm-card-radius, 8px); background: {background}; "
            "display: flex; flex-direction: column; "
            f"box-shadow: {CARD_SHADOW}; opacity: {0.3 if dim else 1}; "
            f"transition: opacity .15s, box-shadow .15s; {halo}"
        )
    )


def _small_button(icon: str, *, cls: str, tip: str, color: str, extra: str = "") -> ui.element:
    el = (
        ui.element("div")
        .classes(cls)
        .style(
            "width: 16px; height: 16px; border-radius: 4px; display: flex; align-items: center; "
            f"justify-content: center; cursor: pointer; color: {color}; flex-shrink: 0; {extra}"
        )
    )
    with el:
        ui.icon(icon, size="13px")
    el.tooltip(tip)
    return el


def header(
    pr: Project,
    spec: CardSpec,
    *,
    on_open=None,
    on_size=None,
    large: bool = False,
    on_method=None,
    on_menu=None,
    on_discard=None,
    on_add=None,
    todo: list[str] = (),
) -> None:
    """The header band: the light, the type's name, the tool or method as a badge, the job number, the size
    toggle. A click on it opens the job's page (`on_open`); the number opens the job's menu (`on_menu`). The new
    job's header carries its method menu and the ✕ / ✓ instead."""
    row = hrow(
        0,
        f"height: {HEAD_H}px; padding: 0 {SIDE - 2}px 0 {SIDE + 2}px; background: {BAND}; "
        f"border-bottom: 1px solid {HAIR}; border-radius: {INNER_R} {INNER_R} 0 0; box-sizing: border-box;",
    ).classes("cb-bm-head")
    with row:
        with ui.element("div").style(
            f"width: {TEXT_X - SIDE - 2}px; display: flex; align-items: center; flex-shrink: 0;"
        ):
            light(spec.status)
        text(
            ("+ " if spec.ghost else "") + spec.title,
            12,
            INK,
            bold=True,
            extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 0 1 auto; min-width: 0; "
            "letter-spacing: -0.01em; margin-right: 7px;",
        )
        if spec.ghost and spec.method and on_method:
            tokel = hrow(
                3,
                "height: 14px; padding: 0 5px; border-radius: var(--cb-tag-radius, 3px); "
                "border: 1px solid var(--cb-btn-line, #b8cce0); background: #fff; cursor: pointer; flex-shrink: 0;",
            ).classes("cb-bm-tok")
            with tokel:
                text(spec.method, 9, INK, bold=True)
                text("▾", 8, SOFT)
            on_method(tokel)
        elif spec.method:
            badge(spec.method, tip=pr.gloss.get(spec.method))
        ui.element("div").style("flex: 1 1 0;")
        if spec.ghost:
            todo_tip = ("to settle: " + ", ".join(todo)) if todo else "every input and parameter is settled"
            x = _small_button("close", cls="cb-bm-x", tip="Discard this job", color=RED)
            if on_discard:
                x.on("click.stop", on_discard)
            ok = _small_button(
                "check",
                cls="cb-bm-ok" + ("" if todo else " cb-bm-ok-ready"),
                tip=("Add to the project · " if not todo else "Not yet · ") + todo_tip,
                color="#fff" if not todo else tok("off-dot", "#cbd5e1"),
                extra=(f"background: {GREEN}; margin-left: 2px;" if not todo else "cursor: default; margin-left: 2px;"),
            )
            if on_add and not todo:
                ok.on("click.stop", on_add)
        else:
            if spec.number:
                num = text(spec.number, 10, MUTED, mono=True, extra="flex-shrink: 0; margin-left: 4px;")
                if on_menu:
                    num.classes("cb-bm-num").style("cursor: pointer; padding: 0 3px; border-radius: 3px;")
                    num.tooltip("Open, clone or remove")
                    on_menu(num)
            if on_size:
                btn = (
                    ui.element("div")
                    .classes("cb-bm-size")
                    .style(
                        "width: 14px; height: 14px; display: flex; align-items: center; justify-content: center; "
                        f"cursor: pointer; flex-shrink: 0; color: {MUTED}; margin-left: 5px;"
                    )
                )
                with btn:
                    ui.icon("close_fullscreen" if large else "open_in_full", size="10px")
                btn.tooltip(
                    "Back to the small card" if large else "Parameters, results and the tilt series on the card"
                )
                btn.on("click.stop", on_size)
    if on_open:
        row.style("cursor: pointer;")
        row.tooltip("Open the job's page")
        row.on("click.stop", on_open)


def output_row(makes: str, *, on_plus=None) -> None:
    with hrow(
        0,
        f"height: {OUT_H}px; padding: 0 10px 0 {SIDE + 2}px; margin-top: auto; background: {BAND}; "
        f"border-top: 1px solid {HAIR}; border-radius: 0 0 {INNER_R} {INNER_R}; box-sizing: border-box;",
    ):
        text("→", 10, MUTED, extra=f"flex-shrink: 0; width: {TEXT_X - SIDE - 2}px;")
        text(
            makes,
            10,
            SOFT,
            extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1 1 0; min-width: 0;",
        )
        if on_plus:
            add = (
                ui.element("div")
                .classes("cb-bm-add")
                .style(
                    "width: 16px; height: 16px; border-radius: var(--cb-field-radius, 4px); "
                    "border: 1px solid var(--cb-btn-line, #b8cce0); background: #fff; "
                    "display: flex; align-items: center; justify-content: center; cursor: pointer; flex-shrink: 0; "
                    f"color: {SOFT};"
                )
            )
            with add:
                ui.icon("add", size="12px")
            add.tooltip(f"Add a job that reads these {makes}")
            on_plus(add)


# ── the magazine: one mark per tilt series ───────────────────────────────────


def series_states(pr: Project, j: Job) -> list[str]:
    """One state per tilt series: as the job's task markers say, else from the counts."""
    if j.statuses is not None:
        return j.statuses
    total = pr.series_count(j)
    fail = j.total - j.ok if j.status in ("failed", "done") else 0
    running = min(3, j.total - j.ok) if j.status == "running" else 0
    out = []
    for i in range(total):
        if i < j.ok:
            out.append("ok")
        elif i < j.ok + fail:
            out.append("fail")
        elif i < j.ok + fail + running:
            out.append("running")
        else:
            out.append("pending")
    return out


def strip_svg(pr: Project, j: Job, sp: StripSpec, w: float, *, active: str | None = None) -> str:
    """One square per tilt series: done green, failed red, running blue and blinking, waiting grey. Each names
    its tilt series and state on hover and carries the id (`data-ts`), so a click picks it; the picked one is
    ringed."""
    states = series_states(pr, j)
    names = pr.ts_ids if len(pr.ts_ids) == len(states) else [f"TS_{i + 1:03d}" for i in range(len(states))]
    parts = []
    ring_at = None
    for i, state in enumerate(states):
        x = (i % sp.per_row) * (sp.bw + sp.gap)
        y = (i // sp.per_row) * (sp.bh + sp.row_gap)
        fill, word = BOX_COLORS.get(state, BOX_COLORS["pending"])
        cls = ' class="cb-bm-run"' if state == "running" else ""
        rx = 1 if sp.bw >= 4 else 0
        parts.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{sp.bw:.2f}" height="{sp.bh:.2f}" rx="{rx}" fill="{fill}"{cls} '
            f'data-ts="{names[i]}"><title>{pr.ts_title(names[i])} · {names[i]} · {word}</title></rect>'
        )
        if active is not None and names[i] == active:
            ring_at = (x, y)
    if ring_at is not None:
        x, y = ring_at
        parts.append(
            f'<rect x="{x - 1.5:.2f}" y="{y - 1.5:.2f}" width="{sp.bw + 3:.2f}" height="{sp.bh + 3:.2f}" rx="2" '
            f'fill="none" stroke="{INK}" stroke-width="1.5"/>'
        )
    return svg(w, sp.height, parts)


def strip_band(pr: Project, st: MapState) -> float:
    """The room above every card for its magazine, one height for the whole map."""
    return strip_spec(pr.max_series, CARD_W).height + STRIP_GAP if st.strips else 0


def strip_above(pr: Project, j: Job, box: Box, st: MapState) -> ui.element:
    """The magazine as the card's escort: one or two rows of marks hovering above it, moving with it."""
    sp = strip_spec(pr.series_count(j), box.w)
    used = sp.width(pr.series_count(j))
    return ui.html(strip_svg(pr, j, sp, used, active=st.active_ts), sanitize=False).style(
        f"position: absolute; left: 0; top: {-(sp.height + STRIP_GAP)}px; width: {used}px; height: {sp.height}px; "
        "line-height: 0;"
    )


# ── the card ─────────────────────────────────────────────────────────────────


def render_card(
    pr: Project,
    spec: CardSpec,
    box: Box,
    st: MapState,
    *,
    x: float | None = None,
    y: float | None = None,
    middle: Callable[[], None] | None = None,
    on_socket=None,
    on_list=None,
    on_plus=None,
    on_method=None,
    on_open=None,
    on_size=None,
    on_menu=None,
    on_discard=None,
    on_add=None,
    todo: list[str] = (),
    candidate: bool = False,
    chosen: bool = False,
    dim: bool = False,
    on_chain: bool = False,
    open_slot: str | None = None,
    use_label: str | None = None,
    on_use=None,
) -> ui.element:
    """One job card. Absolutely placed when x and y are given, in the flow otherwise. `middle` draws the block
    between the input rows and the output row (`box.extra` is its height). `on_socket(key)` is a click on an input
    row or port, `on_list(element, key)` hangs a list of candidates on a row's ▾, `on_plus(element)` hangs the add
    menu on the output row's button, `on_method(element)` a method menu on the new card's badge, `on_open()` a
    click on the header, `on_size()` on the size toggle, `on_menu(element)` the number's menu, `on_discard` /
    `on_add` the new card's ✕ / ✓ (`todo` names what still blocks the ✓); `use_label` grows a "use as …" pill at
    the output of a candidate, `on_use()` its click."""
    large = box.w == LARGE_W
    if spec.ghost:
        border, background = f"1.5px dashed {SLATE}", "#fff"
    elif chosen:
        border, background = f"2px solid {BLUE}", tok("bm-chosen", "#f5f9ff")
    elif candidate:
        border, background = f"1.5px solid {BLUE}", "#fff"
    else:
        border, background = f"1px solid {CARD_BORDER}", "#fff"
    halo = f"outline: 4px solid color-mix(in srgb, {BLUE} 12%, transparent);" if candidate else ""
    card = _card_box(box, x=x, y=y, border=border, background=background, dim=dim, halo=halo)
    if candidate:
        card.style("cursor: pointer;")
    if spec.job is not None and not spec.job.on_roster:
        card.style("opacity: 0.55;")
        card.tooltip("Not on the roster: kept for the jobs that read it")
    with card:
        if box.band and spec.job is not None:
            strip_above(pr, spec.job, box, st)
        header(
            pr,
            spec,
            on_open=on_open,
            on_size=on_size,
            large=large,
            on_method=on_method,
            on_menu=on_menu,
            on_discard=on_discard,
            on_add=on_add,
            todo=todo,
        )
        # One row per input: its name, and the job it reads.
        for sk in spec.sockets:
            is_open = open_slot == sk.slot.key
            row = hrow(
                6,
                f"height: {SLOT_H}px; padding: 0 10px 0 {TEXT_X}px; "
                f"background: {tok('bm-open', '#fffbeb') if is_open else 'transparent'}; "
                f"cursor: {'pointer' if on_socket else 'default'};",
            ).classes("cb-bm-row")
            with row:
                label(
                    sk.slot.label,
                    8,
                    MUTED,
                    extra=f"width: {LABEL_W}px; flex-shrink: 0; white-space: nowrap; overflow: hidden; "
                    "text-overflow: ellipsis;",
                )
                text(
                    sk.value,
                    10,
                    VALUE_COLOR.get(sk.state, INK),
                    bold=sk.state in ("ambiguous", "explicit"),
                    extra="flex: 1 1 0; min-width: 0; white-space: nowrap; overflow: hidden; "
                    "text-overflow: ellipsis;" + (" opacity: .7;" if sk.bracket or sk.slot.quiet else ""),
                )
                if on_list and not sk.slot.bundle and sk.state in ("ambiguous", "explicit", "automatic", "wired"):
                    lst = (
                        ui.element("div")
                        .classes("cb-bm-list")
                        .style(
                            "width: 16px; height: 14px; border-radius: 3px; display: flex; align-items: center; "
                            "justify-content: center; cursor: pointer; flex-shrink: 0;"
                        )
                    )
                    with lst:
                        text("▾", 9, SOFT)
                    on_list(lst, sk.slot.key)
            row.tooltip(sk.tip)
            if on_socket:
                row.on("click.stop", lambda _e, k=sk.slot.key: on_socket(k))
        if middle is not None:
            with ui.element("div").style(
                f"height: {box.extra}px; box-sizing: border-box; margin: 0 {SIDE}px; display: flex; "
                "flex-direction: column; overflow: hidden;"
            ):
                middle()
        # The input ports on the left edge, a bracket where a row rides the one above, the output row and its port.
        for i, sk in enumerate(spec.sockets):
            port(
                0,
                port_y(i),
                sk.state,
                on_click=(lambda _e, k=sk.slot.key: on_socket(k)) if on_socket else None,
                lit=open_slot == sk.slot.key,
            )
            if sk.bracket:
                _bracket_el(port_y(0), port_y(i), BLUE if on_chain or spec.ghost else tok("off-dot", "#cbd5e1"))
        output_row(spec.makes, on_plus=None if spec.ghost else on_plus)
        oy = out_y_of(box.h)
        port(box.w, oy, "output")
        if use_label:
            pill = hrow(
                4,
                f"position: absolute; left: {box.w + 9}px; top: {oy - 9}px; height: 18px; padding: 0 8px; "
                f"border-radius: var(--cb-bm-chip-radius, 9px); background: {BLUE}; color: #fff; white-space: nowrap; "
                "cursor: pointer; z-index: 4; box-shadow: 0 2px 6px rgba(37,99,235,0.35);",
            ).classes("cb-bm-pill")
            with pill:
                text(use_label, 9, "#fff", bold=True)
            if on_use:
                pill.on("click.stop", lambda _e: on_use())
    return card


def trunk_box(pr: Project, shared: list[str], st: MapState) -> Box:
    return Box(CARD_W, card_height(len(shared)), 0, strip_band(pr, st))


def render_trunk(
    pr: Project, shared: list[str], box: Box, st: MapState, *, x: float, y: float, on_plus, on_click, on_open, dim: bool
) -> ui.element:
    """The shared start of every chain, folded into one card: the jobs it stands for are its rows, each a link to
    its page; a click elsewhere on the card unfolds it."""
    card = _card_box(box, x=x, y=y, border=f"1px solid {CARD_BORDER}", background="#fff", dim=dim)
    card.style("cursor: pointer;")
    with card:
        last = pr.by_id[shared[-1]]
        if box.band:
            strip_above(pr, last, box, st)
        first = pr.by_id[shared[0]]
        num = f"{first.number}–{last.number}" if first.number and last.number else ""
        header(pr, CardSpec("trunk", "Shared start", num, "done", "", [], ""))
        for t in shared:
            tj = pr.by_id[t]
            row = hrow(0, f"height: {SLOT_H}px; padding: 0 {SIDE}px 0 {SIDE + 2}px;").classes("cb-bm-row")
            with row:
                with ui.element("div").style(f"width: {TEXT_X - SIDE - 2}px; display: flex; align-items: center;"):
                    light(tj.status, 6)
                text(pr.names.get(tj.kind, tj.kind), 10, BODY, extra="flex: 1 1 0; margin-right: 6px;")
                badge(pr.method_of(tj), tip=pr.gloss.get(pr.method_of(tj)))
                text(tj.number, 10, MUTED, mono=True, extra="margin-left: 6px;")
            row.tooltip(f"Open {pr.ident(tj)}")
            row.on("click.stop", lambda _e, iid=t: on_open(iid))
        output_row(pr.makes.get(last.kind, ""), on_plus=on_plus)
        port(CARD_W, out_y_of(box.h), "output")
        ui.tooltip(
            " → ".join(pr.names.get(pr.by_id[t].kind, t) for t in shared) + ", folded into one. Click to unfold."
        ).props(":delay='350'")
    card.on("click", on_click)
    return card
