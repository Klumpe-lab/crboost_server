"""The job being added, as a card on the map: how its inputs resolve as they are chosen, and its card, a real
form (its method, its parameters typed on it, the ✕ that discards it and the ✓ that adds it). Every other card is
an HTML string (ui/branch_map/html.py); this one is NiceGUI elements wearing the same classes, so it looks the same
and its cables land the same way."""

from __future__ import annotations

from dataclasses import dataclass

from nicegui import ui

from ui.branch_map.blocks import Section, draw_section
from ui.branch_map.geometry import HEAD_H, SIDE, TEXT_X
from ui.branch_map.model import Job, Project, Slot
from ui.branch_map.state import MapState
from ui.branch_map.style import BAND, BODY, GREEN, HAIR, INK, INNER_R, RED, SLATE, SOFT, STATUS, hrow, text, tok, voice


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
    bracket: bool = False  # reads the same job as the main row: it rides the main cable


@dataclass
class CardSpec:
    """What the new job's card shows."""

    kind: str
    title: str
    number: str
    status: str  # new
    method: str
    sockets: list[SocketSpec]
    makes: str
    job: Job | None = None
    ghost: bool = False


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


def header(pr: Project, spec: CardSpec, *, on_method, on_discard, on_add, todo: list[str]) -> ui.element:
    """The new job's header band: the hollow light, "+ " and the type's name, its method as a menu (`on_method`
    hangs it on the badge), the ✕ that discards it and the ✓ that adds it (`todo` names what still blocks it)."""
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
            "+ " + spec.title,
            12,
            INK,
            bold=True,
            extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 0 1 auto; min-width: 0; "
            "letter-spacing: -0.01em; margin-right: 7px;",
        )
        if spec.method and on_method:
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
        todo_tip = ("to settle: " + ", ".join(todo)) if todo else "every input and parameter is settled"
        x = _small_button("close", cls="cb-bm-x", tip="Discard this job", color=RED)
        x.on("click.stop", on_discard)
        ok = _small_button(
            "check",
            cls="cb-bm-ok" + ("" if todo else " cb-bm-ok-ready"),
            tip=("Add to the project · " if not todo else "Not yet · ") + todo_tip,
            color="#fff" if not todo else tok("off-dot", "#cbd5e1"),
            extra=(f"background: {GREEN}; margin-left: 2px;" if not todo else "cursor: default; margin-left: 2px;"),
        )
        if not todo:
            ok.on("click.stop", on_add)
    return row


# ── the new job's card ───────────────────────────────────────────────────────


def render_new_card(
    pr: Project, spec: CardSpec, *, sections: list[Section], on_socket, on_method, on_discard, on_add, todo: list[str]
) -> ui.element:
    """The job being added: its header (the method menu, ✕, ✓), a row per input (a click chooses what it reads,
    `on_socket(key)`), its parameters as the job tab's fields."""
    card = ui.element("div").classes("cb-bm-card cb-bm-newcard")
    with card:
        header(pr, spec, on_method=on_method, on_discard=on_discard, on_add=on_add, todo=todo)
        for i, sk in enumerate(spec.sockets):
            cls = f"cb-bm-row s-{sk.state} edit"
            if i > 0 and sk.state in ("explicit", "automatic") and not sk.bracket and not sk.slot.quiet:
                cls += " side"
            if sk.bracket or sk.slot.quiet:
                cls += " rides"
            row = ui.element("div").classes(cls)
            row.props["data-slot"] = sk.slot.key
            row.props["title"] = sk.tip + " · click to choose what it reads"
            with row:
                ui.label(sk.slot.label).classes("cb-bm-lbl")
                ui.label(sk.value).classes(f"cb-bm-val v-{sk.state}")
            row.on("click", lambda _e, k=sk.slot.key: on_socket(k))
        if sections:
            with ui.element("div").style(f"margin: 0 {SIDE}px; display: flex; flex-direction: column;"):
                for lbl, parts in sections:
                    draw_section(lbl, parts)
    return card
