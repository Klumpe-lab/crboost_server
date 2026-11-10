"""The branch map worked out: one job card as the visual language of every job type, and the map with a job being
added to it. Every card has the same anatomy (a header with the status light, type and number; a line naming the
method and what sets the job apart; one labelled socket per input on its left edge; one output socket on its right
edge that pulls a new job out of it); what varies is the method token, the parameters and how many sockets.

The "+" on a card's output offers the job types that can read that output, saying as which input; the new job
appears as a dashed card hanging off the card, its sockets filled where the choice is forced, amber where it is
not, red where nothing exists yet; clicking a socket fans its candidates out on the map itself, cables and all.

Static mock data (the project of `ui/tree_ideas_preview.py`). Clicks change only this page.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import pairwise

from nicegui import ui

from ui.components.buttons import house_button
from ui.io_ideas_preview import AMBER, BLUE, BODY, FONT, GREEN, INK, LINE, MUTED, RED, SOFT, _dot, _t
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.tree_ideas_preview import (
    BY_ID,
    JOBS,
    NAMES,
    PRODUCES,
    STAGES,
    STATUS,
    Job,
    chips,
    dossier,
    ident,
    main_children,
    path,
    pedigree,
    trunk,
    ts_text,
)

NEW = "new"
SLATE = "#64748b"
HEAD_BG = "#eef3f8"
CARD_BORDER = "#c3cfdc"


# ── what each job type reads and how it is named ─────────────────────────────


@dataclass(frozen=True)
class Slot:
    """One input of a job type. `bundle`: normally read from the same job as the first (main) slot, so it rides
    that choice."""

    key: str
    label: str
    accepts: tuple[str, ...]
    required: bool = True
    bundle: bool = False


SLOTS: dict[str, tuple[Slot, ...]] = {
    "import": (),
    "motion": (Slot("frames", "frames", ("frames",)),),
    "filter": (Slot("tilt_series", "tilt series", ("motion",)),),
    "align": (Slot("tilt_series", "tilt series", ("motion",)),),
    "refine": (Slot("alignment", "alignment", ("aligned",)), Slot("warp_dir", "warp dir", ("aligned",), bundle=True)),
    "ctf": (Slot("alignment", "alignment", ("aligned",)), Slot("warp_dir", "warp dir", ("aligned",), bundle=True)),
    "subset": (Slot("output", "per-TS output", ("aligned", "ctf", "tomograms")),),
    "recon": (Slot("tilt_series", "tilt series", ("ctf",)), Slot("warp_dir", "warp dir", ("ctf",), bundle=True)),
    "dtrain": (Slot("tomograms", "tomograms", ("tomograms",)),),
    "dpredict": (
        Slot("tomograms", "tomograms", ("tomograms",)),
        Slot("model", "model", ("model",)),
        Slot("warp_dir", "warp dir", ("tomograms",), bundle=True),
    ),
    "segment": (Slot("tomograms", "tomograms", ("tomograms", "denoised")),),
    "tm": (
        Slot("tomograms", "tomograms", ("tomograms", "denoised")),
        Slot("tilt_series", "tilt series", ("ctf",)),
        Slot("segmentations", "segmentations", ("segmentations",), required=False),
    ),
}
PRODUCES2 = PRODUCES | {"import": "frames", "motion": "motion", "filter": "motion"}
OUTPUT_WORD = {
    "frames": "frames",
    "motion": "motion-corrected tilt series",
    "aligned": "aligned tilt series",
    "ctf": "CTF'd tilt series",
    "tomograms": "tomograms",
    "model": "denoising model",
    "denoised": "denoised tomograms",
    "segmentations": "segmentations",
}
# The methods a type can run with; the first is the default of a new job.
METHODS = {
    "filter": ("DL model", "by hand"),
    "align": ("AreTomo", "IMOD"),
    "dtrain": ("cryoCARE", "IsoNet"),
    "dpredict": ("cryoCARE", "IsoNet", "easymode"),
    "segment": ("Ais", "MemBrain-seg"),
    "tm": ("PyTOM",),
}
# The tool a type runs when it has no method of its own.
TOOL = {
    "import": "RELION",
    "motion": "WarpTools",
    "refine": "miss-alignment",
    "ctf": "WarpTools",
    "subset": "cut",
    "recon": "WarpTools",
}
ICON = {
    "import": "download",
    "motion": "motion_photos_on",
    "filter": "filter_alt",
    "align": "center_focus_weak",
    "refine": "tune",
    "ctf": "adjust",
    "subset": "content_cut",
    "recon": "view_in_ar",
    "dtrain": "model_training",
    "dpredict": "auto_fix_high",
    "segment": "texture",
    "tm": "my_location",
}
ADDABLE = ("align", "refine", "ctf", "subset", "recon", "dtrain", "dpredict", "segment", "tm")


def slots_of(kind: str, method: str | None = None) -> tuple[Slot, ...]:
    """A type's inputs, given its method: an easymode denoising needs no trained model."""
    base = SLOTS[kind]
    if kind == "dpredict" and method == "easymode":
        return tuple(s for s in base if s.key != "model")
    return base


def method_of(j: Job) -> str:
    return j.params.get("method") or TOOL.get(j.kind, "")


def identity(j: Job) -> list[str]:
    """What sets the job apart besides its method."""
    return [c for c in chips(j) if c != j.params.get("method")]


def source_of(j: Job, slot: Slot, index: int) -> str | None:
    """Which job an existing job's input reads: the main lineage for the first slot and the bundled ones, else
    the side input of the accepted kind."""
    if index == 0 or slot.bundle:
        return j.parent
    return next((s for s in j.side if PRODUCES2.get(BY_ID[s].kind) in slot.accepts), None)


def same_kind(j: Job) -> list[Job]:
    return [x for x in JOBS if x.kind == j.kind]


# ── the new job and how its sockets resolve ──────────────────────────────────


@dataclass
class MapState:
    adding: str | None = None  # the kind of the job being added
    method: str | None = None
    picks: dict[str, str] = field(default_factory=dict)  # slot key → job id, set by hand
    open_slot: str | None = None  # the socket whose candidates are fanned out
    density: str = "cards"
    trunk: bool = True


@dataclass
class Resolved:
    """One socket of the new job: `state` is explicit / automatic / ambiguous / missing / optional, `source`
    the job it reads, `candidates` every job it could read, `note` why."""

    slot: Slot
    state: str
    source: str | None
    candidates: list[Job]
    note: str


def candidates_for(slot: Slot) -> list[Job]:
    return [j for j in JOBS if PRODUCES2.get(j.kind) in slot.accepts]


def resolve(st: MapState) -> list[Resolved]:
    """The new job's sockets. The first slot is the main one; a bundled slot rides it; any other slot is filled by
    itself when exactly one candidate sits on the main choice's chain (the train job of this reconstruction, the
    CTF these tomograms came from), asked when several do, flagged when none does."""
    if st.adding is None:
        return []
    out: list[Resolved] = []
    main_src: str | None = None
    for i, slot in enumerate(slots_of(st.adding, st.method)):
        cands = candidates_for(slot)
        picked = st.picks.get(slot.key)
        if picked is not None:
            out.append(Resolved(slot, "explicit", picked, cands, "set by hand"))
            if i == 0:
                main_src = picked
            continue
        if i == 0:
            out.append(Resolved(slot, "ambiguous", None, cands, f"pick one of {len(cands)}"))
            continue
        if slot.bundle:
            if main_src is not None and PRODUCES2.get(BY_ID[main_src].kind) in slot.accepts:
                out.append(Resolved(slot, "automatic", main_src, cands, f"with {out[0].slot.label}"))
            else:
                out.append(Resolved(slot, "ambiguous", None, cands, f"follows {out[0].slot.label}"))
            continue
        near = cands
        if main_src is not None:
            chain = {a.id for a in path(BY_ID[main_src])}
            # On the chain: an ancestor of the main choice, a job reading it, or a job descending from it.
            near = [c for c in cands if c.id in chain or c.parent == main_src or main_src in {a.id for a in path(c)}]
        if len(near) == 1:
            out.append(Resolved(slot, "automatic", near[0].id, cands, "the one on this chain"))
        elif near:
            out.append(Resolved(slot, "ambiguous", None, near, f"{len(near)} on this chain: pick one"))
        elif not slot.required:
            out.append(Resolved(slot, "optional", None, cands, "optional, none on this chain"))
        else:
            out.append(
                Resolved(
                    slot,
                    "missing",
                    None,
                    cands,
                    f"none reads this yet: add a {NAMES[cands[0].kind] if cands else 'job'}",
                )
            )
    return out


def offered_after(j: Job) -> list[tuple[str, Slot | None, str]]:
    """The job types the "+" on `j`'s output offers: (kind, the slot that would read it, or None with why not)."""
    out = []
    produced = PRODUCES2.get(j.kind)
    for kind in ADDABLE:
        if STAGES.index(kind) <= STAGES.index(j.kind) and kind != "subset":
            continue
        slot = next((s for s in slots_of(kind) if produced in s.accepts), None)
        if slot is None:
            wants = OUTPUT_WORD.get(slots_of(kind)[0].accepts[0], "something else")
            out.append((kind, None, f"reads {wants}, not {OUTPUT_WORD.get(produced, produced)}"))
        else:
            out.append((kind, slot, ""))
    return out


# ── the card ─────────────────────────────────────────────────────────────────

CARD_W = 144
HEAD_H, LINE_H, SLOT_H, PAD_B = 20, 15, 14, 4
SOCKET_FILL = {
    "wired": SLATE,
    "explicit": BLUE,
    "automatic": SLATE,
    "ambiguous": AMBER,
    "missing": "#fff",
    "optional": "#fff",
}
SOCKET_RING = {
    "wired": SLATE,
    "explicit": BLUE,
    "automatic": SLATE,
    "ambiguous": AMBER,
    "missing": RED,
    "optional": "#cbd5e1",
}


@dataclass
class SocketSpec:
    slot: Slot
    state: str
    value: str
    tip: str


@dataclass
class CardSpec:
    """Everything a card shows; built from a Job or from the new job."""

    title: str
    number: str
    icon: str
    status: str  # done / running / failed / queued / new
    method: str
    identity: list[str]
    tally: tuple[str, str]
    sockets: list[SocketSpec]
    ghost: bool = False


def card_height(n_sockets: int, density: str) -> int:
    rows = n_sockets * SLOT_H if density == "cards" else 0
    return HEAD_H + LINE_H + rows + PAD_B


def socket_y(i: int, n: int, density: str) -> float:
    """The vertical centre of socket `i` of `n` on a card's left edge."""
    if density == "cards":
        return HEAD_H + LINE_H + i * SLOT_H + SLOT_H / 2
    h = card_height(n, density)
    return h / 2 + (i - (n - 1) / 2) * 9


def spec_of(j: Job) -> CardSpec:
    sockets = []
    for i, slot in enumerate(slots_of(j.kind, j.params.get("method"))):
        src = source_of(j, slot, i)
        if src is None:
            sockets.append(
                SocketSpec(slot, "optional" if not slot.required else "missing", "—", f"{slot.label}: nothing")
            )
            continue
        s = BY_ID[src]
        value = f"{NAMES[s.kind]} {s.id.removeprefix('job')}"
        how = "with " + slots_of(j.kind)[0].label if slot.bundle else "set at submit"
        sockets.append(SocketSpec(slot, "wired", value, f"{slot.label} ← {ident(s)} · {how}"))
    return CardSpec(
        NAMES[j.kind], j.id.removeprefix("job"), ICON[j.kind], j.status, method_of(j), identity(j), ts_text(j), sockets
    )


def spec_of_new(st: MapState, resolved: list[Resolved]) -> CardSpec:
    sockets = []
    for r in resolved:
        if r.source is not None:
            s = BY_ID[r.source]
            value = f"{NAMES[s.kind]} {s.id.removeprefix('job')}"
        elif r.state == "optional":
            value = "optional"
        elif r.state == "missing":
            value = "none yet"
        else:
            value = r.note
        sockets.append(SocketSpec(r.slot, r.state, value, f"{r.slot.label}: {r.note}"))
    method = st.method or (METHODS.get(st.adding, ("",))[0])
    return CardSpec(
        NAMES[st.adding], "new", ICON[st.adding], "new", method, ["defaults"], ("", MUTED), sockets, ghost=True
    )


def render_card(
    spec: CardSpec,
    density: str,
    *,
    x: float | None = None,
    y: float | None = None,
    on_socket=None,
    on_plus=None,
    on_method=None,
    candidate: bool = False,
    chosen: bool = False,
    dim: bool = False,
    open_slot: str | None = None,
) -> ui.element:
    """One job card. Absolutely placed when x and y are given, in the flow otherwise."""
    n = len(spec.sockets)
    h = card_height(n, density)
    if spec.ghost:
        border = f"1.5px dashed {INK}"
    elif chosen:
        border = f"2px solid {BLUE}"
    elif candidate:
        border = f"1.5px dashed {BLUE}"
    else:
        border = f"1px solid {CARD_BORDER}"
    place = f"position: absolute; left: {x}px; top: {y}px;" if x is not None else "position: relative;"
    card = ui.element("div").style(
        f"{place} width: {CARD_W}px; height: {h}px; box-sizing: border-box; border: {border}; border-radius: 6px; "
        f"background: {'#eff6ff' if chosen else '#fff'}; display: flex; flex-direction: column; overflow: visible; "
        f"box-shadow: 0 1px 2px rgba(15,23,42,0.06); opacity: {0.35 if dim else 1}; "
        f"cursor: {'pointer' if candidate else 'default'};"
    )
    with card:
        # Header: status light, type glyph, type, number.
        with ui.element("div").style(
            f"height: {HEAD_H}px; display: flex; align-items: center; gap: 4px; padding: 0 6px 0 7px; "
            f"background: {'#f8fafc' if spec.ghost else '#dbeafe' if chosen else HEAD_BG}; border-radius: 5px 5px 0 0; "
            f"border-bottom: 1px solid {LINE};"
        ):
            if spec.status == "new":
                ui.element("div").style(
                    f"width: 7px; height: 7px; border-radius: 50%; border: 1.5px solid {SLATE}; "
                    "box-sizing: border-box; "
                    "flex-shrink: 0;"
                )
            elif spec.status == "running":
                ui.element("div").style(
                    f"width: 7px; height: 7px; border-radius: 50%; background: {STATUS['running']}; flex-shrink: 0; "
                    f"box-shadow: 0 0 0 3px {STATUS['running']}33;"
                )
            else:
                _dot(STATUS.get(spec.status, "#9ca3af"), 7)
            ui.icon(spec.icon, size="11px").style(f"color: {SLATE}; flex-shrink: 0;")
            _t(
                ("+ " if spec.ghost else "") + spec.title,
                9,
                INK,
                bold=True,
                extra="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1 1 0; min-width: 0;",
            )
            _t(spec.number, 8, MUTED, mono=True, extra="flex-shrink: 0;")
        # The method and what sets the job apart; the tilt-series tally at the right.
        with ui.element("div").style(
            f"height: {LINE_H}px; display: flex; align-items: center; gap: 4px; padding: 0 6px 0 7px; min-width: 0;"
        ):
            if spec.method:
                tok = ui.label(spec.method + (" ▾" if spec.ghost and on_method else "")).style(
                    f"{FONT} font-size: 8px; line-height: 11px; padding: 0 4px; border-radius: 3px; "
                    "white-space: nowrap; "
                    f"color: {INK}; background: #f1f5f9; border: 1px solid {LINE}; flex-shrink: 0; "
                    f"cursor: {'pointer' if spec.ghost and on_method else 'default'};"
                )
                if spec.ghost and on_method:
                    tok.on("click.stop", lambda _e: on_method())
                    tok.tooltip("The method: click to cycle through the ones this type runs with")
            _t(
                " · ".join(spec.identity),
                8,
                MUTED if spec.ghost else INK,
                bold=not spec.ghost,
                extra="flex: 1 1 0; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;",
            )
            if spec.tally[0]:
                _t(spec.tally[0], 8, spec.tally[1], mono=True, extra="flex-shrink: 0;")
        # Socket rows.
        if density == "cards":
            for sk in spec.sockets:
                is_open = open_slot == sk.slot.key
                row = ui.element("div").style(
                    f"height: {SLOT_H}px; display: flex; align-items: center; gap: 5px; padding: 0 6px 0 9px; "
                    f"min-width: 0; background: {'#fff7ed' if is_open else 'transparent'}; "
                    f"cursor: {'pointer' if on_socket else 'default'};"
                )
                with row:
                    _t(
                        sk.slot.label,
                        8,
                        SOFT,
                        extra="width: 52px; flex-shrink: 0; white-space: nowrap; overflow: hidden;",
                    )
                    color = {"ambiguous": AMBER, "missing": RED, "optional": MUTED, "explicit": BLUE}.get(sk.state, INK)
                    _t(
                        sk.value,
                        8,
                        color,
                        bold=sk.state in ("ambiguous", "explicit"),
                        extra="flex: 1 1 0; min-width: 0; white-space: nowrap; overflow: hidden; "
                        "text-overflow: ellipsis;",
                    )
                row.tooltip(sk.tip)
                if on_socket:
                    row.on("click.stop", lambda _e, k=sk.slot.key: on_socket(k))
        # The sockets on the left edge.
        for i, sk in enumerate(spec.sockets):
            cy = socket_y(i, n, density)
            r = 4.5
            sock = ui.element("div").style(
                f"position: absolute; left: {-r - 0.5}px; top: {cy - r}px; width: {2 * r}px; height: {2 * r}px; "
                f"border-radius: 50%; box-sizing: border-box; background: {SOCKET_FILL[sk.state]}; "
                f"border: 2px solid {SOCKET_RING[sk.state]}; "
                f"cursor: {'pointer' if on_socket else 'default'}; z-index: 2; "
                f"{'box-shadow: 0 0 0 3px ' + AMBER + '44;' if open_slot == sk.slot.key else ''}"
            )
            if density != "cards":
                sock.tooltip(sk.tip)
            if on_socket:
                sock.on("click.stop", lambda _e, k=sk.slot.key: on_socket(k))
        # The output socket: pulls a new job out of this one.
        if not spec.ghost:
            plus = ui.element("div").style(
                f"position: absolute; right: -7px; top: {h / 2 - 7}px; width: 14px; height: 14px; border-radius: 50%; "
                f"box-sizing: border-box; background: #fff; border: 1.5px solid {SLATE}; display: flex; "
                "align-items: center; "
                f"justify-content: center; cursor: {'pointer' if on_plus else 'default'}; z-index: 2;"
            )
            with plus:
                ui.label("+").style(
                    f"{FONT} font-size: 11px; line-height: 1; color: {SLATE}; font-weight: 600; margin-top: -1px;"
                )
            plus.tooltip("Add a job that reads this one")
            if on_plus:
                on_plus(plus)
    return card


# ── section A · the card as a language ───────────────────────────────────────

ANATOMY = (
    ("1", "status light: scheduled, queued, running, done, failed"),
    ("2", "the type's glyph, the type's name"),
    ("3", "job number; 'new' until it has one"),
    ("4", "method token: the tool or the method this instance runs with"),
    ("5", "what sets it apart: the parameters that differ between instances"),
    ("6", "tilt series done of its list"),
    (
        "7",
        "one socket per input, on the edge: filled = reads a job, amber = to pick, red = nothing yet, "
        "hollow = optional",
    ),
    ("8", "each input's name and the job it reads (hidden in the compact card; hover the socket)"),
    ("9", "the output socket: pulls a new job out of this one"),
)


def _specimens() -> list[tuple[Job, str]]:
    """Cards of the mock project that show the variation, with a caption each."""
    extra = [
        Job("job070", "tm", "job023", {"method": "PyTOM"}, "queued", 0, 46, "", side=("job022",)),
        Job("job071", "dpredict", "job030", {"method": "IsoNet"}, "queued", 0, 48, "", side=()),
    ]
    picks = [
        ("job001", "no inputs"),
        ("job003", "a method and a readout"),
        ("job004", "AreTomo, two failed"),
        ("job012", "the same type, IMOD"),
        ("job021", "a refinement"),
        ("job034", "a Subset"),
        ("job006", "two bundled inputs"),
        ("job069", "running; deconv set"),
        ("job010", "three inputs, one a model"),
        ("job031", "easymode: no model socket"),
        ("job041", "MemBrain-seg, failed"),
    ]
    out = [(BY_ID[i], cap) for i, cap in picks]
    out.append((extra[0], "queued; optional socket hollow"))
    out.append((extra[1], "IsoNet, no train job yet: red"))
    return out


def section_cards() -> None:
    _t("The card, enlarged", 10, INK, bold=True)
    with ui.element("div").style("display: flex; gap: 28px; align-items: flex-start; flex-wrap: wrap;"):
        spec = spec_of(BY_ID["job010"])
        scale = 1.9
        h = card_height(len(spec.sockets), "cards")
        with ui.element("div").style(
            f"width: {CARD_W * scale + 30}px; height: {h * scale + 10}px; position: relative; margin-left: 14px;"
        ):
            with ui.element("div").style(
                f"transform: scale({scale}); transform-origin: top left; position: absolute; left: 0; top: 0;"
            ):
                render_card(spec, "cards")
                marks = {
                    "1": (7, 0),
                    "2": (24, 0),
                    "3": (134, 0),
                    "4": (14, 20),
                    "5": (80, 20),
                    "6": (134, 20),
                    "7": (-14, 42),
                    "8": (152, 56),
                    "9": (152, h / 2 - 14),
                }
                for k, (mx, my) in marks.items():
                    ui.label(k).style(
                        f"position: absolute; left: {mx - 6}px; top: {my - 6}px; width: 12px; height: 12px; "
                        "border-radius: 50%; "
                        f"background: {BLUE}; color: #fff; {FONT} font-size: 7px; font-weight: 700; display: flex; "
                        "align-items: center; "
                        "justify-content: center; z-index: 5; box-shadow: 0 0 0 1.5px #fff;"
                    )
        with ui.column().style("gap: 3px; max-width: 520px; padding-top: 2px;"):
            for k, text in ANATOMY:
                with ui.element("div").style("display: flex; gap: 6px; align-items: baseline;"):
                    ui.label(k).style(
                        f"width: 12px; height: 12px; border-radius: 50%; background: {BLUE}; color: #fff; {FONT} "
                        "font-size: 7px; "
                        "font-weight: 700; display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    _t(text, 10, BODY)
            _t(
                "What never varies: the header, the light, the number's place, the method line, the output socket. "
                "What varies: the method token, the parameters named, the number of sockets. A different method of the "
                "same type is the same card with another token, and where the method changes the inputs, the sockets "
                "follow (easymode: no model).",
                9,
                SOFT,
                extra="margin-top: 6px;",
            )
    _t("Specimens", 10, INK, bold=True, extra="margin-top: 14px;")
    with ui.element("div").style(
        "display: flex; gap: 18px 16px; align-items: flex-start; flex-wrap: wrap; padding: 4px 0 0 6px;"
    ):
        for j, caption in _specimens():
            with ui.column().style("gap: 4px; align-items: flex-start;"):
                card = render_card(spec_of(j), "cards")
                dossier(card, j, same_kind(j)) if j.id in BY_ID else card.tooltip(caption)
                _t(caption, 8, MUTED)
    _t("Compact, the same cards", 10, INK, bold=True, extra="margin-top: 14px;")
    with ui.element("div").style(
        "display: flex; gap: 18px 16px; align-items: flex-start; flex-wrap: wrap; padding: 4px 0 0 6px;"
    ):
        for j, _caption in _specimens()[:8]:
            render_card(spec_of(j), "compact")


# ── section B · the map ──────────────────────────────────────────────────────

COL_PITCH = 158
GAP = 14
PAD = 14


def _tidy(roots: list[str], kids_of, height_of) -> dict[str, float]:
    """Top of every node: leaves stacked in depth-first order, a parent centred on its children."""
    top: dict[str, float] = {}
    cursor = [0.0]

    def place(nid: str) -> None:
        kids = kids_of(nid)
        if not kids:
            top[nid] = cursor[0]
            cursor[0] += height_of(nid) + GAP
            return
        for k in kids:
            place(k)
        first, last = kids[0], kids[-1]
        centre = ((top[first] + height_of(first) / 2) + (top[last] + height_of(last) / 2)) / 2
        top[nid] = centre - height_of(nid) / 2

    for r in roots:
        place(r)
    return top


def _cubic(x0: float, y0: float, x1: float, y1: float) -> str:
    mx = (x0 + x1) / 2
    return f"M {x0} {y0} C {mx} {y0}, {mx} {y1}, {x1} {y1}"


def section_map(st: MapState, redraw) -> None:
    resolved = resolve(st)
    by_key = {r.slot.key: r for r in resolved}
    main = resolved[0] if resolved else None
    shared = trunk() if st.trunk else []
    density = st.density

    def anchor(nid: str) -> str:
        return "trunk" if nid in shared else nid

    # ── which nodes, their heights and kids ──
    specs: dict[str, CardSpec] = {j.id: spec_of(j) for j in JOBS if j.id not in shared}
    if st.adding:
        specs[NEW] = spec_of_new(st, resolved)
    heights = {nid: card_height(len(sp.sockets), density) for nid, sp in specs.items()}
    if st.trunk:
        heights["trunk"] = HEAD_H + LINE_H + (len(shared) * SLOT_H if density == "cards" else 0) + PAD_B

    def kids_of(nid: str) -> list[str]:
        if nid == NEW:
            return []
        base = shared[-1] if nid == "trunk" else nid
        kids = sorted(main_children(BY_ID[base]), key=lambda k: (STAGES.index(k.kind), k.id))
        ids = [k.id for k in kids]
        if main is not None and main.source == base:
            ids.append(NEW)
        return ids

    roots = ["trunk" if st.trunk else "job001"]
    if st.adding and (main is None or main.source is None):
        roots.append(NEW)
    top = _tidy(roots, kids_of, heights.__getitem__)
    stage_of = {nid: "trunk" if nid == "trunk" else st.adding if nid == NEW else BY_ID[nid].kind for nid in top}
    present = [s for s in ("trunk", *STAGES) if s in set(stage_of.values())]
    col = {s: i for i, s in enumerate(present)}
    # Boxes of one column must not overlap: push down in order of their tops.
    for s in present:
        column = sorted((nid for nid in top if stage_of[nid] == s), key=top.__getitem__)
        for a, b in pairwise(column):
            if top[b] < top[a] + heights[a] + GAP:
                top[b] = top[a] + heights[a] + GAP
    shift = -min(top.values()) + PAD
    pos = {nid: (PAD + col[stage_of[nid]] * COL_PITCH, top[nid] + shift) for nid in top}
    width = PAD * 2 + len(present) * COL_PITCH - (COL_PITCH - CARD_W)
    height = max(pos[nid][1] + heights[nid] for nid in pos) + PAD + 16

    def out_xy(nid: str) -> tuple[float, float]:
        x, y = pos[nid]
        return x + CARD_W, y + heights[nid] / 2

    def in_xy(nid: str, i: int) -> tuple[float, float]:
        x, y = pos[nid]
        n = len(shared) if nid == "trunk" else len(specs[nid].sockets)
        return x, y + socket_y(i, n, density)

    # ── cables ──
    open_r = by_key.get(st.open_slot) if st.open_slot else None
    fan = {c.id for c in open_r.candidates} if open_r else set()
    lit = {a.id for a in path(BY_ID[main.source])} | {NEW} if main is not None and main.source else set()
    lit = {anchor(n) for n in lit}
    svg = []
    for nid, sp in specs.items():
        if nid == NEW:
            continue
        j = BY_ID[nid]
        for i, sk in enumerate(sp.sockets):
            src = source_of(j, sk.slot, i)
            if src is None:
                continue
            a = anchor(src)
            (x0, y0), (x1, y1) = out_xy(a), in_xy(nid, i)
            on = nid in lit and a in lit and (i == 0 or sk.slot.bundle)
            color = BLUE if on else ("#cbd5e1" if sk.slot.bundle else "#94a3b8")
            dash = ' stroke-dasharray="3 3"' if (i > 0 and not sk.slot.bundle) else ""
            svg.append(
                f'<path d="{_cubic(x0, y0, x1, y1)}" fill="none" stroke="{color}" '
                f'stroke-width="{2 if on else 1.2 if sk.slot.bundle else 1.4}"{dash}/>'
            )
    if st.adding:
        for i, r in enumerate(resolved):
            if r.source is None:
                continue
            (x0, y0), (x1, y1) = out_xy(anchor(r.source)), in_xy(NEW, i)
            dash = "" if r.state == "explicit" else ' stroke-dasharray="5 3"'
            svg.append(f'<path d="{_cubic(x0, y0, x1, y1)}" fill="none" stroke="{BLUE}" stroke-width="2"{dash}/>')
        if open_r is not None:
            i = [r.slot.key for r in resolved].index(open_r.slot.key)
            for c in open_r.candidates:
                if anchor(c.id) not in pos or c.id == open_r.source:
                    continue
                (x0, y0), (x1, y1) = out_xy(anchor(c.id)), in_xy(NEW, i)
                svg.append(
                    f'<path d="{_cubic(x0, y0, x1, y1)}" fill="none" stroke="#93c5fd" stroke-width="1.4" '
                    'stroke-dasharray="4 4"/>'
                )

    # ── handlers ──
    def on_socket(key: str) -> None:
        st.open_slot = None if st.open_slot == key else key
        redraw()

    def choose(nid: str) -> None:
        if open_r is None:
            return
        st.picks[open_r.slot.key] = nid
        # A new main choice lets the other sockets follow it again.
        if open_r is main:
            st.picks = {open_r.slot.key: nid}
        st.open_slot = next((r.slot.key for r in resolve(st) if r.state in ("ambiguous", "missing")), None)
        redraw()

    def start(kind: str, parent: str | None, slot: Slot | None) -> None:
        st.adding, st.method, st.picks = kind, METHODS.get(kind, (None,))[0], {}
        if parent is not None and slot is not None:
            st.picks[slot.key] = parent
        st.open_slot = next((r.slot.key for r in resolve(st) if r.state in ("ambiguous", "missing")), None)
        redraw()

    def cycle_method() -> None:
        opts = METHODS.get(st.adding, ())
        if len(opts) > 1:
            st.method = opts[(opts.index(st.method) + 1) % len(opts)]
            st.picks = {k: v for k, v in st.picks.items() if k in {s.key for s in slots_of(st.adding, st.method)}}
            redraw()

    def plus_menu(plus: ui.element, j: Job) -> None:
        with plus:
            menu = (
                ui.menu()
                .props('no-parent-event anchor="top right" self="top left"')
                .style(
                    f"border: 1px solid {LINE}; border-radius: 6px; box-shadow: 0 8px 24px rgba(15,23,42,0.14); "
                    "min-width: 250px;"
                )
            )
            with menu, ui.column().style("gap: 0; padding: 4px 0;"):
                _t(f"Add after {ident(j)}", 9, SOFT, bold=True, extra="padding: 3px 10px 4px;")
                for kind, slot, why in offered_after(j):
                    row = ui.element("div").style(
                        "display: flex; align-items: center; gap: 8px; padding: 4px 10px; "
                        f"cursor: {'pointer' if slot else 'default'}; "
                        f"opacity: {1 if slot else 0.5};"
                    )
                    with row:
                        ui.icon(ICON[kind], size="12px").style(f"color: {SLATE};")
                        _t(NAMES[kind], 10, INK, bold=bool(slot), extra="width: 110px;")
                        if slot:
                            others = [s for s in slots_of(kind) if s.key != slot.key and not s.bundle and s.required]
                            _t(
                                f"as its {slot.label}"
                                + (f" · also needs {', '.join(s.label for s in others)}" if others else ""),
                                9,
                                SOFT,
                            )
                        else:
                            _t(why, 9, MUTED)
                    if slot:
                        row.on("click", lambda _e, k=kind, s=slot, m=menu: (m.close(), start(k, j.id, s)))
        plus.on("click.stop", lambda _e, m=menu: m.open())

    # ── controls ──
    with ui.element("div").style("display: flex; align-items: center; gap: 6px; flex-wrap: wrap; margin-bottom: 10px;"):
        _t("Add a job", 9, SOFT, bold=True, extra="margin-right: 2px;")
        for kind in ADDABLE:
            house_button(
                NAMES[kind],
                lambda _e, k=kind: start(k, None, None),
                kind="accent" if st.adding == kind else "default",
                icon=ICON[kind],
                tooltip=f"A new {NAMES[kind]} with every input still to choose",
            )
        ui.element("div").style("width: 18px;")
        _t("Cards", 9, SOFT, bold=True)
        for d in ("cards", "compact"):
            house_button(
                d, lambda _e, d=d: (setattr(st, "density", d), redraw()), kind="accent" if density == d else "default"
            )
        house_button(
            "shared start folded" if st.trunk else "shared start unfolded",
            lambda _e: (setattr(st, "trunk", not st.trunk), redraw()),
        )

    # ── the canvas ──
    with ui.element("div").style(f"position: relative; width: {width}px; height: {height}px;"):
        ui.html(
            f'<svg width="{width}" height="{height}" style="position:absolute;left:0;top:0;">{"".join(svg)}</svg>',
            sanitize=False,
        )
        for s, i in col.items():
            if s != "trunk":
                ui.label(NAMES[s]).style(
                    f"position: absolute; left: {PAD + i * COL_PITCH}px; top: {height - 14}px; width: {CARD_W}px; "
                    "text-align: center; "
                    f"{FONT} font-size: 8px; color: {MUTED};"
                )
        for nid, (x, y) in pos.items():
            if nid == "trunk":
                h = heights["trunk"]
                box = ui.element("div").style(
                    f"position: absolute; left: {x}px; top: {y}px; width: {CARD_W}px; height: {h}px; "
                    f"box-sizing: border-box; border: 1px solid {CARD_BORDER}; border-radius: 6px; "
                    "background: #f8fafc; "
                    "cursor: pointer; display: flex; "
                    f"flex-direction: column; opacity: {0.35 if fan and 'trunk' not in fan else 1};"
                )
                with box:
                    with ui.element("div").style(
                        f"height: {HEAD_H}px; display: flex; align-items: center; gap: 4px; padding: 0 6px 0 7px; "
                        f"background: {HEAD_BG}; "
                        f"border-radius: 5px 5px 0 0; border-bottom: 1px solid {LINE};"
                    ):
                        _dot(GREEN, 7)
                        ui.icon("layers", size="11px").style(f"color: {SLATE};")
                        _t("shared start", 9, INK, bold=True, extra="flex: 1 1 0;")
                        _t(f"{len(shared)} jobs", 8, MUTED, mono=True)
                    with ui.element("div").style(
                        f"height: {LINE_H}px; display: flex; align-items: center; padding: 0 7px;"
                    ):
                        _t("every chain begins here", 8, INK, bold=True)
                    if density == "cards":
                        for t in shared:
                            tj = BY_ID[t]
                            with ui.element("div").style(
                                f"height: {SLOT_H}px; display: flex; align-items: center; gap: 5px; "
                                "padding: 0 6px 0 9px;"
                            ):
                                _dot(STATUS[tj.status], 5)
                                _t(NAMES[tj.kind], 8, SOFT, extra="flex: 1 1 0;")
                                _t(tj.id.removeprefix("job"), 8, MUTED, mono=True)
                    plus = ui.element("div").style(
                        f"position: absolute; right: -7px; top: {h / 2 - 7}px; width: 14px; height: 14px; "
                        f"border-radius: 50%; box-sizing: border-box; background: #fff; border: 1.5px solid {SLATE}; "
                        "display: flex; align-items: center; "
                        "justify-content: center; cursor: pointer; z-index: 2;"
                    )
                    with plus:
                        ui.label("+").style(
                            f"{FONT} font-size: 11px; line-height: 1; color: {SLATE}; font-weight: 600; "
                            "margin-top: -1px;"
                        )
                    plus_menu(plus, BY_ID[shared[-1]])
                box.tooltip("Import → Motion & CTF → Tilt filter, folded into one. Click to unfold.")
                box.on("click", lambda _e: (setattr(st, "trunk", False), redraw()))
                continue
            if nid == NEW:
                render_card(
                    specs[NEW], density, x=x, y=y, on_socket=on_socket, on_method=cycle_method, open_slot=st.open_slot
                )
                continue
            j = BY_ID[nid]
            is_cand = nid in fan
            card = render_card(
                specs[nid],
                density,
                x=x,
                y=y,
                on_plus=lambda plus, j=j: plus_menu(plus, j),
                candidate=is_cand,
                chosen=open_r is not None and open_r.source == nid,
                dim=bool(fan) and not is_cand,
            )
            dossier(card, j, open_r.candidates if is_cand else same_kind(j))
            if is_cand:
                card.on("click", lambda _e, n=nid: choose(n))
            elif nid == "job003" and not st.trunk:
                card.on("click", lambda _e: (setattr(st, "trunk", True), redraw()))

    # ── the new job's verdict ──
    if not st.adding:
        with ui.element("div").style(
            f"margin-top: 10px; padding: 6px 10px; border-radius: 6px; border: 1px dashed {LINE};"
        ):
            _t("No job being added. Use a card's output socket (+) or the row above.", 10, MUTED)
        return
    ready = all(r.state in ("explicit", "automatic", "optional") for r in resolved)
    tone = GREEN if ready else AMBER
    with ui.element("div").style(
        f"margin-top: 10px; padding: 8px 12px; border-radius: 6px; background: {tone}10; border: 1px solid {tone}55; "
        "display: flex; flex-direction: column; gap: 3px;"
    ):
        with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
            _dot(tone, 8)
            _t(f"{NAMES[st.adding]} (new) · {st.method or ''}", 10, INK, bold=True)
            _t("ready to add" if ready else "not yet", 10, tone, bold=True)
        for r in resolved:
            with ui.element("div").style(
                "display: flex; align-items: baseline; gap: 8px; padding-left: 16px; flex-wrap: wrap;"
            ):
                _t(r.slot.label, 9, SOFT, bold=True, extra="width: 90px;")
                if r.source is not None:
                    s = BY_ID[r.source]
                    _t(f"← {ident(s)}", 10, INK, bold=r.state == "explicit")
                    _t(" · ".join(pedigree(s, r.candidates)), 9, BODY)
                    _t(r.note, 9, MUTED)
                else:
                    color = {"ambiguous": AMBER, "missing": RED}.get(r.state, MUTED)
                    link = _t(r.note, 10, color, bold=r.state != "optional", extra="cursor: pointer;")
                    link.on("click", lambda _e, k=r.slot.key: on_socket(k))
                    if r.state == "missing" and r.candidates:
                        house_button(
                            f"+ {NAMES[r.candidates[0].kind]} first",
                            lambda _e: ui.notify("Mock: would add that job on this chain and wire this socket to it"),
                        )
        with ui.element("div").style("display: flex; gap: 6px; margin-top: 4px; padding-left: 16px;"):
            house_button(
                "Add to project",
                lambda _e: ui.notify("Mock: would add the job with these inputs"),
                kind="accent" if ready else "default",
            )
            house_button(
                "Discard",
                lambda _e: (setattr(st, "adding", None), setattr(st, "open_slot", None), st.picks.clear(), redraw()),
            )


# ── the page ─────────────────────────────────────────────────────────────────


def render_branch_map() -> None:
    # The page shell hides overflow (the workspace scrolls its own panels); this page scrolls as a whole.
    ui.query(".nicegui-content").style("overflow: auto;")
    st = MapState()
    # ?add=dpredict&after=job023 opens the page with that job being added, for a screenshot or a link.
    q = ui.context.client.request.query_params
    if q.get("add") in ADDABLE:
        st.adding = q["add"]
        st.method = METHODS.get(st.adding, (None,))[0]
        after = BY_ID.get(q.get("after", ""))
        slot = next((s for s in slots_of(st.adding) if after and PRODUCES2.get(after.kind) in s.accepts), None)
        if after is not None and slot is not None:
            st.picks[slot.key] = after.id
        st.open_slot = next((r.slot.key for r in resolve(st) if r.state in ("ambiguous", "missing")), None)
    with ui.column().style("padding: 18px 24px; gap: 26px; width: 100%; max-width: 1560px;"):
        with ui.column().style("gap: 6px;"):
            with ui.element("div").style("display: flex; gap: 14px; align-items: baseline;"):
                ui.link("← 1 · A new Class 3D's inputs", "/dev/io-ideas").style(
                    f"{FONT} font-size: 10px; color: {BLUE};"
                )
                ui.link("← 2 · The preprocessing tree", "/dev/io-ideas/tree").style(
                    f"{FONT} font-size: 10px; color: {BLUE};"
                )
                ui.label("3 · Branch map, worked out").style(f"{FONT} font-size: 10px; color: {INK}; font-weight: 600;")
            ui.label("The branch map, worked out: one card for every job, and a job being added").style(
                PAGE_SECTION_STYLE + " font-size: 14px;"
            )
            ui.label(
                "The same mock project as chapter 2. Hover any card for its dossier. On the map, the + on a card's "
                "output offers the job types that can read it; the new job hangs off the card with its sockets filled "
                "where the "
                "choice is forced and amber where it is not; a click on a socket fans its candidates out on the map."
            ).style(f"{FONT} font-size: 10px; color: {SOFT}; max-width: 980px;")
        with ui.column().style("gap: 6px; width: 100%;"):
            ui.label("A · The job card").classes("cb-mock-panel-title").style(PAGE_SECTION_STYLE)
            ui.label(
                "Every job type on one anatomy, so a card is always recognisably a job and two cards of one type are "
                "told "
                "apart by their method token and their parameters, never by shape."
            ).style(f"{FONT} font-size: 10px; color: {SOFT}; max-width: 980px;")
            with (
                ui.element("div")
                .classes("cb-mock-panel")
                .style(
                    "background: #fff; border: 1px solid #dbe3ec; border-radius: 8px; padding: 14px 16px; width: 100%; "
                    "box-sizing: border-box; overflow-x: auto;"
                )
            ):
                section_cards()
        with ui.column().style("gap: 6px; width: 100%;"):
            ui.label("B · The map, with a job being added").classes("cb-mock-panel-title").style(PAGE_SECTION_STYLE)
            ui.label(
                "Cables run from a job's output socket to the sockets that read it (bundled inputs ride the same "
                "cable, "
                "paler; a side input such as a model is dashed). Try: + on Reconstruct 023 → Denoise predict; then the "
                "model socket; then cycle the method to easymode."
            ).style(f"{FONT} font-size: 10px; color: {SOFT}; max-width: 980px;")

            @ui.refreshable
            def panel() -> None:
                with (
                    ui.element("div")
                    .classes("cb-mock-panel")
                    .style(
                        "background: #fff; border: 1px solid #dbe3ec; border-radius: 8px; padding: 14px 16px; "
                        "width: 100%; "
                        "box-sizing: border-box; overflow-x: auto;"
                    )
                ):
                    section_map(st, panel.refresh)

            panel()
