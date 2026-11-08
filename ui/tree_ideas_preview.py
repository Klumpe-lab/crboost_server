"""Six ways to lay out a project's preprocessing jobs as the tree a new job joins, so two jobs of one type can be told
apart without opening either: which alignment they descend from, whether a refinement sits between, which pixel
size, which tilt series, who reads them.

Static mock data: one project of 23 preprocessing jobs (two alignments, a missAlign branch, a Subset, six
Reconstructs). One switch at the top sets the job being added (Denoise predict, Template matching or Reconstruct);
every design shows that job's candidates for its main input and lets one be picked. Clicks change only this page.

Shared by the six: a job's *pedigree* is the few choices that tell it from the other candidates (the stages where
their ancestries split, then its own parameters); what the candidates share is said once or not at all. Every job
has a dossier on hover: what it is, where it descends from with the roads not taken, who reads it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from nicegui import ui

from services.pipeline_table import assign_lanes
from ui.components.buttons import house_button
from ui.io_ideas_preview import AMBER, BLUE, BODY, FONT, GREEN, INK, LINE, MONO, MUTED, RED, SOFT, _dot, _t
from ui.job_plugins._field_styles import PAGE_SECTION_STYLE
from ui.pipeline_table_view import gutter_svg

NEW = "new"
STATUS = {"done": GREEN, "running": "#3b82f6", "failed": RED, "queued": "#60a5fa"}
TIP = (
    f"{FONT} background: #fff; color: {INK}; border: 1px solid {LINE}; border-radius: 6px; padding: 8px 10px; "
    "box-shadow: 0 8px 24px rgba(15,23,42,0.14); max-width: 400px; text-align: left; display: flex; "
    "flex-direction: column; gap: 2px;"
)

# ── the mock project ─────────────────────────────────────────────────────────

STAGES = (
    "import",
    "motion",
    "filter",
    "align",
    "refine",
    "ctf",
    "subset",
    "recon",
    "dtrain",
    "dpredict",
    "segment",
    "tm",
)
NAMES = {
    "import": "Import",
    "motion": "Motion & CTF",
    "filter": "Tilt filter",
    "align": "Alignment",
    "refine": "missAlign",
    "ctf": "CTF",
    "subset": "Subset",
    "recon": "Reconstruct",
    "dtrain": "Denoise train",
    "dpredict": "Denoise predict",
    "segment": "Segment",
    "tm": "Template matching",
}
ABBR = {
    "import": "Imp",
    "motion": "Mot",
    "filter": "Flt",
    "align": "Aln",
    "refine": "mAl",
    "ctf": "CTF",
    "subset": "Sub",
    "recon": "Rec",
    "dtrain": "DnT",
    "dpredict": "DnP",
    "segment": "Seg",
    "tm": "TM",
}
# What each kind's main output is, as the new job's input accepts it.
PRODUCES = {
    "align": "aligned",
    "refine": "aligned",
    "ctf": "ctf",
    "subset": "ctf",
    "recon": "tomograms",
    "dtrain": "model",
    "dpredict": "denoised",
    "segment": "segmentations",
}
# The job being added: its kind, its name, the input being filled and what that input reads.
ADDING = (
    ("dpredict", "Denoise predict", "tomograms", ("tomograms",)),
    ("tm", "Template matching", "tomograms", ("tomograms", "denoised")),
    ("recon", "Reconstruct", "tilt series", ("ctf",)),
)


@dataclass
class Job:
    id: str
    kind: str
    parent: str | None
    params: dict
    status: str
    ok: int
    total: int
    when: str
    side: tuple[str, ...] = ()  # inputs besides the main one (a predict's model)


def _j(id: str, kind: str, parent: str | None = None, *, status="done", ok=46, total=46, when="", side=(), **params):
    return Job(id, kind, parent, params, status, ok, total, when, tuple(side))


# Producers before their consumers (assign_lanes wants that order).
JOBS = [
    _j("job001", "import", ok=48, total=48, when="Oct 2"),
    _j("job002", "motion", "job001", ok=48, total=48, when="Oct 2"),
    _j("job003", "filter", "job002", ok=48, total=48, when="Oct 3", method="DL model", removed=212),
    _j("job004", "align", "job003", ok=46, total=48, when="Oct 3", method="AreTomo"),
    _j("job012", "align", "job003", ok=48, total=48, when="Oct 9", method="IMOD"),
    _j("job021", "refine", "job004", when="Oct 17"),
    _j("job005", "ctf", "job004", when="Oct 3"),
    _j("job013", "ctf", "job012", ok=48, total=48, when="Oct 9"),
    _j("job022", "ctf", "job021", when="Oct 18"),
    _j("job006", "recon", "job005", when="Oct 4", apix=12),
    _j("job016", "recon", "job005", when="Oct 11", apix=6),
    _j("job023", "recon", "job022", when="Oct 18", apix=12),
    _j("job030", "recon", "job013", ok=48, total=48, when="Oct 20", apix=12),
    _j("job034", "subset", "job005", ok=12, total=12, when="Oct 22", keep=12, of=46),
    _j("job035", "recon", "job034", ok=12, total=12, when="Oct 22", apix=6),
    _j("job069", "recon", "job022", status="running", ok=20, total=46, when="today", apix=6, deconv=True),
    _j("job009", "dtrain", "job006", when="Oct 5", method="cryoCARE"),
    _j("job010", "dpredict", "job006", when="Oct 6", side=("job009",), method="cryoCARE"),
    _j("job024", "dtrain", "job023", when="Oct 19", method="cryoCARE"),
    _j("job025", "dpredict", "job023", when="Oct 19", side=("job024",), method="cryoCARE"),
    _j("job031", "dpredict", "job030", ok=48, total=48, when="Oct 21", method="easymode"),
    _j("job040", "segment", "job010", when="Oct 7", method="Ais", features="membranes"),
    _j(
        "job041",
        "segment",
        "job025",
        status="failed",
        ok=30,
        total=46,
        when="Oct 20",
        method="MemBrain-seg",
        features="membranes",
    ),
]
BY_ID = {j.id: j for j in JOBS}


@dataclass
class State:
    """One design's choices on this page: the job being added and the candidate picked for its input."""

    adding: str
    chosen: str | None = None
    extra: dict = field(default_factory=dict)


def adding_spec(adding: str) -> tuple[str, str, str, tuple[str, ...]]:
    return next(a for a in ADDING if a[0] == adding)


# ── reading the mock graph ───────────────────────────────────────────────────


def path(j: Job) -> list[Job]:
    """Root first, `j` last, along main inputs."""
    out = []
    cur: Job | None = j
    while cur is not None:
        out.append(cur)
        cur = BY_ID.get(cur.parent) if cur.parent else None
    return out[::-1]


def ancestor_at(j: Job, stage: str) -> Job | None:
    return next((a for a in path(j) if a.kind == stage), None)


def _aid(j: Job | None) -> str | None:
    return None if j is None else j.id


def main_children(j: Job) -> list[Job]:
    return [c for c in JOBS if c.parent == j.id]


def readers(j: Job) -> list[Job]:
    return [c for c in JOBS if c.parent == j.id or j.id in c.side]


def descendants(j: Job) -> list[Job]:
    out: list[Job] = []
    for c in main_children(j):
        out.append(c)
        out.extend(descendants(c))
    return out


def chips(j: Job) -> list[str]:
    """The job's own parameters, the way a user names them."""
    p = j.params
    out = []
    if "method" in p:
        out.append(p["method"])
    if "apix" in p:
        out.append(f"{p['apix']:g} Å")
    if p.get("deconv"):
        out.append("deconv")
    if "keep" in p:
        out.append(f"{p['keep']} of {p['of']} TS")
    if "features" in p:
        out.append(p["features"])
    if "removed" in p:
        out.append(f"{p['removed']} tilts out")
    return out


def label(j: Job) -> str:
    """The job as its parameters name it, else its type."""
    return " · ".join(chips(j)) or NAMES[j.kind]


def ident(j: Job) -> str:
    return f"{NAMES[j.kind]} {j.id}"


def candidates(adding: str) -> list[Job]:
    """Every job whose main output the new job's input reads, siblings beside each other."""
    _, _, _, accepts = adding_spec(adding)
    cands = [j for j in JOBS if PRODUCES.get(j.kind) in accepts]
    return sorted(cands, key=lambda c: tuple(_aid(ancestor_at(c, s)) or "" for s in STAGES))


def differing_stages(cands: list[Job]) -> list[str]:
    """The stages at which the candidates' ancestries split, in pipeline order. A stage counts only when two
    candidates equal at every earlier splitting stage differ there: a CTF per alignment is the alignment's
    difference, not its own."""
    kept: list[str] = []
    for s in STAGES:
        groups: dict[tuple, set] = {}
        for c in cands:
            key = tuple(_aid(ancestor_at(c, k)) for k in kept)
            groups.setdefault(key, set()).add(_aid(ancestor_at(c, s)))
        if any(len(v) > 1 for v in groups.values()):
            kept.append(s)
    return kept


def _stage_name(a: Job, distinct: list[Job]) -> str:
    """How an ancestor is named among the distinct ancestors at its stage: by its parameters, by its type when
    it has none, with its number only when that still clashes."""
    base = label(a)
    clash = [d for d in distinct if label(d) == base]
    return base if len(clash) <= 1 else f"{base} {a.id}"


def _distinct_at(cands: list[Job], s: str) -> list[Job]:
    seen: dict[str, Job] = {}
    for c in cands:
        a = ancestor_at(c, s)
        if a is not None:
            seen.setdefault(a.id, a)
    return list(seen.values())


def _peers(j: Job, cands: list[Job], kept: list[str]) -> list[Job]:
    """The candidates that answer every stage in `kept` as `j` does."""
    key = tuple(_aid(ancestor_at(j, k)) for k in kept)
    return [c for c in cands if tuple(_aid(ancestor_at(c, k)) for k in kept) == key]


def pedigree(j: Job, cands: list[Job]) -> list[str]:
    """The choices that tell `j` from the other candidates: its ancestor at each splitting stage (absence is the
    default and goes unsaid), then its own parameters. An ancestor is named among the ancestors of the candidates
    that agree with `j` at the earlier stages: once the alignment is said, '12 Å' needs no job number even when
    another chain has a 12 Å reconstruction too."""
    out = []
    kept: list[str] = []
    for s in differing_stages(cands):
        peers = _peers(j, cands, kept)
        kept.append(s)
        if s == j.kind:
            continue
        a = ancestor_at(j, s)
        if a is not None:
            out.append(_stage_name(a, _distinct_at(peers, s)))
    out.extend(chips(j))
    return out


def facet_value(j: Job, s: str, cands: list[Job]) -> str:
    """`j`'s answer at one splitting stage, absence said: 'AreTomo', 'no missAlign', or at its own stage its
    parameters ('6 Å · deconv'). Named as `pedigree` names it."""
    if s == j.kind:
        return " · ".join(chips(j)) or "plain"
    a = ancestor_at(j, s)
    if a is None:
        return f"no {NAMES[s]}"
    stages = differing_stages(cands)
    kept = stages[: stages.index(s)] if s in stages else []
    return _stage_name(a, _distinct_at(_peers(j, cands, kept), s))


def sentence(j: Job) -> str:
    return " → ".join(f"{NAMES[a.kind]}{' ' + ' · '.join(chips(a)) if chips(a) else ''} ({a.id})" for a in path(j))


def used_by(j: Job) -> str:
    kids = readers(j)
    if not kids:
        return "read by no job yet"
    return "read by " + ", ".join(ident(k) for k in kids)


def ts_text(j: Job) -> tuple[str, str]:
    """The tilt-series tally and its colour, the roster's way."""
    if j.status == "running":
        return f"{j.ok}/{j.total} ▸", BLUE
    if j.status in ("queued", "scheduled"):
        return f"{j.ok}/{j.total}", MUTED
    if j.status == "failed":
        return f"{j.ok}/{j.total} {j.total - j.ok}!", RED
    if j.ok < j.total:
        return f"{j.ok}/{j.total} {j.total - j.ok}!", AMBER
    return f"{j.ok}/{j.total}", GREEN


def trunk() -> list[str]:
    """The shared start of every chain: from the root while a job has one reader."""
    out = []
    cur = BY_ID["job001"]
    while True:
        out.append(cur.id)
        kids = main_children(cur)
        if len(kids) != 1:
            return out
        cur = kids[0]


# ── small parts ──────────────────────────────────────────────────────────────


def _chip(text: str, *, strong: bool = False, color: str | None = None) -> ui.label:
    return ui.label(text).style(
        f"{FONT} font-size: 8px; line-height: 13px; padding: 0 4px; border-radius: 3px; white-space: nowrap; "
        f"color: {color or (INK if strong else SOFT)}; background: {'#e2e8f0' if strong else '#f1f5f9'}; "
        f"border: 1px solid {LINE}; display: inline-block;"
    )


def _chips(texts: list[str], *, strong: bool = False, gap: int = 3) -> None:
    with ui.element("div").style(f"display: flex; gap: {gap}px; flex-wrap: wrap; align-items: center;"):
        for t in texts:
            _chip(t, strong=strong)


def _status_word(j: Job) -> str:
    return f"{j.status} {j.when}".strip()


def dossier(host: ui.element, j: Job, cands: list[Job]) -> None:
    """On hover: what the job is, where it descends from (the splitting stages bold, the roads not taken
    greyed under them) and who reads it."""
    diff = set(differing_stages(cands)) if len(cands) > 1 else set()
    with host, ui.tooltip().style(TIP):
        _t(ident(j) + (f" · {' · '.join(chips(j))}" if chips(j) else ""), 11, INK, bold=True)
        tally, _color = ts_text(j)
        _t(f"{_status_word(j)} · {tally} tilt series", 9, SOFT)
        _t("DESCENDS FROM", 8, MUTED, bold=True, extra="margin-top: 5px; letter-spacing: 0.05em;")
        for a in path(j)[:-1]:
            splits = a.kind in diff
            _t(
                f"{NAMES[a.kind]} {a.id}" + (f" · {' · '.join(chips(a))}" if chips(a) else ""),
                9,
                INK if splits else BODY,
                bold=splits,
            )
            if splits:
                others = [s for s in JOBS if s.kind == a.kind and s.parent == a.parent and s.id != a.id]
                if others:
                    _t("not " + ", ".join(f"{label(s)} {s.id}" for s in others), 8, MUTED, extra="margin-left: 8px;")
        for s in diff:
            if s != j.kind and ancestor_at(j, s) is None and _distinct_at(cands, s):
                _t(f"no {NAMES[s]} (the {', '.join(c.id for c in cands if ancestor_at(c, s))} chain has one)", 8, MUTED)
        _t(used_by(j), 9, SOFT, extra="margin-top: 5px;")


def _verdict(st: State) -> None:
    """The one line every design ends with: what the new job's input reads and how that job is told apart."""
    _kind, name, slot, _ = adding_spec(st.adding)
    cands = candidates(st.adding)
    box = (
        "display: flex; align-items: center; gap: 8px; margin-top: 12px; padding: 6px 10px; border-radius: 6px; "
        "flex-wrap: wrap;"
    )
    if st.chosen is None:
        with ui.element("div").style(f"{box} background: {AMBER}14; border: 1px solid {AMBER}55;"):
            _dot(AMBER, 8)
            _t(f"{name} (new) · {slot}: nothing chosen · {len(cands)} candidates", 10, AMBER, bold=True)
        return
    j = BY_ID[st.chosen]
    tally, color = ts_text(j)
    with ui.element("div").style(f"{box} background: {GREEN}14; border: 1px solid {GREEN}55;"):
        _dot(GREEN, 8)
        _t(f"{name} (new) · {slot} ← {ident(j)}", 10, INK, bold=True)
        _t(" · ".join(pedigree(j, cands)) or "told from the others by its number alone", 10, BODY)
        _t(tally, 10, color, mono=True)
        _t(f"{_status_word(j)} · {used_by(j)}", 9, SOFT)


# ── 1 · branch map ───────────────────────────────────────────────────────────

COL_W, ROW_H, NODE_W, NODE_H, PAD = 124, 40, 110, 30, 12


def _tidy(roots: list[str], kids_of) -> dict[str, float]:
    """Rows of a tree: leaves take rows in depth-first order, a parent sits at the middle of its children."""
    rows: dict[str, float] = {}
    nxt = [0]

    def place(nid: str) -> None:
        kids = kids_of(nid)
        if not kids:
            rows[nid] = nxt[0]
            nxt[0] += 1
            return
        for k in kids:
            place(k)
        rows[nid] = (rows[kids[0]] + rows[kids[-1]]) / 2

    for r in roots:
        place(r)
    return rows


def _cubic(x0: float, y0: float, x1: float, y1: float) -> str:
    mx = (x0 + x1) / 2
    return f"M {x0} {y0} C {mx} {y0}, {mx} {y1}, {x1} {y1}"


def design_branch_map(st: State, redraw) -> None:
    kind, name, slot, _ = adding_spec(st.adding)
    cands = candidates(st.adding)
    cand_ids = {c.id for c in cands}
    collapsed = st.extra.setdefault("trunk", True)
    shared = trunk() if collapsed else []

    def kids_of(nid: str) -> list[str]:
        if nid == NEW:
            return []
        base = shared[-1] if nid == "trunk" else nid
        kids = sorted(main_children(BY_ID[base]), key=lambda k: (STAGES.index(k.kind), k.id))
        ids = [k.id for k in kids]
        if st.chosen == base:
            ids.append(NEW)
        return ids

    roots = ["trunk" if collapsed else "job001"] + ([NEW] if st.chosen is None else [])
    rows = _tidy(roots, kids_of)
    stage_of = {nid: "trunk" if nid == "trunk" else kind if nid == NEW else BY_ID[nid].kind for nid in rows}
    present = [s for s in ("trunk", *STAGES) if s in set(stage_of.values())]
    col = {s: i for i, s in enumerate(present)}
    pos = {nid: (PAD + col[stage_of[nid]] * COL_W, PAD + rows[nid] * ROW_H) for nid in rows}
    width = PAD * 2 + len(present) * COL_W
    height = PAD * 2 + (max(rows.values()) + 1) * ROW_H

    lit = {a.id for a in path(BY_ID[st.chosen])} | {NEW} if st.chosen else set()
    if collapsed:
        lit = {"trunk" if nid in shared else nid for nid in lit}

    def anchor(nid: str) -> str:
        return "trunk" if collapsed and nid in shared else nid

    svg = []
    for nid in rows:
        if nid in ("trunk", NEW):
            continue
        j = BY_ID[nid]
        if j.parent:
            p = anchor(j.parent)
            (x0, y0), (x1, y1) = pos[p], pos[nid]
            on = nid in lit and p in lit
            svg.append(
                f'<path d="{_cubic(x0 + NODE_W, y0 + NODE_H / 2, x1, y1 + NODE_H / 2)}" fill="none" '
                f'stroke="{BLUE if on else "#cbd5e1"}" stroke-width="{2.2 if on else 1.2}"/>'
            )
        for s in j.side:
            (x0, y0), (x1, y1) = pos[anchor(s)], pos[nid]
            svg.append(
                f'<path d="{_cubic(x0 + NODE_W, y0 + NODE_H / 2, x1, y1 + NODE_H / 2)}" fill="none" '
                'stroke="#cbd5e1" stroke-width="1" stroke-dasharray="3 3"/>'
            )
    if st.chosen:
        (x0, y0), (x1, y1) = pos[anchor(st.chosen)], pos[NEW]
        svg.append(
            f'<path d="{_cubic(x0 + NODE_W, y0 + NODE_H / 2, x1, y1 + NODE_H / 2)}" fill="none" '
            f'stroke="{BLUE}" stroke-width="2.2" stroke-dasharray="5 3"/>'
        )

    with ui.element("div").style(f"position: relative; width: {width}px; height: {height}px;"):
        ui.html(
            f'<svg width="{width}" height="{height}" style="position:absolute;left:0;top:0;">{"".join(svg)}</svg>',
            sanitize=False,
        )
        for s, i in col.items():
            if s != "trunk":
                ui.label(NAMES[s]).style(
                    f"position: absolute; left: {PAD + i * COL_W}px; top: {height - 12}px; {FONT} font-size: 8px; "
                    f"color: {MUTED}; width: {NODE_W}px; text-align: center;"
                )
        for nid, (x, y) in pos.items():
            if nid == "trunk":
                node = _node_box(x, y, f"1px solid {LINE}", "#f8fafc", True)
                with node:
                    _t(f"shared start · {len(shared)} jobs", 9, INK, bold=True)
                    _t(f"{shared[0]}–{shared[-1]} · 48 TS", 8, MUTED)
                node.tooltip(
                    "The start every chain shares, folded into one: "
                    + " → ".join(NAMES[BY_ID[t].kind] for t in shared)
                    + ". Click to unfold."
                )
                node.on("click", lambda _e: (st.extra.__setitem__("trunk", False), redraw()))
                continue
            if nid == NEW:
                node = _node_box(x, y, f"1.5px dashed {INK}", "#fff", False, extra=f"box-shadow: 0 0 0 4px {BLUE}22;")
                with node:
                    ui.element("div").style(
                        f"position: absolute; left: -7px; top: {NODE_H / 2 - 6}px; width: 12px; height: 12px; "
                        f"border-radius: 50%; background: #fff; border: 3px solid {BLUE if st.chosen else AMBER};"
                    )
                    _t(f"+ {name}", 9, INK, bold=True)
                    _t(
                        f"{slot} ← {st.chosen}" if st.chosen else f"{slot}: pick a lit job",
                        8,
                        BLUE if st.chosen else AMBER,
                    )
                continue
            j = BY_ID[nid]
            fit = nid in cand_ids
            chosen = nid == st.chosen
            border = f"2px solid {BLUE}" if chosen else f"1.5px dashed {BLUE}88" if fit else f"1px solid {LINE}"
            node = _node_box(x, y, border, "#eff6ff" if chosen else "#fff", fit, dim=not fit and nid not in lit)
            with node:
                with ui.element("div").style("display: flex; align-items: center; gap: 4px; min-width: 0;"):
                    _dot(STATUS[j.status], 6)
                    _t(NAMES[j.kind], 9, INK, bold=True, extra="white-space: nowrap;")
                    _t(j.id.removeprefix("job"), 8, MUTED, mono=True)
                tally, color = ts_text(j)
                with ui.element("div").style("display: flex; align-items: center; gap: 4px; min-width: 0;"):
                    _t(
                        " · ".join(chips(j)),
                        8,
                        SOFT,
                        extra="flex: 1 1 0; min-width: 0; overflow: hidden; white-space: nowrap;",
                    )
                    _t(tally, 8, color, mono=True)
            dossier(node, j, cands)
            if fit:
                node.on("click", lambda _e, n=nid: (setattr(st, "chosen", n), redraw()))
            elif nid == "job003" and not collapsed:
                node.on("click", lambda _e: (st.extra.__setitem__("trunk", True), redraw()))


def _node_box(
    x: float, y: float, border: str, bg: str, pointer: bool, *, dim: bool = False, extra: str = ""
) -> ui.element:
    return ui.element("div").style(
        f"position: absolute; left: {x}px; top: {y}px; width: {NODE_W}px; height: {NODE_H}px; box-sizing: border-box; "
        f"border: {border}; border-radius: 5px; background: {bg}; padding: 2px 6px; display: flex; "
        f"flex-direction: column; justify-content: center; gap: 1px; cursor: {'pointer' if pointer else 'default'}; "
        f"opacity: {0.6 if dim else 1}; {extra}"
    )


# ── 2 · commit log ───────────────────────────────────────────────────────────

LOG_COLS = "170px minmax(220px, 1fr) 78px 64px 48px 40px 60px"


def design_commit_log(st: State, redraw) -> None:
    _kind, name, slot, _ = adding_spec(st.adding)
    cands = candidates(st.adding)
    cand_ids = {c.id for c in cands}
    order = [j.id for j in JOBS] + [NEW]
    parent = {j.id: j.parent for j in JOBS} | {NEW: st.chosen}
    explicit = {j.id: True for j in JOBS} | {NEW: False}
    rows = assign_lanes(order, parent, explicit)
    lanes = 1 + max(max((r.lane, *r.through, *r.forks)) for r in rows)
    lit = {a.id for a in path(BY_ID[st.chosen])} if st.chosen else set()
    head = (
        f"{FONT} font-size: 8px; font-weight: 600; letter-spacing: 0.05em; color: {MUTED}; text-transform: uppercase;"
    )
    with ui.element("div").style(
        f"display: grid; grid-template-columns: {lanes * 12}px {LOG_COLS}; column-gap: 10px; align-items: center; "
        "max-width: 1100px;"
    ):
        for h in ("", "job", "what tells it apart", "state", "TS", "ran", "read", slot):
            ui.label(h).style(head + " padding-bottom: 3px;")
        for r in rows:
            nid = r.instance_id
            is_new = nid == NEW
            j = None if is_new else BY_ID[nid]
            fit = nid in cand_ids
            chosen = nid == st.chosen
            fill = "#fff" if is_new else STATUS[j.status]
            ring = BLUE if is_new or chosen else fill
            bg = "#eff6ff" if chosen else "#f8fafc" if nid in lit else "transparent"
            row = ui.element("div").style(
                f"grid-column: 1 / -1; display: grid; grid-template-columns: subgrid; align-items: center; "
                f"height: 24px; background: {bg}; border-radius: 3px; cursor: {'pointer' if fit else 'default'};"
            )
            with row:
                ui.html(gutter_svg(r, lanes, fill, ring, not is_new and j.status == "running"), sanitize=False)
                if is_new:
                    _t(f"+ {name} (new)", 10, INK, bold=True)
                    _t(
                        f"{slot} ← {st.chosen}" if st.chosen else f"{slot}: pick a row with a ring",
                        9,
                        BLUE if st.chosen else AMBER,
                    )
                    for _ in range(5):
                        ui.element("div")
                    continue
                with ui.element("div").style("display: flex; align-items: baseline; gap: 5px; min-width: 0;"):
                    _t(NAMES[j.kind], 10, INK, bold=fit, extra="white-space: nowrap;")
                    _t(j.id, 8, MUTED, mono=True)
                # Candidates carry their pedigree among the candidates; the rest their own parameters and, when
                # a sibling of the same type reads the same job, that it stands beside it.
                if fit:
                    _chips(pedigree(j, cands), strong=True)
                else:
                    beside = [s for s in JOBS if s.kind == j.kind and s.parent == j.parent and s.id != j.id]
                    texts = chips(j) + ([f"beside {', '.join(s.id for s in beside)}"] if beside else [])
                    if j.kind == "refine":
                        texts.insert(0, f"refines {j.parent}")
                    _chips(texts)
                with ui.element("div").style("display: flex; align-items: center; gap: 5px;"):
                    _dot(STATUS[j.status], 6)
                    _t(j.status, 9, BODY)
                tally, color = ts_text(j)
                _t(tally, 9, color, mono=True)
                _t(j.when, 9, SOFT)
                n_read = len(readers(j))
                _t(f"→ {n_read}" if n_read else "", 9, MUTED, mono=True)
                if fit:
                    with ui.element("div").style("display: flex; align-items: center; gap: 5px;"):
                        ui.element("div").style(
                            f"width: 11px; height: 11px; border-radius: 50%; box-sizing: border-box; "
                            f"border: {'4px solid ' + BLUE if chosen else '1.5px solid #94a3b8'}; background: #fff;"
                        )
                        _t("reads this" if chosen else "", 8, BLUE)
                else:
                    ui.element("div")
            if j is not None:
                dossier(row, j, cands)
            if fit:
                row.on("click", lambda _e, n=nid: (setattr(st, "chosen", n), redraw()))


# ── 3 · lineage columns ──────────────────────────────────────────────────────


def design_lineage_columns(st: State, redraw) -> None:
    cands = candidates(st.adding)
    n = len(cands)
    diff = set(differing_stages(cands))
    # Rows: every stage some candidate descends through, except one that would only repeat the candidates.
    stages = [
        s
        for s in STAGES
        if any(ancestor_at(c, s) for c in cands) and not all(ancestor_at(c, s) in (None, c) for c in cands)
    ]
    chosen_i = next((i for i, c in enumerate(cands) if c.id == st.chosen), None)
    with ui.element("div").style(
        f"display: grid; grid-template-columns: 78px repeat({n}, minmax(112px, 1fr)); gap: 3px; max-width: 1240px;"
    ):
        for s in stages:
            _t(NAMES[s], 9, SOFT, bold=True, extra="align-self: center;")
            i = 0
            while i < n:
                a = ancestor_at(cands[i], s)
                k = i + 1
                while k < n and _aid(ancestor_at(cands[k], s)) == _aid(a):
                    k += 1
                span = k - i
                covers = chosen_i is not None and i <= chosen_i < k
                whole = span == n
                if a is None:
                    cell = ui.element("div").style(
                        f"grid-column: span {span}; border: 1px dashed {LINE}; border-radius: 4px; min-height: 22px; "
                        "display: flex; align-items: center; justify-content: center; "
                        f"opacity: {0.9 if covers else 0.6};"
                    )
                    with cell:
                        _t(f"no {NAMES[s]}", 8, MUTED)
                    i = k
                    continue
                cell = ui.element("div").style(
                    f"grid-column: span {span}; border-radius: 4px; padding: 3px 6px; min-height: 22px; display: flex; "
                    f"align-items: center; gap: 6px; box-sizing: border-box; "
                    f"border: 1px solid {BLUE if covers else LINE}; "
                    f"background: {'#eff6ff' if covers else '#f8fafc' if whole else '#fff'};"
                )
                with cell:
                    _dot(STATUS[a.status], 6)
                    _t(a.id, 9, MUTED if whole else INK, mono=True)
                    if chips(a):
                        _t(" · ".join(chips(a)), 9, MUTED if whole else INK, bold=s in diff and not whole)
                    if whole:
                        _t("· the same for all", 8, MUTED)
                dossier(cell, a, cands)
                i = k
        _t("candidates", 9, SOFT, bold=True, extra="align-self: center;")
        for i, c in enumerate(cands):
            on = i == chosen_i
            card = ui.element("div").style(
                "border-radius: 5px; padding: 5px 8px; cursor: pointer; display: flex; flex-direction: column; "
                f"gap: 2px; border: {'2px solid ' + BLUE if on else '1px solid ' + LINE}; "
                f"background: {'#eff6ff' if on else '#fff'};"
            )
            with card:
                with ui.element("div").style("display: flex; align-items: center; gap: 5px;"):
                    _dot(STATUS[c.status], 6)
                    _t(ident(c), 10, INK, bold=True, extra="white-space: nowrap;")
                _t(" · ".join(chips(c)) or "default parameters", 9, INK, bold=True)
                tally, color = ts_text(c)
                with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                    _t(tally, 9, color, mono=True)
                    _t(_status_word(c), 8, SOFT)
                n_read = len(readers(c))
                _t(f"read by {n_read}" if n_read else "read by no job yet", 8, MUTED)
            dossier(card, c, cands)
            card.on("click", lambda _e, cid=c.id: (setattr(st, "chosen", cid), redraw()))


# ── 4 · the roster, with pedigrees ───────────────────────────────────────────


def design_roster_chips(st: State, redraw) -> None:
    _kind, name, slot, _ = adding_spec(st.adding)
    cands = candidates(st.adding)
    cand_ids = {c.id for c in cands}
    with ui.element("div").style("display: flex; gap: 30px; align-items: flex-start; flex-wrap: wrap;"):
        with ui.column().style("gap: 0; width: 360px;"):
            with ui.element("div").style("display: flex; align-items: center; gap: 5px; padding: 4px 8px 3px 10px;"):
                ui.label("PREPROCESSING").style(
                    f"{FONT} font-size: 9px; font-weight: 700; color: {MUTED}; letter-spacing: 0.07em; line-height: 1;"
                )
            for s in STAGES[:-1]:
                inst = [j for j in JOBS if j.kind == s]
                if not inst:
                    continue
                if len(inst) > 1:
                    with ui.element("div").style(
                        "display: flex; align-items: center; gap: 6px; padding: 3px 8px 3px 22px; background: #f8fafc;"
                    ):
                        _t(NAMES[s], 10, "#374151", bold=True, mono=True)
                        ui.label(str(len(inst))).style(
                            f"{FONT} font-size: 9px; font-weight: 700; color: #6b7280; background: #e5e7eb; "
                            "border-radius: 999px; padding: 1px 5px;"
                        )
                for j in inst:
                    fit = j.id in cand_ids
                    chosen = j.id == st.chosen
                    row = ui.element("div").style(
                        f"display: flex; align-items: center; gap: 6px; padding: 3px 6px 3px 10px; min-width: 0; "
                        f"background: {'#eff6ff' if chosen else 'transparent'}; "
                        f"cursor: {'pointer' if fit else 'default'};"
                    )
                    with row:
                        _dot(STATUS[j.status], 7)
                        _t(NAMES[j.kind], 10, INK if fit else BODY, mono=True, extra="white-space: nowrap;")
                        _t(j.id, 8, MUTED, mono=True)
                        # Among its own type's instances, the pedigree says what this one is.
                        if len(inst) > 1:
                            _chips(pedigree(j, inst), strong=fit)
                        ui.element("div").style("flex: 1 1 0;")
                        n_read = len(readers(j))
                        if n_read:
                            _t(f"→{n_read}", 8, MUTED, mono=True).tooltip(used_by(j))
                        if j.ok < j.total:
                            tally, color = ts_text(j)
                            _t(tally, 8, color, mono=True)
                        if fit:
                            ui.element("div").style(
                                f"width: 10px; height: 10px; border-radius: 50%; box-sizing: border-box; "
                                f"border: {'3.5px solid ' + BLUE if chosen else '1.5px solid #94a3b8'}; "
                                "background: #fff;"
                            )
                    dossier(row, j, inst)
                    if fit:
                        row.on("click", lambda _e, n=j.id: (setattr(st, "chosen", n), redraw()))
        with ui.column().style("gap: 8px; width: 440px;"):
            _t(f"{name} (new) · I/O", 10, INK, bold=True)
            with ui.element("div").style("display: flex; align-items: center; gap: 8px; width: 100%;"):
                _t(slot, 9, SOFT, bold=True, extra="width: 90px; text-align: right;")
                face = ui.element("div").style(
                    "flex: 1 1 0; display: flex; align-items: center; gap: 6px; padding: 0 5px; "
                    f"border: 1px solid {LINE}; "
                    "border-radius: 4px; background: #fff; height: 20px; box-sizing: border-box;"
                )
                with face:
                    if st.chosen:
                        j = BY_ID[st.chosen]
                        _dot(STATUS[j.status], 7)
                        _t(ident(j), 11, INK)
                        _chips(pedigree(j, cands), strong=True)
                    else:
                        _dot(AMBER, 7)
                        _t("Ambiguous — pick a source", 11, INK)
                        _t(f"{len(cands)} candidates", 9, MUTED)
            with ui.element("div").style(
                f"margin-left: 98px; border: 1px solid {LINE}; border-radius: 6px; background: #fff; "
                "box-shadow: 0 8px 24px rgba(15,23,42,0.14); padding: 3px 0; width: 340px;"
            ):
                for c in cands:
                    on = c.id == st.chosen
                    item = ui.element("div").style(
                        f"display: flex; align-items: center; gap: 8px; padding: 4px 10px; cursor: pointer; "
                        f"background: {'#eef2f6' if on else 'transparent'};"
                    )
                    with item:
                        _dot(STATUS[c.status], 7)
                        _t(ident(c), 11, INK, extra="white-space: nowrap;")
                        _chips(pedigree(c, cands), strong=True)
                        ui.element("div").style("flex: 1 1 0;")
                        tally, color = ts_text(c)
                        _t(tally, 8, color, mono=True)
                    dossier(item, c, cands)
                    item.on("click", lambda _e, n=c.id: (setattr(st, "chosen", n), redraw()))
                ui.element("div").style("height: 1px; background: #eef2f6; margin: 2px 0;")
                with ui.element("div").style("display: flex; align-items: center; gap: 8px; padding: 4px 10px;"):
                    _dot("#94a3b8", 7)
                    _t("Manual path…", 11, INK)
                    _t("browse for a file", 9, MUTED)
        with ui.column().style("gap: 2px; opacity: 0.7;"):
            _t("the same menu today", 9, MUTED, bold=True)
            for c in cands:
                with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                    _dot(STATUS[c.status], 6)
                    _t(f"{NAMES[c.kind]} ({c.id})", 10, BODY)
                    _t("ok" if c.status == "done" else c.status, 9, MUTED)


# ── 5 · choose by path ───────────────────────────────────────────────────────


def design_choose_by_path(st: State, redraw) -> None:
    kind, name, _slot, _ = adding_spec(st.adding)
    cands = candidates(st.adding)
    stages = differing_stages(cands)
    picks: dict[str, str] = st.extra.setdefault("picks", {})

    def matching(skip: str | None = None) -> list[Job]:
        return [c for c in cands if all(facet_value(c, s, cands) == v for s, v in picks.items() if s != skip)]

    remaining = matching()
    if len(remaining) == 1:
        st.chosen = remaining[0].id
    elif st.chosen is not None and st.chosen not in {c.id for c in remaining}:
        st.chosen = None

    with ui.element("div").style("display: flex; gap: 36px; align-items: flex-start; flex-wrap: wrap;"):
        with ui.column().style("gap: 7px; width: 520px;"):
            _t("The job you mean, one choice at a time", 10, INK, bold=True)
            for s in stages:
                pool = matching(skip=s)
                counts: dict[str, int] = {}
                for c in pool:
                    v = facet_value(c, s, cands)
                    counts[v] = counts.get(v, 0) + 1
                values = sorted({facet_value(c, s, cands) for c in cands}, key=lambda v: (v.startswith("no "), v))
                with ui.element("div").style("display: flex; align-items: center; gap: 6px; flex-wrap: wrap;"):
                    _t(NAMES[s] if s != kind else f"{NAMES[s]} itself", 9, SOFT, bold=True, extra="width: 110px;")
                    for v in values:
                        on = picks.get(s) == v
                        n = counts.get(v, 0)
                        chip = ui.element("div").style(
                            f"display: flex; align-items: center; gap: 4px; padding: 1px 7px; border-radius: 9px; "
                            f"cursor: {'pointer' if n or on else 'default'}; "
                            f"border: {'1.5px solid ' + BLUE if on else '1px solid ' + LINE}; "
                            f"background: {'#eff6ff' if on else '#fff'}; opacity: {1 if n or on else 0.4};"
                        )
                        with chip:
                            _t(v, 10, INK if on or n else MUTED, bold=on)
                            _t(str(n), 8, MUTED, mono=True)

                        def toggle(_e, s=s, v=v, on=on):
                            if on:
                                picks.pop(s, None)
                            else:
                                picks[s] = v
                            redraw()

                        chip.on("click", toggle)
            if picks:
                clear = _t("start over", 9, BLUE, extra="cursor: pointer; margin-left: 116px;")
                clear.on("click", lambda _e: (picks.clear(), redraw()))
        with ui.column().style("gap: 4px; min-width: 420px;"):
            _t(
                f"{len(remaining)} of {len(cands)} candidates" if picks else f"all {len(cands)} candidates",
                10,
                INK,
                bold=True,
            )
            if not remaining:
                _t("No job has this history yet.", 10, AMBER, bold=True)
                wanted = " · ".join(v for v in picks.values())
                house_button(
                    f"+ Add a {name} chain: {wanted}",
                    lambda: ui.notify("Mock: would add the missing jobs, wired in this order"),
                    kind="accent",
                )
            for c in remaining:
                on = c.id == st.chosen
                row = ui.element("div").style(
                    "display: flex; align-items: center; gap: 8px; padding: 3px 8px; border-radius: 4px; "
                    f"cursor: pointer; border: {'1.5px solid ' + BLUE if on else '1px solid ' + LINE}; "
                    f"background: {'#eff6ff' if on else '#fff'};"
                )
                with row:
                    _dot(STATUS[c.status], 7)
                    _t(ident(c), 10, INK, bold=True, extra="white-space: nowrap;")
                    _chips(pedigree(c, cands), strong=True)
                    ui.element("div").style("flex: 1 1 0;")
                    tally, color = ts_text(c)
                    _t(tally, 8, color, mono=True)
                    _t(c.when, 8, SOFT)
                dossier(row, c, cands)
                row.on("click", lambda _e, n=c.id: (setattr(st, "chosen", n), redraw()))


# ── 6 · transit lines ────────────────────────────────────────────────────────

LINE_COLORS = ("#475569", "#0e7490", "#b45309", "#047857", "#be185d", "#1d4ed8", "#4d7c0f", "#7c2d12")
CW, RH, MPAD = 96, 36, 14


def _lines() -> list[dict]:
    """Branches as lines: a job continues its parent's line when it has the most descendants of the parent's
    readers; every other reader starts a line of its own, named by what it changed."""
    lines: list[dict] = [{"name": "main", "nodes": [], "from": None}]

    def walk(nid: str, li: int) -> None:
        lines[li]["nodes"].append(nid)
        kids = sorted(main_children(BY_ID[nid]), key=lambda k: (-len(descendants(k)), STAGES.index(k.kind), k.id))
        for i, k in enumerate(kids):
            if i == 0:
                walk(k.id, li)
            else:
                lines.append({"name": label(k), "nodes": [], "from": nid})
                walk(k.id, len(lines) - 1)

    walk("job001", 0)
    names = [ln["name"] for ln in lines]
    for ln in lines:
        if names.count(ln["name"]) > 1:
            ln["name"] = f"{ln['name']} {ln['nodes'][0]}"
    return lines


def design_transit_lines(st: State, redraw) -> None:
    kind, name, slot, _ = adding_spec(st.adding)
    cands = candidates(st.adding)
    cand_ids = {c.id for c in cands}
    lines = _lines()
    line_of = {nid: i for i, ln in enumerate(lines) for nid in ln["nodes"]}
    present = [s for s in STAGES if any(j.kind == s for j in JOBS) or s == kind]
    col = {s: i for i, s in enumerate(present)}
    n_rows = len(lines) + 1  # the new job's own line at the foot

    def x_of(stage: str) -> float:
        return MPAD + col[stage] * CW + CW / 2

    def y_of(li: int) -> float:
        return MPAD + 14 + li * RH + RH / 2

    width = MPAD * 2 + len(present) * CW
    height = MPAD * 2 + 14 + n_rows * RH
    svg = []
    for li, ln in enumerate(lines):
        color = LINE_COLORS[li % len(LINE_COLORS)]
        xs = [x_of(BY_ID[n].kind) for n in ln["nodes"]]
        y = y_of(li)
        if ln["from"] is None:
            svg.append(
                f'<path d="M {xs[0]} {y} H {xs[-1]}" stroke="{color}" stroke-width="4" fill="none" '
                'stroke-linecap="round"/>'
            )
        else:
            x0, y0 = x_of(BY_ID[ln["from"]].kind), y_of(line_of[ln["from"]])
            r = 9
            svg.append(
                f'<path d="M {x0} {y0} V {y - r} Q {x0} {y} {x0 + r} {y} H {xs[-1]}" stroke="{color}" stroke-width="4" '
                'fill="none" stroke-linecap="round"/>'
            )
    if st.chosen:
        c = BY_ID[st.chosen]
        x0, y0, y = x_of(c.kind), y_of(line_of[c.id]), y_of(len(lines))
        svg.append(
            f'<path d="M {x0} {y0} V {y - 9} Q {x0} {y} {x0 + 9} {y} H {x_of(kind)}" stroke="{BLUE}" stroke-width="3" '
            'fill="none" stroke-dasharray="6 4" stroke-linecap="round"/>'
        )
    with ui.element("div").style("display: flex; gap: 18px; align-items: flex-start; flex-wrap: wrap;"):
        with ui.column().style("gap: 3px; width: 190px; padding-top: 30px;"):
            _t("LINES", 8, MUTED, bold=True, extra="letter-spacing: 0.05em;")
            for li, ln in enumerate(lines):
                color = LINE_COLORS[li % len(LINE_COLORS)]
                on_line = [n for n in ln["nodes"] if n in cand_ids]
                leg = ui.element("div").style("display: flex; align-items: center; gap: 6px; cursor: pointer;")
                with leg:
                    ui.element("div").style(f"width: 14px; height: 4px; border-radius: 2px; background: {color};")
                    _t(ln["name"], 10, INK, bold=True, extra="white-space: nowrap;")
                    _t(f"{len(ln['nodes'])} jobs" + (f" · {len(on_line)} fit" if on_line else ""), 8, MUTED)
                leg.tooltip(f"Line {ln['name']}: {', '.join(ln['nodes'])}. Click to name it.")
                leg.on("click", lambda _e, nm=ln["name"]: ui.notify(f"Mock: would let you name the line '{nm}'"))
            with ui.element("div").style("display: flex; align-items: center; gap: 6px; margin-top: 4px;"):
                ui.element("div").style(f"width: 14px; height: 0; border-top: 3px dashed {BLUE};")
                _t(f"{name} (new)", 10, BLUE, bold=True)
        with ui.element("div").style(f"position: relative; width: {width}px; height: {height}px; overflow: visible;"):
            ui.html(
                f'<svg width="{width}" height="{height}" style="position:absolute;left:0;top:0;">{"".join(svg)}</svg>',
                sanitize=False,
            )
            for s, i in col.items():
                ui.label(NAMES[s]).style(
                    f"position: absolute; left: {MPAD + i * CW}px; top: 0; width: {CW}px; text-align: center; {FONT} "
                    f"font-size: 8px; color: {MUTED};"
                )
            for li, ln in enumerate(lines):
                color = LINE_COLORS[li % len(LINE_COLORS)]
                for nid in ln["nodes"]:
                    j = BY_ID[nid]
                    fit = nid in cand_ids
                    chosen = nid == st.chosen
                    x, y = x_of(j.kind), y_of(li)
                    r = 8 if fit else 5
                    ring = f"3px solid {BLUE}" if chosen else f"2.5px dashed {BLUE}" if fit else f"2.5px solid {color}"
                    stop = ui.element("div").style(
                        f"position: absolute; left: {x - r}px; top: {y - r}px; width: {2 * r}px; height: {2 * r}px; "
                        f"border-radius: 50%; box-sizing: border-box; cursor: {'pointer' if fit else 'default'}; "
                        f"background: {BLUE if chosen else '#fff'}; "
                        f"border: {ring}; "
                        f"box-shadow: 0 0 0 2px #fff{', 0 0 0 4px ' + BLUE + '44' if chosen else ''};"
                    )
                    dossier(stop, j, cands)
                    if fit:
                        stop.on("click", lambda _e, n=nid: (setattr(st, "chosen", n), redraw()))
                    ui.label(f"{ABBR[j.kind]} {j.id.removeprefix('job')}").style(
                        f"position: absolute; left: {x - CW / 2}px; top: {y + r + 2}px; width: {CW}px; "
                        "text-align: center; "
                        f"{MONO} font-size: 8px; color: {INK if fit else MUTED}; font-weight: {600 if fit else 400};"
                    )
                    if chips(j):
                        ui.label(" · ".join(chips(j))).style(
                            f"position: absolute; left: {x - CW / 2}px; top: {y - r - 12}px; width: {CW}px; "
                            f"text-align: center; {FONT} font-size: 8px; color: {INK if fit else SOFT}; "
                            "white-space: nowrap; overflow: hidden; "
                            "text-overflow: ellipsis;"
                        )
            y = y_of(len(lines))
            x = x_of(kind)
            ghost = ui.element("div").style(
                f"position: absolute; left: {x - 9}px; top: {y - 9}px; width: 18px; height: 18px; border-radius: 50%; "
                f"box-sizing: border-box; background: #fff; border: 2.5px dashed {BLUE if st.chosen else AMBER};"
            )
            ghost.tooltip(
                f"{name}: {slot} ← {st.chosen}" if st.chosen else f"{name}: pick a station with a dashed ring"
            )
            ui.label(f"+ {name}").style(
                f"position: absolute; left: {x - CW / 2}px; top: {y + 12}px; width: {CW}px; text-align: center; {FONT} "
                f"font-size: 8px; font-weight: 600; color: {BLUE if st.chosen else AMBER};"
            )


DESIGNS = (
    (
        "1 · Branch map",
        "The tree itself: a column per stage, a box per job (type, number, status, its own parameters, tilt series), "
        "the shared start folded into one box. Jobs that fit the new job's input have a dashed ring; the one picked "
        "lights its whole ancestry and the new job hangs off it. The CryoSPARC tree, in boxes small enough for "
        "80 jobs.",
        design_branch_map,
    ),
    (
        "2 · Commit log",
        "The pipeline table already built (roadmap 32 §8: one row per job, the lanes drawn git-log style) with the "
        "column it lacks: what tells each job apart. Candidates get a ring; picking one tints its ancestry. Reads like "
        "a project's history, and takes any number of branches without growing sideways.",
        design_commit_log,
    ),
    (
        "3 · Lineage columns",
        "Only the candidates, side by side, their ancestries as rows from import downwards. Where two candidates share "
        "an ancestor the cell spans both, so the shared start is one wide quiet band and every split shows as a seam. "
        "The whole question 'how do these six differ' in one small table.",
        design_lineage_columns,
    ),
    (
        "4 · Pedigree chips in today's roster",
        "The smallest change: the roster stays a list, but each job of a type with several instances carries the few "
        "chips that tell it from its siblings, how many jobs read it, and its tilt-series tally when short; the I/O "
        "menu shows the same chips. Beside it the menu as it reads today.",
        design_roster_chips,
    ),
    (
        "5 · Choose by path",
        "No job numbers until the end: one question per stage at which the candidates split (which alignment, "
        "refined or not, which pixel size), each answer with how many candidates it leaves. One left is the pick; none "
        "left is an offer to add the missing chain. The way a user already thinks about it.",
        design_choose_by_path,
    ),
    (
        "6 · Transit lines",
        "Branches as named, coloured lines, jobs as stations by stage: a job continues its parent's line, every other "
        "reader starts a new one named by what it changed (IMOD, missAlign, 6 Å, 12 of 46 TS). Candidates are the "
        "stations with a dashed ring; the new job is a dashed line of its own. Lines could carry user names.",
        design_transit_lines,
    ),
)


def render_tree_ideas() -> None:
    page = {"adding": "dpredict"}
    states: dict[str, State] = {}

    @ui.refreshable
    def body() -> None:
        for title, blurb, fn in DESIGNS:
            st = states.get(title)
            if st is None or st.adding != page["adding"]:
                st = states[title] = State(page["adding"])
            with ui.column().style("gap: 6px; width: 100%;"):
                ui.label(title).classes("cb-mock-panel-title").style(PAGE_SECTION_STYLE)
                ui.label(blurb).style(f"{FONT} font-size: 10px; color: {SOFT}; max-width: 980px;")
                _render_design(fn, st)

    # The page shell hides overflow (the workspace scrolls its own panels); this page scrolls as a whole.
    ui.query(".nicegui-content").style("overflow: auto;")
    with ui.column().style("padding: 18px 24px; gap: 26px; width: 100%; max-width: 1500px;"):
        with ui.column().style("gap: 6px;"):
            with ui.element("div").style("display: flex; gap: 14px; align-items: baseline;"):
                ui.link("← 1 · A new Class 3D's inputs", "/dev/io-ideas").style(
                    f"{FONT} font-size: 10px; color: {BLUE};"
                )
                ui.label("2 · The preprocessing tree").style(f"{FONT} font-size: 10px; color: {INK}; font-weight: 600;")
                ui.link("3 · Branch map, worked out →", "/dev/io-ideas/branch").style(
                    f"{FONT} font-size: 10px; color: {BLUE};"
                )
            ui.label("Six Reconstructs that all read 'Reconstruct jobNNN': six ways to tell them apart").style(
                PAGE_SECTION_STYLE + " font-size: 14px;"
            )
            ui.label(
                "Mock project of 23 preprocessing jobs: two alignments (AreTomo, IMOD), a missAlign refinement, a "
                "Subset "
                "of 12 tilt series, six Reconstructs at 12 or 6 Å, three denoisings, two segmentations. Every design "
                "shows the candidates for the new job's main input and lets you pick one; hover any job for its "
                "dossier. "
                "The same idea runs through all six: say only what differs, fold what is shared."
            ).style(f"{FONT} font-size: 10px; color: {SOFT}; max-width: 980px;")

            @ui.refreshable
            def switches() -> None:
                with ui.element("div").style("display: flex; gap: 6px; align-items: center;"):
                    _t("Adding", 9, SOFT, bold=True)
                    for key, name, slot, _ in ADDING:
                        house_button(
                            f"{name} · {slot}",
                            lambda _e, k=key: switch(k),
                            kind="accent" if page["adding"] == key else "default",
                        )

            def switch(adding: str) -> None:
                page["adding"] = adding
                switches.refresh()
                body.refresh()

            switches()
        body()


def _render_design(fn, st: State) -> None:
    @ui.refreshable
    def panel() -> None:
        with (
            ui.element("div")
            .classes("cb-mock-panel")
            .style(
                "background: #fff; border: 1px solid #dbe3ec; border-radius: 8px; padding: 14px 16px; width: 100%; "
                "box-sizing: border-box; overflow-x: auto;"
            )
        ):
            fn(st, panel.refresh)
            _verdict(st)

    panel()
