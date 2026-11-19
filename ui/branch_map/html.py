"""The map's cards as HTML strings, drawn by the classes of `_BRANCH_MAP_CSS` (ui/dashboard/css.py). A card is
one element on the page: redrawing it is one small update, and a project of thirty jobs is thirty elements.

Small: the escort above it (the set or file its main input reads, the magazine of tilt-series marks), a header
band (the status light, the type's name, the tool or method, the job number that opens its menu, the size toggle,
the "+" that adds a job reading it), one labelled row per input, and the job's own words. Large: its parameters,
its results over every tilt series and, with a tilt series picked, that tilt series' section, under the rows.
Ports are the card's pseudo-elements: the main input on its top edge, the output on its bottom edge, a side
input on its row's left edge. The browser draws the cables between them after layout."""

from __future__ import annotations

from html import escape
from math import ceil

from ui.branch_map.geometry import CARD_W, LARGE_W, SIDE, strip_spec
from ui.branch_map.model import Facts, InputRow, Job, Project, TsFacts, value_words
from ui.branch_map.plots import svg_plot, svg_tilt
from ui.branch_map.style import BOX_COLORS, INK

INNER_LARGE = LARGE_W - 2 * SIDE
PLOT_H = 56
TILT_PLOT_H = 58
# One letter per tilt-series state in a magazine's `data-st`, read back by the hover (ui/branch_map/assets.py).
STATE_CODE = {"ok": "o", "fail": "f", "running": "r", "pending": "p", "skip": "s", "none": "n"}


def _e(s) -> str:
    return escape(str(s), quote=True)


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


def magazine_html(pr: Project, j: Job, width: int, *, active: str | None) -> str:
    """The marks as one path per state (a running one blinks), the picked tilt series ringed. Which tilt series a
    mark is, and its state, the browser finds from the pointer (`data-mag`, `data-st`), so the magazine of 151
    tilt series is a handful of paths, not 151 elements."""
    states = series_states(pr, j)
    if not states:
        return ""
    sp = strip_spec(len(states), width)
    names = pr.ts_ids if len(pr.ts_ids) == len(states) else [f"TS_{i + 1:03d}" for i in range(len(states))]
    runs: dict[str, list[str]] = {}
    ring = ""
    for i, state in enumerate(states):
        x = (i % sp.per_row) * (sp.bw + sp.gap)
        y = (i // sp.per_row) * (sp.bh + sp.row_gap)
        runs.setdefault(state, []).append(f"M{x:g} {y:g}h{sp.bw:g}v{sp.bh:g}h{-sp.bw:g}z")
        if active is not None and names[i] == active:
            ring = (
                f'<rect x="{x - 1.5:g}" y="{y - 1.5:g}" width="{sp.bw + 3:g}" height="{sp.bh + 3:g}" rx="2" '
                f'fill="none" stroke="{INK}" stroke-width="1.5"/>'
            )
    paths = "".join(
        f'<path d="{"".join(d)}" fill="{BOX_COLORS.get(state, BOX_COLORS["pending"])[0]}"'
        + (' class="cb-bm-run"' if state == "running" else "")
        + "/>"
        for state, d in runs.items()
    )
    w, h = sp.width(len(states)), sp.height
    code = "".join(STATE_CODE.get(s, "p") for s in states)
    return (
        f'<div class="cb-bm-mag"><svg width="{w:g}" height="{h:g}" viewBox="0 0 {w:g} {h:g}" '
        f'data-mag="{sp.per_row},{sp.bw:g},{sp.bh:g},{sp.gap:g},{sp.row_gap:g}" data-st="{code}">'
        f"{paths}{ring}</svg></div>"
    )


# ── the card's parts ─────────────────────────────────────────────────────────


def row_words(pr: Project, r: InputRow) -> tuple[str, str]:
    """What an input row says, and its state (the colour of the value and of its port)."""
    if r.source is not None and r.source in pr.by_id:
        s = pr.by_id[r.source]
        words = f"{pr.names.get(s.kind, s.kind)} {s.number or s.id}" + (f" · {r.value}" if r.value else "")
        return words, "explicit" if r.how == "set by hand" else "wired"
    if r.value:
        return r.value, "named"
    if r.how == "to choose":
        return "to choose", "ambiguous"
    if r.slot.required:
        return "nothing", "missing"
    return "optional, none", "optional"


def reads_from_job(pr: Project, j: Job) -> bool:
    """The job's main input is another job's output on the map (a cable comes into its top)."""
    return j.parent is not None and j.parent in pr.by_id and j.parent != j.id


def source_tab(pr: Project, j: Job) -> str:
    """The set or file the main input reads when no job of the project made it: a tab on the card's top edge."""
    if not j.inputs or reads_from_job(pr, j):
        return ""
    r = j.inputs[0]
    if not r.value:
        return ""
    kind, name = ("set", r.value[4:]) if r.value.startswith("set ") else ("file", r.value)
    tip = f"{r.slot.label}: {r.value}"
    return f'<div class="cb-bm-tab" title="{_e(tip)}"><span class="k">{kind}</span>{_e(name)}</div>'


def _badge(pr: Project, words: str) -> str:
    if not words:
        return ""
    return f'<span class="cb-bm-badge" title="{_e(pr.gloss.get(words, words))}">{_e(words)}</span>'


def _plus(pr: Project, kind: str) -> str:
    makes = pr.makes.get(kind, "")
    tip = f"Add a job that reads its {makes}" if makes else "Add a job that reads this one"
    return f'<span class="cb-bm-add" title="{_e(tip)}"><i class="material-icons">add</i></span>'


def _head(pr: Project, j: Job, *, large: bool) -> str:
    name = pr.names.get(j.kind, j.kind)
    size_icon, size_tip = (
        ("close_fullscreen", "Back to the small card")
        if large
        else ("open_in_full", "Parameters, results and the picked tilt series on the card")
    )
    return (
        f'<div class="cb-bm-head"><span class="cb-bm-light {_e(j.status)}" title="{_e(j.status)}"></span>'
        f'<span class="cb-bm-title">{_e(name)}</span>{_badge(pr, pr.method_of(j))}<span class="cb-bm-fill"></span>'
        '<span class="cb-open-cue">open ↗</span>'
        f'<span class="cb-bm-num" title="Open, clone or remove">{_e(j.number or "···")}</span>'
        f'<i class="material-icons cb-bm-size" title="{size_tip}">{size_icon}</i>{_plus(pr, j.kind)}</div>'
    )


def _rows(pr: Project, j: Job, *, editable: bool) -> str:
    out = []
    main_src = j.inputs[0].source if j.inputs else None
    for i, r in enumerate(j.inputs):
        words, state = row_words(pr, r)
        cls = f"cb-bm-row s-{state}"
        on_map = r.source is not None and r.source in pr.by_id and r.source != j.id
        if i > 0 and on_map and r.source != main_src and not r.slot.quiet:
            cls += " side"
        if r.slot.quiet or (i > 0 and r.slot.bundle and r.source == main_src):
            cls += " rides"
        if editable and not r.slot.quiet:
            cls += " edit"
        gloss = pr.slot_gloss.get(r.slot.label, "")
        tip = f"{r.slot.label} ← {words} · {r.how}" + (f" · {gloss}" if gloss else "")
        if editable and not r.slot.quiet:
            tip += " · click to choose what it reads"
        out.append(
            f'<div class="{cls}" data-slot="{_e(r.slot.key)}" title="{_e(tip)}">'
            f'<span class="cb-bm-lbl">{_e(r.slot.label)}</span>'
            f'<span class="cb-bm-val v-{state}">{_e(words)}</span></div>'
        )
    return "".join(out)


def _foot(pr: Project, j: Job) -> str:
    sp = pr.species_names.get(j.species or "", j.species or "")
    words = " · ".join(w for w in [sp, *pr.chips(j)] if w)
    return f'<div class="cb-bm-foot" title="{_e(words)}">{_e(words)}</div>' if words else ""


# ── a large card's blocks ────────────────────────────────────────────────────


def _sec(title: str, body: str) -> str:
    return f'<div class="cb-bm-sec"><div class="cb-bm-sec-h">{_e(title)}</div>{body}</div>'


def _cap(words: str, cls: str = "") -> str:
    return f'<div class="cb-bm-cap{(" " + cls) if cls else ""}" title="{_e(words)}">{_e(words)}</div>'


def _tiles(tiles: list[tuple[str, str]]) -> str:
    cells = "".join(f'<div><span class="tl">{_e(lbl)}</span><span class="tv">{_e(v)}</span></div>' for lbl, v in tiles)
    return f'<div class="cb-bm-tiles">{cells}</div>'


def _pictures(pics: list[tuple[str, str]], *, arrow: bool) -> str:
    out = []
    for i, (url, tip) in enumerate(pics):
        if arrow and i:
            out.append('<span class="arrow">→</span>')
        out.append(f'<img src="{_e(url)}" title="{_e(tip)}" loading="lazy" alt="">')
    return f'<div class="cb-bm-pics">{"".join(out)}</div>'


def _params(pr: Project, j: Job) -> str:
    """The job's parameters as the job tab arranges them, read only: paths full width, the rest two to a row."""
    fields = pr.param_fields.get(j.kind, ())
    order = {"paths": 0, "numeric": 1, "enum": 2, "text": 3, "toggle": 4}
    cells = []
    for fs in sorted(fields, key=lambda f: order.get(f.group, 5)):
        v = value_words(j.param_values.get(fs.name, fs.default)) or "—"
        wide = " wide" if fs.group == "paths" else ""
        cells.append(
            f'<div class="p{wide}"><span class="pl" title="{_e(fs.hint or fs.label)}">{_e(fs.label)}</span>'
            f'<span class="pv" title="{_e(v)}">{_e(v)}</span></div>'
        )
    body = f'<div class="cb-bm-params">{"".join(cells)}</div>' if cells else _cap("no parameters of its own")
    when = "not run yet" if j.status in ("scheduled", "queued") else "as run"
    return _sec(f"PARAMETERS · {when}", body)


def _results(pr: Project, j: Job, facts: Facts | None) -> str:
    out = []
    if facts is None:
        out.append(_cap("reading the results…"))
    else:
        if facts.tiles:
            out.append(_tiles(facts.tiles))
        if facts.pictures:
            out.append(_pictures(facts.pictures, arrow=facts.arrow))
            if facts.picture_caption:
                out.append(_cap(facts.picture_caption))
        if facts.plot is not None and facts.plot.values:
            out.append(f'<div class="cb-bm-plot">{svg_plot(facts.plot, INNER_LARGE, PLOT_H)}</div>')
            if facts.plot.caption:
                out.append(_cap(facts.plot.caption))
        out.extend(_cap(line) for line in facts.lines)
    if pr.ts_ids:
        out.append(_cap(pr.tally_words(j), "bad" if j.status == "failed" else ""))
    elif j.status == "failed" and facts is not None and not facts.lines:
        out.append(_cap("failed: its log says why", "bad"))
    return _sec("RESULTS · every tilt series" if pr.ts_ids else "RESULTS", "".join(out))


def _ts(pr: Project, j: Job, ts: str, known: bool, f: TsFacts | None) -> str:
    """The picked tilt series in this job: the Journey's section for it, compacted."""
    title = f"TILT SERIES · {pr.ts_title(ts)}"
    state = pr.ts_state(j, ts)
    if state in ("none", "out"):
        return _sec(title, _cap(f"{pr.ts_title(ts)} is not in this job"))
    if state == "pending":
        return _sec(title, _cap(f"{pr.ts_title(ts)} is still waiting in this job"))
    out = []
    if state == "fail":
        out.append(_cap(f"{pr.ts_title(ts)} failed in this job", "bad"))
    if state == "running":
        out.append(_cap(f"{pr.ts_title(ts)} is on the GPU now", "run"))
    if not known:
        out.append(_cap("reading this tilt series…"))
    elif f is None:
        out.append(_cap("nothing recorded for this tilt series"))
    else:
        if f.tiles:
            out.append(_tiles(f.tiles))
        if f.pictures:
            out.append(_pictures(f.pictures, arrow=f.arrow))
        if f.plots:
            col_w = int((INNER_LARGE - 10) / 2)
            cells = "".join(
                f'<div><div class="cb-plot-label">{_e(tp.label)}</div>{svg_tilt(tp, col_w, TILT_PLOT_H)}</div>'
                for tp in f.plots
            )
            out.append(f'<div class="cb-bm-tplots" style="--rows: {ceil(len(f.plots) / 2)}">{cells}</div>')
        out.extend(_cap(line) for line in f.lines)
    return _sec(title, "".join(out))


# ── the card ─────────────────────────────────────────────────────────────────


def card_html(
    pr: Project,
    j: Job,
    *,
    large: bool,
    strips: bool,
    active_ts: str | None,
    editable: bool,
    facts: Facts | None = None,
    ts_facts: tuple[bool, TsFacts | None] = (False, None),
) -> str:
    """One job's card with its escort. `editable`: a scheduled job's inputs, whose rows choose what they read."""
    escort = source_tab(pr, j)
    if strips and pr.ts_ids:
        escort += magazine_html(pr, j, LARGE_W if large else CARD_W, active=active_ts)
    middle = ""
    if large:
        middle = _params(pr, j)
        if j.status in ("done", "failed", "running"):
            middle += _results(pr, j, facts)
            if active_ts:
                middle += _ts(pr, j, active_ts, *ts_facts)
    cls = "cb-bm-card" + ("" if reads_from_job(pr, j) else " root") + ("" if j.on_roster else " off")
    tip = "" if j.on_roster else ' title="Not on the roster: kept for the jobs that read it"'
    return (
        f'{escort}<div class="{cls}"{tip}>{_head(pr, j, large=large)}{_rows(pr, j, editable=editable)}'
        f"{_foot(pr, j)}{middle}</div>"
    )


def trunk_html(pr: Project, shared: list[str], *, strips: bool, active_ts: str | None) -> str:
    """The shared start of every chain, folded into one card: a row per job it stands for (a click selects that
    job); a click elsewhere on the card unfolds it."""
    first, last = pr.by_id[shared[0]], pr.by_id[shared[-1]]
    num = f"{first.number}–{last.number}" if first.number and last.number else ""
    rows = []
    for t in shared:
        tj = pr.by_id[t]
        name = pr.names.get(tj.kind, tj.kind)
        rows.append(
            f'<div class="cb-bm-trow" data-jid="{_e(t)}" title="{_e(pr.ident(tj))}">'
            f'<span class="cb-bm-light {_e(tj.status)}"></span><span class="t">{_e(name)}</span>'
            f"{_badge(pr, pr.method_of(tj))}"
            f'<span class="n">{_e(tj.number)}</span><span class="cb-open-cue">open ↗</span></div>'
        )
    chain = " → ".join(pr.names.get(pr.by_id[t].kind, t) for t in shared)
    escort = magazine_html(pr, last, CARD_W, active=active_ts) if strips and pr.ts_ids else ""
    return (
        f'{escort}<div class="cb-bm-card cb-bm-trunk root" title="{_e(chain)}, folded into one. Click to unfold.">'
        '<div class="cb-bm-head"><span class="cb-bm-light done"></span><span class="cb-bm-title">Shared start</span>'
        f'<span class="cb-bm-fill"></span><span class="cb-bm-num nomenu">{_e(num)}</span>{_plus(pr, last.kind)}'
        f"</div>{''.join(rows)}</div>"
    )


def band_html(title: str, n: int, *, folded: bool) -> str:
    """The head of a species' band: a chevron, its name, a rule, how many jobs. A click folds or unfolds it."""
    chevron, tip = ("chevron_right", "Unfold") if folded else ("expand_more", "Fold")
    return (
        f'<i class="material-icons cb-bm-fold" title="{tip} this band">{chevron}</i>'
        f'<span class="t">{_e(title)}</span><span class="r"></span>'
        f'<span class="m">{n} job{"" if n == 1 else "s"}{" · folded" if folded else ""}</span>'
    )
