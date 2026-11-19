"""The cards of /dev/map-ideas as HTML strings, the branch map's small and large card (ui/branch_map/card.py,
blocks.py) drawn by classes (ui/map_ideas/assets.py). One card is one element: redrawing it is one small update, and
a page of thirty cards is thirty elements."""

from __future__ import annotations

from enum import Enum
from html import escape

from ui.branch_map.card import strip_svg
from ui.branch_map.geometry import CARD_W, LARGE_W, strip_spec
from ui.branch_map.model import Facts, InputRow, Job, Project
from ui.branch_map.plots import svg_plot
from ui.map_ideas.data import MapData, Source

INNER_LARGE = LARGE_W - 24


def _e(s) -> str:
    return escape(str(s), quote=True)


def _value_words(v) -> str:
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, Enum):
        return str(v.value)
    if isinstance(v, float):
        return f"{v:g}"
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v) if v else "—"
    return "—" if v is None or v == "" else str(v)


def _row(pr: Project, r: InputRow) -> tuple[str, str]:
    """What an input row says, and its state (the colour of the value)."""
    if r.source is not None and r.source in pr.by_id:
        s = pr.by_id[r.source]
        text = f"{pr.names.get(s.kind, s.kind)} {s.number or s.id}" + (f" · {r.value}" if r.value else "")
        return text, "explicit" if r.how == "set by hand" else "wired"
    if r.value:
        return r.value, "named"
    if r.how == "to choose":
        return "to choose", "ambiguous"
    if r.slot.required:
        return "nothing", "missing"
    return "optional, none", "optional"


def has_main_in(d: MapData, nid: str) -> bool:
    return any(e.b == nid and e.kind == "main" for e in d.edges)


def tab_html(src: Source) -> str:
    return f'<div class="cbmi-tab" title="{_e(src.detail)}"><span class="k">{_e(src.kind)}</span>{_e(src.label)}</div>'


def source_html(src: Source) -> str:
    """A set or a file several jobs read: a pill of its own at the top of their tree."""
    return (
        f'<div class="cbmi-src" title="{_e(src.detail)}"><span class="k">{_e(src.kind)}</span>'
        f'<span class="cbmi-title">{_e(src.label)}</span><span class="d">{_e(src.detail)}</span></div>'
    )


def _strip(pr: Project, j: Job, width: int) -> str:
    if not pr.ts_ids or pr.series_count(j) <= 0:
        return ""
    sp = strip_spec(pr.series_count(j), width)
    return f'<div class="cbmi-strip">{strip_svg(pr, j, sp, sp.width(pr.series_count(j)))}</div>'


def _params(pr: Project, j: Job) -> str:
    fields = pr.param_fields.get(j.kind, ())
    order = {"paths": 0, "numeric": 1, "enum": 2, "text": 3, "toggle": 4}
    cells = []
    for fs in sorted(fields, key=lambda f: order.get(f.group, 5)):
        v = _value_words(j.param_values.get(fs.name, fs.default))
        wide = " wide" if fs.group == "paths" else ""
        cells.append(
            f'<div class="p{wide}"><span class="pl" title="{_e(fs.hint or fs.label)}">{_e(fs.label)}</span>'
            f'<span class="pv" title="{_e(v)}">{_e(v)}</span></div>'
        )
    body = (
        f'<div class="cbmi-params">{"".join(cells)}</div>'
        if cells
        else '<div class="cbmi-cap">no parameters of its own</div>'
    )
    when = "not run yet" if j.status in ("scheduled", "queued") else "as run"
    return f'<div class="cbmi-sec"><div class="cbmi-sec-h">PARAMETERS · {when}</div>{body}</div>'


def _results(pr: Project, j: Job, facts: Facts | None) -> str:
    if j.status not in ("done", "failed", "running"):
        return ""
    out = []
    if facts is None:
        out.append('<div class="cbmi-cap">reading the results…</div>')
    else:
        if facts.tiles:
            tiles = "".join(
                f'<div><span class="tl">{_e(lbl)}</span><span class="tv">{_e(v)}</span></div>' for lbl, v in facts.tiles
            )
            out.append(f'<div class="cbmi-tiles">{tiles}</div>')
        if facts.pictures:
            pics = []
            for i, (url, tip) in enumerate(facts.pictures):
                if facts.arrow and i:
                    pics.append('<span class="arrow">→</span>')
                pics.append(f'<img src="{_e(url)}" title="{_e(tip)}" loading="lazy" alt="">')
            out.append(f'<div class="cbmi-pics">{"".join(pics)}</div>')
            if facts.picture_caption:
                out.append(f'<div class="cbmi-cap">{_e(facts.picture_caption)}</div>')
        if facts.plot is not None and facts.plot.values:
            out.append(f'<div class="cbmi-plot">{svg_plot(facts.plot, INNER_LARGE, 56)}</div>')
            if facts.plot.caption:
                out.append(f'<div class="cbmi-cap">{_e(facts.plot.caption)}</div>')
        for line in facts.lines:
            out.append(f'<div class="cbmi-cap" title="{_e(line)}">{_e(line)}</div>')
    if pr.ts_ids:
        bad = " bad" if j.status == "failed" else ""
        out.append(f'<div class="cbmi-cap{bad}">{_e(pr.tally_words(j))}</div>')
    elif j.status == "failed" and facts is not None and not facts.lines:
        out.append('<div class="cbmi-cap bad">failed: its log says why</div>')
    return f'<div class="cbmi-sec"><div class="cbmi-sec-h">RESULTS</div>{"".join(out)}</div>'


def card_html(d: MapData, j: Job, *, large: bool, facts: Facts | None = None) -> str:
    """One job: the escort above (the set it reads alone, the magazine), the header, a row per input, the job's own
    words; large, its parameters and results under them."""
    pr = d.pr
    name = pr.names.get(j.kind, j.kind)
    tool = pr.method_of(j)
    src = d.sources.get(d.source_of.get(j.id, ""))
    escort = (tab_html(src) if src is not None and not src.shared else "") + _strip(pr, j, LARGE_W if large else CARD_W)
    badge = f'<span class="cbmi-badge" title="{_e(pr.gloss.get(tool, tool))}">{_e(tool)}</span>' if tool else ""
    size_icon, size_tip = (
        ("close_fullscreen", "Back to the small card")
        if large
        else ("open_in_full", "Parameters and results on the card")
    )
    head = (
        f'<div class="cbmi-head"><span class="cbmi-light {_e(j.status)}" title="{_e(j.status)}"></span>'
        f'<span class="cbmi-title">{_e(name)}</span>{badge}<span class="cbmi-fill"></span>'
        '<span class="cbmi-ltag"></span>'
        f'<span class="cbmi-num">{_e(j.number)}</span>'
        f'<i class="material-icons cbmi-size" title="{size_tip}">{size_icon}</i></div>'
    )
    rows = []
    for i, r in enumerate(j.inputs):
        text, state = _row(pr, r)
        side = " side" if i > 0 and r.source is not None and r.source in pr.by_id and r.source != j.parent else ""
        tip = f"{r.slot.label} ← {text} · {r.how}"
        rows.append(
            f'<div class="cbmi-row{side}" data-slot="{_e(r.slot.key)}" title="{_e(tip)}">'
            f'<span class="cbmi-lbl">{_e(r.slot.label)}</span><span class="cbmi-val v-{state}">{_e(text)}</span></div>'
        )
    sp = pr.species_names.get(j.species or "", j.species or "")
    words = " · ".join(w for w in [sp, *pr.chips(j)] if w)
    foot = f'<div class="cbmi-foot" title="{_e(words)}">{_e(words)}</div>' if words else ""
    middle = (_params(pr, j) + _results(pr, j, facts)) if large else ""
    cls = "cbmi-card" + ("" if has_main_in(d, j.id) else " root") + ("" if j.on_roster else " off")
    tip = "" if j.on_roster else ' title="Not on the roster: kept for the jobs that read it"'
    return f'{escort}<div class="{cls}"{tip}>{head}{"".join(rows)}{foot}{middle}</div>'


def roster_html(d: MapData) -> str:
    """The job roster beside the map, as the Rail roster groups it: a group per job type in pipeline order."""
    pr = d.pr
    kinds = [k for k in pr.stages if any(j.kind == k for j in pr.jobs)]
    kinds += sorted({j.kind for j in pr.jobs} - set(kinds))
    out = []
    for kind in kinds:
        js = sorted((j for j in pr.jobs if j.kind == kind and j.on_roster), key=lambda j: d.order(j.id))
        if not js:
            continue
        out.append(f'<div class="cbmi-rgroup">{_e(pr.names.get(kind, kind))}<span>{len(js)}</span></div>')
        for j in js:
            sp = pr.species_names.get(j.species or "", j.species or "")
            chip = f'<span class="s" title="{_e(sp)}">{_e(sp)}</span>' if sp else ""
            out.append(
                f'<div class="cbmi-rrow" data-nid="{_e(j.id)}" title="{_e(pr.ident(j))}">'
                f'<span class="cbmi-light {_e(j.status)}"></span><span class="t">{_e(pr.names.get(kind, kind))}</span>'
                f'<span class="n">{_e(j.number)}</span>{chip}</div>'
            )
    return "".join(out)
