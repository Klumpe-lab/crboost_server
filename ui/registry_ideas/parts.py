"""The registry prototypes' pieces: the stylesheet, the type and colour rules, and the HTML the Tomograms table and
the overlay pictures are drawn from.

Colour rule, everywhere on the page: inks and hairlines only. The accent marks state (the selected row's ring, the
one template and mask in use); amber marks something to look at (a large file, an unusual σ); red only a broken
volume. A species' overlay colour is drawn on canvases and in its one swatch, never on a row.
"""

from __future__ import annotations

from html import escape

from ui.job_plugins._field_styles import MONO, SANS, voice
from ui.registry_ideas.data import Overlay, Species, Tomo

INK = "var(--cb-head-ink, #12324f)"
BODY = "#1e293b"
SOFT = "#5b7089"
FAINT = "#94a3b8"
LINE = "#e4eaf1"
RULE = "var(--cb-head-rule, #d6e0ea)"
ACCENT = "var(--cb-accent, #4a8fd4)"
AMBER = "#d97706"
WARN_INK = "#8a4b06"
RED = "#b91c1c"

TH = voice("th", font="'IBM Plex Sans', sans-serif", size="9px", weight=600, ink="#7d93ab")
WORDS = f"{SANS} font-size: 10px; color: {BODY};"
CAPTION = f"{SANS} font-size: 9.5px; color: {SOFT}; line-height: 1.45;"
NUM = f"{MONO} font-size: 9.5px; color: {BODY}; font-variant-numeric: tabular-nums;"
MOLSTAR_WARN_MB = 200  # the workbench's own threshold (TemplateWorkbench.MOLSTAR_WARN_MB)

CSS = f"""
.cbri-rrow {{ display: flex; align-items: center; gap: 6px; height: 24px; padding: 0 8px 0 10px; cursor: pointer;
  min-width: 0; }}
.cbri-rrow:hover {{ background: var(--cb-hover, #eaf1f8); }}
.cbri-rrow.sel {{ box-shadow: inset 0 0 0 1px {ACCENT}; background: #f4f8fc; }}
.cbri-radd {{ display: flex; align-items: center; gap: 6px; height: 24px; padding: 0 10px; cursor: pointer;
  {SANS} font-size: 10px; color: {SOFT}; }}
.cbri-radd:hover {{ color: {INK}; background: var(--cb-hover, #eaf1f8); }}

.cbri-vhead, .cbri-vrow {{ display: grid; grid-template-columns: var(--cbri-cols); align-items: center;
  column-gap: 8px; padding: 0 6px; min-width: 0; width: 100%; box-sizing: border-box; }}
.cbri-vhead {{ height: 20px; border-bottom: 1px solid {RULE}; }}
.cbri-vhead > * {{ {TH} white-space: nowrap; overflow: hidden; }}
.cbri-vrow {{ height: 22px; border-bottom: 1px solid #eef2f6; cursor: pointer; }}
.cbri-vrow > * {{ min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.cbri-vrow:hover {{ background: var(--cb-hover, #eaf1f8); }}
.cbri-vrow.sel {{ box-shadow: inset 0 0 0 1px {ACCENT}; background: #f4f8fc; }}
.cbri-vrow .cbri-acts {{ opacity: .35; transition: opacity .1s; }}
.cbri-vrow:hover .cbri-acts {{ opacity: 1; }}
.cbri-vrow.child {{ background: #fbfcfe; }}
.cbri-vrow.child:hover {{ background: var(--cb-hover, #eaf1f8); }}
.cbri-dot-sel {{ width: 6px; height: 6px; border-radius: 50%; background: {ACCENT}; display: inline-block; }}
.cbri-dot-off {{ width: 6px; height: 6px; border-radius: 50%; border: 1px solid #cbd5e1; display: inline-block;
  box-sizing: border-box; }}
.cbri-vrow:hover .cbri-dot-off {{ border-color: {ACCENT}; }}
.cbri-ib {{ {MONO} font-size: 10px; color: {SOFT}; cursor: pointer; padding: 0 3px; line-height: 16px;
  border-radius: 2px; user-select: none; }}
.cbri-ib:hover {{ color: {INK}; background: #e2eaf3; }}
.cbri-ib.x:hover {{ color: {RED}; background: #fee2e2; }}

.cbri-t {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
.cbri-t th {{ {TH} text-align: left; padding: 0 6px; height: 20px; border-bottom: 1px solid {RULE};
  white-space: nowrap; overflow: hidden; cursor: pointer; user-select: none; }}
.cbri-t th:hover {{ color: {INK}; }}
.cbri-t th.on {{ color: {INK}; }}
.cbri-t th.n, .cbri-t td.n {{ text-align: right; }}
.cbri-t th.x {{ cursor: default; }}
.cbri-t td {{ height: 22px; padding: 0 6px; border-bottom: 1px solid #eef2f6; {NUM} white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; }}
.cbri-t tbody:hover tr.r td {{ background: var(--cb-hover, #eaf1f8); }}
.cbri-t td.nm {{ {MONO} font-size: 10px; color: {INK}; }}
.cbri-t .dim {{ color: #b3c0cf; }}
.cbri-t tr.sub {{ display: none; }}
.cbri-t tbody.open tr.sub {{ display: table-row; }}
.cbri-t tr.sub td {{ background: #f8fafc; height: 20px; font-size: 9px; color: #334155;
  border-bottom: 1px solid #f1f5f9; }}
.cbri-t tbody.open tr.r td {{ border-bottom-color: transparent; }}
.cbri-t tr.sub td.w {{ {SANS} font-size: 9.5px; color: {SOFT}; }}
.cbri-car {{ display: inline-block; width: 10px; color: {FAINT}; font-size: 8px; cursor: pointer;
  transition: transform .12s; }}
.cbri-t tbody.open .cbri-car {{ transform: rotate(90deg); color: {INK}; }}
.cbri-nlists {{ {MONO} font-size: 8.5px; color: {SOFT}; margin-left: 6px; }}
.cbri-mk {{ {SANS} font-size: 9.5px; color: #4f6782; border-bottom: 1px dotted #a9b8c9; margin-right: 7px;
  cursor: default; }}
.cbri-vc {{ {MONO} font-size: 8.5px; color: #4f6782; border: 1px solid #d3dde8; border-radius: 2px;
  padding: 0 4px; line-height: 13px; margin-right: 3px; cursor: pointer; }}
.cbri-vc:hover {{ color: {INK}; border-color: #94a3b8; background: #f4f8fc; }}
.cbri-cur {{ {SANS} font-size: 9.5px; font-weight: 600; color: #24405e; background: #fff;
  border: 1px solid var(--cb-btn-line, #b8cce0); border-radius: var(--cb-ctl-radius, 2px); height: 17px;
  padding: 0 8px; line-height: 15px; cursor: pointer; }}
.cbri-cur:hover {{ background: #f4f8fc; border-color: #a9c6e4; }}
.cbri-live {{ width: 5px; height: 5px; border-radius: 50%; background: #10b981; display: inline-block;
  margin-left: 5px; vertical-align: middle; }}

.cbri-pol {{ display: inline-flex; align-items: center; gap: 5px; {MONO} font-size: 9.5px; color: {BODY}; }}
.cbri-pol i {{ width: 7px; height: 7px; display: inline-block; box-sizing: border-box; }}
.cbri-warn {{ width: 5px; height: 5px; border-radius: 50%; background: {AMBER}; display: inline-block;
  margin-right: 4px; vertical-align: middle; }}
.cbri-bad {{ width: 5px; height: 5px; border-radius: 50%; background: {RED}; display: inline-block;
  margin-right: 4px; vertical-align: middle; }}

.cbri-tag {{ {MONO} font-size: 7.5px; text-transform: uppercase; letter-spacing: .05em; color: #4f6782;
  border: 1px solid #dbe3ec; border-radius: 2px; padding: 0 3px; line-height: 11px; white-space: nowrap;
  position: relative; display: inline-flex; align-items: center; gap: 3px; }}
.cbri-tag .d {{ color: {INK}; font-weight: 600; font-size: 9px; line-height: 9px; }}
.cbri-tag.notch::after {{ content: ""; position: absolute; top: -1px; right: -1px; border-style: solid;
  border-width: 0 5px 5px 0; border-color: transparent #4f6782 transparent transparent; }}
"""


def _e(s: object) -> str:
    return escape(str(s), quote=True)


def num(v: float | int | None, fmt: str = "{:,}") -> str:
    return "—" if v is None else fmt.format(v)


def size_text(n: int | None) -> tuple[str, bool]:
    """(text, is_large) for a file size; large is what the workbench warns about before molstar loads it."""
    if n is None:
        return "—", False
    mb = n / (1024 * 1024)
    text = f"{mb / 1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB" if mb >= 1 else f"{n / 1024:.0f} KB"
    return text, mb >= MOLSTAR_WARN_MB / 2


def stats_text(dmin: float | None, dmax: float | None, rms: float | None) -> tuple[str, str, str]:
    """(text, mark, hover): σ alone in the cell, min and max on hover; mark "warn" for an unusual σ, "bad" for a
    constant volume, as the workbench flags them today (there in orange and red text)."""
    if dmin is None or dmax is None:
        return "—", "", "header carries no statistics"
    hover = f"min {dmin:.3g} · max {dmax:.3g} · σ {rms or 0:.3g}"
    if dmin == dmax:
        return "constant", "bad", hover + " — a constant volume: likely an empty mask or a failed run"
    if rms and (rms < 0.1 or rms > 5.0):
        return f"σ {rms:.2g}", "warn", hover + " — unusual σ: values may not be σ-normalized"
    return f"σ {rms or 0:.2g}", "", hover


def header_text(apix: float | None, box: int | None, lowpass: float | None) -> str:
    bits = []
    if apix:
        bits.append(f"{apix:.3g} Å/px")
    if box:
        bits.append(f"{box} px")
    if lowpass:
        bits.append(f"lp {lowpass:g} Å")
    return " · ".join(bits) or "header unreadable"


def pol_html(polarity: str) -> str:
    """Polarity as a word behind a small square: filled for black (dark density), hollow for white."""
    sq = (
        f"background: {BODY}; border: 1px solid {BODY};"
        if polarity == "black"
        else f"background: #fff; border: 1px solid {SOFT};"
    )
    return f'<span class="cbri-pol"><i style="{sq}"></i>{_e(polarity)}</span>'


# ── Tomograms ────────────────────────────────────────────────────────────────

SORTS = ("position", "picks", "kept", "top", "ge10", "extracted", "markers")

_COLS: list[tuple[str, str, str, str]] = [
    # (key, head, width, hover)
    ("position", "tomogram", "", "Sorted by position by default. A click on a name copies the tomogram's folder."),
    ("picks", "picks", "58px", "Picks of this species on the tomogram, every list together."),
    ("kept", "kept", "52px", "Picks kept by a committed keep/drop filter; the total where none is committed."),
    ("top", "top σ", "52px", "The highest template-match peak, in σ."),
    ("ge10", "≥10σ", "46px", "Picks at or above 10σ."),
    ("extracted", "extracted", "66px", "Particles the extraction made / the candidates it was given."),
    (
        "markers",
        "markers",
        "",
        "Rules that point a human at a tomogram; nothing is dropped by them. Hover for the rule.",
    ),
    ("", "open in", "104px", "The viewer chips every tomogram preview carries."),
    ("", "", "70px", ""),
]
_LIST_WORDS = {"auto": "candidates", "manual": "hand-picked", "imported": "imported", "merged": "merged"}
_STATE_WORDS = {"extracted": "yes", "not_extracted": "–", "stale": "stale", "n/a": ""}


def _lower_sigma(text: str) -> str:
    """A column head with σ in it: the head voice may set capitals, and a capital σ is Σ, another symbol."""
    return _e(text).replace("σ", '<span style="text-transform: none;">σ</span>')


def _sort_key(key: str):
    from services.array_tasks import ts_position_sort_key

    return {
        "position": lambda t: ts_position_sort_key(t.name),
        "picks": lambda t: -t.picks,
        "kept": lambda t: -t.kept,
        "top": lambda t: -(t.top or 0.0),
        "ge10": lambda t: -(t.ge10 or 0),
        "extracted": lambda t: -int((t.extracted or "0").split("/")[0]),
        "markers": lambda t: (-len(t.markers), ts_position_sort_key(t.name)),
    }[key]


def tomo_table_html(sp: Species, tomos: list[Tomo], sort: str, live: str = "") -> str:
    cols = "".join(f'<col style="width: {w};">' if w else "<col>" for _k, _h, w, _t in _COLS)
    heads = "".join(
        f'<th class="{"n" if k in ("picks", "kept", "top", "ge10", "extracted") else ""}'
        f'{" on" if k == sort else ""}{"" if k else " x"}" title="{_e(t)}"'
        f"{f' data-act=sort data-key={k}' if k else ''}>"
        f"{_lower_sigma(h)}{' ▾' if k == sort and k != 'position' else ''}</th>"
        for k, h, w, t in _COLS
    )
    rows = sorted(tomos, key=_sort_key(sort))
    body = "".join(_tomo_rows(sp, t, live) for t in rows)
    return f'<table class="cbri-t"><colgroup>{cols}</colgroup><thead><tr>{heads}</tr></thead>{body}</table>'


def _dim_if(v) -> str:
    return ' class="n dim"' if not v else ' class="n"'


def _tomo_rows(sp: Species, t: Tomo, live: str) -> str:
    many = len(t.lists) > 1
    car = '<span class="cbri-car" data-act="toggle">▶</span>' if many else '<span class="cbri-car"></span>'
    nlists = f'<span class="cbri-nlists" data-act="toggle">{len(t.lists)} lists</span>' if many else ""
    marks = "".join(f'<span class="cbri-mk" title="{_e(text)}">{_e(label)}</span>' for label, text in t.markers)
    if not t.has_geometry:
        marks += (
            '<span class="cbri-mk" title="No tomograms.star describes it: nothing can load or import here.">'
            "no geometry</span>"
        )
    extracted = t.extracted or "–"
    cur_tip = f"Open {t.short} in ArtiaX with this species' picks" + (
        f"; {t.n_saves} saved list(s) waiting in its folder" if t.n_saves else ""
    )
    live_dot = '<span class="cbri-live" title="open in the live ArtiaX session"></span>' if t.name == live else ""
    row = (
        '<tr class="r">'
        f'<td class="nm" title="{_e(t.details)}">{car}'
        f'<span data-act="copy" data-tomo="{_e(t.name)}">{_e(t.short)}</span>{nlists}</td>'
        f"<td{_dim_if(t.picks)}>{num(t.picks)}</td>"
        f"<td{_dim_if(t.kept)}>{num(t.kept)}</td>"
        f'<td class="n">{num(t.top, "{:.1f}")}</td>'
        f"<td{_dim_if(t.ge10)}>{num(t.ge10)}</td>"
        f'<td class="n{"" if t.extracted else " dim"}">{_e(extracted)}</td>'
        f'<td style="{SANS}">{marks}</td>'
        f'<td><span class="cbri-vc" data-act="napari" data-tomo="{_e(t.name)}" '
        f'title="Open {_e(t.short)} in napari">napari</span>'
        f'<span class="cbri-vc" data-act="3dmod" data-tomo="{_e(t.name)}" '
        'title="Copy the 3dmod command">3dmod</span></td>'
        f'<td style="text-align: right;"><button class="cbri-cur" data-act="curate" data-tomo="{_e(t.name)}" '
        f'title="{_e(cur_tip)}">Curate</button>{live_dot}</td>'
        "</tr>"
    )
    subs = "".join(_list_row(t, pl) for pl in t.lists) if many else ""
    return f"<tbody>{row}{subs}</tbody>"


def _list_row(t: Tomo, pl) -> str:
    kind = _LIST_WORDS.get(pl.list_type, pl.list_type)
    src = " · ".join(x for x in (kind, pl.source_kind, pl.origin) if x)
    state = _STATE_WORDS.get(pl.state, pl.state)
    job = pl.job_status.lower() if pl.job_status else ""
    acts = (
        f'<span class="cbri-ib" data-act="curate-list" data-tomo="{_e(t.name)}" data-slug="{_e(pl.slug)}" '
        'title="Open this list in ArtiaX">curate</span>'
        f'<span class="cbri-ib" data-act="extract" data-tomo="{_e(t.name)}" data-slug="{_e(pl.slug)}" '
        'title="Extract this list">extract</span>'
        + (
            ""
            if pl.list_type == "auto"
            else f'<span class="cbri-ib x" data-act="delete" data-tomo="{_e(t.name)}" data-slug="{_e(pl.slug)}" '
            'title="Delete this list">×</span>'
        )
    )
    return (
        '<tr class="sub">'
        f'<td class="nm" style="padding-left: 22px; color: {BODY};" title="{_e(pl.star_path or pl.slug)}">'
        f"└ {_e(pl.label)}</td>"
        f'<td class="n">{num(pl.count)}</td>'
        f'<td class="n{"" if pl.kept is not None else " dim"}">{num(pl.kept) if pl.kept is not None else "–"}</td>'
        "<td></td><td></td>"
        f'<td class="n" title="{_e(pl.state + (" · extraction job " + job if job else ""))}">{_e(state)}</td>'
        f'<td class="w" title="{_e(src)}">{_e(src)}</td>'
        f'<td colspan="2" style="text-align: right;">{acts}</td>'
        "</tr>"
    )


# ── Overlays ─────────────────────────────────────────────────────────────────


def overlay_html(ov: Overlay, *, muted: bool, size: int) -> str:
    """The reconstruction with every species' picks as dots, the gallery's way (`_dots_svg`): a filled dot with a thin
    white rim, one SVG per layer over the picture."""
    h = round(size * ov.height / ov.width)
    layers = "".join(
        f'<g fill="{muted_c if muted else vivid}" stroke="rgba(255,255,255,0.8)" stroke-width="0.5">'
        + "".join(f'<circle cx="{x:.2f}%" cy="{y:.2f}%" r="3"/>' for x, y in dots)
        + "</g>"
        for _name, vivid, muted_c, dots in ov.layers
    )
    return (
        f'<div style="position: relative; width: {size}px; height: {h}px; background: #0f1d2e;">'
        f'<img src="{ov.url}" style="width: {size}px; height: {h}px; display: block;">'
        f'<svg xmlns="http://www.w3.org/2000/svg" style="position: absolute; inset: 0; width: 100%; height: 100%;">'
        f"{layers}</svg></div>"
    )
