"""The per-tomogram table of one template match and its extraction: σ, search space, the FP=1
cutoff, the highest peak, the score map's distinct peaks, picks and their σ ladder, the cap, runtime,
node and markers.

One HTML table in one element: sorting or filtering swaps its content, nothing else rebuilds.
Every cell carries its detail on hover (native title), including the rule behind each marker.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nicegui import ui

from services.array_tasks import ts_display_name, ts_position_sort_key
from services.particles import analytics
from services.particles.sigma import LADDER_SIGMAS
from ui.analytics.common import TABLE_ON_PANEL_STYLE, HINT_CLS, esc, fmt_sci
from ui.components.fields import house_select

MARKER_LABELS = {
    "sigma_outlier": "σ outlier",
    "no_peak": "no peak",
    "runtime_outlier": "slow",
    "top_at_edge": "edge max",
    "cap_bound": "cap",
}

# (key, label) of the sort control; each key has an entry in _SORT_KEYS.
SORTS: dict[str, str] = {
    "position": "position",
    "marked": "markers first",
    "top": "top peak",
    "picks": "picks",
    "ge10": "picks ≥ 10σ",
    "sigma": "σ",
    "runtime": "runtime",
}


def _ladder(row: dict[str, Any], level: float) -> int:
    c = row.get("cand") or {}
    ladder = c.get("ladder") or []
    i = LADDER_SIGMAS.index(level)
    return int(ladder[i]) if i < len(ladder) else 0


_SORT_KEYS: dict[str, Callable[[dict[str, Any], int], Any]] = {
    "position": lambda r, m: ts_position_sort_key(r["tomo"]),
    "marked": lambda r, m: (-m, ts_position_sort_key(r["tomo"])),
    "top": lambda r, m: -(r.get("top_sigma") or 0.0),
    "picks": lambda r, m: -((r.get("cand") or {}).get("n") or 0),
    "ge10": lambda r, m: -_ladder(r, 10.0),
    "sigma": lambda r, m: -(r.get("sigma") or 0.0),
    "runtime": lambda r, m: -(r.get("runtime_min") or 0.0),
}

_LADDER_SHOWN = tuple(z for z in LADDER_SIGMAS if z > 7.0)

_HEAD: list[tuple[str, str, bool]] = [
    ("tomogram", "Hover a name for its tilts, defocus and dose.", False),
    ("σ", "Noise level of one correlation (job_stats.std): the unit of every score here.", True),
    ("N", "Search space: voxels × rotations.", True),
    ("FP=1", "The score Gaussian noise would beat once in this tomogram: erfcinv(2/N)·√2, in σ.", True),
    (
        "top",
        "The highest peak, in σ — from the score map when the template-match driver scanned it, else the "
        "extraction's first pick (marked *).",
        True,
    ),
    (
        "peaks",
        "Distinct score-map peaks at or above FP=1 after PyTOM's exclusion at the species radius (what an uncapped "
        "extraction would return, roughly). Only where the driver scanned the map.",
        True,
    ),
    ("picks", "Candidates the extraction kept.", True),
    *[(f"≥{z:g}σ", f"Picks at or above {z:g}σ.", True) for z in _LADDER_SHOWN],
    ("last", "The lowest-scoring pick, in σ.", True),
    (
        "slab",
        "Thickness of the pick slab: the 5th–95th percentile of the picks' distance from the plane fitted through "
        "them (20 or more picks). Hover for its tilt and the share of picks at its surfaces.",
        True,
    ),
    ("edge", f"Picks within {analytics.EDGE_ANG:.0f} Å of a face of the volume.", True),
    ("nn", "Median distance from a pick to its nearest neighbour (Å); hover for pairs closer than 0.65 Ø.", True),
    ("extracted", "Particles the subtomo extraction made / the candidates it was given.", True),
    ("min", "Template-match runtime of this tomogram.", True),
    ("node", "Compute node it ran on.", False),
    ("markers", analytics.MARKERS_DROP_NOTHING, False),
]


def table_html(rows: list[dict[str, Any]], markers: dict[str, list[analytics.Marker]]) -> str:
    head = "".join(
        f'<th class="{"n" if num else ""}" title="{esc(tip)}">{esc(label)}</th>' for label, tip, num in _HEAD
    )
    body = "".join(_row_html(r, markers.get(r["tomo"], [])) for r in rows)
    return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


def _row_html(r: dict[str, Any], marks: list[analytics.Marker]) -> str:
    c = r.get("cand") or {}
    details = [r["tomo"]]
    if r.get("n_tilts"):
        details.append(f"{r['n_tilts']} tilts, {r.get('tilt_min', 0):.0f}° … {r.get('tilt_max', 0):.0f}°")
    if r.get("defocus_um_min") is not None:
        details.append(f"defocus {r['defocus_um_min']:.1f}–{r['defocus_um_max']:.1f} µm")
    if r.get("dose_max") is not None:
        details.append(f"dose {r['dose_max']:.0f} e/Å²")
    if r.get("n_rotations"):
        details.append(f"{r['n_rotations']:,} rotations")
    if r.get("phase_flip") is not None:
        details.append("phase-flip CTF" if r["phase_flip"] else "CTF without phase flip")
    if r.get("random_phase"):
        details.append("random-phase correction on")
    details.append(f"TM facts from {r.get('tm_source') or 'the extraction'}")
    scan = r.get("scan") or {}
    top = r.get("top_sigma")
    top_txt = "—" if top is None else (f"{top:.1f}" if scan.get("top_sigma") is not None else f"{top:.1f}*")
    peaks_title = (
        f"{scan.get('n_local_max')} local maxima above FP=1, {scan.get('n_peaks')} after exclusion at "
        f"{scan.get('radius_px')} px; highest: {', '.join(str(p) for p in scan.get('peaks_sigma') or [])}"
        if scan
        else "no score-map scan for this tomogram (the job predates the driver sidecar)"
    )
    cap_cls = " mk" if c.get("cap_bound") else ""
    cells = [
        f'<td class="nm" title="{esc(chr(10).join(details))}">{esc(ts_display_name(r["tomo"]))}</td>',
        _num(f"{r['sigma']:.4g}" if r.get("sigma") else "—"),
        _num(fmt_sci(r.get("search_space"))),
        _num(f"{r['fp1_sigma']:.2f}" if r.get("fp1_sigma") else "—"),
        _num(top_txt),
        _num("—" if r.get("n_peaks") is None else str(r["n_peaks"]), title=peaks_title),
        _num(str(c.get("n", "—")) if c else "—"),
        *[_num(str(_ladder(r, z)) if c else "—", dim=c and _ladder(r, z) == 0) for z in _LADDER_SHOWN],
        _num(f"{c['last_sigma']:.1f}" if c.get("last_sigma") is not None else "—", extra=cap_cls),
        *_geometry_cells(c.get("geometry") or {}),
        _extracted_cell(r.get("sub")),
        _num(f"{r['runtime_min']:.0f}" if r.get("runtime_min") else "—"),
        f'<td class="dim">{esc((r.get("node") or "—").split(".", 1)[0])}</td>',
        _markers_cell(r, marks),
    ]
    return "<tr>" + "".join(cells) + "</tr>"


def _geometry_cells(geo: dict[str, Any]) -> list[str]:
    plane = geo.get("plane")
    slab = (
        _num(
            f"{plane['thickness_ang']:.0f}",
            title=f"tilt {plane['tilt_deg']}°, {100 * plane['surface_share']:.0f} % of picks at its surfaces",
        )
        if plane
        else _num("—", title="fewer than 20 picks: no plane")
    )
    edge = _num("—" if geo.get("edge_n") is None else str(geo["edge_n"]), dim=not geo.get("edge_n"))
    close = geo.get("nn_close_n")
    nn = _num(
        "—" if geo.get("nn_median_ang") is None else f"{geo['nn_median_ang']:.0f}",
        title="" if close is None else f"{close} picks closer than 0.65 Ø to another",
        extra=" mk" if close else "",
    )
    return [slab, edge, nn]


def _extracted_cell(sub: dict[str, Any] | None) -> str:
    if not sub:
        return _num("—", dim=True)
    text = f"{sub['n_out']}" + (f"/{sub['n_in']}" if sub.get("n_in") is not None else "")
    title = f"{sub['lost']} lost outside the tilt images" if sub.get("lost") else ""
    if sub.get("visible_median") is not None:
        title += ("; " if title else "") + f"median {sub['visible_median']:.0f} tilts per particle"
    return _num(text, title=title, extra=" mk" if sub.get("lost") else "")


def _num(text: str, *, title: str = "", dim: bool = False, extra: str = "") -> str:
    cls = "n" + (" dim" if dim else "") + extra
    t = f' title="{esc(title)}"' if title else ""
    return f'<td class="{cls}"{t}>{esc(text)}</td>'


def _markers_cell(r: dict[str, Any], marks: list[analytics.Marker]) -> str:
    parts = [f'<span class="mk" title="{esc(m.text)}">{esc(MARKER_LABELS.get(m.key, m.key))}</span>' for m in marks]
    if r.get("error"):
        parts.append(f'<span class="err" title="{esc(r["error"])}">!</span>')
    if not r.get("done"):
        parts.append('<span class="dim" title="No score map yet: not matched, or still running.">pending</span>')
    return "<td>" + " ".join(parts) + "</td>"


class TomogramTable:
    """The table plus its sort and "marked only" controls."""

    def __init__(self, rows: list[dict[str, Any]], markers: list[analytics.Marker]) -> None:
        self.rows = rows
        self.markers: dict[str, list[analytics.Marker]] = {}
        for m in markers:
            self.markers.setdefault(m.tomo, []).append(m)
        self.sort = "position"
        self.marked_only = False
        self._html = None

    def build(self) -> None:
        with ui.element("div").style("display: flex; align-items: center; gap: 12px; flex-wrap: wrap;"):
            house_select("sort", SORTS, value=self.sort, width="w-32", on_change=self._on_sort)
            ui.checkbox("marked only", value=False, on_change=self._on_marked).props("dense size=xs").style(
                "font-size: 10px; color: #475569;"
            )
            n_marked = len(self.markers)
            ui.label(f"{len(self.rows)} tomograms · {n_marked} marked").classes(HINT_CLS)
        with ui.element("div").classes("cb-atable-wrap").style(TABLE_ON_PANEL_STYLE):
            self._html = ui.html(self._content(), sanitize=False)

    def _content(self) -> str:
        rows = [r for r in self.rows if not self.marked_only or r["tomo"] in self.markers]
        key = _SORT_KEYS.get(self.sort, _SORT_KEYS["position"])
        rows = sorted(rows, key=lambda r: key(r, len(self.markers.get(r["tomo"], []))))
        return table_html(rows, self.markers)

    def _refresh(self) -> None:
        if self._html is not None and not self._html.is_deleted:
            self._html.set_content(self._content())

    def _on_sort(self, e) -> None:
        if e.value and e.value != self.sort:
            self.sort = e.value
            self._refresh()

    def _on_marked(self, e) -> None:
        self.marked_only = bool(e.value)
        self._refresh()
