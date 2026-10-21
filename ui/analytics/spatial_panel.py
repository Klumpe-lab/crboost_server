"""Where the picks sit, and content against quality.

- The pick slab: the plane through each tomogram's picks (the lamella as the picks see it), its
  thickness, tilt and the share of picks at its surfaces; picks near the volume faces; nearest
  neighbours. The depth profile pools the slabs: real particles peak mid-slab, surface artefacts do
  not, and the share at or above the cut per depth bin shows which.
- Yield against quality: picks at or above the cut per tomogram against a data-quality fact (CTF
  fit resolution, tilts in the tomogram, blank tilts, σ). A tomogram with few picks and normal
  quality holds little; one with bad data says nothing about content.
- Subtomo extraction: candidates in, particles out, and how many tilts each particle is seen in.
- Overlap with another species: picks at or above the cut that sit within a radius of the other
  species' picks, counted on request.

Charts follow the cut in place.
"""

from __future__ import annotations

import statistics
from collections.abc import Awaitable, Callable
from typing import Any

from nicegui import ui

from services.array_tasks import ts_display_name
from services.particles import analytics
from services.particles.particle_sets import ParentRef
from ui.analytics.common import CHART_CELL_STYLE, HINT_CLS, TOOLTIP_STYLE
from ui.components.buttons import house_button
from ui.components.fields import house_number, house_select
from ui.components.reactive import SingleFlight
from ui.dashboard.figures import build_category_bars, build_depth_profile_chart, build_yield_scatter
from ui.styles import SANS

CHART_PX = 150

QUALITY_METRICS: dict[str, str] = {
    "ctf_resolution": "CTF fit resolution (Å)",
    "n_used": "tilts in the tomogram",
    "n_blank": "blank tilts in it",
    "sigma": "σ (noise level)",
}


class SpatialPanel:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        quality: dict[str, dict[str, Any]],
        subtomo: dict[str, Any] | None,
        *,
        cut: float,
        nn_close_ang: float | None,
        others: list[ParentRef] = (),
        on_overlap: Callable[[ParentRef, float, float | None], Awaitable[dict]] | None = None,
    ) -> None:
        self.rows = rows
        self.cand_rows = [r["cand"] for r in rows if r.get("cand")]
        self.quality = quality
        self.subtomo = subtomo
        self.cut = cut
        self.nn_close_ang = nn_close_ang
        self.metric = "ctf_resolution"
        self._depth = None
        self._yield = None
        self._others = {p.key: p for p in others}
        self._on_overlap = on_overlap
        self._overlap = {"other": next(iter(self._others), None), "radius": None, "min_sigma": None}
        self._overlap_text = None
        self._flight = SingleFlight()

    def build(self) -> None:
        self._render_summary()
        with ui.element("div").style("display: flex; gap: 14px; flex-wrap: wrap; min-width: 0; width: 100%;"):
            with ui.element("div").style(f"flex: 1 1 280px; min-width: 0; {CHART_CELL_STYLE}"):
                self._title(
                    "depth through the pick slab",
                    "Pooled over tomograms with 20 or more picks: picks per tenth of the slab (between its 5th and "
                    "95th percentile; the outer bins hold everything beyond) and the share of each at or above the "
                    "cut.",
                )
                self._depth = ui.echart(self._depth_options()).style(f"height: {CHART_PX}px; width: 100%;")
            with ui.element("div").style(f"flex: 1 1 280px; min-width: 0; {CHART_CELL_STYLE}"):
                with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                    self._title(
                        "yield against quality",
                        "One point per tomogram: its picks at or above the cut against a data-quality fact from the "
                        "tilt-series registry (the numbers the Journey shows). Low yield at normal quality means "
                        "little content; low yield with bad data says nothing about content.",
                    )
                    house_select("x", QUALITY_METRICS, value=self.metric, width="w-40", on_change=self._on_metric)
                self._yield = ui.echart(self._yield_options()).style(f"height: {CHART_PX}px; width: 100%;")
            if self.subtomo and self.subtomo.get("visible_hist"):
                with ui.element("div").style(f"flex: 1 1 220px; min-width: 0; {CHART_CELL_STYLE}"):
                    self._title(
                        "tilts each particle is seen in",
                        "rlnTomoVisibleFrames of the extraction: how many tilts carry each particle. Few views mean a "
                        "weak particle.",
                    )
                    hist = self.subtomo["visible_hist"]
                    keys = sorted(hist, key=int)
                    ui.echart(
                        build_category_bars(keys, [hist[k] for k in keys], x_name="tilts", y_name="particles")
                    ).style(f"height: {CHART_PX}px; width: 100%;")
        if self._others and self._on_overlap is not None:
            self._render_overlap()

    def _render_overlap(self) -> None:
        o = self._overlap
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap;"):
            self._title(
                "overlap with another species",
                "Picks at or above the cut that sit within the radius of the other species' picks (at or above its "
                "σ, when given): one position claimed by two species. Counted on request; nothing is dropped. The "
                "subset rule 'away from another set' acts on it.",
            )
            house_select(
                "with",
                {k: p.label for k, p in self._others.items()},
                value=o["other"],
                width="w-64",
                on_change=lambda e: o.update(other=e.value),
            )
            house_number(
                "within Å", value=o["radius"], width="w-20", step=10, on_change=lambda e: o.update(radius=e.value)
            )
            house_number(
                "its σ ≥", value=o["min_sigma"], width="w-16", step=0.5, on_change=lambda e: o.update(min_sigma=e.value)
            )
            house_button("Count", self._count)
            self._overlap_text = ui.label("").classes(HINT_CLS)

    async def _count(self) -> None:
        async with self._flight("overlap") as acquired:
            if not acquired:
                return
            o = self._overlap
            other = self._others.get(o["other"] or "")
            if other is None or not o["radius"]:
                self._overlap_text.set_text("Pick a set and a radius.")
                return
            self._overlap_text.set_text("counting…")
            res = await self._on_overlap(other, float(o["radius"]), o["min_sigma"])
            if self._overlap_text.is_deleted:
                return
            if not res["success"]:
                self._overlap_text.set_text(res["error"])
                return
            worst = sorted(res["per_tomo"].items(), key=lambda kv: -kv[1])[:5]
            tail = ", ".join(f"{ts_display_name(t)} {n}" for t, n in worst)
            self._overlap_text.set_text(
                f"{res['n_overlap']:,} of {res['n_a']:,} picks ≥ {self.cut:.1f}σ within {o['radius']:g} Å of "
                f"{res['n_b']:,} {other.label} picks" + (f" — most in {tail}" if tail else "")
            )

    @staticmethod
    def _title(text: str, tip: str) -> None:
        with ui.label(text).style(f"{SANS} font-size: 9px; font-weight: 600; color: #334155;"):
            ui.tooltip(tip).style(TOOLTIP_STYLE)

    def _render_summary(self) -> None:
        geos = [c.get("geometry") or {} for c in self.cand_rows]
        planes = [g["plane"] for g in geos if g.get("plane")]
        parts = []
        if planes:
            parts.append(
                f"pick slab: median {statistics.median(p['thickness_ang'] for p in planes):.0f} Å thick, "
                f"{statistics.median(p['tilt_deg'] for p in planes):.0f}° tilt, "
                f"{100 * statistics.median(p['surface_share'] for p in planes):.0f} % of picks at its surfaces "
                f"({len(planes)} tomograms)"
            )
        edges = [g["edge_n"] for g in geos if g.get("edge_n") is not None]
        if edges:
            parts.append(f"{sum(edges):,} picks within {analytics.EDGE_ANG:.0f} Å of a face")
        nn = [g["nn_median_ang"] for g in geos if g.get("nn_median_ang") is not None]
        if nn:
            parts.append(f"nearest neighbour median {statistics.median(nn):.0f} Å")
        close = [g["nn_close_n"] for g in geos if g.get("nn_close_n") is not None]
        if close and self.nn_close_ang:
            parts.append(f"{sum(close):,} picks closer than {self.nn_close_ang:.0f} Å (0.65 Ø) to another")
        if self.subtomo:
            s = self.subtomo
            lost = [r for r in s.get("tomograms") or [] if r.get("lost")]
            if s.get("n_in") is not None:
                parts.append(
                    f"extracted {s['n_out']:,} of {s['n_in']:,} candidates"
                    + (
                        f" ({sum(r['lost'] for r in lost):,} outside the tilt images in {len(lost)} tomograms)"
                        if lost
                        else ""
                    )
                )
        if not parts:
            return
        with ui.label(" · ".join(parts)).classes(HINT_CLS):
            ui.tooltip(
                "Slab: a plane fitted through each tomogram's picks, refitted on the 80 % closest; thickness is the "
                "5th–95th percentile of the picks' distance from it, surface share the picks in its outer tenths and "
                "beyond. Close pairs overlap if the particles are as large as the species diameter says: a particle "
                "picked twice, or a diameter that does not fit. Nothing is dropped by these numbers."
            ).style(TOOLTIP_STYLE)

    def _depth_options(self) -> dict:
        prof = analytics.depth_profile(self.cand_rows, self.cut)
        return build_depth_profile_chart(prof["n"], prof["at_cut"], cut=self.cut)

    def _yield_options(self) -> dict:
        counts = analytics.counts_at_cut(self.cand_rows, self.cut).per_tomo
        points = []
        for r in self.rows:
            if not r.get("cand"):
                continue
            x = r.get("sigma") if self.metric == "sigma" else (self.quality.get(r["tomo"]) or {}).get(self.metric)
            if x is None:
                continue
            points.append((round(float(x), 4), counts.get(r["tomo"], 0), ts_display_name(r["tomo"])))
        return build_yield_scatter(points, x_name=QUALITY_METRICS[self.metric], y_name=f"picks ≥ {self.cut:.1f}σ")

    def _on_metric(self, e) -> None:
        if e.value and e.value != self.metric:
            self.metric = e.value
            self._push(self._yield, self._yield_options)

    def set_cut(self, cut: float) -> None:
        self.cut = cut
        self._push(self._depth, self._depth_options)
        self._push(self._yield, self._yield_options)

    @staticmethod
    def _push(chart, options) -> None:
        if chart is not None and not chart.is_deleted:
            chart.options.clear()
            chart.options.update(options())
            chart.update()
