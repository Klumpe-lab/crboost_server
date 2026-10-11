"""Scores in σ for one candidate set: stat tiles, the σ ladder, the cut with live counts, and the
σ histogram and survival curve (with the Gaussian null drawn over it).

Built once per record. A cut change — typed, or a click on a histogram bar — updates the counts,
the capped-tomogram flag and both charts' cut lines in place; nothing rebuilds.
"""

from __future__ import annotations

import statistics
from collections.abc import Callable
from typing import Any

from nicegui import ui

from services.array_tasks import ts_display_name
from services.particles import analytics
from ui.analytics.common import LABEL_CLS, TOOLTIP_STYLE, WARN_COLOR, fmt_sigma
from ui.components.buttons import house_button
from ui.components.fields import house_number
from ui.dashboard.figures import build_sigma_histogram, build_survival_chart
from ui.styles import MONO, SANS

HIST_BIN_SIGMA = 0.25
SURVIVAL_STEP_SIGMA = 0.1
CHART_PX = 150

SIGMA_TIP = (
    "σ is the noise level of one correlation in that tomogram (PyTOM's job_stats.std, also rlnSearchStd in "
    "each candidate). A score in σ is LCC ÷ σ of its own tomogram, so 9σ means the same in every tomogram "
    "and for every template; raw LCC does not."
)
FP1_TIP = (
    "The score Gaussian noise would beat once per tomogram, given its search space N (voxels × rotations): "
    "erfcinv(2/N)·√2 σ. Real background (membranes, edges, granules, gold) is not Gaussian, so this is a gate, "
    "not an error rate."
)
SURVIVAL_TIP = (
    "How many picks score at or above z (log scale), against z. The dashed curve is what pure Gaussian noise "
    "would give, N·Q(z) summed over the tomograms; the gap between the curves is what the data hold beyond the "
    "noise model. Below the extraction cutoff there are no candidates, and where the cap bound the observed "
    "curve is flattened."
)
HIST_TIP = "Picks per 0.25σ bin. Dashed: the FP=1 cutoff and the cut. Click a bar to move the cut to it."


class ScorePanel:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        cand: dict[str, Any] | None,
        *,
        cut: float,
        on_cut: Callable[[float], None] | None = None,
        on_make_subset: Callable[[], Any] | None = None,
    ) -> None:
        self.rows = rows
        self.cand = cand or {}
        self.cand_rows = [r["cand"] for r in rows if r.get("cand")]
        self.cut = cut
        self._on_cut = on_cut
        self._on_make_subset = on_make_subset
        scores = analytics.pooled_scores(self.cand_rows)
        self._edges, self._counts = analytics.sigma_histogram(scores, width=HIST_BIN_SIGMA)
        self._survival = analytics.survival_curves(self.cand_rows, step=SURVIVAL_STEP_SIGMA)
        fp1 = [float(r["fp1_sigma"]) for r in rows if r.get("fp1_sigma")]
        self._fp1 = statistics.median(fp1) if fp1 else None
        self._cut_input = None
        self._cut_text = None
        self._capped_text = None
        self._capped_tip = None
        self._hist = None
        self._surv = None

    # ── Layout ──

    def build(self) -> None:
        self._render_tiles()
        self._render_ladder()
        if self.cand_rows:
            self._render_cut_row()
            self._render_charts()
            self._update_cut_numbers()

    def _render_tiles(self) -> None:
        sigmas = [float(r["sigma"]) for r in self.rows if r.get("sigma")]
        n_done = sum(1 for r in self.rows if r.get("done"))
        cutoffs = [float(c["cutoff_sigma"]) for c in self.cand_rows if c.get("cutoff_sigma") is not None]
        cap = self.cand.get("cap")
        n_bound = sum(1 for c in self.cand_rows if c.get("cap_bound"))
        method = self.cand.get("cutoff_method") or ""
        how = (
            f"false positives = {self.cand.get('false_positives')}"
            if method == "false_positives"
            else (f"manual LCC ≥ {self.cand.get('cc_threshold')}" if method == "manual" else method or "unknown")
        )
        tiles: list[tuple[str, str, str]] = [
            ("tomograms", f"{n_done}/{len(self.rows)}" if self.rows else "—", "Template-matched / in the job."),
            (
                "candidates",
                f"{self.cand.get('n_total', 0):,}" if self.cand else "—",
                f"In {self.cand.get('n_with_picks', 0)} tomograms." if self.cand else "No extraction yet.",
            ),
            (
                "σ (median)",
                f"{statistics.median(sigmas):.4g}" if sigmas else "—",
                SIGMA_TIP + (f"\nRange {min(sigmas):.4g}–{max(sigmas):.4g}." if sigmas else ""),
            ),
            ("FP=1 cutoff", fmt_sigma(self._fp1, 2), FP1_TIP),
            (
                "extraction cutoff",
                fmt_sigma(statistics.median(cutoffs), 2) if cutoffs else "—",
                f"The cutoff the extraction job used ({how}), in σ. Every candidate scores at or above it.",
            ),
            (
                "cap",
                f"{cap} · bound in {n_bound}" if cap else "—",
                analytics.MARKER_RULES["cap_bound"] + " " + analytics.MARKERS_DROP_NOTHING,
            ),
        ]
        with ui.element("div").classes("cb-stats").style("margin: 2px 0 4px; gap: 22px;"):
            for label, value, tip in tiles:
                with ui.element("div").classes("cb-stat"):
                    ui.label(label).classes("cb-stat-label")
                    ui.label(value).classes("cb-stat-value")
                    ui.tooltip(tip).style(TOOLTIP_STYLE)

    def _render_ladder(self) -> None:
        if not self.cand_rows:
            return
        with ui.element("div").style("display: flex; align-items: baseline; gap: 14px; flex-wrap: wrap;"):
            with ui.label("σ ladder").classes(LABEL_CLS):
                ui.tooltip(
                    "Picks scoring at or above each σ level, and in how many tomograms. Fixed levels, the same in "
                    "every project and for every species."
                ).style(TOOLTIP_STYLE)
            for z, n, n_tomo in analytics.ladder_totals(self.cand_rows):
                ui.label(f"≥ {z:g}σ {n:,} ({n_tomo})").style(f"{MONO} font-size: 10px; color: #334155;").tooltip(
                    f"{n:,} picks at or above {z:g}σ, in {n_tomo} tomograms"
                )

    def _render_cut_row(self) -> None:
        lo = min((float(c["cutoff_sigma"]) for c in self.cand_rows if c.get("cutoff_sigma") is not None), default=0.0)
        with ui.element("div").style("display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin: 4px 0;"):
            self._cut_input = house_number(
                "σ cut",
                value=round(self.cut, 2),
                min=0.0,
                max=100.0,
                step=0.1,
                format="%.1f",
                width="w-16",
                hint=f"Picks at or above this score, in σ. The lowest candidate scores {lo:.2f}σ (the extraction "
                "cutoff), so a cut there keeps everything. The cut chooses what you look at; it changes no file.",
                on_change=self._on_cut_field,
            )
            self._cut_text = ui.label("").style(f"{MONO} font-size: 10px; color: #1e293b;")
            with ui.label("").style(f"{SANS} font-size: 10px; color: {WARN_COLOR};") as capped:
                self._capped_tip = ui.tooltip("").style(TOOLTIP_STYLE)
            self._capped_text = capped
            if self._on_make_subset is not None:
                house_button(
                    "Make subset…",
                    self._on_make_subset,
                    kind="accent",
                    tooltip="Create a new particle set from these candidates with this cut (and any other rules). "
                    "The candidates stay as they are; the subset is a new set with its recipe recorded.",
                )

    def _render_charts(self) -> None:
        with ui.element("div").style("display: flex; gap: 14px; flex-wrap: wrap; min-width: 0; width: 100%;"):
            with ui.element("div").style("flex: 1 1 280px; min-width: 0;"):
                ui.label("score histogram").style(f"{SANS} font-size: 9px; font-weight: 600; color: #334155;").tooltip(
                    HIST_TIP
                )
                self._hist = ui.echart(self._hist_options(), on_point_click=self._on_hist_click).style(
                    f"height: {CHART_PX}px; width: 100%;"
                )
            with ui.element("div").style("flex: 1 1 280px; min-width: 0;"):
                ui.label("picks ≥ z vs Gaussian noise").style(
                    f"{SANS} font-size: 9px; font-weight: 600; color: #334155;"
                ).tooltip(SURVIVAL_TIP)
                self._surv = ui.echart(self._surv_options()).style(f"height: {CHART_PX}px; width: 100%;")

    # ── The cut ──

    def _hist_options(self) -> dict:
        return build_sigma_histogram(self._edges, self._counts, width=HIST_BIN_SIGMA, fp1=self._fp1, cut=self.cut)

    def _surv_options(self) -> dict:
        s = self._survival
        return build_survival_chart(s["z"], s["observed"], s["null"], cut=self.cut)

    def _on_cut_field(self, e) -> None:
        if e.value is None:
            return  # half-typed or cleared: keep the last cut
        self.set_cut(float(e.value))

    def _on_hist_click(self, e) -> None:
        value = getattr(e, "value", None)
        x = value[0] if isinstance(value, list | tuple) and value else None
        if x is None:
            return
        cut = round(float(x) - HIST_BIN_SIGMA / 2, 2)
        if self._cut_input is not None:
            self._cut_input.value = cut  # fires _on_cut_field
        else:
            self.set_cut(cut)

    def set_cut(self, cut: float) -> None:
        if abs(cut - self.cut) < 1e-9:
            return
        self.cut = cut
        self._update_cut_numbers()
        for chart, options in ((self._hist, self._hist_options), (self._surv, self._surv_options)):
            if chart is not None and not chart.is_deleted:
                chart.options.clear()
                chart.options.update(options())
                chart.update()
        if self._on_cut is not None:
            self._on_cut(cut)

    def _update_cut_numbers(self) -> None:
        if self._cut_text is None or self._cut_text.is_deleted:
            return
        counts = analytics.counts_at_cut(self.cand_rows, self.cut)
        self._cut_text.set_text(f"→ {counts.n_picks:,} picks in {counts.n_tomograms} tomograms")
        capped = analytics.capped_above_cut(self.cand_rows, self.cut)
        if capped:
            self._capped_text.set_text(f"⚑ {len(capped)} capped tomograms with every pick ≥ cut")
            names = ", ".join(f"{ts_display_name(c['tomo'])} (last {c['last_sigma']}σ)" for c in capped)
            self._capped_tip.set_text(
                f"{names}\n{analytics.MARKER_RULES['cap_above_cut']} {analytics.MARKERS_DROP_NOTHING}"
            )
        else:
            self._capped_text.set_text("")
            self._capped_tip.set_text("")
