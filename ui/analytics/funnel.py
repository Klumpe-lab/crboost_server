"""The species funnel in this project: template-matched tomograms → candidates → at or above the
cut → extracted → curated → class-kept → refined. Each step names where its number comes from; a
job's number opens that job. The "≥ cut" step follows the cut live.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nicegui import ui

from services.particles import analytics
from ui.analytics.common import HINT_CLS, TOOLTIP_STYLE
from ui.styles import MONO, SANS


@dataclass(frozen=True, slots=True)
class FunnelStep:
    label: str
    value: str
    tip: str
    instance_id: str | None = None  # opens this job on click


class FunnelLine:
    def __init__(
        self,
        steps: list[FunnelStep],
        cand_rows: list[dict[str, Any]],
        *,
        cut: float,
        cut_index: int,
        open_job: Callable[[str], Any] | None,
    ) -> None:
        self.steps = steps
        self.cand_rows = cand_rows
        self.cut = cut
        self.cut_index = cut_index  # where the live "≥ cut" step goes
        self._open_job = open_job
        self._cut_value = None
        self._cut_label = None

    def build(self) -> None:
        with ui.element("div").style("display: flex; align-items: baseline; gap: 6px; flex-wrap: wrap;"):
            for i, step in enumerate(self.steps):
                if i == self.cut_index:
                    self._cut_step()
                    self._arrow()
                self._step(step)
                if i < len(self.steps) - 1:
                    self._arrow()

    def _cut_step(self) -> None:
        with ui.element("div").style("display: flex; flex-direction: column;"):
            self._cut_label = ui.label("").style(f"{SANS} font-size: 9px; color: #94a3b8;")
            self._cut_value = ui.label("").style(f"{MONO} font-size: 11px; color: #1e293b;")
            ui.tooltip("Candidates at or above the cut on screen. Not a set until you make one.").style(TOOLTIP_STYLE)
        self.set_cut(self.cut)

    def _step(self, step: FunnelStep) -> None:
        with ui.element("div").style("display: flex; flex-direction: column;"):
            ui.label(step.label).style(f"{SANS} font-size: 9px; color: #94a3b8;")
            value = ui.label(step.value).style(f"{MONO} font-size: 11px; color: #1e293b;")
            if step.instance_id and self._open_job is not None:
                value.style("cursor: pointer; text-decoration: underline dotted;")
                value.on("click", lambda _e, i=step.instance_id: self._open_job(i))
            ui.tooltip(step.tip).style(TOOLTIP_STYLE)

    @staticmethod
    def _arrow() -> None:
        ui.label("→").classes(HINT_CLS)

    def set_cut(self, cut: float) -> None:
        self.cut = cut
        if self._cut_value is None or self._cut_value.is_deleted:
            return
        counts = analytics.counts_at_cut(self.cand_rows, cut)
        self._cut_label.set_text(f"≥ {cut:.1f}σ")
        self._cut_value.set_text(f"{counts.n_picks:,}")
