"""Per-project rows of a cross-project merge: tomograms, particles, the σ ladder and the count at
or above the cut, one row per contributing project. The cut column follows the cut live."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from nicegui import ui

from services.particles import analytics
from services.particles.sigma import LADDER_SIGMAS
from ui.analytics.common import esc

_SHOWN = tuple(z for z in LADDER_SIGMAS if z > 7.0)


class ProjectBreakdown:
    def __init__(self, rows: list[dict[str, Any]], projects: dict[str, str], *, cut: float) -> None:
        self.groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            if r.get("cand"):
                self.groups[projects.get(r["tomo"], "(unknown project)")].append(r["cand"])
        self.cut = cut
        self._html = None

    def build(self) -> None:
        with ui.element("div").classes("cb-atable-wrap").style("max-height: none;"):
            self._html = ui.html(self._content(), sanitize=False)

    def set_cut(self, cut: float) -> None:
        self.cut = cut
        if self._html is not None and not self._html.is_deleted:
            self._html.set_content(self._content())

    def _content(self) -> str:
        head = (
            "<th>project</th><th class='n'>tomograms</th><th class='n'>particles</th>"
            + "".join(f"<th class='n'>≥{z:g}σ</th>" for z in _SHOWN)
            + f"<th class='n'>≥ cut {self.cut:.1f}σ</th>"
        )
        body = []
        for project in sorted(self.groups):
            cands = self.groups[project]
            ladder = analytics.ladder_totals(cands)
            at_cut = analytics.counts_at_cut(cands, self.cut)
            cells = [
                f"<td class='nm'>{esc(project)}</td>",
                f"<td class='n'>{len(cands)}</td>",
                f"<td class='n'>{sum(c['n'] for c in cands):,}</td>",
                *[f"<td class='n'>{n:,} ({t})</td>" for z, n, t in ladder if z in _SHOWN],
                f"<td class='n'>{at_cut.n_picks:,} ({at_cut.n_tomograms})</td>",
            ]
            body.append("<tr>" + "".join(cells) + "</tr>")
        return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table>'
