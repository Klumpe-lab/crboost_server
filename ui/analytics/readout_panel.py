"""Results of a Class 3D or a Reconstruct particle job, from its readout record
(services/particles/class_readout.py).

Class 3D: convergence (class shares per iteration), the class table (size, resolution, score bins,
tomogram spread, concentration marker, projects), the σ-bin × class shares, each class map's central
slices with copyable open commands, the most and least confidently assigned members of each class,
and "Make class subset".

Reconstruct particle: the map's central slices and the half-map FSC with its 0.143 / 0.5 resolutions.
"""

from __future__ import annotations

import math
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.array_tasks import ts_display_name
from services.dashboard_data import vis_asset_url
from services.particles.class_readout import CONCENTRATION_RULE, CLASS3D_KIND, class_iteration_table
from services.particles.records import Record
from ui.analytics.common import (
    TABLE_ON_PANEL_STYLE,
    CHART_CELL_STYLE,
    HINT_CLS,
    LABEL_CLS,
    TOOLTIP_STYLE,
    esc,
    render_provenance,
)
from ui.components.buttons import house_button
from ui.components.check import house_check
from ui.components.fields import house_text
from ui.components.reactive import SingleFlight
from ui.components.viewer_chips import viewer_chips
from ui.components.plot import fsc_plot, plot, shares_plot
from ui.dashboard.figures import class_color
from ui.styles import MONO, SANS

# The plots' own size: they shrink with a narrower cell and keep their proportions.
PLOT_WH = (520, 180)
# Three central slices side by side, at the 160 px a tile is rendered at.
SLICES_W, SLICES_H = 360, 120
INK = "#1e293b"
MUTED = "#94a3b8"
NO_ESTIMATE = (
    "RELION gave no resolution estimate for this class (it wrote inf): its estimate failed, which says nothing "
    "about the map. Look at the map."
)


def _swatch(k: int, px: int = 8) -> str:
    """The class's colour as a small square: the colour carries the class, the text beside it stays in ink."""
    return (
        f"<span style='display:inline-block;width:{px}px;height:{px}px;border-radius:var(--cb-r-sm, 2px);"
        f"background:{class_color(k)};vertical-align:middle'></span>"
    )


def _class_label(k: int, extra: str = "", *, width: str | None = None) -> None:
    """`■ class k` in ink, the square in the class's colour, and `extra` muted after it."""
    w = f"width: {width};" if width else ""
    html = f"{_swatch(k)} <span style='color:{INK};font-weight:600'>class {k}</span>"
    if extra:
        html += f" <span style='color:#64748b'>{esc(extra)}</span>"
    ui.html(html, sanitize=False).style(f"{SANS} font-size: 10px; white-space: nowrap; {w}")


def _map_preview(png: str | None, volume: str, what: str) -> None:
    """A volume's central slices with the napari / 3dmod / ChimeraX chips in their corner."""
    with ui.element("div").style(
        f"position: relative; width: {SLICES_W}px; height: {SLICES_H}px; background: #0f172a; "
        "border-radius: var(--cb-r-md, 4px); overflow: hidden;"
    ):
        if png:
            ui.image(vis_asset_url(png)).style(f"width: {SLICES_W}px; height: {SLICES_H}px;").props("fit=contain")
        else:
            ui.label("map not on disk").classes(HINT_CLS).style("padding: 6px;")
        viewer_chips(volume, what=what, chimerax=True)


def _tomogram_names(classes: list[dict]) -> dict[str, str]:
    """What the class table calls each tomogram: its position, and before it what tells its dataset from the
    others' when the table holds more than one (a pooled set: every project has a Position_10). The datasets'
    common start and end are dropped: lab_20260311_grid2_rerun_Position_10 -> 20260311_grid2 · Position_10."""
    full = {t for c in classes for t, _n in c.get("top_tomograms") or []}
    pos = {t: ts_display_name(t) for t in full}
    source = {t: t[: -len(pos[t])].rstrip("_") if pos[t] != t and t.endswith(pos[t]) else "" for t in full}
    sources = sorted({v for v in source.values() if v})
    if len(sources) < 2:
        return pos
    head = os.path.commonprefix(sources)
    tail = os.path.commonprefix([x[::-1] for x in sources])[::-1]
    head, tail = head[: head.rfind("_") + 1], tail[tail.find("_") :] if "_" in tail else ""
    out = {}
    for t in full:
        src = source[t]
        short = src[len(head) : len(src) - len(tail)] if src else ""
        out[t] = f"{short or src} · {pos[t]}" if src else pos[t]
    return out


class ReadoutPanel:
    def __init__(
        self,
        record: Record,
        *,
        decoys: dict[int, int] | None = None,
        on_make_subset: Callable[[list[int], str], Awaitable[dict]] | None = None,
    ) -> None:
        self.record = record
        self.decoys = {int(k): int(v) for k, v in (decoys or {}).items()}
        self._on_make_subset = on_make_subset
        self._chosen: set[int] = set()
        self._name = ""
        self._status = None
        self._flight = SingleFlight()

    def build(self) -> None:
        render_provenance([("readout", self.record)])
        if self.record.provenance.kind == CLASS3D_KIND:
            self._class3d()
        else:
            self._reconstruct()

    # ── Class 3D ──

    def _class3d(self) -> None:
        d = self.record.data
        ui.label(
            f"iteration {d['final_iteration']} · {d['n_classes']} classes · {d['n_particles']:,} particles"
            + (f" · scores from {Path(d['sigma_from']).name}" if d.get("sigma_from") else " · no scores in σ")
        ).classes(HINT_CLS)
        its, per = class_iteration_table(self.record)
        with ui.element("div").style("display: flex; gap: 14px; flex-wrap: wrap; min-width: 0; width: 100%;"):
            with ui.element("div").style(f"flex: 1 1 280px; min-width: 0; {CHART_CELL_STYLE}"):
                with ui.label("convergence").style(f"{SANS} font-size: 9px; font-weight: 600; color: #334155;"):
                    ui.tooltip(
                        "Class shares at each iteration (model.star). The classification has settled when the bars "
                        "stop changing."
                    ).style(TOOLTIP_STYLE)
                plot(shares_plot(its, per, class_color), *PLOT_WH)
            with ui.element("div").style(f"flex: 1 1 280px; min-width: 0; {CHART_CELL_STYLE}"):
                with ui.label("each score bin, by class").style(
                    f"{SANS} font-size: 9px; font-weight: 600; color: #334155;"
                ):
                    ui.tooltip(
                        "For each band of template-matching score, the share of its particles in each class: the σ "
                        "below which the good classes stop being the majority is where background takes over."
                    ).style(TOOLTIP_STYLE)
                # One column per class: wider than its chart cell with many classes, so it scrolls in the cell.
                ui.html(self._bin_shares_html(), sanitize=False).style("overflow-x: auto;")
        ui.html(self._class_table_html(), sanitize=False).classes("cb-atable-wrap").style(
            f"max-height: none; {TABLE_ON_PANEL_STYLE}"
        )
        self._class_maps()
        self._galleries()
        if self._on_make_subset is not None:
            self._subset_controls()

    def _class_table_html(self) -> str:
        d = self.record.data
        bins = d.get("sigma_bins") or []
        total = max(1, d["n_particles"])
        resolutions = {}
        if d.get("iterations"):
            resolutions = {c["class"]: c.get("resolution_ang") for c in d["iterations"][-1]["classes"]}
        names = _tomogram_names(d["classes"])
        with_projects = any(c.get("by_project") for c in d["classes"])
        head = (
            "<th>class</th><th class='n'>particles</th><th class='n'>%</th>"
            "<th class='n' title='RELION&#39;s own estimate for each class map (model.star), not a gold-standard "
            "FSC: a guide to which classes hold structure, never a resolution to report.'>Class 3D est. Å</th>"
            + "".join(f"<th class='n' title='{esc(b)}σ'>{esc(b)}</th>" for b in bins)
            + "<th class='n'>tomograms</th><th>biggest tomograms</th>"
            + ("<th>projects</th>" if with_projects else "")
            + (
                "<th class='n' title='Random-position decoys (0σ) that this class took in: the classifier's "
                "false acceptances.'>decoys</th>"
                if self.decoys
                else ""
            )
            + f"<th title='{esc(CONCENTRATION_RULE)}'>spread</th>"
        )
        rows = []
        for c in d["classes"]:
            k = c["class"]
            res = resolutions.get(k)
            if res is None:
                res_cell = "<td class='n'>—</td>"
            elif not math.isfinite(res):
                res_cell = f"<td class='n' title='{esc(NO_ESTIMATE)}' style='color:{MUTED};cursor:help'>none</td>"
            else:
                res_cell = f"<td class='n'>{res:.1f}</td>"
            cells = [
                f"<td class='nm'>{_swatch(k)} {k}</td>",
                f"<td class='n'>{c['n']:,}</td>",
                f"<td class='n'>{100 * c['n'] / total:.1f}</td>",
                res_cell,
                *[f"<td class='n'>{v:,}</td>" for v in c.get("sigma_bins") or [""] * len(bins)],
                f"<td class='n'>{c.get('n_tomograms', '—')}</td>",
                "<td title='"
                + esc("\n".join(t for t, _n in c.get("top_tomograms") or []))
                + "'>"
                + "<br>".join(esc(f"{names.get(t, t)} ({n})") for t, n in c.get("top_tomograms") or [])
                + "</td>",
                (
                    "<td>" + esc(", ".join(f"{p} {n}" for p, n in (c.get("by_project") or {}).items())) + "</td>"
                    if with_projects
                    else ""
                ),
                self._decoy_cell(k, c["n"]),
                (
                    f"<td title='{esc(CONCENTRATION_RULE)} Ratio {c.get('concentration')}.' "
                    f"style='color:#64748b;cursor:help'>concentrated</td>"
                    if c.get("concentrated")
                    else "<td></td>"
                ),
            ]
            rows.append("<tr>" + "".join(cells) + "</tr>")
        return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>'

    def _decoy_cell(self, k: int, n: int) -> str:
        if not self.decoys:
            return ""
        d = self.decoys.get(k, 0)
        return f"<td class='n'>{d:,} ({100 * d / max(1, n):.0f} %)</td>"

    def _bin_shares_html(self) -> str:
        d = self.record.data
        bins = d.get("sigma_bins") or []
        classes = d["classes"]
        if not bins or not classes or "sigma_bins" not in classes[0]:
            return f"<span class='{HINT_CLS}'>These particles carry no score in σ.</span>"
        head = "<th>σ</th><th class='n'>n</th>" + "".join(
            f"<th class='n'>{_swatch(c['class'], 7)} {c['class']}</th>" for c in classes
        )
        rows = []
        for i, b in enumerate(bins):
            n = sum(c["sigma_bins"][i] for c in classes)
            cells = [f"<td class='nm'>{esc(b)}</td>", f"<td class='n'>{n:,}</td>"]
            for c in classes:
                share = 100.0 * c["sigma_bins"][i] / n if n else 0.0
                top = max(x["sigma_bins"][i] for x in classes)
                weight = "font-weight:700;" if n and c["sigma_bins"][i] == top else ""
                cells.append(f"<td class='n' style='{weight}'>{'' if not n else f'{share:.0f}%'}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")
        return f'<table class="cb-atable"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>'

    def _class_maps(self) -> None:
        maps = self.record.data.get("class_maps") or []
        if not maps:
            return
        d = self.record.data
        total = max(1, d["n_particles"])
        sizes = {c["class"]: c["n"] for c in d["classes"]}
        with ui.label("class maps · central slices XY | XZ | YZ").classes(LABEL_CLS):
            ui.tooltip(
                "Each slice is scaled to its own 1–99th percentile, so a weak class looks as contrasted as a strong "
                "one: compare shapes here, and open the maps (napari, 3dmod, ChimeraX) to compare densities."
            ).style(TOOLTIP_STYLE)
        with ui.element("div").style("display: flex; gap: 14px; flex-wrap: wrap;"):
            for m in maps:
                k = m["class"]
                with ui.element("div").style("display: flex; flex-direction: column; gap: 3px;"):
                    n = sizes.get(k)
                    _class_label(k, "" if n is None else f"{n:,} · {100 * n / total:.1f} %")
                    _map_preview(m.get("png"), m["map"], "class map")

    def _galleries(self) -> None:
        g = self.record.data.get("galleries") or {}
        per = g.get("classes") or {}
        if not per:
            if g.get("note"):
                ui.label(f"No member gallery: {g['note']}.").classes(HINT_CLS)
            return
        with ui.label(f"members · most and least confident by {g.get('by')}").classes(LABEL_CLS):
            ui.tooltip(
                "Each class's members with the highest and the lowest value, as the mean of their extracted 2D "
                "stacks over the tilts they are visible in. A class of junk shows it here first."
            ).style(TOOLTIP_STYLE)
        for k in sorted(per, key=int):
            with ui.element("div").style("display: flex; align-items: center; gap: 8px; flex-wrap: wrap;"):
                _class_label(int(k), width="62px")
                for end in ("best", "worst"):
                    self._tiles(per[k].get(end) or {}, end)

    @staticmethod
    def _tiles(entry: dict[str, Any], end: str) -> None:
        atlas, meta = entry.get("atlas"), entry.get("layout")
        if not atlas or not meta:
            ui.label(f"{end}: no images").classes(HINT_CLS)
            return
        tile = int(meta.get("tile_px", 96))
        shown = 48
        cols, rows = int(meta.get("cols", 8)), int(meta.get("rows", 1))
        url = vis_asset_url(atlas)
        values = entry.get("values") or []
        # Eight 48 px tiles and the label need ~430 px; a narrower panel wraps them.
        with ui.element("div").style("display: flex; flex-wrap: wrap; gap: 2px;"):
            ui.label(end).style(f"{SANS} font-size: 9px; color: {MUTED}; width: 30px;")
            for i in range(len(values)):
                pos = (meta.get("index") or {}).get(str(i))
                if not pos:
                    continue
                r, c = pos
                scale = shown / tile
                ui.element("div").style(
                    f"width: {shown}px; height: {shown}px; background-image: url({url}); "
                    f"background-size: {cols * tile * scale}px {rows * tile * scale}px; "
                    f"background-position: {-c * tile * scale}px {-r * tile * scale}px; "
                    "border-radius: var(--cb-r-sm, 2px);"
                ).tooltip(f"{end} #{i + 1}: {values[i]}")

    def _subset_controls(self) -> None:
        d = self.record.data
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-top: 4px;"
        ):
            ui.label("classes").classes(LABEL_CLS)
            for c in d["classes"]:
                with ui.element("div").style("display: flex; align-items: center; gap: 0;"):
                    ui.html(_swatch(c["class"]), sanitize=False)
                    house_check(str(c["class"]), on_change=lambda e, k=c["class"]: self._toggle(k, e.value))
            house_text(
                "name",
                value="",
                placeholder="e.g. ribosome classes",
                width="w-48",
                on_change=lambda e: setattr(self, "_name", e.value or ""),
            )
            house_button(
                "Make class subset",
                self._make,
                kind="accent",
                tooltip="A new particle set of the chosen classes' members from the last iteration, with their refined "
                "poses. The classification is not changed.",
            )
            self._status = ui.label("").classes(HINT_CLS)

    def _toggle(self, k: int, on: bool) -> None:
        if on:
            self._chosen.add(k)
        else:
            self._chosen.discard(k)

    async def _make(self) -> None:
        async with self._flight("make") as acquired:
            if not acquired:
                return
            if not self._chosen:
                self._status.set_text("Choose at least one class.")
                return
            name = self._name.strip() or "classes " + "+".join(str(k) for k in sorted(self._chosen))
            res = await self._on_make_subset(sorted(self._chosen), name)
            if not res["success"]:
                self._status.set_text(res["error"])
                return
            ps = res["set"]
            self._status.set_text(f"Set {ps.name}: {ps.n_particles:,} particles")

    # ── Reconstruct particle ──

    def _reconstruct(self) -> None:
        d = self.record.data
        with ui.element("div").style("display: flex; gap: 14px; flex-wrap: wrap; align-items: flex-start;"):
            with ui.element("div").style("display: flex; flex-direction: column; gap: 2px;"):
                ui.label("merged.mrc · central slices XY | XZ | YZ").classes(LABEL_CLS)
                _map_preview(d["slices_png"], d["merged"], "map")
            fsc = d.get("fsc")
            with ui.element("div").style(f"flex: 1 1 280px; min-width: 0; {CHART_CELL_STYLE}"):
                if not fsc:
                    ui.label(d.get("note") or "No half maps.").classes(HINT_CLS)
                    return
                parts = []
                for name, c in fsc.items():
                    r = c.get("resolution") or {}
                    label = "unmasked" if name == "unmasked" else f"sphere Ø {d.get('mask_diameter_ang'):g} Å"
                    parts.append(
                        f"{label}: FSC 0.143 at {r.get('0.143') or 'Nyquist'} Å, 0.5 at {r.get('0.5') or 'Nyquist'} Å"
                    )
                with ui.label(" · ".join(parts)).style(f"{MONO} font-size: 10px; color: #1e293b;"):
                    ui.tooltip(
                        "Fourier shell correlation between half1.mrc and half2.mrc, over the whole box and inside a "
                        "soft sphere of the species diameter. The halves come from RELION's random split; particles "
                        "closer than a diameter in opposite halves inflate it."
                    ).style(TOOLTIP_STYLE)
                plot(fsc_plot({k: v for k, v in fsc.items()}, *PLOT_WH), *PLOT_WH)
