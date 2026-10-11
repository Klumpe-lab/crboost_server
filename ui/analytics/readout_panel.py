"""Results of a Class 3D or a Reconstruct particle job, from its readout record
(services/particles/class_readout.py).

Class 3D: convergence (class shares per iteration), the class table (size, resolution, score bins,
tomogram spread, concentration marker, projects), the σ-bin × class shares, each class map's central
slices with copyable open commands, the most and least confidently assigned members of each class,
and "Make class subset".

Reconstruct particle: the map's central slices and the half-map FSC with its 0.143 / 0.5 resolutions.
"""

from __future__ import annotations

import shlex
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.array_tasks import ts_display_name
from services.dashboard_data import vis_asset_url
from services.particles.class_readout import CONCENTRATION_RULE, CLASS3D_KIND, class_iteration_table
from services.particles.records import Record
from ui.analytics.common import HINT_CLS, LABEL_CLS, TOOLTIP_STYLE, esc, render_provenance
from ui.components.buttons import house_button
from ui.components.copyable import copy_on_click
from ui.components.fields import house_text
from ui.components.reactive import SingleFlight
from ui.dashboard.figures import build_class_convergence_chart, build_fsc_chart, class_color
from ui.styles import MONO, SANS

CHART_PX = 150


def _open_commands(path: str) -> None:
    """'3dmod' and 'ChimeraX' labels that copy the command opening `path`."""
    for label, cmd in (("3dmod", f"3dmod {shlex.quote(path)}"), ("ChimeraX", f"chimerax {shlex.quote(path)}")):
        el = ui.label(label).style(f"{MONO} font-size: 9px; color: #6366f1;")
        copy_on_click(el, cmd)
        el.tooltip(f"Copy: {cmd}")


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
            with ui.element("div").style("flex: 1 1 280px; min-width: 0;"):
                with ui.label("convergence").style(f"{SANS} font-size: 9px; font-weight: 600; color: #334155;"):
                    ui.tooltip(
                        "Class shares at each iteration (model.star). The classification has settled when the bars "
                        "stop changing."
                    ).style(TOOLTIP_STYLE)
                ui.echart(build_class_convergence_chart(its, per)).style(f"height: {CHART_PX}px; width: 100%;")
            with ui.element("div").style("flex: 1 1 280px; min-width: 0;"):
                with ui.label("each score bin, by class").style(
                    f"{SANS} font-size: 9px; font-weight: 600; color: #334155;"
                ):
                    ui.tooltip(
                        "For each band of template-matching score, the share of its particles in each class: the σ "
                        "below which the good classes stop being the majority is where background takes over."
                    ).style(TOOLTIP_STYLE)
                ui.html(self._bin_shares_html(), sanitize=False)
        ui.html(self._class_table_html(), sanitize=False).classes("cb-atable-wrap").style("max-height: none;")
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
        head = (
            "<th>class</th><th class='n'>particles</th><th class='n'>%</th><th class='n'>res Å</th>"
            + "".join(f"<th class='n' title='{esc(b)}σ'>{esc(b)}</th>" for b in bins)
            + "<th class='n'>tomograms</th><th>biggest tomograms</th><th>projects</th>"
            + (
                "<th class='n' title='Random-position decoys (0σ) that this class took in: the classifier's "
                "false acceptances.'>decoys</th>"
                if self.decoys
                else ""
            )
            + "<th></th>"
        )
        rows = []
        for c in d["classes"]:
            k = c["class"]
            swatch = (
                "<span style='display:inline-block;width:8px;height:8px;border-radius:2px;"
                f"background:{class_color(k)}'></span>"
            )
            res = resolutions.get(k)
            cells = [
                f"<td class='nm'>{swatch} {k}</td>",
                f"<td class='n'>{c['n']:,}</td>",
                f"<td class='n'>{100 * c['n'] / total:.1f}</td>",
                f"<td class='n'>{'—' if not res else f'{res:.1f}'}</td>",
                *[f"<td class='n'>{v:,}</td>" for v in c.get("sigma_bins") or [""] * len(bins)],
                f"<td class='n'>{c.get('n_tomograms', '—')}</td>",
                "<td>"
                + esc(", ".join(f"{ts_display_name(t)} ({n})" for t, n in c.get("top_tomograms") or []))
                + "</td>",
                "<td>" + esc(", ".join(f"{p} {n}" for p, n in (c.get("by_project") or {}).items())) + "</td>",
                self._decoy_cell(k, c["n"]),
                (
                    f"<td class='mk' title='{esc(CONCENTRATION_RULE)} Ratio {c.get('concentration')}.'>"
                    "concentrated</td>"
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
            f"<th class='n' style='color:{class_color(c['class'])}'>{c['class']}</th>" for c in classes
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
        ui.label("class maps · central slices XY | XZ | YZ").classes(LABEL_CLS)
        with ui.element("div").style("display: flex; gap: 12px; flex-wrap: wrap;"):
            for m in maps:
                with ui.element("div").style("display: flex; flex-direction: column; gap: 2px;"):
                    with ui.element("div").style("display: flex; align-items: baseline; gap: 6px;"):
                        ui.label(f"class {m['class']}").style(
                            f"{SANS} font-size: 10px; font-weight: 600; color: {class_color(m['class'])};"
                        )
                        _open_commands(m["map"])
                    if m.get("png"):
                        ui.image(vis_asset_url(m["png"])).style("width: 240px; height: 80px;").props("fit=contain")
                    else:
                        ui.label("map not on disk").classes(HINT_CLS)

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
                ui.label(f"class {k}").style(f"{SANS} font-size: 10px; width: 52px; color: {class_color(int(k))};")
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
        with ui.element("div").style("display: flex; gap: 2px;"):
            ui.label(end).style(f"{SANS} font-size: 9px; color: #94a3b8; width: 30px;")
            for i in range(len(values)):
                pos = (meta.get("index") or {}).get(str(i))
                if not pos:
                    continue
                r, c = pos
                scale = shown / tile
                ui.element("div").style(
                    f"width: {shown}px; height: {shown}px; background-image: url({url}); "
                    f"background-size: {cols * tile * scale}px {rows * tile * scale}px; "
                    f"background-position: {-c * tile * scale}px {-r * tile * scale}px; border-radius: 2px;"
                ).tooltip(f"{end} #{i + 1}: {values[i]}")

    def _subset_controls(self) -> None:
        d = self.record.data
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-top: 4px;"
        ):
            ui.label("classes").classes(LABEL_CLS)
            for c in d["classes"]:
                ui.checkbox(str(c["class"]), on_change=lambda e, k=c["class"]: self._toggle(k, e.value)).props(
                    "dense size=xs"
                ).style(f"font-size: 10px; color: {class_color(c['class'])};")
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
                with ui.element("div").style("display: flex; align-items: baseline; gap: 6px;"):
                    ui.label("merged.mrc · XY | XZ | YZ").classes(LABEL_CLS)
                    _open_commands(d["merged"])
                ui.image(vis_asset_url(d["slices_png"])).style("width: 360px; height: 120px;").props("fit=contain")
            fsc = d.get("fsc")
            with ui.element("div").style("flex: 1 1 280px; min-width: 0;"):
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
                ui.echart(build_fsc_chart({k: v for k, v in fsc.items()})).style(f"height: {CHART_PX}px; width: 100%;")
