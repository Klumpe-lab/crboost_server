"""`/dev/particle-card-ideas`: the branch map's cards for particle jobs, drawn again on real jobs of the STA home
project, read and never written. Round 2.

The type rule, everywhere on the page: words (names, captions, project names, sentences) in the sans face at 10 px
or more (captions 9.5), never mono; numbers, ids (job013, s003), counts and values with their units (Å, %) in the
mono face with even figures, right-aligned in columns; labels (a field's, a column's, a block's name) in the label
voice, which the theme sets (Harbor Mono: mono capitals, 8.5 px), the one place mono carries words.

The look: flat cards (no shadows), hover and selection as hairline rings, a particle job marked only by a muted
accent on its left edge, its ports and its number. The switches at the top: `?accent=ochre|verdigris|graphite`,
`?classes=muted|today` (a proposed muted class palette against today's), `?strip=S1..S6` (the set strip the cards
carry).

1  Class 3D results. A Mosaic: one tile per class, the class's map as projections at its own sampling, hover for
   the central slice, click to select, an iteration scrubber. B Table: one row per class. C Small cards.
2  The particle-job card: the tilt-series card's skeleton with the slight accent, and a set strip above it in place
   of the tilt-series magazine. Large cards carry pictures, numbers and bars; the graphs live in the job page's
   Results tab.
3  Graphs, as the Results tab draws them (ui/components/plot.py), against today's bare lines.
4  The set strip: six ways to say which set, how many particles, from where.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from nicegui import ui

from services.workers import WorkKind, run_in_worker
from ui.branch_map.model import Plot as OldPlot
from ui.branch_map.plots import svg_plot as old_svg_plot
from ui.branch_map.style import BODY, HAIR, INK, MUTED, RED, SOFT, hrow
from ui.components.buttons import house_button
from ui.components.plot import Axis, Plot, Series, fsc_plot, plot, shares_plot
from ui.job_plugins._field_styles import voice
from ui.particle_card_ideas import data
from ui.particle_card_ideas.parts import (
    ACCENTS,
    CSS,
    GRID,
    PART,
    SMALL_W,
    WARN,
    WHITE,
    Look,
    card,
    head,
    img,
    input_row,
    lab,
    magazine,
    num,
    out_port,
    project_bar,
    project_shade,
    section,
    share_bar,
    words,
)
from ui.particle_card_ideas.strips import STRIPS

logger = logging.getLogger(__name__)

LARGE_W = 720  # the proposed large card; the map draws 600 today
TODAY_LARGE_W = 600
PIC = 256  # the pictures' own size; shown smaller, sharp on a dense screen
PAGE_BG = "var(--cb-desk, #e9eef4)"
SANS_HEAD = voice("title", font="'IBM Plex Sans', sans-serif", size="16px", weight=600, ink=INK)
STRIP_BY_KEY = {key: fn for key, _t, _w, fn in STRIPS}

# Pictures already made in this process, by (map, mtime, size).
_PICS: dict[tuple[str, float, int], dict[str, Any]] = {}


async def _pictures(paths: list[str], px: int = PIC) -> dict[str, dict[str, Any]]:
    def key(p: str) -> tuple[str, float, int]:
        return (p, Path(p).stat().st_mtime if Path(p).exists() else 0.0, px)

    todo = [p for p in paths if key(p) not in _PICS]
    if todo:
        got = await run_in_worker(WorkKind.RENDER, data.pictures, todo, px)
        for p, v in got.items():
            _PICS[key(p)] = v
    return {p: _PICS[key(p)] for p in paths if key(p) in _PICS}


def _res_words(res: float | None) -> str:
    return f"{res:.1f} Å" if res else "no estimate"


def _heading(n: str, title: str, about: str) -> None:
    with hrow(10, "margin: 24px 0 8px; align-items: baseline;"):
        num(n, 15, PART, bold=True)
        words(title, 14, INK, bold=True)
        words(about, 10.5, SOFT, extra="max-width: 980px;")


def _variant(key: str, title: str, about: str) -> ui.element:
    box = ui.element("div").style("display: flex; flex-direction: column; gap: 6px;")
    with box:
        with hrow(6, "align-items: baseline;"):
            num(key, 11, PART, bold=True)
            words(title, 11, INK, bold=True)
        words(about, 10, SOFT, extra="max-width: 720px; line-height: 1.45;")
    return box


class Page:
    """What one visit asked for: the look, the strip the cards carry, the projects in order."""

    def __init__(self, look: Look, strip: str, order: list[str]) -> None:
        self.look = look
        self.strip_key = strip if strip in STRIP_BY_KEY else "S4"
        self.order = order

    def strip(self, s: dict | None, w: int) -> None:
        with ui.element("div").style("margin-bottom: 6px;"):
            STRIP_BY_KEY[self.strip_key](s, self.order, w)


# ── 1 · Class 3D ─────────────────────────────────────────────────────────────


class ClassView:
    """One Class 3D's results, shared by the mosaic, the table and the small card: which iteration is shown,
    projections or slices, which classes are selected."""

    def __init__(self, c3: dict[str, Any], pg: Page) -> None:
        self.c3 = c3
        self.pg = pg
        self.color = pg.look.class_color
        self.it = c3["final"]
        self.views = "proj"
        self.selected: set[int] = set()
        self.pics: dict[str, dict[str, Any]] = {}
        self._tiles: dict[int, ui.element] = {}
        self._rows: dict[int, ui.element] = {}
        self._sel_words: list[ui.label] = []
        self._make: list[ui.element] = []
        self.mosaic_box: ui.element | None = None

    def it_index(self) -> int:
        return self.c3["iterations"].index(self.it)

    def classes(self) -> list[dict[str, Any]]:
        """The classes at the shown iteration, largest first."""
        i = self.it_index()
        rows = [{**c, "share_now": c["history"][i]} for c in self.c3["classes"]]
        return sorted(rows, key=lambda c: -c["share_now"])

    def map_of(self, k: int) -> str:
        return str(Path(self.c3["classes"][0]["map"]).parent / f"run_it{self.it:03d}_class{k:03d}.mrc")

    # ── selection ──
    def toggle(self, k: int) -> None:
        self.selected ^= {k}
        for kk, el in [*self._tiles.items(), *self._rows.items()]:
            el.classes(add="pci-sel") if kk in self.selected else el.classes(remove="pci-sel")
        n = sum(c["n"] for c in self.c3["classes"] if c["class"] in self.selected)
        said = (
            f"{len(self.selected)} class{'es' if len(self.selected) != 1 else ''} · {n:,} particles"
            if self.selected
            else "click classes to select them"
        )
        for lbl in self._sel_words:
            lbl.set_text(said)
        for b in self._make:
            b.set_visibility(bool(self.selected))

    def selection_bar(self) -> None:
        with hrow(8, "height: 22px;"):
            self._sel_words.append(words("click classes to select them", 10, BODY))
            ui.element("div").style("flex: 1 1 0;")
            b = house_button(
                "Make set from the selection",
                lambda: ui.notify("Prototype: nothing is made here", type="info"),
                kind="accent",
            )
            b.set_visibility(False)
            self._make.append(b)

    # ── the mosaic ──
    async def set_iteration(self, it: int) -> None:
        if it == self.it:
            return
        self.it = it
        self.pics = await _pictures([self.map_of(c["class"]) for c in self.c3["classes"]])
        self.draw_mosaic()

    def set_views(self, v: str) -> None:
        self.views = v
        self.draw_mosaic()

    def draw_mosaic(self) -> None:
        box = self.mosaic_box
        if box is None or box.is_deleted:
            return
        box.clear()
        self._tiles = {}
        inner = LARGE_W - 28
        cols = 3
        tw = (inner - (cols - 1) * 10) // cols
        main_px = tw - 20
        alt = "slice" if self.views == "proj" else "proj"
        caption = {"proj": "projection along z", "slice": "central slice, z"}
        on_pic = "var(--cb-plot-read-ink, #f2f6fb)"
        with box, ui.element("div").style(f"display: grid; grid-template-columns: repeat({cols}, {tw}px); gap: 10px;"):
            for c in self.classes():
                k = c["class"]
                color = self.color(k)
                v = self.pics.get(self.map_of(k))
                tile = (
                    ui.element("div")
                    .classes("pci-tile" + (" pci-sel" if k in self.selected else ""))
                    .style(
                        f"--pci-k: {color}; width: {tw}px; box-sizing: border-box; padding: 8px 10px 9px; "
                        f"background: {WHITE}; border: 1px solid {HAIR}; border-top: 2px solid {color}; "
                        "border-radius: var(--cb-r-sm, 2px);"
                    )
                    .on("click", lambda _e, kk=k: self.toggle(kk))
                )
                self._tiles[k] = tile
                with tile:
                    with hrow(6, "align-items: baseline; margin-bottom: 5px;"):
                        words("class", 10.5, SOFT)
                        num(str(k), 11, color, bold=True)
                        ui.element("div").style("flex: 1 1 0;")
                        num(f"{c['share_now']:.1%}", 11, INK, bold=True)
                        if self.it == self.c3["final"]:
                            num(f"{c['n']:,}", 10, SOFT, extra="width: 42px; text-align: right;")
                    with ui.element("div").style(f"position: relative; width: {main_px}px; height: {main_px}px;"):
                        img(v[self.views][0] if v else None, main_px)
                        img(
                            v[alt][0] if v else None,
                            main_px,
                            cls="pci-alt",
                            extra="position: absolute; left: 0; top: 0;",
                        )
                        for which, cls in ((self.views, "pci-main-word"), (alt, "pci-alt-word")):
                            words(
                                caption[which],
                                9.5,
                                on_pic,
                                extra="position: absolute; left: 5px; bottom: 3px; opacity: .85;",
                            ).classes(cls)
                    small = (main_px - 8) // 3
                    with hrow(4, "margin-top: 4px;"):
                        for i, axis in enumerate(("z", "y", "x")):
                            with ui.element("div").style("position: relative;"):
                                img(v[self.views][i] if v else None, small)
                                num(axis, 9, on_pic, extra="position: absolute; right: 3px; bottom: 1px; opacity: .8;")
                    ui.element("div").style("height: 6px;")
                    share_bar(c["share_now"], color, main_px)
                    with ui.element("div").style(
                        "display: grid; grid-template-columns: auto 1fr; gap: 1px 8px; margin-top: 6px; "
                        "align-items: baseline;"
                    ):
                        lab("Class 3D est.")
                        num(_res_words(c["res"]), 10.5, INK if c["res"] else WARN, extra="text-align: right;")
                        if c["acc_rot"] is not None:
                            lab("Rot. acc.")
                            num(f"{c['acc_rot']:.2f}°", 10.5, BODY, extra="text-align: right;")
                        if c["top_tomo"]:
                            lab("Tomograms")
                            num(f"{c['n_tomos']}", 10.5, BODY, extra="text-align: right;")
                    if c["res"] is None:
                        words("RELION gave no estimate (inf): read the map, not the number", 9.5, WARN)
                    if c["top_tomo"]:
                        t, n = c["top_tomo"]
                        pos = t.split("_rerun_", 1)[-1]
                        words(
                            f"{n / max(1, c['n']):.0%} in {data.short_project(data.project_of(t))} {pos}",
                            9.5,
                            SOFT,
                            extra="margin-top: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;",
                        )
                    ui.element("div").style("height: 4px;")
                    project_bar(c["projects"], self.pg.order, main_px, 4)

    def mosaic_card(self) -> None:
        """Variant A at the proposed large width."""
        c3 = self.c3
        self.pg.strip(c3["set"], LARGE_W)
        with card(LARGE_W, particle=True):
            head("Class 3D", c3["job"], "done" if c3["done"] else "running", "RELION", particle=True)
            input_row("particles", f"set {c3['set']['short']}" if c3["set"] else "—", particle_port=True, y=26 + 8.5)
            input_row("reference", "featureless shell (rotational average)", particle_port=False, y=26 + 17 + 8.5)
            out_port(13, LARGE_W, particle=True)
            sec = section(f"Classes · iteration {self.it} of {c3['final']} · {c3['n_particles']:,} particles", LARGE_W)
            with sec:
                with hrow(10, "margin-bottom: 8px;"):
                    for key, said in (("proj", "projections"), ("slice", "central slices")):
                        house_button(
                            said, lambda _e, k=key: self.set_views(k), kind="accent" if self.views == key else "default"
                        )
                    lab("iteration", "margin-left: 10px;")
                    its = c3["iterations"]
                    slider = (
                        ui.slider(min=its[0], max=its[-1], step=1, value=self.it)
                        .props("dense label")
                        .style("width: 230px;")
                    )
                    slider.on("change", lambda e: self.set_iteration(int(e.args)))
                self.mosaic_box = ui.element("div")
                self.draw_mosaic()
                ui.element("div").style("height: 8px;")
                self.selection_bar()

    def table_card(self) -> None:
        """Variant B: one row per class, its views small, its numbers in right-aligned columns."""
        c3 = self.c3
        self.pg.strip(c3["set"], LARGE_W)
        with card(LARGE_W, particle=True):
            head("Class 3D", c3["job"], "done" if c3["done"] else "running", "RELION", particle=True)
            input_row("particles", f"set {c3['set']['short']}" if c3["set"] else "—", particle_port=True, y=26 + 8.5)
            out_port(13, LARGE_W, particle=True)
            sec = section(f"Classes · iteration {c3['final']} · largest first · click to select", LARGE_W)
            px = 62
            cols = f"22px {3 * px + 8}px 64px 64px 70px 52px 1fr"
            with sec:
                with ui.element("div").style(
                    f"display: grid; grid-template-columns: {cols}; gap: 0 10px; padding: 0 4px 4px; "
                    f"border-bottom: 1px solid {HAIR};"
                ):
                    for h, right in (
                        ("", False),
                        ("z · y · x", False),
                        ("share", True),
                        ("particles", True),
                        ("Class 3D est.", True),
                        ("rot. acc.", True),
                        ("from", False),
                    ):
                        lab(h, "text-align: right;" if right else "")
                for c in sorted(c3["classes"], key=lambda c: -c["share"]):
                    k = c["class"]
                    color = self.color(k)
                    v = self.pics.get(c["map"])
                    row = (
                        ui.element("div")
                        .classes("pci-row")
                        .style(
                            f"--pci-k: {color}; display: grid; grid-template-columns: {cols}; gap: 0 10px; "
                            f"align-items: center; padding: 5px 4px; border-bottom: 1px solid {HAIR};"
                        )
                        .on("click", lambda _e, kk=k: self.toggle(kk))
                    )
                    self._rows[k] = row
                    with row:
                        num(str(k), 11, color, bold=True)
                        with hrow(4):
                            for i in range(3):
                                img(v["proj"][i] if v else None, px)
                        with ui.element("div").style("display: flex; flex-direction: column; align-items: flex-end;"):
                            num(f"{c['share']:.1%}", 11, INK, bold=True)
                            share_bar(c["share"], color, 56)
                        num(f"{c['n']:,}", 10.5, BODY, extra="text-align: right;")
                        num(_res_words(c["res"]), 10.5, INK if c["res"] else WARN, extra="text-align: right;")
                        num(
                            f"{c['acc_rot']:.2f}°" if c["acc_rot"] is not None else "—",
                            10.5,
                            BODY,
                            extra="text-align: right;",
                        )
                        with ui.element("div").style("min-width: 0;"):
                            project_bar(c["projects"], self.pg.order, 150, 5, caption=True)
                ui.element("div").style("height: 8px;")
                self.selection_bar()


def small_class_card(c3: dict[str, Any], pics: dict[str, dict[str, Any]], pg: Page) -> None:
    """Variant C: what a small card says of a classification: one projection per class, largest first, its share as
    the bar under it; the iteration it is at."""
    pg.strip(c3["set"], SMALL_W)
    with card(SMALL_W, particle=True):
        head("Class 3D", c3["job"], "done" if c3["done"] else "running", "", particle=True)
        input_row("particles", f"set {c3['set']['short']}" if c3["set"] else "—", particle_port=True, y=26 + 8.5)
        out_port(13, SMALL_W, particle=True)
        inner = SMALL_W - 20
        classes = sorted(c3["classes"], key=lambda c: -c["share"])
        with ui.element("div").style("padding: 6px 10px 8px;"):
            k = len(classes)
            tw = min(46, (inner - 3 * (k - 1)) // max(1, k))
            top = max(c["share"] for c in classes) or 1
            with ui.element("div").style(f"display: flex; gap: 3px; width: {inner}px;"):
                for c in classes:
                    v = pics.get(c["map"])
                    tile = ui.element("div").classes("pci-mini").style(f"width: {tw}px;")
                    with tile:
                        img(v["proj"][0] if v else None, tw)
                        with ui.element("div").style(
                            f"height: 3px; width: {tw}px; background: {GRID}; margin-top: 2px;"
                        ):
                            ui.element("div").style(
                                f"height: 100%; width: {c['share'] / top * 100:.0f}%; "
                                f"background: {pg.look.class_color(c['class'])};"
                            )
                    tile.props(
                        f'title="class {c["class"]}: {c["share"]:.1%}, {c["n"]:,} particles, '
                        f'{_res_words(c["res"])} (Class 3D estimate)"'
                    )
            with hrow(6, "margin-top: 4px; align-items: baseline;"):
                lab("K")
                num(str(len(classes)), 10, BODY, bold=True)
                num(" ".join(f"{c['share']:.0%}" for c in classes), 10, SOFT, extra="overflow: hidden;")
                ui.element("div").style("flex: 1 1 0;")
                lab("it")
                num(f"{c3['final']}" + ("" if c3["done"] else " …"), 10, MUTED)


# ── 2 · the other particle jobs ──────────────────────────────────────────────


def refine_card(r: dict[str, Any], m: dict[str, Any], pics: dict[str, dict[str, Any]], pg: Page) -> None:
    """Refine 3D, large: the map's six views, the resolution as a number with what it means, the set's projects.
    The FSC is in the job page's Results tab."""
    pg.strip(r["set"], LARGE_W)
    with card(LARGE_W, particle=True):
        head("Refine 3D", r["job"], "done", "RELION · I1", particle=True)
        input_row("particles", f"set {r['set']['short']}" if r["set"] else "—", particle_port=True, y=26 + 8.5)
        input_row("mask", m["job"], particle_port=False, y=26 + 17 + 8.5)
        out_port(13, LARGE_W, particle=True)
        v = pics.get(r.get("map", ""))
        sec = section("The map · run_class001.mrc · projections, then central slices", LARGE_W)
        with sec, hrow(6):
            for kind in ("proj", "slice"):
                for i in range(3):
                    img(v[kind][i] if v else None, 108)
        with sec, hrow(16, "margin-top: 6px; align-items: baseline;"):
            if v:
                lab("box")
                num(f"{v['box']} px", 10.5, BODY)
                lab("pixel")
                num(f"{v['apix']:g} Å", 10.5, BODY)
                lab("extent")
                num(f"{v['box'] * v['apix']:.0f} Å", 10.5, BODY)
        sec2 = section("Resolution · gold-standard FSC, unmasked half maps", LARGE_W)
        with sec2, hrow(14, "align-items: baseline;"):
            num(_res_words(r.get("res")), 18, INK, bold=True)
            words(
                "the curve stays above 0.143 to the last shell, so this is the sampling's limit, not the map's: "
                "refine at a finer pixel"
                if not r.get("cross_143")
                else "at FSC 0.143",
                10,
                WARN if not r.get("cross_143") else SOFT,
                extra="max-width: 520px; line-height: 1.4;",
            )
        sec3 = section("Particles · where the set came from", LARGE_W)
        with sec3:
            project_bar(r.get("projects", {}), pg.order, LARGE_W - 28, 8, caption=True)


def export_card(e: dict[str, Any], pg: Page) -> None:
    """Warp export, large: particles per project as labelled bars with their numbers in columns."""
    pg.strip(e["set"], LARGE_W)
    with card(LARGE_W, particle=True):
        head("Warp export", e["job"], "done", "WarpTools", particle=True)
        input_row("particles", f"set {e['set']['short']}" if e["set"] else "—", particle_port=True, y=26 + 8.5)
        out_port(13, LARGE_W, particle=True)
        sec = section(f"Exported · {e['n_particles']:,} particles from {e['n_tomos']} tomograms", LARGE_W)
        total = e["n_particles"] or 1
        top = max(e["projects"].values()) if e["projects"] else 1
        with sec:
            for name, n in sorted(e["projects"].items(), key=lambda kv: -kv[1]):
                i = pg.order.index(name) if name in pg.order else 0
                with hrow(8, "height: 16px;"):
                    ui.element("div").style(f"width: 8px; height: 8px; background: {project_shade(i)};")
                    words(name, 10, BODY, extra="width: 150px; white-space: nowrap; overflow: hidden;")
                    with ui.element("div").style(f"flex: 1 1 0; height: 5px; background: {GRID};"):
                        ui.element("div").style(
                            f"height: 100%; width: {n / top * 100:.1f}%; background: {project_shade(i)};"
                        )
                    num(f"{n:,}", 10.5, INK, extra="width: 48px; text-align: right;")
                    num(f"{n / total:.0%}", 10, SOFT, extra="width: 36px; text-align: right;")
        counts = e["per_tomo"]
        if counts:
            sec2 = section("Per tomogram", LARGE_W)
            with sec2, hrow(18, "align-items: baseline;"):
                for name, value in (
                    ("median", sorted(counts)[len(counts) // 2]),
                    ("most", max(counts)),
                    ("fewest", min(counts)),
                    ("tomograms", len(counts)),
                ):
                    lab(name)
                    num(f"{value:,}", 10.5, INK)


def small_job_card(title: str, job: str, status: str, method: str, s: dict | None, pg: Page, body) -> None:
    pg.strip(s, SMALL_W)
    with card(SMALL_W, particle=True):
        head(title, job, status, method, particle=True)
        input_row("particles", f"set {s['short']}" if s else "—", particle_port=True, y=26 + 8.5)
        out_port(13, SMALL_W, particle=True)
        with ui.element("div").style("padding: 6px 10px 8px;"):
            body()


def tilt_series_card_today() -> None:
    """A tilt-series job's small card as the map draws it, for comparison: the magazine above, square ports."""
    magazine(120, SMALL_W, done=114)
    with card(SMALL_W, particle=False):
        head("Reconstruct", "job012", "done", "WarpTools", particle=False)
        input_row("tilt series", "Align job010", particle_port=False, y=26 + 8.5)
        out_port(13, SMALL_W, particle=False)
        with hrow(4, "padding: 6px 10px 8px; align-items: baseline;"):
            num("114", 10, INK)
            words("of", 10, SOFT)
            num("120", 10, INK)
            words("tilt series", 10, SOFT)
            lab("bin", "margin-left: 6px;")
            num("4", 10, INK)


# ── 3 · graphs ───────────────────────────────────────────────────────────────


def graphs(r: dict[str, Any], c3: dict[str, Any], pg: Page) -> None:
    ny = 1 / (2 * r["apix"]) if r.get("apix") else max(r["freq"])
    pts = [(f, v) for f, v in zip(r["freq"], r["fsc"], strict=True) if 0 < f <= ny * 1.0001]
    curves = {"FSC": {"freq": [p[0] for p in pts], "fsc": [p[1] for p in pts]}}
    with ui.element("div").style("display: grid; grid-template-columns: 480px 480px; gap: 18px 30px;"):
        box = _variant("Today", "FSC as the card draws it", "values only: no axes, no units, no threshold, no readout")
        with box:
            ui.html(
                old_svg_plot(OldPlot("curve", r["fsc"], "gold-standard FSC by shell"), 452, 56), sanitize=False
            ).style("line-height: 0;")
        box = _variant(
            "Results tab",
            "FSC",
            "x in 1/Å named in Å at the resolutions people quote, 0.143 and 0.5 named, the crossing or the Nyquist "
            "limit marked; hover a shell for its resolution and FSC",
        )
        with box:
            plot(fsc_plot(curves, 452, 170), 452, 170)
        box = _variant("Today", "class shares as the card draws it", "bars of the last iteration only")
        with box:
            ui.html(
                old_svg_plot(OldPlot("bars", [c["share"] * 100 for c in c3["classes"]]), 452, 56), sanitize=False
            ).style("line-height: 0;")
        box = _variant(
            "Results tab",
            "class shares by iteration",
            "every class in its colour, stacked to 100 %; hover an iteration for every class's share",
        )
        with box:
            per = {c["class"]: c["history"] for c in c3["classes"]}
            plot(shares_plot(c3["iterations"], per, pg.look.class_color), 452, 170)
        box = _variant(
            "Results tab",
            "class shares by iteration, as lines",
            "the same numbers unstacked: a class that keeps losing particles reads as a falling line",
        )
        with box:
            its = [float(i) for i in c3["iterations"]]
            series = [
                Series(its, [h * 100 for h in c["history"]], pg.look.class_color(c["class"]), f"class {c['class']}")
                for c in c3["classes"]
            ]
            hi = max(max(s.ys) for s in series) * 1.1
            step = 5 if len(its) > 12 else 1
            plot(
                Plot(
                    series,
                    Axis(its[0], its[-1], [(x, f"{x:g}") for x in its if int(x) % step == 0], "iteration"),
                    Axis(
                        0,
                        hi,
                        [(v, f"{v:g}") for v in (0, 10, 20, 30, 40, 50) if v <= hi],
                        "% of particles",
                        fmt=lambda v: f"{v:.1f} %",
                    ),
                ),
                452,
                170,
            )


# ── 4 · the set strip ────────────────────────────────────────────────────────


def strips(d: dict[str, Any], pg: Page) -> None:
    """Every strip on the same three jobs at the small card's width, and on Refine 3D at the large card's."""
    jobs = [("Class 3D", d["main"]), ("Refine 3D", d["refine"]), ("Warp export", d["export"])]
    for key, title, about, fn in STRIPS:
        box = _variant(key, title, about)
        with box, ui.element("div").style("display: flex; gap: 28px; align-items: flex-start; flex-wrap: wrap;"):
            for name, j in jobs:
                with ui.element("div"):
                    with ui.element("div").style("margin-bottom: 6px;"):
                        fn(j.get("set"), pg.order, SMALL_W)
                    with card(SMALL_W, particle=True):
                        head(name, j["job"], "done", "", particle=True)
                        out_port(13, SMALL_W, particle=True)
            with ui.element("div"):
                with ui.element("div").style("margin-bottom: 6px;"):
                    fn(d["refine"].get("set"), pg.order, LARGE_W - 140)
                with card(LARGE_W - 140, particle=True):
                    head("Refine 3D", d["refine"]["job"], "done", "RELION · I1", particle=True)
                    out_port(13, LARGE_W - 140, particle=True)
        ui.element("div").style("height: 14px;")


# ── the page ─────────────────────────────────────────────────────────────────


def _switches(look: Look, strip: str) -> None:
    """Links that redraw the page in another accent, class palette or strip."""

    def link(key: str, value: str, on: bool) -> None:
        q = {"accent": look.accent, "classes": look.classes, "strip": strip, key: value}
        ui.link(value, f"?{urlencode(q)}").style(
            voice("ctl", font="'IBM Plex Sans', sans-serif", size="10px", weight=500, ink=INK if on else SOFT)
            + f" padding: 1px 6px; text-decoration: none; border: 1px solid {HAIR if not on else PART}; "
            "border-radius: var(--cb-r-sm, 2px);"
        )

    with hrow(14, "margin: 8px 0 2px; flex-wrap: wrap;"):
        for key, values, current in (
            ("accent", list(ACCENTS), look.accent),
            ("classes", ["muted", "today"], look.classes),
            ("strip", [k for k, *_ in STRIPS], strip),
        ):
            with hrow(4):
                lab(key, "margin-right: 2px;")
                for v in values:
                    link(key, v, v == current)


def render_particle_card_ideas() -> None:
    params = ui.context.client.request.query_params
    look = Look(
        accent=params.get("accent", "ochre") if params.get("accent", "ochre") in ACCENTS else "ochre",
        classes="today" if params.get("classes") == "today" else "muted",
    )
    strip = params.get("strip", "S4")
    ui.add_css(CSS)
    # Its own scroll box: the app's shell holds the page body at the window's height.
    page = ui.column().style(
        f"{look.root_style()} padding: 18px 26px 80px; gap: 4px; background: {PAGE_BG}; height: 100vh; "
        "overflow-y: auto; flex-wrap: nowrap; box-sizing: border-box; width: 100%;"
    )
    with page:
        ui.label("Particle cards and class mosaics · round 2").style(SANS_HEAD)
        words(
            f"Real jobs of {data.HOME.name}, read and never written: Class 3D job013 (the 412, K 6), job014 (its "
            "capsid class again, K 3), job005 (one project's ribosomes, still iterating), Refine 3D job015, "
            f"mask job016, PostProcess job017, Warp export job003. Large cards at {LARGE_W} px (the map draws "
            f"{TODAY_LARGE_W} today), small at {SMALL_W}. Words in the sans face, numbers and ids in the mono "
            "face, labels in the label voice.",
            10.5,
            SOFT,
            extra="max-width: 1100px; line-height: 1.45;",
        )
        _switches(look, strip)
        status = words("reading the jobs…", 10, MUTED, extra="margin-top: 10px;")
        body = ui.element("div")

    async def load() -> None:
        try:
            d = await run_in_worker(WorkKind.LINEAGE, data.everything, str(data.HOME))
        except Exception as e:
            logger.exception("particle card ideas: reading %s", data.HOME)
            status.set_text(f"could not read the jobs: {e}")
            status.style(f"color: {RED};")
            return
        if body.is_deleted:
            return
        status.set_text("rendering the maps…")
        maps = [c["map"] for k in ("main", "small", "running") for c in d[k]["classes"]]
        maps += [d["refine"].get("map", ""), d["mask"]["map"]]
        pics = await _pictures([m for m in maps if m])
        if body.is_deleted:
            return  # the page was closed while its maps were drawn
        status.set_visibility(False)
        order = list(d["main"]["set"]["projects"]) if d["main"]["set"] else []
        for k in ("main", "small", "running", "refine", "export"):
            for name in (d[k].get("set") or {}).get("projects") or {}:
                if name not in order:
                    order.append(name)
        with body:
            _draw(d, pics, Page(look, strip, order))

    ui.timer(0.05, load, once=True)


def _draw(d: dict[str, Any], pics: dict[str, dict[str, Any]], pg: Page) -> None:
    main = ClassView(d["main"], pg)
    main.pics = pics
    _heading(
        "1",
        "Class 3D results",
        "A and B show the same classification; selecting in one selects in the other. Classes are sorted by share. "
        "No graphs on the card: the shares by iteration are in the Results tab (3).",
    )
    with ui.element("div").style(f"display: grid; grid-template-columns: {LARGE_W}px {LARGE_W}px; gap: 30px;"):
        box = _variant(
            "A",
            "Mosaic",
            "One tile per class: its map projected along z at the map's own sampling, the three axes under it, hover "
            "for the central slice; share, count, Class 3D's own estimate (not a gold-standard FSC), the rotational "
            "accuracy, its tomograms and projects. The scrubber brings that iteration's maps.",
        )
        with box:
            main.mosaic_card()
        box = _variant(
            "B",
            "Table",
            "One row per class, its three projections small and its numbers in right-aligned columns, the projects it "
            "came from: more classes on a screen, easier to compare across.",
        )
        with box:
            main.table_card()
    box = _variant(
        "C",
        "Small cards",
        "One tile per class, largest first, its share as the bar under it; hover a class for its numbers. A running "
        "job shows the iteration it is at.",
    )
    with box, ui.element("div").style("display: flex; gap: 40px; align-items: flex-start; margin-top: 6px;"):
        for k in ("main", "small", "running"):
            with ui.element("div"):
                small_class_card(d[k], pics, pg)

    _heading(
        "2",
        "The particle-job card",
        "The tilt-series card's skeleton (header height, input rows, ports), flat. What tells a particle job apart is "
        "slight: the accent down its left edge, on its ports where a particle set goes in or out (discs; squares stay "
        "for tilt series and maps) and on its number; and the set strip above it in place of the tilt-series "
        "magazine.",
    )
    r, m, e, post = d["refine"], d["mask"], d["export"], d["post"]
    with ui.element("div").style("display: flex; gap: 34px; align-items: flex-start; flex-wrap: wrap;"):
        with ui.element("div"):
            lab("a tilt-series job, as today", "margin-bottom: 4px;")
            tilt_series_card_today()
        with ui.element("div"):
            lab("a particle job", "margin-bottom: 4px;")

            def export_body() -> None:
                project_bar(e["projects"], pg.order, SMALL_W - 20, 5, caption=True)
                with hrow(4, "margin-top: 3px; align-items: baseline;"):
                    num(f"{e['n_particles']:,}", 10, INK)
                    words("particles", 10, SOFT)
                    num(f"{e['n_tomos']}", 10, INK, extra="margin-left: 4px;")
                    words("tomograms", 10, SOFT)

            small_job_card("Warp export", e["job"], "done", "", e["set"], pg, export_body)
        with ui.element("div"):
            lab("refine", "margin-bottom: 4px;")

            def refine_body() -> None:
                v = pics.get(r.get("map", ""))
                with hrow(3):
                    for i in range(3):
                        img(v["proj"][i] if v else None, 70)
                with hrow(6, "margin-top: 4px; align-items: baseline;"):
                    num(_res_words(r.get("res")), 12, INK, bold=True)
                    words(
                        "Nyquist-limited" if not r.get("cross_143") else "FSC 0.143",
                        10,
                        WARN if not r.get("cross_143") else SOFT,
                    )

            small_job_card("Refine 3D", r["job"], "done", "", r["set"], pg, refine_body)
        with ui.element("div"):
            lab("mask · reads a map", "margin-bottom: 4px;")
            ui.element("div").style("height: 22px;")
            with card(SMALL_W, particle=True):
                head("Mask", m["job"], "done", "", particle=True)
                input_row("map", "class 2, I1-aligned", particle_port=False, y=26 + 8.5)
                out_port(13, SMALL_W, particle=False)
                with ui.element("div").style("padding: 6px 10px 8px;"):
                    v = pics.get(m["map"])
                    with hrow(3):
                        for i in range(3):
                            img(v["slice"][i] if v else None, 70)
                    st = m["stats"] or {}
                    if st:
                        with hrow(4, "margin-top: 3px; align-items: baseline; flex-wrap: wrap;"):
                            num(f"{st['enclosed_fraction']:.0%}", 10, INK)
                            words("of the box", 10, SOFT)
                            num(f"{st['lowpass_ang']:g} Å", 10, INK, extra="margin-left: 4px;")
                            num(f"+{st['extend_px']} px", 10, INK, extra="margin-left: 4px;")
        with ui.element("div"):
            lab("failed", "margin-bottom: 4px;")
            ui.element("div").style("height: 22px;")
            with card(SMALL_W, particle=True):
                head("PostProcess", post["job"], "failed", "", particle=True)
                input_row("half maps", r["job"], particle_port=True, y=26 + 8.5)
                input_row("mask", m["job"], particle_port=False, y=26 + 17 + 8.5)
                out_port(13, SMALL_W, particle=True)
                with ui.element("div").style("padding: 6px 10px 8px;"):
                    words(post["last_error"] or "failed", 10, RED, extra="line-height: 1.35;")

    with ui.element("div").style(
        f"display: grid; grid-template-columns: {LARGE_W}px {LARGE_W}px; gap: 30px; margin-top: 18px;"
    ):
        box = _variant(
            "2",
            "Refine 3D, large",
            "the map's six views at a readable size, the resolution and what it means, the set's projects",
        )
        with box:
            refine_card(r, m, pics, pg)
        box = _variant("2", "Warp export, large", "particles per project as bars with their numbers in columns")
        with box:
            export_card(e, pg)

    _heading(
        "3",
        "Graphs · in the job page's Results tab",
        "Taken off the large cards: they live in the Results tab of Class 3D, Refine 3D and PostProcess, drawn by "
        "ui/components/plot.py: a y grid at labelled ticks, units on both axes, named reference lines, marks, and a "
        "readout of every series under the pointer.",
    )
    graphs(r, d["main"], pg)

    _heading(
        "4",
        "The set strip",
        "What sits above a particle job's card in place of the tilt-series magazine: which set, how many "
        "particles, from where. Each strip on three jobs at the small card's width, and on Refine 3D wider. The "
        "cards above carry the one picked in the switches.",
    )
    strips(d, pg)
    with hrow(8, f"margin-top: 26px; padding-top: 10px; border-top: 1px solid {HAIR};"):
        ui.element("div").style(f"width: 10px; height: 10px; background: {PART}; border-radius: 50%;")
        words(f"particle accent: {pg.look.accent}", 10, SOFT)
        lab("classes", "margin-left: 14px;")
        for k in range(1, 9):
            ui.element("div").style(f"width: 14px; height: 10px; background: {pg.look.class_color(k)};")
