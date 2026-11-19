"""`/dev/registry-ideas`: the Particles registry laid out again (roadmap 60 S0), on real projects read and never
written. Nothing here saves or submits; an action says what it would do.

A  The registry: the title line with the five tabs (D2), the rail counting picks, the Species tab (S3). The tabs
   work: Templates & masks and Tomograms open the drafts of B and C in the frame.
B  Templates & masks (D3): (a) two tables stacked, the viewer beside them; (c) one table, each mask under the
   template it was made from. Selection is the roster's ring; New template… and New mask… open the forms in a
   dialog.
C  Tomograms: one row per tomogram, sortable by any column head; a tomogram with several lists opens to them.
D  Colour (D1): the chips today and as proposed; a real reconstruction with every species' candidates, in the
   vivid overlay palette and in a muted one.
E  Drift on the roster (D4): the particle jobs' rows with a mark on the species tag.

Switches (the address's query): `?project=grid2|sta|mystery&species=<id>&pane=wide|narrow&tm=a|c`; `&reload=1`
reads the project again.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from urllib.parse import urlencode

from nicegui import ui

from services.models_base import SPECIES_OVERLAY_COLORS
from services.workers import WorkKind, run_in_worker
from ui.components.buttons import house_button, icon_button
from ui.components.check import house_check
from ui.components.chip import render_method_chip, render_polarity_chip
from ui.components.fields import house_field
from ui.components.path_text import path_text
from ui.components.segmented import render_segmented
from ui.components.svg_icon import load_icon_svg
from ui.job_plugins._field_styles import (
    FIELD_LABEL_STYLE,
    FIELD_ROW_STYLE,
    L,
    M,
    MONO,
    PANEL_TITLE_STYLE,
    S,
    SANS,
    SECTION_STYLE,
    VIEW_HEAD_STYLE,
    VIEW_TITLE_STYLE,
    field_grid,
    panel_head,
    section,
    section_head,
    text_field,
    view_head_icon,
)
from ui.registry_ideas import data, parts
from ui.registry_ideas.data import Registry, Species, Vol
from ui.registry_ideas.parts import AMBER, BODY, CAPTION, FAINT, INK, SOFT, WARN_INK, WORDS

logger = logging.getLogger(__name__)

PANE = {"wide": 1060, "narrow": 720}
RAIL_W = 200
TABS = (
    ("species", "Species"),
    ("templates", "Templates & masks"),
    ("tomograms", "Tomograms"),
    ("scores", "Scores"),
    ("kitchen", "STA kitchen"),
)
ROW_CAP = 150  # a table longer than this shows its first rows here (the STA home project has 897 tomograms)
SHOWN_WHEN_CAPPED = 60

# Read once per server process; `?reload=1` reads again.
_REG: dict[str, Registry] = {}
_OVERLAY: dict[str, data.Overlay | None] = {}

_TABLE_JS = (
    "(e) => { const t = e.target.closest('[data-act]'); if (!t) return; "
    "if (t.dataset.act === 'toggle') { t.closest('tbody').classList.toggle('open'); return; } "
    "emit({act: t.dataset.act, key: t.dataset.key || '', tomo: t.dataset.tomo || '', slug: t.dataset.slug || ''}); }"
)


def _noop() -> None:
    """Nothing here saves."""


def _would(what: str) -> None:
    ui.notify(f"Prototype: {what}", position="bottom-right", timeout=2500)


# ── page furniture ───────────────────────────────────────────────────────────


def _heading(letter: str, title: str, words: str) -> None:
    ui.label(f"{letter} · {title}").style(f"{SANS} font-size: 13px; font-weight: 600; color: {INK}; margin-top: 30px;")
    ui.label(words).style(f"{CAPTION} font-size: 10.5px; max-width: 980px;")


def _panel(title: str, words: str, width: int) -> ui.element:
    """One draft: its name, a line on it, then a white frame as wide as where it would live. Classed for
    scripts/dev_screenshot.py --panels."""
    with (
        ui.element("div")
        .classes("cb-mock-panel")
        .style("display: flex; flex-direction: column; gap: 6px; margin-top: 16px;")
    ):
        ui.label(title).classes("cb-mock-panel-title").style(
            f"{MONO} font-size: 9.5px; text-transform: uppercase; letter-spacing: .08em; color: {INK};"
        )
        if words:
            ui.label(words).style(f"{CAPTION} max-width: {max(width, 700)}px;")
        frame = ui.element("div").style(
            f"width: {width}px; background: #fff; border: 1px solid #d6e0ea; padding: 12px 14px 16px; "
            "display: flex; flex-direction: column; gap: 14px; box-sizing: border-box;"
        )
    return frame


def _switch(name: str, options: list[tuple[str, str]], current: str, q: dict) -> None:
    with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
        ui.label(name).style(
            f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; color: {SOFT}; width: 60px;"
        )
        with ui.element("div").style("display: inline-flex; border: 1px solid #cbd5e1; background: #fff;"):
            for i, (key, label) in enumerate(options):
                on = key == current
                href = "?" + urlencode({**q, name: key})
                ui.link(label, href).style(
                    f"{MONO} font-size: 8.5px; text-transform: uppercase; letter-spacing: .07em; padding: 2px 9px; "
                    f"line-height: 14px; text-decoration: none; color: {INK if on else SOFT}; "
                    f"background: {'#dbe7f3' if on else '#fff'}; {'border-left: 1px solid #cbd5e1;' if i else ''}"
                )


def _glyph(size: int = 11, color: str = "#94a3b8") -> None:
    ui.html(load_icon_svg("particle.svg", color, size=size), sanitize=False).style("display: flex;")


# ── A: the frame, the rail and the Species tab ───────────────────────────────


class _Frame:
    """The registry as one view: its title line (the active species and the tabs), the rail, the tab's page."""

    def __init__(self, reg: Registry, sp: Species, pane: str, tm: str, q: dict) -> None:
        self.reg, self.sp, self.pane, self.tm, self.q = reg, sp, pane, tm, q
        self.tab = "species"
        self.body: ui.element | None = None
        self.seg = None

    def build(self) -> None:
        with ui.element("div").style(
            f"width: {RAIL_W + PANE[self.pane]}px; display: flex; flex-direction: column; background: #fff;"
        ):
            with ui.element("div").style(VIEW_HEAD_STYLE + " margin-bottom: 0;"):
                view_head_icon("particle.svg")
                ui.label("Particles registry").style(VIEW_TITLE_STYLE + " margin-right: 10px;")
                with ui.element("div").style(
                    "display: inline-flex; align-items: center; gap: 4px; margin-right: 10px;"
                ):
                    _glyph()
                    ui.label(self.sp.name).style(f"{SANS} font-size: 10px; color: #334155; font-weight: 600;")
                self.seg = render_segmented(TABS, self.tab, self._switch)
            with ui.element("div").style("display: flex; min-height: 420px;"):
                with ui.element("div").style(
                    f"width: {RAIL_W}px; flex-shrink: 0; border-right: 1px solid #e4eaf1; padding: 6px 0;"
                ):
                    self._rail()
                self.body = ui.element("div").style(
                    f"width: {PANE[self.pane]}px; padding: 12px 14px 18px; box-sizing: border-box; "
                    "display: flex; flex-direction: column; gap: 14px; min-width: 0;"
                )
                self._fill()

    def _rail(self) -> None:
        for s in self.reg.species:
            on = s.id == self.sp.id
            row = ui.element("div").classes("cbri-rrow" + (" sel" if on else ""))
            row.on("click", lambda _e, sid=s.id: ui.navigate.to("?" + urlencode({**self.q, "species": sid})))
            with row:
                _glyph()
                ui.label(s.name).style(
                    f"{SANS} font-size: 10px; color: #334155; font-weight: 600; overflow: hidden; "
                    "text-overflow: ellipsis; white-space: nowrap; min-width: 0;"
                )
                ui.element("div").style("flex: 1 1 auto;")
                if not s.templates:
                    ui.label("no template").style(f"{SANS} font-size: 9px; color: {WARN_INK};").tooltip(
                        "No template registered: template matching has nothing to search with"
                    )
                meta = f"{s.n_picks:,} picks" if s.n_picks else "no picks"
                ui.label(meta).style(
                    f"{MONO} font-size: 8.5px; color: {SOFT if s.n_picks else FAINT}; white-space: nowrap;"
                ).tooltip(
                    f"{s.n_picks:,} picks on {s.n_tomos_with_picks} tomograms, every list together"
                    if s.n_picks
                    else "Nothing picked yet"
                )
        with ui.element("div").classes("cbri-radd").on("click", lambda: _would("the New species dialog opens")):
            ui.label("+")
            ui.label("New species")

    def _switch(self, key: str) -> None:
        self.tab = key
        self.seg.set_active(key)
        self._fill()

    def _fill(self) -> None:
        self.body.clear()
        with self.body:
            if self.tab == "species":
                species_tab(self.reg, self.sp, go=self._switch)
            elif self.tab == "templates":
                _Volumes(self.reg, self.sp, self.tm, self.pane).build()
            elif self.tab == "tomograms":
                _Tomograms(self.reg, self.sp).build()
            elif self.tab == "scores":
                ui.label(
                    "Today's Analytics, less two blocks: its per-tomogram table (now the Tomograms tab) and Classes "
                    "and maps (each job's Results tab). Scores in σ, Where the picks sit and the overlap stay as they "
                    "are; nothing new is drawn for it here."
                ).style(f"{WORDS} max-width: 640px; line-height: 1.5;")
            else:
                ui.label("The STA kitchen, unchanged; it takes the whole pane and hides the rail, as today.").style(
                    WORDS
                )


def species_tab(reg: Registry, sp: Species, *, go) -> None:
    """The Species tab: what defines the species, in the job page's field geometry, then its status and its two
    actions."""
    ext = sp.extraction
    model = SimpleNamespace(
        name=sp.name,
        diameter=f"{sp.diameter:g}" if sp.diameter else "",
        symmetry=sp.symmetry,
        notes=sp.notes,
        box=str(ext[0]) if ext else "",
        binning=f"{ext[1]:g}" if ext else "",
        crop=str(ext[2]) if ext else "",
    )
    meta = " · ".join(x for x in (sp.origin, sp.created, f"catalog {sp.catalog}" if sp.catalog else "") if x)
    with section("Species", right=meta, tooltip=f"id {sp.id}"):
        with field_grid():
            text_field("Name", model, "name", is_frozen=False, save_handler=_noop, width=L)
            with ui.element("div").style(FIELD_ROW_STYLE):
                ui.label("Overlay colour").style(FIELD_LABEL_STYLE).tooltip(
                    "Drawn on the canvases only: tomogram overlays, the pick viewer, napari, the gallery. Here, and "
                    "nowhere else in the chrome."
                )
                _swatch(sp.color)
            text_field("Diameter", model, "diameter", is_frozen=False, save_handler=_noop, width=S, suffix="Å")
            text_field("Symmetry", model, "symmetry", is_frozen=False, save_handler=_noop, width=M)
            text_field("Notes", model, "notes", is_frozen=False, save_handler=_noop, width=L)
            with ui.element("div").style(FIELD_ROW_STYLE):
                ui.label("Template matching").style(FIELD_LABEL_STYLE)
                tpl = next((v for v in sp.templates if v.selected), None)
                msk = next((v for v in sp.masks if v.selected), None)
                with ui.element("div").style(
                    f"display: flex; align-items: baseline; gap: 6px; min-width: 0; flex: 1 1 0; {MONO} "
                    f"font-size: 10px; color: {BODY};"
                ):
                    if tpl is None and msk is None:
                        ui.label("no template or mask selected").style(f"{SANS} font-size: 10px; color: {WARN_INK};")
                    else:
                        for word, v in (("template", tpl), ("mask", msk)):
                            ui.label(word).style(f"{SANS} font-size: 9.5px; color: {SOFT}; flex-shrink: 0;")
                            if v is None:
                                ui.label("none").style(f"color: {WARN_INK}; flex-shrink: 0;")
                            else:
                                path_text(v.path, reg.path).style("flex: 0 1 280px; min-width: 90px;")
                    ui.label("Templates & masks →").style(
                        f"{SANS} font-size: 10px; color: {INK}; cursor: pointer; text-decoration: underline dotted; "
                        "flex-shrink: 0; margin-left: 4px;"
                    ).on("click", lambda: go("templates"))
    with section(
        "Extraction geometry",
        right="" if ext else "not set",
        tooltip=(
            "Feeds the Subtomo extraction job: the box cut around every pick, in binned voxels. Set here once and a "
            "new Subtomo extraction job starts with these values; hand-picked lists with no such job are extracted "
            "with them directly. One geometry per species per project."
        ),
    ):
        with field_grid():
            for label, attr, unit in (("Box", "box", "px"), ("Binning", "binning", "×"), ("Crop", "crop", "px")):
                inp = text_field(label, model, attr, is_frozen=False, save_handler=_noop, width=S, suffix=unit)
                inp.props('placeholder="not set"')
    with section("Status"):
        ui.label(
            f"{sp.n_picks:,} picks on {sp.n_tomos_with_picks} of {len(sp.tomos)} tomograms · {sp.n_kept:,} kept · "
            f"{sp.n_extracted_lists} lists extracted · {sp.n_sets} particle sets"
        ).style(WORDS)
        if sp.source_label:
            ui.label(f"Scores from {sp.source_label}.").style(CAPTION)
    with ui.element("div").style("display: flex; gap: 8px; align-items: center; margin-top: 2px;"):
        house_button(
            "Delete species",
            lambda: _would("the delete confirmation opens"),
            kind="danger",
            tooltip="Registry entry, pick lists, templates and masks on disk, and the bound jobs",
        )
        house_button(
            "Publish to catalog",
            lambda: _would("the definition is copied to the lab catalog"),
            tooltip="Copy this species' definition (name, Ø, symmetry, notes, templates, masks) to the lab catalog as "
            "a new version. Picks and extractions stay here.",
        )


def _swatch(color: str) -> None:
    """The one place the overlay colour shows in the chrome: a 10 px square and its value, the palette on click."""
    with ui.element("div").style("display: inline-flex; align-items: center; gap: 6px; cursor: pointer;") as box:
        sq = ui.element("div").style(
            f"width: 10px; height: 10px; background: {color}; box-shadow: 0 0 0 1px rgba(15, 29, 46, .25);"
        )
        label = ui.label(color).style(f"{MONO} font-size: 9.5px; color: {SOFT};")
        with ui.menu().props("auto-close"), ui.element("div").style("display: flex; gap: 4px; padding: 6px;"):
            for c in SPECIES_OVERLAY_COLORS:

                def pick(c=c) -> None:
                    sq.style(f"background: {c};")
                    label.set_text(c)

                ui.element("div").style(
                    f"width: 14px; height: 14px; background: {c}; cursor: pointer; "
                    f"box-shadow: 0 0 0 1px {'#12324f' if c == color else 'rgba(15, 29, 46, .2)'};"
                ).on("click", pick)
    box.tooltip("The overlay palette: saturated on purpose, to stand out on a greyscale tomogram")


# ── B: Templates & masks ─────────────────────────────────────────────────────

_VOL_COLS = "10px minmax(0, 1fr) 96px 150px 70px 58px 46px"


class _Volumes:
    """Templates & masks as tables (`variant` "a": two, stacked; "c": one, masks under their template), the viewer
    beside them in a wide pane and under them in a narrow one."""

    def __init__(self, reg: Registry, sp: Species, variant: str, pane: str) -> None:
        self.reg, self.sp, self.variant, self.pane = reg, sp, variant, pane
        self.sel = {
            "template": next((v.id for v in sp.templates if v.selected), ""),
            "mask": next((v.id for v in sp.masks if v.selected), ""),
        }
        self.loaded: Vol | None = None
        self.tables: ui.element | None = None
        self.viewer: ui.element | None = None

    def build(self) -> None:
        wide = self.pane == "wide"
        with ui.element("div").style(
            "display: grid; width: 100%; gap: 16px 18px; align-items: start; "
            f"grid-template-columns: {'minmax(0, 1fr) 300px' if wide else 'minmax(0, 1fr)'};"
        ):
            self.tables = ui.element("div").style("display: flex; flex-direction: column; gap: 16px; min-width: 0;")
            self.viewer = ui.element("div").style("display: flex; flex-direction: column; min-width: 0;")
        self._draw_tables()
        self._draw_viewer()
        section_head("Activity", right="nothing this session", caret="▸").tooltip(
            "The workbench's log, folded: what was made, imported or deleted here"
        )

    # tables

    def _draw_tables(self) -> None:
        self.tables.clear()
        with self.tables:
            if self.variant == "a":
                with ui.column().style(SECTION_STYLE):
                    with panel_head("Templates", right=f"{len(self.sp.templates)}"):
                        house_button("New template…", lambda: self._dialog("template"))
                    self._head(("", "file", "polarity", "header", "stats", "size", ""))
                    for v in self.sp.templates:
                        self._row(v)
                    if not self.sp.templates:
                        self._empty(
                            "No template yet: New template… makes one from a shape, a PDB or EMDB entry, or a file."
                        )
                with ui.column().style(SECTION_STYLE):
                    with panel_head("Masks", right=f"{len(self.sp.masks)}"):
                        house_button("New mask…", lambda: self._dialog("mask"))
                    self._head(("", "file", "method", "header", "stats", "size", ""))
                    for v in self.sp.masks:
                        self._row(v)
                    if not self.sp.masks:
                        self._empty("No mask yet: New mask… makes one from a template, a sphere, or a file.")
            else:
                with ui.column().style(SECTION_STYLE):
                    meta = f"{len(self.sp.templates)} templates · {len(self.sp.masks)} masks"
                    with panel_head("Templates & masks", right=meta):
                        house_button("New template…", lambda: self._dialog("template"))
                        house_button("New mask…", lambda: self._dialog("mask"))
                    self._head(("", "file", "polarity · method", "header", "stats", "size", ""))
                    placed: set[str] = set()
                    for t in self.sp.templates:
                        self._row(t)
                        for m in self.sp.masks:
                            if m.parent == t.id:
                                self._row(m, child=True)
                                placed.add(m.id)
                    rest = [m for m in self.sp.masks if m.id not in placed]
                    if rest:
                        ui.label("masks of no registered template").style(
                            f"{SANS} font-size: 9.5px; color: {SOFT}; padding: 6px 6px 2px;"
                        )
                        for m in rest:
                            self._row(m, child=True)
                    if not self.sp.templates and not self.sp.masks:
                        self._empty("Nothing registered: New template… and New mask… make them.")

    def _head(self, labels: tuple[str, ...]) -> None:
        with ui.element("div").classes("cbri-vhead").style(f"--cbri-cols: {_VOL_COLS};"):
            for i, text in enumerate(labels):
                ui.label(text).style("text-align: right;" if i in (5,) else "")

    def _empty(self, text: str) -> None:
        ui.label(text).style(f"{CAPTION} padding: 6px;")

    def _row(self, v: Vol, *, child: bool = False) -> None:
        on = self.sel[v.kind] == v.id
        row = ui.element("div").classes("cbri-vrow" + (" sel" if on else "") + (" child" if child else ""))
        row.style(f"--cbri-cols: {_VOL_COLS};")
        row.on("click", lambda _e, v=v: self._select(v))
        what = "template" if v.kind == "template" else "mask"
        with row:
            ui.html(f'<span class="cbri-dot-{"sel" if on else "off"}"></span>', sanitize=False).tooltip(
                f"The {what} template matching uses" if on else f"Use this {what}"
            )
            with ui.element("div").style(
                f"display: flex; align-items: center; gap: 4px; min-width: 0; {'padding-left: 12px;' if child else ''}"
            ):
                if child:
                    ui.label("└").style(f"{MONO} font-size: 9.5px; color: {FAINT}; flex-shrink: 0;")
                p = path_text(v.path, self.reg.path).style("flex: 1 1 0; min-width: 0;")
                if v.source:
                    p.tooltip(v.source)
            if v.kind == "template":
                ui.html(parts.pol_html(v.polarity), sanitize=False)
            else:
                lbl = ui.label(v.method or "—").style(f"{MONO} font-size: 9.5px; color: {BODY};")
                if v.knobs:
                    lbl.tooltip(v.knobs)
            ui.label(parts.header_text(v.apix, v.box, v.lowpass)).style(
                f"{MONO} font-size: 9.5px; color: {BODY}; font-variant-numeric: tabular-nums;"
            )
            text, mark, hover = parts.stats_text(v.dmin, v.dmax, v.rms)
            dot = {"warn": '<span class="cbri-warn"></span>', "bad": '<span class="cbri-bad"></span>'}.get(mark, "")
            ui.html(
                f'<span style="{MONO} font-size: 9.5px; color: {SOFT if not mark else BODY};" title="{hover}">'
                f"{dot}{text}</span>",
                sanitize=False,
            )
            size, large = parts.size_text(v.size)
            ui.html(
                f'<span style="{MONO} font-size: 9.5px; color: {SOFT};"'
                f"{' title=' + chr(34) + 'Large: molstar may stall loading it' + chr(34) if large else ''}>"
                f"{'<span class=cbri-warn></span>' if large else ''}{size}</span>",
                sanitize=False,
            ).style("text-align: right;")
            with (
                ui.element("div")
                .classes("cbri-acts")
                .style("display: flex; justify-content: flex-end; align-items: center;")
            ):
                icon_button("visibility", tooltip="Load into the viewer").on(
                    "click.stop", lambda _e, v=v: self._load(v)
                )
                icon_button("close", tooltip="Delete", kind="danger").on(
                    "click.stop", lambda _e, v=v: _would(f"the delete confirmation for {v.path.rsplit('/', 1)[-1]}")
                )

    def _select(self, v: Vol) -> None:
        if self.sel[v.kind] == v.id:
            return
        self.sel[v.kind] = v.id
        self._draw_tables()

    # the viewer

    def _load(self, v: Vol) -> None:
        self.loaded = v
        self._draw_viewer()

    def _draw_viewer(self) -> None:
        self.viewer.clear()
        v = self.loaded
        with self.viewer, section("Viewer", right=(v.path.rsplit("/", 1)[-1][:28] if v else "")):
            if v is None:
                ui.label("No volume loaded: the eye on a row loads it.").style(CAPTION)
                return
            if not v.pics:
                ui.label("Unreadable: no header or no data.").style(f"{CAPTION} color: {WARN_INK};")
                return
            wide = self.pane == "wide"
            px = 92 if wide else 120
            with ui.element("div").style("display: flex; gap: 4px;"):
                for url, axis in zip(v.pics, ("z", "y", "x"), strict=False):
                    with ui.element("div").style("display: flex; flex-direction: column; gap: 2px;"):
                        ui.html(
                            f'<img src="{url}" style="width: {px}px; height: {px}px; display: block; '
                            f'background: #0f1d2e;">',
                            sanitize=False,
                        )
                        ui.label(f"along {axis}").style(f"{MONO} font-size: 8px; color: {FAINT};")
            ui.label(
                "Stand-in for molstar: the build mounts today's viewer here, sized to this column, and draws it only "
                "once something is loaded."
            ).style(CAPTION)

    # the creation dialogs

    def _dialog(self, kind: str) -> None:
        tpl = next((v for v in self.sp.templates if v.id == self.sel["template"]), None)
        apix = f"{tpl.apix:.3g}" if tpl and tpl.apix else ""
        box = str(tpl.box) if tpl and tpl.box else ""
        tabs = (
            [("shape", "Shape"), ("pdb", "PDB"), ("emdb", "EMDB"), ("import", "Import"), ("edit", "Edit current")]
            if kind == "template"
            else [("relion", "From template"), ("sphere", "Sphere"), ("import", "Import")]
        )
        actions = {
            "shape": "Generate",
            "pdb": "Fetch & simulate",
            "emdb": "Fetch & resample",
            "import": "Choose file…",
            "edit": "Make edited copy",
            "relion": "Create mask",
            "sphere": "Create sphere",
        }
        with (
            ui.dialog() as dlg,
            ui.card().style(
                "width: 560px; max-width: 92vw; padding: 14px 16px; gap: 10px; border-radius: 3px; box-shadow: "
                "0 6px 18px rgba(16, 42, 67, .14);"
            ),
        ):
            ui.label(f"New {kind} · {self.sp.name}").style(PANEL_TITLE_STYLE)
            panels: dict[str, ui.element] = {}
            state = {"tab": tabs[0][0]}
            action = None

            def show(key: str) -> None:
                state["tab"] = key
                seg.set_active(key)
                for k, el in panels.items():
                    el.style(f"visibility: {'visible' if k == key else 'hidden'};")
                action.set_text(actions[key])

            seg = render_segmented(tabs, tabs[0][0], show)
            # Grid-stacked so the dialog keeps the tallest form's height on a switch.
            with ui.element("div").style("display: grid; min-height: 76px;"):
                for key, _label in tabs:
                    panels[key] = ui.element("div").style(
                        "grid-area: 1 / 1; display: flex; flex-direction: column; gap: 6px; "
                        f"visibility: {'visible' if key == tabs[0][0] else 'hidden'};"
                    )
                    with panels[key]:
                        self._form(key, apix, box, tpl)
            with ui.element("div").style(
                "display: flex; justify-content: flex-end; gap: 8px; margin-top: 4px; align-self: stretch;"
            ):
                house_button("Cancel", dlg.close)
                action = house_button(
                    actions[tabs[0][0]],
                    lambda: (
                        _would(f"{actions[state['tab']]} runs as a background task; the new row appears"),
                        dlg.close(),
                    ),
                    kind="accent",
                )
        dlg.open()

    def _form(self, key: str, apix: str, box: str, tpl: Vol | None) -> None:
        row = "display: flex; align-items: center; gap: 12px; flex-wrap: wrap;"
        if key == "shape":
            with ui.element("div").style(row):
                house_field("ellipsoid", lambda: ui.input(value="550:550:550"), width="w-32", hint="x:y:z in Å")
                house_field("apix", lambda: ui.input(value=apix), hint="Pixel size of the volume (Å)")
                house_field("box", lambda: ui.input(value=box), hint="Box edge in pixels")
                house_check("auto box", value=True)
                house_field("lowpass", lambda: ui.input(placeholder="—"), hint="Optional filter (Å)")
            ui.label("Writes _white.mrc and _black.mrc and registers both.").style(CAPTION)
        elif key in ("pdb", "emdb"):
            with ui.element("div").style(row):
                house_field(key.upper(), lambda: ui.input(placeholder="6Z6J" if key == "pdb" else "30210"))
                house_field("apix", lambda: ui.input(value=apix))
                house_field("box", lambda: ui.input(value=box))
                house_field("resolution" if key == "pdb" else "lowpass", lambda: ui.input(placeholder="10"))
            ui.label(
                "Simulates a map from the model at this sampling."
                if key == "pdb"
                else "Fetches the map and resamples it."
            ).style(CAPTION)
        elif key == "import":
            ui.label(
                "Choose an .mrc: the header is read, you confirm its metadata, and a copy is registered in the project."
            ).style(CAPTION)
        elif key == "edit":
            if tpl is None:
                ui.label("Select a template first.").style(f"{CAPTION} color: {WARN_INK};")
                return
            ui.label(f"From {tpl.path.rsplit('/', 1)[-1]}:").style(f"{MONO} font-size: 9.5px; color: {BODY};")
            with ui.element("div").style(row):
                house_field("resample to", lambda: ui.input(value=apix), hint="Å/px")
                house_field("box", lambda: ui.input(value=box))
                house_field("lowpass", lambda: ui.input(placeholder="—"), hint="Å")
                house_check("flip polarity", value=False)
        elif key == "relion":
            with ui.element("div").style(row):
                house_field("method", lambda: ui.input(value="flexible_bounds"), width="w-32")
                house_field("threshold", lambda: ui.input(placeholder="auto"))
                house_field("ext", lambda: ui.input(value="3"), hint="Extend (px)")
                house_field("soft", lambda: ui.input(value="5"), hint="Soft edge (px)")
                house_field("lowpass", lambda: ui.input(placeholder="—"), hint="Å")
            ui.label("relion_mask_create on the selected template (its white sibling when it is black).").style(CAPTION)
        elif key == "sphere":
            with ui.element("div").style(row):
                house_field(
                    "diameter",
                    lambda: ui.input(value=f"{self.sp.diameter:g}" if self.sp.diameter else "", placeholder="Å"),
                    hint="Å, from the species",
                )
                house_field("soft edge", lambda: ui.input(value="5"), hint="px")
            ui.label("A soft-edged sphere centred in the box, sized to the species' diameter.").style(CAPTION)


# ── C: Tomograms ─────────────────────────────────────────────────────────────


class _Tomograms:
    def __init__(self, reg: Registry, sp: Species) -> None:
        self.reg, self.sp = reg, sp
        self.sort = "position"
        self.html: ui.html | None = None
        self.tomos = list(sp.tomos) if len(sp.tomos) <= ROW_CAP else list(sp.tomos[:SHOWN_WHEN_CAPPED])

    def build(self) -> None:
        sp = self.sp
        n_lists = sum(len(t.lists) for t in sp.tomos)
        meta = f"{len(sp.tomos)} tomograms · {sp.n_picks:,} picks · {sp.n_extracted_lists} of {n_lists} lists extracted"
        with ui.column().style(SECTION_STYLE):
            with panel_head("Tomograms", right=meta):
                house_button(
                    "Import picks…",
                    lambda: _would(
                        "a file chooser; each .coords is matched to its tomogram by name, the unmatched "
                        "are listed, never guessed"
                    ),
                )
                house_button("Extract all pending", lambda: _would("every list not yet extracted is submitted"))
                icon_button("refresh", lambda: _would("read again"), tooltip="Read again")
            lists = [pl for t in sp.tomos for pl in t.lists]
            if lists and all(len(t.lists) <= 1 for t in sp.tomos):
                one = lists[0]
                origin = f", template {one.origin}" if one.origin else ""
                ui.label(
                    f"Every tomogram holds one list: {one.label} ({one.source_kind or one.list_type}{origin})"
                    + (f", from {sp.source_label}." if sp.source_label else ".")
                ).style(CAPTION)
            saves = sum(t.n_saves for t in sp.tomos)
            with ui.element("div").style("display: flex; align-items: baseline; gap: 6px;"):
                ui.label("▸").style(f"font-size: 8px; color: {FAINT};")
                ui.label("watcher").style(parts.TH)
                ui.label(f"{saves} saved list{'s' if saves != 1 else ''} in the curation folders").style(
                    f"{SANS} font-size: 9.5px; color: {SOFT};"
                ).tooltip(
                    "Folded: opens to today's footer (what the server did with each save, and the folders it could "
                    "not attribute)"
                )
            self.html = ui.html(parts.tomo_table_html(sp, self.tomos, self.sort), sanitize=False).style("width: 100%;")
            self.html.on("click", self._click, js_handler=_TABLE_JS)
            if len(self.tomos) < len(sp.tomos):
                ui.label(
                    f"The first {len(self.tomos)} of {len(sp.tomos)} shown here. A table this long pages or "
                    "virtualizes in the build."
                ).style(CAPTION)

    def _click(self, e) -> None:
        a = e.args or {}
        act, tomo, slug = a.get("act"), a.get("tomo", ""), a.get("slug", "")
        short = tomo.rsplit("_Position_", 1)[-1] if "_Position_" in tomo else tomo
        if act == "sort":
            key = a.get("key") or "position"
            if key in parts.SORTS and key != self.sort:
                self.sort = key
                self.html.set_content(parts.tomo_table_html(self.sp, self.tomos, self.sort))
            return
        words = {
            "copy": f"copies {tomo}'s curation folder",
            "napari": f"opens Position_{short} in napari",
            "3dmod": f"copies the 3dmod command for Position_{short}",
            "curate": f"opens Position_{short} in ArtiaX with this species' picks",
            "curate-list": f"opens list {slug} of Position_{short} in ArtiaX",
            "extract": f"extracts list {slug} of Position_{short}",
            "delete": f"asks before deleting list {slug} of Position_{short}",
        }
        _would(words.get(act, act or "?"))


# ── D: colour ────────────────────────────────────────────────────────────────


def _today_size_badge(text: str, color: str) -> None:
    ui.label(text).style(
        f"color: {color}; font-size: 9px; font-weight: 700; font-variant-numeric: tabular-nums; padding: 1px 4px; "
        f"border-radius: 3px; background: {color}1a;"
    )


def colours_chips(sp: Species) -> None:
    """Each colour the registry draws today against what it becomes."""
    with ui.element("div").style(
        "display: grid; grid-template-columns: 170px 240px 240px; align-items: center; gap: 9px 16px;"
    ):
        for word in ("", "today", "proposed"):
            ui.label(word).style(parts.TH)

        def line(word: str, today, proposed) -> None:
            ui.label(word).style(f"{SANS} font-size: 10px; color: {SOFT};")
            with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                today()
            with ui.element("div").style("display: flex; align-items: center; gap: 8px;"):
                proposed()

        line(
            "Polarity",
            lambda: (render_polarity_chip("black"), render_polarity_chip("white")),
            lambda: (
                ui.html(parts.pol_html("black"), sanitize=False),
                ui.html(parts.pol_html("white"), sanitize=False),
            ),
        )
        line(
            "Mask method",
            lambda: (render_method_chip("spherical"), render_method_chip("relion"), render_method_chip("imported")),
            lambda: [
                ui.label(m).style(f"{MONO} font-size: 9.5px; color: {BODY};")
                for m in ("spherical", "relion", "imported")
            ],
        )
        line(
            "File size",
            lambda: (
                _today_size_badge("8 MB", "#6b7280"),
                _today_size_badge("130 MB", "#ea580c"),
                _today_size_badge("1.2 GB", "#dc2626"),
            ),
            lambda: [
                ui.html(
                    f'<span style="{MONO} font-size: 9.5px; color: {SOFT};">'
                    f"{'<span class=cbri-warn></span>' if big else ''}{t}</span>",
                    sanitize=False,
                )
                for t, big in (("8 MB", False), ("130 MB", True), ("1.2 GB", True))
            ],
        )
        line(
            "Unusual σ",
            lambda: ui.label("min -0.031 · max 0.21 · σ 0.0082").style("color: #ea580c; font-size: 9px;"),
            lambda: ui.html(
                f'<span style="{MONO} font-size: 9.5px; color: {BODY};"><span class="cbri-warn"></span>σ 0.0082</span>',
                sanitize=False,
            ),
        )
        line(
            "A pick-list row",
            lambda: (
                ui.element("div").style(f"width: 8px; height: 8px; border-radius: 50%; background: {sp.color};"),
                ui.label("candidates").style(f"{MONO} font-size: 10px; color: {BODY};"),
                ui.label("tm").style(f"{MONO} font-size: 9.5px; color: {SOFT};"),
            ),
            lambda: (
                ui.label("candidates").style(f"{MONO} font-size: 10px; color: {BODY};"),
                ui.label("tm").style(f"{MONO} font-size: 9.5px; color: {SOFT};"),
            ),
        )
        line(
            "The species' colour",
            lambda: ui.element("div").style(
                f"width: 12px; height: 12px; border-radius: 50%; background: {sp.color}; "
                "box-shadow: 0 0 0 2px #fff, 0 0 0 3px #e5e7eb;"
            ),
            lambda: _swatch(sp.color),
        )

        def today_sel() -> None:
            for c in ("#6366f1", "#a855f7"):
                ui.label("selected row").style(
                    f"{SANS} font-size: 9.5px; color: {BODY}; padding: 2px 8px; box-shadow: inset 0 0 0 1px {c}; "
                    f"background: {c}14; border-radius: 3px;"
                )

        line(
            "A selected template / mask",
            today_sel,
            lambda: ui.label("selected row").style(
                f"{SANS} font-size: 9.5px; color: {BODY}; padding: 2px 8px; "
                "box-shadow: inset 0 0 0 1px var(--cb-accent, #4a8fd4); background: #f4f8fc;"
            ),
        )


def colours_overlay(ov: data.Overlay) -> None:
    size = 420
    total = sum(len(d) for *_x, d in ov.layers)
    with ui.element("div").style("display: flex; gap: 18px; flex-wrap: wrap;"):
        for muted, title in ((False, "(a) the overlay palette, kept on canvases"), (True, "(b) a muted palette")):
            with ui.element("div").style("display: flex; flex-direction: column; gap: 5px;"):
                ui.label(title).style(parts.TH)
                ui.html(parts.overlay_html(ov, muted=muted, size=size), sanitize=False)
                with ui.element("div").style("display: flex; gap: 12px; flex-wrap: wrap;"):
                    for name, vivid, muted_c, dots in ov.layers:
                        with ui.element("div").style("display: flex; align-items: center; gap: 5px;"):
                            c = muted_c if muted else vivid
                            ui.element("div").style(f"width: 8px; height: 8px; border-radius: 50%; background: {c};")
                            ui.label(f"{name} · {len(dots):,}").style(f"{MONO} font-size: 9px; color: {SOFT};")
    ui.label(
        f"{ov.short} as WarpTools drew it (its central slice), with all {total:,} candidates of every species. The "
        "muted colours are roadmap 58's proposed class palette, matched by hue."
    ).style(CAPTION)


# ── E: drift on the roster ───────────────────────────────────────────────────

_LIGHT = {"Succeeded": "#22a06b", "Running": "#4a8fd4", "Failed": "#d14343", "Queued": "#60a5fa"}


def drift_roster(reg: Registry, mark: str) -> None:
    """The particle jobs' roster rows; a job whose snapshot differs from its species carries the mark on its tag."""
    with ui.element("div").style("display: flex; flex-direction: column; width: 400px;"):
        for sp in reg.species:
            for j in sp.jobs:
                with ui.element("div").style(
                    "display: flex; align-items: center; gap: 6px; height: 22px; padding: 0 6px; "
                    "border-bottom: 1px solid #f1f5f9;"
                ):
                    ui.element("div").style(
                        f"width: 7px; height: 7px; background: {_LIGHT.get(j.status, '#b3c0cf')}; flex-shrink: 0;"
                    ).tooltip(j.status)
                    ui.label(j.kind).style(f"{MONO} font-size: 10px; color: {INK}; width: 20ch; flex-shrink: 0;")
                    ui.label(j.number).style(f"{MONO} font-size: 8px; color: {FAINT}; width: 7ch;")
                    ui.element("div").style("flex: 1 1 auto;")
                    tip = f"Species: {sp.name}"
                    if j.drift:
                        tip += " · differs from the species: " + "; ".join(f"{k} {m}" for k, m in j.drift)
                        tip += " (a job keeps the values it was made with)"
                    cls = "cbri-tag" + (" notch" if j.drift and mark == "notch" else "")
                    with ui.element("span").classes(cls).props(f'title="{tip}"'):
                        ui.label(sp.name)
                        if j.drift and mark == "neq":
                            ui.label("≠").classes("d")


# ── the page ─────────────────────────────────────────────────────────────────


async def _registry(key: str, fresh: bool) -> Registry:
    if fresh or key not in _REG:
        # Star and header reads, a few seconds on a 48-tomogram project; the readers it joins live in ui/species, so
        # it runs in a thread rather than a worker (a worker imports no UI).
        _REG[key] = await asyncio.to_thread(data.registry, key)
    return _REG[key]


async def _overlay(key: str, fresh: bool) -> data.Overlay | None:
    if fresh or key not in _OVERLAY:
        _OVERLAY[key] = await run_in_worker(WorkKind.RENDER, data.overlay, key)
    return _OVERLAY[key]


async def render_registry_ideas() -> None:
    params = ui.context.client.request.query_params

    def pick(name: str, options, default: str) -> str:
        v = params.get(name, "")
        return v if v in options else default

    key = pick("project", data.PROJECTS, "grid2")
    pane = pick("pane", PANE, "wide")
    tm = pick("tm", ("a", "c"), "a")
    fresh = params.get("reload") == "1"
    ui.add_css(parts.CSS)
    with ui.column().style(
        "padding: 18px 26px 80px; gap: 6px; background: var(--cb-desk, #e9eef4); min-height: 100vh; width: 100%;"
    ):
        ui.label("Particles registry: Species, Templates & masks, Tomograms").style(
            f"{SANS} font-size: 16px; font-weight: 600; color: {INK};"
        )
        ui.label(
            "Roadmap 60 S0. Real projects, read and never written: nothing here saves, an action says what it would "
            "do. The switches apply to the whole page."
        ).style(f"{CAPTION} font-size: 10.5px;")
        waiting = ui.label(f"Reading {data.PROJECTS[key][0]} …").style(CAPTION)
        reg = await _registry(key, fresh)
        waiting.delete()
        sp = next((s for s in reg.species if s.id == params.get("species")), reg.species[0] if reg.species else None)
        q = {"project": key, "species": sp.id if sp else "", "pane": pane, "tm": tm}
        with ui.element("div").style("display: flex; flex-direction: column; gap: 5px; margin-top: 8px;"):
            _switch("project", [(k, k) for k in data.PROJECTS], key, {k: v for k, v in q.items() if k != "species"})
            if reg.species:
                _switch("species", [(s.id, s.name) for s in reg.species], sp.id, q)
            _switch("pane", [("wide", f"wide {PANE['wide']}"), ("narrow", f"narrow {PANE['narrow']}")], pane, q)
            _switch("tm", [("a", "D3 (a) stacked"), ("c", "D3 (c) one table")], tm, q)
        ui.label(f"{reg.name}: {data.PROJECTS[key][1]}.").style(f"{CAPTION} margin-top: 4px;")
        if sp is None:
            ui.label("This project has no species.").style(WORDS)
            return

        _heading(
            "A",
            "The registry (D2, S3)",
            "The title line carries the species and the five tabs: Species · Templates & masks · Tomograms · Scores · "
            "STA kitchen; Jobs is gone (its jobs are the roster's, its drift is E). The rail counts picks. The "
            "Species tab is the job page's field geometry: what defines the species, its extraction geometry, one "
            "status line, the two actions under the sections. The tabs work; the switch tm picks which Templates & "
            "masks the frame shows.",
        )
        frame = _panel("The registry at the workspace's width", "", RAIL_W + PANE[pane] + 30)
        with frame:
            _Frame(reg, sp, pane, tm, q).build()

        _heading(
            "B",
            "Templates & masks (D3, S4)",
            "Two drafts, the same rows. A row: in use (the dot; a click on the row selects), the file cut in its "
            "middle (hover unrolls it, a click copies it; its provenance on hover), polarity or method as words, the "
            "header, σ (min and max on hover, an amber dot when unusual), the size (an amber dot when molstar may "
            "stall), then the eye and the cross. The forms are in New template… and New mask…. The viewer sits beside "
            "the tables in a wide pane and under them in a narrow one.",
        )
        for variant, title, words in (
            ("a", "(a) Two tables, stacked", "Templates over masks, each table as tall as its rows."),
            (
                "c",
                "(c) One table",
                "Each mask under the template it was made from; masks of no registered template at the end.",
            ),
        ):
            with _panel(title, words, PANE[pane]):
                _Volumes(reg, sp, variant, pane).build()

        _heading(
            "C",
            "Tomograms (S5)",
            "One row per tomogram, 22 px: picks, kept, top σ, picks at or above 10σ, extracted, markers (hover for the "
            "rule), the viewer chips, Curate. A click on a column head sorts by it. A tomogram with more than one "
            "list says so beside its name and opens to them (▶), each with its own actions; with one list nothing "
            "opens, and what that list is is said once above the table. Import picks… is on the head, once.",
        )
        with _panel("Tomograms", "", PANE[pane]):
            _Tomograms(reg, sp).build()

        _heading(
            "D",
            "Colour (D1)",
            "Left, what the registry draws today; right, the proposal: words in ink and slate, the accent for "
            "selection only, an amber dot for what to look at. The overlay colour leaves the rows; it stays on the "
            "canvases and in the Species tab's one swatch.",
        )
        with _panel("Chips and marks", "", 760):
            colours_chips(sp)
        ov = await _overlay(key, fresh)
        with _panel(
            "The overlay palette on a real slice",
            "D1 (a) keeps today's palette on canvases; (b) would mute it everywhere, overlays included.",
            900,
        ):
            if ov is None:
                ui.label(
                    "This project has no WarpTools reconstruction picture with candidates on it; switch to grid2."
                ).style(CAPTION)
            else:
                colours_overlay(ov)

        _heading(
            "E",
            "Drift on the roster (D4)",
            "The roster's particle rows as they end, at the species tag. A job whose values differ from its "
            "species' (symmetry, Ø, template, mask) carries a mark on the tag; the hover says what differs. Two "
            "marks to choose from.",
        )
        n_drift = sum(1 for s in reg.species for j in s.jobs if j.drift)
        with ui.element("div").style("display: flex; gap: 18px; flex-wrap: wrap;"):
            for mark, title in (("neq", "M1 · ≠ in the tag"), ("notch", "M2 · a corner notch")):
                with _panel(title, "", 440):
                    if not any(s.jobs for s in reg.species):
                        ui.label("No particle jobs in this project.").style(CAPTION)
                    else:
                        drift_roster(reg, mark)
        if not n_drift:
            ui.label("No job here differs from its species: switch to sta or mystery to see the mark.").style(
                f"{CAPTION} color: {AMBER};"
            )
