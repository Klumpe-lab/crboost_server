"""The Species tab: what defines a species, in the job page's field geometry, then its status
and its two actions.

Species: name, overlay colour, glyph, diameter, symmetry, notes, and one line naming the template
and mask template matching uses (changed on Templates & masks). Extraction geometry: box,
binning and crop, empty until set. Status: one sentence. Then `[Delete species] [Publish
to catalog]`.

The editable rows are built once per species, outside every rev-gated view (the rev moves on
every edit they make): writes go through `set_species`, persistence is
`backend.save_project(path, debounce_s=1.0)` behind a Quasar input debounce, one save per
pause rather than per keystroke. `_TemplateLineView` is an in-memory `FingerprintedView`;
`_StatusView` is fed by an off-loop compute (`species_overview` reads star files) that runs
when the in-memory key (registry rev + job statuses) moves, on show, and at most every 15 s
while shown. Delete = confirm dialog → `species_admin.delete_species`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nicegui import ui

from services.aggregation.extraction import extraction_params_for_species
from services.jobs._base import SymmetryGroup
from services.jobs.subtomo_extraction import SubtomoExtractionParams
from services.models_base import (
    DEFAULT_GLYPH_HUE,
    DEFAULT_SPECIES_GLYPH,
    SPECIES_GLYPH_HUES,
    SPECIES_GLYPHS,
    SpeciesOrigin,
    species_glyph,
)
from services.particles.catalog import is_enabled as catalog_is_enabled
from services.particles.species_overview import SpeciesOverview, species_overview
from services.project_state import ParticleSpecies, TemplateMask, get_project_state_for
from services.species_admin import delete_species
from services.species_edit import set_species
from ui.components.buttons import house_button
from ui.components.color_swatch import render_color_swatch
from ui.components.path_text import path_text
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.components.species_pill import species_glyph_svg
from ui.job_plugins._field_styles import (
    FIELD_LABEL_STYLE,
    FIELD_ROW_STYLE,
    L,
    M,
    MONO,
    SANS,
    S,
    UNIT_STYLE,
    box_input,
    box_select,
    field_grid,
    section,
    voice,
)
from ui.particles.list_actions import commit_extraction_geometry
from ui.species.catalog import publish_to_catalog
from ui.species.tab import TabContext

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0  # status recompute cadence while shown (it touches disk)
_TITLE_CLS = "text-sm font-semibold text-gray-800"
_HINT_CLS = "text-[10px] text-gray-400"
_BODY_CLS = "text-xs text-gray-700"
_MONO_CLS = "text-[10px] font-mono text-gray-600"
_BODY = "#1e293b"
_SOFT = "#5b7089"
_WARN_INK = "#8a4b06"
_WORDS = f"{SANS} font-size: 10px; color: {_BODY};"
_TH = voice("th", font="'IBM Plex Sans', sans-serif", size="9px", weight=600, ink="#7d93ab")

# `C1` IS "no symmetry", and most complexes are C1 (ribosome, proteasome) — the option
# list says so instead of leaving the user to know it. One spelling across this tab,
# the creation dialog (`ui.species.prompt`) and the TM job tab's override row.
SYMMETRY_OPTIONS: dict[str, str] = {g.value: ("None (C1)" if g is SymmetryGroup.C1 else g.value) for g in SymmetryGroup}

_ORIGIN_TEXT = {
    SpeciesOrigin.WORKBENCH.value: "workbench",
    SpeciesOrigin.MANUAL.value: "manual",
    SpeciesOrigin.IMPORTED.value: "imported",
}


def _label(text: str, tooltip: str | None = None) -> ui.label:
    lbl = ui.label(text).style(FIELD_LABEL_STYLE)
    if tooltip:
        lbl.tooltip(tooltip)
    return lbl


# ── Species: the editable rows (built once, never inside a rev-gated view) ─────


def _mask_source(sp: ParticleSpecies, m: TemplateMask) -> str:
    """Where a mask came from: the template it was derived from (a soft link) or its method.
    Unknown stays "?" rather than a plausible guess."""
    if m.derived_from_template_id:
        tpl = sp.get_template_by_id(m.derived_from_template_id)
        if tpl is None:
            return "derived (source template no longer registered)"
        return f"derived from {os.path.basename(tpl.template_path)}"
    return m.method or "?"


def _provenance(sp: ParticleSpecies) -> tuple[str, str]:
    """(meta, hover) for the section's head: origin · created · catalog, the template and mask
    sources on hover."""
    origin = sp.origin or SpeciesOrigin.WORKBENCH.value  # "" on older species = workbench
    bits = [_ORIGIN_TEXT.get(origin, origin)]
    if sp.created_at:
        bits.append(sp.created_at.strftime("%Y-%m-%d"))
    if sp.catalog_id:
        bits.append(f"catalog {sp.catalog_id}" + (f" v{sp.catalog_version}" if sp.catalog_version else ""))
    sources = [
        f"template {os.path.basename(t.template_path)}: {t.source or t.imported_from or '?'}" for t in sp.templates
    ] + [f"mask {os.path.basename(m.mask_path)}: {_mask_source(sp, m)}" for m in sp.masks]
    hover = f"id {sp.id}" + ("\n" + "\n".join(sources) if sources else "")
    return " · ".join(bits), hover


def render_species_rows(backend, project_path: Path, species_id: str) -> None:
    """Name · overlay colour · glyph · diameter · symmetry · notes. Each edit = `set_species` (dirty +
    rev) and a 1-s debounced save."""
    sp = get_project_state_for(project_path).get_species(species_id)
    if sp is None:
        return

    def _set(**fields) -> None:
        res = set_species(get_project_state_for(project_path), species_id, **fields)
        if not res["success"]:
            ui.notify(res["error"], type="warning")
            return
        asyncio.create_task(backend.save_project(project_path, debounce_s=1.0))

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label("Name", f"The display name; the id stays `{species_id}`")
        name = box_input(ui.input(value=sp.name, placeholder="species name"), L).props("debounce=400")

        def _on_name(e) -> None:
            v = (e.value or "").strip()
            if v:  # an empty label is not a rename; the box keeps what was typed
                _set(name=v)

        name.on_value_change(_on_name)

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(
            "Overlay colour",
            "What this species' picks are drawn in on the canvases: tomogram overlays, the pick viewer, napari, the "
            "gallery. Here, and nowhere else in the chrome.",
        )
        render_color_swatch(sp.color or "#3b82f6", lambda c: _set(color=c), tooltip="The overlay palette")

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(
            "Glyph",
            "How this species is marked in the app's chrome: the rail, the roster, job pages, the layers. Grey unless "
            "a hue is picked; the canvases draw the overlay colour.",
        )
        slot = ui.element("div").style("display: inline-flex; min-width: 0;")
        glyph_row = _GlyphRow(slot, project_path, species_id, _set)
        glyph_row.refresh()

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label("Diameter", "Ø of the particle — the default for new Pick candidates jobs")
        diam = box_input(ui.number(value=sp.diameter_ang, min=0, step=10, placeholder="not set"), S)
        diam.props("debounce=400")
        ui.label("Å").style(UNIT_STYLE)

        def _on_diam(e) -> None:
            try:
                v = float(e.value) if e.value not in (None, "") else None
            except (TypeError, ValueError):
                return  # mid-typing garbage; the next valid value lands
            if v is not None and v <= 0:
                return  # a zero on the way to a number; the door refuses it
            _set(diameter_ang=v)

        diam.on_value_change(_on_diam)

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label(
            "Symmetry",
            "Point-group symmetry of the particle. Most complexes are C1; the default for new Template Match, "
            "Reconstruct and Class 3D jobs, which can still override it.",
        )
        sym = box_select(ui.select(options=SYMMETRY_OPTIONS, value=sp.symmetry or SymmetryGroup.C1.value), M)
        sym.on_value_change(lambda e: _set(symmetry=e.value or SymmetryGroup.C1.value))
        ui.label("").style(UNIT_STYLE)

    with ui.element("div").style(FIELD_ROW_STYLE):
        _label("Notes")
        notes = box_input(ui.input(value=sp.notes or "", placeholder="free-form, optional"), L)
        notes.props("debounce=400").on_value_change(lambda e: _set(notes=e.value or ""))


class _GlyphRow(FingerprintedView):
    """The species' glyph at 14 px, its name and its hue; a click opens the picker: the glyphs in a 6-wide grid of
    36 px tiles, the default first and the current one ringed in the accent, the hues as 14 px squares under them.
    A pick sets it at once and closes the menu; the row draws again when the species' glyph or hue moved."""

    def __init__(self, container: ui.element, project_path: Path, species_id: str, set_fields) -> None:
        super().__init__(container)
        self._project_path = project_path
        self._species_id = species_id
        self._set = set_fields

    def _species(self) -> ParticleSpecies | None:
        return get_project_state_for(self._project_path).get_species(self._species_id)

    def signature(self) -> Any:
        sp = self._species()
        return None if sp is None else (sp.glyph, sp.glyph_hue)

    def _pick(self, **fields) -> None:
        self._set(**fields)
        self.refresh()

    def render(self) -> None:
        sp = self._species()
        if sp is None:
            return
        key, _ink = species_glyph(sp)
        hue = sp.glyph_hue or DEFAULT_GLYPH_HUE
        name = SPECIES_GLYPHS[key].lower() + (" (default)" if key == DEFAULT_SPECIES_GLYPH else "")
        with ui.element("div").style(
            "display: inline-flex; align-items: center; gap: 7px; cursor: pointer; padding: 1px 6px 1px 3px; "
            "border: 1px solid #dbe3ec; border-radius: 2px; min-width: 0;"
        ):
            ui.html(species_glyph_svg(sp, 14), sanitize=False).style("display: flex; flex-shrink: 0;")
            ui.label(name).style(f"{SANS} font-size: 10px; color: {_BODY}; white-space: nowrap;")
            ui.label(SPECIES_GLYPH_HUES.get(hue, SPECIES_GLYPH_HUES[DEFAULT_GLYPH_HUE])[0]).style(
                f"{MONO} font-size: 9px; color: {_SOFT}; white-space: nowrap;"
            )
            ui.label("▾").style("font-size: 8px; color: #94a3b8;")
            with ui.menu().props("auto-close"):
                self._picker(sp, key, hue)

    def _picker(self, sp: ParticleSpecies, current: str, hue: str) -> None:
        with ui.element("div").style("display: flex; flex-direction: column; gap: 8px; padding: 8px; width: 252px;"):
            ui.label("glyph").style(_TH)
            with ui.element("div").style("display: grid; grid-template-columns: repeat(6, 36px); gap: 4px;"):
                for key, title in SPECIES_GLYPHS.items():
                    on = key == current
                    tile = ui.element("div").style(
                        "width: 36px; height: 36px; display: flex; align-items: center; justify-content: center; "
                        f"cursor: pointer; background: {'#f4f8fc' if on else '#ffffff'}; box-shadow: inset 0 0 0 1px "
                        f"{'var(--cb-accent, #4a8fd4)' if on else '#e4eaf1'};"
                    )
                    tile.on("click", lambda _e, k=key: self._pick(glyph=k))
                    with tile:
                        ui.html(species_glyph_svg(sp, 22, key=key), sanitize=False).style("display: flex;")
                    tile.tooltip(f"{title}: the default" if key == DEFAULT_SPECIES_GLYPH else title)
            ui.label("hue").style(_TH)
            with ui.element("div").style("display: flex; gap: 5px; flex-wrap: wrap;"):
                for key, (word, ink) in SPECIES_GLYPH_HUES.items():
                    ring = "2px #ffffff, 0 0 0 3px #12324f" if key == hue else "1px rgba(15, 29, 46, 0.15)"
                    sw = ui.element("div").style(
                        f"width: 14px; height: 14px; background: {ink}; cursor: pointer; box-shadow: 0 0 0 {ring};"
                    )
                    sw.on("click", lambda _e, k=key: self._pick(glyph_hue=k))
                    sw.tooltip(word + (" (the default)" if key == DEFAULT_GLYPH_HUE else ""))


# ── The template-matching line (in-memory only — safe to re-render on every tick) ──


class _TemplateLineView(FingerprintedView):
    """One row: the template and the mask template matching uses, and the way to where they are
    changed. "None" is a real state (de-novo picking), said in amber, never filled in."""

    def __init__(self, container: ui.element, tab: OverviewTab) -> None:
        super().__init__(container)
        self._tab = tab

    def _species(self) -> ParticleSpecies | None:
        return get_project_state_for(self._tab.ctx.project_path).get_species(self._tab.ctx.species_id)

    def signature(self) -> Any:
        sp = self._species()
        if sp is None:
            return ()
        t, m = sp.get_selected_template(), sp.get_selected_mask()
        return (t.template_path if t else "", m.mask_path if m else "")

    def render(self) -> None:
        sp = self._species()
        if sp is None:
            return
        select_tab = (self._tab.ctx.callbacks or {}).get("species_select_tab")
        tpl, mask = sp.get_selected_template(), sp.get_selected_mask()
        with ui.element("div").style(FIELD_ROW_STYLE):
            _label(
                "Template matching",
                "What a new Template Match job starts with: a job keeps the files it was made with, so changing the "
                "selection does not reach jobs that exist. The selection is changed on Templates & masks.",
            )
            with ui.element("div").style(
                f"display: flex; align-items: center; gap: 6px; min-width: 0; flex: 1 1 0; {MONO} font-size: 10px; "
                f"color: {_BODY};"
            ):
                if tpl is None and mask is None:
                    ui.label("no template or mask selected").style(f"{SANS} font-size: 10px; color: {_WARN_INK};")
                else:
                    for word, path in (
                        ("template", tpl.template_path if tpl else ""),
                        ("mask", mask.mask_path if mask else ""),
                    ):
                        ui.label(word).style(f"{SANS} font-size: 9.5px; color: {_SOFT}; flex-shrink: 0;")
                        if not path:
                            ui.label("none").style(f"color: {_WARN_INK}; flex-shrink: 0;")
                        else:
                            path_text(path, self._tab.ctx.project_path).style("flex: 0 1 280px; min-width: 90px;")
                if select_tab is not None:
                    ui.label("Templates & masks →").style(
                        f"{SANS} font-size: 10px; color: var(--cb-head-ink, #12324f); cursor: pointer; "
                        "text-decoration: underline dotted; flex-shrink: 0; margin-left: 4px;"
                    ).on("click", lambda _e: select_tab("templates"))


# ── Extraction geometry (built once — it holds inputs) ────────────────────────

# (ExtractionParams attr, label, unit, min, step) — the min/step match `geometry_inputs()` in
# ui/particles/list_actions.py, so this section and the first-extract modal ask for the same
# thing. The hovers come verbatim from the job model, so this section and the job tab explain
# box/bin/crop the same way (services/jobs/subtomo_extraction.py).
_GEOMETRY_FIELDS = (
    ("box_size", "Box", "px", 16, 2),
    ("binning", "Binning", "×", 0.1, 0.5),
    ("crop_size", "Crop", "px", 16, 2),
)
_GEOMETRY_TOOLTIP = (
    "Feeds the Subtomo extraction job: the box cut around every pick, in binned voxels. Set here once and a new "
    "Subtomo extraction job starts with these values; hand-picked lists with no such job are extracted with them "
    "directly, instead of stopping to ask. One geometry per species per project: a species extracted from tomogram "
    "sets at different binnings would need one per set, which is not supported."
)


class ExtractionGeometry:
    """`species.extraction_params` — box / binning / crop.

    Not a `FingerprintedView`: it owns three inputs, and every commit bumps the registry
    rev, so a signature-gated rebuild would destroy the field the user just typed into.
    Only the section's meta (what is in use) is refreshed, and only when its text moved.

    Empty is a real state: unset renders empty with "not set" in the boxes, never a default
    dressed up as a choice the user made — a box size is not ours to guess.
    """

    def __init__(self, tab: OverviewTab) -> None:
        self._tab = tab
        self._inputs: dict[str, ui.number] = {}
        self._meta: ui.label | None = None
        self._meta_tip: ui.tooltip | None = None

    def build(self) -> None:
        sp = get_project_state_for(self._tab.ctx.project_path).get_species(self._tab.ctx.species_id)
        ep = getattr(sp, "extraction_params", None) if sp is not None else None
        descriptions = _geometry_descriptions()
        with section("Extraction geometry", right=" ", tooltip=_GEOMETRY_TOOLTIP) as col:
            self._meta = col.head.meta_label
            with self._meta:
                self._meta_tip = ui.tooltip("")
            with field_grid():
                for attr, label, unit, minimum, step in _GEOMETRY_FIELDS:
                    with ui.element("div").style(FIELD_ROW_STYLE):
                        _label(label, descriptions.get(attr) or None)
                        inp = box_input(
                            ui.number(value=getattr(ep, attr, None), min=minimum, step=step, placeholder="not set"), S
                        ).props("debounce=600")
                        inp.on_value_change(lambda _e: self._commit())
                        ui.label(unit).style(UNIT_STYLE)
                        self._inputs[attr] = inp
        self.refresh()

    def _commit(self) -> None:
        """All three or nothing — a partial edit saves nothing, exactly the rule
        `commit_extraction_geometry` already enforces (it is the one writer)."""
        values = {attr: self._inputs[attr].value for attr, *_ in _GEOMETRY_FIELDS}
        if not all(values.values()):
            return  # mid-edit
        ctx = self._tab.ctx
        sp = get_project_state_for(ctx.project_path).get_species(ctx.species_id)
        ep = getattr(sp, "extraction_params", None) if sp is not None else None
        if ep is not None and (ep.box_size, ep.binning, ep.crop_size) == (
            int(values["box_size"]),
            float(values["binning"]),
            int(values["crop_size"]),
        ):
            return  # nothing moved — do not bump the rev on a re-render's echo
        asyncio.create_task(self._save(values))

    async def _save(self, values: dict) -> None:
        ctx = self._tab.ctx
        if await commit_extraction_geometry(
            ctx.backend, ctx.project_path, ctx.species_id, values["box_size"], values["binning"], values["crop_size"]
        ):
            self.refresh()

    def refresh(self) -> None:
        if self._meta is None:
            return
        text, hover = self._in_use()
        if self._meta.text != text:
            self._meta.set_text(text)
            self._meta_tip.set_text(hover)

    def _in_use(self) -> tuple[str, str]:
        """`extraction_params_for_species`' precedence, in words: the subtomo job wins, then
        these values, then nothing (never a guess)."""
        ctx = self._tab.ctx
        state = get_project_state_for(ctx.project_path)
        params = extraction_params_for_species(state, ctx.species_id)
        if params is None:
            return "not set", "Asked once at the first extraction, never guessed."
        sp = state.get_species(ctx.species_id)
        ep = getattr(sp, "extraction_params", None) if sp is not None else None
        geometry = f"box {params['box_size']} · bin {params['binning']:g} · crop {params['crop_size']}"
        if ep is not None and (ep.box_size, ep.binning, ep.crop_size) == (
            params["box_size"],
            params["binning"],
            params["crop_size"],
        ):
            return "in use", f"These values are in use ({geometry})."
        return (
            f"the Subtomo extraction job's in use: {geometry}",
            "The species' Subtomo extraction job sets the geometry its lists are cut with. Values here apply to "
            "hand-picked lists that have no such job.",
        )


def _geometry_descriptions() -> dict[str, str]:
    """The job model's own field descriptions — one wording, not two."""
    fields = SubtomoExtractionParams.model_fields
    return {attr: (fields[attr].description or "") for attr, *_ in _GEOMETRY_FIELDS}


# ── Status line (rev-gated view over an off-loop compute) ─────────────────────


@dataclass(frozen=True, slots=True)
class _Computed:
    overview: SpeciesOverview | None
    error: str | None


def _compute(state, project_path: Path, species_id: str) -> _Computed:
    """Thread body: star reads happen here, never on the event loop."""
    return _Computed(species_overview(state, project_path, species_id), None)


class _StatusView(FingerprintedView):
    def __init__(self, container: ui.element, tab: OverviewTab) -> None:
        super().__init__(container)
        self._tab = tab

    def signature(self) -> Any:
        c = self._tab.computed
        return (self._tab.ctx.species_id, None if c is None else (c.overview, c.error))

    def render(self) -> None:
        """One sentence of plain numbers. Per-list state and the extract actions are on the
        Picks tab, the jobs on the roster, the template and mask above."""
        c = self._tab.computed
        if c is None:
            ui.label("computing…").style(f"{SANS} font-size: 10px; color: #94a3b8;")
            return
        if c.error:
            ui.label(f"Status unavailable — {c.error}").classes("text-[11px] text-red-600").tooltip(
                "species_overview raised; full traceback in the server log"
            )
            return
        ov = c.overview
        ui.label(
            f"{ov.n_picks:,} pick{'s' if ov.n_picks != 1 else ''} on "
            f"{ov.n_tomos_with_picks} tomogram{'s' if ov.n_tomos_with_picks != 1 else ''} · "
            f"{ov.n_kept:,} kept · "
            f"{ov.n_extracted_lists} list{'s' if ov.n_extracted_lists != 1 else ''} extracted"
        ).style(_WORDS).tooltip(
            "kept = after a committed keep/drop filter. Per-list state, and the extract actions, are on the Picks & "
            "curation tab."
        )


class OverviewTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self._last_key: Any = None
        self._last_at: float = 0.0
        self._flight = SingleFlight()
        self._template_line: _TemplateLineView | None = None
        self._geometry: ExtractionGeometry | None = None
        self._status: _StatusView | None = None

    # ── Tab protocol ──────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        ctx = self.ctx
        sp = get_project_state_for(ctx.project_path).get_species(ctx.species_id)
        meta, hover = _provenance(sp) if sp is not None else ("", "")
        with container, ui.column().classes("w-full").style("gap: 18px; padding: 12px 14px 18px;"):
            with section("Species", right=meta, tooltip=hover), field_grid():
                render_species_rows(ctx.backend, ctx.project_path, ctx.species_id)
                line_slot = ui.element("div").style("width: 100%; min-width: 0;")
                self._template_line = _TemplateLineView(line_slot, self)
            self._geometry = ExtractionGeometry(self)
            self._geometry.build()
            with section("Status"):
                status_slot = ui.element("div").style("width: 100%;")
                self._status = _StatusView(status_slot, self)
            with ui.element("div").style("display: flex; gap: 8px; align-items: center;"):
                house_button(
                    "Delete species",
                    self._request_delete,
                    kind="danger",
                    tooltip="Registry entry, pick lists, templates and masks on disk, and the bound jobs",
                )
                # Only when a lab catalog is configured — see ui/species/catalog.py.
                if catalog_is_enabled():
                    house_button(
                        "Publish to catalog",
                        self._publish_to_catalog,
                        tooltip=(
                            "Copy this species' definition (name, Ø, symmetry, notes, templates, masks) to the lab "
                            "catalog as a new version. Picks and extractions stay here."
                        ),
                    )
        self._template_line.refresh()
        self._status.refresh()

    def refresh(self) -> None:
        """Page tick / on show (only while this tab is visible): recompute when the
        in-memory key moved, else at most every `_DISK_REFRESH_S`; repaint through the
        signature gate either way."""
        if self._template_line is not None:
            self._template_line.refresh()  # in-memory + signature-gated: free on a quiet tick
        if self._geometry is not None:
            self._geometry.refresh()
        key = self._memory_key()
        stale = time.monotonic() - self._last_at >= _DISK_REFRESH_S
        if self.computed is None or key != self._last_key or stale:
            asyncio.create_task(self._recompute(key))
        elif self._status is not None:
            self._status.refresh()

    # ── Compute ───────────────────────────────────────────────────────────────

    def _memory_key(self) -> tuple:
        state = get_project_state_for(self.ctx.project_path)
        return (
            state.registry_rev,
            tuple(sorted((iid, str(getattr(jm, "execution_status", ""))) for iid, jm in state.jobs.items())),
        )

    async def _recompute(self, key: tuple) -> None:
        async with self._flight("compute") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            state = get_project_state_for(ctx.project_path)
            try:
                computed = await asyncio.to_thread(_compute, state, ctx.project_path, ctx.species_id)
            except Exception as e:
                # Reported, not swallowed: traceback to the log, the cause into the block.
                logger.exception("Species overview compute failed for %s", ctx.species_id)
                computed = _Computed(None, f"{type(e).__name__}: {e}")
            self.computed = computed
            self._last_key = key
            self._last_at = time.monotonic()
            if self._status is not None:
                self._status.refresh()

    # ── Delete ────────────────────────────────────────────────────────────────

    def _request_delete(self) -> None:
        ctx = self.ctx
        state = get_project_state_for(ctx.project_path)
        sp = state.get_species(ctx.species_id)
        if sp is None:
            return
        refs = state.species_references(sp.id)
        folder = ctx.project_path / "templates" / sp.id
        with ui.dialog() as dialog, ui.card().classes("p-4 gap-2"):
            ui.label(f"Delete species '{sp.name}'?").classes(_TITLE_CLS)
            with ui.column().classes("gap-0 mt-1"):
                n_tpl, n_mask = len(sp.templates), len(sp.masks)
                ui.label(f"• {n_tpl} template{'s' if n_tpl != 1 else ''} on disk").classes(_BODY_CLS)
                ui.label(f"• {n_mask} mask{'es' if n_mask != 1 else ''} on disk").classes(_BODY_CLS)
                n_lists = len(refs["pick_lists"])
                if n_lists:
                    ui.label(f"• {n_lists} pick list{'s' if n_lists != 1 else ''} (curation)").classes(_BODY_CLS)
                n_ovr = len(refs["source_overrides"])
                if n_ovr:
                    ui.label(f"• {n_ovr} downstream input override{'s' if n_ovr != 1 else ''}").classes(_BODY_CLS)
                ui.label(f"• Folder: {folder}").classes(_MONO_CLS)
            # Jobs cascade: their job dirs and default_pipeline.star rows go with the
            # species, so the roster is left clean rather than holding rows that point
            # at a species which no longer exists.
            if refs["jobs"]:
                ui.label(f"• {len(refs['jobs'])} pipeline job(s), with their job folders: ").classes(_BODY_CLS)
                ui.label(", ".join(refs["jobs"])).classes(_MONO_CLS + " text-red-600")
            ui.label(
                "All registered files (+ sidecars) get removed. The folder is removed only if empty "
                "afterwards (manual drops are preserved)."
            ).classes(_HINT_CLS + " mt-1")
            ui.label("This cannot be undone.").classes(_HINT_CLS + " text-red-600")
            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                house_button("Cancel", dialog.close)

                async def _confirm():
                    dialog.close()
                    await self._do_delete()

                house_button("Delete species", _confirm, kind="danger")
        dialog.open()

    async def _publish_to_catalog(self) -> None:
        """Publish this species as a new catalog version.
        The confirm, the file check and the SingleFlight all live in `ui.species.catalog`."""
        ctx = self.ctx
        await publish_to_catalog(ctx.backend, ctx.project_path, ctx.species_id, on_done=self.refresh)

    async def _do_delete(self) -> None:
        # SingleFlight: the confirm button can be double-clicked; the second run would
        # find no species and only report "unknown".
        async with self._flight("delete") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            result = await delete_species(ctx.backend, ctx.project_path, ctx.species_id)
            if not result.get("success"):
                ui.notify(result["error"], type="negative")
                return
            for problem in result.get("errors") or []:
                ui.notify(problem, type="warning", timeout=5000)
            ui.notify(
                f"Deleted species '{ctx.species_id}' ({len(result.get('deleted_jobs') or [])} job(s), "
                f"{result.get('deleted_files', 0)} file(s))",
                type="positive",
            )
            ctx.on_species_deleted(ctx.species_id)
