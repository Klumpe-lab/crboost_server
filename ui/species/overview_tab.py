"""Overview tab (roadmap 10 S3): identity editor · status block · provenance · sanity row · delete.

The identity editor is the species header that used to sit on top of the Template
Workbench (swatch, name, Ø, symmetry, notes) — built ONCE per species in its own
container, outside every rev-gated view (the rev moves on every edit it makes). Writes
go through `state.mutate_species`; persistence is `backend.save_project(path,
debounce_s=1.0)` + Quasar input debounce, i.e. one save per pause instead of one per
keystroke (peeve P-02). The status block + sanity row are one `FingerprintedView` fed by
an off-loop compute (`species_overview` reads star files, `compute_pixel_chain` reads MRC
headers) that runs when the in-memory key (registry rev + job statuses) moves, on show,
and at most every 15 s while shown. Delete = confirm dialog → `species_admin.delete_species`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nicegui import ui

from services.jobs._base import SymmetryGroup
from services.models_base import SPECIES_OVERLAY_COLORS, SpeciesOrigin
from services.particles.species_overview import SpeciesOverview, species_overview
from services.pixel_chain import apply_sanity_rules, compute_pixel_chain
from services.project_state import ParticleSpecies, get_project_state_for
from services.species_admin import delete_species
from ui.components.chip import render_chip
from services.particles.catalog import is_enabled as catalog_is_enabled
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.species.catalog import publish_to_catalog
from ui.dashboard.pixel_sanity import render_pixel_sanity_table
from ui.species.tab import TabContext

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0  # status/sanity recompute cadence while shown (both touch disk)
_TITLE_CLS = "text-sm font-semibold text-gray-800"
_LABEL_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_HINT_CLS = "text-[10px] text-gray-400"
_BODY_CLS = "text-xs text-gray-700"
_MONO_CLS = "text-[10px] font-mono text-gray-600"
_INDIGO = "#6366f1"

_ORIGIN_TEXT = {
    SpeciesOrigin.WORKBENCH.value: "workbench (template-driven)",
    SpeciesOrigin.MANUAL.value: "manual (created for hand picking)",
    SpeciesOrigin.IMPORTED.value: "imported",
}


# ── Identity editor (built once, never inside a rev-gated view) ────────────────


def _render_color_swatch(backend, project_path: Path, species_id: str, current: str) -> None:
    """The species' overlay color, click to change. Species share one tomogram canvas,
    so color is how a user tells two picks apart — editable, constrained to the palette
    that stays legible over greyscale (SPECIES_OVERLAY_COLORS)."""

    def _dot_style(color: str) -> str:
        return (
            f"width: 12px; height: 12px; border-radius: 50%; background: {color}; "
            f"flex-shrink: 0; cursor: pointer; box-shadow: 0 0 0 2px #fff, 0 0 0 3px #e5e7eb;"
        )

    dot = ui.element("div").style(_dot_style(current)).tooltip("Overlay color")
    with dot, ui.menu().props("auto-close"), ui.row().classes("p-2 gap-1 flex-wrap").style("max-width: 128px;"):
        for color in SPECIES_OVERLAY_COLORS:
            selected = color.lower() == (current or "").lower()

            def _pick(c=color):
                def _apply(s: ParticleSpecies) -> None:
                    s.color = c

                get_project_state_for(project_path).mutate_species(species_id, _apply)
                asyncio.create_task(backend.save_project(project_path, debounce_s=1.0))
                # Repaint just the swatch — one attribute driving one visual property
                # needs no rebuild (and a rebuild would destroy the menu mid-click).
                dot.style(_dot_style(c))

            ui.element("div").style(
                f"width: 16px; height: 16px; border-radius: 50%; background: {color}; cursor: pointer; "
                f"box-shadow: 0 0 0 2px #fff, 0 0 0 {'3px #111827' if selected else '3px #e5e7eb'};"
            ).on("click", _pick)


def render_identity_editor(backend, project_path: Path, species_id: str) -> None:
    """Swatch · name · Ø · symmetry · notes, plus the provenance line. Each edit =
    `mutate_species` (dirty + rev) and a 1-s debounced save."""
    state = get_project_state_for(project_path)
    sp = state.get_species(species_id)
    if sp is None:
        return

    def _mutate(fn) -> None:
        get_project_state_for(project_path).mutate_species(species_id, fn)
        asyncio.create_task(backend.save_project(project_path, debounce_s=1.0))

    with (
        ui.card()
        .tight()
        .classes("w-full overflow-hidden")
        .style(f"border: 1px solid #e5e7eb; border-left: 4px solid {_INDIGO}; box-shadow: none;")
    ):
        with ui.row().classes("w-full items-center px-3 py-1 gap-3"):
            _render_color_swatch(backend, project_path, species_id, sp.color or "#3b82f6")

            name_input = ui.input(value=sp.name, placeholder="species name").props("dense outlined debounce=400")
            name_input.classes("w-44").tooltip(f"Display name (id stays `{species_id}`)")

            def _on_name(e):
                v = (e.value or "").strip()
                if not v:
                    return  # an empty label is not a rename; the input keeps what was typed

                def _apply(s: ParticleSpecies) -> None:
                    s.name = v

                _mutate(_apply)

            name_input.on_value_change(_on_name)

            with ui.row().classes("items-center gap-1 ml-2"):
                ui.label("Ø").classes(_BODY_CLS)
                diam_input = (
                    ui.number(value=sp.diameter_ang, placeholder="e.g. 250", step=10, min=0, suffix="Å")
                    .props("dense outlined debounce=400")
                    .classes("w-24")
                )

                def _on_diam(e):
                    try:
                        new_val = float(e.value) if e.value not in (None, "") else None
                    except (TypeError, ValueError):
                        return  # mid-typing garbage; the next valid value lands

                    def _apply(s: ParticleSpecies) -> None:
                        s.diameter_ang = new_val

                    _mutate(_apply)

                diam_input.on_value_change(_on_diam)

            with ui.row().classes("items-center gap-1"):
                ui.label("sym").classes(_BODY_CLS)
                sym_select = (
                    ui.select(options=[g.value for g in SymmetryGroup], value=sp.symmetry or "C1")
                    .props("dense outlined")
                    .classes("w-20")
                )

                def _on_sym(e):
                    new_val = e.value or "C1"

                    def _apply(s: ParticleSpecies) -> None:
                        s.symmetry = new_val

                    _mutate(_apply)

                sym_select.on_value_change(_on_sym)

            with ui.row().classes("items-center gap-1 flex-1"):
                notes_input = (
                    ui.input(value=sp.notes or "", placeholder="notes (free-form, optional)")
                    .props("dense outlined debounce=400")
                    .classes("flex-1")
                )

                def _on_notes(e):
                    v = e.value or ""

                    def _apply(s: ParticleSpecies) -> None:
                        s.notes = v

                    _mutate(_apply)

                notes_input.on_value_change(_on_notes)

        with ui.row().classes("w-full items-center px-3 pb-1 gap-3"):
            ui.label(
                "Defaults for new TM (symmetry) and pick-candidates (diameter) jobs. Existing jobs aren't auto-updated."
            ).classes(_HINT_CLS)
            ui.space()
            _render_provenance(sp)


def _render_provenance(sp: ParticleSpecies) -> None:
    """origin · created · catalog — the first reader of `ParticleSpecies.origin`;
    template / mask sources on hover."""
    origin = sp.origin or SpeciesOrigin.WORKBENCH.value  # "" pre-dates the field = workbench
    created = sp.created_at.strftime("%Y-%m-%d %H:%M") if sp.created_at else "—"
    if sp.catalog_id and sp.catalog_version:
        catalog = f"{sp.catalog_id} v{sp.catalog_version}"
    else:
        catalog = sp.catalog_id or "— (project-local)"
    sources = [
        f"template {os.path.basename(t.template_path)}: {t.source or t.imported_from or '?'}" for t in sp.templates
    ] + [
        f"mask {os.path.basename(m.mask_path)}: {m.imported_from or ('derived' if m.derived_from_template_id else '?')}"
        for m in sp.masks
    ]
    ui.label(f"origin {_ORIGIN_TEXT.get(origin, origin)} · created {created} · catalog {catalog}").classes(
        _MONO_CLS
    ).tooltip("\n".join(sources) if sources else "no templates or masks registered")


# ── Status block + sanity row (rev-gated view over an off-loop compute) ────────


@dataclass(frozen=True, slots=True)
class _Computed:
    overview: SpeciesOverview | None
    sanity_rows: list[dict]  # this species' pixel-chain rows, sanity rules applied
    sanity_key: str  # JSON of sanity_rows — the comparable form for the view signature
    error: str | None


def _compute(state, project_path: Path, species_id: str) -> _Computed:
    """Thread body: star / MRC-header reads happen here, never on the event loop."""
    overview = species_overview(state, project_path, species_id)
    rows = compute_pixel_chain(state)
    apply_sanity_rules(rows)
    rows = [r for r in rows if r.get("species_id") == species_id]
    return _Computed(overview, rows, json.dumps(rows, sort_keys=True, default=str), None)


class _StatusView(FingerprintedView):
    def __init__(self, container: ui.element, tab: OverviewTab) -> None:
        super().__init__(container)
        self._tab = tab

    def _species_terms(self) -> tuple:
        sp = get_project_state_for(self._tab.ctx.project_path).get_species(self._tab.ctx.species_id)
        if sp is None:
            return ()
        ep = sp.extraction_params
        return (
            len(sp.templates),
            sp.selected_template_id,
            len(sp.masks),
            sp.selected_mask_id,
            (ep.box_size, ep.binning, ep.crop_size) if ep else None,
        )

    def signature(self) -> Any:
        c = self._tab.computed
        computed = None if c is None else (c.overview, c.sanity_key, c.error)
        return (self._tab.ctx.species_id, computed, self._species_terms())

    def render(self) -> None:
        c = self._tab.computed
        sp = get_project_state_for(self._tab.ctx.project_path).get_species(self._tab.ctx.species_id)
        if sp is None:
            return
        with ui.column().classes("w-full gap-1"):
            with ui.row().classes("w-full items-baseline gap-2 px-1"):
                ui.label("STATUS").classes(_LABEL_CLS)
                ui.label("picks · extraction · bound jobs").classes(_HINT_CLS)
            if c is None:
                ui.label("computing…").classes(_HINT_CLS + " px-1")
                return
            if c.error:
                ui.label(f"Status unavailable — {c.error}").classes("text-[11px] text-red-600 px-1").tooltip(
                    "species_overview / pixel chain raised; full traceback in the server log"
                )
                return
            ov = c.overview
            gate_status = {"READY": "ok", "PENDING": "warn", "BLOCKED": "error"}.get(ov.gate, "neutral")
            with ui.element("div").classes("cb-chip-strip"):
                render_chip("tomos with picks", str(ov.n_tomos_with_picks), tooltip="tomograms with at least one pick")
                render_chip("picks", str(ov.n_picks), tooltip="sum over every list on every tomogram")
                render_chip("kept", str(ov.n_kept), tooltip="after committed keep/drop curation (= picks where none)")
                render_chip("extracted lists", str(ov.n_extracted_lists), tooltip="lists whose optimisation set exists")
                render_chip(
                    "gate",
                    ov.gate,
                    status=gate_status,
                    tooltip="Authoritative-list roll-up over every tomogram: READY = nothing left to extract, "
                    "PENDING = authoritative lists still to extract, BLOCKED = a choice cannot be extracted "
                    "(details per list on the Picks tab).",
                )
            with ui.element("div").classes("cb-chip-strip"):
                render_chip(
                    "pick candidates",
                    ov.ce_iid or "—",
                    status="info" if ov.ce_iid else "neutral",
                    tooltip="candidate-extract instance attributed to this species (Jobs tab)",
                )
                render_chip(
                    "subtomo",
                    ov.subtomo_iid or "—",
                    status="info" if ov.subtomo_iid else "neutral",
                    tooltip="subtomogram-extraction instance attributed to this species (Jobs tab)",
                )
                ep = sp.extraction_params
                render_chip(
                    "extraction",
                    f"box {ep.box_size} · bin {ep.binning:g} · crop {ep.crop_size}" if ep else "not set",
                    status="neutral" if ep else "warn",
                    tooltip="Per-species extraction geometry for hand-picked lists (box px · binning · crop px)."
                    + ("" if ep else " Undecided — asked on the first per-list extract, never defaulted."),
                )
                sel_t = sp.get_selected_template()
                sel_m = sp.get_selected_mask()
                render_chip(
                    "templates",
                    f"{len(sp.templates)}" + (f" · {os.path.basename(sel_t.template_path)}" if sel_t else ""),
                    tooltip="registered templates · selected one (Templates & masks tab)",
                )
                render_chip(
                    "masks",
                    f"{len(sp.masks)}" + (f" · {os.path.basename(sel_m.mask_path)}" if sel_m else ""),
                    tooltip="registered masks · selected one (Templates & masks tab)",
                )
            if c.sanity_rows:
                render_pixel_sanity_table(c.sanity_rows)
            else:
                ui.label("No pixel-chain rows for this species yet (no TM / pick / subtomo job).").classes(
                    _HINT_CLS + " px-1"
                )


class OverviewTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self._last_key: Any = None
        self._last_at: float = 0.0
        self._flight = SingleFlight()
        self._status: _StatusView | None = None

    # ── Tab protocol ──────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        ctx = self.ctx
        with container, ui.column().classes("w-full gap-3 p-2"):
            render_identity_editor(ctx.backend, ctx.project_path, ctx.species_id)
            status_slot = ui.element("div").classes("w-full")
            self._status = _StatusView(status_slot, self)
            with ui.row().classes("w-full items-center gap-2 px-1"):
                ui.button("Delete species", icon="delete_forever", on_click=self._request_delete).props(
                    "flat dense no-caps color=negative size=sm"
                )
                ui.label("registry entry, pick lists, templates / masks on disk and the bound jobs").classes(_HINT_CLS)
                # Only when a lab catalog is configured (roadmap 12) — see ui/species/catalog.py.
                if catalog_is_enabled():
                    ui.space()
                    ui.button("Publish to catalog", icon="publish", on_click=self._publish_to_catalog).props(
                        "flat dense no-caps color=indigo size=sm"
                    ).tooltip(
                        "Copy this species DEFINITION (name, Ø, symmetry, notes, templates, masks) up to the "
                        "lab catalog as a new version. Picks and extractions stay here."
                    )
        self._status.refresh()

    def refresh(self) -> None:
        """Page tick / on show (only while this tab is visible): recompute when the
        in-memory key moved, else at most every `_DISK_REFRESH_S`; repaint through the
        signature gate either way."""
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
                computed = _Computed(None, [], "", f"{type(e).__name__}: {e}")
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
                n_auth = len(refs["authoritative_pick_lists"])
                if n_auth:
                    ui.label(f"• {n_auth} authoritative-list choice{'s' if n_auth != 1 else ''}").classes(_BODY_CLS)
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
                ui.button("Cancel", on_click=dialog.close).props("flat dense no-caps")

                async def _confirm():
                    dialog.close()
                    await self._do_delete()

                ui.button("Delete species", on_click=_confirm).props("unelevated dense color=negative no-caps")
        dialog.open()

    async def _publish_to_catalog(self) -> None:
        """Overview action (roadmap 12): publish this species as a new catalog version.
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
