"""Overview tab: identity editor · bound files · status line · extraction geometry ·
provenance · delete (roadmap 10 S3, decluttered by picking-UI roadmaps 03 + 04).

The identity editor is the species header that used to sit on top of the Template
Workbench (swatch, name, diameter, symmetry, notes) — built ONCE per species in its own
container, outside every rev-gated view (the rev moves on every edit it makes). Writes
go through `state.mutate_species`; persistence is `backend.save_project(path,
debounce_s=1.0)` + Quasar input debounce, i.e. one save per pause instead of one per
keystroke (peeve P-02).

`_BindingsView` is an in-memory `FingerprintedView`, cheap on every tick.
`ExtractionGeometryPanel` is built once for the same reason as the identity editor (it
owns inputs; a rev-gated rebuild would destroy one mid-keystroke) and only refreshes its
effective-value line. `_StatusView` is fed by an off-loop compute (`species_overview`
reads star files) that runs when the in-memory key (registry rev + job statuses) moves,
on show, and at most every 15 s while shown. Delete = confirm dialog →
`species_admin.delete_species`.
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

from services.jobs._base import SymmetryGroup
from services.models_base import SpeciesOrigin
from services.aggregation.authoritative import extraction_params_for_species
from services.jobs.subtomo_extraction import SubtomoExtractionParams
from services.particles.species_overview import SpeciesOverview, species_overview
from services.project_state import ParticleSpecies, TemplateMask, get_project_state_for
from services.species_admin import delete_species
from ui.components.chip import render_method_chip, render_polarity_chip
from ui.components.color_swatch import render_color_swatch
from ui.components.path_link import render_path_link
from ui.job_plugins._field_styles import section_header, section_rule
from ui.particles.list_actions import commit_extraction_geometry
from services.particles.catalog import is_enabled as catalog_is_enabled
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.species.catalog import publish_to_catalog
from ui.species.tab import TabContext

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0  # status/sanity recompute cadence while shown (both touch disk)
# Three ranks, and they must not collide (the maintainer, 2026-08-19: "separate them
# visually based on what's separate conceptually"):
#   SECTION  — `section_header()` from the house vocabulary: 11 px semibold MIXED case.
#              "Species info" / "Template matching files" / "Extraction geometry".
#   FIELD    — 9 px uppercase, muted: the label beside one input.
#   HINT     — 10 px, lightest: the sentence explaining a section or a value.
# A section title is never uppercase and a field label never is not — that difference is
# the whole hierarchy, so do not reach for the other one to "emphasise" something.
_TITLE_CLS = "text-sm font-semibold text-gray-800"
_LABEL_CLS = "text-[9px] font-bold text-gray-400 uppercase tracking-wide"
_HINT_CLS = "text-[10px] text-gray-400"
_BODY_CLS = "text-xs text-gray-700"
_MONO_CLS = "text-[10px] font-mono text-gray-600"
# The name is the one input allowed to read larger — it is the title of the species.
_NAME_INPUT_STYLE = "font-size: 12px; font-weight: 600; color: #1f2937;"


def _section(title: str, hint: str = "", *, first: bool = False, tooltip: str | None = None) -> None:
    """A top-level block header + its rule. One helper so a new section cannot invent a
    fourth heading style."""
    with ui.row().classes("w-full items-baseline gap-2"):
        section_header(title, first=first)
        if hint:
            lbl = ui.label(hint).classes(_HINT_CLS)
            if tooltip:
                lbl.tooltip(tooltip)
    section_rule()


# `C1` IS "no symmetry", and most complexes are C1 (ribosome, proteasome) — the option
# list says so instead of leaving the user to know it. One spelling across this block,
# the creation dialog (`ui.species.prompt`) and the TM job tab's override row.
SYMMETRY_OPTIONS: dict[str, str] = {g.value: ("None (C1)" if g is SymmetryGroup.C1 else g.value) for g in SymmetryGroup}

_ORIGIN_TEXT = {
    SpeciesOrigin.WORKBENCH.value: "workbench (template-driven)",
    SpeciesOrigin.MANUAL.value: "manual (created for hand picking)",
    SpeciesOrigin.IMPORTED.value: "imported",
}


# ── Identity editor (built once, never inside a rev-gated view) ────────────────


def _render_color_swatch(backend, project_path: Path, species_id: str, current: str) -> None:
    """The species' overlay color, click to change — `ui.components.color_swatch` with
    this tab's persistence closure (the creation dialog passes a different one)."""

    def _on_pick(c: str) -> None:
        def _apply(s: ParticleSpecies) -> None:
            s.color = c

        get_project_state_for(project_path).mutate_species(species_id, _apply)
        asyncio.create_task(backend.save_project(project_path, debounce_s=1.0))

    render_color_swatch(current, _on_pick)


def render_identity_editor(backend, project_path: Path, species_id: str) -> None:
    """Swatch · name · diameter · symmetry · notes, plus the provenance line. Each edit =
    `mutate_species` (dirty + rev) and a 1-s debounced save.

    No card and no coloured stripe: the species colour is already carried by the swatch
    and the header pill, and every input reads at the scale of the label beside it
    (Quasar's own ~14 px against 10 px labels was the whole "huge / heterogenous"
    complaint)."""
    state = get_project_state_for(project_path)
    sp = state.get_species(species_id)
    if sp is None:
        return

    def _mutate(fn) -> None:
        get_project_state_for(project_path).mutate_species(species_id, fn)
        asyncio.create_task(backend.save_project(project_path, debounce_s=1.0))

    with ui.column().classes("w-full gap-1 px-1"):
        _section("Species info", "what this particle is", first=True)
        # Row 1 — identity line.
        with ui.row().classes("w-full items-center gap-2 no-wrap"):
            _render_color_swatch(backend, project_path, species_id, sp.color or "#3b82f6")

            name_input = ui.input(value=sp.name, placeholder="species name").props("dense debounce=400")
            name_input.props(f'input-style="{_NAME_INPUT_STYLE}"')
            name_input.classes("cb-field w-56").tooltip(f"Display name (id stays `{species_id}`)")

            def _on_name(e):
                v = (e.value or "").strip()
                if not v:
                    return  # an empty label is not a rename; the input keeps what was typed

                def _apply(s: ParticleSpecies) -> None:
                    s.name = v

                _mutate(_apply)

            name_input.on_value_change(_on_name)

        # Row 2 — the two sized-to-content particle facts.
        with ui.row().classes("w-full items-center gap-6 no-wrap"):
            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.label("Diameter").classes(_LABEL_CLS)
                diam_input = (
                    ui.number(value=sp.diameter_ang, placeholder="e.g. 250", step=10, min=0)
                    .props("dense debounce=400")
                    .classes("cb-field w-24")
                )
                # The unit is a sibling label, not Quasar's in-field suffix: at this width
                # the suffix used to compete with the value and clip it.
                diam_input.tooltip("Ø of the particle in ångström — the default for new pick-candidates jobs.")
                ui.label("Å").classes(_HINT_CLS)

                def _on_diam(e):
                    try:
                        new_val = float(e.value) if e.value not in (None, "") else None
                    except (TypeError, ValueError):
                        return  # mid-typing garbage; the next valid value lands

                    def _apply(s: ParticleSpecies) -> None:
                        s.diameter_ang = new_val

                    _mutate(_apply)

                diam_input.on_value_change(_on_diam)

            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.label("Symmetry").classes(_LABEL_CLS)
                sym_select = (
                    ui.select(options=SYMMETRY_OPTIONS, value=sp.symmetry or SymmetryGroup.C1.value)
                    .props("dense")
                    .props('popup-content-class="cb-select-popup"')
                    .classes("cb-field w-32")
                )
                sym_select.tooltip(
                    "Point-group symmetry of the particle. Most complexes are C1; this is the default "
                    "for new TM jobs, which can still override it."
                )

                def _on_sym(e):
                    new_val = e.value or SymmetryGroup.C1.value

                    def _apply(s: ParticleSpecies) -> None:
                        s.symmetry = new_val

                    _mutate(_apply)

                sym_select.on_value_change(_on_sym)

        # Row 3 — free-form notes.
        with ui.row().classes("w-full items-start gap-2 no-wrap"):
            ui.label("Notes").classes(_LABEL_CLS + " mt-1")
            notes_input = (
                ui.textarea(value=sp.notes or "", placeholder="free-form, optional")
                .props("dense autogrow rows=2 debounce=400")
                .classes("cb-field flex-1")
                .style("min-width: 0;")
            )

            def _on_notes(e):
                v = e.value or ""

                def _apply(s: ParticleSpecies) -> None:
                    s.notes = v

                _mutate(_apply)

            notes_input.on_value_change(_on_notes)

        with ui.row().classes("w-full items-center gap-3"):
            ui.label(
                "Defaults for new TM (symmetry) and pick-candidates (diameter) jobs. Existing jobs aren't auto-updated."
            ).classes(_HINT_CLS)
            ui.space()
            _render_provenance(sp)


def _mask_source(sp: ParticleSpecies, m: TemplateMask) -> str:
    """Where a mask came from. `imported_from` is a `ParticleTemplate` field and does NOT
    exist on `TemplateMask` — a mask's provenance is `derived_from_template_id` (the soft
    link v3 kept for this) plus the creation `method`. Unknown stays "?" rather than being
    filled in with a plausible guess."""
    if m.derived_from_template_id:
        tpl = sp.get_template_by_id(m.derived_from_template_id)
        if tpl is None:
            return "derived (source template no longer registered)"
        return f"derived from {os.path.basename(tpl.template_path)}"
    return m.method or "?"


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
    ] + [f"mask {os.path.basename(m.mask_path)}: {_mask_source(sp, m)}" for m in sp.masks]
    ui.label(f"origin {_ORIGIN_TEXT.get(origin, origin)} · created {created} · catalog {catalog}").classes(
        _MONO_CLS
    ).tooltip("\n".join(sources) if sources else "no templates or masks registered")


# ── Bindings block (in-memory only — safe to re-render on every tick) ─────────


class _BindingsView(FingerprintedView):
    """The template and the mask this species is currently bound to, with full paths, a
    copy affordance and a route to where the selection is *changed*.

    Both rows render whether or not anything is bound: "no template" is a legitimate
    state (de-novo picking) and has to look different from "we couldn't tell you". No
    disk reads here — apix / box / σ come from the MRC header and stay on the Templates
    & masks tab, so this view is cheap enough for the page's 3-s observe.
    """

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
        return (
            sp.selected_template_id,
            sp.selected_mask_id,
            len(sp.templates),
            len(sp.masks),
            t.template_path if t else "",
            m.mask_path if m else "",
        )

    def _open_templates_tab(self):
        """`species_select_tab` is the page's cross-tab hook. Absent (no page hosting
        this tab) renders the rows without a link rather than a dead one."""
        fn = (self._tab.ctx.callbacks or {}).get("species_select_tab")
        return (lambda: fn("templates")) if fn else None

    def render(self) -> None:
        sp = self._species()
        if sp is None:
            return
        on_open = self._open_templates_tab()
        tpl, mask = sp.get_selected_template(), sp.get_selected_mask()
        with ui.column().classes("w-full gap-1 px-1"):
            _section(
                "Template matching files",
                "what a new TM job defaults to",
                tooltip=(
                    "Snapshot at job creation — changing the selection here does not reach jobs that "
                    "already exist. Selection itself is changed on Templates & masks."
                ),
            )
            with ui.column().classes("w-full gap-0"):
                self._binding_row(
                    "Template",
                    tpl.template_path if tpl else "",
                    on_open,
                    chip=(
                        (lambda t=tpl: render_polarity_chip(t.polarity, tooltip="Density polarity at registration"))
                        if tpl
                        else None
                    ),
                )
                self._binding_row(
                    "Mask",
                    mask.mask_path if mask else "",
                    on_open,
                    chip=(
                        (lambda m=mask: render_method_chip(m.method, tooltip="How this mask was made"))
                        if mask
                        else None
                    ),
                )
            n_t, n_m = len(sp.templates), len(sp.masks)
            counts = f"{n_t} template{'s' if n_t != 1 else ''} · {n_m} mask{'s' if n_m != 1 else ''} registered"
            if on_open is not None:
                ui.label(counts).classes(_HINT_CLS + " cursor-pointer underline decoration-dotted").on(
                    "click", lambda _e: on_open()
                ).tooltip("Open Templates & masks")
            else:
                ui.label(counts).classes(_HINT_CLS)

    @staticmethod
    def _binding_row(label: str, path: str, on_open, *, chip=None) -> None:
        """One file per row. Mask filenames are long enough to push everything else off a
        shared row, which is why they get their own and why the name is drawn at 10 px
        with the directory left in the hover (`render_path_link`)."""
        with ui.row().classes("w-full items-center gap-2 no-wrap").style("min-height: 20px;"):
            ui.label(label).classes(_LABEL_CLS).style("width: 56px; flex-shrink: 0;")
            if chip is not None:
                chip()
            render_path_link(
                path,
                on_open=on_open,
                open_tooltip="Open Templates & masks — selection is changed there",
                empty_text=f"no {label.lower()} selected",
            )


# ── Extraction geometry (built once — it holds inputs) ────────────────────────

# (ExtractionParams attr, label, min, step) — the min/step match `geometry_inputs()` in
# ui/particles/list_actions.py, so the panel and the first-extract modal ask for the same
# thing. The tooltips come verbatim from the job model, so the panel and the job tab
# explain box/bin/crop the same way (services/jobs/subtomo_extraction.py).
_GEOMETRY_FIELDS = (("box_size", "Box", 16, 2), ("binning", "Binning", 0.1, 0.5), ("crop_size", "Crop", 16, 2))


class ExtractionGeometryPanel:
    """`species.extraction_params` — box / binning / crop for hand-picked lists.

    NOT a `FingerprintedView`: it owns three inputs, and every commit bumps the registry
    rev, so a signature-gated rebuild would destroy the field the user just typed into
    (the same reason the identity editor is built once). Only the effective-value line
    is refreshed, and only when its text actually moved.

    Empty is a real state. Unset renders empty, never `384 / 1.0 / 224` dressed up as a
    choice the user made (de-novo D-3: a box size is not ours to guess).
    """

    def __init__(self, tab: OverviewTab) -> None:
        self._tab = tab
        self._inputs: dict[str, ui.number] = {}
        self._effective: ui.label | None = None

    def build(self) -> None:
        sp = get_project_state_for(self._tab.ctx.project_path).get_species(self._tab.ctx.species_id)
        ep = getattr(sp, "extraction_params", None) if sp is not None else None
        descriptions = _geometry_descriptions()
        with ui.column().classes("w-full gap-1 px-1"):
            _section(
                "Extraction geometry",
                "this project",
                tooltip=(
                    "One geometry per species per project. A species extracted from tomogram sets at "
                    "different binnings needs one per set — see docs/roadmaps/picking_ui/07."
                ),
            )
            # Which job this actually feeds. Without it the panel reads as three orphan
            # numbers — it is the box the SUBTOMO EXTRACTION job cuts, and the answer the
            # per-list extract modal would otherwise stop and ask for.
            ui.label(
                "Feeds the Subtomo extraction job: the box cut around every pick. Set here once and a new "
                "Subtomo extraction job starts with these values; hand-picked lists that have no such job "
                "are extracted with them directly, instead of stopping to ask."
            ).classes(_HINT_CLS)
            with ui.row().classes("w-full items-center gap-3 no-wrap"):
                for attr, label, minimum, step in _GEOMETRY_FIELDS:
                    with ui.row().classes("items-center gap-1 no-wrap"):
                        ui.label(label).classes(_LABEL_CLS)
                        inp = (
                            ui.number(value=getattr(ep, attr, None), min=minimum, step=step)
                            .props("dense debounce=600")
                            .classes("cb-field w-20")
                        )
                        inp.tooltip(descriptions.get(attr, ""))
                        inp.on_value_change(lambda _e: self._commit())
                        self._inputs[attr] = inp
                ui.label("box / crop in binned voxels").classes(_HINT_CLS)
            self._effective = ui.label("").classes(_HINT_CLS)
        self.refresh()

    # ── Write ────────────────────────────────────────────────────────────────

    def _commit(self) -> None:
        """All three or nothing — a partial edit saves nothing and says so, exactly the
        rule `commit_extraction_geometry` already enforces (it is the one writer)."""
        values = {attr: self._inputs[attr].value for attr, *_ in _GEOMETRY_FIELDS}
        if not all(values.values()):
            return  # mid-edit; `commit_extraction_geometry` toasts on an explicit attempt
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

    # ── Effective-value line ─────────────────────────────────────────────────

    def refresh(self) -> None:
        if self._effective is None:
            return
        text = self._effective_text()
        if self._effective.text != text:
            self._effective.set_text(text)

    def _effective_text(self) -> str:
        """`extraction_params_for_species`' precedence, in words: the subtomo job wins,
        then this panel, then nothing (never a guess)."""
        ctx = self._tab.ctx
        state = get_project_state_for(ctx.project_path)
        params = extraction_params_for_species(state, ctx.species_id)
        if params is None:
            return "not set — asked once at the first extraction, never guessed."
        sp = state.get_species(ctx.species_id)
        ep = getattr(sp, "extraction_params", None) if sp is not None else None
        geometry = f"box {params['box_size']} · bin {params['binning']:g} · crop {params['crop_size']}"
        if ep is not None and (ep.box_size, ep.binning, ep.crop_size) == (
            params["box_size"],
            params["binning"],
            params["crop_size"],
        ):
            return f"In use: these values ({geometry})."
        return (
            f"In use: from the subtomoExtraction job ({geometry}). "
            "Values here apply to hand-picked lists that have no such job."
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
        """One muted sentence of plain numbers.

        What used to be here and is now elsewhere: the counts moved to the tab strip;
        the `gate` chip went (the concept stays — the Picks tab's "Extract all pending"
        still runs its gate-report pre-flight, and per-list state is a column in that
        table); the pick-candidates / subtomo instance-id pills went (the Jobs tab lists
        exactly those jobs with type, status and drift chips — it is the owner); the
        templates / masks chips went (roadmap 03's bindings block supersedes them); and
        the pixel/binning sanity table went to the Tomogram Dashboard, which is where it
        belongs — the chain is a property of (this project's tomograms x this binning),
        not of the particle. Do not bring them back here.
        """
        c = self._tab.computed
        with ui.column().classes("w-full gap-1 px-1"):
            if c is None:
                ui.label("computing…").classes(_HINT_CLS)
                return
            if c.error:
                ui.label(f"Status unavailable — {c.error}").classes("text-[11px] text-red-600").tooltip(
                    "species_overview raised; full traceback in the server log"
                )
                return
            ov = c.overview
            ui.label(
                f"{ov.n_picks} pick{'s' if ov.n_picks != 1 else ''} on "
                f"{ov.n_tomos_with_picks} tomogram{'s' if ov.n_tomos_with_picks != 1 else ''} · "
                f"{ov.n_kept} kept · "
                f"{ov.n_extracted_lists} list{'s' if ov.n_extracted_lists != 1 else ''} extracted"
            ).classes(_HINT_CLS).tooltip(
                "kept = after committed keep/drop curation. Per-list state, and the extract actions, "
                "are on the Picks tab."
            )


class OverviewTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self._last_key: Any = None
        self._last_at: float = 0.0
        self._flight = SingleFlight()
        self._bindings: _BindingsView | None = None
        self._geometry: ExtractionGeometryPanel | None = None
        self._status: _StatusView | None = None

    # ── Tab protocol ──────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        ctx = self.ctx
        with container, ui.column().classes("w-full gap-3 p-2"):
            render_identity_editor(ctx.backend, ctx.project_path, ctx.species_id)
            bindings_slot = ui.element("div").classes("w-full")
            self._bindings = _BindingsView(bindings_slot, self)
            self._geometry = ExtractionGeometryPanel(self)
            self._geometry.build()
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
        self._bindings.refresh()
        self._status.refresh()

    def refresh(self) -> None:
        """Page tick / on show (only while this tab is visible): recompute when the
        in-memory key moved, else at most every `_DISK_REFRESH_S`; repaint through the
        signature gate either way."""
        if self._bindings is not None:
            self._bindings.refresh()  # in-memory + signature-gated: free on a quiet tick
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
