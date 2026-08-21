"""Species creation prompt + the one create path (roadmap 10 S1, widened by
picking-UI roadmap 01 S2).

`prompt_species_draft` is the dialog every "+" shares; `create_species` applies the
draft (`ProjectState.add_species` marks dirty + bumps the registry rev, then the
optional template / mask registrations, then one forced save). Callers only differ in
`origin` (`SpeciesOrigin`) and in what they do afterwards (select it on the Species
page, rebuild the roster, refresh the Journey).

The dialog used to be a single name field, which meant a species was never usable
straight after creation: the user had to go back to the Species page and bind a
template and a mask before any TM job could run. Binding them here is optional —
a de-novo species legitimately has neither, and nothing is invented for it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from nicegui import ui

from services import species_admin
from services.jobs._base import SymmetryGroup
from services.models_base import SpeciesOrigin, species_palette_color
from services.project_state import ParticleSpecies, TemplateMask, get_project_state_for, slugify
from ui.components.buttons import house_button
from ui.components.color_swatch import render_color_swatch
from ui.species.overview_tab import SYMMETRY_OPTIONS
from ui.local_file_picker import local_file_picker
from ui.template_import_dialog import open_template_import_dialog

logger = logging.getLogger(__name__)

_VOLUME_GLOB = "*.mrc,*.map,*.rec,*.ccp4"

_BLOCK_CLS = "text-[10px] font-bold text-gray-500 uppercase tracking-wider"
_LABEL_CLS = "text-xs text-gray-600"
_HINT_CLS = "text-[10px] text-gray-400"


@dataclass(frozen=True, slots=True)
class SpeciesDraft:
    """What the creation dialog collected. `color` empty = let `add_species` pick the
    deterministic palette slot for the generated id; `template_path` / `mask_path` empty
    = nothing to bind (the de-novo path)."""

    name: str
    color: str
    diameter_ang: float | None
    symmetry: str
    notes: str
    template_path: str
    mask_path: str


def _path_row(label: str, hint: str, on_pick, state: dict, key: str) -> None:
    """Read-only path field + "choose…" + clear "×". The field is read-only because the
    picker is the only honest way to name a file that must exist."""
    with ui.row().classes("w-full items-center gap-2 no-wrap"):
        ui.label(label).classes(_LABEL_CLS).style("width: 64px; flex-shrink: 0;")
        field = (
            ui.input(placeholder="none — add later in the Particles registry")
            .props("dense outlined readonly")
            .classes("flex-1")
            .style("min-width: 0;")
        )
        field.tooltip(hint)

        def _set(path: str) -> None:
            state[key] = path
            field.value = path

        async def _choose() -> None:
            picked = await on_pick()
            if picked:
                _set(picked)

        ui.button("choose…", on_click=_choose).props("flat dense no-caps size=sm color=primary")
        ui.button(icon="close", on_click=lambda: _set("")).props("flat dense round size=sm").tooltip("clear")


async def _pick_volume() -> str | None:
    """One MRC-family file, or None when the picker was cancelled / the pick is not a
    volume. A wrong suffix is reported, never silently accepted."""
    result = await local_file_picker("/", upper_limit=None, mode="file", glob=_VOLUME_GLOB)
    if not result or not result[0]:
        return None
    picked = result[0]
    if Path(picked).suffix.lower() not in (".mrc", ".map", ".rec", ".ccp4"):
        ui.notify("Must be an MRC family file (.mrc / .map / .rec / .ccp4)", type="warning")
        return None
    return picked


async def prompt_species_draft() -> SpeciesDraft | None:
    """Modal creation dialog; returns the draft or None on cancel."""
    draft: dict = {
        "color": "",  # "" = auto (deterministic palette slot for the generated id)
        "template_path": "",
        "mask_path": "",
    }

    with ui.dialog() as dialog, ui.card().classes("w-[30rem] p-4 gap-3"):
        ui.label("New species").classes("text-sm font-semibold text-gray-800")

        # ── Identity ──────────────────────────────────────────────────────────
        ui.label("Identity").classes(_BLOCK_CLS)
        with ui.row().classes("w-full items-center gap-2 no-wrap"):
            swatch = render_color_swatch(
                species_palette_color(""), lambda c: draft.__setitem__("color", c), tooltip="Overlay color"
            )
            name_input = (
                ui.input(placeholder="e.g. Ribosome, 26S Proteasome")
                .props("dense outlined autofocus")
                .classes("flex-1")
                .style("min-width: 0;")
            )
            name_input.tooltip("Display name. The species id is slugified from it.")

        def _on_name(e) -> None:
            # Until the user picks explicitly, the swatch previews what `add_species`
            # would derive from the id — so the dot never lies about the colour.
            if not draft["color"]:
                swatch.set_color(species_palette_color(slugify((e.value or "").strip())))

        name_input.on_value_change(_on_name)

        with ui.row().classes("w-full items-center gap-4 no-wrap"):
            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.label("Diameter").classes(_LABEL_CLS)
                diam_input = ui.number(placeholder="e.g. 250", step=10, min=0).props("dense outlined").classes("w-24")
                diam_input.tooltip("Ø of the particle in ångström — the default for new pick-candidates jobs.")
                ui.label("Å").classes(_HINT_CLS)
            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.label("Symmetry").classes(_LABEL_CLS)
                sym_select = (
                    ui.select(options=SYMMETRY_OPTIONS, value=SymmetryGroup.C1.value)
                    .props("dense outlined")
                    .classes("w-32")
                )
                sym_select.tooltip(
                    "Point-group symmetry of the particle. Most complexes are C1; this is the default "
                    "for new TM jobs, which can still override it."
                )

        notes_input = (
            ui.textarea(placeholder="notes (free-form, optional)")
            .props("dense outlined autogrow rows=2")
            .classes("w-full")
        )

        ui.element("div").style("height: 1px; background: #eef2f6; margin: 2px 0;")

        # ── Templates & masks (optional) ──────────────────────────────────────
        ui.label("Templates & masks — optional").classes(_BLOCK_CLS)
        _path_row(
            "Template",
            "A reference volume for template matching. Its header is inspected and confirmed after you create.",
            _pick_volume,
            draft,
            "template_path",
        )
        _path_row(
            "Mask",
            "A mask volume registered alongside the template. Registered as-is (not copied into the project).",
            _pick_volume,
            draft,
            "mask_path",
        )
        ui.label(
            "Leave empty and add them later on the Species page. The template's header is inspected "
            "and confirmed after you create."
        ).classes(_HINT_CLS)

        # ── Actions ───────────────────────────────────────────────────────────
        def _confirm() -> None:
            name = (name_input.value or "").strip()
            if not name:
                ui.notify("A species needs a name", type="warning")
                return
            try:
                diameter = float(diam_input.value) if diam_input.value not in (None, "") else None
            except (TypeError, ValueError):
                diameter = None  # the field is numeric; a mid-typing value means "not stated"
            dialog.submit(
                SpeciesDraft(
                    name=name,
                    color=draft["color"],
                    diameter_ang=diameter,
                    symmetry=sym_select.value or SymmetryGroup.C1.value,
                    notes=notes_input.value or "",
                    template_path=draft["template_path"],
                    mask_path=draft["mask_path"],
                )
            )

        name_input.on("keydown.enter", _confirm)

        with ui.row().classes("w-full justify-end gap-2"):
            house_button("Cancel", lambda: dialog.submit(None))
            house_button("Create", _confirm, kind="accent")

    return await dialog


async def _bind_template(project_path: Path, species: ParticleSpecies, path: str) -> None:
    """Run the import inspector (polarity / apix / lowpass confirm + copy into the
    project) and register the result. Reported on failure, never silent: a species with
    no template is legitimate, one that silently lost the template the user picked is not."""
    tpl = await open_template_import_dialog(str(project_path), species, initial_path=path)
    if tpl is None:
        ui.notify(f"Template not registered — import cancelled ({Path(path).name})", type="warning")
        return
    res = species_admin.register_template(
        get_project_state_for(project_path),
        species.id,
        tpl.template_path,
        polarity=tpl.polarity,
        source=tpl.source or "imported",
        lowpass=tpl.lowpass_resolution_ang,
        imported_from=tpl.imported_from,
        notes=tpl.notes,
    )
    if not res["success"]:
        ui.notify(res["error"], type="negative")


async def create_species(backend, project_path: Path, *, origin: str) -> ParticleSpecies | None:
    """Prompt for a draft and register a new species with the given `origin`
    (`SpeciesOrigin` value). Returns the species, or None when the prompt was
    cancelled. Persists with ONE forced save at the end (explicit path — safe from any
    surface), after the optional template / mask registrations. No template directory
    is created here; the Templates & masks tab makes it on first mount."""
    draft = await prompt_species_draft()
    if draft is None:
        return None
    state = get_project_state_for(project_path)
    species = state.add_species(draft.name, origin=SpeciesOrigin(origin).value, color=draft.color)

    def _apply(sp: ParticleSpecies) -> None:
        sp.diameter_ang = draft.diameter_ang
        sp.symmetry = draft.symmetry
        sp.notes = draft.notes

    state.mutate_species(species.id, _apply)

    if draft.template_path:
        await _bind_template(project_path, species, draft.template_path)
    if draft.mask_path:
        res = species_admin.register_mask(state, species.id, TemplateMask(mask_path=draft.mask_path, method="imported"))
        if not res["success"]:
            ui.notify(res["error"], type="negative")

    await backend.save_project(project_path, force=True)
    return species
