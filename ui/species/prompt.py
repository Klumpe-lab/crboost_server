"""Species creation prompt + the one create path (roadmap 10 S1).

`prompt_species_name` is the small name dialog every "+" shares; `create_species` runs it
and registers the species (`ProjectState.add_species` marks dirty + bumps the registry
rev, then a forced save). Callers only differ in `origin` (`SpeciesOrigin`) and in what
they do afterwards (select it on the Species page, rebuild the roster, refresh the Journey).
"""

from __future__ import annotations

from pathlib import Path

from nicegui import ui

from services.models_base import SpeciesOrigin
from services.project_state import ParticleSpecies, get_project_state_for


async def prompt_species_name() -> str | None:
    """Modal name prompt; returns the stripped name or None on cancel."""
    with ui.dialog() as dialog, ui.card().classes("w-80 p-4 gap-3"):
        ui.label("New Species").classes("text-base font-bold text-gray-800")
        name_input = (
            ui.input(label="Species name", placeholder="e.g. Ribosome, 26S Proteasome")
            .props("outlined dense autofocus")
            .classes("w-full")
        )

        def _confirm():
            v = name_input.value.strip()
            if v:
                dialog.submit(v)

        def _cancel():
            dialog.submit(None)

        name_input.on("keydown.enter", _confirm)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=_cancel).props("flat dense no-caps")
            ui.button("Create", on_click=_confirm).props("dense no-caps unelevated color=primary")

    return await dialog


async def create_species(backend, project_path: Path, *, origin: str) -> ParticleSpecies | None:
    """Prompt for a name and register a new species with the given `origin`
    (`SpeciesOrigin` value). Returns the species, or None when the prompt was
    cancelled. Persists with a forced save (explicit path — safe from any surface).
    No template directory is created here; the Templates & masks tab makes it on
    first mount."""
    name = await prompt_species_name()
    if not name:
        return None
    species = get_project_state_for(project_path).add_species(name, origin=SpeciesOrigin(origin).value)
    await backend.save_project(project_path, force=True)
    return species
