"""The species a particle job is added for: a list of the project's species, one click adds the job for that one.

Nothing is chosen until the user clicks: a pre-selected first species would be a default the user never picked. Each
row says what the job will inherit from the species (Ø, symmetry; for template matching, whether it has a template)
and how many jobs of this type it already has, so a second one is a decision, not an accident. "New species…" makes
one in the species prompt and adds the job for it.
"""

from __future__ import annotations

from pathlib import Path

from nicegui import ui

from services.models_base import InstanceId, JobType
from services.project_state import ParticleSpecies, get_project_state_for
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.job_plugins._field_styles import HELPER_STYLE, PANEL_TITLE_STYLE, voice
from ui.styles import MONO
from ui.ui_state import get_job_display_name

_ROW_H = 28
_SWATCH = 9
_INK = "var(--cb-ink-2, #1e293b)"
_INK_SOFT = "var(--cb-ink-3, #5b7089)"
_INK_FAINT = "var(--cb-ink-4, #94a3b8)"
_WARN = "var(--cb-warn-ink, #b45309)"
_LINE = "var(--cb-line, #e4eaf1)"
_FACT = f"{MONO} font-size: 9px; white-space: nowrap; flex-shrink: 0;"
_TAG = (
    voice(
        "tag", font="'IBM Plex Mono', monospace", size="8px", weight=500, ink=_INK_SOFT, case="uppercase", track=".06em"
    )
    + " background: var(--cb-tag, #f1f5f9); "
    "border: 1px solid var(--cb-tag-line, #dbe3ec); border-radius: var(--cb-tag-radius, 2px); "
    "padding: 0 4px; line-height: 13px; white-space: nowrap; flex-shrink: 0;"
)


def _jobs_for(state, job_type: JobType, species_id: str) -> int:
    return sum(
        1
        for iid, jm in state.jobs.items()
        if InstanceId.matches(iid, job_type) and getattr(jm, "species_id", None) == species_id
    )


def _species_row(species: ParticleSpecies, job_type: JobType, n_jobs: int, on_pick) -> None:
    """One species: its colour and name, then what the job takes from it; a click (or Enter) picks it."""
    job_name = get_job_display_name(job_type)
    row = (
        ui.element("div")
        .classes("cb-sp-pick")
        .props('tabindex="0"')
        .style(
            f"display: flex; align-items: center; gap: 8px; height: {_ROW_H}px; padding: 0 8px; cursor: pointer; "
            f"border-bottom: 1px solid {_LINE}; outline: none;"
        )
        .on("click", lambda _e, sid=species.id: on_pick(sid))
        .on("keydown.enter", lambda _e, sid=species.id: on_pick(sid))
    )
    with row:
        ui.element("span").style(
            f"width: {_SWATCH}px; height: {_SWATCH}px; flex-shrink: 0; background: {species.color}; "
            "border-radius: var(--cb-r-sm, 2px);"
        )
        ui.label(species.name).style(
            f"{MONO} font-size: 11px; color: {_INK}; flex: 1 1 auto; min-width: 0; "
            "white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
        ).props(f'title="{species.name} ({species.id})"')
        if n_jobs:
            tag = ui.label(f"{n_jobs} already").style(_TAG)
            tag.props(f'title="This project has {n_jobs} {job_name} job(s) for {species.name}; this adds another."')
        if job_type == JobType.TEMPLATE_MATCH_PYTOM:
            template = species.get_selected_template()
            if template is None:
                ui.label("no template").style(f"{_FACT} color: {_WARN};").props(
                    'title="The species has no template selected: the job will say so until one is (Particles '
                    'registry, Templates & masks)."'
                )
            else:
                ui.label("template").style(f"{_FACT} color: {_INK_SOFT};").props(f'title="{template.name}"')
        if species.diameter_ang:
            ui.label(f"Ø {species.diameter_ang:g} Å").style(f"{_FACT} color: {_INK_SOFT};")
        else:
            ui.label("Ø ?").style(f"{_FACT} color: {_WARN};").props(
                'title="No diameter on the species: set it on its Overview in the Particles registry."'
            )
        ui.label(species.symmetry or "?").style(f"{_FACT} color: {_INK_FAINT}; min-width: 3ch; text-align: right;")


async def choose_species(backend, project_path: Path, job_type: JobType) -> str | None:
    """The id of the species a `job_type` job is added for, or None when the dialog was closed."""
    state = get_project_state_for(project_path)
    job_name = get_job_display_name(job_type)
    registry = list(state.species_registry)

    with (
        dialog_host(),
        ui.dialog() as dialog,
        ui.card().style("width: 420px; max-width: 94vw; padding: 14px 16px 12px; gap: 10px;"),
    ):

        def _pick(species_id: str) -> None:
            dialog.submit(species_id)

        async def _new() -> None:
            from ui.species.prompt import create_species

            species = await create_species(backend, project_path, origin="manual")
            if species is None:
                return
            ui.notify(f"Created species '{species.name}'", type="positive")
            dialog.submit(species.id)

        with ui.column().classes("w-full").style("gap: 3px;"):
            ui.label(f"Add {job_name}").style(PANEL_TITLE_STYLE)
            ui.label(
                "For which species? A click adds the job; it takes the species' values as its own."
                if registry
                else "This project has no species yet. Make one, and the job is added for it."
            ).style(HELPER_STYLE)
        if registry:
            with ui.element("div").style(
                f"width: 100%; border-top: 1px solid {_LINE}; max-height: 50vh; overflow-y: auto;"
            ):
                for species in registry:
                    _species_row(species, job_type, _jobs_for(state, job_type, species.id), _pick)
        with ui.row().classes("w-full items-center no-wrap").style("gap: 6px; margin-top: 2px;"):
            house_button("New species…", _new, kind="default" if registry else "accent")
            ui.space()
            house_button("Cancel", lambda: dialog.submit(None))

    return await dialog
