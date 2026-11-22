"""Species rail — the left column of the Species page.

One row per registered species: the species mark, then "n picks" (every list of the species
on every tomogram, the count the Species tab's status line gives) and, for a species with no
template, "no template" in amber. A "+" at the bottom. A `FingerprintedView`: the page's 3-s
observe tick and every select / create / delete call `refresh()`, and the DOM rebuilds only
when the active id, `species_identity()` (id · name · color per species), a species'
templates or its pick count moved — all in-memory reads. The count comes from the last
overview computed for the species (`latest_overview`); the page computes the missing and
stale ones off the event loop.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.particles.catalog import is_enabled as catalog_is_enabled
from services.particles.species_overview import latest_overview
from services.particles.species_progress import PICKS, latest_progress
from services.project_state import get_project_state_for
from ui.components.reactive import FingerprintedView
from ui.components.species_pill import render_species_mark
from ui.styles import MONO, SANS

_SOFT = "#5b7089"
_FAINT = "#94a3b8"
_WARN_INK = "#8a4b06"


def _counts(project_path: Path, state) -> tuple[tuple[str, bool, int | None, int, tuple[str, ...]], ...]:
    """(species_id, has_template, n_picks, n_tomograms_with_picks, what its pick jobs made) per species, registry
    order; n_picks None before the first overview. The overview counts the curated lists; a species picked from a
    segmentation has its counts on its pick jobs (`latest_progress`)."""
    out = []
    for sp in state.species_registry:
        ov = latest_overview(project_path, sp.id)
        progress = latest_progress(project_path, sp.id)
        made = tuple(s.fact for s in progress.stage(PICKS) if s.fact) if progress else ()
        out.append((sp.id, bool(sp.templates), ov.n_picks if ov else None, ov.n_tomos_with_picks if ov else 0, made))
    return tuple(out)


class SpeciesRail(FingerprintedView):
    def __init__(
        self,
        container: ui.element,
        project_path: Path,
        *,
        active_id: Callable[[], str | None],
        on_select: Callable[[str], None],
        on_add: Callable[[], Any],
        on_add_from_catalog: Callable[[], Any] | None = None,
        count_errors: Callable[[], dict[str, str]] = dict,
    ) -> None:
        super().__init__(container)
        self._project_path = project_path
        self._active_id = active_id
        self._on_select = on_select
        self._on_add = on_add
        self._on_add_from_catalog = on_add_from_catalog
        self._count_errors = count_errors

    def signature(self) -> Any:
        state = get_project_state_for(self._project_path)
        errors = tuple(sorted(self._count_errors().items()))
        return (self._active_id(), state.species_identity(), _counts(self._project_path, state), errors)

    def render(self) -> None:
        state = get_project_state_for(self._project_path)
        active = self._active_id()
        counts = {sid: rest for sid, *rest in _counts(self._project_path, state)}
        errors = self._count_errors()
        for species in state.species_registry:
            has_tpl, n_picks, n_tomos, made = counts.get(species.id, (True, None, 0, ()))
            row = ui.element("div").classes("cb-srail-row" + (" selected" if species.id == active else ""))
            row.style(
                f"border-left-color: {'#4a8fd4' if species.id == active else 'transparent'}; border-bottom: none;"
            )
            row.on("click", lambda _e, sid=species.id: self._on_select(sid))
            with row:
                render_species_mark(species, named=True).classes("cb-species-mark")
                ui.element("div").style("flex: 1 1 auto;")
                if not has_tpl:
                    ui.label("no template").style(
                        f"{SANS} font-size: 9px; color: {_WARN_INK}; white-space: nowrap; flex-shrink: 0;"
                    ).tooltip("No template registered: template matching has nothing to search with")
                if species.id in errors:
                    meta, tip = "?", f"Picks could not be counted: {errors[species.id]}"
                elif n_picks is None:
                    meta, tip = "…", "Counting picks"
                elif n_picks:
                    meta = f"{n_picks:,} pick{'s' if n_picks != 1 else ''}"
                    tip = f"{n_picks:,} picks on {n_tomos} tomogram{'s' if n_tomos != 1 else ''}, every list together"
                elif made:
                    meta = f"{len(made)} pick job{'s' if len(made) != 1 else ''}"
                    tip = "Picked by " + "; ".join(made) + ". The Species tab's Status has each job."
                else:
                    meta, tip = "no picks", "Nothing picked yet"
                ui.label(meta).style(
                    f"{MONO} font-size: 8.5px; color: {_SOFT if n_picks or made else _FAINT}; white-space: nowrap; "
                    "flex-shrink: 0;"
                ).tooltip(tip)
        add = (
            ui.element("div")
            .classes("cb-srail-add")
            .style("border-bottom: none;")
            .on("click", lambda _e: self._on_add())
        )
        with add:
            ui.icon("add", size="13px")
            ui.label("New species")
        # Second entry point only when a lab catalog is configured. Not a disabled
        # row when it is off: an affordance for a missing feature is worse than none.
        if self._on_add_from_catalog is not None and catalog_is_enabled():
            from_cat = (
                ui.element("div")
                .classes("cb-srail-add")
                .style("border-bottom: none;")
                .on("click", lambda _e: self._on_add_from_catalog())
            )
            with from_cat:
                ui.icon("library_books", size="13px")
                ui.label("From catalog")
