"""Species rail (roadmap 10 S1) — the left column of the Species page.

One row per registered species (the one species pill + a muted "n tpl · n lists"
meta) and a "+" at the bottom. A `FingerprintedView`: the page's 3-s observe tick and
every select / create / delete call `refresh()`, and the DOM rebuilds only when the
active id, `species_identity()` (id · name · color per species) or the per-species
counts moved — all in-memory reads. Successor of the strip in the deleted
`ui/species_workbench_panel.py` (08-S0's `_StripView`).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from nicegui import ui

from services.project_state import get_project_state_for
from ui.components.reactive import FingerprintedView
from services.particles.catalog import is_enabled as catalog_is_enabled
from ui.components.species_pill import render_species_pill


def _counts(state) -> tuple[tuple[str, int, int], ...]:
    """(species_id, n_templates, n_workbench_lists) per species, registry order."""
    n_lists = Counter(pl.species_id for pl in state.pick_lists)
    return tuple((sp.id, len(sp.templates), n_lists.get(sp.id, 0)) for sp in state.species_registry)


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
    ) -> None:
        super().__init__(container)
        self._project_path = project_path
        self._active_id = active_id
        self._on_select = on_select
        self._on_add = on_add
        self._on_add_from_catalog = on_add_from_catalog

    def signature(self) -> Any:
        state = get_project_state_for(self._project_path)
        return (self._active_id(), state.species_identity(), _counts(state))

    def render(self) -> None:
        state = get_project_state_for(self._project_path)
        active = self._active_id()
        counts = {sid: (n_tpl, n_lists) for sid, n_tpl, n_lists in _counts(state)}
        for species in state.species_registry:
            n_tpl, n_lists = counts.get(species.id, (0, 0))
            row = ui.element("div").classes("cb-srail-row" + (" selected" if species.id == active else ""))
            row.style(f"border-left-color: {species.color if species.id == active else 'transparent'};")
            row.on("click", lambda _e, sid=species.id: self._on_select(sid))
            with row:
                render_species_pill(species)
                ui.label(f"{n_tpl} tpl · {n_lists} list{'s' if n_lists != 1 else ''}").classes("cb-srail-meta").tooltip(
                    "templates registered · pick lists (hand-picked / imported / merged)"
                )
        add = ui.element("div").classes("cb-srail-add").on("click", lambda _e: self._on_add())
        with add:
            ui.icon("add", size="13px")
            ui.label("New species")
        # Second entry point only when a lab catalog is configured (roadmap 12). Not a
        # disabled row when it is off: an affordance for a feature this install does not
        # have is worse than no affordance.
        if self._on_add_from_catalog is not None and catalog_is_enabled():
            from_cat = ui.element("div").classes("cb-srail-add").on("click", lambda _e: self._on_add_from_catalog())
            with from_cat:
                ui.icon("library_books", size="13px")
                ui.label("From catalog")
