"""The Species page (roadmap 10) — `[rail 200px | detail]`.

Detail = header row (active species' pill + segmented tabs) + one container per
(species, tab), built lazily on first selection and cached; a switch only flips
visibility. Tabs: Overview (S3: identity editor, status, sanity, delete) · Templates &
masks (the mounted workbench, S2) · Picks (11-S2) · Curation (11-S4) · Jobs (S4). The page
owns the 3-s registry observe (species created / deleted elsewhere — roster "+", another
browser tab, the Overview's delete) and the `on_workbench_active` / `workbench_select_species`
hooks the workspace registers (`workspace_page._switch_to` / `_open_species`); the
internal mode string stays "workbench" (roster `set_active_mode` compares it).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from nicegui import ui

from services.models_base import SpeciesOrigin
from services.particles.species_jobs import jobs_for_species
from services.project_state import get_project_state_for
from ui.components.buttons import house_button
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.components.segmented import Segmented, render_segmented
from ui.components.species_pill import render_species_pill
from ui.components.svg_icon import load_icon_svg
from ui.dashboard.css import ensure_assets_loaded
from ui.species.catalog import import_from_catalog
from ui.species.curation_tab import CurationTab
from ui.species.jobs_tab import JobsTab
from ui.species.overview_tab import OverviewTab
from ui.species.picks_tab import PicksTab
from ui.species.prompt import create_species
from ui.species.rail import SpeciesRail
from ui.species.tab import SpeciesTab, TabContext
from ui.species.templates_tab import TemplatesTab
from ui.ui_state import get_ui_state_manager

logger = logging.getLogger(__name__)

TABS: tuple[tuple[str, str], ...] = (
    ("overview", "Overview"),
    ("templates", "Templates & masks"),
    ("picks", "Picks"),
    ("curation", "Curation"),
    ("jobs", "Jobs"),
)
DEFAULT_TAB = "overview"


class _PillView(FingerprintedView):
    """The header's species pill: repaints on select / rename / recolor only."""

    def __init__(self, container: ui.element, page: SpeciesPage) -> None:
        super().__init__(container)
        self._page = page

    def signature(self) -> Any:
        return (self._page.active_species_id, get_project_state_for(self._page.project_path).species_identity())

    def render(self) -> None:
        sid = self._page.active_species_id
        species = get_project_state_for(self._page.project_path).get_species(sid) if sid else None
        if species is not None:
            render_species_pill(species, tooltip=f"id: {species.id}")


class SpeciesPage:
    def __init__(self, backend, project_path: Path, callbacks: dict) -> None:
        self.backend = backend
        self.project_path = project_path
        self.callbacks = callbacks
        self.active_species_id: str | None = None
        self.active_tab: str = DEFAULT_TAB
        # Whether the workspace currently shows this view (`on_workbench_active`); tab
        # refreshes (the Overview's disk-backed recompute) only run while shown — the
        # 3-s observe itself is in-memory and keeps ticking.
        self.visible: bool = False
        # (species_id, tab_key) → (container, tab object); built on first selection, kept.
        self._tabs: dict[tuple[str, str], tuple[ui.element, SpeciesTab]] = {}
        self._refs: dict[str, ui.element] = {}
        self._rail: SpeciesRail | None = None
        self._pill: _PillView | None = None
        self._segmented: Segmented | None = None
        self._flight = SingleFlight()

    # ── Layout ────────────────────────────────────────────────────────────────

    def build(self) -> None:
        ensure_assets_loaded()
        with ui.element("div").style("display: flex; flex-direction: row; width: 100%; height: 100%; min-height: 0;"):
            rail = ui.element("div").classes("cb-srail")
            self._rail = SpeciesRail(
                rail,
                self.project_path,
                active_id=lambda: self.active_species_id,
                on_select=self.select_species,
                on_add=self.add_species,
                on_add_from_catalog=self.add_species_from_catalog,
            )
            with ui.element("div").style(
                "display: flex; flex-direction: column; flex: 1 1 0%; min-width: 0; height: 100%; min-height: 0;"
            ):
                header = ui.element("div").classes("cb-species-header")
                self._refs["header"] = header
                with header:
                    pill_slot = ui.element("div").style("display: flex; align-items: center; min-width: 0;")
                    self._pill = _PillView(pill_slot, self)
                    ui.element("div").style("flex: 1;")
                    self._segmented = render_segmented(TABS, self.active_tab, self.select_tab)
                body = ui.element("div").style(
                    "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; min-height: 0; overflow: hidden;"
                )
                self._refs["body"] = body
                with body:
                    empty = ui.column().classes("w-full h-full items-center justify-center gap-4")
                    self._refs["empty"] = empty
                    with empty:
                        ui.html(load_icon_svg("particle.svg", "#d1d5db", size=48), sanitize=False).style(
                            "width: 48px; height: 48px; display: flex;"
                        )
                        ui.label("No species registered yet").classes("text-sm text-gray-400")
                        house_button("Add first species", self.add_species, kind="accent")

        state = get_project_state_for(self.project_path)
        # Paint the rail BEFORE building any tab. A tab that raises during build kills the
        # rest of this page's render, and with the refresh after the selection that meant
        # one broken tab presented as "the registry is empty" — the species were there all
        # along. The rail reads only `state.species_registry`, so it never needs the
        # selection to have succeeded.
        self._rail.refresh()
        if state.species_registry:
            self.select_species(state.species_registry[0].id)
        else:
            self._apply_visibility()

        # Observe the registry: 3-s poll (in-memory tuples only) + instant on show.
        ui.timer(3.0, self.observe)
        self.callbacks["on_workbench_active"] = self._set_active
        # "open in Species" from a job's Config tab (workspace_page._open_species).
        self.callbacks["workbench_select_species"] = self.select_species
        # Cross-tab links inside this page (the Picks tab's empty state points at
        # Curation / Jobs) — a tab never reaches into the page object itself.
        self.callbacks["species_select_tab"] = self.select_tab

    def _set_active(self, on: bool) -> None:
        self.visible = on
        if on:
            self.observe()

    # ── Selection ─────────────────────────────────────────────────────────────

    def select_species(self, species_id: str) -> None:
        self.active_species_id = species_id
        self._ensure_tab(species_id, self.active_tab)
        self._apply_visibility()
        self._refresh_chrome()
        self._refresh_visible_tab()
        self._push_tab_badges()

    def select_tab(self, key: str) -> None:
        self.active_tab = key
        if self._segmented is not None:
            self._segmented.set_active(key)
        if self.active_species_id is not None:
            self._ensure_tab(self.active_species_id, key)
        self._apply_visibility()
        self._refresh_visible_tab()

    def _ensure_tab(self, species_id: str, key: str) -> None:
        if (species_id, key) in self._tabs:
            return
        body = self._refs["body"]
        with body:
            container = ui.column().classes("w-full overflow-auto").style("flex: 1 1 0%; min-height: 0;")
        container.set_visibility(False)
        tab = self._make_tab(key, species_id)
        self._tabs[(species_id, key)] = (container, tab)
        tab.build(container)

    def _make_tab(self, key: str, species_id: str) -> SpeciesTab:
        ctx = TabContext(
            backend=self.backend,
            project_path=self.project_path,
            species_id=species_id,
            callbacks=self.callbacks,
            on_species_deleted=self._on_species_deleted,
        )
        match key:
            case "templates":
                return TemplatesTab(ctx)
            case "overview":
                return OverviewTab(ctx)
            case "picks":
                return PicksTab(ctx)
            case "curation":
                return CurationTab(ctx)
            case "jobs":
                return JobsTab(ctx)
        raise KeyError(f"unknown Species tab {key!r}")

    def _apply_visibility(self) -> None:
        has_species = self.active_species_id is not None
        self._refs["empty"].set_visibility(not has_species)
        self._refs["header"].set_visibility(has_species)
        for (sid, key), (container, _tab) in self._tabs.items():
            container.set_visibility(has_species and sid == self.active_species_id and key == self.active_tab)

    def _refresh_chrome(self) -> None:
        if self._rail is not None:
            self._rail.refresh()
        if self._pill is not None:
            self._pill.refresh()

    def _refresh_visible_tab(self) -> None:
        if not self.visible or self.active_species_id is None:
            return
        entry = self._tabs.get((self.active_species_id, self.active_tab))
        if entry is not None:
            entry[1].refresh()

    # ── Registry hooks ────────────────────────────────────────────────────────

    def _drop_species(self, species_id: str) -> None:
        """Delete every cached container of `species_id` (a same-id re-create must
        not reuse a stale workbench)."""
        for key in [k for k in self._tabs if k[0] == species_id]:
            container, _tab = self._tabs.pop(key)
            container.delete()

    def _on_species_deleted(self, species_id: str) -> None:
        """A tab (the workbench's delete) just removed the species — registry and
        files are gone. Drop its containers and select what remains."""
        self._drop_species(species_id)
        self.observe()

    def observe(self) -> None:
        """3-s poll + on-show: reconcile with the live registry. Species created
        elsewhere get selected; ones deleted elsewhere lose their containers and the
        page falls back to the remainder / the empty state; the rail and pill repaint
        only when their signatures moved. In-memory reads only — fine while hidden."""
        ids = [s.id for s in get_project_state_for(self.project_path).species_registry]
        for sid in {k[0] for k in self._tabs if k[0] not in ids}:
            self._drop_species(sid)
        if not ids:
            if self.active_species_id is not None:  # deleted elsewhere → empty state
                self.active_species_id = None
                self._apply_visibility()
        elif self.active_species_id not in ids:  # created elsewhere / active deleted → pick first
            self.select_species(ids[0])
        else:
            self._refresh_visible_tab()
        self._refresh_chrome()
        self._push_tab_badges()

    def _push_tab_badges(self) -> None:
        """Counts on the tab strip for the active species (picking-UI roadmap 04 S1) —
        they used to be chips in the Overview's "kitchen sink" status block. All three
        reads are in-memory, so this rides the 3-s observe with no disk cost;
        `set_badge` is a no-op when the number has not moved."""
        if self._segmented is None:
            return
        state = get_project_state_for(self.project_path)
        sp = state.get_species(self.active_species_id) if self.active_species_id else None
        if sp is None:
            for key in ("templates", "picks", "jobs"):
                self._segmented.set_badge(key, "")
            return
        n_picks = sum(1 for pl in state.pick_lists if pl.species_id == sp.id)
        self._segmented.set_badge("templates", str(len(sp.templates)) if sp.templates else "")
        self._segmented.set_badge("picks", str(n_picks) if n_picks else "")
        n_jobs = len(jobs_for_species(state, sp.id))
        self._segmented.set_badge("jobs", str(n_jobs) if n_jobs else "")

    async def add_species(self) -> None:
        # SingleFlight: the "+" lives in a rail that repaints on registry changes, so a
        # double click could otherwise queue two prompts.
        async with self._flight("add_species") as acquired:
            if not acquired:
                return
            species = await create_species(self.backend, self.project_path, origin=SpeciesOrigin.WORKBENCH)
            if species is not None:
                self.select_species(species.id)

    async def add_species_from_catalog(self) -> None:
        """Rail "From catalog" (roadmap 12): pick a lab-catalog species and instantiate it
        here. Only rendered when `species_catalog_root` is configured; the import itself is
        SingleFlight-guarded inside `ui.species.catalog`."""
        await import_from_catalog(self.backend, self.project_path, on_done=self.select_species)
        self.observe()


def build_species_page(backend, callbacks: dict | None = None) -> None:
    """Build the Species page into the current slot. `callbacks` (the workspace dict)
    receives `on_workbench_active(bool)` and `workbench_select_species(species_id)`."""
    project_path = get_ui_state_manager().project_path
    if not project_path:
        with ui.column().classes("w-full h-full items-center justify-center"):
            ui.label("No project loaded").classes("text-sm text-gray-400")
        return
    SpeciesPage(backend, project_path, callbacks if callbacks is not None else {}).build()
