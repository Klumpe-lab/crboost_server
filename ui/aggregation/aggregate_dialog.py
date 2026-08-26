"""The coordinate half of the "Aggregate" dialog — unite one species' picks at any
granularity (roadmap `picking_ui/12-S4/S5/S6`).

Opened from the roster's PARTICLES-header icon. **Two grades, two modes of one surface**
(`docs/particle-data-flow.md` §3), switched by the segmented control in the header:

  COORDINATE grade   picks not yet extracted — manual lists, imported `.coords`, a
                     candidate-extract `candidates.star`. Owned here.
  EXTRACTED grade    particles that already have pixels. Owned by
                     `merge_card.open_aggregation_merge_dialog`, which has done this
                     correctly for a while and is not worth reimplementing.

Why the coordinate half is a separate MODULE rather than a panel inside `merge_card`: that
dialog is built around `AggregationSource`, one entry per optimisation set, and its whole
selection model is optset-shaped. A coordinate source is a LIST — several per tomogram,
most of them with no optset anywhere. Bolting a second identity model into the same tree
would make both harder to read than either is now.

Separate modules, ONE surface: the header switch, the palette, the project→tomogram→list
tree grammar and the right-flush numeric column are deliberately shared with `merge_card`
so switching grade does not feel like switching application. Keep them in step — the first
version of this dialog rendered bare white rows and read as a different program.

**One species per aggregate, always** (roadmap 12, D2). The species picker is the first
control and everything below re-derives from it. This is what removes the geometry
ambiguity — whose box, whose Ø — that made this feature hard to scope.

The terminal action follows the payload GRADE, never a user choice (roadmap 12, D1): a
coordinate union is what a Pick-candidates job WOULD have produced, so the offer is to
skip that job and extract. ② is a peak-finder over score volumes; handing it coordinates
would leave it nothing to do.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from nicegui import run, ui

from backend import get_backend
from services.array_tasks import ts_position_sort_key, ts_pretty_name
from services.particles.coord_merge import CoordSource, clash_report, merge_coordinate_sources, plan_merge
from services.particles.list_ref import fs_slug
from services.models_base import PickListType, PickSourceKind
from services.project_state import MERGED_DIR_NAME, PickList, get_project_state_for
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.components.segmented import render_segmented
from ui.dashboard.css import ensure_assets_loaded
from ui.projects_overview import avatar_color

log = logging.getLogger(__name__)

# Same palette as ui/aggregation/merge_card.py — the two grades are two modes of ONE
# surface, so they must not read as two applications. Steelblue is the single accent,
# reserved for the curated/original highlight; everything else stays neutral slate.
STEEL = "#4682b4"
SLATE = "#475569"
SLATE_MUTED = "#94a3b8"
AMBER = "#b45309"

# The two payload grades (docs/particle-data-flow.md §3). The switch between them is a
# real control in both dialogs' headers, not a text link: they are the two modes of
# "aggregate this species", and which one you want is the first thing you decide.
GRADE_COORDS = "coords"
GRADE_EXTRACTED = "extracted"
GRADE_TABS = ((GRADE_COORDS, "Picks · coordinates"), (GRADE_EXTRACTED, "Extracted · subtomograms"))

# Lists whose picks a human has vetted. `curated_only` narrows the tree to these, the
# coordinate-grade counterpart of merge_card's "Show curated only" switch: there, curation
# is a filtered star beside an original; here it is what KIND of list this is.
_CURATED_TYPES = frozenset(
    {PickListType.MANUAL.value, PickListType.FILTERED.value, PickListType.MERGED.value, PickListType.IMPORTED.value}
)

# Per-list-type glyph + tooltip. Fixed by type (colour varies per list) — the same
# contract PickListType's own docstring states.
_TYPE_GLYPH = {
    PickListType.AUTO.value: ("blur_on", "machine picks — a template-matching candidate set"),
    PickListType.FILTERED.value: ("filter_alt", "curated: a CC / top-N subset of a parent list"),
    PickListType.MANUAL.value: ("back_hand", "curated: placed by hand in ArtiaX"),
    PickListType.IMPORTED.value: ("file_download", "curated: supplied as .coords / .star"),
    PickListType.MERGED.value: ("merge_type", "curated: two or more lists already combined"),
}

# Fallback clash radius when the species states no diameter. NOT a guessed particle size:
# the field is disabled and labelled "no Ø on the species" so the number on screen is
# visibly ours, and the species page is where it gets fixed.
_NO_DIAMETER_RADIUS = 0.0


@dataclass
class _Row:
    """One selectable pick list in the tree."""

    project_path: Path
    project_name: str
    # Three-word nickname (`ProjectState.mnemonic`, e.g. "amber-vagrant-fermi"). Shown
    # beside the name exactly as merge_card shows it: two projects can carry the same
    # directory name in different roots, and the handle is what people actually say out
    # loud. Empty on a legacy project that never got one.
    mnemonic: str
    species_id: str
    species_label: str
    species_color: str
    tomo_name: str
    slug: str
    label: str
    list_type: str
    star_path: str
    tomograms_star: str
    count: int

    @property
    def is_curated(self) -> bool:
        """A human vetted these picks. AUTO is the only grade that never qualifies."""
        return self.list_type in _CURATED_TYPES

    @property
    def key(self) -> str:
        return f"{self.project_path}\x1f{self.species_id}\x1f{self.tomo_name}\x1f{self.slug}"

    def to_source(self) -> CoordSource:
        return CoordSource(
            star_path=Path(self.star_path),
            tomograms_star=Path(self.tomograms_star),
            tomo_name=self.tomo_name,
            project_path=self.project_path,
            list_type=self.list_type,
            label=f"{self.project_name}:{self.tomo_name}:{self.label or self.slug}",
        )


# ---------------------------------------------------------------------------
# Enumeration (disk; runs off the event loop)
# ---------------------------------------------------------------------------


def _rows_for_project(project_path: Path) -> list[_Row]:
    """Every coordinate-grade list in one project, as selectable rows.

    Reads the project's own ProjectState — including a FOREIGN one, which is exactly how
    cross-project aggregation reaches lists that have never been extracted and therefore
    have no optimisation set for `discover_subtomo_optimisation_sets` to find.

    Rows whose star is missing on disk are dropped rather than offered: selecting one
    would fail at merge time with a path error instead of at selection time with an
    explanation.
    """
    from services.particles.list_ref import species_tomo_map

    try:
        state = get_project_state_for(project_path)
    except Exception:
        log.warning("Aggregate: cannot read project state at %s", project_path, exc_info=True)
        return []

    # `project_name` is the state's own label, not the directory name — that is what the
    # merge dialog shows and what the landing page named the project.
    proj_name = state.project_name or project_path.name
    mnemonic = state.mnemonic or ""

    species = {sp.id: sp for sp in (state.species_registry or [])}
    rows: list[_Row] = []
    for species_id, sp in species.items():
        try:
            anchors, tomo_stars = species_tomo_map(state, project_path, species_id)
        except Exception:
            log.warning("Aggregate: cannot map tomograms for %s in %s", species_id, project_path, exc_info=True)
            continue

        # The species' auto list: one candidates.star spanning every tomogram the CE job
        # processed. Offered once per tomogram, because selection is per tomogram.
        auto_star = anchors.ce_job_dir / "candidates.star" if anchors.ce_job_dir else None
        for tomo_name, tstar in tomo_stars.items():
            if tstar is None:
                continue
            if auto_star is not None and auto_star.exists():
                rows.append(
                    _Row(
                        project_path=project_path,
                        project_name=proj_name,
                        mnemonic=mnemonic,
                        species_id=species_id,
                        species_label=sp.name,
                        species_color=sp.color or SLATE,
                        tomo_name=tomo_name,
                        slug="auto",
                        label="candidates",
                        list_type="auto",
                        star_path=str(auto_star),
                        tomograms_star=str(tstar),
                        count=0,
                    )
                )
        for pl in state.pick_lists:
            if pl.species_id != species_id or not pl.path:
                continue
            tstar = tomo_stars.get(pl.tomo_name)
            if tstar is None or not Path(pl.path).exists():
                continue
            rows.append(
                _Row(
                    project_path=project_path,
                    project_name=proj_name,
                    mnemonic=mnemonic,
                    species_id=species_id,
                    species_label=sp.name,
                    species_color=sp.color or SLATE,
                    tomo_name=pl.tomo_name,
                    slug=pl.slug,
                    label=pl.label or pl.slug,
                    list_type=str(getattr(pl.list_type, "value", pl.list_type)),
                    star_path=pl.path,
                    tomograms_star=str(tstar),
                    count=int(pl.count or 0),
                )
            )
    return rows


def _enumerate(project_path: Path) -> list[_Row]:
    """This project's rows first, then every other discoverable project's."""
    from services.aggregation.discovery import discover_pick_list_projects
    from services.configs.user_prefs_service import get_prefs_service

    here = Path(project_path).resolve()
    rows = _rows_for_project(here)
    prefs = get_prefs_service().prefs
    base_paths = [r.path for r in prefs.recent_project_roots if r.path]
    for other in discover_pick_list_projects(base_paths):
        if other == here:
            continue
        rows.extend(_rows_for_project(other))
    return rows


# ---------------------------------------------------------------------------
# Dialog
# ---------------------------------------------------------------------------


class _AggregateDialog:
    """State of ONE open aggregate dialog. Per-tab, never a module global — the same
    reason `merge_card._MergeDialog` exists rather than the `_DIALOG_REFS` it replaced."""

    def __init__(self, project_path: Path) -> None:
        self.project_path = Path(project_path)
        self.state = get_project_state_for(self.project_path)
        self.flight = SingleFlight()
        self.rows: list[_Row] = []
        self.selected: set[str] = set()
        self.species_id: str = ""
        self.radius: float = _NO_DIAMETER_RADIUS
        self.name: str = ""
        self.spawn: bool = True
        self.tree: ui.element | None = None
        self.summary_host: ui.element | None = None
        self.action_btn: ui.element | None = None
        self.action_tip: ui.element | None = None
        # Tree chrome, mirroring merge_card's selector: a text filter, a curated-only
        # narrowing, and per-node expansion. Expansion is state, not a render detail —
        # a selection click rebuilds the tree and must not collapse what the user opened.
        self.filter: str = ""
        self.curated_only: bool = False
        self.expanded_projects: set[Path] = set()
        self.collapsed_tomos: set[str] = set()

    # ---- derived ----

    def species_options(self) -> dict[str, str]:
        """Species present in ANY discovered project, keyed by id. Two projects can label
        one species differently; the id is what the pick lists agree on."""
        out: dict[str, str] = {}
        for r in self.rows:
            out.setdefault(r.species_id, r.species_label or r.species_id)
        return dict(sorted(out.items(), key=lambda kv: kv[1].lower()))

    def visible_rows(self) -> list[_Row]:
        """Rows of the chosen species that survive the tree's filter + curated-only
        narrowing. Selection is checked against `self.selected` and NOT re-filtered:
        narrowing the view must never silently drop something already chosen."""
        rows = [r for r in self.rows if r.species_id == self.species_id]
        if self.curated_only:
            rows = [r for r in rows if r.is_curated or r.key in self.selected]
        f = (self.filter or "").strip().lower()
        if f:
            rows = [
                r
                for r in rows
                if f in r.project_name.lower()
                or f in r.tomo_name.lower()
                or f in (r.label or "").lower()
                or f in r.list_type.lower()
                or r.key in self.selected
            ]
        return rows

    def species_rows(self) -> list[_Row]:
        """Every row of the chosen species, filters ignored — what `chosen()` scores
        against, so a selection made before a filter was typed still counts."""
        return [r for r in self.rows if r.species_id == self.species_id]

    def chosen(self) -> list[_Row]:
        return [r for r in self.species_rows() if r.key in self.selected]

    def default_radius(self) -> tuple[float, str]:
        """(radius, why). The species' Ø is the honest default; absent, it stays 0 and the
        clash report says it cannot be computed rather than inventing a particle size."""
        for sp in self.state.species_registry or []:
            if sp.id == self.species_id and sp.diameter_ang:
                return float(sp.diameter_ang), f"species Ø {sp.diameter_ang:g} Å"
        return _NO_DIAMETER_RADIUS, "no Ø on the species — set one on the Particles registry"

    # ---- rendering ----

    def on_species(self, species_id: str) -> None:
        self.species_id = species_id
        self.selected.clear()
        self.radius, _ = self.default_radius()
        # Open this project by default and leave the others shut: the common case is
        # "unite what is in front of me", and a cross-project scan can return many.
        self.expanded_projects = {self.project_path.resolve()}
        self.collapsed_tomos.clear()
        self.render_tree()
        self.render_summary()

    def set_filter(self, text: str) -> None:
        self.filter = text or ""
        # A filter that matches nothing in a shut project is invisible, so open every
        # project while one is active and restore the default when it is cleared.
        if self.filter.strip():
            self.expanded_projects = {r.project_path for r in self.species_rows()}
        else:
            self.expanded_projects = {self.project_path.resolve()}
        self.render_tree()

    def set_curated_only(self, on: bool) -> None:
        self.curated_only = bool(on)
        self.render_tree()

    def toggle_project(self, project_path: Path) -> None:
        if project_path in self.expanded_projects:
            self.expanded_projects.discard(project_path)
        else:
            self.expanded_projects.add(project_path)
        self.render_tree()

    def toggle_tomo_expansion(self, tomo_key: str) -> None:
        if tomo_key in self.collapsed_tomos:
            self.collapsed_tomos.discard(tomo_key)
        else:
            self.collapsed_tomos.add(tomo_key)
        self.render_tree()

    def toggle(self, key: str, on: bool) -> None:
        """One list's checkbox. Re-renders the tree, not just the summary: the ancestor
        rows carry `n_selected/total` counters and a select-all checkbox, and leaving them
        stale after a leaf click is how a user ends up merging a set they can't read off
        the screen. Same trade `merge_card._mutate` makes."""
        if on:
            self.selected.add(key)
        else:
            self.selected.discard(key)
        self.render_summary()
        self.render_tree()

    def toggle_rows(self, rows: list[_Row], on: bool) -> None:
        """Select/deselect a whole tomogram's lists."""
        for r in rows:
            if on:
                self.selected.add(r.key)
            else:
                self.selected.discard(r.key)
        self.render_summary()
        self.render_tree()

    # ---- tree (project → tomogram → list) ----------------------------------------
    #
    # Deliberately the same grammar as merge_card's `_MergeSelector`: expandable project
    # header with the project avatar, indented children, a right-flush numeric column that
    # lines up across all three levels, steelblue reserved for the curated highlight. The
    # two dialogs are two modes of one surface and a user should not have to relearn the
    # tree when switching grade.

    _PICKS_W = 96

    def _picks_cell(self, count: int, *, curated: int = 0, muted: bool = False) -> None:
        with (
            ui.row()
            .classes("items-baseline gap-1")
            .style(f"flex-shrink: 0; width: {self._PICKS_W}px; justify-content: flex-end;")
        ):
            ui.space()
            if count:
                tip = f"{curated} from curated lists · {count} picks" if curated else "picks"
            else:
                # Never a fabricated zero: a candidates.star's row count is not recorded on
                # the list, so the honest answer is that we did not read it.
                tip = "count not recorded for this list — the star is read at merge time"
            ui.label(str(count) if count else "—").style(
                f"font-family: monospace; font-size: 10px; font-weight: 600; "
                f"color: {SLATE_MUTED if (muted or not count) else SLATE};"
            ).tooltip(tip)
            ui.label("picks").style(f"font-size: 9px; color: {SLATE_MUTED};")

    @staticmethod
    def _num_cell(value: str, unit: str, accent: bool = False) -> None:
        color = SLATE if accent else SLATE_MUTED
        with ui.row().classes("items-baseline gap-1").style("flex-shrink: 0; justify-content: flex-end; width: 56px;"):
            ui.space()
            ui.label(value).style(f"font-family: monospace; font-size: 10px; font-weight: 600; color: {color};")
            if unit:
                ui.label(unit).style(f"font-size: 9px; color: {SLATE_MUTED};")

    def render_tree(self) -> None:
        if self.tree is None:
            return
        self.tree.clear()
        rows = self.visible_rows()
        with self.tree:
            if not self.species_rows():
                ui.label("No coordinate lists for this species in any discoverable project.").classes(
                    "text-[11px] italic p-3"
                ).style(f"color: {SLATE_MUTED};")
                return
            if not rows:
                ui.label("No lists match the filter." if self.filter.strip() else "No curated lists here.").classes(
                    "text-[11px] italic p-3"
                ).style(f"color: {SLATE_MUTED};")
                return
            by_project: dict[Path, list[_Row]] = {}
            for r in rows:
                by_project.setdefault(r.project_path, []).append(r)
            here = self.project_path.resolve()
            # This project first, then the rest alphabetically — the near case is the
            # one being answered most of the time.
            for project_path in sorted(by_project, key=lambda p: (p != here, by_project[p][0].project_name.lower())):
                self._render_project(project_path, by_project[project_path])

    def _render_project(self, project_path: Path, prows: list[_Row]) -> None:
        name = prows[0].project_name
        color = avatar_color(name)
        expanded = project_path in self.expanded_projects
        is_here = project_path == self.project_path.resolve()
        n_sel = sum(1 for r in prows if r.key in self.selected)
        tomos = {r.tomo_name for r in prows}
        picks = sum(r.count for r in prows)
        curated = sum(r.count for r in prows if r.is_curated)

        header = (
            ui.row()
            .classes("w-full items-center gap-2 px-2 py-1.5 cursor-pointer hover:bg-slate-50")
            .style("border-bottom: 1px solid #eef2f6;")
        )
        header.on("click", lambda _e, p=project_path: self.toggle_project(p))
        with header:
            ui.icon("expand_more" if expanded else "chevron_right", size="16px").classes("text-slate-400")
            with ui.element("div").style(
                f"width: 20px; height: 20px; border-radius: 50%; flex-shrink: 0; "
                f"background: {color}1a; border: 1px solid {color}44; "
                "display: flex; align-items: center; justify-content: center;"
            ):
                ui.label(name[:3].upper()).style(
                    f"font-size: 8px; font-weight: 600; color: {color}; line-height: 1; pointer-events: none;"
                )
            ui.label(name).classes("text-xs font-semibold text-slate-700 truncate").style("flex: 1; min-width: 0;")
            # Name + three-word handle, exactly as merge_card draws it. The handle is the
            # identity people actually use, and it disambiguates two projects that share a
            # directory name — which "other project" never could.
            if prows[0].mnemonic:
                ui.label(prows[0].mnemonic).classes("text-[9px] font-mono text-slate-400 italic").style(
                    "flex-shrink: 0;"
                )
            if not is_here:
                # Foreign-ness is a small marker, not a text chip competing with the name:
                # it matters because pooling across projects has to clear the tomogram
                # identity gate, and that is what the tooltip says.
                ui.icon("link", size="11px").style(f"color: {AMBER}; flex-shrink: 0;").tooltip(
                    "Another project — coordinates pool only once the tomogram identity gate passes "
                    "(matching acquisition, handedness and reconstruction geometry)."
                )
            # No select-all checkbox on this row on purpose: the whole header is the
            # expand target (as it is in merge_card), and a checkbox inside a clickable
            # row needs a propagation guard whose interaction with QCheckbox's own click
            # handling is exactly the kind of thing that reads as a dead control. Bulk
            # selection is the per-tomogram checkbox one level down; bulk NARROWING is the
            # filter and the curated-only switch.
            self._num_cell(f"{n_sel}/{len(prows)}", "lists", accent=bool(n_sel))
            self._num_cell(str(len(tomos)), "tomos")
            self._picks_cell(picks, curated=curated)

        if not expanded:
            return
        by_tomo: dict[str, list[_Row]] = {}
        for r in prows:
            by_tomo.setdefault(r.tomo_name, []).append(r)
        # A visible spine down the indent. merge_card gets its hierarchy for free from a
        # species level drawn in the species colour; this tree has no species level (the
        # species is the dropdown), so without a rule the three depths read as one flat
        # white staircase.
        species_color = prows[0].species_color or SLATE
        with (
            ui.element("div")
            .classes("w-full")
            .style(f"padding-left: 22px; border-left: 2px solid {species_color}33; margin-left: 9px;")
        ):
            # Position-sorted high → low, matching the merge dialog and the roster.
            for tomo_name in sorted(by_tomo, key=ts_position_sort_key, reverse=True):
                self._render_tomo(project_path, tomo_name, by_tomo[tomo_name])

    def _render_tomo(self, project_path: Path, tomo_name: str, trows: list[_Row]) -> None:
        tomo_key = f"{project_path}\x1f{tomo_name}"
        expanded = tomo_key not in self.collapsed_tomos
        n_sel = sum(1 for r in trows if r.key in self.selected)
        picks = sum(r.count for r in trows)
        curated = sum(r.count for r in trows if r.is_curated)
        pretty = ts_pretty_name(tomo_name)

        with ui.row().classes("w-full items-center gap-1.5 px-2 py-1 hover:bg-slate-50"):
            ui.checkbox(
                value=n_sel == len(trows) and bool(trows),
                on_change=lambda e, rs=trows: self.toggle_rows(rs, bool(e.value)),
            ).props("dense size=xs")
            arrow = ui.icon("expand_more" if expanded else "chevron_right", size="14px").classes(
                "text-slate-400 cursor-pointer"
            )
            arrow.on("click", lambda _e, k=tomo_key: self.toggle_tomo_expansion(k))
            # The species' own colour, on every tomogram row. This is the anchor merge_card
            # gets from its species level; here it is the only place the species is visible
            # once you have picked it in the dropdown.
            ui.element("div").style(
                f"width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; "
                f"background: {trows[0].species_color or SLATE};"
            ).tooltip(trows[0].species_label or trows[0].species_id)
            ui.label(pretty).style(f"width: 92px; flex-shrink: 0; font-size: 11px; color: {SLATE}; font-weight: 500;")
            ui.label(tomo_name if pretty != tomo_name else "").style(
                f"flex: 1; min-width: 0; font-family: monospace; font-size: 9px; color: {SLATE_MUTED}; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
            ).tooltip(tomo_name)
            self._num_cell(f"{n_sel}/{len(trows)}", "lists", accent=bool(n_sel))
            self._picks_cell(picks, curated=curated)

        if not expanded:
            return
        with (
            ui.element("div")
            .classes("w-full")
            .style("padding-left: 22px; border-left: 1px solid #eef2f6; margin-left: 14px;")
        ):
            for r in sorted(trows, key=lambda x: (not x.is_curated, x.list_type, x.slug)):
                self._render_list(r)

    def _render_list(self, r: _Row) -> None:
        is_sel = r.key in self.selected
        glyph, tip = _TYPE_GLYPH.get(r.list_type, ("circle", r.list_type))
        with ui.row().classes("w-full items-center gap-1.5 px-2 py-0.5 hover:bg-slate-50"):
            ui.checkbox(value=is_sel, on_change=lambda e, k=r.key: self.toggle(k, bool(e.value))).props("dense size=xs")
            # Steelblue marks a curated list, grey a machine one — the coordinate-grade
            # reading of merge_card's curated/original highlight, and the single accent
            # this dialog spends.
            ui.icon(glyph, size="11px").style(
                f"color: {STEEL if r.is_curated else '#cbd5e1'}; flex-shrink: 0;"
            ).tooltip(tip)
            ui.label(r.label or r.slug).style(
                f"font-size: 11px; font-weight: {'600' if r.is_curated else '400'}; "
                f"color: {r.species_color if r.is_curated else SLATE}; flex-shrink: 0;"
            )
            ui.label(r.slug).classes("truncate").style(
                f"flex: 1; min-width: 0; font-family: monospace; font-size: 9px; color: {SLATE_MUTED};"
            ).tooltip(r.star_path)
            ui.label(r.list_type).style(
                f"font-size: 9px; color: {SLATE_MUTED}; border: 1px solid #e2e8f0; border-radius: 3px; "
                "padding: 0 4px; flex-shrink: 0;"
            )
            self._picks_cell(r.count, muted=not is_sel)

    def render_summary(self) -> None:
        """Counts, the plan's renames and unverified lines. Computed from state already in
        memory — the clash report needs the merged star and so waits for the merge."""
        if self.summary_host is None:
            return
        self.refresh_action()
        self.summary_host.clear()
        chosen = self.chosen()
        with self.summary_host:
            if len(chosen) < 2:
                ui.label("Select at least 2 lists.").classes("text-[10px]").style(f"color: {SLATE_MUTED};")
                return
            tomos = {r.tomo_name for r in chosen}
            projects = {r.project_path for r in chosen}
            ui.label(
                f"{len(chosen)} lists · {len(tomos)} tomogram{'s' if len(tomos) != 1 else ''} · "
                f"{len(projects)} project{'s' if len(projects) != 1 else ''}"
            ).classes("text-[10px]").style(f"color: {SLATE};")
            _, why = self.default_radius()
            ui.label(f"clash radius: {self.radius:g} Å ({why})").classes("text-[9px]").style(f"color: {SLATE_MUTED};")

    # ---- terminal action ----------------------------------------------------------
    #
    # The verb follows the payload GRADE, never a dropdown (roadmap 12, D1). ② Pick
    # candidates is a peak-finder over score volumes; a union of coordinate lists is what
    # it WOULD have produced, so the offer is to skip it and extract. What varies is only
    # the shape of the union — one tomogram is a pick list and extracts per-list; several
    # need the project's subtomo-extraction job.

    def terminal_action(self) -> tuple[str, str]:
        """(button label, what it will do) for the current selection."""
        chosen = self.chosen()
        if len(chosen) < 2:
            return "Aggregate", "Select at least two lists to aggregate."
        if _single_tomo(chosen) is not None:
            return (
                "Aggregate & extract",
                "One tomogram — the union becomes a merged pick list and its subtomograms are cut directly.",
            )
        return (
            "Aggregate & set up extraction",
            f"{len({r.tomo_name for r in chosen})} tomograms — the union is wired into this project's "
            "Subtomo extraction job, which you then run from the roster.",
        )

    def refresh_action(self) -> None:
        """Keep the footer button naming the job it is about to start. Text-only update —
        never a rebuild: the button is the one element the user is aiming at, and the
        tooltip is a mounted child whose text is set, not a fresh `.tooltip()` each time
        (that appends another one every call)."""
        if self.action_btn is None:
            return
        label, tip = self.terminal_action()
        self.action_btn.set_text(label)
        if self.action_tip is not None:
            self.action_tip.set_text(tip)

    # ---- action ----

    async def run(self, dlg) -> None:
        async with self.flight("aggregate") as acquired:
            if not acquired:
                ui.notify("An aggregate is already running.", type="info", timeout=2000)
                return
            chosen = self.chosen()
            if len(chosen) < 2:
                ui.notify("Select at least 2 lists.", type="warning", timeout=2500)
                return
            root = self.state.project_path
            if root is None:
                ui.notify("No project loaded.", type="negative", timeout=4000)
                return

            sources = [r.to_source() for r in chosen]
            try:
                plan = await run.io_bound(plan_merge, sources)
            except Exception as e:
                log.exception("Aggregate: planning failed")
                ui.notify(f"Cannot aggregate: {e}", type="negative", timeout=9000)
                return
            if not plan.ok:
                await _show_blockers(plan.blocking)
                return
            if plan.unverified and not await _confirm_unverified(plan.unverified, plan.renamed):
                return

            label = (self.name or "").strip() or f"{self.species_id}-aggregate"
            slug = fs_slug(label)
            out_dir = Path(root) / MERGED_DIR_NAME / slug
            ui.notify("Aggregating…", type="info", timeout=2500)
            try:
                summary = await run.io_bound(merge_coordinate_sources, sources, out_dir=out_dir, name=label)
            except Exception as e:
                log.exception("Aggregate into %s failed", out_dir)
                ui.notify(f"Aggregate failed: {e}", type="negative", timeout=9000)
                return

            report = None
            if self.radius > 0:
                report = await run.io_bound(clash_report, Path(summary["outputs"]["particles_star"]), self.radius)

            single_tomo = _single_tomo(chosen)
            if single_tomo is not None:
                await self._register_pick_list(chosen, single_tomo, slug, label, summary)

            dlg.close()
            await _show_result(self, summary, report, single_tomo, out_dir)

    async def _register_pick_list(self, chosen, tomo_name: str, slug: str, label: str, summary: dict) -> None:
        """L1 case: an aggregate confined to ONE tomogram of THIS project is a merged pick
        list, so register it and it gets a chip on the picks surface like any other. This
        is the replacement 09-S3 promised when it deleted the merge bar — until now nothing
        could create a merged list at all (roadmap 12-S6)."""
        if any(r.project_path != self.project_path.resolve() for r in chosen):
            return  # a cross-project union is not addressable by (species, tomo, slug)
        bk = get_backend()
        self.state.add_pick_list(
            PickList(
                slug=f"merged__{slug}",
                label=label,
                list_type=PickListType.MERGED,
                species_id=self.species_id,
                tomo_name=tomo_name,
                path=summary["outputs"]["particles_star"],
                count=int(summary["n_picks"]),
                parent_slugs=[r.slug for r in chosen],
                source_kind=PickSourceKind.MERGE.value,
                source_ref="+".join(r.slug for r in chosen),
                created_by=getattr(bk, "username", "") if bk else "",
            )
        )
        if bk is not None:
            await bk.save_project(self.project_path, force=True)


def _single_tomo(rows: list[_Row]) -> str | None:
    names = {r.tomo_name for r in rows}
    return next(iter(names)) if len(names) == 1 else None


# ---------------------------------------------------------------------------
# Secondary dialogs
# ---------------------------------------------------------------------------


async def _show_blockers(lines: list[str]) -> None:
    """A wall, not a question. Every line here is a STATED disagreement between two
    reconstructions of the same acquisition — pooling their coordinates would put picks in
    the wrong physical place, which no amount of user intent makes correct."""
    with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-[38rem] max-w-full gap-2"):
        ui.label("These lists cannot be aggregated").classes("text-sm font-bold")
        ui.label(
            "The selected tomograms are the same acquisition but were reconstructed differently, "
            "so their centred-Å coordinates do not describe the same places."
        ).classes("text-[11px] text-gray-600")
        for line in lines[:20]:
            ui.label(f"• {line}").classes("text-[10px] font-mono text-red-700")
        with ui.row().classes("w-full justify-end"):
            house_button("Close", lambda: dlg.submit(None))
    await dlg
    dlg.delete()


async def _confirm_unverified(lines: list[str], renamed: dict[str, str]) -> bool:
    """A question, not a wall. These are facts nothing on disk states (handedness is the
    usual one) plus any tomogram name we had to disambiguate. Neither is proof of a
    problem, and silently assuming either way is what the never-invent-defaults policy
    forbids — so the user is shown them and decides."""
    with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-[38rem] max-w-full gap-2"):
        ui.label("Aggregate with unverified facts?").classes("text-sm font-bold")
        for line in lines[:20]:
            ui.label(f"• {line}").classes("text-[10px] font-mono text-orange-700")
        if renamed:
            ui.label("Tomogram names disambiguated:").classes("text-[11px] text-gray-600 pt-1")
            for old, new in list(renamed.items())[:10]:
                ui.label(f"• {old} → {new}").classes("text-[10px] font-mono text-gray-600")
        with ui.row().classes("w-full justify-end gap-2"):
            house_button("Cancel", lambda: dlg.submit(None))
            house_button("Aggregate anyway", lambda: dlg.submit(True), kind="accent")
    go = await dlg
    dlg.delete()
    return bool(go)


async def _show_result(d: _AggregateDialog, summary: dict, report, single_tomo: str | None, out_dir: Path) -> None:
    """What was produced, what clashes, and the ONE thing to do next.

    The verb follows the payload grade, never a dropdown (roadmap 12, D1): a coordinate
    union is what a Pick-candidates job would have produced, so the offer is to extract.
    """
    dropped = summary["columns"]["sidecar_only"]
    with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-[40rem] max-w-full gap-2"):
        ui.label(f"Aggregated “{summary['name']}”").classes("text-sm font-bold")
        ui.label(
            f"{summary['n_picks']} picks · {summary['n_tomograms']} tomograms · {summary['n_sources']} lists"
        ).classes("text-[11px] text-gray-700")
        ui.label(str(out_dir)).classes("text-[9px] font-mono text-gray-500")

        if report is not None:
            t = report["total"]
            if t["n_removed"]:
                ui.label(
                    f"⚠ {t['n_clashing']} picks clash within {report['radius_angst']:g} Å; "
                    f"deduplicating would drop {t['n_removed']}, leaving {t['n_after']}."
                ).classes("text-[11px] text-orange-700")
                ui.label(
                    "Nothing was deduplicated. The union keeps clashers on purpose — hand placements "
                    "sort before machine picks, so a dedup keeps the human one."
                ).classes("text-[10px] text-gray-600")
            else:
                ui.label(f"No clashes within {report['radius_angst']:g} Å.").classes("text-[11px] text-gray-600")
        else:
            ui.label(
                "Clash report not computed — the species states no diameter. Set Ø on the "
                "Particles registry to get one."
            ).classes("text-[10px] text-orange-700")

        if dropped:
            ui.label(
                f"Columns only some lists carried are in provenance.star, not particles.star: {', '.join(dropped)}."
            ).classes("text-[10px] text-gray-600")

        with ui.row().classes("w-full justify-end gap-2 pt-1"):
            house_button("Done", lambda: dlg.submit(None))
            if single_tomo is not None:
                house_button("Extract this list", lambda: dlg.submit("extract"), kind="accent")
            else:
                house_button("Set up extraction", lambda: dlg.submit("subtomo"), kind="accent")
    choice = await dlg
    dlg.delete()
    if choice == "extract":
        await _extract_merged_list(d, summary, single_tomo, out_dir)
    elif choice == "subtomo":
        _prepopulate_subtomo(d, summary)


async def _extract_merged_list(d: _AggregateDialog, summary: dict, tomo_name: str, out_dir: Path) -> None:
    """L1 terminal action: subtomo-extract the merged list through the per-list extraction
    roadmap 07 already built.

    Geometry comes from the species' COMMITTED `extraction_params` or nowhere. There is no
    fallback box here on purpose: `ExtractionParams` carries no defaults precisely because a
    guessed box produces wrong-but-plausible subtomograms, and this is not the surface that
    gets to decide one. Absent, the user is sent to the panel that owns the decision.
    """
    species = next((sp for sp in (d.state.species_registry or []) if sp.id == d.species_id), None)
    geom = getattr(species, "extraction_params", None) if species else None
    if geom is None:
        ui.notify(
            f"‘{d.species_id}’ has no committed extraction geometry. Set box / crop / binning on the "
            "Particles registry → Overview, then Extract the new merged list from Picks & curation.",
            type="warning",
            timeout=10000,
        )
        return

    from services.particles.list_ref import species_anchors

    anchors = await run.io_bound(species_anchors, d.state, d.project_path, d.species_id)
    ce_optset = anchors.ce_job_dir / "optimisation_set.star" if anchors.ce_job_dir else None
    bk = get_backend()
    if bk is None:
        ui.notify("No backend — cannot submit.", type="negative")
        return
    result = await bk.extract_pick_list(
        d.project_path,
        ce_optset if (ce_optset and ce_optset.exists()) else None,
        Path(summary["outputs"]["particles_star"]),
        tomo_name,
        d.species_id,
        f"merged__{Path(out_dir).name}",
        box_size=int(geom.box_size),
        binning=float(geom.binning),
        crop_size=int(geom.crop_size),
        tomograms_star=Path(summary["outputs"]["tomograms_star"]),
    )
    if not result.get("success"):
        ui.notify(f"Extraction failed to submit: {result.get('error')}", type="negative", timeout=9000)
        return
    ui.notify(
        f"Extraction submitted (job {result.get('slurm_job_id', '?')}). Watch it on Picks & curation.",
        type="positive",
        timeout=6000,
    )


def _prepopulate_subtomo(d: _AggregateDialog, summary: dict) -> None:
    """Point the project's subtomo-extraction job at the aggregate.

    Deliberately does NOT submit. Extraction geometry (box / crop / binning) is a decision
    the species owns and the job tab states; silently launching one with whatever numbers
    happened to be on the instance is exactly the invented-default this codebase refuses.
    The user lands on a job whose input is already correct and presses run themselves.
    """
    from services.models_base import JobType

    optset = summary["outputs"]["optimisation_set"]
    targets = [iid for iid, j in d.state.jobs.items() if j.job_type == JobType.SUBTOMO_EXTRACTION]
    if not targets:
        ui.notify(
            f"No Subtomo extraction job in this project yet — add one from the roster, then set its input to {optset}",
            type="warning",
            timeout=9000,
        )
        return
    iid = targets[0]
    d.state.jobs[iid].paths["input_optimisation"] = optset
    d.state.mark_dirty()
    bk = get_backend()
    if bk is not None:
        asyncio.create_task(bk.save_project(d.project_path, force=True))
    ui.notify(f"‘{iid}’ input set to the aggregate. Open its tab, check the box size, then run.", type="positive")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def open_aggregate_dialog(project_path) -> None:
    """Open "Aggregate" for ``project_path``, on the COORDINATE grade."""
    d = _AggregateDialog(project_path)
    ensure_assets_loaded()  # the dialog can open on pages that never mounted the dashboard

    with (
        dialog_host(),
        ui.dialog() as dlg,
        ui.card()
        .classes("w-[1060px] max-w-[96vw] max-h-[92vh] overflow-hidden border border-slate-200 bg-white p-0")
        .style("color: #1e293b;"),
    ):
        with ui.row().classes("w-full items-center gap-3 px-4 py-2 border-b border-slate-200 bg-slate-50"):
            ui.icon("merge_type", size="20px").style(f"color: {SLATE};")
            ui.label("Aggregate").classes("text-sm font-bold text-slate-700")
            render_segmented(GRADE_TABS, GRADE_COORDS, lambda key: _switch_grade(dlg, d.project_path, key))
            ui.space()
            ui.button(icon="close", on_click=dlg.close).props("flat dense round size=sm").classes("text-slate-500")

        # Same vertical order as merge_card, top to bottom: SCOPE selectors (what am I
        # looking at) → the tree → NAME the output → the action. The name input used to sit
        # in the top row here and at the bottom there, which is the inconsistency that made
        # the two modes feel like different programs.
        toolbar = ui.row().classes("w-full items-center gap-3 px-4 py-1.5 border-b border-slate-100 bg-slate-50")
        filter_row = ui.row().classes("w-full items-center gap-2 px-3 pt-2")
        tree = ui.column().classes("w-full px-2 gap-0 overflow-auto bg-white").style("max-height: 48vh;")
        summary_host = ui.column().classes("w-full px-4 pt-2 gap-0")
        name_bar = ui.row().classes("w-full items-center gap-2 px-3 py-2 border-t border-slate-200 bg-slate-50")
        footer = ui.row().classes("w-full items-center gap-2 px-4 py-2 border-t border-slate-200")

        d.tree = tree
        d.summary_host = summary_host

        with toolbar:
            species_select = (
                ui.select({}, label="Species", on_change=lambda e: d.on_species(str(e.value or "")))
                .props("dense outlined")
                .classes("text-xs")
                .style("min-width: 200px;")
            )
            ui.space()
            ui.switch("Curated only", value=False, on_change=lambda e: d.set_curated_only(bool(e.value))).props(
                "dense color=blue-grey"
            ).classes("text-xs").tooltip(
                "Hide machine candidate sets — show only lists a human placed, filtered, imported or already merged. "
                "Anything already selected stays visible."
            )

        with filter_row:
            ui.input(placeholder="Filter projects, tomograms, lists…", on_change=lambda e: d.set_filter(e.value)).props(
                "dense outlined clearable debounce=200"
            ).classes("w-full")

        # Name bar: sits above the action, mounted once and never rebuilt, so typing a name
        # survives every checkbox click (the same reason merge_card keeps its there).
        with name_bar:
            ui.input(
                placeholder="Name this aggregate (optional)", on_change=lambda e: setattr(d, "name", e.value or "")
            ).props("dense outlined").classes("text-xs").style("width: 220px;")
            ui.label("Output lands in MergedSources/<name>/ — leave blank for the species' name.").classes(
                "text-[10px]"
            ).style(f"color: {SLATE_MUTED};")

        with footer:
            ui.icon("info", size="13px").style(f"color: {SLATE_MUTED};")
            ui.label("Nothing is deduplicated: the union keeps clashers so you can see them.").classes(
                "text-[10px]"
            ).style(f"color: {SLATE_MUTED};")
            ui.space()
            house_button("Cancel", dlg.close)
            # The accent button NAMES the job it starts, and re-names itself as the shape of
            # the selection changes (roadmap 12, D1). `refresh_action` only sets text, so a
            # click landing mid-update still hits this element.
            # `lambda: d.run(dlg)`, NOT `asyncio.create_task(d.run(dlg))`. NiceGUI keys its
            # slot stack on `id(asyncio.current_task())` (`nicegui/slot.py`), so a bare task
            # starts with an EMPTY stack and the first `ui.notify` in `run()` dies with
            # "The current slot cannot be determined". Returning the coroutine instead makes
            # NiceGUI await it inside `with parent_slot:` (`events.handle_event`), which is
            # what gives `run()` a client — for its notifies AND for the `dialog_host()`
            # lookups in the blocker / unverified / result dialogs it awaits.
            action = house_button("Aggregate", lambda: d.run(dlg), kind="accent")
            with action:
                d.action_tip = ui.tooltip("")
            d.action_btn = action
            d.refresh_action()

        with tree:
            with ui.row().classes("w-full justify-center py-8"):
                ui.spinner("dots", size="md").classes("text-slate-400")
                ui.label("Scanning your projects…").classes("text-xs text-slate-500 ml-2 self-center")

    dlg.open()

    async def _load() -> None:
        d.rows = await run.io_bound(_enumerate, d.project_path)
        options = d.species_options()
        species_select.set_options(options)
        if options:
            first = next(iter(options))
            species_select.set_value(first)
            d.on_species(first)
        else:
            d.render_tree()

    asyncio.create_task(_load())


def _switch_grade(dlg, project_path: Path, key: str) -> None:
    """Swap to the other payload grade: close this dialog, open that one.

    Two dialogs rather than two panels because their selection models genuinely differ —
    an optimisation set versus a pick list — but the switch is one control in both headers
    so they read as two modes of one surface. ``dialog_host`` is what makes this work at
    all: without it the new dialog is built inside the card that is closing and never
    paints (see ui/components/dialogs.py).
    """
    if key == GRADE_COORDS:
        return
    from ui.aggregation.merge_card import open_aggregation_merge_dialog

    dlg.close()
    open_aggregation_merge_dialog(project_path)
