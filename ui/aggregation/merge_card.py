"""
Merge-sources dialog — collect the same species' particles from several projects.

Opened from the PARTICLES phase header of ANY project (de-novo S6 removed the
`is_aggregation` project type: merging is a capability, not a kind of project).
The user builds a list of upstream optimisation_set.star sources and merges them
into <project>/MergedSources/<slug>/. The output is a project-level resource that
any downstream job (Reconstruct/Class3D/Refine3D/...) reads through the synthetic
`mergedSources` producer the path resolver registers for the active merge
(apply_aggregation_overrides wires it via a source_overrides key).

Selection is a navigable hierarchy — Project → Species → Tomogram — so the user
sees what's actually inside each source (per-tomogram pick counts, curation
state) and can fine-select down to individual tomograms. The selection persists
as `AggregationSource` entries on ProjectState; a `tomo_names` of None means
"all tomograms in that set".

Everything one open dialog owns lives on a `_MergeDialog` instance, never on a
module global, and its ProjectState comes from an explicit project path — see that
class for why both matter.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path

from nicegui import ui, run

from backend import get_backend
from services.aggregation.extraction import apply_aggregation_overrides
from services.project_state import (
    MERGED_DIR_NAME,
    AggregationMerge,
    AggregationMergeSource,
    AggregationSource,
    get_project_state_for,
)
from services.array_tasks import ts_position_sort_key, ts_pretty_name
from ui.components.buttons import house_button
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.components.segmented import render_segmented
from ui.dashboard.css import ensure_assets_loaded
from ui.local_file_picker import local_file_picker
from ui.projects_overview import avatar_color

log = logging.getLogger(__name__)


# Steelblue is the single accent — reserved for the curated/uncurated highlight.
# Everything else stays neutral slate.
STEEL = "#4682b4"
SLATE = "#475569"
SLATE_MUTED = "#94a3b8"


def _active_merge(state) -> AggregationMerge | None:
    """The merge downstream consumers use: the explicitly-active one, else the
    newest recorded merge. Thin wrapper over ProjectState.active_merge()."""
    return state.active_merge()


def active_merged_optset(state) -> Path | None:
    """Resolved optimisation_set.star of the active merge. Falls back to a
    legacy MergedSources/optimisation_set.star (pre-registry projects). Thin
    wrapper over ProjectState.active_merged_optset()."""
    return state.active_merged_optset()


def _slugify(name: str, existing: set) -> str:
    """Filesystem-safe, unique-within-registry slug from a user name; falls back
    to a timestamp when the name is empty."""
    base = re.sub(r"[^A-Za-z0-9_-]+", "-", (name or "").strip()).strip("-").lower()[:48]
    if not base:
        base = "merge-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    slug, i = base, 2
    while slug in existing:
        slug = f"{base}-{i}"
        i += 1
    return slug


def has_merged_outputs(state) -> bool:
    """True if ``state`` has at least one usable merged optset. Lets a caller render a
    'merged' badge without opening the dialog. Takes the state explicitly — every caller
    already holds one, and reaching for the client context made this unusable off a
    request (S6)."""
    return active_merged_optset(state) is not None


def _selection_label(src: AggregationSource | None, n_total: int | None) -> str:
    """Compact "selected" descriptor for a (collapsed) species row."""
    if src is None:
        return ""
    if src.tomo_names is None:
        return f"all{f' {n_total}' if n_total else ''} tomos"
    return f"{len(src.tomo_names)}{f'/{n_total}' if n_total else ''} tomos"


# ---------------------------------------------------------------------------
# One open dialog and everything it owns
# ---------------------------------------------------------------------------


class _MergeDialog:
    """The state of ONE open merge dialog (S6).

    What this replaced: module-level `_DIALOG_REFS` / `_registry_expanded` globals plus a
    `current_project_state()` reach in a dozen helpers. Both were per-PROCESS where the
    thing they describe is per-TAB — a second browser tab opening the dialog overwrote the
    first tab's element refs, so the first tab's "add a manual path" then rebuilt a selector
    that had already been destroyed, and expanding a registry row in one tab expanded it in
    the other. Documented as a bug in docs/PICKS_FILTER_AGGREGATION_ROADMAP.md; this is the
    fix, not a tidy-up.

    ``state`` is resolved from an explicit ``project_path`` at open. That is what lets the
    merge itself run without a client context (a `current_project_state()` inside a
    background task silently hands back a blank throwaway), and it is why every helper below
    takes ``self.state`` instead of reaching for one."""

    def __init__(self, project_path: Path) -> None:
        self.project_path = Path(project_path)
        self.state = get_project_state_for(self.project_path)
        self.flight = SingleFlight()
        self.selector: _MergeSelector | None = None
        self.footer: ui.element | None = None
        self.registry_holder: ui.element | None = None
        self.name_input = None
        self.meta: dict[str, str] = {"name": "", "description": ""}
        self.expanded_registry: set = set()

    # ---- paths ----

    def merged_root(self) -> Path | None:
        if self.state.project_path is None:
            return None
        return self.state.project_path / MERGED_DIR_NAME

    def merge_dir_for(self, slug: str) -> Path | None:
        root = self.merged_root()
        return (root / slug) if root else None

    # ---- persistence ----

    def persist(self) -> None:
        """Persist deferred + off the event loop. A full ProjectState.save() does a
        model_dump of every job/species + a JSON disk write (~hundreds of ms on a
        real project), so doing it inline made each checkbox click hang. Debounced
        (0.4 s trailing edge, coalesced per project) via the facade; force=True
        because update_modified() doesn't mark the state dirty."""
        self.state.update_modified()
        bk = get_backend()
        if bk is None:
            self.state.save()  # pre-backend startup edge — save inline
            return
        try:
            asyncio.create_task(bk.save_project(self.project_path, force=True, debounce_s=0.4))
        except RuntimeError:
            self.state.save()  # no loop (shouldn't happen from a handler) — save inline

    # ---- selection over state.aggregation_sources ----

    def find_source(self, optset_path: str) -> AggregationSource | None:
        return next((s for s in self.state.aggregation_sources if s.optset_path == optset_path), None)

    def selected_tomos(self, optset_path: str, all_tomos: list[str]) -> set:
        """Currently-selected tomo names for a source. tomo_names=None => all."""
        src = self.find_source(optset_path)
        if src is None:
            return set()
        if src.tomo_names is None:
            return set(all_tomos)
        return set(src.tomo_names)

    def set_species_selection(self, cand, selected: set, all_tomos: list[str]) -> None:
        """Replace the source entry for one (project, species) with the given tomo
        selection. Empty selection removes the source; full selection normalizes to
        tomo_names=None (=all). Per-tomo original overrides are preserved (pruned to
        the tomos still selected)."""
        prev = self.find_source(cand.optset_path)
        prev_orig = set(prev.original_tomos) if prev else set()
        srcs = [s for s in self.state.aggregation_sources if s.optset_path != cand.optset_path]
        sel = selected & set(all_tomos) if all_tomos else selected
        if sel:
            tomo_names = None if (all_tomos and sel == set(all_tomos)) else sorted(sel)
            srcs.append(
                AggregationSource(
                    optset_path=cand.optset_path,
                    tomo_names=tomo_names,
                    original_tomos=sorted(prev_orig & sel),
                    project_name=cand.project_name,
                    project_path=cand.project_path,
                    species_id=cand.species_id or "",
                    species_label=cand.species_label or "",
                )
            )
        self.state.aggregation_sources = srcs
        self.persist()

    def set_tomo_origin(self, cand, ts_name: str, use_original: bool) -> None:
        """Pin a single tomogram to original (True) or curated (False) picks. The
        species must already contribute a source; a no-op otherwise (the toggle is
        only shown for included tomos)."""
        src = self.find_source(cand.optset_path)
        if src is None:
            return
        orig = set(src.original_tomos or [])
        if use_original:
            orig.add(ts_name)
        else:
            orig.discard(ts_name)
        src.original_tomos = sorted(orig)
        self.persist()

    def toggle_species_all(self, cand, on: bool) -> None:
        """Master toggle for a whole species: select all tomos (tomo_names=None) or
        remove the source entirely. Preserves per-tomo original overrides on select."""
        prev = self.find_source(cand.optset_path)
        prev_orig = list(prev.original_tomos) if prev else []
        srcs = [s for s in self.state.aggregation_sources if s.optset_path != cand.optset_path]
        if on:
            srcs.append(
                AggregationSource(
                    optset_path=cand.optset_path,
                    tomo_names=None,
                    original_tomos=prev_orig,
                    project_name=cand.project_name,
                    project_path=cand.project_path,
                    species_id=cand.species_id or "",
                    species_label=cand.species_label or "",
                )
            )
        self.state.aggregation_sources = srcs
        self.persist()

    # ---- footer / registry redraws ----

    def refresh_footer(self) -> None:
        if self.footer is None:
            return
        self.footer.clear()
        sources = list(self.state.aggregation_sources or [])
        with self.footer:
            ui.label(_footer_summary(sources)).classes("text-xs text-slate-600")
            ui.space()
            merge_btn = house_button(
                f"Merge {len(sources)} source(s)" if sources else "Merge",
                lambda: self.run_merge(),  # NOT create_task: a bare task has an empty NiceGUI slot stack
                kind="accent",
            )
            if not sources:
                merge_btn.disable()
            else:
                # The extracted grade's terminal action, stated before the click. Unlike the
                # coordinate grade — where the union has to be extracted before anything can
                # read it — merged particles already have pixels, so the merge itself IS the
                # handoff: `apply_aggregation_overrides` registers the synthetic
                # `mergedSources` producer and every downstream job that consumes particles
                # (Reconstruct / Class3D / Refine3D) is repointed at it.
                merge_btn.tooltip(
                    "Merges into MergedSources/<name>/ and wires it as this project's active source — "
                    "Reconstruct, Class3D and Refine3D then read the merged particles. No job is submitted."
                )

    def render_registry(self) -> None:
        if self.registry_holder is not None:
            _render_registry(self, self.registry_holder)

    def toggle_registry(self, slug: str) -> None:
        if slug in self.expanded_registry:
            self.expanded_registry.discard(slug)
        else:
            self.expanded_registry.add(slug)
        self.render_registry()

    def set_active_merge(self, slug: str) -> None:
        self.state.active_merge_slug = slug
        apply_aggregation_overrides(self.state)
        self.persist()
        self.render_registry()

    # ---- actions ----

    async def pick_manual_path(self) -> None:
        """Add an optimisation_set path from outside the discovered project roots."""
        async with self.flight("pick_path") as acquired:
            if not acquired:
                return
            state = self.state
            start_dir = str(state.project_path) if state.project_path else "/"
            result = await local_file_picker(start_dir, upper_limit=None, mode="directory")
            if not result or not result[0]:
                return
            chosen = result[0]
            if any(s.optset_path == chosen for s in (state.aggregation_sources or [])):
                ui.notify("Already in list", type="warning", timeout=2000)
                return
            state.aggregation_sources = [
                *list(state.aggregation_sources or []),
                AggregationSource(optset_path=chosen, project_name=Path(chosen).parent.name),
            ]
            self.persist()
            ui.notify(f"Added: {Path(chosen).name}", type="positive", timeout=1500)
            if self.selector is not None:
                self.selector.rebuild()
            self.refresh_footer()

    async def preflight_blockers(self) -> list[str]:
        """Selected sources of THIS project whose optimisation_set is BEHIND its pick list,
        as lines to show before merging (docs/LIST_EXTRACTION_AND_AGGREGATION.md §8.9).

        A merge consumes each source's optimisation_set exactly as it stands on disk. When
        the list behind one has been re-picked or re-curated since it was cut, that source
        contributes yesterday's particles — silently, with no error anywhere. This is the
        one place that can notice, and it is cheap next to the merge itself.

        Matched by PATH, not by species: a list that was never extracted has no optset and
        therefore cannot be one of the sources, so warning about it would be crying wolf.
        Sources from OTHER projects are not checked at all — this project's state cannot
        answer for them, and inventing a verdict would be worse than saying nothing."""
        bk = get_backend()
        if bk is None:
            return []
        here = str(self.project_path.resolve())
        mine = [
            s
            for s in (self.state.aggregation_sources or [])
            if s.species_id and str(Path(s.project_path or "").resolve()) == here
        ]
        if not mine:
            return []
        stale: dict[str, dict] = {}
        for sid in sorted({s.species_id for s in mine}):
            for row in await bk.get_pending_extractions(self.project_path, sid):
                if row["extracted_path"]:
                    stale[str(Path(row["extracted_path"]))] = row
        lines: list[str] = []
        for s in mine:
            row = stale.get(str(Path(s.optset_path or "")))
            if row is None:
                continue
            lines.append(
                f"{row['species_id']} · {row['tomo_name']} · {row['label']}: {row['extraction_state']} — "
                "its picks changed since this optimisation_set was cut; re-extract it first"
            )
        return lines

    async def run_merge(self) -> None:
        """Pre-flight, then merge. SingleFlight-guarded: the merge is minutes of driver work
        and the button lives in a footer that a selection change rebuilds, so a second click
        used to start a second merge into the same directory."""
        async with self.flight("merge") as acquired:
            if not acquired:
                ui.notify("A merge is already running.", type="info", timeout=2000)
                return
            state = self.state
            if not state.aggregation_sources:
                ui.notify("Add at least one source first.", type="warning", timeout=2500)
                return
            root = self.merged_root()
            if root is None:
                ui.notify("No project loaded — cannot merge.", type="negative", timeout=4000)
                return

            blockers = await self.preflight_blockers()
            if blockers and not await _confirm_blockers(blockers):
                return

            name = (self.meta.get("name") or "").strip()
            description = (self.meta.get("description") or "").strip()
            slug = _slugify(name, {m.slug for m in (state.aggregation_merges or [])})
            merged_dir = root / slug

            sources = _build_merge_sources(state)
            ui.notify("Merging…", type="info", timeout=2500)
            try:
                summary = await run.io_bound(_run_merge_sync, merged_dir, sources)
            except Exception as e:
                log.exception("Merge into %s failed", merged_dir)
                ui.notify(f"Merge failed: {e}", type="negative", timeout=8000)
                return

            record = _build_merge_record(state, slug, name or slug, description, summary)
            state.aggregation_merges = [*list(state.aggregation_merges or []), record]
            state.active_merge_slug = slug  # newest becomes active
            # Wire downstream consumers to the new active optset.
            n_wired = apply_aggregation_overrides(state)
            await run.io_bound(state.save)  # persist the registry now, not debounced

            msg = f"Merge “{record.name}” complete"
            if n_wired:
                msg += f" · wired {n_wired} downstream job{'s' if n_wired != 1 else ''}"
            ui.notify(msg, type="positive", timeout=4000)
            self.meta["name"] = ""  # clear so the next merge needs a fresh name
            if self.name_input is not None:
                self.name_input.value = ""
            self.render_registry()


async def _confirm_blockers(lines: list[str]) -> bool:
    """Show what the gate found and let the user merge anyway. A question, not a block —
    a stale list is sometimes exactly what you meant to merge (comparing against an older
    extraction), and the roll-up gate is advisory by design."""
    with dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[34rem] max-w-full gap-2"):
        ui.label(f"{len(lines)} selected source(s) are out of date").classes("text-sm font-bold")
        ui.label(
            "The merge reads each source's optimisation_set as it is on disk right now. These would "
            "contribute their PREVIOUS extraction, not the picks they have today — silently."
        ).classes("text-[11px] text-gray-600")
        for line in lines[:20]:
            ui.label(f"• {line}").classes("text-[10px] font-mono text-orange-700")
        if len(lines) > 20:
            ui.label(f"… and {len(lines) - 20} more").classes("text-[10px] text-gray-400")
        with ui.row().classes("w-full justify-end gap-2"):
            house_button("Cancel", lambda: confirm.submit(None))
            house_button("Merge anyway", lambda: confirm.submit(True), kind="accent")
    go = await confirm
    confirm.delete()
    return bool(go)


# ---------------------------------------------------------------------------
# Hierarchical selector
# ---------------------------------------------------------------------------


class _MergeSelector:
    """Project → Species → Tomogram selection tree for the merge dialog.

    Discovery (cross-project scan) is async + cached. Per-tomogram curation is
    loaded lazily when a species node is expanded — reading every particles.star
    up front would not scale (PICKS_FILTER_AGGREGATION_ROADMAP.md §scale)."""

    def __init__(self, dlg: _MergeDialog, body: ui.element, on_change) -> None:
        self.dlg = dlg
        self.body = body
        self.on_change = on_change  # called after any selection mutation
        self.tree: ui.element | None = None  # rebuilt subtree (filter input persists)
        self.candidates: list = []
        self.by_project: dict[str, list] = {}
        self.project_meta: dict[str, dict] = {}
        self.expanded_projects: set = set()
        self.expanded_species: set = set()
        self.curation: dict[str, list] = {}  # optset_path -> List[TomoCuration]
        # Species narrowing, the control the coordinate dialog already had and this one
        # did not. "" = every species. Unlike the coordinate grade — where one species per
        # aggregate is ENFORCED (roadmap 12, D2) because the geometry has to be unambiguous
        # — a pixel-grade merge of several species is merely unusual, so this narrows the
        # tree without forbidding anything.
        self.species_filter: str = ""
        self.species_select = None  # bound by the dialog; options filled after discovery
        self.filter = ""
        self.show_curated_only = False

    async def load(self) -> None:
        from services.aggregation.discovery import discover_subtomo_optimisation_sets
        from services.configs.user_prefs_service import get_prefs_service

        self.body.clear()
        with self.body:
            spinner_row = ui.row().classes("w-full justify-center py-8")
            with spinner_row:
                ui.spinner("dots", size="md").classes("text-slate-400")
                ui.label("Scanning your projects…").classes("text-xs text-slate-500 ml-2 self-center")

        prefs = get_prefs_service().prefs
        base_paths = [r.path for r in prefs.recent_project_roots if r.path]
        cands = await run.io_bound(discover_subtomo_optimisation_sets, base_paths)
        self.candidates = cands
        self._group()
        if self.species_select is not None:
            self.species_select.set_options(self.species_options(), value="")
        # Auto-expand projects that already contribute a selected source.
        selected_paths = {s.optset_path for s in self.dlg.state.aggregation_sources}
        for c in cands:
            if c.optset_path in selected_paths:
                self.expanded_projects.add(c.project_path)

        # Build persistent chrome once: filter input stays mounted so typing
        # keeps focus; only `self.tree` is cleared/rebuilt on interaction.
        self.body.clear()
        with self.body:
            self._render_filter()
            self.tree = ui.column().classes("w-full gap-0")
        self.rebuild()

    def _group(self) -> None:
        self.by_project = {}
        self.project_meta = {}
        for c in self.candidates:
            self.by_project.setdefault(c.project_path, []).append(c)
            if c.project_path not in self.project_meta:
                self.project_meta[c.project_path] = {"name": c.project_name, "mnemonic": c.mnemonic}

    # ---- async expansion (lazy curation load) ----

    async def _ensure_curation(self, cand) -> None:
        if cand.optset_path not in self.curation:
            from services.aggregation.discovery import load_tomo_curation

            self.curation[cand.optset_path] = await run.io_bound(load_tomo_curation, cand.job_dir)

    async def _toggle_species(self, cand) -> None:
        key = cand.optset_path
        if key in self.expanded_species:
            self.expanded_species.discard(key)
        else:
            self.expanded_species.add(key)
            await self._ensure_curation(cand)
        self.rebuild()

    async def _toggle_project(self, project_path: str) -> None:
        if project_path in self.expanded_projects:
            self.expanded_projects.discard(project_path)
            self.rebuild()
            return
        self.expanded_projects.add(project_path)
        self.rebuild()  # show children immediately
        # Pre-load every species' curation so the per-species and per-project
        # pick rollups appear without drilling into each one.
        for c in self.by_project.get(project_path, []):
            await self._ensure_curation(c)
        self.rebuild()

    # ---- selection mutation wrappers (persist + bubble up) ----

    def _mutate(self, fn, *args) -> None:
        fn(*args)
        self.on_change()
        self.rebuild()

    # ---- rendering ----

    def rebuild(self) -> None:
        if self.tree is None:
            return
        self.tree.clear()
        with self.tree:
            cands = self._filtered_candidates()
            shown = {c.optset_path for c in cands}
            if not self.candidates:
                ui.label(
                    "No completed SubtomoExtraction jobs found in your recent project locations. "
                    "Open a project base path on the landing page first."
                ).classes("text-xs text-gray-500 italic p-3")
            elif not cands:
                ui.label("No matches for filter.").classes("text-xs text-gray-400 italic p-3")
            else:
                shown_projects = [p for p in self.by_project if any(c.optset_path in shown for c in self.by_project[p])]
                for project_path in sorted(shown_projects, key=lambda p: self.project_meta[p]["name"].lower()):
                    visible = [c for c in self.by_project[project_path] if c.optset_path in shown]
                    self._render_project(project_path, visible)
            self._render_orphans()

    def _render_filter(self) -> None:
        def on_filter(e):
            self.filter = e.value or ""
            self.rebuild()

        ui.input(placeholder="Filter projects, species…", on_change=on_filter, value=self.filter).props(
            "dense outlined clearable debounce=200"
        ).classes("w-full mb-1")

    def species_options(self) -> dict[str, str]:
        """`{key: label}` for the species picker, "" first meaning every species. Keyed on
        the DISPLAY label rather than the local species id on purpose: each project mints
        its own id, so the same particle would otherwise appear as N unrelated entries —
        the same problem the catalog chip solves one level down."""
        labels = sorted({(c.species_label or "").strip() for c in self.candidates if (c.species_label or "").strip()})
        return {"": "All species", **{lbl: lbl for lbl in labels}}

    def set_species_filter(self, label: str) -> None:
        self.species_filter = label or ""
        # A narrowing that matches nothing in a shut project is invisible.
        if self.species_filter:
            self.expanded_projects = set(self.by_project)
        self.rebuild()

    def _filtered_candidates(self) -> list:
        cands = list(self.candidates)
        if self.show_curated_only:
            cands = [c for c in cands if c.has_filter]
        if self.species_filter:
            # Never hide something already selected: narrowing the view must not silently
            # drop a source from the merge (same rule as the coordinate dialog).
            cands = [
                c
                for c in cands
                if (c.species_label or "").strip() == self.species_filter
                or self.dlg.find_source(c.optset_path) is not None
            ]
        f = (self.filter or "").strip().lower()
        if not f:
            return cands
        return [
            c
            for c in cands
            if f in c.project_name.lower()
            or f in c.instance_id.lower()
            or (c.species_label and f in c.species_label.lower())
            or (c.mnemonic and f in c.mnemonic.lower())
            or (c.catalog_id and f in c.catalog_id.lower())  # roadmap 12: one term finds every copy
        ]

    @staticmethod
    def _rollup(cur: list) -> tuple:
        """(kept, total, n_reviewed) across a species' tomograms. kept counts the
        curated subset where a tomo was reviewed, else its full original count."""
        kept = sum((t.kept if (t.reviewed and t.kept is not None) else t.total) for t in cur)
        total = sum(t.total for t in cur)
        n_reviewed = sum(1 for t in cur if t.reviewed)
        return kept, total, n_reviewed

    def _render_project(self, project_path: str, cands: list) -> None:
        meta = self.project_meta[project_path]
        name = meta["name"]
        color = avatar_color(name)
        expanded = project_path in self.expanded_projects
        sel_species = sum(1 for c in cands if self.dlg.find_source(c.optset_path) is not None)
        n_tomos = sum(c.n_tomograms or 0 for c in cands)

        header = (
            ui.row()
            .classes("w-full items-center gap-2 px-2 py-1.5 cursor-pointer hover:bg-slate-50")
            .style("border-bottom: 1px solid #eef2f6;")
        )
        header.on("click", lambda _e, p=project_path: self._toggle_project(p))
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
            if meta["mnemonic"]:
                ui.label(meta["mnemonic"]).classes("text-[9px] font-mono text-slate-400 italic").style(
                    "flex-shrink: 0;"
                )
            # Secondary stats, then the pick column flush-right (aligned across
            # all levels). The pick rollup shows once species curation is loaded.
            self._num_cell(f"{sel_species}/{len(cands)}", "spp", accent=bool(sel_species))
            self._num_cell(f"{n_tomos}", "tomos")
            loaded = [self.curation.get(c.optset_path) for c in cands]
            if all(x is not None for x in loaded) and loaded:
                kept = sum(self._rollup(x)[0] for x in loaded)
                total = sum(self._rollup(x)[1] for x in loaded)
                self._picks_cell(kept, total)
            else:
                self._picks_spacer()

        if expanded:
            with ui.element("div").classes("w-full").style("padding-left: 22px;"):
                for c in sorted(cands, key=lambda x: (x.species_label or "~").lower()):
                    self._render_species(c)

    def _render_species(self, cand) -> None:
        src = self.dlg.find_source(cand.optset_path)
        sp_color = cand.species_color or SLATE
        expanded = cand.optset_path in self.expanded_species
        fully_selected = src is not None and src.tomo_names is None
        partial = src is not None and src.tomo_names is not None
        cur = self.curation.get(cand.optset_path)

        with ui.row().classes("w-full items-center gap-1.5 px-2 py-1 hover:bg-slate-50"):
            ui.checkbox(
                value=fully_selected,
                on_change=lambda e, c=cand: self._mutate(self.dlg.toggle_species_all, c, bool(e.value)),
            ).props("dense size=xs")

            arrow = ui.icon("expand_more" if expanded else "chevron_right", size="14px").classes(
                "text-slate-400 cursor-pointer"
            )
            arrow.on("click", lambda _e, c=cand: self._toggle_species(c))

            if cand.species_label:
                ui.label(cand.species_label).style(
                    f"color: {sp_color}; font-size: 11px; font-weight: 600; flex-shrink: 0;"
                )
            else:
                ui.label("Unassigned").classes("text-[11px] text-slate-400 italic").style("flex-shrink: 0;")
            ui.label(cand.instance_id).classes("text-[10px] font-mono text-slate-400 truncate").style(
                "flex: 1; min-width: 0;"
            ).tooltip("SubtomoExtraction job instance")

            # Catalog link (roadmap 12): the ONE identity that is comparable across projects.
            # Local species ids are minted per project, so "the same species elsewhere" was
            # previously something the user matched by eye.
            if cand.catalog_id:
                twins = [c for c in self.candidates if c.catalog_id == cand.catalog_id]
                chip = ui.label(f"⌗ {cand.catalog_id}").style(
                    f"font-size: 9px; font-family: ui-monospace, monospace; color: {STEEL}; "
                    f"border: 1px solid {STEEL}; border-radius: 3px; padding: 0 4px; flex-shrink: 0; cursor: pointer;"
                )
                chip.tooltip(
                    f"Lab-catalog species '{cand.catalog_id}' — found in {len(twins)} project(s). "
                    "Click to select every one of them (whole sets)."
                )
                chip.on("click.stop", lambda _e, cid=cand.catalog_id: self._select_catalog_twins(cid))

            if partial:
                ui.label(_selection_label(src, cand.n_tomograms)).style(
                    f"color: {SLATE}; font-size: 10px; font-weight: 600; flex-shrink: 0;"
                )
            n = len(cur) if cur else cand.n_tomograms
            self._num_cell(str(n) if n is not None else "?", "tomos")
            if cur:
                kept, total, n_reviewed = self._rollup(cur)
                self._picks_cell(kept, total, reviewed=n_reviewed)
            else:
                self._picks_spacer()

        if expanded:
            self._render_tomos(cand)

    def _select_catalog_twins(self, catalog_id: str) -> None:
        """Select every discovered copy of one lab-catalog species, whole sets (roadmap 12-S4).

        Grouping across projects has to key on `catalog_id`: each project mints its own local
        species id, so the same particle looks like N unrelated species here. Selects rather
        than toggles — the reason to click a catalog chip is "give me all of these", and
        un-selecting is what the per-row checkboxes are for."""
        twins = [c for c in self.candidates if c.catalog_id == catalog_id]
        for c in twins:
            self.dlg.toggle_species_all(c, True)
            self.expanded_projects.add(c.project_path)
        ui.notify(f"Selected {len(twins)} project(s) holding '{catalog_id}'", type="positive", timeout=2500)
        self.on_change()
        self.rebuild()

    def _render_tomos(self, cand) -> None:
        tomos = self.curation.get(cand.optset_path) or []
        if self.show_curated_only:
            tomos = [t for t in tomos if t.reviewed and t.kept is not None]
        all_tomos = [t.ts_name for t in (self.curation.get(cand.optset_path) or [])]
        selected = self.dlg.selected_tomos(cand.optset_path, all_tomos)

        with ui.element("div").classes("w-full").style("padding-left: 30px;"):
            if not tomos:
                ui.label("No tomograms in this set.").classes("text-[10px] text-slate-400 italic px-2 py-1")
                return
            # Sort by (stage, beam) descending so positions read high → low.
            for t in sorted(tomos, key=lambda x: ts_position_sort_key(x.ts_name), reverse=True):
                self._render_tomo_row(cand, t, selected, all_tomos)

    def _render_tomo_row(self, cand, t, selected: set, all_tomos: list[str]) -> None:
        is_sel = t.ts_name in selected
        src = self.dlg.find_source(cand.optset_path)
        use_orig = bool(src and t.ts_name in (src.original_tomos or []))
        # "reviewed" — not merely has_filter — marks a genuine curation: the
        # filtered star carries all rows for tomos the user never reviewed.
        curated = t.reviewed and t.kept is not None
        show_curated = curated and not use_orig

        def on_toggle(e, c=cand, ts=t.ts_name, allt=all_tomos):
            sel = self.dlg.selected_tomos(c.optset_path, allt)
            if e.value:
                sel.add(ts)
            else:
                sel.discard(ts)
            self._mutate(self.dlg.set_species_selection, c, sel, allt)

        pretty = ts_pretty_name(t.ts_name)
        with ui.row().classes("w-full items-center gap-1.5 px-2 py-0.5 hover:bg-slate-50"):
            ui.checkbox(value=is_sel, on_change=on_toggle).props("dense size=xs")
            ui.icon("filter_alt" if show_curated else "circle", size="9px").style(
                f"color: {STEEL if show_curated else '#e2e8f0'}; flex-shrink: 0;"
            ).tooltip("curated picks" if show_curated else "original picks")
            # Fixed-width prettified label → operational name lands in a column.
            ui.label(pretty).style(f"width: 92px; flex-shrink: 0; font-size: 11px; color: {SLATE}; font-weight: 500;")
            ui.label(t.ts_name if pretty != t.ts_name else "").style(
                f"flex: 1; min-width: 0; font-family: monospace; font-size: 9px; color: {SLATE_MUTED}; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
            ).tooltip(t.ts_name)
            # Per-tomo curated/original choice (only where a curated set exists
            # and the tomo is selected — otherwise the choice is moot).
            if curated and is_sel:
                self._origin_toggle(cand, t.ts_name, use_orig)
            self._picks_cell(t.kept if show_curated else t.total, t.total)

    def _origin_toggle(self, cand, ts_name: str, use_orig: bool) -> None:
        """Compact mutually-exclusive curated/original segmented control."""
        with ui.row().classes("items-center").style("gap: 0; flex-shrink: 0;"):
            for label, val, radius in (("curated", False, "4px 0 0 4px"), ("orig", True, "0 4px 4px 0")):
                active = use_orig == val
                seg = ui.label(label).style(
                    f"font-size: 9px; line-height: 16px; padding: 0 6px; cursor: pointer; "
                    f"border: 1px solid {STEEL if active else '#e2e8f0'}; border-radius: {radius}; "
                    f"color: {'white' if active else SLATE_MUTED}; "
                    f"background: {STEEL if active else 'transparent'};"
                )
                seg.on("click", lambda _e, v=val: self._mutate(self.dlg.set_tomo_origin, cand, ts_name, v))

    # ---- right-aligned numeric cells (pick column aligns across all levels) ----

    _PICKS_W = 96

    def _picks_cell(self, kept: int, total: int, reviewed: int = 0) -> None:
        txt = f"{kept}/{total}" if kept != total else str(total)
        tip = f"{reviewed} curated · " if reviewed else ""
        with (
            ui.row()
            .classes("items-baseline gap-1")
            .style(f"flex-shrink: 0; width: {self._PICKS_W}px; justify-content: flex-end;")
        ):
            ui.label(txt).style(f"font-family: monospace; font-size: 10px; font-weight: 600; color: {SLATE};").tooltip(
                f"{tip}kept / total picks"
            )
            ui.label("picks").style(f"font-size: 9px; color: {SLATE_MUTED};")

    def _picks_spacer(self) -> None:
        """Hold the pick column's width when there's no count yet, so the column
        stays aligned for collapsed/unloaded rows."""
        ui.element("div").style(f"width: {self._PICKS_W}px; flex-shrink: 0;")

    @staticmethod
    def _num_cell(value: str, unit: str, accent: bool = False) -> None:
        color = SLATE if accent else SLATE_MUTED
        with ui.row().classes("items-baseline gap-1").style("flex-shrink: 0; justify-content: flex-end; width: 56px;"):
            ui.space()
            ui.label(value).style(f"font-family: monospace; font-size: 10px; font-weight: 600; color: {color};")
            if unit:
                ui.label(unit).style(f"font-size: 9px; color: {SLATE_MUTED};")

    def _render_orphans(self) -> None:
        """Sources that aren't in the discovered tree — manually-added paths or
        projects outside the recent roots. Flat rows with a remove button so
        they don't silently vanish."""
        discovered = {c.optset_path for c in self.candidates}
        orphans = [s for s in self.dlg.state.aggregation_sources if s.optset_path not in discovered]
        if not orphans:
            return
        with ui.column().classes("w-full gap-0 mt-2 pt-2").style("border-top: 1px dashed #e2e8f0;"):
            ui.label("Other sources (outside your project roots)").classes("text-[10px] text-gray-400 px-2")
            for s in orphans:
                exists = Path(s.optset_path).exists()
                with ui.row().classes("w-full items-center gap-2 px-2 py-1"):
                    ui.icon("description" if exists else "error_outline", size="13px").classes(
                        "text-green-500" if exists else "text-red-400"
                    )
                    lbl = s.project_name or Path(s.optset_path).parent.name
                    ui.label(lbl).classes("text-[10px] text-gray-700 truncate flex-1").tooltip(s.optset_path)
                    if s.tomo_names is not None:
                        ui.label(f"{len(s.tomo_names)} tomos").classes("text-[10px] text-slate-500")
                    ui.button(icon="close", on_click=lambda _e, sp=s.optset_path: self._remove_orphan(sp)).props(
                        "flat dense round size=xs"
                    ).classes("text-gray-400 hover:text-red-500")

    def _remove_orphan(self, optset_path: str) -> None:
        state = self.dlg.state
        state.aggregation_sources = [s for s in state.aggregation_sources if s.optset_path != optset_path]
        self.dlg.persist()
        self.on_change()
        self.rebuild()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def open_aggregation_merge_dialog(project_path) -> None:
    """Open the merge-sources dialog for ``project_path``. Modal, scrollable.

    Open to EVERY project since S6 — the `is_aggregation` flag it used to no-op on is
    gone. Merging picks from several projects is a capability, not a project type: the
    resolver's merged-sources candidate injection was already unconditional, and gating
    the only door to it behind a checkbox chosen at creation time meant a regular project
    could never reach it.

    This is the EXTRACTED half of one two-mode surface; the coordinate half is
    ``ui/aggregation/aggregate_dialog.py`` and the header switch moves between them. The
    dialog is parented at the page layout slot (``dialog_host``) precisely so that switch
    works: built in the caller's slot, this card would land inside the closing dialog that
    opened it and never paint."""
    from ui.aggregation.aggregate_dialog import GRADE_EXTRACTED, GRADE_TABS, open_aggregate_dialog

    d = _MergeDialog(project_path)
    ensure_assets_loaded()  # the dialog can open on pages that never mounted the dashboard

    def _switch_grade(key: str) -> None:
        if key == GRADE_EXTRACTED:
            return
        dlg.close()
        open_aggregate_dialog(project_path)

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
            render_segmented(GRADE_TABS, GRADE_EXTRACTED, _switch_grade)
            if has_merged_outputs(d.state):
                ui.badge("merged", color="green").classes("text-[10px]")
            ui.space()
            ui.button(icon="add", on_click=lambda: d.pick_manual_path()).props("flat dense round size=sm").classes(
                "text-slate-500"
            ).tooltip("Add an optimisation_set.star path outside your project roots")
            ui.button(icon="close", on_click=dlg.close).props("flat dense round size=sm").classes("text-slate-500")

        # Toolbar lives below; the selector reference is bound there.
        toolbar = ui.row().classes("w-full items-center gap-3 px-4 py-1.5 border-b border-slate-100 bg-slate-50")

        # Scrollable tree body, then a merge bar (name + run), then the registry.
        tree_body = ui.column().classes("w-full p-2 gap-0 overflow-auto bg-white").style("max-height: 48vh;")
        merge_bar = ui.row().classes("w-full items-center gap-2 px-3 py-2 border-t border-slate-200 bg-slate-50")
        footer = ui.row().classes("w-full items-center gap-2 px-3 pb-1")
        registry_holder = ui.column().classes("w-full px-3 pb-3 pt-1 overflow-auto").style("max-height: 30vh;")

        selector = _MergeSelector(d, tree_body, on_change=d.refresh_footer)

        def _set_name(e):
            d.meta["name"] = e.value or ""

        def _set_desc(e):
            d.meta["description"] = e.value or ""

        def _toggle_curated_only(e):
            selector.show_curated_only = bool(e.value)
            selector.rebuild()

        with toolbar:
            # Species picker first, mirroring the coordinate dialog's toolbar. Options are
            # filled once discovery returns (see selector.load) — the tree does not exist
            # yet at build time.
            species_select = (
                ui.select(
                    {"": "All species"},
                    label="Species",
                    on_change=lambda e: selector.set_species_filter(str(e.value or "")),
                )
                .props("dense outlined")
                .classes("text-xs")
                .style("min-width: 200px;")
            )
            selector.species_select = species_select
            ui.switch("Show curated only", value=False, on_change=_toggle_curated_only).props(
                "dense color=blue-grey"
            ).classes("text-xs").tooltip(
                "Show only sources that have a curated (filtered) set, and within them only the tomograms you reviewed."
            )
            ui.space()
            ui.icon("info", size="13px").style(f"color: {SLATE_MUTED};")
            ui.label("Curated tomograms can merge curated or original picks — toggle per row.").classes(
                "text-[10px]"
            ).style(f"color: {SLATE_MUTED};")

        # Merge bar: name the merge + run. Inputs persist (not rebuilt on
        # selection) so typing a name survives checkbox clicks.
        with merge_bar:
            name_input = (
                ui.input(placeholder="Name this merge (optional)", on_change=_set_name)
                .props("dense outlined")
                .classes("text-xs")
                .style("width: 220px;")
            )
            ui.input(placeholder="Description (optional)", on_change=_set_desc).props("dense outlined").classes(
                "text-xs"
            ).style("flex: 1;")

        # Bound to THIS dialog, not to a module global — a second tab gets its own.
        d.selector = selector
        d.footer = footer
        d.registry_holder = registry_holder
        d.name_input = name_input

        d.refresh_footer()
        d.render_registry()

    dlg.open()
    asyncio.create_task(selector.load())


def _footer_summary(sources: list) -> str:
    """ "3 sources · 1 full · 40 picked tomos", or the empty-selection line."""
    n_all = sum(1 for s in sources if s.tomo_names is None)
    n_explicit = sum(len(s.tomo_names) for s in sources if s.tomo_names is not None)
    if not sources:
        return "No sources selected"
    parts = [f"{len(sources)} source{'s' if len(sources) != 1 else ''}"]
    if n_all:
        parts.append(f"{n_all} full")
    if n_explicit:
        parts.append(f"{n_explicit} picked tomos")
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# Merge action
# ---------------------------------------------------------------------------


def _run_merge_sync(merged_dir: Path, sources: list) -> dict:
    from services.subtomo_merge import merge_optimisation_sets_into_jobdir

    merged_dir.mkdir(parents=True, exist_ok=True)
    return merge_optimisation_sets_into_jobdir(job_dir=merged_dir, additional_sources=sources, allow_no_primary=True)


def _build_merge_sources(state) -> list:
    """Turn AggregationSource entries into the driver's source dicts.

    Base path is each source's curated (filtered-if-present) optimisation_set,
    so curation done after selection is honored. `original_tomos` carries the
    per-tomo overrides the user pinned back to original picks, with
    `original_path` so the driver can pull those tomos' rows from the original.
    Manually-added directory sources are passed through for the driver to
    resolve and don't support per-tomo overrides."""
    from services.particles.picks_filter import resolve_canonical_optset

    out = []
    for s in state.aggregation_sources or []:
        p = Path(s.optset_path)
        if p.is_file() and p.name.endswith(".star"):
            canonical = resolve_canonical_optset(p.parent)
            out.append(
                {
                    "path": str(canonical) if canonical.exists() else str(p),
                    "tomos": s.tomo_names,
                    "original_path": str(p),
                    "original_tomos": list(s.original_tomos or []),
                }
            )
        else:
            out.append({"path": s.optset_path, "tomos": s.tomo_names})  # dir — driver resolves
    return out


def _metadata_warnings(sources: list[AggregationMergeSource]) -> list[str]:
    """Flag acquisition params that shouldn't be co-merged but differ across
    sources. The driver hard-blocks pixel-size mismatch; box/binning are
    softer, surfaced here so the user notices."""
    out: list[str] = []
    for attr, label in (("box_size", "box size"), ("pixel_size", "pixel size"), ("binning", "binning")):
        vals = sorted({getattr(s, attr) for s in sources if getattr(s, attr) is not None})
        if len(vals) > 1:
            out.append(f"Mixed {label}: {', '.join(str(v) for v in vals)}")
    return out


def _build_merge_record(state, slug: str, name: str, description: str, summary: dict) -> AggregationMerge:
    """Assemble the registry record from the driver summary + the per-source
    project/species labels (joined by subtomo job dir)."""
    src_by_dir: dict[str, AggregationSource] = {}
    for s in state.aggregation_sources or []:
        try:
            src_by_dir[str(Path(s.optset_path).parent.resolve())] = s
        except Exception:
            pass

    rows: list[AggregationMergeSource] = []
    for ss in summary.get("sources", []):
        opt = ss.get("optimisation_set") or ss.get("source_input") or ""
        jobdir = str(Path(opt).parent.resolve()) if opt else ""
        agg = src_by_dir.get(jobdir)
        rows.append(
            AggregationMergeSource(
                project_name=(agg.project_name if agg else (Path(jobdir).parent.name if jobdir else "")),
                species_label=(agg.species_label if agg else ""),
                n_particles=int(ss.get("n_particles") or 0),
                n_tomograms=len(ss.get("tomo_names") or []),
                box_size=ss.get("box_size"),
                pixel_size=ss.get("pixel_size"),
                binning=ss.get("binning"),
            )
        )

    totals = summary.get("totals", {})
    return AggregationMerge(
        slug=slug,
        name=name,
        description=description,
        n_particles=int(totals.get("n_particles") or 0),
        n_tomograms=int(totals.get("n_tomograms") or 0),
        n_sources=int(totals.get("n_sources") or len(rows)),
        sources=rows,
        warnings=_metadata_warnings(rows),
    )


# ---------------------------------------------------------------------------
# Merge registry — recorded merges, what made the cut, which one is active
# ---------------------------------------------------------------------------


def _render_registry(d: _MergeDialog, container) -> None:
    container.clear()
    state = d.state
    merges = list(reversed(state.aggregation_merges or []))  # newest first
    with container:
        if not merges:
            ui.label("No merges recorded yet — select sources above, name the merge, and click Merge.").classes(
                "text-xs text-slate-400 italic px-1"
            )
            return
        active = _active_merge(state)
        active_slug = active.slug if active else ""
        with ui.row().classes("w-full items-center gap-2 mb-1"):
            ui.label("Merge registry").style(
                f"font-size: 10px; font-weight: 700; letter-spacing: 0.05em; color: {SLATE_MUTED}; "
                "text-transform: uppercase;"
            )
            ui.label(f"{len(merges)} merge{'s' if len(merges) != 1 else ''}").classes("text-[10px] text-slate-400")
        for m in merges:
            _render_merge_record(d, m, m.slug == active_slug)


def _render_merge_record(d: _MergeDialog, m: AggregationMerge, is_active: bool) -> None:
    expanded = m.slug in d.expanded_registry
    border = STEEL if is_active else "#e2e8f0"
    with (
        ui.element("div")
        .classes("w-full")
        .style(f"border: 1px solid {border}; border-radius: 6px; margin-bottom: 6px; overflow: hidden;")
    ):
        # Header line
        head = (
            ui.row()
            .classes("w-full items-center gap-2 px-2 py-1.5 cursor-pointer hover:bg-slate-50")
            .style("background: #fafbfc;")
        )
        head.on("click", lambda _e, s=m.slug: d.toggle_registry(s))
        with head:
            ui.icon("expand_more" if expanded else "chevron_right", size="15px").classes("text-slate-400")
            if is_active:
                ui.icon("radio_button_checked", size="14px").style(f"color: {STEEL};").tooltip(
                    "Active — feeds downstream jobs"
                )
            else:
                act = (
                    ui.icon("radio_button_unchecked", size="14px")
                    .classes("text-slate-300 cursor-pointer")
                    .tooltip("Make active (re-point downstream jobs here)")
                )
                act.on("click.stop", lambda _e, s=m.slug: d.set_active_merge(s))
            ui.label(m.name or m.slug).classes("text-xs font-semibold text-slate-700 truncate").style(
                "flex: 1; min-width: 0;"
            )
            if m.warnings:
                ui.icon("warning", size="13px").classes("text-amber-500").tooltip("; ".join(m.warnings))
            ui.label(m.created_at.strftime("%Y-%m-%d %H:%M") if m.created_at else "").classes(
                "text-[9px] font-mono text-slate-400"
            )
            _reg_stat(f"{m.n_particles:,}", "picks")
            _reg_stat(str(m.n_tomograms), "tomos")
            _reg_stat(str(m.n_sources), "src")

        if expanded:
            with ui.element("div").classes("w-full").style("padding: 4px 8px 8px; background: white;"):
                if m.description:
                    ui.label(m.description).classes("text-[11px] text-slate-500 italic mb-1")
                _render_merge_table(m)
                for w in m.warnings:
                    with ui.row().classes("items-center gap-1 mt-1"):
                        ui.icon("warning", size="12px").classes("text-amber-500")
                        ui.label(w).classes("text-[10px] text-amber-700")
                optset = (d.merge_dir_for(m.slug) / "optimisation_set.star") if d.merge_dir_for(m.slug) else None
                if optset is not None:
                    with ui.row().classes("items-center gap-1 mt-1"):
                        ui.label(str(optset)).classes("text-[9px] font-mono text-slate-400 truncate").style(
                            "flex: 1; min-width: 0;"
                        ).tooltip(str(optset))
                        ui.button(
                            icon="content_copy",
                            on_click=lambda _e, p=str(optset): (
                                ui.clipboard.write(p),
                                ui.notify("Path copied", type="info", timeout=1200),
                            ),
                        ).props("flat dense round size=xs").classes("text-slate-400")


def _render_merge_table(m: AggregationMerge) -> None:
    """Boring-on-purpose table of what made the cut: one row per contributing
    (project, species), with picks/tomos and the acquisition params."""

    def cell(text, w, *, mono=False, header=False, color=None):
        c = color or (SLATE_MUTED if header else SLATE)
        ui.label(text).style(
            f"width: {w}; flex-shrink: 0; font-size: {'9px' if header else '10px'}; "
            f"color: {c}; {'font-family: monospace;' if mono else ''} "
            f"{'font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em;' if header else ''}"
        )

    def grow(text, *, header=False):
        ui.label(text).style(
            f"flex: 1; min-width: 0; font-size: {'9px' if header else '10px'}; "
            f"color: {SLATE_MUTED if header else SLATE}; overflow: hidden; text-overflow: ellipsis; "
            f"white-space: nowrap; {'font-weight: 600; text-transform: uppercase;' if header else ''}"
        )

    with ui.element("div").classes("w-full").style("border-top: 1px solid #eef2f6;"):
        with ui.row().classes("w-full items-center gap-2 py-1").style("border-bottom: 1px solid #eef2f6;"):
            grow("Project", header=True)
            cell("Species", "96px", header=True)
            cell("Picks", "56px", header=True)
            cell("Tomos", "48px", header=True)
            cell("Box", "44px", header=True)
            cell("Å/px", "52px", header=True)
            cell("Bin", "40px", header=True)
        for s in m.sources:
            with ui.row().classes("w-full items-center gap-2 py-0.5"):
                grow(s.project_name or "—")
                cell(s.species_label or "—", "96px")
                cell(f"{s.n_particles:,}", "56px", mono=True)
                cell(str(s.n_tomograms), "48px", mono=True)
                cell(str(s.box_size) if s.box_size is not None else "—", "44px", mono=True)
                cell(f"{s.pixel_size:.3g}" if s.pixel_size is not None else "—", "52px", mono=True)
                cell(f"{s.binning:.3g}" if s.binning is not None else "—", "40px", mono=True)


def _reg_stat(value: str, unit: str) -> None:
    with ui.row().classes("items-baseline gap-1").style("flex-shrink: 0; justify-content: flex-end;"):
        ui.label(value).style(f"font-family: monospace; font-size: 10px; font-weight: 600; color: {SLATE};")
        ui.label(unit).style(f"font-size: 9px; color: {SLATE_MUTED};")
