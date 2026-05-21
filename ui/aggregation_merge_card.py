"""
Merge-sources card for aggregation projects.

Lives at the top of the workspace (above the pipeline tabs) and lets the user
build up a list of upstream optimisation_set.star sources, then merge them into
<project>/MergedSources/. The output is a project-level resource that any
downstream job (Reconstruct/Class3D/Refine3D/...) can read via a manual:
override on its input_optimisation slot.

Selection is a navigable hierarchy — Project → Species → Tomogram — so the user
sees what's actually inside each source (per-tomogram pick counts, curation
state) and can fine-select down to individual tomograms. The selection persists
as `AggregationSource` entries on ProjectState; a `tomo_names` of None means
"all tomograms in that set".
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from nicegui import ui, run

from services.project_state import (
    AggregationMerge,
    AggregationMergeSource,
    AggregationSource,
    get_project_state,
)
from ui.components.task_utils import ts_position_sort_key, ts_pretty_name
from ui.local_file_picker import local_file_picker
from ui.projects_overview import avatar_color

log = logging.getLogger(__name__)


# Project-relative location for merged outputs. Stable name, not under External/
# so it can't collide with the schemer's jobNNN allocation.
MERGED_DIR_NAME = "MergedSources"

# Steelblue is the single accent — reserved for the curated/uncurated highlight.
# Everything else stays neutral slate.
STEEL = "#4682b4"
SLATE = "#475569"
SLATE_MUTED = "#94a3b8"


def _merged_root() -> Optional[Path]:
    state = get_project_state()
    if state.project_path is None:
        return None
    return state.project_path / MERGED_DIR_NAME


def _merge_dir_for(slug: str) -> Optional[Path]:
    root = _merged_root()
    return (root / slug) if root else None


def _active_merge(state) -> Optional[AggregationMerge]:
    """The merge downstream consumers use: the explicitly-active one, else the
    newest recorded merge."""
    merges = state.aggregation_merges or []
    if not merges:
        return None
    if state.active_merge_slug:
        m = next((x for x in merges if x.slug == state.active_merge_slug), None)
        if m:
            return m
    return merges[-1]


def active_merged_optset(state) -> Optional[Path]:
    """Resolved optimisation_set.star of the active merge. Falls back to a
    legacy MergedSources/optimisation_set.star (pre-registry projects)."""
    root = getattr(state, "project_path", None)
    if root is None:
        return None
    m = _active_merge(state)
    if m is not None:
        p = root / MERGED_DIR_NAME / m.slug / "optimisation_set.star"
        if p.exists():
            return p
    legacy = root / MERGED_DIR_NAME / "optimisation_set.star"
    return legacy if legacy.exists() else None


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


_pending_save_task = None


def _persist_state() -> None:
    """Persist deferred + off the event loop. A full ProjectState.save() does a
    model_dump of every job/species + a JSON disk write (~hundreds of ms on a
    real project), so doing it inline made each checkbox click hang. Debounce
    rapid toggles and run the write in a thread."""
    global _pending_save_task
    state = get_project_state()
    state.update_modified()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        state.save()  # no loop (shouldn't happen from a handler) — save inline
        return
    if _pending_save_task and not _pending_save_task.done():
        _pending_save_task.cancel()
    _pending_save_task = loop.create_task(_debounced_save(state))


async def _debounced_save(state) -> None:
    try:
        await asyncio.sleep(0.4)
    except asyncio.CancelledError:
        return
    await run.io_bound(state.save)


def apply_aggregation_overrides(state) -> int:
    """For aggregation projects with a completed merge, point every consumer
    job's input_optimisation slot at MergedSources/optimisation_set.star.
    Idempotent. Returns count of jobs updated.

    Belt-and-braces: writes BOTH `source_overrides[slot]` (the resolver path)
    AND `paths[slot]` (the value the driver reads at run time). That way the
    job is correctly wired even if the IO config UI fails to reflect the
    override or the path resolver is bypassed at deploy time.

    Also clears stale `is_orphaned` / `missing_inputs` markers since they
    were written before the override existed.

    Called from three sites:
      - When a new RP/Class3D/Refine3D is added in an aggregation project.
      - After a successful merge (retro-wires already-added consumers).
      - On every workspace render (idempotent self-heal for jobs that pre-date
        either of the above hooks).
    """
    from services.io_slots import JobFileType

    if not getattr(state, "is_aggregation", False):
        return 0
    optset = active_merged_optset(state)
    if optset is None or not optset.exists():
        log.debug("apply_aggregation_overrides: no active merged optset")
        return 0

    optset_str = str(optset)
    override_value = f"manual:{optset_str}"
    updated = 0
    for instance_id, job_model in state.jobs.items():
        schema = getattr(type(job_model), "INPUT_SCHEMA", None) or []
        slot_keys = [
            s.key for s in schema if s.accepts and JobFileType.OPTIMISATION_SET_STAR in s.accepts
        ]
        if not slot_keys:
            continue
        if getattr(job_model, "source_overrides", None) is None:
            job_model.source_overrides = {}
        if getattr(job_model, "paths", None) is None:
            job_model.paths = {}
        changed = False
        for k in slot_keys:
            if job_model.source_overrides.get(k) != override_value:
                job_model.source_overrides[k] = override_value
                changed = True
            # Pre-populate paths so the driver finds the optset even if path
            # resolution at deploy time somehow loses the override.
            if job_model.paths.get(k) != optset_str:
                job_model.paths[k] = optset_str
                changed = True
        # Clear stale orphan markers — they were written before the override
        # existed and would otherwise stick around forever.
        if changed:
            if getattr(job_model, "is_orphaned", False):
                job_model.is_orphaned = False
            if getattr(job_model, "missing_inputs", None):
                job_model.missing_inputs = []
            updated += 1
            log.info(
                "apply_aggregation_overrides: wired %s slots %s -> %s",
                instance_id, slot_keys, optset_str,
            )
    return updated


def has_merged_outputs() -> bool:
    """True if the current project has at least one usable merged optset. Lets
    the sidebar render a 'merged' badge without opening the dialog."""
    return active_merged_optset(get_project_state()) is not None


# ---------------------------------------------------------------------------
# Selection helpers — operate on state.aggregation_sources (List[AggregationSource])
# ---------------------------------------------------------------------------


def _find_source(optset_path: str) -> Optional[AggregationSource]:
    return next((s for s in get_project_state().aggregation_sources if s.optset_path == optset_path), None)


def _selected_tomos(optset_path: str, all_tomos: List[str]) -> set:
    """Currently-selected tomo names for a source. tomo_names=None => all."""
    src = _find_source(optset_path)
    if src is None:
        return set()
    if src.tomo_names is None:
        return set(all_tomos)
    return set(src.tomo_names)


def _selection_label(src: Optional[AggregationSource], n_total: Optional[int]) -> str:
    """Compact "selected" descriptor for a (collapsed) species row."""
    if src is None:
        return ""
    if src.tomo_names is None:
        return f"all{f' {n_total}' if n_total else ''} tomos"
    return f"{len(src.tomo_names)}{f'/{n_total}' if n_total else ''} tomos"


def _set_species_selection(cand, selected: set, all_tomos: List[str]) -> None:
    """Replace the source entry for one (project, species) with the given tomo
    selection. Empty selection removes the source; full selection normalizes to
    tomo_names=None (=all). Per-tomo original overrides are preserved (pruned to
    the tomos still selected)."""
    state = get_project_state()
    prev = _find_source(cand.optset_path)
    prev_orig = set(prev.original_tomos) if prev else set()
    srcs = [s for s in state.aggregation_sources if s.optset_path != cand.optset_path]
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
    state.aggregation_sources = srcs
    _persist_state()


def _set_tomo_origin(cand, ts_name: str, use_original: bool) -> None:
    """Pin a single tomogram to original (True) or curated (False) picks. The
    species must already contribute a source; a no-op otherwise (the toggle is
    only shown for included tomos)."""
    src = _find_source(cand.optset_path)
    if src is None:
        return
    orig = set(src.original_tomos or [])
    if use_original:
        orig.add(ts_name)
    else:
        orig.discard(ts_name)
    src.original_tomos = sorted(orig)
    _persist_state()


def _toggle_species_all(cand, on: bool) -> None:
    """Master toggle for a whole species: select all tomos (tomo_names=None) or
    remove the source entirely. Preserves per-tomo original overrides on select."""
    state = get_project_state()
    prev = _find_source(cand.optset_path)
    prev_orig = list(prev.original_tomos) if prev else []
    srcs = [s for s in state.aggregation_sources if s.optset_path != cand.optset_path]
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
    state.aggregation_sources = srcs
    _persist_state()


# ---------------------------------------------------------------------------
# Hierarchical selector
# ---------------------------------------------------------------------------


class _MergeSelector:
    """Project → Species → Tomogram selection tree for the merge dialog.

    Discovery (cross-project scan) is async + cached. Per-tomogram curation is
    loaded lazily when a species node is expanded — reading every particles.star
    up front would not scale (PICKS_FILTER_AGGREGATION_ROADMAP.md §scale)."""

    def __init__(self, body: ui.element, on_change) -> None:
        self.body = body
        self.on_change = on_change  # called after any selection mutation
        self.tree: Optional[ui.element] = None  # rebuilt subtree (filter input persists)
        self.candidates: list = []
        self.by_project: Dict[str, list] = {}
        self.project_meta: Dict[str, dict] = {}
        self.expanded_projects: set = set()
        self.expanded_species: set = set()
        self.curation: Dict[str, list] = {}  # optset_path -> List[TomoCuration]
        self.filter = ""
        self.show_curated_only = False

    async def load(self) -> None:
        from services.aggregation_discovery import discover_subtomo_optimisation_sets
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
        # Auto-expand projects that already contribute a selected source.
        selected_paths = {s.optset_path for s in get_project_state().aggregation_sources}
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
                self.project_meta[c.project_path] = {
                    "name": c.project_name,
                    "mnemonic": c.mnemonic,
                    "is_aggregation": c.is_aggregation,
                }

    # ---- async expansion (lazy curation load) ----

    async def _ensure_curation(self, cand) -> None:
        if cand.optset_path not in self.curation:
            from services.aggregation_discovery import load_tomo_curation

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

    def _filtered_candidates(self) -> list:
        cands = list(self.candidates)
        if self.show_curated_only:
            cands = [c for c in cands if c.has_filter]
        f = (self.filter or "").strip().lower()
        if not f:
            return cands
        return [
            c for c in cands
            if f in c.project_name.lower()
            or f in c.instance_id.lower()
            or (c.species_label and f in c.species_label.lower())
            or (c.mnemonic and f in c.mnemonic.lower())
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
        sel_species = sum(1 for c in cands if _find_source(c.optset_path) is not None)
        n_tomos = sum(c.n_tomograms or 0 for c in cands)

        header = ui.row().classes(
            "w-full items-center gap-2 px-2 py-1.5 cursor-pointer hover:bg-slate-50"
        ).style("border-bottom: 1px solid #eef2f6;")
        header.on("click", lambda _e, p=project_path: asyncio.create_task(self._toggle_project(p)))
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
        src = _find_source(cand.optset_path)
        sp_color = cand.species_color or SLATE
        expanded = cand.optset_path in self.expanded_species
        fully_selected = src is not None and src.tomo_names is None
        partial = src is not None and src.tomo_names is not None
        cur = self.curation.get(cand.optset_path)

        with ui.row().classes("w-full items-center gap-1.5 px-2 py-1 hover:bg-slate-50"):
            ui.checkbox(
                value=fully_selected,
                on_change=lambda e, c=cand: self._mutate(_toggle_species_all, c, bool(e.value)),
            ).props("dense size=xs")

            arrow = ui.icon("expand_more" if expanded else "chevron_right", size="14px").classes(
                "text-slate-400 cursor-pointer"
            )
            arrow.on("click", lambda _e, c=cand: asyncio.create_task(self._toggle_species(c)))

            if cand.species_label:
                ui.label(cand.species_label).style(
                    f"color: {sp_color}; font-size: 11px; font-weight: 600; flex-shrink: 0;"
                )
            else:
                ui.label("Unassigned").classes("text-[11px] text-slate-400 italic").style("flex-shrink: 0;")
            ui.label(cand.instance_id).classes("text-[10px] font-mono text-slate-400 truncate").style(
                "flex: 1; min-width: 0;"
            ).tooltip("SubtomoExtraction job instance")

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

    def _render_tomos(self, cand) -> None:
        tomos = self.curation.get(cand.optset_path) or []
        if self.show_curated_only:
            tomos = [t for t in tomos if t.reviewed and t.kept is not None]
        all_tomos = [t.ts_name for t in (self.curation.get(cand.optset_path) or [])]
        selected = _selected_tomos(cand.optset_path, all_tomos)

        with ui.element("div").classes("w-full").style("padding-left: 30px;"):
            if not tomos:
                ui.label("No tomograms in this set.").classes("text-[10px] text-slate-400 italic px-2 py-1")
                return
            # Sort by (stage, beam) descending so positions read high → low.
            for t in sorted(tomos, key=lambda x: ts_position_sort_key(x.ts_name), reverse=True):
                self._render_tomo_row(cand, t, selected, all_tomos)

    def _render_tomo_row(self, cand, t, selected: set, all_tomos: List[str]) -> None:
        is_sel = t.ts_name in selected
        src = _find_source(cand.optset_path)
        use_orig = bool(src and t.ts_name in (src.original_tomos or []))
        # "reviewed" — not merely has_filter — marks a genuine curation: the
        # filtered star carries all rows for tomos the user never reviewed.
        curated = t.reviewed and t.kept is not None
        show_curated = curated and not use_orig

        def on_toggle(e, c=cand, ts=t.ts_name, allt=all_tomos):
            sel = _selected_tomos(c.optset_path, allt)
            if e.value:
                sel.add(ts)
            else:
                sel.discard(ts)
            self._mutate(_set_species_selection, c, sel, allt)

        pretty = ts_pretty_name(t.ts_name)
        with ui.row().classes("w-full items-center gap-1.5 px-2 py-0.5 hover:bg-slate-50"):
            ui.checkbox(value=is_sel, on_change=on_toggle).props("dense size=xs")
            ui.icon("filter_alt" if show_curated else "circle", size="9px").style(
                f"color: {STEEL if show_curated else '#e2e8f0'}; flex-shrink: 0;"
            ).tooltip("curated picks" if show_curated else "original picks")
            # Fixed-width prettified label → operational name lands in a column.
            ui.label(pretty).style(
                f"width: 92px; flex-shrink: 0; font-size: 11px; color: {SLATE}; font-weight: 500;"
            )
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
                seg.on("click", lambda _e, v=val: self._mutate(_set_tomo_origin, cand, ts_name, v))

    # ---- right-aligned numeric cells (pick column aligns across all levels) ----

    _PICKS_W = 96

    def _picks_cell(self, kept: int, total: int, reviewed: int = 0) -> None:
        txt = f"{kept}/{total}" if kept != total else str(total)
        tip = f"{reviewed} curated · " if reviewed else ""
        with ui.row().classes("items-baseline gap-1").style(
            f"flex-shrink: 0; width: {self._PICKS_W}px; justify-content: flex-end;"
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
        orphans = [s for s in get_project_state().aggregation_sources if s.optset_path not in discovered]
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
        state = get_project_state()
        state.aggregation_sources = [s for s in state.aggregation_sources if s.optset_path != optset_path]
        _persist_state()
        self.on_change()
        self.rebuild()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def open_aggregation_merge_dialog() -> None:
    """Open the merge-sources dialog. Modal, scrollable. No-op if the current
    project isn't flagged as aggregation."""
    state = get_project_state()
    if not getattr(state, "is_aggregation", False):
        return

    with ui.dialog() as dlg, ui.card().classes(
        "w-[1060px] max-w-[96vw] max-h-[92vh] overflow-hidden border border-slate-200 bg-white p-0"
    ).style("color: #1e293b;"):
        with ui.row().classes("w-full items-center gap-2 px-4 py-2 border-b border-slate-200 bg-slate-50"):
            ui.icon("merge_type", size="20px").style(f"color: {SLATE};")
            ui.label("Merge sources (aggregation project)").classes("text-sm font-bold text-slate-700")
            if has_merged_outputs():
                ui.badge("merged", color="green").classes("text-[10px]")
            ui.space()
            ui.button(icon="add", on_click=_pick_manual_path).props("flat dense round size=sm").classes(
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

        selector = _MergeSelector(tree_body, on_change=lambda: _refresh_footer(footer, selector))

        merge_meta: Dict[str, str] = {"name": "", "description": ""}

        def _set_name(e):
            merge_meta["name"] = e.value or ""

        def _set_desc(e):
            merge_meta["description"] = e.value or ""

        def _toggle_curated_only(e):
            selector.show_curated_only = bool(e.value)
            selector.rebuild()

        with toolbar:
            ui.switch("Show curated only", value=False, on_change=_toggle_curated_only).props(
                "dense color=blue-grey"
            ).classes("text-xs").tooltip(
                "Show only sources that have a curated (filtered) set, and within them only the "
                "tomograms you reviewed."
            )
            ui.space()
            ui.icon("info", size="13px").style(f"color: {SLATE_MUTED};")
            ui.label("Curated tomograms can merge curated or original picks — toggle per row.").classes(
                "text-[10px]"
            ).style(f"color: {SLATE_MUTED};")

        # Merge bar: name the merge + run. Inputs persist (not rebuilt on
        # selection) so typing a name survives checkbox clicks.
        with merge_bar:
            name_input = ui.input(placeholder="Name this merge (optional)", on_change=_set_name).props(
                "dense outlined"
            ).classes("text-xs").style("width: 220px;")
            ui.input(placeholder="Description (optional)", on_change=_set_desc).props("dense outlined").classes(
                "text-xs"
            ).style("flex: 1;")

        # Stash so handlers can refresh pieces.
        _DIALOG_REFS["selector"] = selector
        _DIALOG_REFS["footer"] = footer
        _DIALOG_REFS["registry"] = registry_holder
        _DIALOG_REFS["merge_meta"] = merge_meta
        _DIALOG_REFS["name_input"] = name_input

        _refresh_footer(footer, selector)
        _render_registry(registry_holder)

    dlg.open()
    asyncio.create_task(selector.load())


_DIALOG_REFS: Dict[str, object] = {}


def _refresh_footer(footer: ui.element, selector: "_MergeSelector") -> None:
    footer.clear()
    sources = list(get_project_state().aggregation_sources or [])
    n_sources = len(sources)
    # Total selected tomos: explicit count, or "all" sources flagged separately.
    n_all = sum(1 for s in sources if s.tomo_names is None)
    n_explicit = sum(len(s.tomo_names) for s in sources if s.tomo_names is not None)
    parts = []
    if n_sources:
        parts.append(f"{n_sources} source{'s' if n_sources != 1 else ''}")
        if n_all:
            parts.append(f"{n_all} full")
        if n_explicit:
            parts.append(f"{n_explicit} picked tomos")
    label = " · ".join(parts) if parts else "No sources selected"
    with footer:
        ui.label(label).classes("text-xs text-slate-600")
        ui.space()
        merge_btn = ui.button(
            f"Merge {n_sources} source(s)" if n_sources else "Merge",
            icon="merge_type",
            on_click=_run_merge,
        ).props("unelevated no-caps").classes("text-xs bg-slate-700 text-white")
        if not n_sources:
            merge_btn.disable()


async def _pick_manual_path() -> None:
    state = get_project_state()
    start_dir = str(state.project_path) if state.project_path else "/"
    picker = local_file_picker(start_dir, upper_limit=None, mode="directory")
    result = await picker
    if not result or not result[0]:
        return
    chosen = result[0]
    if any(s.optset_path == chosen for s in (state.aggregation_sources or [])):
        ui.notify("Already in list", type="warning", timeout=2000)
        return
    state.aggregation_sources = list(state.aggregation_sources or []) + [
        AggregationSource(optset_path=chosen, project_name=Path(chosen).parent.name)
    ]
    _persist_state()
    ui.notify(f"Added: {Path(chosen).name}", type="positive", timeout=1500)
    selector = _DIALOG_REFS.get("selector")
    footer = _DIALOG_REFS.get("footer")
    if isinstance(selector, _MergeSelector):
        selector.rebuild()
    if footer is not None and isinstance(selector, _MergeSelector):
        _refresh_footer(footer, selector)


# ---------------------------------------------------------------------------
# Merge action
# ---------------------------------------------------------------------------


def _run_merge_sync(merged_dir: Path, sources: list) -> dict:
    from drivers.subtomo_merge import merge_optimisation_sets_into_jobdir

    merged_dir.mkdir(parents=True, exist_ok=True)
    return merge_optimisation_sets_into_jobdir(
        job_dir=merged_dir, additional_sources=sources, allow_no_primary=True
    )


def _build_merge_sources(state) -> list:
    """Turn AggregationSource entries into the driver's source dicts.

    Base path is each source's curated (filtered-if-present) optimisation_set,
    so curation done after selection is honored. `original_tomos` carries the
    per-tomo overrides the user pinned back to original picks, with
    `original_path` so the driver can pull those tomos' rows from the original.
    Manually-added directory sources are passed through for the driver to
    resolve and don't support per-tomo overrides."""
    from services.visualization.picks_filter import resolve_canonical_optset

    out = []
    for s in state.aggregation_sources or []:
        p = Path(s.optset_path)
        if p.is_file() and p.name.endswith(".star"):
            canonical = resolve_canonical_optset(p.parent)
            out.append({
                "path": str(canonical) if canonical.exists() else str(p),
                "tomos": s.tomo_names,
                "original_path": str(p),
                "original_tomos": list(s.original_tomos or []),
            })
        else:
            out.append({"path": s.optset_path, "tomos": s.tomo_names})  # dir — driver resolves
    return out


def _metadata_warnings(sources: List[AggregationMergeSource]) -> List[str]:
    """Flag acquisition params that shouldn't be co-merged but differ across
    sources. The driver hard-blocks pixel-size mismatch; box/binning are
    softer, surfaced here so the user notices."""
    out: List[str] = []
    for attr, label in (("box_size", "box size"), ("pixel_size", "pixel size"), ("binning", "binning")):
        vals = sorted({getattr(s, attr) for s in sources if getattr(s, attr) is not None})
        if len(vals) > 1:
            out.append(f"Mixed {label}: {', '.join(str(v) for v in vals)}")
    return out


def _build_merge_record(state, slug: str, name: str, description: str, summary: dict) -> AggregationMerge:
    """Assemble the registry record from the driver summary + the per-source
    project/species labels (joined by subtomo job dir)."""
    src_by_dir: Dict[str, AggregationSource] = {}
    for s in state.aggregation_sources or []:
        try:
            src_by_dir[str(Path(s.optset_path).parent.resolve())] = s
        except Exception:
            pass

    rows: List[AggregationMergeSource] = []
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


async def _run_merge() -> None:
    state = get_project_state()
    if not state.aggregation_sources:
        ui.notify("Add at least one source first.", type="warning", timeout=2500)
        return
    root = _merged_root()
    if root is None:
        ui.notify("No project loaded — cannot merge.", type="negative", timeout=4000)
        return

    meta = _DIALOG_REFS.get("merge_meta") or {}
    name = (meta.get("name") or "").strip()
    description = (meta.get("description") or "").strip()
    slug = _slugify(name, {m.slug for m in (state.aggregation_merges or [])})
    merged_dir = root / slug

    sources = _build_merge_sources(state)
    ui.notify("Merging…", type="info", timeout=2500)
    try:
        summary = await run.io_bound(_run_merge_sync, merged_dir, sources)
    except Exception as e:
        ui.notify(f"Merge failed: {e}", type="negative", timeout=8000)
        return

    record = _build_merge_record(state, slug, name or slug, description, summary)
    state.aggregation_merges = list(state.aggregation_merges or []) + [record]
    state.active_merge_slug = slug  # newest becomes active
    # Wire downstream consumers to the new active optset.
    n_wired = apply_aggregation_overrides(state)
    await run.io_bound(state.save)  # persist the registry now, not debounced

    msg = f"Merge “{record.name}” complete"
    if n_wired:
        msg += f" · wired {n_wired} downstream job{'s' if n_wired != 1 else ''}"
    ui.notify(msg, type="positive", timeout=4000)
    if isinstance(meta, dict):
        meta["name"] = ""  # clear so the next merge needs a fresh name
    name_input = _DIALOG_REFS.get("name_input")
    if name_input is not None:
        name_input.value = ""
    _refresh_registry()


# ---------------------------------------------------------------------------
# Merge registry — recorded merges, what made the cut, which one is active
# ---------------------------------------------------------------------------

_registry_expanded: set = set()


def _refresh_registry() -> None:
    holder = _DIALOG_REFS.get("registry")
    if holder is not None:
        _render_registry(holder)


def _set_active_merge(slug: str) -> None:
    state = get_project_state()
    state.active_merge_slug = slug
    apply_aggregation_overrides(state)
    _persist_state()
    _refresh_registry()


def _render_registry(container) -> None:
    container.clear()
    state = get_project_state()
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
            _render_merge_record(m, m.slug == active_slug)


def _render_merge_record(m: AggregationMerge, is_active: bool) -> None:
    expanded = m.slug in _registry_expanded
    border = STEEL if is_active else "#e2e8f0"
    with ui.element("div").classes("w-full").style(
        f"border: 1px solid {border}; border-radius: 6px; margin-bottom: 6px; overflow: hidden;"
    ):
        # Header line
        head = ui.row().classes("w-full items-center gap-2 px-2 py-1.5 cursor-pointer hover:bg-slate-50").style(
            "background: #fafbfc;"
        )
        head.on("click", lambda _e, s=m.slug: _toggle_registry(s))
        with head:
            ui.icon("expand_more" if expanded else "chevron_right", size="15px").classes("text-slate-400")
            if is_active:
                ui.icon("radio_button_checked", size="14px").style(f"color: {STEEL};").tooltip(
                    "Active — feeds downstream jobs"
                )
            else:
                act = ui.icon("radio_button_unchecked", size="14px").classes("text-slate-300 cursor-pointer").tooltip(
                    "Make active (re-point downstream jobs here)"
                )
                act.on("click.stop", lambda _e, s=m.slug: _set_active_merge(s))
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
                optset = (_merge_dir_for(m.slug) / "optimisation_set.star") if _merge_dir_for(m.slug) else None
                if optset is not None:
                    with ui.row().classes("items-center gap-1 mt-1"):
                        ui.label(str(optset)).classes("text-[9px] font-mono text-slate-400 truncate").style(
                            "flex: 1; min-width: 0;"
                        ).tooltip(str(optset))
                        ui.button(
                            icon="content_copy",
                            on_click=lambda _e, p=str(optset): (
                                ui.clipboard.write(p), ui.notify("Path copied", type="info", timeout=1200)
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


def _toggle_registry(slug: str) -> None:
    if slug in _registry_expanded:
        _registry_expanded.discard(slug)
    else:
        _registry_expanded.add(slug)
    _refresh_registry()
