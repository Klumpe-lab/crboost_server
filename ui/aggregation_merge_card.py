"""
Merge-sources card for aggregation projects.

Lives at the top of the workspace (above the pipeline tabs) and lets the user
build up a list of upstream optimisation_set.star sources, then merge them into
<project>/MergedSources/. The output is a project-level resource that any
downstream job (Reconstruct/Class3D/Refine3D/...) can read via a manual:
override on its input_optimisation slot.

Replaces the in-job merge panel for aggregation projects (the SubtomoExtraction
node is no longer auto-added to aggregation projects).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

from nicegui import ui, run

from services.project_state import get_project_state
from ui.local_file_picker import local_file_picker


# Project-relative location for merged outputs. Stable name, not under External/
# so it can't collide with the schemer's jobNNN allocation.
MERGED_DIR_NAME = "MergedSources"


def _merged_dir() -> Optional[Path]:
    state = get_project_state()
    if state.project_path is None:
        return None
    return state.project_path / MERGED_DIR_NAME


def _merged_optset() -> Optional[Path]:
    d = _merged_dir()
    return (d / "optimisation_set.star") if d else None


def _merged_summary_path() -> Optional[Path]:
    d = _merged_dir()
    return (d / "merge_summary.json") if d else None


def _read_merged_summary() -> Optional[dict]:
    p = _merged_summary_path()
    if p is None or not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _persist_state() -> None:
    """Save ProjectState to project_params.json synchronously."""
    state = get_project_state()
    state.update_modified()
    state.save()


def merged_optset_path_for(project_path: Optional[Path]) -> Optional[Path]:
    """Where the merged optimisation_set.star lives for an aggregation project."""
    if project_path is None:
        return None
    return project_path / MERGED_DIR_NAME / "optimisation_set.star"


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
    import logging
    from services.io_slots import JobFileType

    log = logging.getLogger(__name__)

    if not getattr(state, "is_aggregation", False):
        return 0
    optset = merged_optset_path_for(getattr(state, "project_path", None))
    if optset is None or not optset.exists():
        log.debug(
            "apply_aggregation_overrides: no merged optset at %s",
            optset if optset else "(no project_path)",
        )
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def has_merged_outputs() -> bool:
    """True if MergedSources/optimisation_set.star already exists for the
    current project. Lets the sidebar render a 'merged' badge without opening
    the dialog."""
    optset = _merged_optset()
    return bool(optset and optset.exists())


def open_aggregation_merge_dialog() -> None:
    """Open the merge-sources dialog. Modal, scrollable. No-op if the current
    project isn't flagged as aggregation."""
    state = get_project_state()
    if not getattr(state, "is_aggregation", False):
        return

    with ui.dialog() as dlg, ui.card().classes(
        "w-[820px] max-w-[92vw] max-h-[88vh] overflow-auto border border-purple-200 bg-purple-50/30"
    ):
        with ui.row().classes("w-full items-center gap-2 px-4 py-2 border-b border-purple-100"):
            ui.icon("merge_type", size="20px").classes("text-purple-600")
            ui.label("Merge sources (aggregation project)").classes("text-sm font-bold text-gray-800")
            ui.badge("aggregation", color="purple").props("outline").classes("text-[10px]")
            if has_merged_outputs():
                ui.badge("merged", color="green").classes("text-[10px]")
            ui.space()
            ui.button(icon="close", on_click=dlg.close).props("flat dense round size=sm").classes(
                "text-gray-500"
            )

        with ui.column().classes("w-full p-3 gap-3"):
            _render_source_list_and_actions()
            _render_summary()

    dlg.open()


# ---------------------------------------------------------------------------
# Source list + add buttons
# ---------------------------------------------------------------------------


@ui.refreshable
def _render_source_list_and_actions() -> None:
    state = get_project_state()
    sources = list(state.aggregation_sources or [])

    with ui.column().classes("w-full gap-1"):
        if not sources:
            ui.label("No sources added yet — pick from your existing projects with Browse.").classes(
                "text-xs text-gray-500 italic"
            )
        else:
            for idx, src in enumerate(sources):
                _render_source_row(idx, src)

        with ui.row().classes("items-center gap-2 mt-2"):
            ui.button(
                "Browse projects",
                icon="folder_special",
                on_click=_open_browse_dialog,
            ).props("unelevated dense no-caps").classes("text-xs bg-blue-600 text-white").tooltip(
                "Pick from completed SubtomoExtraction jobs in your recent project locations"
            )
            ui.button(
                "Add path",
                icon="add",
                on_click=_pick_manual_path,
            ).props("flat dense no-caps").classes("text-xs text-gray-600").tooltip(
                "Manually pick an optimisation_set.star or extraction directory"
            )
            ui.space()
            sources_count = len(sources)
            merge_btn = ui.button(
                f"Merge {sources_count} source(s)" if sources_count else "Merge",
                icon="merge_type",
                on_click=_run_merge,
            ).props("unelevated no-caps").classes("text-xs bg-purple-600 text-white")
            if not sources:
                merge_btn.disable()


def _render_source_row(idx: int, src: str) -> None:
    p = Path(src)
    exists = p.exists()
    icon_color = "text-green-500" if exists else "text-red-400"
    with ui.row().classes("items-center gap-2 w-full px-2 py-1 rounded hover:bg-purple-50/40"):
        ui.icon("description" if exists else "error_outline", size="14px").classes(icon_color)
        ui.label(src).classes("text-xs font-mono text-gray-700 truncate flex-1").tooltip(src)
        ui.button(icon="close", on_click=lambda i=idx: _remove_source(i)).props(
            "flat dense round size=xs"
        ).classes("text-gray-400 hover:text-red-500")


def _remove_source(idx: int) -> None:
    state = get_project_state()
    sources = list(state.aggregation_sources or [])
    if 0 <= idx < len(sources):
        removed = sources.pop(idx)
        state.aggregation_sources = sources
        _persist_state()
        ui.notify(f"Removed: {Path(removed).name}", type="info", timeout=1500)
        _render_source_list_and_actions.refresh()


async def _pick_manual_path() -> None:
    state = get_project_state()
    start_dir = str(state.project_path) if state.project_path else "/"
    picker = local_file_picker(start_dir, upper_limit=None, mode="directory")
    result = await picker
    if not result or not result[0]:
        return
    chosen = result[0]
    if chosen in (state.aggregation_sources or []):
        ui.notify("Already in list", type="warning", timeout=2000)
        return
    state.aggregation_sources = list(state.aggregation_sources or []) + [chosen]
    _persist_state()
    ui.notify(f"Added: {Path(chosen).name}", type="positive", timeout=1500)
    _render_source_list_and_actions.refresh()


# ---------------------------------------------------------------------------
# Browse-projects dialog (multi-select across recent project base paths)
# ---------------------------------------------------------------------------


def _open_browse_dialog() -> None:
    from services.aggregation_discovery import discover_subtomo_optimisation_sets
    from services.configs.user_prefs_service import get_prefs_service

    prefs = get_prefs_service().prefs
    base_paths = [r.path for r in prefs.recent_project_roots if r.path]

    state = get_project_state()
    existing_sources = set(state.aggregation_sources or [])

    dlg_state = {"candidates": [], "filter": "", "selected_keys": set()}

    with ui.dialog() as dialog, ui.card().classes("w-[760px] max-w-[90vw]"):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("folder_special", size="20px").classes("text-blue-600")
            ui.label("Pick SubtomoExtraction sources").classes("text-sm font-bold")
            ui.space()
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm")

        ui.separator()

        list_container = ui.column().classes("w-full gap-1 max-h-[460px] overflow-auto pr-1")
        footer_row = ui.row().classes("w-full items-center justify-between mt-2")

        def on_filter_change(e):
            dlg_state["filter"] = e.value or ""
            _render_list()

        ui.input(
            placeholder="Filter by project name, instance, or species…",
            on_change=on_filter_change,
        ).props("dense outlined clearable").classes("w-full")

        async def _load() -> None:
            list_container.clear()
            with list_container:
                with ui.row().classes("w-full justify-center py-6"):
                    ui.spinner("dots", size="md").classes("text-blue-500")
                    ui.label("Scanning…").classes("text-xs text-gray-500 ml-2")
            cands = await run.io_bound(discover_subtomo_optimisation_sets, base_paths)
            dlg_state["candidates"] = cands
            _render_list()

        def _render_list() -> None:
            list_container.clear()
            cands = dlg_state["candidates"]
            f = (dlg_state["filter"] or "").strip().lower()
            if f:
                cands = [
                    c for c in cands
                    if f in c.project_name.lower()
                    or f in c.instance_id.lower()
                    or (c.species_label and f in c.species_label.lower())
                ]
            with list_container:
                if not dlg_state["candidates"]:
                    ui.label(
                        "No completed SubtomoExtraction jobs found in your recent project locations. "
                        "Open a project base path on the landing page first."
                    ).classes("text-xs text-gray-500 italic p-3")
                    return
                if not cands:
                    ui.label("No matches for filter.").classes("text-xs text-gray-400 italic p-3")
                    return
                for c in cands:
                    _render_row(c)
            _refresh_footer()

        def _render_row(c) -> None:
            already_added = c.optset_path in existing_sources
            checked_initial = c.optset_path in dlg_state["selected_keys"] or already_added

            def on_toggle(e, key=c.optset_path):
                if e.value:
                    dlg_state["selected_keys"].add(key)
                else:
                    dlg_state["selected_keys"].discard(key)
                _refresh_footer()

            with ui.row().classes("items-center gap-2 w-full px-2 py-1 hover:bg-gray-50 rounded"):
                cb = ui.checkbox(value=checked_initial, on_change=on_toggle).props("dense")
                if already_added:
                    cb.disable()
                if checked_initial and not already_added:
                    dlg_state["selected_keys"].add(c.optset_path)

                with ui.column().classes("flex-1 min-w-0 gap-0"):
                    with ui.row().classes("items-center gap-2 min-w-0"):
                        ui.label(c.project_name).classes("text-xs font-bold text-gray-800 truncate")
                        if c.is_aggregation:
                            ui.badge("aggregation", color="purple").props("outline").classes("text-[9px]")
                        if c.species_label:
                            ui.badge(c.species_label, color="teal").props("outline").classes("text-[9px]")
                        if already_added:
                            ui.badge("already added", color="gray").props("outline").classes("text-[9px]")
                    with ui.row().classes("items-center gap-2 min-w-0"):
                        ui.label(c.instance_id).classes("text-[10px] font-mono text-gray-500 truncate")
                        if c.n_tomograms is not None:
                            ui.label(f"{c.n_tomograms} tomos").classes("text-[10px] text-gray-400")
                    ui.label(c.optset_path).classes("text-[9px] font-mono text-gray-400 truncate").tooltip(
                        c.optset_path
                    )

        def _refresh_footer() -> None:
            footer_row.clear()
            new_count = len(dlg_state["selected_keys"])
            with footer_row:
                ui.label(
                    f"{new_count} new selection(s)" if new_count else "No new sources selected"
                ).classes("text-xs text-gray-500")
                with ui.row().classes("items-center gap-2"):
                    ui.button("Cancel", on_click=dialog.close).props("flat no-caps").classes(
                        "text-xs text-gray-500"
                    )
                    add = ui.button(
                        f"Add {new_count}" if new_count else "Add",
                        icon="check",
                        on_click=_confirm,
                    ).props("unelevated no-caps").classes("text-xs bg-blue-600 text-white")
                    if new_count == 0:
                        add.disable()

        def _confirm() -> None:
            new_paths = [k for k in dlg_state["selected_keys"] if k not in existing_sources]
            if not new_paths:
                dialog.close()
                return
            current = list(get_project_state().aggregation_sources or [])
            get_project_state().aggregation_sources = current + new_paths
            _persist_state()
            ui.notify(f"Added {len(new_paths)} source(s)", type="positive", timeout=2000)
            dialog.close()
            _render_source_list_and_actions.refresh()

        _refresh_footer()

    dialog.open()
    asyncio.create_task(_load())


# ---------------------------------------------------------------------------
# Merge action
# ---------------------------------------------------------------------------


def _run_merge_sync(merged_dir: Path, sources: list) -> dict:
    from drivers.subtomo_merge import merge_optimisation_sets_into_jobdir

    merged_dir.mkdir(parents=True, exist_ok=True)
    return merge_optimisation_sets_into_jobdir(
        job_dir=merged_dir, additional_sources=sources, allow_no_primary=True
    )


async def _run_merge() -> None:
    state = get_project_state()
    sources = list(state.aggregation_sources or [])
    if not sources:
        ui.notify("Add at least one source first.", type="warning", timeout=2500)
        return
    merged_dir = _merged_dir()
    if merged_dir is None:
        ui.notify("No project loaded — cannot merge.", type="negative", timeout=4000)
        return

    ui.notify("Merging…", type="info", timeout=2500)
    try:
        await run.io_bound(_run_merge_sync, merged_dir, sources)
    except Exception as e:
        ui.notify(f"Merge failed: {e}", type="negative", timeout=8000)
        return
    # Retroactively wire any already-added consumer jobs (RP/Class3D/etc.) to
    # the freshly-written merged optset so the user doesn't have to re-add them.
    n_wired = apply_aggregation_overrides(state)
    if n_wired:
        _persist_state()
    msg = "Merge complete"
    if n_wired:
        msg += f" (wired {n_wired} downstream job{'s' if n_wired != 1 else ''})"
    ui.notify(msg, type="positive", timeout=4000)
    _render_source_list_and_actions.refresh()
    _render_summary.refresh()


# ---------------------------------------------------------------------------
# Summary display
# ---------------------------------------------------------------------------


@ui.refreshable
def _render_summary() -> None:
    summary = _read_merged_summary()
    if summary is None:
        return
    totals = summary.get("totals", {})
    n_deduped = totals.get("n_tomograms_deduplicated", 0)
    merged_dir = _merged_dir()

    with ui.card().classes("w-full border border-green-200 bg-green-50 p-3 gap-2"):
        with ui.row().classes("items-center gap-2 mb-1"):
            ui.icon("check_circle", size="18px").classes("text-green-600")
            ui.label("Last merge result").classes("text-xs font-bold text-green-800")

        with ui.row().classes("gap-3 flex-wrap"):
            _stat_chip("Particles", str(totals.get("n_particles", "?")))
            _stat_chip("Tomograms", str(totals.get("n_tomograms", "?")))
            _stat_chip("Sources", str(totals.get("n_sources", "?")))
            if n_deduped > 0:
                _stat_chip("Deduplicated tomos", str(n_deduped), color="yellow")

        if merged_dir is not None:
            optset = merged_dir / "optimisation_set.star"
            ui.label(str(optset)).classes("text-[10px] font-mono text-gray-500 truncate").tooltip(
                "Downstream jobs (ReconstructParticle, Class3D, Refine3D, ...) "
                "will pick this up automatically via a manual: override."
            )


def _stat_chip(label: str, value: str, color: str = "green") -> None:
    colors = {
        "green": ("bg-green-100", "text-green-800", "text-green-600"),
        "yellow": ("bg-yellow-100", "text-yellow-800", "text-yellow-600"),
    }
    bg, val_color, label_color = colors.get(color, colors["green"])
    with ui.row().classes(f"items-center gap-1 {bg} rounded px-2 py-0.5"):
        ui.label(value).classes(f"text-xs font-bold {val_color}")
        ui.label(label).classes(f"text-[10px] {label_color}")
