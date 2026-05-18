# ui/pipeline_builder/merge_panel_component.py
"""
Merge panel for subtomogram extraction jobs.
Allows combining particles from multiple extraction/picking sources.
"""

import asyncio
import json
import shutil
import traceback
from pathlib import Path
from typing import Callable, Optional

from nicegui import ui, run

from services.project_state import JobType, get_project_state
from ui.local_file_picker import local_file_picker


def _job_dir_for_subtomo(job_model) -> Optional[Path]:
    """Derive the job directory from stored paths or relion_job_name."""
    # The output_particles path lives directly in job_dir
    p = job_model.paths.get("output_particles")
    if p:
        return Path(p).parent

    if job_model.relion_job_name:
        state = get_project_state()
        if state.project_path:
            return state.project_path / job_model.relion_job_name.rstrip("/")

    return None


def _has_extraction_outputs(job_dir: Optional[Path]) -> bool:
    if job_dir is None:
        return False
    return (job_dir / "optimisation_set.star").exists()


def _restore_primary_backups(job_dir: Path) -> bool:
    """
    Restore from *_primary.star backups so re-merge starts from
    the original extraction output, not from a previous merge.
    Returns True if any backup was restored.
    """
    restored = False
    for name in ["particles.star", "tomograms.star", "optimisation_set.star"]:
        backup = job_dir / name.replace(".star", "_primary.star")
        target = job_dir / name
        if backup.exists():
            shutil.copy2(backup, target)
            restored = True
    return restored


def _read_merge_summary(job_dir: Optional[Path]) -> Optional[dict]:
    if job_dir is None:
        return None
    summary_path = job_dir / "merge_summary.json"
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text())
    except Exception:
        return None


def _run_merge_sync(job_dir: Path, additional_sources: list[str], allow_no_primary: bool = False) -> dict:
    """
    Runs the merge in the calling thread (meant to be called via run.cpu_bound
    or run.io_bound). Returns either the summary dict or an error dict.
    """
    from drivers.subtomo_merge import merge_optimisation_sets_into_jobdir

    _restore_primary_backups(job_dir)

    return merge_optimisation_sets_into_jobdir(
        job_dir=job_dir,
        additional_sources=additional_sources,
        allow_no_primary=allow_no_primary,
    )


# --------------------------------------------------------------------------
# Component
# --------------------------------------------------------------------------

def render_merge_panel(
    job_model,
    frozen: bool,  # kept in signature for interface consistency, but ignored
    save_handler: Callable,
) -> None:
    """
    Self-contained merge panel rendered inside the subtomo extraction config tab.
    Merge is always interactive regardless of job execution status.
    """
    job_dir = _job_dir_for_subtomo(job_model)
    has_outputs = _has_extraction_outputs(job_dir)
    state = get_project_state()
    is_aggregation = bool(getattr(state, "is_aggregation", False))

    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
        with ui.row().classes(
            "w-full items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100"
        ):
            with ui.row().classes("items-center gap-2"):
                ui.icon("merge_type", size="18px").classes("text-gray-500")
                ui.label("Particle Merge").classes("text-sm font-bold text-gray-800")
                if is_aggregation:
                    ui.badge("aggregation", color="purple").props("outline").classes("text-[10px]")

            if job_model.additional_sources:
                ui.badge(f"{len(job_model.additional_sources)} source(s)", color="blue").props(
                    "outline"
                ).classes("text-[10px]")

        with ui.column().classes("w-full p-3 gap-3"):
            # Aggregation projects have no primary extraction; allow the merge
            # UI immediately so the user can attach upstream optimisation_sets.
            if not has_outputs and not is_aggregation:
                ui.label(
                    "Run extraction first -- merge becomes available once "
                    "this job has produced an optimisation_set.star."
                ).classes("text-xs text-gray-400 italic")
                return

            # Never frozen -- merge is always allowed on completed jobs
            _render_source_list(job_model, False, save_handler, is_aggregation)
            ui.separator().classes("my-1")
            _render_merge_controls(job_model, job_dir, False, save_handler, is_aggregation)
            _render_merge_summary(job_dir)


# --------------------------------------------------------------------------
# Source list
# --------------------------------------------------------------------------

@ui.refreshable
def _render_source_list(job_model, frozen: bool, save_handler: Callable, is_aggregation: bool = False) -> None:
    sources: list[str] = job_model.additional_sources

    # -- primary (always shown, not removable) --
    # Aggregation projects have no extraction primary; show a placeholder row
    # to keep the layout but make it clear there's no own contribution.
    job_dir = _job_dir_for_subtomo(job_model)
    with ui.row().classes("items-center gap-2 w-full"):
        if is_aggregation:
            ui.icon("merge", size="16px").classes("text-purple-400")
            ui.label("(aggregation -- no primary extraction)").classes(
                "text-xs font-mono text-gray-400 italic truncate flex-1"
            ).tooltip("This job is a merge target; particles come entirely from the sources below")
            ui.badge("aggregator", color="purple").props("outline").classes("text-[9px]")
        else:
            ui.icon("star", size="16px").classes("text-yellow-500")
            primary_label = str(job_dir) if job_dir else "(this job)"
            ui.label(primary_label).classes("text-xs font-mono text-gray-600 truncate flex-1").tooltip(
                "Primary source -- particles from this extraction job"
            )
            ui.badge("primary", color="gray").props("outline").classes("text-[9px]")

    # -- additional sources --
    if sources:
        for idx, src in enumerate(sources):
            _render_source_row(job_model, idx, src, frozen, save_handler)
    else:
        ui.label("No additional sources added yet.").classes("text-xs text-gray-400 italic ml-6")

    # -- add buttons --
    if not frozen:
        with ui.row().classes("items-center gap-1 mt-1"):
            ui.button(
                "Browse projects",
                icon="folder_special",
                on_click=lambda: _open_browse_dialog(job_model, save_handler),
            ).props("flat dense no-caps").classes("text-xs text-blue-600").tooltip(
                "Pick from completed SubtomoExtraction jobs in your recent project locations"
            )
            ui.button(
                "Add path",
                icon="add",
                on_click=lambda: _pick_source(job_model, save_handler),
            ).props("flat dense no-caps").classes("text-xs text-gray-500").tooltip(
                "Manually pick an optimisation_set.star or extraction directory"
            )


def _render_source_row(
    job_model, idx: int, src: str, frozen: bool, save_handler: Callable
) -> None:
    p = Path(src)
    exists = p.exists()
    icon_color = "text-green-500" if exists else "text-red-400"

    with ui.row().classes("items-center gap-2 w-full pl-6"):
        ui.icon("folder" if p.is_dir() else "description", size="16px").classes(icon_color)
        ui.label(src).classes("text-xs font-mono text-gray-700 truncate flex-1").tooltip(src)

        if not exists:
            ui.icon("error_outline", size="14px").classes("text-red-400").tooltip("Path not found")

        if not frozen:
            ui.button(
                icon="close",
                on_click=lambda i=idx: _remove_source(job_model, i, save_handler),
            ).props("flat dense round size=xs").classes("text-gray-400 hover:text-red-500")


async def _pick_source(job_model, save_handler: Callable) -> None:
    state = get_project_state()
    start_dir = str(state.project_path) if state.project_path else "/"

    picker = local_file_picker(start_dir, upper_limit=None, mode="directory")
    result = await picker

    if result and result[0]:
        chosen = result[0]
        if chosen in job_model.additional_sources:
            ui.notify("Source already in list", type="warning", timeout=2000)
            return

        job_model.additional_sources.append(chosen)
        save_handler()
        _render_source_list.refresh()
        _render_merge_controls.refresh()   # <-- add this
        ui.notify(f"Added: {Path(chosen).name}", type="positive", timeout=1500)


def _remove_source(job_model, idx: int, save_handler: Callable) -> None:
    try:
        sources = list(job_model.additional_sources or [])
        if idx < 0 or idx >= len(sources):
            return

        removed = sources[idx]
        job_model.additional_sources = [
            s for i, s in enumerate(sources) if i != idx
        ]

        save_handler()
        _render_source_list.refresh()
        _render_merge_controls.refresh()
        ui.notify(f"Removed: {Path(removed).name}", type="info", timeout=1500)

    except Exception:
        traceback.print_exc()



# --------------------------------------------------------------------------
# Merge controls
# --------------------------------------------------------------------------

@ui.refreshable
def _render_merge_controls(
    job_model, job_dir: Optional[Path], frozen: bool, save_handler: Callable, is_aggregation: bool = False
) -> None:
    sources = job_model.additional_sources
    # Aggregation projects can merge as soon as at least one source is added —
    # there's no primary extraction to wait on, and we synthesize a job_dir at
    # click time if the schemer hasn't allocated one yet.
    if is_aggregation:
        can_merge = bool(sources)
    else:
        can_merge = bool(sources) and job_dir is not None and _has_extraction_outputs(job_dir)

    async def on_merge_click() -> None:
        # runs in UI context automatically
        await _execute_merge(job_model, job_dir, save_handler, is_aggregation)

    merge_btn = ui.button("Merge", icon="merge_type", on_click=on_merge_click)
    if not can_merge:
        merge_btn.disable()



async def _execute_merge(
    job_model, job_dir: Optional[Path], save_handler: Callable, is_aggregation: bool = False
) -> None:
    from ui.background_task import BackgroundTask

    # Aggregation projects: if the schemer hasn't allocated a job dir yet,
    # synthesize one under External/aggregator/ (won't collide with the
    # schemer's jobNNN numbering) and persist relion_job_name so subsequent
    # renders + reloads find it.
    if job_dir is None:
        if not is_aggregation:
            ui.notify(
                "Job directory not allocated yet. Run the pipeline once to create it, then merge.",
                type="warning",
                timeout=4000,
            )
            return
        state = get_project_state()
        if state.project_path is None:
            ui.notify("No project loaded — cannot merge.", type="negative", timeout=4000)
            return
        rel_name = "External/aggregator/"
        job_dir = state.project_path / rel_name.rstrip("/")
        job_dir.mkdir(parents=True, exist_ok=True)
        job_model.relion_job_name = rel_name
        save_handler()
    elif is_aggregation:
        # Existing dir (e.g. from a prior synthesized merge) — ensure it exists.
        job_dir.mkdir(parents=True, exist_ok=True)

    sources = list(job_model.additional_sources)
    state = get_project_state()
    project_path = state.project_path if state is not None else None

    async def _run(progress_cb):
        progress_cb(0, 0, "Merging optset sources…")
        await run.io_bound(_run_merge_sync, job_dir, sources, is_aggregation)
        return f"merged {len(sources)} source(s)"

    def _on_complete(task):
        if task.status != "succeeded":
            ui.notify(
                f"Merge {task.status}: {task.error or 'see tray for details'}",
                type="negative", timeout=4000,
            )
            return
        # Pass job_dir explicitly: ui.refreshable re-uses the args from the
        # prior call by default, but for fresh aggregation projects job_dir
        # was None at initial panel render and only got synthesized above.
        _render_merge_summary.refresh(job_dir)
        _render_merge_controls.refresh(job_model, job_dir, False, save_handler, is_aggregation)
        _render_source_list.refresh(job_model, False, save_handler, is_aggregation)

    BackgroundTask(
        title=f"Merge · {job_dir.name}",
        subtitle=f"{len(sources)} source(s){' (aggregation)' if is_aggregation else ''}",
        dedup_key=f"merge:{job_dir}",
        project_path=str(project_path) if project_path else None,
    ).submit(_run, on_complete=_on_complete)




def _handle_restore(job_dir: Path, save_handler: Callable) -> None:
    restored = _restore_primary_backups(job_dir)
    # Remove the merge summary so the panel reflects the clean state
    summary_path = job_dir / "merge_summary.json"
    if summary_path.exists():
        summary_path.unlink()

    if restored:
        ui.notify("Restored original extraction outputs", type="positive", timeout=2000)
    else:
        ui.notify("No backups found", type="warning", timeout=2000)

    _render_merge_summary.refresh()
    _render_merge_controls.refresh()


# --------------------------------------------------------------------------
# Summary display
# --------------------------------------------------------------------------

@ui.refreshable
def _render_merge_summary(job_dir: Optional[Path]) -> None:
    summary = _read_merge_summary(job_dir)
    if summary is None:
        return

    totals = summary.get("totals", {})
    sources = summary.get("sources", [])
    n_deduped = totals.get("n_tomograms_deduplicated", 0)

    with ui.card().classes("w-full border border-green-200 bg-green-50 p-3 gap-2"):
        with ui.row().classes("items-center gap-2 mb-1"):
            ui.icon("check_circle", size="18px").classes("text-green-600")
            ui.label("Last merge result").classes("text-xs font-bold text-green-800")

        # Totals row
        with ui.row().classes("gap-4 flex-wrap"):
            _stat_chip("Particles", str(totals.get("n_particles", "?")))
            _stat_chip("Tomograms", str(totals.get("n_tomograms", "?")))
            _stat_chip("Sources", str(totals.get("n_sources", "?")))
            if n_deduped > 0:
                _stat_chip("Deduplicated tomos", str(n_deduped), color="yellow")

        # Per-source breakdown
        if len(sources) > 1:
            with ui.column().classes("mt-2 gap-1"):
                ui.label("Per source:").classes("text-[10px] font-bold text-gray-500 uppercase")
                for i, src in enumerate(sources):
                    label = "primary" if i == 0 else Path(src["source_input"]).parent.name
                    with ui.row().classes("items-center gap-2 pl-2"):
                        ui.label(label).classes("text-xs font-mono text-gray-600 w-40 truncate").tooltip(
                            src.get("source_input", "")
                        )
                        ui.label(f"{src.get('n_particles', '?')} particles").classes(
                            "text-xs text-gray-500"
                        )
                        tomo_names = src.get("tomo_names", [])
                        if tomo_names:
                            ui.label(f"({len(tomo_names)} tomos)").classes(
                                "text-xs text-gray-400"
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


# --------------------------------------------------------------------------
# Browse-projects dialog: pick from completed SubtomoExtraction jobs
# --------------------------------------------------------------------------


def _open_browse_dialog(job_model, save_handler: Callable) -> None:
    """Open a dialog listing completed SubtomoExtraction jobs across the user's
    recent project base paths. User can multi-select; selected optset paths are
    appended to job_model.additional_sources."""
    from services.aggregation_discovery import discover_subtomo_optimisation_sets
    from services.configs.user_prefs_service import get_prefs_service

    prefs = get_prefs_service().prefs
    base_paths = [r.path for r in prefs.recent_project_roots if r.path]

    # State held in mutable dicts so closures can mutate without `nonlocal` chains
    state = {
        "candidates": [],  # list[SubtomoCandidate]
        "filter": "",
        "selected_keys": set(),  # set[str] of optset_path
    }
    existing_sources = set(job_model.additional_sources or [])

    with ui.dialog() as dialog, ui.card().classes("w-[720px] max-w-[90vw]"):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("folder_special", size="20px").classes("text-blue-600")
            ui.label("Pick SubtomoExtraction sources").classes("text-sm font-bold")
            ui.space()
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm")

        ui.separator()

        list_container = ui.column().classes("w-full gap-1 max-h-[420px] overflow-auto pr-1")
        footer_row = ui.row().classes("w-full items-center justify-between mt-2")

        def on_filter_change(e):
            state["filter"] = e.value or ""
            _render_list()

        # Filter row (created after handler so on_change can reference it)
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
            state["candidates"] = cands
            _render_list()

        def _render_list() -> None:
            list_container.clear()
            cands = state["candidates"]
            f = (state["filter"] or "").strip().lower()
            if f:
                cands = [
                    c for c in cands
                    if f in c.project_name.lower()
                    or f in c.instance_id.lower()
                    or (c.species_label and f in c.species_label.lower())
                ]
            with list_container:
                if not state["candidates"]:
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
            checked_initial = c.optset_path in state["selected_keys"] or already_added

            def on_toggle(e, key=c.optset_path):
                if e.value:
                    state["selected_keys"].add(key)
                else:
                    state["selected_keys"].discard(key)
                _refresh_footer()

            row_classes = "items-center gap-2 w-full px-2 py-1 hover:bg-gray-50 rounded"
            with ui.row().classes(row_classes):
                cb = ui.checkbox(value=checked_initial, on_change=on_toggle).props("dense")
                if already_added:
                    cb.disable()
                # mirror initial state into selected_keys so "Add selected" picks
                # up new defaults but skips already-added ones
                if checked_initial and not already_added:
                    state["selected_keys"].add(c.optset_path)

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

        # ---- Footer (count + actions) ----
        def _refresh_footer() -> None:
            footer_row.clear()
            new_count = len(state["selected_keys"])
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
            new_paths = [k for k in state["selected_keys"] if k not in existing_sources]
            if not new_paths:
                dialog.close()
                return
            job_model.additional_sources = list(job_model.additional_sources or []) + new_paths
            save_handler()
            _render_source_list.refresh()
            _render_merge_controls.refresh()
            ui.notify(f"Added {len(new_paths)} source(s)", type="positive", timeout=2000)
            dialog.close()

        _refresh_footer()

    dialog.open()
    asyncio.create_task(_load())