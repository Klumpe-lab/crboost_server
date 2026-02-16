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


def _run_merge_sync(job_dir: Path, additional_sources: list[str]) -> dict:
    """
    Runs the merge in the calling thread (meant to be called via run.cpu_bound
    or run.io_bound). Returns either the summary dict or an error dict.
    """
    from drivers.subtomo_merge import merge_optimisation_sets_into_jobdir

    _restore_primary_backups(job_dir)

    return merge_optimisation_sets_into_jobdir(
        job_dir=job_dir,
        additional_sources=additional_sources,
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

    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
        with ui.row().classes(
            "w-full items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100"
        ):
            with ui.row().classes("items-center gap-2"):
                ui.icon("merge_type", size="18px").classes("text-gray-500")
                ui.label("Particle Merge").classes("text-sm font-bold text-gray-800")

            if job_model.additional_sources:
                ui.badge(f"{len(job_model.additional_sources)} source(s)", color="blue").props(
                    "outline"
                ).classes("text-[10px]")

        with ui.column().classes("w-full p-3 gap-3"):
            if not has_outputs:
                ui.label(
                    "Run extraction first -- merge becomes available once "
                    "this job has produced an optimisation_set.star."
                ).classes("text-xs text-gray-400 italic")
                return

            # Never frozen -- merge is always allowed on completed jobs
            _render_source_list(job_model, False, save_handler)
            ui.separator().classes("my-1")
            _render_merge_controls(job_model, job_dir, False, save_handler)
            _render_merge_summary(job_dir)


# --------------------------------------------------------------------------
# Source list
# --------------------------------------------------------------------------

@ui.refreshable
def _render_source_list(job_model, frozen: bool, save_handler: Callable) -> None:
    sources: list[str] = job_model.additional_sources

    # -- primary (always shown, not removable) --
    job_dir = _job_dir_for_subtomo(job_model)
    with ui.row().classes("items-center gap-2 w-full"):
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

    # -- add button --
    if not frozen:
        ui.button(
            "Add source",
            icon="add",
            on_click=lambda: _pick_source(job_model, save_handler),
        ).props("flat dense no-caps").classes("text-xs text-blue-600 mt-1")


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
def _render_merge_controls(job_model, job_dir: Optional[Path], frozen: bool, save_handler: Callable) -> None:
    sources = job_model.additional_sources
    can_merge = bool(sources) and job_dir is not None and _has_extraction_outputs(job_dir)

    async def on_merge_click() -> None:
        # runs in UI context automatically
        await _execute_merge(job_model, job_dir, save_handler)

    merge_btn = ui.button("Merge", icon="merge_type", on_click=on_merge_click)
    if not can_merge:
        merge_btn.disable()



async def _execute_merge(job_model, job_dir: Path, save_handler: Callable) -> None:
    ui.notify("Merging...", type="info", timeout=2000)
    summary = await run.io_bound(_run_merge_sync, job_dir, list(job_model.additional_sources))
    ui.notify("Merge complete", type="positive", timeout=4000)
    _render_merge_summary.refresh()
    _render_merge_controls.refresh()




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