"""Subtomo Extraction plugin.

The cross-project merge ("Particle Merge") used to live here, but for
aggregation projects it now lives in a standalone workspace card
(ui/aggregation_merge_card.py). For normal pipelines, merging across
extraction outputs is rare enough that the dedicated UI was removed; if
you need it back per-job, re-mount render_merge_panel from
ui/pipeline_builder/merge_panel_component.

Job-specific fields (`box_size`, `crop_size`, `binning`) stay on the
job — these ARE subtomo decisions. But there's a real cross-link worth
surfacing inline: subtomo box (in Å) vs species particle diameter. The
dashboard's `_apply_sanity_rules` already does this post-hoc; this
plugin surfaces the same check at submission time so the user sees it
before they hit Run.

Empty-upstream guard: if the upstream tmextractcand step produced zero
candidates across every tomogram, running this job has nothing to do.
We mirror the per-TS .skip pattern at the whole-job level — banner
here so the user knows before they touch Run, and the driver writes a
.skipped_no_candidates.json sidecar + RELION_JOB_EXIT_SUCCESS rather
than a hard fail (see drivers/subtomo_extraction.py).
"""

import json
import logging
from pathlib import Path
from typing import Optional

from nicegui import ui

from services.models_base import JobType
from services.project_state import get_project_state_for
from services.templating.template_metadata import resolve_species_from_job
from ui.components.template_summary_card import render_template_summary_card
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card

logger = logging.getLogger(__name__)


@register_params_renderer(JobType.SUBTOMO_EXTRACTION)
def render_subtomo_extraction_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    instance_id = ctx.get("instance_id")
    species = None
    state = None
    if ui_mgr and ui_mgr.project_path:
        state = get_project_state_for(ui_mgr.project_path)
        species, _ = resolve_species_from_job(state, job_model, instance_id)

    # Loud, top-of-panel banner if upstream picks are empty across the board.
    # Goes first so it shadows everything else — the user shouldn't be tweaking
    # box_size for a job that has nothing to extract.
    _render_empty_upstream_banner(job_model)

    if species is not None:
        render_template_summary_card(species)
    else:
        with ui.card().classes("w-full border border-dashed border-amber-300 bg-amber-50 mt-1"):
            with ui.row().classes("w-full items-center px-3 py-2 gap-2"):
                ui.icon("warning", size="14px").classes("text-amber-600")
                ui.label("No species linked to this job").classes("text-xs text-amber-800 font-semibold")
            with ui.column().classes("w-full px-3 pb-2 gap-1"):
                ui.label(
                    "Without a species link the box-vs-diameter check can't run. "
                    "Assign a species or use a `__<species_id>` instance suffix."
                ).classes("text-[11px] text-amber-700")

    # Inline box-vs-diameter warning. Mirrors the dashboard's sanity rule
    # so a user editing box_size sees the same advice before submission.
    warning_container = ui.column().classes("w-full")
    _render_box_vs_diameter_warning(warning_container, state, species, job_model)

    # Wrap save_handler so the warning re-renders when box_size / binning changes.
    def _on_save():
        save_handler()
        _render_box_vs_diameter_warning(warning_container, state, species, job_model)

    render_default_params_card(job_type, job_model, is_frozen, _on_save, ui_mgr=ui_mgr)


def _render_empty_upstream_banner(job_model) -> None:
    """Render a red banner when upstream picks are zero across every TS.

    Two states are surfaced:
      - Post-run: this job already ran and exited via the all-skip path
        (drivers/subtomo_extraction.py writes .skipped_no_candidates.json).
      - Pre-run: upstream candidate-extraction's particles.star is empty.
        Surfaces the condition before the user clicks Run / before the
        schemer dispatches.
    """
    job_dir, upstream_particles = _resolve_upstream_paths(job_model)
    sentinel_msg = _read_skip_sentinel(job_dir)
    upstream_status = _check_upstream_particles(upstream_particles) if upstream_particles else None

    # Post-run state wins over pre-run state — the job actually ran and
    # produced the explicit sidecar, so quote it verbatim.
    if sentinel_msg is not None:
        _draw_banner(
            title="Skipped — no candidates upstream",
            body=sentinel_msg,
            ran_already=True,
        )
        return

    if upstream_status == "empty":
        _draw_banner(
            title="Upstream produced 0 picks across every tomogram",
            body=(
                "The upstream candidate-extraction step found no particles above the "
                "matching threshold in any tomogram, so subtomo extraction has nothing "
                "to extract. Running this job would be a no-op; the driver will mark "
                "every tilt-series as SKIP and exit immediately."
            ),
            ran_already=False,
        )


def _draw_banner(title: str, body: str, ran_already: bool) -> None:
    bg = "bg-red-50 border-red-300"
    icon_color = "text-red-700"
    text_color = "text-red-900"
    icon = "block" if not ran_already else "info"
    label_pill = "ran with no work" if ran_already else "blocked"
    with ui.row().classes(f"w-full {bg} border-2 rounded px-3 py-2 gap-2 items-start mt-1"):
        ui.icon(icon, size="18px").classes(f"{icon_color} mt-0.5")
        with ui.column().classes("gap-1 flex-1"):
            with ui.row().classes("items-center gap-2"):
                ui.label(title).classes(f"text-sm font-bold {text_color}")
                ui.label(label_pill).classes(
                    "text-[10px] uppercase tracking-wide font-mono px-1.5 py-0.5 "
                    "rounded bg-red-200 text-red-900"
                )
            ui.label(body).classes(f"text-[11px] {text_color}")


def _resolve_upstream_paths(job_model):
    """Return (job_dir, upstream_particles_path), either as Path or None.

    Upstream particles are resolved from the optimisation_set this job
    consumes; for tmextractcand→subtomoExtraction the sibling file is
    `candidates.star`, but parsing the opt_set keeps us robust to future
    wiring changes."""
    paths = getattr(job_model, "paths", {}) or {}
    job_dir_raw = paths.get("job_dir")
    job_dir = Path(job_dir_raw) if job_dir_raw else None

    opt_raw = paths.get("input_optimisation")
    if not opt_raw:
        return job_dir, None
    opt_path = Path(opt_raw)
    if not opt_path.exists():
        return job_dir, None

    # Prefer the sibling candidates.star — that's what tmextractcand writes
    # and avoids pulling in a starfile parse on the UI thread. Only fall
    # back to parsing the opt_set if the sibling isn't present.
    sibling = opt_path.parent / "candidates.star"
    if sibling.exists():
        return job_dir, sibling
    try:
        import starfile

        data = starfile.read(opt_path, always_dict=True)
        for block in data.values():
            if hasattr(block, "columns") and "rlnTomoParticlesFile" in block.columns:
                rel = str(block["rlnTomoParticlesFile"].iloc[0])
                resolved = (opt_path.parent / rel).resolve() if not Path(rel).is_absolute() else Path(rel)
                return job_dir, resolved
            if isinstance(block, dict) and "rlnTomoParticlesFile" in block:
                rel = str(block["rlnTomoParticlesFile"])
                resolved = (opt_path.parent / rel).resolve() if not Path(rel).is_absolute() else Path(rel)
                return job_dir, resolved
    except Exception:
        logger.debug("Could not parse upstream opt_set %s for empty-check", opt_path, exc_info=True)
    return job_dir, None


def _check_upstream_particles(particles_path: Path) -> Optional[str]:
    """Return 'empty' if the upstream particles file has zero rows in its
    data_particles block, 'has_picks' if it has any rows, or None if we
    couldn't read it (in which case we silently skip the banner — better to
    show no warning than a false one)."""
    if not particles_path.exists():
        return None
    try:
        import starfile

        data = starfile.read(particles_path, always_dict=True)
        for block in data.values():
            if hasattr(block, "shape"):
                return "has_picks" if block.shape[0] > 0 else "empty"
        return None
    except Exception:
        logger.debug("Could not read upstream particles %s", particles_path, exc_info=True)
        return None


def _read_skip_sentinel(job_dir: Optional[Path]) -> Optional[str]:
    """Return the `message` field of `.skipped_no_candidates.json` if the
    driver already wrote one, else None."""
    if job_dir is None:
        return None
    sentinel = job_dir / ".skipped_no_candidates.json"
    if not sentinel.exists():
        return None
    try:
        payload = json.loads(sentinel.read_text())
        msg = payload.get("message")
        return msg if isinstance(msg, str) and msg.strip() else None
    except Exception:
        logger.debug("Could not parse skip sentinel %s", sentinel, exc_info=True)
        return None


def _render_box_vs_diameter_warning(container, state, species, job_model) -> None:
    container.clear()
    if species is None or state is None:
        return
    diameter = getattr(species, "diameter_ang", None)
    if not diameter or diameter <= 0:
        return

    bx = int(getattr(job_model, "box_size", 0) or 0)
    if bx <= 0:
        return

    binning = float(getattr(job_model, "binning", 1.0) or 1.0)
    native_px = _native_pixel_size(state)
    if not native_px or native_px <= 0:
        # Unknown native px — can't compute box in Å; bail rather than
        # show a misleading warning.
        return
    eff_px = native_px * binning
    box_ang = bx * eff_px
    ratio = box_ang / float(diameter)

    level: Optional[str] = None
    msg = ""
    if ratio < 1.5:
        level = "error"
        msg = (
            f"Box {box_ang:g} Å is {ratio:.2f}× particle diameter {diameter:g} Å — "
            "particle won't fit. Aim for 1.5–3×."
        )
    elif ratio > 3.0:
        level = "warn"
        msg = (
            f"Box {box_ang:g} Å is {ratio:.2f}× particle diameter {diameter:g} Å — "
            "wasted compute. Aim for 1.5–3×."
        )

    if level is None:
        return

    with container:
        bg = "bg-red-50 border-red-200" if level == "error" else "bg-amber-50 border-amber-200"
        icon_color = "text-red-600" if level == "error" else "text-amber-600"
        text_color = "text-red-800" if level == "error" else "text-amber-800"
        with ui.row().classes(f"w-full {bg} border rounded px-3 py-2 gap-2 items-start mt-1"):
            ui.icon("error" if level == "error" else "warning", size="16px").classes(f"{icon_color} mt-0.5")
            with ui.column().classes("gap-0.5"):
                ui.label("Box vs particle diameter").classes(f"text-xs font-semibold {text_color}")
                ui.label(msg).classes(f"text-[11px] {text_color}")
                ui.label(
                    f"box_size={bx} px • binning={binning:g} • effective apix={eff_px:.3g} Å/px"
                ).classes("text-[11px] text-gray-500 font-mono")


def _native_pixel_size(state) -> Optional[float]:
    mic = getattr(state, "microscope", None)
    if mic is None:
        return None
    px = getattr(mic, "pixel_size_angstrom", None)
    try:
        return float(px) if px else None
    except (TypeError, ValueError):
        return None
