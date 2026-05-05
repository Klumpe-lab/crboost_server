"""
Candidate-preview dialog launched from the workspace sidebar.

Replaces the in-job-tab preview/3dmod section. Shows, for every candidate-extract
job in the current project, a per-tomogram card containing:
  - Stage / beam position derived from the tomo name (so users can find a
    specific position at-a-glance instead of squinting at long unique names).
  - The MIP+circles preview thumbnail (rendered server-side, served via
    /api/candidate-preview).
  - A 3dmod copy-command row with the IMOD model overlay if available, falling
    back to volume-only viewing.

If multiple extract instances exist (one per species), they're shown as tabs.
"""

from __future__ import annotations

import logging
import traceback
import urllib.parse
from pathlib import Path
from typing import Optional

import pandas as pd
from nicegui import ui, run

from services.models_base import JobStatus, JobType
from services.project_state import get_project_state
from services.tilt_series.build import _infer_position
from services.visualization.imod_vis import generate_candidate_vis
from services.visualization.preview_orchestrator import (
    generate_candidate_previews,
    read_preview_manifest,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Discovery / data helpers
# ---------------------------------------------------------------------------


def _candidate_extract_instances(state) -> list[tuple[str, object]]:
    """Return [(instance_id, job_model), …] for every TEMPLATE_EXTRACT_PYTOM job
    in the current project, sorted by instance_id."""
    out: list[tuple[str, object]] = []
    for instance_id, job_model in state.jobs.items():
        if getattr(job_model, "job_type", None) == JobType.TEMPLATE_EXTRACT_PYTOM:
            out.append((instance_id, job_model))
    return sorted(out, key=lambda kv: kv[0])


def _job_dir_for(instance_id: str, job_model, project_path: Path) -> Optional[Path]:
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn:
        d = project_path / rjn.rstrip("/")
        if d.is_dir():
            return d
    state = get_project_state()
    mapped = (state.job_path_mapping or {}).get(instance_id)
    if mapped:
        d = project_path / mapped.rstrip("/")
        if d.is_dir():
            return d
    return None


def _read_tomograms_table(tomograms_star: Path) -> Optional[pd.DataFrame]:
    if not tomograms_star.exists():
        return None
    try:
        import starfile

        data = starfile.read(tomograms_star, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                return v
    except Exception as e:
        logger.warning("Could not read %s: %s", tomograms_star, e)
    return None


def _resolve_volume_for_3dmod(tomo_row: pd.Series, project_path: Path) -> Optional[Path]:
    """Same f32-preference as the preview renderer so the 3dmod cmd points at the
    file IMOD4 likes (and the same one the preview was rendered from)."""
    if "rlnTomoReconstructedTomogram" not in tomo_row.index:
        return None
    p = Path(str(tomo_row["rlnTomoReconstructedTomogram"]))
    if not p.is_absolute():
        p = project_path / p
    f32 = p.with_name(p.stem + "_f32.mrc")
    if f32.exists():
        return f32
    if p.exists():
        return p
    return None


def _preview_url(png_path: str) -> str:
    return "/api/candidate-preview?path=" + urllib.parse.quote(png_path, safe="")


def _position_label(tomo_name: str) -> tuple[str, tuple[int, int]]:
    """Return ('Pos 11 · Beam 2', (11, 2)). Falls back to the raw tail if parse fails."""
    stage, beam = _infer_position(tomo_name)
    if stage == 0:
        # Parser gives (0, 1) on no-match; show a more honest label in that case.
        return tomo_name.rsplit("_", 1)[-1], (stage, beam)
    return f"Pos {stage} · Beam {beam}", (stage, beam)


def _collect_tomos_for_instance(
    job_dir: Path, project_path: Path
) -> list[dict]:
    """Build a per-tomogram row list combining tomograms.star + the preview manifest
    + the IMOD .mod path. Sorted by (stage, beam, name)."""
    tomograms_star = job_dir / "tomograms.star"
    tomo_df = _read_tomograms_table(tomograms_star)
    manifest = read_preview_manifest(job_dir) or {}
    entries = manifest.get("entries") or {}
    summary = manifest.get("summary") or {}
    missing_volume = set(summary.get("missing_volume") or [])
    errored_map = {e["tomo"]: e.get("error", "") for e in (summary.get("errored") or [])}

    rows: list[dict] = []
    if tomo_df is None:
        # No tomograms.star yet — fall back to enumerating manifest entries.
        for tomo_name, entry in entries.items():
            label, (stage, beam) = _position_label(tomo_name)
            rows.append({
                "tomo_name": tomo_name,
                "position_label": label,
                "stage": stage,
                "beam": beam,
                "vol_path": entry.get("tomo_mrc"),
                "mod_path": str(job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"),
                "preview_png": entry.get("png"),
                "n_candidates": entry.get("n_candidates"),
                "score_range": entry.get("score_range"),
                "status": "ok" if entry.get("png") else "no-preview",
                "error": errored_map.get(tomo_name),
            })
    else:
        for _, tomo_row in tomo_df.iterrows():
            tomo_name = str(tomo_row["rlnTomoName"])
            label, (stage, beam) = _position_label(tomo_name)
            entry = entries.get(tomo_name) or {}
            vol_path = _resolve_volume_for_3dmod(tomo_row, project_path)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            png = entry.get("png")
            if tomo_name in missing_volume:
                status = "missing-volume"
            elif tomo_name in errored_map:
                status = "errored"
            elif png and Path(png).exists():
                status = "ok"
            else:
                status = "no-preview"
            rows.append({
                "tomo_name": tomo_name,
                "position_label": label,
                "stage": stage,
                "beam": beam,
                "vol_path": str(vol_path) if vol_path else None,
                "mod_path": str(mod_path) if mod_path.exists() else None,
                "preview_png": png if (png and Path(png).exists()) else None,
                "n_candidates": entry.get("n_candidates"),
                "score_range": entry.get("score_range"),
                "status": status,
                "error": errored_map.get(tomo_name),
            })

    rows.sort(key=lambda r: (r["stage"], r["beam"], r["tomo_name"]))
    return rows


def has_any_extract_jobs() -> bool:
    state = get_project_state()
    return any(_candidate_extract_instances(state))


def has_any_previews_rendered() -> bool:
    """Cheap check used for the sidebar green dot."""
    state = get_project_state()
    if state.project_path is None:
        return False
    for instance_id, job_model in _candidate_extract_instances(state):
        job_dir = _job_dir_for(instance_id, job_model, state.project_path)
        if not job_dir:
            continue
        if (job_dir / "vis" / "preview" / "manifest.json").exists():
            return True
    return False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def open_candidate_preview_dialog() -> None:
    state = get_project_state()
    if state.project_path is None:
        ui.notify("No project loaded.", type="warning")
        return

    instances = _candidate_extract_instances(state)
    if not instances:
        ui.notify("No candidate-extract jobs in this project yet.", type="info")
        return

    project_path = Path(state.project_path)

    with ui.dialog().props("maximized") as dlg, ui.card().classes(
        "w-full h-full bg-gray-50 overflow-hidden flex flex-col"
    ):
        with ui.row().classes(
            "w-full items-center gap-2 px-4 py-2 border-b border-gray-200 bg-white"
        ):
            ui.icon("scatter_plot", size="20px").classes("text-blue-600")
            ui.label("Candidate Previews").classes("text-sm font-bold text-gray-800")
            ui.label(state.project_name or "").classes("text-xs text-gray-400 font-mono")
            ui.space()
            ui.button(icon="close", on_click=dlg.close).props(
                "flat dense round size=sm"
            ).classes("text-gray-500")

        body = ui.element("div").classes("w-full flex-1 overflow-auto").style(
            "min-height: 0;"
        )

        with body:
            if len(instances) == 1:
                instance_id, job_model = instances[0]
                _render_instance_section(instance_id, job_model, project_path)
            else:
                with ui.tabs().classes("w-full bg-white border-b") as tabs:
                    for instance_id, _ in instances:
                        ui.tab(instance_id).classes("text-xs")
                with ui.tab_panels(tabs, value=instances[0][0]).classes(
                    "w-full bg-gray-50"
                ):
                    for instance_id, job_model in instances:
                        with ui.tab_panel(instance_id).classes("p-0"):
                            _render_instance_section(instance_id, job_model, project_path)

    dlg.open()


# ---------------------------------------------------------------------------
# Per-instance section
# ---------------------------------------------------------------------------


@ui.refreshable
def _render_instance_section(instance_id: str, job_model, project_path: Path) -> None:
    job_dir = _job_dir_for(instance_id, job_model, project_path)
    job_succeeded = job_model.execution_status == JobStatus.SUCCEEDED
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    with ui.column().classes("w-full p-4 gap-3"):
        # ---- header row: instance metadata + bulk actions ----
        with ui.row().classes("w-full items-center gap-3 flex-wrap"):
            ui.label(instance_id).classes("text-sm font-bold text-gray-800 font-mono")
            if not job_succeeded:
                ui.label(f"({job_model.execution_status})").classes(
                    "text-[11px] text-amber-600"
                )
            if diameter:
                ui.label(f"diameter {diameter:.0f} Å").classes("text-[11px] text-gray-500")
            if job_dir:
                ui.label(str(job_dir)).classes(
                    "text-[10px] text-gray-400 font-mono truncate flex-1"
                ).tooltip(str(job_dir))

        if not job_dir:
            ui.label("Job directory not found on disk.").classes("text-xs text-red-500")
            return

        if not job_succeeded and not (job_dir / "candidates.star").exists():
            ui.label(
                "Extraction hasn't produced candidates.star yet. "
                "Previews can be generated once the job completes."
            ).classes("text-xs text-gray-500 italic")
            return

        # ---- bulk action row ----
        has_imod_models = (job_dir / "vis" / "imodPartRad").exists() and any(
            (job_dir / "vis" / "imodPartRad").glob("*.mod")
        )

        with ui.row().classes("w-full items-center gap-2 flex-wrap"):
            gen_missing_btn = ui.button(
                "Generate Missing Previews",
                icon="image",
                on_click=lambda: _handle_generate_for_instance(
                    instance_id, job_model, job_dir, project_path, False, gen_missing_btn
                ),
            ).props("dense no-caps unelevated").classes(
                "bg-purple-50 text-purple-700 border border-purple-200 px-3"
            )
            regen_btn = ui.button(
                "Force Re-render All",
                icon="refresh",
                on_click=lambda: _handle_generate_for_instance(
                    instance_id, job_model, job_dir, project_path, True, regen_btn
                ),
            ).props("dense no-caps flat").classes("text-xs text-gray-500")

            ui.element("div").classes("border-l border-gray-200 h-5 mx-1")

            imod_btn = ui.button(
                "Regenerate IMOD Models" if has_imod_models else "Generate IMOD Models",
                icon="scatter_plot",
                on_click=lambda: _handle_generate_imod_for_instance(
                    instance_id, job_model, job_dir, project_path, imod_btn
                ),
            ).props("dense no-caps unelevated").classes(
                "bg-blue-50 text-blue-700 border border-blue-200 px-3"
            )
            ui.label(
                f"diameter {diameter:.0f} Å · drives the .mod overlays in 3dmod commands below"
            ).classes("text-[10px] text-gray-500 italic")

            ui.space()
            manifest = read_preview_manifest(job_dir)
            if manifest:
                if manifest.get("score_field"):
                    ui.label(f"colored by {manifest['score_field']}").classes(
                        "text-[10px] text-gray-500 italic"
                    )
                if manifest.get("slab_mode"):
                    ui.label(f"slab: {manifest['slab_mode']}").classes(
                        "text-[10px] text-gray-500 italic"
                    )

        # ---- per-tomogram cards ----
        rows = _collect_tomos_for_instance(job_dir, project_path)
        if not rows:
            ui.label("No tomograms found for this job.").classes("text-xs text-gray-500 italic")
            return

        with ui.row().classes("w-full flex-wrap gap-3"):
            for r in rows:
                _render_tomo_card(r)


def _render_tomo_card(r: dict) -> None:
    with ui.card().classes(
        "border border-gray-200 shadow-sm overflow-hidden bg-white p-0"
    ).style("width: 460px;"):
        # title bar — position prominently shown so the user can find what they want
        with ui.row().classes(
            "w-full items-center justify-between px-3 py-1.5 bg-gray-50 border-b border-gray-100"
        ):
            with ui.column().classes("gap-0"):
                ui.label(r["position_label"]).classes("text-sm font-bold text-gray-800")
                ui.label(r["tomo_name"]).classes(
                    "text-[10px] font-mono text-gray-500 truncate"
                ).tooltip(r["tomo_name"])
            with ui.column().classes("gap-0 items-end"):
                if r["n_candidates"] is not None:
                    ui.label(f"N={r['n_candidates']}").classes(
                        "text-xs font-mono text-gray-700"
                    )
                if r["score_range"]:
                    ui.label(
                        f"{r['score_range'][0]:.2f}–{r['score_range'][1]:.2f}"
                    ).classes("text-[10px] font-mono text-gray-500")

        # image area
        if r["status"] == "ok" and r["preview_png"]:
            ui.image(_preview_url(r["preview_png"])).props("loading=lazy").style(
                "width: 100%; max-height: 460px; object-fit: contain; "
                "background: #000; display: block; cursor: zoom-in;"
            ).on(
                "click",
                lambda _e=None, name=r["tomo_name"], png=r["preview_png"], data=r: _open_zoom(name, png, data),
            )
        elif r["status"] == "missing-volume":
            with ui.element("div").classes(
                "w-full flex items-center justify-center bg-amber-50 text-amber-700"
            ).style("height: 220px;"):
                with ui.column().classes("items-center gap-1"):
                    ui.icon("image_not_supported", size="32px")
                    ui.label("No reconstructed tomogram on disk").classes("text-xs")
        elif r["status"] == "errored":
            with ui.element("div").classes(
                "w-full flex items-center justify-center bg-red-50 text-red-700"
            ).style("height: 220px;"):
                with ui.column().classes("items-center gap-1"):
                    ui.icon("error_outline", size="32px")
                    ui.label("Render error").classes("text-xs")
                    if r.get("error"):
                        ui.label(r["error"]).classes("text-[10px] font-mono px-2 truncate").tooltip(
                            r["error"]
                        )
        else:
            with ui.element("div").classes(
                "w-full flex items-center justify-center bg-gray-50 text-gray-500"
            ).style("height: 220px;"):
                with ui.column().classes("items-center gap-1"):
                    ui.icon("hourglass_empty", size="32px")
                    ui.label("Preview not generated yet").classes("text-xs")
                    ui.label("Use Generate Missing above").classes("text-[10px] italic")

        # 3dmod copy command row — included whether or not the preview rendered,
        # since 3dmod is the fallback for inspecting the volume in 3D
        if r["vol_path"]:
            mod = r.get("mod_path")
            cmd = f"3dmod {r['vol_path']} {mod}" if mod else f"3dmod {r['vol_path']}"
            with ui.row().classes("w-full items-center gap-1 px-2 py-1.5 border-t border-gray-100"):
                ui.label("3dmod").classes("text-[9px] uppercase font-bold text-gray-400 w-12")
                ui.input(value=cmd).props(
                    "dense outlined readonly hide-bottom-space"
                ).classes("text-xs font-mono flex-1").style("min-width: 0;")
                if not mod:
                    ui.icon("info", size="14px").classes("text-gray-400").tooltip(
                        "No IMOD model overlay — generate them in the extract job tab"
                    )
                ui.button(
                    icon="content_copy",
                    on_click=lambda c=cmd: (
                        ui.clipboard.write(c),
                        ui.notify("Copied", type="positive", timeout=800),
                    ),
                ).props("flat dense round size=sm").classes(
                    "text-gray-500 hover:text-gray-800"
                ).tooltip("Copy 3dmod command")
        else:
            with ui.row().classes("w-full items-center px-2 py-1.5 border-t border-gray-100"):
                ui.label("No volume path resolved — cannot build 3dmod command").classes(
                    "text-[10px] text-amber-600 italic"
                )


# ---------------------------------------------------------------------------
# Zoom dialog
# ---------------------------------------------------------------------------


def _open_zoom(tomo_name: str, png_path: str, entry: dict) -> None:
    with ui.dialog() as dlg, ui.card().classes("p-2 bg-black"):
        with ui.row().classes("w-full items-center justify-between mb-1 px-1"):
            with ui.column().classes("gap-0"):
                ui.label(entry.get("position_label") or tomo_name).classes(
                    "text-sm font-bold text-white"
                )
                ui.label(tomo_name).classes("text-[10px] text-gray-300 font-mono")
            ui.button(icon="close", on_click=dlg.close).props(
                "flat dense round size=sm"
            ).classes("text-white")
        ui.image(_preview_url(png_path)).style(
            "max-width: 92vw; max-height: 84vh; object-fit: contain; display: block;"
        )
    dlg.open()


# ---------------------------------------------------------------------------
# Generation handler
# ---------------------------------------------------------------------------


def _make_imod_command_runner():
    """Build a command runner that wraps shell commands in the IMOD container."""
    from services.computing.container_service import get_container_service

    container_service = get_container_service()

    def runner(cmd: str, cwd: Path) -> None:
        import subprocess

        wrapped = container_service.wrap_command_for_tool(
            cmd, cwd=cwd, tool_name="imod", additional_binds=[str(cwd)]
        )
        result = subprocess.run(wrapped, shell=True, capture_output=True, text=True, cwd=cwd)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container command failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

    return runner


def _generate_imod_sync(
    candidates_star: Path, tomograms_star: Path, diameter: float, job_dir: Path, project_path: Path
) -> None:
    """Run inside run.io_bound — point2model is invoked through the container."""
    generate_candidate_vis(
        candidates_star=candidates_star,
        tomograms_star=tomograms_star,
        particle_diameter_ang=diameter,
        output_dir=job_dir,
        command_runner=_make_imod_command_runner(),
        project_root=project_path,
    )


async def _handle_generate_imod_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, btn
) -> None:
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot generate IMOD models",
            type="negative", timeout=4000,
        )
        return

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    btn.props("loading")
    try:
        await run.io_bound(
            _generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path,
        )
        ui.notify("IMOD models generated — 3dmod commands now include the overlay", type="positive", timeout=3000)
    except Exception as e:
        traceback.print_exc()
        ui.notify(f"IMOD generation failed: {e}", type="negative", timeout=5000)
    finally:
        btn.props(remove="loading")
        # Refresh so the per-tomo cards rebuild their 3dmod cmds with the new .mod paths.
        _render_instance_section.refresh(instance_id, job_model, project_path)


async def _handle_generate_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, force: bool, btn
) -> None:
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot render previews",
            type="negative", timeout=4000,
        )
        return

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    btn.props("loading")
    try:
        summary = await run.io_bound(
            generate_candidate_previews,
            candidates_star,
            tomograms_star,
            diameter,
            job_dir,
            project_path,
            None,
            force,
            "central",
        )
        n_new = len(summary["ok"])
        n_cached = len(summary["skipped_cached"])
        n_missing = len(summary["missing_volume"])
        n_err = len(summary["errored"])
        msg = f"Previews: {n_new} rendered, {n_cached} cached"
        if n_missing:
            msg += f", {n_missing} missing volume"
        if n_err:
            msg += f", {n_err} errored"
        ui.notify(msg, type="positive" if not n_err else "warning", timeout=4000)
    except Exception as e:
        traceback.print_exc()
        ui.notify(f"Preview generation failed: {e}", type="negative", timeout=5000)
    finally:
        btn.props(remove="loading")
        _render_instance_section.refresh(instance_id, job_model, project_path)
