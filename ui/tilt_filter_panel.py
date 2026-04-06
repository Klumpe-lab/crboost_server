# ui/tilt_filter_panel.py
"""
Standalone tilt filtering tool — accessed via the sidebar funnel icon.

Auto-detects TS_CTF output, generates thumbnails from MRC,
provides a gallery grouped by position/beam/tilt-series for manual good/bad labelling.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
from pathlib import Path
from typing import Dict

from nicegui import ui

from services.models_base import JobStatus
from services.project_state import get_project_state, get_state_service
from services.tilt_series_service import (
    apply_labels,
    filter_good_tilts,
    get_label_summary,
    get_tilt_image_paths,
    load_tilt_series,
    write_tilt_series,
)
from ui.styles import MONO
from ui.ui_state import get_ui_state_manager

logger = logging.getLogger(__name__)

# ── Palette ──────────────────────────────────────────────────────────────────
FONT = "font-family: system-ui, -apple-system, sans-serif;"
CLR_HEADING = "#0f172a"
CLR_LABEL = "#475569"
CLR_SUBLABEL = "#94a3b8"
CLR_GHOST = "#cbd5e1"
CLR_BORDER = "#e2e8f0"
CLR_ACCENT = "#2563eb"
CLR_SUCCESS = "#0d9488"
CLR_ERROR = "#be4343"
CLR_POS_BG = "#f1f5f9"
CARD = (
    f"background: white; border-radius: 6px; border: 1px solid {CLR_BORDER}; box-shadow: 0 1px 2px rgba(15,23,42,0.04);"
)
SEC = f"border: 1px solid {CLR_BORDER}; border-radius: 5px; padding: 6px 8px; background: #f8fafc;"

_POS_RE = re.compile(r"Position_(\d+)(?:_(\d+))?")


# ── Tiny helpers ─────────────────────────────────────────────────────────────


def _hdr(icon, text):
    with ui.row().classes("items-center gap-1"):
        ui.icon(icon, size="12px").style(f"color: {CLR_SUBLABEL};")
        ui.label(text).style(
            f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_HEADING}; "
            "text-transform: uppercase; letter-spacing: 0.03em;"
        )


def _chip(label, value, color=CLR_LABEL):
    with ui.column().classes("items-center gap-0"):
        ui.label(str(value)).style(f"{MONO} font-size: 13px; font-weight: 700; color: {color};")
        ui.label(label).style(f"{FONT} font-size: 7px; color: {CLR_SUBLABEL}; text-transform: uppercase;")


def _btn_primary(text, icon, on_click):
    return (
        ui.button(text, icon=icon, on_click=on_click)
        .props("no-caps unelevated dense")
        .style(
            f"{FONT} font-size: 10px; font-weight: 500; padding: 2px 10px; "
            f"border-radius: 5px; background: {CLR_ACCENT}; color: white;"
        )
    )


def _btn_flat(text, icon, on_click):
    return (
        ui.button(text, icon=icon, on_click=on_click)
        .props("no-caps flat dense")
        .style(f"{FONT} font-size: 10px; font-weight: 500; padding: 2px 8px; color: {CLR_LABEL};")
    )


def _meta_row(label, value):
    with ui.row().classes("items-baseline gap-2"):
        ui.label(label).style(
            f"{FONT} font-size: 7px; color: {CLR_SUBLABEL}; text-transform: uppercase; min-width: 60px;"
        )
        ui.label(str(value)).style(f"{MONO} font-size: 9px; color: {CLR_LABEL}; word-break: break-all;")


def _parse_pos_beam(ts_name: str):
    """Extract (position, beam) from a tilt-series name like Position_9 or Position_9_2."""
    m = _POS_RE.search(ts_name)
    if m:
        return int(m.group(1)), int(m.group(2)) if m.group(2) else 1
    return None, None


def _find_ts_ctf_star(project_path):
    state = get_project_state()
    if not state:
        return None
    for _iid, jm in state.jobs.items():
        if jm.job_type and jm.job_type.value == "tsCtf" and jm.execution_status == JobStatus.SUCCEEDED:
            star = jm.paths.get("output_star")
            if star:
                p = Path(star) if Path(star).is_absolute() else project_path / star
                if p.exists():
                    return p
            if jm.relion_job_name:
                p = project_path / jm.relion_job_name / "ts_ctf_tilt_series.star"
                if p.exists():
                    return p
    return None


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL
# ═════════════════════════════════════════════════════════════════════════════


def build_tilt_filter_panel(backend) -> None:
    ui_mgr = get_ui_state_manager()
    project_path = ui_mgr.project_path
    if not project_path:
        ui.label("No project loaded").classes("text-red-500 p-4")
        return

    # ── Header ──
    with (
        ui.row()
        .classes("w-full items-center px-4 py-2 border-b bg-white gap-2 shrink-0")
        .style(f"border-color: {CLR_BORDER};")
    ):
        ui.icon("filter_alt", size="18px").style(f"color: {CLR_ACCENT};")
        ui.label("Tilt Filter").style(
            f"{FONT} font-size: 14px; font-weight: 700; color: {CLR_HEADING}; letter-spacing: -0.02em;"
        )
        ui.space()
        meta_vis = {"v": False}
        meta_ref = {"el": None}

        def _toggle_meta():
            meta_vis["v"] = not meta_vis["v"]
            if meta_ref["el"]:
                meta_ref["el"].style(f"display: {'flex' if meta_vis['v'] else 'none'};")

        ui.button(icon="info_outline", on_click=_toggle_meta).props("flat dense round size=xs").style(
            f"color: {CLR_SUBLABEL};"
        ).tooltip("Source metadata")

    with ui.scroll_area().classes("w-full flex-1"):
        with ui.column().classes("w-full gap-2 p-3"):
            ts_ctf_star = _find_ts_ctf_star(project_path)

            # ── Metadata (hidden) ──
            mc = ui.column().classes("w-full gap-0.5").style(f"{SEC} display: none;")
            meta_ref["el"] = mc
            with mc:
                if ts_ctf_star:
                    _meta_row("Source star", str(ts_ctf_star))
                state = get_project_state()
                pd_str = state.tilt_filter_png_dir if state else None
                if pd_str:
                    _meta_row("Thumbnails", pd_str)
                _meta_row("Project", str(project_path))

            if not ts_ctf_star:
                _render_waiting()
                return

            # ── DL config (collapsed) ──
            _render_dl_config()

            # ── Stats ──
            stats_c = ui.element("div").classes("w-full")

            # ── Gallery ──
            png_dir = Path(pd_str) if pd_str else project_path / "TiltFilter" / "png"
            has_pngs = png_dir.exists() and any(png_dir.glob("*.png"))
            gallery_c = ui.column().classes("w-full gap-0")

            if not has_pngs:
                with gallery_c:
                    _render_generate(ts_ctf_star, project_path, png_dir, gallery_c, stats_c)
            else:
                _build_gallery(ts_ctf_star, project_path, png_dir, gallery_c, stats_c)


def render_tilt_filter_job_panel(job_type, instance_id, job_model, backend, ui_mgr, save_handler) -> None:
    """Entry point for the tilt filter when rendered as a pipeline job (full-panel plugin)."""
    project_path = ui_mgr.project_path
    if not project_path:
        ui.label("No project loaded").classes("text-red-500 p-4")
        return

    with ui.scroll_area().classes("w-full flex-1"):
        with ui.column().classes("w-full gap-2 p-3"):
            ts_ctf_star = _find_ts_ctf_star(project_path)

            if not ts_ctf_star:
                _render_waiting()
                return

            # ── Stats + Gallery containers (created before DL config so it can reference them) ──
            stats_c = ui.element("div").classes("w-full")
            state = get_project_state()
            pd_str = state.tilt_filter_png_dir if state else None
            png_dir = Path(pd_str) if pd_str else project_path / "TiltFilter" / "png"
            gallery_c = ui.column().classes("w-full gap-0")

            # ── DL config (collapsed) ──
            _render_dl_config(
                job_model=job_model,
                backend=backend,
                project_path=project_path,
                gallery_c=gallery_c,
                stats_c=stats_c,
                png_dir=png_dir,
            )

            # ── Gallery ──
            has_pngs = png_dir.exists() and any(png_dir.glob("*.png"))

            if not has_pngs:
                with gallery_c:
                    _render_generate(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model=job_model)
            else:
                _build_gallery(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model=job_model)


def _render_waiting():
    with ui.column().classes("w-full items-center justify-center gap-2 py-12"):
        ui.icon("hourglass_empty", size="48px").style(f"color: {CLR_GHOST};")
        ui.label("Run the pipeline through TS CTF first.").style(f"{FONT} font-size: 12px; color: {CLR_LABEL};")
        ui.label("The tilt filter auto-detects completed CTF results.").style(
            f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};"
        )


def _render_dl_config(job_model=None, backend=None, project_path=None, gallery_c=None, stats_c=None, png_dir=None):
    with (
        ui.expansion("Deep Learning Auto-Filter", icon="smart_toy")
        .props("dense")
        .classes("w-full")
        .style(f"{CARD} overflow: hidden;")
    ):
        with ui.column().classes("w-full gap-2 px-2 pb-2"):
            vals = {
                "model": getattr(job_model, "model_name", "default") if job_model else "default",
                "threshold": getattr(job_model, "prob_threshold", 0.1) if job_model else 0.1,
                "action": getattr(job_model, "prob_action", "assignToGood") if job_model else "assignToGood",
            }

            with ui.row().classes("gap-3 flex-wrap items-end"):
                with ui.column().classes("gap-0"):
                    ui.label("Model").style(f"{FONT} font-size: 8px; color: {CLR_SUBLABEL};")
                    model_sel = (
                        ui.select(["default", "binary", "oneclass"], value=vals["model"])
                        .props("dense outlined hide-bottom-space")
                        .classes("w-32")
                        .style(f"{MONO} font-size: 10px;")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Threshold").style(f"{FONT} font-size: 8px; color: {CLR_SUBLABEL};")
                    thresh_inp = (
                        ui.number(value=vals["threshold"], min=0.0, max=1.0, step=0.05, format="%.2f")
                        .props("dense outlined hide-bottom-space")
                        .classes("w-20")
                        .style(f"{MONO} font-size: 10px;")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Low-conf. action").style(f"{FONT} font-size: 8px; color: {CLR_SUBLABEL};")
                    action_sel = (
                        ui.select({"assignToGood": "Keep", "assignToBad": "Remove"}, value=vals["action"])
                        .props("dense outlined hide-bottom-space")
                        .classes("w-28")
                        .style(f"{FONT} font-size: 10px;")
                    )

            status_row = ui.row().classes("w-full items-center gap-2")

            if job_model is not None and backend is not None and project_path is not None:

                async def _run_dl():
                    # Sync UI values to job model
                    job_model.model_name = model_sel.value
                    job_model.prob_threshold = thresh_inp.value
                    job_model.prob_action = action_sel.value

                    state = get_project_state()
                    if state:
                        state.mark_dirty()
                        await get_state_service().save_project()

                    status_row.clear()
                    with status_row:
                        ui.spinner(size="sm").style(f"color: {CLR_ACCENT};")
                        status_lbl = ui.label("Submitting to SLURM...").style(
                            f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};"
                        )

                    instance_id = None
                    for iid, jm in (state.jobs if state else {}).items():
                        if jm is job_model:
                            instance_id = iid
                            break
                    if not instance_id:
                        status_row.clear()
                        with status_row:
                            ui.label("Error: could not find job instance").style(f"color: {CLR_ERROR};")
                        return

                    result = await backend.submit_tilt_filter_dl(project_path, instance_id)
                    if not result.get("success"):
                        status_row.clear()
                        with status_row:
                            ui.icon("error", size="14px").style(f"color: {CLR_ERROR};")
                            ui.label(result.get("error", "Unknown error")).style(
                                f"{FONT} font-size: 9px; color: {CLR_ERROR};"
                            )
                        return

                    slurm_id = result.get("slurm_job_id", "?")
                    job_dir = Path(result.get("job_dir", ""))
                    status_lbl.text = f"SLURM job {slurm_id} running..."

                    # Poll for completion
                    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
                    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"
                    while True:
                        await asyncio.sleep(5)
                        if success_file.exists():
                            break
                        if failure_file.exists():
                            status_row.clear()
                            with status_row:
                                ui.icon("error", size="14px").style(f"color: {CLR_ERROR};")
                                ui.label("DL filter failed. Check logs in TiltFilter/dl_run/").style(
                                    f"{FONT} font-size: 9px; color: {CLR_ERROR};"
                                )
                            job_model.execution_status = JobStatus.FAILED
                            if state:
                                state.mark_dirty()
                                await get_state_service().save_project()
                            return

                    # Success — reload labels into gallery
                    status_row.clear()
                    with status_row:
                        ui.icon("check_circle", size="14px").style(f"color: {CLR_SUCCESS};")
                        ui.label("DL filter complete. Reloading labels...").style(
                            f"{FONT} font-size: 9px; color: {CLR_SUCCESS};"
                        )

                    labeled_star = job_dir / "filtered" / "tiltseries_labeled.star"
                    if labeled_star.exists():
                        try:
                            ts_data = await asyncio.to_thread(load_tilt_series, str(labeled_star), str(project_path))
                            # Extract DL labels into job model
                            if "cryoBoostDlLabel" in ts_data.all_tilts_df.columns:
                                new_labels = {}
                                for _, row in ts_data.all_tilts_df.iterrows():
                                    key = row.get("cryoBoostKey", "")
                                    label = row.get("cryoBoostDlLabel", "good")
                                    if key:
                                        new_labels[key] = label
                                job_model.tilt_labels = new_labels
                                if state:
                                    state.tilt_filter_labels = new_labels

                            # Write filtered output for downstream
                            good_data = filter_good_tilts(ts_data)
                            out_dir = project_path / "TiltFilter"
                            out_dir.mkdir(parents=True, exist_ok=True)
                            filtered_p = out_dir / "tiltseries_filtered.star"
                            await asyncio.to_thread(write_tilt_series, good_data, filtered_p, "tilt_series_filtered")
                            labeled_p = out_dir / "tiltseries_labeled.star"
                            await asyncio.to_thread(write_tilt_series, ts_data, labeled_p, "tilt_series_labeled")

                            job_model.execution_status = JobStatus.SUCCEEDED
                            job_model.paths["output_star"] = str(filtered_p)
                            if state:
                                state.mark_dirty()
                                await get_state_service().save_project()

                            ui.notify(
                                f"DL filter applied: {good_data.num_tilts} good tilts", type="positive", timeout=5000
                            )

                            # Refresh gallery if containers available
                            ts_ctf_star = _find_ts_ctf_star(project_path)
                            if gallery_c is not None and stats_c is not None and ts_ctf_star:
                                gallery_c.clear()
                                with gallery_c:
                                    _build_gallery(
                                        ts_ctf_star,
                                        project_path,
                                        png_dir or (project_path / "TiltFilter" / "png"),
                                        gallery_c,
                                        stats_c,
                                        job_model=job_model,
                                    )
                        except Exception as e:
                            logger.exception("Failed to reload DL labels")
                            status_row.clear()
                            with status_row:
                                ui.label(f"Reload error: {e}").style(f"color: {CLR_ERROR};")

                _btn_primary("Run DL Filter", "smart_toy", _run_dl)
            else:
                ui.label("Add this job to the pipeline to enable DL auto-filtering.").style(
                    f"{FONT} font-size: 8px; color: {CLR_SUBLABEL}; font-style: italic;"
                )


# ── Generate ─────────────────────────────────────────────────────────────────


def _render_generate(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model=None):
    with ui.column().classes("w-full items-center justify-center gap-3 py-10"):
        ui.icon("collections", size="48px").style(f"color: {CLR_GHOST};")
        ui.label("Generate tilt thumbnails to begin inspection.").style(f"{FONT} font-size: 11px; color: {CLR_LABEL};")
        spinner = ui.spinner(size="lg").style("display: none;")
        status_lbl = ui.label("").style(f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};")

        async def go():
            spinner.style("display: block;")
            status_lbl.text = "Loading tilt series..."
            try:
                td = await asyncio.to_thread(load_tilt_series, str(ts_ctf_star), str(project_path))
                paths = get_tilt_image_paths(td, project_path)
                n = len(paths)
                status_lbl.text = f"Converting {n} MRC images..."
                png_dir.mkdir(parents=True, exist_ok=True)
                from filterTilts.image_processor import ImageProcessor

                proc = ImageProcessor(target_size=384, max_workers=min(16, max(1, n)))
                await asyncio.to_thread(proc.batch_convert, paths, n, str(png_dir), True)
                st = get_project_state()
                if st:
                    st.tilt_filter_png_dir = str(png_dir)
                    st.mark_dirty()
                    await get_state_service().save_project()
                spinner.style("display: none;")
                status_lbl.text = ""
                ui.notify(f"{n} thumbnails generated", type="positive")
                gallery_c.clear()
                with gallery_c:
                    _build_gallery(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model=job_model)
            except Exception as e:
                logger.exception("Thumbnail generation failed")
                spinner.style("display: none;")
                status_lbl.text = f"Error: {e}"
                ui.notify(f"Failed: {e}", type="negative")

        _btn_primary("Generate Thumbnails", "auto_fix_high", go)


# ═════════════════════════════════════════════════════════════════════════════
# GALLERY
# ═════════════════════════════════════════════════════════════════════════════


def _build_gallery(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model=None):
    try:
        ts_data = load_tilt_series(str(ts_ctf_star), str(project_path))
    except Exception as e:
        ui.label(f"Failed to load tilt series: {e}").style(f"color: {CLR_ERROR};")
        return
    _render_gallery_content(ts_data, project_path, png_dir, gallery_c, stats_c, job_model=job_model)


def _render_gallery_content(ts_data, project_path, png_dir, gallery_c, stats_c, job_model=None):
    state = get_project_state()
    if job_model is not None:
        labels = dict(job_model.tilt_labels) if job_model.tilt_labels else {}
    else:
        labels = dict(state.tilt_filter_labels) if state else {}

    if labels:
        apply_labels(ts_data, labels)
    elif "cryoBoostDlLabel" not in ts_data.all_tilts_df.columns:
        ts_data.all_tilts_df["cryoBoostDlLabel"] = "good"
        ts_data.all_tilts_df["cryoBoostDlProbability"] = 1.0

    df = ts_data.all_tilts_df
    png_map: Dict[str, Path] = {f.stem: f for f in sorted(png_dir.glob("*.png"))}
    df["_png"] = df["cryoBoostKey"].map(lambda k: str(png_map.get(k, "")))

    # ── Live stats ──
    def _refresh_stats():
        stats_c.clear()
        s = get_label_summary(ts_data)
        with stats_c:
            with ui.row().classes("w-full items-center gap-4 py-1"):
                _chip("Total", s["total"])
                _chip("Good", s["good"], CLR_SUCCESS)
                _chip("Bad", s["bad"], CLR_ERROR)
                pct = s["bad"] / max(1, s["total"]) * 100
                _chip("Removed", f"{pct:.1f}%", CLR_ERROR if pct > 20 else CLR_LABEL)

    _refresh_stats()

    # ── Build position → tilt-series hierarchy ──
    ts_names = sorted(df["rlnTomoName"].unique().tolist())
    hierarchy: Dict[int, list] = {}
    for tn in ts_names:
        pos, beam = _parse_pos_beam(tn)
        pos_key = pos if pos is not None else 0
        hierarchy.setdefault(pos_key, []).append((tn, beam))

    # ── Actions + collapse controls ──
    group_refs: list = []

    view_opts = {"sort": "acquisition"}

    with ui.row().classes("w-full items-center gap-2 py-1 flex-wrap"):
        _btn_primary("Save Labels", "save", lambda: _save())
        _btn_flat("Set All Good", "check_circle_outline", lambda: _set_all_good())

        ui.element("div").style("width: 1px; height: 16px; background: #e2e8f0; margin: 0 2px;")
        _btn_flat("Expand All", "unfold_more", lambda: _expand_all(True))
        _btn_flat("Collapse All", "unfold_less", lambda: _expand_all(False))

        ui.element("div").style("width: 1px; height: 16px; background: #e2e8f0; margin: 0 2px;")
        with ui.column().classes("gap-0"):
            ui.label("Sort within group").style(f"{FONT} font-size: 7px; color: {CLR_SUBLABEL};")
            sort_sel = (
                ui.select(
                    {"acquisition": "Acquisition order", "angle": "Tilt angle", "probability": "DL probability"},
                    value="acquisition",
                )
                .props("dense borderless hide-bottom-space")
                .style(f"{FONT} font-size: 10px; color: {CLR_LABEL};")
                .classes("w-36")
            )

        ui.space()
        bad_only = ui.checkbox("Show only removed").style(f"{FONT} font-size: 10px; color: {CLR_LABEL};")

    # ── Save explanation ──
    save_info_c = ui.column().classes("w-full gap-0.5").style(f"{SEC} display: none;")

    # ── Groups ──
    group_c = ui.column().classes("w-full gap-1")
    # Track which groups are expanded by ts_name so we can preserve across re-renders
    expand_state: Dict[str, bool] = {}

    def _sort_ts_df(ts_df):
        s = view_opts["sort"]
        if s == "angle" and "rlnTomoNominalStageTiltAngle" in ts_df.columns:
            return ts_df.sort_values("rlnTomoNominalStageTiltAngle").reset_index(drop=True)
        if s == "probability" and "cryoBoostDlProbability" in ts_df.columns:
            return ts_df.sort_values("cryoBoostDlProbability", ascending=True).reset_index(drop=True)
        return ts_df  # acquisition order = default dataframe order

    rendering = {"active": False}

    def _render_groups():
        # Save current expand states before clearing
        for refs in group_refs:
            expand_state[refs["ts_name"]] = refs["expanded"]["v"]

        group_c.clear()
        group_refs.clear()
        show_bad = bad_only.value

        with group_c:
            # Show spinner briefly if many groups
            if rendering["active"]:
                return
            rendering["active"] = True

            for pos_key in sorted(hierarchy):
                items = hierarchy[pos_key]
                for ts_name, beam in items:
                    ts_df = df[df["rlnTomoName"] == ts_name]
                    if show_bad:
                        ts_df = ts_df[ts_df["cryoBoostDlLabel"] == "bad"]
                    if ts_df.empty:
                        continue
                    ts_df = _sort_ts_df(ts_df)
                    # Restore expand state or default to collapsed
                    start_expanded = expand_state.get(ts_name, False)
                    refs = _render_ts_group(
                        ts_name, pos_key, beam, ts_df, labels, df, ts_data, _refresh_stats, project_path, start_expanded
                    )
                    group_refs.append(refs)

            rendering["active"] = False

    _render_groups()
    bad_only.on_value_change(lambda _: _render_groups())
    sort_sel.on_value_change(lambda e: (view_opts.update(sort=e.value), _render_groups()))

    def _expand_all(expand):
        for refs in group_refs:
            body_el, chev_el, exp_state = refs["body"], refs["chevron"], refs["expanded"]
            exp_state["v"] = expand
            body_el.style(f"display: {'flex' if expand else 'none'};")
            chev_el.style(
                f"color: {CLR_SUBLABEL}; transition: transform 0.15s; transform: rotate({'0' if expand else '-90'}deg);"
            )

    async def _save():
        try:
            out_dir = project_path / "TiltFilter"
            out_dir.mkdir(parents=True, exist_ok=True)
            apply_labels(ts_data, labels)

            labeled_p = out_dir / "tiltseries_labeled.star"
            await asyncio.to_thread(write_tilt_series, ts_data, labeled_p, "tilt_series_labeled")

            good = filter_good_tilts(ts_data)
            filtered_p = out_dir / "tiltseries_filtered.star"
            await asyncio.to_thread(write_tilt_series, good, filtered_p, "tilt_series_filtered")

            # Persist labels and mark job complete
            if job_model is not None:
                job_model.tilt_labels = dict(labels)
                job_model.execution_status = JobStatus.SUCCEEDED
                job_model.paths["output_star"] = str(filtered_p)
                job_model.paths["output_processing"] = job_model.paths.get("input_processing", "")
            if state:
                state.tilt_filter_labels = labels
                state.mark_dirty()
                await get_state_service().save_project()

            sm = get_label_summary(ts_data)
            ui.notify(f"Saved: {sm['good']} good, {sm['bad']} bad", type="positive", timeout=4000)

            # Show save info
            save_info_c.clear()
            save_info_c.style("display: flex;")
            with save_info_c:
                _meta_row("Labeled (all tilts)", str(labeled_p))
                _meta_row("Filtered (good only)", str(filtered_p))
                if job_model is not None:
                    ui.label(
                        "Labels saved to the Tilt Filter job. Downstream jobs "
                        "(Reconstruct, Template Match) will use the filtered tilt set."
                    ).style(f"{FONT} font-size: 9px; color: {CLR_SUCCESS}; line-height: 1.3;")

        except Exception as e:
            logger.exception("Save failed")
            ui.notify(f"Save failed: {e}", type="negative")

    async def _set_all_good():
        for key in df["cryoBoostKey"].tolist():
            labels[key] = "good"
        df["cryoBoostDlLabel"] = "good"
        ts_data.all_tilts_df["cryoBoostDlLabel"] = "good"
        if job_model is not None:
            job_model.tilt_labels = dict(labels)
        if state:
            state.tilt_filter_labels = labels
            state.mark_dirty()
        _refresh_stats()
        _render_groups()
        ui.notify("All tilts set to good", type="info")


# ── Tilt-series group ────────────────────────────────────────────────────────


def _render_ts_group(
    ts_name, pos, beam, ts_df, labels, full_df, ts_data, refresh_stats, project_path, start_expanded=False
):
    n_total = len(ts_df)
    n_bad = int((ts_df["cryoBoostDlLabel"] == "bad").sum())
    expanded = {"v": start_expanded}

    with ui.element("div").classes("w-full").style(CARD):
        hdr = (
            ui.element("div")
            .classes("w-full")
            .style(
                f"display: flex; align-items: center; gap: 6px; padding: 4px 8px; "
                f"background: {CLR_POS_BG}; cursor: pointer; border-radius: 5px 5px 0 0;"
            )
        )
        body_display = "flex" if start_expanded else "none"
        body = ui.column().classes("w-full gap-0 px-1 pb-1").style(f"display: {body_display};")
        chev_rot = "0" if start_expanded else "-90"

        with hdr:
            chevron = ui.icon("expand_more", size="14px").style(
                f"color: {CLR_SUBLABEL}; transition: transform 0.15s; transform: rotate({chev_rot}deg);"
            )
            # Position / beam badge
            pos_txt = f"Pos {pos}" if pos else ts_name
            ui.label(pos_txt).style(f"{MONO} font-size: 10px; font-weight: 600; color: {CLR_HEADING};")
            if beam and beam > 1:
                ui.label(f"beam {beam}").style(
                    f"{FONT} font-size: 8px; color: {CLR_SUBLABEL}; "
                    f"background: {CLR_BORDER}; padding: 0 4px; border-radius: 3px;"
                )
            ui.label(ts_name).style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};")

            ui.space()
            cnt_lbl = ui.label(f"{n_total}").style(f"{MONO} font-size: 9px; color: {CLR_LABEL};")
            bad_lbl = ui.label(f"\u2212{n_bad}").style(
                f"{MONO} font-size: 9px; font-weight: 600; color: {CLR_ERROR}; {'display: none' if n_bad == 0 else ''};"
            )

        def _toggle():
            expanded["v"] = not expanded["v"]
            body.style(f"display: {'flex' if expanded['v'] else 'none'};")
            chevron.style(
                f"color: {CLR_SUBLABEL}; transition: transform 0.15s; "
                f"transform: rotate({'0' if expanded['v'] else '-90'}deg);"
            )

        hdr.on("click", _toggle)

        with body:
            with (
                ui.element("div")
                .classes("w-full")
                .style("display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 4px;")
            ):
                for _, row in ts_df.iterrows():
                    _render_card(
                        row, labels, full_df, ts_data, refresh_stats, cnt_lbl, bad_lbl, n_total, ts_df, project_path
                    )

    return {"body": body, "chevron": chevron, "expanded": expanded, "ts_name": ts_name}


# ── Card ─────────────────────────────────────────────────────────────────────


def _render_card(row, labels, full_df, ts_data, refresh_stats, cnt_lbl, bad_lbl, n_total, ts_df, project_path):
    key = row["cryoBoostKey"]
    png_path = row.get("_png", "")
    label = row.get("cryoBoostDlLabel", "good")
    prob = row.get("cryoBoostDlProbability", 1.0)
    angle = row.get("rlnTomoNominalStageTiltAngle", None)
    defocus_u = row.get("rlnDefocusU", None)
    motion = row.get("rlnAccumMotionTotal", None)

    is_bad = label == "bad"
    bdr = "#ef4444" if is_bad else "#d1d5db"

    with (
        ui.card()
        .tight()
        .classes("overflow-hidden p-0")
        .style(
            f"border: 1.5px solid {bdr}; border-radius: 4px; transition: border-color 0.12s; position: relative;"
        ) as card
    ):
        # ── Image ──
        img_src = None
        if png_path and Path(png_path).exists():
            try:
                with open(png_path, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                img_src = f"data:image/png;base64,{b64}"
            except Exception:
                pass

        if img_src:
            ui.image(img_src).classes("w-full").style("aspect-ratio: 1; object-fit: cover;")
        else:
            ui.icon("broken_image", size="28px").style(f"color: {CLR_GHOST}; margin: 20px auto;")

        # ── Bad dot ──
        indicator = ui.element("div").style(
            "position: absolute; top: 3px; right: 3px; width: 8px; height: 8px; "
            f"border-radius: 50%; background: {'#ef4444' if is_bad else 'transparent'}; "
            "border: 1px solid white;"
        )

        # ── Upsample button (stop propagation so it doesn't toggle label) ──
        if png_path:
            mrc_path = row.get("rlnMicrographName", "")

            def _upsample(_mrc=mrc_path, _key=key):
                _show_upsample(_mrc, _key, project_path)

            zoom_btn = (
                ui.button(icon="zoom_in")
                .props("flat round dense size=xs")
                .style(
                    "position: absolute; top: 2px; left: 2px; color: white; "
                    "background: rgba(0,0,0,0.3); width: 18px; height: 18px;"
                )
                .tooltip("Upsample & zoom")
            )
            zoom_btn.on("click.stop", _upsample)

        # ── Info strip ──
        with (
            ui.row()
            .classes("w-full px-1 py-0.5 items-center gap-1")
            .style(f"background: {'#fef2f2' if is_bad else '#fafafa'};")
        ):
            if angle is not None:
                ui.label(f"{angle:.0f}\u00b0").style(f"{MONO} font-size: 8px; font-weight: 600; color: {CLR_HEADING};")
            if isinstance(prob, (int, float)) and prob < 1.0:
                ui.label(f"p{prob:.2f}").style(f"{MONO} font-size: 7px; color: {CLR_SUBLABEL};")
            if defocus_u is not None and defocus_u > 0:
                ui.label(f"{defocus_u / 10000:.1f}\u00b5").style(f"{MONO} font-size: 7px; color: {CLR_SUBLABEL};")
            if motion is not None and motion > 0:
                ui.label(f"{motion:.1f}px").style(f"{MONO} font-size: 7px; color: {CLR_SUBLABEL};")

        # ── Toggle handler ──
        def toggle(k=key, c=card, ind=indicator, _ts=ts_df, _cl=cnt_lbl, _bl=bad_lbl):
            cur = labels.get(k)
            if cur is None:
                mask = full_df["cryoBoostKey"] == k
                cur = full_df.loc[mask, "cryoBoostDlLabel"].iloc[0] if mask.any() else "good"

            new = "good" if cur == "bad" else "bad"
            labels[k] = new
            full_df.loc[full_df["cryoBoostKey"] == k, "cryoBoostDlLabel"] = new
            ts_data.all_tilts_df.loc[ts_data.all_tilts_df["cryoBoostKey"] == k, "cryoBoostDlLabel"] = new

            nb = "#ef4444" if new == "bad" else "#d1d5db"
            c.style(
                f"border: 1.5px solid {nb}; border-radius: 4px; transition: border-color 0.12s; position: relative;"
            )
            ind.style(
                "position: absolute; top: 3px; right: 3px; width: 8px; height: 8px; "
                f"border-radius: 50%; background: {'#ef4444' if new == 'bad' else 'transparent'}; "
                "border: 1px solid white;"
            )

            n_bad_now = int((_ts["cryoBoostDlLabel"] == "bad").sum())
            _bl.text = f"\u2212{n_bad_now}"
            _bl.style(
                f"{MONO} font-size: 9px; font-weight: 600; color: {CLR_ERROR}; "
                f"{'display: none' if n_bad_now == 0 else ''};"
            )

            st = get_project_state()
            if st:
                st.tilt_filter_labels = labels
                st.mark_dirty()
            refresh_stats()

        card.on("click", toggle)


# ── Zoom / Upsample ─────────────────────────────────────────────────────────


def _render_mrc_preview(mrc_path: str, target_size: int = 1024) -> str:
    """Read an MRC, Fourier-crop to target_size, apply display-quality
    normalization, return base64-encoded PNG string.

    target_size=0 means full resolution (no cropping).
    """
    import io as _io

    import mrcfile
    import numpy as np
    from PIL import Image
    from scipy.fft import fft2, fftshift, ifft2, ifftshift
    from scipy.ndimage import gaussian_filter

    with mrcfile.open(mrc_path, permissive=True) as mrc:
        data = mrc.data.astype(np.float32)

    orig_shape = data.shape

    # Fourier crop if requested and needed
    if target_size > 0 and (data.shape[0] > target_size or data.shape[1] > target_size):
        ft = fftshift(fft2(data))
        new = np.zeros((target_size, target_size), dtype=ft.dtype)
        cy, cx = [d // 2 for d in data.shape]
        ny, nx = target_size // 2, target_size // 2
        sy = slice(cy - min(cy, ny), cy + min(cy, ny))
        sx = slice(cx - min(cx, nx), cx + min(cx, nx))
        dy = slice(ny - min(cy, ny), ny + min(cy, ny))
        dx = slice(nx - min(cx, nx), nx + min(cx, nx))
        new[dy, dx] = ft[sy, sx]
        data = ifft2(ifftshift(new)).real

    # Gentle denoise — scale sigma with resolution
    sigma = 0.4 if data.shape[0] <= 1024 else 0.7
    data = gaussian_filter(data, sigma=sigma)

    # Percentile-based contrast (robust to hot/dead pixels)
    p_lo, p_hi = np.percentile(data, [1.0, 99.0])
    data = np.clip(data, p_lo, p_hi)
    rng = p_hi - p_lo
    if rng > 1e-9:
        data = (data - p_lo) / rng
    else:
        data = np.zeros_like(data)

    data = (data * 255).astype(np.uint8)
    img = Image.fromarray(data, mode="L")
    buf = _io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode(), orig_shape


def _show_upsample(mrc_path: str, key: str, project_path):
    """Open a dialog with resolution selector and live re-rendering."""
    if not mrc_path:
        ui.notify("No MRC path available", type="warning")
        return

    abs_mrc = Path(mrc_path) if Path(mrc_path).is_absolute() else project_path / mrc_path

    if not abs_mrc.exists():
        ui.notify(f"MRC not found: {abs_mrc}", type="warning")
        return

    current = {"size": 1024}

    dlg = ui.dialog().props("maximized")
    with dlg:
        with ui.column().style("width: 100%; height: 100%; background: #0a0a0a; padding: 0; position: relative;"):
            # ── Top bar ──
            with ui.row().style(
                "position: absolute; top: 0; left: 0; right: 0; z-index: 10; padding: 8px 12px; "
                "background: rgba(0,0,0,0.7); align-items: center; gap: 8px; justify-content: space-between;"
            ):
                with ui.row().style("align-items: center; gap: 6px;"):
                    ui.label(key).style(f"{MONO} font-size: 10px; color: {CLR_GHOST};")
                    size_label = ui.label("").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};")

                res_buttons: dict = {}
                _BTN_BASE = f"{MONO} font-size: 9px; padding: 1px 8px; border-radius: 3px; min-height: 0; "
                _ACT = _BTN_BASE + "color: white; background: #334155;"
                _INACT = _BTN_BASE + f"color: {CLR_GHOST}; background: transparent;"

                def _select_res(s):
                    current["size"] = s
                    for sz_key, b in res_buttons.items():
                        b.style(_ACT if sz_key == s else _INACT)
                    _load_at_size(s)

                with ui.row().style("align-items: center; gap: 4px;"):
                    for sz, label in [(512, "512"), (1024, "1K"), (2048, "2K"), (0, "Full")]:
                        btn = (
                            ui.button(label, on_click=lambda _, s=sz: _select_res(s))
                            .props("dense no-caps flat")
                            .style(_ACT if sz == 1024 else _INACT)
                        )
                        res_buttons[sz] = btn

                    ui.button(icon="close", on_click=dlg.close).props("flat round dense").style("color: white;")

            # ── Image area ──
            img_holder = ui.column().style(
                "width: 100%; height: 100%; align-items: center; justify-content: center; padding-top: 40px;"
            )
            with img_holder:
                ui.spinner(size="lg").style("color: white;")
                ui.label("Generating preview...").style(f"{FONT} font-size: 10px; color: {CLR_GHOST}; margin-top: 8px;")

    def _load_at_size(sz):
        img_holder.clear()
        with img_holder:
            ui.spinner(size="lg").style("color: white;")
            sz_txt = "full resolution" if sz == 0 else f"{sz}px"
            ui.label(f"Rendering at {sz_txt}...").style(f"{FONT} font-size: 10px; color: {CLR_GHOST}; margin-top: 8px;")

        async def _do():
            try:
                b64, orig = await asyncio.to_thread(_render_mrc_preview, str(abs_mrc), sz)
                out_px = orig[0] if sz == 0 else min(sz, orig[0])
                size_label.text = f"{out_px}px (source: {orig[0]}\u00d7{orig[1]})"
                img_holder.clear()
                with img_holder:
                    ui.html(
                        f'<img src="data:image/png;base64,{b64}" '
                        'style="max-width: 95vw; max-height: calc(100vh - 50px); object-fit: contain;" />',
                        sanitize=False,
                    )
            except Exception as e:
                logger.exception("Upsample failed")
                img_holder.clear()
                with img_holder:
                    ui.label(f"Error: {e}").style(f"color: {CLR_ERROR};")

        ui.timer(0.05, _do, once=True)

    dlg.open()
    ui.timer(0.1, lambda: _load_at_size(1024), once=True)
