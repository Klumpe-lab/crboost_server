# ui/tilt_filter_panel.py
"""
The tilt-filter job panel (full-panel job plugin).

Runs the DL prediction as a SLURM job, generates thumbnails of fsMotion's
motion-corrected tilts, and provides a gallery grouped by position/beam/tilt-series
where the model's predictions are reviewed and overridden; Approve commits the verdict.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import urllib.parse
from pathlib import Path

import pandas as pd
from nicegui import ui

from backend import get_backend
from services.configs.config_service import get_config_service
from services.models_base import JobStatus
from services.tilt_series.build import parse_position
from services.project_state import get_state_service
from ui.components.buttons import house_button
from ui.components.fields import house_number, house_select
from ui.components.reactive import SingleFlight
from ui.current_project import current_project_state
from services.jobs.tilt_filter import FilterMode, effective_label, prediction_liveness, predictions_for, resolve_model
from services.tilt_series_service import fs_motion_star, generate_tilt_thumbnails, get_label_summary, load_tilt_series
from ui.status_indicator import _running_spinner_html
from ui.styles import MONO

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
CLR_WARN = "#d97706"
CLR_POS_BG = "#f1f5f9"
CARD = (
    f"background: white; border-radius: 6px; border: 1px solid {CLR_BORDER}; box-shadow: 0 1px 2px rgba(15,23,42,0.04);"
)
SEC = f"border: 1px solid {CLR_BORDER}; border-radius: 5px; padding: 6px 8px; background: #f8fafc;"
# Tilt cards: red is bad, grey good.
BAD_EDGE = "#ef4444"
GOOD_EDGE = "#d1d5db"
GOOD_DOT = "#64748b"


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


def _parse_pos_beam(ts_name: str):
    """Extract (position, beam) from a tilt-series name like Position_9 or Position_9_2."""
    parsed = parse_position(ts_name)
    if parsed is None:
        return None, None
    stage, beam = parsed
    return stage, beam or 1


def _find_ts_ctf_star(project_path):
    state = current_project_state()
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


def _find_fs_motion_warp_dir(project_path):
    """Locate the FS-motion job's `warp_frameseries` folder — it holds the
    per-tilt WarpTools XMLs with the real CTF-fit resolution + motion that the
    star hides behind 1e-6 placeholders. Returns None if not found / not run.
    See docs/preprocessing-metrics-inventory.md."""
    state = current_project_state()
    if not state:
        return None
    for _iid, jm in state.jobs.items():
        if jm.job_type and jm.job_type.value == "fsMotionAndCtf" and jm.relion_job_name:
            d = project_path / jm.relion_job_name.rstrip("/") / "warp_frameseries"
            if d.is_dir():
                return d
    return None


def _notify_finalize(res: dict) -> None:
    """Surface an Approve's outcome."""
    if res.get("success"):
        ui.notify(
            f"Filter committed: {res['kept']} tilts kept, {res['dropped']} dropped — "
            "alignment trims the tomostar to match when it runs.",
            type="positive",
            timeout=5000,
        )
    else:
        ui.notify(res.get("error") or "Filter commit failed.", type="negative")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL
# ═════════════════════════════════════════════════════════════════════════════


def render_tilt_filter_job_panel(job_type, instance_id, job_model, backend, ui_mgr, save_handler) -> None:
    """Entry point for the tilt filter when rendered as a pipeline job (full-panel plugin)."""
    project_path = ui_mgr.project_path
    if not project_path:
        ui.label("No project loaded").classes("text-red-500 p-4")
        return

    with ui.scroll_area().classes("w-full flex-1"):
        with ui.column().classes("w-full gap-2 p-3"):
            state = current_project_state()
            source_star = fs_motion_star(project_path, state)

            if not source_star:
                _render_waiting()
                return

            pd_str = state.tilt_filter_png_dir if state else None
            png_dir = Path(pd_str) if pd_str else project_path / "TiltFilter" / "png"

            def _reload_gallery():
                gallery_c.clear()
                with gallery_c:
                    _build_gallery(source_star, project_path, png_dir, gallery_c, stats_c, job_model, instance_id)

            # ── DL config (collapsed; DL review only, the mode is switched on the job row) ──
            if job_model.mode == FilterMode.DL_REVIEW:
                _render_dl_config(job_model, backend, project_path, instance_id, on_predictions=_reload_gallery)

            # ── Stats + Gallery ──
            stats_c = ui.element("div").classes("w-full")
            gallery_c = ui.column().classes("w-full gap-0")
            has_pngs = png_dir.exists() and any(png_dir.glob("*.png"))

            with gallery_c:
                if not has_pngs:
                    _render_generate(source_star, project_path, png_dir, gallery_c, stats_c, job_model, instance_id)
                else:
                    _build_gallery(source_star, project_path, png_dir, gallery_c, stats_c, job_model, instance_id)


def _render_waiting():
    with ui.column().classes("w-full items-center justify-center gap-2 py-12"):
        ui.icon("hourglass_empty", size="48px").style(f"color: {CLR_GHOST};")
        ui.label("Run motion correction (fsMotion) first.").style(f"{FONT} font-size: 12px; color: {CLR_LABEL};")
        ui.label("The tilt filter reads the motion-corrected tilt images.").style(
            f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};"
        )


def _render_dl_config(job_model, backend, project_path, instance_id, on_predictions) -> None:
    """The DL section: the model, Run DL, and the latest prediction run's status.
    The run is a SLURM job that the server monitor settles; this view only observes
    `job_model.predict_run`, so closing the tab changes nothing. `on_predictions` runs
    when a run this view saw in flight lands. The threshold lives with the gallery, whose
    labels it moves."""
    models = get_config_service().tilt_filter
    with (
        ui.expansion("Deep Learning Auto-Filter", icon="smart_toy")
        .props("dense")
        .classes("w-full")
        .style(f"{CARD} overflow: hidden;")
    ):
        with ui.column().classes("w-full gap-2 px-2 pb-2"):
            with ui.row().classes("gap-3 flex-wrap items-center"):
                chosen = job_model.model or models.default_model
                options = list(models.models)
                if chosen and chosen not in options:
                    options.append(chosen)  # gone from conf.yaml: still shown, and the marker says so

                async def _pick(e) -> None:
                    job_model.model = e.value
                    _check_model()
                    await get_backend().save_project(project_path, force=True, debounce_s=1.0)

                model_sel = house_select("Model", options, value=chosen, width="w-40", on_change=_pick)
                with ui.icon("error", size="14px").style(f"color: {CLR_ERROR};") as model_marker:
                    marker_tip = ui.tooltip("")

            flight = SingleFlight()

            async def _run_dl():
                async with flight("run") as acquired:
                    if not acquired:
                        return
                    res = await backend.submit_tilt_filter_predict(project_path, instance_id)
                    if not res.get("success"):
                        ui.notify(res.get("error") or "Could not submit the DL run.", type="negative")
                    _observe()

            with ui.row().classes("w-full items-center gap-2"):
                run_btn = house_button("Run DL filter", _run_dl, kind="accent")
                status_c = ui.row().classes("items-center gap-1")

            def _check_model() -> None:
                """Red marker and a disabled Run DL while the chosen model cannot run here."""
                try:
                    resolve_model(model_sel.value)
                except ValueError as e:
                    marker_tip.text = str(e)
                    model_marker.set_visibility(True)
                    run_btn.disable()
                    return
                model_marker.set_visibility(False)
                run_btn.enable()

            _check_model()

            seen = {"key": None, "in_flight": job_model.predict_in_flight}

            def _observe():
                run = job_model.predict_run
                key = None if run is None else (run.job_dir, run.status, run.error)
                if key == seen["key"]:
                    return
                landed = seen["in_flight"] and run is not None and run.status == JobStatus.SUCCEEDED
                seen.update(key=key, in_flight=job_model.predict_in_flight)
                status_c.clear()
                with status_c:
                    _render_predict_status(run)
                if landed:
                    ui.notify("DL predictions ready.", type="positive")
                    on_predictions()

            _observe()
            ui.timer(3.0, _observe)


def _render_predict_status(run) -> None:
    """One line for the latest prediction run: in flight, ready, or failed with the reason."""
    if run is None:
        return
    name = Path(run.job_dir).name
    if run.status in (JobStatus.QUEUED, JobStatus.RUNNING):
        ui.html(_running_spinner_html(12, CLR_ACCENT), sanitize=False)
        ui.label(f"Run {name} {run.status.value.lower()} · SLURM job {run.slurm_job_id or '…'}").style(
            f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};"
        )
    elif run.status == JobStatus.SUCCEEDED:
        ui.icon("check_circle", size="14px").style(f"color: {CLR_SUCCESS};")
        ui.label(f"Predictions from run {name} ({run.model})").style(f"{FONT} font-size: 9px; color: {CLR_SUCCESS};")
    else:
        ui.icon("error", size="14px").style(f"color: {CLR_ERROR};")
        ui.label(f"Run {name} failed: {run.error}").style(
            f"{FONT} font-size: 9px; color: {CLR_ERROR}; word-break: break-word;"
        )


# ── Generate ─────────────────────────────────────────────────────────────────


def _render_generate(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model, instance_id):
    from ui.background_task import BackgroundTask

    dedup_key = f"tilt-filter-thumbnails:{project_path}:{png_dir}"

    def _swap_to_gallery():
        gallery_c.clear()
        with gallery_c:
            _build_gallery(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model, instance_id)

    with ui.column().classes("w-full items-center justify-center gap-3 py-10"):
        ui.icon("collections", size="48px").style(f"color: {CLR_GHOST};")
        ui.label("Generate tilt thumbnails to begin inspection.").style(f"{FONT} font-size: 11px; color: {CLR_LABEL};")
        status_lbl = ui.label("").style(f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};")

        def _on_progress(task):
            if task.progress_total > 0:
                status_lbl.text = f"Converting {task.progress_current} / {task.progress_total}…"

        def _on_complete(task):
            if task.status == "succeeded":
                ui.notify(task.result_message or "Thumbnails ready", type="positive")
                _swap_to_gallery()
            else:
                status_lbl.text = f"{task.status.capitalize()}: {task.error or ''}"
                ui.notify(f"Thumbnail generation {task.status}: {task.error or ''}", type="negative")

        def _start():
            async def _run(progress_cb):
                n = await asyncio.to_thread(generate_tilt_thumbnails, ts_ctf_star, project_path, png_dir, progress_cb)
                # Resolve by explicit path: this runs in a BackgroundTask with no
                # client/tab context, where bare current_project_state() returns a blank
                # throwaway and a path-less save silently does nothing.
                if project_path:
                    st = get_state_service().state_for(project_path)
                    st.tilt_filter_png_dir = str(png_dir)
                    st.mark_dirty()
                    await get_backend().save_project(project_path, force=True)
                return f"{n} thumbnails generated"

            status_lbl.text = "Running — gallery will appear here when complete."
            BackgroundTask(
                title=f"Tilt thumbnails · {png_dir.name}",
                subtitle="Converting MRC tilts to PNG previews",
                project_path=str(project_path),
                dedup_key=dedup_key,
            ).submit(_run, on_complete=_on_complete, on_progress=_on_progress)

        existing = BackgroundTask.existing(dedup_key)
        if existing is not None:
            status_lbl.text = "Generation in flight — see tray (bottom-right)."
            house_button("Generation running…", lambda: None, kind="accent")
            BackgroundTask.attach(existing.id, on_complete=_on_complete, on_progress=_on_progress)
        else:
            house_button("Generate thumbnails", _start, kind="accent")


# ═════════════════════════════════════════════════════════════════════════════
# GALLERY
# ═════════════════════════════════════════════════════════════════════════════


def _build_gallery(ts_ctf_star, project_path, png_dir, gallery_c, stats_c, job_model, instance_id):
    try:
        ts_data = load_tilt_series(str(ts_ctf_star), str(project_path))
    except Exception as e:
        ui.label(f"Failed to load tilt series: {e}").style(f"color: {CLR_ERROR};")
        return
    _render_gallery_content(ts_data, project_path, png_dir, gallery_c, stats_c, job_model, instance_id)


def _render_liveness_banner(df) -> None:
    """Warn when the predictions do not depend on the image (see prediction_liveness)."""
    by_series: dict[str, list[float]] = {}
    for ts_name, p in zip(df["rlnTomoName"], df["_p_bad"], strict=True):
        if pd.notna(p):
            by_series.setdefault(ts_name, []).append(p)
    liveness = prediction_liveness(by_series)
    if liveness is None or liveness[0]:
        return
    with (
        ui.row()
        .classes("w-full items-center gap-2 no-wrap")
        .style(f"{SEC} background: #fef2f2; border-color: #fecaca;")
    ):
        ui.icon("warning", size="14px").style(f"color: {CLR_ERROR};")
        ui.label(f"The model gives every tilt P(bad) ≈ {liveness[1]:.2f} — its verdicts are meaningless.").style(
            f"{FONT} font-size: 10px; color: {CLR_ERROR};"
        )


def _render_gallery_content(ts_data, project_path, png_dir, gallery_c, stats_c, job_model, instance_id):
    # The tilts a human labelled. Clicks write straight into it, so a label outlives a
    # gallery rebuild, a DL re-run and the tab; every other tilt takes the prediction.
    labels = job_model.tilt_labels
    flight = SingleFlight()

    df = ts_data.all_tilts_df
    # Manual mode ignores predictions: no P(bad) on the cards, no threshold, no banner.
    dl = job_model.mode == FilterMode.DL_REVIEW
    df["_p_bad"] = df["cryoBoostKey"].map(predictions_for(project_path, df["cryoBoostKey"].unique()) if dl else {})
    has_predictions = bool(df["_p_bad"].notna().any())
    png_map: dict[str, Path] = {f.stem: f for f in sorted(png_dir.glob("*.png"))}
    df["_png"] = df["cryoBoostKey"].map(lambda k: str(png_map.get(k, "")))
    # Unique row ID for DOM identification (cryoBoostKey can have duplicates)
    df["_row_id"] = [f"r{i}" for i in range(len(df))]

    # Real per-tilt CTF-fit resolution + motion from the FS-motion warp XMLs
    # (cryoBoostKey == the XML basename == frame stem). The star's own res/motion
    # columns are 1e-6 placeholders. None where the XML is absent (no invented value).
    fs_warp_dir = _find_fs_motion_warp_dir(project_path)
    if fs_warp_dir is not None:
        from services.tilt_series.frameseries_quality import read_frame_quality

        qual = {k: read_frame_quality(fs_warp_dir, k) for k in df["cryoBoostKey"].unique()}
        df["_xmlRes"] = df["cryoBoostKey"].map(lambda k: qual[k].ctf_resolution if qual.get(k) else None)
        df["_xmlMotion"] = df["cryoBoostKey"].map(lambda k: qual[k].mean_frame_movement if qual.get(k) else None)
    else:
        df["_xmlRes"] = None
        df["_xmlMotion"] = None

    def _relabel() -> None:
        """Every tilt's effective label into cryoBoostDlLabel, which the stats, the group
        counts and the commit read."""
        df["cryoBoostDlLabel"] = [
            effective_label(k, p, labels, job_model.threshold, job_model.mode)
            for k, p in zip(df["cryoBoostKey"], df["_p_bad"], strict=True)
        ]

    _relabel()

    async def _persist() -> None:
        await get_backend().save_project(project_path, force=True, debounce_s=1.0)

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

    if has_predictions:
        _render_liveness_banner(df)

    # ── Build position → tilt-series hierarchy ──
    ts_names = sorted(df["rlnTomoName"].unique().tolist())
    hierarchy: dict[int, list] = {}
    for tn in ts_names:
        pos, beam = _parse_pos_beam(tn)
        pos_key = pos if pos is not None else 0
        hierarchy.setdefault(pos_key, []).append((tn, beam))

    # ── Actions + collapse controls ──
    group_refs: list = []

    view_opts = {"sort": "p_bad" if has_predictions else "acquisition"}
    applied = {"threshold": job_model.threshold}

    async def _approve():
        # The same commit as the job row's Approve: labels are re-derived from the job and the
        # registry, which this gallery writes through, so both commit the verdict shown here.
        async with flight("approve") as acquired:
            if not acquired:
                return
            _notify_finalize(await get_backend().approve_tilt_filter(project_path, instance_id))

    async def _set_all_good():
        for key in df["cryoBoostKey"]:
            labels[key] = "good"
        df["cryoBoostDlLabel"] = "good"
        _refresh_stats()
        # Bulk-update all visible cards via JS — no full re-render needed
        ui.run_javascript(
            "document.querySelectorAll('.tilt-card').forEach(card => {"
            + _js_apply_look("card", _card_look(False, touched=True))
            + "});"
        )
        for refs in group_refs:
            _set_bad_count(refs["bad_lbl"], 0)
        ui.notify("All tilts set to good", type="info")
        await _persist()

    async def _on_threshold(_e) -> None:
        # The field refuses out-of-range and half-typed values, which leave the threshold as it was.
        if job_model.threshold == applied["threshold"]:
            return
        applied["threshold"] = job_model.threshold
        _relabel()
        _refresh_stats()
        _render_groups()
        await _persist()

    with ui.row().classes("w-full items-center gap-2 py-1 flex-wrap"):
        house_button("Approve", _approve, kind="accent", tooltip="Commit these labels; alignment drops the bad tilts.")
        house_button("Set all good", _set_all_good)

        ui.element("div").style("width: 1px; height: 16px; background: #e2e8f0; margin: 0 2px;")
        house_button("Expand all", lambda: _expand_all(True))
        house_button("Collapse all", lambda: _expand_all(False))

        ui.element("div").style("width: 1px; height: 16px; background: #e2e8f0; margin: 0 2px;")
        with ui.column().classes("gap-0"):
            ui.label("Sort within group").style(f"{FONT} font-size: 7px; color: {CLR_SUBLABEL};")
            sort_sel = (
                ui.select(
                    {"acquisition": "Acquisition order", "angle": "Tilt angle", "p_bad": "P(bad), worst first"},
                    value=view_opts["sort"],
                )
                .props("dense borderless hide-bottom-space")
                .style(f"{FONT} font-size: 10px; color: {CLR_LABEL};")
                .classes("w-36")
            )

        if has_predictions:
            ui.element("div").style("width: 1px; height: 16px; background: #e2e8f0; margin: 0 2px;")
            house_number(
                "Threshold",
                model=job_model,
                attr="threshold",
                min=0.0,
                max=1.0,
                step=0.05,
                format="%.2f",
                width="w-20",
                hint="A tilt with P(bad) at or above this is predicted bad.",
                on_change=_on_threshold,
            )
            ui.label("uncalibrated").style(f"{FONT} font-size: 8px; color: {CLR_WARN};").tooltip(
                "Not calibrated on labelled tilts for this model: the default 0.5 is the network's own "
                "decision boundary, not a validated cut."
            )

        ui.space()
        bad_only = ui.checkbox("Show only removed").style(f"{FONT} font-size: 10px; color: {CLR_LABEL};")

    if has_predictions:
        ui.label(
            "Dashed border: predicted bad. Solid border with a filled dot: your label, which a new threshold "
            "or a DL re-run leaves alone."
        ).style(f"{FONT} font-size: 8px; color: {CLR_SUBLABEL};")

    # ── Groups ──
    group_c = ui.column().classes("w-full gap-1")
    # Track which groups are expanded by ts_name so we can preserve across re-renders
    expand_state: dict[str, bool] = {}

    def _sort_ts_df(ts_df):
        s = view_opts["sort"]
        if s == "angle" and "rlnTomoNominalStageTiltAngle" in ts_df.columns:
            return ts_df.sort_values("rlnTomoNominalStageTiltAngle").reset_index(drop=True)
        if s == "p_bad":
            return ts_df.sort_values("_p_bad", ascending=False, na_position="last").reset_index(drop=True)
        return ts_df  # acquisition order = default dataframe order

    async def _on_card_click(e, refs) -> None:
        args = e.args or {}
        key = args.get("key", "")
        if args.get("action") == "zoom":
            _show_upsample(args.get("mrc", ""), key, project_path)
            return
        rows = df["cryoBoostKey"] == key
        if args.get("action") != "toggle" or not rows.any():
            return
        new = "good" if df.loc[rows, "cryoBoostDlLabel"].iloc[0] == "bad" else "bad"
        labels[key] = new
        df.loc[rows, "cryoBoostDlLabel"] = new
        # Unique row ID for DOM targeting (cryoBoostKey can have duplicates)
        rid = args.get("rid", "")
        restyle = _js_apply_look("card", _card_look(new == "bad", touched=True))
        ui.run_javascript(f"const card = document.querySelector('[data-rid=\"{rid}\"]'); if (card) {{ {restyle} }}")
        in_group = df["rlnTomoName"] == refs["ts_name"]
        _set_bad_count(refs["bad_lbl"], int((df.loc[in_group, "cryoBoostDlLabel"] == "bad").sum()))
        _refresh_stats()
        await _persist()

    def _populate(refs) -> None:
        """A group's cards, rendered on its first expand, with one click handler for the grid."""
        with refs["body"]:
            grid = ui.html(
                _build_cards_html(refs["ts_df"], labels, job_model.threshold, job_model.mode), sanitize=False
            ).classes("w-full")
        grid.on("click", handler=lambda e: _on_card_click(e, refs), js_handler=_CARD_CLICK_JS)

    rendering = {"active": False}

    def _render_groups():
        # Save current expand states before clearing
        for refs in group_refs:
            expand_state[refs["ts_name"]] = refs["expanded"]["v"]

        group_c.clear()
        group_refs.clear()
        show_bad = bad_only.value

        with group_c:
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
                    start_expanded = expand_state.get(ts_name, False)
                    refs = _render_ts_group(ts_name, pos_key, beam, ts_df, _populate, start_expanded)
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
            # Deferred render on expand-all
            if expand and not refs["rendered"]["v"]:
                refs["rendered"]["v"] = True
                _populate(refs)


# ── Card grid HTML builder ──────────────────────────────────────────────────

# One delegated click handler per grid: the zoom button opens the upsample view, anywhere
# else on a card toggles its label.
_CARD_CLICK_JS = """(event) => {
    const zoom = event.target.closest('.tilt-zoom');
    if (zoom) {
        event.stopPropagation();
        const card = zoom.closest('.tilt-card');
        if (card) emit({action: 'zoom', key: card.dataset.key, mrc: card.dataset.mrc});
        return;
    }
    const card = event.target.closest('.tilt-card');
    if (card) emit({action: 'toggle', key: card.dataset.key, rid: card.dataset.rid});
}"""


def _card_look(is_bad: bool, touched: bool) -> dict[str, str]:
    """Card chrome for one tilt. Red is bad, grey good; a solid border with a filled dot is a
    human's label, a dashed border with a ring dot the model's prediction. An untouched good
    tilt carries no dot."""
    edge = BAD_EDGE if is_bad else GOOD_EDGE
    info_bg = "#fef2f2" if is_bad else "#fafafa"
    if touched:
        dot_bg = BAD_EDGE if is_bad else GOOD_DOT
        return {"border": f"1.5px solid {edge}", "dot_bg": dot_bg, "dot_border": "1px solid white", "info_bg": info_bg}
    if is_bad:
        return {
            "border": f"1.5px dashed {edge}",
            "dot_bg": "transparent",
            "dot_border": f"1.5px solid {edge}",
            "info_bg": info_bg,
        }
    return {"border": f"1.5px solid {edge}", "dot_bg": "transparent", "dot_border": "none", "info_bg": info_bg}


def _js_apply_look(card: str, look: dict[str, str]) -> str:
    """JS statements restyling the card element held in the JS variable `card`."""
    return (
        f"{card}.style.border = '{look['border']}';"
        f"const dot = {card}.querySelector('.tilt-dot');"
        f"if (dot) {{ dot.style.background = '{look['dot_bg']}'; dot.style.border = '{look['dot_border']}'; }}"
        f"const info = {card}.querySelector('.tilt-info');"
        f"if (info) info.style.background = '{look['info_bg']}';"
    )


def _set_bad_count(bad_lbl, n_bad: int) -> None:
    """A group header's bad-tilt count; hidden at zero. Written in full each time because
    `.style()` merges, so a display left out would keep an earlier `display: none`."""
    bad_lbl.text = f"−{n_bad}"
    bad_lbl.style(
        f"{MONO} font-size: 9px; font-weight: 600; color: {CLR_ERROR}; display: {'none' if n_bad == 0 else 'block'};"
    )


def _build_cards_html(ts_df, labels, threshold, mode) -> str:
    """Build the entire card grid for one tilt-series group as a single HTML string. Labels
    come from `labels` and the threshold as they are now, not from `ts_df`, which is a copy
    taken when the group was laid out."""
    cards = []
    for _, row in ts_df.iterrows():
        key = row["cryoBoostKey"]
        rid = row["_row_id"]
        png_path = row.get("_png", "")
        p_bad = row["_p_bad"]
        label = effective_label(key, p_bad, labels, threshold, mode)
        look = _card_look(label == "bad", touched=key in labels)
        angle = row.get("rlnTomoNominalStageTiltAngle", None)
        defocus_u = row.get("rlnDefocusU", None)
        # Real CTF-fit resolution + motion from the WarpTools XML (see
        # _render_gallery_content); the star's rlnAccumMotionTotal is a 1e-6
        # placeholder.
        ctf_res = row.get("_xmlRes", None)
        motion = row.get("_xmlMotion", None)
        mrc_path = row.get("rlnMicrographName", "")

        if png_path:
            encoded_path = urllib.parse.quote(str(png_path), safe="/")
            img_html = (
                f'<img src="/api/tilt-thumb?path={encoded_path}" loading="lazy" '
                'style="width:100%;aspect-ratio:1;object-fit:cover;display:block;">'
            )
        else:
            img_html = (
                '<div style="width:100%;aspect-ratio:1;display:flex;align-items:center;'
                f'justify-content:center;color:{CLR_GHOST};font-size:28px;">&#x1f5bc;</div>'
            )

        info_parts = []
        if angle is not None:
            info_parts.append(f'<span style="font-weight:600;color:{CLR_HEADING};">{angle:.0f}°</span>')
        if pd.notna(p_bad):
            p_color = CLR_ERROR if p_bad >= threshold else CLR_SUBLABEL
            info_parts.append(
                f'<span style="color:{p_color};" title="P(bad) from the latest DL run">p{p_bad:.2f}</span>'
            )
        if defocus_u is not None and defocus_u > 0:
            info_parts.append(f'<span style="color:{CLR_SUBLABEL};">{defocus_u / 10000:.1f}µ</span>')
        if isinstance(ctf_res, (int, float)) and ctf_res > 0:
            info_parts.append(
                f'<span style="color:{CLR_SUBLABEL};" title="CTF fit resolution (Å)">{ctf_res:.1f}Å</span>'
            )
        if isinstance(motion, (int, float)) and motion > 0:
            info_parts.append(
                f'<span style="color:{CLR_SUBLABEL};" title="beam-induced motion (WarpTools)">{motion:.2f}</span>'
            )

        escaped_mrc = mrc_path.replace("&", "&amp;").replace('"', "&quot;")
        escaped_key = key.replace("&", "&amp;").replace('"', "&quot;")

        cards.append(
            f'<div class="tilt-card" data-rid="{rid}" data-key="{escaped_key}" data-mrc="{escaped_mrc}" '
            f'style="border:{look["border"]};border-radius:4px;overflow:hidden;'
            'position:relative;cursor:pointer;transition:border-color 0.12s;">'
            f"{img_html}"
            f'<div class="tilt-dot" style="position:absolute;top:3px;right:3px;width:8px;height:8px;'
            f'border-radius:50%;background:{look["dot_bg"]};border:{look["dot_border"]};pointer-events:none;"></div>'
            '<button class="tilt-zoom" style="position:absolute;top:2px;left:2px;color:white;'
            "background:rgba(0,0,0,0.3);width:18px;height:18px;border:none;border-radius:50%;"
            'cursor:pointer;font-size:12px;display:flex;align-items:center;justify-content:center;" '
            'title="Upsample &amp; zoom">&#x1F50D;</button>'
            f'<div class="tilt-info" style="display:flex;gap:3px;padding:2px 4px;align-items:center;'
            f"background:{look['info_bg']};font-family:'IBM Plex Mono',monospace;font-size:8px;\">"
            f"{''.join(info_parts)}</div>"
            "</div>"
        )

    return (
        '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:4px;width:100%;">'
        + "".join(cards)
        + "</div>"
    )


# ── Tilt-series group ────────────────────────────────────────────────────────


def _render_ts_group(ts_name, pos, beam, ts_df, populate, start_expanded=False):
    n_total = len(ts_df)
    n_bad = int((ts_df["cryoBoostDlLabel"] == "bad").sum())
    expanded = {"v": start_expanded}
    rendered = {"v": False}

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
            pos_txt = f"Pos {pos}" if pos else ts_name
            ui.label(pos_txt).style(f"{MONO} font-size: 10px; font-weight: 600; color: {CLR_HEADING};")
            if beam and beam > 1:
                ui.label(f"beam {beam}").style(
                    f"{FONT} font-size: 8px; color: {CLR_SUBLABEL}; "
                    f"background: {CLR_BORDER}; padding: 0 4px; border-radius: 3px;"
                )
            ui.label(ts_name).style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};")

            ui.space()
            ui.label(f"{n_total}").style(f"{MONO} font-size: 9px; color: {CLR_LABEL};")
            bad_lbl = ui.label("")
            _set_bad_count(bad_lbl, n_bad)

        refs = {
            "body": body,
            "chevron": chevron,
            "expanded": expanded,
            "ts_name": ts_name,
            "rendered": rendered,
            "bad_lbl": bad_lbl,
            "ts_df": ts_df,
        }

        # Render cards immediately only if starting expanded
        if start_expanded:
            rendered["v"] = True
            populate(refs)

        def _toggle():
            expanded["v"] = not expanded["v"]
            body.style(f"display: {'flex' if expanded['v'] else 'none'};")
            chevron.style(
                f"color: {CLR_SUBLABEL}; transition: transform 0.15s; "
                f"transform: rotate({'0' if expanded['v'] else '-90'}deg);"
            )
            # Deferred render on first expand
            if expanded["v"] and not rendered["v"]:
                rendered["v"] = True
                populate(refs)

        hdr.on("click", _toggle)

    return refs


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
