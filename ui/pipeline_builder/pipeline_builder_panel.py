"""
Pipeline builder panel.

Populates three externally-created shells:
  primary_sidebar  48px dark strip
  roster_panel     186px light panel, hidden until a phase button is clicked
  current context  tab strip + lazy job tab content (inside main_area)

Roster interaction:
  click unselected job → add to pipeline, auto-focus tab
  click selected job   → remove from pipeline   (FIX: was broken)
Tab strip: navigate between selected jobs without modifying selection.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable, List, Set

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobType, get_project_state, get_state_service
from ui.status_indicator import BoundStatusDot
from ui.ui_state import get_ui_state_manager, get_job_display_name
from ui.pipeline_builder.job_tab_component import render_job_tab


# ── Phase / dependency constants ──────────────────────────────────────────────

PHASE_PREPROCESSING = "preprocessing"
PHASE_PARTICLES = "particles"

PHASE_JOBS: Dict[str, List[JobType]] = {
    PHASE_PREPROCESSING: [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
        JobType.TS_CTF,
        JobType.TS_RECONSTRUCT,
        JobType.DENOISE_TRAIN,
        JobType.DENOISE_PREDICT,
    ],
    PHASE_PARTICLES: [
        JobType.TEMPLATE_MATCH_PYTOM,
        JobType.TEMPLATE_EXTRACT_PYTOM,
        JobType.SUBTOMO_EXTRACTION,
        JobType.RECONSTRUCT_PARTICLE,
        JobType.CLASS3D,
    ],
}

PHASE_META: Dict[str, tuple] = {
    PHASE_PREPROCESSING: ("looks_one", "Preprocessing", "Import → Denoise"),
    PHASE_PARTICLES: ("looks_two", "Particles", "Template Match → Class3D"),
}

ROSTER_ANCHOR: Dict[str, str] = {
    PHASE_PREPROCESSING: "roster-anchor-preprocessing",
    PHASE_PARTICLES: "roster-anchor-particles",
}

JOB_DEPENDENCIES: Dict[JobType, List[JobType]] = {
    JobType.IMPORT_MOVIES: [],
    JobType.FS_MOTION_CTF: [JobType.IMPORT_MOVIES],
    JobType.TS_ALIGNMENT: [JobType.FS_MOTION_CTF],
    JobType.TS_CTF: [JobType.TS_ALIGNMENT],
    JobType.TS_RECONSTRUCT: [JobType.TS_CTF],
    JobType.DENOISE_TRAIN: [JobType.TS_RECONSTRUCT],
    JobType.DENOISE_PREDICT: [JobType.DENOISE_TRAIN, JobType.TS_RECONSTRUCT],
    JobType.TEMPLATE_MATCH_PYTOM: [JobType.TS_CTF],
    JobType.TEMPLATE_EXTRACT_PYTOM: [JobType.TEMPLATE_MATCH_PYTOM],
    JobType.SUBTOMO_EXTRACTION: [JobType.TEMPLATE_EXTRACT_PYTOM],
    JobType.RECONSTRUCT_PARTICLE: [JobType.SUBTOMO_EXTRACTION],
    JobType.CLASS3D: [JobType.RECONSTRUCT_PARTICLE],
}

# Sidebar palette
_SB_BG   = "#f8fafc"
_SB_SEP  = "#e2e8f0"
_SB_MUTE = "#94a3b8"   # inactive icons / labels
_SB_ACT  = "#3b82f6"   # active accent
_SB_ABG  = "#eff6ff"   # active button background



def _missing_deps(job_type: JobType, selected: Set[JobType]) -> List[JobType]:
    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        return [] if {JobType.TS_RECONSTRUCT, JobType.DENOISE_PREDICT} & selected else [JobType.TS_RECONSTRUCT]
    return [d for d in JOB_DEPENDENCIES.get(job_type, []) if d not in selected]


def _fmt(v) -> str:
    if v is None:
        return "---"
    return f"{v:.4g}" if isinstance(v, float) else str(v)


# ── Main function ─────────────────────────────────────────────────────────────


def build_pipeline_builder_panel(
    backend: CryoBoostBackend, callbacks: Dict[str, Callable], primary_sidebar=None, roster_panel=None
) -> None:
    ui_mgr = get_ui_state_manager()
    state_service = get_state_service()

    _job_content_containers: Dict[str, object] = {}
    _tab_strip_ref: Dict[str, object] = {}
    _content_wrapper_ref: Dict[str, object] = {}
    _refs: Dict[str, object] = {}

    # Add near the other _refs/_roster_state dicts at the top of build_pipeline_builder_panel:
    _flash_state = {"phase": None}
    _roster_state = {"visible": False, "phase": None}
    _spinner_frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    _spinner_idx = [0]

    # ── Roster ────────────────────────────────────────────────────────────────

    def _refresh_roster():
        if roster_panel is None:
            return
        roster_panel.clear()
        flashing_phase = _flash_state.get("phase")

        with roster_panel:
            selected = set(ui_mgr.selected_jobs)

            for phase_id, jobs in PHASE_JOBS.items():
                icon_name, phase_label, _ = PHASE_META[phase_id]
                is_flashing = flashing_phase == phase_id

                with (
                    ui.element("div")
                    .props(f'id="{ROSTER_ANCHOR[phase_id]}"')
                    .style(
                        "display: flex; align-items: center; gap: 5px; "
                        "padding: 8px 10px 5px 12px; "
                        "background: #f1f5f9; border-bottom: 1px solid #e5e7eb; "
                        "position: sticky; top: 0; z-index: 2;"
                    )
                ):
                    ui.icon(icon_name, size="12px").style("color: #94a3b8; flex-shrink: 0;")
                    ui.label(phase_label.upper()).style(
                        "font-size: 9px; font-weight: 700; color: #94a3b8; "
                        "letter-spacing: 0.07em; line-height: 1;"
                    )

                for job_type in jobs:
                    is_sel    = job_type in selected
                    is_active = is_sel and (ui_mgr.active_job == job_type)
                    missing   = _missing_deps(job_type, selected) if is_sel else []

                    if is_active:
                        row_bg, l_border = "#eff6ff", "#3b82f6"
                        name_color, name_wt = "#1e40af", "600"
                    elif is_sel:
                        row_bg, l_border = "#f8fafc", "#cbd5e1"
                        name_color, name_wt = "#374151", "500"
                    elif is_flashing:
                        # Soft two-second backlight for jobs in the clicked phase
                        row_bg, l_border = "#fefce8", "#fde68a"
                        name_color, name_wt = "#78716c", "400"
                    else:
                        row_bg, l_border = "transparent", "transparent"
                        name_color, name_wt = "#9ca3af", "400"

                    with (
                        ui.element("div")
                        .style(
                            f"display: flex; align-items: center; gap: 6px; "
                            f"padding: 5px 8px 5px 10px; cursor: pointer; "
                            f"background: {row_bg}; border-left: 2px solid {l_border}; "
                            f"border-bottom: 1px solid #f3f4f6;"
                        )
                        .on("click", lambda j=job_type: _on_row_click(j))
                    ):
                        ui.icon(
                            "check_box" if is_sel else "check_box_outline_blank",
                            size="13px",
                        ).style(f"color: {'#3b82f6' if is_sel else '#d1d5db'}; flex-shrink: 0;")

                        ui.label(get_job_display_name(job_type)).style(
                            f"font-size: 11px; font-weight: {name_wt}; color: {name_color}; "
                            f"flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                        )

                        if is_sel:
                            if missing:
                                (
                                    ui.icon("warning", size="11px")
                                    .style("color: #f59e0b; flex-shrink: 0;")
                                    .tooltip(
                                        "Missing: "
                                        + ", ".join(get_job_display_name(d) for d in missing)
                                    )
                                )
                            else:
                                with ui.element("span").style(
                                    "flex-shrink: 0; overflow: visible; line-height: 0;"
                                ):
                                    BoundStatusDot(job_type)

    def _on_row_click(job_type: JobType):
        """Toggle: add if unselected, remove if selected. Tab strip handles navigation."""
        if ui_mgr.is_running:
            return
        if ui_mgr.is_job_selected(job_type):
            remove_job_from_pipeline(job_type)
        else:
            selected = set(ui_mgr.selected_jobs)
            missing = _missing_deps(job_type, selected)
            if missing:
                ui.notify(
                    f"{get_job_display_name(job_type)} typically requires: "
                    + ", ".join(get_job_display_name(d) for d in missing),
                    type="warning",
                    timeout=3000,
                )
            add_job_to_pipeline(job_type)

    def _scroll_to_phase(phase_id: str):
        anchor = ROSTER_ANCHOR[phase_id]
        # requestAnimationFrame ensures visibility has been applied before scrolling.
        ui.run_javascript(
            f"requestAnimationFrame(function(){{"
            f"  var e=document.getElementById('{anchor}');"
            f"  if(e)e.scrollIntoView({{behavior:'smooth',block:'start'}});"
            f"}});"
        )

    def _toggle_roster(phase_id: str):
        same_and_open = _roster_state["visible"] and _roster_state["phase"] == phase_id
        if same_and_open:
            _roster_state["visible"] = False
            _roster_state["phase"]   = None
            _flash_state["phase"]    = None
            if roster_panel is not None:
                roster_panel.style("display: none;")
        else:
            _roster_state["visible"] = True
            _roster_state["phase"]   = phase_id
            _flash_state["phase"]    = phase_id
            if roster_panel is not None:
                roster_panel.style("display: flex;")
            _refresh_roster()
            _scroll_to_phase(phase_id)

            def _clear_flash():
                _flash_state["phase"] = None
                _refresh_roster()

            ui.timer(2.0, _clear_flash, once=True)

        _update_phase_btn_styles()
        _refresh_roster()

    def _update_phase_btn_styles():
        for phase_id in PHASE_JOBS:
            btn = _refs.get(f"phase_btn_{phase_id}")
            if btn is None:
                continue
            active = _roster_state["visible"] and _roster_state["phase"] == phase_id
            btn.style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {_SB_ABG if active else 'transparent'}; "
                f"color: {(_SB_ACT if active else _SB_MUTE)}; min-width: 0;"
            )

    # ── Sidebar ───────────────────────────────────────────────────────────────

    def _sb_sep():
        ui.element("div").style(
            f"height: 1px; background: {_SB_SEP}; width: 24px; margin: 3px auto;"
        )


    def _sb_icon_btn(icon_name: str, on_click=None, tooltip_text: str = "") -> object:
        btn = (
            ui.button(icon=icon_name, on_click=on_click)
            .props("flat dense")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"color: {_SB_MUTE}; background: transparent; min-width: 0;"
            )
        )
        if tooltip_text:
            btn.tooltip(tooltip_text)
        return btn


    def _info_popup_btn(icon_name: str, title: str, rows: list):
        """Light-themed info popup card to the right of the sidebar."""
        btn = (
            ui.button(icon=icon_name)
            .props("flat dense")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"color: {_SB_MUTE}; background: transparent; min-width: 0;"
            )
        )
        with btn:
            with ui.menu().props(
                'anchor="center right" self="center left" :offset="[8,0]"'
            ).style(
                "background: #ffffff; border: 1px solid #e2e8f0; "
                "border-radius: 5px; overflow: hidden; min-width: 210px; "
                "padding: 0; box-shadow: 0 4px 12px rgba(0,0,0,0.08);"
            ):
                with ui.element("div").style(
                    "padding: 7px 11px 5px; font-size: 9px; font-weight: 700; "
                    "color: #94a3b8; letter-spacing: 0.09em; text-transform: uppercase; "
                    "border-bottom: 1px solid #f1f5f9;"
                ):
                    ui.label(title)
                for row_lbl, row_val in rows:
                    with ui.element("div").style(
                        "display: flex; justify-content: space-between; align-items: baseline; "
                        "padding: 5px 11px; border-bottom: 1px solid #f8fafc; gap: 10px;"
                    ):
                        ui.label(row_lbl).style(
                            "font-size: 10px; color: #94a3b8; "
                            "flex-shrink: 0; white-space: nowrap;"
                        )
                        ui.label(str(row_val)).style(
                            "font-size: 10px; font-family: 'IBM Plex Mono', monospace; "
                            "color: #1e40af; text-align: right; word-break: break-all;"
                        )
                ui.element("div").style("height: 4px;")
        return btn

    def _build_sidebar():
        if primary_sidebar is None:
            return
        state = get_project_state()

        with primary_sidebar:
            ui.element("div").style("height: 8px;")
            ui.icon("biotech", size="14px").style(f"color: {_SB_MUTE}; margin: 0 auto 4px;")

            _sb_sep()

            # Info popovers
            _info_popup_btn(
                icon_name="folder_open",
                title="Project",
                rows=[
                    ("Name",   state.project_name),
                    ("Root",   str(state.project_path) if state.project_path else "---"),
                    ("Movies", state.movies_glob or "---"),
                    ("MDOC",   state.mdocs_glob  or "---"),
                ],
            )
            _info_popup_btn(
                icon_name="science",
                title="Acquisition",
                rows=[
                    ("Pixel",    f"{_fmt(state.microscope.pixel_size_angstrom)} Å"),
                    ("Voltage",  f"{_fmt(state.microscope.acceleration_voltage_kv)} kV"),
                    ("Cs",       f"{_fmt(state.microscope.spherical_aberration_mm)} mm"),
                    ("Amp. C.",  _fmt(state.microscope.amplitude_contrast)),
                    ("Dose",     f"{_fmt(state.acquisition.dose_per_tilt)} e⁻/Å²"),
                    ("Tilt ax.", f"{_fmt(state.acquisition.tilt_axis_degrees)} °"),
                ],
            )

            _sb_sep()

            # Run/stop slot — lives here now
            run_slot = ui.element("div").style(
                "width: 100%; display: flex; flex-direction: column; "
                "align-items: center; padding: 2px 6px 2px 6px; gap: 3px;"
            )
            _refs["run_slot"] = run_slot

            # Close project
            close_btn = (
                ui.button(icon="close", on_click=lambda: ui.navigate.to("/"))
                .props("flat dense")
                .style(
                    f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                    f"color: {_SB_MUTE}; background: transparent; min-width: 0;"
                )
            )
            close_btn.tooltip("Close project")

            _sb_sep()

            # Phase buttons
            for phase_id in PHASE_JOBS:
                icon_name, label, sub = PHASE_META[phase_id]
                btn = (
                    ui.button(icon=icon_name, on_click=lambda p=phase_id: _toggle_roster(p))
                    .props("flat dense")
                    .style(
                        f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                        f"background: transparent; color: {_SB_MUTE}; min-width: 0;"
                    )
                )
                btn.tooltip(f"{label} — {sub}")
                _refs[f"phase_btn_{phase_id}"] = btn

            # Spacer
            ui.element("div").style("flex: 1;")

        _rebuild_run_slot()

    def _rebuild_run_slot():
        run_slot = _refs.get("run_slot")
        if run_slot is None:
            return
        run_slot.clear()
        with run_slot:
            if ui_mgr.is_running:
                with (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #fef2f2; border: 1px solid #fecaca; "
                        "display: flex; align-items: center; justify-content: center; "
                        "flex-shrink: 0;"
                    )
                    .on("click", handle_stop_pipeline)
                    .tooltip("Stop pipeline")
                ):
                    ui.icon("stop", size="16px").style("color: #b91c1c; pointer-events: none;")

                spinner = ui.label("⠋").style(
                    "font-family: 'IBM Plex Mono', monospace; font-size: 18px; "
                    "color: #3b82f6; text-align: center; line-height: 1; "
                    "display: block; width: 100%; margin-top: 2px;"
                )
                _refs["spinner"] = spinner

                status_lbl = ui.label("").style(
                    f"font-size: 8px; color: {_SB_MUTE}; font-family: 'IBM Plex Mono', monospace; "
                    "text-align: center; line-height: 1.4; word-break: break-all; "
                    "display: block; width: 100%; padding: 0 3px;"
                )
                _refs["status_label"] = status_lbl

            else:
                with (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #f0fdf4; border: 1px solid #bbf7d0; "
                        "display: flex; align-items: center; justify-content: center; "
                        "flex-shrink: 0;"
                    )
                    .on("click", handle_run_pipeline)
                    .tooltip("Run pipeline")
                ):
                    ui.icon("play_arrow", size="16px").style("color: #15803d; pointer-events: none;")

    # ── Spinner ───────────────────────────────────────────────────────────────

    def _advance_spinner():
        el = _refs.get("spinner")
        if el is None:
            return
        _spinner_idx[0] = (_spinner_idx[0] + 1) % len(_spinner_frames)
        el.set_text(_spinner_frames[_spinner_idx[0]])

    def _start_spinner_timer():
        if _refs.get("spinner_timer"):
            return
        _refs["spinner_timer"] = ui.timer(0.17, _advance_spinner)

    def _stop_spinner_timer():
        t = _refs.pop("spinner_timer", None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

    def _update_status_label(overview: Dict):
        el = _refs.get("status_label")
        if el is None:
            return
        done = overview.get("completed", 0) + overview.get("failed", 0)
        total = max(overview.get("total", 0), len(ui_mgr.selected_jobs))
        el.set_text(f"{done}/{total}")

    # ── Tab strip ─────────────────────────────────────────────────────────────

    def _refresh_tab_strip():
        strip = _tab_strip_ref.get("el")
        if strip is None:
            return
        strip.clear()
        with strip:
            for job_type in ui_mgr.selected_jobs:
                is_active = ui_mgr.active_job == job_type
                with (
                    ui.button(on_click=lambda j=job_type: switch_tab(j))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 6px 14px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#9ca3af'}; "
                        f"border-top: 2px solid {'#3b82f6' if is_active else 'transparent'}; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-size: 11px; font-weight: {'500' if is_active else '400'};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(get_job_display_name(job_type))
                        BoundStatusDot(job_type)

    # ── Lazy tab content ──────────────────────────────────────────────────────

    def _ensure_job_rendered(job_type: JobType):
        if job_type.value in _job_content_containers:
            return
        wrapper = _content_wrapper_ref.get("el")
        if wrapper is None:
            return
        with wrapper:
            container = ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;")
            container.set_visibility(False)
            _job_content_containers[job_type.value] = container
            with container:
                render_job_tab(
                    job_type=job_type,
                    backend=backend,
                    ui_mgr=ui_mgr,
                    callbacks={
                        **callbacks,
                        "check_and_update_statuses": check_and_update_statuses,
                        "rebuild_pipeline_ui": rebuild_pipeline_ui,
                        "remove_job_from_pipeline": remove_job_from_pipeline,
                    },
                )

    def switch_tab(job_type: JobType):
        ui_mgr.set_active_job(job_type)
        _ensure_job_rendered(job_type)
        for jt_str, c in _job_content_containers.items():
            c.set_visibility(jt_str == job_type.value)
        _refresh_tab_strip()
        _refresh_roster()

    # ── Job management ────────────────────────────────────────────────────────

    def add_job_to_pipeline(job_type: JobType):
        if not ui_mgr.add_job(job_type):
            return
        state = state_service.state
        if job_type not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            star = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(job_type, star if star.exists() else None)
        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())
        ui_mgr.set_active_job(job_type)
        rebuild_pipeline_ui()

    def _cleanup_stale_overrides(removed: JobType):
        prefix = removed.value + ":"
        for _, job_model in state_service.state.jobs.items():
            overrides = getattr(job_model, "source_overrides", None)
            if not overrides:
                continue
            for k in [k for k, v in overrides.items() if v.startswith(prefix)]:
                del overrides[k]

    def remove_job_from_pipeline(job_type: JobType):
        if not ui_mgr.remove_job(job_type):
            return
        _cleanup_stale_overrides(job_type)
        _job_content_containers.pop(job_type.value, None)
        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())
        rebuild_pipeline_ui()

    # ── Full rebuild ──────────────────────────────────────────────────────────

    def rebuild_pipeline_ui():
        _job_content_containers.clear()
        _tab_strip_ref.pop("el", None)
        _content_wrapper_ref.pop("el", None)

        _rebuild_run_slot()
        _refresh_roster()

        tabs_container = ui_mgr.panel_refs.job_tabs_container
        if tabs_container is None:
            return
        tabs_container.clear()

        if not ui_mgr.is_project_created:
            with tabs_container:
                with ui.column().classes("w-full h-full items-center justify-center gap-3"):
                    ui.icon("create_new_folder", size="44px").classes("text-gray-300")
                    ui.label("Create a project to begin.").classes("text-sm text-gray-400")
            return

        selected = ui_mgr.selected_jobs
        if not selected:
            with tabs_container:
                ui.label("Select jobs from the left panel.").classes(
                    "text-xs text-gray-400 italic p-8"
                )
            return

        if ui_mgr.active_job not in selected:
            ui_mgr.set_active_job(selected[0])

        with tabs_container:
            strip = ui.element("div").style(
                "display: flex; flex-direction: row; width: 100%; flex-shrink: 0; "
                "border-bottom: 1px solid #e5e7eb; overflow-x: auto; gap: 0;"
            )
            _tab_strip_ref["el"] = strip
            _refresh_tab_strip()

            wrapper = ui.element("div").style(
                "display: flex; flex-direction: column; width: 100%; "
                "flex: 1 1 0%; min-height: 0; overflow: hidden;"
            )
            _content_wrapper_ref["el"] = wrapper

        active = ui_mgr.active_job
        if active:
            _ensure_job_rendered(active)
            _job_content_containers[active.value].set_visibility(True)

        if ui_mgr.is_running:
            _start_spinner_timer()
            # Always cancel and recreate — old timer may be from a dead client connection.
            ui_mgr.status_timer = ui.timer(3.0, safe_status_check)

    # ── Status polling ────────────────────────────────────────────────────────

    async def check_and_update_statuses():
        project_path = ui_mgr.project_path
        if not project_path:
            return

        sbatch_errors = backend.pipeline_runner.get_sbatch_errors()
        if sbatch_errors:
            await backend.pipeline_runner.stop_pipeline()
            await backend.pipeline_runner.reset_submission_failure(ui_mgr.project_path)
            await backend.pipeline_runner.status_sync.sync_all_jobs(str(ui_mgr.project_path))
            ui_mgr.set_pipeline_running(False)
            stop_all_timers()
            rebuild_pipeline_ui()
            ui.notify(f"SLURM submission failed: {sbatch_errors[0]}", type="negative", timeout=10000)
            return

        await backend.pipeline_runner.status_sync.sync_all_jobs(str(project_path))
        if not ui_mgr.is_running:
            return

        overview = await backend.get_pipeline_overview(str(project_path))
        _update_status_label(overview)

        all_done = (
            overview.get("total", 0) > 0
            and overview.get("running", 0) == 0
            and overview.get("scheduled", 0) == 0
            and (overview.get("completed", 0) > 0 or overview.get("failed", 0) > 0)
            and backend.pipeline_runner.active_schemer_process is None
        )
        if not all_done:
            return

        ui_mgr.set_pipeline_running(False)
        stop_all_timers()
        try:
            if overview.get("failed", 0) > 0:
                ui.notify(f"Pipeline finished with {overview['failed']} failed job(s).", type="warning")
            else:
                ui.notify("Pipeline execution finished.", type="positive")
        except RuntimeError:
            pass
        rebuild_pipeline_ui()

    def stop_all_timers():
        ui_mgr.cleanup_all_timers()
        _stop_spinner_timer()

    async def safe_status_check():
        try:
            await check_and_update_statuses()
        except Exception as e:
            print(f"[UI] Status check failed: {e}")

    # ── Run / Stop ────────────────────────────────────────────────────────────

    async def handle_run_pipeline():
        if not ui_mgr.is_project_created:
            ui.notify("Create a project first", type="warning")
            return

        await state_service.save_project(force=True)
        run_btn = ui_mgr.panel_refs.run_button
        if run_btn:
            run_btn.props("loading")

        try:
            result = await backend.start_pipeline(
                project_path=str(ui_mgr.project_path),
                scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
                selected_jobs=[j.value for j in ui_mgr.selected_jobs],
                required_paths=[],
            )
            if result.get("already_complete"):
                ui.notify("All selected jobs already completed.", type="info")
                return
            if result.get("success"):
                ui_mgr.set_pipeline_running(True)
                ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                ui_mgr.status_timer = ui.timer(3.0, safe_status_check)
                rebuild_pipeline_ui()
            else:
                ui.notify(f"Failed to start: {result.get('error')}", type="negative")
        except Exception as e:
            ui.notify(f"Error: {e}", type="negative")
        finally:
            rb = ui_mgr.panel_refs.run_button
            if rb:
                rb.props(remove="loading")

    async def handle_stop_pipeline():
        slurm_result = await backend.slurm_service.get_user_slurm_jobs(force_refresh=True)
        running_slurm = [j for j in slurm_result.get("jobs", []) if j["state"] in ("RUNNING", "PENDING")]

        with ui.dialog() as dialog, ui.card().style("min-width: 360px; padding: 16px;"):
            ui.label("Stop Pipeline?").classes("text-base font-bold text-gray-800")
            if running_slurm:
                ui.label(f"{len(running_slurm)} SLURM job(s) will be cancelled:").classes("text-sm text-gray-600 mt-2")
                for j in running_slurm:
                    ui.label(f"[{j['job_id']}]  {j['name']}  ({j['state']})").classes(
                        "text-xs font-mono text-gray-500 ml-2"
                    )
            else:
                ui.label("No active SLURM jobs found.").classes("text-sm text-gray-500 mt-2")
            ui.label("Running and queued jobs will be marked Failed.").classes("text-xs text-amber-600 mt-3")
            with ui.row().classes("mt-4 gap-2 justify-end w-full"):
                ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
                ui.button("Stop Pipeline", on_click=lambda: dialog.submit(True)).props("dense no-caps").style(
                    "background: #ef4444; color: white; padding: 4px 16px; border-radius: 3px;"
                )

        confirmed = await dialog
        if not confirmed:
            return

        stop_all_timers()
        ui_mgr.set_pipeline_running(False)
        slurm_ids = [j["job_id"] for j in running_slurm]
        result = await backend.pipeline_runner.stop_and_cleanup(ui_mgr.project_path, slurm_ids)
        await backend.pipeline_runner.status_sync.sync_all_jobs(str(ui_mgr.project_path))
        rebuild_pipeline_ui()

        if result.get("success"):
            ui.notify("Pipeline stopped.", type="warning", timeout=4000)
        else:
            ui.notify(f"Stopped (with warnings: {'; '.join(result.get('errors', []))})", type="warning", timeout=6000)

    # ── Tab content area (inside main_area from workspace_page) ───────────────

    tabs_container = ui.element("div").style(
        "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; min-height: 0; overflow: hidden;"
    )
    ui_mgr.panel_refs.job_tabs_container = tabs_container

    # ── Init ──────────────────────────────────────────────────────────────────
    ui_mgr.cleanup_all_timers()
    _build_sidebar()
    _refresh_roster()

    ui_mgr.set_rebuild_callback(rebuild_pipeline_ui)
    callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
    callbacks["stop_all_timers"] = stop_all_timers
    callbacks["check_and_update_statuses"] = check_and_update_statuses
    callbacks["enable_run_button"] = _rebuild_run_slot
    callbacks["add_job_to_pipeline"] = add_job_to_pipeline
    callbacks["remove_job_from_pipeline"] = remove_job_from_pipeline

    rebuild_pipeline_ui()

    if ui_mgr.is_running:
        ui.timer(0.2, safe_status_check, once=True)
