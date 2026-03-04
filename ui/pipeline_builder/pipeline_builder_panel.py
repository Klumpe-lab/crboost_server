"""
Pipeline builder panel.

- "Create Project First" workflow
- Lazy rendering of job tab content (rendered once, cached)
- Tab switching via visibility toggle (preserves bindings, scroll, focus)
- Full rebuild only on structural changes (add/remove job)
- Pills hidden + run button disabled while pipeline is active
- CLI-style spinner + live status text while pipeline runs
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Callable, List, Set

from nicegui import ui

from backend import CryoBoostBackend
from services.project_state import JobType, get_state_service
from ui.status_indicator import BoundStatusDot
from ui.ui_state import PIPELINE_ORDER, get_ui_state_manager, get_job_display_name
from typing import Any, Dict, Callable, List, Set
from ui.pipeline_builder.job_tab_component import render_job_tab


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


def get_missing_dependencies(job_type: JobType, selected_jobs: Set[JobType]) -> List[JobType]:
    deps = JOB_DEPENDENCIES.get(job_type, [])
    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        tomogram_sources = {JobType.TS_RECONSTRUCT, JobType.DENOISE_PREDICT}
        if not (tomogram_sources & selected_jobs):
            return [JobType.TS_RECONSTRUCT]
        return []
    return [d for d in deps if d not in selected_jobs]


def render_job_flow(selected_jobs: List[JobType], on_toggle: Callable[[JobType], None]):
    selected_set = set(selected_jobs)

    for i, job_type in enumerate(PIPELINE_ORDER):
        is_selected = job_type in selected_set
        name = get_job_display_name(job_type)

        missing_deps = get_missing_dependencies(job_type, selected_set)
        has_warning = is_selected and len(missing_deps) > 0

        if is_selected:
            if has_warning:
                bg, border, text, icon_color = "#fef3c7", "#f59e0b", "#92400e", "text-yellow-600"
            else:
                bg, border, text, icon_color = "#dbeafe", "#3b82f6", "#1e40af", "text-blue-600"
        else:
            bg, border, text, icon_color = "#f9fafb", "#e5e7eb", "#6b7280", "text-gray-400"

        with ui.row().classes("items-center").style("gap: 2px;"):
            with (
                ui.button(on_click=lambda j=job_type: on_toggle(j))
                .props("flat dense no-caps")
                .style(
                    f"background: {bg}; color: {text}; padding: 4px 12px; "
                    f"border-radius: 16px; font-weight: 500; font-size: 11px; "
                    f"border: 1.5px solid {border}; min-height: 28px;"
                )
            ):
                with ui.row().classes("items-center gap-1"):
                    if is_selected:
                        ui.icon("check_circle", size="14px").classes(icon_color)
                    else:
                        ui.icon("radio_button_unchecked", size="14px").classes(icon_color)
                    ui.label(name)
                    if has_warning:
                        ui.icon("warning", size="12px").classes("text-yellow-600").tooltip(
                            f"Missing: {', '.join(get_job_display_name(d) for d in missing_deps)}"
                        )

            if i < len(PIPELINE_ORDER) - 1:
                next_job = PIPELINE_ORDER[i + 1]
                is_connected = is_selected and next_job in selected_set
                ui.icon("arrow_forward", size="14px").classes(
                    "text-blue-400" if is_connected else "text-gray-300"
                ).style("margin: 0 2px;")



def build_pipeline_builder_panel(backend: CryoBoostBackend, callbacks: Dict[str, Callable]) -> None:
    ui_mgr = get_ui_state_manager()
    state_service = get_state_service()

    _job_content_containers: Dict[str, ui.column] = {}
    _tab_strip_ref: Dict[str, object] = {}
    _content_wrapper_ref: Dict[str, object] = {}

    _pipeline_status_ref: Dict[str, Any] = {}
    _spinner_frames = "\u28cb\u2819\u2839\u2838\u283c\u2834\u2826\u2827\u2807\u280f"
    _spinner_state = {"index": 0}


    @ui.refreshable
    def _render_header_slurm_chip():
        from services.project_state import get_project_state
        state = get_project_state()

        if not state.slurm_info:
            return

        relion_path, info = next(iter(state.slurm_info.items()))
        short_name = relion_path.strip("/").split("/")[-1]

        with ui.row().classes("items-center gap-2"):
            ui.label("·").classes("text-gray-300 text-xs")
            ui.label(f"{short_name}  {info.slurm_job_id}  {info.elapsed}").classes(
                "text-xs font-mono text-blue-700 bg-blue-50 border border-blue-200 "
                "px-2 py-0.5 rounded-full"
            )
    # ===========================================
    # Tab switching
    # ===========================================

    def _refresh_tab_strip():
        strip = _tab_strip_ref.get("el")
        if not strip:
            return
        strip.clear()
        with strip:
            for job_type in ui_mgr.selected_jobs:
                name = get_job_display_name(job_type)
                is_active = ui_mgr.active_job == job_type

                with (
                    ui.button(on_click=lambda j=job_type: switch_tab(j))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 8px 20px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#6b7280'}; "
                        f"border-top: 2px solid {'#3b82f6' if is_active else 'transparent'}; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-weight: {500 if is_active else 400};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        BoundStatusDot(job_type)

    def _ensure_job_rendered(job_type: JobType):
        if job_type.value in _job_content_containers:
            return

        wrapper = _content_wrapper_ref.get("el")
        if not wrapper:
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
        for jt_str, container in _job_content_containers.items():
            container.set_visibility(jt_str == job_type.value)
        _refresh_tab_strip()

    # ===========================================
    # Job add/remove
    # ===========================================

    def add_job_to_pipeline(job_type: JobType):
        result = ui_mgr.add_job(job_type)
        if not result:
            return

        state = state_service.state
        if job_type not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            job_star_path = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(job_type, job_star_path if job_star_path.exists() else None)

        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())

        ui_mgr.set_active_job(job_type)
        rebuild_pipeline_ui()

    def _cleanup_stale_overrides(removed_job_type: JobType):
        state = state_service.state
        removed_prefix = removed_job_type.value + ":"
        for jt, job_model in state.jobs.items():
            overrides = getattr(job_model, "source_overrides", None)
            if not overrides:
                continue
            stale_keys = [k for k, v in overrides.items() if v.startswith(removed_prefix)]
            for k in stale_keys:
                del overrides[k]

    def remove_job_from_pipeline(job_type: JobType):
        if not ui_mgr.remove_job(job_type):
            return

        _cleanup_stale_overrides(job_type)
        _job_content_containers.pop(job_type.value, None)

        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())
        rebuild_pipeline_ui()

    def toggle_job_in_pipeline(job_type: JobType):
        if ui_mgr.is_job_selected(job_type):
            remove_job_from_pipeline(job_type)
        else:
            selected = set(ui_mgr.selected_jobs)
            missing = get_missing_dependencies(job_type, selected)
            if missing:
                missing_names = ", ".join(get_job_display_name(j) for j in missing)
                ui.notify(
                    f"Warning: {get_job_display_name(job_type)} typically requires: {missing_names}",
                    type="warning",
                    timeout=4000,
                )
            add_job_to_pipeline(job_type)

    # ===========================================
    # Pipeline UI state helpers
    # ===========================================

    def _set_pipeline_ui_locked(locked: bool):
        if ui_mgr.panel_refs.job_tags_container:
            ui_mgr.panel_refs.job_tags_container.set_visibility(not locked)
        if ui_mgr.panel_refs.run_button:
            if locked:
                ui_mgr.panel_refs.run_button.set_visibility(False)
            else:
                ui_mgr.panel_refs.run_button.set_visibility(True)
                ui_mgr.panel_refs.run_button.props(remove="disable loading")

        status_row = _pipeline_status_ref.get("row")
        if status_row:
            status_row.set_visibility(locked)

    # ===========================================
    # Spinner
    # ===========================================

    def _advance_spinner():
        spinner_el = _pipeline_status_ref.get("spinner")
        if not spinner_el:
            return
        _spinner_state["index"] = (_spinner_state["index"] + 1) % len(_spinner_frames)
        spinner_el.set_text(_spinner_frames[_spinner_state["index"]])

    def _start_spinner_timer():
        existing = _pipeline_status_ref.get("spinner_timer")
        if existing:
            return
        _pipeline_status_ref["spinner_timer"] = ui.timer(0.17, _advance_spinner)

    def _stop_spinner_timer():
        timer = _pipeline_status_ref.pop("spinner_timer", None)
        if timer:
            try:
                timer.cancel()
            except Exception:
                pass

    # ===========================================
    # Running status display
    # ===========================================

    def _update_pipeline_status_display(overview: Dict):
        label_el = _pipeline_status_ref.get("label")
        if not label_el:
            return

        total     = overview.get("total", 0)
        completed = overview.get("completed", 0)
        failed    = overview.get("failed", 0)
        running   = overview.get("running", 0)
        scheduled = overview.get("scheduled", 0)

        parts = []
        if completed > 0:
            parts.append(f"{completed} done")
        if running > 0:
            parts.append(f"{running} running")
        if scheduled > 0:
            parts.append(f"{scheduled} queued")
        if failed > 0:
            parts.append(f"{failed} failed")

        msg = f"{completed + failed}/{total} -- " + ", ".join(parts) if parts else f"0/{total} jobs"
        label_el.set_text(msg)

        chip = _pipeline_status_ref.get("slurm_chip")
        chip_row = _pipeline_status_ref.get("slurm_chip_row")
        if not chip or not chip_row:
            return

        from services.project_state import get_project_state
        state = get_project_state()

        print(f"[DEBUG] slurm_info: {state.slurm_info}")

        if not state.slurm_info:
            chip_row.set_visibility(False)
            return

        relion_path, info = next(iter(state.slurm_info.items()))
        short_name = relion_path.strip("/").split("/")[-1]
        chip.set_text(f"{short_name}  {info.slurm_job_id}  {info.elapsed}")
        chip_row.set_visibility(True)

    # ===========================================
    # Full rebuild
    # ===========================================

    def rebuild_pipeline_ui():
        _job_content_containers.clear()
        _tab_strip_ref.pop("el", None)
        _content_wrapper_ref.pop("el", None)

        pipeline_active = ui_mgr.is_running
        _set_pipeline_ui_locked(pipeline_active)

        flow_container = ui_mgr.panel_refs.job_tags_container
        if flow_container and not pipeline_active:
            if ui_mgr.is_project_created:
                flow_container.clear()
                with flow_container:
                    render_job_flow(ui_mgr.selected_jobs, toggle_job_in_pipeline)

        tabs_container = ui_mgr.panel_refs.job_tabs_container
        if not tabs_container:
            return

        tabs_container.clear()

        if not ui_mgr.is_project_created:
            with tabs_container:
                with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-4 mt-16"):
                    ui.icon("create_new_folder", size="64px").classes("text-gray-300")
                    ui.label("Step 1: Create a Project").classes("text-xl font-bold text-gray-500")
                    ui.label("Configure your project on the left to begin building the pipeline.").classes("text-sm")
            return

        selected = ui_mgr.selected_jobs
        if not selected:
            with tabs_container:
                ui.label("Click jobs above to add them to your pipeline").classes(
                    "text-xs text-gray-500 italic text-center p-8"
                )
            return

        if ui_mgr.active_job not in selected:
            ui_mgr.set_active_job(selected[0])

        with tabs_container:
            strip = ui.row().classes("w-full border-b border-gray-200 shrink-0").style("gap: 0;")
            _tab_strip_ref["el"] = strip
            _refresh_tab_strip()

            wrapper = ui.column().classes("w-full flex-grow overflow-hidden").style("position: relative;")
            _content_wrapper_ref["el"] = wrapper

        active = ui_mgr.active_job
        if active:
            _ensure_job_rendered(active)
            _job_content_containers[active.value].set_visibility(True)

        if pipeline_active:
            _start_spinner_timer()
            if not ui_mgr.status_timer:
                ui_mgr.status_timer = ui.timer(3.0, safe_status_check)

    # ===========================================
    # Status polling
    # ===========================================

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
            _set_pipeline_ui_locked(False)
            rebuild_pipeline_ui()
            ui.notify(
                f"SLURM submission failed: {sbatch_errors[0]}",
                type="negative",
                timeout=10000,
            )
            return

        await backend.pipeline_runner.status_sync.sync_all_jobs(str(project_path))

        if not ui_mgr.is_running:
            return

        overview = await backend.get_pipeline_overview(str(project_path))
        _update_pipeline_status_display(overview)

        total     = overview.get("total", 0)
        completed = overview.get("completed", 0)
        failed    = overview.get("failed", 0)
        running   = overview.get("running", 0)
        scheduled = overview.get("scheduled", 0)

        all_done = total > 0 and running == 0 and scheduled == 0 and (completed > 0 or failed > 0)

        if not all_done:
            return

        ui_mgr.set_pipeline_running(False)
        stop_all_timers()

        try:
            if failed > 0:
                ui.notify(f"Pipeline finished with {failed} failed job(s).", type="warning")
            else:
                ui.notify("Pipeline execution finished.", type="positive")
        except RuntimeError:
            pass

        _set_pipeline_ui_locked(False)
        rebuild_pipeline_ui()

    def stop_all_timers():
        ui_mgr.cleanup_all_timers()
        _stop_spinner_timer()
        timer = _pipeline_status_ref.pop("slurm_chip_timer", None)
        if timer:
            try:
                timer.cancel()
            except Exception:
                pass

    async def safe_status_check():
        try:
            await check_and_update_statuses()
        except Exception as e:
            print(f"[UI] Status check failed: {e}")

    # ===========================================
    # Pipeline execution
    # ===========================================

    async def handle_run_pipeline():
        if not ui_mgr.is_project_created:
            ui.notify("Create a project first", type="warning")
            return

        await state_service.save_project(force=True)

        selected_job_strings = [j.value for j in ui_mgr.selected_jobs]

        if ui_mgr.panel_refs.run_button:
            ui_mgr.panel_refs.run_button.props("loading")

        try:
            result = await backend.start_pipeline(
                project_path=str(ui_mgr.project_path),
                scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
                selected_jobs=selected_job_strings,
                required_paths=[],
            )

            if result.get("success"):
                ui_mgr.set_pipeline_running(True)
                ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
                ui_mgr.status_timer = ui.timer(3.0, safe_status_check)
                _start_spinner_timer()
                _set_pipeline_ui_locked(True)
            else:
                ui.notify(f"Failed to start: {result.get('error')}", type="negative")

        except Exception as e:
            ui.notify(f"Error: {e}", type="negative")
        finally:
            if ui_mgr.panel_refs.run_button:
                ui_mgr.panel_refs.run_button.props(remove="loading")

    # ===========================================
    # Main Layout
    # ===========================================

    with (
        ui.column()
        .classes("w-full h-full overflow-hidden")
        .style("gap: 0px; font-family: 'IBM Plex Sans', sans-serif;")
    ):
        with ui.row().classes("w-full items-center p-3 bg-white border-b border-gray-200 shrink-0").style("gap: 8px;"):
            cont_container = ui.column().classes("w-full")
            cont_container.set_visibility(False)
            ui_mgr.panel_refs.continuation_container = cont_container

            job_tags_container = (
                ui.row().classes("flex-1 items-center flex-nowrap").style("gap: 4px; overflow-x: auto; min-width: 0;")
            )
            ui_mgr.panel_refs.job_tags_container = job_tags_container

            with job_tags_container:
                render_job_flow(ui_mgr.selected_jobs, toggle_job_in_pipeline)

            run_btn = (
                ui.button("Run Pipeline", icon="play_arrow", on_click=handle_run_pipeline)
                .props("dense flat no-caps")
                .style(
                    "background: #f3f4f6; color: #1f2937; padding: 6px 20px; "
                    "border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb; "
                    "flex-shrink: 0;"
                )
            )
            ui_mgr.panel_refs.run_button = run_btn

            with ui.row().classes("items-center gap-2 flex-shrink-0") as status_row:
                status_row.set_visibility(False)
                _pipeline_status_ref["row"] = status_row

                spinner_label = (
                    ui.label("\u28cb")
                    .classes("text-blue-500 font-mono text-sm font-bold")
                    .style("width: 1ch; text-align: center;")
                )
                _pipeline_status_ref["spinner"] = spinner_label

                ui.label("Pipeline running").classes("text-xs font-semibold text-blue-600")

                status_msg = ui.label("starting...").classes("text-xs text-gray-500 font-mono")
                _pipeline_status_ref["label"] = status_msg

                with ui.row().classes("items-center gap-2 flex-shrink-0") as slurm_chip_row:
                    slurm_chip_row.set_visibility(False)
                    _pipeline_status_ref["slurm_chip_row"] = slurm_chip_row
                    ui.label("·").classes("text-gray-300 text-xs")
                    slurm_chip = ui.label("").classes(
                        "text-xs font-mono text-blue-700 bg-blue-50 border border-blue-200 "
                        "px-2 py-0.5 rounded-full"
                    )
                    _pipeline_status_ref["slurm_chip"] = slurm_chip

                # Separator
                ui.label("·").classes("text-gray-300 text-xs")

                # Live SLURM status -- refreshed by the existing status polling timer
                slurm_chip = ui.label("").classes(
                    "text-xs font-mono text-blue-700 bg-blue-50 border border-blue-200 "
                    "px-2 py-0.5 rounded-full"
                ).style("display: none;")
                _pipeline_status_ref["slurm_chip"] = slurm_chip

        ui_mgr.panel_refs.status_label = None
        ui_mgr.panel_refs.stop_button = None

        tabs_container = ui.column().classes("w-full flex-grow overflow-hidden")
        ui_mgr.panel_refs.job_tabs_container = tabs_container

    # ===========================================
    # Register callbacks
    # ===========================================

    ui_mgr.set_rebuild_callback(rebuild_pipeline_ui)

    callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
    callbacks["stop_all_timers"] = stop_all_timers
    callbacks["check_and_update_statuses"] = check_and_update_statuses
    callbacks["enable_run_button"] = lambda: _set_pipeline_ui_locked(False)
    callbacks["add_job_to_pipeline"] = add_job_to_pipeline
    callbacks["remove_job_from_pipeline"] = remove_job_from_pipeline

    rebuild_pipeline_ui()
