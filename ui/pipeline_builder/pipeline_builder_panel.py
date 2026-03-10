# ui/pipeline_builder/pipeline_builder_panel.py
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable, List, Optional, Set

from nicegui import ui

from backend import CryoBoostBackend
from services.models_base import JobStatus
from services.project_state import JobType, get_project_state, get_state_service
from ui.status_indicator import BoundStatusDot
from ui.ui_state import (
    get_ui_state_manager,
    get_job_display_name,
    get_instance_display_name,
    instance_id_to_job_type,
    get_instance_order,
)
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

_SB_BG = "#f8fafc"
_SB_SEP = "#e2e8f0"
_SB_MUTE = "#94a3b8"
_SB_ACT = "#3b82f6"
_SB_ABG = "#eff6ff"


def _missing_deps(job_type: JobType, selected_instance_ids: Set[str]) -> List[JobType]:
    """Job-type-level dependency check. A dep is satisfied if any instance of
    that type is present in the pipeline."""

    def type_present(jt: JobType) -> bool:
        prefix = jt.value
        return any(s == prefix or s.startswith(prefix + "__") for s in selected_instance_ids)

    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        if type_present(JobType.TS_RECONSTRUCT) or type_present(JobType.DENOISE_PREDICT):
            return []
        return [JobType.TS_RECONSTRUCT]

    return [d for d in JOB_DEPENDENCIES.get(job_type, []) if not type_present(d)]


def _next_instance_id(job_type: JobType, existing_ui_ids: List[str], state_keys: List[str]) -> str:
    """Generate the next available instance_id, avoiding collisions in both
    the UI roster and the persisted state dict."""
    taken = set(existing_ui_ids) | set(state_keys)
    base = job_type.value
    if base not in taken:
        return base
    for n in range(2, 200):
        candidate = f"{base}__{n}"
        if candidate not in taken:
            return candidate
    return f"{base}__{len(taken) + 1}"


def _fmt(v) -> str:
    if v is None:
        return "---"
    return f"{v:.4g}" if isinstance(v, float) else str(v)


# NEW function -- insert alongside the other inner helpers, before _refresh_roster


async def _prompt_species_and_add(job_type: JobType):
    """
    Gate for PHASE_PARTICLES job creation. Non-particle jobs pass straight through.
    For particle jobs: require at least one registered species, prompt user to pick one,
    then create the instance with species_id set from birth.
    """
    if ui_mgr.is_running:
        return

    if job_type not in PHASE_JOBS[PHASE_PARTICLES]:
        add_instance_to_pipeline(job_type)
        return

    project_path = ui_mgr.project_path
    if not project_path:
        return

    from services.project_state import get_project_state_for

    state = get_project_state_for(project_path)

    if not state.species_registry:
        ui.notify(
            "Register at least one particle species in the Template Workbench first.", type="warning", timeout=4000
        )
        return

    chosen = {"id": state.species_registry[0].id}

    with ui.dialog() as dialog, ui.card().style("min-width: 300px; padding: 16px;"):
        ui.label(f"Add {get_job_display_name(job_type)}").classes("text-base font-bold text-gray-800 mb-3")

        options = {s.id: s.name for s in state.species_registry}
        sel = (
            ui.select(options=options, value=chosen["id"], label="Particle species")
            .props("outlined dense")
            .classes("w-full")
        )

        def _on_change(e):
            chosen["id"] = e.value

        sel.on_value_change(_on_change)

        with ui.row().classes("w-full justify-end gap-2 mt-4"):
            ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
            ui.button("Add", on_click=lambda: dialog.submit(True)).props("dense no-caps").style(
                "background: #3b82f6; color: white; padding: 4px 16px; border-radius: 3px;"
            )

    confirmed = await dialog
    if not confirmed:
        return

    add_instance_to_pipeline(job_type, species_id=chosen["id"])


# ── Main function ─────────────────────────────────────────────────────────────


def build_pipeline_builder_panel(
    backend: CryoBoostBackend,
    callbacks: Dict[str, Callable],
    primary_sidebar=None,
    roster_panel=None,
    toggle_workbench: Optional[Callable] = None,
    ensure_pipeline_mode: Optional[Callable] = None,
) -> None:
    ui_mgr = get_ui_state_manager()
    state_service = get_state_service()

    _job_content_containers: Dict[str, object] = {}
    _tab_strip_ref: Dict[str, object] = {}
    _content_wrapper_ref: Dict[str, object] = {}
    _refs: Dict[str, object] = {}
    _flash_state = {"phase": None}
    _roster_state = {"visible": False, "phase": None}
    _spinner_frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    _spinner_idx = [0]
    _refs["ensure_pipeline_mode"] = ensure_pipeline_mode

    # ── Species gate for particle job creation ────────────────────────────────

    async def _prompt_species_and_add(job_type: JobType):
        if ui_mgr.is_running:
            return

        if job_type not in PHASE_JOBS[PHASE_PARTICLES]:
            add_instance_to_pipeline(job_type)
            return

        project_path = ui_mgr.project_path
        if not project_path:
            return

        from services.project_state import get_project_state_for
        state = get_project_state_for(project_path)

        if not state.species_registry:
            ui.notify(
                "Register at least one particle species in the Template Workbench first.",
                type="warning",
                timeout=4000,
            )
            return

        chosen = {"id": state.species_registry[0].id}

        with ui.dialog() as dialog, ui.card().style("min-width: 300px; padding: 16px;"):
            ui.label(f"Add {get_job_display_name(job_type)}").classes("text-base font-bold text-gray-800 mb-3")

            options = {s.id: s.name for s in state.species_registry}
            sel = (
                ui.select(options=options, value=chosen["id"], label="Particle species")
                .props("outlined dense")
                .classes("w-full")
            )

            def _on_change(e):
                chosen["id"] = e.value

            sel.on_value_change(_on_change)

            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button("Cancel", on_click=lambda: dialog.submit(False)).props("flat dense no-caps")
                ui.button("Add", on_click=lambda: dialog.submit(True)).props("dense no-caps").style(
                    "background: #3b82f6; color: white; padding: 4px 16px; border-radius: 3px;"
                )

        confirmed = await dialog
        if not confirmed:
            return

        add_instance_to_pipeline(job_type, species_id=chosen["id"])

    def _refresh_roster():
        if roster_panel is None:
            return
        roster_panel.clear()
        flashing_phase = _flash_state.get("phase")

        with roster_panel:
            for phase_id, jobs in PHASE_JOBS.items():
                icon_name, phase_label, _ = PHASE_META[phase_id]
                is_flashing = flashing_phase == phase_id

                # Phase header (sticky)
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
                        "font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.07em; line-height: 1;"
                    )

                for job_type in jobs:
                    instances = ui_mgr.get_instances_for_type(job_type)
                    has_instances = bool(instances)

                    if not has_instances:
                        # ── Unselected row: click to add first instance ──
                        if is_flashing:
                            row_bg, l_border, name_color = "#fefce8", "#fde68a", "#78716c"
                        else:
                            row_bg, l_border, name_color = "transparent", "transparent", "#9ca3af"

                        with (
                            ui.element("div")
                            .style(
                                f"display: flex; align-items: center; gap: 6px; "
                                f"padding: 5px 8px 5px 10px; cursor: pointer; "
                                f"background: {row_bg}; border-left: 2px solid {l_border}; "
                                f"border-bottom: 1px solid #f3f4f6;"
                            )
                            .on("click", lambda j=job_type: _on_unselected_click(j))
                        ):
                            ui.icon("check_box_outline_blank", size="13px").style("color: #d1d5db; flex-shrink: 0;")
                            ui.label(get_job_display_name(job_type)).style(
                                f"font-size: 11px; font-weight: 400; color: {name_color}; "
                                "flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            )

                    else:
                        # ── Job type header row ──
                        missing = _missing_deps(job_type, set(ui_mgr.selected_jobs))
                        any_active = any(ui_mgr.active_instance_id == iid for iid in instances)
                        header_border = "#3b82f6" if any_active else "#e5e7eb"

                        with ui.element("div").style(
                            f"display: flex; align-items: center; gap: 6px; "
                            f"padding: 5px 8px 5px 10px; "
                            f"background: #f8fafc; border-left: 2px solid {header_border}; "
                            f"border-bottom: 1px solid #f3f4f6;"
                        ):
                            ui.label(get_job_display_name(job_type)).style(
                                "font-size: 11px; font-weight: 600; color: #374151; "
                                "flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            )

                            if len(instances) > 1:
                                ui.label(str(len(instances))).style(
                                    "font-size: 9px; font-weight: 700; color: #6b7280; "
                                    "background: #e5e7eb; border-radius: 999px; "
                                    "padding: 1px 5px; flex-shrink: 0;"
                                )

                            if missing:
                                ui.icon("warning", size="11px").style("color: #f59e0b; flex-shrink: 0;").tooltip(
                                    "Missing: " + ", ".join(get_job_display_name(d) for d in missing)
                                )

                            (
                                ui.button(icon="add", on_click=lambda j=job_type: _prompt_species_and_add(j))
                                .props("flat dense round size=xs")
                                .style("color: #6b7280; flex-shrink: 0;")
                                .tooltip(f"Add another {get_job_display_name(job_type)}")
                            )

                        # ── Instance sub-rows ──
                        for instance_id in instances:
                            job_model = state_service.state.jobs.get(instance_id)

                            base_name = get_job_display_name(job_type)
                            relion_job_name = getattr(job_model, "relion_job_name", None) if job_model else None
                            if relion_job_name:
                                job_folder = relion_job_name.rstrip("/").split("/")[-1]
                                display_text = f"{base_name} ({job_folder})"
                            else:
                                parts = instance_id.split("__", 1)
                                if len(parts) > 1:
                                    suffix = parts[1]
                                    display_text = (
                                        f"{base_name} #{suffix}" if suffix.isdigit() else f"{base_name} ({suffix})"
                                    )
                                else:
                                    display_text = base_name

                            species_id = getattr(job_model, "species_id", None) if job_model else None
                            species = None
                            if species_id and ui_mgr.project_path:
                                from services.project_state import get_project_state_for

                                s_state = get_project_state_for(ui_mgr.project_path)
                                species = s_state.get_species(species_id)

                            is_active = ui_mgr.active_instance_id == instance_id

                            if is_active:
                                row_bg, l_border = "#eff6ff", "#3b82f6"
                                name_color, name_wt = "#1e40af", "600"
                            else:
                                row_bg, l_border = "#fafafa", "#e5e7eb"
                                name_color, name_wt = "#374151", "400"

                            with ui.element("div").style(
                                f"display: flex; align-items: center; gap: 4px; "
                                f"padding: 4px 6px 4px 22px; "
                                f"background: {row_bg}; border-left: 2px solid {l_border}; "
                                f"border-bottom: 1px solid #f3f4f6; "
                                f"min-width: 0; overflow: hidden;"
                            ):
                                # Click target: name + optional species pill, all on one line
                                with (
                                    ui.element("div")
                                    .style(
                                        "display: flex; align-items: center; gap: 5px; "
                                        "flex: 1; cursor: pointer; min-width: 0; overflow: hidden;"
                                    )
                                    .on("click", lambda iid=instance_id: switch_tab(iid))
                                ):
                                    ui.label(display_text).style(
                                        f"font-size: 11px; font-weight: {name_wt}; color: {name_color}; "
                                        "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; "
                                        "flex-shrink: 1; min-width: 0;"
                                    )
                                    if species:
                                        with ui.element("div").style(
                                            f"display: inline-flex; align-items: center; flex-shrink: 0; "
                                            f"background: {species.color}18; border: 1px solid {species.color}55; "
                                            f"border-radius: 999px; padding: 1px 7px;"
                                        ):
                                            ui.label(species.name).style(
                                                f"font-size: 9px; color: {species.color}; "
                                                "font-weight: 600; white-space: nowrap;"
                                            )

                                # Status dot + remove, fixed right side
                                with ui.element("div").style(
                                    "display: flex; align-items: center; gap: 3px; flex-shrink: 0;"
                                ):
                                    with ui.element("span").style("overflow: visible; line-height: 0;"):
                                        BoundStatusDot(instance_id)

                                    if not ui_mgr.is_running:
                                        (
                                            ui.button(
                                                icon="close", on_click=lambda _, iid=instance_id: _on_remove_click(iid)
                                            )
                                            .props("flat dense round size=xs")
                                            .style("color: #9ca3af;")
                                            .tooltip("Remove this instance")
                                        )

    # UPDATED -- _on_unselected_click becomes async, delegates to _prompt_species_and_add

    async def _on_unselected_click(job_type: JobType):
        epm = _refs.get("ensure_pipeline_mode")
        if epm:
            epm()
        if ui_mgr.is_running:
            return
        selected = set(ui_mgr.selected_jobs)
        missing = _missing_deps(job_type, selected)
        if missing:
            ui.notify(
                f"{get_job_display_name(job_type)} typically requires: "
                + ", ".join(get_job_display_name(d) for d in missing),
                type="warning",
                timeout=3000,
            )
        await _prompt_species_and_add(job_type)

    def _scroll_to_phase(phase_id: str):
        anchor = ROSTER_ANCHOR[phase_id]
        ui.run_javascript(
            f"requestAnimationFrame(function(){{"
            f"  var e=document.getElementById('{anchor}');"
            f"  if(e)e.scrollIntoView({{behavior:'smooth',block:'start'}});"
            f"}});"
        )

    async def _on_remove_click(instance_id: str):
        if ui_mgr.is_running:
            return

        project_path = ui_mgr.project_path
        if not project_path:
            remove_instance_from_pipeline(instance_id)
            return

        from services.project_state import get_project_state_for

        state = get_project_state_for(project_path)
        job_model = state.jobs.get(instance_id)
        status = job_model.execution_status if job_model else None

        # Scheduled or never-ran: just pop from roster and free the index.
        if status not in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            remove_instance_from_pipeline(instance_id)
            return

        # Completed or failed jobs: they have real output on disk,
        # so go through the full delete dialog (move to Trash, update pipeline star).
        from ui.ui_state import get_instance_display_name
        from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service

        deletion_service = get_deletion_service()
        project_path = ui_mgr.project_path
        preview = None
        if project_path and job_model.relion_job_name:
            preview = deletion_service.preview_deletion(
                project_path, job_model.relion_job_name, job_resolver=backend.pipeline_orchestrator.job_resolver
            )

        with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
            ui.label(f"Delete {get_instance_display_name(instance_id, job_model)}?").classes("text-lg font-bold")
            ui.label("This will move the job files to Trash/ and remove it from the pipeline.").classes(
                "text-sm text-gray-600 mb-2"
            )

            if preview and preview.get("success") and preview.get("downstream_count", 0) > 0:
                downstream = preview.get("downstream_jobs", [])
                with ui.card().classes("w-full bg-orange-50 border border-orange-200 p-3 mb-2"):
                    with ui.row().classes("items-center gap-2 mb-2"):
                        ui.icon("warning", size="20px").classes("text-orange-600")
                        ui.label(f"{len(downstream)} job(s) will become orphaned:").classes(
                            "text-sm font-bold text-orange-800"
                        )
                    with ui.column().classes("gap-1 ml-6"):
                        for detail in downstream:
                            with ui.row().classes("items-center gap-2"):
                                ui.label(detail.get("path", "Unknown")).classes("text-xs font-mono text-gray-700")
                                if detail.get("type"):
                                    ui.label(f"({detail['type']})").classes("text-xs text-gray-500")
                                ui.label(f"- {detail.get('status', 'Unknown')}").classes("text-xs text-gray-500")
                    ui.label("These jobs will have broken input references and may fail if re-run.").classes(
                        "text-xs text-orange-700 mt-2"
                    )
            else:
                ui.label("No downstream jobs will be affected.").classes(
                    "text-sm text-green-600 bg-green-50 p-2 rounded"
                )

            with ui.row().classes("w-full justify-end mt-4 gap-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat")

                async def confirm():
                    dialog.close()
                    try:
                        result = await backend.delete_job(
                            instance_id_to_job_type(instance_id).value, instance_id=instance_id
                        )
                        if result.get("success"):
                            orphans = result.get("orphaned_jobs", [])
                            if orphans:
                                ui.notify(
                                    f"Deleted. {len(orphans)} downstream job(s) orphaned.", type="warning", timeout=5000
                                )
                            else:
                                ui.notify("Job deleted.", type="positive")
                            remove_instance_from_pipeline(instance_id)
                        else:
                            ui.notify(f"Delete failed: {result.get('error')}", type="negative", timeout=8000)
                    except Exception as e:
                        ui.notify(f"Error: {e}", type="negative")

                delete_btn = ui.button("Delete", color="red", on_click=confirm)
                if preview and preview.get("downstream_count", 0) > 0:
                    delete_btn.props('icon="delete_forever"')

        dialog.open()

    def _toggle_roster(phase_id: str):
        same_and_open = _roster_state["visible"] and _roster_state["phase"] == phase_id
        if same_and_open:
            _roster_state["visible"] = False
            _roster_state["phase"] = None
            _flash_state["phase"] = None
            if roster_panel is not None:
                roster_panel.style("display: none;")
        else:
            _roster_state["visible"] = True
            _roster_state["phase"] = phase_id
            _flash_state["phase"] = phase_id
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
        ui.element("div").style(f"height: 1px; background: {_SB_SEP}; width: 24px; margin: 3px auto;")

    def _info_popup_btn(icon_name: str, title: str, rows: list):
        btn = (
            ui.button(icon=icon_name)
            .props("flat dense")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"color: {_SB_MUTE}; background: transparent; min-width: 0;"
            )
        )
        with btn:
            with (
                ui.menu()
                .props('anchor="center right" self="center left" :offset="[8,0]"')
                .style(
                    "background: #ffffff; border: 1px solid #e2e8f0; "
                    "border-radius: 5px; overflow: hidden; min-width: 210px; "
                    "padding: 0; box-shadow: 0 4px 12px rgba(0,0,0,0.08);"
                )
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
                        ui.label(row_lbl).style("font-size: 10px; color: #94a3b8; flex-shrink: 0;")
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

            _info_popup_btn(
                "folder_open",
                "Project",
                [
                    ("Name", state.project_name),
                    ("Root", str(state.project_path) if state.project_path else "---"),
                    ("Movies", state.movies_glob or "---"),
                    ("MDOC", state.mdocs_glob or "---"),
                ],
            )
            _info_popup_btn(
                "science",
                "Acquisition",
                [
                    ("Pixel", f"{_fmt(state.microscope.pixel_size_angstrom)} Å"),
                    ("Voltage", f"{_fmt(state.microscope.acceleration_voltage_kv)} kV"),
                    ("Cs", f"{_fmt(state.microscope.spherical_aberration_mm)} mm"),
                    ("Amp. C.", _fmt(state.microscope.amplitude_contrast)),
                    ("Dose", f"{_fmt(state.acquisition.dose_per_tilt)} e⁻/Å²"),
                    ("Tilt ax.", f"{_fmt(state.acquisition.tilt_axis_degrees)} °"),
                ],
            )

            _sb_sep()

            run_slot = ui.element("div").style(
                "width: 100%; display: flex; flex-direction: column; align-items: center; padding: 2px 6px; gap: 3px;"
            )
            _refs["run_slot"] = run_slot

            (
                ui.button(icon="close", on_click=lambda: ui.navigate.to("/"))
                .props("flat dense")
                .style(
                    f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                    f"color: {_SB_MUTE}; background: transparent; min-width: 0;"
                )
                .tooltip("Close project")
            )

            _sb_sep()

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

            if toggle_workbench is not None:
                _sb_sep()
                wb_btn = (
                    ui.button(icon="biotech", on_click=toggle_workbench)
                    .props("flat dense")
                    .style(
                        f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                        f"background: transparent; color: {_SB_MUTE}; min-width: 0;"
                    )
                    .tooltip("Template Workbench")
                )
                _refs["wb_btn"] = wb_btn
                callbacks["wb_btn"] = wb_btn

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
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
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
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
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
            for instance_id in ui_mgr.selected_jobs:
                job_model = state_service.state.jobs.get(instance_id)
                job_type = instance_id_to_job_type(instance_id)

                base_name = get_job_display_name(job_type)
                relion_job_name = getattr(job_model, "relion_job_name", None) if job_model else None
                if relion_job_name:
                    job_folder = relion_job_name.rstrip("/").split("/")[-1]
                    display_text = f"{base_name} ({job_folder})"
                else:
                    parts = instance_id.split("__", 1)
                    if len(parts) > 1:
                        suffix = parts[1]
                        display_text = f"{base_name} #{suffix}" if suffix.isdigit() else f"{base_name} ({suffix})"
                    else:
                        display_text = base_name

                species_id = getattr(job_model, "species_id", None) if job_model else None
                species = None
                if species_id and ui_mgr.project_path:
                    from services.project_state import get_project_state_for

                    s_state = get_project_state_for(ui_mgr.project_path)
                    species = s_state.get_species(species_id)

                is_active = ui_mgr.active_instance_id == instance_id

                tab_border_color = "#3b82f6" if is_active else "transparent"

                with (
                    ui.button(on_click=lambda iid=instance_id: switch_tab(iid))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 0 14px; height: 36px; border-radius: 0; flex-shrink: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#9ca3af'}; "
                        f"border-top: 2px solid {tab_border_color}; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-size: 11px; font-weight: {'500' if is_active else '400'};"
                    )
                ):
                    with ui.element("div").style(
                        "display: flex; align-items: center; gap: 6px; white-space: nowrap; overflow: hidden;"
                    ):
                        ui.label(display_text).style("flex-shrink: 0;")

                        if species:
                            with ui.element("div").style(
                                f"display: inline-flex; align-items: center; flex-shrink: 0; "
                                f"background: {species.color}18; border: 1px solid {species.color}55; "
                                f"border-radius: 999px; padding: 1px 7px;"
                            ):
                                ui.label(species.name).style(
                                    f"font-size: 9px; color: {species.color}; font-weight: 600;"
                                )

                        BoundStatusDot(instance_id)

    # ── Lazy tab content ──────────────────────────────────────────────────────

    def _ensure_job_rendered(instance_id: str):
        if instance_id in _job_content_containers:
            return
        try:
            job_type = instance_id_to_job_type(instance_id)
        except ValueError:
            print(f"[PANEL] Unknown job type for instance_id '{instance_id}'")
            return

        wrapper = _content_wrapper_ref.get("el")
        if wrapper is None:
            return

        with wrapper:
            container = ui.column().classes("w-full overflow-hidden").style("flex: 1 1 0%; min-height: 0;")
            container.set_visibility(False)
            _job_content_containers[instance_id] = container
            with container:
                render_job_tab(
                    job_type=job_type,
                    instance_id=instance_id,
                    backend=backend,
                    ui_mgr=ui_mgr,
                    callbacks={
                        **callbacks,
                        "check_and_update_statuses": check_and_update_statuses,
                        "rebuild_pipeline_ui": rebuild_pipeline_ui,
                        "remove_instance_from_pipeline": remove_instance_from_pipeline,
                    },
                )

    def invalidate_tm_tabs():
        """Clear cached renders for TM jobs and re-render the active one if applicable."""
        tm_prefix = JobType.TEMPLATE_MATCH_PYTOM.value
        stale = [iid for iid in list(_job_content_containers.keys()) if iid.split("__")[0] == tm_prefix]
        for iid in stale:
            container = _job_content_containers.pop(iid, None)
            if container:
                try:
                    container.delete()
                except Exception:
                    pass

        active = ui_mgr.active_instance_id
        if active and active.split("__")[0] == tm_prefix:
            _ensure_job_rendered(active)
            for iid, c in _job_content_containers.items():
                c.set_visibility(iid == active)

    def switch_tab(instance_id: str):
        epm = _refs.get("ensure_pipeline_mode")
        if epm:
            epm()
        ui_mgr.set_active_instance(instance_id)
        _ensure_job_rendered(instance_id)
        for iid, c in _job_content_containers.items():
            c.set_visibility(iid == instance_id)
        _refresh_tab_strip()
        _refresh_roster()

    # ── Job / instance management ─────────────────────────────────────────────

    # UPDATED -- add_instance_to_pipeline gains species_id parameter

    def add_instance_to_pipeline(
        job_type: JobType, instance_id: Optional[str] = None, species_id: Optional[str] = None
    ):
        if ui_mgr.is_running:
            return

        if instance_id is None:
            state = state_service.state
            instance_id = _next_instance_id(job_type, ui_mgr.selected_jobs, list(state.jobs.keys()))

        if not ui_mgr.add_instance(instance_id, job_type):
            return

        state = state_service.state
        if instance_id not in state.jobs:
            template_base = Path.cwd() / "config" / "Schemes" / "warp_tomo_prep"
            star = template_base / job_type.value / "job.star"
            state.ensure_job_initialized(
                job_type, instance_id=instance_id, template_path=star if star.exists() else None
            )

        if species_id is not None:
            job_model = state.jobs.get(instance_id)
            if job_model is not None:
                job_model.species_id = species_id
                # For TM, pre-populate template/mask from registry as a convenience default.
                # These will be overwritten by the scoped selector in the config tab,
                # and again at submission time from the live registry.
                if job_type == JobType.TEMPLATE_MATCH_PYTOM and ui_mgr.project_path:
                    from services.project_state import get_project_state_for

                    p_state = get_project_state_for(ui_mgr.project_path)
                    sp = p_state.get_species(species_id)
                    if sp:
                        job_model.template_path = sp.template_path or ""
                        job_model.mask_path = sp.mask_path or ""

        if ui_mgr.is_project_created:
            asyncio.create_task(state_service.save_project())

        ui_mgr.set_active_instance(instance_id)
        rebuild_pipeline_ui()

    def _cleanup_stale_overrides_for_instance(instance_id: str):
        """Clean source_overrides in other jobs that reference this specific instance."""
        state = state_service.state
        removed_model = state.jobs.get(instance_id)
        job_type_str = instance_id.split("__")[0]

        refs_to_clean: set = set()

        if removed_model:
            relion_name = getattr(removed_model, "relion_job_name", None)
            if relion_name:
                refs_to_clean.add(f"{job_type_str}:{relion_name.rstrip('/')}")
        # Also catch pending path references
        refs_to_clean.add(f"{job_type_str}:External/pending_{instance_id}")

        for _, job_model in state.jobs.items():
            overrides = getattr(job_model, "source_overrides", None)
            if not overrides:
                continue
            stale = [k for k, v in overrides.items() if v in refs_to_clean]
            for k in stale:
                del overrides[k]

    def remove_instance_from_pipeline(instance_id: str):
        if ui_mgr.is_running:
            return
        if not ui_mgr.remove_instance(instance_id):
            return
        _cleanup_stale_overrides_for_instance(instance_id)
        _job_content_containers.pop(instance_id, None)

        # Remove from state.jobs if the job never succeeded - keeps the ID
        # from being recycled and inheriting stale Failed/Scheduled state.
        state = state_service.state
        job_model = state.jobs.get(instance_id)
        if job_model and job_model.execution_status != JobStatus.SUCCEEDED:
            del state.jobs[instance_id]
            state.job_path_mapping.pop(instance_id, None)

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

        selected = ui_mgr.selected_jobs  # List[str] instance_ids
        if not selected:
            with tabs_container:
                ui.label("Select jobs from the left panel.").classes("text-xs text-gray-400 italic p-8")
            return

        if ui_mgr.active_instance_id not in selected:
            ui_mgr.set_active_instance(selected[0])

        with tabs_container:
            strip = ui.element("div").style(
                "display: flex; flex-direction: row; width: 100%; flex-shrink: 0; "
                "border-bottom: 1px solid #e5e7eb; overflow-x: auto; gap: 0;"
            )
            _tab_strip_ref["el"] = strip
            _refresh_tab_strip()

            wrapper = ui.element("div").style(
                "display: flex; flex-direction: column; width: 100%; flex: 1 1 0%; min-height: 0; overflow: hidden;"
            )
            _content_wrapper_ref["el"] = wrapper

        active = ui_mgr.active_instance_id
        if active:
            _ensure_job_rendered(active)
            _job_content_containers[active].set_visibility(True)

        if ui_mgr.is_running:
            _start_spinner_timer()
            try:
                ui_mgr.status_timer = ui.timer(3.0, safe_status_check)
            except RuntimeError:
                # Called from a background task (e.g. logs_tab placeholder swap)
                # without a live client slot. The polling loop will be re-attached
                # the next time the user interacts with the page.
                pass

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

        try:
            result = await backend.start_pipeline(
                project_path=str(ui_mgr.project_path),
                scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
                selected_jobs=ui_mgr.selected_jobs,  # already List[str] instance_ids
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

    # ── Tab content area ──────────────────────────────────────────────────────

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
    callbacks["add_job_to_pipeline"] = lambda jt: add_instance_to_pipeline(jt)
    callbacks["add_instance_to_pipeline"] = add_instance_to_pipeline
    callbacks["remove_instance_from_pipeline"] = remove_instance_from_pipeline
    callbacks["invalidate_tm_tabs"] = invalidate_tm_tabs

    rebuild_pipeline_ui()

    # On first render, do an immediate sync to recover status after a server
    # restart. If default_pipeline.star shows Running jobs, re-attach the
    # polling loop even when pipeline_active is False in persisted state.
    if ui_mgr.is_project_created:

        async def _startup_sync():
            if not ui_mgr.project_path:
                return
            await backend.pipeline_runner.status_sync.sync_all_jobs(str(ui_mgr.project_path))
            state = get_project_state()
            any_running = any(m.execution_status == JobStatus.RUNNING for m in state.jobs.values())
            if (any_running or state.pipeline_active) and not ui_mgr.is_running:
                ui_mgr.set_pipeline_running(True)
                rebuild_pipeline_ui()
            elif ui_mgr.is_running:
                ui_mgr.status_timer = ui.timer(3.0, safe_status_check)

        ui.timer(0.3, _startup_sync, once=True)
    elif ui_mgr.is_running:
        ui.timer(0.2, safe_status_check, once=True)
