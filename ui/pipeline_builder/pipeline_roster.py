import asyncio
from typing import Dict, Optional, TYPE_CHECKING

from nicegui import ui

from services.models_base import JobStatus
from services.project_state import JobType
from ui.status_indicator import BoundStatusDot
from ui.ui_state import get_job_display_name, get_instance_display_name, instance_id_to_job_type
from ui.pipeline_builder.pipeline_constants import (
    PHASE_JOBS, PHASE_META, ROSTER_ANCHOR,
    SB_MUTE, SB_ACT, SB_ABG, SB_SEP,
    missing_deps, fmt,
)

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel


class RosterWidget:
    def __init__(self, panel: "PipelineBuilderPanel"):
        self.panel = panel
        self._flash_phase: Optional[str] = None
        self._roster_visible: bool = False
        self._roster_phase: Optional[str] = None
        self._refs: Dict = {}
        self._spinner_frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self._spinner_idx: int = 0

    # ── Roster ────────────────────────────────────────────────────────────────

    def refresh(self):
        panel = self.panel
        if panel.roster_panel is None:
            return
        panel.roster_panel.clear()

        with panel.roster_panel:
            for phase_id, jobs in PHASE_JOBS.items():
                icon_name, phase_label, _ = PHASE_META[phase_id]
                is_flashing = self._flash_phase == phase_id

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
                    instances = panel.ui_mgr.get_instances_for_type(job_type)
                    has_instances = bool(instances)

                    if not has_instances:
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
                            .on("click", lambda j=job_type: self._on_unselected_click(j))
                        ):
                            ui.icon("check_box_outline_blank", size="13px").style(
                                "color: #d1d5db; flex-shrink: 0;"
                            )
                            ui.label(get_job_display_name(job_type)).style(
                                f"font-size: 11px; font-weight: 400; color: {name_color}; "
                                "flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            )
                    else:
                        missing = missing_deps(job_type, set(panel.ui_mgr.selected_jobs))
                        any_active = any(panel.ui_mgr.active_instance_id == iid for iid in instances)
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
                                ui.icon("warning", size="11px").style(
                                    "color: #f59e0b; flex-shrink: 0;"
                                ).tooltip("Missing: " + ", ".join(get_job_display_name(d) for d in missing))
                            (
                                ui.button(
                                    icon="add",
                                    on_click=lambda j=job_type: panel.prompt_species_and_add(j),
                                )
                                .props("flat dense round size=xs")
                                .style("color: #6b7280; flex-shrink: 0;")
                                .tooltip(f"Add another {get_job_display_name(job_type)}")
                            )

                        for instance_id in instances:
                            job_model = panel.state_service.state.jobs.get(instance_id)

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
                                        f"{base_name} #{suffix}"
                                        if suffix.isdigit()
                                        else f"{base_name} ({suffix})"
                                    )
                                else:
                                    display_text = base_name

                            species_id = getattr(job_model, "species_id", None) if job_model else None
                            species = None
                            if species_id and panel.ui_mgr.project_path:
                                from services.project_state import get_project_state_for
                                s_state = get_project_state_for(panel.ui_mgr.project_path)
                                species = s_state.get_species(species_id)

                            is_active = panel.ui_mgr.active_instance_id == instance_id
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
                                with (
                                    ui.element("div")
                                    .style(
                                        "display: flex; align-items: center; gap: 5px; "
                                        "flex: 1; cursor: pointer; min-width: 0; overflow: hidden;"
                                    )
                                    .on("click", lambda iid=instance_id: panel.switch_tab(iid))
                                ):
                                    ui.label(display_text).style(
                                        f"font-size: 11px; font-weight: {name_wt}; color: {name_color}; "
                                        "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; "
                                        "flex-shrink: 1; min-width: 0;"
                                    )
                                    if species:
                                        with ui.element("div").style(
                                            f"display: inline-flex; align-items: center; flex-shrink: 0; "
                                            f"background: {species.color}18; "
                                            f"border: 1px solid {species.color}55; "
                                            f"border-radius: 999px; padding: 1px 7px;"
                                        ):
                                            ui.label(species.name).style(
                                                f"font-size: 9px; color: {species.color}; "
                                                "font-weight: 600; white-space: nowrap;"
                                            )

                                with ui.element("div").style(
                                    "display: flex; align-items: center; gap: 3px; flex-shrink: 0;"
                                ):
                                    with ui.element("span").style("overflow: visible; line-height: 0;"):
                                        BoundStatusDot(instance_id)
                                    if not panel.ui_mgr.is_running:
                                        (
                                            ui.button(
                                                icon="close",
                                                on_click=lambda _, iid=instance_id: self._on_remove_click(iid),
                                            )
                                            .props("flat dense round size=xs")
                                            .style("color: #9ca3af;")
                                            .tooltip("Remove this instance")
                                        )

    async def _on_unselected_click(self, job_type: JobType):
        panel = self.panel
        if panel.ensure_pipeline_mode:
            panel.ensure_pipeline_mode()
        if panel.ui_mgr.is_running:
            return
        missing = missing_deps(job_type, set(panel.ui_mgr.selected_jobs))
        if missing:
            ui.notify(
                f"{get_job_display_name(job_type)} typically requires: "
                + ", ".join(get_job_display_name(d) for d in missing),
                type="warning",
                timeout=3000,
            )
        await panel.prompt_species_and_add(job_type)

    async def _on_remove_click(self, instance_id: str):
        panel = self.panel
        if panel.ui_mgr.is_running:
            return

        project_path = panel.ui_mgr.project_path
        if not project_path:
            panel.remove_instance_from_pipeline(instance_id)
            return

        from services.project_state import get_project_state_for
        state = get_project_state_for(project_path)
        job_model = state.jobs.get(instance_id)
        status = job_model.execution_status if job_model else None

        if status not in (JobStatus.SUCCEEDED, JobStatus.FAILED):
            panel.remove_instance_from_pipeline(instance_id)
            return

        from services.scheduling_and_orchestration.pipeline_deletion_service import get_deletion_service
        deletion_service = get_deletion_service()
        preview = None
        if project_path and job_model.relion_job_name:
            preview = deletion_service.preview_deletion(
                project_path,
                job_model.relion_job_name,
                job_resolver=panel.backend.pipeline_orchestrator.job_resolver,
            )

        with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
            ui.label(f"Delete {get_instance_display_name(instance_id, job_model)}?").classes("text-lg font-bold")
            ui.label(
                "This will move the job files to Trash/ and remove it from the pipeline."
            ).classes("text-sm text-gray-600 mb-2")

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
                                ui.label(detail.get("path", "Unknown")).classes(
                                    "text-xs font-mono text-gray-700"
                                )
                                if detail.get("type"):
                                    ui.label(f"({detail['type']})").classes("text-xs text-gray-500")
                                ui.label(f"- {detail.get('status', 'Unknown')}").classes(
                                    "text-xs text-gray-500"
                                )
                    ui.label(
                        "These jobs will have broken input references and may fail if re-run."
                    ).classes("text-xs text-orange-700 mt-2")
            else:
                ui.label("No downstream jobs will be affected.").classes(
                    "text-sm text-green-600 bg-green-50 p-2 rounded"
                )

            with ui.row().classes("w-full justify-end mt-4 gap-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat")

                async def confirm():
                    dialog.close()
                    try:
                        result = await panel.backend.delete_job(
                            instance_id_to_job_type(instance_id).value, instance_id=instance_id
                        )
                        if result.get("success"):
                            orphans = result.get("orphaned_jobs", [])
                            if orphans:
                                ui.notify(
                                    f"Deleted. {len(orphans)} downstream job(s) orphaned.",
                                    type="warning",
                                    timeout=5000,
                                )
                            else:
                                ui.notify("Job deleted.", type="positive")
                            panel.remove_instance_from_pipeline(instance_id)
                        else:
                            ui.notify(
                                f"Delete failed: {result.get('error')}", type="negative", timeout=8000
                            )
                    except Exception as e:
                        ui.notify(f"Error: {e}", type="negative")

                delete_btn = ui.button("Delete", color="red", on_click=confirm)
                if preview and preview.get("downstream_count", 0) > 0:
                    delete_btn.props('icon="delete_forever"')

        dialog.open()

    # ── Roster toggle ─────────────────────────────────────────────────────────

    def toggle(self, phase_id: str):
        same_and_open = self._roster_visible and self._roster_phase == phase_id
        if same_and_open:
            self._roster_visible = False
            self._roster_phase = None
            self._flash_phase = None
            if self.panel.roster_panel is not None:
                self.panel.roster_panel.style("display: none;")
        else:
            self._roster_visible = True
            self._roster_phase = phase_id
            self._flash_phase = phase_id
            if self.panel.roster_panel is not None:
                self.panel.roster_panel.style("display: flex;")
            self.refresh()
            self._scroll_to_phase(phase_id)
            ui.timer(2.0, lambda: self._clear_flash(), once=True)

        self._update_phase_btn_styles()
        self.refresh()

    def _clear_flash(self):
        self._flash_phase = None
        self.refresh()

    def _scroll_to_phase(self, phase_id: str):
        anchor = ROSTER_ANCHOR[phase_id]
        ui.run_javascript(
            f"requestAnimationFrame(function(){{"
            f"  var e=document.getElementById('{anchor}');"
            f"  if(e)e.scrollIntoView({{behavior:'smooth',block:'start'}});"
            f"}});"
        )

    def _update_phase_btn_styles(self):
        for phase_id in PHASE_JOBS:
            btn = self._refs.get(f"phase_btn_{phase_id}")
            if btn is None:
                continue
            active = self._roster_visible and self._roster_phase == phase_id
            btn.style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {SB_ABG if active else 'transparent'}; "
                f"color: {SB_ACT if active else SB_MUTE}; min-width: 0;"
            )

    # ── Sidebar ───────────────────────────────────────────────────────────────

    def build_sidebar(self):
        panel = self.panel
        if panel.primary_sidebar is None:
            return
        from services.project_state import get_project_state
        state = get_project_state()

        with panel.primary_sidebar:
            ui.element("div").style("height: 8px;")
            ui.icon("biotech", size="14px").style(f"color: {SB_MUTE}; margin: 0 auto 4px;")
            self._sb_sep()

            self._info_popup_btn(
                "folder_open",
                "Project",
                [
                    ("Name", state.project_name),
                    ("Root", str(state.project_path) if state.project_path else "---"),
                    ("Movies", state.movies_glob or "---"),
                    ("MDOC", state.mdocs_glob or "---"),
                ],
            )
            self._info_popup_btn(
                "science",
                "Acquisition",
                [
                    ("Pixel", f"{fmt(state.microscope.pixel_size_angstrom)} Å"),
                    ("Voltage", f"{fmt(state.microscope.acceleration_voltage_kv)} kV"),
                    ("Cs", f"{fmt(state.microscope.spherical_aberration_mm)} mm"),
                    ("Amp. C.", fmt(state.microscope.amplitude_contrast)),
                    ("Dose", f"{fmt(state.acquisition.dose_per_tilt)} e⁻/Å²"),
                    ("Tilt ax.", f"{fmt(state.acquisition.tilt_axis_degrees)} °"),
                ],
            )

            self._sb_sep()

            run_slot = ui.element("div").style(
                "width: 100%; display: flex; flex-direction: column; "
                "align-items: center; padding: 2px 6px; gap: 3px;"
            )
            self._refs["run_slot"] = run_slot

            (
                ui.button(icon="close", on_click=lambda: ui.navigate.to("/"))
                .props("flat dense")
                .style(
                    f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                    f"color: {SB_MUTE}; background: transparent; min-width: 0;"
                )
                .tooltip("Close project")
            )

            self._sb_sep()

            for phase_id in PHASE_JOBS:
                icon_name, label, sub = PHASE_META[phase_id]
                btn = (
                    ui.button(icon=icon_name, on_click=lambda p=phase_id: self.toggle(p))
                    .props("flat dense")
                    .style(
                        f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                        f"background: transparent; color: {SB_MUTE}; min-width: 0;"
                    )
                )
                btn.tooltip(f"{label} — {sub}")
                self._refs[f"phase_btn_{phase_id}"] = btn

            if panel.toggle_workbench is not None:
                self._sb_sep()
                wb_btn = (
                    ui.button(icon="biotech", on_click=panel.toggle_workbench)
                    .props("flat dense")
                    .style(
                        f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                        f"background: transparent; color: {SB_MUTE}; min-width: 0;"
                    )
                    .tooltip("Template Workbench")
                )
                self._refs["wb_btn"] = wb_btn
                panel.callbacks["wb_btn"] = wb_btn

            ui.element("div").style("flex: 1;")

        self.rebuild_run_slot()

    def rebuild_run_slot(self):
        run_slot = self._refs.get("run_slot")
        if run_slot is None:
            return
        run_slot.clear()
        panel = self.panel
        with run_slot:
            if panel.ui_mgr.is_running:
                with (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #fef2f2; border: 1px solid #fecaca; "
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    .on("click", panel.handle_stop_pipeline)
                    .tooltip("Stop pipeline")
                ):
                    ui.icon("stop", size="16px").style("color: #b91c1c; pointer-events: none;")

                spinner = ui.label("⠋").style(
                    "font-family: 'IBM Plex Mono', monospace; font-size: 18px; "
                    "color: #3b82f6; text-align: center; line-height: 1; "
                    "display: block; width: 100%; margin-top: 2px;"
                )
                self._refs["spinner"] = spinner

                status_lbl = ui.label("").style(
                    f"font-size: 8px; color: {SB_MUTE}; "
                    "font-family: 'IBM Plex Mono', monospace; "
                    "text-align: center; line-height: 1.4; word-break: break-all; "
                    "display: block; width: 100%; padding: 0 3px;"
                )
                self._refs["status_label"] = status_lbl
            else:
                with (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #f0fdf4; border: 1px solid #bbf7d0; "
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    .on("click", panel.handle_run_pipeline)
                    .tooltip("Run pipeline")
                ):
                    ui.icon("play_arrow", size="16px").style("color: #15803d; pointer-events: none;")

    # ── Tab strip ─────────────────────────────────────────────────────────────

    def refresh_tab_strip(self):
        panel = self.panel
        strip = panel._tab_strip_ref.get("el")
        if strip is None:
            return
        strip.clear()
        with strip:
            for instance_id in panel.ui_mgr.selected_jobs:
                job_model = panel.state_service.state.jobs.get(instance_id)
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
                        display_text = (
                            f"{base_name} #{suffix}" if suffix.isdigit() else f"{base_name} ({suffix})"
                        )
                    else:
                        display_text = base_name

                species_id = getattr(job_model, "species_id", None) if job_model else None
                species = None
                if species_id and panel.ui_mgr.project_path:
                    from services.project_state import get_project_state_for
                    s_state = get_project_state_for(panel.ui_mgr.project_path)
                    species = s_state.get_species(species_id)

                is_active = panel.ui_mgr.active_instance_id == instance_id
                tab_border_color = "#3b82f6" if is_active else "transparent"

                with (
                    ui.button(on_click=lambda iid=instance_id: panel.switch_tab(iid))
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
                        "display: flex; align-items: center; gap: 6px; "
                        "white-space: nowrap; overflow: hidden;"
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

    # ── Spinner / status label ────────────────────────────────────────────────

    def advance_spinner(self):
        el = self._refs.get("spinner")
        if el is None:
            return
        self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_frames)
        el.set_text(self._spinner_frames[self._spinner_idx])

    def start_spinner_timer(self):
        if self._refs.get("spinner_timer"):
            return
        self._refs["spinner_timer"] = ui.timer(0.17, self.advance_spinner)

    def stop_spinner_timer(self):
        t = self._refs.pop("spinner_timer", None)
        if t:
            try:
                t.cancel()
            except Exception:
                pass

    def update_status_label(self, overview: Dict):
        el = self._refs.get("status_label")
        if el is None:
            return
        done = overview.get("completed", 0) + overview.get("failed", 0)
        total = max(overview.get("total", 0), len(self.panel.ui_mgr.selected_jobs))
        el.set_text(f"{done}/{total}")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _sb_sep(self):
        ui.element("div").style(f"height: 1px; background: {SB_SEP}; width: 24px; margin: 3px auto;")

    def _info_popup_btn(self, icon_name: str, title: str, rows: list):
        btn = (
            ui.button(icon=icon_name)
            .props("flat dense")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"color: {SB_MUTE}; background: transparent; min-width: 0;"
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