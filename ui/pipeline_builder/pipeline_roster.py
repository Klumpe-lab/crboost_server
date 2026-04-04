import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING
from nicegui import ui
from services.models_base import JobStatus
from services.project_state import JobType, get_project_state

from ui.styles import MONO, SANS as FONT
from ui.status_indicator import BoundStatusDot
from ui.ui_state import get_job_display_name, get_instance_display_name, instance_id_to_job_type
from ui.pipeline_builder.pipeline_constants import (
    PHASE_JOBS,
    PHASE_META,
    ROSTER_ANCHOR,
    SB_MUTE,
    SB_ACT,
    SB_ABG,
    SB_SEP,
    missing_deps,
    fmt,
)

if TYPE_CHECKING:
    from ui.pipeline_builder.pipeline_builder_panel import PipelineBuilderPanel

logger = logging.getLogger(__name__)

_GEAR_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
    '<circle cx="12" cy="12" r="3"/>'
    '<path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06'
    'a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09'
    'A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83'
    'l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09'
    'A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83'
    'l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09'
    'a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83'
    'l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09'
    'a1.65 1.65 0 0 0-1.51 1z"/>'
    '</svg>'
)

_TOMO_PREVIEW_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
    '<circle cx="12" cy="12" r="8"/>'
    '<line x1="4.5" y1="9" x2="19.5" y2="9"/>'
    '<line x1="4" y1="12" x2="20" y2="12"/>'
    '<line x1="4.5" y1="15" x2="19.5" y2="15"/>'
    '</svg>'
)
_SB_INFO = "#c0cad4"
_AVATAR_PALETTE = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b", "#ec4899"]


def _avatar_color(name: str) -> str:
    return _AVATAR_PALETTE[hash(name) % len(_AVATAR_PALETTE)]


def _inject_svg_color(svg: str, color: str) -> str:
    """Replace currentColor AND inject explicit fill/stroke so Quasar button doesn't swallow it."""
    svg = svg.replace("currentColor", color)
    # If the SVG has no explicit fill or stroke referencing the color yet,
    # stamp a style onto the root element as a fallback.
    if 'style="' in svg:
        svg = svg.replace('style="', f'style="fill:{color};stroke:{color};', 1)
    else:
        svg = svg.replace("<svg", f'<svg style="fill:{color};stroke:{color};"', 1)
    return svg


class RosterWidget:
    def __init__(self, panel: "PipelineBuilderPanel"):
        self.panel = panel
        self._flash_phase: Optional[str] = None
        self._roster_visible: bool = False
        self._roster_phase: Optional[str] = None
        self._refs: Dict = {}
        self._spinner_frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self._spinner_idx: int = 0
        self._live_spinners: List = []

    def _status_widget(self, instance_id: str):
        from services.project_state import get_project_state
        from ui.status_indicator import _dot_html

        job_model = get_project_state().jobs.get(instance_id)
        if not job_model:
            BoundStatusDot(instance_id)
            return

        def _content(status, jm=job_model):
            if status == JobStatus.RUNNING:
                frame = self._spinner_frames[self._spinner_idx]
                return (
                    f'<span class="cb-row-spinner" '
                    f"style=\"font-family:'IBM Plex Mono',monospace;font-size:13px;"
                    f'color:#3b82f6;line-height:1;flex-shrink:0;">{frame}</span>'
                )
            return _dot_html(status, is_orphaned=jm.is_orphaned)

        ui.html("", sanitize=False, tag="span").bind_content_from(job_model, "execution_status", backward=_content)

    # ── Roster ────────────────────────────────────────────────────────────────

    def refresh(self):
        panel = self.panel
        if panel.roster_panel is None:
            return
        panel.roster_panel.clear()

        with panel.roster_panel:
            for phase_id, jobs in PHASE_JOBS.items():
                icon_or_svg, phase_label, _ = PHASE_META[phase_id]
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
                    if icon_or_svg.startswith("<svg"):
                        ui.html(self._load_svg(icon_or_svg).replace("currentColor", "#94a3b8"), sanitize=False).style(
                            "width: 12px; height: 12px; flex-shrink: 0; display: flex;"
                        )
                    else:
                        ui.icon(icon_or_svg, size="12px").style("color: #94a3b8; flex-shrink: 0;")

                    ui.label(phase_label.upper()).style(
                        "font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.07em; line-height: 1;"
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
                            ui.icon("check_box_outline_blank", size="13px").style("color: #d1d5db; flex-shrink: 0;")
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
                                ui.icon("warning", size="11px").style("color: #f59e0b; flex-shrink: 0;").tooltip(
                                    "Missing: " + ", ".join(get_job_display_name(d) for d in missing)
                                )
                            (
                                ui.button(icon="add", on_click=lambda j=job_type: panel.prompt_species_and_add(j))
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
                                        self._status_widget(instance_id)
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
                project_path, job_model.relion_job_name, job_resolver=panel.backend.pipeline_orchestrator.job_resolver
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
                        result = await panel.backend.delete_job(
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
                            panel.remove_instance_from_pipeline(instance_id)
                        else:
                            ui.notify(f"Delete failed: {result.get('error')}", type="negative", timeout=8000)
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
            container = self._refs.get(f"phase_btn_{phase_id}")
            if container is None:
                continue
            active = self._roster_visible and self._roster_phase == phase_id
            bg = SB_ABG if active else "transparent"
            color = SB_ACT if active else SB_MUTE

            container.style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {bg}; "
                f"display: flex; align-items: center; justify-content: center; "
                f"cursor: pointer; flex-shrink: 0;"
            )
            svg_source = PHASE_META[phase_id][0]
            svg = self._load_svg(svg_source).replace("currentColor", color)
            container.clear()
            with container:
                ui.html(svg, sanitize=False).style("width: 18px; height: 18px; display: flex; pointer-events: none;")

    # ── Sidebar ───────────────────────────────────────────────────────────────




    def build_sidebar(self):
        panel = self.panel
        if panel.primary_sidebar is None:
            return

        state = get_project_state()

        with panel.primary_sidebar:
            ui.element("div").style("height: 8px;")

            self._build_project_avatar(state)
            ui.element("div").style("height: 2px;")
            self._build_metadata_btn(state)

            ui.element("div").style("height: 4px;")
            self._sb_sep()
            ui.element("div").style("height: 4px;")

            for phase_id in PHASE_JOBS:
                svg_source, label, sub = PHASE_META[phase_id]
                self._sb_svg_btn(
                    svg_source, f"{label} — {sub}", lambda p=phase_id: self.toggle(p), ref_key=f"phase_btn_{phase_id}"
                )

            if panel.toggle_workbench is not None:
                ui.element("div").style("height: 1px;")
                wb_btn = self._sb_svg_btn("vial.svg", "Template Workbench", panel.toggle_workbench, ref_key="wb_btn")
                panel.callbacks["wb_btn"] = wb_btn

            ui.element("div").style("height: 1px;")
            self._sb_svg_btn(
                _TOMO_PREVIEW_SVG,
                "Tomogram Previews",
                self._open_tomo_previews,
                ref_key="preview_btn",
            )

            ui.element("div").style("height: 10px;")
            self._sb_sep()
            ui.element("div").style("height: 6px;")

            run_slot = ui.element("div").style(
                "width: 100%; display: flex; flex-direction: column; align-items: center; padding: 2px 6px; gap: 3px;"
            )
            self._refs["run_slot"] = run_slot

            self._sb_svg_btn("cross.svg", "Close project", lambda: ui.navigate.to("/"))

            ui.element("div").style("flex: 1;")
            ui.element("div").style("height: 6px;")

        self.rebuild_run_slot()

    def _build_project_avatar(self, state):
        name = state.project_name or "---"
        initials = name[:3].upper()
        color = _avatar_color(name)

        avatar = (
            ui.element("div")
            .style(
                f"width: 34px; height: 34px; border-radius: 50%; "
                f"background: {color}1a; border: 1.5px solid {color}66; "
                f"display: flex; align-items: center; justify-content: center; "
                f"cursor: pointer; flex-shrink: 0;"
            )
            .on("click", self._open_project_hub)
            .tooltip(name)
        )
        with avatar:
            ui.label(initials).style(
                f"font-size: 9px; font-weight: 700; color: {color}; "
                "letter-spacing: 0.04em; line-height: 1; pointer-events: none;"
            )
        return avatar

    def _build_metadata_btn(self, state):
        """
        Dense gear button that opens an anchored popup with project + acquisition params.
        Uses a plain div (not ui.button) so Quasar doesn't clobber SVG color,
        but still hosts a ui.menu via a zero-size anchor button trick.
        """
        GEAR_COLOR = "#475569"  # slate-600 -- clearly visible against #f8fafc sidebar

        # Outer clickable div matches the style of every other sidebar button
        outer = (
            ui.element("div")
            .style(
                "width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                "background: transparent; display: flex; align-items: center; "
                "justify-content: center; cursor: pointer; flex-shrink: 0; position: relative;"
            )
            .tooltip("Project parameters")
        )

        with outer:


            svg = _GEAR_SVG.replace("currentColor", GEAR_COLOR)
            ui.html(svg, sanitize=False).style("width: 17px; height: 17px; display: flex; pointer-events: none;")

            # Zero-size invisible button that owns the menu anchor --
            # positioned absolutely so it doesn't affect layout
            anchor_btn = (
                ui.button()
                .props("flat dense")
                .style(
                    "position: absolute; width: 0; height: 0; min-width: 0; "
                    "padding: 0; opacity: 0; pointer-events: none;"
                )
            )
            with anchor_btn:
                with (
                    ui.menu()
                    .props('anchor="center right" self="center left" :offset="[8,0]"')
                    .style(
                        "background: #ffffff; border: 1px solid #e2e8f0; "
                        "border-radius: 5px; overflow: hidden; min-width: 210px; "
                        "padding: 0; box-shadow: 0 4px 12px rgba(0,0,0,0.08);"
                    )
                ) as menu:
                    for section_title, rows in [
                        (
                            "Project",
                            [
                                ("Name", state.project_name),
                                ("Root", str(state.project_path) if state.project_path else "---"),
                                ("Movies", state.movies_glob or "---"),
                                ("MDOC", state.mdocs_glob or "---"),
                            ],
                        ),
                        (
                            "Acquisition",
                            [
                                ("Pixel", f"{fmt(state.microscope.pixel_size_angstrom)} Å"),
                                ("Voltage", f"{fmt(state.microscope.acceleration_voltage_kv)} kV"),
                                ("Cs", f"{fmt(state.microscope.spherical_aberration_mm)} mm"),
                                ("Amp. C.", fmt(state.microscope.amplitude_contrast)),
                                ("Dose", f"{fmt(state.acquisition.dose_per_tilt)} e⁻/Å²"),
                                ("Tilt ax.", f"{fmt(state.acquisition.tilt_axis_degrees)} °"),
                            ],
                        ),
                    ]:
                        with ui.element("div").style(
                            "padding: 7px 11px 5px; font-size: 9px; font-weight: 700; "
                            "color: #94a3b8; letter-spacing: 0.09em; text-transform: uppercase; "
                            "border-bottom: 1px solid #f1f5f9;"
                        ):
                            ui.label(section_title)
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

            # clicking the outer div opens the menu via JS
            outer.on("click", lambda: menu.open())

        return outer

    async def _open_project_hub(self):
        from nicegui import app as ng_app
        from services.project_state import get_project_state_for
        from services.configs.user_prefs_service import get_prefs_service

        panel = self.panel

        prefs_service = get_prefs_service()
        prefs = prefs_service.load_from_app_storage(ng_app.storage.user)
        base_path = prefs.project_base_path or await panel.backend.get_default_project_base()

        # Mutable scan state -- updated when user picks a different location
        scan_state = {"base": base_path, "list": await panel.backend.scan_for_projects(base_path)}

        current_path_str = str(panel.ui_mgr.project_path.resolve()) if panel.ui_mgr.project_path else None

        dialog_ref: Dict = {}
        list_ref: Dict = {}
        history_refs: Dict = {"container": None, "visible": False, "dropdown": None}

        # ── switch handler ────────────────────────────────────────────────────

        def _make_switch_handler(path_str: str):
            async def handler():
                d = dialog_ref.get("dialog")
                if d:
                    d.close()
                await panel.backend.load_existing_project(path_str)
                p = Path(path_str)
                loaded_state = get_project_state_for(p)
                panel.ui_mgr.load_from_project(
                    project_path=p, scheme_name="loaded", jobs=list(loaded_state.jobs.keys())
                )
                ui.navigate.to("/workspace")

            return handler

        # ── project list renderer ─────────────────────────────────────────────

        def _render_list(projects):
            c = list_ref.get("el")
            if c is None:
                return
            c.clear()
            other = [p for p in projects if p["path"] != current_path_str]
            with c:
                if not other:
                    with ui.element("div").style("padding: 16px 14px;"):
                        ui.label("No other projects found.").style(
                            f"{FONT} font-size: 10px; color: #94a3b8; font-style: italic;"
                        )
                    return
                for proj in other:
                    is_running = panel.backend.pipeline_runner.is_active(Path(proj["path"])) or proj.get(
                        "pipeline_active", False
                    )
                    proj_name = proj["name"]
                    proj_color = _avatar_color(proj_name)
                    creator = proj.get("creator") or ""
                    date_str = (proj.get("modified") or "")[:10]

                    with (
                        ui.element("div")
                        .style(
                            "display: flex; align-items: center; gap: 10px; "
                            "padding: 8px 14px; border-bottom: 1px solid #f3f4f6; "
                            "cursor: pointer;"
                        )
                        .classes("hover:bg-slate-50")
                        .on("click", _make_switch_handler(proj["path"]))
                    ):
                        # Mini avatar
                        with ui.element("div").style(
                            f"width: 26px; height: 26px; border-radius: 50%; flex-shrink: 0; "
                            f"background: {proj_color}1a; border: 1px solid {proj_color}55; "
                            f"display: flex; align-items: center; justify-content: center;"
                        ):
                            ui.label(proj_name[:3].upper()).style(
                                f"font-size: 7px; font-weight: 700; color: {proj_color}; "
                                "letter-spacing: 0.03em; line-height: 1;"
                            )

                        # Name + meta
                        with ui.element("div").style(
                            "display: flex; flex-direction: column; flex: 1; min-width: 0; gap: 2px;"
                        ):
                            with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                                ui.label(proj_name).style(
                                    f"{FONT} font-size: 11px; font-weight: 500; color: #1e293b; "
                                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                                )
                                if is_running:
                                    ui.label("running").style(
                                        f"{FONT} font-size: 8px; color: #3b82f6; font-weight: 700; "
                                        "background: #eff6ff; border: 1px solid #bfdbfe; "
                                        "border-radius: 3px; padding: 0 5px; flex-shrink: 0;"
                                    )
                            with ui.element("div").style("display: flex; gap: 8px; align-items: center;"):
                                if creator:
                                    ui.label(creator).style(f"{MONO} font-size: 9px; color: #64748b;")
                                if date_str:
                                    ui.label(date_str).style(f"{MONO} font-size: 9px; color: #cbd5e1;")

        # ── rescan ────────────────────────────────────────────────────────────

        async def _rescan(path_input_el):
            new_base = path_input_el.value.strip()
            if not new_base:
                return
            scan_state["base"] = new_base
            scan_state["list"] = await panel.backend.scan_for_projects(new_base)
            _render_list(scan_state["list"])
            # Add to recent roots if valid
            projects = scan_state["list"]
            if projects:
                prefs_service.prefs.add_recent_root(new_base)
                prefs_service.save_to_app_storage(ng_app.storage.user)
                _render_history()

        # ── history helpers ───────────────────────────────────────────────────

        def _render_history():
            c = history_refs.get("container")
            if c is None:
                return
            c.clear()
            roots = prefs_service.prefs.recent_project_roots
            with c:
                if not roots:
                    ui.label("No saved locations").style(
                        f"{FONT} font-size: 10px; color: #cbd5e1; font-style: italic; padding: 8px 12px;"
                    )
                else:
                    for root in roots[:12]:
                        with (
                            ui.element("div")
                            .style("display: flex; align-items: center; gap: 4px; padding: 5px 10px; cursor: pointer;")
                            .classes("hover:bg-slate-50")
                        ):
                            ui.label(root.path).style(
                                f"{MONO} font-size: 10px; color: #475569; flex: 1; "
                                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                            ).on("click", lambda p=root.path: _use_history(p))
                            (
                                ui.button(icon="close", on_click=lambda p=root.path: _remove_history(p))
                                .props("flat dense round size=xs")
                                .style("color: #cbd5e1; flex-shrink: 0;")
                            )
                with ui.element("div").style(
                    "display: flex; justify-content: flex-end; padding: 4px 10px; border-top: 1px solid #f1f5f9;"
                ):
                    ui.button("Clear all", on_click=_clear_history).props("flat dense no-caps").style(
                        f"{FONT} font-size: 10px; color: #94a3b8;"
                    )

        def _toggle_history():
            dd = history_refs.get("dropdown")
            if dd is None:
                return
            if history_refs["visible"]:
                dd.style("display: none;")
                history_refs["visible"] = False
            else:
                _render_history()
                dd.style("display: block;")
                history_refs["visible"] = True

        def _close_history():
            dd = history_refs.get("dropdown")
            if dd:
                dd.style("display: none;")
            history_refs["visible"] = False

        def _use_history(path: str):
            _close_history()
            path_input_ref = history_refs.get("path_input")
            if path_input_ref:
                path_input_ref.value = path
            asyncio.create_task(_rescan_from_path(path))

        async def _rescan_from_path(path: str):
            scan_state["base"] = path
            scan_state["list"] = await panel.backend.scan_for_projects(path)
            _render_list(scan_state["list"])

        def _remove_history(path: str):
            prefs_service.prefs.remove_recent_root(path)
            prefs_service.save_to_app_storage(ng_app.storage.user)
            _render_history()

        def _clear_history():
            prefs_service.prefs.clear_recent_roots()
            prefs_service.save_to_app_storage(ng_app.storage.user)
            _render_history()

        # ── dialog ────────────────────────────────────────────────────────────

        with (
            ui.dialog() as dialog,
            ui.card().style(
                "width: 560px; max-width: 560px; padding: 0; overflow: hidden; "
                "border-radius: 6px; box-shadow: 0 8px 24px rgba(0,0,0,0.12);"
            ),
        ):
            dialog_ref["dialog"] = dialog

            # Base path bar with history dropdown
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 6px; "
                "padding: 7px 10px; border-bottom: 1px solid #e5e7eb; "
                "background: #f8fafc; position: relative;"
            ):
                ui.label("BASE").style(
                    f"{FONT} font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.09em; flex-shrink: 0;"
                )

                # Wrapper for input + history dropdown
                with ui.element("div").style("flex: 1; position: relative; min-width: 0;"):
                    with ui.element("div").style("display: flex; align-items: center; gap: 4px;"):
                        path_input = (
                            ui.input(value=scan_state["base"])
                            .props("dense borderless")
                            .style(f"flex: 1; font-size: 10px; {MONO} color: #1e293b; background: transparent;")
                        )
                        history_refs["path_input"] = path_input
                        (
                            ui.button(icon="expand_more", on_click=_toggle_history)
                            .props("flat dense round size=xs")
                            .style("color: #94a3b8; flex-shrink: 0;")
                            .tooltip("Recent locations")
                        )

                    # History dropdown
                    history_dropdown = ui.element("div").style(
                        "display: none; position: absolute; top: calc(100% + 4px); left: 0; right: 0; "
                        "z-index: 9999; background: white; "
                        "border: 1px solid #e2e8f0; border-radius: 5px; "
                        "box-shadow: 0 4px 16px rgba(15,23,42,0.10); "
                        "max-height: 200px; overflow-y: auto;"
                    )
                    history_refs["dropdown"] = history_dropdown
                    with history_dropdown:
                        history_refs["container"] = ui.element("div").style("width: 100%;")

                (
                    ui.button(icon="refresh", on_click=lambda: asyncio.create_task(_rescan(path_input)))
                    .props("flat dense round size=xs")
                    .style("color: #64748b; flex-shrink: 0;")
                    .tooltip("Rescan")
                )

            # Section header
            with ui.element("div").style(
                "padding: 7px 14px 5px; font-size: 9px; font-weight: 700; "
                "color: #94a3b8; letter-spacing: 0.09em; text-transform: uppercase; "
                "border-bottom: 1px solid #f1f5f9;"
            ):
                ui.label("Projects")

            # Scrollable project list -- taller so you can see more at once
            with ui.scroll_area().style("height: 420px; width: 100%;"):
                list_container = ui.element("div").style("width: 100%;")
                list_ref["el"] = list_container

            ui.element("div").style("height: 4px;")

        _render_list(scan_state["list"])
        dialog.open()

    # ── Run slot ──────────────────────────────────────────────────────────────

    def rebuild_run_slot(self):
        run_slot = self._refs.get("run_slot")
        if run_slot is None:
            return
        run_slot.clear()
        panel = self.panel
        with run_slot:
            if panel.ui_mgr.is_running:
                stop_div = (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #fef2f2; border: 1px solid #fecaca; color: #b91c1c; "
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    .on("click", panel.handle_stop_pipeline)
                    .tooltip("Stop pipeline")
                )
                with stop_div:
                    ui.html(self._load_svg("stop.svg"), sanitize=False).style(
                        "width: 16px; height: 16px; display: flex; pointer-events: none;"
                    )

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
                play_div = (
                    ui.element("div")
                    .style(
                        "width: 30px; height: 30px; border-radius: 50%; cursor: pointer; "
                        "background: #f0fdf4; border: 1px solid #bbf7d0; color: #15803d; "
                        "display: flex; align-items: center; justify-content: center; flex-shrink: 0;"
                    )
                    .on("click", panel.handle_run_pipeline)
                    .tooltip("Run pipeline")
                )
                with play_div:
                    ui.html(self._load_svg("play.svg"), sanitize=False).style(
                        "width: 16px; height: 16px; display: flex; pointer-events: none;"
                    )

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
                        display_text = f"{base_name} #{suffix}" if suffix.isdigit() else f"{base_name} ({suffix})"
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
                        self._status_widget(instance_id)

    # ── Spinner / status label ────────────────────────────────────────────────

    def advance_spinner(self):
        self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_frames)
        frame = self._spinner_frames[self._spinner_idx]

        el = self._refs.get("spinner")
        if el:
            el.set_text(frame)

        ui.run_javascript(f"document.querySelectorAll('.cb-row-spinner').forEach(e => e.textContent = {repr(frame)});")

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

    def _load_svg(self, name: str) -> str:
        if name.startswith("<svg"):
            return name
        p = Path("static/icons") / name
        try:
            return p.read_text()
        except FileNotFoundError:
            return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"/>'

    def _sb_svg_btn(self, svg_name, tooltip, on_click, active=False, ref_key=None, color_override=None):
        bg = SB_ABG if active else "transparent"
        color = color_override or (SB_ACT if active else SB_MUTE)

        svg = self._load_svg(svg_name).replace("currentColor", color)

        container = (
            ui.element("div")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"background: {bg}; "
                f"display: flex; align-items: center; justify-content: center; "
                f"cursor: pointer; flex-shrink: 0;"
            )
            .on("click", on_click)
            .tooltip(tooltip)
        )
        with container:
            ui.html(svg, sanitize=False).style("width: 18px; height: 18px; display: flex; pointer-events: none;")
        if ref_key:
            self._refs[ref_key] = container
        return container

    def _info_popup_btn(self, icon_name: str, title: str, rows: list, icon_color: str = None):
        color = icon_color or SB_MUTE
        btn = (
            ui.button(icon=icon_name)
            .props("flat dense")
            .style(
                f"width: 30px; height: 30px; border-radius: 4px; margin: 1px 0; "
                f"color: {color}; background: transparent; min-width: 0;"
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

    # In RosterWidget -- add these two methods alongside the other helpers at the bottom

    def _find_reconstruct_pngs(self) -> list:
        """Return sorted PNG paths from the most recent succeeded TS_RECONSTRUCT job."""
        project_path = self.panel.ui_mgr.project_path
        if not project_path:
            return []

        state = get_project_state()
        succeeded = []
        for iid, job_model in state.jobs.items():
            try:
                if instance_id_to_job_type(iid) != JobType.TS_RECONSTRUCT:
                    continue
            except ValueError:
                continue
            if job_model.execution_status != JobStatus.SUCCEEDED:
                continue
            relion_name = getattr(job_model, "relion_job_name", None)
            if relion_name:
                succeeded.append(relion_name.rstrip("/"))

        if not succeeded:
            return []

        succeeded.sort()  # lexicographic sort puts job005 after job004
        recon_dir = project_path / succeeded[-1] / "warp_tiltseries" / "reconstruction"
        if not recon_dir.exists():
            return []

        return sorted(recon_dir.glob("*.png"))


    async def _open_tomo_previews(self):
        import base64
        import shlex

        pngs = self._find_reconstruct_pngs()

        items = []
        for png_path in pngs:
            try:
                data = base64.b64encode(png_path.read_bytes()).decode()
                recon_dir = png_path.parent
                f32_path = recon_dir / f"{png_path.stem}_f32.mrc"
                mrc_path = recon_dir / f"{png_path.stem}.mrc"
                resolved = f32_path if f32_path.exists() else mrc_path
                items.append({
                    "stem": png_path.stem,
                    "src": f"data:image/png;base64,{data}",
                    "mrc": str(resolved),
                })
            except Exception as e:
                logger.info("Could not read %s: %s", png_path, e)

        def _copy_cmd(cmd: str):
            ui.clipboard.write(cmd)
            ui.notify("Copied to clipboard", timeout=1500)

        refs = {}

        with ui.dialog() as dialog, ui.card().style(
            "width: 90vw; max-width: 1400px; height: 85vh; max-height: 85vh; padding: 0; "
            "overflow: hidden; border-radius: 6px; box-shadow: 0 8px 24px rgba(0,0,0,0.12); "
            "display: flex; flex-direction: column;"
        ):
            with ui.element("div").style(
                "display: flex; align-items: center; gap: 10px; flex-shrink: 0; "
                "padding: 10px 14px; border-bottom: 1px solid #e5e7eb; background: #f8fafc;"
            ):
                ui.label("Tomogram Previews").style(
                    f"{FONT} font-size: 12px; font-weight: 600; color: #1e293b; flex-shrink: 0;"
                )
                if items:
                    filter_input = (
                        ui.input(placeholder="filter by name...")
                        .props("dense borderless clearable")
                        .style(
                            f"{MONO} font-size: 10px; color: #1e293b; "
                            "background: white; border: 1px solid #e2e8f0; border-radius: 4px; "
                            "padding: 0 8px; width: 260px;"
                        )
                    )
                    refs["filter"] = filter_input
                ui.element("div").style("flex: 1;")
                (
                    ui.button(icon="close", on_click=dialog.close)
                    .props("flat dense round size=xs")
                    .style("color: #94a3b8;")
                )

            if not items:
                with ui.element("div").style(
                    "flex: 1; display: flex; align-items: center; justify-content: center;"
                ):
                    ui.label("No previews available -- run Reconstruct first.").style(
                        f"{FONT} font-size: 11px; color: #94a3b8; font-style: italic;"
                    )
            else:
                with ui.scroll_area().style(
                    "flex: 1; min-height: 0; width: 100%; height: calc(85vh - 52px);"
                ):
                    grid_container = ui.element("div").style("width: 100%;")
                    refs["grid"] = grid_container

                def render_grid(ft: str = ""):
                    c = refs.get("grid")
                    if c is None:
                        return
                    c.clear()
                    visible = [it for it in items if ft.lower() in it["stem"].lower()]
                    with c:
                        if not visible:
                            with ui.element("div").style("padding: 32px; text-align: center;"):
                                ui.label("No tomograms match the filter.").style(
                                    f"{FONT} font-size: 11px; color: #94a3b8; font-style: italic;"
                                )
                            return
                        with ui.element("div").style(
                            "display: grid; grid-template-columns: repeat(4, 1fr); "
                            "gap: 16px; padding: 16px;"
                        ):
                            for item in visible:
                                cmd = f"3dmod {shlex.quote(item['mrc'])}"
                                with ui.element("div").style(
                                    "display: flex; flex-direction: column; "
                                    "border-radius: 4px; overflow: hidden; "
                                    "border: 1px solid #1e293b; background: #0f172a;"
                                ):
                                    ui.image(item["src"]).style("width: 100%; display: block;")
                                    with ui.element("div").style(
                                        "display: flex; align-items: center; gap: 4px; "
                                        "padding: 5px 8px; background: #1e293b;"
                                    ):
                                        ui.label(item["stem"]).style(
                                            f"{MONO} font-size: 9px; color: #94a3b8; flex: 1; "
                                            "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                                        )
                                        (
                                            ui.button(
                                                icon="content_copy",
                                                on_click=lambda c=cmd: _copy_cmd(c),
                                            )
                                            .props("flat dense round size=xs")
                                            .style("color: #475569; flex-shrink: 0;")
                                            .tooltip(cmd)
                                        )

                render_grid()

                fi = refs.get("filter")
                if fi:
                    fi.on_value_change(lambda e: render_grid(e.value or ""))

        dialog.open()