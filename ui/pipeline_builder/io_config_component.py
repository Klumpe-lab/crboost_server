# ui/pipeline_builder/io_config_component.py
"""
Interactive I/O Configuration component for job tabs.
Allows users to inspect and override input sources, view outputs, and validate paths.
"""

import asyncio
from pathlib import Path
from typing import Callable, Dict, List, Optional

from nicegui import ui

from services.io_slots import InputSlot, OutputSlot, JobFileType
from services.models_base import JobType, JobStatus
from services.path_resolution_service import PathResolutionService, OutputCandidate, InputSlotValidation
from services.project_state import get_project_state, get_state_service
from ui.ui_state import get_job_display_name


def snake_to_title(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split("_"))


STATUS_STYLE = {
    JobStatus.SUCCEEDED: {"color": "#10b981", "bg": "#ecfdf5", "label": "done", "icon": "check_circle"},
    JobStatus.RUNNING  : {"color": "#3b82f6", "bg": "#eff6ff", "label": "running", "icon": "sync"},
    JobStatus.SCHEDULED: {"color": "#f59e0b", "bg": "#fffbeb", "label": "scheduled", "icon": "schedule"},
    JobStatus.FAILED   : {"color": "#ef4444", "bg": "#fef2f2", "label": "failed", "icon": "error"},
    JobStatus.UNKNOWN  : {"color": "#6b7280", "bg": "#f9fafb", "label": "?", "icon": "help"},
}


class IOConfigComponent:
    def __init__(self, job_type: JobType, on_change: Optional[Callable[[], None]] = None):
        self.job_type = job_type
        self.on_change = on_change
        self._validation_cache: Dict[str, InputSlotValidation] = {}
        self._slot_containers: Dict[str, ui.element] = {}

    def render(self):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)

        if not job_model:
            ui.label("Job not initialized").classes("text-red-500 italic")
            return

        resolver = PathResolutionService(state)
        input_schema = resolver.get_input_schema_for_job(self.job_type)
        output_schema = resolver.get_output_schema_for_job(self.job_type)

        with ui.column().classes("w-full gap-4"):
            if input_schema:
                self._render_input_slots(resolver, input_schema, job_model)
            else:
                with ui.row().classes("items-center gap-2 text-gray-400 italic"):
                    ui.icon("input", size="16px")
                    ui.label("This job has no input dependencies")

            if output_schema:
                self._render_output_slots(output_schema, job_model)

    def _render_input_slots(self, resolver, schema, job_model):
        ui.label("Input Slots").classes("text-xs font-black text-gray-400 uppercase tracking-wide")

        with ui.column().classes("w-full gap-3"):
            for slot in schema:
                container = ui.column().classes("w-full")
                self._slot_containers[slot.key] = container
                with container:
                    self._render_input_slot_content(resolver, slot, job_model)

    def _render_input_slot_content(self, resolver, slot, job_model):
        overrides = getattr(job_model, "source_overrides", {}) or {}
        current_override = overrides.get(slot.key)

        candidates = resolver.get_candidates_for_slot(self.job_type, slot.key)
        validation = resolver.validate_input_slot(self.job_type, job_model, slot.key, check_filesystem=True)

        self._validation_cache[slot.key] = validation

        if current_override and current_override.startswith("manual:"):
            current_value = "manual"
            manual_path = current_override[7:]
        elif validation.source_key:
            current_value = validation.source_key
            manual_path = ""
        else:
            current_value = None
            manual_path = ""

        options = self._build_dropdown_options(slot, candidates)

        # Card border color based on state
        if validation.awaiting_upstream:
            border_class = "border-blue-200 bg-blue-50/30"
        elif validation.is_valid:
            border_class = "border-green-200 bg-green-50/30"
        else:
            border_class = "border-red-200 bg-red-50/30"

        with ui.card().classes(f"w-full p-3 border shadow-none {border_class}"):
            with ui.row().classes("w-full items-start justify-between gap-4"):
                with ui.column().classes("flex-1 gap-2"):
                    # Header
                    with ui.row().classes("items-center gap-2"):
                        ui.label(snake_to_title(slot.key)).classes("text-sm font-semibold text-gray-700")

                        if not slot.required:
                            ui.label("optional").classes("text-[10px] px-1.5 py-0.5 bg-gray-100 text-gray-500 rounded")

                        if validation.is_user_override:
                            ui.label("overridden").classes(
                                "text-[10px] px-1.5 py-0.5 bg-blue-100 text-blue-600 rounded"
                            )

                    accepts_str = ", ".join(t.value for t in slot.accepts)
                    ui.label(f"Accepts: {accepts_str}").classes("text-[10px] text-gray-400 font-mono")

                    # Dropdown
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.select(
                            options=options,
                            value=current_value,
                            label="Source",
                            on_change=lambda e, s=slot: self._handle_source_change(s, e.value),
                        ).props("outlined dense options-dense").classes("flex-1")

                        if validation.is_user_override:
                            ui.button(icon="restart_alt", on_click=lambda s=slot: self._clear_override(s)).props(
                                "flat dense round size=sm"
                            ).classes("text-gray-400").tooltip("Reset to auto")

                    # Manual path input
                    if current_value == "manual":
                        with ui.row().classes("w-full items-center gap-2 mt-2"):
                            ui.input(
                                value=manual_path,
                                placeholder="/path/to/file.star",
                                on_change=lambda e, s=slot: self._handle_manual_path_change(s, e.value),
                            ).props("outlined dense").classes("flex-1 font-mono text-xs")

                            ui.button(icon="folder_open", on_click=lambda s=slot: self._open_file_picker(s)).props(
                                "flat dense"
                            ).classes("text-gray-500").tooltip("Browse...")

                # Right: Validation status
                with ui.column().classes("items-end gap-1 min-w-[200px]"):
                    self._render_validation_status(validation)

                    if validation.resolved_path:
                        path_display = self._truncate_path(validation.resolved_path, 50)
                        ui.label(path_display).classes(
                            "text-[10px] font-mono text-gray-500 text-right break-all"
                        ).tooltip(validation.resolved_path)

    def _build_dropdown_options(self, slot: InputSlot, candidates: List[OutputCandidate]) -> Dict[str, str]:
        """Build dropdown options with clean formatting."""
        options = {}

        for c in candidates:
            is_pending = "pending_" in c.instance_path
            is_preferred = slot.preferred_source == c.producer_job_type.value
            style = STATUS_STYLE.get(c.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])

            # Build label parts
            if is_pending:
                display_name = get_job_display_name(JobType(c.producer_job_type.value))
                location = "not yet deployed"
            else:
                display_name = get_job_display_name(JobType(c.producer_job_type.value))
                location = c.instance_path

            # Clean label: "Motion & CTF - External/job002 [running]"
            # or for pending: "Motion & CTF - not yet deployed [scheduled]"
            parts = [display_name]
            if location:
                parts.append(f"- {location}")
            parts.append(f"[{style['label']}]")
            if is_preferred:
                parts.append("*")

            label = " ".join(parts)
            options[c.source_key] = label

        options["manual"] = "Manual path..."

        return options

    def _render_validation_status(self, validation: InputSlotValidation):
        if validation.awaiting_upstream:
            # Source job is still running or scheduled - this is expected
            with ui.row().classes("items-center gap-1"):
                ui.icon("hourglass_empty", size="16px").classes("text-blue-500")
                ui.label("Awaiting").classes("text-xs text-blue-600 font-medium")
            ui.label("Upstream job in progress").classes("text-[10px] text-blue-400")
        elif validation.is_valid:
            if validation.file_exists:
                with ui.row().classes("items-center gap-1"):
                    ui.icon("check_circle", size="16px").classes("text-green-500")
                    ui.label("Valid").classes("text-xs text-green-600 font-medium")
            else:
                is_pending_path = validation.resolved_path and "pending_" in validation.resolved_path
                if is_pending_path:
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("schedule", size="16px").classes("text-blue-500")
                        ui.label("Ready").classes("text-xs text-blue-600 font-medium")
                    ui.label("Path resolved at run time").classes("text-[10px] text-gray-400")
                else:
                    with ui.row().classes("items-center gap-1"):
                        ui.icon("schedule", size="16px").classes("text-yellow-500")
                        ui.label("Pending").classes("text-xs text-yellow-600 font-medium")
                    ui.label("Will be created").classes("text-[10px] text-gray-400")
        else:
            with ui.row().classes("items-center gap-1"):
                ui.icon("error", size="16px").classes("text-red-500")
                ui.label("Invalid").classes("text-xs text-red-600 font-medium")

            if validation.error_message:
                ui.label(validation.error_message).classes("text-[10px] text-red-500")

    def _render_output_slots(self, schema, job_model):
        ui.label("Output Slots").classes("text-xs font-black text-gray-400 uppercase tracking-wide mt-4")

        with ui.card().classes("w-full p-0 border border-gray-200 shadow-none"):
            for i, slot in enumerate(schema):
                bg_class = "bg-gray-50" if i % 2 == 0 else "bg-white"

                with ui.row().classes(
                    f"w-full p-3 {bg_class} border-b border-gray-100 last:border-0 items-center justify-between gap-4"
                ):
                    with ui.column().classes("gap-0"):
                        ui.label(snake_to_title(slot.key)).classes("text-xs font-semibold text-gray-600")
                        ui.label(f"Produces: {slot.produces.value}").classes("text-[10px] text-gray-400 font-mono")

                    resolved = (job_model.paths or {}).get(slot.key)
                    if resolved:
                        path_display = self._truncate_path(resolved, 60)
                        ui.label(path_display).classes("text-xs font-mono text-gray-500").tooltip(resolved)
                    else:
                        ui.label(slot.path_template).classes("text-xs font-mono text-gray-400 italic")

    def _handle_source_change(self, slot, value):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)
        if not job_model:
            return

        if not hasattr(job_model, "source_overrides") or job_model.source_overrides is None:
            job_model.source_overrides = {}

        if value == "manual":
            job_model.source_overrides[slot.key] = "manual:"
        elif value:
            job_model.source_overrides[slot.key] = value
        else:
            job_model.source_overrides.pop(slot.key, None)

        asyncio.create_task(self._save_and_refresh(slot))

    def _handle_manual_path_change(self, slot, path):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)
        if not job_model:
            return

        if not hasattr(job_model, "source_overrides") or job_model.source_overrides is None:
            job_model.source_overrides = {}

        job_model.source_overrides[slot.key] = f"manual:{path}"
        asyncio.create_task(self._save_and_refresh(slot))

    def _clear_override(self, slot):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)
        if not job_model:
            return

        if hasattr(job_model, "source_overrides") and job_model.source_overrides:
            job_model.source_overrides.pop(slot.key, None)

        asyncio.create_task(self._save_and_refresh(slot))

    async def _save_and_refresh(self, slot):
        await get_state_service().save_project()

        container = self._slot_containers.get(slot.key)
        if container:
            state = get_project_state()
            job_model = state.jobs.get(self.job_type)
            resolver = PathResolutionService(state)

            container.clear()
            with container:
                self._render_input_slot_content(resolver, slot, job_model)

        if self.on_change:
            self.on_change()

    def _open_file_picker(self, slot):
        ui.notify("File picker not yet implemented", type="info")

    def _truncate_path(self, path: str, max_len: int) -> str:
        if len(path) <= max_len:
            return path
        return "..." + path[-(max_len - 3) :]


def render_io_config(job_type: JobType, on_change: Optional[Callable[[], None]] = None):
    component = IOConfigComponent(job_type, on_change)
    component.render()
