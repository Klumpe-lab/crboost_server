# ui/pipeline_builder/io_config_component.py
"""
Interactive I/O Configuration component for job tabs.
Allows users to inspect and override input sources, view outputs, and validate paths.
"""

import asyncio
import re
from pathlib import Path
from typing import Callable, Dict, List, Optional, Iterable

from nicegui import ui

from services.io_slots import InputSlot, JobFileType
from services.models_base import JobType, JobStatus
from services.path_resolution_service import PathResolutionService, OutputCandidate, InputSlotValidation
from services.project_state import get_project_state, get_state_service
from ui.ui_state import get_job_display_name


def snake_to_title(s: str) -> str:
    return " ".join(word.capitalize() for word in s.split("_"))


STATUS_STYLE = {
    JobStatus.SUCCEEDED: {"color": "#10b981", "label": "done"},
    JobStatus.RUNNING  : {"color": "#3b82f6", "label": "running"},
    JobStatus.SCHEDULED: {"color": "#f59e0b", "label": "scheduled"},
    JobStatus.FAILED   : {"color": "#ef4444", "label": "failed"},
    JobStatus.UNKNOWN  : {"color": "#6b7280", "label": "?"},
}


# ----------------------------
# Clipboard helper (FIXED)
# ----------------------------
def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _compact_template(s: str) -> str:
    """Replace {vars} with ellipsis but preserve full path shape."""
    return re.sub(r"{[^}]+}", "…", s or "")


def _make_copy_handler(text: str):
    async def handler():
        await _copy_to_clipboard(text)
    return handler



async def _copy_to_clipboard(text: str) -> None:
    """
    ui.clipboard.write is synchronous (returns None) -> DO NOT await it.
    Note: Clipboard APIs generally require HTTPS or localhost.
    """
    try:
        ui.clipboard.write(text)
        ui.notify("Copied", type="positive", timeout=900)
        return
    except Exception:
        # fallback (still requires secure context in most browsers)
        safe = text.replace("`", "\\`")
        try:
            await ui.run_javascript(f"navigator.clipboard.writeText(`{safe}`)", respond=False)
            ui.notify("Copied", type="positive", timeout=900)
        except Exception as e:
            ui.notify(f"Clipboard failed: {e}", type="negative", timeout=2500)


def _short_instance_label(instance_path: str) -> str:
    try:
        p = Path(instance_path.strip("/"))
        return p.name if p.name else instance_path
    except Exception:
        return instance_path


def _looks_like_filename(s: str) -> bool:
    s = s.lower()
    return any(
        s.endswith(ext)
        for ext in (
            ".star",
            ".mrc",
            ".mrcs",
            ".eer",
            ".tif",
            ".tiff",
            ".png",
            ".jpg",
            ".jpeg",
            ".txt",
            ".log",
            ".json",
            ".yaml",
            ".yml",
            ".mdoc",
            ".aln",
            ".tlt",
        )
    )


def _filetype_to_filename_guess(ft: JobFileType) -> str:
    v = str(ft.value)
    if _looks_like_filename(v):
        return v
    low = v.lower()
    if "star" in low:
        return f"{v}.star"
    if "mdoc" in low:
        return f"{v}.mdoc"
    if "eer" in low:
        return f"{v}.eer"
    if "mrcs" in low:
        return f"{v}.mrcs"
    if "mrc" in low:
        return f"{v}.mrc"
    return v


def _primary_accept_label(accepts: Iterable[JobFileType]) -> str:
    labels = [_filetype_to_filename_guess(a) for a in accepts]
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    return f"{labels[0]} (+{len(labels) - 1})"


def _is_pending_path(p: Optional[str]) -> bool:
    return bool(p) and "pending_" in p


class IOConfigComponent:
    def __init__(self, job_type: JobType, on_change: Optional[Callable[[], None]] = None,active_job_types: Optional[set] = None):
        self.job_type = job_type
        self.on_change = on_change

        self.active_job_types = active_job_types  # NEW
        self._validation_cache: Dict[str, InputSlotValidation] = {}
        self._slot_containers: Dict[str, ui.element] = {}

    def render(self):
        state = get_project_state()
        job_model = state.jobs.get(self.job_type)
        if not job_model:
            ui.label("Job not initialized").classes("text-red-500 italic")
            return

        resolver = PathResolutionService(state, active_job_types=self.active_job_types)  # CHANGED
        input_schema = resolver.get_input_schema_for_job(self.job_type)
        output_schema = resolver.get_output_schema_for_job(self.job_type)

        # tighter spacing overall
        with ui.column().classes("w-full gap-2"):
            if input_schema:
                self._render_input_slots(resolver, input_schema, job_model)

            if output_schema:
                self._render_output_slots(output_schema, job_model)

    def _get_job_dir(self, job_model) -> tuple[Optional[Path], bool]:
        """
        Returns (job_dir, is_predicted).
        - resolved: project_path + relion_job_name
        - otherwise: best-effort predicted folder (pending_* pattern)
        """
        state = get_project_state()
        project_path = getattr(state, "project_path", None)
        if not project_path:
            return None, True

        if getattr(job_model, "relion_job_name", None):
            # definitive
            return (project_path / job_model.relion_job_name.strip("/")).resolve(), False

        # best-effort guess using relion_job_number if it exists
        n = _safe_int(getattr(job_model, "relion_job_number", None))
        if n is not None:
            return (project_path / "External" / f"job{n:03d}").resolve(), True

        # fallback to your existing pending naming convention
        return (project_path / "External" / f"pending_{self.job_type.value}").resolve(), True

    # ----------------------------
    # Input Slots (compact)
    # ----------------------------

    def _render_input_slots(self, resolver, schema, job_model):
        # tiny section header
        ui.label("Input Slots").classes("text-[10px] font-black text-gray-400 uppercase tracking-wide mb-1")

        # responsive grid so 3–5 slots stay in view
        with ui.element("div").classes("grid grid-cols-1 lg:grid-cols-2 gap-2"):
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

        accept_primary = _primary_accept_label(slot.accepts)
        accepts_full = ", ".join(_filetype_to_filename_guess(a) for a in slot.accepts) if slot.accepts else "—"

        # card border color based on state
        if validation.awaiting_upstream:
            border_class = "border-blue-200 bg-blue-50/20"
        elif validation.is_valid:
            border_class = "border-green-200 bg-green-50/20"
        else:
            border_class = "border-red-200 bg-red-50/20"

        # stable path for copy: manual path OR resolved path, but NEVER pending_
        resolved_for_copy = ""
        if current_value == "manual" and manual_path:
            resolved_for_copy = manual_path
        elif validation.resolved_path and (not _is_pending_path(validation.resolved_path)):
            # also avoid showing/copying predicted “pending_*”
            resolved_for_copy = validation.resolved_path

        # compact card
        with ui.card().classes(f"w-full p-2 border shadow-none {border_class}"):
            # header row
            with ui.row().classes("w-full items-center justify-between gap-2"):
                with ui.row().classes("items-center gap-2 min-w-0"):
                    title = ui.label(snake_to_title(slot.key)).classes("text-sm font-semibold text-gray-700 truncate")

                    # tooltip carries lots of “hidden” info
                    tooltip_lines = [f"Accepts: {accepts_full}"]
                    if validation.error_message:
                        tooltip_lines.append(f"Error: {validation.error_message}")
                    if validation.resolved_path and not _is_pending_path(validation.resolved_path):
                        tooltip_lines.append(f"Resolved: {validation.resolved_path}")
                    title.tooltip("\n".join(tooltip_lines))

                    if accept_primary:
                        ui.label(accept_primary).classes(
                            "text-[10px] px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 font-mono"
                        )

                    if not slot.required:
                        ui.label("optional").classes("text-[10px] px-1.5 py-0.5 bg-gray-100 text-gray-500 rounded")

                    if validation.is_user_override:
                        ui.label("overridden").classes("text-[10px] px-1.5 py-0.5 bg-blue-100 text-blue-600 rounded")

                # right side: status + tiny actions
                with ui.row().classes("items-center gap-1"):
                    self._render_validation_badge(validation)

                    if resolved_for_copy:
                        ui.button(
                            icon="content_copy",
                            on_click=_make_copy_handler(resolved_for_copy),
                        ).props("flat dense round size=sm").classes("text-gray-500").tooltip("Copy resolved path")

                    if validation.is_user_override:
                        ui.button(icon="restart_alt", on_click=lambda s=slot: self._clear_override(s)).props(
                            "flat dense round size=sm"
                        ).classes("text-gray-500").tooltip("Reset to auto")

            # source pills row (no extra labels)
            with ui.row().classes("w-full flex-wrap items-center gap-2 mt-2"):
                for c in candidates:
                    self._render_candidate_pill(slot, c, current_value, validation)

                self._render_manual_pill(slot, current_value)

            # manual input only when selected (keeps card compact otherwise)
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

    def _render_validation_badge(self, validation: InputSlotValidation):
        # extremely compact: dot + 1-word label, tooltip for details
        if validation.awaiting_upstream:
            label = "awaiting"
            color = STATUS_STYLE[JobStatus.RUNNING]["color"]
            tip = "Upstream job in progress"
        elif validation.is_valid:
            if validation.file_exists:
                label = "valid"
                color = STATUS_STYLE[JobStatus.SUCCEEDED]["color"]
                tip = "File exists"
            else:
                label = "pending"
                color = STATUS_STYLE[JobStatus.SCHEDULED]["color"]
                tip = "Will be created"
        else:
            label = "invalid"
            color = STATUS_STYLE[JobStatus.FAILED]["color"]
            tip = validation.error_message or "Invalid input"

        with ui.row().classes("items-center gap-1"):
            ui.element("div").style(f"width: 7px; height: 7px; border-radius: 50%; background: {color};")
            ui.label(label).classes("text-[10px] text-gray-600 font-medium").tooltip(tip)

    def _render_candidate_pill(
        self, slot: InputSlot, c: OutputCandidate, current_value: Optional[str], validation: InputSlotValidation
    ):
        is_selected = current_value == c.source_key
        is_pending = "pending_" in (c.instance_path or "")
        is_preferred = (
            bool(getattr(slot, "preferred_source", None)) and slot.preferred_source == c.producer_job_type.value
        )

        style = STATUS_STYLE.get(c.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
        display_name = get_job_display_name(JobType(c.producer_job_type.value))

        inst = "scheduled" if is_pending else _short_instance_label(c.instance_path or "")

        # smaller pill styling
        if is_selected:
            bg = "white"
            border = "#3b82f6"
            text = "#1f2937"
        else:
            bg = "#f3f4f6"
            border = "#e5e7eb"
            text = "#374151"

        border_style = "2px dashed #cbd5e1" if is_pending and not is_selected else f"1px solid {border}"
        # star = " ★" if is_preferred else ""

        btn = (
            ui.button(on_click=lambda sk=c.source_key, s=slot: self._handle_source_change(s, sk))
            .props("flat dense no-caps")
            .style(f"background: {bg}; color: {text}; padding: 3px 9px; border-radius: 999px; border: {border_style};")
        )

        with btn:
            with ui.row().classes("items-center gap-2"):
                ui.element("div").style(f"width: 7px; height: 7px; border-radius: 50%; background: {style['color']};")
                ui.label(display_name).classes("text-[11px] font-medium")
                if is_preferred:
                    ui.icon("star", size="14px").classes("text-amber-500").tooltip("Preferred default source")

                if inst:
                    ui.label(inst).classes("text-[10px] font-mono text-gray-500")

        # tooltip: more detail lives here
        tip_lines = [
            f"{display_name}",
            f"Status: {style['label']}",
        ]

        # Only show instance path if it isn't pending
        if c.instance_path and "pending_" not in c.instance_path:
            tip_lines.append(f"Instance: {c.instance_path}")

        # Only show resolved if it isn't pending
        if is_selected and validation.resolved_path and "pending_" not in validation.resolved_path:
            tip_lines.append(f"Resolved: {validation.resolved_path}")

        btn.tooltip("\n".join(tip_lines))


    def _render_manual_pill(self, slot: InputSlot, current_value: Optional[str]):
        is_selected = current_value == "manual"
        bg = "white" if is_selected else "#f3f4f6"
        border = "#3b82f6" if is_selected else "#e5e7eb"
        text = "#1f2937" if is_selected else "#374151"

        btn = (
            ui.button(on_click=lambda s=slot: self._handle_source_change(s, "manual"))
            .props("flat dense no-caps")
            .style(
                f"background: {bg}; color: {text}; padding: 3px 9px; border-radius: 999px; border: 1px solid {border};"
            )
        )
        with btn:
            with ui.row().classes("items-center gap-2"):
                ui.icon("edit", size="14px").classes("text-gray-500")
                ui.label("Manual…").classes("text-[11px] font-medium")
        btn.tooltip("Specify an explicit file path for this input")

    # ----------------------------
    # Output Slots (compact)
    # ----------------------------

    def _render_output_slots(self, schema, job_model):
        ui.label("Output Slots").classes("text-[10px] font-black text-gray-400 uppercase tracking-wide mt-2 mb-1")

        job_dir, job_dir_pred = self._get_job_dir(job_model)

        with ui.card().classes("w-full p-0 border border-gray-200 shadow-none"):
            # --- Job folder row ---
            if job_dir:
                with ui.row().classes(
                    "w-full p-2 bg-white border-b border-gray-100 items-center gap-3"
                ):
                    ui.label("Job Folder").classes("text-xs font-semibold text-gray-700 min-w-[220px]")

                    if job_dir_pred:
                        ui.label("Assigned at deploy time").classes(
                            "text-[10px] font-mono text-gray-400 italic flex-1"
                        )
                    else:
                        ui.label(str(job_dir)).classes("text-[10px] font-mono text-gray-700 break-all flex-1")
                        ui.button(
                            icon="content_copy",
                            on_click=_make_copy_handler(str(job_dir)),
                        ).props("flat dense round size=sm").classes("text-gray-500").tooltip("Copy job folder path")

            # --- Per-output rows ---
            for i, slot in enumerate(schema):
                bg_class = "bg-gray-50" if i % 2 == 0 else "bg-white"
                resolved = (job_model.paths or {}).get(slot.key)

                # 1) Determine full path to show/copy
                full_path: str = ""

                if resolved:
                    p = Path(str(resolved))
                    state = get_project_state()
                    project_path = getattr(state, "project_path", None)
                    if project_path and not p.is_absolute():
                        p = (project_path / p)
                    full_path = str(p)
                else:
                    if job_dir and getattr(slot, "path_template", None):
                        t = str(slot.path_template)
                        tp = Path(t)
                        if tp.is_absolute():
                            full_path = t
                        else:
                            full_path = str(job_dir / tp)
                    else:
                        full_path = str(getattr(slot, "path_template", "") or "")

                # 2) Label what it produces
                produces_label = ""
                if getattr(slot, "path_template", None):
                    produces_label = _compact_template(Path(str(slot.path_template)).name)
                else:
                    produces_label = _filetype_to_filename_guess(slot.produces)

                # 3) Determine display for pending vs real paths
                if _is_pending_path(full_path):
                    display_path = slot.path_template
                    path_class = "text-[10px] font-mono text-gray-400 italic break-all flex-1"
                    copy_path = None
                else:
                    display_path = _compact_template(full_path)
                    path_class = (
                        "text-[10px] font-mono text-gray-700 break-all flex-1"
                        if resolved
                        else "text-[10px] font-mono text-gray-500 break-all flex-1 italic"
                    )
                    copy_path = full_path

                # 4) Row
                with ui.row().classes(
                    f"w-full p-2 {bg_class} border-b border-gray-100 last:border-0 items-center gap-3"
                ):
                    with ui.column().classes("gap-0 min-w-[220px]"):
                        ui.label(snake_to_title(slot.key)).classes("text-xs font-semibold text-gray-700")
                        ui.label(f"Produces: {produces_label}").classes("text-[10px] text-gray-400 font-mono")

                    ui.label(display_path).classes(path_class).tooltip(full_path if full_path else "")

                    if copy_path:
                        ui.button(
                            icon="content_copy",
                            on_click=_make_copy_handler(copy_path),
                        ).props("flat dense round size=sm").classes("text-gray-500").tooltip("Copy full path")


    # ----------------------------
    # State updates
    # ----------------------------

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
            resolver = PathResolutionService(state, active_job_types=self.active_job_types)  # CHANGED

            container.clear()
            with container:
                self._render_input_slot_content(resolver, slot, job_model)

        if self.on_change:
            self.on_change()

    def _open_file_picker(self, slot):
        ui.notify("File picker not yet implemented", type="info")


def render_io_config(job_type: JobType, on_change: Optional[Callable[[], None]] = None,
                     active_job_types: Optional[set] = None):
    component = IOConfigComponent(job_type, on_change, active_job_types)
    component.render()
