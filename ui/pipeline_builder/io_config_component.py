# ui/pipeline_builder/io_config_component.py
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
from ui.utils import snake_to_title


STATUS_STYLE = {
    JobStatus.SUCCEEDED: {"color": "#10b981", "label": "done"},
    JobStatus.RUNNING: {"color": "#3b82f6", "label": "running"},
    JobStatus.SCHEDULED: {"color": "#f59e0b", "label": "scheduled"},
    JobStatus.FAILED: {"color": "#ef4444", "label": "failed"},
    JobStatus.UNKNOWN: {"color": "#6b7280", "label": "?"},
}


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _compact_template(s: str) -> str:
    return re.sub(r"{[^}]+}", "…", s or "")


def _make_copy_handler(text: str):
    async def handler():
        await _copy_to_clipboard(text)

    return handler


async def _copy_to_clipboard(text: str) -> None:
    try:
        ui.clipboard.write(text)
        ui.notify("Copied", type="positive", timeout=900)
        return
    except Exception:
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

    def __init__(
        self,
        job_type: JobType,
        instance_id: str,
        on_change: Optional[Callable[[], None]] = None,
        active_instance_ids: Optional[set] = None,
    ):
        self.job_type = job_type
        self.instance_id = instance_id
        self.on_change = on_change
        self.active_instance_ids = active_instance_ids
        self._validation_cache: Dict[str, InputSlotValidation] = {}
        self._slot_containers: Dict[str, ui.element] = {}

    def render(self):
        state = get_project_state()
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            ui.label(f"Job instance '{self.instance_id}' not initialized").classes("text-red-500 italic")
            return

        resolver = PathResolutionService(state, active_instance_ids=self.active_instance_ids)
        input_schema = resolver.get_input_schema_for_job(self.job_type)
        output_schema = resolver.get_output_schema_for_job(self.job_type)

        with ui.column().classes("w-full gap-2"):
            if input_schema:
                self._render_input_slots(resolver, input_schema, job_model)
            if output_schema:
                self._render_output_slots(output_schema, job_model)

    def _get_job_dir(self, job_model) -> tuple[Optional[Path], bool]:
        state = get_project_state()
        project_path = getattr(state, "project_path", None)
        if not project_path:
            return None, True

        if getattr(job_model, "relion_job_name", None):
            return (project_path / job_model.relion_job_name.strip("/")).resolve(), False

        n = _safe_int(getattr(job_model, "relion_job_number", None))
        if n is not None:
            return (project_path / "External" / f"job{n:03d}").resolve(), True

        return (project_path / "External" / f"pending_{self.instance_id}").resolve(), True

    def _render_input_slots(self, resolver, schema, job_model):
        ui.label("Input Slots").classes("text-[10px] font-black text-gray-400 uppercase tracking-wide mb-1")
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

        if validation.awaiting_upstream:
            border_class = "border-blue-200 bg-blue-50/20"
        elif validation.is_valid:
            border_class = "border-green-200 bg-green-50/20"
        else:
            border_class = "border-red-200 bg-red-50/20"

        resolved_for_copy = ""
        if current_value == "manual" and manual_path:
            resolved_for_copy = manual_path
        elif validation.resolved_path and not _is_pending_path(validation.resolved_path):
            resolved_for_copy = validation.resolved_path

        with ui.card().classes(f"w-full p-2 border shadow-none {border_class}"):
            with ui.row().classes("w-full items-center justify-between gap-2"):
                with ui.row().classes("items-center gap-2 min-w-0"):
                    title = ui.label(snake_to_title(slot.key)).classes("text-sm font-semibold text-gray-700 truncate")
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

                with ui.row().classes("items-center gap-1"):
                    self._render_validation_badge(validation)
                    if resolved_for_copy:
                        ui.button(icon="content_copy", on_click=_make_copy_handler(resolved_for_copy)).props(
                            "flat dense round size=sm"
                        ).classes("text-gray-500").tooltip("Copy resolved path")
                    if validation.is_user_override:
                        ui.button(icon="restart_alt", on_click=lambda s=slot: self._clear_override(s)).props(
                            "flat dense round size=sm"
                        ).classes("text-gray-500").tooltip("Reset to auto")

            with ui.row().classes("w-full flex-wrap items-center gap-2 mt-2"):
                for c in candidates:
                    self._render_candidate_pill(slot, c, current_value, validation)
                self._render_manual_pill(slot, current_value)

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
        if validation.awaiting_upstream:
            label, color, tip = "awaiting", STATUS_STYLE[JobStatus.RUNNING]["color"], "Upstream job in progress"
        elif validation.is_valid:
            if validation.file_exists:
                label, color, tip = "valid", STATUS_STYLE[JobStatus.SUCCEEDED]["color"], "File exists"
            else:
                label, color, tip = "pending", STATUS_STYLE[JobStatus.SCHEDULED]["color"], "Will be created"
        else:
            label = "invalid"
            color = STATUS_STYLE[JobStatus.FAILED]["color"]
            tip = validation.error_message or "Invalid input"

        with ui.row().classes("items-center gap-1"):
            ui.element("div").style(f"width: 7px; height: 7px; border-radius: 50%; background: {color};")
            ui.label(label).classes("text-[10px] text-gray-600 font-medium").tooltip(tip)

    def _render_candidate_pill(self, slot, c, current_value, validation):
        is_selected = current_value == c.source_key
        is_pending = "pending_" in (c.instance_path or "")
        is_preferred = (
            bool(getattr(slot, "preferred_source", None)) and slot.preferred_source == c.producer_job_type.value
        )

        style = STATUS_STYLE.get(c.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
        display_name = get_job_display_name(JobType(c.producer_job_type.value))
        inst = "scheduled" if is_pending else _short_instance_label(c.instance_path or "")

        if is_selected:
            bg, border, text = "white", "#3b82f6", "#1f2937"
        else:
            bg, border, text = "#f3f4f6", "#e5e7eb", "#374151"

        border_style = "2px dashed #cbd5e1" if is_pending and not is_selected else f"1px solid {border}"

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

        tip_lines = [display_name, f"Status: {style['label']}"]
        if c.instance_path and "pending_" not in c.instance_path:
            tip_lines.append(f"Instance: {c.instance_path}")
        if is_selected and validation.resolved_path and "pending_" not in validation.resolved_path:
            tip_lines.append(f"Resolved: {validation.resolved_path}")
        btn.tooltip("\n".join(tip_lines))

    def _render_manual_pill(self, slot, current_value):
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

    def _render_output_slots(self, schema, job_model):
        ui.label("Output Slots").classes("text-[10px] font-black text-gray-400 uppercase tracking-wide mt-2 mb-1")
        job_dir, job_dir_pred = self._get_job_dir(job_model)

        with ui.card().classes("w-full p-0 border border-gray-200 shadow-none"):
            if job_dir:
                with ui.row().classes("w-full p-2 bg-white border-b border-gray-100 items-center gap-3"):
                    ui.label("Job Folder").classes("text-xs font-semibold text-gray-700 min-w-[220px]")
                    if job_dir_pred:
                        ui.label("Assigned at deploy time").classes("text-[10px] font-mono text-gray-400 italic flex-1")
                    else:
                        ui.label(str(job_dir)).classes("text-[10px] font-mono text-gray-700 break-all flex-1")
                        ui.button(icon="content_copy", on_click=_make_copy_handler(str(job_dir))).props(
                            "flat dense round size=sm"
                        ).classes("text-gray-500").tooltip("Copy job folder path")

            for i, slot in enumerate(schema):
                bg_class = "bg-gray-50" if i % 2 == 0 else "bg-white"
                resolved = (job_model.paths or {}).get(slot.key)

                full_path: str = ""
                if resolved:
                    p = Path(str(resolved))
                    state = get_project_state()
                    project_path = getattr(state, "project_path", None)
                    if project_path and not p.is_absolute():
                        p = project_path / p
                    full_path = str(p)
                else:
                    if job_dir and getattr(slot, "path_template", None):
                        t = str(slot.path_template)
                        tp = Path(t)
                        full_path = str(job_dir / tp) if not tp.is_absolute() else t
                    else:
                        full_path = str(getattr(slot, "path_template", "") or "")

                produces_label = ""
                if getattr(slot, "path_template", None):
                    produces_label = _compact_template(Path(str(slot.path_template)).name)
                else:
                    produces_label = _filetype_to_filename_guess(slot.produces)

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

                with ui.row().classes(
                    f"w-full p-2 {bg_class} border-b border-gray-100 last:border-0 items-center gap-3"
                ):
                    with ui.column().classes("gap-0 min-w-[220px]"):
                        ui.label(snake_to_title(slot.key)).classes("text-xs font-semibold text-gray-700")
                        ui.label(f"Produces: {produces_label}").classes("text-[10px] text-gray-400 font-mono")
                    ui.label(display_path).classes(path_class).tooltip(full_path if full_path else "")
                    if copy_path:
                        ui.button(icon="content_copy", on_click=_make_copy_handler(copy_path)).props(
                            "flat dense round size=sm"
                        ).classes("text-gray-500").tooltip("Copy full path")

    # ── State updates ──────────────────────────────────────────────────────────

    def _handle_source_change(self, slot, value):
        state = get_project_state()
        project_path = state.project_path  # capture while tab context is live
        job_model = state.jobs.get(self.instance_id)
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
        asyncio.create_task(self._save_and_refresh(slot, project_path))

    def _handle_manual_path_change(self, slot, path):
        state = get_project_state()
        project_path = state.project_path
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            return
        if not hasattr(job_model, "source_overrides") or job_model.source_overrides is None:
            job_model.source_overrides = {}
        job_model.source_overrides[slot.key] = f"manual:{path}"
        asyncio.create_task(self._save_and_refresh(slot, project_path))

    def _clear_override(self, slot):
        state = get_project_state()
        project_path = state.project_path
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            return
        if hasattr(job_model, "source_overrides") and job_model.source_overrides:
            job_model.source_overrides.pop(slot.key, None)
        asyncio.create_task(self._save_and_refresh(slot, project_path))

    async def _save_and_refresh(self, slot, project_path):
        from services.project_state import get_project_state_for

        await get_state_service().save_project(project_path=project_path)
        container = self._slot_containers.get(slot.key)
        if container:
            state = get_project_state_for(project_path)
            job_model = state.jobs.get(self.instance_id)
            resolver = PathResolutionService(state, active_instance_ids=self.active_instance_ids)
            container.clear()
            with container:
                self._render_input_slot_content(resolver, slot, job_model)
        if self.on_change:
            self.on_change()

    def _open_file_picker(self, slot):
        ui.notify("File picker not yet implemented", type="info")


def render_io_config(
    job_type: JobType,
    instance_id: str,
    on_change: Optional[Callable[[], None]] = None,
    active_instance_ids: Optional[set] = None,
):
    component = IOConfigComponent(job_type, instance_id, on_change, active_instance_ids)
    component.render()
