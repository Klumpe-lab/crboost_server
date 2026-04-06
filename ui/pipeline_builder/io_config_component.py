# ui/pipeline_builder/io_config_component.py
"""
IO slot configuration — structured table layout with dropdowns.
"""

import asyncio
import re
from pathlib import Path
from typing import Callable, Dict, Optional, Iterable

from nicegui import ui

from services.io_slots import JobFileType
from services.models_base import JobType, JobStatus
from services.path_resolution_service import PathResolutionService, InputSlotValidation
from services.project_state import get_project_state, get_state_service
from ui.ui_state import get_job_display_name
from ui.utils import snake_to_title

MONO = "font-family: 'IBM Plex Mono', monospace;"
FONT = "font-family: 'IBM Plex Sans', sans-serif;"

STATUS_STYLE = {
    JobStatus.SUCCEEDED: {"color": "#10b981", "label": "done", "icon": "check_circle"},
    JobStatus.RUNNING: {"color": "#3b82f6", "label": "running", "icon": "sync"},
    JobStatus.SCHEDULED: {"color": "#f59e0b", "label": "scheduled", "icon": "schedule"},
    JobStatus.FAILED: {"color": "#ef4444", "label": "failed", "icon": "error"},
    JobStatus.UNKNOWN: {"color": "#6b7280", "label": "?", "icon": "help"},
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

        with ui.column().classes("w-full gap-1"):
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

    # ── Input slots (structured rows with dropdowns) ─────────────────────────

    def _render_input_slots(self, resolver, schema, job_model):
        for slot in schema:
            container = ui.column().classes("w-full gap-0")
            self._slot_containers[slot.key] = container
            with container:
                self._render_input_slot_row(resolver, slot, job_model)

    def _render_input_slot_row(self, resolver, slot, job_model):
        """Render a single input slot as a compact row: name | dropdown | status."""
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

        # Build dropdown options: each candidate becomes an option
        options = {}
        for c in candidates:
            style = STATUS_STYLE.get(c.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
            is_pending = "pending_" in (c.instance_path or "")
            display_name = get_job_display_name(JobType(c.producer_job_type.value))
            inst = "scheduled" if is_pending else _short_instance_label(c.instance_path or "")
            status_label = style["label"]
            option_label = f"[{status_label}] {inst} — {display_name}"
            options[c.source_key] = option_label
        options["manual"] = "Manual path…"

        # Status dot color
        if validation.awaiting_upstream:
            dot_color = STATUS_STYLE[JobStatus.RUNNING]["color"]
        elif validation.is_valid:
            dot_color = STATUS_STYLE[JobStatus.SUCCEEDED]["color"]
        else:
            dot_color = STATUS_STYLE[JobStatus.FAILED]["color"]

        with ui.row().classes("w-full items-center gap-2").style("min-height: 26px;"):
            # Status dot
            ui.element("div").style(
                f"width: 6px; height: 6px; border-radius: 50%; background: {dot_color}; flex-shrink: 0;"
            )
            # Slot name
            ui.label(snake_to_title(slot.key)).style(
                f"{FONT} font-size: 10px; font-weight: 500; color: #374151; flex-shrink: 0; width: 90px;"
            )
            # File type hint
            if accept_primary:
                ui.label(accept_primary).style(f"{MONO} font-size: 9px; color: #94a3b8; flex-shrink: 0;")
            # Dropdown
            sel = ui.select(
                options=options,
                value=current_value or (list(options.keys())[0] if options else None),
                on_change=lambda e, s=slot: self._handle_source_change(s, e.value),
            )
            sel.props("dense borderless hide-bottom-space")
            sel.style(f"{MONO} font-size: 10px; color: #475569; flex: 1; min-width: 120px;")
            # Optional badge
            if not slot.required:
                ui.label("opt").style(
                    f"{FONT} font-size: 8px; color: #94a3b8; background: #f1f5f9; "
                    "border-radius: 3px; padding: 0 4px; flex-shrink: 0;"
                )
            # Override reset
            if validation.is_user_override:
                (
                    ui.button(icon="restart_alt", on_click=lambda s=slot: self._clear_override(s))
                    .props("flat dense round size=xs")
                    .style("color: #94a3b8; flex-shrink: 0;")
                    .tooltip("Reset to auto")
                )

        # Manual path input row
        if current_value == "manual":
            with ui.row().classes("w-full items-center gap-2").style("padding-left: 98px;"):
                inp = ui.input(
                    value=manual_path,
                    placeholder="/path/to/file",
                    on_change=lambda e, s=slot: self._handle_manual_path_change(s, e.value),
                )
                inp.props("dense borderless hide-bottom-space")
                inp.style(f"{MONO} font-size: 10px; flex: 1; border-bottom: 1px solid #cbd5e1; padding: 1px 2px;")
                ui.button(icon="folder_open", on_click=lambda s=slot: self._open_file_picker(s)).props(
                    "flat dense round size=xs"
                ).style("color: #64748b;").tooltip("Browse…")

    # ── Output slots ─────────────────────────────────────────────────────────

    def _render_output_slots(self, schema, job_model):
        job_dir, job_dir_pred = self._get_job_dir(job_model)

        if job_dir and not job_dir_pred:
            with ui.row().classes("w-full items-baseline gap-2").style("min-height: 20px; margin-bottom: 2px;"):
                ui.label("Job folder").style(f"{FONT} font-size: 9px; color: #94a3b8; flex-shrink: 0;")
                ui.label(str(job_dir)).style(
                    f"{MONO} font-size: 10px; color: #64748b; flex: 1; min-width: 0; "
                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                ).tooltip(str(job_dir))
                (
                    ui.button(icon="content_copy", on_click=_make_copy_handler(str(job_dir)))
                    .props("flat dense round size=xs")
                    .style("color: #94a3b8; flex-shrink: 0;")
                    .tooltip("Copy")
                )

        for slot in schema:
            resolved = (job_model.paths or {}).get(slot.key)

            full_path: str = ""
            if resolved:
                p = Path(str(resolved))
                state = get_project_state()
                project_path = getattr(state, "project_path", None)
                if project_path and not p.is_absolute():
                    p = project_path / p
                full_path = str(p)
            elif job_dir and getattr(slot, "path_template", None):
                t = str(slot.path_template)
                tp = Path(t)
                full_path = str(job_dir / tp) if not tp.is_absolute() else t
            else:
                full_path = str(getattr(slot, "path_template", "") or "")

            if _is_pending_path(full_path):
                display_path = str(getattr(slot, "path_template", ""))
                path_color = "#94a3b8"
            else:
                display_path = _compact_template(full_path)
                path_color = "#64748b" if resolved else "#94a3b8"

            with ui.row().classes("w-full items-baseline gap-2").style("min-height: 20px;"):
                ui.label(snake_to_title(slot.key)).style(
                    f"{FONT} font-size: 9px; color: #94a3b8; flex-shrink: 0; width: 90px;"
                )
                ui.label(display_path).style(
                    f"{MONO} font-size: 10px; color: {path_color}; flex: 1; min-width: 0; "
                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                ).tooltip(full_path)
                if full_path and not _is_pending_path(full_path):
                    (
                        ui.button(icon="content_copy", on_click=_make_copy_handler(full_path))
                        .props("flat dense round size=xs")
                        .style("color: #94a3b8; flex-shrink: 0;")
                        .tooltip("Copy")
                    )

    # ── State updates ────────────────────────────────────────────────────────

    def _handle_source_change(self, slot, value):
        state = get_project_state()
        project_path = state.project_path
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
                self._render_input_slot_row(resolver, slot, job_model)
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
