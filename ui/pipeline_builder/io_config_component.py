# ui/pipeline_builder/io_config_component.py
"""
I/O slot configuration.

Renders a job's typed input/output slots as two labelled sections:

  Inputs   — each slot is a status-dotted source *selector* (a compact menu of
             candidate upstream jobs, each row styled like a pipeline-roster job
             row: colored status dot + disambiguated job name + status word),
             plus a "Manual path…" escape hatch with a real file browser.
  Outputs  — each slot shows its predicted absolute path (computed now; the file
             only exists once the job runs), with a copy button.

Every slot name carries a rich hover panel explaining the artifact type it deals
in (for inputs: what each accepted type means + which producer it prefers; for
outputs: the full path and that it is predicted until the job finishes).
"""

from pathlib import Path
from typing import Callable, Dict, Optional

from nicegui import ui

from services.io_slots import JobFileType
from services.models_base import JobType, JobStatus
from services.path_resolution_service import PathResolutionService, InputSlotValidation
from services.project_state import get_project_state, get_state_service, get_project_state_for
from ui.ui_state import get_job_display_name
from ui.utils import snake_to_title
from ui.components.copyable import copyable_path, copy_button

MONO = "font-family: 'IBM Plex Mono', monospace;"
FONT = "font-family: 'IBM Plex Sans', sans-serif;"

# Jobs whose driver consumes a gain reference. The project-wide gain reference
# (services.models_base.AcquisitionParams.gain_reference_path, set at project
# setup) is surfaced as an auto-populated input row at the top of these jobs'
# Inputs so the user can see/override it at registration time.
GAIN_CONSUMERS = {JobType.FS_MOTION_CTF, JobType.TS_IMPORT}

STATUS_STYLE = {
    JobStatus.SUCCEEDED: {"color": "#10b981", "label": "done"},
    JobStatus.RUNNING: {"color": "#3b82f6", "label": "running"},
    JobStatus.SCHEDULED: {"color": "#f59e0b", "label": "scheduled"},
    JobStatus.QUEUED: {"color": "#a855f7", "label": "queued"},
    JobStatus.FAILED: {"color": "#ef4444", "label": "failed"},
    JobStatus.UNKNOWN: {"color": "#94a3b8", "label": "pending"},
}

# Human name + one-line meaning per artifact type, for the input-slot hover.
# Sourced from the inline path comments in services/io_slots.py.
_FILETYPE_INFO: Dict[JobFileType, tuple] = {
    JobFileType.TILT_SERIES_STAR: (
        "Tilt-series star",
        "RELION tilt_series.star listing every tilt series (from Import).",
    ),
    JobFileType.WARP_FRAMESERIES_SETTINGS: (
        "Warp frameseries settings",
        "WarpTools .settings describing the raw frame series.",
    ),
    JobFileType.FS_MOTION_CTF_STAR: (
        "Frameseries motion+CTF star",
        "Per-frameseries motion & CTF estimates (fsMotionAndCtf).",
    ),
    JobFileType.WARP_FRAMESERIES_DIR: (
        "Warp frameseries dir",
        "WarpTools frameseries working dir with per-frame XML metadata.",
    ),
    JobFileType.WARP_TILTSERIES_SETTINGS: (
        "Warp tiltseries settings",
        "WarpTools .settings for tilt-series processing (tsImport); required by all downstream TS jobs.",
    ),
    JobFileType.TOMOSTAR_DIR: ("Tomostar dir", "Directory of per-tilt-series .tomostar files (tsImport)."),
    JobFileType.ALIGNED_TILT_SERIES_STAR: (
        "Aligned tilt-series star",
        "Tilt-series star after alignment (aligntiltsWarp).",
    ),
    JobFileType.TS_CTF_TILT_SERIES_STAR: ("TS-CTF tilt-series star", "Tilt-series star with per-tilt CTF (tsCtf)."),
    JobFileType.FILTERED_TILT_SERIES_STAR: (
        "Filtered tilt-series star",
        "Tilt-series star after tilt filtering (tiltFilter).",
    ),
    JobFileType.WARP_TILTSERIES_DIR: (
        "Warp tiltseries dir",
        "WarpTools tilt-series working dir with per-TS XML metadata.",
    ),
    JobFileType.TOMOGRAMS_STAR: ("Tomograms star", "Reconstructed tomograms star (tsReconstruct)."),
    JobFileType.DENOISED_TOMOGRAMS_STAR: ("Denoised tomograms star", "Tomograms star pointing at denoised volumes."),
    JobFileType.DENOISE_MODEL_TAR: ("Denoise model", "Trained denoising model archive."),
    JobFileType.TM_RESULTS_DIR: ("Template-match results dir", "PyTOM score/angle volumes from template matching."),
    JobFileType.CANDIDATES_STAR: ("Candidates star", "Picked candidate positions (template matching)."),
    JobFileType.OPTIMISATION_SET_STAR: (
        "Optimisation set",
        "RELION optimisation_set.star bundling particles + tomograms + references.",
    ),
    JobFileType.PARTICLES_STAR: ("Particles star", "Extracted subtomogram particles (subtomoExtraction)."),
    JobFileType.REFERENCE_MAP: ("Reference map", "3D reference map (merged.mrc)."),
    JobFileType.HALF_MAP: ("Half map", "Independent half-map (half1.mrc)."),
}

# ── Shared visual tokens ─────────────────────────────────────────────────────
# Same name style for BOTH input and output slots (the user asked for these to
# stop looking like two different things — one darker, one grayed).
_SLOT_NAME_STYLE = (
    f"{FONT} font-size: 10px; font-weight: 500; color: #334155; cursor: help; "
    "flex-shrink: 0; width: 130px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
)
_SECTION_TITLE_STYLE = (
    f"{FONT} font-size: 9px; font-weight: 700; color: #94a3b8; letter-spacing: 0.06em; text-transform: uppercase;"
)
_TIP_PANEL_STYLE = (
    "background: #0f172a; color: #e2e8f0; border-radius: 6px; padding: 8px 10px; "
    "max-width: 380px; box-shadow: 0 8px 24px rgba(0,0,0,0.28);"
)
_TIP_TITLE = f"{FONT} font-size: 11px; font-weight: 700; color: #f8fafc;"
_TIP_LEAD = f"{FONT} font-size: 10px; color: #cbd5e1; margin-top: 1px;"
_TIP_KEY = f"{FONT} font-size: 10px; font-weight: 600; color: #e2e8f0;"
_TIP_DESC = f"{FONT} font-size: 10px; color: #94a3b8; line-height: 1.35;"
_TIP_PATH = f"{MONO} font-size: 10px; color: #cbd5e1; word-break: break-all; white-space: normal;"


def _safe_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


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


def _is_pending_path(p: Optional[str]) -> bool:
    return bool(p) and "pending_" in p


def _candidate_name(c) -> str:
    """Human, DISAMBIGUATED name for one upstream producer candidate.

    Two queued jobs of the same type (e.g. two Alignments) must not collapse to
    the same label. We build "<Display Name>" plus a per-instance discriminator:
    the instance-id suffix for not-yet-reconciled jobs, or the RELION job folder
    (job005) once the job exists on disk.
    """
    if getattr(c, "label", None):
        return c.label
    base = get_job_display_name(JobType(c.producer_job_type.value))
    iid = getattr(c, "producer_instance_id", "") or ""
    parts = iid.split("__", 1)
    if len(parts) > 1:
        suffix = parts[1]
        base = f"{base} #{suffix}" if suffix.isdigit() else f"{base} · {suffix}"
    folder = _short_instance_label(c.instance_path or "")
    # Species suffix disambiguates same-type producers wired to different species
    # (e.g. two subtomo extractions, one per species) at the point of selection.
    sp = getattr(c, "species_id", None)
    sp_tag = f" · {sp}" if sp else ""
    if folder and not folder.startswith("pending_"):
        return f"{base} ({folder}){sp_tag}"
    return f"{base}{sp_tag}"


class IOConfigComponent:
    def __init__(
        self,
        job_type: JobType,
        instance_id: str,
        on_change: Optional[Callable[[], None]] = None,
        active_instance_ids: Optional[set] = None,
        read_only: bool = False,
    ):
        self.job_type = job_type
        self.instance_id = instance_id
        self.on_change = on_change
        self.active_instance_ids = active_instance_ids
        # read_only: a completed/frozen job. Same sectioned layout, but the
        # source selectors become static faces and the edit affordances (manual
        # path, reset, gain browse) are hidden.
        self.read_only = read_only
        self._validation_cache: Dict[str, InputSlotValidation] = {}
        self._slot_containers: Dict[str, ui.element] = {}
        self._gain_container: Optional[ui.element] = None

    def render(self):
        state = get_project_state()
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            ui.label(f"Job instance '{self.instance_id}' not initialized").classes("text-red-500 italic")
            return

        resolver = PathResolutionService(state, active_instance_ids=self.active_instance_ids)
        input_schema = resolver.get_input_schema_for_job(self.job_type)
        output_schema = resolver.get_output_schema_for_job(self.job_type)
        wants_gain = self.job_type in GAIN_CONSUMERS

        with ui.column().classes("w-full gap-1"):
            if input_schema or wants_gain:
                ui.label("Inputs").style(_SECTION_TITLE_STYLE)
                if wants_gain:
                    self._gain_container = ui.column().classes("w-full gap-0")
                    with self._gain_container:
                        self._render_gain_row()
                if input_schema:
                    self._render_input_slots(resolver, input_schema, job_model)
            if output_schema:
                # A little breathing room + a title to separate outputs from inputs.
                ui.element("div").style("height: 8px;")
                ui.label("Outputs").style(_SECTION_TITLE_STYLE)
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

    # ── Gain reference (project-wide external input) ─────────────────────────

    def _render_gain_row(self):
        state = get_project_state()
        gain = getattr(state.acquisition, "gain_reference_path", None)
        is_set = bool(gain and str(gain).strip() and str(gain) != "None")
        dot_color = "#10b981" if is_set else "#f59e0b"

        with ui.row().classes("w-full items-center gap-2").style("min-height: 26px;"):
            ui.element("div").style(
                f"width: 6px; height: 6px; border-radius: 50%; background: {dot_color}; flex-shrink: 0;"
            )
            name = ui.label("Gain Reference").style(_SLOT_NAME_STYLE)
            with name, ui.tooltip().style(_TIP_PANEL_STYLE):
                ui.label("Gain Reference").style(_TIP_TITLE)
                ui.label("Project-wide external input, auto-filled from project setup.").style(_TIP_LEAD)
                ui.label(
                    "Used by every job that applies gain correction — frameseries "
                    "motion/CTF and tilt-series import. Setting it here changes it for "
                    "the whole project; leave empty if your frames are already "
                    "gain-corrected."
                ).style(_TIP_DESC)
            # Value / placeholder
            if is_set:
                ui.label(str(gain)).style(
                    f"{MONO} font-size: 10px; color: #475569; flex: 1 1 0; min-width: 0; "
                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                ).tooltip(str(gain))
                copy_button(str(gain), tooltip="Copy gain path")
            else:
                ui.label("not set · optional").style(
                    f"{FONT} font-size: 10px; color: #94a3b8; font-style: italic; flex: 1 1 0; min-width: 0;"
                )
            if not self.read_only:
                ui.button(icon="folder_open", on_click=self._pick_gain).props("flat dense round size=xs").style(
                    "color: #64748b; flex-shrink: 0;"
                ).tooltip("Browse for gain reference…")
                if is_set:
                    ui.button(icon="close", on_click=self._clear_gain).props("flat dense round size=xs").style(
                        "color: #94a3b8; flex-shrink: 0;"
                    ).tooltip("Clear")

    async def _pick_gain(self):
        from ui.local_file_picker import local_file_picker

        state = get_project_state()
        cur = getattr(state.acquisition, "gain_reference_path", None)
        start = str(Path(cur).parent) if cur else (str(state.project_path) if state.project_path else "~")
        result = await local_file_picker(directory=start, mode="file")
        if result:
            await self._set_gain(result[0])

    async def _clear_gain(self):
        await self._set_gain(None)

    async def _set_gain(self, value: Optional[str]):
        state = get_project_state()
        project_path = state.project_path
        state.acquisition.gain_reference_path = value or None
        state.mark_dirty()
        await get_state_service().save_project(project_path=project_path)
        if self._gain_container is not None:
            self._gain_container.clear()
            with self._gain_container:
                self._render_gain_row()
        if self.on_change:
            self.on_change()

    # ── Input slots ──────────────────────────────────────────────────────────

    def _render_input_slots(self, resolver, schema, job_model):
        for slot in schema:
            container = ui.column().classes("w-full gap-0")
            self._slot_containers[slot.key] = container
            with container:
                self._render_input_slot_row(resolver, slot, job_model)

    def _render_input_slot_row(self, resolver, slot, job_model):
        overrides = getattr(job_model, "source_overrides", {}) or {}
        current_override = overrides.get(slot.key)

        candidates = resolver.get_candidates_for_slot(self.job_type, slot.key, consumer_instance_id=self.instance_id)
        validation = resolver.validate_input_slot(
            self.job_type, job_model, slot.key, check_filesystem=True, consumer_instance_id=self.instance_id
        )
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

        # Row status dot: blue while an upstream is still producing, green when
        # resolved, red when required-but-missing, muted when optional & empty.
        if validation.awaiting_upstream:
            dot_color = STATUS_STYLE[JobStatus.RUNNING]["color"]
        elif validation.is_valid:
            dot_color = STATUS_STYLE[JobStatus.SUCCEEDED]["color"]
        elif not slot.required and current_value is None:
            dot_color = "#cbd5e1"
        else:
            dot_color = STATUS_STYLE[JobStatus.FAILED]["color"]

        with ui.row().classes("w-full items-center gap-2").style("min-height: 26px;"):
            ui.element("div").style(
                f"width: 6px; height: 6px; border-radius: 50%; background: {dot_color}; flex-shrink: 0;"
            )
            self._input_name_label(slot)
            if self.read_only:
                self._render_static_source(slot, candidates, current_value)
            else:
                self._render_source_selector(slot, candidates, current_value)
                if not slot.required:
                    ui.label("opt").style(
                        f"{FONT} font-size: 8px; color: #94a3b8; background: #f1f5f9; "
                        "border-radius: 3px; padding: 0 4px; flex-shrink: 0;"
                    )
                if validation.is_user_override:
                    (
                        ui.button(icon="restart_alt", on_click=lambda s=slot: self._clear_override(s))
                        .props("flat dense round size=xs")
                        .style("color: #94a3b8; flex-shrink: 0;")
                        .tooltip("Reset to auto")
                    )

        # Manual-path editor (editable mode only).
        manual_editing = current_value == "manual" and not self.read_only
        if manual_editing:
            with ui.row().classes("w-full items-center gap-2").style("padding-left: 20px;"):
                inp = ui.input(
                    value=manual_path,
                    placeholder="/path/to/file",
                    on_change=lambda e, s=slot: self._handle_manual_path_change(s, e.value),
                )
                inp.props(
                    "dense borderless hide-bottom-space "
                    "input-style=\"font-family: 'IBM Plex Mono', monospace; font-size: 11px; "
                    'color: #1e293b; padding: 1px 2px; min-height: 0;"'
                )
                inp.style("flex: 1 1 0; min-width: 0;")
                ui.button(icon="folder_open", on_click=lambda s=slot: self._open_file_picker(s)).props(
                    "flat dense round size=xs"
                ).style("color: #64748b;").tooltip("Browse…")

        # Full resolved input path — always visible (wraps, never truncated) with
        # a copy icon. Skipped only while the manual editor above already shows it.
        if validation.resolved_path and not manual_editing:
            with ui.row().classes("w-full items-start").style("padding-left: 20px; margin-bottom: 2px;"):
                copyable_path(validation.resolved_path, copy_tooltip="Copy input path")
            note = None
            if validation.awaiting_upstream:
                note = "not written yet — produced when the upstream job runs"
            elif not validation.file_exists:
                note = "path resolved, file not on disk yet"
            if note:
                with ui.row().classes("w-full").style("padding-left: 20px;"):
                    ui.label(note).style(f"{FONT} font-size: 8px; color: #94a3b8; font-style: italic;")

    def _render_static_source(self, slot, candidates, current_value):
        """Read-only counterpart of the source selector: the chosen source shown
        as a static face (no menu), for completed/frozen jobs."""
        cur = next((c for c in candidates if c.source_key == current_value), None)
        with ui.element("div").style(
            "flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: 6px; padding: 0 6px; height: 24px;"
        ):
            if current_value == "manual":
                self._selector_face("#94a3b8", "Manual path", "custom file")
            elif cur is not None:
                st = STATUS_STYLE.get(cur.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
                self._selector_face(st["color"], _candidate_name(cur), st["label"])
            elif current_value:
                self._selector_face("#f59e0b", current_value, "unresolved")
            else:
                self._selector_face("#cbd5e1", "—", "")

    def _input_name_label(self, slot):
        # Explains the artifact TYPE only. The concrete resolved path is shown
        # inline beneath the row (copyable), not buried in this hover.
        name = ui.label(snake_to_title(slot.key)).style(_SLOT_NAME_STYLE)
        with name, ui.tooltip().style(_TIP_PANEL_STYLE):
            ui.label(snake_to_title(slot.key)).style(_TIP_TITLE)
            ui.element("div").style("height: 4px;")
            for a in slot.accepts:
                info = _FILETYPE_INFO.get(a)
                hname = info[0] if info else a.value
                hdesc = info[1] if info else ""
                ui.label(f"• {hname}").style(_TIP_KEY)
                if hdesc:
                    ui.label(hdesc).style(_TIP_DESC + " margin: 0 0 3px 8px;")
            pref = getattr(slot, "preferred_source", None)
            if pref:
                try:
                    pref_disp = get_job_display_name(JobType(pref))
                except Exception:
                    pref_disp = pref
                ui.label(f"Prefers output from: {pref_disp}").style(_TIP_LEAD)
            if not slot.required:
                ui.label("Optional — leave unset to skip this input.").style(_TIP_LEAD)
        return name

    def _render_source_selector(self, slot, candidates, current_value):
        cur = next((c for c in candidates if c.source_key == current_value), None)

        selector = (
            ui.element("div")
            .classes("cb-io-src")
            .style(
                "flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: 6px; "
                "cursor: pointer; padding: 0 6px; border: 1px solid #e2e8f0; border-radius: 4px; "
                "background: #fff; height: 24px;"
            )
        )
        with selector:
            if current_value == "manual":
                self._selector_face("#94a3b8", "Manual path", "custom file")
            elif cur is not None:
                st = STATUS_STYLE.get(cur.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
                self._selector_face(st["color"], _candidate_name(cur), st["label"])
            elif current_value:
                self._selector_face("#f59e0b", current_value, "unresolved")
            else:
                self._selector_face("#cbd5e1", "Select source…", "")
            ui.element("div").style("flex: 1 1 0; min-width: 4px;")
            ui.icon("expand_more", size="16px").style("color: #94a3b8; flex-shrink: 0;")

            # `no-parent-event` disables Quasar's implicit open-on-parent-click and
            # we drive the menu explicitly from the selector's click handler below.
            # This keeps opening deterministic even though the anchor is a plain
            # div rather than a button (the codebase's usual QMenu host).
            menu = (
                ui.menu()
                .props('no-parent-event anchor="bottom left" self="top left"')
                .style(
                    "border: 1px solid #e2e8f0; border-radius: 6px; box-shadow: 0 8px 24px rgba(15,23,42,0.14); "
                    "min-width: 240px;"
                )
            )
            with menu:
                if not candidates:
                    ui.label("No upstream producer yet — pick a manual path below.").style(
                        f"{FONT} font-size: 10px; color: #94a3b8; padding: 6px 10px; font-style: italic;"
                    )
                for c in candidates:
                    self._render_menu_candidate(slot, c, menu, selected=(c.source_key == current_value))
                ui.element("div").style("height: 1px; background: #eef2f6; margin: 2px 0;")
                self._render_menu_row(
                    slot,
                    menu,
                    "manual",
                    "#94a3b8",
                    "Manual path…",
                    "browse for a file",
                    selected=(current_value == "manual"),
                )

        selector.on("click", lambda _e, m=menu: m.open())

    def _selector_face(self, color: str, name: str, status_word: str):
        ui.element("div").style(f"width: 7px; height: 7px; border-radius: 50%; background: {color}; flex-shrink: 0;")
        ui.label(name).style(
            f"{FONT} font-size: 10px; color: #334155; min-width: 0; "
            "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        )
        if status_word:
            ui.label(status_word).style(f"{FONT} font-size: 8px; color: #94a3b8; flex-shrink: 0;")

    def _render_menu_candidate(self, slot, c, menu, selected: bool):
        st = STATUS_STYLE.get(c.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
        self._render_menu_row(slot, menu, c.source_key, st["color"], _candidate_name(c), st["label"], selected=selected)

    def _render_menu_row(self, slot, menu, value, color, name, status_word, selected: bool):
        bg = "background: #eef2f6;" if selected else ""
        row = (
            ui.element("div")
            .classes("cb-io-menu-item")
            .style(f"display: flex; align-items: center; gap: 8px; padding: 5px 10px; cursor: pointer; {bg}")
        )
        row.on("click", lambda _e, s=slot, v=value, m=menu: self._select_source(s, v, m))
        with row:
            ui.element("div").style(
                f"width: 8px; height: 8px; border-radius: 50%; background: {color}; flex-shrink: 0;"
            )
            ui.label(name).style(
                f"{FONT} font-size: 11px; color: #1e293b; flex: 1 1 0; min-width: 0; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
            )
            if status_word:
                ui.label(status_word).style(
                    f"{FONT} font-size: 8px; font-weight: 600; color: {color}; "
                    "text-transform: uppercase; flex-shrink: 0;"
                )

    def _select_source(self, slot, value, menu):
        try:
            menu.close()
        except Exception:
            pass
        self._handle_source_change(slot, value)

    # ── Output slots ─────────────────────────────────────────────────────────

    def _render_output_slots(self, schema, job_model):
        job_dir, job_dir_pred = self._get_job_dir(job_model)

        if job_dir and not job_dir_pred:
            with ui.row().classes("w-full items-center gap-2").style("min-height: 20px; margin-bottom: 2px;"):
                ui.element("div").style("width: 6px; flex-shrink: 0;")
                ui.label("Job folder").style(f"{FONT} font-size: 9px; color: #94a3b8; flex-shrink: 0; width: 130px;")
                ui.label(str(job_dir)).style(
                    f"{MONO} font-size: 10px; color: #64748b; flex: 1 1 0; min-width: 0; "
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

            pending = _is_pending_path(full_path)
            # Name on top, full path (wrapping, copyable) beneath — one slot after
            # another so long absolute paths are always fully visible, never truncated.
            with ui.column().classes("w-full").style("gap: 0; margin-bottom: 2px;"):
                with ui.row().classes("w-full items-center gap-2").style("min-height: 22px;"):
                    ui.element("div").style(
                        "width: 6px; height: 6px; border-radius: 50%; border: 1.5px solid #cbd5e1; flex-shrink: 0;"
                    )
                    self._output_name_label(slot, bool(resolved))
                with ui.row().classes("w-full items-start").style("padding-left: 20px;"):
                    if pending or not full_path:
                        ui.label(str(getattr(slot, "path_template", "")) or "—").style(
                            f"{MONO} font-size: 10px; color: #94a3b8; font-style: italic;"
                        )
                    else:
                        copyable_path(
                            full_path, color=("#64748b" if resolved else "#94a3b8"), copy_tooltip="Copy output path"
                        )

    def _output_name_label(self, slot, resolved: bool):
        produces = getattr(slot, "produces", None)
        info = _FILETYPE_INFO.get(produces)
        hname = info[0] if info else (produces.value if produces else "")
        name = ui.label(snake_to_title(slot.key)).style(_SLOT_NAME_STYLE)
        with name, ui.tooltip().style(_TIP_PANEL_STYLE):
            ui.label(snake_to_title(slot.key)).style(_TIP_TITLE)
            if hname:
                ui.label(hname).style(_TIP_KEY)
            if info and info[1]:
                ui.label(info[1]).style(_TIP_DESC + " margin-top: 2px;")
            if not resolved:
                ui.label("Predicted — exists once this job finishes.").style(
                    f"{FONT} font-size: 10px; color: #fbbf24; margin-top: 3px;"
                )
        return name

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
        import asyncio

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
        import asyncio

        asyncio.create_task(self._save_and_refresh(slot, project_path))

    def _clear_override(self, slot):
        state = get_project_state()
        project_path = state.project_path
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            return
        if hasattr(job_model, "source_overrides") and job_model.source_overrides:
            job_model.source_overrides.pop(slot.key, None)
        import asyncio

        asyncio.create_task(self._save_and_refresh(slot, project_path))

    async def _save_and_refresh(self, slot, project_path):
        # The source-override handlers mutate job_model.source_overrides in place,
        # which bypasses ProjectState's dirty tracking — so force the write or the
        # user's source pick / manual path silently vanishes on the next load.
        await get_state_service().save_project(project_path=project_path, force=True)
        container = self._slot_containers.get(slot.key)
        job_model = None
        if container:
            state = get_project_state_for(project_path)
            job_model = state.jobs.get(self.instance_id)
        if container and job_model is not None:
            resolver = PathResolutionService(state, active_instance_ids=self.active_instance_ids)
            container.clear()
            with container:
                self._render_input_slot_row(resolver, slot, job_model)
        if self.on_change:
            self.on_change()

    async def _open_file_picker(self, slot):
        from ui.local_file_picker import local_file_picker

        state = get_project_state()
        overrides = getattr(state.jobs.get(self.instance_id), "source_overrides", {}) or {}
        cur = overrides.get(slot.key, "")
        cur_path = cur[7:] if isinstance(cur, str) and cur.startswith("manual:") else ""
        if cur_path:
            start = str(Path(cur_path).parent)
        elif state.project_path:
            start = str(state.project_path)
        else:
            start = "~"
        result = await local_file_picker(directory=start, mode="file")
        if result:
            self._handle_manual_path_change(slot, result[0])


def render_io_config(
    job_type: JobType,
    instance_id: str,
    on_change: Optional[Callable[[], None]] = None,
    active_instance_ids: Optional[set] = None,
    read_only: bool = False,
):
    component = IOConfigComponent(job_type, instance_id, on_change, active_instance_ids, read_only=read_only)
    component.render()
