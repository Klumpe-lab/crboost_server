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
from collections.abc import Callable

from nicegui import ui

from services.io_slots import JobFileType
from services.models_base import InstanceId, JobType, JobStatus
from services.path_resolution_service import PathResolutionService, InputSlotValidation, InputState, auto_exclusion
from backend import get_backend
from services.project_state import get_project_state_for
from ui.current_project import current_project_state
from ui.ui_state import get_job_display_name
from ui.utils import snake_to_title
from ui.components.buttons import house_button
from ui.components.copyable import copyable_path
from ui.job_plugins._field_styles import LABEL_STYLE, LABEL_W, config_section

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
    JobStatus.QUEUED: {"color": "#60a5fa", "label": "queued"},
    JobStatus.FAILED: {"color": "#ef4444", "label": "failed"},
    JobStatus.UNKNOWN: {"color": "#94a3b8", "label": "pending"},
}

# An ambiguous slot (two equally good sources) and a hand-picked source automatic wiring would skip.
_AMBER = "#d97706"

# Why automatic wiring skips a source (services.path_resolution_service.auto_exclusion), as hover text.
_EXCLUSION_TIPS = {
    "other species": "Bound to another species. Automatic wiring never picks it; only an explicit choice does.",
    "not upstream": (
        "Runs at the same or a later pipeline stage. Automatic wiring never picks it; only an explicit choice does."
    ),
}

# Human name + one-line meaning per artifact type, for the input-slot hover.
# Sourced from the inline path comments in services/io_slots.py.
_FILETYPE_INFO: dict[JobFileType, tuple] = {
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
# Slot names use the Parameters label column, so the three Config sections line up.
# Long slot names wrap rather than truncate.
_SLOT_NAME_STYLE = LABEL_STYLE + " white-space: normal; cursor: help;"
# One row shape for gain, input and output rows; detail lines sit under the value column.
_ROW_STYLE = "display: flex; align-items: center; gap: 8px; width: 100%; min-width: 0; min-height: 22px;"
_DETAIL_STYLE = f"padding-left: {LABEL_W + 8}px; width: 100%;"
_PATH_COLOR = "#64748b"
_MUTED = "#94a3b8"
# Secondary text (status words, notes, tags) is one size everywhere.
_SMALL = f"{FONT} font-size: 9px;"
_DOT = "width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0;"
# The app tooltip's panel and frame come from ui/dashboard/css.py; only the room is set here.
_TIP_PANEL_STYLE = "padding: 8px 10px; max-width: 380px;"
_TIP_TITLE = f"{FONT} font-size: 11px; font-weight: 700; color: #0f172a;"
_TIP_LEAD = f"{FONT} font-size: 10px; color: #475569; margin-top: 1px;"
_TIP_KEY = f"{FONT} font-size: 10px; font-weight: 600; color: #1e293b;"
_TIP_DESC = f"{FONT} font-size: 10px; color: #64748b; line-height: 1.35;"
_TIP_PATH = f"{MONO} font-size: 10px; color: #475569; word-break: break-all; white-space: normal;"


def _safe_int(x) -> int | None:
    try:
        return int(x)
    except Exception:
        return None


def _detail_line(text: str, color: str = _MUTED, *, italic: bool = False) -> None:
    """A note or problem under an input row, aligned with the value column."""
    with ui.element("div").style(_DETAIL_STYLE):
        ui.label(text).style(f"{_SMALL} color: {color};" + (" font-style: italic;" if italic else ""))


def _short_instance_label(instance_path: str) -> str:
    try:
        p = Path(instance_path.strip("/"))
        return p.name if p.name else instance_path
    except Exception:
        return instance_path


def _is_pending_path(p: str | None) -> bool:
    return bool(p) and "pending_" in p


def _candidate_name(c) -> str:
    """Human, disambiguated name for one upstream producer candidate.

    Two queued jobs of the same type (e.g. two Alignments) must not collapse to
    the same label. We build "<Display Name>" plus a per-instance discriminator:
    the instance-id suffix for not-yet-reconciled jobs, or the RELION job folder
    (job005) once the job exists on disk.
    """
    if getattr(c, "label", None):
        return c.label
    base = get_job_display_name(JobType(c.producer_job_type.value))
    iid = getattr(c, "producer_instance_id", "") or ""
    suffix = InstanceId.split(iid)[1]
    if suffix is not None:
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
        on_change: Callable[[], None] | None = None,
        active_instance_ids: set | None = None,
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
        self._inputs_read_only = read_only
        self._validation_cache: dict[str, InputSlotValidation] = {}
        self._slot_containers: dict[str, ui.element] = {}
        self._gain_container: ui.element | None = None

    def render(self):
        # The Inputs and Outputs sections keep the Config tab's section gap.
        self._root = ui.column().classes("w-full").style("gap: 18px;")
        with self._root:
            self._render_body()

    def _render_body(self):
        state = current_project_state()
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            ui.label(f"Job instance '{self.instance_id}' not initialized").classes("text-red-500 italic")
            return

        resolver = PathResolutionService(state, active_instance_ids=self.active_instance_ids)
        input_schema = resolver.get_input_schema_for_job(self.job_type)
        output_schema = resolver.get_output_schema_for_job(self.job_type, job_model)
        wants_gain = self.job_type in GAIN_CONSUMERS
        # A job the chain submitted reads what was resolved then; only a Failed one may choose again.
        fixed = job_model.inputs_fixed
        # What a Subset cuts is set when it is made; another cut is another Subset.
        self._inputs_read_only = self.read_only or fixed or self.job_type == JobType.SUBSET

        # Two Config-tab sections of their own, at the same scale as Parameters / SLURM resources.
        if input_schema or wants_gain:
            with config_section("Inputs"):
                if fixed and job_model.execution_status == JobStatus.FAILED:
                    with ui.element("div").style(_ROW_STYLE):
                        house_button(
                            "Reset inputs",
                            self._reset_inputs,
                            tooltip="Choose this job's inputs again at the next Run instead of the ones fixed at "
                            "its last submit. If they come out different, the job starts over in a new folder; "
                            "otherwise it resumes where it stopped.",
                        )
                mixed = resolver.mixed_chains(self.instance_id) if input_schema else None
                if mixed:
                    ui.label(
                        f"Reads from {mixed[0]} and {mixed[1]}, and neither is upstream of the other: "
                        "its inputs come from two chains."
                    ).style(f"{_SMALL} color: {_AMBER};")
                if wants_gain:
                    self._gain_container = ui.column().classes("w-full gap-0")
                    with self._gain_container:
                        self._render_gain_row()
                if input_schema:
                    self._render_input_slots(resolver, input_schema, job_model)
        if output_schema:
            with config_section("Outputs"):
                self._render_output_slots(output_schema, job_model)

    async def _reset_inputs(self):
        state = current_project_state()
        job_model = state.jobs.get(self.instance_id)
        if job_model is None:
            return
        job_model.submitted_inputs = None
        await get_backend().save_project(state.project_path, force=True)
        self._root.clear()
        with self._root:
            self._render_body()
        if self.on_change:
            self.on_change()

    def _get_job_dir(self, job_model) -> tuple[Path | None, bool]:
        state = current_project_state()
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
        state = current_project_state()
        gain = getattr(state.acquisition, "gain_reference_path", None)
        is_set = bool(gain and str(gain).strip() and str(gain) != "None")

        with ui.element("div").style(_ROW_STYLE):
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
                copyable_path(str(gain), color=_PATH_COLOR, copy_tooltip="Copy gain path")
            else:
                ui.label("not set · optional").style(
                    f"{FONT} font-size: 10px; color: {_MUTED}; font-style: italic; flex: 1 1 0; min-width: 0;"
                )
            if not self.read_only:
                ui.button(icon="folder_open", on_click=self._pick_gain).props("flat dense round size=xs").style(
                    f"color: {_PATH_COLOR}; flex-shrink: 0;"
                ).tooltip("Browse for gain reference…")
                if is_set:
                    ui.button(icon="close", on_click=self._clear_gain).props("flat dense round size=xs").style(
                        f"color: {_MUTED}; flex-shrink: 0;"
                    ).tooltip("Clear")

    async def _pick_gain(self):
        from ui.local_file_picker import local_file_picker

        state = current_project_state()
        cur = getattr(state.acquisition, "gain_reference_path", None)
        start = str(Path(cur).parent) if cur else (str(state.project_path) if state.project_path else "~")
        result = await local_file_picker(directory=start, mode="file")
        if result:
            await self._set_gain(result[0])

    async def _clear_gain(self):
        await self._set_gain(None)

    async def _set_gain(self, value: str | None):
        state = current_project_state()
        project_path = state.project_path
        state.acquisition.gain_reference_path = value or None
        state.mark_dirty()
        await get_backend().save_project(project_path)
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
        species_id = getattr(job_model, "species_id", None)

        candidates = resolver.get_candidates_for_slot(
            self.job_type, slot.key, consumer_species_id=species_id, consumer_instance_id=self.instance_id
        )
        # Sources automatic wiring would skip; an explicit choice may still name them.
        skipped = {c.source_key: auto_exclusion(self.job_type, species_id, c) for c in candidates}
        validation = resolver.validate_input_slot(
            self.job_type, job_model, slot.key, check_filesystem=True, consumer_instance_id=self.instance_id
        )
        self._validation_cache[slot.key] = validation

        # The source the slot reads: the recorded one on a job fixed at submit, else the override.
        key = validation.source_key
        if key and key.startswith("manual:"):
            current_value = "manual"
            manual_path = key[len("manual:") :]
        elif key:
            current_value = key
            manual_path = ""
        else:
            current_value = None
            manual_path = ""

        with ui.element("div").style(_ROW_STYLE):
            self._input_name_label(slot)
            if self._inputs_read_only:
                self._render_static_source(slot, candidates, current_value, validation, skipped)
            else:
                self._render_source_selector(slot, candidates, current_value, validation, skipped)
                if not slot.required:
                    ui.label("optional").style(f"{_SMALL} color: {_MUTED}; flex-shrink: 0;")
                if validation.is_user_override:
                    (
                        ui.button(icon="restart_alt", on_click=lambda s=slot: self._clear_override(s))
                        .props("flat dense round size=xs")
                        .style(f"color: {_MUTED}; flex-shrink: 0;")
                        .tooltip("Reset to auto")
                    )
            if current_value is not None:
                self._input_state_tag(validation)

        # Manual-path editor (editable mode only), in the same box as the Parameters inputs.
        manual_editing = current_value == "manual" and not self._inputs_read_only
        if manual_editing:
            with ui.element("div").style(_DETAIL_STYLE + " display: flex; align-items: center; gap: 4px;"):
                inp = ui.input(
                    value=manual_path,
                    placeholder="/path/to/file",
                    on_change=lambda e, s=slot: self._handle_manual_path_change(s, e.value),
                )
                inp.classes("cb-select").props(
                    "dense borderless hide-bottom-space "
                    "input-style=\"font-family: 'IBM Plex Mono', monospace; font-size: 11px; "
                    'color: #1e293b; padding: 1px 2px; min-height: 0;"'
                )
                inp.style("flex: 1 1 0; min-width: 0;")
                ui.button(icon="folder_open", on_click=lambda s=slot: self._open_file_picker(s)).props(
                    "flat dense round size=xs"
                ).style(f"color: {_PATH_COLOR};").tooltip("Browse…")

        # Full resolved input path — always visible (wraps, never truncated) with
        # a copy icon. Skipped only while the manual editor above already shows it.
        if validation.resolved_path and not manual_editing:
            with ui.element("div").style(_DETAIL_STYLE + " display: flex;"):
                copyable_path(validation.resolved_path, color=_PATH_COLOR, copy_tooltip="Copy input path")
            if validation.awaiting_upstream:
                _detail_line("not written yet — produced when the upstream job runs", italic=True)
            elif not validation.file_exists:
                _detail_line("path resolved, file not on disk yet", italic=True)

        # Why there is no source: a tie, or an override that stopped resolving.
        if validation.tied:
            names = ", ".join(_candidate_name(c) for c in validation.tied)
            _detail_line(f"{len(validation.tied)} equally good sources: {names}. Pick one.", _AMBER)
        elif (
            validation.input_state != InputState.AUTOMATIC and validation.error_message and not validation.resolved_path
        ):
            _detail_line(validation.error_message, STATUS_STYLE[JobStatus.FAILED]["color"])

    def _input_state_tag(self, validation):
        """How the source was chosen, as a quiet word beside it."""
        tips = {
            InputState.AUTOMATIC: "Chosen by automatic wiring. It can still change until this job is submitted.",
            InputState.SET_BY_HAND: "Set in this section. It stays until you change it.",
            InputState.FIXED_AT_SUBMIT: "Recorded when this job was submitted: it reads this, whatever is added "
            "to the project since.",
        }
        word, tip = validation.input_state.value, tips[validation.input_state]
        if validation.input_state == InputState.AUTOMATIC and validation.refined_by:
            word = f"follows {validation.refined_by}"
            tip = f"{validation.refined_by} refines the source automatic wiring found, so this input reads its output."
        ui.label(word).style(f"{_SMALL} color: {_MUTED}; flex-shrink: 0;").tooltip(tip)

    def _render_current_face(self, slot, candidates, current_value, validation, skipped, empty_name):
        """The chosen source, a tie, or nothing, as one face (shared by the editable selector
        and the read-only view). A hand-picked source automatic wiring would skip carries the
        reason as an amber tag. A required slot with nothing chosen shows a red dot."""
        cur = next((c for c in candidates if c.source_key == current_value), None)
        if current_value == "manual":
            self._selector_face(_MUTED, "Manual path", "custom file")
        elif cur is not None:
            st = STATUS_STYLE.get(cur.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
            self._selector_face(st["color"], _candidate_name(cur), st["label"], tag=skipped.get(cur.source_key))
        elif validation.tied:
            self._selector_face(_AMBER, "Ambiguous — pick a source", f"{len(validation.tied)} candidates")
        elif current_value:
            self._selector_face("#f59e0b", current_value, "unresolved")
        else:
            missing = STATUS_STYLE[JobStatus.FAILED]["color"] if slot.required else "#cbd5e1"
            self._selector_face(missing, empty_name, "")

    def _render_static_source(self, slot, candidates, current_value, validation, skipped):
        """Read-only counterpart of the source selector: the chosen source shown
        as a static face (no menu), for completed/frozen jobs."""
        with ui.element("div").style(
            "flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: 6px; padding: 0 5px; height: 20px;"
        ):
            self._render_current_face(slot, candidates, current_value, validation, skipped, "—")

    def _input_name_label(self, slot):
        # Explains the artifact type only. The concrete resolved path is shown
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

    def _render_source_selector(self, slot, candidates, current_value, validation, skipped):
        selector = (
            ui.element("div")
            .classes("cb-io-src")
            # The .cb-select box: 20 px tall, 1 px slate border, 5 px side padding.
            .style(
                "flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: 6px; "
                "cursor: pointer; padding: 0 5px; border: 1px solid #e2e8f0; border-radius: 4px; "
                "background: #fff; height: 20px; box-sizing: border-box;"
            )
        )
        with selector:
            self._render_current_face(slot, candidates, current_value, validation, skipped, "Select source…")
            ui.element("div").style("flex: 1 1 0; min-width: 4px;")
            ui.icon("expand_more", size="14px").style(f"color: {_MUTED}; flex-shrink: 0;")

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
                        f"{FONT} font-size: 10px; color: {_MUTED}; padding: 6px 10px; font-style: italic;"
                    )
                for c in candidates:
                    self._render_menu_candidate(
                        slot, c, menu, selected=(c.source_key == current_value), skipped=skipped.get(c.source_key)
                    )
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

    def _selector_face(self, color: str, name: str, status_word: str, tag: str | None = None):
        ui.element("div").style(f"{_DOT} background: {color};")
        ui.label(name).style(
            f"{FONT} font-size: 11px; color: #1e293b; min-width: 0; "
            "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        )
        if status_word:
            ui.label(status_word).style(f"{_SMALL} color: {_MUTED}; flex-shrink: 0;")
        if tag:
            ui.label(tag).style(f"{_SMALL} font-weight: 600; color: {_AMBER}; flex-shrink: 0;").tooltip(
                _EXCLUSION_TIPS.get(tag, tag)
            )

    def _render_menu_candidate(self, slot, c, menu, selected: bool, skipped: str | None = None):
        st = STATUS_STYLE.get(c.execution_status, STATUS_STYLE[JobStatus.UNKNOWN])
        self._render_menu_row(
            slot, menu, c.source_key, st["color"], _candidate_name(c), st["label"], selected=selected, skipped=skipped
        )

    def _render_menu_row(self, slot, menu, value, color, name, status_word, selected: bool, skipped: str | None = None):
        """One menu option. `skipped` (an `auto_exclusion` reason) dims a source automatic
        wiring never picks; it stays selectable."""
        bg = "background: #eef2f6;" if selected else ""
        dim = "opacity: 0.55;" if skipped else ""
        row = (
            ui.element("div")
            .classes("cb-io-menu-item")
            .style(f"display: flex; align-items: center; gap: 8px; padding: 4px 10px; cursor: pointer; {bg}{dim}")
        )
        row.on("click", lambda _e, s=slot, v=value, m=menu: self._select_source(s, v, m))
        with row:
            ui.element("div").style(f"{_DOT} background: {color};")
            ui.label(name).style(
                f"{FONT} font-size: 11px; color: #1e293b; flex: 1 1 0; min-width: 0; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
            )
            if skipped:
                ui.label(skipped).style(f"{_SMALL} color: {_PATH_COLOR}; flex-shrink: 0;").tooltip(
                    _EXCLUSION_TIPS.get(skipped, skipped)
                )
            if status_word:
                ui.label(status_word).style(f"{_SMALL} color: {_MUTED}; flex-shrink: 0;")

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
            with ui.element("div").style(_ROW_STYLE):
                ui.label("Job folder").style(LABEL_STYLE)
                copyable_path(str(job_dir), color=_PATH_COLOR, copy_tooltip="Copy job folder")

        for slot in schema:
            resolved = (job_model.paths or {}).get(slot.key)

            full_path: str = ""
            if resolved:
                p = Path(str(resolved))
                state = current_project_state()
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
            # Name left, full path (wrapping, copyable) right, like every other row.
            with ui.element("div").style(_ROW_STYLE):
                self._output_name_label(slot, bool(resolved))
                if pending or not full_path:
                    ui.label(str(getattr(slot, "path_template", "")) or "—").style(
                        f"{MONO} font-size: 10px; color: {_MUTED}; font-style: italic; flex: 1 1 0; min-width: 0;"
                    )
                else:
                    copyable_path(
                        full_path, color=(_PATH_COLOR if resolved else _MUTED), copy_tooltip="Copy output path"
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
        state = current_project_state()
        project_path = state.project_path
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            return
        if not hasattr(job_model, "source_overrides") or job_model.source_overrides is None:
            job_model.source_overrides = {}
        if value and value != "manual":
            loop = PathResolutionService(state).cycle_with_override(self.instance_id, slot.key, value)
            if loop:
                ui.notify(
                    "Not set: this source would close a loop — " + "; ".join(w.describe() for w in loop),
                    type="negative",
                    timeout=8000,
                )
                return
        if value == "manual":
            job_model.source_overrides[slot.key] = "manual:"
        elif value:
            job_model.source_overrides[slot.key] = value
        else:
            job_model.source_overrides.pop(slot.key, None)
        import asyncio

        asyncio.create_task(self._save_and_refresh(slot, project_path))

    def _handle_manual_path_change(self, slot, path):
        state = current_project_state()
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
        state = current_project_state()
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
        await get_backend().save_project(project_path, force=True)
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

        state = current_project_state()
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
    on_change: Callable[[], None] | None = None,
    active_instance_ids: set | None = None,
    read_only: bool = False,
):
    component = IOConfigComponent(job_type, instance_id, on_change, active_instance_ids, read_only=read_only)
    component.render()
