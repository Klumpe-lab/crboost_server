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

import asyncio
import logging
from pathlib import Path
from collections.abc import Callable

from nicegui import ui

from services.io_slots import JobFileType
from services.models_base import InstanceId, JobType, JobStatus
from services.particles.particle_sets import list_sets, reads_by_facets, set_lineage, set_read_by
from services.particles.set_facets import ALL, IMAGES, INPUT_DIR, PICKED, POSES, facet_of, poses_choice, resolve
from services.particles.set_preflight import last_check
from services.path_resolution_service import (
    PathResolutionService,
    InputSlotValidation,
    InputState,
    auto_exclusion,
    unreadable_reason,
)
from backend import get_backend
from services.job_params import reset_inputs, set_gain, set_input
from services.project_state import get_project_state_for
from ui.current_project import current_project_state
from ui.ui_state import get_job_display_name
from ui.utils import snake_to_title
from ui.components.buttons import house_button, icon_button
from ui.components.copyable import copyable_path
from ui.components.reactive import FocusHold, owned_timer
from ui.job_plugins._field_styles import LABEL_STYLE, LABEL_W, config_section

logger = logging.getLogger(__name__)

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
    JobFileType.SEGMENTATIONS: (
        "Segmentations",
        "segmentations.json: feature probability volumes per tomogram, with their provenance (segment).",
    ),
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
_TIP_LEAD = f"{FONT} font-size: 10px; color: #334155; margin-top: 1px;"
_TIP_KEY = f"{FONT} font-size: 10px; font-weight: 600; color: #1e293b;"
_TIP_DESC = f"{FONT} font-size: 10px; color: #475569; line-height: 1.35;"
_TIP_PATH = f"{MONO} font-size: 10px; color: #334155; word-break: break-all; white-space: normal;"
_GREEN = "#10b981"
_TEXT = "#334155"
# A facet line under a kitchen set's row: its name in a column of its own, a dot, what it reads.
_FACET_ROW = "display: flex; align-items: center; gap: 6px; min-width: 0; flex-wrap: wrap;"
_FACET_NAME = f"{_SMALL} color: {_PATH_COLOR}; width: 40px; flex-shrink: 0;"
_PICK_BOX = (
    "display: flex; align-items: center; gap: 4px; cursor: pointer; padding: 0 5px; border: 1px solid #e2e8f0; "
    "border-radius: 4px; background: #fff; height: 18px; box-sizing: border-box;"
)
_MENU_STYLE = (
    "border: 1px solid #e2e8f0; border-radius: 6px; box-shadow: 0 8px 24px rgba(15,23,42,0.14); min-width: 240px;"
)
_FACET_WORD = {IMAGES: "particle images", POSES: "poses"}


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
        callbacks: dict[str, Callable] | None = None,
    ):
        self.job_type = job_type
        self.instance_id = instance_id
        self.on_change = on_change
        self.active_instance_ids = active_instance_ids
        # The workspace's callbacks: a greyed set's "+ Subtomo extraction" adds a job through them.
        self.callbacks = callbacks or {}
        # read_only: a completed/frozen job. Same sectioned layout, but the
        # source selectors become static faces and the edit affordances (manual
        # path, reset, gain browse) are hidden.
        self.read_only = read_only
        self._inputs_read_only = read_only
        self._validation_cache: dict[str, InputSlotValidation] = {}
        self._slot_containers: dict[str, ui.element] = {}
        self._gain_container: ui.element | None = None
        self._input_keys: list[str] = []  # the inputs the job had at the last render
        # What the section showed at its last render, for observe(); and whether the user is in its path
        # field or one of its menus, which a redraw would take from under them.
        self._shown: tuple | None = None
        self._hold = FocusHold()

    def render(self):
        # The Inputs and Outputs sections keep the Config tab's section gap.
        self._root = ui.column().classes("w-full").style("gap: 10px;")
        with self._root:
            self._render_body()

    def _signature(self) -> tuple | None:
        """Everything the section draws from the project, in memory: the job's inputs as set and as fixed, the
        jobs it could read and their state, the roster, the species and pick lists, the gain reference."""
        state = current_project_state()
        jm = state.jobs.get(self.instance_id)
        if jm is None:
            return None
        return (
            jm.execution_status,
            jm.inputs_fixed,
            tuple(sorted((jm.source_overrides or {}).items())),
            tuple(sorted((k, tuple(sorted(v.items()))) for k, v in (jm.facet_choices or {}).items())),
            tuple((iid, m.execution_status, m.relion_job_name) for iid, m in state.jobs.items()),
            tuple(state.roster()),
            state.registry_rev,
            state.acquisition.gain_reference_path,
            state.active_merge_slug,
            len(state.aggregation_merges),
            state.imported_tomograms.star_path if state.imported_tomograms else None,
        )

    def observe(self) -> None:
        """The page's poll: draw the section again when something it shows changed elsewhere (another tab,
        the API), except while the user is in its path field or one of its menus."""
        if self._root.is_deleted or self._hold.held:
            return
        if self._signature() == self._shown:
            return
        self._root.clear()
        with self._root:
            self._render_body()

    def _render_body(self):
        state = current_project_state()
        job_model = state.jobs.get(self.instance_id)
        if not job_model:
            ui.label(f"Job instance '{self.instance_id}' not initialized").classes("text-red-500 italic")
            return

        resolver = PathResolutionService(state, active_instance_ids=self.active_instance_ids)
        input_schema = resolver.get_input_schema_for_job(self.job_type, job_model)
        self._input_keys = [s.key for s in input_schema]
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
        self._shown = self._signature()

    def refresh_if_inputs_changed(self) -> None:
        """Re-render when the job's inputs changed with its parameters (an input that exists only while
        a parameter is set, `InputSlot.when`); otherwise nothing, so no control is rebuilt under the user."""
        state = current_project_state()
        job_model = state.jobs.get(self.instance_id)
        if job_model is None or self._root.is_deleted:
            return
        keys = [s.key for s in PathResolutionService(state).get_input_schema_for_job(self.job_type, job_model)]
        if keys == self._input_keys:
            return
        self._root.clear()
        with self._root:
            self._render_body()

    async def _reset_inputs(self):
        state = current_project_state()
        res = reset_inputs(state, self.instance_id)
        if not res["success"]:
            ui.notify(res["error"], type="warning")
            return
        await get_backend().save_project(state.project_path)
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
                icon_button("folder_open", self._pick_gain, tooltip="Browse for gain reference…")
                if is_set:
                    icon_button("close", self._clear_gain, tooltip="Clear", kind="danger")

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
        res = await set_gain(state, value)
        if not res["success"]:
            ui.notify(res["error"], type="warning")
            return
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
                    icon_button("restart_alt", lambda s=slot: self._clear_override(s), tooltip="Reset to auto")
            if current_value is not None:
                self._input_state_tag(validation)

        # Manual-path editor (editable mode only), in the same box as the Parameters inputs.
        manual_editing = current_value == "manual" and not self._inputs_read_only
        if manual_editing:
            with ui.element("div").style(_DETAIL_STYLE + " display: flex; align-items: center; gap: 4px;"):
                inp = self._hold.watch(
                    ui.input(
                        value=manual_path,
                        placeholder="/path/to/file",
                        on_change=lambda e, s=slot: self._handle_manual_path_change(s, e.value),
                    )
                )
                inp.classes("cb-select").props(
                    "dense borderless hide-bottom-space "
                    "input-style=\"font-family: 'IBM Plex Mono', monospace; font-size: 11px; "
                    'color: #1e293b; padding: 1px 2px; min-height: 0;"'
                )
                inp.style("flex: 1 1 0; min-width: 0;")
                icon_button("folder_open", lambda s=slot: self._open_file_picker(s), tooltip="Browse…")

        # Full resolved input path — always visible (wraps, never truncated); a click
        # copies it. Skipped only while the manual editor above already shows it.
        if validation.resolved_path and not manual_editing:
            with ui.element("div").style(_DETAIL_STYLE + " display: flex;"):
                copyable_path(validation.resolved_path, color=_PATH_COLOR, copy_tooltip="Copy input path")
            if validation.awaiting_upstream:
                _detail_line("not written yet — produced when the upstream job runs", italic=True)
            elif not validation.file_exists:
                _detail_line("path resolved, file not on disk yet", italic=True)
        self._render_set_lines(slot, job_model)

        # Why there is no source: a tie, or an override that stopped resolving.
        if validation.tied:
            names = ", ".join(_candidate_name(c) for c in validation.tied)
            _detail_line(f"{len(validation.tied)} equally good sources: {names}. Pick one.", _AMBER)
        elif (
            validation.input_state != InputState.AUTOMATIC and validation.error_message and not validation.resolved_path
        ):
            _detail_line(validation.error_message, STATUS_STYLE[JobStatus.FAILED]["color"])

    def _render_set_lines(self, slot, job_model) -> None:
        """Under an input that reads a particle set: where the set came from (its subsets, kitchen, projects), for a
        kitchen set where each facet the job takes of its picks comes from, and what the set checks of the last Run
        found (`services/particles/set_preflight.py`)."""
        set_id = set_read_by(job_model, slot.key)
        project = current_project_state().project_path
        if set_id is None or project is None:
            return
        pset = next((s for s in list_sets(Path(project)) if s.id == set_id), None)
        if pset is not None:
            projects = ", ".join(pset.details.get("per_project") or {})
            text = f"{pset.id} · {set_lineage(pset)}" + (f": {projects}" if projects else "")
            _detail_line(text, "#334155")
            if reads_by_facets(pset):
                self._render_facets(slot, job_model, pset)
        check = last_check(project, self.instance_id)
        if check is None or check.set_id != set_id:
            return
        red = STATUS_STYLE[JobStatus.FAILED]["color"]
        for problem in check.problems:
            _detail_line(f"Run refused ({check.at}): {problem}", red)
        for note in check.notes:
            _detail_line(f"checked at Run ({check.at}): {note}")

    # ── A kitchen set's facets (one line each under the set's row) ───────────────────────────

    def _render_facets(self, slot, job_model, pset) -> None:
        """What the job takes of the kitchen set's picks, one line per facet: images (or poses), the poses those
        carry, the halves. Filled once the set's sources are read, in a thread. A project whose picks leave a
        choice gets an amber menu; images from jobs that made them differently, or none, a red line with the offer of
        one Warp export on the set. A job fixed at submit shows what it read, and the copy of the set it reads."""
        project = current_project_state().project_path
        rec = (job_model.submitted_inputs or {}).get(slot.key) if job_model.inputs_fixed else None
        read = (job_model.paths or {}).get(slot.key, "")
        if rec is not None and not rec.facets:
            _detail_line("read the set as written, as its rows are", _MUTED)
            return
        if rec is not None and Path(read).parent.name == INPUT_DIR:
            with ui.element("div").style(_DETAIL_STYLE + " display: flex; align-items: center; gap: 6px;"):
                ui.label("reads its copy, written at Run:").style(f"{_SMALL} color: {_MUTED}; flex-shrink: 0;")
                copyable_path(read, color=_PATH_COLOR, copy_tooltip="Copy the path of the copy it reads")
        choices = dict(rec.facets) if rec is not None else dict(job_model.facet_choices.get(slot.key) or {})
        box = ui.element("div").style(_DETAIL_STYLE + " display: flex; flex-direction: column; gap: 2px;")
        with box:
            ui.label("finding the jobs that hold its picks…").style(f"{_SMALL} color: {_MUTED}; font-style: italic;")
        editable = not self._inputs_read_only

        async def fill() -> None:
            problem = ""
            try:
                plan = await asyncio.to_thread(resolve, Path(project), pset, facet_of(slot), choices)
            except (OSError, ValueError) as e:
                logger.exception("The facets of set %s could not be read for %s", pset.id, self.instance_id)
                plan, problem = None, str(e)
            if box.is_deleted:
                return
            box.clear()
            with box:
                if plan is None:
                    red = STATUS_STYLE[JobStatus.FAILED]["color"]
                    self._facet_line("", red, f"where its picks are cannot be read: {problem}")
                else:
                    self._facet_body(slot, job_model, pset, plan, editable)

        owned_timer(0, fill, box, once=True)

    def _facet_line(self, name: str, color: str, text: str, tip: str | None = None) -> ui.element:
        """One facet line: its name, a dot, what it reads. Returns the row, for a menu or button after the text."""
        row = ui.element("div").style(_FACET_ROW)
        with row:
            ui.label(name).style(_FACET_NAME)
            ui.element("div").style(f"{_DOT} background: {color};")
            label = ui.label(text).style(f"{_SMALL} color: {_TEXT if color == _GREEN else color}; white-space: normal;")
            if tip:
                label.tooltip(tip)
        return row

    def _facet_body(self, slot, job_model, pset, plan, editable: bool) -> None:
        red = STATUS_STYLE[JobStatus.FAILED]["color"]
        word = _FACET_WORD[plan.facet]
        name = "images" if plan.facet == IMAGES else "poses"
        asks = [p for p in plan.projects if p.how == "ask"]
        for p in plan.projects:
            if p.how == "ask":
                text = (
                    f"{poses_choice(plan, p)}:"
                    if plan.facet == POSES
                    else f"{p.label}: {len(p.candidates)} jobs hold {word} of its picks:"
                )
                row = self._facet_line(name, _AMBER, text)
                if editable:
                    with row:
                        self._facet_menu(slot, plan, p)
            elif p.how == "none":
                row = self._facet_line(name, red, f"{p.label}: no {word} of its {p.picks:,} picks yet")
                if editable:
                    with row:
                        self._set_export_button(slot, job_model, pset, "Add a Warp export reading this set")
            elif p.how == "gone":
                self._facet_line(name, red, f"{p.label}: the job chosen for it holds none of its picks now")
        common = [r for r in asks[0].candidates if all(r in p.candidates for p in asks)] if len(asks) > 1 else []
        if editable and common:
            with ui.element("div").style(f"{_FACET_ROW} padding-left: 46px;"):
                for ref in common:
                    s = plan.sources[ref]
                    house_button(
                        f"{s.label} for all {len(asks)}",
                        lambda _e, r=ref: self._choose_facet(slot, {p.key: r for p in asks}),
                        tooltip=f"Take {word} from {s.label} for each of {', '.join(p.label for p in asks)}",
                    )
        if plan.differ:
            self._facet_line(name, red, "differ: " + "; ".join(plan.differ), tip=self._sources_tip(plan))
            if editable:
                with ui.element("div").style("padding-left: 46px;"):
                    self._set_export_button(slot, job_model, pset, "Run one Warp export on this set")
        if not plan.problems:
            self._facet_line(name, _GREEN, self._facet_summary(plan), tip=self._sources_tip(plan))
        elif not any(p.how in ("ask", "none", "gone") for p in plan.projects) and not plan.differ:
            for problem in plan.problems:
                self._facet_line(name, red, problem)
        if plan.facet == IMAGES:
            self._facet_line("poses", _GREEN, "as the image rows carry them")
        h1, h2 = plan.halves
        self._facet_line("halves", _GREEN, f"from the set, by pick uid · {h1:,} / {h2:,}")
        for note in plan.notes[:3]:
            _detail_line(note)
        if len(plan.notes) > 3:
            ui.label(f"and {len(plan.notes) - 3} more notes").style(f"{_SMALL} color: {_MUTED};").tooltip(
                "\n".join(plan.notes[3:])
            )
        chosen_here = job_model.facet_choices.get(slot.key) or {}
        if editable and chosen_here:
            labels = "; ".join(
                f"{'the set' if k == ALL else Path(k).name}: {self._ref_label(plan, r)}" for k, r in chosen_here.items()
            )
            row = self._facet_line("", _MUTED, f"chosen here: {labels}")
            with row:
                icon_button("restart_alt", lambda: self._choose_facet(slot, None), tooltip="Forget these choices")

    @staticmethod
    def _ref_label(plan, ref: str) -> str:
        s = plan.sources.get(ref)
        if s is not None:
            return s.label
        return "as picked" if ref == PICKED else ref.partition("|")[2]

    @staticmethod
    def _geometry_text(source) -> str:
        geo = dict(source.geometry)
        if "rlnImageSize" not in geo:
            return ""
        return f"box {geo['rlnImageSize']} · {geo.get('rlnImagePixelSize', '?')} Å"

    def _facet_summary(self, plan) -> str:
        chosen = plan.chosen_sources()
        dropped = plan.n_picks - plan.n_held
        count = f"{plan.n_held:,} of {plan.n_picks:,} picks" + (f" · {dropped:,} left out" if dropped else "")
        if plan.facet != IMAGES:
            head = "as picked (the set's own angles)" if chosen[0].ref == PICKED else f"from {chosen[0].label}"
            return f"{head} · {count}"
        kinds = {s.job_type for s in chosen}
        if len(chosen) == 1:
            jobs = chosen[0].label
        elif len(kinds) == 1:
            jobs = f"{get_job_display_name(JobType(kinds.pop()))} ×{len(chosen)}"
        else:
            jobs = f"{len(chosen)} jobs"
        geometry = self._geometry_text(chosen[0])
        return " · ".join(x for x in (jobs, geometry, count) if x)

    @staticmethod
    def _sources_tip(plan) -> str:
        """Per project, the job it reads and how many of its picks that job holds."""
        lines = []
        for p in plan.projects:
            s = plan.sources.get(p.chosen) if p.chosen else None
            if s is None:
                lines.append(f"{p.label}: {p.picks:,} picks, nothing chosen")
                continue
            how = " (matched by position)" if s.by == "position" else ""
            lines.append(f"{p.label}: {s.label}{how} · {p.held:,} of {p.picks:,} picks")
        return "\n".join(lines)

    def _facet_menu(self, slot, plan, pf) -> None:
        """A small "pick one" box opening `pf`'s candidate sources, each with its images' size and what it holds."""
        box = ui.element("div").style(_PICK_BOX)
        with box:
            ui.label("pick one").style(f"{_SMALL} color: {_AMBER};")
            ui.icon("expand_more", size="12px").style(f"color: {_MUTED};")
            menu = ui.menu().props('no-parent-event anchor="bottom left" self="top left"').style(_MENU_STYLE)
            self._hold.watch_open(menu)
            with menu:
                what = _FACET_WORD[plan.facet]
                ui.label(f"{what} for {pf.label}'s {pf.picks:,} picks").style(
                    f"{_SMALL} color: {_PATH_COLOR}; padding: 6px 10px 2px;"
                )
                for ref in pf.candidates:
                    s = plan.sources[ref]
                    held = f"{pf.held_by.get(ref, 0):,} of {pf.picks:,} picks"
                    row = (
                        ui.element("div")
                        .classes("cb-io-menu-item")
                        .style("display: flex; align-items: center; gap: 8px; padding: 4px 10px; cursor: pointer;")
                    )
                    row.on("click", lambda _e, r=ref, m=menu: self._pick_facet(slot, {pf.key: r}, m))
                    with row:
                        ui.element("div").style(f"{_DOT} background: {_GREEN};")
                        ui.label(s.label).style(f"{FONT} font-size: 11px; color: #1e293b; flex: 1 1 0; min-width: 0;")
                        detail = " · ".join(x for x in (self._geometry_text(s), held) if x)
                        ui.label(detail).style(f"{_SMALL} color: {_MUTED}; flex-shrink: 0;")
        box.on("click", lambda _e, m=menu: m.open())

    def _pick_facet(self, slot, picked: dict[str, str], menu) -> None:
        menu.close()
        self._choose_facet(slot, picked)

    def _choose_facet(self, slot, picked: dict[str, str] | None) -> None:
        """Record where the job takes the facet from for the projects `picked` names; None forgets every choice."""
        self._write(slot, facet=picked or {})

    def _set_export_button(self, slot, job_model, pset, text: str) -> None:
        house_button(
            text,
            lambda: self._add_set_export(slot, job_model, pset),
            tooltip="Add one Warp export over every pick of the set, with one box and pixel size, and point "
            f"{snake_to_title(slot.key)} at its particles",
        )

    def _add_set_export(self, slot, job_model, pset) -> None:
        add = self.callbacks.get("add_job_reading")
        if add is None:
            ui.notify("Open this job from the pipeline view to add the Warp export.", type="warning")
            return
        species_id = getattr(job_model, "species_id", None) or pset.species_id
        reads = f"{JobType.MERGED_SOURCES.value}:{pset.producer_id}"
        add(JobType.WARP_EXPORT, reads=reads, consumer=self.instance_id, slot=slot.key, species_id=species_id)

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
            self._hold.watch_open(menu)
            with menu:
                if not candidates:
                    ui.label("No upstream producer yet — pick a manual path below.").style(
                        f"{FONT} font-size: 10px; color: {_MUTED}; padding: 6px 10px; font-style: italic;"
                    )
                for c in candidates:
                    blocked = unreadable_reason(self.job_type, slot, c)
                    if blocked:
                        self._render_blocked_candidate(slot, c, menu, blocked)
                        continue
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

    def _render_blocked_candidate(self, slot, c, menu, reason: str):
        """A source this input cannot read: greyed, not selectable, with the reason. One that holds positions
        only offers to add the extraction that reads it, and to point this input at that job's particles."""
        with ui.element("div").style("padding: 4px 10px; cursor: default;"):
            with ui.element("div").style("display: flex; align-items: center; gap: 8px; opacity: 0.5;"):
                ui.element("div").style(f"{_DOT} background: #cbd5e1;")
                ui.label(_candidate_name(c)).style(
                    f"{FONT} font-size: 11px; color: #1e293b; flex: 1 1 0; min-width: 0; "
                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                )
            ui.label(reason).style(f"{_SMALL} color: {_PATH_COLOR}; padding-left: 15px; white-space: normal;")
            if slot.needs_extracted and c.coordinates_only:
                with ui.element("div").style("display: flex; gap: 6px; padding: 3px 0 0 15px;"):
                    for job_type in (JobType.SUBTOMO_EXTRACTION, JobType.WARP_EXPORT):
                        house_button(
                            f"+ {get_job_display_name(job_type)}",
                            lambda _e, j=job_type, c=c, m=menu: self._add_extraction(slot, c, j, m),
                            tooltip=f"Add a {get_job_display_name(job_type)} job reading this, and point "
                            f"{snake_to_title(slot.key)} at its particles",
                        )

    def _add_extraction(self, slot, candidate, job_type: JobType, menu) -> None:
        menu.close()
        add = self.callbacks.get("add_job_reading")
        if add is None:
            ui.notify("Open this job from the pipeline view to add the extraction.", type="warning")
            return
        job_model = current_project_state().jobs.get(self.instance_id)
        species_id = getattr(job_model, "species_id", None) or candidate.species_id
        add(job_type, reads=candidate.source_key, consumer=self.instance_id, slot=slot.key, species_id=species_id)

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
                    f"{FONT} font-size: 10px; color: #b45309; margin-top: 3px;"
                )
        return name

    # ── State updates ────────────────────────────────────────────────────────

    def _write(self, slot, **change) -> None:
        """One input's change through the setting door (`job_params.set_input`), then the save and the row
        drawn again. A new source forgets where a set's picks took their facets from: those choices belong to
        the set they were made for."""
        state = current_project_state()
        res = set_input(state, self.instance_id, slot.key, **change)
        if not res["success"]:
            ui.notify(res["error"], type="negative", timeout=8000)
            return
        asyncio.create_task(self._save_and_refresh(slot, state.project_path))

    def _handle_source_change(self, slot, value):
        if value == "manual":
            self._write(slot, path="")
        elif value:
            self._write(slot, source=value)
        else:
            self._write(slot, auto=True)

    def _handle_manual_path_change(self, slot, path):
        self._write(slot, path=path)

    def _clear_override(self, slot):
        self._write(slot, auto=True)

    async def _save_and_refresh(self, slot, project_path):
        # The door marked the state dirty, so a plain save writes the change.
        await get_backend().save_project(project_path)
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
            self._shown = self._signature()
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
    callbacks: dict[str, Callable] | None = None,
) -> IOConfigComponent:
    component = IOConfigComponent(
        job_type, instance_id, on_change, active_instance_ids, read_only=read_only, callbacks=callbacks
    )
    component.render()
    return component
