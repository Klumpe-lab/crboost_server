# ui/ui_state.py
"""
Typed UI State Management - Single source of truth for all UI state.

Phase 2: UIStateManager is now stored per-browser-tab via app.storage.tab
instead of a module-level global. This means:
  - Two tabs from the same user get independent UI state
  - Widget refs, active job selection, form inputs don't bleed across tabs
  - The module-level get_ui_state_manager() reads from tab storage
    (must be called within a page handler context after await client.connected())
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING
from pydantic import BaseModel, Field, ConfigDict
from services.project_state import JobType, JobStatus

if TYPE_CHECKING:
    from nicegui.element import Element


class MonitorTab(str, Enum):
    """Explicitly typed tab values."""

    CONFIG = "config"
    LOGS = "logs"
    FILES = "files"


class JobCardUIState(BaseModel):
    """UI state for a single job card. Serializable."""

    model_config = ConfigDict(use_enum_values=True)

    active_monitor_tab: MonitorTab = MonitorTab.CONFIG
    user_switched_tab: bool = False


class DataImportFormState(BaseModel):
    """State for the data import form. Cached for restoration."""

    model_config = ConfigDict(use_enum_values=True)

    project_name: str = ""
    project_base_path: str = ""
    movies_glob: str = ""
    mdocs_glob: str = ""
    import_prefix: str = ""

    # Validation state
    movies_valid: bool = False
    mdocs_valid: bool = False

    # Detected parameters cache
    detected_pixel_size: Optional[float] = None
    detected_voltage: Optional[float] = None
    detected_dose_per_tilt: Optional[float] = None
    detected_tilt_axis: Optional[float] = None


class UIState(BaseModel):
    """
    Complete UI state - typed and validated.
    """

    model_config = ConfigDict(use_enum_values=True, arbitrary_types_allowed=True)

    # --- Project Context ---
    current_project_path: Optional[str] = None
    current_scheme_name: Optional[str] = None
    project_created: bool = False
    continuation_mode: bool = False

    # --- Pipeline Execution ---
    selected_jobs: List[str] = Field(default_factory=list)  # JobType values as strings
    pipeline_running: bool = False

    # --- Navigation ---
    active_job_tab: Optional[str] = None  # JobType value as string

    # --- Per-Job UI State ---
    job_ui_states: Dict[str, JobCardUIState] = Field(default_factory=dict)

    # --- Data Import Form ---
    data_import: DataImportFormState = Field(default_factory=DataImportFormState)

    # --- Timestamps ---
    last_status_refresh: Optional[str] = None


@dataclass
class JobWidgetRefs:
    """
    Non-serializable UI element references for a single job.
    These hold NiceGUI elements that are per-client and per-page-load.
    """

    logs_timer: Optional[Any] = None
    content_container: Optional[Any] = None
    switcher_container: Optional[Any] = None

    monitor_logs: Dict[str, Any] = field(default_factory=dict)

    def cleanup(self):
        """Cancel timers and clear refs."""
        if self.logs_timer:
            try:
                self.logs_timer.cancel()
            except Exception:
                pass
            self.logs_timer = None
        self.content_container = None
        self.switcher_container = None
        self.monitor_logs.clear()


@dataclass
class PanelWidgetRefs:
    """Panel-level widget references."""

    job_tabs_container: Optional[Any] = None
    run_button: Optional[Any] = None
    stop_button: Optional[Any] = None
    status_label: Optional[Any] = None
    continuation_container: Optional[Any] = None
    job_tags_container: Optional[Any] = None
    job_tag_buttons: Dict[str, Any] = field(default_factory=dict)

    # Data import panel refs
    movies_input: Optional[Any] = None
    mdocs_input: Optional[Any] = None
    project_name_input: Optional[Any] = None
    project_path_input: Optional[Any] = None
    create_button: Optional[Any] = None
    load_button: Optional[Any] = None
    autodetect_button: Optional[Any] = None
    params_display_container: Optional[Any] = None

    # Validation hint labels
    movies_hint_label: Optional[Any] = None
    mdocs_hint_label: Optional[Any] = None
    status_indicator: Optional[Any] = None

    def cleanup(self):
        """Clear all refs."""
        self.job_tabs_container = None
        self.run_button = None
        self.stop_button = None
        self.status_label = None
        self.continuation_container = None
        self.job_tags_container = None
        self.job_tag_buttons.clear()
        self.movies_input = None
        self.mdocs_input = None
        self.project_name_input = None
        self.project_path_input = None
        self.create_button = None
        self.load_button = None
        self.autodetect_button = None
        self.params_display_container = None
        self.movies_hint_label = None
        self.mdocs_hint_label = None
        self.status_indicator = None


# Pipeline ordering - centralized
PIPELINE_ORDER: List[JobType] = [
    JobType.IMPORT_MOVIES,
    JobType.FS_MOTION_CTF,
    JobType.TS_ALIGNMENT,
    JobType.TS_CTF,
    JobType.TS_RECONSTRUCT,
    JobType.DENOISE_TRAIN,
    JobType.DENOISE_PREDICT,
    JobType.TEMPLATE_MATCH_PYTOM,
    JobType.TEMPLATE_EXTRACT_PYTOM,
    JobType.SUBTOMO_EXTRACTION,
    JobType.RECONSTRUCT_PARTICLE,
    JobType.CLASS3D,
]

JOB_DISPLAY_NAMES: Dict[JobType, str] = {
    JobType.IMPORT_MOVIES: "Import",
    JobType.FS_MOTION_CTF: "Motion & CTF",
    JobType.TS_ALIGNMENT: "Alignment",
    JobType.TS_CTF: "TS CTF",
    JobType.TS_RECONSTRUCT: "Reconstruct",
    JobType.DENOISE_TRAIN: "Denoise Train",
    JobType.DENOISE_PREDICT: "Denoise Predict",
    JobType.TEMPLATE_MATCH_PYTOM: "Template Match",
    JobType.TEMPLATE_EXTRACT_PYTOM: "Template Extract",
    JobType.SUBTOMO_EXTRACTION: "Subtomo Extraction",
    JobType.RECONSTRUCT_PARTICLE: "Reconstruct Particle",
    JobType.CLASS3D: "Class 3D",
}


def get_job_order(job_type: JobType) -> int:
    """Get the pipeline order index for a job type."""
    try:
        return PIPELINE_ORDER.index(job_type)
    except ValueError:
        return 999


def get_job_display_name(job_type: JobType) -> str:
    """Get human-readable name for a job type."""
    return JOB_DISPLAY_NAMES.get(job_type, job_type.value)


def get_ordered_jobs() -> List[JobType]:
    """Get all available jobs in pipeline order."""
    return PIPELINE_ORDER.copy()


class UIStateManager:
    """
    Centralized UI state manager.

    Phase 2: One instance per browser tab, stored in app.storage.tab.
    Holds all per-tab UI concerns: which job tab is active, widget refs,
    form state, timer refs, subscriber callbacks.
    """

    def __init__(self):
        self._state = UIState()
        self._job_widget_refs: Dict[str, JobWidgetRefs] = {}
        self._panel_refs = PanelWidgetRefs()
        self._subscribers: List[Callable[[UIState], None]] = []
        self._status_timer: Optional[Any] = None
        self._rebuild_callback: Optional[Callable[[], None]] = None

    # ===========================================
    # Persistence Loading
    # ===========================================
    def load_from_storage(self, storage_dict: Dict[str, Any]):
        """Hydrate state from app.storage.user."""
        if not storage_dict:
            return
        try:
            print("[UI_STATE] Hydrating from browser storage...")
            restored_state = UIState(**storage_dict)
            self._state = restored_state

            for job_str in self._state.selected_jobs:
                if job_str not in self._job_widget_refs:
                    self._job_widget_refs[job_str] = JobWidgetRefs()

            self._notify()
        except Exception as e:
            print(f"[UI_STATE] Error hydrating state from storage: {e}")

    # ===========================================
    # Properties for clean access
    # ===========================================

    @property
    def state(self) -> UIState:
        return self._state

    @property
    def panel_refs(self) -> PanelWidgetRefs:
        return self._panel_refs

    @property
    def selected_jobs(self) -> List[JobType]:
        """Get selected jobs as JobType enums."""
        return [JobType(j) for j in self._state.selected_jobs]

    @property
    def active_job(self) -> Optional[JobType]:
        """Get the currently active job tab as JobType."""
        if self._state.active_job_tab:
            return JobType(self._state.active_job_tab)
        return None

    @property
    def is_running(self) -> bool:
        return self._state.pipeline_running

    @property
    def is_project_created(self) -> bool:
        return self._state.project_created

    @property
    def project_path(self) -> Optional[Path]:
        if self._state.current_project_path:
            return Path(self._state.current_project_path)
        return None

    @property
    def scheme_name(self) -> Optional[str]:
        return self._state.current_scheme_name

    @property
    def is_continuation_mode(self) -> bool:
        return self._state.continuation_mode

    @property
    def data_import(self) -> DataImportFormState:
        return self._state.data_import

    @property
    def status_timer(self) -> Optional[Any]:
        return self._status_timer

    @status_timer.setter
    def status_timer(self, timer: Optional[Any]):
        if self._status_timer:
            try:
                self._status_timer.cancel()
            except Exception:
                pass
        self._status_timer = timer

    # ===========================================
    # Job Management
    # ===========================================

    def add_job(self, job_type: JobType) -> bool:
        job_str = job_type.value

        if job_str in self._state.selected_jobs:
            return False

        self._state.selected_jobs.append(job_str)
        self._state.selected_jobs.sort(key=lambda j: get_job_order(JobType(j)))

        self._state.job_ui_states[job_str] = JobCardUIState()
        self._job_widget_refs[job_str] = JobWidgetRefs()

        if self._state.active_job_tab is None:
            self._state.active_job_tab = job_str

        self._notify()
        return True

    def remove_job(self, job_type: JobType) -> bool:
        job_str = job_type.value

        if job_str not in self._state.selected_jobs:
            return False

        if job_str in self._job_widget_refs:
            self._job_widget_refs[job_str].cleanup()
            del self._job_widget_refs[job_str]

        self._state.selected_jobs.remove(job_str)

        if job_str in self._state.job_ui_states:
            del self._state.job_ui_states[job_str]

        if self._state.active_job_tab == job_str:
            self._state.active_job_tab = self._state.selected_jobs[0] if self._state.selected_jobs else None

        self._notify()
        return True

    def toggle_job(self, job_type: JobType) -> bool:
        if job_type.value in self._state.selected_jobs:
            return self.remove_job(job_type)
        else:
            return self.add_job(job_type)

    def is_job_selected(self, job_type: JobType) -> bool:
        return job_type.value in self._state.selected_jobs

    def set_active_job(self, job_type: JobType):
        job_str = job_type.value
        if job_str in self._state.selected_jobs:
            self._state.active_job_tab = job_str
            self._notify()

    def get_job_ui_state(self, job_type: JobType) -> JobCardUIState:
        job_str = job_type.value
        if job_str not in self._state.job_ui_states:
            self._state.job_ui_states[job_str] = JobCardUIState()
        return self._state.job_ui_states[job_str]

    def get_job_widget_refs(self, job_type: JobType) -> JobWidgetRefs:
        job_str = job_type.value
        if job_str not in self._job_widget_refs:
            self._job_widget_refs[job_str] = JobWidgetRefs()
        return self._job_widget_refs[job_str]

    def set_job_monitor_tab(self, job_type: JobType, tab: MonitorTab, user_initiated: bool = False):
        ui_state = self.get_job_ui_state(job_type)
        ui_state.active_monitor_tab = tab
        if user_initiated:
            ui_state.user_switched_tab = True

    # ===========================================
    # Project Lifecycle
    # ===========================================

    def set_project_created(self, project_path: Path, scheme_name: str):
        self._state.current_project_path = str(project_path)
        self._state.current_scheme_name = scheme_name
        self._state.project_created = True
        self._notify()

    def set_pipeline_running(self, running: bool):
        self._state.pipeline_running = running
        self._notify()

    def set_continuation_mode(self, enabled: bool):
        self._state.continuation_mode = enabled
        self._notify()

    def load_from_project(self, project_path: Path, scheme_name: str, jobs: List[JobType]):
        self._state.current_project_path = str(project_path)
        self._state.current_scheme_name = scheme_name
        self._state.project_created = True
        self._state.selected_jobs = sorted([j.value for j in jobs], key=lambda j: get_job_order(JobType(j)))

        for job_str in self._state.selected_jobs:
            if job_str not in self._state.job_ui_states:
                self._state.job_ui_states[job_str] = JobCardUIState()
            if job_str not in self._job_widget_refs:
                self._job_widget_refs[job_str] = JobWidgetRefs()

        if self._state.selected_jobs and self._state.active_job_tab is None:
            self._state.active_job_tab = self._state.selected_jobs[0]

        self._notify()

    def reset(self):
        """Full reset: clear all state, timers, refs, subscribers."""
        self.cleanup_all_timers()
        self._state = UIState()
        self._job_widget_refs.clear()
        self._panel_refs.cleanup()
        # Phase 2: also clear subscribers and rebuild callback.
        # Without this, stale callbacks from a previous page load
        # would fire against dead UI elements.
        self._subscribers.clear()
        self._rebuild_callback = None
        # Don't _notify here -- subscribers were just cleared,
        # and we're about to build a fresh page anyway.

    # ===========================================
    # Phase 2c: Page rebuild preparation
    # ===========================================

    def prepare_for_page_rebuild(self):
        """Clean up stale references from a previous page load.

        NiceGUI destroys DOM elements when navigating between pages,
        but our widget refs and callbacks still point to the dead
        objects.  Call this at the top of any page-building function
        (build_workspace_page, etc.) to ensure a clean slate for
        new element references.

        Unlike reset(), this preserves logical state (selected jobs,
        active tab, project path, etc.) -- only the ephemeral DOM
        refs are cleared.
        """
        self._panel_refs.cleanup()
        for refs in self._job_widget_refs.values():
            refs.cleanup()
        self._rebuild_callback = None
        self._subscribers.clear()

    # ===========================================
    # Data Import Form
    # ===========================================

    def update_data_import(
        self,
        project_name: Optional[str] = None,
        project_base_path: Optional[str] = None,
        movies_glob: Optional[str] = None,
        mdocs_glob: Optional[str] = None,
        import_prefix: Optional[str] = None,
        movies_valid: Optional[bool] = None,
        mdocs_valid: Optional[bool] = None,
    ):
        di = self._state.data_import
        if project_name is not None:
            di.project_name = project_name
        if project_base_path is not None:
            di.project_base_path = project_base_path
        if movies_glob is not None:
            di.movies_glob = movies_glob
        if mdocs_glob is not None:
            di.mdocs_glob = mdocs_glob
        if import_prefix is not None:
            di.import_prefix = import_prefix
        if movies_valid is not None:
            di.movies_valid = movies_valid
        if mdocs_valid is not None:
            di.mdocs_valid = mdocs_valid

    def update_detected_params(
        self,
        pixel_size: Optional[float] = None,
        voltage: Optional[float] = None,
        dose_per_tilt: Optional[float] = None,
        tilt_axis: Optional[float] = None,
    ):
        di = self._state.data_import
        if pixel_size is not None:
            di.detected_pixel_size = pixel_size
        if voltage is not None:
            di.detected_voltage = voltage
        if dose_per_tilt is not None:
            di.detected_dose_per_tilt = dose_per_tilt
        if tilt_axis is not None:
            di.detected_tilt_axis = tilt_axis

    def clear_data_import(self):
        self._state.data_import = DataImportFormState()

    # ===========================================
    # Callbacks
    # ===========================================

    def set_rebuild_callback(self, callback: Callable[[], None]):
        self._rebuild_callback = callback

    def request_rebuild(self):
        if self._rebuild_callback:
            self._rebuild_callback()

    # ===========================================
    # Cleanup
    # ===========================================

    def cleanup_all_timers(self):
        if self._status_timer:
            try:
                self._status_timer.cancel()
            except Exception:
                pass
            self._status_timer = None

        for refs in self._job_widget_refs.values():
            refs.cleanup()

    def cleanup_job_logs_timer(self, job_type: JobType):
        refs = self._job_widget_refs.get(job_type.value)
        if refs and refs.logs_timer:
            try:
                refs.logs_timer.cancel()
            except Exception:
                pass
            refs.logs_timer = None

    # ===========================================
    # Subscription System
    # ===========================================

    def subscribe(self, callback: Callable[[UIState], None]) -> Callable[[], None]:
        self._subscribers.append(callback)
        return lambda: self._subscribers.remove(callback) if callback in self._subscribers else None

    def _notify(self):
        for sub in self._subscribers:
            try:
                sub(self._state)
            except Exception as e:
                print(f"[UIStateManager] Subscriber error: {e}")


# =========================================================================
# Phase 2: Per-tab accessor (replaces module-level singleton)
#
# get_ui_state_manager() now reads from app.storage.tab, which NiceGUI
# scopes to each browser tab.  It can hold arbitrary Python objects
# (not serialized), so the full UIStateManager instance lives there.
#
# IMPORTANT: must be called within a page handler context AFTER
#   await client.connected()
# This is because app.storage.tab requires an active client connection.
# All page handlers in main_ui.py do this before calling any build
# functions.
# =========================================================================


def get_ui_state_manager() -> UIStateManager:
    """Get or create the UIStateManager for the current browser tab.

    Requires an active client connection (call await client.connected()
    in the page handler before this is reachable).
    """
    from nicegui import app

    tab = app.storage.tab
    if "ui_mgr" not in tab:
        tab["ui_mgr"] = UIStateManager()
    return tab["ui_mgr"]
