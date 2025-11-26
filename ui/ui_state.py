# services/ui_state.py
"""
Typed UI State Management - Single source of truth for all UI state.

Separates:
  - UIState: Ephemeral UI state (tabs, loading flags, selections) - Pydantic model
  - WidgetRefs: Non-serializable UI element references - dataclass
  - UIStateManager: Centralized manager with controlled mutations

This replaces the fragile Dict[str, Any] pattern used previously.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime

from services.project_state import JobType, JobStatus

if TYPE_CHECKING:
    from nicegui.element import Element


class MonitorTab(str, Enum):
    """Explicitly typed tab values - no more string typos"""
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
    This replaces the `shared_state` dict.
    """
    model_config = ConfigDict(use_enum_values=True, arbitrary_types_allowed=True)
    
    # --- Project Context ---
    current_project_path: Optional[str] = None  # String for JSON serialization
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
    Kept separate because these can't be persisted.
    """
    status_dot: Optional[Any] = None
    status_badge: Optional[Any] = None
    logs_timer: Optional[Any] = None
    content_container: Optional[Any] = None
    switcher_container: Optional[Any] = None
    monitor_logs: Dict[str, Any] = field(default_factory=dict)
    
    def cleanup(self):
        """Cancel timers and clear refs"""
        if self.logs_timer:
            try:
                self.logs_timer.cancel()
            except Exception:
                pass
            self.logs_timer = None
        self.status_dot = None
        self.status_badge = None
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
        """Clear all refs"""
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
    JobType.TEMPLATE_MATCH,
    JobType.SUBTOMO_RECONSTRUCT,
]

JOB_DISPLAY_NAMES: Dict[JobType, str] = {
    JobType.IMPORT_MOVIES: "Import",
    JobType.FS_MOTION_CTF: "Motion & CTF",
    JobType.TS_ALIGNMENT: "Alignment",
    JobType.TS_CTF: "TS CTF",
    JobType.TS_RECONSTRUCT: "Reconstruct",
    JobType.DENOISE_TRAIN: "Denoise",
    JobType.TEMPLATE_MATCH: "Template Match",
    JobType.SUBTOMO_RECONSTRUCT: "STA",
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
    Centralized state manager with controlled mutations.
    
    Benefits:
    - All state changes go through methods (auditable)
    - Type-safe access via properties
    - Subscription system for reactivity
    - Clean separation of serializable state vs widget refs
    """
    
    def __init__(self):
        self._state = UIState()
        self._job_widget_refs: Dict[str, JobWidgetRefs] = {}
        self._panel_refs = PanelWidgetRefs()
        self._subscribers: List[Callable[[UIState], None]] = []
        self._status_timer: Optional[Any] = None
        self._rebuild_callback: Optional[Callable[[], None]] = None
    
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
        """
        Add a job to the pipeline. 
        Returns False if already present or if pipeline is locked.
        """
        job_str = job_type.value
        
        if job_str in self._state.selected_jobs:
            return False
        
        if self._state.project_created and not self._state.continuation_mode:
            return False
        
        self._state.selected_jobs.append(job_str)
        self._state.selected_jobs.sort(key=lambda j: get_job_order(JobType(j)))
        
        # Initialize UI state for this job
        self._state.job_ui_states[job_str] = JobCardUIState()
        self._job_widget_refs[job_str] = JobWidgetRefs()
        
        # Auto-select as active if first job
        if self._state.active_job_tab is None:
            self._state.active_job_tab = job_str
        
        self._notify()
        return True
    
    def remove_job(self, job_type: JobType) -> bool:
        """
        Remove a job from the pipeline. 
        Returns False if not present or if pipeline is locked.
        """
        job_str = job_type.value
        
        if job_str not in self._state.selected_jobs:
            return False
        
        if self._state.project_created:
            return False
        
        # Cleanup widget refs
        if job_str in self._job_widget_refs:
            self._job_widget_refs[job_str].cleanup()
            del self._job_widget_refs[job_str]
        
        self._state.selected_jobs.remove(job_str)
        
        if job_str in self._state.job_ui_states:
            del self._state.job_ui_states[job_str]
        
        # Update active tab if needed
        if self._state.active_job_tab == job_str:
            self._state.active_job_tab = (
                self._state.selected_jobs[0] if self._state.selected_jobs else None
            )
        
        self._notify()
        return True
    
    def toggle_job(self, job_type: JobType) -> bool:
        """Toggle a job's presence in the pipeline."""
        if job_type.value in self._state.selected_jobs:
            return self.remove_job(job_type)
        else:
            return self.add_job(job_type)
    
    def is_job_selected(self, job_type: JobType) -> bool:
        """Check if a job is in the pipeline."""
        return job_type.value in self._state.selected_jobs
    
    def set_active_job(self, job_type: JobType):
        """Set the currently active job tab."""
        job_str = job_type.value
        if job_str in self._state.selected_jobs:
            self._state.active_job_tab = job_str
            self._notify()
    
    def get_job_ui_state(self, job_type: JobType) -> JobCardUIState:
        """Get UI state for a job, creating if needed."""
        job_str = job_type.value
        if job_str not in self._state.job_ui_states:
            self._state.job_ui_states[job_str] = JobCardUIState()
        return self._state.job_ui_states[job_str]
    
    def get_job_widget_refs(self, job_type: JobType) -> JobWidgetRefs:
        """Get widget refs for a job, creating if needed."""
        job_str = job_type.value
        if job_str not in self._job_widget_refs:
            self._job_widget_refs[job_str] = JobWidgetRefs()
        return self._job_widget_refs[job_str]
    
    def set_job_monitor_tab(self, job_type: JobType, tab: MonitorTab, user_initiated: bool = False):
        """Switch the monitor tab for a job."""
        ui_state = self.get_job_ui_state(job_type)
        ui_state.active_monitor_tab = tab
        if user_initiated:
            ui_state.user_switched_tab = True
        # Don't call _notify() here - let the caller handle UI rebuild
    
    # ===========================================
    # Project Lifecycle
    # ===========================================
    
    def set_project_created(self, project_path: Path, scheme_name: str):
        """Mark project as created with its path and scheme."""
        self._state.current_project_path = str(project_path)
        self._state.current_scheme_name = scheme_name
        self._state.project_created = True
        self._notify()
    
    def set_pipeline_running(self, running: bool):
        """Update pipeline running state."""
        self._state.pipeline_running = running
        self._notify()
    
    def set_continuation_mode(self, enabled: bool):
        """Toggle continuation mode."""
        self._state.continuation_mode = enabled
        self._notify()
    
    def load_from_project(self, project_path: Path, scheme_name: str, jobs: List[JobType]):
        """Load state when opening an existing project."""
        self._state.current_project_path = str(project_path)
        self._state.current_scheme_name = scheme_name
        self._state.project_created = True
        self._state.selected_jobs = sorted(
            [j.value for j in jobs], 
            key=lambda j: get_job_order(JobType(j))
        )
        
        for job_str in self._state.selected_jobs:
            if job_str not in self._state.job_ui_states:
                self._state.job_ui_states[job_str] = JobCardUIState()
            if job_str not in self._job_widget_refs:
                self._job_widget_refs[job_str] = JobWidgetRefs()
        
        if self._state.selected_jobs and self._state.active_job_tab is None:
            self._state.active_job_tab = self._state.selected_jobs[0]
        
        self._notify()
    
    def reset(self):
        """Full reset for starting a new session."""
        self.cleanup_all_timers()
        self._state = UIState()
        self._job_widget_refs.clear()
        self._panel_refs.cleanup()
        self._notify()
    
    # ===========================================
    # Data Import Form
    # ===========================================
    
    def update_data_import(
        self,

        project_name     : Optional[str]  = None,
        project_base_path: Optional[str]  = None,
        movies_glob      : Optional[str]  = None,
        mdocs_glob       : Optional[str]  = None,
        import_prefix    : Optional[str]  = None,
        movies_valid     : Optional[bool] = None,
        mdocs_valid      : Optional[bool] = None,

    ):
        """Update data import form state."""
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
        # No notify - form updates are frequent
    
    def update_detected_params(
        self,
        pixel_size: Optional[float] = None,
        voltage: Optional[float] = None,
        dose_per_tilt: Optional[float] = None,
        tilt_axis: Optional[float] = None,
    ):
        """Update auto-detected parameters."""
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
        """Clear the data import form."""
        self._state.data_import = DataImportFormState()
    
    # ===========================================
    # Callbacks
    # ===========================================
    
    def set_rebuild_callback(self, callback: Callable[[], None]):
        """Set the callback to rebuild the pipeline UI."""
        self._rebuild_callback = callback
    
    def request_rebuild(self):
        """Request a UI rebuild."""
        if self._rebuild_callback:
            self._rebuild_callback()
    
    # ===========================================
    # Cleanup
    # ===========================================
    
    def cleanup_all_timers(self):
        """Cancel all active timers."""
        if self._status_timer:
            try:
                self._status_timer.cancel()
            except Exception:
                pass
            self._status_timer = None
        
        for refs in self._job_widget_refs.values():
            refs.cleanup()
    
    def cleanup_job_logs_timer(self, job_type: JobType):
        """Cancel the logs timer for a specific job."""
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
        """Subscribe to state changes. Returns unsubscribe function."""
        self._subscribers.append(callback)
        return lambda: self._subscribers.remove(callback) if callback in self._subscribers else None
    
    def _notify(self):
        """Notify all subscribers of state change."""
        print(f"[UIStateManager] _notify called, {len(self._subscribers)} subscribers, jobs: {self._state.selected_jobs}")
        for sub in self._subscribers:
            try:
                sub(self._state)
            except Exception as e:
                print(f"[UIStateManager] Subscriber error: {e}")
    
    # ===========================================
    # Persistence
    # ===========================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize state for persistence (e.g., browser storage)."""
        return self._state.model_dump(mode="json")
    
    def restore_from_dict(self, data: Dict[str, Any]):
        """Restore state from persisted data."""
        try:
            self._state = UIState.model_validate(data)
            # Rebuild widget refs for loaded jobs
            for job_str in self._state.selected_jobs:
                if job_str not in self._job_widget_refs:
                    self._job_widget_refs[job_str] = JobWidgetRefs()
            self._notify()
        except Exception as e:
            print(f"[UIStateManager] Failed to restore state: {e}")
    
    # ===========================================
    # Debug
    # ===========================================
    
    def debug_dump(self) -> str:
        """Return a debug string of current state."""
        return (
            f"UIState:\n"
            f"  project_path: {self._state.current_project_path}\n"
            f"  project_created: {self._state.project_created}\n"
            f"  pipeline_running: {self._state.pipeline_running}\n"
            f"  selected_jobs: {self._state.selected_jobs}\n"
            f"  active_job_tab: {self._state.active_job_tab}\n"
            f"  continuation_mode: {self._state.continuation_mode}\n"
        )


_ui_state_manager: Optional[UIStateManager] = None


def get_ui_state_manager() -> UIStateManager:
    """Get the global UI state manager singleton."""
    global _ui_state_manager
    if _ui_state_manager is None:
        _ui_state_manager = UIStateManager()
    return _ui_state_manager


def reset_ui_state_manager():
    """Reset the global UI state manager. Use when starting fresh."""
    global _ui_state_manager
    if _ui_state_manager:
        _ui_state_manager.cleanup_all_timers()
    _ui_state_manager = UIStateManager()
    return _ui_state_manager