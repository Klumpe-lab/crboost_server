# ui/ui_state.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Tuple, TYPE_CHECKING
from pydantic import BaseModel, Field, ConfigDict
from services.project_state import JobType, JobStatus

if TYPE_CHECKING:
    from nicegui.element import Element


class MonitorTab(str, Enum):
    CONFIG = "config"
    LOGS = "logs"
    FILES = "files"


class JobCardUIState(BaseModel):
    model_config = ConfigDict(use_enum_values=True)
    active_monitor_tab: str = MonitorTab.CONFIG.value
    user_switched_tab: bool = False


class DataImportFormState(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    project_name: str = ""
    project_base_path: str = ""
    movies_glob: str = ""
    mdocs_glob: str = ""
    import_prefix: str = ""

    movies_valid: bool = False
    mdocs_valid: bool = False

    detected_pixel_size: Optional[float] = None
    detected_voltage: Optional[float] = None
    detected_dose_per_tilt: Optional[float] = None
    detected_tilt_axis: Optional[float] = None


class UIState(BaseModel):
    model_config = ConfigDict(use_enum_values=True, arbitrary_types_allowed=True)

    current_project_path: Optional[str] = None
    current_scheme_name: Optional[str] = None
    project_created: bool = False
    continuation_mode: bool = False

    # Instance IDs as strings — singletons use job_type.value,
    # multi-instance uses "jobtype__2", "jobtype__ribosome", etc.
    selected_jobs: List[str] = Field(default_factory=list)
    pipeline_running: bool = False

    active_job_tab: Optional[str] = None  # instance_id string

    job_ui_states: Dict[str, JobCardUIState] = Field(default_factory=dict)
    data_import: DataImportFormState = Field(default_factory=DataImportFormState)
    last_status_refresh: Optional[str] = None


@dataclass
class JobWidgetRefs:
    logs_timer: Optional[Any] = None
    content_container: Optional[Any] = None
    switcher_container: Optional[Any] = None
    monitor_logs: Dict[str, Any] = field(default_factory=dict)

    def cleanup(self):
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
    job_tabs_container: Optional[Any] = None
    job_list_container: Optional[Any] = None
    job_tags_container: Optional[Any] = None
    run_button: Optional[Any] = None
    stop_button: Optional[Any] = None
    status_label: Optional[Any] = None
    continuation_container: Optional[Any] = None
    job_tag_buttons: Dict[str, Any] = field(default_factory=dict)

    movies_input: Optional[Any] = None
    mdocs_input: Optional[Any] = None
    project_name_input: Optional[Any] = None
    project_path_input: Optional[Any] = None
    create_button: Optional[Any] = None
    load_button: Optional[Any] = None
    autodetect_button: Optional[Any] = None
    params_display_container: Optional[Any] = None
    movies_hint_label: Optional[Any] = None
    mdocs_hint_label: Optional[Any] = None
    status_indicator: Optional[Any] = None

    def cleanup(self):
        self.job_tabs_container = None
        self.job_list_container = None
        self.job_tags_container = None
        self.run_button = None
        self.stop_button = None
        self.status_label = None
        self.continuation_container = None
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


# ── Pipeline ordering ─────────────────────────────────────────────────────────

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
    try:
        return PIPELINE_ORDER.index(job_type)
    except ValueError:
        return 999


def get_job_display_name(job_type: JobType) -> str:
    return JOB_DISPLAY_NAMES.get(job_type, job_type.value)


def get_ordered_jobs() -> List[JobType]:
    return PIPELINE_ORDER.copy()


# ── Instance ID helpers ───────────────────────────────────────────────────────


def instance_id_to_job_type(instance_id: str) -> JobType:
    """Extract JobType from an instance_id.

    'templatematching'          -> JobType.TEMPLATE_MATCH_PYTOM
    'templatematching__2'       -> JobType.TEMPLATE_MATCH_PYTOM
    'templatematching__ribosome'-> JobType.TEMPLATE_MATCH_PYTOM
    """
    base = instance_id.split("__")[0]
    return JobType(base)


def get_instance_order(instance_id: str) -> Tuple:
    """Stable sort key: (type_order, numeric_suffix_or_999, text_suffix)."""
    try:
        type_order = get_job_order(instance_id_to_job_type(instance_id))
    except ValueError:
        type_order = 999
    parts = instance_id.split("__", 1)
    if len(parts) == 1:
        # base instance always sorts before any suffixed variant
        return (type_order, 0, "")
    suffix = parts[1]
    try:
        return (type_order, int(suffix), "")
    except ValueError:
        return (type_order, 999, suffix)


def get_instance_display_name(instance_id: str, job_model=None) -> str:
    if job_model is not None:
        label = getattr(job_model, "display_label", None)
        if label:
            return label

    parts = instance_id.split("__", 1)
    base = parts[0]

    try:
        base_name = JOB_DISPLAY_NAMES.get(JobType(base), base)
    except ValueError:
        base_name = base

    # If the job has actually run, show its real directory name (e.g. "job004")
    if job_model is not None:
        relion_job_name = getattr(job_model, "relion_job_name", None)
        if relion_job_name:
            job_dir = relion_job_name.rstrip("/").split("/")[-1]
            return f"{base_name} ({job_dir})"

    # Not yet run — fall back to instance_id suffix
    if len(parts) == 1:
        return base_name

    suffix = parts[1]
    if suffix.isdigit():
        return f"{base_name} #{suffix}"
    return f"{base_name} ({suffix})"


# ── UIStateManager ────────────────────────────────────────────────────────────


class UIStateManager:
    """One instance per browser tab, stored in app.storage.tab."""

    def __init__(self):
        self._state = UIState()
        self._job_widget_refs: Dict[str, JobWidgetRefs] = {}
        self._panel_refs = PanelWidgetRefs()
        self._subscribers: List[Callable[[UIState], None]] = []
        self._status_timer: Optional[Any] = None
        self._rebuild_callback: Optional[Callable[[], None]] = None

    # ── Persistence ───────────────────────────────────────────────────────────

    def load_from_storage(self, storage_dict: Dict[str, Any]):
        if not storage_dict:
            return
        try:
            print("[UI_STATE] Hydrating from browser storage...")
            self._state = UIState(**storage_dict)
            for iid in self._state.selected_jobs:
                if iid not in self._job_widget_refs:
                    self._job_widget_refs[iid] = JobWidgetRefs()
            self._notify()
        except Exception as e:
            print(f"[UI_STATE] Error hydrating state from storage: {e}")

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def state(self) -> UIState:
        return self._state

    @property
    def panel_refs(self) -> PanelWidgetRefs:
        return self._panel_refs

    @property
    def selected_jobs(self) -> List[str]:
        """Ordered list of selected instance_ids."""
        return list(self._state.selected_jobs)

    @property
    def active_instance_id(self) -> Optional[str]:
        """The instance_id of the currently focused tab."""
        return self._state.active_job_tab

    @property
    def active_job(self) -> Optional[JobType]:
        """JobType of the active tab. Derived from active_instance_id."""
        if self._state.active_job_tab:
            try:
                return instance_id_to_job_type(self._state.active_job_tab)
            except ValueError:
                return None
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

    # ── Instance queries ──────────────────────────────────────────────────────

    def get_instances_for_type(self, job_type: JobType) -> List[str]:
        """All selected instance_ids for a given job type, in pipeline order."""
        prefix = job_type.value
        return [s for s in self._state.selected_jobs if s == prefix or s.startswith(prefix + "__")]

    def is_job_type_selected(self, job_type: JobType) -> bool:
        return len(self.get_instances_for_type(job_type)) > 0

    def is_job_selected(self, job_type: JobType) -> bool:
        """Backward compat alias for is_job_type_selected."""
        return self.is_job_type_selected(job_type)

    # ── Instance management ───────────────────────────────────────────────────

    def add_instance(self, instance_id: str, job_type: JobType) -> bool:
        if instance_id in self._state.selected_jobs:
            return False

        self._state.selected_jobs.append(instance_id)
        self._state.selected_jobs.sort(key=get_instance_order)

        self._state.job_ui_states[instance_id] = JobCardUIState()
        self._job_widget_refs[instance_id] = JobWidgetRefs()

        if self._state.active_job_tab is None:
            self._state.active_job_tab = instance_id

        self._notify()
        return True

    def add_job(self, job_type: JobType) -> bool:
        """Backward compat: add the singleton instance (instance_id = job_type.value)."""
        return self.add_instance(job_type.value, job_type)

    def remove_instance(self, instance_id: str) -> bool:
        if instance_id not in self._state.selected_jobs:
            return False

        if instance_id in self._job_widget_refs:
            self._job_widget_refs[instance_id].cleanup()
            del self._job_widget_refs[instance_id]

        self._state.selected_jobs.remove(instance_id)

        if instance_id in self._state.job_ui_states:
            del self._state.job_ui_states[instance_id]

        if self._state.active_job_tab == instance_id:
            self._state.active_job_tab = self._state.selected_jobs[0] if self._state.selected_jobs else None

        self._notify()
        return True

    def remove_job(self, job_type: JobType) -> bool:
        """Backward compat: remove the singleton instance."""
        return self.remove_instance(job_type.value)

    def toggle_job(self, job_type: JobType) -> bool:
        instances = self.get_instances_for_type(job_type)
        if instances:
            for iid in list(instances):
                self.remove_instance(iid)
            return True
        return self.add_job(job_type)

    # ── Active tab ────────────────────────────────────────────────────────────

    def set_active_instance(self, instance_id: str):
        if instance_id in self._state.selected_jobs:
            self._state.active_job_tab = instance_id
            self._notify()

    def set_active_job(self, job_type: JobType):
        """Backward compat: activate the first instance of a type."""
        instances = self.get_instances_for_type(job_type)
        if instances:
            self.set_active_instance(instances[0])

    # ── Per-instance UI state & refs ──────────────────────────────────────────

    def get_job_ui_state(self, instance_id: str) -> JobCardUIState:
        if instance_id not in self._state.job_ui_states:
            self._state.job_ui_states[instance_id] = JobCardUIState()
        return self._state.job_ui_states[instance_id]

    def get_job_widget_refs(self, instance_id: str) -> JobWidgetRefs:
        if instance_id not in self._job_widget_refs:
            self._job_widget_refs[instance_id] = JobWidgetRefs()
        return self._job_widget_refs[instance_id]

    def set_job_monitor_tab(self, instance_id: str, tab: str, user_initiated: bool = False):
        ui_state = self.get_job_ui_state(instance_id)
        ui_state.active_monitor_tab = tab if isinstance(tab, str) else tab.value
        if user_initiated:
            ui_state.user_switched_tab = True

    # ── Project lifecycle ─────────────────────────────────────────────────────

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

    def load_from_project(self, project_path: Path, scheme_name: str, jobs: List[str]):
        self._state.current_project_path = str(project_path)
        self._state.current_scheme_name = scheme_name
        self._state.project_created = True

        self._state.selected_jobs = sorted(jobs, key=get_instance_order)

        for iid in self._state.selected_jobs:
            if iid not in self._state.job_ui_states:
                self._state.job_ui_states[iid] = JobCardUIState()
            if iid not in self._job_widget_refs:
                self._job_widget_refs[iid] = JobWidgetRefs()

        if self._state.selected_jobs and self._state.active_job_tab is None:
            self._state.active_job_tab = self._state.selected_jobs[0]

        self._notify()

    def reset(self):
        self.cleanup_all_timers()
        self._state = UIState()
        self._job_widget_refs.clear()
        self._panel_refs.cleanup()
        self._subscribers.clear()
        self._rebuild_callback = None

    def prepare_for_page_rebuild(self):
        self._panel_refs.cleanup()
        for refs in self._job_widget_refs.values():
            refs.cleanup()
        self._rebuild_callback = None
        self._subscribers.clear()

    # ── Data import form ──────────────────────────────────────────────────────

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

    # ── Rebuild callback ──────────────────────────────────────────────────────

    def set_rebuild_callback(self, callback: Callable[[], None]):
        self._rebuild_callback = callback

    def request_rebuild(self):
        if self._rebuild_callback:
            self._rebuild_callback()

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def cleanup_all_timers(self):
        if self._status_timer:
            try:
                self._status_timer.cancel()
            except Exception:
                pass
            self._status_timer = None
        for refs in self._job_widget_refs.values():
            refs.cleanup()

    def cleanup_job_logs_timer(self, instance_id: str):
        refs = self._job_widget_refs.get(instance_id)
        if refs and refs.logs_timer:
            try:
                refs.logs_timer.cancel()
            except Exception:
                pass
            refs.logs_timer = None

    # ── Subscription system ───────────────────────────────────────────────────

    def subscribe(self, callback: Callable[[UIState], None]) -> Callable[[], None]:
        self._subscribers.append(callback)
        return lambda: self._subscribers.remove(callback) if callback in self._subscribers else None

    def _notify(self):
        for sub in self._subscribers:
            try:
                sub(self._state)
            except Exception as e:
                print(f"[UIStateManager] Subscriber error: {e}")


def get_ui_state_manager() -> UIStateManager:
    from nicegui import app

    tab = app.storage.tab
    if "ui_mgr" not in tab:
        tab["ui_mgr"] = UIStateManager()
    return tab["ui_mgr"]
