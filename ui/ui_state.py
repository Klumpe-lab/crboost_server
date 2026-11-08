# ui/ui_state.py
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from collections.abc import Callable
from pydantic import BaseModel, Field, ConfigDict
from services.jobs.spec import display_name
from services.models_base import instance_id_to_job_type
from services.project_state import JobType, get_project_state_for

logger = logging.getLogger(__name__)


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
    # Optional project-wide gain reference file, chosen at setup. Threaded into
    # detected_params -> ProjectState.acquisition.gain_reference_path.
    gain_reference_path: str = ""

    movies_valid: bool = False
    mdocs_valid: bool = False

    # Dose per tilt when the mdocs record none: prefilled with the
    # scan's estimate ("estimated"), overwritten by the user ("user"). None = the row
    # is not in play (the mdocs carry the dose) or the user cleared it.
    dose_per_tilt_override: float | None = None
    dose_per_tilt_source: str = ""

    # Create the project as shared/lab-owned (owner = SHARED_OWNER) rather than
    # belonging to the creating user.
    is_shared: bool = False

    detected_pixel_size: float | None = None
    detected_voltage: float | None = None
    detected_dose_per_tilt: float | None = None
    detected_tilt_axis: float | None = None


class UIState(BaseModel):
    model_config = ConfigDict(use_enum_values=True, arbitrary_types_allowed=True)

    current_project_path: str | None = None
    current_scheme_name: str | None = None
    project_created: bool = False
    continuation_mode: bool = False

    pipeline_running: bool = False

    active_job_tab: str | None = None  # instance_id string

    job_ui_states: dict[str, JobCardUIState] = Field(default_factory=dict)
    data_import: DataImportFormState = Field(default_factory=DataImportFormState)
    last_status_refresh: str | None = None


_JOB_SCOPE_PREFIX = "job:"


def job_scope_key(instance_id: str) -> str:
    return f"{_JOB_SCOPE_PREFIX}{instance_id}"


@dataclass
class JobWidgetRefs:
    logs_timer: Any | None = None
    # The job page's folded sections (Tasks, Log, Files, ...) by key: each opens its section.
    section_openers: dict[str, Any] = field(default_factory=dict)
    monitor_logs: dict[str, Any] = field(default_factory=dict)

    def cleanup(self):
        if self.logs_timer:
            try:
                self.logs_timer.cancel()
            except Exception:
                pass
            self.logs_timer = None
        self.section_openers.clear()
        self.monitor_logs.clear()


@dataclass
class PanelWidgetRefs:
    job_tabs_container: Any | None = None
    job_list_container: Any | None = None
    job_tags_container: Any | None = None
    run_button: Any | None = None
    stop_button: Any | None = None
    status_label: Any | None = None
    continuation_container: Any | None = None
    job_tag_buttons: dict[str, Any] = field(default_factory=dict)

    movies_input: Any | None = None
    mdocs_input: Any | None = None
    project_name_input: Any | None = None
    project_path_input: Any | None = None
    create_button: Any | None = None
    load_button: Any | None = None
    autodetect_button: Any | None = None
    params_display_container: Any | None = None
    movies_hint_label: Any | None = None
    mdocs_hint_label: Any | None = None
    status_indicator: Any | None = None

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


# ── Display names (all data lives in services.jobs.spec) ─────────────────────


def get_job_display_name(job_type: JobType) -> str:
    return display_name(job_type)


# ── UIStateManager ────────────────────────────────────────────────────────────


class UIStateManager:
    """One instance per browser tab, stored in app.storage.tab."""

    def __init__(self):
        self._state = UIState()
        self._job_widget_refs: dict[str, JobWidgetRefs] = {}
        self._panel_refs = PanelWidgetRefs()
        self._subscribers: list[Callable[[UIState], None]] = []
        self._status_timer: Any | None = None
        self._rebuild_callback: Callable[[], None] | None = None
        self._view_scopes: dict[str, Any] = {}  # view_scope()
        # Deep-link target for the Tasks tab: when the user clicks a per-TS
        # row in the roster, we set {instance_id: ts_name} here and switch
        # the job to its "tasks" tab. The tracker pops this on render to
        # auto-expand + scroll the matching row into view (one-shot).
        self.focus_ts_by_instance: dict[str, str] = {}

    # ── Persistence ───────────────────────────────────────────────────────────

    def load_from_storage(self, storage_dict: dict[str, Any]):
        if not storage_dict:
            return
        try:
            logger.info("Hydrating from browser storage...")
            self._state = UIState(**storage_dict)
            self._notify()
        except Exception as e:
            logger.info("Error hydrating state from storage: %s", e)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def state(self) -> UIState:
        return self._state

    @property
    def panel_refs(self) -> PanelWidgetRefs:
        return self._panel_refs

    @property
    def active_instance_id(self) -> str | None:
        """The instance_id of the currently focused tab."""
        return self._state.active_job_tab

    @property
    def active_job(self) -> JobType | None:
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
    def project_path(self) -> Path | None:
        if self._state.current_project_path:
            return Path(self._state.current_project_path)
        return None

    @property
    def scheme_name(self) -> str | None:
        return self._state.current_scheme_name

    @property
    def is_continuation_mode(self) -> bool:
        return self._state.continuation_mode

    @property
    def data_import(self) -> DataImportFormState:
        return self._state.data_import

    @property
    def status_timer(self) -> Any | None:
        return self._status_timer

    @status_timer.setter
    def status_timer(self, timer: Any | None):
        if self._status_timer:
            try:
                self._status_timer.cancel()
            except Exception:
                pass
        self._status_timer = timer

    # ── The roster: the project's, never the tab's ────────────────────────────

    def roster(self) -> list[str]:
        """The jobs on the project's roster (`ProjectState.roster()`), the one list every tab and the
        API read: a job added or removed anywhere is here on the next read."""
        if self.project_path is None:
            return []
        return get_project_state_for(self.project_path).roster()

    def forget_job(self, instance_id: str) -> None:
        """Drop this tab's state for a job that has left the roster."""
        refs = self._job_widget_refs.pop(instance_id, None)
        if refs is not None:
            refs.cleanup()
        self._state.job_ui_states.pop(instance_id, None)
        if self._state.active_job_tab == instance_id:
            self._state.active_job_tab = None

    # ── Active tab ────────────────────────────────────────────────────────────

    def set_active_instance(self, instance_id: str):
        if instance_id in self.roster():
            self._state.active_job_tab = instance_id
            self._notify()

    def clear_active_instance(self):
        """No job open: the job page is closed, the roster stays."""
        self._state.active_job_tab = None
        self._notify()

    # ── Per-instance UI state & refs ──────────────────────────────────────────

    def get_job_ui_state(self, instance_id: str) -> JobCardUIState:
        if instance_id not in self._state.job_ui_states:
            self._state.job_ui_states[instance_id] = JobCardUIState()
        return self._state.job_ui_states[instance_id]

    def get_job_widget_refs(self, instance_id: str) -> JobWidgetRefs:
        if instance_id not in self._job_widget_refs:
            self._job_widget_refs[instance_id] = JobWidgetRefs()
        return self._job_widget_refs[instance_id]

    def view_scope(self, key: str):
        """This tab's `ViewScope` for `key`: "pipeline" (the roster and the job pages) or
        `job_scope_key(instance_id)` (one job's page). Made on first use, inactive for a job until
        its panel shows it; a page rebuild starts them afresh."""
        from ui.components.reactive import ViewScope

        if key not in self._view_scopes:
            self._view_scopes[key] = ViewScope(active=not key.startswith(_JOB_SCOPE_PREFIX))
        return self._view_scopes[key]

    def job_scope(self, instance_id: str):
        return self.view_scope(job_scope_key(instance_id))

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

    def load_from_project(self, project_path: Path, scheme_name: str):
        """Point this tab at a project. The roster is read from the project whenever it is needed; the first job
        opens unless the tab already has one open that is on the roster."""
        self._state.current_project_path = str(project_path)
        self._state.current_scheme_name = scheme_name
        self._state.project_created = True

        roster = self.roster()
        if self._state.active_job_tab not in roster:
            self._state.active_job_tab = roster[0] if roster else None

        self._notify()

    def reset(self):
        self.cleanup_all_timers()
        self._state = UIState()
        self._job_widget_refs.clear()
        self._view_scopes.clear()
        self._panel_refs.cleanup()
        self._subscribers.clear()
        self._rebuild_callback = None

    def prepare_for_page_rebuild(self):
        self._panel_refs.cleanup()
        for refs in self._job_widget_refs.values():
            refs.cleanup()
        self._view_scopes.clear()
        self._rebuild_callback = None
        self._subscribers.clear()

    # ── Data import form ──────────────────────────────────────────────────────

    def update_data_import(
        self,
        project_name: str | None = None,
        project_base_path: str | None = None,
        movies_glob: str | None = None,
        mdocs_glob: str | None = None,
        import_prefix: str | None = None,
        gain_reference_path: str | None = None,
        movies_valid: bool | None = None,
        mdocs_valid: bool | None = None,
        is_shared: bool | None = None,
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
        if gain_reference_path is not None:
            di.gain_reference_path = gain_reference_path
        if movies_valid is not None:
            di.movies_valid = movies_valid
        if mdocs_valid is not None:
            di.mdocs_valid = mdocs_valid
        if is_shared is not None:
            di.is_shared = is_shared

    def update_detected_params(
        self,
        pixel_size: float | None = None,
        voltage: float | None = None,
        dose_per_tilt: float | None = None,
        tilt_axis: float | None = None,
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
                logger.info("Subscriber error: %s", e)


def get_ui_state_manager() -> UIStateManager:
    from nicegui import app

    tab = app.storage.tab
    if "ui_mgr" not in tab:
        tab["ui_mgr"] = UIStateManager()
    return tab["ui_mgr"]
