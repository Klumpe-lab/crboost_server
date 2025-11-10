# ui/pipeline_builder/job_tab_component.py
"""
Single job tab rendering component.
Handles Parameters, Logs, and Files sub-tabs for a given job.
REFACTORED to use direct state access and remove sync logic.
"""
import asyncio
from pathlib import Path
from nicegui import ui
from services.project_state import AlignmentMethod, JobStatus, JobType, get_project_state # Use the new global state getter
from ui.utils import _snake_to_title
from typing import Dict, Any, Set


def get_job_status(job_type: JobType) -> JobStatus:
    """Query status from job model, handle reset jobs properly"""
    state = get_project_state() # Get current state
    job_model = state.jobs.get(job_type)
    if not job_model:
        return JobStatus.UNKNOWN
    
    # If job has been reset (no relion_job_name) but is marked as something other than scheduled, fix it
    if not job_model.relion_job_name and job_model.execution_status != JobStatus.SCHEDULED:
        job_model.execution_status = JobStatus.SCHEDULED
        
    return job_model.execution_status


def get_status_color(job_type: JobType) -> str:
    """Get status indicator color"""
    status = get_job_status(job_type)
    colors = {
        JobStatus.SCHEDULED: "#fbbf24",
        JobStatus.RUNNING: "#3b82f6",
        JobStatus.SUCCEEDED: "#10b981",
        JobStatus.FAILED: "#ef4444",
    }
    return colors.get(status, "#6b7280")


def is_job_frozen(job_type: JobType) -> bool:
    """Check if job params should be frozen"""
    return get_job_status(job_type) not in [JobStatus.SCHEDULED]


def render_job_tab(
    job_type: JobType,
    backend,
    shared_state: Dict[str, Any],
    callbacks: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Render a complete job tab with sub-tabs.
    Returns dict with UI element references for surgical updates.
    """
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    if not job_model:
        ui.label(f"Error: Job model for {job_type.value} not found.").classes("text-xs text-red-600")
        return {}

    job_state = shared_state["job_cards"].get(job_type, {})
    is_frozen = is_job_frozen(job_type)
    active_sub_tab = job_state.get("active_monitor_tab", "config")
    
    if is_frozen and active_sub_tab == "config":
        active_sub_tab = "logs"
        job_state["active_monitor_tab"] = "logs"

    # Sub-tab buttons
    with ui.row().classes("w-full p-4 pb-0 items-center").style("gap: 8px;"):
        for tab_name, tab_label in [("config", "Parameters"), ("logs", "Logs"), ("files", "Files")]:
            is_active = active_sub_tab == tab_name
            ui.button(
                tab_label,
                on_click=lambda t=tab_name: _switch_monitor_tab(job_type, t, backend, shared_state, callbacks)
            ).props("dense flat no-caps").style(
                f"padding: 6px 12px; border-radius: 3px; font-weight: 500; font-size: 11px; "
                f"background: {'#3b82f6' if is_active else '#f3f4f6'}; "
                f"color: {'white' if is_active else '#1f2937'}; "
                f"border: 1px solid {'#3b82f6' if is_active else '#e5e7eb'};"
            )

        status_label = ui.label(f"Status: {job_model.execution_status.value}").classes(
            "text-xs font-medium text-gray-600 ml-auto"
        )
        job_state["ui_status_label"] = status_label
        
        ui.button(
            "Refresh",
            icon="refresh",
            on_click=lambda: _force_status_refresh(backend, shared_state, callbacks) 
        ).props("dense flat no-caps").style(
            "background: #f3f4f6; color: #1f2937; padding: 4px 12px; border-radius: 3px; font-size: 11px;"
        )

    # Content area
    content_container = ui.column().classes("w-full p-4")
    job_state["monitor_content_container"] = content_container

    with content_container:
        if active_sub_tab == "config":
            _render_config_tab(job_type, job_model, is_frozen, shared_state, callbacks)
        elif active_sub_tab == "logs":
            _render_logs_tab(job_type, job_model, backend, shared_state)
        elif active_sub_tab == "files":
            _render_files_tab(job_type, job_model, shared_state)

    return {
        "status_label": status_label,
        "content_container": content_container,
    }


def _switch_monitor_tab(job_type, tab_name, backend, shared_state, callbacks):
    """Switch to a different sub-tab"""
    job_state = shared_state["job_cards"][job_type]
    job_state["active_monitor_tab"] = tab_name
    
    if tab_name != "logs" and job_state.get("logs_timer"):
        job_state["logs_timer"].cancel()
        job_state["logs_timer"] = None
    
    container = job_state.get("monitor_content_container")
    if container:
        state = get_project_state()
        job_model = state.jobs.get(job_type)
        is_frozen = is_job_frozen(job_type)

        container.clear()
        with container:
            if tab_name == "config":
                _render_config_tab(job_type, job_model, is_frozen, shared_state, callbacks)
            elif tab_name == "logs":
                _render_logs_tab(job_type, job_model, backend, shared_state)
            elif tab_name == "files":
                _render_files_tab(job_type, job_model, shared_state)

# --- Fields to exclude from the generic parameter UI ---
BASE_JOB_FIELDS: Set[str] = {
    'execution_status', 'relion_job_name', 'relion_job_number'
}
GLOBAL_PROPERTIES: Set[str] = {
    'microscope', 'acquisition', 'pixel_size', 'voltage', 
    'spherical_aberration', 'amplitude_contrast', 'dose_per_tilt', 
    'tilt_axis_angle', 'thickness_nm', 'eer_ngroups', 'gain_path', 
    'invert_tilt_angles'
}

def _render_config_tab(job_type, job_model, is_frozen, shared_state, callbacks):
    """
    Render parameters sub-tab.
    REFACTORED to bind job-specific params to job_model and
    global params to job_model.microscope / job_model.acquisition.
    """
    if is_frozen:
        # ... (Frozen message - no change) ...
        status_color = get_status_color(job_type)
        icon_map = {
            JobStatus.SUCCEEDED: "check_circle",
            JobStatus.FAILED: "error",
            JobStatus.RUNNING: "sync"
        }
        icon = icon_map.get(job_model.execution_status, "info")
        with ui.row().classes("w-full items-center mb-3 p-2").style(
            f"background: #fafafa; border-left: 3px solid {status_color}; border-radius: 3px;"
        ):
            ui.icon(icon).style(f"color: {status_color};")
            ui.label(f"Job status is {job_model.execution_status.value}. Parameters are frozen.").classes(
                "text-xs text-gray-700"
            )

    # --- Inputs & Outputs (no change) ---
    ui.label("Inputs & Outputs").classes("text-xs font-semibold text-black mb-2")
    with ui.column().classes("w-full mb-4 p-3").style("background: #fafafa; border-radius: 3px; gap: 8px;"):
        if shared_state.get("project_created"):
            # This relies on the backend snapshot, which is fine
            paths_data = shared_state.get("params_snapshot", {}).get(job_type, {}).get("paths", {})
            if paths_data:
                for key, value in paths_data.items():
                    with ui.row().classes("w-full items-start").style("gap: 8px;"):
                        ui.label(f"{_snake_to_title(key)}:").classes("text-xs font-medium text-gray-600").style(
                            "min-width: 140px;"
                        )
                        ui.label(str(value)).classes("text-xs text-gray-800 font-mono flex-1")
            else:
                ui.label("Paths not yet calculated").classes("text-xs text-gray-500 italic")
        else:
            ui.label("Paths will be generated when project is created").classes("text-xs text-gray-500 italic")

    # --- REFACTORED PARAMETER RENDERING ---
    
    # 1. Job-Specific Parameters
    ui.label("Job-Specific Parameters").classes("text-xs font-semibold text-black mb-2")
    job_specific_fields = set(job_model.model_fields.keys()) - BASE_JOB_FIELDS
    
    if not job_specific_fields:
        ui.label("This job has no job-specific parameters.").classes("text-xs text-gray-500 italic mb-4")
    else:
        with ui.grid(columns=3).classes("w-full mb-4").style("gap: 10px;"):
            for param_name in sorted(list(job_specific_fields)):
                label = _snake_to_title(param_name)
                value = getattr(job_model, param_name)

                if isinstance(value, bool):
                    element = ui.checkbox(label, value=value).props("dense")
                    if not is_frozen:
                        element.bind_value(job_model, param_name)
                    else:
                        element.disable()
                
                elif isinstance(value, (int, float)) or value is None:
                    element = ui.input(label=label, value=str(value) if value is not None else "").props("outlined dense").classes("w-full")
                    element.enabled = not is_frozen
                    if is_frozen:
                        element.classes("bg-gray-50")
                    else:
                        # Bind directly to the model. Pydantic will handle type conversion.
                        element.bind_value(job_model, param_name)

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        element = ui.select(
                            label=label, options=[e.value for e in AlignmentMethod], value=value
                        ).props("outlined dense").classes("w-full")
                    else:
                        element = ui.input(label=label, value=value).props("outlined dense").classes("w-full")
                    
                    element.enabled = not is_frozen
                    if is_frozen:
                        element.classes("bg-gray-50")
                    else:
                        element.bind_value(job_model, param_name)
                
                # NO sync indicator callback!

    # 2. Global Experimental Parameters (Accessed via properties)
    ui.label("Global Experimental Parameters").classes("text-xs font-semibold text-black mb-2")
    with ui.grid(columns=3).classes("w-full").style("gap: 10px;"):
        # These inputs are ALWAYS bound to the global state, via the job_model properties
        # They are disabled because they should be edited in the Data Import panel
        ui.input('Pixel Size (Å)').bind_value(
            job_model.microscope, 'pixel_size_angstrom'
        ).props("dense outlined readonly").tooltip("Global parameter (edit in Data Import)")
        
        ui.input('Voltage (kV)').bind_value(
            job_model.microscope, 'acceleration_voltage_kv'
        ).props("dense outlined readonly").tooltip("Global parameter (edit in Data Import)")
        
        ui.input('Cs (mm)').bind_value(
            job_model.microscope, 'spherical_aberration_mm'
        ).props("dense outlined readonly").tooltip("Global parameter (edit in Data Import)")
        
        ui.input('Amplitude Contrast').bind_value(
            job_model.microscope, 'amplitude_contrast'
        ).props("dense outlined readonly").tooltip("Global parameter (edit in Data Import)")
        
        ui.input('Dose per Tilt').bind_value(
            job_model.acquisition, 'dose_per_tilt'
        ).props("dense outlined readonly").tooltip("Global parameter (edit in Data Import)")

        ui.input('Tilt Axis (°)').bind_value(
            job_model.acquisition, 'tilt_axis_degrees'
        ).props("dense outlined readonly").tooltip("Global parameter (edit in Data Import)")


def _render_logs_tab(job_type, job_model, backend, shared_state):
    """Render logs sub-tab (No change needed)"""
    job_state = shared_state["job_cards"][job_type]
    
    if job_state.get("logs_timer"):
        job_state["logs_timer"].cancel()
        job_state["logs_timer"] = None

    if not job_model.relion_job_name:
        ui.label("Job has not run yet. Logs will appear once it starts.").classes("text-xs text-gray-500 italic")
        return

    with ui.grid(columns=2).classes("w-full").style("gap: 10px; height: calc(100vh - 450px); min-height: 400px;"):
        with ui.column().classes("w-full h-full"):
            ui.label("stdout").classes("text-xs font-medium mb-1")
            stdout_log = ui.log(max_lines=500).classes(
                "w-full h-full border rounded bg-gray-50 p-2 text-xs font-mono"
            ).style("font-family: 'IBM Plex Mono', monospace;")
        
        with ui.column().classes("w-full h-full"):
            ui.label("stderr").classes("text-xs font-medium mb-1")
            stderr_log = ui.log(max_lines=500).classes(
                "w-full h-full border rounded bg-red-50 p-2 text-xs font-mono"
            ).style("font-family: 'IBM Plex Mono', monospace;")

    job_state["monitor_logs"] = {"stdout": stdout_log, "stderr": stderr_log}
    
    asyncio.create_task(_refresh_job_logs(job_type, backend, shared_state))
    
    if job_model.execution_status == JobStatus.RUNNING:
        job_state["logs_timer"] = ui.timer(
            5.0,
            lambda: asyncio.create_task(_refresh_job_logs(job_type, backend, shared_state))
        )


def _render_files_tab(job_type, job_model, shared_state):
    """Render files browser sub-tab (No change needed)"""
    project_path = shared_state.get("current_project_path")
    
    if not project_path or not job_model.relion_job_name:
        ui.label("Job has not run yet. Files will appear once it starts.").classes("text-xs text-gray-500 italic")
        return
        
    job_dir = Path(project_path) / job_model.relion_job_name.rstrip("/")

    ui.label("Job Directory Browser").classes("text-xs font-semibold text-black mb-2")
    current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono mb-2")
    file_list_container = ui.column().classes("w-full border rounded p-2 bg-gray-50").style(
        "height: calc(100vh - 450px); min-height: 400px; overflow-y: auto;"
    )

    def view_file(file_path: Path):
        try:
            text_extensions = [
                ".script", ".txt", ".xml", ".settings", ".log", ".star",
                ".json", ".yaml", ".sh", ".py", ".out", ".err", ".md",
                ".tlt", ".aln", "",
            ]
            if file_path.suffix.lower() in text_extensions:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read(50000)
            else:
                content = "Cannot preview binary file."

            with ui.dialog() as dialog, ui.card().classes("w-[60rem] max-w-full"):
                ui.label(file_path.name).classes("text-sm font-medium mb-2")
                ui.code(content).classes("w-full max-h-96 overflow-auto text-xs")
                ui.button("Close", on_click=dialog.close).props("flat")
            dialog.open()
        except Exception as e:
            ui.notify(f"Error reading file: {e}", type="negative")

    def browse_directory(path: Path):
        file_list_container.clear()
        current_path_label.set_text(str(path))
        
        try:
            if not path.exists():
                with file_list_container:
                    ui.label("Directory not yet created").classes("text-xs text-gray-500 italic p-4")
                    ui.button("Check again", icon="refresh", on_click=lambda: browse_directory(path)).props(
                        "dense flat no-caps"
                    ).style(
                        "background: #f3f4f6; color: #1f2937; padding: 4px 12px; border-radius: 3px; font-size: 11px; margin-top: 8px;"
                    )
                return

            with file_list_container:
                if path != job_dir and path.parent.exists() and job_dir in path.parents:
                    with ui.row().classes(
                        "items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded w-full"
                    ).on("click", lambda p=path.parent: browse_directory(p)):
                        ui.icon("folder_open").classes("text-sm")
                        ui.label("..").classes("text-xs")

                items = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name))
                if not items:
                    ui.label("Directory is empty.").classes("text-xs text-gray-500 italic")

                for item in items:
                    if item.is_dir():
                        with ui.row().classes(
                            "items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded w-full"
                        ).on("click", lambda i=item: browse_directory(i)):
                            ui.icon("folder").classes("text-sm text-blue-600")
                            ui.label(item.name).classes("text-xs")
                    else:
                        with ui.row().classes("items-center gap-2 w-full"):
                            with ui.row().classes(
                                "items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded flex-grow"
                            ).on("click", lambda i=item: view_file(i)):
                                ui.icon("insert_drive_file").classes("text-sm text-gray-600")
                                ui.label(item.name).classes("text-xs")
                            size_kb = item.stat().st_size // 1024
                            ui.label(f"{size_kb} KB").classes("text-xs text-gray-500 ml-auto")
        except Exception as e:
            with file_list_container:
                ui.label(f"Error listing directory: {e}").classes("text-xs text-red-600")
    
    browse_directory(job_dir)


async def _refresh_job_logs(job_type, backend, shared_state):
    """Fetch and update logs (No change needed)"""
    card_data = shared_state["job_cards"].get(job_type)
    if not card_data:
        return

    state = get_project_state()
    job_model = state.jobs.get(job_type)
    if not job_model or not job_model.relion_job_name:
        if card_data.get("logs_timer"):
            card_data["logs_timer"].cancel()
            card_data["logs_timer"] = None
        return

    monitor = card_data.get("monitor_logs")
    if not monitor or monitor["stdout"].is_deleted:
        if card_data.get("logs_timer"):
            card_data["logs_timer"].cancel()
            card_data["logs_timer"] = None
        return

    logs = await backend.get_job_logs(shared_state["current_project_path"], job_model.relion_job_name)
    
    stdout_content = logs.get("stdout", "No output") or "No output yet"
    monitor["stdout"].clear()
    monitor["stdout"].push(stdout_content)
    
    stderr_content = logs.get("stderr", "No errors") or "No errors yet"
    monitor["stderr"].clear()
    monitor["stderr"].push(stderr_content)


def _force_status_refresh(backend, shared_state, callbacks):
    """Force a status refresh (No change needed)"""
    ui.notify("Refreshing statuses...", timeout=1)
    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())

