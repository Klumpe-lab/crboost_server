# ui/pipeline_builder/job_tab_component.py
import asyncio
from pathlib import Path
from datetime import datetime
from nicegui import ui
from services.project_state import AlignmentMethod, JobStatus, JobType, get_project_state, get_state_service
from ui.utils import _snake_to_title
from typing import Dict, Any, Set

# --- Auto-save handler ---
async def auto_save_state():
    try:
        await get_state_service().save_project()
    except Exception as e:
        print(f"[UI] Job tab auto-save failed: {e}")

save_handler = lambda: asyncio.create_task(auto_save_state())

def get_status_class(status: JobStatus) -> str:
    """Get CSS class for pulsating dots based on status"""
    if status == JobStatus.RUNNING: return "pulse-running"
    if status == JobStatus.SUCCEEDED: return "pulse-success"
    if status == JobStatus.FAILED: return "pulse-failed"
    return "pulse-scheduled"

def get_status_hex_color(status: JobStatus) -> str:
    """Get the actual Hex code for the inline style"""
    if status == JobStatus.RUNNING: return "#3b82f6"   # Blue
    if status == JobStatus.SUCCEEDED: return "#10b981" # Green
    if status == JobStatus.FAILED: return "#ef4444"    # Red
    return "#fbbf24"

def _update_badge_color(label, status: JobStatus):
    """Helper to color the status badge in the header"""
    colors = {
        JobStatus.SCHEDULED: ("bg-yellow-100", "text-yellow-800"),
        JobStatus.RUNNING: ("bg-blue-100", "text-blue-800"),
        JobStatus.SUCCEEDED: ("bg-green-100", "text-green-800"),
        JobStatus.FAILED: ("bg-red-100", "text-red-800"),
        JobStatus.UNKNOWN: ("bg-gray-100", "text-gray-800")
    }
    bg, txt = colors.get(status, ("bg-gray-100", "text-gray-800"))
    label.classes(remove="bg-yellow-100 text-yellow-800 bg-blue-100 text-blue-800 bg-green-100 text-green-800 bg-red-100 text-red-800 bg-gray-100 text-gray-800")
    label.classes(f"{bg} {txt}")

def is_job_frozen(job_type: JobType) -> bool:
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    if not job_model: return False
    return job_model.execution_status in [JobStatus.RUNNING, JobStatus.SUCCEEDED, JobStatus.FAILED]

def render_job_tab(
    job_type: JobType,
    backend,
    shared_state: Dict[str, Any],
    callbacks: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Render a complete job tab with Header and Content.
    """
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    
    if not job_model:
        ui.label(f"Error: Job model for {job_type.value} not found.").classes("text-xs text-red-600")
        return {}

    job_state = shared_state["job_cards"].get(job_type, {})
    
    # Logic: default to logs if frozen, unless user manually switched
    is_frozen = is_job_frozen(job_type)
    active_sub_tab = job_state.get("active_monitor_tab", "config")
    
    if is_frozen and active_sub_tab == "config" and not job_state.get("user_switched_tab", False):
        active_sub_tab = "logs"
        job_state["active_monitor_tab"] = "logs"

    # --- HEADER SECTION ---
    with ui.column().classes("w-full border-b border-gray-200 bg-white pl-6 pr-6 pt-4 pb-4"):
        
        with ui.row().classes("w-full justify-between items-center"):
            # Left: Project Metadata
            with ui.column().classes("gap-0"):
                with ui.row().classes("items-center gap-2"):
                    ui.label(state.project_name).classes("text-lg font-bold text-gray-800")
                    # Status Badge
                    status_badge = ui.label(job_model.execution_status.value).classes("text-xs font-bold px-2 py-0.5 rounded-full")
                    _update_badge_color(status_badge, job_model.execution_status)
                    job_state["ui_status_label"] = status_badge

                # Dates
                created = state.created_at.strftime("%Y-%m-%d %H:%M") if isinstance(state.created_at, datetime) else str(state.created_at)
                modified = state.modified_at.strftime("%Y-%m-%d %H:%M") if isinstance(state.modified_at, datetime) else str(state.modified_at)
                ui.label(f"Created: {created} • Modified: {modified}").classes("text-xs text-gray-400")

            # Right: Controls (Inset Switch + Refresh)
            with ui.row().classes("items-center gap-4"):
                
                # Container for the switcher buttons so we can refresh them
                switcher_container = ui.row().classes("bg-gray-100 p-1 rounded-lg gap-0 border border-gray-200")
                job_state["switcher_container"] = switcher_container
                
                # Render initial buttons
                _render_tab_switcher(switcher_container, job_type, active_sub_tab, backend, shared_state, callbacks)

                ui.button(icon="refresh", on_click=lambda: _force_status_refresh(backend, shared_state, callbacks)) \
                    .props("flat dense round").classes("text-gray-400 hover:text-gray-800")

    # --- CONTENT SECTION ---
    content_container = ui.column().classes("w-full flex-grow p-6 overflow-hidden") 
    job_state["monitor_content_container"] = content_container

    with content_container:
        if active_sub_tab == "config":
            _render_config_tab(job_type, job_model, is_frozen, shared_state)
        elif active_sub_tab == "logs":
            _render_logs_tab(job_type, job_model, backend, shared_state)
        elif active_sub_tab == "files":
            _render_files_tab(job_type, job_model, shared_state)

    return {
        "status_badge": status_badge,
        "content_container": content_container,
    }

def _render_tab_switcher(container, job_type, active_tab, backend, shared_state, callbacks):
    """Renders the 3 buttons. Called initially and when switching tabs."""
    container.clear()
    with container:
        for tab_name, tab_label in [("config", "Parameters"), ("logs", "Logs"), ("files", "Files")]:
            is_active = active_tab == tab_name
            
            btn = ui.button(
                tab_label, 
                on_click=lambda t=tab_name: _handle_tab_switch(job_type, t, backend, shared_state, callbacks)
            )
            btn.props("flat dense no-caps")
            
            base_style = "font-size: 12px; font-weight: 500; padding: 4px 16px; border-radius: 6px; transition: all 0.2s;"
            if is_active:
                btn.style(f"{base_style} background: white; color: #111827; box-shadow: 0 1px 3px rgba(0,0,0,0.1);")
            else:
                btn.style(f"{base_style} background: transparent; color: #6b7280;")

def _handle_tab_switch(job_type, tab_name, backend, shared_state, callbacks):
    """Handler that updates both the content AND the button styles."""
    job_state = shared_state["job_cards"][job_type]
    job_state["active_monitor_tab"] = tab_name
    job_state["user_switched_tab"] = True
    
    # 1. Re-render the buttons to update "Active" style
    if switcher := job_state.get("switcher_container"):
        _render_tab_switcher(switcher, job_type, tab_name, backend, shared_state, callbacks)

    # 2. Cleanup timers
    if tab_name != "logs" and job_state.get("logs_timer"):
        job_state["logs_timer"].cancel()
        job_state["logs_timer"] = None
    
    # 3. Re-render Content
    container = job_state.get("monitor_content_container")
    if container:
        state = get_project_state()
        job_model = state.jobs.get(job_type)
        is_frozen = is_job_frozen(job_type)

        container.clear()
        with container:
            if tab_name == "config":
                _render_config_tab(job_type, job_model, is_frozen, shared_state)
            elif tab_name == "logs":
                _render_logs_tab(job_type, job_model, backend, shared_state)
            elif tab_name == "files":
                _render_files_tab(job_type, job_model, shared_state)

def _render_config_tab(job_type, job_model, is_frozen, shared_state):
    with ui.column().classes("w-full max-w-4xl h-full overflow-y-auto pr-2"):
        
        # I/O
        ui.label("I/O Configuration").classes("text-sm font-bold text-gray-900 mb-3")
        with ui.card().classes("w-full p-0 gap-0 border border-gray-200 shadow-none mb-6"):
            paths_data = shared_state.get("params_snapshot", {}).get(job_type, {}).get("paths", job_model.paths)
            if paths_data:
                for i, (key, value) in enumerate(paths_data.items()):
                    bg_class = "bg-gray-50" if i % 2 == 0 else "bg-white"
                    with ui.row().classes(f"w-full p-3 {bg_class} border-b border-gray-100 last:border-0 justify-between items-start gap-4"):
                        ui.label(_snake_to_title(key)).classes("text-xs font-semibold text-gray-500 uppercase w-32 pt-0.5")
                        ui.label(str(value)).classes("text-xs font-mono text-gray-700 break-all flex-1")
            else:
                ui.label("Paths calculated upon pipeline creation.").classes("text-sm text-gray-400 italic p-4")

        # Parameters
        ui.label("Job Parameters").classes("text-sm font-bold text-gray-900 mb-3")
        base_fields = {'execution_status', 'relion_job_name', 'relion_job_number', 'paths', 'additional_binds', 'JOB_CATEGORY'}
        job_specific_fields = set(job_model.model_fields.keys()) - base_fields

        if not job_specific_fields:
            ui.label("This job has no configurable parameters.").classes("text-xs text-gray-500 italic mb-4")
        
        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            for param_name in sorted(list(job_specific_fields)):
                label = _snake_to_title(param_name)
                value = getattr(job_model, param_name)

                def style_input(el):
                    el.props("outlined dense").classes("w-full")
                    if is_frozen: el.classes("bg-gray-50 text-gray-500").props("readonly")
                    return el

                if isinstance(value, bool):
                    c = ui.checkbox(label).bind_value(job_model, param_name)
                    if not is_frozen: c.on_value_change(save_handler)
                    else: c.disable()
                
                elif isinstance(value, (int, float)) or value is None:
                    el = ui.input(label).bind_value(job_model, param_name)
                    if not is_frozen: el.on_value_change(save_handler)
                    style_input(el)
                
                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        s = ui.select(options=[e.value for e in AlignmentMethod], value=value, label=label)
                        s.bind_value(job_model, param_name)
                        if not is_frozen: s.on_value_change(save_handler)
                        style_input(s)
                        if is_frozen: s.disable()
                    else:
                        el = ui.input(label).bind_value(job_model, param_name)
                        if not is_frozen: el.on_value_change(save_handler)
                        style_input(el)

        # Global
        ui.label("Global Experimental Parameters (Read-Only)").classes("text-sm font-bold text-gray-900 mb-3")
        with ui.grid(columns=3).classes("w-full gap-4"):
            ui.input('Pixel Size (Å)').bind_value(job_model.microscope, 'pixel_size_angstrom').props("dense outlined readonly").tooltip("Global parameter")
            ui.input('Voltage (kV)').bind_value(job_model.microscope, 'acceleration_voltage_kv').props("dense outlined readonly").tooltip("Global parameter")
            ui.input('Cs (mm)').bind_value(job_model.microscope, 'spherical_aberration_mm').props("dense outlined readonly").tooltip("Global parameter")
            ui.input('Amplitude Contrast').bind_value(job_model.microscope, 'amplitude_contrast').props("dense outlined readonly").tooltip("Global parameter")
            ui.input('Dose per Tilt').bind_value(job_model.acquisition, 'dose_per_tilt').props("dense outlined readonly").tooltip("Global parameter")
            ui.input('Tilt Axis (°)').bind_value(job_model.acquisition, 'tilt_axis_degrees').props("dense outlined readonly").tooltip("Global parameter")

def _render_logs_tab(job_type, job_model, backend, shared_state):
    job_state = shared_state["job_cards"][job_type]
    
    if job_state.get("logs_timer"):
        job_state["logs_timer"].cancel()
        job_state["logs_timer"] = None

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job scheduled. Logs will appear here once running.")
        return

    with ui.grid(columns=2).classes("w-full h-full gap-4"):
        with ui.column().classes("h-full overflow-hidden flex flex-col border border-gray-200 rounded-lg"):
            ui.label("Standard Output").classes("text-xs font-bold text-gray-500 uppercase px-3 py-2 bg-gray-50 border-b border-gray-200 w-full")
            stdout_log = ui.log(max_lines=1000).classes("w-full p-3 font-mono text-xs bg-white").style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
        
        with ui.column().classes("h-full overflow-hidden flex flex-col border border-red-100 rounded-lg"):
            ui.label("Standard Error").classes("text-xs font-bold text-red-500 uppercase px-3 py-2 bg-red-50 border-b border-red-100 w-full")
            stderr_log = ui.log(max_lines=1000).classes("w-full p-3 font-mono text-xs bg-white text-red-700").style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")

    job_state["monitor_logs"] = {"stdout": stdout_log, "stderr": stderr_log}
    asyncio.create_task(_refresh_job_logs(job_type, backend, shared_state))
    
    if job_model.execution_status == JobStatus.RUNNING:
        job_state["logs_timer"] = ui.timer(3.0, lambda: asyncio.create_task(_refresh_job_logs(job_type, backend, shared_state)))

async def _refresh_job_logs(job_type, backend, shared_state):
    card_data = shared_state["job_cards"].get(job_type)
    if not card_data: return
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
    monitor["stdout"].clear()
    monitor["stdout"].push(logs.get("stdout", "No output") or "No output yet")
    monitor["stderr"].clear()
    monitor["stderr"].push(logs.get("stderr", "No errors") or "No errors yet")

def _render_files_tab(job_type, job_model, shared_state):
    project_path = shared_state.get("current_project_path")
    if not project_path or not job_model.relion_job_name:
        ui.label("Job not started.").classes("text-gray-400 p-4")
        return

    job_dir = Path(project_path) / job_model.relion_job_name.rstrip("/")
    
    with ui.column().classes("w-full h-full flex flex-col border border-gray-200 rounded-lg overflow-hidden"):
        ui.label(f"Browsing: {job_dir.name}").classes("text-xs font-bold bg-gray-50 w-full px-3 py-2 border-b border-gray-200 text-gray-600")
        current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono px-3 py-1 bg-gray-50 border-b border-gray-200")
        file_list_container = ui.column().classes("w-full flex-grow overflow-y-auto p-0")

    def view_file(file_path: Path):
        try:
            text_extensions = [".script", ".txt", ".xml", ".settings", ".log", ".star", ".json", ".yaml", ".sh", ".py", ".out", ".err", ".md", ".tlt", ".aln", ""]
            if file_path.suffix.lower() in text_extensions:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read(50000)
            else: content = "Cannot preview binary file."

            with ui.dialog() as dialog, ui.card().classes("w-[60rem] max-w-full"):
                ui.label(file_path.name).classes("text-sm font-medium mb-2")
                ui.code(content).classes("w-full max-h-96 overflow-auto text-xs")
                ui.button("Close", on_click=dialog.close).props("flat")
            dialog.open()
        except Exception as e: ui.notify(f"Error reading file: {e}", type="negative")

    def browse_directory(path: Path):
        file_list_container.clear()
        current_path_label.set_text(str(path))
        try:
            if not path.exists():
                with file_list_container: ui.label("Directory not yet created").classes("text-xs text-gray-500 italic p-4")
                return
            with file_list_container:
                if path != job_dir and path.parent.exists() and job_dir in path.parents:
                    with ui.row().classes("w-full items-center gap-2 cursor-pointer hover:bg-gray-100 p-2 border-b border-gray-100").on("click", lambda p=path.parent: browse_directory(p)):
                        ui.icon("folder_open").classes("text-sm text-gray-400")
                        ui.label("..").classes("text-xs font-medium")
                for item in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name)):
                    if item.is_dir():
                        with ui.row().classes("w-full items-center gap-2 cursor-pointer hover:bg-gray-100 p-2 border-b border-gray-100").on("click", lambda i=item: browse_directory(i)):
                            ui.icon("folder").classes("text-sm text-blue-400")
                            ui.label(item.name).classes("text-xs font-medium text-gray-700")
                    else:
                        with ui.row().classes("w-full items-center gap-2 cursor-pointer hover:bg-gray-100 p-2 border-b border-gray-100").on("click", lambda i=item: view_file(i)):
                            ui.icon("insert_drive_file").classes("text-sm text-gray-400")
                            ui.label(item.name).classes("text-xs text-gray-700 flex-1")
                            ui.label(f"{item.stat().st_size // 1024} KB").classes("text-xs text-gray-400")
        except Exception as e:
            with file_list_container: ui.label(f"Error: {e}").classes("text-xs text-red-600 p-4")

    browse_directory(job_dir)

def _force_status_refresh(backend, shared_state, callbacks):
    ui.notify("Refreshing statuses...", timeout=1)
    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())
