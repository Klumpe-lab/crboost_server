# ui/pipeline_builder/job_tab_component.py
"""
Job tab component.
Refactored to use @ui.refreshable for status badges and dots.
This ensures valid state on every render and avoids stale widget references.
"""
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Callable

from nicegui import ui

from services.project_state import (
    AlignmentMethod,
    JobStatus,
    JobType,
    get_project_state,
    get_state_service,
)
from ui.status_indicator import ReactiveStatusBadge
from ui.ui_state import (
    get_ui_state_manager,
    UIStateManager,
    MonitorTab,
    get_job_display_name,
)

# ===========================================
# REACTIVE COMPONENTS (The Fix)
# ===========================================

@ui.refreshable
def render_status_badge(job_type: JobType):
    """
    A reactive badge that fetches fresh state on every refresh.
    No more manually updating DOM classes.
    """
    # 1. FETCH FRESH STATE DIRECTLY
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    
    # Fallback if job missing
    status = job_model.execution_status if job_model else JobStatus.SCHEDULED
    
    # 2. DETERMINE STYLES
    colors = {
        JobStatus.SCHEDULED: ("bg-yellow-100", "text-yellow-800"),
        JobStatus.RUNNING:   ("bg-blue-100", "text-blue-800"),
        JobStatus.SUCCEEDED: ("bg-green-100", "text-green-800"),
        JobStatus.FAILED:    ("bg-red-100", "text-red-800"),
        JobStatus.UNKNOWN:   ("bg-gray-100", "text-gray-800"),
    }
    bg, txt = colors.get(status, ("bg-gray-100", "text-gray-800"))
    
    # 3. RENDER
    ui.label(status.value).classes(f"text-xs font-bold px-2 py-0.5 rounded-full {bg} {txt}")


@ui.refreshable
def render_status_dot(job_type: JobType):
    """
    A reactive dot that pulses when running.
    Used in the unified tab switcher.
    """
    # 1. FETCH FRESH STATE DIRECTLY
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    
    status = job_model.execution_status if job_model else JobStatus.SCHEDULED

    # 2. DETERMINE STYLES
    # CSS classes defined in main_ui.py styles (pulse-running, etc.)
    class_map = {
        JobStatus.RUNNING: "pulse-running",
        JobStatus.SUCCEEDED: "pulse-success",
        JobStatus.FAILED: "pulse-failed",
    }
    css_class = class_map.get(status, "pulse-scheduled")
    
    color_map = {
        JobStatus.RUNNING: "#3b82f6",
        JobStatus.SUCCEEDED: "#10b981",
        JobStatus.FAILED: "#ef4444",
    }
    color = color_map.get(status, "#fbbf24")

    # 3. RENDER
    ui.element("div").classes(f"status-dot {css_class}").style(
        f"width: 8px; height: 8px; border-radius: 50%; display: inline-block; background-color: {color};"
    )

# ===========================================
# Helpers
# ===========================================

def is_job_frozen(job_type: JobType) -> bool:
    """Check if a job is frozen (running/succeeded/failed)."""
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    if not job_model:
        return False
    return job_model.execution_status in [
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
    ]

def snake_to_title(s: str) -> str:
    """Convert snake_case to Title Case."""
    return " ".join(word.capitalize() for word in s.split("_"))

async def auto_save_state():
    """Save project state after parameter changes."""
    try:
        await get_state_service().save_project()
        print("[UI] Auto-saved project state")
    except Exception as e:
        print(f"[UI] Auto-save failed: {e}")

def create_save_handler() -> Callable:
    """Create a handler that triggers auto-save."""
    return lambda: asyncio.create_task(auto_save_state())

# ===========================================
# Main Render Function
# ===========================================

def render_job_tab(
    job_type: JobType,
    backend,
    ui_mgr: UIStateManager,
    callbacks: Dict[str, Callable],
) -> None:
    """
    Render a complete job tab with header and content.
    """
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    
    if not job_model:
        ui.label(f"Error: Job model for {job_type.value} not found.").classes(
            "text-xs text-red-600"
        )
        return
    
    job_ui_state = ui_mgr.get_job_ui_state(job_type)
    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    
    # Determine active tab - default to logs if frozen and user hasn't manually switched
    is_frozen = is_job_frozen(job_type)
    active_tab = job_ui_state.active_monitor_tab
    
    if is_frozen and active_tab == MonitorTab.CONFIG and not job_ui_state.user_switched_tab:
        active_tab = MonitorTab.LOGS
        job_ui_state.active_monitor_tab = MonitorTab.LOGS
    

    async def handle_delete():
            # Confirmation Dialog
            with ui.dialog() as dialog, ui.card():
                ui.label(f"Delete {get_job_display_name(job_type)}?").classes("text-lg font-bold")
                ui.label("This will move the files to Trash and remove it from the pipeline.").classes("text-sm text-gray-600")
                
                with ui.row().classes("w-full justify-end mt-4"):
                    ui.button("Cancel", on_click=dialog.close).props("flat")
                    
                async def confirm():
                    dialog.close()
                    # Add ID so we can reference it if needed, or just use try/except
                    notification = ui.notify("Deleting job...", type="ongoing", timeout=0)
                    
                    result = await backend.delete_job(job_type.value)
                    
                    # FIX: Safe dismissal
                    try:
                        if notification:
                            notification.dismiss()
                    except Exception:
                        pass # Notification likely already gone or context lost

                    if result["success"]:
                        ui.notify("Job deleted.", type="positive")
                        
                        # --- FIX: Safe callback invocation ---
                        remove_cb = callbacks.get("remove_job_from_pipeline")
                        if remove_cb:
                            remove_cb(job_type)
                        else:
                            print("[UI WARNING] 'remove_job_from_pipeline' callback not found")
                            
                    else:
                        ui.notify(f"Error: {result.get('error')}", type="negative")

                ui.button("Delete", color="red", on_click=confirm)
            
            dialog.open()




    # ===========================================
    # Header Section
    # ===========================================


    
    with ui.column().classes("w-full border-b border-gray-200 bg-white pl-6 pr-6 pt-4 pb-4"):
        with ui.row().classes("w-full justify-between items-center"):
            # Left: Project metadata
            with ui.column().classes("gap-0"):
                with ui.row().classes("items-center gap-2"):
                    ui.label(state.project_name).classes("text-lg font-bold text-gray-800")
                    
                    # --- REACTIVE COMPONENT ---
                    # render_status_badge(job_type)
                    ReactiveStatusBadge(job_type)
                    # --------------------------
                
                # Timestamps
                created = (
                    state.created_at.strftime("%Y-%m-%d %H:%M")
                    if isinstance(state.created_at, datetime)
                    else str(state.created_at)
                )
                modified = (
                    state.modified_at.strftime("%Y-%m-%d %H:%M")
                    if isinstance(state.modified_at, datetime)
                    else str(state.modified_at)
                )
                ui.label(f"Created: {created} · Modified: {modified}").classes(
                    "text-xs text-gray-400"
                )
            
            # Right: Tab switcher and refresh
            with ui.row().classes("items-center gap-4"):
                # Tab switcher container
                switcher_container = ui.row().classes(
                    "bg-gray-100 p-1 rounded-lg gap-0 border border-gray-200"
                )
                widget_refs.switcher_container = switcher_container
                
                _render_tab_switcher(
                    switcher_container,
                    job_type,
                    active_tab,
                    backend,
                    ui_mgr,
                    callbacks,
                )
                
                ui.button(
                    icon="refresh",
                    on_click=lambda: _force_status_refresh(callbacks),
                ).props("flat dense round").classes("text-gray-400 hover:text-gray-800")
                if ui_mgr.is_project_created:
                    ui.button(icon="delete", on_click=handle_delete).props("flat round dense color=red").tooltip("Delete this job")
    
    # ===========================================
    # Content Section
    # ===========================================
    
    content_container = ui.column().classes("w-full flex-grow p-6 overflow-hidden")
    widget_refs.content_container = content_container
    
    with content_container:
        if active_tab == MonitorTab.CONFIG:
            _render_config_tab(job_type, job_model, is_frozen, ui_mgr)
        elif active_tab == MonitorTab.LOGS:
            _render_logs_tab(job_type, job_model, backend, ui_mgr)
        elif active_tab == MonitorTab.FILES:
            _render_files_tab(job_type, job_model, ui_mgr)


# ===========================================
# Tab Switcher
# ===========================================

def _render_tab_switcher(
    container,
    job_type: JobType,
    active_tab: MonitorTab,
    backend,
    ui_mgr: UIStateManager,
    callbacks: Dict[str, Callable],
):
    """Render the tab switcher buttons."""
    container.clear()
    
    tabs = [
        (MonitorTab.CONFIG, "Parameters"),
        (MonitorTab.LOGS, "Logs"),
        (MonitorTab.FILES, "Files"),
    ]
    
    with container:
        for tab, label in tabs:
            is_active = active_tab == tab
            
            btn = ui.button(
                label,
                on_click=lambda t=tab: _handle_tab_switch(
                    job_type, t, backend, ui_mgr, callbacks
                ),
            )
            btn.props("flat dense no-caps")
            
            base_style = (
                "font-size: 12px; font-weight: 500; padding: 4px 16px; "
                "border-radius: 6px; transition: all 0.2s;"
            )
            
            if is_active:
                btn.style(
                    f"{base_style} background: white; color: #111827; "
                    "box-shadow: 0 1px 3px rgba(0,0,0,0.1);"
                )
            else:
                btn.style(f"{base_style} background: transparent; color: #6b7280;")


def _handle_tab_switch(
    job_type: JobType,
    tab: MonitorTab,
    backend,
    ui_mgr: UIStateManager,
    callbacks: Dict[str, Callable],
):
    """Handle switching between tabs."""
    # Update state
    ui_mgr.set_job_monitor_tab(job_type, tab, user_initiated=True)
    
    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    
    # Update tab switcher buttons
    if widget_refs.switcher_container:
        _render_tab_switcher(
            widget_refs.switcher_container,
            job_type,
            tab,
            backend,
            ui_mgr,
            callbacks,
        )
    
    # Cleanup logs timer if switching away from logs
    if tab != MonitorTab.LOGS:
        ui_mgr.cleanup_job_logs_timer(job_type)
    
    # Re-render content
    content_container = widget_refs.content_container
    if content_container:
        state = get_project_state()
        job_model = state.jobs.get(job_type)
        is_frozen = is_job_frozen(job_type)
        
        content_container.clear()
        with content_container:
            if tab == MonitorTab.CONFIG:
                _render_config_tab(job_type, job_model, is_frozen, ui_mgr)
            elif tab == MonitorTab.LOGS:
                _render_logs_tab(job_type, job_model, backend, ui_mgr)
            elif tab == MonitorTab.FILES:
                _render_files_tab(job_type, job_model, ui_mgr)

# ===========================================
# Config Tab
# ===========================================

def _render_config_tab(job_type: JobType, job_model, is_frozen: bool, ui_mgr: UIStateManager):
    """Render the configuration/parameters tab."""
    save_handler = create_save_handler()
    
    with ui.column().classes("w-full max-w-4xl h-full overflow-y-auto pr-2"):
        # I/O Configuration
        ui.label("I/O Configuration").classes("text-sm font-bold text-gray-900 mb-3")
        
        with ui.card().classes("w-full p-0 gap-0 border border-gray-200 shadow-none mb-6"):
            paths_data = job_model.paths
            
            if paths_data:
                for i, (key, value) in enumerate(paths_data.items()):
                    bg_class = "bg-gray-50" if i % 2 == 0 else "bg-white"
                    with ui.row().classes(
                        f"w-full p-3 {bg_class} border-b border-gray-100 "
                        "last:border-0 justify-between items-start gap-4"
                    ):
                        ui.label(snake_to_title(key)).classes(
                            "text-xs font-semibold text-gray-500 uppercase w-32 pt-0.5"
                        )
                        ui.label(str(value)).classes(
                            "text-xs font-mono text-gray-700 break-all flex-1"
                        )
            else:
                ui.label("Paths calculated upon pipeline creation.").classes(
                    "text-sm text-gray-400 italic p-4"
                )
        
        # Job Parameters
        ui.label("Job Parameters").classes("text-sm font-bold text-gray-900 mb-3")
        
        # Get job-specific fields
        base_fields = {
            "execution_status",
            "relion_job_name",
            "relion_job_number",
            "paths",
            "additional_binds",
            "JOB_CATEGORY",
        }
        job_specific_fields = set(job_model.model_fields.keys()) - base_fields
        
        if not job_specific_fields:
            ui.label("This job has no configurable parameters.").classes(
                "text-xs text-gray-500 italic mb-4"
            )
        
        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            for param_name in sorted(list(job_specific_fields)):
                label = snake_to_title(param_name)
                value = getattr(job_model, param_name)
                
                if isinstance(value, bool):
                    checkbox = ui.checkbox(label).bind_value(job_model, param_name)
                    if not is_frozen:
                        checkbox.on_value_change(save_handler)
                    else:
                        checkbox.disable()
                
                elif isinstance(value, (int, float)) or value is None:
                    inp = ui.input(label).bind_value(job_model, param_name)
                    inp.props("outlined dense").classes("w-full")
                    if is_frozen:
                        inp.classes("bg-gray-50 text-gray-500").props("readonly")
                    else:
                        inp.on_value_change(save_handler)
                
                elif isinstance(value, str):
                    # Special handling for alignment method enum
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        sel = ui.select(
                            options=[e.value for e in AlignmentMethod],
                            value=value,
                            label=label,
                        )
                        sel.bind_value(job_model, param_name)
                        sel.props("outlined dense").classes("w-full")
                        if is_frozen:
                            sel.classes("bg-gray-50 text-gray-500")
                            sel.disable()
                        else:
                            sel.on_value_change(save_handler)
                    else:
                        inp = ui.input(label).bind_value(job_model, param_name)
                        inp.props("outlined dense").classes("w-full")
                        if is_frozen:
                            inp.classes("bg-gray-50 text-gray-500").props("readonly")
                        else:
                            inp.on_value_change(save_handler)
        
        # Global Parameters (read-only)
        ui.label("Global Experimental Parameters (Read-Only)").classes(
            "text-sm font-bold text-gray-900 mb-3"
        )
        
        with ui.grid(columns=3).classes("w-full gap-4"):
            ui.input("Pixel Size (Å)").bind_value(
                job_model.microscope, "pixel_size_angstrom"
            ).props("dense outlined readonly").tooltip("Global parameter")
            
            ui.input("Voltage (kV)").bind_value(
                job_model.microscope, "acceleration_voltage_kv"
            ).props("dense outlined readonly").tooltip("Global parameter")
            
            ui.input("Cs (mm)").bind_value(
                job_model.microscope, "spherical_aberration_mm"
            ).props("dense outlined readonly").tooltip("Global parameter")
            
            ui.input("Amplitude Contrast").bind_value(
                job_model.microscope, "amplitude_contrast"
            ).props("dense outlined readonly").tooltip("Global parameter")
            
            ui.input("Dose per Tilt").bind_value(
                job_model.acquisition, "dose_per_tilt"
            ).props("dense outlined readonly").tooltip("Global parameter")
            
            ui.input("Tilt Axis (°)").bind_value(
                job_model.acquisition, "tilt_axis_degrees"
            ).props("dense outlined readonly").tooltip("Global parameter")


# ===========================================
# Logs Tab
# ===========================================

def _render_logs_tab(job_type: JobType, job_model, backend, ui_mgr: UIStateManager):
    """Render the logs tab."""
    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    
    # Cleanup existing timer
    ui_mgr.cleanup_job_logs_timer(job_type)
    
    if not job_model.relion_job_name:
        with ui.column().classes(
            "w-full h-full items-center justify-center text-gray-400 gap-2"
        ):
            ui.icon("schedule", size="48px")
            ui.label("Job scheduled. Logs will appear here once running.")
        return
    
    with ui.grid(columns=2).classes("w-full h-full gap-4"):
        # Stdout
        with ui.column().classes(
            "h-full overflow-hidden flex flex-col border border-gray-200 rounded-lg"
        ):
            ui.label("Standard Output").classes(
                "text-xs font-bold text-gray-500 uppercase px-3 py-2 "
                "bg-gray-50 border-b border-gray-200 w-full"
            )
            stdout_log = ui.log(max_lines=1000).classes(
                "w-full p-3 font-mono text-xs bg-white"
            ).style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
        
        # Stderr
        with ui.column().classes(
            "h-full overflow-hidden flex flex-col border border-red-100 rounded-lg"
        ):
            ui.label("Standard Error").classes(
                "text-xs font-bold text-red-500 uppercase px-3 py-2 "
                "bg-red-50 border-b border-red-100 w-full"
            )
            stderr_log = ui.log(max_lines=1000).classes(
                "w-full p-3 font-mono text-xs bg-white text-red-700"
            ).style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
    
    widget_refs.monitor_logs = {"stdout": stdout_log, "stderr": stderr_log}
    
    # Initial refresh
    asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))
    
    # Start polling if job is running
    if job_model.execution_status == JobStatus.RUNNING:
        widget_refs.logs_timer = ui.timer(
            3.0,
            lambda: asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr)),
        )


async def _refresh_job_logs(job_type: JobType, backend, ui_mgr: UIStateManager):
    """Refresh the logs display for a job."""
    widget_refs = ui_mgr.get_job_widget_refs(job_type)
    monitor = widget_refs.monitor_logs
    
    if not monitor or "stdout" not in monitor:
        return
    
    # Check if widgets are still valid
    if monitor["stdout"].is_deleted:
        ui_mgr.cleanup_job_logs_timer(job_type)
        return
    
    state = get_project_state()
    job_model = state.jobs.get(job_type)
    
    if not job_model or not job_model.relion_job_name:
        ui_mgr.cleanup_job_logs_timer(job_type)
        return
    
    project_path = ui_mgr.project_path
    if not project_path:
        return
    
    logs = await backend.get_job_logs(str(project_path), job_model.relion_job_name)
    
    monitor["stdout"].clear()
    monitor["stdout"].push(logs.get("stdout", "No output") or "No output yet")
    
    monitor["stderr"].clear()
    monitor["stderr"].push(logs.get("stderr", "No errors") or "No errors yet")


# ===========================================
# Files Tab
# ===========================================

def _render_files_tab(job_type: JobType, job_model, ui_mgr: UIStateManager):
    """Render the files browser tab."""
    project_path = ui_mgr.project_path
    
    if not project_path or not job_model.relion_job_name:
        ui.label("Job not started.").classes("text-gray-400 p-4")
        return
    
    job_dir = project_path / job_model.relion_job_name.rstrip("/")
    
    with ui.column().classes(
        "w-full h-full flex flex-col border border-gray-200 rounded-lg overflow-hidden"
    ):
        ui.label(f"Browsing: {job_dir.name}").classes(
            "text-xs font-bold bg-gray-50 w-full px-3 py-2 "
            "border-b border-gray-200 text-gray-600"
        )
        
        current_path_label = ui.label(str(job_dir)).classes(
            "text-xs text-gray-600 font-mono px-3 py-1 "
            "bg-gray-50 border-b border-gray-200"
        )
        
        file_list_container = ui.column().classes("w-full flex-grow overflow-y-auto p-0")
    
    def view_file(file_path: Path):
        """Open a file viewer dialog."""
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
        """Browse a directory and display its contents."""
        file_list_container.clear()
        current_path_label.set_text(str(path))
        
        try:
            if not path.exists():
                with file_list_container:
                    ui.label("Directory not yet created").classes(
                        "text-xs text-gray-500 italic p-4"
                    )
                return
            
            with file_list_container:
                # Parent directory link
                if path != job_dir and path.parent.exists() and job_dir in path.parents:
                    with ui.row().classes(
                        "w-full items-center gap-2 cursor-pointer "
                        "hover:bg-gray-100 p-2 border-b border-gray-100"
                    ).on("click", lambda p=path.parent: browse_directory(p)):
                        ui.icon("folder_open").classes("text-sm text-gray-400")
                        ui.label("..").classes("text-xs font-medium")
                
                # Directory contents
                items = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name))
                
                for item in items:
                    if item.is_dir():
                        with ui.row().classes(
                            "w-full items-center gap-2 cursor-pointer "
                            "hover:bg-gray-100 p-2 border-b border-gray-100"
                        ).on("click", lambda i=item: browse_directory(i)):
                            ui.icon("folder").classes("text-sm text-blue-400")
                            ui.label(item.name).classes("text-xs font-medium text-gray-700")
                    else:
                        with ui.row().classes(
                            "w-full items-center gap-2 cursor-pointer "
                            "hover:bg-gray-100 p-2 border-b border-gray-100"
                        ).on("click", lambda i=item: view_file(i)):
                            ui.icon("insert_drive_file").classes("text-sm text-gray-400")
                            ui.label(item.name).classes("text-xs text-gray-700 flex-1")
                            size_kb = item.stat().st_size // 1024
                            ui.label(f"{size_kb} KB").classes("text-xs text-gray-400")
        
        except Exception as e:
            with file_list_container:
                ui.label(f"Error: {e}").classes("text-xs text-red-600 p-4")
    
    # Initial browse
    browse_directory(job_dir)


def _force_status_refresh(callbacks: Dict[str, Callable]):
    """Force a status refresh via the new reactive mechanism."""
    ui.notify("Refreshing statuses...", timeout=1)
    
    # Trigger global reactive refresh
    render_status_badge.refresh()
    render_status_dot.refresh()
    
    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())
