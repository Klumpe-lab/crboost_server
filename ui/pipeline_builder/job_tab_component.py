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

from services.project_state import AlignmentMethod, JobStatus, JobType, get_project_state, get_state_service
from ui.status_indicator import ReactiveStatusBadge
from ui.template_workbench import TemplateWorkbench
from ui.ui_state import get_ui_state_manager, UIStateManager, MonitorTab, get_job_display_name


# ===========================================
# REACTIVE COMPONENTS
# ===========================================

def _render_slurm_config_section(job_model, is_frozen: bool, save_handler: Callable):
    """
    Main container for SLURM section. 
    The expansion panel itself is NOT refreshable so it stays open.
    """
    with ui.expansion("SLURM Resources", icon="memory").classes(
        "w-full border border-gray-200 rounded-lg mb-6 shadow-sm overflow-hidden"
    ).props("dense header-class='bg-gray-50 text-gray-700 font-bold'"):
        # We wrap the INNER content in the refreshable function
        _render_slurm_content(job_model, is_frozen, save_handler)


@ui.refreshable
def _render_slurm_content(job_model, is_frozen: bool, save_handler: Callable):
    """The actual interactive content that updates when presets are clicked."""
    from services.project_state import SlurmPreset, SLURM_PRESET_MAP
    
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    # Use .get because the key might be missing or an enum/string
    raw_preset = overrides.get("preset", SlurmPreset.CUSTOM)
    current_preset = raw_preset.value if hasattr(raw_preset, 'value') else str(raw_preset)
    
    # --- PRESET PILLS ---
    with ui.row().classes("w-full items-center gap-2 p-3 bg-white border-b border-gray-100"):
        ui.label("Presets:").classes("text-[10px] font-black text-gray-400 uppercase mr-2")
        
        for preset in [SlurmPreset.SMALL, SlurmPreset.MEDIUM, SlurmPreset.LARGE]:
            preset_info = SLURM_PRESET_MAP[preset]
            is_active = current_preset == preset.value
            
            def apply_preset(p=preset):
                job_model.apply_slurm_preset(p)
                _render_slurm_content.refresh()
                # ui.notify(f" {p.value}", icon="done")

            ui.button(preset_info["label"], on_click=apply_preset) \
                .props(f"unelevated no-caps dense") \
                .classes(f"rounded-full px-3 text-xs {'bg-blue-600 text-white' if is_active else 'bg-gray-100 text-gray-600'}")
        
        ui.space()
        
        if overrides:
            ui.button(icon="restart_alt", on_click=lambda: (job_model.clear_slurm_overrides(), _render_slurm_content.refresh())) \
                .props("flat dense").classes("text-red-400").tooltip("Clear Overrides")

    # --- PARAMETER GRID ---
    with ui.grid(columns=4).classes("w-full gap-x-6 gap-y-4 p-4 bg-white"):
        fields = [
            ("partition", "Partition"), ("constraint", "Constraint"),
            ("nodes", "Nodes"), ("ntasks_per_node", "Tasks/Node"),
            ("cpus_per_task", "CPUs/Task"), ("gres", "GRES (GPU)"),
            ("mem", "Memory"), ("time", "Time Limit"),
        ]
        
        for field_name, label in fields:
            val = getattr(effective_config, field_name)
            
            with ui.column().classes("gap-0"):
                ui.label(label).classes("text-[10px] font-bold text-gray-400 uppercase ml-1")
                
                # FIX: Use 'e.sender.value' for GenericEventArguments (blur)
                def make_blur_handler(fname):
                    return lambda e: (
                        job_model.set_slurm_override(fname, e.sender.value),
                        _render_slurm_content.refresh()
                    )
                
                inp = ui.input(value=str(val)).props("outlined dense shadow-0")
                inp.classes("w-full text-xs font-mono")
                
                if is_frozen:
                    inp.props("readonly bg-color=grey-1")
                else:
                    inp.on("blur", make_blur_handler(field_name))


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
        JobStatus.RUNNING: ("bg-blue-100", "text-blue-800"),
        JobStatus.SUCCEEDED: ("bg-green-100", "text-green-800"),
        JobStatus.FAILED: ("bg-red-100", "text-red-800"),
        JobStatus.UNKNOWN: ("bg-gray-100", "text-gray-800"),
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
    class_map = {
        JobStatus.RUNNING: "pulse-running",
        JobStatus.SUCCEEDED: "pulse-success",
        JobStatus.FAILED: "pulse-failed",
    }
    css_class = class_map.get(status, "pulse-scheduled")

    color_map = {JobStatus.RUNNING: "#3b82f6", JobStatus.SUCCEEDED: "#10b981", JobStatus.FAILED: "#ef4444"}
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
    return job_model.execution_status in [JobStatus.RUNNING, JobStatus.SUCCEEDED, JobStatus.FAILED]


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


def render_job_tab(job_type: JobType, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]) -> None:
    state = get_project_state()
    job_model = state.jobs.get(job_type)

    if not job_model:
        ui.label(f"Error: Job model for {job_type.value} not found.").classes("text-xs text-red-600")
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
        """Handle job deletion with orphan preview."""
        from services.pipeline_deletion_service import get_deletion_service

        deletion_service = get_deletion_service()
        project_path = ui_mgr.project_path

        # Get preview of what will be orphaned
        preview = None
        if project_path and job_model.relion_job_name:
            preview = deletion_service.preview_deletion(
                project_path, job_model.relion_job_name, job_resolver=backend.pipeline_orchestrator.job_resolver
            )

        with ui.dialog() as dialog, ui.card().classes("w-[28rem]"):
            ui.label(f"Delete {get_job_display_name(job_type)}?").classes("text-lg font-bold")
            ui.label("This will move the job files to Trash/ and remove it from the pipeline.").classes(
                "text-sm text-gray-600 mb-2"
            )

            # Show orphan warning if applicable
            if preview and preview.get("success") and preview.get("downstream_count", 0) > 0:
                downstream = preview.get("downstream_jobs", [])

                with ui.card().classes("w-full bg-orange-50 border border-orange-200 p-3 mb-2"):
                    with ui.row().classes("items-center gap-2 mb-2"):
                        ui.icon("warning", size="20px").classes("text-orange-600")
                        ui.label(f"{len(downstream)} job(s) will become orphaned:").classes(
                            "text-sm font-bold text-orange-800"
                        )

                    with ui.column().classes("gap-1 ml-6"):
                        for detail in downstream:
                            job_path = detail.get("path", "Unknown")
                            job_type_name = detail.get("type", "Unknown")
                            job_status = detail.get("status", "Unknown")

                            # Format: "External/job007/ (denoisetrain) - Succeeded"
                            with ui.row().classes("items-center gap-2"):
                                ui.label(job_path).classes("text-xs font-mono text-gray-700")
                                if job_type_name:
                                    ui.label(f"({job_type_name})").classes("text-xs text-gray-500")
                                ui.label(f"- {job_status}").classes("text-xs text-gray-500")

                    ui.label("These jobs will have broken input references and may fail if re-run.").classes(
                        "text-xs text-orange-700 mt-2"
                    )
            else:
                ui.label("No downstream jobs will be affected.").classes(
                    "text-sm text-green-600 bg-green-50 p-2 rounded"
                )

            with ui.row().classes("w-full justify-end mt-4 gap-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat")

                async def confirm():
                    dialog.close()
                    ui.notify("Deleting job...", type="info", timeout=1500)

                    try:
                        result = await backend.delete_job(job_type.value)

                        if result.get("success"):
                            orphans = result.get("orphaned_jobs", [])

                            if orphans:
                                ui.notify(
                                    f"Job deleted. {len(orphans)} downstream job(s) orphaned.",
                                    type="warning",
                                    timeout=5000,
                                )
                            else:
                                ui.notify("Job deleted successfully.", type="positive")

                            remove_cb = callbacks.get("remove_job_from_pipeline")
                            if remove_cb:
                                remove_cb(job_type)
                        else:
                            ui.notify(f"Delete failed: {result.get('error')}", type="negative", timeout=8000)

                    except Exception as e:
                        ui.notify(f"Error: {e}", type="negative")
                        import traceback

                        traceback.print_exc()

                delete_btn = ui.button("Delete", color="red", on_click=confirm)

                # Make delete button more prominent if there are downstream impacts
                if preview and preview.get("downstream_count", 0) > 0:
                    delete_btn.props('icon="delete_forever"')

        dialog.open()

    with ui.column().classes("w-full border-b border-gray-200 bg-white pl-6 pr-6 pt-4 pb-4"):
        with ui.row().classes("w-full justify-between items-center"):
            with ui.column().classes("gap-0"):
                with ui.row().classes("items-center gap-2"):
                    ui.label(state.project_name).classes("text-lg font-bold text-gray-800")
                    ReactiveStatusBadge(job_type)
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
                ui.label(f"Created: {created} · Modified: {modified}").classes("text-xs text-gray-400")

            with ui.row().classes("items-center gap-4"):
                switcher_container = ui.row().classes("bg-gray-100 p-1 rounded-lg gap-0 border border-gray-200")
                widget_refs.switcher_container = switcher_container
                _render_tab_switcher(switcher_container, job_type, active_tab, backend, ui_mgr, callbacks)
                ui.button(icon="refresh", on_click=lambda: _force_status_refresh(callbacks)).props(
                    "flat dense round"
                ).classes("text-gray-400 hover:text-gray-800")
                if ui_mgr.is_project_created:
                    ui.button(icon="delete", on_click=handle_delete).props("flat round dense color=red").tooltip(
                        "Delete this job"
                    )

    # ===========================================
    # Content Section
    # ===========================================

    content_container = ui.column().classes("w-full flex-grow overflow-y-auto p-6")
    widget_refs.content_container = content_container

    with content_container:
        if active_tab == MonitorTab.CONFIG:
            _render_config_tab(job_type, job_model, is_frozen, ui_mgr, backend)
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

    tabs = [(MonitorTab.CONFIG, "Parameters"), (MonitorTab.LOGS, "Logs"), (MonitorTab.FILES, "Files")]

    with container:
        for tab, label in tabs:
            is_active = active_tab == tab

            btn = ui.button(label, on_click=lambda t=tab: _handle_tab_switch(job_type, t, backend, ui_mgr, callbacks))
            btn.props("flat dense no-caps")

            base_style = (
                "font-size: 12px; font-weight: 500; padding: 4px 16px; border-radius: 6px; transition: all 0.2s;"
            )

            if is_active:
                btn.style(f"{base_style} background: white; color: #111827; box-shadow: 0 1px 3px rgba(0,0,0,0.1);")
            else:
                btn.style(f"{base_style} background: transparent; color: #6b7280;")


def _handle_tab_switch(
    job_type: JobType, tab: MonitorTab, backend, ui_mgr: UIStateManager, callbacks: Dict[str, Callable]
):
    """Handle switching between tabs."""
    # Update state
    ui_mgr.set_job_monitor_tab(job_type, tab, user_initiated=True)

    widget_refs = ui_mgr.get_job_widget_refs(job_type)

    # Update tab switcher buttons
    if widget_refs.switcher_container:
        _render_tab_switcher(widget_refs.switcher_container, job_type, tab, backend, ui_mgr, callbacks)

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
                _render_config_tab(job_type, job_model, is_frozen, ui_mgr, backend)
            elif tab == MonitorTab.LOGS:
                _render_logs_tab(job_type, job_model, backend, ui_mgr)
            elif tab == MonitorTab.FILES:
                _render_files_tab(job_type, job_model, ui_mgr)


# ===========================================
# Config Tab
# ===========================================


def _render_config_tab(job_type: JobType, job_model, is_frozen: bool, ui_mgr: UIStateManager, backend):
    """Render the configuration/parameters tab."""
    save_handler = create_save_handler()

    with ui.column().classes("w-full"):
        # ==========================================================
        # 1. I/O CONFIGURATION (First)
        # ==========================================================
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
                        ui.label(str(value)).classes("text-xs font-mono text-gray-700 break-all flex-1")
            else:
                ui.label("Paths calculated upon pipeline creation.").classes("text-sm text-gray-400 italic p-4")

        # ==========================================================
        # 2. JOB PARAMETERS
        # ==========================================================
        ui.label("Job Parameters").classes("text-sm font-bold text-gray-900 mb-3")

        base_fields = {
            "execution_status",
            "relion_job_name",
            "relion_job_number",
            "paths",
            "additional_binds",
            "slurm_overrides",  # Add this to excluded fields
            "is_orphaned",
            "missing_inputs",
            "JOB_CATEGORY",
        }
        job_specific_fields = set(job_model.model_fields.keys()) - base_fields

        if not job_specific_fields:
            ui.label("This job has no configurable parameters.").classes("text-xs text-gray-500 italic mb-4")

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
                    inp = ui.number(label, value=value, format="%.4g").bind_value(job_model, param_name)
                    inp.props("outlined dense").classes("w-full")
                    if is_frozen:
                        inp.classes("bg-gray-50 text-gray-500").props("readonly")
                    else:
                        inp.on_value_change(save_handler)

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        sel = ui.select(options=[e.value for e in AlignmentMethod], value=value, label=label)
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

        # ==========================================================
        # 3. SLURM RESOURCES (NEW - Collapsible)
        # ==========================================================
        _render_slurm_config_section(job_model, is_frozen, save_handler)

        # ==========================================================
        # 4. GLOBAL PARAMETERS (Read-Only)
        # ==========================================================
        ui.label("Global Experimental Parameters (Read-Only)").classes("text-sm font-bold text-gray-900 mb-3")

        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            ui.input("Pixel Size (Å)").bind_value(job_model.microscope, "pixel_size_angstrom").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

            ui.input("Voltage (kV)").bind_value(job_model.microscope, "acceleration_voltage_kv").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

            ui.input("Cs (mm)").bind_value(job_model.microscope, "spherical_aberration_mm").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

            ui.input("Amplitude Contrast").bind_value(job_model.microscope, "amplitude_contrast").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

            ui.input("Dose per Tilt").bind_value(job_model.acquisition, "dose_per_tilt").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

            ui.input("Tilt Axis (°)").bind_value(job_model.acquisition, "tilt_axis_degrees").props(
                "dense outlined readonly"
            ).tooltip("Global parameter")

        # ==========================================================
        # 5. TEMPLATE WORKBENCH (Only for Template Matching, at bottom)
        # ==========================================================
        if job_type == JobType.TEMPLATE_MATCH_PYTOM:
            print("[DEBUG] About to render TemplateWorkbench...")
            ui.separator().classes("mb-6")
            ui.label("Template Workbench").classes("text-sm font-bold text-gray-900 mb-3")

            with ui.card().classes("w-full p-0 border border-gray-200 shadow-none bg-white"):
                TemplateWorkbench(backend, str(ui_mgr.project_path))
            print("[DEBUG] TemplateWorkbench rendered")


# ===========================================
# Logs Tab
# ===========================================


def _render_logs_tab(job_type: JobType, job_model, backend, ui_mgr: UIStateManager):
    """Render the logs tab."""
    widget_refs = ui_mgr.get_job_widget_refs(job_type)

    # Cleanup existing timer
    ui_mgr.cleanup_job_logs_timer(job_type)

    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job scheduled. Logs will appear here once running.")
        return

    with ui.grid(columns=2).classes("w-full h-full gap-4"):
        # Stdout
        with ui.column().classes("h-full overflow-hidden flex flex-col border border-gray-200 rounded-lg"):
            ui.label("Standard Output").classes(
                "text-xs font-bold text-gray-500 uppercase px-3 py-2 bg-gray-50 border-b border-gray-200 w-full"
            )
            stdout_log = (
                ui.log(max_lines=1000)
                .classes("w-full p-3 font-mono text-xs bg-white")
                .style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
            )

        # Stderr
        with ui.column().classes("h-full overflow-hidden flex flex-col border border-red-100 rounded-lg"):
            ui.label("Standard Error").classes(
                "text-xs font-bold text-red-500 uppercase px-3 py-2 bg-red-50 border-b border-red-100 w-full"
            )
            stderr_log = (
                ui.log(max_lines=1000)
                .classes("w-full p-3 font-mono text-xs bg-white text-red-700")
                .style("flex: 1; overflow-y: auto; font-family: 'IBM Plex Mono', monospace;")
            )

    widget_refs.monitor_logs = {"stdout": stdout_log, "stderr": stderr_log}

    # Initial refresh
    asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))

    # Start polling if job is running
    if job_model.execution_status == JobStatus.RUNNING:
        widget_refs.logs_timer = ui.timer(
            3.0, lambda: asyncio.create_task(_refresh_job_logs(job_type, backend, ui_mgr))
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

    # Truncate massive logs
    MAX_LOG_LINES = 500

    stdout = logs.get("stdout", "No output") or "No output yet"
    stderr = logs.get("stderr", "No errors") or "No errors yet"

    # Keep last N lines only
    stdout_lines = stdout.split("\n")
    stderr_lines = stderr.split("\n")

    if len(stdout_lines) > MAX_LOG_LINES:
        stdout = f"[... truncated {len(stdout_lines) - MAX_LOG_LINES} lines ...]\n" + "\n".join(
            stdout_lines[-MAX_LOG_LINES:]
        )
    if len(stderr_lines) > MAX_LOG_LINES:
        stderr = f"[... truncated {len(stderr_lines) - MAX_LOG_LINES} lines ...]\n" + "\n".join(
            stderr_lines[-MAX_LOG_LINES:]
        )

    monitor["stdout"].clear()
    monitor["stdout"].push(stdout)

    monitor["stderr"].clear()
    monitor["stderr"].push(stderr)


# ===========================================
# Files Tab
# ===========================================


# ui/pipeline_builder/job_tab_component.py

# ui/pipeline_builder/job_tab_component.py

def _render_files_tab(job_type: JobType, job_model, ui_mgr: UIStateManager):
    """Render the files browser tab with reactive directory listing."""
    
    if not ui_mgr.project_path:
        with ui.column().classes("w-full p-4"):
            ui.label("Error: Project path not loaded").classes("text-red-600")
        return
    
    if not job_model.relion_job_name:
        with ui.column().classes("w-full h-full items-center justify-center text-gray-400 gap-2"):
            ui.icon("schedule", size="48px")
            ui.label("Job not started. Files will appear here once the job runs.")
        return
    
    project_path = ui_mgr.project_path
    job_dir = (project_path / job_model.relion_job_name.strip("/")).resolve()

    # Define the refreshable content area
    @ui.refreshable
    def render_contents(target_path: Path):
        file_list_container.clear()
        path_label.set_text(str(target_path))
        
        if not target_path.exists():
            with file_list_container:
                ui.label("Path does not exist").classes("p-4 text-gray-400 italic")
            return

        try:
            items = sorted(target_path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except Exception as e:
            with file_list_container:
                ui.label(f"Error accessing directory: {e}").classes("p-4 text-red-500")
            return

        with file_list_container:
            if not items:
                ui.label("Directory is empty").classes("p-4 text-gray-400 italic")
                return

            # Navigation: Go Up (limited to job_dir root)
            if target_path != job_dir and job_dir in target_path.parents:
                with ui.row().classes("w-full items-center gap-2 cursor-pointer hover:bg-gray-50 p-2 border-b border-gray-100") \
                    .on("click", lambda: render_contents.refresh(target_path.parent)):
                    ui.icon("folder_open", size="16px").classes("text-gray-400")
                    ui.label("..").classes("text-xs font-bold")

            for item in items:
                is_dir = item.is_dir()
                icon = "folder" if is_dir else "insert_drive_file"
                color = "text-blue-400" if is_dir else "text-gray-400"
                
                with ui.row().classes("w-full items-center gap-2 cursor-pointer hover:bg-gray-100 p-2 border-b border-gray-100") as row:
                    ui.icon(icon, size="16px").classes(color)
                    ui.label(item.name).classes("text-xs text-gray-700 flex-1 truncate")
                    
                    if not is_dir:
                        try:
                            size = item.stat().st_size
                            ui.label(f"{size // 1024} KB").classes("text-[10px] text-gray-400")
                        except: pass
                    
                    if is_dir:
                        row.on("click", lambda i=item: render_contents.refresh(i))
                    else:
                        row.on("click", lambda i=item: view_file_dialog(i))

    # --- Layout Construction ---
    # We use flex-col and flex-grow to occupy the tab space properly
    with ui.column().classes("w-full border border-gray-200 rounded-lg bg-white overflow-hidden") \
        .style("min-height: 350px; flex: 1 1 0%;"):
        
        # Header: Fixed Height
        with ui.column().classes("w-full bg-gray-50 border-b border-gray-200 gap-0"):
            ui.label(f"Browsing Job Directory").classes("text-[10px] font-black text-gray-400 uppercase px-3 pt-2")
            
            with ui.row().classes("w-full items-center justify-between px-3 py-1 pb-2"):
                path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono truncate flex-1")
                ui.button(icon="refresh", on_click=lambda: render_contents.refresh(job_dir)) \
                    .props("flat dense round size=sm").classes("text-gray-400 hover:text-blue-500")

        # Scroll Area: This is the part that was collapsing
        file_list_container = ui.column().classes("w-full flex-grow overflow-y-auto p-0 gap-0").style("background: white;")
        
        # Initial render
        render_contents(job_dir)

def view_file_dialog(file_path: Path):
    """Simple file content viewer."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read(50000)
        
        with ui.dialog() as dialog, ui.card().classes("w-[70vw] max-w-4xl"):
            with ui.row().classes("w-full items-center justify-between mb-2"):
                ui.label(file_path.name).classes("text-sm font-bold")
                ui.button(icon="close", on_click=dialog.close).props("flat round dense")
            
            ui.code(content).classes("w-full max-h-[60vh] overflow-auto text-xs")
        dialog.open()
    except Exception as e:
        ui.notify(f"Cannot read file: {e}", type="negative")


def _force_status_refresh(callbacks: Dict[str, Callable]):
    """Force a status refresh via the new reactive mechanism."""
    ui.notify("Refreshing statuses...", timeout=1)

    # Trigger global reactive refresh
    render_status_badge.refresh()
    render_status_dot.refresh()

    if "check_and_update_statuses" in callbacks:
        asyncio.create_task(callbacks["check_and_update_statuses"]())
