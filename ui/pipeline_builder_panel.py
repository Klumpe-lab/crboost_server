# ui/pipeline_builder_panel.py (COMPLETE - PROPERLY INDENTED)
import asyncio
import json
from pathlib import Path
from nicegui import ui
from services.parameter_models import JobType
from app_state import state as app_state, sync_job_with_global, is_job_synced_with_global
from ui.utils import JobConfig, _snake_to_title
from typing import Dict, Any


def get_job_directory(job_type: JobType, job_index: int) -> str:
    """Get the job directory name based on type"""
    if job_type == JobType.IMPORT_MOVIES:
        return f"Import/job{job_index:03d}"
    else:
        job_name = job_type.value.capitalize()
        return f"{job_name}/job{job_index:03d}"


def build_pipeline_builder_panel(backend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    """Build the right panel for pipeline construction and monitoring"""

    panel_state = {
        "pipeline_container": None,
        "job_tabs_container": None,
        "run_button": None,
        "stop_button": None,
        "status_label": None,
    }

    if "active_job_tab" not in shared_state:
        shared_state["active_job_tab"] = None

    def add_job_to_pipeline(job_type: JobType):
        """Add a job to the selected pipeline"""
        if job_type in shared_state["selected_jobs"]:
            return

        shared_state["selected_jobs"].append(job_type)
        shared_state["selected_jobs"].sort(key=lambda j: JobConfig.PIPELINE_ORDER.index(j))

        if job_type.value not in app_state.jobs:
            try:
                from app_state import prepare_job_params

                prepare_job_params(job_type.value)
            except Exception as e:
                print(f"[WARN] Job {job_type.value} not implemented yet: {e}")

        if job_type not in shared_state["job_cards"]:
            shared_state["job_cards"][job_type] = {
                "job_index": len(shared_state["selected_jobs"]),
                "status": "pending",
                "is_synced": True,
            }

        shared_state["active_job_tab"] = job_type.value
        rebuild_pipeline_ui()

    def toggle_job_in_pipeline(job_type: JobType):
        """Toggle a job in/out of the pipeline"""
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            remove_job_from_pipeline(job_type)
        else:
            add_job_to_pipeline(job_type)

        update_job_tag_button(job_type)

    def remove_job_from_pipeline(job_type: JobType):
        """Remove a job from the selected pipeline"""
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            shared_state["selected_jobs"].remove(job_type)
            if job_type in shared_state["job_cards"]:
                del shared_state["job_cards"][job_type]

            if shared_state["active_job_tab"] == job_type.value:
                shared_state["active_job_tab"] = (
                    shared_state["selected_jobs"][0].value if shared_state["selected_jobs"] else None
                )

            rebuild_pipeline_ui()

    def rebuild_pipeline_ui():
        """Rebuild the entire pipeline UI"""
        update_status_label()

        if "job_tags_container" in shared_state:
            shared_state["job_tags_container"].set_visibility(not shared_state["pipeline_running"])

        container = panel_state["job_tabs_container"]
        if not container:
            return

        container.clear()

        if not shared_state["selected_jobs"]:
            with container:
                ui.label("Select jobs from the tags above to build your pipeline").classes(
                    "text-xs text-gray-500 italic text-center p-8"
                )
            return

        if shared_state["active_job_tab"] not in [j.value for j in shared_state["selected_jobs"]]:
            shared_state["active_job_tab"] = shared_state["selected_jobs"][0].value

        with container:
            if not shared_state["pipeline_running"]:
                build_configuration_tabs()
            else:
                build_monitoring_tabs()

    def update_status_label():
        """Update the status label showing job count"""
        if panel_state["status_label"]:
            count = len(shared_state["selected_jobs"])
            if count == 0:
                panel_state["status_label"].set_text("No jobs selected")
            elif shared_state["pipeline_running"]:
                panel_state["status_label"].set_text(f"{count} jobs running")
            else:
                panel_state["status_label"].set_text(f"{count} jobs · Ready to run")

    def get_status_indicator_color(job_type: JobType) -> str:
        """Get the color for the status dot"""
        status = shared_state["job_cards"].get(job_type, {}).get("status", "pending")
        colors = {"pending": "#fbbf24", "running": "#3b82f6", "success": "#10b981", "failed": "#ef4444"}
        return colors.get(status, "#6b7280")

    def build_configuration_tabs():
        """Build parameter editing tabs"""

        def switch_tab(job_type: JobType):
            shared_state["active_job_tab"] = job_type.value
            rebuild_pipeline_ui()

        with ui.row().classes("w-full border-b").style("gap: 0;"):
            for idx, job_type in enumerate(shared_state["selected_jobs"]):
                name = JobConfig.get_job_display_name(job_type)
                is_active = shared_state["active_job_tab"] == job_type.value

                with (
                    ui.button(on_click=lambda j=job_type: switch_tab(j))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 8px 20px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#6b7280'}; "
                        f"border-top: 2px solid {'#3b82f6' if is_active else 'transparent'}; "
                        f"border-left: 1px solid #e5e7eb; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-weight: {500 if is_active else 400};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        ui.element("div").style(
                            f"width: 6px; height: 6px; border-radius: 50%; "
                            f"background: {get_status_indicator_color(job_type)};"
                        )

        with ui.column().classes("w-full p-4").style("gap: 16px;"):
            for job_type in shared_state["selected_jobs"]:
                if shared_state["active_job_tab"] == job_type.value:
                    job_model = app_state.jobs.get(job_type.value)
                    if job_model:
                        build_configuration_content(job_type, job_model)
                    else:
                        ui.label(f"{JobConfig.get_job_display_name(job_type)} - Not yet implemented").classes(
                            "text-sm text-gray-500 italic p-8 text-center"
                        )

    def build_monitoring_tabs():
        """Build monitoring tabs"""

        def switch_tab(job_type: JobType):
            shared_state["active_job_tab"] = job_type.value
            rebuild_pipeline_ui()

        with ui.row().classes("w-full border-b").style("gap: 0;"):
            for idx, job_type in enumerate(shared_state["selected_jobs"]):
                name = JobConfig.get_job_display_name(job_type)
                is_active = shared_state["active_job_tab"] == job_type.value

                with (
                    ui.button(on_click=lambda j=job_type: switch_tab(j))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 8px 20px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#6b7280'}; "
                        f"border-top: 2px solid {'#3b82f6' if is_active else 'transparent'}; "
                        f"border-left: 1px solid #e5e7eb; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-weight: {500 if is_active else 400};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        ui.element("div").style(
                            f"width: 6px; height: 6px; border-radius: 50%; "
                            f"background: {get_status_indicator_color(job_type)};"
                        )

        with ui.column().classes("w-full p-4").style("gap: 16px;"):
            for idx, job_type in enumerate(shared_state["selected_jobs"]):
                if shared_state["active_job_tab"] == job_type.value:
                    build_monitoring_content(job_type, idx + 1)

    def build_configuration_content(job_type: JobType, job_model):
        """Build configuration tab content"""
        job_card_state = shared_state["job_cards"][job_type]

        ui.label("Inputs & Outputs").classes("text-xs font-semibold text-black mb-2")

        with ui.column().classes("w-full mb-4 p-3").style("background: #fafafa; border-radius: 3px; gap: 8px;"):
            # Only show paths if project is created
            if shared_state.get("project_created"):
                paths_data = shared_state.get("params_snapshot", {}).get(job_type, {}).get("paths", {})

                if paths_data:
                    for key, value in paths_data.items():
                        with ui.row().classes("w-full items-start").style("gap: 8px;"):
                            ui.label(f"{_snake_to_title(key)}:").classes("text-xs font-medium text-gray-600").style(
                                "min-width: 140px;"
                            )
                            ui.label(str(value)).classes("text-xs text-gray-800 font-mono flex-1")
                else:
                    ui.label("No paths information available").classes("text-xs text-gray-500 italic")
            else:
                ui.label("Paths will be generated when project is created").classes("text-xs text-gray-500 italic")

        ui.label("Parameters").classes("text-xs font-semibold text-black mb-2")

        param_updaters = {}

        with ui.grid(columns=3).classes("w-full").style("gap: 10px;"):
            for param_name, value in job_model.model_dump().items():
                label = _snake_to_title(param_name)
                element = None

                if isinstance(value, bool):
                    element = ui.checkbox(label, value=value).props("dense")
                    element.bind_value(job_model, param_name)
                    element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))

                elif isinstance(value, (int, float)):
                    element = (
                        ui.input(
                            label=label,
                            value=str(value),
                            validation={
                                "Enter valid number": lambda v: v == ""
                                or v.replace(".", "", 1).replace("-", "", 1).isdigit()
                            },
                        )
                        .props("dense outlined")
                        .classes("w-full")
                    )
                    current_display_value = str(value)

                    def create_binding(field_name, is_float=False, ui_element=element):
                        def model_to_ui():
                            nonlocal current_display_value
                            current_val = getattr(job_model, field_name)
                            new_display = str(current_val) if current_val is not None else ""
                            if new_display != current_display_value:
                                ui_element.value = new_display
                                current_display_value = new_display

                        def ui_to_model():
                            nonlocal current_display_value
                            try:
                                val = ui_element.value.strip()
                                parsed = (float(val) if is_float else int(float(val))) if val else 0

                                if "do_at_most" in field_name and not val:
                                    parsed = -1

                                setattr(job_model, field_name, parsed)
                                current_display_value = str(parsed)
                                update_job_card_sync_indicator(job_type)

                            except (ValueError, Exception):
                                current = getattr(job_model, field_name, 0)
                                ui_element.value = str(current)
                                current_display_value = str(current)

                        ui_element.on("blur", ui_to_model)
                        return model_to_ui

                    updater_fn = create_binding(param_name, isinstance(value, float))
                    param_updaters[param_name] = updater_fn

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        options = ["AreTomo", "IMOD", "Relion"]
                        element = (
                            ui.select(label=label, options=options, value=value)
                            .props("dense outlined")
                            .classes("w-full")
                        )
                    else:
                        element = ui.input(label=label, value=value).props("dense outlined").classes("w-full")
                    element.bind_value(job_model, param_name)
                    element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))

        job_card_state["param_updaters"] = param_updaters

    def build_monitoring_content(job_type: JobType, job_index: int):
        """Build monitoring tab content - parameters + logs + file browser"""
        job_state = shared_state["job_cards"][job_type]

        if "active_monitor_tab" not in job_state:
            job_state["active_monitor_tab"] = "logs"

        # Store button references for updating
        tab_buttons = {}

        with ui.row().classes("w-full").style("gap: 8px; margin-bottom: 12px;"):
            for tab_name, tab_label in [("logs", "Logs"), ("params", "Parameters"), ("files", "Files")]:
                is_active = job_state.get("active_monitor_tab", "logs") == tab_name
                btn = (
                    ui.button(tab_label, on_click=lambda t=tab_name, j=job_type: switch_monitor_tab(j, t))
                    .props("dense flat no-caps")
                    .style(
                        f"flex: 1; padding: 6px 12px; border-radius: 3px; font-weight: 500; font-size: 11px; "
                        f"background: {'#3b82f6' if is_active else '#f3f4f6'}; "
                        f"color: {'white' if is_active else '#1f2937'}; "
                        f"border: 1px solid {'#3b82f6' if is_active else '#e5e7eb'};"
                    )
                )
                tab_buttons[tab_name] = btn

        # Store buttons for later updates
        job_state["tab_buttons"] = tab_buttons

        content_container = ui.column().classes("w-full")
        job_state["monitor_content"] = content_container

        render_monitor_tab(job_type, job_index)

    def switch_monitor_tab(job_type: JobType, tab_name: str):
        """Switch to a different monitor tab"""
        job_state = shared_state["job_cards"][job_type]
        job_state["active_monitor_tab"] = tab_name

        # Update button styles
        if "tab_buttons" in job_state:
            for btn_name, btn in job_state["tab_buttons"].items():
                is_active = btn_name == tab_name
                btn.style(
                    f"flex: 1; padding: 6px 12px; border-radius: 3px; font-weight: 500; font-size: 11px; "
                    f"background: {'#f3f4f6' if not is_active else '#3b82f6'}; "
                    f"color: {'#1f2937' if not is_active else 'white'}; "
                    f"border: 1px solid {'#e5e7eb' if not is_active else '#3b82f6'};"
                )

        job_index = job_state["job_index"]
        render_monitor_tab(job_type, job_index)

    def update_job_tag_button(job_type: JobType):
        """Update the visual state of a job tag button"""
        btn = shared_state.get("job_buttons", {}).get(job_type)
        if not btn:
            return

        is_selected = job_type in shared_state["selected_jobs"]

        btn.style(
            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
        )

    def render_monitor_tab(job_type: JobType, job_index: int):
        """Render the active monitor tab content"""
        job_state = shared_state["job_cards"][job_type]
        active_tab = job_state.get("active_monitor_tab", "logs")
        content = job_state["monitor_content"]

        content.clear()

        with content:
            if active_tab == "logs":
                render_logs_tab(job_type, job_index)
            elif active_tab == "params":
                render_params_tab(job_type)
            elif active_tab == "files":
                render_files_tab(job_type, job_index)

    def render_logs_tab(job_type: JobType, job_index: int):
        """Render logs tab content"""
        job_state = shared_state["job_cards"][job_type]

        with ui.row().classes("w-full justify-end mb-2"):
            ui.button(
                "Refresh", icon="refresh", on_click=lambda: asyncio.create_task(refresh_job_logs(job_type))
            ).props("dense flat no-caps").style(
                "background: #f3f4f6; color: #1f2937; padding: 4px 12px; border-radius: 3px; font-size: 11px;"
            )

        with ui.grid(columns=2).classes("w-full").style("gap: 10px; height: calc(100vh - 400px); min-height: 400px;"):
            with ui.column().classes("w-full h-full"):
                ui.label("stdout").classes("text-xs font-medium mb-1")
                stdout_log = (
                    ui.log(max_lines=500)
                    .classes("w-full h-full border rounded bg-gray-50 p-2 text-xs font-mono")
                    .style("font-family: 'IBM Plex Mono', monospace;")
                )
            with ui.column().classes("w-full h-full"):
                ui.label("stderr").classes("text-xs font-medium mb-1")
                stderr_log = (
                    ui.log(max_lines=500)
                    .classes("w-full h-full border rounded bg-red-50 p-2 text-xs font-mono")
                    .style("font-family: 'IBM Plex Mono', monospace;")
                )

        job_state["monitor"] = {"stdout": stdout_log, "stderr": stderr_log}
        asyncio.create_task(refresh_job_logs(job_type, notify=False))

    def render_params_tab(job_type: JobType):
        """Render parameters tab content - direct display, no expansion"""
        ui.label("Job Parameters (Snapshot)").classes("text-xs font-semibold text-black mb-2")

        params_snapshot = shared_state["params_snapshot"].get(job_type, {})

        if "job_model" in params_snapshot:
            ui.label("Job Model:").classes("text-xs font-medium text-gray-600 mb-1")
            with ui.column().classes("w-full mb-3 p-3").style("background: #fafafa; border-radius: 3px;"):
                params_json = json.dumps(params_snapshot["job_model"], indent=2)
                ui.code(params_json, language="json").classes("w-full text-xs")

        if "paths" in params_snapshot:
            ui.label("Paths:").classes("text-xs font-medium text-gray-600 mb-1")
            with ui.column().classes("w-full mb-3 p-3").style("background: #fafafa; border-radius: 3px; gap: 6px;"):
                for key, value in params_snapshot["paths"].items():
                    with ui.row().classes("w-full items-start").style("gap: 8px;"):
                        ui.label(f"{_snake_to_title(key)}:").classes("text-xs font-medium text-gray-600").style(
                            "min-width: 140px;"
                        )
                        ui.label(str(value)).classes("text-xs text-gray-800 font-mono flex-1")

    def render_files_tab(job_type: JobType, job_index: int):
        """Render files tab content - full file browser"""
        project_path = shared_state.get("current_project_path")
        if not project_path:
            ui.label("Project path not set.").classes("text-xs text-red-500")
            return

        job_dir_rel = get_job_directory(job_type, job_index)
        job_dir = Path(project_path) / job_dir_rel

        ui.label("Job Directory Browser").classes("text-xs font-semibold text-black mb-2")
        current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono mb-2")
        file_list_container = (
            ui.column()
            .classes("w-full border rounded p-2 bg-gray-50")
            .style("height: calc(100vh - 400px); min-height: 400px; overflow-y: auto;")
        )

        def view_file(file_path: Path):
            """Show file content in a dialog"""
            try:
                text_extensions = [
                    ".script",
                    ".txt",
                    ".log",
                    ".star",
                    ".json",
                    ".yaml",
                    ".sh",
                    ".py",
                    ".out",
                    ".err",
                    ".md",
                    ".tlt",
                    ".aln",
                    "",
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
            """Browse directory"""
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
                    if path != job_dir and path.parent.exists():
                        with (
                            ui.row()
                            .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded w-full")
                            .on("click", lambda p=path.parent: browse_directory(p))
                        ):
                            ui.icon("folder_open").classes("text-sm")
                            ui.label("..").classes("text-xs")

                    items = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name))
                    if not items:
                        ui.label("Directory is empty.").classes("text-xs text-gray-500 italic")

                    for item in items:
                        if item.is_dir():
                            with (
                                ui.row()
                                .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded w-full")
                                .on("click", lambda i=item: browse_directory(i))
                            ):
                                ui.icon("folder").classes("text-sm text-blue-600")
                                ui.label(item.name).classes("text-xs")
                        else:
                            with ui.row().classes("items-center gap-2 w-full"):
                                with (
                                    ui.row()
                                    .classes(
                                        "items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded flex-grow"
                                    )
                                    .on("click", lambda i=item: view_file(i))
                                ):
                                    ui.icon("insert_drive_file").classes("text-sm text-gray-600")
                                    ui.label(item.name).classes("text-xs")
                                size_kb = item.stat().st_size // 1024
                                ui.label(f"{size_kb} KB").classes("text-xs text-gray-500 ml-auto")
            except Exception as e:
                with file_list_container:
                    ui.label(f"Error listing directory: {e}").classes("text-xs text-red-600")

        browse_directory(job_dir)

    async def refresh_job_logs(job_type: JobType, notify: bool = True):
        """Refresh logs for a job"""
        card_data = shared_state["job_cards"].get(job_type)
        if not card_data or "monitor" not in card_data:
            return

        monitor = card_data["monitor"]
        job_index = card_data["job_index"]

        logs = await backend.get_pipeline_job_logs(shared_state["current_project_path"], job_type.value, str(job_index))
        monitor["stdout"].clear()
        monitor["stdout"].push(logs.get("stdout", "No output"))
        monitor["stderr"].clear()
        monitor["stderr"].push(logs.get("stderr", "No errors"))

        if notify:
            ui.notify("Logs refreshed", type="positive")

    def update_job_card_sync_indicator(job_type: JobType):
        """Update sync indicator"""
        if job_type not in shared_state["job_cards"]:
            return
        is_synced = is_job_synced_with_global(job_type)
        shared_state["job_cards"][job_type]["is_synced"] = is_synced

    async def handle_run_pipeline():
        """Handle run button click"""
        if not shared_state["project_created"]:
            ui.notify("Create a project first", type="warning")
            return

        panel_state["run_button"].props("loading")

        result = await backend.start_pipeline(
            project_path=shared_state["current_project_path"],
            scheme_name=shared_state["current_scheme_name"],
            selected_jobs=[j.value for j in shared_state["selected_jobs"]],
            required_paths=[],
        )

        panel_state["run_button"].props(remove="loading")

        if result.get("success"):
            shared_state["pipeline_running"] = True
            ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
            panel_state["run_button"].props("disable")
            panel_state["stop_button"].props(remove="disable")
            rebuild_pipeline_ui()
        else:
            ui.notify(f"Failed: {result.get('error')}", type="negative")

    # === UI CONSTRUCTION ===
    with (
        ui.column()
        .classes("w-full h-full overflow-y-auto")
        .style("padding: 20px; gap: 0px; font-family: 'IBM Plex Sans', sans-serif;")
    ):
        with ui.row().classes("w-full items-center justify-between mb-4").style("gap: 12px;"):
            job_tags_container = ui.row().classes("flex-1 flex-wrap").style("gap: 8px;")
            shared_state["job_tags_container"] = job_tags_container

            with job_tags_container:
                for job_type in JobConfig.get_ordered_jobs():
                    name = JobConfig.get_job_display_name(job_type)
                    is_selected = job_type in shared_state["selected_jobs"]

                    btn = (
                        ui.button(name, on_click=lambda j=job_type: toggle_job_in_pipeline(j))
                        .props("no-caps dense flat")
                        .style(
                            f"padding: 6px 16px; border-radius: 3px; font-weight: 500; "
                            f"background: {'#dbeafe' if is_selected else '#f3f4f6'}; "
                            f"color: {'#1e40af' if is_selected else '#6b7280'}; "
                            f"border: 1px solid {'#93c5fd' if is_selected else '#e5e7eb'};"
                        )
                    )
                    shared_state.setdefault("job_buttons", {})[job_type] = btn

            with ui.row().classes("items-center").style("gap: 10px;"):
                panel_state["status_label"] = ui.label("No jobs selected").classes("text-xs text-gray-600")

                panel_state["run_button"] = (
                    ui.button("Run Pipeline", icon="play_arrow", on_click=handle_run_pipeline)
                    .props("dense flat no-caps")
                    .style(
                        "background: #f3f4f6; color: #1f2937; padding: 6px 20px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                    )
                )

                panel_state["stop_button"] = (
                    ui.button("Stop", icon="stop")
                    .props("dense flat no-caps disable")
                    .style(
                        "background: #f3f4f6; color: #1f2937; padding: 6px 20px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                    )
                )

        panel_state["job_tabs_container"] = ui.column().classes("w-full")

        with panel_state["job_tabs_container"]:
            ui.label("Select jobs from the tags above to build your pipeline").classes(
                "text-xs text-gray-500 italic text-center p-8"
            )

    callbacks["rebuild_pipeline_ui"] = rebuild_pipeline_ui
    callbacks["update_job_card_sync_indicator"] = update_job_card_sync_indicator

    return panel_state
