# ui/pipeline_builder_panel.py
import asyncio
from pathlib import Path
from backend import CryoBoostBackend
from nicegui import ui
from services.parameter_models import JobType, JobStatus
from app_state import state as app_state, is_job_synced_with_global
from ui.utils import JobConfig, _snake_to_title
from typing import Dict, Any


def get_job_status(job_type: JobType) -> JobStatus:
    """Query status from job model"""
    job_model = app_state.jobs.get(job_type.value)
    return job_model.execution_status if job_model else JobStatus.UNKNOWN


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


def get_job_dir(job_type: JobType, project_path: str) -> Path:
    """Get job directory from model"""
    job_model = app_state.jobs.get(job_type.value)
    if not job_model or not job_model.relion_job_name:
        return None
    return Path(project_path) / job_model.relion_job_name.rstrip("/")


def is_job_frozen(job_type: JobType) -> bool:
    """Check if job params should be frozen"""
    return get_job_status(job_type) not in [JobStatus.SCHEDULED]


def build_pipeline_builder_panel(backend: CryoBoostBackend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    panel_state = {
        "job_tabs_container": None,
        "run_button": None,
        "stop_button": None,
        "status_label": None,
        "status_timer": None,
    }

    if "active_job_tab" not in shared_state:
        shared_state["active_job_tab"] = None

    if "job_cards" not in shared_state:
        shared_state["job_cards"] = {}

    def add_job_to_pipeline(job_type: JobType):
        if job_type in shared_state["selected_jobs"]:
            return
        
        shared_state["selected_jobs"].append(job_type)
        shared_state["selected_jobs"].sort(key=lambda j: JobConfig.PIPELINE_ORDER.index(j))

        if job_type.value not in app_state.jobs:
            from app_state import prepare_job_params
            prepare_job_params(job_type.value)
        
        # Reset status to SCHEDULED for new pipelines (not loaded/continuation mode)
        if not shared_state.get("continuation_mode", False):
            job_model = app_state.jobs.get(job_type.value)
            if job_model:
                job_model.execution_status = JobStatus.SCHEDULED
                job_model.relion_job_name = None
                job_model.relion_job_number = None

        if job_type not in shared_state["job_cards"]:
            shared_state["job_cards"][job_type] = {"active_monitor_tab": "config"}

        shared_state["active_job_tab"] = job_type.value
        rebuild_pipeline_ui()

    def toggle_job_in_pipeline(job_type: JobType):
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            remove_job_from_pipeline(job_type)
        else:
            add_job_to_pipeline(job_type)

        update_job_tag_button(job_type)

    def remove_job_from_pipeline(job_type: JobType):
        if shared_state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in shared_state["selected_jobs"]:
            shared_state["selected_jobs"].remove(job_type)

            if job_type in shared_state["job_cards"]:
                job_state = shared_state["job_cards"][job_type]
                if job_state.get("logs_timer"):
                    job_state["logs_timer"].cancel()
                del shared_state["job_cards"][job_type]

            if shared_state["active_job_tab"] == job_type.value:
                shared_state["active_job_tab"] = (
                    shared_state["selected_jobs"][0].value if shared_state["selected_jobs"] else None
                )

            rebuild_pipeline_ui()

    def stop_all_timers():
        if panel_state.get("status_timer"):
            panel_state["status_timer"].cancel()
            panel_state["status_timer"] = None

        for job_type in shared_state.get("job_cards", {}):
            job_state = shared_state["job_cards"][job_type]
            if job_state.get("logs_timer"):
                job_state["logs_timer"].cancel()
                job_state["logs_timer"] = None

    def rebuild_pipeline_ui():
        update_status_label()

        if "job_tags_container" in shared_state:
            should_hide = shared_state["pipeline_running"] or shared_state.get("continuation_mode", False)
            shared_state["job_tags_container"].set_visibility(not should_hide)

        container = panel_state["job_tabs_container"]
        if not container:
            return

        stop_all_timers()
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
            build_unified_job_tabs()

    async def check_and_update_statuses():
        if not shared_state["pipeline_running"]:
            if panel_state.get("status_timer"):
                panel_state["status_timer"].deactivate()
            return

        project_path = shared_state.get("current_project_path")
        if not project_path:
            return

        changes = await backend.pipeline_runner.status_sync.sync_all_jobs(project_path)

        if any(changes.values()):
            refresh_status_indicators()

            active_job = shared_state.get("active_job_tab")
            if active_job and changes.get(active_job):
                refresh_active_tab_content()

    def refresh_status_indicators():
        for job_type in shared_state["selected_jobs"]:
            color = get_status_color(job_type)
            status_text = get_job_status(job_type).value

            card = shared_state.get("job_cards", {}).get(job_type, {})

            if dot := card.get("ui_status_dot"):
                if not dot.is_deleted:
                    dot.style(f"background: {color};")

            if job_type.value == shared_state.get("active_job_tab"):
                if label := card.get("ui_status_label"):
                    if not label.is_deleted:
                        label.set_text(f"Status: {status_text}")

    def refresh_active_tab_content():
        active_job_str = shared_state.get("active_job_tab")
        if not active_job_str:
            return

        try:
            active_job_type = JobType(active_job_str)
            card_data = shared_state["job_cards"].get(active_job_type, {})
            active_tab = card_data.get("active_monitor_tab", "config")
            switch_monitor_tab(active_job_type, active_tab)
        except:
            pass

    def update_status_label():
        if panel_state["status_label"]:
            count = len(shared_state["selected_jobs"])
            if count == 0:
                panel_state["status_label"].set_text("No jobs selected")
            elif shared_state["pipeline_running"]:
                panel_state["status_label"].set_text(f"Pipeline running...")
            else:
                panel_state["status_label"].set_text(f"{count} jobs · Ready to run")

    def build_unified_job_tabs():
        def switch_tab(job_type: JobType):
            shared_state["active_job_tab"] = job_type.value
            rebuild_pipeline_ui()

        with ui.row().classes("w-full border-b").style("gap: 0;"):
            for job_type in shared_state["selected_jobs"]:
                name = JobConfig.get_job_display_name(job_type)
                is_active = shared_state["active_job_tab"] == job_type.value

                with (
                    ui.button(on_click=lambda j=job_type: switch_tab(j))
                    .props("flat no-caps dense")
                    .style(
                        f"padding: 8px 20px; border-radius: 0; "
                        f"background: {'white' if is_active else '#fafafa'}; "
                        f"color: {'#1f2937' if is_active else '#6b7280'}; "
                        f"border-top: 2px solid {get_status_color(job_type) if is_active else 'transparent'}; "
                        f"border-left: 1px solid #e5e7eb; "
                        f"border-right: 1px solid #e5e7eb; "
                        f"font-weight: {500 if is_active else 400};"
                    )
                ):
                    with ui.row().classes("items-center gap-2"):
                        ui.label(name).classes("text-sm")
                        status_dot = ui.element("div").style(
                            f"width: 6px; height: 6px; border-radius: 50%; background: {get_status_color(job_type)};"
                        )
                        if job_type in shared_state["job_cards"]:
                            shared_state["job_cards"][job_type]["ui_status_dot"] = status_dot

        with ui.column().classes("w-full"):
            for job_type in shared_state["selected_jobs"]:
                if shared_state["active_job_tab"] == job_type.value:
                    build_unified_tab_content(job_type)

    def build_unified_tab_content(job_type: JobType):
        job_model = app_state.jobs.get(job_type.value)
        if not job_model:
            ui.label(f"Error: Job model for {job_type.value} not found.").classes("text-xs text-red-600")
            return

        job_state = shared_state["job_cards"].get(job_type, {})
        is_frozen = is_job_frozen(job_type)
        active_sub_tab = job_state.get("active_monitor_tab", "config")

        if is_frozen and active_sub_tab == "config":
            active_sub_tab = "logs"
            job_state["active_monitor_tab"] = "logs"

        with ui.row().classes("w-full p-4 pb-0 items-center").style("gap: 8px;"):
            for tab_name, tab_label in [("config", "Parameters"), ("logs", "Logs"), ("files", "Files")]:
                is_active = active_sub_tab == tab_name
                ui.button(tab_label, on_click=lambda t=tab_name, j=job_type: switch_monitor_tab(j, t)).props(
                    "dense flat no-caps"
                ).style(
                    f"padding: 6px 12px; border-radius: 3px; font-weight: 500; font-size: 11px; "
                    f"background: {'#3b82f6' if is_active else '#f3f4f6'}; "
                    f"color: {'white' if is_active else '#1f2937'}; "
                    f"border: 1px solid {'#3b82f6' if is_active else '#e5e7eb'};"
                )

            status_label = ui.label(f"Status: {job_model.execution_status.value}").classes(
                "text-xs font-medium text-gray-600 ml-auto"
            )
            job_state["ui_status_label"] = status_label

            ui.button("Refresh", icon="refresh", on_click=force_status_refresh).props("dense flat no-caps").style(
                "background: #f3f4f6; color: #1f2937; padding: 4px 12px; border-radius: 3px; font-size: 11px;"
            )

        content_container = ui.column().classes("w-full p-4")
        job_state["monitor_content_container"] = content_container

        with content_container:
            if active_sub_tab == "config":
                render_config_tab(job_type, job_model, is_frozen)
            elif active_sub_tab == "logs":
                render_logs_tab(job_type, job_model)
            elif active_sub_tab == "files":
                render_files_tab(job_type, job_model)

    def render_config_tab(job_type: JobType, job_model, is_frozen: bool):
        job_state = shared_state["job_cards"][job_type]

        if is_frozen:
            status_color = get_status_color(job_type)
            icon_map = {JobStatus.SUCCEEDED: "check_circle", JobStatus.FAILED: "error", JobStatus.RUNNING: "sync"}
            icon = icon_map.get(job_model.execution_status, "info")

            with (
                ui.row()
                .classes("w-full items-center mb-3 p-2")
                .style(f"background: #fafafa; border-left: 3px solid {status_color}; border-radius: 3px;")
            ):
                ui.icon(icon).style(f"color: {status_color};")
                ui.label(f"Job status is {job_model.execution_status.value}. Parameters are frozen.").classes(
                    "text-xs text-gray-700"
                )

        ui.label("Inputs & Outputs").classes("text-xs font-semibold text-black mb-2")
        with ui.column().classes("w-full mb-4 p-3").style("background: #fafafa; border-radius: 3px; gap: 8px;"):
            if shared_state.get("project_created"):
                paths_data = shared_state.get("params_snapshot", {}).get(job_type.value, {}).get("paths", {})
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

        ui.label("Parameters").classes("text-xs font-semibold text-black mb-2")

        job_params = job_model.model_dump()
        with ui.grid(columns=3).classes("w-full").style("gap: 10px;"):
            for param_name, value in job_params.items():
                label = _snake_to_title(param_name)

                if isinstance(value, bool):
                    element = ui.checkbox(label, value=value).props("dense")
                    if not is_frozen:
                        element.bind_value(job_model, param_name)
                        element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))
                    else:
                        element.disable()

                elif isinstance(value, (int, float)):
                    element = ui.input(label=label, value=str(value)).props("outlined dense").classes("w-full")
                    element.enabled = not is_frozen

                    if is_frozen:
                        element.classes("bg-gray-50")
                    else:
                        # Fix closure capture bug
                        def create_blur_handler(field_name, is_float, input_element):
                            def on_blur():
                                try:
                                    val = input_element.value.strip()
                                    parsed = (float(val) if is_float else int(float(val))) if val else 0
                                    if "do_at_most" in field_name and not val:
                                        parsed = -1
                                    setattr(job_model, field_name, parsed)
                                    update_job_card_sync_indicator(job_type)
                                except:
                                    input_element.value = str(getattr(job_model, field_name, 0))
                            return on_blur
                        
                        element.on("blur", create_blur_handler(param_name, isinstance(value, float), element))

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        element = (
                            ui.select(label=label, options=["AreTomo", "IMOD", "Relion"], value=value)
                            .props("outlined dense")
                            .classes("w-full")
                        )
                    else:
                        element = ui.input(label=label, value=value).props("outlined dense").classes("w-full")

                    element.enabled = not is_frozen

                    if is_frozen:
                        element.classes("bg-gray-50")
                    else:
                        element.bind_value(job_model, param_name)
                        element.on_value_change(lambda j=job_type: update_job_card_sync_indicator(j))

    async def force_status_refresh():
        ui.notify("Refreshing statuses...", timeout=1)
        await check_and_update_statuses()

    def switch_monitor_tab(job_type: JobType, tab_name: str):
        job_state = shared_state["job_cards"][job_type]
        job_state["active_monitor_tab"] = tab_name

        if tab_name != "logs" and job_state.get("logs_timer"):
            job_state["logs_timer"].cancel()
            job_state["logs_timer"] = None

        container = job_state.get("monitor_content_container")
        if container:
            job_model = app_state.jobs.get(job_type.value)
            is_frozen = is_job_frozen(job_type)

            container.clear()
            with container:
                if tab_name == "config":
                    render_config_tab(job_type, job_model, is_frozen)
                elif tab_name == "logs":
                    render_logs_tab(job_type, job_model)
                elif tab_name == "files":
                    render_files_tab(job_type, job_model)
        else:
            rebuild_pipeline_ui()

    def update_job_tag_button(job_type: JobType):
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

    def render_logs_tab(job_type: JobType, job_model):
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

        job_state["monitor_logs"] = {"stdout": stdout_log, "stderr": stderr_log}

        asyncio.create_task(refresh_job_logs(job_type))

        if job_model.execution_status == JobStatus.RUNNING:
            job_state["logs_timer"] = ui.timer(5.0, lambda j=job_type: asyncio.create_task(refresh_job_logs(j)))

    def render_files_tab(job_type: JobType, job_model):
        project_path = shared_state.get("current_project_path")

        if not project_path or not job_model.relion_job_name:
            ui.label("Job has not run yet. Files will appear once it starts.").classes("text-xs text-gray-500 italic")
            return

        job_dir = Path(project_path) / job_model.relion_job_name.rstrip("/")

        ui.label("Job Directory Browser").classes("text-xs font-semibold text-black mb-2")
        current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono mb-2")
        file_list_container = (
            ui.column()
            .classes("w-full border rounded p-2 bg-gray-50")
            .style("height: calc(100vh - 450px); min-height: 400px; overflow-y: auto;")
        )

        def view_file(file_path: Path):
            try:
                text_extensions = [
                    ".script",
                    ".txt",
                    ".xml",
                    ".settings",
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

    async def refresh_job_logs(job_type: JobType):
        card_data = shared_state["job_cards"].get(job_type)
        if not card_data:
            return

        job_model = app_state.jobs.get(job_type.value)
        if not job_model or not job_model.relion_job_name:
            # Job hasn't started yet, stop timer if it exists
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

    def update_job_card_sync_indicator(job_type: JobType):
        if job_type not in shared_state["job_cards"]:
            return
        is_synced = is_job_synced_with_global(job_type)
        shared_state["job_cards"][job_type]["is_synced"] = is_synced

    async def handle_run_pipeline():
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

            for job_type in shared_state["selected_jobs"]:
                job_model = app_state.jobs.get(job_type.value)
                if job_model and job_model.execution_status != JobStatus.SUCCEEDED:
                    job_model.execution_status = JobStatus.SCHEDULED

            if panel_state.get("status_timer"):
                panel_state["status_timer"].activate()
            else:
                panel_state["status_timer"] = ui.timer(
                    5.0, lambda: asyncio.create_task(check_and_update_statuses()), active=True
                )

            await check_and_update_statuses()

            ui.notify(f"Pipeline started (PID: {result.get('pid')})", type="positive")
            panel_state["run_button"].props("disable")
            panel_state["stop_button"].props(remove="disable")

            rebuild_pipeline_ui()
        else:
            ui.notify(f"Failed: {result.get('error')}", type="negative")

    # Main panel layout
    with (
        ui.column()
        .classes("w-full h-full overflow-y-auto")
        .style("padding: 10px; gap: 0px; font-family: 'IBM Plex Sans', sans-serif;")
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
    callbacks["stop_all_timers"] = stop_all_timers
    callbacks["enable_run_button"] = (
        lambda: panel_state["run_button"].props(remove="disable") if panel_state["run_button"] else None
    )

    return panel_state
