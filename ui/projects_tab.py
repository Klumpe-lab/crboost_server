# ui/projects_tab.py (SIMPLIFIED & FIXED)
import asyncio
import json
import math
from pathlib import Path
from nicegui import ui
from backend import CryoBoostBackend
from services.parameter_models import JobType
from ui.utils import create_path_input_with_picker
from typing import Dict, Any, List

from app_state import state as app_state, update_from_mdoc
from typing import List, Dict, Any

class JobConfig:
    """Central configuration for job pipeline ordering and metadata"""
    
    # Define job order and dependencies
    PIPELINE_ORDER = [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
        # Future jobs (commented out for now):
        # JobType.TS_CTF,
        # JobType.DENOISE_TRAIN,
        # JobType.DENOISE_PREDICT,
        # JobType.TS_RECONSTRUCT,
        # JobType.TEMPLATE_MATCH,
        # JobType.EXTRACT_CANDIDATES,
        # JobType.SUBTOMO_RECONSTRUCT,
    ]
    
    # Job metadata for UI display
    JOB_METADATA = {
        JobType.IMPORT_MOVIES: {
            'icon': '',
            'short_name': 'Import',
            'description': 'Import raw movies and mdocs',
        },
        JobType.FS_MOTION_CTF: {
            'icon': '',
            'short_name': 'Motion & CTF',
            'description': 'Motion correction and CTF estimation',
        },
        JobType.TS_ALIGNMENT: {
            'icon': '',
            'short_name': 'Alignment',
            'description': 'Tilt series alignment',
        },
    }
    
    @classmethod
    def get_ordered_jobs(cls) -> List[JobType]:
        """Get jobs in pipeline execution order"""
        return cls.PIPELINE_ORDER.copy()
    
    @classmethod
    def get_job_display_name(cls, job_type: JobType) -> str:
        """Get display name for a job"""
        return cls.JOB_METADATA.get(job_type, {}).get('short_name', job_type.value)
    
    @classmethod
    def get_job_icon(cls, job_type: JobType) -> str:
        """Get icon for a job"""
        return cls.JOB_METADATA.get(job_type, {}).get('icon', '📦')
    
    @classmethod
    def get_job_description(cls, job_type: JobType) -> str:
        """Get description for a job"""
        return cls.JOB_METADATA.get(job_type, {}).get('description', '')


def _snake_to_title(snake_str: str) -> str:
    return " ".join(word.capitalize() for word in snake_str.split("_"))


def build_projects_tab(backend: CryoBoostBackend):
    """Projects tab with unified job cards"""

    # Local UI state
    state = {
        "selected_jobs": [],  # List of JobType enums
        "current_project_path": None,
        "current_scheme_name": None,
        "auto_detected_values": {},
        "job_cards": {},  # JobType -> {card, param_inputs, monitor_components}
        "params_snapshot": {},  # JobType -> dict at run time
        "project_created": False,
        "pipeline_running": False,
    }

    # =============================================================================
    # HELPER FUNCTIONS
    # =============================================================================

    def get_job_param_snapshot(job_type: JobType) -> Dict[str, Any]:
        """Capture current parameters for a job"""
        job_model = app_state.jobs.get(job_type.value)
        if job_model:
            return job_model.model_dump()
        return {}

    def is_job_synced_with_global(job_type: JobType) -> bool:
        """Check if job params match global params - with proper comparison"""
        job_model = app_state.jobs.get(job_type.value)
        if not job_model:
            return True

        # Define sync mappings for each job type with tolerance for floating point
        sync_mappings = {
            JobType.IMPORT_MOVIES: {
                "pixel_size": app_state.microscope.pixel_size_angstrom,
                "voltage": app_state.microscope.acceleration_voltage_kv,
                "spherical_aberration": app_state.microscope.spherical_aberration_mm,
                "amplitude_contrast": app_state.microscope.amplitude_contrast,
                "dose_per_tilt_image": app_state.acquisition.dose_per_tilt,
                "tilt_axis_angle": app_state.acquisition.tilt_axis_degrees,
                "invert_defocus_hand": app_state.acquisition.invert_defocus_hand,
            },
            JobType.FS_MOTION_CTF: {
                "pixel_size": app_state.microscope.pixel_size_angstrom,
                "voltage": app_state.microscope.acceleration_voltage_kv,
                "cs": app_state.microscope.spherical_aberration_mm,
                "amplitude": app_state.microscope.amplitude_contrast,
                "eer_ngroups": app_state.acquisition.eer_fractions_per_frame or 32,
            },
            JobType.TS_ALIGNMENT: {"thickness_nm": app_state.acquisition.sample_thickness_nm},
        }

        mapping = sync_mappings.get(job_type, {})

        for field, global_value in mapping.items():
            job_value = getattr(job_model, field, None)

            # Handle floating point comparison with tolerance
            if isinstance(global_value, float) and isinstance(job_value, float):
                if abs(job_value - global_value) > 1e-6:
                    print(
                        f"[SYNC CHECK] {job_type.value}.{field}: job={job_value}, global={global_value} → OUT OF SYNC"
                    )
                    return False
            elif job_value != global_value:
                print(f"[SYNC CHECK] {job_type.value}.{field}: job={job_value}, global={global_value} → OUT OF SYNC")
                return False

        return True

    async def sync_job_with_global(job_type: JobType):
        """Sync a job's parameters with global state - with better UI updates"""
        job_model = app_state.jobs.get(job_type.value)
        if job_model:
            # Use the proper sync method from the parameter model
            job_model.sync_from_pipeline_state(app_state)

            # Force UI updates for all parameter inputs
            card_data = state["job_cards"].get(job_type, {})
            if "param_updaters" in card_data:
                for param_name, updater_fn in card_data["param_updaters"].items():
                    updater_fn()  # Refresh UI input values

            # Update sync indicator
            update_job_card_sync_indicator(job_type)

            ui.run(
                lambda: ui.notify(
                    f"Synced {JobConfig.get_job_display_name(job_type)} with global params", type="positive"
                )
            )

    def update_job_card_sync_indicator(job_type: JobType):
        """Update sync indicator on job card"""
        if job_type not in state["job_cards"]:
            return

        card_data = state["job_cards"][job_type]
        is_synced = is_job_synced_with_global(job_type)

        if "sync_badge" in card_data and card_data["sync_badge"]:
            card_data["sync_badge"].set_visibility(not is_synced)
        if "sync_button" in card_data and card_data["sync_button"]:
            card_data["sync_button"].set_visibility(not is_synced)

    # =============================================================================
    # DATA DETECTION
    # =============================================================================

    async def auto_detect_metadata():
        movies_path = movies_path_input.value
        mdocs_path = mdocs_path_input.value
        if not movies_path or not mdocs_path:
            return

        detection_status.set_text("Detecting...")

        try:
            # This updates global state
            update_from_mdoc(mdocs_path)

            # Update the display values
            state["auto_detected_values"]["pixel_size"] = app_state.microscope.pixel_size_angstrom
            state["auto_detected_values"]["dose_per_tilt"] = app_state.acquisition.dose_per_tilt
            dims = app_state.acquisition.detector_dimensions
            state["auto_detected_values"]["image_size"] = f"{dims[0]}x{dims[1]}"

            # CRITICAL: Force sync all jobs and update UI
            for job_type in state["selected_jobs"]:
                job_model = app_state.jobs.get(job_type.value)
                if job_model and hasattr(job_model, "sync_from_pipeline_state"):
                    job_model.sync_from_pipeline_state(app_state)
                    print(f"[AUTO-DETECT] Synced {job_type.value} with detected values")

                # Update UI inputs
                card_data = state["job_cards"].get(job_type, {})
                if "param_updaters" in card_data:
                    for param_name, updater_fn in card_data["param_updaters"].items():
                        updater_fn()  # Refresh UI input values

                # Update sync indicator
                update_job_card_sync_indicator(job_type)

            # Update global parameter displays
            pixel_size_input.value = str(app_state.microscope.pixel_size_angstrom)
            dose_per_tilt_input.value = str(app_state.acquisition.dose_per_tilt)
            tilt_axis_input.value = str(app_state.acquisition.tilt_axis_degrees)

            detection_status.set_text("Complete")
            ui.notify("Parameters detected and synced to all jobs", type="positive")

        except Exception as e:
            detection_status.set_text("Failed")
            ui.notify(f"Detection failed: {e}", type="negative")
            print(f"[ERROR] Auto-detect failed: {e}")

    def refresh_all_job_parameter_displays():
        """Force refresh all job parameter input displays"""
        for job_type, card_data in state["job_cards"].items():
            if "param_updaters" in card_data:
                for param_name, updater_fn in card_data["param_updaters"].items():
                    updater_fn()
            update_job_card_sync_indicator(job_type)

    def calculate_eer_grouping():
        if not dose_per_tilt_input.value or not eer_grouping_input.value:
            return
        try:
            total_dose = float(dose_per_tilt_input.value)
            grouping = int(eer_grouping_input.value)
            frames = state["auto_detected_values"].get("frames_per_tilt", 40)
            dose_per_frame = (total_dose / frames) * grouping
            rendered = math.floor(frames / grouping)
            lost = frames - (rendered * grouping)
            eer_info_label.set_text(
                f"{grouping} → {rendered} frames, {lost} lost ({lost / frames * 100:.1f}%) | {dose_per_frame:.2f} e⁻/Å²"
            )
        except Exception:
            pass

    # =============================================================================
    # PIPELINE BUILDER
    # =============================================================================

    def add_job_to_pipeline(job_type: JobType):
        """Add a job to the selected pipeline"""
        if job_type in state["selected_jobs"]:
            return

        state["selected_jobs"].append(job_type)
        state["selected_jobs"].sort(key=lambda j: JobConfig.PIPELINE_ORDER.index(j))

        if job_type.value not in app_state.jobs:
            from app_state import prepare_job_params

            prepare_job_params(job_type.value)

        rebuild_pipeline_cards()

    def remove_job_from_pipeline(job_type: JobType):
        """Remove a job from the selected pipeline"""
        if state["project_created"]:
            ui.notify("Cannot modify pipeline after project creation", type="warning")
            return

        if job_type in state["selected_jobs"]:
            state["selected_jobs"].remove(job_type)
            # Clean up state
            if job_type in state["job_cards"]:
                del state["job_cards"][job_type]
            rebuild_pipeline_cards()

    def rebuild_pipeline_cards():
        """Rebuild all job cards"""
        pipeline_container.clear()
        # DON'T clear job_cards - we need to preserve monitor references!
        # state["job_cards"].clear()  # REMOVE THIS LINE

        with pipeline_container:
            if not state["selected_jobs"]:
                ui.label("No jobs selected. Click buttons above to add jobs.").classes("text-xs text-gray-500 italic")
                return

            for idx, job_type in enumerate(state["selected_jobs"]):
                build_job_card(job_type, idx)

    def build_job_card(job_type: JobType, index: int):
        """Build a unified card for a job that transforms based on state"""
        job_model = app_state.jobs.get(job_type.value)
        if not job_model:
            return

        icon = JobConfig.get_job_icon(job_type)
        name = JobConfig.get_job_display_name(job_type)
        is_synced = is_job_synced_with_global(job_type)

        # CRITICAL: Initialize dict entry FIRST
        if job_type not in state["job_cards"]:
            state["job_cards"][job_type] = {"job_index": index + 1}

        with ui.card().classes("w-full mb-2 p-3") as card:
            # Header
            with ui.row().classes("w-full items-center gap-2 mb-2"):
                ui.label(f"{index + 1}.").classes("text-sm font-bold text-gray-700 w-6")
                ui.label(icon).classes("text-lg")
                ui.label(name).classes("text-sm font-semibold flex-grow")

                # Sync indicator (only before run)
                if not state["pipeline_running"]:
                    sync_badge = ui.badge("out of sync", color="orange").classes("text-xs")
                    sync_badge.set_visibility(not is_synced)

                    sync_button = (
                        ui.button(icon="sync", on_click=lambda j=job_type: asyncio.create_task(confirm_sync_job(j)))
                        .props("flat dense round size=sm")
                        .classes("text-blue-600")
                    )
                    sync_button.set_visibility(not is_synced)
                    sync_button.tooltip("Sync with global parameters")
                else:
                    sync_badge = None
                    sync_button = None

                # Remove button (only before create)
                if not state["project_created"]:
                    ui.button(icon="close", on_click=lambda j=job_type: remove_job_from_pipeline(j)).props(
                        "flat dense round size=sm"
                    ).classes("text-red-600")

            # Content area - changes based on state
            content_container = ui.column().classes("w-full")

            with content_container:
                if not state["pipeline_running"]:
                    # BEFORE RUN: Show parameters inline
                    param_inputs = build_parameters_section(job_type, job_model)
                else:
                    # AFTER RUN: Show monitoring tabs
                    build_monitoring_section(job_type, index + 1)
                    param_inputs = None

            # Store/update card references
            state["job_cards"][job_type].update(
                {
                    "card": card,
                    "content_container": content_container,
                    "sync_badge": sync_badge,
                    "sync_button": sync_button,
                    "param_inputs": param_inputs,
                    "job_index": index + 1,
                }
            )

    def build_parameters_section(job_type: JobType, job_model) -> List:
        """Build inline parameters section with proper two-way binding"""
        param_inputs = []
        param_updaters = {}

        with ui.grid(columns=3).classes("gap-2 w-full"):
            for param_name, value in job_model.model_dump().items():
                label = _snake_to_title(param_name)
                element = None

                if isinstance(value, bool):
                    element = ui.checkbox(label, value=value).props("dense")
                    element.bind_value(job_model, param_name)

                elif isinstance(value, (int, float)):
                    # Create input with proper validation
                    element = ui.input(
                        label=label,
                        value=str(value),
                        validation={
                            "Enter valid number": lambda v: v == ""
                            or v.replace(".", "", 1).replace("-", "", 1).isdigit()
                        },
                    ).props("dense outlined")

                    # Store current value for comparison
                    current_display_value = str(value)

                    def create_binding(field_name, is_float=False, ui_element=element):
                        # Model → UI updater
                        def model_to_ui():
                            nonlocal current_display_value
                            current_val = getattr(job_model, field_name)
                            new_display = str(current_val) if current_val is not None else ""
                            if new_display != current_display_value:
                                ui_element.value = new_display
                                current_display_value = new_display
                                print(f"[UI UPDATE] {job_type.value}.{field_name} → {new_display}")

                        # UI → Model handler
                        def ui_to_model():
                            nonlocal current_display_value
                            try:
                                val = ui_element.value.strip()
                                if val:
                                    if is_float:
                                        parsed = float(val)
                                    else:
                                        parsed = int(float(val))  # Handle "2.0" -> 2

                                    current_val = getattr(job_model, field_name)
                                    if parsed != current_val:
                                        setattr(job_model, field_name, parsed)
                                        current_display_value = str(parsed)
                                        print(f"[MODEL UPDATE] {job_type.value}.{field_name} ← {parsed}")
                                        update_job_card_sync_indicator(job_type)
                                else:
                                    # Set default for empty
                                    default = -1 if "do_at_most" in field_name else 0
                                    setattr(job_model, field_name, default)
                                    ui_element.value = str(default)
                            except (ValueError, Exception):
                                # Revert to current model value on error
                                current = getattr(job_model, field_name, 0)
                                ui_element.value = str(current)
                                current_display_value = str(current)

                        ui_element.on("blur", ui_to_model)
                        return model_to_ui

                    # Store the updater function
                    updater_fn = create_binding(param_name, isinstance(value, float))
                    param_updaters[param_name] = updater_fn

                elif isinstance(value, str):
                    if param_name == "alignment_method" and job_type == JobType.TS_ALIGNMENT:
                        options = ["AreTomo", "IMOD", "Relion"]
                        element = ui.select(label=label, options=options, value=value).props("dense outlined")
                    else:
                        element = ui.input(label=label, value=value).props("dense outlined")

                    element.bind_value(job_model, param_name)

                if element:
                    param_inputs.append(element)

        # Store updaters for this job card
        state["job_cards"][job_type]["param_updaters"] = param_updaters
        return param_inputs

    def build_monitoring_section(job_type: JobType, job_index: int):
        """Build monitoring section with tabs (logs, params, files)"""

        # ALREADY have job_cards[job_type] from build_job_card initialization

        # Create sub-tabs
        logs_panel = ui.column().classes("w-full")
        params_panel = ui.column().classes("w-full hidden")
        files_panel = ui.column().classes("w-full hidden")

        with ui.row().classes("w-full bg-gray-100 rounded-t p-1 gap-1") as tab_row:
            logs_btn = (
                ui.button("Logs", icon="description", on_click=lambda: show_panel("logs"))
                .props("flat dense")
                .classes("text-xs")
            )
            params_btn = (
                ui.button("Parameters", icon="settings", on_click=lambda: show_panel("params"))
                .props("flat dense")
                .classes("text-xs")
            )
            files_btn = (
                ui.button("Files", icon="folder", on_click=lambda: show_panel("files"))
                .props("flat dense")
                .classes("text-xs")
            )

        def show_panel(panel_name):
            logs_panel.set_visibility(panel_name == "logs")
            params_panel.set_visibility(panel_name == "params")
            files_panel.set_visibility(panel_name == "files")

        # Build panels
        with logs_panel:
            with ui.row().classes("w-full justify-end mb-2"):
                ui.button(
                    "Refresh", icon="refresh", on_click=lambda: asyncio.create_task(refresh_job_logs(job_type))
                ).props("dense size=sm outline")

            with ui.grid(columns=2).classes("w-full gap-2"):
                with ui.column().classes("w-full"):
                    ui.label("stdout").classes("text-xs font-medium mb-1")
                    stdout_log = ui.log(max_lines=500).classes(
                        "w-full h-64 border rounded bg-gray-50 p-2 text-xs font-mono"
                    )

                with ui.column().classes("w-full"):
                    ui.label("stderr").classes("text-xs font-medium mb-1")
                    stderr_log = ui.log(max_lines=500).classes(
                        "w-full h-64 border rounded bg-red-50 p-2 text-xs font-mono"
                    )

        with params_panel:
            ui.label("Job Parameters Snapshot").classes("text-xs font-medium mb-2")
            ui.label("Parameters used when job was started:").classes("text-xs text-gray-600 mb-2")

            params_json = json.dumps(state["params_snapshot"].get(job_type, {}), indent=2)
            ui.code(params_json, language="json").classes("w-full text-xs")

        with files_panel:
            build_file_browser(job_type, job_index)

        # Store monitor components in EXISTING dict
        state["job_cards"][job_type]["monitor"] = {"stdout": stdout_log, "stderr": stderr_log}


    def build_file_browser(job_type: JobType, job_index: int):
        """Build simple file browser"""
        ui.label("Job Directory Browser").classes("text-xs font-medium mb-2")

        # Get job directory
        job_dir_rel = get_job_directory(job_type, job_index)
        job_dir = Path(state["current_project_path"]) / job_dir_rel

        # Status label
        status_label = ui.label("Waiting for job to start...").classes("text-xs text-gray-500 font-mono mb-1")

        current_path_label = ui.label(str(job_dir)).classes("text-xs text-gray-600 font-mono mb-2")
        file_list_container = ui.column().classes("w-full border rounded p-2 bg-gray-50 max-h-96 overflow-auto")

        async def browse_directory(path: Path):
            file_list_container.clear()
            current_path_label.set_text(str(path))

            # Check if path exists
            if not path.exists():
                status_label.set_text(f"Directory not yet created: {path.name}")
                with file_list_container:
                    ui.label("Job directory will be created when job starts").classes("text-xs text-blue-600")
                    ui.label(f"Expected path: {path}").classes("text-xs text-gray-500 mt-2 font-mono")

                    # Show button to retry
                    ui.button(
                        "Check again", icon="refresh", on_click=lambda: asyncio.create_task(browse_directory(path))
                    ).props("outline dense size=sm").classes("mt-2")
                return

            status_label.set_text("Directory found")

            with file_list_container:
                # Parent directory
                if path != job_dir and path.parent.exists():
                    with (
                        ui.row()
                        .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded")
                        .on("click", lambda p=path.parent: asyncio.create_task(browse_directory(p)))
                    ):
                        ui.icon("folder_open").classes("text-sm")
                        ui.label("..").classes("text-xs")

                # List contents
                try:
                    items = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name))

                    if not items:
                        ui.label("Directory is empty (job may not have started yet)").classes(
                            "text-xs text-gray-500 italic"
                        )

                    for item in items:
                        if item.is_dir():
                            with (
                                ui.row()
                                .classes("items-center gap-2 cursor-pointer hover:bg-gray-200 p-1 rounded")
                                .on("click", lambda i=item: asyncio.create_task(browse_directory(i)))
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
                                ui.label(f"{size_kb} KB").classes("text-xs text-gray-500")

                except PermissionError:
                    ui.label("Permission denied").classes("text-xs text-red-600")
                except Exception as e:
                    ui.label(f"Error: {e}").classes("text-xs text-red-600")

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
                    "",
                ]
                if file_path.suffix.lower() in text_extensions:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read(50000)  # Limit to 50KB

                    with ui.dialog() as dialog, ui.card().classes("w-[60rem] max-w-full"):
                        ui.label(file_path.name).classes("text-sm font-medium mb-2")
                        ui.code(content).classes("w-full max-h-96 overflow-auto text-xs")
                        ui.button("Close", on_click=dialog.close).props("flat")

                    dialog.open()
                else:
                    ui.notify("Cannot preview binary files", type="warning")

            except Exception as e:
                ui.notify(f"Error reading file: {e}", type="negative")

        # Initial load - try a few times since job might not have started
        async def initial_check():
            for i in range(3):
                await browse_directory(job_dir)
                if job_dir.exists():
                    break
                await asyncio.sleep(2)

        asyncio.create_task(initial_check())

    def get_job_directory(job_type: JobType, job_index: int) -> str:
        """Get the job directory name based on type"""
        if job_type == JobType.IMPORT_MOVIES:
            return f"Import/job{job_index:03d}"
        else:
            return f"External/job{job_index:03d}"

    async def confirm_sync_job(job_type: JobType):
        """Show confirmation dialog before syncing"""
        with ui.dialog() as dialog, ui.card():
            ui.label(f"Sync {JobConfig.get_job_display_name(job_type)} with global parameters?").classes("text-sm")
            ui.label("This will overwrite job-specific parameter changes.").classes("text-xs text-gray-600")
            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                ui.button(
                    "Sync", on_click=lambda: (asyncio.create_task(sync_job_with_global(job_type)), dialog.close())
                ).props("color=primary")
        dialog.open()

    # =============================================================================
    # PROJECT CREATION & EXECUTION
    # =============================================================================

    async def handle_create_project():
        name = project_name_input.value
        location = project_location_input.value
        movies = movies_path_input.value
        mdocs = mdocs_path_input.value

        if not all([name, location, movies, mdocs, state["selected_jobs"]]):
            ui.notify("All fields required", type="negative")
            return

        create_button.props("loading")

        # Capture parameter snapshots
        for job_type in state["selected_jobs"]:
            state["params_snapshot"][job_type] = get_job_param_snapshot(job_type)

        result = await backend.create_project_and_scheme(
            project_name=name,
            project_base_path=location,
            selected_jobs=[j.value for j in state["selected_jobs"]],
            movies_glob=movies,
            mdocs_glob=mdocs,
        )

        create_button.props(remove="loading")
        if result.get("success"):
            state["current_project_path"] = result["project_path"]
            state["current_scheme_name"] = f"scheme_{name}"
            state["project_created"] = True

            ui.notify(result["message"], type="positive")
            active_project_label.set_text(name)
            project_status.set_text("Ready")
            run_button.props(remove="disabled")

            # Disable all configuration inputs
            project_name_input.disable()
            project_location_input.disable()
            movies_path_input.disable()
            mdocs_path_input.disable()
            create_button.disable()

            for el in parameter_inputs:
                el.disable()

            # Disable job parameter inputs
            for job_type, card_data in state["job_cards"].items():
                if card_data.get("param_inputs"):
                    for input_el in card_data["param_inputs"]:
                        input_el.disable()
        else:
            ui.notify(f"Error: {result.get('error')}", type="negative")

    async def handle_run_pipeline():
        project_status.classes(remove="text-red-600 text-green-600")
        run_button.props("loading")
        project_status.set_text("Starting...")
        progress_bar.classes(remove="hidden").value = 0
        progress_message.classes(remove="hidden").set_text("Starting...")

        result = await backend.start_pipeline(
            project_path=state["current_project_path"],
            scheme_name=state["current_scheme_name"],
            selected_jobs=[j.value for j in state["selected_jobs"]],
            required_paths=[project_location_input.value, movies_path_input.value, mdocs_path_input.value],
        )

        run_button.props(remove="loading")
        if result.get("success"):
            state["pipeline_running"] = True

            pid = result.get("pid", "N/A")
            ui.notify(f"Started (PID: {pid})", type="positive")
            project_status.set_text(f"Running ({pid})")
            run_button.props("disabled")
            stop_button.props(remove="disabled")

            # Transform job cards to show monitoring
            rebuild_pipeline_cards()

            asyncio.create_task(monitor_all_jobs())
            asyncio.create_task(_monitor_pipeline_progress())
        else:
            project_status.set_text(f"Failed: {result.get('error')}")
            progress_bar.classes("hidden")
            progress_message.classes("hidden")

    async def _monitor_pipeline_progress():
        """Monitor overall pipeline progress"""
        while state["current_project_path"] and not stop_button.props.get("disabled"):
            progress = await backend.get_pipeline_progress(state["current_project_path"])
            if not progress or progress.get("status") != "ok":
                break
            total = progress.get("total", 0)
            completed = progress.get("completed", 0)
            running = progress.get("running", 0)
            failed = progress.get("failed", 0)

            if total > 0:
                progress_bar.value = completed / total
                progress_message.text = f"{completed}/{total} ({running} running, {failed} failed)"

            if progress.get("is_complete") and total > 0:
                msg = "Complete" if failed == 0 else f"Done ({failed} failed)"
                project_status.set_text(msg)
                project_status.classes(add="text-green-600" if failed == 0 else "text-red-600")
                stop_button.props("disabled")
                run_button.props(remove="disabled")
                break
            await asyncio.sleep(5)


    async def refresh_job_logs(job_type: JobType):
        """Manually refresh logs for a job"""
        card_data = state["job_cards"].get(job_type)
        if not card_data or "monitor" not in card_data:
            return

        monitor = card_data["monitor"]
        job_index = card_data["job_index"]

        logs = await backend.get_pipeline_job_logs(state["current_project_path"], job_type.value, str(job_index))

        monitor["stdout"].clear()
        monitor["stdout"].push(logs.get("stdout", "No output"))

        monitor["stderr"].clear()
        monitor["stderr"].push(logs.get("stderr", "No errors"))

        # FIX: Use ui.run to safely show notifications from background tasks
        ui.run(lambda: ui.notify("Logs refreshed", type="positive"))


    async def monitor_all_jobs():
        """Auto-refresh logs for all jobs - no notifications here"""
        last_content = {}

        while state["pipeline_running"] and state["current_project_path"]:
            for job_type in state["selected_jobs"]:
                card_data = state["job_cards"].get(job_type)
                if not card_data or "monitor" not in card_data:
                    continue

                monitor = card_data["monitor"]
                job_index = card_data["job_index"]

                logs = await backend.get_pipeline_job_logs(
                    state["current_project_path"], job_type.value, str(job_index)
                )

                if job_type not in last_content:
                    last_content[job_type] = {"stdout": "", "stderr": ""}

                # Only update if content changed (silent refresh)
                if logs.get("stdout", "") != last_content[job_type]["stdout"]:
                    monitor["stdout"].clear()
                    monitor["stdout"].push(logs.get("stdout", "No output"))
                    last_content[job_type]["stdout"] = logs.get("stdout", "")

                if logs.get("stderr", "") != last_content[job_type]["stderr"]:
                    monitor["stderr"].clear()
                    monitor["stderr"].push(logs.get("stderr", "No errors"))
                    last_content[job_type]["stderr"] = logs.get("stderr", "")

            await asyncio.sleep(5)

    # =============================================================================
    # UI CONSTRUCTION
    # =============================================================================

    parameter_inputs = []

    async def debug_current_state():
        """Debug function to see current state values"""
        print("\n=== DEBUG CURRENT STATE ===")
        print(f"Global pixel_size: {app_state.microscope.pixel_size_angstrom}")
        print(f"Global dose_per_tilt: {app_state.acquisition.dose_per_tilt}")

        for job_type in state["selected_jobs"]:
            job_model = app_state.jobs.get(job_type.value)
            if job_model:
                print(f"\n{job_type.value}:")
                if hasattr(job_model, "pixel_size"):
                    print(f"  pixel_size: {job_model.pixel_size}")
                if hasattr(job_model, "dose_per_tilt_image"):
                    print(f"  dose_per_tilt_image: {job_model.dose_per_tilt_image}")
                if hasattr(job_model, "voltage"):
                    print(f"  voltage: {job_model.voltage}")

        print("=== END DEBUG ===\n")

    with ui.column().classes("w-full gap-2 p-2"):
        # DATA IMPORT
        ui.label("DATA IMPORT").classes("text-xs font-bold text-gray-700")

        with ui.row().classes("w-full gap-2 items-end"):
            ui.button("DEBUG", on_click=debug_current_state).props("outline dense size=sm")
            movies_path_input = create_path_input_with_picker(
                label="Movies",
                mode="directory",
                glob_pattern="*.eer",
                default_value="/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer",
            )
            movies_path_input.classes("flex-grow")

            mdocs_path_input = create_path_input_with_picker(
                label="MDOCs",
                mode="directory",
                glob_pattern="*.mdoc",
                default_value="/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc",
            )
            mdocs_path_input.classes("flex-grow")

            ui.button("Detect", on_click=auto_detect_metadata, icon="auto_fix_high").props("dense size=sm")
            detection_status = ui.label("").classes("text-xs text-gray-500")

        # MICROSCOPE & ACQUISITION
        ui.label("MICROSCOPE & ACQUISITION").classes("text-xs font-bold text-gray-700 mt-3")

        with ui.row().classes("w-full gap-2"):
            with ui.column().classes("gap-1"):
                pixel_size_input = (
                    ui.input(label="Pixel (Å)")
                    .props("dense outlined type=number step=0.01")
                    .classes("w-28")
                    .bind_value(app_state.microscope, "pixel_size_angstrom")
                )

                voltage_input = (
                    ui.input(label="Voltage (kV)")
                    .props("dense outlined type=number")
                    .classes("w-28")
                    .bind_value(app_state.microscope, "acceleration_voltage_kv")
                )

                cs_input = (
                    ui.input(label="Cs (mm)")
                    .props("dense outlined type=number step=0.1")
                    .classes("w-28")
                    .bind_value(app_state.microscope, "spherical_aberration_mm")
                )

            with ui.column().classes("gap-1"):
                amplitude_contrast_input = (
                    ui.input(label="Amp. Contrast")
                    .props("dense outlined type=number step=0.01")
                    .classes("w-28")
                    .bind_value(app_state.microscope, "amplitude_contrast")
                )

                dose_per_tilt_input = (
                    ui.input(label="Dose/Tilt")
                    .props("dense outlined type=number step=0.1")
                    .classes("w-28")
                    .bind_value(app_state.acquisition, "dose_per_tilt")
                )

                tilt_axis_input = (
                    ui.input(label="Tilt Axis (°)")
                    .props("dense outlined type=number step=0.1")
                    .classes("w-28")
                    .bind_value(app_state.acquisition, "tilt_axis_degrees")
                )

            with ui.column().classes("gap-1"):
                image_size_input = ui.input(label="Detector").props("dense outlined readonly").classes("w-32")

                eer_grouping_input = (
                    ui.input(label="EER Group")
                    .props("dense outlined type=number")
                    .classes("w-32")
                    .bind_value(app_state.acquisition, "eer_fractions_per_frame")
                )

                target_dose_input = (
                    ui.input(label="Target Dose").props("dense outlined type=number step=0.01").classes("w-32")
                )

        eer_info_label = ui.label("").classes("text-xs text-blue-600 ml-1")

        parameter_inputs.extend(
            [
                pixel_size_input,
                voltage_input,
                cs_input,
                amplitude_contrast_input,
                dose_per_tilt_input,
                tilt_axis_input,
                eer_grouping_input,
                target_dose_input,
            ]
        )

        ui.label("PROJECT & PIPELINE").classes("text-xs font-bold text-gray-700 mt-3")

        with ui.row().classes("w-full gap-2"):
            project_name_input = ui.input("Name").props("dense outlined").classes("w-48")
            project_location_input = create_path_input_with_picker(
                label="Location", mode="directory", default_value="/users/artem.kushner/dev/crboost_server/projects"
            )
            project_location_input.classes("flex-grow")

        # PIPELINE BUILDER
        ui.label("Build Pipeline").classes("text-xs font-medium text-gray-600 mt-2")

        with ui.row().classes("w-full gap-1 flex-wrap mb-2"):
            for job_type in JobConfig.get_ordered_jobs():
                icon = JobConfig.get_job_icon(job_type)
                name = JobConfig.get_job_display_name(job_type)
                desc = JobConfig.get_job_description(job_type)

                with (
                    ui.button(on_click=lambda j=job_type: add_job_to_pipeline(j))
                    .props("outline dense")
                    .classes("text-xs")
                ):
                    with ui.row().classes("items-center gap-1"):
                        ui.label(icon)
                        ui.label(name)
                        ui.icon("add").classes("text-sm")
                    ui.tooltip(desc)

        # Pipeline container (jobs appear here)
        pipeline_container = ui.column().classes("w-full")

        with pipeline_container:
            ui.label("No jobs selected. Click buttons above to add jobs.").classes("text-xs text-gray-500 italic")

        # Control buttons
        with ui.row().classes("gap-2 mt-2"):
            create_button = ui.button("CREATE", on_click=handle_create_project).props("dense size=sm color=primary")

            with ui.row().classes("items-center gap-1 ml-4"):
                ui.label("Active:").classes("text-xs")
                active_project_label = ui.label("None").classes("text-xs text-gray-600")
                ui.label("|").classes("text-xs text-gray-400")
                project_status = ui.label("No project").classes("text-xs text-gray-600")

            with ui.row().classes("gap-1 ml-auto"):
                run_button = ui.button("RUN", on_click=handle_run_pipeline, icon="play_arrow").props(
                    "disabled dense size=sm"
                )
                stop_button = ui.button(
                    "STOP", on_click=lambda: ui.notify("Stop not implemented", type="warning"), icon="stop"
                ).props("disabled dense size=sm")

        progress_bar = ui.linear_progress(value=0, show_value=False).classes("hidden w-full")
        progress_message = ui.label("").classes("text-xs text-gray-600 hidden")

    # Add after UI construction, at the end of build_projects_tab:

    # Add change listeners to global parameters to update sync indicators
    def setup_global_param_listeners():
        """Setup listeners to update job sync indicators when global params change"""

        def on_global_param_change():
            """When any global param changes, sync all jobs and update UI"""
            # Sync all jobs with new global values
            for job_type in state["selected_jobs"]:
                job_model = app_state.jobs.get(job_type.value)
                if job_model and hasattr(job_model, "sync_from_pipeline_state"):
                    job_model.sync_from_pipeline_state(app_state)

            # Update all job parameter displays
            refresh_all_job_parameter_displays()

        # Listen to all global parameter inputs
        global_inputs = [
            pixel_size_input,
            voltage_input,
            cs_input,
            amplitude_contrast_input,
            dose_per_tilt_input,
            tilt_axis_input,
            eer_grouping_input,
        ]

        for input_element in global_inputs:
            input_element.on("blur", on_global_param_change)
            input_element.on_value_change(lambda: asyncio.create_task(update_sync_indicators_after_delay()))

    async def update_sync_indicators_after_delay():
        """Update sync indicators after a short delay to allow value propagation"""
        await asyncio.sleep(0.1)
        for job_type in state["selected_jobs"]:
            update_job_card_sync_indicator(job_type)

    setup_global_param_listeners()
    # Event handlers
    dose_per_tilt_input.on_value_change(lambda: calculate_eer_grouping())
    target_dose_input.on_value_change(lambda: calculate_eer_grouping())

    def update_image_size_display():
        dims = app_state.acquisition.detector_dimensions
        image_size_input.set_value(f"{dims[0]}x{dims[1]}")

    update_image_size_display()

    async def load_page_data():
        pass

    return load_page_data
