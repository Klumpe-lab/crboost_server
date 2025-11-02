# ui/data_import_panel.py (UPDATED)
import asyncio
import math
from nicegui import ui
from ui.utils import create_path_input_with_picker
from app_state import state as app_state, update_from_mdoc, sync_job_with_global, is_job_synced_with_global
from services.parameter_models import JobType
from typing import Dict, Any


def build_data_import_panel(backend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    """Build the left panel for data import and project configuration"""

    # Local state for this panel
    panel_state = {
        "parameter_inputs": [],
        "movies_path_input": None,
        "mdocs_path_input": None,
        "detection_status": None,
        "project_name_input": None,
        "project_location_input": None,
        "create_button": None,
        "active_project_label": None,
        "project_status": None,
        "run_button": None,
        "stop_button": None,
        "progress_bar": None,
        "progress_message": None,
    }

    async def auto_detect_metadata():
        movies_path = panel_state["movies_path_input"].value
        mdocs_path = panel_state["mdocs_path_input"].value
        if not movies_path or not mdocs_path:
            return

        panel_state["detection_status"].set_text("Detecting...")

        try:
            # This updates global state
            update_from_mdoc(mdocs_path)

            # Update the display values
            shared_state["auto_detected_values"]["pixel_size"] = app_state.microscope.pixel_size_angstrom
            shared_state["auto_detected_values"]["dose_per_tilt"] = app_state.acquisition.dose_per_tilt
            dims = app_state.acquisition.detector_dimensions
            shared_state["auto_detected_values"]["image_size"] = f"{dims[0]}x{dims[1]}"

            # CRITICAL: Force sync all jobs and update UI
            for job_type in shared_state["selected_jobs"]:
                job_model = app_state.jobs.get(job_type.value)
                if job_model and hasattr(job_model, "sync_from_pipeline_state"):
                    job_model.sync_from_pipeline_state(app_state)
                    print(f"[AUTO-DETECT] Synced {job_type.value} with detected values")

                # Update UI inputs
                card_data = shared_state["job_cards"].get(job_type, {})
                if "param_updaters" in card_data:
                    for param_name, updater_fn in card_data["param_updaters"].items():
                        updater_fn()  # Refresh UI input values

                # Update sync indicator
                if "update_job_card_sync_indicator" in callbacks:
                    callbacks["update_job_card_sync_indicator"](job_type)

            # Update global parameter displays
            pixel_size_input.value = str(app_state.microscope.pixel_size_angstrom)
            dose_per_tilt_input.value = str(app_state.acquisition.dose_per_tilt)
            tilt_axis_input.value = str(app_state.acquisition.tilt_axis_degrees)
            update_image_size_display()  # Update detector size

            panel_state["detection_status"].set_text("Complete")
            ui.notify("Parameters detected and synced to all jobs", type="positive")

        except Exception as e:
            panel_state["detection_status"].set_text("Failed")
            ui.notify(f"Detection failed: {e}", type="negative")
            print(f"[ERROR] Auto-detect failed: {e}")

    def calculate_eer_grouping():
        if not dose_per_tilt_input.value or not eer_grouping_input.value:
            return
        try:
            total_dose = float(dose_per_tilt_input.value)
            grouping = int(eer_grouping_input.value)
            frames = shared_state["auto_detected_values"].get("frames_per_tilt", 40)
            if frames == 0:
                return  # Avoid division by zero

            dose_per_frame = (total_dose / frames) * grouping
            rendered = math.floor(frames / grouping)
            lost = frames - (rendered * grouping)
            eer_info_label.set_text(
                f"{grouping} → {rendered} frames, {lost} lost ({lost / frames * 100:.1f}%) | {dose_per_frame:.2f} e⁻/Å²"
            )
        except Exception:
            pass

    async def handle_create_project():
        name = panel_state["project_name_input"].value
        location = panel_state["project_location_input"].value
        movies = panel_state["movies_path_input"].value
        mdocs = panel_state["mdocs_path_input"].value

        if not all([name, location, movies, mdocs, shared_state["selected_jobs"]]):
            ui.notify("All fields, and at least one job, are required", type="negative")
            return

        panel_state["create_button"].props("loading")

        # Capture parameter snapshots
        for job_type in shared_state["selected_jobs"]:
            shared_state["params_snapshot"][job_type] = get_job_param_snapshot(job_type)

        result = await backend.create_project_and_scheme(
            project_name=name,
            project_base_path=location,
            selected_jobs=[j.value for j in shared_state["selected_jobs"]],
            movies_glob=movies,
            mdocs_glob=mdocs,
        )

        panel_state["create_button"].props(remove="loading")
        if result.get("success"):
            shared_state["current_project_path"] = result["project_path"]
            shared_state["current_scheme_name"] = f"scheme_{name}"
            shared_state["project_created"] = True

            ui.notify(result["message"], type="positive")
            panel_state["active_project_label"].set_text(name)
            panel_state["project_status"].set_text("Ready")
            panel_state["run_button"].props(remove="disabled")

            # Disable all configuration inputs
            panel_state["project_name_input"].disable()
            panel_state["project_location_input"].disable()
            panel_state["movies_path_input"].disable()
            panel_state["mdocs_path_input"].disable()
            panel_state["create_button"].disable()

            for el in panel_state["parameter_inputs"]:
                el.disable()

            # ---
            # NOTE: Disabling job parameters now needs to happen
            # in pipeline_builder_panel.py
            # ---
            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()
            ui.notify("Project created. Job parameters are now locked.", type="info")

        else:
            ui.notify(f"Error: {result.get('error')}", type="negative")

    async def handle_run_pipeline():
        panel_state["project_status"].classes(remove="text-red-600 text-green-600")
        panel_state["run_button"].props("loading")
        panel_state["project_status"].set_text("Starting...")
        panel_state["progress_bar"].classes(remove="hidden").value = 0
        panel_state["progress_message"].classes(remove="hidden").set_text("Starting...")

        result = await backend.start_pipeline(
            project_path=shared_state["current_project_path"],
            scheme_name=shared_state["current_scheme_name"],
            selected_jobs=[j.value for j in shared_state["selected_jobs"]],
            required_paths=[
                panel_state["project_location_input"].value,
                panel_state["movies_path_input"].value,
                panel_state["mdocs_path_input"].value,
            ],
        )

        panel_state["run_button"].props(remove="loading")
        if result.get("success"):
            shared_state["pipeline_running"] = True  # Set the flag

            pid = result.get("pid", "N/A")
            ui.notify(f"Started (PID: {pid})", type="positive")
            panel_state["project_status"].set_text(f"Running ({pid})")
            panel_state["run_button"].props("disabled")
            panel_state["stop_button"].props(remove="disabled")

            # +++ FIX 1: CALL THE CALLBACK +++
            # This tells the pipeline_builder_panel to rebuild
            # itself in "Monitoring" mode.
            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()
            # +++ END FIX 1 +++

            # Start monitoring tasks
            asyncio.create_task(monitor_all_jobs())
            asyncio.create_task(_monitor_pipeline_progress())

            # +++ FIX 2: START JOB STATUS MONITORING +++
            asyncio.create_task(monitor_job_statuses())
            # +++ END FIX 2 +++

        else:
            panel_state["project_status"].set_text(f"Failed: {result.get('error')}")
            panel_state["progress_bar"].classes("hidden")
            panel_state["progress_message"].classes("hidden")

    async def _monitor_pipeline_progress():
        """Monitor overall pipeline progress"""
        try:
            while shared_state["current_project_path"] and shared_state["pipeline_running"]:
                progress = await backend.get_pipeline_progress(shared_state["current_project_path"])

                if not progress or progress.get("status") != "ok":
                    panel_state["project_status"].set_text("Monitoring Error")
                    panel_state["project_status"].classes(add="text-red-600")
                    break  # Exit loop on error

                total = progress.get("total", 0)
                completed = progress.get("completed", 0)
                running = progress.get("running", 0)
                failed = progress.get("failed", 0)

                if total > 0:
                    panel_state["progress_bar"].value = completed / total
                    panel_state["progress_message"].text = f"{completed}/{total} ({running} running, {failed} failed)"

                if progress.get("is_complete") and total > 0:
                    msg = "Complete" if failed == 0 else f"Done ({failed} failed)"
                    panel_state["project_status"].set_text(msg)
                    panel_state["project_status"].classes(add="text-green-600" if failed == 0 else "text-red-600")
                    break  # Exit loop on complete

                await asyncio.sleep(5)

        except Exception as e:
            print(f"[ERROR] Pipeline monitor failed: {e}")
            panel_state["project_status"].set_text("Monitor Failed")
            panel_state["project_status"].classes(add="text-red-600")

        finally:
            # This logic runs on success (break), error (break), or exception
            print("Overall progress monitoring loop finished.")
            shared_state["pipeline_running"] = False  # Signal completion/failure
            panel_state["stop_button"].props("disabled")
            panel_state["run_button"].props(remove="disabled").props(remove="loading")

    # +++ FIX 2 (Continued): ADD THE NEW FUNCTION +++
    async def monitor_job_statuses():
        """
        Monitors the status of individual jobs (pending/running/success/failed)
        and updates the shared state to reflect in the UI.
        """
        try:
            while shared_state["pipeline_running"] and shared_state["current_project_path"]:
                # --- TODO: IMPLEMENT YOUR BACKEND CALL HERE ---
                # You need a backend endpoint that returns the status of each job.
                # Example:
                # job_statuses = await backend.get_all_job_statuses(shared_state["current_project_path"])
                # e.g., job_statuses = {"import_movies": "success", "fs_motion_ctf": "running", "ts_alignment": "pending"}

                # --- Mock data for testing (REMOVE THIS) ---
                import random

                job_statuses = {}
                for jt in shared_state["selected_jobs"]:
                    if shared_state["job_cards"][jt]["status"] == "success":
                        job_statuses[jt.value] = "success"
                        continue
                    job_statuses[jt.value] = random.choice(["pending", "running", "success", "failed"])
                # --- End Mock data ---

                for job_type in shared_state["selected_jobs"]:
                    status = job_statuses.get(job_type.value, "pending")

                    # Update state only if it changed
                    if shared_state["job_cards"][job_type]["status"] != status:
                        shared_state["job_cards"][job_type]["status"] = status

                await asyncio.sleep(3)  # Poll for status every 3 seconds

        except Exception as e:
            print(f"[ERROR] Job status monitor failed: {e}")

        finally:
            print("Job status monitoring loop finished.")
            # Once the main progress loop sets pipeline_running=False, this loop will exit.
            # We can do final status check here if needed.

            # Example: Mark any remaining 'running' jobs as 'failed' or 'unknown'
            for job_type in shared_state["selected_jobs"]:
                if shared_state["job_cards"][job_type]["status"] == "running":
                    shared_state["job_cards"][job_type]["status"] = "failed"  # Or 'unknown'

    # +++ END FIX 2 +++

    async def monitor_all_jobs():
        """Auto-refresh logs for all jobs"""
        last_content = {}

        # Wait for monitoring UI to be built
        await asyncio.sleep(1)

        while shared_state["pipeline_running"] and shared_state["current_project_path"]:
            try:
                for job_type in shared_state["selected_jobs"]:
                    card_data = shared_state["job_cards"].get(job_type)

                    # +++ FIX 3: ADDED CHECK FOR 'monitor' KEY AND VALUE +++
                    if not card_data or "monitor" not in card_data or not card_data["monitor"]:
                        # UI hasn't built the log panel for this tab yet, skip
                        continue
                    # +++ END FIX 3 +++

                    monitor = card_data["monitor"]
                    job_index = card_data["job_index"]

                    logs = await backend.get_pipeline_job_logs(
                        shared_state["current_project_path"], job_type.value, str(job_index)
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

            except Exception as e:
                print(f"[ERROR] Log monitoring loop failed: {e}")
                await asyncio.sleep(10)  # Wait longer on error

    def get_job_param_snapshot(job_type):
        """Capture current parameters for a job"""
        job_model = app_state.jobs.get(job_type.value)
        if job_model:
            return job_model.model_dump()
        return {}

    def update_job_card_sync_indicator(job_type):
        """Update sync indicator on job card"""
        if "update_job_card_sync_indicator" in callbacks:
            callbacks["update_job_card_sync_indicator"](job_type)

    def refresh_all_job_parameter_displays():
        """Force refresh all job parameter input displays"""
        for job_type in shared_state["selected_jobs"]:
            card_data = shared_state["job_cards"].get(job_type, {})
            if "param_updaters" in card_data:
                for param_name, updater_fn in card_data["param_updaters"].items():
                    updater_fn()
            update_job_card_sync_indicator(job_type)

    def update_image_size_display():
        dims = app_state.acquisition.detector_dimensions
        if image_size_input:
            image_size_input.set_value(f"{dims[0]}x{dims[1]}")

    # UI Construction
    with ui.column().classes("w-full h-full gap-3 p-3 overflow-y-auto"):
        # DATA IMPORT SECTION
        ui.label("DATA IMPORT").classes("text-xs font-bold text-gray-700 uppercase tracking-wide")

        with ui.card().classes("w-full p-4 bg-blue-50/50 border border-blue-200"):
            with ui.column().classes("w-full gap-3"):
                with ui.row().classes("w-full gap-2 items-end"):
                    panel_state["movies_path_input"] = create_path_input_with_picker(
                        label="Movies Directory",
                        mode="directory",
                        glob_pattern="*.eer",
                        default_value="/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer",
                    ).classes("flex-grow min-w-0")

                    panel_state["mdocs_path_input"] = create_path_input_with_picker(
                        label="MDOCs Directory",
                        mode="directory",
                        glob_pattern="*.mdoc",
                        default_value="/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc",
                    ).classes("flex-grow min-w-0")

                    detect_btn = (
                        ui.button("Detect", on_click=auto_detect_metadata, icon="auto_fix_high")
                        .props("dense")
                        .classes("min-w-20")
                    )
                    panel_state["detection_status"] = ui.label("").classes("text-xs text-gray-500 min-w-20")

        # MICROSCOPE & ACQUISITION SECTION
        ui.label("MICROSCOPE & ACQUISITION").classes("text-xs font-bold text-gray-700 uppercase tracking-wide mt-4")

        with ui.card().classes("w-full p-4 bg-green-50/50 border border-green-200"):
            with ui.grid(columns=3).classes("w-full gap-3"):
                # Column 1
                pixel_size_input = (
                    ui.input(label="Pixel Size (Å)").props("dense outlined type=number step=0.01").classes("w-full")
                )
                voltage_input = ui.input(label="Voltage (kV)").props("dense outlined type=number").classes("w-full")
                cs_input = ui.input(label="Cs (mm)").props("dense outlined type=number step=0.1").classes("w-full")

                # Column 2
                amplitude_contrast_input = (
                    ui.input(label="Amplitude Contrast").props("dense outlined type=number step=0.01").classes("w-full")
                )
                dose_per_tilt_input = (
                    ui.input(label="Dose per Tilt").props("dense outlined type=number step=0.1").classes("w-full")
                )
                tilt_axis_input = (
                    ui.input(label="Tilt Axis (°)").props("dense outlined type=number step=0.1").classes("w-full")
                )

                # Column 3
                image_size_input = ui.input(label="Detector Size").props("dense outlined readonly").classes("w-full")
                eer_grouping_input = (
                    ui.input(label="EER Grouping").props("dense outlined type=number").classes("w-full")
                )
                target_dose_input = (
                    ui.input(label="Target Dose").props("dense outlined type=number step=0.01").classes("w-full")
                )

            eer_info_label = ui.label("").classes("text-xs text-blue-600 mt-2")

        # Bind values
        pixel_size_input.bind_value(app_state.microscope, "pixel_size_angstrom")
        voltage_input.bind_value(app_state.microscope, "acceleration_voltage_kv")
        cs_input.bind_value(app_state.microscope, "spherical_aberration_mm")
        amplitude_contrast_input.bind_value(app_state.microscope, "amplitude_contrast")
        dose_per_tilt_input.bind_value(app_state.acquisition, "dose_per_tilt")
        tilt_axis_input.bind_value(app_state.acquisition, "tilt_axis_degrees")
        eer_grouping_input.bind_value(app_state.acquisition, "eer_fractions_per_frame")

        panel_state["parameter_inputs"].extend(
            [
                pixel_size_input,
                voltage_input,
                cs_input,
                amplitude_contrast_input,
                dose_per_tilt_input,
                tilt_axis_input,
                eer_grouping_input,
                target_dose_input,
                image_size_input,  # Also disable image size input
            ]
        )

        # PROJECT CONFIGURATION SECTION
        ui.label("PROJECT CONFIGURATION").classes("text-xs font-bold text-gray-700 uppercase tracking-wide mt-4")

        with ui.card().classes("w-full p-4 bg-purple-50/50 border border-purple-200"):
            with ui.column().classes("w-full gap-3"):
                with ui.grid(columns=2).classes("w-full gap-3"):
                    panel_state["project_name_input"] = (
                        ui.input("Project Name").props("dense outlined").classes("w-full")
                    )
                    panel_state["project_location_input"] = create_path_input_with_picker(
                        label="Project Location",
                        mode="directory",
                        default_value="/users/artem.kushner/dev/crboost_server/projects",
                    ).classes("w-full")

                panel_state["create_button"] = (
                    ui.button("CREATE PROJECT", on_click=handle_create_project)
                    .props("dense color=primary")
                    .classes("w-full")
                )

        # PROJECT STATUS SECTION
        ui.label("PIPELINE CONTROL").classes("text-xs font-bold text-gray-700 uppercase tracking-wide mt-4")

        with ui.card().classes("w-full p-4 bg-orange-50/50 border border-orange-200"):
            with ui.column().classes("w-full gap-3"):
                # Status row
                with ui.row().classes("w-full items-center justify-between"):
                    with ui.row().classes("items-center gap-2"):
                        ui.label("Active Project:").classes("text-xs font-medium")
                        panel_state["active_project_label"] = ui.label("None").classes(
                            "text-xs text-gray-600 font-mono"
                        )
                        ui.label("|").classes("text-xs text-gray-400")
                        panel_state["project_status"] = ui.label("No project").classes("text-xs text-gray-600")

                # Control buttons
                with ui.row().classes("w-full gap-2"):
                    panel_state["run_button"] = (
                        ui.button("RUN PIPELINE", on_click=handle_run_pipeline, icon="play_arrow")
                        .props("disabled dense")
                        .classes("flex-1")
                    )
                    panel_state["stop_button"] = (
                        ui.button("STOP", icon="stop").props("disabled dense").classes("flex-1")
                    )

                # Progress
                panel_state["progress_bar"] = ui.linear_progress(value=0, show_value=False).classes("w-full hidden")
                panel_state["progress_message"] = ui.label("").classes(
                    "text-xs text-gray-600 hidden w-full text-center"
                )

        # In ui/data_import_panel.py, add after the PROJECT STATUS section:

        ui.label("SLURM CONFIGURATION").classes("text-xs font-bold text-gray-700 uppercase tracking-wide mt-4")

        with ui.card().classes("w-full p-4 bg-cyan-50/50 border border-cyan-200"):
            with ui.column().classes("w-full gap-3"):
                # Job Configuration Inputs
                ui.label("Job Resource Requirements").classes("text-sm font-medium mb-2")

                with ui.grid(columns=3).classes("w-full gap-3"):
                    # Column 1
                    partition_input = (
                        ui.select(label="Partition", options=[], value=None).props("dense outlined").classes("w-full")
                    )

                    nodes_input = (
                        ui.input(label="Nodes", value="1").props("dense outlined type=number min=1").classes("w-full")
                    )

                    constraint_input = (
                        ui.input(label="Constraint", value="g2|g3|g4", placeholder="e.g. g2|g3|g4")
                        .props("dense outlined")
                        .classes("w-full")
                    )

                    # Column 2
                    cpus_input = (
                        ui.input(label="CPUs per Task", value="16")
                        .props("dense outlined type=number min=1")
                        .classes("w-full")
                    )

                    gpus_input = (
                        ui.input(label="GPUs", value="4").props("dense outlined type=number min=0").classes("w-full")
                    )

                    memory_input = (
                        ui.input(label="Memory (GB)", value="96")
                        .props("dense outlined type=number min=1")
                        .classes("w-full")
                    )

                    # Column 3
                    time_input = (
                        ui.input(label="Time Limit", value="5:00:00", placeholder="HH:MM:SS")
                        .props("dense outlined")
                        .classes("w-full")
                    )

                    ntasks_input = (
                        ui.input(label="Tasks per Node", value="1")
                        .props("dense outlined type=number min=1")
                        .classes("w-full")
                    )

                # Store inputs in panel state
                panel_state["slurm_inputs"] = {
                    "partition": partition_input,
                    "nodes": nodes_input,
                    "cpus": cpus_input,
                    "gpus": gpus_input,
                    "memory": memory_input,
                    "time": time_input,
                    "ntasks": ntasks_input,
                    "constraint": constraint_input,
                }

                # Cluster Overview (collapsed by default)
                with (
                    ui.expansion("Cluster Overview", icon="info")
                    .props("dense")
                    .classes("w-full mt-2") as overview_expansion
                ):
                    with ui.column().classes("w-full gap-3 p-3"):
                        # Summary row with refresh
                        with ui.row().classes("w-full items-center justify-between mb-2"):
                            ui.label("Cluster Status").classes("text-xs font-medium")
                            refresh_btn = ui.button(
                                icon="refresh",
                                on_click=lambda: asyncio.create_task(refresh_slurm_info(force_refresh=True))
                            ).props("dense flat round size=sm").classes("text-blue-600")
                            refresh_btn.tooltip("Refresh cluster info")

                        # Summary stats container
                        summary_container = ui.row().classes("w-full gap-4 mb-3")

                        # Partition details
                        with ui.row().classes("w-full items-center gap-2 mb-2"):
                            ui.label("View Partition:").classes("text-xs font-medium")
                            partition_view_select = (
                                ui.select(
                                    options=[],
                                    value=None,
                                    on_change=lambda e: asyncio.create_task(load_partition_details(e.value)),
                                )
                                .props("dense outlined")
                                .classes("flex-grow")
                            )

                        partition_details = ui.column().classes("w-full")

                        # User jobs section
                        with ui.expansion("My Jobs", icon="work").props("dense").classes("w-full"):
                            jobs_container = ui.column().classes("w-full gap-2")

        # Store references
        panel_state["slurm_summary"] = summary_container
        panel_state["partition_view_select"] = partition_view_select
        panel_state["partition_details"] = partition_details
        panel_state["jobs_container"] = jobs_container
        panel_state["refresh_btn"] = refresh_btn

        async def refresh_slurm_info(force_refresh: bool = False):
            """Refresh all SLURM information - works in any context"""
            try:
                refresh_btn.props("loading")
                if force_refresh:
                    backend.slurm_service.clear_cache()
                    print("[DEBUG] SLURM cache cleared")

                summary_result = await backend.get_slurm_summary()
                if summary_result.get("success"):
                    summary = summary_result["summary"]
                    
                    # Clear and update summary container
                    summary_container.clear()
                    with summary_container:
                        with ui.row().classes("gap-4"):
                            with ui.column().classes("items-center"):
                                ui.label(str(summary["total_partitions"])).classes("text-lg font-semibold text-blue-600")
                                ui.label("Partitions").classes("text-xs text-gray-600")
                            
                            with ui.column().classes("items-center"):
                                ui.label(str(summary["total_nodes"])).classes("text-lg font-semibold text-green-600")
                                ui.label("Nodes").classes("text-xs text-gray-600")
                            
                            with ui.column().classes("items-center"):
                                ui.label(str(summary["total_gpus"])).classes("text-lg font-semibold text-purple-600")
                                ui.label("GPUs").classes("text-xs text-gray-600")
                            
                            with ui.column().classes("items-center"):
                                ui.label(f"{summary['running_jobs']}/{summary['user_jobs']}").classes("text-lg font-semibold text-orange-600")
                                ui.label("My Jobs").classes("text-xs text-gray-600")
                
                # Get partitions
                partitions_result = await backend.get_slurm_partitions()
                if partitions_result.get("success"):
                    partitions = partitions_result["partitions"]
                    # Deduplicate partition names
                    unique_partitions = {}
                    for p in partitions:
                        name = p["name"]
                        if name not in unique_partitions:
                            unique_partitions[name] = p
                    
                    partition_names = sorted(unique_partitions.keys())
                    
                    # Update both selects
                    partition_input.options = partition_names
                    partition_view_select.options = partition_names
                    
                    # Set default partition if not set
                    if partition_names:
                        if not partition_input.value:
                            partition_input.value = partition_names[0]
                        if not partition_view_select.value:
                            partition_view_select.value = partition_names[0]
                            await load_partition_details(partition_names[0])
                
                # Get user jobs with force refresh flag
                jobs_result = await backend.get_user_slurm_jobs(force_refresh=force_refresh)
                if jobs_result.get("success"):
                    jobs = jobs_result["jobs"]
                    
                    # Clear and update jobs container
                    jobs_container.clear()
                    with jobs_container:
                        if not jobs:
                            ui.label("No active jobs").classes("text-xs text-gray-500 italic")
                        else:
                            for job in jobs:
                                state_color = {
                                    "RUNNING": "green",
                                    "PENDING": "orange",
                                    "COMPLETED": "blue",
                                    "FAILED": "red"
                                }.get(job["state"], "gray")
                                
                                with ui.row().classes("w-full items-center gap-2 p-2 border rounded bg-white"):
                                    ui.badge(job["state"], color=state_color).classes("text-xs")
                                    ui.label(job["name"]).classes("text-xs font-medium flex-grow")
                                    ui.label(f"ID: {job['job_id']}").classes("text-xs text-gray-500 font-mono")
                                    ui.label(job["time"]).classes("text-xs text-gray-500")
                
                print("[INFO] SLURM info refreshed successfully")
                
            except Exception as e:
                print(f"[ERROR] SLURM refresh failed: {e}")
                import traceback
                traceback.print_exc()
            finally:
                refresh_btn.props(remove="loading")

        async def load_partition_details(partition_name: str):
            """Load and display partition details - works in any context"""
            if not partition_name:
                return

            try:
                # Clear the container
                partition_details.clear()

                nodes_result = await backend.get_slurm_nodes(partition_name)
                if nodes_result.get("success"):
                    nodes = nodes_result["nodes"]

                    # Update partition details
                    with partition_details:
                        ui.label(f"{partition_name} Nodes").classes("text-xs font-medium mt-2 mb-1")

                        if not nodes:
                            ui.label("No nodes available").classes("text-xs text-gray-500 italic")
                        else:
                            # Group nodes by state for summary
                            state_counts = {}
                            for node in nodes:
                                state = node["state"].lower()
                                state_counts[state] = state_counts.get(state, 0) + 1

                            # Show state summary
                            with ui.row().classes("w-full gap-2 mb-2"):
                                for state, count in sorted(state_counts.items()):
                                    state_color = {
                                        "idle": "green",
                                        "allocated": "blue",
                                        "mix": "orange",
                                        "down": "red",
                                        "drain": "red",
                                    }.get(state, "gray")
                                    ui.badge(f"{state}: {count}", color=state_color).classes("text-xs")

                            # Show first 6 nodes as examples
                            with ui.grid(columns=2).classes("w-full gap-2"):
                                for node in nodes[:6]:
                                    state_color = {
                                        "idle": "green",
                                        "allocated": "blue",
                                        "alloc": "blue",
                                        "mix": "orange",
                                        "down": "red",
                                        "drain": "red",
                                        "comp": "orange",
                                    }.get(node["state"].lower(), "gray")

                                    with ui.card().classes("p-2 bg-white"):
                                        with ui.row().classes("w-full items-center justify-between mb-1"):
                                            ui.label(node["name"]).classes("text-xs font-medium font-mono")
                                            ui.badge(node["state"], color=state_color).classes("text-xs")

                                        with ui.column().classes("gap-1"):
                                            ui.label(f"{node['cpus']} CPUs").classes("text-xs text-gray-600")
                                            ui.label(f"{node['memory_mb']} MB").classes("text-xs text-gray-600")
                                            if node["gpus"] > 0:
                                                gpu_label = f"{node['gpus']} x {node['gpu_type'] or 'GPU'}"
                                                ui.label(gpu_label).classes("text-xs text-purple-600 font-medium")

                            if len(nodes) > 6:
                                ui.label(f"+ {len(nodes) - 6} more nodes").classes("text-xs text-gray-500 italic mt-2")

            except Exception as e:
                print(f"[ERROR] Failed to load partition details: {e}")
                with partition_details:
                    ui.label(f"Error loading nodes: {e}").classes("text-xs text-red-600")

        # Store callbacks
        panel_state["refresh_slurm_info"] = refresh_slurm_info
        panel_state["load_partition_details"] = load_partition_details
        # Set up event handlers
        dose_per_tilt_input.on_value_change(calculate_eer_grouping)
        target_dose_input.on_value_change(calculate_eer_grouping)
        update_image_size_display()

        # Add global parameter change listeners
        def setup_global_param_listeners():
            def on_global_param_change():
                for job_type in shared_state["selected_jobs"]:
                    job_model = app_state.jobs.get(job_type.value)
                    if job_model and hasattr(job_model, "sync_from_pipeline_state"):
                        job_model.sync_from_pipeline_state(app_state)
                refresh_all_job_parameter_displays()

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

        setup_global_param_listeners()

    async def delayed_slurm_init():
        await asyncio.sleep(0.5)  # Give UI time to initialize
        try:
            await refresh_slurm_info()
            print("[INFO] Initial SLURM data loaded")
        except Exception as e:
            print(f"[WARN] Initial SLURM load failed: {e}")

    asyncio.create_task(delayed_slurm_init())

    # Store callback for external access
    panel_state["refresh_slurm_info"] = refresh_slurm_info

    return panel_state
