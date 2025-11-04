# ui/data_import_panel.py
import asyncio
import json
from pathlib import Path
import math
from backend import CryoBoostBackend
from nicegui import ui
from ui.slurm_components import build_cluster_overview, build_slurm_job_config
from ui.utils import create_path_input_with_picker
from app_state import state as app_state, update_from_mdoc
from typing import Dict, Any


def build_data_import_panel(backend: CryoBoostBackend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    """Build the left panel for data import and project configuration"""

    panel_state = {
        "parameter_inputs": [],
        "movies_path_input": None,
        "mdocs_path_input": None,
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
    # Add this near the top of build_data_import_panel, after panel_state definition

    async def handle_open_project():
        """Load an existing project for continuation"""
        project_path_input = None

        with ui.dialog() as dialog, ui.card():
            ui.label("Open Existing Project").classes("text-lg font-semibold mb-4")
            project_path_input = ui.input("Project Path").props("dense outlined").classes("w-96")
            project_path_input.value = "/users/artem.kushner/dev/crboost_server/projects/"

            with ui.row().classes("w-full justify-end mt-4").style("gap: 8px;"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                # Remove asyncio.create_task wrapper - just call directly
                ui.button("Load", on_click=lambda: load_project(project_path_input.value, dialog)).props("flat")

        dialog.open()

    async def load_project(project_path: str, dialog):
        """Actually load the project"""
        if not project_path:
            ui.notify("Please enter a project path", type="warning")
            return

        dialog.close()
        ui.notify("Loading project...", type="info")

        result = await backend.load_existing_project(project_path)

        if not result.get("success"):
            ui.notify(f"Failed to load project: {result.get('error')}", type="negative")
            return

        # Populate UI fields with loaded data
        panel_state["project_name_input"].value = result["project_name"]
        panel_state["project_location_input"].value = str(Path(project_path).parent)
        panel_state["movies_path_input"].value = result.get("movies_glob", "")
        panel_state["mdocs_path_input"].value = result.get("mdocs_glob", "")

        # Load project parameters from the saved JSON
        params_file = Path(project_path) / "project_params.json"
        if params_file.exists():
            with open(params_file, "r") as f:
                project_params = json.load(f)

            # Update global app_state from saved params
            if "microscope" in project_params:
                for key, value in project_params["microscope"].items():
                    if hasattr(app_state.microscope, key):
                        setattr(app_state.microscope, key, value)

            if "acquisition" in project_params:
                for key, value in project_params["acquisition"].items():
                    if hasattr(app_state.acquisition, key):
                        setattr(app_state.acquisition, key, value)

            # Update UI inputs to reflect loaded state
            update_image_size_display()

        # Set shared state for continuation
        shared_state["current_project_path"] = project_path
        shared_state["current_scheme_name"] = f"scheme_{result['project_name']}"
        shared_state["project_created"] = True
        shared_state["continuation_mode"] = True
        shared_state["discovered_jobs"] = result.get("discovered_jobs", [])
        shared_state["next_job_number"] = result.get("next_job_number", 1)

        # Update selected_jobs from loaded project
        from services.parameter_models import JobType

        shared_state["selected_jobs"] = [JobType.from_string(j) for j in result.get("selected_jobs", [])]

        # ===== NEW: Populate job_cards with discovered job info =====
        # ===== Load job parameters from saved job_params.json files =====
        from services.parameter_models import jobtype_paramclass

        for discovered_job in result.get("discovered_jobs", []):
            job_type = JobType.from_string(discovered_job["type"])
            job_number = discovered_job["number"]
            job_status = discovered_job["status"]

            # Get the param class for this job type
            param_classes = jobtype_paramclass()
            param_class = param_classes.get(job_type)

            if not param_class:
                print(f"[WARN] No param class for {job_type.value}")
                continue

            category = param_class.JOB_CATEGORY
            job_params_path = Path(project_path) / category.value / f"job{job_number:03d}" / "job_params.json"

            if job_params_path.exists():
                try:
                    with open(job_params_path, "r") as f:
                        saved_params = json.load(f)

                    # Store the snapshot for display
                    shared_state["params_snapshot"][job_type] = saved_params

                    # Deserialize the job model directly from saved params
                    if "job_model" in saved_params:
                        job_model = param_class(**saved_params["job_model"])
                        app_state.jobs[job_type.value] = job_model
                        print(f"[CONTINUATION] Loaded {job_type.value} from saved params")

                except Exception as e:
                    print(f"[ERROR] Could not load job params for {job_type.value}: {e}")
                    import traceback

                    traceback.print_exc()
            else:
                print(f"[WARN] No saved params found at {job_params_path}")

            # Create job card entry (only once!)
            shared_state["job_cards"][job_type] = {"job_index": job_number, "status": job_status, "is_synced": True}

        panel_state["project_name_input"].disable()
        panel_state["project_location_input"].disable()
        panel_state["movies_path_input"].disable()
        panel_state["mdocs_path_input"].disable()
        panel_state["create_button"].disable()

        discovered = result.get("discovered_jobs", [])
        if discovered:
            last_job = discovered[-1]
            if last_job["status"] == "failed":
                shared_state["last_job_failed"] = True
                if panel_state.get("project_status"):
                    panel_state["project_status"].set_text(f"Last job failed: {last_job['type']}")
                    panel_state["project_status"].classes(add="text-red-600")
            else:
                if panel_state.get("project_status"):
                    panel_state["project_status"].set_text(f"Loaded: {len(discovered)} jobs")
                if panel_state.get("run_button"):
                    panel_state["run_button"].props(remove="disabled")

        # Rebuild pipeline UI to show discovered jobs
        if "rebuild_pipeline_ui" in callbacks:
            callbacks["rebuild_pipeline_ui"]()

        ui.notify(f"Loaded project: {result['project_name']}", type="positive")

    async def auto_detect_metadata():
        movies_path = panel_state["movies_path_input"].value
        mdocs_path = panel_state["mdocs_path_input"].value
        if not movies_path or not mdocs_path:
            return

        try:
            update_from_mdoc(mdocs_path)

            shared_state["auto_detected_values"]["pixel_size"] = app_state.microscope.pixel_size_angstrom
            shared_state["auto_detected_values"]["dose_per_tilt"] = app_state.acquisition.dose_per_tilt
            dims = app_state.acquisition.detector_dimensions
            shared_state["auto_detected_values"]["image_size"] = f"{dims[0]}x{dims[1]}"

            for job_type in shared_state["selected_jobs"]:
                job_model = app_state.jobs.get(job_type.value)
                if job_model and hasattr(job_model, "sync_from_pipeline_state"):
                    job_model.sync_from_pipeline_state(app_state)

                card_data = shared_state["job_cards"].get(job_type, {})
                if "param_updaters" in card_data:
                    for param_name, updater_fn in card_data["param_updaters"].items():
                        updater_fn()

                if "update_job_card_sync_indicator" in callbacks:
                    callbacks["update_job_card_sync_indicator"](job_type)

            pixel_size_input.value = str(app_state.microscope.pixel_size_angstrom)
            dose_per_tilt_input.value = str(app_state.acquisition.dose_per_tilt)
            tilt_axis_input.value = str(app_state.acquisition.tilt_axis_degrees)
            update_image_size_display()

            ui.notify("Parameters detected and synced", type="positive")

        except Exception as e:
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
                return

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
            ui.notify("All fields and at least one job required", type="negative")
            return

        panel_state["create_button"].props("loading")

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
            panel_state["project_status"].set_text("Ready")
            panel_state["run_button"].props(remove="disabled")

            panel_state["project_name_input"].disable()
            panel_state["project_location_input"].disable()
            panel_state["movies_path_input"].disable()
            panel_state["mdocs_path_input"].disable()
            panel_state["create_button"].disable()

            for el in panel_state["parameter_inputs"]:
                el.disable()

            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()
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
            shared_state["pipeline_running"] = True

            pid = result.get("pid", "N/A")
            ui.notify(f"Started (PID: {pid})", type="positive")
            panel_state["project_status"].set_text(f"Running ({pid})")
            panel_state["run_button"].props("disabled")
            panel_state["stop_button"].props(remove="disabled")

            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()
            asyncio.create_task(monitor_all_jobs())
            asyncio.create_task(_monitor_pipeline_progress())
            asyncio.create_task(monitor_job_statuses())

        else:
            panel_state["project_status"].set_text(f"Failed: {result.get('error')}")
            panel_state["progress_bar"].classes("hidden")
            panel_state["progress_message"].classes("hidden")

    async def _monitor_pipeline_progress():
        try:
            while shared_state["current_project_path"] and shared_state["pipeline_running"]:
                progress = await backend.get_pipeline_progress(shared_state["current_project_path"])

                if not progress or progress.get("status") != "ok":
                    panel_state["project_status"].set_text("Monitoring Error")
                    panel_state["project_status"].classes(add="text-red-600")
                    break

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
                    break

                await asyncio.sleep(5)

        except Exception as e:
            print(f"[ERROR] Pipeline monitor failed: {e}")
            panel_state["project_status"].set_text("Monitor Failed")
            panel_state["project_status"].classes(add="text-red-600")

        finally:
            shared_state["pipeline_running"] = False
            panel_state["stop_button"].props("disabled")
            panel_state["run_button"].props(remove="disabled").props(remove="loading")

    async def monitor_job_statuses():
        try:
            while shared_state["pipeline_running"] and shared_state["current_project_path"]:
                import random

                job_statuses = {}
                for jt in shared_state["selected_jobs"]:
                    if shared_state["job_cards"][jt]["status"] == "success":
                        job_statuses[jt.value] = "success"
                        continue
                    job_statuses[jt.value] = random.choice(["pending", "running", "success", "failed"])

                for job_type in shared_state["selected_jobs"]:
                    status = job_statuses.get(job_type.value, "pending")
                    if shared_state["job_cards"][job_type]["status"] != status:
                        shared_state["job_cards"][job_type]["status"] = status

                await asyncio.sleep(3)

        except Exception as e:
            print(f"[ERROR] Job status monitor failed: {e}")

        finally:
            for job_type in shared_state["selected_jobs"]:
                if shared_state["job_cards"][job_type]["status"] == "running":
                    shared_state["job_cards"][job_type]["status"] = "failed"

    async def monitor_all_jobs():
        last_content = {}
        await asyncio.sleep(1)

        while shared_state["pipeline_running"] and shared_state["current_project_path"]:
            try:
                for job_type in shared_state["selected_jobs"]:
                    card_data = shared_state["job_cards"].get(job_type)
                    if not card_data or "monitor" not in card_data or not card_data["monitor"]:
                        continue

                    monitor = card_data["monitor"]
                    job_index = card_data["job_index"]

                    logs = await backend.get_pipeline_job_logs(
                        shared_state["current_project_path"], job_type.value, str(job_index)
                    )

                    if job_type not in last_content:
                        last_content[job_type] = {"stdout": "", "stderr": ""}

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
                await asyncio.sleep(10)

    def get_job_param_snapshot(job_type):
        job_model = app_state.jobs.get(job_type.value)
        if job_model:
            return job_model.model_dump()
        return {}

    def update_job_card_sync_indicator(job_type):
        if "update_job_card_sync_indicator" in callbacks:
            callbacks["update_job_card_sync_indicator"](job_type)

    def refresh_all_job_parameter_displays():
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

    with (
        ui.column()
        .classes("w-full h-full overflow-y-auto")
        .style("padding: 10px; gap: 0px; font-family: 'IBM Plex Sans', sans-serif;")
    ):
        with ui.row().classes("w-full items-center justify-between mb-3"):
            ui.label("DATA IMPORT & PROJECT").classes("text-xs font-semibold text-black uppercase tracking-wider")
            with ui.row().style("gap: 8px;"):
                ui.button("Open Project", on_click=handle_open_project).props("dense flat no-caps").style(
                    "background: #e0f2fe; color: #0369a1; padding: 6px 16px; border-radius: 3px; font-weight: 500; border: 1px solid #bae6fd;"
                )
                panel_state["create_button"] = (
                    ui.button("Create Project", on_click=handle_create_project)
                    .props("dense flat no-caps")
                    .style(
                        "background: #f3f4f6; color: #1f2937; padding: 6px 16px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                    )
                )

        # Add the project status label here
        panel_state["project_status"] = ui.label("").classes("text-xs text-gray-600 mb-2").style("font-style: italic;")

        with ui.column().classes("w-full mb-6").style("gap: 10px;"):
            # Project Name and Location
            with ui.row().classes("w-full").style("gap: 10px;"):
                panel_state["project_name_input"] = ui.input("Project Name").props("dense outlined").classes("flex-1")
                panel_state["project_location_input"] = create_path_input_with_picker(
                    label="Location", mode="directory", default_value="/users/artem.kushner/dev/crboost_server/projects"
                ).classes("flex-1")

            # Movies
            panel_state["movies_path_input"] = create_path_input_with_picker(
                label="Movies",
                mode="directory",
                glob_pattern="*.eer",
                default_value="/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer",
            ).classes("w-full")

            # MDocs with detect button
            with ui.row().classes("w-full items-end").style("gap: 8px;"):
                panel_state["mdocs_path_input"] = create_path_input_with_picker(
                    label="MDocs",
                    mode="directory",
                    glob_pattern="*.mdoc",
                    default_value="/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc",
                ).classes("flex-1")

                ui.button(icon="settings", on_click=auto_detect_metadata).props("dense flat round").style(
                    "background: #f3f4f6; color: #6b7280; width: 32px; height: 32px;"
                ).tooltip("Auto-detect parameters")

        ui.label("MICROSCOPE & ACQUISITION").classes(
            "text-xs font-semibold text-black uppercase tracking-wider mb-3 mt-6"
        )

        with ui.column().classes("w-full mb-6").style("gap: 10px;"):
            with ui.grid(columns=3).classes("w-full").style("gap: 10px;"):
                pixel_size_input = (
                    ui.input(label="Pixel Size (Å)").props("dense outlined type=number step=0.01").classes("w-full")
                )
                voltage_input = ui.input(label="Voltage (kV)").props("dense outlined type=number").classes("w-full")
                cs_input = ui.input(label="Cs (mm)").props("dense outlined type=number step=0.1").classes("w-full")

                amplitude_contrast_input = (
                    ui.input(label="Amplitude Contrast").props("dense outlined type=number step=0.01").classes("w-full")
                )
                dose_per_tilt_input = (
                    ui.input(label="Dose/Tilt").props("dense outlined type=number step=0.1").classes("w-full")
                )
                tilt_axis_input = (
                    ui.input(label="Tilt Axis (°)").props("dense outlined type=number step=0.1").classes("w-full")
                )

                image_size_input = ui.input(label="Detector").props("dense outlined readonly").classes("w-full")
                eer_grouping_input = (
                    ui.input(label="EER Grouping").props("dense outlined type=number").classes("w-full")
                )
                target_dose_input = (
                    ui.input(label="Target Dose").props("dense outlined type=number step=0.01").classes("w-full")
                )

            eer_info_label = ui.label("").classes("text-xs text-gray-500 mt-1")

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
                image_size_input,
            ]
        )

        ui.label("SLURM CONFIGURATION").classes("text-xs font-semibold text-black uppercase tracking-wider mb-3 mt-6")

        slurm_inputs = build_slurm_job_config(backend, panel_state)
        panel_state["slurm_inputs"] = slurm_inputs

        with ui.expansion("Cluster Overview", icon="info").props("dense").classes("w-full mt-3"):
            with ui.column().classes("w-full gap-3 p-3"):
                overview_state = build_cluster_overview(backend, panel_state)
                panel_state["cluster_overview"] = overview_state

        async def delayed_slurm_init():
            await asyncio.sleep(0.5)
            try:
                partitions_result = await backend.slurm_service.get_slurm_partitions()
                if partitions_result.get("success"):
                    partitions = partitions_result["partitions"]
                    unique_partitions = {}
                    for p in partitions:
                        name = p["name"]
                        if name not in unique_partitions:
                            unique_partitions[name] = p

                    partition_names = sorted(unique_partitions.keys())
                    slurm_inputs["partition"].options = partition_names
                    if partition_names:
                        slurm_inputs["partition"].value = partition_names[0]

                if "refresh_cluster_data" in overview_state:
                    await overview_state["refresh_cluster_data"]()

                print("[INFO] SLURM components initialized")
            except Exception as e:
                print(f"[WARN] SLURM initialization failed: {e}")

        asyncio.create_task(delayed_slurm_init())
        dose_per_tilt_input.on_value_change(calculate_eer_grouping)
        target_dose_input.on_value_change(calculate_eer_grouping)
        update_image_size_display()

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

    return panel_state
