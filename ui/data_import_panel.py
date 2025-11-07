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
from services.parameter_models import JobType
from typing import Dict, Any


def build_data_import_panel(backend: CryoBoostBackend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
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

    async def handle_open_project():
        project_path_input = None

        with ui.dialog() as dialog, ui.card():
            ui.label("Open Existing Project").classes("text-lg font-semibold mb-4")
            project_path_input = ui.input("Project Path").props("dense outlined").classes("w-96")
            project_path_input.value = "/users/artem.kushner/dev/crboost_server/projects/"

            with ui.row().classes("w-full justify-end mt-4").style("gap: 8px;"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                ui.button("Load", on_click=lambda: load_project(project_path_input.value, dialog)).props("flat")

        dialog.open()

    async def load_project(project_path: str, dialog):
        if not project_path:
            ui.notify("Please enter a project path", type="warning")
            return

        dialog.close()
        ui.notify("Loading project...", type="info")

        result = await backend.load_existing_project(project_path)

        if not result.get("success"):
            ui.notify(f"Failed to load project: {result.get('error')}", type="negative")
            return

        # Update UI fields
        panel_state["project_name_input"].value = result["project_name"]
        panel_state["project_location_input"].value = str(Path(project_path).parent)
        panel_state["movies_path_input"].value = result.get("movies_glob", "")
        panel_state["mdocs_path_input"].value = result.get("mdocs_glob", "")

        # Load project params into app_state
        params_file = Path(project_path) / "project_params.json"
        if params_file.exists():
            with open(params_file, "r") as f:
                project_params = json.load(f)

            if "microscope" in project_params:
                for key, value in project_params["microscope"].items():
                    if hasattr(app_state.microscope, key):
                        setattr(app_state.microscope, key, value)

            if "acquisition" in project_params:
                for key, value in project_params["acquisition"].items():
                    if hasattr(app_state.acquisition, key):
                        setattr(app_state.acquisition, key, value)

            update_image_size_display()

        # Load saved job params snapshots for path display
        from services.parameter_models import jobtype_paramclass
        param_classes = jobtype_paramclass()
        
        for job_type_str in result.get("selected_jobs", []):
            job_type = JobType.from_string(job_type_str)
            param_class = param_classes.get(job_type)
            
            if not param_class:
                continue
            
            # Find the job params file - need to search since we don't know job number yet
            category = param_class.JOB_CATEGORY
            category_dir = Path(project_path) / category.value
            
            if category_dir.exists():
                # Find the job directory (there should only be one for each job type in a linear pipeline)
                job_dirs = sorted(category_dir.glob("job*"))
                if job_dirs:
                    params_file = job_dirs[0] / "job_params.json"
                    if params_file.exists():
                        try:
                            with open(params_file, "r") as f:
                                saved_params = json.load(f)
                            shared_state["params_snapshot"][job_type] = saved_params
                        except:
                            pass

        # Set project state
        shared_state["current_project_path"] = project_path
        shared_state["current_scheme_name"] = f"scheme_{result['project_name']}"
        shared_state["project_created"] = True
        shared_state["continuation_mode"] = True
        
        # Update selected jobs (these are already populated in app_state by backend)
        shared_state["selected_jobs"] = [JobType.from_string(j) for j in result.get("selected_jobs", [])]

        # Initialize job_cards with minimal UI state only
        for job_type in shared_state["selected_jobs"]:
            shared_state["job_cards"][job_type] = {
                "active_monitor_tab": "logs"  # Default to logs for loaded projects
            }

        # Lock UI
        panel_state["project_name_input"].disable()
        panel_state["project_location_input"].disable()
        panel_state["movies_path_input"].disable()
        panel_state["mdocs_path_input"].disable()
        panel_state["create_button"].disable()
        
        for el in panel_state["parameter_inputs"]:
            el.disable()

        # Update status display
        job_count = len(shared_state["selected_jobs"])
        if panel_state.get("project_status"):
            panel_state["project_status"].set_text(f"Loaded: {job_count} jobs")
        
        if panel_state.get("run_button"):
            panel_state["run_button"].props(remove="disabled")

        # Rebuild UI to show loaded jobs
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

            # Sync all job models from updated global state
            for job_type in shared_state["selected_jobs"]:
                job_model = app_state.jobs.get(job_type.value)
                if job_model and hasattr(job_model, "sync_from_pipeline_state"):
                    job_model.sync_from_pipeline_state(app_state)

            # Update data import panel inputs
            pixel_size_input.value = str(app_state.microscope.pixel_size_angstrom)
            dose_per_tilt_input.value = str(app_state.acquisition.dose_per_tilt)
            tilt_axis_input.value = str(app_state.acquisition.tilt_axis_degrees)
            update_image_size_display()

            # CRITICAL: Rebuild pipeline UI so job parameter inputs show the synced values
            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()

            ui.notify("Parameters detected and synced to all jobs", type="positive")

        except Exception as e:
            ui.notify(f"Detection failed: {e}", type="negative")

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
        except:
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
            job_model = app_state.jobs.get(job_type.value)
            if job_model:
                shared_state["params_snapshot"][job_type] = job_model.model_dump()

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
            
            if "enable_run_button" in callbacks:
                callbacks["enable_run_button"]()

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

    def update_image_size_display():
        dims = app_state.acquisition.detector_dimensions
        if image_size_input:
            image_size_input.set_value(f"{dims[0]}x{dims[1]}")

    with ui.column().classes("w-full h-full overflow-y-auto").style(
        "padding: 10px; gap: 0px; font-family: 'IBM Plex Sans', sans-serif;"
    ):
        with ui.row().classes("w-full items-center justify-between mb-3"):
            ui.label("DATA IMPORT & PROJECT").classes("text-xs font-semibold text-black uppercase tracking-wider")
            with ui.row().style("gap: 8px;"):
                ui.button("Open Project", on_click=handle_open_project).props("dense flat no-caps").style(
                    "background: #e0f2fe; color: #0369a1; padding: 6px 16px; border-radius: 3px; font-weight: 500; border: 1px solid #bae6fd;"
                )
                panel_state["create_button"] = ui.button(
                    "Create Project", on_click=handle_create_project
                ).props("dense flat no-caps").style(
                    "background: #f3f4f6; color: #1f2937; padding: 6px 16px; border-radius: 3px; font-weight: 500; border: 1px solid #e5e7eb;"
                )

        panel_state["project_status"] = ui.label("").classes("text-xs text-gray-600 mb-2").style("font-style: italic;")

        with ui.column().classes("w-full mb-6").style("gap: 10px;"):
            with ui.row().classes("w-full").style("gap: 10px;"):
                panel_state["project_name_input"] = ui.input("Project Name").props("dense outlined").classes("flex-1")
                panel_state["project_location_input"] = create_path_input_with_picker(
                    label="Location", mode="directory", default_value="/users/artem.kushner/dev/crboost_server/projects"
                ).classes("flex-1")

            panel_state["movies_path_input"] = create_path_input_with_picker(
                label="Movies",
                mode="directory",
                glob_pattern="*.eer",
                default_value="/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer",
            ).classes("w-full")

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
                pixel_size_input = ui.input(label="Pixel Size (Å)").props(
                    "dense outlined type=number step=0.01"
                ).classes("w-full")
                voltage_input = ui.input(label="Voltage (kV)").props("dense outlined type=number").classes("w-full")
                cs_input = ui.input(label="Cs (mm)").props("dense outlined type=number step=0.1").classes("w-full")

                amplitude_contrast_input = ui.input(label="Amplitude Contrast").props(
                    "dense outlined type=number step=0.01"
                ).classes("w-full")
                dose_per_tilt_input = ui.input(label="Dose/Tilt").props(
                    "dense outlined type=number step=0.1"
                ).classes("w-full")
                tilt_axis_input = ui.input(label="Tilt Axis (°)").props(
                    "dense outlined type=number step=0.1"
                ).classes("w-full")

                image_size_input = ui.input(label="Detector").props("dense outlined readonly").classes("w-full")
                eer_grouping_input = ui.input(label="EER Grouping").props("dense outlined type=number").classes(
                    "w-full"
                )
                target_dose_input = ui.input(label="Target Dose").props(
                    "dense outlined type=number step=0.01"
                ).classes("w-full")

            eer_info_label = ui.label("").classes("text-xs text-gray-500 mt-1")

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
                    unique_partitions = {p["name"]: p for p in partitions}
                    partition_names = sorted(unique_partitions.keys())
                    slurm_inputs["partition"].options = partition_names
                    if partition_names:
                        slurm_inputs["partition"].value = partition_names[0]

                if "refresh_cluster_data" in overview_state:
                    await overview_state["refresh_cluster_data"]()
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
                
                if "update_job_card_sync_indicator" in callbacks:
                    for job_type in shared_state["selected_jobs"]:
                        callbacks["update_job_card_sync_indicator"](job_type)

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
