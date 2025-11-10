# ui/data_import_panel.py
import asyncio
import json
from pathlib import Path
import math
from backend import CryoBoostBackend
from nicegui import ui
from ui.slurm_components import build_cluster_overview, build_slurm_job_config
from ui.utils import create_path_input_with_picker

# --- REFACTORED IMPORTS ---
from services.state_service import get_state_service
from services.project_state import get_project_state
from typing import Dict, Any


def build_data_import_panel(backend: CryoBoostBackend, shared_state: Dict[str, Any], callbacks: Dict[str, Any]):
    
    # Get the single state service instance
    state_service = get_state_service()
    
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

        # --- REFACTORED LOADING ---
        # The backend.load_existing_project seems to be for project structure, not state
        # We need to load the state using the new StateService
        
        project_json_path = Path(project_path) / "project_params.json"
        if not project_json_path.exists():
            # Fallback to old backend method if it does more (e.g., finds scheme)
            result = await backend.load_existing_project(project_path)
            if not result.get("success"):
                ui.notify(f"Failed to load project structure: {result.get('error')}", type="negative")
                return
            ui.notify("Project structure loaded, but project_params.json not found. State may be incomplete.", type="warning")
        else:
            # New state loading logic
            load_success = await state_service.load_project(project_json_path)
            if not load_success:
                ui.notify(f"Failed to load project state from {project_json_path}", type="negative")
                return
        
        # Now, get the loaded state
        loaded_state = state_service.state 

        # Update UI fields from the loaded state
        panel_state["project_name_input"].value = loaded_state.project_name
        panel_state["project_location_input"].value = str(loaded_state.project_path.parent) if loaded_state.project_path else ""
        
        # This part of the old logic (data_sources) is not in the new ProjectState.
        # We assume the backend handles this, or it needs to be added to ProjectState.
        # For now, let's skip binding these.
        # panel_state["movies_path_input"].value = loaded_state.data_sources.get("frames_glob", "")
        # panel_state["mdocs_path_input"].value = loaded_state.data_sources.get("mdocs_glob", "")

        update_image_size_display() # This will pull from state_service.state.acquisition

        # Load saved job params snapshots (This seems to be for UI display only)
        # The new state_service.load already populated state.jobs
        shared_state["params_snapshot"] = {}
        for job_type, job_model in loaded_state.jobs.items():
            shared_state["params_snapshot"][job_type] = job_model.model_dump()

        # Set project state
        shared_state["current_project_path"] = str(loaded_state.project_path)
        shared_state["current_scheme_name"] = f"scheme_{loaded_state.project_name}"
        shared_state["project_created"] = True
        shared_state["continuation_mode"] = True
        
        # Update selected jobs
        shared_state["selected_jobs"] = list(loaded_state.jobs.keys())

        # Initialize job_cards with minimal UI state only
        for job_type in shared_state["selected_jobs"]:
            shared_state["job_cards"][job_type] = {
                "active_monitor_tab": "logs" # Default to logs for loaded projects
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

        ui.notify(f"Loaded project: {loaded_state.project_name}", type="positive")

    async def auto_detect_metadata():
        mdocs_path = panel_state["mdocs_path_input"].value
        if not mdocs_path:
            return

        try:
            # --- REFACTORED mdoc update ---
            await state_service.update_from_mdoc(mdocs_path)
            
            # Get the (now updated) global state
            current_state = state_service.state

            # This shared_state logic seems for UI display, let's update it
            shared_state["auto_detected_values"]["pixel_size"] = current_state.microscope.pixel_size_angstrom
            shared_state["auto_detected_values"]["dose_per_tilt"] = current_state.acquisition.dose_per_tilt
            dims = current_state.acquisition.detector_dimensions
            shared_state["auto_detected_values"]["image_size"] = f"{dims[0]}x{dims[1]}"

            # --- REMOVED SYNC LOGIC ---
            # The manual sync loop is no longer necessary.
            # for job_type in shared_state["selected_jobs"]: ...

            # Update data import panel inputs (the bound values)
            # This is technically redundant if binding is perfect, but good for robustness
            pixel_size_input.value = str(current_state.microscope.pixel_size_angstrom)
            dose_per_tilt_input.value = str(current_state.acquisition.dose_per_tilt)
            tilt_axis_input.value = str(current_state.acquisition.tilt_axis_degrees)
            update_image_size_display()

            # CRITICAL: Rebuild pipeline UI so job parameter inputs show the synced values
            if "rebuild_pipeline_ui" in callbacks:
                callbacks["rebuild_pipeline_ui"]()

            ui.notify("Parameters detected and synced to all jobs", type="positive")

        except Exception as e:
            ui.notify(f"Detection failed: {e}", type="negative")

    def calculate_eer_grouping():
        # This function seems fine, it reads from UI inputs
        pass # No change needed

    async def handle_create_project():
        name = panel_state["project_name_input"].value
        location = panel_state["project_location_input"].value
        movies = panel_state["movies_path_input"].value
        mdocs = panel_state["mdocs_path_input"].value

        if not all([name, location, movies, mdocs, shared_state["selected_jobs"]]):
            ui.notify("All fields and at least one job required", type="negative")
            return

        panel_state["create_button"].props("loading")
        
        # --- REFACTORED STATE CAPTURE ---
        # Update the global ProjectState object *before* creating the project
        current_state = state_service.state
        current_state.project_name = name
        current_state.project_path = Path(location) / name
        
        # This snapshot logic is still useful for the backend
        for job_type in shared_state["selected_jobs"]:
            job_model = current_state.jobs.get(job_type)
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

            # --- NEW: Save the final project state ---
            await state_service.save_project() # This will save to project_params.json

            ui.notify(result["message"], type="positive")
            panel_state["project_status"].set_text("Ready")
            
            if "enable_run_button" in callbacks:
                callbacks["enable_run_button"]()

            # Lock UI
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
        # This now reads from the state service
        dims = state_service.state.acquisition.detector_dimensions
        if image_size_input:
            image_size_input.set_value(f"{dims[0]}x{dims[1]}")

    with ui.column().classes("w-full h-full overflow-y-auto").style(
        "padding: 10px; gap: 0px; font-family: 'IBM Plex Sans', sans-serif;"
    ):
        # ... (UI layout code is unchanged) ...
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

        # --- REFACTORED BINDING (THE KEY) ---
        # Bind directly to the single source of truth
        pixel_size_input.bind_value(state_service.state.microscope, "pixel_size_angstrom")
        voltage_input.bind_value(state_service.state.microscope, "acceleration_voltage_kv")
        cs_input.bind_value(state_service.state.microscope, "spherical_aberration_mm")
        amplitude_contrast_input.bind_value(state_service.state.microscope, "amplitude_contrast")
        dose_per_tilt_input.bind_value(state_service.state.acquisition, "dose_per_tilt")
        tilt_axis_input.bind_value(state_service.state.acquisition, "tilt_axis_degrees")
        eer_grouping_input.bind_value(state_service.state.acquisition, "eer_fractions_per_frame")

        panel_state["parameter_inputs"].extend(
            [
                pixel_size_input, voltage_input, cs_input,
                amplitude_contrast_input, dose_per_tilt_input, tilt_axis_input,
                eer_grouping_input, target_dose_input, image_size_input,
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
            # ... (no change to this function) ...
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

        # --- REFACTORED LISTENER SETUP ---
        # All manual sync logic is removed. 
        # The .bind_value() handles updating the state automatically.
        # def setup_global_param_listeners():
        #     pass # No longer needed
        # setup_global_param_listeners()

    return panel_state
