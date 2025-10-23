import asyncio
import glob
import math
from nicegui import ui
from backend import CryoBoostBackend
from models import User
from ui.utils import create_path_input_with_picker
from typing import Dict, Any, Callable


def build_projects_tab(backend: CryoBoostBackend, user: User):
    """Projects tab - project management and pipeline execution"""
    state = {
        "selected_jobs": [],
        "current_project_path": None,
        "current_scheme_name": None,
    }
    async def load_page_data():
        """Asynchronously load initial jobs and parameters."""
        print("--- [DEBUG] load_page_data: STARTING ---") # <-- ADD
        try:
            print("--- [DEBUG] load_page_data: Awaiting _load_initial_params... ---") # <-- ADD
            await _load_initial_params()
            print("--- [DEBUG] load_page_data: FINISHED _load_initial_params. ---") # <-- ADD
            
            print("--- [DEBUG] load_page_data: Awaiting _load_available_jobs... ---") # <-- ADD
            await _load_available_jobs()
            print("--- [DEBUG] load_page_data: FINISHED _load_available_jobs. ---") # <-- ADD
            
        except Exception as e:
            print(f"--- [DEBUG] CRITICAL ERROR in load_page_data: {e} ---") # <-- ADD
            
        print("--- [DEBUG] load_page_data: COMPLETED ---") # <-- ADD
    # Store references to log displays
    job_log_displays = {}
    async def _load_initial_params():
            """Fetch default parameters from the backend and populate the UI."""
            print("[UI] Loading initial parameters...")
            try:
                initial_state_dict = await backend.get_initial_parameters()
                await _update_ui_from_state(initial_state_dict)
            except Exception as e:
                print(f"[ERROR] Failed to load initial parameters: {e}")
                ui.notify("Could not load default parameters from backend", type='negative')

    async def _update_ui_from_state(state_dict: Dict[str, Any]):
            """Populates all UI inputs from a PipelineState dict"""
            print("[UI] Updating UI from state...")
            try:
                # Microscope
                pixel_size_input.set_value(state_dict['pixel_size_angstrom']['value'])
                voltage_input.set_value(state_dict['acceleration_voltage_kv']['value'])
                cs_input.set_value(state_dict['spherical_aberration_mm']['value'])
                amplitude_contrast_input.set_value(state_dict['amplitude_contrast']['value'])
                
                # Acquisition
                dose_per_tilt_input.set_value(state_dict['dose_per_tilt']['value'])
                dims = state_dict['detector_dimensions']['value']
                image_size_input.set_value(f"{dims[0]}x{dims[1]}")
                tilt_axis_input.set_value(state_dict['tilt_axis_degrees']['value'])
                
                # Processing
                rec_binning_input.set_value(state_dict['reconstruction_binning']['value'])
                sample_thickness_input.set_value(state_dict['sample_thickness_nm']['value'])
                if state_dict.get('eer_fractions_per_frame') and state_dict['eer_fractions_per_frame']['value']:
                    eer_grouping_input.set_value(state_dict['eer_fractions_per_frame']['value'])
                
                alignment_method_select.set_value(state_dict['alignment_method']['value'])
                
                # Computing
                # (We could add these later if needed)
                
                ui.notify("Parameters loaded", type='positive', timeout=1000)
            except Exception as e:
                print(f"[ERROR] Failed to update UI from state: {e}")
                ui.notify(f"Failed to load all parameters: {e}", type='negative')


    async def on_param_change(param_name: str, value: Any, cast_func: Callable):
            """Generic handler to update backend on UI change"""
            try:
                casted_value = cast_func(value)
                await backend.update_parameter({
                    "param_name": param_name,
                    "value": casted_value
                })
            except (ValueError, TypeError) as e:
                print(f"[UI] Invalid input for {param_name}: {value}. Error: {e}")
                # Optionally show a UI error
            except Exception as e:
                print(f"[UI] Error updating {param_name}: {e}")


    async def auto_detect_metadata():
            movies_path = movies_path_input.value
            mdocs_path = mdocs_path_input.value

            if not movies_path or not mdocs_path:
                ui.notify("Please provide both movies and mdoc paths first", type='warning')
                return

            detection_status.set_text("Detecting metadata...")
            
            # Call backend to parse mdoc and update state
            try:
                updated_state_dict = await backend.autodetect_parameters(mdocs_path)
                # Repopulate UI with new state
                await _update_ui_from_state(updated_state_dict)
                ui.notify("MDOC metadata detected and applied!", type='positive')
                
                # Update local state for EER calculation
                state["auto_detected_values"]["pixel_size"] = updated_state_dict['pixel_size_angstrom']['value']
                state["auto_detected_values"]["dose_per_tilt"] = updated_state_dict['dose_per_tilt']['value']
                dims = updated_state_dict['detector_dimensions']['value']
                state["auto_detected_values"]["image_size"] = f"{dims[0]}x{dims[1]}"
                
            except Exception as e:
                ui.notify(f"Error reading mdoc: {e}", type='negative')
                detection_status.set_text("Metadata detection failed")
                return

            # EER frame analysis (this can stay as it calls backend)
            eer_files = glob.glob(movies_path)
            if eer_files and eer_files[0].endswith('.eer'):
                try:
                    frames_per_tilt = await backend.get_eer_frames_per_tilt(eer_files[0])
                    if frames_per_tilt:
                        state["auto_detected_values"]["frames_per_tilt"] = frames_per_tilt
                        
                        # Auto-calculate EER grouping if dose is available
                        if dose_per_tilt_input.value:
                            total_dose = float(dose_per_tilt_input.value)
                            target_dose_per_frame = 0.3 # Default
                            try:
                                target_dose_per_frame = float(target_dose_input.value)
                            except: pass # Use default
                            
                            dose_per_frame = total_dose / frames_per_tilt
                            num_frames_to_group = math.floor(target_dose_per_frame / dose_per_frame)
                            
                            if num_frames_to_group > 0:
                                eer_grouping_input.set_value(str(num_frames_to_group))
                                # This will trigger its on_change handler
                                
                        ui.notify(f"Detected {frames_per_tilt} frames per tilt", type='positive')
                except Exception as e:
                    ui.notify(f"Error analyzing EER: {e}", type='negative')

            detection_status.set_text("Metadata detection complete")



    def calculate_eer_grouping():
            if not dose_per_tilt_input.value or not eer_grouping_input.value:
                return
            try:
                total_dose = float(dose_per_tilt_input.value)
                current_grouping = int(eer_grouping_input.value)
                frames_per_tilt = state["auto_detected_values"].get("frames_per_tilt", 40) # Default 40

                dose_per_rendered_frame = (total_dose / frames_per_tilt) * current_grouping
                rendered_frames = math.floor(frames_per_tilt / current_grouping)
                lost_frames = frames_per_tilt - (rendered_frames * current_grouping)

                eer_info_label.set_text(
                    f"Grouping: {current_grouping} -> {rendered_frames} frames, "
                    f"{lost_frames} lost ({lost_frames / frames_per_tilt * 100:.1f}%) | "
                    f"Dose per frame: {dose_per_rendered_frame:.2f} e⁻/Å²"
                )
                # Also update the backend state
                asyncio.create_task(on_param_change('eer_fractions_per_frame', current_grouping, int))
            except Exception as e:
                print(f"Error calculating EER grouping: {e}")



    async def _load_available_jobs():
        print("--- [DEBUG] _load_available_jobs: Fetching from backend... ---") # <-- ADD
        job_types = await backend.get_available_jobs()
        print(f"--- [DEBUG] _load_available_jobs: Got jobs: {job_types} ---") # <-- ADD
        job_selector.options = job_types
        job_selector.update()
        print("--- [DEBUG] _load_available_jobs: Dropdown updated. ---") # <-- ADD

    def remove_job(job_name: str, row: ui.element):
        state["selected_jobs"].remove(job_name)
        row.delete()
        job_status_label.set_text('No jobs added yet.' if not state["selected_jobs"] else 'Current pipeline:')
        ui.notify(f"Removed '{job_name}'", type='info')

    def handle_add_job():
        job_name = job_selector.value
        if not job_name or job_name in state["selected_jobs"]:
            return
        state["selected_jobs"].append(job_name)
        if len(state["selected_jobs"]) == 1:
            job_status_label.set_text('Current pipeline:')
        with selected_jobs_container:
            with ui.row().classes('w-full items-center justify-between bg-gray-200 p-1 rounded') as row:
                ui.label(job_name).classes('text-xs')
                ui.button(icon='delete', on_click=lambda: remove_job(job_name, row)).props(
                    'flat round dense text-red-500')
        job_selector.set_value(None)

    async def handle_create_project():
            name = project_name_input.value
            project_location = project_location_input.value
            movies_glob = movies_path_input.value
            mdocs_glob = mdocs_path_input.value

            if not all([name, project_location, movies_glob, mdocs_glob, state["selected_jobs"]]):
                ui.notify("Project name, project location, data paths, and at least one job are required.", type='negative')
                return

            # Parameters are already synced to backend via on_change handlers
            # The backend will call parameter_manager.get_legacy_user_params_dict()
            
            create_button.props('loading')

            result = await backend.create_project_and_scheme(
                project_name=name,
                project_base_path=project_location,
                selected_jobs=state["selected_jobs"],
                movies_glob=movies_glob,
                mdocs_glob=mdocs_glob
            )

            create_button.props(remove='loading')
            if result.get("success"):
                state["current_project_path"] = result["project_path"]
                state["current_scheme_name"] = f"scheme_{name}"
                ui.notify(result["message"], type='positive')
                active_project_label.set_text(name)
                pipeline_status.set_text("Project created. Ready to run.")
                run_button.props(remove='disabled')

                # Disable project creation fields
                project_name_input.disable()
                project_location_input.disable()
                movies_path_input.disable()
                mdocs_path_input.disable()
                create_button.disable()
                
                # Disable parameter fields
                for el in parameter_inputs:
                    el.disable()

            else:
                ui.notify(f"Error: {result.get('error', 'Unknown')}", type='negative')




    async def _monitor_pipeline_progress():
        while state["current_project_path"] and not stop_button.props.get('disabled'):
            progress = await backend.get_pipeline_progress(state["current_project_path"])
            if not progress or progress.get('status') != 'ok':
                break
            total, completed, running, failed = progress.get('total', 0), progress.get('completed', 0), progress.get(
                'running', 0), progress.get('failed', 0)
            if total > 0:
                progress_bar.value = completed / total
                progress_message.text = f"Progress: {completed}/{total} completed ({running} running, {failed} failed)"
            if progress.get('is_complete') and total > 0:
                msg = f"Pipeline finished with {failed} failures." if failed > 0 else "Pipeline completed successfully."

                pipeline_status.set_text(msg)
                if failed > 0:
                    pipeline_status.classes(add='text-red-500', remove='text-green-500')
                else:
                    pipeline_status.classes(add='text-green-500', remove='text-red-500')

                stop_button.props('disabled')
                run_button.props(remove='disabled')
                break
            await asyncio.sleep(5)
        print("[UI] Pipeline monitoring stopped.")



    async def create_pipeline_job_tabs():
            """Create tabs for each pipeline job"""
            if not state["current_project_path"] or not state["selected_jobs"]:
                return
                
            # Clear existing tabs
            pipeline_job_tabs.clear()
            pipeline_job_panels.clear()
            job_log_displays.clear()
            
            # Create a tab for each job
            for idx, job_name in enumerate(state["selected_jobs"], 1):
                with pipeline_job_tabs:
                    tab = ui.tab(f'{job_name} (job{idx:03d})')
                
                with pipeline_job_panels:
                    with ui.tab_panel(tab).classes('p-2'):
                        # Create split view for stdout and stderr
                        with ui.grid(columns=2).classes('w-full gap-2'):
                            # Stdout column
                            with ui.column().classes('w-full'):
                                with ui.row().classes('w-full justify-between items-center mb-1'):
                                    ui.label('run.out (stdout)').classes('text-xs font-medium')
                                    refresh_btn_out = ui.button(icon='refresh').props('flat round dense size=xs')
                                out_log = ui.log(max_lines=500).classes(
                                    'w-full h-64 border rounded-md bg-gray-50 p-2 text-xs font-mono'
                                )
                            
                            # Stderr column  
                            with ui.column().classes('w-full'):
                                with ui.row().classes('w-full justify-between items-center mb-1'):
                                    ui.label('run.err (stderr)').classes('text-xs font-medium')
                                    refresh_btn_err = ui.button(icon='refresh').props('flat round dense size=xs')
                                err_log = ui.log(max_lines=500).classes(
                                    'w-full h-64 border rounded-md bg-red-50 p-2 text-xs font-mono'
                                )
                        
                        # Store references
                        job_log_displays[job_name] = {
                            'stdout': out_log,
                            'stderr': err_log,
                            'idx': idx
                        }
                        
                        # Set up refresh button handlers
                        async def refresh_this_job(job=job_name, idx=idx):
                            await refresh_job_logs(job, idx)
                        
                        refresh_btn_out.on('click', refresh_this_job)
                        refresh_btn_err.on('click', refresh_this_job)
            
            # Select first tab
            if state["selected_jobs"]:
                pipeline_job_tabs.set_value(f'{state["selected_jobs"][0]} (job001)')





    async def refresh_job_logs(job_name: str, job_idx: int):
            """Refresh logs for a specific job"""
            if not state["current_project_path"]:
                return
                
            logs = await backend.get_pipeline_job_logs(
                state["current_project_path"], 
                job_name, 
                str(job_idx)
            )
            
            if job_name in job_log_displays:
                display = job_log_displays[job_name]
                
                # Update stdout
                display['stdout'].clear()
                if logs['stdout']:
                    display['stdout'].push(logs['stdout'])
                else:
                    display['stdout'].push("No output yet...")
                
                # Update stderr
                display['stderr'].clear() 
                if logs['stderr']:
                    display['stderr'].push(logs['stderr'])
                else:
                    display['stderr'].push("No errors...")
                
                ui.notify(f"Refreshed logs for {job_name}", type='positive', timeout=1000)

    async def auto_refresh_all_logs():
        """Auto-refresh all job logs periodically"""
        # Track last content to avoid unnecessary updates
        last_content = {}
        
        while state["current_project_path"] and job_log_displays:
            for job_name, display in job_log_displays.items():
                logs = await backend.get_pipeline_job_logs(
                    state["current_project_path"],
                    job_name,
                    str(display['idx'])
                )
                
                # Initialize tracking for this job if needed
                if job_name not in last_content:
                    last_content[job_name] = {'stdout': '', 'stderr': ''}
                
                # Update stdout if changed
                if logs['stdout'] != last_content[job_name]['stdout']:
                    display['stdout'].clear()
                    display['stdout'].push(logs['stdout'] or "No output yet...")
                    last_content[job_name]['stdout'] = logs['stdout']
                
                # Update stderr if changed
                if logs['stderr'] != last_content[job_name]['stderr']:
                    display['stderr'].clear()
                    display['stderr'].push(logs['stderr'] or "No errors...")
                    last_content[job_name]['stderr'] = logs['stderr']
            
            await asyncio.sleep(5)

            
            
    async def handle_run_pipeline():
            pipeline_status.classes(remove='text-red-500 text-green-500')
            run_button.props('loading')
            pipeline_status.set_text("Starting pipeline...")
            progress_bar.classes(remove='hidden').value = 0
            progress_message.classes(remove='hidden').set_text("Pipeline is starting...")

            required_paths = [
                project_location_input.value,
                movies_path_input.value,
                mdocs_path_input.value,
            ]

            result = await backend.start_pipeline(
                project_path=state["current_project_path"],
                scheme_name=state["current_scheme_name"],
                selected_jobs=state["selected_jobs"],
                required_paths=required_paths
            )
            run_button.props(remove='loading')
            if result.get("success"):
                pid = result.get('pid', 'N/A')
                ui.notify(f"Pipeline started successfully! (PID: {pid})", type="positive")
                pipeline_status.set_text(f"Pipeline running (PID: {pid})")
                run_button.props('disabled')
                stop_button.props(remove='disabled')
                
                # Create the job monitoring tabs after pipeline starts
                await create_pipeline_job_tabs()
                # Start auto-refresh
                asyncio.create_task(auto_refresh_all_logs())
                # Start monitoring progress
                asyncio.create_task(_monitor_pipeline_progress())

            else:
                pipeline_status.set_text(f"Failed to start: {result.get('error', 'Unknown')}")
                ui.notify(pipeline_status.text, type='negative')
                progress_bar.classes('hidden')
                progress_message.classes('hidden')

    async def handle_stop_pipeline():
        ui.notify("Stop functionality not fully implemented yet.", type="warning")
        pipeline_status.set_text("Pipeline stopped by user.")
        stop_button.props('disabled')
        run_button.props(remove='disabled')
        progress_bar.classes('hidden')
        progress_message.classes('hidden')




    # -------- UI

    parameter_inputs = []
    
    with ui.column().classes('w-full gap-3'):
        
        # --- Data Import Section (from data_parameters_tab) ---
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('1. Data Import Configuration').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3'):
                movies_path_input = create_path_input_with_picker(
                    label='Movie Files (EER/TIF)',
                    mode='directory',
                    glob_pattern='*.eer',
                    default_value='/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer'
                )
                mdocs_path_input = create_path_input_with_picker(
                    label='MDOC Files',
                    mode='directory',
                    glob_pattern='*.mdoc',
                    default_value='/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc'
                )

            with ui.row().classes('w-full justify-between items-center mt-2'):
                ui.button('Auto-detect Metadata', on_click=auto_detect_metadata, icon='auto_fix_high').props(
                    'outline dense')
                detection_status = ui.label('Ready to detect metadata').classes('text-xs text-gray-600')

        # --- Microscope Parameters Section (from data_parameters_tab) ---
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('2. Microscope Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-3'):
                pixel_size_input = ui.input(label='Pixel Size (Å)', placeholder='1.35').props(
                    'dense outlined type=number step=0.01')
                voltage_input = ui.input(label='Voltage (kV)', placeholder='300').props('dense outlined type=number')
                cs_input = ui.input(label='Spherical Aberration (mm)', placeholder='2.7').props(
                    'dense outlined type=number step=0.1')
            
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-3 mt-2'):
                amplitude_contrast_input = ui.input(label='Amplitude Contrast', placeholder='0.1').props(
                    'dense outlined type=number step=0.01')
                dose_per_tilt_input = ui.input(label='Dose per Tilt (e⁻/Å²)', placeholder='3.0').props(
                    'dense outlined type=number step=0.1')
                
            parameter_inputs.extend([
                pixel_size_input, voltage_input, cs_input, 
                amplitude_contrast_input, dose_per_tilt_input
            ])

        # --- Tilt Series Parameters Section (from data_parameters_tab) ---
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('3. Tilt Series Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3'):
                tilt_axis_input = ui.input(label='Tilt Axis Angle (°)', placeholder='-95.0').props(
                    'dense outlined type=number step=0.1')
                image_size_input = ui.input(label='Image Size (WxH)', placeholder='4096x4096').props('dense outlined readonly') # Readonly, set by mdoc
            
            with ui.expansion('EER Processing Settings', icon='movie_filter').classes('w-full mt-2 text-xs'):
                with ui.column().classes('w-full gap-3 p-3 bg-gray-50 rounded-lg'):
                    with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-2'):
                        eer_grouping_input = ui.input(label='EER Fractions per Frame', placeholder='32').props(
                            'dense outlined type=number')
                        target_dose_input = ui.input(label='Target Dose per Frame (e⁻/Å²)',
                                                    placeholder='0.3').props('dense outlined type=number step=0.01')
                    eer_info_label = ui.label('Grouping calculation will appear here').classes('text-xs text-blue-600 h-4')
                    ui.button('Recalculate Grouping', on_click=calculate_eer_grouping, icon='calculate').props(
                        'outline dense')

            parameter_inputs.extend([
                tilt_axis_input, image_size_input, eer_grouping_input, target_dose_input
            ])

        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('4. Reconstruction Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-3'):
                rec_binning_input = ui.input(label='Recon. Binning Factor', placeholder='4').props(
                    'dense outlined type=number step=1')
                tomogram_size_input = ui.input(label='Tomogram Size (XYZ)', placeholder='1024x1024x512').props(
                    'dense outlined') # TODO: Hook up
                sample_thickness_input = ui.input(label='Sample Thickness (nm)', placeholder='300').props(
                    'dense outlined type=number')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3 mt-2'):
                alignment_method_select = ui.select(label='Alignment Method', options=['AreTomo', 'IMOD', 'Relion'],
                                                    value='AreTomo').props('dense outlined')
                patch_size_input = ui.input(label='Patch Size (Alignment)', placeholder='800').props(
                    'dense outlined type=number') # TODO: Hook up
            
            parameter_inputs.extend([
                rec_binning_input, tomogram_size_input, sample_thickness_input,
                alignment_method_select, patch_size_input
            ])
            
        # --- Project Creation Section (Original projects_tab) ---
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('5. Configure Project and Pipeline').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.column().classes('w-full gap-2'):
                project_name_input = ui.input('Project Name', placeholder='e.g., my_first_dataset').props('dense outlined')
                project_location_input = create_path_input_with_picker(
                    label='Project Location',
                    mode='directory',
                    default_value='/users/artem.kushner/dev/crboost_server/projects'
                )

                job_status_label = ui.label('No jobs added yet.').classes('text-xs text-gray-600 mt-2')
                with ui.expansion('Add Job to Pipeline', icon='add').classes('w-full text-xs'):
                    with ui.row().classes('w-full items-center gap-2'):
                        job_selector = ui.select(label='Select job type', options=[]).classes('flex-grow').props(
                            'dense outlined')
                        ui.button('ADD', on_click=handle_add_job).props('dense')
                selected_jobs_container = ui.column().classes('w-full mt-1 gap-1')
                create_button = ui.button('CREATE PROJECT', on_click=handle_create_project).props('dense color=primary')

        ui.separator()

        # --- Pipeline Execution Section (Original projects_tab) ---
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('6. Schedule and Execute Pipeline').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.column().classes('w-full gap-2'):
                with ui.row():
                    ui.label('Active Project:').classes('text-xs font-medium mr-2')
                    active_project_label = ui.label('No active project').classes('text-xs')
                pipeline_status = ui.label('Create and configure a project first.').classes('text-xs text-gray-600 my-1')
                with ui.row().classes('gap-2'):
                    run_button = ui.button('RUN', on_click=handle_run_pipeline, icon='play_arrow').props('disabled dense')
                    stop_button = ui.button('STOP', on_click=handle_stop_pipeline, icon='stop').props('disabled dense')
                progress_bar = ui.linear_progress(value=0, show_value=False).classes('hidden w-full')
                progress_message = ui.label('').classes('text-xs text-gray-600 hidden')

        ui.separator()
        
        # --- Pipeline Monitoring Section (Original projects_tab) ---
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('7. Pipeline Job Monitoring').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            pipeline_job_tabs = ui.tabs().classes('w-full')
            pipeline_job_panels = ui.tab_panels(pipeline_job_tabs).classes('w-full mt-2')

    pixel_size_input.on('change', lambda e: on_param_change('pixel_size_angstrom', e.value, float))
    voltage_input.on('change', lambda e: on_param_change('acceleration_voltage_kv', e.value, float))
    cs_input.on('change', lambda e: on_param_change('spherical_aberration_mm', e.value, float))
    amplitude_contrast_input.on('change', lambda e: on_param_change('amplitude_contrast', e.value, float))
    dose_per_tilt_input.on('change', lambda e: on_param_change('dose_per_tilt', e.value, float))
    tilt_axis_input.on('change', lambda e: on_param_change('tilt_axis_degrees', e.value, float))
    rec_binning_input.on('change', lambda e: on_param_change('reconstruction_binning', e.value, int))
    sample_thickness_input.on('change', lambda e: on_param_change('sample_thickness_nm', e.value, float))
    alignment_method_select.on('change', lambda e: on_param_change('alignment_method', e.value, str))
    
    # EER inputs
    dose_per_tilt_input.on('change', calculate_eer_grouping)
    eer_grouping_input.on('change', calculate_eer_grouping)
    target_dose_input.on('change', calculate_eer_grouping) # Recalculate if target changes
    
    
    # Initialize the state dict for auto-detection
    state["auto_detected_values"] = {}
    
    # Return the loader function so main_ui.py can call it
    return load_page_data