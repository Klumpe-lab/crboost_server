# ui/projects_tab.py - COMPACT VERSION

import asyncio
import glob
import math
from pathlib import Path
from nicegui import ui
from backend import CryoBoostBackend
from ui.utils import create_path_input_with_picker
from typing import Dict, Any, Callable


def build_projects_tab(backend: CryoBoostBackend):
    """Projects tab - ultra-compact UI"""
    state = {
        "selected_jobs": [],
        "current_project_path": None,
        "current_scheme_name": None,
        "auto_detected_values": {},
        "job_param_tabs": {}
    }
    
    job_log_displays = {}
    
    async def load_page_data():
        try:
            await _load_initial_params()
            await _load_available_jobs()
        except Exception as e:
            print(f"[ERROR] load_page_data: {e}")
    
    async def _load_initial_params():
        try:
            initial_state_dict = await backend.get_initial_parameters()
            await _update_ui_from_state(initial_state_dict)
        except Exception as e:
            print(f"[ERROR] Failed to load initial parameters: {e}")

    async def _update_ui_from_state(state_dict: Dict[str, Any]):
        try:
            pixel_size_input.set_value(state_dict['pixel_size_angstrom']['value'])
            voltage_input.set_value(state_dict['acceleration_voltage_kv']['value'])
            cs_input.set_value(state_dict['spherical_aberration_mm']['value'])
            amplitude_contrast_input.set_value(state_dict['amplitude_contrast']['value'])
            dose_per_tilt_input.set_value(state_dict['dose_per_tilt']['value'])
            dims = state_dict['detector_dimensions']['value']
            image_size_input.set_value(f"{dims[0]}x{dims[1]}")
            tilt_axis_input.set_value(state_dict['tilt_axis_degrees']['value'])
            
            if state_dict.get('eer_fractions_per_frame') and state_dict['eer_fractions_per_frame']['value']:
                eer_grouping_input.set_value(state_dict['eer_fractions_per_frame']['value'])
        except Exception as e:
            print(f"[ERROR] Failed to update UI: {e}")

    async def on_param_change(param_name: str, value: Any, cast_func: Callable):
        try:
            casted_value = cast_func(value)
            await backend.update_parameter({
                "param_name": param_name,
                "value": casted_value,
                "mark_as_user_input": True
            })
        except Exception as e:
            print(f"[UI] Error updating {param_name}: {e}")

    async def auto_detect_metadata():
        movies_path = movies_path_input.value
        mdocs_path = mdocs_path_input.value
        if not movies_path or not mdocs_path:
            return

        detection_status.set_text("Detecting...")
        
        try:
            updated_state_dict = await backend.autodetect_parameters(mdocs_path)
            await _update_ui_from_state(updated_state_dict)
            
            state["auto_detected_values"]["pixel_size"] = updated_state_dict['pixel_size_angstrom']['value']
            state["auto_detected_values"]["dose_per_tilt"] = updated_state_dict['dose_per_tilt']['value']
            dims = updated_state_dict['detector_dimensions']['value']
            state["auto_detected_values"]["image_size"] = f"{dims[0]}x{dims[1]}"
            
        except Exception as e:
            detection_status.set_text("Failed")
            return

        eer_files = glob.glob(movies_path)
        if eer_files and eer_files[0].endswith('.eer'):
            try:
                frames_per_tilt = await backend.get_eer_frames_per_tilt(eer_files[0])
                if frames_per_tilt:
                    state["auto_detected_values"]["frames_per_tilt"] = frames_per_tilt
                    if dose_per_tilt_input.value:
                        total_dose = float(dose_per_tilt_input.value)
                        target = 0.3
                        try:
                            target = float(target_dose_input.value)
                        except: 
                            pass
                        dose_per_frame = total_dose / frames_per_tilt
                        grouping = math.floor(target / dose_per_frame)
                        if grouping > 0:
                            eer_grouping_input.set_value(str(grouping))
            except Exception as e:
                pass

        detection_status.set_text("Complete")

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
                f"{grouping} → {rendered} frames, {lost} lost ({lost/frames*100:.1f}%) | {dose_per_frame:.2f} e⁻/Å²"
            )
            asyncio.create_task(on_param_change('eer_fractions_per_frame', grouping, int))
        except Exception as e:
            pass

    async def _load_available_jobs():
        job_types = await backend.get_available_jobs()
        job_selector.options = job_types
        job_selector.update()

    def handle_job_selection():
        """Handle multi-select job changes"""
        selected = job_selector.value or []
        
        # Add new jobs
        for job in selected:
            if job not in state["selected_jobs"]:
                state["selected_jobs"].append(job)
                add_job_parameter_tab(job)
        
        # Remove deselected jobs
        to_remove = [job for job in state["selected_jobs"] if job not in selected]
        for job in to_remove:
            state["selected_jobs"].remove(job)
            if job in state["job_param_tabs"]:
                state["job_param_tabs"][job].delete()
                del state["job_param_tabs"][job]

    def add_job_parameter_tab(job_name: str):
        """Add a parameter tab for a specific job"""
        with job_param_tabs:
            tab = ui.tab(job_name).classes('text-xs')
        
        with job_param_panels:
            with ui.tab_panel(tab).classes('p-2'):
                ui.label(f'{job_name} Parameters').classes('text-xs font-medium mb-2')
                
                # Job-specific parameters
                if job_name == 'importmovies':
                    with ui.column().classes('gap-1'):
                        ui.input(label='Tilt axis', value=str(tilt_axis_input.value)).props('dense outlined readonly').classes('w-48')
                        ui.input(label='Pixel size', value=str(pixel_size_input.value)).props('dense outlined readonly').classes('w-48')
                        ui.input(label='Voltage', value=str(voltage_input.value)).props('dense outlined readonly').classes('w-48')
                
                elif job_name == 'fsMotionAndCtf':
                    with ui.column().classes('gap-1'):
                        ui.input(label='Pixel size', value=str(pixel_size_input.value)).props('dense outlined readonly').classes('w-48')
                        ui.input(label='EER grouping', value=str(eer_grouping_input.value or 32)).props('dense outlined readonly').classes('w-48')
                        ui.input(label='Voltage', value=str(voltage_input.value)).props('dense outlined readonly').classes('w-48')
                
                elif job_name == 'tsAlignment':
                    with ui.column().classes('gap-1'):
                        ui.input(label='Alignment method', value='AreTomo').props('dense outlined readonly').classes('w-48')
                        ui.input(label='Binning', value='4').props('dense outlined readonly').classes('w-48')
                
                ui.label('Parameters are read-only (synced with main config)').classes('text-xs text-gray-500 mt-2')
        
        state["job_param_tabs"][job_name] = tab

    async def handle_create_project():
        name = project_name_input.value
        location = project_location_input.value
        movies = movies_path_input.value
        mdocs = mdocs_path_input.value

        if not all([name, location, movies, mdocs, state["selected_jobs"]]):
            ui.notify("All fields required", type='negative')
            return
        
        create_button.props('loading')

        result = await backend.create_project_and_scheme(
            project_name=name,
            project_base_path=location,
            selected_jobs=state["selected_jobs"],
            movies_glob=movies,
            mdocs_glob=mdocs
        )

        create_button.props(remove='loading')
        if result.get("success"):
            state["current_project_path"] = result["project_path"]
            state["current_scheme_name"] = f"scheme_{name}"
            
            ui.notify(result["message"], type='positive')
            active_project_label.set_text(name)
            project_status.set_text("Ready")
            run_button.props(remove='disabled')

            # Disable fields
            project_name_input.disable()
            project_location_input.disable()
            movies_path_input.disable()
            mdocs_path_input.disable()
            create_button.disable()
            job_selector.disable()
            
            for el in parameter_inputs:
                el.disable()
        else:
            ui.notify(f"Error: {result.get('error')}", type='negative')

    async def _monitor_pipeline_progress():
        while state["current_project_path"] and not stop_button.props.get('disabled'):
            progress = await backend.get_pipeline_progress(state["current_project_path"])
            if not progress or progress.get('status') != 'ok':
                break
            total = progress.get('total', 0)
            completed = progress.get('completed', 0)
            running = progress.get('running', 0)
            failed = progress.get('failed', 0)
            
            if total > 0:
                progress_bar.value = completed / total
                progress_message.text = f"{completed}/{total} ({running} running, {failed} failed)"
            
            if progress.get('is_complete') and total > 0:
                msg = "Complete" if failed == 0 else f"Done ({failed} failed)"
                project_status.set_text(msg)
                project_status.classes(add='text-green-600' if failed == 0 else 'text-red-600')
                stop_button.props('disabled')
                run_button.props(remove='disabled')
                break
            await asyncio.sleep(5)

    async def create_pipeline_job_tabs():
        if not state["current_project_path"] or not state["selected_jobs"]:
            return
        
        pipeline_job_tabs.clear()
        pipeline_job_panels.clear()
        job_log_displays.clear()
        
        for idx, job_name in enumerate(state["selected_jobs"], 1):
            with pipeline_job_tabs:
                tab = ui.tab(f'{job_name}').classes('text-xs')
            
            with pipeline_job_panels:
                with ui.tab_panel(tab).classes('p-1'):
                    with ui.grid(columns=2).classes('w-full gap-1'):
                        with ui.column().classes('w-full'):
                            ui.label('stdout').classes('text-xs font-medium mb-1')
                            out_log = ui.log(max_lines=500).classes('w-full h-64 border rounded bg-gray-50 p-1 text-xs font-mono')
                        
                        with ui.column().classes('w-full'):
                            ui.label('stderr').classes('text-xs font-medium mb-1')
                            err_log = ui.log(max_lines=500).classes('w-full h-64 border rounded bg-red-50 p-1 text-xs font-mono')
                    
                    job_log_displays[job_name] = {'stdout': out_log, 'stderr': err_log, 'idx': idx}

    async def auto_refresh_all_logs():
        last_content = {}
        while state["current_project_path"] and job_log_displays:
            for job_name, display in job_log_displays.items():
                logs = await backend.get_pipeline_job_logs(state["current_project_path"], job_name, str(display['idx']))
                
                if job_name not in last_content:
                    last_content[job_name] = {'stdout': '', 'stderr': ''}
                
                if logs['stdout'] != last_content[job_name]['stdout']:
                    display['stdout'].clear()
                    display['stdout'].push(logs['stdout'] or "No output")
                    last_content[job_name]['stdout'] = logs['stdout']
                
                if logs['stderr'] != last_content[job_name]['stderr']:
                    display['stderr'].clear()
                    display['stderr'].push(logs['stderr'] or "No errors")
                    last_content[job_name]['stderr'] = logs['stderr']
            
            await asyncio.sleep(5)

    async def handle_run_pipeline():
        project_status.classes(remove='text-red-600 text-green-600')
        run_button.props('loading')
        project_status.set_text("Starting...")
        progress_bar.classes(remove='hidden').value = 0
        progress_message.classes(remove='hidden').set_text("Starting...")

        result = await backend.start_pipeline(
            project_path=state["current_project_path"],
            scheme_name=state["current_scheme_name"],
            selected_jobs=state["selected_jobs"],
            required_paths=[project_location_input.value, movies_path_input.value, mdocs_path_input.value]
        )
        
        run_button.props(remove='loading')
        if result.get("success"):
            pid = result.get('pid', 'N/A')
            ui.notify(f"Started (PID: {pid})", type="positive")
            project_status.set_text(f"Running ({pid})")
            run_button.props('disabled')
            stop_button.props(remove='disabled')
            
            await create_pipeline_job_tabs()
            asyncio.create_task(auto_refresh_all_logs())
            asyncio.create_task(_monitor_pipeline_progress())
        else:
            project_status.set_text(f"Failed: {result.get('error')}")
            progress_bar.classes('hidden')
            progress_message.classes('hidden')

    async def handle_stop_pipeline():
        ui.notify("Stop not implemented", type="warning")

    # ========== ULTRA-COMPACT UI ==========
    
    parameter_inputs = []
    
    with ui.column().classes('w-full gap-2 p-2'):
        
        # DATA IMPORT
        ui.label('DATA IMPORT').classes('text-xs font-bold text-gray-700')
        
        with ui.row().classes('w-full gap-2 items-end'):
            movies_path_input = create_path_input_with_picker(
                label='Movies',
                mode='directory',
                glob_pattern='*.eer',
                default_value='/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer'
            )
            movies_path_input.classes('flex-grow')
            
            mdocs_path_input = create_path_input_with_picker(
                label='MDOCs',
                mode='directory',
                glob_pattern='*.mdoc',
                default_value='/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc'
            )
            mdocs_path_input.classes('flex-grow')
            
            ui.button('Detect', on_click=auto_detect_metadata, icon='auto_fix_high').props('dense size=sm')
            detection_status = ui.label('').classes('text-xs text-gray-500')
        
        # MICROSCOPE & ACQUISITION
        ui.label('MICROSCOPE & ACQUISITION').classes('text-xs font-bold text-gray-700 mt-3')
        
        with ui.row().classes('w-full gap-2'):
            with ui.column().classes('gap-1'):
                pixel_size_input = ui.input(label='Pixel (Å)').props('dense outlined type=number step=0.01').classes('w-28')
                voltage_input = ui.input(label='Voltage (kV)').props('dense outlined type=number').classes('w-28')
                cs_input = ui.input(label='Cs (mm)').props('dense outlined type=number step=0.1').classes('w-28')
            
            with ui.column().classes('gap-1'):
                amplitude_contrast_input = ui.input(label='Amp. Contrast').props('dense outlined type=number step=0.01').classes('w-28')
                dose_per_tilt_input = ui.input(label='Dose/Tilt').props('dense outlined type=number step=0.1').classes('w-28')
                tilt_axis_input = ui.input(label='Tilt Axis (°)').props('dense outlined type=number step=0.1').classes('w-28')
            
            with ui.column().classes('gap-1'):
                image_size_input = ui.input(label='Detector').props('dense outlined readonly').classes('w-32')
                eer_grouping_input = ui.input(label='EER Group').props('dense outlined type=number').classes('w-32')
                target_dose_input = ui.input(label='Target Dose').props('dense outlined type=number step=0.01').classes('w-32')
        
        eer_info_label = ui.label('').classes('text-xs text-blue-600 ml-1')
        
        parameter_inputs.extend([pixel_size_input, voltage_input, cs_input, amplitude_contrast_input, 
                                dose_per_tilt_input, tilt_axis_input, eer_grouping_input, target_dose_input])
        
        # PROJECT & PIPELINE
        ui.label('PROJECT & PIPELINE').classes('text-xs font-bold text-gray-700 mt-3')
        
        with ui.row().classes('w-full gap-2'):
            project_name_input = ui.input('Name').props('dense outlined').classes('w-48')
            project_location_input = create_path_input_with_picker(
                label='Location',
                mode='directory',
                default_value='/users/artem.kushner/dev/crboost_server/projects'
            )
            project_location_input.classes('flex-grow')
        

        job_selector = ui.select(
            label='Select jobs',
            options=[],
            multiple=True,
            with_input=True
        ).props('dense outlined use-chips options-dense').classes('w-full').on('update:model-value', handle_job_selection)
        
        # Job parameter tabs
        with ui.row().classes('w-full gap-2 mt-2'):
            job_param_tabs = ui.tabs().classes('text-xs')
        job_param_panels = ui.tab_panels(job_param_tabs).classes('w-full')
        
        with ui.row().classes('gap-2 mt-2'):
            create_button = ui.button('CREATE', on_click=handle_create_project).props('dense size=sm color=primary')
            
            with ui.row().classes('items-center gap-1 ml-4'):
                ui.label('Active:').classes('text-xs')
                active_project_label = ui.label('None').classes('text-xs text-gray-600')
                ui.label('|').classes('text-xs text-gray-400')
                project_status = ui.label('No project').classes('text-xs text-gray-600')
            
            with ui.row().classes('gap-1 ml-auto'):
                run_button = ui.button('RUN', on_click=handle_run_pipeline, icon='play_arrow').props('disabled dense size=sm')
                stop_button = ui.button('STOP', on_click=handle_stop_pipeline, icon='stop').props('disabled dense size=sm')
        
        progress_bar = ui.linear_progress(value=0, show_value=False).classes('hidden w-full')
        progress_message = ui.label('').classes('text-xs text-gray-600 hidden')
        
        # JOB MONITORING
        ui.label('JOB LOGS').classes('text-xs font-bold text-gray-700 mt-3')
        
        pipeline_job_tabs = ui.tabs().classes('w-full text-xs')
        pipeline_job_panels = ui.tab_panels(pipeline_job_tabs).classes('w-full')

    # Event handlers
    movies_path_input.on('change', lambda: asyncio.create_task(auto_detect_metadata()))
    mdocs_path_input.on('change', lambda: asyncio.create_task(auto_detect_metadata()))
    
    pixel_size_input.on('change', lambda e: asyncio.create_task(on_param_change('pixel_size_angstrom', e.args, float)))
    voltage_input.on('change', lambda e: asyncio.create_task(on_param_change('acceleration_voltage_kv', e.args, float)))
    cs_input.on('change', lambda e: asyncio.create_task(on_param_change('spherical_aberration_mm', e.args, float)))
    amplitude_contrast_input.on('change', lambda e: asyncio.create_task(on_param_change('amplitude_contrast', e.args, float)))
    dose_per_tilt_input.on('change', lambda e: asyncio.create_task(on_param_change('dose_per_tilt', e.args, float)))
    tilt_axis_input.on('change', lambda e: asyncio.create_task(on_param_change('tilt_axis_degrees', e.args, float)))
    
    dose_per_tilt_input.on('change', lambda: calculate_eer_grouping())
    eer_grouping_input.on('change', lambda: calculate_eer_grouping())
    target_dose_input.on('change', lambda: calculate_eer_grouping())
    
    return load_page_data