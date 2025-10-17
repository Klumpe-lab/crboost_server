import asyncio
import glob
import math
from pathlib import Path
from typing import List
from nicegui import ui

from backend import CryoBoostBackend
from local_file_picker import local_file_picker
from models import Job, User

HARDCODED_USER = User(username="artem.kushner")
STATUS_MAP = {
    "PENDING": ("orange", "PD"),
    "RUNNING": ("green", "R"),
    "COMPLETED": ("blue", "CG"),
    "FAILED": ("red", "F"),
    "CANCELLED": ("gray", "CA"),
    "TIMEOUT": ("red", "TO"),
}


def create_path_input_with_picker(label: str, mode: str, glob_pattern: str = '', default_value: str = '') -> ui.input:
    """A factory for creating a text input with a file/folder picker button."""

    async def _pick_path():
        start_dir = Path(path_input.value).parent if path_input.value and Path(path_input.value).exists() else '~'
        result = await local_file_picker(
            start_dir,
            mode=mode,
            glob_pattern_annotation=glob_pattern or None
        )
        if result:
            selected_path = Path(result[0])
            if mode == 'directory' and glob_pattern:
                path_input.set_value(str(selected_path / glob_pattern))
            else:
                path_input.set_value(str(selected_path))

    with ui.row().classes('w-full items-center no-wrap'):
        hint = f"Provide a path to a {mode}"
        if glob_pattern:
            hint = f"Provide a glob pattern, e.g., /path/to/files/{glob_pattern}"

        path_input = ui.input(label=label, value=default_value) \
            .classes('flex-grow') \
            .props(f'dense outlined hint="{hint}"')

        ui.button(icon='folder', on_click=_pick_path).props('flat dense')

    return path_input


def create_ui_router(backend: CryoBoostBackend):
    @ui.page('/')
    async def main_page():
        # REFACTOR: Swapped font to "Inter" for a cleaner, more modern UI look.
        ui.add_head_html('''
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500&display=swap');
                
                body, .nicegui-content {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 12px !important;
                    font-weight: 400; /* Regular weight for body */
                }
                .q-field__native, .q-field__label, .q-select__option, .q-item__label, .q-field__hint {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 12px !important;
                }
                .q-btn, .q-tab__label {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 11px !important;
                    font-weight: 500; /* Medium weight for controls */
                    text-transform: none !important;
                }
                /* Make header and section titles slightly bolder */
                .font-semibold {
                     font-weight: 500 !important;
                }
                .q-badge {
                    font-family: 'Inter', sans-serif !important;
                    font-size: 10px !important;
                    font-weight: 500;
                }
            </style>
        ''')
        await create_main_ui(backend, HARDCODED_USER)


async def create_main_ui(backend: CryoBoostBackend, user: User):
    with ui.header(elevated=True).classes('bg-white text-gray-800'):
        with ui.row().classes('w-full items-center justify-between p-2'):
            ui.label('CryoBoost Server').classes('text-sm font-semibold')

            with ui.tabs().classes('w-1/2') as tabs:
                setup_tab = ui.tab('Tomogram Setup')
                projects_tab = ui.tab('Projects')
                jobs_tab = ui.tab('Job Status')
                info_tab = ui.tab('Cluster Info')

            ui.label(f'User: {user.username}').classes('text-xs')

    # REFACTOR: Removed text-sm as CSS now controls the global font size
    with ui.tab_panels(tabs, value=setup_tab).classes('w-full p-3'):
        with ui.tab_panel(setup_tab):
            create_setup_page(backend, user)
        with ui.tab_panel(projects_tab):
            build_projects_tab(backend, user)
        with ui.tab_panel(jobs_tab):
            await create_jobs_page(backend, user)
        with ui.tab_panel(info_tab):
            create_info_page(backend)


def create_info_page(backend: CryoBoostBackend):
    ui.label('SLURM Cluster Information').classes('text-sm font-medium mb-2')
    output_area = ui.log().classes('w-full h-96 border rounded-md p-2 bg-gray-50 text-xs font-mono')

    async def get_info():
        output_area.push("Loading sinfo...")
        result = await backend.get_slurm_info()
        output_area.clear()
        output_area.push(result["output"] if result["success"] else result["error"])

    ui.button('Get SLURM Info', on_click=get_info).props('dense')
    asyncio.create_task(get_info())


async def create_jobs_page(backend: CryoBoostBackend, user: User):
    with ui.column().classes('w-full'):
        with ui.row().classes('w-full justify-between items-center mb-2'):
            ui.label('Individual Job Management').classes('text-xs font-medium uppercase')
            ui.button('Submit Test GPU Job',
                      on_click=lambda: submit_and_track_job(backend, user, job_tabs, job_tab_panels)).props('dense')

        with ui.tabs().classes('w-full') as job_tabs:
            pass

        with ui.tab_panels(job_tabs, value=None).classes('w-full mt-2 border rounded-md') as job_tab_panels:
            user_jobs = backend.get_user_jobs()
            if not user_jobs:
                with ui.tab_panel('placeholder').classes('items-center justify-center'):
                    ui.label('No jobs submitted yet.').classes('text-gray-500')
            else:
                for job in user_jobs:
                    create_job_tab(backend, user, job, job_tabs, job_tab_panels)
                job_tabs.set_value(user_jobs[-1].internal_id)


async def submit_and_track_job(backend: CryoBoostBackend, user: User, job_tabs, job_tab_panels):
    result = await backend.submit_test_gpu_job()
    if not result['success']:
        ui.notify(f"Job submission failed: {result['error']}", type='negative')
        return
    job = result['job']
    ui.notify(f"Submitted job {job.slurm_id}", type='positive')
    if 'placeholder' in job_tab_panels:
        job_tab_panels.clear()
        job_tabs.clear()
    create_job_tab(backend, user, job, job_tabs, job_tab_panels)
    job_tabs.set_value(job.internal_id)


def create_job_tab(backend: CryoBoostBackend, user: User, job: Job, job_tabs, job_tab_panels):
    with job_tabs:
        new_tab = ui.tab(name=job.internal_id, label=f'Job {job.slurm_id}')

    with job_tab_panels:
        with ui.tab_panel(new_tab).classes('p-2'):
            with ui.row().classes('w-full justify-between items-center'):
                ui.label(f'Tracking logs for Job ID: {job.slurm_id}').classes('text-xs font-medium')
                with ui.row().classes('items-center gap-2'):
                    color, label = STATUS_MAP.get(job.status, ("gray", job.status))
                    status_badge = ui.badge(label, color=color).props('outline')
                    refresh_button = ui.button(icon='refresh', on_click=lambda: update_log_display(True)).props(
                        'flat round dense')

            log_output = ui.log(max_lines=1000).classes(
                'w-full h-screen border rounded-md bg-gray-50 p-2 mt-2 text-xs')
            log_output.push(job.log_content)

    def update_log_display(manual_refresh=False):
        job_info = backend.get_job_log(job.internal_id)
        if job_info:
            log_output.clear()
            log_output.push(job_info.log_content)
            status_text = job_info.status
            color, label = STATUS_MAP.get(status_text, ("gray", status_text))
            status_badge.text = label
            status_badge.color = color
            if status_text in {"COMPLETED", "FAILED", "CANCELLED"}:
                timer.deactivate()
                refresh_button.disable()
            if manual_refresh:
                ui.notify('Logs refreshed!', type='positive', timeout=1000)

    timer = ui.timer(interval=5, callback=update_log_display, active=True)


def build_projects_tab(backend: CryoBoostBackend, user: User):
    state = {
        "selected_jobs": [],
        "current_project_path": None,
        "current_scheme_name": None,
    }

    async def _load_available_jobs():
        job_types = await backend.get_available_jobs()
        job_selector.options = job_types
        job_selector.update()

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

            project_name_input.disable()
            project_location_input.disable()
            movies_path_input.disable()
            mdocs_path_input.disable()
            create_button.disable()
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

    with ui.column().classes('w-full gap-3'):
        with ui.column().classes('w-full gap-2'):
            ui.label('1. Configure and Create Project').classes('text-xs font-semibold uppercase tracking-wider')
            project_name_input = ui.input('Project Name', placeholder='e.g., my_first_dataset').props('dense outlined')
            project_location_input = create_path_input_with_picker(
                label='Project Location',
                mode='directory',
                default_value='/users/artem.kushner/dev/crboost_server/projects'
            )
            movies_path_input = create_path_input_with_picker(
                label='Movie Files Path/Glob',
                mode='directory',
                glob_pattern='*.eer',
                default_value='/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer'
            )
            mdocs_path_input = create_path_input_with_picker(
                label='MDOC Files Path/Glob',
                mode='directory',
                glob_pattern='*.mdoc',
                default_value='/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc'
            )

            job_status_label = ui.label('No jobs added yet.').classes('text-xs text-gray-600 mt-2')
            with ui.expansion('Add Job to Pipeline', icon='add').classes('w-full text-xs'):
                with ui.row().classes('w-full items-center gap-2'):
                    job_selector = ui.select(label='Select job type', options=[]).classes('flex-grow').props(
                        'dense outlined')
                    ui.button('ADD', on_click=handle_add_job).props('dense')
            selected_jobs_container = ui.column().classes('w-full mt-1 gap-1')
            create_button = ui.button('CREATE PROJECT', on_click=handle_create_project).props('dense')

        ui.separator()

        with ui.column().classes('w-full gap-2'):
            ui.label('2. Schedule and Execute Pipeline').classes('text-xs font-semibold uppercase tracking-wider')
            with ui.row():
                ui.label('Active Project:').classes('text-xs font-medium mr-2')
                active_project_label = ui.label('No active project').classes('text-xs')
            pipeline_status = ui.label('Create and configure a project first.').classes('text-xs text-gray-600 my-1')
            with ui.row().classes('gap-2'):
                run_button = ui.button('RUN', on_click=handle_run_pipeline, icon='play_arrow').props('disabled dense')
                stop_button = ui.button('STOP', on_click=handle_stop_pipeline, icon='stop').props('disabled dense')
            progress_bar = ui.linear_progress(value=0, show_value=False).classes('hidden w-full')
            progress_message = ui.label('').classes('text-xs text-gray-600 hidden')

    asyncio.create_task(_load_available_jobs())


def create_setup_page(backend: CryoBoostBackend, user: User):
    state = {
        "microscope_params": {},
        "tilt_series_params": {},
        "reconstruction_params": {},
        "container_settings": {},
        "auto_detected_values": {}
    }

    async def auto_detect_metadata():
        movies_path = movies_glob_input.value
        mdocs_path = mdocs_glob_input.value

        if not movies_path or not mdocs_path:
            ui.notify("Please provide both movies and mdoc paths first", type='warning')
            return

        mdoc_files = glob.glob(mdocs_path)
        if mdoc_files:
            try:
                with open(mdoc_files[0], 'r') as f:
                    content = f.read()

                if 'PixelSpacing = ' in content:
                    pix_size = float(content.split('PixelSpacing = ')[1].split('\n')[0])
                    pixel_size_input.set_value(str(pix_size))

                if 'ExposureDose = ' in content:
                    dose = float(content.split('ExposureDose = ')[1].split('\n')[0])
                    dose_per_tilt_input.set_value(str(dose * 1.5))

                if 'ImageSize = ' in content:
                    img_size = content.split('ImageSize = ')[1].split('\n')[0].replace(' ', 'x')
                    image_size_input.set_value(img_size)

            except Exception as e:
                ui.notify(f"Error reading mdoc: {e}", type='negative')

        eer_files = glob.glob(movies_path)
        if eer_files and eer_files[0].endswith('.eer'):
            try:
                frames_per_tilt = await backend.get_eer_frames_per_tilt(eer_files[0])
                if frames_per_tilt:
                    total_dose = float(dose_per_tilt_input.value) if dose_per_tilt_input.value else 3.0
                    target_dose_per_frame = 0.3

                    dose_per_frame = total_dose / frames_per_tilt
                    num_frames_to_group = math.floor(target_dose_per_frame / dose_per_frame)

                    eer_grouping_input.set_value(str(num_frames_to_group))
                    state["auto_detected_values"]["frames_per_tilt"] = frames_per_tilt
                    state["auto_detected_values"]["dose_per_frame"] = dose_per_frame

            except Exception as e:
                ui.notify(f"Error analyzing EER: {e}", type='negative')

    def calculate_eer_grouping():
        if not dose_per_tilt_input.value or not eer_grouping_input.value:
            return

        try:
            total_dose = float(dose_per_tilt_input.value)
            current_grouping = int(eer_grouping_input.value)
            frames_per_tilt = state["auto_detected_values"].get("frames_per_tilt", 40)

            dose_per_rendered_frame = (total_dose / frames_per_tilt) * current_grouping
            rendered_frames = math.floor(frames_per_tilt / current_grouping)
            lost_frames = frames_per_tilt - (rendered_frames * current_grouping)

            eer_info_label.set_text(
                f"Grouping: {current_grouping} -> {rendered_frames} frames, "
                f"{lost_frames} lost ({lost_frames / frames_per_tilt * 100:.1f}%)"
            )

        except Exception as e:
            print(f"Error calculating EER grouping: {e}")

    with ui.column().classes('w-full gap-3'):
        ui.label('Tomogram Setup & Data Import').classes('text-sm font-bold text-gray-800')

        with ui.column().classes('w-full gap-2'):
            ui.label('1. Data Import Configuration').classes('text-xs font-semibold uppercase tracking-wider')
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-1'):
                movies_glob_input = create_path_input_with_picker(
                    label='Movie Files (EER/TIF)',
                    mode='directory',
                    glob_pattern='*.eer',
                    default_value='/users/artem.kushner/dev/001_CopiaTestSet/frames/*.eer'
                )
                mdocs_glob_input = create_path_input_with_picker(
                    label='MDOC Files',
                    mode='directory',
                    glob_pattern='*.mdoc',
                    default_value='/users/artem.kushner/dev/001_CopiaTestSet/mdoc/*.mdoc'
                )
            with ui.row().classes('w-full justify-between items-center mt-2'):
                ui.button('Auto-detect Metadata', on_click=auto_detect_metadata, icon='auto_fix_high').props(
                    'outline dense')
                detection_status = ui.label('Ready to detect metadata').classes('text-xs text-gray-600')
        ui.separator()

        with ui.column().classes('w-full gap-2'):
            ui.label('2. Microscope Parameters').classes('text-xs font-semibold uppercase tracking-wider')
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-1'):
                pixel_size_input = ui.input(label='Pixel Size (Å)', placeholder='1.35').props(
                    'dense outlined type=number step=0.01')
                voltage_input = ui.input(label='Voltage (kV)', placeholder='300').props('dense outlined type=number')
                cs_input = ui.input(label='Spherical Aberration (mm)', placeholder='2.7').props(
                    'dense outlined type=number step=0.1')
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-1 mt-1'):
                amplitude_contrast_input = ui.input(label='Amplitude Contrast', placeholder='0.1').props(
                    'dense outlined type=number step=0.01')
                dose_per_tilt_input = ui.input(label='Dose per Tilt (e⁻/Å²)', placeholder='3.0').props(
                    'dense outlined type=number step=0.1')
        ui.separator()

        with ui.column().classes('w-full gap-2'):
            ui.label('3. Tilt Series Parameters').classes('text-xs font-semibold uppercase tracking-wider')
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-1'):
                tilt_axis_input = ui.input(label='Tilt Axis Angle (°)', placeholder='82.5').props(
                    'dense outlined type=number step=0.1')
                image_size_input = ui.input(label='Image Size (WxH)', placeholder='4096x4096').props('dense outlined')
            with ui.expansion('Advanced EER Settings', icon='settings').classes('w-full mt-2 text-xs'):
                with ui.column().classes('w-full gap-2 p-2 bg-gray-50 rounded-lg'):
                    with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-1'):
                        eer_grouping_input = ui.input(label='EER Frames to Group', placeholder='5').props(
                            'dense outlined type=number')
                        target_dose_input = ui.input(label='Target Dose per Frame (e⁻/Å²)',
                                                     placeholder='0.3').props('dense outlined type=number step=0.01')
                    eer_info_label = ui.label('').classes('text-xs text-blue-600 h-4')
                    ui.button('Calculate Optimal Grouping', on_click=calculate_eer_grouping, icon='calculate').props(
                        'outline dense')
        ui.separator()

        with ui.column().classes('w-full gap-2'):
            ui.label('4. Reconstruction Parameters').classes('text-xs font-semibold uppercase tracking-wider')
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-1'):
                rec_pixel_size_input = ui.input(label='Recon. Pixel Size (Å)', placeholder='5.4').props(
                    'dense outlined type=number step=0.01')
                tomogram_size_input = ui.input(label='Tomogram Size (XYZ)', placeholder='1024x1024x512').props(
                    'dense outlined')
                sample_thickness_input = ui.input(label='Sample Thickness (nm)', placeholder='300').props(
                    'dense outlined type=number')
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-1 mt-1'):
                alignment_method_select = ui.select(label='Alignment Method', options=['AreTomo', 'IMOD', 'Warp'],
                                                    value='AreTomo').props('dense outlined')
                patch_size_input = ui.input(label='Patch Size (Alignment)', placeholder='800').props(
                    'dense outlined type=number')
        ui.separator()

        with ui.column().classes('w-full gap-2'):
            ui.label('5. Container & Tool Configuration').classes('text-xs font-semibold uppercase tracking-wider')
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-1 items-center'):
                container_runtime_select = ui.select(label='Container Runtime',
                                                     options=['apptainer', 'singularity', 'docker'],
                                                     value='apptainer').props('dense outlined')
                gpu_mode_toggle = ui.toggle(['CPU Only', 'GPU Accelerated'], value='GPU Accelerated').props('dense')
            with ui.expansion('Advanced Container Settings', icon='settings').classes('w-full mt-2 text-xs'):
                with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-1 p-2 bg-gray-50 rounded-lg'):
                    bind_paths_input = ui.input(label='Additional Bind Paths', placeholder='/scratch,/groups').props(
                        'dense outlined')
                    memory_limit_input = ui.input(label='Memory Limit (GB)', placeholder='32').props(
                        'dense outlined type=number')

        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button('Save Config', icon='save').props('outline dense')
            ui.button('Validate', icon='check_circle').props('dense color=primary')
            ui.button('Apply to Project', icon='play_arrow').props('dense color=positive')

    dose_per_tilt_input.on('change', calculate_eer_grouping)
    eer_grouping_input.on('change', calculate_eer_grouping)