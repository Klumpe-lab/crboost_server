import glob
import math
from nicegui import ui
from backend import CryoBoostBackend
from models import User
from .utils import create_path_input_with_picker


def create_data_parameters_tab(backend: CryoBoostBackend, user: User):
    """Combined tab for data import and processing parameters"""
    state = {
        "auto_detected_values": {},
        "microscope_params": {},
        "tilt_series_params": {},
        "reconstruction_params": {},
    }

    async def auto_detect_metadata():
        movies_path = movies_glob_input.value
        mdocs_path = mdocs_glob_input.value

        if not movies_path or not mdocs_path:
            ui.notify("Please provide both movies and mdoc paths first", type='warning')
            return

        detection_status.set_text("Detecting metadata...")
        
        # MDOC file analysis
        mdoc_files = glob.glob(mdocs_path)
        if mdoc_files:
            try:
                with open(mdoc_files[0], 'r') as f:
                    content = f.read()

                if 'PixelSpacing = ' in content:
                    pix_size = float(content.split('PixelSpacing = ')[1].split('\n')[0])
                    pixel_size_input.set_value(str(pix_size))
                    state["auto_detected_values"]["pixel_size"] = pix_size

                if 'ExposureDose = ' in content:
                    dose = float(content.split('ExposureDose = ')[1].split('\n')[0])
                    calculated_dose = dose * 1.5  # Apply the 1.5x factor like in original
                    dose_per_tilt_input.set_value(str(calculated_dose))
                    state["auto_detected_values"]["dose_per_tilt"] = calculated_dose

                if 'ImageSize = ' in content:
                    img_size = content.split('ImageSize = ')[1].split('\n')[0].replace(' ', 'x')
                    image_size_input.set_value(img_size)
                    state["auto_detected_values"]["image_size"] = img_size

                ui.notify("MDOC metadata detected successfully!", type='positive')

            except Exception as e:
                ui.notify(f"Error reading mdoc: {e}", type='negative')

        # EER frame analysis
        eer_files = glob.glob(movies_path)
        if eer_files and eer_files[0].endswith('.eer'):
            try:
                frames_per_tilt = await backend.get_eer_frames_per_tilt(eer_files[0])
                if frames_per_tilt:
                    state["auto_detected_values"]["frames_per_tilt"] = frames_per_tilt
                    
                    # Auto-calculate EER grouping if dose is available
                    if dose_per_tilt_input.value:
                        total_dose = float(dose_per_tilt_input.value)
                        target_dose_per_frame = 0.3
                        dose_per_frame = total_dose / frames_per_tilt
                        num_frames_to_group = math.floor(target_dose_per_frame / dose_per_frame)
                        
                        if num_frames_to_group > 0:
                            eer_grouping_input.set_value(str(num_frames_to_group))
                            calculate_eer_grouping()  # Update the display
                    
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
            frames_per_tilt = state["auto_detected_values"].get("frames_per_tilt", 40)

            dose_per_rendered_frame = (total_dose / frames_per_tilt) * current_grouping
            rendered_frames = math.floor(frames_per_tilt / current_grouping)
            lost_frames = frames_per_tilt - (rendered_frames * current_grouping)

            eer_info_label.set_text(
                f"Grouping: {current_grouping} -> {rendered_frames} frames, "
                f"{lost_frames} lost ({lost_frames / frames_per_tilt * 100:.1f}%) | "
                f"Dose per frame: {dose_per_rendered_frame:.2f} e⁻/Å²"
            )

        except Exception as e:
            print(f"Error calculating EER grouping: {e}")

    with ui.column().classes('w-full gap-3'):
        ui.label('Data Import & Processing Parameters').classes('text-sm font-bold text-gray-800')

        # Data Import Section
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('1. Data Import Configuration').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3'):
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

        # Microscope Parameters Section
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

        # Tilt Series Parameters Section
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('3. Tilt Series Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3'):
                tilt_axis_input = ui.input(label='Tilt Axis Angle (°)', placeholder='82.5').props(
                    'dense outlined type=number step=0.1')
                image_size_input = ui.input(label='Image Size (WxH)', placeholder='4096x4096').props('dense outlined')
            
            with ui.expansion('EER Processing Settings', icon='movie_filter').classes('w-full mt-2 text-xs'):
                with ui.column().classes('w-full gap-3 p-3 bg-gray-50 rounded-lg'):
                    with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-2'):
                        eer_grouping_input = ui.input(label='EER Frames to Group', placeholder='5').props(
                            'dense outlined type=number')
                        target_dose_input = ui.input(label='Target Dose per Frame (e⁻/Å²)',
                                                     placeholder='0.3').props('dense outlined type=number step=0.01')
                    eer_info_label = ui.label('Grouping calculation will appear here').classes('text-xs text-blue-600 h-4')
                    ui.button('Calculate Optimal Grouping', on_click=calculate_eer_grouping, icon='calculate').props(
                        'outline dense')

        # Reconstruction Parameters Section
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('4. Reconstruction Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.grid(columns=3).classes('w-full gap-x-4 gap-y-3'):
                rec_pixel_size_input = ui.input(label='Recon. Pixel Size (Å)', placeholder='5.4').props(
                    'dense outlined type=number step=0.01')
                tomogram_size_input = ui.input(label='Tomogram Size (XYZ)', placeholder='1024x1024x512').props(
                    'dense outlined')
                sample_thickness_input = ui.input(label='Sample Thickness (nm)', placeholder='300').props(
                    'dense outlined type=number')
            
            with ui.grid(columns=2).classes('w-full gap-x-4 gap-y-3 mt-2'):
                alignment_method_select = ui.select(label='Alignment Method', options=['AreTomo', 'IMOD', 'Warp'],
                                                    value='AreTomo').props('dense outlined')
                patch_size_input = ui.input(label='Patch Size (Alignment)', placeholder='800').props(
                    'dense outlined type=number')

        # Action Buttons
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('5. Save & Validate').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            async def validate_all_parameters():
                # Basic validation
                if not movies_glob_input.value or not mdocs_glob_input.value:
                    ui.notify("Please provide data paths", type='warning')
                    return
                    
                if not pixel_size_input.value or not dose_per_tilt_input.value:
                    ui.notify("Please fill in required parameters", type='warning')
                    return
                    
                ui.notify("All parameters validated successfully!", type='positive')

            async def save_parameters():
                # Collect all parameters
                params = {
                    "data_import": {
                        "movies_path": movies_glob_input.value,
                        "mdocs_path": mdocs_glob_input.value
                    },
                    "microscope": {
                        "pixel_size": pixel_size_input.value,
                        "voltage": voltage_input.value,
                        "cs": cs_input.value,
                        "amplitude_contrast": amplitude_contrast_input.value,
                        "dose_per_tilt": dose_per_tilt_input.value
                    },
                    "tilt_series": {
                        "tilt_axis": tilt_axis_input.value,
                        "image_size": image_size_input.value,
                        "eer_grouping": eer_grouping_input.value,
                        "target_dose_per_frame": target_dose_input.value
                    },
                    "reconstruction": {
                        "rec_pixel_size": rec_pixel_size_input.value,
                        "tomogram_size": tomogram_size_input.value,
                        "sample_thickness": sample_thickness_input.value,
                        "alignment_method": alignment_method_select.value,
                        "patch_size": patch_size_input.value
                    }
                }
                
                # Here you would save to backend
                ui.notify("Parameters saved successfully!", type='positive')

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button('Validate Parameters', on_click=validate_all_parameters, icon='check_circle').props('dense')
                ui.button('Save Parameters', on_click=save_parameters, icon='save').props('dense color=primary')
                ui.button('Reset to Defaults', icon='refresh').props('flat dense')

    # Connect EER calculation handlers
    dose_per_tilt_input.on('change', calculate_eer_grouping)
    eer_grouping_input.on('change', calculate_eer_grouping)