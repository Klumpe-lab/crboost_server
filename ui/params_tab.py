import math
from nicegui import ui
from backend import CryoBoostBackend
from models import User


def create_parameters_tab(backend: CryoBoostBackend, user: User):
    """Tab for all processing parameters"""
    state = {
        "microscope_params": {},
        "tilt_series_params": {},
        "reconstruction_params": {},
    }

    def calculate_eer_grouping():
        if not dose_per_tilt_input.value or not eer_grouping_input.value:
            return

        try:
            total_dose = float(dose_per_tilt_input.value)
            current_grouping = int(eer_grouping_input.value)
            frames_per_tilt = 40  # Default, should come from data import tab

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
        ui.label('Processing Parameters').classes('text-sm font-bold text-gray-800')

        # Microscope Parameters
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Microscope Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
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

        # Tilt Series Parameters
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Tilt Series Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
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
                    eer_info_label = ui.label('').classes('text-xs text-blue-600 h-4')
                    ui.button('Calculate Optimal Grouping', on_click=calculate_eer_grouping, icon='calculate').props(
                        'outline dense')

        # Reconstruction Parameters
        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Reconstruction Parameters').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
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
        with ui.row().classes('w-full justify-end gap-2 mt-4'):
            ui.button('Save Parameters', icon='save').props('outline dense')
            ui.button('Validate Parameters', icon='check_circle').props('dense color=primary')
            ui.button('Reset to Defaults', icon='refresh').props('flat dense')

    # Connect EER calculation handlers
    dose_per_tilt_input.on('change', calculate_eer_grouping)
    eer_grouping_input.on('change', calculate_eer_grouping)