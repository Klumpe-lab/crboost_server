import glob
from nicegui import ui
from backend import CryoBoostBackend
from models import User
from ui.utils import create_path_input_with_picker


def create_data_import_tab(backend: CryoBoostBackend, user: User):
    """Tab for data import configuration and auto-detection"""
    state = {
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
                    state["auto_detected_values"]["pixel_size"] = pix_size

                if 'ExposureDose = ' in content:
                    dose = float(content.split('ExposureDose = ')[1].split('\n')[0])
                    state["auto_detected_values"]["dose_per_tilt"] = dose * 1.5

                if 'ImageSize = ' in content:
                    img_size = content.split('ImageSize = ')[1].split('\n')[0].replace(' ', 'x')
                    state["auto_detected_values"]["image_size"] = img_size

                ui.notify("Metadata auto-detected successfully!", type='positive')
                detection_status.set_text("Metadata detected - see Parameters tab")

            except Exception as e:
                ui.notify(f"Error reading mdoc: {e}", type='negative')

        eer_files = glob.glob(movies_path)
        if eer_files and eer_files[0].endswith('.eer'):
            try:
                frames_per_tilt = await backend.get_eer_frames_per_tilt(eer_files[0])
                if frames_per_tilt:
                    state["auto_detected_values"]["frames_per_tilt"] = frames_per_tilt
                    ui.notify(f"Detected {frames_per_tilt} frames per tilt", type='positive')
            except Exception as e:
                ui.notify(f"Error analyzing EER: {e}", type='negative')

    with ui.column().classes('w-full gap-3'):
        ui.label('Data Import Configuration').classes('text-sm font-bold text-gray-800')

        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Data Sources').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
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

        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Auto-Detection').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            with ui.column().classes('w-full gap-2'):
                with ui.row().classes('w-full justify-between items-center'):
                    ui.button('Auto-detect Metadata', on_click=auto_detect_metadata, icon='auto_fix_high').props(
                        'outline dense')
                    detection_status = ui.label('Ready to detect metadata').classes('text-xs text-gray-600')
                
                ui.markdown('''
                **What will be detected:**
                - Pixel size from MDOC
                - Dose per tilt from MDOC  
                - Image dimensions from MDOC
                - Frames per tilt from EER files
                ''').classes('text-xs text-gray-600 bg-blue-50 p-2 rounded')

        with ui.card().classes('w-full p-4 gap-3'):
            ui.label('Data Validation').classes('text-xs font-semibold uppercase tracking-wider text-gray-600')
            
            async def validate_data_paths():
                movies_path = movies_glob_input.value
                mdocs_path = mdocs_glob_input.value
                
                movie_files = glob.glob(movies_path) if movies_path else []
                mdoc_files = glob.glob(mdocs_path) if mdocs_path else []
                
                validation_result.set_text(
                    f"Found {len(movie_files)} movie files and {len(mdoc_files)} mdoc files"
                )
                
                if movie_files and mdoc_files:
                    ui.notify("Data paths validated successfully!", type='positive')
                else:
                    ui.notify("Some data paths may be invalid", type='warning')

            validation_result = ui.label('Click to validate data paths').classes('text-xs text-gray-600')
            ui.button('Validate Data Paths', on_click=validate_data_paths, icon='check_circle').props('outline dense')