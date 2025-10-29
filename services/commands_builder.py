# services/command_builders.py
from pathlib import Path
from typing import Dict, Any, Optional, List
from services.parameter_models import (
    ImportMoviesParams, FsMotionCtfParams, TsAlignmentParams,
    ComputingParams
)

class BaseCommandBuilder:
    """Base class for all command builders"""
    
    def format_paths(self, paths: Dict[str, Path]) -> Dict[str, str]:
        """Convert Path objects to strings"""
        return {k: str(v) for k, v in paths.items()}
    
    def add_optional_param(self, cmd_parts: List[str], flag: str, value: Any, condition: bool = True):
        """Add parameter only if condition is met"""
        if condition and value is not None:
            cmd_parts.extend([flag, str(value)])

class ImportMoviesCommandBuilder(BaseCommandBuilder):
    """Build import movies command from params"""
    
    def build(self, params: ImportMoviesParams, paths: Dict[str, Path]) -> str:
        """Build the relion_import command"""
        cmd_parts = [
            "relion_import",
            "--do_movies",
            "--optics_group_name", params.optics_group_name,
            "--angpix", str(params.pixel_size),
            "--kV", str(params.voltage),
            "--Cs", str(params.spherical_aberration),
            "--Q0", str(params.amplitude_contrast),
            "--dose_per_tilt_image", str(params.dose_per_tilt_image),
            "--nominal_tilt_axis_angle", str(params.tilt_axis_angle),
        ]
        
        # Add optional parameters
        if params.invert_defocus_hand:
            cmd_parts.append("--invert_defocus_hand")
        
        if params.do_at_most > 0:
            cmd_parts.extend(["--do_at_most", str(params.do_at_most)])
        
        # Add paths
        if 'input_dir' in paths:
            # The input glob should be relative to the mdoc dir
            input_pattern = str(paths['input_dir']) + "/*.mdoc"
            cmd_parts.extend(["--i", input_pattern])
        
        if 'output_dir' in paths:
            cmd_parts.extend(["--o", str(paths['output_dir'])])
        
        if 'pipeline_control' in paths:
            cmd_parts.extend(["--pipeline_control", str(paths['pipeline_control'])])
        
        return " ".join(cmd_parts)

class FsMotionCtfCommandBuilder(BaseCommandBuilder):
    """Build complete WarpTools motion correction and CTF command"""
    
    def build(self, params: FsMotionCtfParams, paths: Dict[str, Path]) -> str:
        test_cmd = f"echo 'ACTUAL VALUES: pixel_size={params.pixel_size}, voltage={params.voltage}' && "
        test_cmd += "WarpTools --help"  # Just to see if WarpTools works
        
        create_settings_parts = [
            "WarpTools create_settings",
            "--folder_data ../../frames",
            "--extension '*.eer'",
            "--folder_processing ./warp_frameseries",
            "--output ./warp_frameseries.settings",
            "--angpix", str(params.pixel_size),  
            "--eer_ngroups", str(params.eer_ngroups),
        ]
        
        # Add gain reference if provided
        if params.gain_path and params.gain_path != "None":
            create_settings_parts.extend(["--gain_reference", params.gain_path])
            if params.gain_operations and params.gain_operations != "None":
                create_settings_parts.extend(["--gain_operations", params.gain_operations])
        
        # Step 2: Run motion correction and CTF estimation
        run_main_parts = [
            "WarpTools fs_motion_and_ctf",
            "--settings ./warp_frameseries.settings",  # From step 1
            # Motion correction parameters
            "--m_grid", params.m_grid,
            "--m_range_min", str(params.m_range_min),
            "--m_range_max", str(params.m_range_max),
            "--m_bfac", str(params.m_bfac),
            # CTF parameters
            "--c_grid", params.c_grid,
            "--c_window", str(params.c_window),
            "--c_range_min", str(params.c_range_min),
            "--c_range_max", str(params.c_range_max),
            "--c_defocus_min", str(params.defocus_min_angstroms),  # Converted to Å
            "--c_defocus_max", str(params.defocus_max_angstroms),  # Converted to Å
            "--c_voltage", str(round(float(params.voltage))),
            "--c_cs", str(params.cs),
            "--c_amplitude", str(params.amplitude),
            # Processing control
            "--perdevice", str(params.perdevice),
            "--out_averages",  # Output motion-corrected averages
        ]
        
        if params.do_at_most > 0:
            run_main_parts.extend(["--do_at_most", str(params.do_at_most)])
        
        full_command = " && ".join([
            " ".join(create_settings_parts),
            " ".join(run_main_parts)
        ])
        print(f"[COMMAND BUILDER] Built WarpTools command with {len(create_settings_parts)} create_settings args and {len(run_main_parts)} fs_motion_and_ctf args")
        return full_command
        

#TODO Work in progress
class TsAlignmentCommandBuilder(BaseCommandBuilder):
    ...

class TsCtf(BaseCommandBuilder):
    ...
class TsReconstruct(BaseCommandBuilder):
    ...

class denoiseTrain(BaseCommandBuilder):
    ...
class denoiseInfer(BaseCommandBuilder):
    ...
class templateMatching(BaseCommandBuilder):
    ...

class tmExtractCandidates(BaseCommandBuilder):
    ...
class subTomoReconstruction(BaseCommandBuilder):
    ...













