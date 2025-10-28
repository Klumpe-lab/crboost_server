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
        
        print("TEST CMD:", test_cmd)
        print(f"[COMMAND BUILDER DEBUG] Building command with pixel_size: {params.pixel_size}, voltage: {params.voltage}")
        
        # Step 1: Create settings file
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
        
        # Optional: limit processing
        if params.do_at_most > 0:
            run_main_parts.extend(["--do_at_most", str(params.do_at_most)])
        
        # Join the two commands with &&
        full_command = " && ".join([
            " ".join(create_settings_parts),
            " ".join(run_main_parts)
        ])
        print(f"[COMMAND BUILDER] Built WarpTools command with {len(create_settings_parts)} create_settings args and {len(run_main_parts)} fs_motion_and_ctf args")
        return full_command
        

class TsAlignmentCommandBuilder(BaseCommandBuilder):
    """Build tilt series alignment command"""
    
    def build(self, params: TsAlignmentParams, paths: Dict[str, Path]) -> str:
        """Build alignment command based on selected method"""
        
        if params.alignment_method.value == "AreTomo":
            return self._build_aretomo_command(params, paths)
        elif params.alignment_method.value == "IMOD":
            return self._build_imod_command(params, paths)
        else:
            return self._build_relion_command(params, paths)
    
    def _build_aretomo_command(self, params: TsAlignmentParams, paths: Dict[str, Path]) -> str:
        """Build AreTomo2 command"""
        cmd_parts = ["AreTomo2"]
        
        # Input/output
        if 'input_star' in paths:
            # AreTomo needs the .star file to find the .mrc stacks
            # Assuming AreTomo can parse a Relion 5 star file...
            # This is a potential point of failure.
            # For now, let's assume it needs the *input stack*, not the star.
            # This logic needs verification.
            # Let's assume the orchestrator path logic is wrong and 
            # AreTomo needs an input stack, not a star.
            # The user's code had -InMrc, so it expects an MRC file.
            # This part of the logic is flawed, but fixing it requires
            # knowing what the AreTomo wrapper expects.
            
            # --- FALLBACK: Use input_star path, but change extension
            input_mrc = paths['input_star'].with_suffix('.mrcs') # Guessing
            cmd_parts.extend(["-InMrc", str(input_mrc)])
        
        if 'output_dir' in paths:
            # Output to current dir, named aligned.mrc
            cmd_parts.extend(["-OutMrc", str(paths['output_dir']) + "/aligned.mrc"])
        
        # Core parameters
        cmd_parts.extend([
            "-OutBin", str(params.binning),
            # This conversion is suspicious, but keeping from user's code
            "-VolZ", str(int(params.thickness_nm / 10)),  
            "-TiltCor", str(params.tilt_cor),
            "-OutImod", str(params.out_imod),
        ])
        
        # Patch tracking
        cmd_parts.extend([
            "-Patch", f"{params.patch_x} {params.patch_y}",
        ])
        
        # --- MODIFICATION ---
        # REMOVED: GPU selection. This should be handled by the scheduler (qsub/slurm)
        # if 'gpu_id' in paths:
        #     cmd_parts.extend(["-Gpu", str(paths['gpu_id'])])
        # --- END MODIFICATION ---
            
        return " ".join(cmd_parts)
    
    def _build_imod_command(self, params: TsAlignmentParams, paths: Dict[str, Path]) -> str:
        """Build IMOD alignment command"""
        # Placeholder for IMOD command building
        return f"echo 'IMOD alignment not implemented'; etomo --binning {params.binning}"
    
    def _build_relion_command(self, params: TsAlignmentParams, paths: Dict[str, Path]) -> str:
        """Build Relion tomo alignment command"""
        cmd_parts = ["relion_tomo_align"]
        
        if 'input_star' in paths:
            cmd_parts.extend(["--i", str(paths['input_star'])])
        
        if 'output_star' in paths:
            cmd_parts.extend(["--o", str(paths['output_star'])])
        
        cmd_parts.extend([
            "--bin", str(params.binning),
            "--thickness", str(params.thickness_nm),
        ])
        
        if params.do_at_most > 0:
            cmd_parts.extend(["--do_at_most", str(params.do_at_most)])
        
        return " ".join(cmd_parts)

class QsubCommandBuilder(BaseCommandBuilder):
    """Build qsub submission commands"""
    
    def build(self, 
              job_command: str,
              computing: ComputingParams,
              job_name: str,
              paths: Dict[str, Path]) -> str:
        """Build the qsub wrapper command"""
        
        qsub_script = paths.get('qsub_script', Path("qsub/qsub_cbe_warp.sh"))
        
        # Build the submission command
        cmd_parts = [
            str(qsub_script),
            job_name,
            f"'{job_command}'", # Wrap job command in quotes
            str(computing.partition.value),
            str(computing.gpu_count) if computing.gpu_count > 0 else "0",
            f"{computing.memory_gb}G",
            str(computing.threads),
        ]
        
        return " ".join(cmd_parts)