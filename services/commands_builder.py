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
            input_pattern = str(paths['input_dir']) + "/*.mdoc"
            cmd_parts.extend(["--i", input_pattern])
        
        if 'output_dir' in paths:
            cmd_parts.extend(["--o", str(paths['output_dir'])])
        
        if 'pipeline_control' in paths:
            cmd_parts.extend(["--pipeline_control", str(paths['pipeline_control'])])
        
        return " ".join(cmd_parts)

class FsMotionCtfCommandBuilder(BaseCommandBuilder):
    """Build WarpTools fs_motion_and_ctf command"""
    
    def build(self, params: FsMotionCtfParams, paths: Dict[str, Path]) -> str:
        """Build the WarpTools command"""
        
        # Base command
        cmd_parts = ["WarpTools", "fs_motion_and_ctf"]
        
        # Input/output paths
        if 'input_star' in paths:
            cmd_parts.extend(["--input_star", str(paths['input_star'])])
        
        if 'output_star' in paths:
            cmd_parts.extend(["--output_star", str(paths['output_star'])])
        
        # Core parameters
        cmd_parts.extend([
            "--angpix", str(params.pixel_size),
            "--voltage", str(params.voltage),
            "--cs", str(params.cs),
            "--amplitude", str(params.amplitude),
            "--eer_ngroups", str(params.eer_ngroups),
        ])
        
        # CTF parameters
        cmd_parts.extend([
            "--window", str(params.window),
            "--range_min", str(params.range_min),
            "--range_max", str(params.range_max),
            "--defocus_min", str(params.defocus_min),
            "--defocus_max", str(params.defocus_max),
        ])
        
        # Optional: limit processing
        if params.do_at_most > 0:
            cmd_parts.extend(["--do_at_most", str(params.do_at_most)])
        
        # Gain reference if provided
        if 'gain_reference' in paths and paths['gain_reference']:
            cmd_parts.extend(["--gain_reference", str(paths['gain_reference'])])
        
        return " ".join(cmd_parts)

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
            cmd_parts.extend(["-InMrc", str(paths['input_star'])])
        
        if 'output_dir' in paths:
            cmd_parts.extend(["-OutMrc", str(paths['output_dir']) + "/aligned.mrc"])
        
        # Core parameters
        cmd_parts.extend([
            "-OutBin", str(params.binning),
            "-VolZ", str(int(params.thickness_nm / 10)),  # Convert nm to pixels at ~10Å/pixel
            "-TiltCor", str(params.tilt_cor),
            "-OutImod", str(params.out_imod),
        ])
        
        # Patch tracking
        cmd_parts.extend([
            "-Patch", f"{params.patch_x} {params.patch_y}",
        ])
        
        # GPU selection if available
        if 'gpu_id' in paths:
            cmd_parts.extend(["-Gpu", str(paths['gpu_id'])])
        
        return " ".join(cmd_parts)
    
    def _build_imod_command(self, params: TsAlignmentParams, paths: Dict[str, Path]) -> str:
        """Build IMOD alignment command"""
        # Placeholder for IMOD command building
        return f"etomo --binning {params.binning}"
    
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
            job_command,
            str(computing.partition.value),
            str(computing.gpu_count) if computing.gpu_count > 0 else "0",
            f"{computing.memory_gb}G",
            str(computing.threads),
        ]
        
        return " ".join(cmd_parts)