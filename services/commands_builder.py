# services/command_builders.py
from pathlib import Path
from typing import Dict, Any, List
import shlex
from services.parameter_models import ImportMoviesParams 


class BaseCommandBuilder:
    """Base class for all command builders"""

    def format_paths(self, paths: Dict[str, Path]) -> Dict[str, str]:
        return {k: str(v) for k, v in paths.items()}

    def add_optional_param(self, cmd_parts: List[str], flag: str, value: Any, condition: bool = True):
        if condition and value is not None and str(value) != "None" and str(value) != "":
            cmd_parts.extend([flag, str(value)])


class ImportMoviesCommandBuilder(BaseCommandBuilder):
    """Build import movies command from params"""

    def build(self, params: ImportMoviesParams, paths: Dict[str, Path]) -> str:
        """Build the relion_import command"""
        cmd_parts = [
            "relion_import",
            "--do_movies",
            "--optics_group_name",
            params.optics_group_name,
            "--angpix",
            str(params.pixel_size),
            "--kV",
            str(params.voltage),
            "--Cs",
            str(params.spherical_aberration),
            "--Q0",
            str(params.amplitude_contrast),
            "--dose_per_tilt_image",
            str(params.dose_per_tilt_image),
            "--nominal_tilt_axis_angle",
            str(params.tilt_axis_angle),
        ]

        # Add optional parameters
        if params.invert_defocus_hand:
            cmd_parts.append("--invert_defocus_hand")

        if params.do_at_most > 0:
            cmd_parts.extend(["--do_at_most", str(params.do_at_most)])

        # Add paths
        if "mdoc_dir" in paths:
            # The input glob should be relative to the mdoc dir
            input_pattern = str(paths["mdoc_dir"]) + "/*.mdoc"
            cmd_parts.extend(["--i", input_pattern])

        if "job_dir" in paths:
            # Use job_dir as the output dir
            cmd_parts.extend(["--o", str(paths["job_dir"]) + "/"])

        # Note: 'pipeline_control' is not typically in paths,
        # it's a RELION env var set by schemer.

        return " ".join(cmd_parts)

