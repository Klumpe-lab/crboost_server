# services/command_builders.py
from pathlib import Path
from typing import Dict, Any, Optional, List
from services.parameter_models import (
    ImportMoviesParams,
    FsMotionCtfParams,
    TsAlignmentParams,
    ComputingParams,
    AlignmentMethod,
)


class BaseCommandBuilder:
    """Base class for all command builders"""

    def format_paths(self, paths: Dict[str, Path]) -> Dict[str, str]:
        """Convert Path objects to strings"""
        return {k: str(v) for k, v in paths.items()}

    def add_optional_param(
        self, cmd_parts: List[str], flag: str, value: Any, condition: bool = True
    ):
        """Add parameter only if condition is met"""
        if (
            condition
            and value is not None
            and str(value) != "None"
            and str(value) != ""
        ):
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
        if "input_dir" in paths:
            # The input glob should be relative to the mdoc dir
            input_pattern = str(paths["input_dir"]) + "/*.mdoc"
            cmd_parts.extend(["--i", input_pattern])

        if "output_dir" in paths:
            cmd_parts.extend(["--o", str(paths["output_dir"])])

        if "pipeline_control" in paths:
            cmd_parts.extend(["--pipeline_control", str(paths["pipeline_control"])])

        return " ".join(cmd_parts)


class FsMotionCtfCommandBuilder(BaseCommandBuilder):
    """Build complete WarpTools motion correction and CTF command with metadata update"""

    def build(self, params: FsMotionCtfParams, paths: Dict[str, Path]) -> str:
        # Step 1: Create settings
        create_settings_parts = [
            "WarpTools create_settings",
            "--folder_data ../../frames",
            "--extension '*.eer'",
            "--folder_processing ./warp_frameseries",
            "--output ./warp_frameseries.settings",
            "--angpix",
            str(params.pixel_size),
            "--eer_ngroups",
            str(params.eer_ngroups),
        ]

        if params.gain_path and params.gain_path != "None":
            create_settings_parts.extend(["--gain_reference", params.gain_path])
            if params.gain_operations and params.gain_operations != "None":
                create_settings_parts.extend(
                    ["--gain_operations", params.gain_operations]
                )

        # Step 2: Run motion correction and CTF estimation
        run_main_parts = [
            "WarpTools fs_motion_and_ctf",
            "--settings ./warp_frameseries.settings",
            "--m_grid", params.m_grid,
            "--m_range_min", str(params.m_range_min),
            "--m_range_max", str(params.m_range_max),
            "--m_bfac", str(params.m_bfac),
            "--c_grid", params.c_grid,
            "--c_window", str(params.c_window),
            "--c_range_min", str(params.c_range_min),
            "--c_range_max", str(params.c_range_max),
            "--c_defocus_min", str(params.defocus_min_angstroms),
            "--c_defocus_max", str(params.defocus_max_angstroms),
            "--c_voltage", str(round(float(params.voltage))),
            "--c_cs", str(params.cs),
            "--c_amplitude", str(params.amplitude),
            "--perdevice", str(params.perdevice),
            "--out_averages",
        ]

        if params.do_at_most > 0:
            run_main_parts.extend(["--do_at_most", str(params.do_at_most)])

        # Join WarpTools commands (these will be containerized)
        warp_commands = " && ".join([
            " ".join(create_settings_parts),
            " ".join(run_main_parts),
        ])
        
        # Metadata update runs AFTER container exits (native Python)
        server_dir = Path(__file__).parent.parent
        helper_script = server_dir / "config" / "binAdapters" / "update_fs_metadata.py"
        
        # Use CRBOOST_PYTHON environment variable if set, otherwise fall back to python3
        metadata_cmd = f"${{CRBOOST_PYTHON:-python3}} {helper_script}"
        
        # IMPORTANT: Use '; ' not ' && ' to run metadata OUTSIDE the container
        # The container service will only wrap up to the first ';'
        full_command = f"{warp_commands} ; {metadata_cmd}"
        
        print(f"[COMMAND BUILDER] Built WarpTools command (containerized) + metadata update (native)")
        return full_command

class TsAlignmentCommandBuilder(BaseCommandBuilder):
    """
    Build complete WarpTools tilt series alignment command.
    This is a 3-step process:
    1. ts_import: Import mdocs and frame series paths into Warp's format.
    2. create_settings: Create a settings file for the tilt series.
    3. ts_aretomo / ts_etomo_patches: Run the actual alignment.
    """

    def build(self, params: TsAlignmentParams, paths: Dict[str, Path]) -> str:
        # Ensure output directories exist
        mkdir_cmds = [
            f"mkdir -p {paths['tomostar_dir']}",
            f"mkdir -p {paths['processing_dir']}",
        ]

        # === Step 1: WarpTools ts_import ===
        cmd_parts_import = [
            "WarpTools ts_import",
            "--mdocs",
            str(paths["mdoc_dir"]),
            "--pattern",
            "*.mdoc",
            "--frameseries",
            str(paths["frameseries_dir"]),
            "--output",
            str(paths["tomostar_dir"]),
            "--tilt_exposure",
            str(params.dose_per_tilt),
            "--override_axis",
            str(params.tilt_axis_angle),
        ]

        # Handle tilt angle inversion
        # Old code: if self.st.tsInfo.keepHand==1: command.append("--dont_invert")
        # We assume keepHand=1 maps to invert_tilt_angles=False
        if not params.invert_tilt_angles:
            cmd_parts_import.append("--dont_invert")

        if params.do_at_most > 0:
            cmd_parts_import.extend(["--do_at_most", str(params.do_at_most)])

        # === Step 2: WarpTools create_settings ===
        cmd_parts_settings = [
            "WarpTools create_settings",
            "--folder_data",
            str(paths["tomostar_dir"]),
            "--extension",
            "*.tomostar",
            "--folder_processing",
            str(paths["processing_dir"]),
            "--output",
            str(paths["settings_file"]),
            "--angpix",
            str(params.pixel_size),  # Original pixel size
            "--exposure",
            str(params.dose_per_tilt),
            "--tomo_dimensions",
            params.tomo_dimensions,
        ]

        # Add optional gain reference
        self.add_optional_param(
            cmd_parts_settings, "--gain_reference", params.gain_path
        )
        self.add_optional_param(
            cmd_parts_settings, "--gain_operations", params.gain_operations
        )

        # === Step 3: WarpTools ts_aretomo / ts_etomo_patches ===
        cmd_parts_align = []

        if params.alignment_method == AlignmentMethod.ARETOMO:
            cmd_parts_align = [
                "WarpTools ts_aretomo",
                "--settings",
                str(paths["settings_file"]),
                "--angpix",
                str(params.rescale_angpixs),  # Target pixel size
                "--alignz",
                str(int(params.thickness_nm * 10)),  # Convert nm to Å
                "--perdevice",
                str(params.perdevice),
                "--patches",
                f"{params.patch_x}x{params.patch_y}",
                "--out_imod",
                str(params.out_imod),
                "--tilt_cor",
                str(params.tilt_cor),
            ]

            # Add axis refinement if iter > 0
            if params.axis_iter > 0:
                cmd_parts_align.extend(
                    [
                        "--axis_iter",
                        str(params.axis_iter),
                        "--axis_batch",
                        str(params.axis_batch),
                    ]
                )

        elif params.alignment_method == AlignmentMethod.IMOD:
            cmd_parts_align = [
                "WarpTools ts_etomo_patches",
                "--settings",
                str(paths["settings_file"]),
                "--angpix",
                str(params.rescale_angpixs),  # Target pixel size
                # Old code: "--patch_size",str(float(self.args.imod_patch_size_and_overlap.split(":")[0])*10)
                # Assuming imod_patch_size is the first part, multiplied by 10
                "--patch_size",
                str(int(params.imod_patch_size * 10)),
                # Note: The 'overlap' parameter doesn't seem to be used by ts_etomo_patches
            ]

        else:
            return f"echo 'ERROR: Alignment method {params.alignment_method} not implemented'; exit 1;"

        if params.do_at_most > 0:
            cmd_parts_align.extend(["--do_at_most", str(params.do_at_most)])

        # === Combine all commands ===
        full_command = " && ".join(
            [
                " ".join(mkdir_cmds),
                " ".join(cmd_parts_import),
                " ".join(cmd_parts_settings),
                " ".join(cmd_parts_align),
            ]
        )

        print(f"[COMMAND BUILDER] Built TsAlignment command")
        return full_command


class TsCtf(BaseCommandBuilder): ...


class TsReconstruct(BaseCommandBuilder): ...


class denoiseTrain(BaseCommandBuilder): ...


class denoiseInfer(BaseCommandBuilder): ...


class templateMatching(BaseCommandBuilder): ...


class tmExtractCandidates(BaseCommandBuilder): ...


class subTomoReconstruction(BaseCommandBuilder): ...
