#!/usr/bin/env python
# drivers/ts_alignment.py
import sys
import os
import shlex
import pandas as pd
from pathlib import Path
import traceback

server_dir = Path(__file__).parent.parent
sys.path.append(str(server_dir))

from drivers.driver_base import get_driver_context, run_command
from services.project_state import AlignmentMethod, TsAlignmentParams
from services.metadata_service import MetadataTranslator
from services.starfile_service import StarfileService
from services.container_service import get_container_service


def build_alignment_commands(params: TsAlignmentParams, paths: dict[str, Path], num_tomograms: int, job_dir: Path) -> str:
    """
    Builds the multi-step WarpTools alignment command string.
    
    FIX applied: 
    1. Outputs (--output) use RELATIVE paths to prevent Warp from nesting paths incorrectly.
    2. Inputs/References (--folder_data) use ABSOLUTE paths so the generated XML is valid globally.
    """

    gain_path_str = ""
    if params.gain_path and params.gain_path != "None":
        gain_path_str = shlex.quote(params.gain_path)

    gain_ops_str = params.gain_operations if params.gain_operations else ""

    # --- HYBRID PATH CALCULATION ---
    try:
        # Settings Output: Must be RELATIVE (e.g., ../../warp_tiltseries.settings)
        settings_abs = paths["warp_tiltseries_settings"]
        settings_rel = os.path.relpath(settings_abs, job_dir)
        settings_file_out = shlex.quote(settings_rel)

        # Tomostar Output: Must be RELATIVE (e.g., ../../tomostar)
        tomostar_abs = paths["tomostar_dir"]
        tomostar_rel = os.path.relpath(tomostar_abs, job_dir)
        tomostar_dir_out = shlex.quote(tomostar_rel)
        
        # Tomostar Reference: Must be ABSOLUTE for the XML (e.g., /users/.../tomostar)
        # This ensures that when ts_aretomo reads the settings file from ProjectRoot, 
        # it finds the folder correctly.
        tomostar_dir_ref = shlex.quote(str(tomostar_abs))
        
    except ValueError:
        # Fallback for Windows/Edge cases
        settings_file_out = shlex.quote(str(paths["warp_tiltseries_settings"]))
        tomostar_dir_out = shlex.quote(str(paths["tomostar_dir"]))
        tomostar_dir_ref = tomostar_dir_out

    # Input paths (mdoc, processing) are inputs, so Absolute is safe and preferred
    mdoc_dir = shlex.quote(str(paths["mdoc_dir"]))
    
    # Handle optional paths
    input_processing_raw = paths.get("input_processing")
    input_processing = shlex.quote(str(input_processing_raw)) if input_processing_raw and str(input_processing_raw) != "None" else ""
    output_processing = shlex.quote(str(paths["output_processing"])) 

    # === Step 1: ts_import ===
    # OUTPUT: Use RELATIVE (tomostar_dir_out) to avoid path nesting bug
    cmd_parts_import = [
        "WarpTools ts_import",
        "--mdocs", mdoc_dir,
        "--pattern", shlex.quote(params.mdoc_pattern),
        "--frameseries", input_processing if input_processing else shlex.quote(str(paths.get("frameseries_dir", ""))),
        "--output", tomostar_dir_out, 
        "--tilt_exposure", str(params.dose_per_tilt),
        "--override_axis", str(params.tilt_axis_angle),
    ]

    if not params.invert_tilt_angles:
        cmd_parts_import.append("--dont_invert")

    if params.do_at_most > 0:
        cmd_parts_import.extend(["--do_at_most", str(params.do_at_most)])

    # === Step 2: Create master settings ===
    # OUTPUT: Use RELATIVE (settings_file_out) to avoid path nesting bug
    # INPUT:  Use ABSOLUTE (tomostar_dir_ref) so the XML contains the full path
    cmd_parts_settings = [
        "WarpTools create_settings",
        "--folder_data", tomostar_dir_ref,
        "--extension '*.tomostar'",
        "--folder_processing", output_processing, 
        "--output", settings_file_out,
        "--angpix", str(params.pixel_size),
        "--exposure", str(params.dose_per_tilt),
        "--tomo_dimensions", params.tomo_dimensions,
    ]
    if gain_path_str:
        cmd_parts_settings.extend(["--gain_reference", gain_path_str])
        if gain_ops_str:
            cmd_parts_settings.extend(["--gain_operations", gain_ops_str])

    # === Step 3: Alignment ===
    # We reference the settings file via the relative path, which works fine 
    # for locating the file itself.
    if params.alignment_method == AlignmentMethod.ARETOMO:
        cmd_parts_align = [
            "WarpTools ts_aretomo",
            f"--settings {settings_file_out}",
            f"--input_processing {input_processing}" if input_processing else "",
            f"--output_processing {output_processing}",
            "--angpix", str(params.rescale_angpixs),
            "--alignz", str(int(params.sample_thickness_nm * 10)),
            "--perdevice", str(params.perdevice),
        ]
        if params.patch_x > 0 and params.patch_y > 0:
            cmd_parts_align.extend(["--patches", f"{params.patch_x}x{params.patch_y}"])
        if params.axis_iter > 0:
            batch_size = min(params.axis_batch, num_tomograms)
            cmd_parts_align.extend(["--axis_iter", str(params.axis_iter)])
            cmd_parts_align.extend(["--axis_batch", str(batch_size)])

    elif params.alignment_method == AlignmentMethod.IMOD:
        cmd_parts_align = [
            "WarpTools ts_etomo_patches",
            f"--settings {settings_file_out}",
            f"--input_processing {input_processing}" if input_processing else "",
            f"--output_processing {output_processing}",
            "--angpix", str(params.rescale_angpixs),
            "--patch_size", str(int(params.imod_patch_size * 10)),
        ]
    else:
        return f"echo 'ERROR: Alignment method {params.alignment_method} not implemented'; exit 1;"

    if params.do_at_most > 0:
        cmd_parts_align.extend(["--do_at_most", str(params.do_at_most)])

    # Clean up commands
    cmd_parts_align = [part for part in cmd_parts_align if part] 

    # Logic:
    # 1. If tomostar dir exists AND has files, skip import. Else run import.
    # 2. If settings file exists, skip creation. Else run create.
    # 3. Always run alignment.
    return " && ".join([
        f"test -d {tomostar_dir_out} && ls {tomostar_dir_out}/*.tomostar >/dev/null 2>&1 || ({' '.join(cmd_parts_import)})",
        f"test -f {settings_file_out} || ({' '.join(cmd_parts_settings)})", 
        ' '.join(cmd_parts_align)
    ])


def main():
    try:
        (
            project_state,
            params, 
            local_params_data, 
            job_dir,
            project_path,
            job_type,
        ) = get_driver_context()
        
    except Exception as e:
        job_dir = Path.cwd()
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Load paths
        print(f"[DRIVER] Params loaded for job type {job_type} in {job_dir}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        input_star_abs = paths["input_star"]
        output_star_abs = paths["output_star"]

        star_data = StarfileService().read(input_star_abs)
        tilt_series_df = star_data.get("global", pd.DataFrame())
        num_tomograms = len(tilt_series_df)
        print(f"[DRIVER] Found {num_tomograms} tomograms in input STAR file", flush=True)

        # 2. Build and run alignment commands
        # Pass job_dir to allow relative path calculation
        align_command_str = build_alignment_commands(params, paths, num_tomograms, job_dir)

        # 3. Execute
        container_svc = get_container_service()
        apptainer_command = container_svc.wrap_command_for_tool(
            command=align_command_str, cwd=job_dir, tool_name="warptools", additional_binds=additional_binds
        )

        print(f"[DRIVER] Command: {align_command_str}", flush=True)
        print("[DRIVER] Executing container...", flush=True)
        run_command(apptainer_command, cwd=job_dir)

        # 4. Metadata
        print("[DRIVER] Alignment finished. Starting metadata processing.", flush=True)
        translator = MetadataTranslator(StarfileService())

        result = translator.update_ts_alignment_metadata(
            job_dir=job_dir,
            input_star_path=input_star_abs,
            output_star_path=output_star_abs,
            project_root=project_path,  
            tomo_dimensions=params.tomo_dimensions,
            alignment_method=params.alignment_method.value,
        )

        if not result["success"]:
            raise Exception(f"Metadata update failed: {result['error']}")

        print("[DRIVER] Metadata processing successful.", flush=True)

        success_file.touch()
        print("[DRIVER] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(str(e), file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("[DRIVER] Job failed.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
