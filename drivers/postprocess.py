#!/usr/bin/env python3
"""
Driver for relion_postprocess: the masked gold-standard FSC of a refinement's unfiltered half maps, and their sum
sharpened.

The half maps and the mask must share box and pixel size; that is checked before RELION runs, so a mask made on
another map fails here with both geometries named rather than as a RELION error. The resolution in the last line is
the masked FSC's, and the line says so.
"""

import sys
import traceback
from pathlib import Path

import mrcfile
import starfile

from drivers.driver_base import ToolCommand, get_driver_context, require_producer_input, run_tool, start_supervisor
from services.jobs.postprocess import PostProcessParams

OUTPUT_ROOT = "postprocess"
# Pixel sizes agree when they differ by less than this (Å): headers round differently.
APIX_TOLERANCE = 1e-3


def geometry(path: Path) -> tuple[tuple[int, int, int], float]:
    with mrcfile.open(path, header_only=True, permissive=True) as m:
        h = m.header
        return (int(h.nx), int(h.ny), int(h.nz)), float(m.voxel_size.x)


def second_half(half1: Path) -> Path:
    """RELION's own rule: half2 is half1's name with the last 'half1' replaced."""
    name = half1.name
    i = name.rfind("half1")
    if i < 0:
        raise ValueError(f"{half1.name} has no 'half1' in its name: relion_postprocess cannot find the second half")
    return half1.with_name(name[:i] + "half2" + name[i + len("half1") :])


def main():
    start_supervisor()
    print("--- SLURM JOB START (PostProcess) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(PostProcessParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        half1 = paths["input_half_map"]
        mask = paths["input_mask"]
        half2 = second_half(half1)
        require_producer_input(half1, "Half map 1")
        require_producer_input(half2, "Half map 2")
        require_producer_input(mask, "Mask")

        g1, g2, gm = geometry(half1), geometry(half2), geometry(mask)
        for label, (box, apix) in (("half map 2", g2), ("mask", gm)):
            if box != g1[0] or abs(apix - g1[1]) > APIX_TOLERANCE:
                raise ValueError(
                    f"The {label} is {box} at {apix:g} Å, the half maps {g1[0]} at {g1[1]:g} Å: "
                    "make the mask on a map of this refinement."
                )
        apix = g1[1]
        print(f"[DRIVER] Half maps: {half1}, {half2.name} ({g1[0]} at {apix:g} Å)", flush=True)
        print(f"[DRIVER] Mask: {mask}", flush=True)

        cmd = (
            ToolCommand("relion_postprocess")
            .opt_path("--i", half1, quote=False)
            .opt_path("--o", job_dir / OUTPUT_ROOT, quote=False)
            .opt_path("--mask", mask, quote=False)
            .opt("--angpix", f"{apix:g}")
        )
        if params.auto_bfac:
            cmd.flag("--auto_bfac").opt("--autob_lowres", params.autob_lowres)
        elif params.adhoc_bfac < 0:
            cmd.opt("--adhoc_bfac", params.adhoc_bfac)
        if params.skip_fsc_weighting:
            cmd.flag("--skip_fsc_weighting")
            if params.low_pass > 0:
                cmd.opt("--low_pass", params.low_pass)
        if params.other_args.strip():
            cmd.raw(params.other_args.strip())
        print(f"[DRIVER] Command: {cmd}", flush=True)

        run_tool(
            cmd,
            tool_name=params.get_tool_name(),
            cwd=job_dir,
            binds=[str(half1.parent.resolve()), str(mask.parent.resolve())],
        )

        out_map = job_dir / f"{OUTPUT_ROOT}.mrc"
        out_star = job_dir / f"{OUTPUT_ROOT}.star"
        missing = [p.name for p in (out_map, out_star) if not p.exists()]
        if missing:
            raise RuntimeError(f"relion_postprocess finished without {missing}; see run.out")
        blocks = starfile.read(out_star, always_dict=True)
        general = blocks.get("general")
        if general is None:
            raise RuntimeError(f"{out_star.name} has no data_general block; see run.out")
        # A one-row block reads as a dict of values, a loop as a table.
        row = general.iloc[0] if hasattr(general, "iloc") else general
        resolution = float(row["rlnFinalResolution"])
        bfac = row.get("rlnBfactorUsedForSharpening")
        bfac_txt = f"{float(bfac):g} Å²" if bfac is not None else "none"
        print(f"[DRIVER] Masked gold-standard FSC 0.143: {resolution:.2f} Å · B-factor used: {bfac_txt}", flush=True)
        if params.auto_bfac and resolution > params.autob_lowres:
            print(
                f"[DRIVER] NOTE: the B-factor fit uses shells beyond {params.autob_lowres:g} Å and the map reaches "
                f"{resolution:.1f} Å, so nothing was fitted and the map is not sharpened. Raise autob_lowres above "
                "the resolution, or turn auto_bfac off and give adhoc_bfac.",
                flush=True,
            )
        print(f"[DRIVER] Output: {out_map}", flush=True)
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
