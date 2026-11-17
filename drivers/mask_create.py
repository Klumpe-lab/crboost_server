#!/usr/bin/env python3
"""
Driver for a solvent mask from a map (relion_mask_create).

The map is low-pass filtered here, so the threshold is a value of the very map that is thresholded: a fraction of
its maximum, or an absolute value when one is given. relion_mask_create then binarises it, grows the mask and
gives it a soft edge. The map's range, the threshold and the fraction of the box the mask encloses go to
mask_stats.json and the log; a mask that encloses almost nothing or almost everything fails the job with those
numbers, since a run would only find out later.
"""

import json
import sys
import traceback
from pathlib import Path

import mrcfile
import numpy as np

from drivers.driver_base import ToolCommand, get_driver_context, require_producer_input, run_tool, start_supervisor
from services.jobs.mask_create import MaskCreateParams

LOWPASS_NAME = "map_lowpass.mrc"
MASK_NAME = "mask.mrc"
STATS_NAME = "mask_stats.json"
# Below and above these fractions of the box a mask is no solvent mask: the threshold is off the map's scale.
MIN_ENCLOSED = 0.002
MAX_ENCLOSED = 0.9
# Width of the raised-cosine edge of the low-pass, in Fourier shells (RELION's filter_edge_width default).
EDGE_SHELLS = 2


def read_map(path: Path) -> tuple[np.ndarray, float]:
    """The map's voxels and its pixel size (Å) from the header; a header without one is refused."""
    with mrcfile.open(path, permissive=True) as m:
        data = np.asarray(m.data, dtype=np.float32).copy()
        apix = float(m.voxel_size.x)
    if data.ndim != 3:
        raise ValueError(f"{path} is not a 3D map (shape {data.shape})")
    if apix <= 0:
        raise ValueError(f"{path} has no pixel size in its header: the low-pass needs it")
    return data, apix


def lowpass(data: np.ndarray, apix: float, resolution_ang: float) -> np.ndarray:
    """`data` low-pass filtered at `resolution_ang`, with a raised-cosine edge EDGE_SHELLS shells wide."""
    nz, ny, nx = data.shape
    fz = np.fft.fftfreq(nz)[:, None, None] * nz
    fy = np.fft.fftfreq(ny)[None, :, None] * ny
    fx = np.fft.rfftfreq(nx)[None, None, :] * nx
    # Radius in shells, scaled so a non-cubic box filters at the same resolution along every axis.
    n = max(nz, ny, nx)
    r = np.sqrt((fz * n / nz) ** 2 + (fy * n / ny) ** 2 + (fx * n / nx) ** 2)
    cutoff = n * apix / resolution_ang
    lo, hi = cutoff - EDGE_SHELLS / 2, cutoff + EDGE_SHELLS / 2
    weight = np.clip((hi - r) / (hi - lo), 0.0, 1.0)
    weight = 0.5 - 0.5 * np.cos(np.pi * weight)
    return np.fft.irfftn(np.fft.rfftn(data) * weight, s=data.shape).astype(np.float32)


def main():
    start_supervisor()
    print("--- SLURM JOB START (Mask) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(MaskCreateParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        input_map = paths["input_map"]
        require_producer_input(input_map, "Input map")
        print(f"[DRIVER] Input map: {input_map}", flush=True)

        data, apix = read_map(input_map)
        lp = lowpass(data, apix, params.lowpass_ang)
        lp_path = job_dir / LOWPASS_NAME
        with mrcfile.new(lp_path, overwrite=True) as m:
            m.set_data(lp)
            m.voxel_size = apix
        lp_max = float(lp.max())
        if params.threshold > 0:
            threshold, source = float(params.threshold), "given"
        else:
            if lp_max <= 0:
                raise ValueError(
                    f"The map low-passed at {params.lowpass_ang:g} Å has no positive value (max {lp_max:.4g}): "
                    "a fraction of its maximum is no threshold. Is the map's contrast inverted?"
                )
            threshold, source = params.threshold_fraction * lp_max, f"{params.threshold_fraction:g} × the maximum"
        print(
            f"[DRIVER] Map {data.shape[::-1]} at {apix:g} Å; low-passed at {params.lowpass_ang:g} Å: "
            f"min {float(lp.min()):.4g}, max {lp_max:.4g}; threshold {threshold:.4g} ({source})",
            flush=True,
        )

        cmd = (
            ToolCommand("relion_mask_create")
            .opt_path("--i", lp_path, quote=False)
            .opt_path("--o", job_dir / MASK_NAME, quote=False)
            .opt("--ini_threshold", f"{threshold:.6g}")
            .opt("--extend_inimask", params.extend_px)
            .opt("--width_soft_edge", params.soft_edge_px)
            .opt("--angpix", f"{apix:g}")
            .opt("--j", 4)
        )
        if params.other_args.strip():
            cmd.raw(params.other_args.strip())
        print(f"[DRIVER] Command: {cmd}", flush=True)
        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=[str(input_map.parent.resolve())])

        mask_path = job_dir / MASK_NAME
        if not mask_path.exists():
            raise RuntimeError(f"relion_mask_create wrote no {MASK_NAME}; see run.out")
        with mrcfile.open(mask_path, permissive=True) as m:
            mask = np.asarray(m.data, dtype=np.float32)
        enclosed = float(mask.mean())
        hard = float((lp >= threshold).mean())
        stats = {
            "input_map": str(input_map),
            "box": list(data.shape[::-1]),
            "apix": apix,
            "lowpass_ang": params.lowpass_ang,
            "map_min": float(data.min()),
            "map_max": float(data.max()),
            "lowpass_min": float(lp.min()),
            "lowpass_max": lp_max,
            "threshold": threshold,
            "threshold_source": source,
            "above_threshold_fraction": hard,
            "enclosed_fraction": enclosed,
            "extend_px": params.extend_px,
            "soft_edge_px": params.soft_edge_px,
        }
        (job_dir / STATS_NAME).write_text(json.dumps(stats, indent=1))
        print(
            f"[DRIVER] Mask encloses {enclosed:.1%} of the box ({hard:.1%} of it above the threshold before "
            f"growing and the soft edge)",
            flush=True,
        )
        if not MIN_ENCLOSED <= enclosed <= MAX_ENCLOSED:
            raise ValueError(
                f"The mask encloses {enclosed:.2%} of the box: threshold {threshold:.4g} against a low-passed map "
                f"from {float(lp.min()):.4g} to {lp_max:.4g}. Choose a threshold inside the map's range."
            )
        print(f"[DRIVER] Output: {mask_path}", flush=True)
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
