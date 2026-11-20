#!/usr/bin/env python3
"""
Driver for RELION's per-tilt CTF refinement of tomograms (relion_tomo_refine_ctf).

The reference is checked before RELION runs: the two half maps, the mask and the FSC must belong together, and the
half maps must be at the tilt series' pixel size, because relion_tomo_refine_ctf pads a smaller reference to the box
and compares it with the tilt images voxel for voxel without reading its pixel size. A binned refinement's half maps
would be compared at the wrong scale and give a fit that is wrong without any error.

After the run every tilt's defocus and astigmatism are compared with what the job read (Warp's fit, or an earlier
refinement's): the per-tilt-series table goes to run.out and `ctf_changes.json`. A tilt whose defocus comes out at or
below zero, that moved more than `max_defocus_change_um` (the search range bounds only RELION's first scan, not where
the fit ends), or whose contrast scale came out negative fails the job, naming the tilts; the refined files stay in the
job directory to be looked at, and nothing downstream reads them.
"""

import json
import sys
import traceback
from pathlib import Path
from statistics import median

from drivers.driver_base import ToolCommand, get_driver_context, require_producer_input, run_tool, start_supervisor
from drivers.postprocess import second_half
from drivers.tomo_reference import box_for, check_reference, read_table, tilt_series_pixel_size, tilt_series_stars
from drivers.warp_bridge import carry_warp_geometry
from services.jobs.ctf_refine import CtfRefineParams
from services.subtomo_merge import _parse_optimisation_set

OUTPUT_SET = "optimisation_set.star"
OUTPUT_TOMOGRAMS = "tomograms.star"
CHANGES_NAME = "ctf_changes.json"
CTF_COLS = ("rlnDefocusU", "rlnDefocusV", "rlnDefocusAngle")
ANGLE_COL = "rlnTomoNominalStageTiltAngle"
NAME_COL = "rlnMicrographName"
SCALE_COL = "rlnCtfScalefactor"


def ctf_of(u: float, v: float, angle: float) -> tuple[float, float, float]:
    """(larger defocus, smaller defocus, axis of the larger mod 180°): the same astigmatism whichever of U and V a
    program calls the larger."""
    if u >= v:
        return u, v, angle % 180.0
    return v, u, (angle + 90.0) % 180.0


def compare(before: dict[str, Path], after: dict[str, Path], limit_ang: float) -> dict:
    """Per tilt series and tilt, the CTF before and after, the moves, and the tilts that fail the checks."""
    series, flagged, unmatched = [], [], 0
    for name, after_star in sorted(after.items()):
        if name not in before:
            unmatched += 1
            continue
        b = read_table(before[name])
        a = read_table(after_star)
        missing = [c for c in (*CTF_COLS, NAME_COL) if c not in a.columns or c not in b.columns]
        if missing:
            raise ValueError(f"{after_star.name} or the star it refines has no {missing}")
        b_rows = {str(r[NAME_COL]): r for _, r in b.iterrows()}
        tilts = []
        for _, r in a.iterrows():
            prev = b_rows.get(str(r[NAME_COL]))
            if prev is None:
                unmatched += 1
                continue
            angle = float(r[ANGLE_COL]) if ANGLE_COL in a.columns else float("nan")
            old = ctf_of(*(float(prev[c]) for c in CTF_COLS))
            new = ctf_of(*(float(r[c]) for c in CTF_COLS))
            move = max(abs(new[0] - old[0]), abs(new[1] - old[1]))
            tilt = {
                "angle": angle,
                "before": old,
                "after": new,
                "mean_move_ang": (new[0] + new[1]) / 2 - (old[0] + old[1]) / 2,
                "move_ang": move,
                "scale": float(r[SCALE_COL]) if SCALE_COL in a.columns else None,
            }
            tilts.append(tilt)
            why = []
            if min(float(r["rlnDefocusU"]), float(r["rlnDefocusV"])) <= 0:
                why.append(f"defocus {new[1] / 1e4:.3f} µm")
            if limit_ang > 0 and move > limit_ang:
                why.append(f"moved {move / 1e4:.2f} µm")
            if tilt["scale"] is not None and tilt["scale"] < 0:
                why.append(f"contrast scale {tilt['scale']:.2f}")
            if why:
                flagged.append({"tilt_series": name, "angle": angle, "why": ", ".join(why)})
        if not tilts:
            continue
        worst = max(tilts, key=lambda t: abs(t["mean_move_ang"]))
        scales = [t["scale"] for t in tilts if t["scale"] is not None]
        series.append(
            {
                "name": name,
                "tilts": tilts,
                "median_move_um": median(abs(t["mean_move_ang"]) for t in tilts) / 1e4,
                "largest_move_um": worst["mean_move_ang"] / 1e4,
                "largest_move_angle": worst["angle"],
                "astig_before_um": median(t["before"][0] - t["before"][1] for t in tilts) / 1e4,
                "astig_after_um": median(t["after"][0] - t["after"][1] for t in tilts) / 1e4,
                "mean_defocus_before_um": median((t["before"][0] + t["before"][1]) / 2 for t in tilts) / 1e4,
                "mean_defocus_after_um": median((t["after"][0] + t["after"][1]) / 2 for t in tilts) / 1e4,
                "scale_range": [min(scales), max(scales)] if scales else None,
            }
        )
    return {"tilt_series": series, "flagged": flagged, "unmatched": unmatched}


def print_table(changes: dict) -> None:
    print("[DRIVER] Per tilt series (µm; defocus = mean of U and V; astigmatism = |U − V|, medians over the tilts):")
    for s in changes["tilt_series"]:
        scale = s["scale_range"]
        scale_txt = f" · scale {scale[0]:.2f}–{scale[1]:.2f}" if scale else ""
        print(
            f"[DRIVER]   {s['name']}: {len(s['tilts'])} tilts · defocus {s['mean_defocus_before_um']:.3f} → "
            f"{s['mean_defocus_after_um']:.3f} · moved median {s['median_move_um']:.3f}, largest "
            f"{s['largest_move_um']:+.3f} at {s['largest_move_angle']:+.0f}° · "
            f"astigmatism {s['astig_before_um']:.3f} → {s['astig_after_um']:.3f}{scale_txt}",
            flush=True,
        )
    if changes["unmatched"]:
        print(f"[DRIVER] {changes['unmatched']} tilt series or tilts had no counterpart in the input", flush=True)


def main():
    start_supervisor()
    print("--- SLURM JOB START (CTF refine) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(CtfRefineParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        problems = params.run_problems()
        if problems:
            raise ValueError("; ".join(problems))
        paths = {k: Path(v) for k, v in context["paths"].items()}
        input_set = paths["input_optimisation"]
        half1 = paths["input_half_map"]
        half2 = second_half(half1)
        mask = paths["input_mask"]
        fsc = paths["input_fsc"]
        for path, label in (
            (input_set, "Optimisation set"),
            (half1, "Half map 1"),
            (half2, "Half map 2"),
            (mask, "Mask"),
            (fsc, "FSC (postprocess.star)"),
        ):
            require_producer_input(path, label)

        # RELION cuts the particles from the tilt images: it must place them by Warp's whole model (H26)
        input_set = carry_warp_geometry(input_set, job_dir)
        particles_star, tomograms_star = _parse_optimisation_set(input_set)
        require_producer_input(particles_star, "Particles star")
        require_producer_input(tomograms_star, "Tomograms star")
        ts_apix = tilt_series_pixel_size(tomograms_star)

        ref = check_reference(half1, half2, mask, fsc, ts_apix, "relion_tomo_refine_ctf")
        box = box_for(ref, params.box_size)

        print(f"[DRIVER] Optimisation set: {input_set}", flush=True)
        print(f"[DRIVER]   particles {particles_star}", flush=True)
        print(f"[DRIVER]   tomograms {tomograms_star} (tilt images at {ts_apix:g} Å/px)", flush=True)
        print(f"[DRIVER] Reference: {ref.describe()}", flush=True)
        print(f"[DRIVER] Mask: {mask}", flush=True)
        print(f"[DRIVER] Box: {box} px ({box * ts_apix:.0f} Å)", flush=True)

        before = tilt_series_stars(tomograms_star)

        r = params.defocus_range_ang
        cmd = (
            ToolCommand("relion_tomo_refine_ctf")
            .opt_path("--i", input_set, quote=False)
            .opt_path("--ref1", half1, quote=False)
            .opt_path("--ref2", half2, quote=False)
            .opt_path("--mask", mask, quote=False)
            .opt_path("--fsc", fsc, quote=False)
            .opt_path("--o", f"{job_dir}/", quote=False)
            .opt("--b", box)
            .opt("--j", params.threads)
        )
        if params.do_defocus:
            cmd.flag("--do_defocus").opt("--d0", f"{-r:g}").opt("--d1", f"{r:g}").opt("--ds", params.defocus_steps)
            cmd.opt("--kmin", f"{params.kmin_ang:g}")
            if params.regularise:
                cmd.flag("--do_reg_defocus").opt("--lambda", f"{params.reg_lambda:g}")
            if params.reset_to_common:
                cmd.flag("--reset_to_common")
        if params.do_scale:
            cmd.flag("--do_scale")
            if params.per_frame_scale:
                cmd.flag("--per_frame_scale")
            if params.per_tomogram_scale:
                cmd.flag("--per_tomogram_scale")
        if params.even_aberrations:
            cmd.flag("--do_even_aberrations").opt("--ne", params.even_order)
        if params.odd_aberrations:
            cmd.flag("--do_odd_aberrations").opt("--no", params.odd_order)
        if params.other_args.strip():
            cmd.raw(params.other_args.strip())
        print(f"[DRIVER] Command: {cmd}", flush=True)

        binds = {p.parent.resolve() for p in (input_set, particles_star, tomograms_star, half1, mask, fsc)}
        binds |= {p.parent.resolve() for p in before.values()}
        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=sorted(str(b) for b in binds))

        out_set, out_tomograms = job_dir / OUTPUT_SET, job_dir / OUTPUT_TOMOGRAMS
        missing = [p.name for p in (out_set, out_tomograms) if not p.exists()]
        if missing:
            raise RuntimeError(f"relion_tomo_refine_ctf finished without {missing}; see run.out")
        out_particles, out_tomo_ref = _parse_optimisation_set(out_set)
        print(f"[DRIVER] Output set: particles {out_particles}, tomograms {out_tomo_ref}", flush=True)

        limit_ang = params.max_defocus_change_um * 1e4
        changes = compare(before, tilt_series_stars(out_tomograms), limit_ang)
        changes.update(
            box=box,
            pixel_size=ts_apix,
            reference=str(half1),
            fsc_resolution=ref.fsc_resolution,
            limit_um=params.max_defocus_change_um,
            defocus=params.do_defocus,
            scale=params.do_scale,
        )
        (job_dir / CHANGES_NAME).write_text(json.dumps(changes, indent=1))
        print_table(changes)
        if not changes["tilt_series"]:
            raise RuntimeError("No refined tilt series matched the input's: nothing to compare; see run.out")
        if changes["flagged"]:
            shown = "; ".join(f"{f['tilt_series']} {f['angle']:+.0f}°: {f['why']}" for f in changes["flagged"][:12])
            ref_txt = f" ({ref.fsc_resolution:.1f} Å, masked FSC 0.143)" if ref.fsc_resolution is not None else ""
            more = len(changes["flagged"]) - 12
            raise ValueError(
                f"{len(changes['flagged'])} tilts failed the checks (defocus above zero, a move of at most "
                f"{params.max_defocus_change_um:g} µm, a contrast scale of at least zero): {shown}"
                f"{f'; and {more} more' if more > 0 else ''}. First look at the reference"
                f"{ref_txt}: past its "
                "resolution the fit has nothing to hold on to. Then regularise across tilts, narrow the "
                "search range or refine the scale over the dose instead of per tilt; raise the limit if a move is real"
            )
        print(f"[DRIVER] Output: {out_set}", flush=True)
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
