#!/usr/bin/env python3
"""
Filament pick driver: one CPU job over every tomogram of a Segment job (no array).

1. Reads the Segment job's segmentations.json: the chosen feature's volume of each tomogram, its grid and
   its voxel size, and the tomograms star the Segment job read (the frame the points live in).
2. Links each volume into ais/in/ and runs `ais pick -filament` once, with one process per CPU, into
   ais/out/. Ais's own `_coords.star` files stay there as the raw record.
3. Turns them into candidate rows (`services/particles/filament_picks.py`) and writes filaments_all.star
   (every point, with its mark), candidates.star (the unmarked points), tomograms.star,
   optimisation_set.star and summary.json.
"""

import json
import os
import shlex
import shutil
import sys
import traceback
from pathlib import Path

import mrcfile
import numpy as np
import pandas as pd
import starfile

from drivers.array_job_base import load_excluded_ts
from drivers.driver_base import (
    DriverContext,
    ToolCommand,
    print_cmd_only,
    require_producer_input,
    run_tool,
    start_supervisor,
)
from services.jobs.filament_pick import FilamentPickParams
from services.particles.coords import pixel_size_from_tomo_row
from services.particles.filament_picks import MarkRules, candidates_of, filament_points, finish_table, tomogram_summary
from services.segmentation.manifest import read_manifest
from services.subtomo_merge import _read_tomograms_star, write_optimisation_set

AIS_DIR = "ais"
# Ais compares the stored value with this (halved for int8 volumes): p > 0.5 on p × 127 int8 and p × 255.
AIS_THRESHOLD = 128


def log(msg: str) -> None:
    print(f"[DRIVER] {msg}", flush=True)


def stored_threshold_p(volume: Path, full_scale: float) -> float:
    """The probability Ais's threshold means on this volume: it compares stored values with 128, or 64 for
    int8."""
    with mrcfile.open(volume, header_only=True, permissive=True) as m:
        dtype = mrcfile.utils.data_dtype_from_header(m.header)
    threshold = AIS_THRESHOLD / 2 if dtype == np.int8 else AIS_THRESHOLD
    return threshold / full_scale


def read_ais_trace(path: Path) -> pd.DataFrame:
    """One tomogram's Ais output; a tomogram where Ais traced nothing has a star with no rows."""
    blocks = starfile.read(path, always_dict=True)
    for block in blocks.values():
        if isinstance(block, pd.DataFrame):
            return block
    return pd.DataFrame()


def main():
    start_supervisor()
    print("--- SLURM JOB START (Filament pick) ---", flush=True)
    try:
        ctx = DriverContext.load(FilamentPickParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        run(ctx)
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


def run(ctx: DriverContext[FilamentPickParams]) -> None:
    p = ctx.params
    job_dir = ctx.job_dir
    seg_json = ctx.paths["input_segmentations"]
    require_producer_input(seg_json, "Input segmentations")
    manifest = read_manifest(seg_json.parent)
    log(f"Segmentations: {seg_json} ({manifest.method}, {len(manifest.entries)} volumes)")

    entries = [e for e in manifest.entries if e.feature == p.feature]
    if not entries:
        raise ValueError(f"{seg_json} has no volumes of {p.feature!r}; it holds {manifest.features()}")
    excluded = load_excluded_ts(ctx.project_path)
    skipped = sorted(e.tomogram for e in entries if e.tomogram in excluded)
    entries = sorted((e for e in entries if e.tomogram not in excluded), key=lambda e: e.tomogram)
    if skipped:
        log(f"Excluded from processing, not traced: {', '.join(skipped)}")
    if not entries:
        raise ValueError(f"Every tomogram with a {p.feature} volume is excluded from processing")

    tomograms_star = Path(str(manifest.parameters.get("input_tomograms") or ""))
    if not tomograms_star.name:
        raise ValueError(f"{seg_json} does not name the tomograms star it segmented (input_tomograms)")
    require_producer_input(tomograms_star, "The Segment job's input tomograms STAR")
    tomo_df = _read_tomograms_star(tomograms_star)
    rows = tomo_df.set_index(tomo_df["rlnTomoName"].astype(str))
    absent = [e.tomogram for e in entries if e.tomogram not in rows.index]
    if absent:
        raise ValueError(f"{tomograms_star} has no rows for {absent[:10]}")
    for e in entries:
        apix = pixel_size_from_tomo_row(rows.loc[e.tomogram])
        if abs(apix - e.voxel_angstrom) > 0.01 * e.voxel_angstrom:
            raise ValueError(
                f"{e.tomogram}: the segmentation's voxel is {e.voxel_angstrom:g} Å but its tomogram row says "
                f"{apix:g} Å; the points would land in the wrong place"
            )
        if not print_cmd_only():
            p_cut = stored_threshold_p(e.path, e.full_scale)
            if abs(p_cut - 0.5) > 0.02:
                raise ValueError(
                    f"{e.path.name}: Ais's threshold would mean p > {p_cut:.2f} on this volume (full scale "
                    f"{e.full_scale:g}), not p > 0.5"
                )
    log(f"{len(entries)} tomogram(s) with a {p.feature} volume; frame: {tomograms_star}")

    ais_dir = job_dir / AIS_DIR
    if ais_dir.exists():
        shutil.rmtree(ais_dir)
    in_dir, out_dir = ais_dir / "in", ais_dir / "out"
    for d in (in_dir, out_dir, ais_dir / ".config", ais_dir / ".mpl"):
        d.mkdir(parents=True)
    for e in entries:
        (in_dir / f"{e.tomogram}__{p.feature}.mrc").symlink_to(e.path.resolve())

    processes = int(os.environ.get("SLURM_CPUS_PER_TASK") or p.get_effective_slurm_config().cpus_per_task)
    env = " ".join(
        f"{k}={shlex.quote(str(v))}"
        for k, v in (("AIS_CONFIG_DIR", ais_dir / ".config"), ("MPLCONFIGDIR", ais_dir / ".mpl"))
    )
    cmd = (
        ToolCommand(f"{env} ais pick")
        .opt_path("-d", in_dir, quote=True)
        .opt("-t", shlex.quote(p.feature))
        .opt_path("-ou", out_dir, quote=True)
        .flag("-filament")
        .opt("-threshold", AIS_THRESHOLD)
        .opt("-b", p.binning)
        .opt("-spacing", f"{p.spacing_ang:g}")
        .opt("-size", f"{p.min_volume_nm3 * 1000:.0f}")
        .opt("-length", f"{p.min_length_ang:g}")
        .opt("-p", max(1, processes))
    )
    binds = sorted({str(e.path.resolve().parent) for e in entries})
    run_tool(cmd, tool_name=p.get_tool_name(), cwd=job_dir, binds=[*ctx.additional_binds, *binds])
    if print_cmd_only():
        return

    rules = MarkRules(
        spacing_ang=p.spacing_ang,
        min_length_ang=p.min_length_ang,
        min_curvature_radius_ang=p.min_curvature_radius_ang,
        min_angle_from_z_deg=p.min_angle_from_z_deg,
    )
    blocks, no_output = [], []
    for e in entries:
        trace = out_dir / f"{e.tomogram}__{p.feature}_coords.star"
        if not trace.is_file():
            no_output.append(e.tomogram)
            continue
        blocks.append(
            filament_points(
                read_ais_trace(trace),
                tomo=e.tomogram,
                shape_zyx=e.shape_zyx,
                voxel_angstrom=e.voxel_angstrom,
                binning=p.binning,
                rules=rules,
            )
        )
    if no_output:
        log(f"WARNING: Ais wrote no trace for {len(no_output)} tomogram(s): {', '.join(no_output)}")
    blocks = [b for b in blocks if not b.empty]
    table = finish_table(pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame(columns=["rlnTomoName"]))
    candidates = candidates_of(table) if not table.empty else table

    starfile.write({"particles": table}, job_dir / "filaments_all.star", overwrite=True)
    per_tomo = tomogram_summary(table) if not table.empty else {}
    summary = {
        "feature": p.feature,
        "segmentations": str(seg_json),
        "tomograms_star": str(tomograms_star),
        "tomograms_traced": len(entries),
        "tomograms_with_filaments": len(per_tomo),
        "filaments": sum(t["filaments"] for t in per_tomo.values()),
        "points": len(table),
        "candidate_points": len(candidates),
        "ais_tangent_diff_deg_median": float(np.median(table["cbAisTangentDiffDeg"])) if len(table) else None,
        "no_ais_output": no_output,
        "excluded": skipped,
        "per_tomogram": per_tomo,
    }
    (job_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    log(
        f"{summary['points']} points in {summary['filaments']} filaments in {summary['tomograms_with_filaments']} "
        f"tomograms; {summary['candidate_points']} unmarked"
    )
    if summary["ais_tangent_diff_deg_median"] is not None:
        log(f"Direction vs Ais's own tilt/psi: median {summary['ais_tangent_diff_deg_median']:.1f}°")
    if candidates.empty:
        raise RuntimeError("No unmarked filament points: see filaments_all.star (cbMarkReason) and summary.json")

    starfile.write({"particles": candidates}, job_dir / "candidates.star", overwrite=True)
    shutil.copy2(tomograms_star, job_dir / "tomograms.star")
    write_optimisation_set(
        job_dir / "optimisation_set.star",
        particles_star=job_dir / "candidates.star",
        tomograms_star=job_dir / "tomograms.star",
    )
    log("Wrote candidates.star, tomograms.star and optimisation_set.star")


if __name__ == "__main__":
    main()
