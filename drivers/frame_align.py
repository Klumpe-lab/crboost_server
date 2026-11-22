#!/usr/bin/env python3
"""
Driver for RELION's frame alignment of tomograms (relion_tomo_align; roadmap 53 §15.10 T3, §15.11).

Per tilt series RELION refines each tilt's rigid alignment (angles, shifts), the particles' positions and, with
motion on, each particle's motion over the tilts, against a reference at the tilt series' pixel size (checked as for
CTF refine: drivers/tomo_reference.py). What it writes is RELION's whole model: the trajectories it reads only centre
its search windows and its motion starts from zero, so Warp's local geometry (the bridge's offsets) is replaced, not
refined. Without motion RELION's own optimisation set still names the trajectories it read, which belong to the old
rigid model: that set is never handed on.

After the run the driver writes the set downstream jobs read, beside RELION's files (`assemble`):
- a tilt series with fewer than `min_particles` particles keeps what it came with: its tilt-series star, its
  particles' rows and their trajectories (RELION's fit of one or two particles' tilt series ran up to 81 Å and 6°);
- the tomograms star is marked (services/particles/warp_geometry.GEOMETRY_COL): `relion` for the refined tilt series,
  with the star their motion belongs to, the input's marker or `warp` for the others; the bridge hands such a set on
  as it is;
- every particle has a trajectory (RELION's motion, the kept ones' own, else zeros), so RELION never mixes models;
- the particles name no images: their stacks were cut with the old model, and Subtomo Extraction cuts them anew.
What moved, per tilt series, goes to run.out and `frame_align_changes.json`.
"""

import json
import math
import shutil
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from drivers.driver_base import ToolCommand, get_driver_context, require_producer_input, run_tool, start_supervisor
from drivers.postprocess import second_half
from drivers.tomo_reference import box_for, check_reference, read_table, tilt_series_pixel_size, tilt_series_stars
from drivers.warp_bridge import carry_warp_geometry
from services.jobs.frame_align import FrameAlignParams
from services.particles import warp_geometry as wg
from services.subtomo_merge import (
    _parse_optimisation_set,
    _read_input_particles_lenient,
    _read_tomograms_star,
    _write_particles_star,
    _write_tomograms_star,
    optimisation_set_trajectories,
    write_optimisation_set,
)

RELION_SET = "optimisation_set.star"
RELION_MOTION = "motion.star"
TEMP_DIR = "temp"
COMMAND_RECORD = "crboost_command.txt"  # in temp/: the command whose per-tomogram results temp/ holds
OUT_SET = "aligned_optimisation_set.star"
OUT_PARTICLES = "aligned_particles.star"
OUT_TOMOGRAMS = "aligned_tomograms.star"
OUT_MOTION = "aligned_motion.star"
CHANGES_NAME = "frame_align_changes.json"
NAME = "rlnTomoParticleName"
TOMO = "rlnTomoName"
COORDS = [f"rlnCenteredCoordinate{a}Angst" for a in "XYZ"]
ORIGINS = [f"rlnOrigin{a}Angst" for a in "XYZ"]
SHIFTS = ("rlnTomoXShiftAngst", "rlnTomoYShiftAngst")
ANGLES = ("rlnTomoXTilt", "rlnTomoYTilt", "rlnTomoZRot")

Block = list[tuple[float, float, float]]


# ── trajectories files (RELION's Trajectory format: data_general, then one block of per-tilt offsets per particle)


def read_trajectories(path: Path) -> dict[str, Block]:
    """Each particle's per-tilt offsets (Å), by particle name."""
    out: dict[str, Block] = {}
    current: Block | None = None
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if line.startswith("data_"):
                name = line[5:]
                current = None if name == "general" else out.setdefault(name, [])
            elif current is not None and line and not line.startswith(("_", "loop_", "#")):
                x, y, z = (float(v) for v in line.split()[:3])
                current.append((x, y, z))
    return out


def write_trajectories(path: Path, names: list[str], blocks: dict[str, Block]) -> None:
    with open(path, "w") as f:
        f.write(f"# version 50001\n\ndata_general\n\n_rlnParticleNumber {len(names)}\n\n")
        for name in names:
            f.write(f"# version 50001\n\ndata_{name}\n\nloop_\n")
            f.write("_rlnOriginXAngst #1\n_rlnOriginYAngst #2\n_rlnOriginZAngst #3\n")
            for x, y, z in blocks[name]:
                f.write(f"{x:.6f} {y:.6f} {z:.6f}\n")
            f.write("\n")


def rms(values: list[float]) -> float:
    return math.sqrt(sum(v * v for v in values) / len(values)) if values else 0.0


def block_rms(blocks: dict[str, Block], names: list[str]) -> float | None:
    vals = [math.sqrt(x * x + y * y + z * z) for n in names if n in blocks for x, y, z in blocks[n]]
    return rms(vals) if vals else None


def rigid_changes(before: Path, after: Path) -> dict:
    """Per tilt of one tilt series, the rigid model after against before: shift moves (Å) and angle moves (°)."""
    b, a = read_table(before), read_table(after)
    by_name = b.set_index(b["rlnMicrographName"].astype(str))
    shift, angle = [], []
    for _, t in a.iterrows():
        prev = by_name.loc[str(t["rlnMicrographName"])]
        shift.append(math.hypot(*(float(t[c]) - float(prev[c]) for c in SHIFTS)))
        angle.append(max(abs(float(t[c]) - float(prev[c])) for c in ANGLES))
    if any(not math.isfinite(v) for v in shift + angle):
        raise ValueError(f"{after} holds a rigid model that is not a number: RELION's fit failed there")
    return {
        "tilts": len(a),
        "shift_rms_ang": rms(shift),
        "shift_max_ang": max(shift, default=0.0),
        "angle_max_deg": max(angle, default=0.0),
    }


# ── what went in, and what is handed on


@dataclass
class Input:
    """The set RELION reads: its particles, tomograms, tilt-series stars and trajectories, split by `min_particles`."""

    particles_star: Path
    tomograms_star: Path
    trajectories: Path | None
    particles: pd.DataFrame
    tomograms: pd.DataFrame
    tilt_stars: dict[str, Path]
    counts: dict[str, int]
    kept: list[str]
    refined: list[str]


def read_input(input_set: Path, min_particles: int) -> Input:
    particles_star, tomograms_star = _parse_optimisation_set(input_set)
    require_producer_input(particles_star, "Particles star")
    require_producer_input(tomograms_star, "Tomograms star")
    particles, _, _ = _read_input_particles_lenient(particles_star)
    missing = [c for c in (NAME, TOMO, *COORDS) if c not in particles.columns]
    if missing:
        raise ValueError(f"{particles_star} has no {missing}: frame alignment needs named, placed particles")
    if particles[NAME].astype(str).duplicated().any():
        raise ValueError(f"{particles_star}: particle names repeat; RELION matches trajectories by name")
    counts = {str(k): int(v) for k, v in particles.groupby(particles[TOMO].astype(str)).size().items()}
    kept = sorted(t for t, n in counts.items() if n < min_particles)
    refined = sorted(t for t in counts if t not in kept)
    if not refined:
        raise ValueError(
            f"Every tilt series has fewer than {min_particles} particles ({counts}): no model the particles can hold. "
            "Lower 'Fewest particles' if a tilt series this sparse should be refined"
        )
    return Input(
        particles_star,
        tomograms_star,
        optimisation_set_trajectories(input_set),
        particles,
        _read_tomograms_star(tomograms_star),
        tilt_series_stars(tomograms_star),
        counts,
        kept,
        refined,
    )


def assemble(job_dir: Path, inp: Input, motion: bool) -> tuple[Path, list[dict]]:
    """RELION's model with the kept tilt series put back, written as the job's set; and, per tilt series, what
    moved against the model it came with."""
    relion_set = job_dir / RELION_SET
    if not relion_set.is_file():
        raise RuntimeError(f"relion_tomo_align finished without {relion_set.name}; see run.out")
    relion_particles_star, relion_tomograms_star = _parse_optimisation_set(relion_set)
    relion_particles, optics, general = _read_input_particles_lenient(relion_particles_star)
    relion_stars = tilt_series_stars(relion_tomograms_star)
    for t in inp.refined:
        star = relion_stars.get(t)
        if star is None or not star.is_file() or star.resolve() == inp.tilt_stars[t].resolve():
            raise RuntimeError(f"relion_tomo_align wrote no refined tilt-series star for {t}; see run.out")

    motion_blocks: dict[str, Block] = {}
    if motion:
        motion_path = job_dir / RELION_MOTION
        if not motion_path.is_file():
            raise RuntimeError(f"relion_tomo_align ran with motion and wrote no {RELION_MOTION}; see run.out")
        motion_blocks = read_trajectories(motion_path)
    input_blocks = read_trajectories(inp.trajectories) if inp.trajectories is not None else {}

    # The input's rows and columns, with RELION's values where it wrote them for the refined tilt series (positions,
    # origins folded into them); a column only RELION writes is left out, never half filled.
    rows = relion_particles[relion_particles[TOMO].astype(str).isin(inp.refined)]
    rows = rows.set_index(rows[NAME].astype(str))
    particles = inp.particles.set_index(inp.particles[NAME].astype(str), drop=False)
    n_refined = sum(inp.counts[t] for t in inp.refined)
    if len(rows) != n_refined or rows.index.duplicated().any() or not rows.index.isin(particles.index).all():
        raise RuntimeError(
            f"relion_tomo_align wrote {len(rows)} particles of the refined tilt series; {n_refined} went in"
        )
    shared = [c for c in rows.columns if c in particles.columns]
    particles.loc[rows.index, shared] = rows[shared]
    only_relion = [c for c in rows.columns if c not in particles.columns]
    if only_relion:
        print(f"[DRIVER] Columns only RELION wrote, left out: {only_relion}", flush=True)
    particles = particles.reset_index(drop=True).drop(columns=["rlnImageName"], errors="ignore")

    names = particles[NAME].astype(str).tolist()
    tomo_of = dict(zip(names, particles[TOMO].astype(str), strict=True))
    n_tilts = {t: len(read_table(inp.tilt_stars[t])) for t in inp.counts}
    blocks: dict[str, Block] = {}
    for name in names:
        kept = tomo_of[name] in inp.kept
        block = (input_blocks if kept else motion_blocks).get(name)
        if block is None and kept and inp.trajectories is not None:
            raise RuntimeError(f"{name} has no trajectory in {inp.trajectories}, which the bridge checked")
        if block is None and motion and not kept:
            raise RuntimeError(f"{name} has no motion in {job_dir / RELION_MOTION}")
        block = block if block is not None else [(0.0, 0.0, 0.0)] * n_tilts[tomo_of[name]]
        if len(block) != n_tilts[tomo_of[name]] or any(not math.isfinite(v) for xyz in block for v in xyz):
            raise ValueError(f"The trajectory of {name} has {len(block)} tilts or a value that is not a number")
        blocks[name] = block

    tomograms = inp.tomograms.copy()
    for col in (wg.GEOMETRY_COL, wg.GEOMETRY_TILT_STAR_COL):
        if col not in tomograms.columns:
            tomograms[col] = ""
    for i, r in tomograms.iterrows():
        t = str(r[TOMO])
        if t in inp.refined:
            star = str(relion_stars[t].resolve())
            tomograms.at[i, "rlnTomoTiltSeriesStarFile"] = star
            tomograms.at[i, wg.GEOMETRY_COL] = wg.RELION_MODEL
            tomograms.at[i, wg.GEOMETRY_TILT_STAR_COL] = star
        elif wg.geometry_mark(r) not in wg.GEOMETRY_MARKS:
            tomograms.at[i, wg.GEOMETRY_COL] = wg.WARP_MODEL
    # A star loop holds no empty field: an empty one shifts every later column of its row.
    tomograms[wg.GEOMETRY_TILT_STAR_COL] = [
        v if isinstance(v, str) and v.strip() else wg.NO_STAR for v in tomograms[wg.GEOMETRY_TILT_STAR_COL]
    ]
    for label, df in (("particles", particles), ("tomograms", tomograms)):
        empty = [c for c in df.columns if df[c].isna().any() or (df[c].astype(str).str.strip() == "").any()]
        if empty:
            raise RuntimeError(f"The {label} to write have empty fields in {empty}: a star file cannot hold them")

    out_particles, out_tomograms = job_dir / OUT_PARTICLES, job_dir / OUT_TOMOGRAMS
    out_motion, out_set = job_dir / OUT_MOTION, job_dir / OUT_SET
    _write_particles_star(out_particles, optics_df=optics, particles_df=particles, general_kv=general)
    _write_tomograms_star(out_tomograms, tomograms)
    write_trajectories(out_motion, names, blocks)
    write_optimisation_set(
        out_set, particles_star=out_particles, tomograms_star=out_tomograms, trajectories_star=out_motion
    )

    before = inp.particles.set_index(inp.particles[NAME].astype(str))
    series = []
    for t in sorted(inp.counts):
        members = [n for n in names if tomo_of[n] == t]
        entry = {
            "name": t,
            "particles": inp.counts[t],
            "kept": t in inp.kept,
            "trajectories_in_rms_ang": block_rms(input_blocks, members),
        }
        if t in inp.refined:
            entry |= rigid_changes(inp.tilt_stars[t], relion_stars[t])
            entry["motion_rms_ang"] = block_rms(motion_blocks, members) if motion else None
            moves = []
            for n in members:
                old, new = before.loc[n], rows.loc[n]
                origin = [float(old[c]) if c in before.columns else 0.0 for c in ORIGINS]
                was = [float(old[c]) - o for c, o in zip(COORDS, origin, strict=True)]
                moves.append(math.dist([float(new[c]) for c in COORDS], was))
            entry["position_rms_ang"] = rms(moves)
        series.append(entry)
    return out_set, series


def print_series(series: list[dict]) -> None:
    print("[DRIVER] Per tilt series, against the model it came with (Å, °):", flush=True)
    for s in series:
        came = s["trajectories_in_rms_ang"]
        came_txt = f" · its trajectories in: {came:.1f} RMS" if came is not None else ""
        if s["kept"]:
            print(f"[DRIVER]   {s['name']}: {s['particles']} particles · kept as it came{came_txt}", flush=True)
            continue
        motion = s.get("motion_rms_ang")
        print(
            f"[DRIVER]   {s['name']}: {s['particles']} particles · tilt shifts moved {s['shift_rms_ang']:.1f} RMS, "
            f"largest {s['shift_max_ang']:.1f} · angles up to {s['angle_max_deg']:.2f}° · positions moved "
            f"{s['position_rms_ang']:.1f} RMS"
            + (f" · motion {motion:.1f} RMS" if motion is not None else "")
            + came_txt,
            flush=True,
        )


# ── the run


def reuse_temp(job_dir: Path, command: str) -> bool:
    """True when temp/ holds per-tomogram results of this very command (a run that stopped, run again): RELION then
    skips the tomograms it finished (--only_do_unfinished). Results of another command are removed."""
    temp = job_dir / TEMP_DIR
    record = temp / COMMAND_RECORD
    if record.is_file() and record.read_text() == command:
        return any(temp.glob("*_projections.star"))
    if temp.is_dir():
        shutil.rmtree(temp)
    temp.mkdir(parents=True)
    record.write_text(command)
    return False


def build_command(params: FrameAlignParams, input_set: Path, ref, box: int, job_dir: Path) -> ToolCommand:
    cmd = (
        ToolCommand("relion_tomo_align")
        .opt_path("--i", input_set, quote=False)
        .opt_path("--ref1", ref.half1, quote=False)
        .opt_path("--ref2", ref.half2, quote=False)
        .opt_path("--mask", ref.mask, quote=False)
        .opt_path("--fsc", ref.fsc, quote=False)
        .opt_path("--o", f"{job_dir}/", quote=False)
        .opt("--b", box)
        .opt("--r", params.max_shift_px)
        .opt("--it", params.iterations)
        .opt("--j", params.threads)
    )
    if not params.refine_angles:
        cmd.flag("--const_a")
    if not params.refine_shifts:
        cmd.flag("--const_s")
    if not params.refine_positions:
        cmd.flag("--const_p")
    if params.motion:
        cmd.flag("--motion").opt("--s_vel", f"{params.sigma_velocity:g}").opt("--s_div", f"{params.sigma_divergence:g}")
        if params.square_exp_kernel:
            cmd.flag("--sq_exp_ker")
    if params.per_frame_noise:
        cmd.flag("--per_frame_noise")
    if params.other_args.strip():
        cmd.raw(params.other_args.strip())
    return cmd


def main():
    start_supervisor()
    print("--- SLURM JOB START (frame alignment) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(FrameAlignParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        problems = params.run_problems()
        if problems:
            raise ValueError("; ".join(problems))
        paths = {k: Path(v) for k, v in context["paths"].items()}
        given_set = paths["input_optimisation"]
        half1 = paths["input_half_map"]
        half2 = second_half(half1)
        mask = paths["input_mask"]
        fsc = paths["input_fsc"]
        for path, label in (
            (given_set, "Optimisation set"),
            (half1, "Half map 1"),
            (half2, "Half map 2"),
            (mask, "Mask"),
            (fsc, "FSC (postprocess.star)"),
        ):
            require_producer_input(path, label)

        # RELION places the particles from the tilt images by the set's model: Warp's whole model through the bridge
        # (its offsets centre RELION's windows and stay with the kept tilt series), or a frame alignment's own.
        input_set = carry_warp_geometry(given_set, job_dir)
        inp = read_input(input_set, params.min_particles)
        ts_apix = tilt_series_pixel_size(inp.tomograms_star)
        ref = check_reference(half1, half2, mask, fsc, ts_apix, "relion_tomo_align")
        box = box_for(ref, params.box_size)

        print(f"[DRIVER] Optimisation set: {input_set}", flush=True)
        print(f"[DRIVER]   particles {inp.particles_star}", flush=True)
        print(f"[DRIVER]   tomograms {inp.tomograms_star} (tilt images at {ts_apix:g} Å/px)", flush=True)
        print(f"[DRIVER]   trajectories {inp.trajectories or '(none)'}", flush=True)
        print(f"[DRIVER] Reference: {ref.describe()}", flush=True)
        print(f"[DRIVER] Mask: {mask}", flush=True)
        print(f"[DRIVER] Box: {box} px ({box * ts_apix:.0f} Å)", flush=True)
        n_kept = sum(inp.counts[t] for t in inp.kept)
        print(
            f"[DRIVER] {len(inp.refined)} tilt series refined ({sum(inp.counts[t] for t in inp.refined)} particles); "
            f"{len(inp.kept)} keep the model they came with, under {params.min_particles} particles each "
            f"({n_kept} particles{': ' + ', '.join(inp.kept) if inp.kept else ''})",
            flush=True,
        )

        cmd = build_command(params, input_set, ref, box, job_dir)
        if reuse_temp(job_dir, str(cmd)):
            print("[DRIVER] temp/ holds this command's finished tomograms: RELION skips them", flush=True)
            cmd.flag("--only_do_unfinished")
        print(f"[DRIVER] Command: {cmd}", flush=True)
        binds = {p.parent.resolve() for p in (input_set, inp.particles_star, inp.tomograms_star, half1, mask, fsc)}
        binds |= {p.parent.resolve() for p in inp.tilt_stars.values()}
        if inp.trajectories is not None:
            binds.add(inp.trajectories.parent.resolve())
        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=sorted(str(b) for b in binds))

        out_set, series = assemble(job_dir, inp, params.motion)
        changes = {
            "tilt_series": series,
            "kept": inp.kept,
            "min_particles": params.min_particles,
            "motion": params.motion,
            "box": box,
            "pixel_size": ts_apix,
            "reference": str(ref.half1),
            "fsc_resolution": ref.fsc_resolution,
            "input_set": str(given_set),
            "trajectories_in": str(inp.trajectories) if inp.trajectories else None,
        }
        (job_dir / CHANGES_NAME).write_text(json.dumps(changes, indent=1))
        print_series(series)
        print(f"[DRIVER] Output: {out_set} (no images: cut the particles with Subtomo Extraction)", flush=True)
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
