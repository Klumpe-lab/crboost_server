#!/usr/bin/env python3
"""
Driver for RELION gold-standard 3D refinement (relion_refine_mpi --auto_refine --split_random_halves).

A re-run in a job dir where an earlier attempt finished an iteration continues from the latest one
(relion_refine --continue) instead of starting over.
"""

import shutil
import sys
import traceback
from pathlib import Path

from drivers.driver_base import ToolCommand, chosen_solvent_mask, get_driver_context, run_tool, start_supervisor
from drivers.relion_continue import changed_since_start, continue_command, latest_checkpoint, program_index
from services.jobs.refine3d import Refine3DParams
from services.particles.helical import helical_box_problem
from services.particles.warp_geometry import require_images
from services.subtomo_merge import _parse_optimisation_set, _read_particles_star, write_optimisation_set

ANGLE_COLS = ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")
# 0,0,0 is how hand-picked lists and merges say "no orientation"; template matching lands there for only a handful
# of particles (one grid point among thousands).
MAX_UNORIENTED_FRACTION = 0.01

# A finished iteration: its optimiser star and the files that star points at. A continuation names them
# run_ct<N>_it<M>_*.
ITERATION_FILES = (
    "_data.star",
    "_sampling.star",
    "_half1_model.star",
    "_half2_model.star",
    "_half1_class001.mrc",
    "_half2_class001.mrc",
)
# What relion_refine --helix reads of a filament segment besides its angles: the priors its tilt and psi are searched
# around, and the filament it belongs to (half-sets are split by tube).
HELICAL_COLS = ("rlnAngleTiltPrior", "rlnAnglePsiPrior", "rlnHelicalTubeID")
FLIP_RATIO_COL = "rlnAnglePsiFlipRatio"
# RELION tries a segment in both directions along its filament while its flip ratio is above this
# (HealpixSampling::selectOrientationsWithNonZeroPriorProbabilityFor3DHelicalReconstruction).
FLIP_RATIO_UNIMODAL = 0.01

# The converged result carries no iteration tag (RELION's MlOptimiser::write with fn_root = fn_out): the
# OUTPUT_SCHEMA files plus the two half-maps.
RESULT_FILES = ("_optimisation_set.star", "_class001.mrc", "_half1_class001_unfil.mrc", "_half2_class001_unfil.mrc")


def check_angle_priors(input_optimisation: Path) -> None:
    """A local search only refines orientations the particles already have: refuse a set without them."""
    particles_star, _ = _parse_optimisation_set(input_optimisation)
    _, particles, _ = _read_particles_star(particles_star)
    missing = [c for c in ANGLE_COLS if c not in particles.columns]
    if missing:
        raise ValueError(
            f"{particles_star} has no {missing}: a local angular search needs input angles. "
            "Set sigma_ang to -1 for a global search."
        )
    unoriented = int((particles[list(ANGLE_COLS)] == 0).all(axis=1).sum())
    print(f"[DRIVER] {unoriented}/{len(particles)} particles at angles 0,0,0", flush=True)
    if unoriented > MAX_UNORIENTED_FRACTION * len(particles):
        raise ValueError(
            f"{unoriented}/{len(particles)} particles in {particles_star} sit at 0,0,0 (no orientation): a local "
            "angular search would keep them there. Set sigma_ang to -1 for a global search."
        )


def check_helical_priors(input_optimisation: Path) -> None:
    """A filament refinement searches around each segment's priors: refuse a set without them, and say how many
    segments it tries in both directions along their filament."""
    particles_star, _ = _parse_optimisation_set(input_optimisation)
    _, particles, _ = _read_particles_star(particles_star)
    missing = [c for c in (*ANGLE_COLS, *HELICAL_COLS) if c not in particles.columns]
    if missing:
        raise ValueError(
            f"{particles_star} has no {missing}: a helical refinement needs each segment's tilt and psi priors and "
            "its filament's tube id, as filament picks carry them. Particles of another picker are refined with "
            "Helical off."
        )
    tubes = particles.groupby(["rlnTomoName", "rlnHelicalTubeID"]).ngroups if "rlnTomoName" in particles.columns else 0
    if FLIP_RATIO_COL in particles.columns:
        both = int((particles[FLIP_RATIO_COL].astype(float) > FLIP_RATIO_UNIMODAL).sum())
        polarity = f"{both} tried in both directions along their filament ({FLIP_RATIO_COL} > {FLIP_RATIO_UNIMODAL})"
    else:
        # RELION reads an absent flip ratio as 0.5.
        polarity = f"all tried in both directions along their filament (no {FLIP_RATIO_COL}: RELION takes 0.5)"
    print(f"[DRIVER] Helical: {len(particles)} segments in {tubes} filaments; {polarity}", flush=True)


# A template may differ from the particles' pixel size by this fraction: --trust_ref_size takes it as theirs.
TEMPLATE_PIXEL_TOLERANCE = 0.01


def check_template_reference(input_optimisation: Path, reference: Path) -> None:
    """A template stands in for a job's map only on the particles' own grid: relion_refine runs with
    --trust_ref_size, which takes the reference's box and pixel size as the particles' without resampling it."""
    import mrcfile

    with mrcfile.open(reference, header_only=True, permissive=True) as mrc:
        box_px, pixel_ang = int(mrc.header.nx), float(mrc.voxel_size.x)
    particles_star, _ = _parse_optimisation_set(input_optimisation)
    optics, _, _ = _read_particles_star(particles_star)
    lacking = [c for c in ("rlnImageSize", "rlnImagePixelSize") if c not in optics.columns]
    if lacking:
        raise ValueError(f"{particles_star} states no {lacking} in its optics: the template cannot be checked")
    boxes, pixels = set(optics["rlnImageSize"].astype(int)), set(optics["rlnImagePixelSize"].astype(float).round(4))
    if len(boxes) != 1 or len(pixels) != 1:
        raise ValueError(f"{particles_star} holds particles of several grids ({sorted(boxes)} px, {sorted(pixels)} Å)")
    want_box, want_pixel = boxes.pop(), pixels.pop()
    problems = []
    if box_px != want_box:
        problems.append(f"its box is {box_px} px, the particles' {want_box} px")
    if pixel_ang <= 0 or abs(pixel_ang - want_pixel) > TEMPLATE_PIXEL_TOLERANCE * want_pixel:
        problems.append(f"its pixel is {pixel_ang:g} Å, the particles' {want_pixel:g} Å")
    if problems:
        raise ValueError(
            f"The template {reference.name} is not on the particles' grid: {'; '.join(problems)}. Make it at "
            f"{want_pixel:g} Å in a box of {want_box} px (Species page: resample)."
        )
    print(f"[DRIVER] Template reference on the particles' grid: {box_px} px at {pixel_ang:g} Å", flush=True)


def check_helical_box(input_reference: Path, params: Refine3DParams) -> None:
    """The reference's box stands for the particles': --trust_ref_size takes its pixel size as theirs."""
    import mrcfile

    with mrcfile.open(input_reference, header_only=True, permissive=True) as mrc:
        box_px, pixel_ang = int(mrc.header.nx), float(mrc.voxel_size.x)
    if pixel_ang <= 0:
        raise ValueError(f"{input_reference} states no pixel size: the tube diameter cannot be checked against its box")
    problem = helical_box_problem(box_px, pixel_ang, params.helical_outer_diameter, params.particle_diameter)
    if problem:
        raise ValueError(f"Helical: {problem}")
    print(
        f"[DRIVER] Helical: tube {params.helical_outer_diameter:g} Å in a box of {box_px} px at {pixel_ang:g} Å/px",
        flush=True,
    )


def add_helical_options(cmd: ToolCommand, params: Refine3DParams) -> None:
    """relion_refine's filament options. The three sigmas stand in for --sigma_ang, which would put one prior on
    all three angles and so hold rot at the picker's random draw."""
    cmd.flag("--helix")
    cmd.opt("--helical_outer_diameter", params.helical_outer_diameter)
    for name in ("rot", "tilt", "psi"):
        value = getattr(params, f"sigma_{name}")
        if value > 0:
            cmd.opt(f"--sigma_{name}", value)
    if params.keep_tilt_prior_fixed:
        cmd.flag("--helical_keep_tilt_prior_fixed")
    if params.ignore_helical_symmetry:
        cmd.flag("--ignore_helical_symmetry")
        return
    cmd.opt("--helical_twist_initial", params.helical_twist)
    cmd.opt("--helical_rise_initial", params.helical_rise)
    cmd.opt("--helical_nr_asu", params.helical_nr_asu)
    # RELION takes the fraction of the box.
    cmd.opt("--helical_z_percentage", params.helical_z_percentage / 100.0)


def main():
    start_supervisor()
    print("--- SLURM JOB START (Refine3D) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(Refine3DParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = list(context.get("additional_binds", []))

        input_optimisation = paths["input_optimisation"]
        if params.reference_from_template:
            if not params.reference_path.strip():
                raise ValueError("Template as reference is on and the job names no template")
            input_reference = Path(params.reference_path)
        else:
            input_reference = paths["input_reference"]

        if not input_optimisation.exists():
            raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")
        if not input_reference.exists():
            raise FileNotFoundError(f"Input reference map not found: {input_reference}")

        print(f"[DRIVER] Input optimisation_set: {input_optimisation}", flush=True)
        print(f"[DRIVER] Input reference: {input_reference}", flush=True)
        if params.reference_from_template:
            check_template_reference(input_optimisation, input_reference)
        print(f"[DRIVER] Job dir: {job_dir}", flush=True)
        require_images(_parse_optimisation_set(input_optimisation)[0])
        if params.helix:
            check_helical_priors(input_optimisation)
            check_helical_box(input_reference, params)
        elif params.sigma_ang > 0:
            check_angle_priors(input_optimisation)

        output_root = job_dir / "run"

        # Split random halves needs MPI: a leader plus one follower per half-set. The container runs with
        # --cleanenv and SLURM_* unset, so mpirun cannot see the allocation and counts physical cores as
        # slots; --oversubscribe lets three ranks start on a hyperthreaded 4-CPU allocation, --bind-to none
        # keeps them off a shared core.
        cmd = (
            ToolCommand("mpirun")
            .flag("--oversubscribe")
            .opt("--bind-to", "none")
            .opt("-n", params.mpi_ranks)
            .raw("relion_refine_mpi")
            .opt_path("--ios", input_optimisation, quote=False)
            .opt_path("--ref", input_reference, quote=False)
            .opt_path("--o", output_root, quote=False)
            .flag("--auto_refine")
            .flag("--split_random_halves")
            .opt("--healpix_order", params.healpix_order)
            .opt("--auto_local_healpix_order", params.auto_local_healpix_order)
            .opt("--offset_range", params.offset_range)
            .opt("--offset_step", params.offset_step)
            .opt("--oversampling", 1)
            .opt("--sym", params.symmetry.value if hasattr(params.symmetry, "value") else params.symmetry)
            .opt("--low_resol_join_halves", params.low_resol_join_halves)
            .opt("--j", params.threads)
            .opt("--pool", params.pool)
            .opt("--pad", params.pad)
            .flag("--trust_ref_size")
        )

        if params.helix:
            add_helical_options(cmd, params)
        elif params.sigma_ang > 0:
            cmd.opt("--sigma_ang", params.sigma_ang)
        if params.ini_high > 0:
            cmd.opt("--ini_high", params.ini_high)
        if params.particle_diameter > 0:
            cmd.opt("--particle_diameter", params.particle_diameter)
        if params.random_seed >= 0:
            cmd.opt("--random_seed", params.random_seed)

        if params.do_ctf:
            cmd.flag("--ctf")
        if params.do_norm:
            cmd.flag("--norm")
        if params.do_scale:
            cmd.flag("--scale")
        if params.zero_mask:
            cmd.flag("--zero_mask")
        if params.dont_combine_weights_via_disc:
            cmd.flag("--dont_combine_weights_via_disc")
        if params.flatten_solvent:
            cmd.flag("--flatten_solvent")
        if params.firstiter_cc:
            cmd.flag("--firstiter_cc")
        if params.preread_images:
            cmd.flag("--preread_images")

        mask_path = chosen_solvent_mask(params, paths)
        if mask_path is not None:
            print(f"[DRIVER] Solvent mask: {mask_path}", flush=True)
            cmd.opt_path("--solvent_mask", mask_path, quote=False)
            if params.solvent_correct_fsc:
                cmd.flag("--solvent_correct_fsc")
            additional_binds.append(str(mask_path.parent.resolve()))

        if params.use_gpu:
            cmd.flag("--gpu")

        checkpoint = latest_checkpoint(job_dir, ITERATION_FILES)
        if checkpoint is not None:
            iteration, optimiser = checkpoint
            changed = changed_since_start(job_dir, cmd.parts[program_index(cmd.parts) + 1 :])
            if changed:
                raise RuntimeError(
                    f"{optimiser.name} holds iteration {iteration} of a run started with other settings "
                    f"({'; '.join(changed)}). A continued run reads its settings from that checkpoint, so it would "
                    "not apply these. Restore them to continue, or run the new settings as a new Refine 3D job."
                )
            output_root = job_dir / f"run_ct{iteration}"
            cmd = continue_command(cmd, optimiser, output_root)
            print(
                f"[DRIVER] CONTINUE: {optimiser.name} finished iteration {iteration}; continuing from it "
                f"as {output_root.name}_*",
                flush=True,
            )

        print(f"[DRIVER] Command: {cmd}", flush=True)

        additional_binds.append(str(input_optimisation.parent.resolve()))
        additional_binds.append(str(input_reference.parent.resolve()))

        run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

        root = output_root.name
        result_set = job_dir / f"{root}_optimisation_set.star"
        if root != "run" and not result_set.exists() and (job_dir / f"{root}_data.star").exists():
            # The checkpoint does not record the input optimisation set, so a continued relion_refine may not
            # write one; the tomograms are the input set's.
            _, tomograms = _parse_optimisation_set(input_optimisation)
            write_optimisation_set(result_set, particles_star=job_dir / f"{root}_data.star", tomograms_star=tomograms)
            print(f"[DRIVER] relion_refine wrote no {result_set.name}; the driver wrote it", flush=True)
        missing = [f"{root}{s}" for s in RESULT_FILES if not (job_dir / f"{root}{s}").exists()]
        if missing:
            raise RuntimeError(
                f"relion_refine_mpi finished without the converged outputs {missing} in {job_dir}. "
                "Check run.out for relion_refine errors."
            )
        for s in RESULT_FILES:
            # A continuation wrote run_ct<N>_*; downstream jobs read the run_* names.
            if root != "run":
                shutil.copy2(job_dir / f"{root}{s}", job_dir / f"run{s}")
            print(f"[DRIVER] Output: {job_dir / f'run{s}'}", flush=True)

        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
