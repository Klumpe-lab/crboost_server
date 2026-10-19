#!/usr/bin/env python3
"""
Driver for RELION gold-standard 3D refinement (relion_refine_mpi --auto_refine --split_random_halves).

A re-run in a job dir where an earlier attempt finished an iteration continues from the latest one
(relion_refine --continue) instead of starting over.
"""

import re
import shutil
import sys
import traceback
from pathlib import Path

from drivers.driver_base import ToolCommand, get_driver_context, run_tool
from services.jobs.refine3d import Refine3DParams
from services.subtomo_merge import _parse_optimisation_set, _read_particles_star, write_optimisation_set

ANGLE_COLS = ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")
# 0,0,0 is how hand-picked lists and merges say "no orientation"; template matching lands there for only a handful
# of particles (one grid point among thousands).
MAX_UNORIENTED_FRACTION = 0.01

# On --continue relion_refine reads every option back from the checkpoint except these computation ones.
CONTINUE_OPTIONS = ("--j", "--pool", "--gpu", "--preread_images", "--dont_combine_weights_via_disc")
# A finished iteration: its optimiser star and the files that star points at. A continuation names them
# run_ct<N>_it<M>_*.
OPTIMISER_RE = re.compile(r"run(?:_ct\d+)?_it(\d{3})_optimiser\.star")
ITERATION_FILES = (
    "_data.star",
    "_sampling.star",
    "_half1_model.star",
    "_half2_model.star",
    "_half1_class001.mrc",
    "_half2_class001.mrc",
)
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


def latest_checkpoint(job_dir: Path) -> tuple[int, Path] | None:
    """The last iteration (>= 1) an earlier attempt finished writing in job_dir, and its optimiser star."""
    done = []
    for optimiser in job_dir.glob("run*_optimiser.star"):
        m = OPTIMISER_RE.fullmatch(optimiser.name)
        prefix = optimiser.name.removesuffix("_optimiser.star")
        if m and int(m.group(1)) >= 1 and all((job_dir / f"{prefix}{s}").is_file() for s in ITERATION_FILES):
            done.append((int(m.group(1)), optimiser))
    return max(done, default=None)


def options_of(argv: list[str]) -> dict[str, tuple[str, ...]]:
    """relion_refine arguments by option: '--sym C1 --ctf' -> {'--sym': ('C1',), '--ctf': ()}."""
    options: dict[str, tuple[str, ...]] = {}
    key = None
    for token in argv:
        if token.startswith("--"):
            key = token
            options[key] = ()
        elif key is not None:
            options[key] += (token,)
    return options


def changed_since_start(job_dir: Path, argv: list[str]) -> list[str]:
    """The refinement options in argv that differ from those the run in job_dir was started with, as recorded in
    its run_it000_optimiser.star. A continued run reads them from its checkpoint, so it would not apply these."""
    first = job_dir / "run_it000_optimiser.star"
    recorded = next((line[1:].split() for line in first.read_text().splitlines() if line.startswith("# --")), None)
    if recorded is None:
        raise ValueError(f"{first} does not record the command its run was started with")
    skip = (*CONTINUE_OPTIONS, "--o")
    then = {k: v for k, v in options_of(recorded).items() if k not in skip}
    now = {k: v for k, v in options_of(argv).items() if k not in skip}

    def shown(values: tuple[str, ...] | None) -> str:
        return "unset" if values is None else " ".join(values) or "on"

    return [f"{k} {shown(then.get(k))} -> {shown(now.get(k))}" for k in sorted(then | now) if then.get(k) != now.get(k)]


def continue_command(fresh: ToolCommand, optimiser: Path, output_root: Path) -> ToolCommand:
    """fresh as a continuation from optimiser: the same mpirun prefix and computation options; every other option
    comes from the checkpoint."""
    i = fresh.parts.index("relion_refine_mpi") + 1
    cmd = ToolCommand(" ".join(fresh.parts[:i]))
    cmd.opt_path("--continue", optimiser, quote=False).opt_path("--o", output_root, quote=False)
    for option, values in options_of(fresh.parts[i:]).items():
        if option in CONTINUE_OPTIONS:
            cmd.raw(" ".join((option, *values)))
    return cmd


def main():
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
        input_reference = paths["input_reference"]

        if not input_optimisation.exists():
            raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")
        if not input_reference.exists():
            raise FileNotFoundError(f"Input reference map not found: {input_reference}")

        print(f"[DRIVER] Input optimisation_set: {input_optimisation}", flush=True)
        print(f"[DRIVER] Input reference: {input_reference}", flush=True)
        print(f"[DRIVER] Job dir: {job_dir}", flush=True)
        if params.sigma_ang > 0:
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

        if params.sigma_ang > 0:
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

        solvent_mask = params.solvent_mask_path.strip()
        if solvent_mask:
            mask_path = Path(solvent_mask)
            if not mask_path.exists():
                raise FileNotFoundError(f"Solvent mask not found: {mask_path}")
            cmd.opt_path("--solvent_mask", mask_path, quote=False)
            if params.solvent_correct_fsc:
                cmd.flag("--solvent_correct_fsc")
            additional_binds.append(str(mask_path.parent.resolve()))

        if params.use_gpu:
            cmd.flag("--gpu")

        checkpoint = latest_checkpoint(job_dir)
        if checkpoint is not None:
            iteration, optimiser = checkpoint
            changed = changed_since_start(job_dir, cmd.parts[cmd.parts.index("relion_refine_mpi") + 1 :])
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
