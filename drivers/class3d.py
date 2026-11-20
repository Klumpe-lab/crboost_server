#!/usr/bin/env python3
"""
Driver for RELION 3D Classification (relion_refine without --auto_refine).

mpi_ranks 1 runs one relion_refine; more run relion_refine_mpi, a leader handing the particles out to followers that
RELION spreads over the job's GPUs. A re-run in a job dir where an earlier attempt finished an iteration continues
from the latest one (relion_refine --continue) instead of starting over, and gives what the continuation wrote the
run_itNNN_* names the job's readers know.
"""

import os
import re
import shutil
import sys
import traceback
from pathlib import Path

from drivers.driver_base import ToolCommand, chosen_solvent_mask, get_driver_context, run_tool, start_supervisor
from drivers.relion_continue import changed_since_start, continue_command, latest_checkpoint, program_index
from services.jobs.class3d import Class3DParams
from services.particles.warp_geometry import require_images
from services.subtomo_merge import _parse_optimisation_set, write_optimisation_set

# What a continuation from iteration N writes: run_ctN_itMMM_<rest>.
CONTINUED_RE = re.compile(r"run_ct(\d+)_it(\d{3})_(.+)")


def iteration_files(n_classes: int) -> tuple[str, ...]:
    """What a finished Class 3D iteration has beside its optimiser star; a checkpoint needs every one."""
    return ("_data.star", "_model.star", "_sampling.star", *(f"_class{k:03d}.mrc" for k in range(1, n_classes + 1)))


def fresh_command(
    params: Class3DParams, input_optimisation: Path, input_reference: Path, output_root: Path, mask_path: Path | None
) -> ToolCommand:
    """The relion_refine command of a run that starts at iteration 1."""
    if params.mpi_ranks > 1:
        # The container runs with --cleanenv and SLURM_* unset, so mpirun cannot see the allocation and counts
        # physical cores as slots: --oversubscribe lets the ranks start, --bind-to none keeps them off a shared core.
        cmd = (
            ToolCommand("mpirun")
            .flag("--oversubscribe")
            .opt("--bind-to", "none")
            .opt("-n", params.mpi_ranks)
            .raw("relion_refine_mpi")
        )
    else:
        cmd = ToolCommand("relion_refine")
    (
        cmd.opt_path("--ios", input_optimisation, quote=False)
        .opt_path("--ref", input_reference, quote=False)
        .opt_path("--o", output_root, quote=False)
        .opt("--K", params.n_classes)
        .opt("--iter", params.n_iterations)
        .opt("--healpix_order", params.healpix_order)
        .opt("--offset_range", params.offset_range)
        .opt("--offset_step", params.offset_step)
        .opt("--oversampling", params.oversampling)
        .opt("--sym", params.symmetry.value if hasattr(params.symmetry, "value") else params.symmetry)
        .opt("--j", params.threads)
        .opt("--pool", params.pool)
        .opt("--pad", params.pad)
        .flag("--trust_ref_size")
    )

    if params.ini_high > 0:
        cmd.opt("--ini_high", params.ini_high)
    if params.particle_diameter > 0:
        cmd.opt("--particle_diameter", params.particle_diameter)
    if params.tau_fudge > 0:
        cmd.opt("--tau2_fudge", params.tau_fudge)
    if params.sigma_ang > 0 and not params.skip_align:
        cmd.opt("--sigma_ang", params.sigma_ang)
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

    if mask_path is not None:
        cmd.opt_path("--solvent_mask", mask_path, quote=False)
    if params.skip_align:
        # Classes from the poses the particles carry; with a mask on one region, a focused classification.
        cmd.flag("--skip_align")

    if params.use_gpu:
        cmd.flag("--gpu")
    return cmd


def mirror_continued(job_dir: Path) -> int:
    """Give every file a continuation wrote its run_itMMM_* name, from the latest continuation that wrote it (a later
    one went on from a later checkpoint), replacing what a stopped attempt left half written under that name. Hard
    links where the file system allows them, else copies. Returns how many names it set."""
    newest: dict[str, tuple[int, Path]] = {}
    for f in job_dir.glob("run_ct*_it*"):
        m = CONTINUED_RE.fullmatch(f.name)
        if m is None:
            continue
        name = f"run_it{m.group(2)}_{m.group(3)}"
        if name not in newest or int(m.group(1)) > newest[name][0]:
            newest[name] = (int(m.group(1)), f)
    for name, (_, src) in newest.items():
        dst = job_dir / name
        dst.unlink(missing_ok=True)
        try:
            os.link(src, dst)
        except OSError:
            # No hard links on this file system: a copy holds the same content.
            shutil.copy2(src, dst)
    return len(newest)


def write_missing_optimisation_sets(job_dir: Path, n_iterations: int, tomograms_star: Path) -> list[str]:
    """The checkpoint does not record the input optimisation set, so a continued relion_refine may write no
    run_itMMM_optimisation_set.star: write one for each iteration that has its data star and lacks it."""
    written = []
    for it in range(1, n_iterations + 1):
        data = job_dir / f"run_it{it:03d}_data.star"
        optset = job_dir / f"run_it{it:03d}_optimisation_set.star"
        if data.is_file() and not optset.exists():
            write_optimisation_set(optset, particles_star=data, tomograms_star=tomograms_star)
            written.append(optset.name)
    return written


def say_mpi(params: Class3DParams) -> None:
    """What the ranks run on, from the job's own environment (the container sees none of it)."""
    gpus = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    n_gpus = len([g for g in gpus.split(",") if g.strip()])
    followers = params.mpi_ranks - 1
    print(
        f"[DRIVER] MPI: {params.mpi_ranks} processes, a leader and {followers} follower(s), --j {params.threads} each; "
        f"GPUs visible: {gpus or 'none named'} ({n_gpus}); CPUs: {os.environ.get('SLURM_CPUS_PER_TASK', '?')}",
        flush=True,
    )
    if params.use_gpu and n_gpus and followers > 2 * n_gpus:
        print(f"[DRIVER] WARN: {followers} followers share {n_gpus} GPU(s), more than two a card", flush=True)


def main():
    start_supervisor()
    print("--- SLURM JOB START (Class3D) ---", flush=True)

    try:
        (_state, params, context, job_dir, _project_path, _job_type) = get_driver_context(Class3DParams)
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
        require_images(_parse_optimisation_set(input_optimisation)[0])

        mask_path = chosen_solvent_mask(params, paths)
        if mask_path is not None:
            print(f"[DRIVER] Solvent mask: {mask_path}", flush=True)
            additional_binds.append(str(mask_path.parent.resolve()))
        cmd = fresh_command(params, input_optimisation, input_reference, job_dir / "run", mask_path)
        if params.mpi_ranks > 1:
            say_mpi(params)

        run_relion = True
        checkpoint = latest_checkpoint(job_dir, iteration_files(params.n_classes))
        if checkpoint is not None:
            iteration, optimiser = checkpoint
            changed = changed_since_start(job_dir, cmd.parts[program_index(cmd.parts) + 1 :])
            if changed:
                raise RuntimeError(
                    f"{optimiser.name} holds iteration {iteration} of a run started with other settings "
                    f"({'; '.join(changed)}). A continued run reads its settings from that checkpoint, so it would "
                    "not apply these. Restore them to continue, or run the new settings as a new Class 3D job."
                )
            if iteration >= params.n_iterations:
                run_relion = False
                print(
                    f"[DRIVER] {optimiser.name}: all {params.n_iterations} iterations are on disk; "
                    "finishing without relion_refine",
                    flush=True,
                )
            else:
                output_root = job_dir / f"run_ct{iteration}"
                cmd = continue_command(cmd, optimiser, output_root)
                print(
                    f"[DRIVER] CONTINUE: {optimiser.name} finished iteration {iteration}; continuing from it "
                    f"as {output_root.name}_*",
                    flush=True,
                )

        if run_relion:
            print(f"[DRIVER] Command: {cmd}", flush=True)
            additional_binds.append(str(input_optimisation.parent.resolve()))
            additional_binds.append(str(input_reference.parent.resolve()))
            run_tool(cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

        if any(job_dir.glob("run_ct*_it*")):
            print(
                f"[DRIVER] Continued iterations under their run_it names: {mirror_continued(job_dir)} files", flush=True
            )
            _, tomograms_star = _parse_optimisation_set(input_optimisation)
            written = write_missing_optimisation_sets(job_dir, params.n_iterations, tomograms_star)
            if written:
                print(f"[DRIVER] relion_refine wrote no {', '.join(written)}; the driver wrote them", flush=True)

        # relion_refine for Class3D (fixed --iter N, no --auto_refine) writes its
        # outputs with the iteration number: run_it{N}_optimisation_set.star -- there
        # is no bare run_optimisation_set.star (that name is an auto-refine/Refine3D
        # convention). The numbered file is the completion signal; it is mirrored to the
        # un-numbered name the OUTPUT_SCHEMA path_template + downstream resolver expect.
        final_optset = job_dir / f"run_it{params.n_iterations:03d}_optimisation_set.star"
        expected_optset = job_dir / "run_optimisation_set.star"
        if not final_optset.exists():
            raise RuntimeError(
                f"Expected output {final_optset.name} not found in {job_dir}. Check run.out for relion_refine errors."
            )
        shutil.copy2(final_optset, expected_optset)
        print(f"[DRIVER] Output optimisation_set: {expected_optset}", flush=True)

        class_maps = sorted(job_dir.glob("run_it*_class*.mrc"))
        if class_maps:
            last_iter_maps = [m for m in class_maps if m.stem.startswith(f"run_it{params.n_iterations:03d}")]
            if not last_iter_maps:
                last_iter_maps = class_maps[-params.n_classes :]
            for cm in last_iter_maps:
                print(f"[DRIVER] Class volume: {cm}", flush=True)
        else:
            print("[DRIVER] WARN: No class volumes found", flush=True)

        _write_readout_sidecar(job_dir, params, context["instance_id"])

        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


def _write_readout_sidecar(job_dir: Path, params: Class3DParams, instance_id: str) -> None:
    """Class distribution per iteration, classes against score, tomogram and project, class slices
    and member galleries, as the job's analytics sidecar. A readout of a finished classification:
    its failure — including an import failure of the analytics code — is reported and never fails
    the job."""
    try:
        from services.particles.class_readout import CLASS3D_KIND, compute_class3d_record, sidecar_paths
        from services.particles.records import write_record

        record_path, assets = sidecar_paths(job_dir, CLASS3D_KIND)
        rec = compute_class3d_record(job_dir, params, instance_id=instance_id, assets_dir=assets, computed_by="driver")
        write_record(record_path, rec)
        print(
            f"[DRIVER] Analytics sidecar: {rec.data['n_classes']} classes, {rec.data['n_particles']} particles",
            flush=True,
        )
    except Exception as e:
        print(f"[DRIVER WARN] Analytics sidecar failed (the classification itself succeeded): {e}", flush=True)
        traceback.print_exc()


if __name__ == "__main__":
    main()
