# Roadmap 31 — Refine3D job

**Status:** asked for and started 2026-10-12; stage 1 built the same day, **not run**.

## Why
Roadmap 24's verdict on miss-alignment is a gold-standard ribosome refinement per arm (same reference, mask and
parameters, two seeds each), and its stage 4 success bar is "a sub-15 Å gold-standard map through the Warp-export
path". crboost has Class 3D only; Class 3D with one class is neither gold-standard nor converged by a criterion.
Until now Refine3D ran by hand (`docs/reports/` copia chain, the `tutorial_sta/` scripts).

## Facts (RELION 5.0.1 source)
- `--auto_refine --split_random_halves` refuses to run without MPI. The GUI runs
  `mpirun -n 3 relion_refine_mpi`: a leader and one follower per half-set. Two followers can share one GPU.
- The tomo form is `--ios <optimisation_set.star> --ref <map>`; particles from 2D stacks (subtomo extraction or
  the Warp Export job) and RELION's own 3D subtomograms both work.
- The final iteration writes without the `_itNNN` tag (`MlOptimiser::write` with `fn_root = fn_out`):
  `run_class001.mrc`, `run_half{1,2}_class001_unfil.mrc`, `run_data.star`, `run_model.star`,
  `run_optimisation_set.star`. relion prints the final resolution itself.
- GUI flags on a fresh run: `--auto_refine --split_random_halves --firstiter_cc` (unless the reference greyscale
  is trusted) `--ini_high --dont_combine_weights_via_disc --preread_images --pool --pad 2 --ctf
  --particle_diameter --flatten_solvent --zero_mask [--solvent_mask --solvent_correct_fsc] --oversampling 1
  --healpix_order --auto_local_healpix_order --offset_range --offset_step --sym --low_resol_join_halves 40
  --norm --scale --j --gpu`.

## Stage 1 — the job (built 2026-10-12, not run)
- `JobType.REFINE3D = "refine3d"`, `services/jobs/refine3d.py` (`Refine3DParams`), `drivers/refine3d.py`; spec row
  after Class 3D, depends on Reconstruct Particle (the reference).
- I/O: `input_optimisation` (any particle set: subtomo extraction, Warp Export, a set) + `input_reference`
  (Reconstruct Particle's `merged.mrc`); outputs `run_optimisation_set.star` (optimisation set) and
  `run_class001.mrc` (reference map), so a second refinement or a Class 3D can start from it.
- Command: `mpirun --oversubscribe --bind-to none -n <mpi_ranks> relion_refine_mpi …` inside `tools.relion`
  (`relion5.0_tomo.sif`, which installs OpenMPI). `--oversubscribe` because SLURM's allocation is hidden from
  the container (`--cleanenv`, `SLURM_*` unset) and OpenMPI counts physical cores; `--bind-to none` so the
  ranks do not share one core.
- Parameters: initial sampling 7.5° (`healpix_order` 3) with local searches from 3.75°
  (`auto_local_healpix_order` 4), offsets 5 px / 1 px, `ini_high` 40 Å, `low_resol_join_halves` 40 Å, optional
  solvent mask with `--solvent_correct_fsc`, `random_seed` (two seeds per arm), `mpi_ranks` 3.
- Resources: `job_resource_profiles.refine3d` = 2 GPUs (one per half-set), 8 CPUs, 96 GB, 8 h.
- Species drift chip: symmetry, as for Class 3D and Reconstruct Particle.

## Stage 2 — local angular searches (built 2026-10-18, not run)
- First runs (AGG412_20260311_MyrGFP_Grid2 job033 / job035, 26k / 33k Warp Export particles at 3.1 Å/px) searched
  globally: 36,864 orientations × 515 translations per particle, ETA 115 / 144 h for iteration 1 alone against a
  120 h walltime. The template-matching angles were never used.
- `sigma_ang` (default 4°: RELION searches ±3σ = ±12°, one 12° template-matching step) → `--sigma_ang`, so
  auto-refine searches locally around the input angles from iteration 1; `-1` restores the global search.
- The driver refuses a local search when the particles carry no angle columns or more than 1 % of them sit at
  0,0,0 (hand-picked lists and merges encode "no orientation" that way), and logs the 0,0,0 count either way.

## Owed
- Runtime: the first run on a Warp Export set (AGG412_20260311_MyrGFP_Grid2, ribosome). Checks: mpirun starts
  three ranks inside the container, the two half-sets land on two GPUs, the run converges and
  `run_optimisation_set.star` appears.
- PostProcess (masked FSC, B-factor) is not a job; roadmap 24's FSC curves come from `run_model.star` (unmasked
  half-map FSC) until it is.
- The Results tab (roadmap 30's `particle_results`) covers Class 3D and Reconstruct Particle; a Refine3D readout
  (FSC curve, angular accuracy per iteration) would be its next row.
