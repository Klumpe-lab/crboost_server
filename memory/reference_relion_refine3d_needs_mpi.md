---
name: relion-refine3d-needs-mpi
description: RELION 5 relion_refine refuses --split_random_halves (Refine3D / auto-refine) without MPI; the server only ever ran serial relion_refine (Class3D), so a Refine3D job type needs mpirun -n 3 relion_refine_mpi inside relion5.0_tomo.sif
metadata:
  type: reference
---

Serial `relion_refine --auto_refine --split_random_halves` dies at once with "Cannot split data into random halves without using MPI! For debugging ONLY, use --debug_split_random_half 1 (or 2)" (hit 2026-09-08 on the copia tutorial chain). The GUI layout is `mpirun -n 3 relion_refine_mpi` (leader + one follower per half-set); two followers can share one GPU. Nothing in `drivers/` or `services/computing/container_service.py` launches MPI; the `unset SLURM_* PMI_* OMPI_*` list there plus `--cleanenv` keep a container-internal mpirun from trying the SLURM launcher. Whether `relion5.0_tomo.sif` ships mpirun/relion_refine_mpi was still unverified when written — see [[copia-tutorial-sta-chain]] stage 02 self-check.
