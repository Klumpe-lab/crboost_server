---
name: project-refine3d-job-roadmap31
description: "Refine 3D pipeline job (roadmap 31) built 2026-10-12; ran on Grid2 2026-10-18..20 (missAlign 8.27 Å vs 11.23 Å), --continue works; mpirun -n 3 relion_refine_mpi --auto_refine inside relion5.0_tomo.sif"
metadata:
  node_type: memory
  type: project
  originSessionId: f2ccfb83-54d7-4366-8c8a-33aa5f0cfec3
  modified: 2026-10-12T19:25:43.168Z
---

Asked for 2026-10-12 after the Warp Export job's first run (roadmap 24's verdict is gold-standard Refine3D per arm,
two seeds). Built in `~/dev/crboost_server_warp_export` (uncommitted at time of writing): `services/jobs/refine3d.py`,
`drivers/refine3d.py`, `JobType.REFINE3D="refine3d"`, spec row after Class 3D (depends on Reconstruct Particle),
`job_resource_profiles.refine3d` (gpu:2, 96G, 8 CPUs, 8 h) in the template + both conf.yaml files, species sym
drift chip, relion cluster probe checks mpirun + relion_refine_mpi. Roadmap `docs/roadmaps/roadmap_31-refine3d-job.md`.

Command: `mpirun --oversubscribe --bind-to none -n 3 relion_refine_mpi --ios … --ref … --auto_refine
--split_random_halves …` (container hides the SLURM allocation, so --oversubscribe). Completion = un-tagged
`run_optimisation_set.star`, `run_class001.mrc`, both `run_half*_class001_unfil.mrc` (RELION writes the converged
iteration with fn_root = fn_out). Outputs: optimisation set + reference map.

**Why:** [[project_warp_export_job]] success bar is a sub-15 Å gold-standard map; Class 3D K=1 is not gold-standard.
**How to apply:** first run = on the Warp Export set of AGG412_20260311_MyrGFP_Grid2 (ribosome, job016 + job017
reference). Unknowns: mpirun inside the sif, GPU mapping of the two followers. No PostProcess job, no Results tab yet.
Merge note: the main tree's spec.py adds `plugins=("particle_results",)` to the Class 3D row right above the new
row — possible one-hunk conflict, keep both.

2026-10-19: re-runs continue from the last finished iteration (see [[project_warp_export_job]] for the mechanics);
first real use = Grid2 job036/037 after the /groups drop. Not run yet.
