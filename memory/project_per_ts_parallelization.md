---
name: Per-TS Parallelization Refactor
description: Major architectural refactor to parallelize WarpTools jobs over tilt-series boundaries using SLURM array jobs, driven by scaling to 18-100+ TS datasets
type: project
originSessionId: f80f4ac9-eb40-4ab9-ad61-a75e3f73af81
---
Datasets are growing from 1 TS (toy) to 18-25 TS (current) and potentially 100s (future). Monolithic single-SLURM-job-per-jobtype model hits walltime and memory limits.

**Why:** Warp processes tilt series sequentially within a single invocation. Coupled with containerized execution and SLURM resource limits, this becomes unusable at scale.

**How to apply:**
- ts_reconstruct.py already implements supervisor/array-task pattern (the blueprint)
- Generalize to ts_alignment (with ts_import as global pre-step) and ts_ctf
- Extract reusable ArrayJobDispatcher from ts_reconstruct
- WarpTools metadata is per-TS independent (confirmed): each TS has its own .xml and .tomostar
- Key gotcha: tomostar files have relative _wrpMovieName paths that break when copied; must convert to absolute (see _copy_tomostar_with_absolute_paths in ts_reconstruct.py)
- ts_import must remain a global pre-step (reads all mdocs, creates all tomostars)
- Supervisor SLURM config should be generalized beyond tsreconstruct_supervisor_slurm
