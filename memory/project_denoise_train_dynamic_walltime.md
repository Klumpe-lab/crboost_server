---
name: project_denoise_train_dynamic_walltime
description: "denoise_train SLURM --time now scales with selected-TS count; \"run_command TIMEOUT after Ns\" is really SLURM walltime exhaustion, not a hang"
metadata: 
  node_type: memory
  type: project
  originSessionId: 6f960113-f2cc-4fef-94d9-9b2d9ee93bfd
---

denoise_train (cryoCARE/IsoNet) is a SINGLE (non-array) job — one model reduced from ALL selected tilt-series, so its SLURM `--time` must cover every tomogram **sequentially** (unlike the per-task array jobs in [[project_per_ts_parallelization]] / [[project_denoise_per_ts_split]], train/predict never migrated to the array model).

**The bug (2026-06-24):** `External/job012` IsoNet refine on a 45-TS project died at epoch 5/10 with a flat 2 h allocation (conf.yaml `job_resource_profiles.denoisetrain.time`). Needed ~168 min = ~48 min prepare_star/make_mask + ~120 min refine (10 epochs × ~12 min). Watchdog `make_mask`+`refine` are 10-epoch defaults (`isonet.py refine` takes no `--epochs`).

**Diagnostic gotcha:** the kill message was `[run_command] TIMEOUT after 3892s — killing process group` → `Signals.SIGKILL`. That is NOT a hung process and NOT SLURM's own `--time` SIGTERM. `drivers/driver_base.py:_derive_watchdog_timeout` sets the watchdog to **0.9 × remaining SLURM walltime** (`SLURM_JOB_END_TIME - now`). So 3892s = 0.9 × (remaining when `refine` started) ⇒ the real cause is the SLURM allocation being too short. Reading any `run_command TIMEOUT after Ns` → back-solve N/0.9 to recover the walltime that was actually too small.

**The fix:** `services/jobs/denoise_train.py` overrides `get_effective_slurm_config()` to scale `time = base + per_ts × n` floored at the profile time and capped, only when the user hasn't pinned `time` in `slurm_overrides`. Constants: BASE=30 min, PER_TS=5 min, CAP=8 h (sized for IsoNet, the slow path; cryoCARE over-allocates harmlessly). Scaling input = `ProjectState.import_selected_tilt_series` (in-memory, no disk I/O, known at deploy time before the upstream tomograms.star exists — important because the orchestrator submits the whole afterok DAG up front). 45 TS → 4:15:00. Count=0 (unknown) → unchanged 2 h, no regression. A narrowing `tomograms_for_training` filter only over-estimates (safe). Bigger `--time` ⇒ bigger watchdog ⇒ no premature kill; the watchdog logic itself needed no change.
