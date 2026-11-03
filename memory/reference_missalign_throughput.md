---
name: reference-missalign-throughput
description: "miss-alignment 0.2.0 training speed is set by the number of reconstruction workers (CPU-bound producers), not GPUs; one worker starves 3 A100 trainers"
metadata:
  type: reference
---

Source read 2026-10-14 (miss-alignment 0.2.0 wheel, warpylib 1.0.1/1.1.2): trainers consume a /tmp pool of
pickled 96³ triplets written by `--reconstruction-devices` workers (one process each, `torch.set_num_threads(1)`,
reload + CPU-downsample the full stack every 80 samples, 2-particle reconstructions). Each dataloader blocks until
its partition holds ≥ batch_size files; files are used once. Pool file count ≈ 15×31 (half-empty) = producer-bound.

AGG412 Grid2 job022: 1 worker for 3 trainers → 6.65 s/step (96 samples), GPUs 0%, CPU 97% idle. Tool docs use
~3 workers per trainer; repeat GPU indices (`3,3,3,3,3,3,0,1,2,...`), workers may share training GPUs (~1 GB each).
Budget CPUs: n_recon + n_train×dataloaders + n_train ≤ cpus_per_task.

Progress bar total = steps per rank (epoch = steps_per_epoch × batch_size samples, split over ranks).
Checkpoints: per-epoch Lightning ckpts (top-3) + iter{N}/model.ckpt per macro-iteration; only whole
macro-iterations resume (`--start-at-iteration`); mid-iteration = weights-only warm start at best.
Each macro-iteration fine-tunes the previous weights. Early stopping can end an iteration after epoch ~20.
See [[project_missalign_perf_walltime_qos]] (its "data loading not the bottleneck" note predates this).

**Built 2026-10-14 (uncommitted, NOT RUN):** `reconstruction_devices` replaced by `patch_builders` (0 = auto = every
CPU left after trainers + loaders; spread over non-training GPUs first). Shared `worker_plan()` in
services/jobs/miss_align.py feeds the driver, the job-tab summary (layout rows + warnings) and the walltime
estimate (min(builders × 12, trainers × batch/3.5 s) cubes/s). Docs §8.1 + CONFIG_PREAMBLE carry the diagram.

**Compile race:** two 3-trainer job022 runs died ~1 min into training on a missing Triton cache .json
(pytorch#134384), the second (2026-10-15 05:38, clip-g4-5, after ~10.5 h in the queue) even with
TORCHINDUCTOR_COMPILE_THREADS=1. Driver now sets TORCHDYNAMO_DISABLE=1 for every run (eager training; compiled vs
eager checkpoint keys differ, so it must not flip between resumes). Eager speed cost unmeasured. After a rank dies
the tool hangs until the 45-min idle watchdog kills it.

**Loaders are the second limit (2026-10-15, clip-g4-0, 1 trainer / 11 builders / 4 loaders, 16 CPUs, eager):**
1.45 s/step = 22 cubes/s, pool full (~1000), trainer GPU mostly 0 % with bursts → loader-bound (each loader builds a
whole 32-cube batch, ~5.5 cubes/s). worker_plan auto now balances builders (12/s) vs loaders (5/s) instead of
"loaders ≤ 4": 16 CPUs → 10 loaders + 5 builders (~50 cubes/s expected, unverified). 4-GPU jobs waited days
(squeue --start: 6 d); 2 GPUs on g2|g3|g4 started in minutes. P100 (sm_60) unsupported by the container.

**Balanced run (2026-10-15 11:10, job 3449137, clip-g4-5, 2 GPUs/16 CPUs, 1 trainer, 5 builders, 10 loaders, eager):**
3.05 step/s ≈ 98 cubes/s, iteration-1 training ~2 h 45 (vs 18.5 h first run). Pool steady ~320 (= 10 partitions ×
32, the loaders' wait level → builders just keep up); trainer GPU ~80 % with dips. Constants recalibrated: builder
16/s, loader 8/s, fed trainer 1.0 s/step (A100 measured 0.33). Next levers: a 2nd trainer (needs ~2× CPUs and
pool_size 2000, nodes rarely have >19 idle CPUs) or compile back for single-trainer runs (resume key mismatch risk).
Alignment-phase cost still unmeasured.
