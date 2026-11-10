# Roadmap 50 — missAlign on CBE.next: the training job on the card, memory and CPUs the cluster has

**Status:** S1 BUILT 2026-11-10 (H14, H15, the profile, `num_gpus` 2 and `pool_size` 3000 by default, `num_gpus` ≤ 8; ruff and boundaries clean, toy checks in HOTFIXES), NOT RUN: roadmap 49's R2 is the check. Scoped the same day from two running jobs (roadmap 49's R1 on two RTX 6000s, a 22-series K3 project's on one
B200) and the tool's source. Nothing built. HOTFIXES H14 (the profile cannot name a card) and H15 (the training pool
streams through BeeGFS) are its first stage. Tool reference: `docs/miss-alignment.md` §8.1; the campaign record of
the last rollout: `completed/roadmap_29-agg412-sta.md` §6.2; the cluster: roadmap 45 §2 and roadmap 47.

**In one line.** A PAPER run over ~140 series takes 27–31 h today because the job lands on an RTX 6000 (the card is
the limit there) and, on any card, feeds its trainer from a pool on the shared BeeGFS; put it on two B200s with
48 CPUs and the pool in RAM and the same run is a working day or less, and once one project's models exist the other
seven need no training at all (`infer`).

## 1. What a run costs today (measured 2026-11-10)

One macro-iteration = a training phase (`steps_per_epoch × epochs` steps, 32 triplet files a step, early stop at
epoch 21–24 of 30 in every run so far, so ~21–24 k steps) + an alignment phase (one process per visible GPU, series
pulled from a queue).

| Job | Card, layout | Training | Alignment | The limit |
|---|---|---|---|---|
| R1 missAlign, SLURM 1526891 (`AGG412_20251113_412_rerun/External/job006`, 139 series, PAPER) | 2 × Quadro RTX 6000, 16 CPUs: 1 trainer, 5 builders, 10 loaders | **2.29 steps/s** → 2.5–2.9 h per iteration | not reached yet | GPU 0 (trainer + 2 builders) at 100 % in every sample; GPU 1 (3 builders) 16–38 %. The card. CPUs would buy nothing |
| K3 project missAlign, SLURM 1514330 (22 series, PAPER, 5.53 Å stacks) | 1 × B200, 8 CPUs: 1 trainer, 3 builders, 4 loaders | **3.65–4.09 steps/s** → 1.4–1.65 h per iteration (21 k, 21 k, 24 k, 21 k steps for iterations 1–4) | 8.8 s per series (30 Å anchoring), 12.1–12.3 s (20 Å anchoring), 7.2 s (10 Å global) | The producers: the trainer holds 24 % of the card's SMs (`nvidia-smi pmon`), the card averages 58 % (`sstat gpuutil`); loaders and builders sit in `D` (I/O wait) on the BeeGFS pool |
| Grid2 job022 (roadmap 29 §6.2, 2026-10-15, 48 series) | 2 × A100, 16 CPUs: 1 trainer, 5 builders, 10 loaders | 2.74–3.14 steps/s → 1 h 55 – 2 h 22 per iteration (19 h 36 over nine) | 61 min in all; 13 min for each 18.6 Å anchoring round | Balanced; the trainer ~80 % busy |

So, per card with crboost's current layout: RTX 6000 2.3 steps/s (card-bound), A100 3.0 (balanced at 16 CPUs), B200
3.7–4.1 with half the CPUs and three quarters of the card idle.

**Where the pool lives.** The tool writes its cubes to `tempfile.gettempdir()` → the job's `/tmp`, which on CBE.next
is a private directory on the shared BeeGFS (no local disk: `TmpDisk=0`). `sstat` 2026-11-10: SLURM 1514330 had moved
**26.2 TB** through it in 7 h 22 (3.7 steps/s × 32 files × 5.3 MB, written once and read once ≈ 1 GB/s); R1 2.8 TB in
1 h 27. The working set is 0.7–5 GB (135–991 files). `/dev/shm` is RAM (756 GB tmpfs, charged to `--mem`), the
container sees the job's private one, and SLURM's `namespace/tmpfs` removes it at job end (probe in HOTFIXES H15).

**What a B200 node offers.** 8 full B200s (4 nodes) or 56 slices (4 nodes), 192 CPUs and 1.5 TB each; the full nodes
ran 32–36 CPUs busy of 192 on 2026-11-10 with every card taken (another user's array of 31 single-card jobs; the
A100s taken too; 9 of 10 RTX 6000 nodes idle, which is why R1 got one). Billing is the largest TRES: a B200 is 24 an
hour, a CPU 1, memory 0.134 a GB — two B200s carry 48 CPUs and 179 GB at no extra cost. QOS `g_medium` allows 16
B200s, 768 CPUs and 2 days a job; `g_long` 8, 384 and 14 days. The tool is one node (DDP over local devices).

**What crboost gets wrong on this cluster.**
1. `MissAlignParams.get_effective_slurm_config` replaces the profile's `gres` with `gpu:{num_gpus}`
   (`services/jobs/miss_align.py:806-807`), so `job_resource_profiles.missAlign` can never name a card; a `constraint:
   b200` would also match the MIG nodes, where `gpu:2` is two 23 GB slices (H14).
2. No `TMPDIR` for the tool, so the pool goes to BeeGFS (H15).
3. The layout constants are the old cluster's (`_BUILDER_CUBES_PER_SEC` 16, `_LOADER_CUBES_PER_SEC` 8,
   `_TRAINER_STEP_SEC` 1.0; `:219-220, :358`): on a B200 node three builders and four loaders delivered 117–131 files/s
   (≥ 39 per builder, ≥ 29 per loader, with the pool on BeeGFS), so the estimate asks ~5× the real time (roadmap 29
   §6.2.6) and the auto split puts CPUs in the wrong place: at 48 CPUs and `pool_size` 1000 it makes 32 builders
   because the pool caps loaders at 15.
4. `num_gpus` ≤ 4 (`:569-575`) on nodes of 8; the SLURM note and the config preamble still describe `g2|g3|g4` and
   A100 times (`:422, :459-473`).

## 2. The levers, in order of what they buy

| # | Lever | Buys | Costs | Stage |
|---|---|---|---|---|
| L1 | **Two full B200s** by typed GRES (`gpu:nvidia_b200:2`), trainer on one, builders on the other | training 2.3 → ≥ 3.7 steps/s before any other change (R1's 2.7 h per iteration → ≤ 1.6 h); alignment on two cards | 48 billing/h (R1 bills 16); the B200s are the contended card: a 2-card request waits for two to free on one node | S1 (H14) |
| L2 | **The pool in RAM** (`TMPDIR` under `/dev/shm`) | the builders' and loaders' I/O waits go, and ~1 GB/s of churn on the shared scratch with them; the loaders' per-sample directory listing becomes a memory operation | `pool_size × 5.3 MB` of the memory request (16 GB at 3000); cleanup by the driver and by SLURM | S1 (H15) |
| L3 | **48 CPUs** (the two cards' free share), `pool_size` 3000, 160 G | feeds the trainer: it has 3–4× headroom on the card at today's rate | nothing in billing; ~125 GB of RSS + pool | S1 |
| L4 | **Constants and the split from the S1 run**; builders off the training card while the other has room; `num_gpus` ≤ 8; `pool_size` sized with the loaders | a right estimate (no more 5½-day asks clamped to the QOS) and a layout that matches the card | one run's `run.out` | S2 |
| L5 | **Two trainers (DDP)** once the single-trainer ceiling is known | halves the training phase if L3 keeps both fed (each trainer does half the steps at the full batch; the tool scales the learning rate × 2 and its own examples run two) | 2 more B200s (or the builders share), 2× CPUs; MA7's history (3 trainers starved by 1 builder) was a layout fault, not DDP's | S2, D3 |
| L6 | **Train once, infer the rest** (`miss-alignment infer`: a previous run's `iter1..N/model.ckpt` aligns a new dataset, no training) | for roadmap 49's seven remaining projects the training phase (9 × 1–1.6 h) disappears: a project is its alignment phase, ~1.5 h on two B200s | a `model_from` input and the inference config in the driver (roadmap 24 §infer already specced it); the maintainer's call that one project's models serve the others (same sample, scope, pixel size); roadmap 49's own comparison per project is the check | S3, D4 |

**What is not a lever.** A MIG slice (a seventh of a card: slower than the RTX for the trainer; a job cannot mix a
whole card and slices). More than one node (the tool spawns its trainers and alignment processes over local
devices). `torch.compile` (the Triton cache race, roadmap 29 MA7; and the trainer is not the limit until the
producers are). A bigger batch (the budget is in steps: more cubes a step is more producer work for the same steps).
A shorter schedule or budget is the maintainer's science call, not this roadmap's; early stop already ends every
iteration at epoch 21–24 of 30.

**Alignment phase, for ~140 series on two B200s** (from SLURM 1514330's per-series times; the `[3,3]` rounds are
unmeasured on a B200, on the A100s they cost what `global` did): three anchoring rounds at ~12 s a series → 14 min
each, six global / `[3,3]` rounds at ~7 s → 8 min each: **~1.5 h a run**, 45 min on four cards.

## 3. The arithmetic (one project of ~140 series, PAPER, early stop at ~22 k steps)

| Setup | Training (9 iterations) | Alignment | Run | Notes |
|---|---|---|---|---|
| Today: 2 RTX 6000, 16 CPUs (R1) | 9 × 2.7 h = 24 h | ~3 h (RTX, unmeasured) | **~27–31 h** | inside its 2-day limit; bills 16/h |
| B200 as the K3 run: 1–2 cards, 8 CPUs, pool on BeeGFS | 9 × 1.5 h = 13.5 h | 1.5 h (2 cards) / 3 h (1) | **~15–17 h** | L1 alone |
| S1: 2 B200s, 48 CPUs, pool in RAM, `pool_size` 3000 | 9 × 0.5–0.8 h if the fed trainer runs 2–3× today's rate (its 24 % share of the card allows it); 9 × 1.5 h if the CPUs buy nothing | 1.5 h | **~6–9 h** (≤ 15 h) | the S1 run measures it; bills 48/h |
| S2: two trainers on 4 B200s, 96 CPUs | about half of S1's | 45 min | **~4–5 h** | when four cards are free on one node |
| S3: `infer` from R1's models | none | 1.5 h | **~1.5–2 h** | roadmap 49 R2–R8 |

Over the seven remaining AGG412 projects (815 series, roadmap 29 §6.2): today's path ~190 h of wall and two
weeks of a 2-day QOS babysitting; S1 ~50–60 h; S3 ~12 h.

## 4. Stages

### S1 — The card, the pool, the CPUs (H14 + H15 + conf)
- H14: `get_effective_slurm_config` keeps the profile's GPU type and sets only the count (`gpu[:type]:{num_gpus}`);
  an override is untouched. Profile (dev conf, `conf.template.yaml`, the lab conf): `gres: gpu:nvidia_b200:1`,
  `cpus_per_task: 48`, `mem: 160G`, `qos: g_medium`, `time` floor as now; `MissAlignParams.num_gpus` default 2,
  `pool_size` default 3000.
- H15: the driver makes `/dev/shm/crboost-<user>-<job id>`, passes `TMPDIR` in the tool's environment prefix, removes
  it in `finally`; the estimate and the SLURM note add the pool to the memory.
- *Success:* R2's missAlign `sacct` shows `gres/gpu:nvidia_b200=2`, `cpu=48`; its `run.out` says `Created pool
  directory: /dev/shm/...`; `sstat fs/disk` stays in the tens of GB; no `D` state in `top`; steps/s and the
  per-iteration times in the run log below.

### S2 — The layout from the S1 run
- Constants from S1's `run.out` (steps/s, per-series alignment times by mode and pixel size); the trainer's fed step
  time on a B200; `_balanced_loaders` with the pool cap sized from `pool_size`; builders on the non-training card
  only while it has room; `num_gpus` ≤ 8; the SLURM note and the preamble rewritten for CBE.next (B200 / A100 / RTX
  6000 numbers, no `g2|g3|g4`); the estimate within 1.5× of the run.
- D3 two trainers: a `training_gpus` 2 run on four B200s when the single-trainer ceiling is known.
- *Success:* the job tab's layout and estimate match the next run within 1.5×; a 4-card, 2-trainer run at ≥ 1.8× the
  S1 steps/s.

### S3 — Train once, infer the rest
- `MissAlignParams.mode: train | infer`, `model_from` (a missAlign job, any project, with `iter1..N/model.ckpt` for
  the schedule's N); the driver writes the inference config (`general.data_directory`, `model_run_directory`,
  `iteration_settings`, `seed`; `tilt_series_alignment`) and runs `miss-alignment infer --config-file …
  --start-at-iteration k`; same staging, stamp, z-box, resume by iteration, emit and registry as `train`; refuses a
  model whose iteration count, modes or stack pixel size differ from the job's schedule.
- *Success:* R2 refined from R1's models in ~2 h; roadmap 49 §5's comparison for R2 (per-tilt change, picks per
  tomogram, overlap with the old project) reads as R1's did.

## 5. Decisions

| # | Decision | Why | Who, when |
|---|---|---|---|
| D1 | R1's missAlign (1526891) is left to finish on the RTX 6000s | 1.5 h sunk, ~27–31 h to go inside its limit; a resubmission queues behind a 2-B200 wait of unknown length while every B200 and A100 is taken; S1 goes into R2 | proposed 2026-11-10 |
| D2 | When no B200 is free and the wait is long, the job's SLURM override names the A100 (`gpu:nvidia_a100-sxm4-40gb:2`, 3.0 steps/s balanced at 16 CPUs, 6 billing/h) or the RTX 6000 (2.3 steps/s, idle most days); the profile stays B200 | every card used, the faster one when the user picks (roadmap 47) | proposed 2026-11-10 |
| D3 | Two trainers only after S1 measures the single-trainer ceiling | MA7: a starved multi-trainer run was 20× slower than a fed single one | proposed 2026-11-10 |
| D4 | `infer` from R1's models for roadmap 49's R2–R8 | the sample, scope, dose and 6.2 Å stacks are the same across the eight projects; the per-project comparison of roadmap 49 §5 is the check | maintainer, owed |
| D5 | `pool_size` 3000 (16 GB in RAM), loaders capped by it, not the other way round | 1000 caps the loaders at 15 and sends every further CPU to builders | proposed 2026-11-10 |

## 6. Run log (append; newest last)

- **2026-11-10** — Scoped. R1's missAlign (1526891) on `cbe-g3-1`, two RTX 6000s, 16 CPUs: 2.29 steps/s, GPU 0 at
  100 %; 11.3 k of 30 k steps after 1 h 22 of iteration 1. The K3 project's (1514330) on `cbe-g5-4`, one B200, 8
  CPUs: 3.65–4.09 steps/s, trainer at 24 % of the card, iterations 1–4 done in 7 h 22, 26.2 TB through BeeGFS.
  `/dev/shm` probe: the container sees the job's private tmpfs; `TMPDIR=/dev/shm` redirects the tool's `tempfile`.
- **2026-11-10, later** — S1 built, not committed: `services/jobs/miss_align.py` (`gres_with_count`, `mem_plus`,
  `POOL_FILE_BYTES`; the effective config keeps the profile's card and adds the pool to `mem`; `num_gpus` default 2,
  ≤ 8; `pool_size` default 3000; the SLURM note), `drivers/miss_align.py` (`pool_root()`, `TMPDIR` in the tool's
  environment, `rmtree` in `finally`), `config/conf.yaml`, `config/conf.template.yaml` and the lab copy
  (`gpu:nvidia_b200:1`, 48 CPUs, 160G). Toy: profile `gpu:nvidia_b200:1` + `num_gpus` 2 → `gpu:nvidia_b200:2`,
  mem 160G + 3000 files → 176G, an A100 override untouched. The server that submits must restart to use it.
- **2026-11-10, evening** — S2's builder placement pulled forward: `worker_plan` sends builders round the non-training
  cards only (the trainers' cards only when there are no others); 4 cards, 2 trainers, 48 CPUs → 16 builders on
  GPUs 2–3, 15 loaders a trainer. Restart plan for R1: every B200 and A100 held by one other user's array
  (`sbatch --test-only` says 2026-11-16 for either), nine RTX 6000 nodes idle → cancel job006 once `iter1/model.ckpt`
  exists (the resume keeps it), reset its overrides, `gpu:quadro_rtx_6000:4`, `num_gpus` 4, `training_gpus` 2,
  `pool_size` 3000, Run in place. First DDP run in crboost; expected ~11–14 h for the eight iterations left against ~24.
