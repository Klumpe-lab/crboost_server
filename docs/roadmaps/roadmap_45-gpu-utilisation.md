# Roadmap 45 — GPU use on CBE.next: measure every job, then close the gaps

**Status:** scoped 2026-11-06. S0's probes P2 and P4–P7 ran 2026-11-08 (§11); S1 built the same day and piloted in
two projects (§11): the CPU steps run without a GPU, and a template-matching task took 186 s against 165 s for a reason
not yet found. Open: D1 (what IT's number is; no message sent yet), S2–S6 and D2–D8. Pickup (§11, 2026-11-09): the
slices pilot, P9 (angle lists, HOTFIXES H7), P10 (Warp chunks), P11 (tsImport) and P12 (item dependencies), all
written and not run; then two to four template-matching processes per GPU under MPS (P7: 1.45× at four; P7b not run).

**In one line.** Cluster IT told a colleague that crboost jobs idle on the B200s about a quarter of the time while
their data is read into GPU memory, and advised putting data on scratch. (The report has been passed on in two ways:
idle a quarter of the time, which is 75 % busy, and only 25 % utilised.) Counted by GPU-second the lab's crboost jobs
were 92 % busy in the first six days; counted per job they were 49 % (median 32 %). This roadmap pins down which of
those IT saw, has every job report its own GPU, CPU, memory and read use through a small recorder (the meter, S2), and
fixes the causes the numbers name, so that "full" is a figure per job type and not a hope.

**Not in this roadmap:** pytom's splits, precision and scores (roadmap 43); the Python import cost of every array
task (roadmap 39 §6 item 1: measured there, and one of the phases the meter below splits out); what a tool computes
(Warp, AreTomo, RELION and easymode keep their algorithms; their own throughput flags are in scope); anything the
cluster owns — MIG layout, DCGM, MPS, the scratch policy — which is asked of IT, not built around.

## 1. Goal

**Use case.** The maintainer opens a finished job and reads, from the job's own record, how much of the GPU, CPUs and
memory it asked for it used, and where the rest of the wall time went: start-up, reading inputs, waiting on the CPU,
the GPU working, writing outputs. For the main job types a full-B200 job is busy at least 80 % of its wall time, or
its page says why it is not. No step asks for a GPU it never touches. When IT's dashboard shows one of our jobs idle,
one command lists the jobs behind it and the phase they were in.

**What "utilisation" means here.** Three levels, because one percentage has been standing in for all of them:

1. *Used at all.* Did the job put anything on the GPU (`gres/gpumem` above 0)?
2. *Busy.* The share of time some kernel is running: nvidia-smi's `utilization.gpu`, SLURM's `gres/gpuutil`, what most
   dashboards plot. It is coarse: one kernel on a few of the card's SMs counts as 100 %.
3. *Filled.* How much of the card the kernels use: SM activity, tensor pipes, memory bandwidth (DCGM's profiling
   fields). Only this level says a B200 is full.

The 80 % target is on level 2, over the job's wall time on a full B200, with level 1 at 100 % of GPU requests. Level 3
is a diagnostic: a job that is busy but not filled is not fixed by making it busier but by giving it a smaller GPU (a
slice) or sharing the card. CPUs and memory get the same treatment: requested within a stated factor of the peak used
(D2).

## 2. What exists (checked 2026-11-06)

**The cluster.**

- Partition `g`: 8 nodes `cbe-g5-*`, each 192 CPUs, 1.5 TB of memory, 4 full B200 and 28 MIG `1g.23gb` slices (4 more
  cards cut in sevenths). Partition `c`: 12 CPU nodes, 192 CPUs, 772 GB. Billing is the largest of CPUs (1 each),
  memory (0.134 per GB), a B200 (24) and a slice (3.43; roadmap 43 §3), which is a card's fair share: a full B200 is
  paid for like 24 CPUs or 179 GB, so up to that much of both comes with it at no extra billing, and a slice like 3.4
  CPUs or 26 GB.
- No node-local disk (`TmpDisk=0`). Per the site's storage documentation (as the `cbe-data` skill of the clip-hpc
  plugin restates it, 2026-10-29), a job's private `/tmp` and `$SCRATCHDIR` are on the same all-flash BeeGFS (12
  storage servers, 180 NVMe targets, RDMA; 155 GiB/s measured for the whole system, less for one busy job). `/dev/shm`
  is RAM and counts against `--mem`. All three are deleted automatically.
- `/groups` is campus NFSv3 storage (project data, the container images, the shared CUDA kernel cache; the group is at
  82 % of its quota), spread over 128 storage units with placement by folder: one folder holding a dataset, or many
  tasks writing into one folder, makes a hot spot for the job and for everyone on that unit. The site advises one
  subfolder per task and fewer than 10,000 files in a folder. `/users` is NFSv4.2 (home, the dev checkout and its
  venv); `/scratch` is BeeGFS (1.3 PB, deleted after 28 days: roadmap 43 §3).
- Site tools: `jobeff` (CPU and memory efficiency only), `jobinfo`, `psjob`, `stage` (a parallel copy submitted as a
  batch job), `myquota`. The public docs name no GPU-utilisation monitoring, dashboard or threshold.
- SLURM 26.05.3 gathers `gres/gpumem` and `gres/gpuutil` for each job step every 30 s through cgroups, keeps batch
  scripts (readable for one's own jobs only) and records each job's `StdOut` path. The default QOS is `g_short` (8 h).

**crboost today.**

- Every job is named `CryoBoost` (`config/qsub.sh:2`), so accounting and IT's dashboard cannot tell a Warp motion
  task from a RELION refine. §3 recovers the type from the stored batch script (the `drivers/<name>.py` it runs) and
  the job's directory from `StdOut`.
- The GPU is asked for per job type (`config/conf.yaml:59-178`; `slurm_defaults` is `gpu:1`). Typed
  (`gpu:nvidia_b200:N`) for segment, templatematching, class3d and refine3d; everything else is untyped and lands on a
  full B200 or a slice, unplanned (roadmap 43 §2). Candidate extraction (`:135`), subtomogram extraction (`:148`) and
  particle reconstruction (`:160`) ask for `gpu:1`, though the code says the first and the last are CPU-only
  (`services/jobs/candidate_extract.py:125-132`, `services/jobs/reconstruct_particle.py:84`) and the subtomogram driver
  passes `relion_tomo_subtomo` no GPU option while a comment calls it GPU-heavy
  (`services/jobs/subtomo_extraction.py:128-136`). The runs in §3 never touched the GPU in any of the three.
- Ten job types run as arrays with one tilt series or tomogram per task: Warp motion + CTF, AreTomo alignment, Warp
  CTF, Warp reconstruct, Warp export, denoise predict, segment, template matching, candidate extraction and subtomogram
  extraction. A CPU supervisor on `c` submits `--array=…%throttle` (20 for Warp motion, alignment, CTF and reconstruct
  and for denoise predict; 16 for Warp export, template matching and the two extractions; 8 for segment), and each task
  holds the job's full GPU request through its container start, reads, tool run and output moves. Singles:
  tilt filter, missAlign, denoise training, particle reconstruction, Class3D, Refine3D.
- Inputs are read where they live. The Warp motion task symlinks its tilt series' frames into a staging directory
  (`stage_fs_environment`, `drivers/fs_motion_and_ctf.py:165`) and the tool reads them from `/groups`; nothing is
  copied to faster storage. The tool images (5–13 GB each) and the shared CUDA cache are on `/groups` too. Warp motion
  runs `--perdevice 2` on 2 CPUs (`services/jobs/fs_motion_ctf.py:67`, `conf.yaml:67`); the other Warp tools run
  `--perdevice 1` on one item per task, where the flag has nothing to share. No driver overlaps reading with compute
  (only the tilt filter's data loader does); what overlap exists sits inside the tools (Warp `--perdevice`, RELION `--j`
  and `--pool`, IsoNet `--ncpus`). Every tool's compile cache (cupy, Triton, torch, numba) is in the job's private
  `/tmp` (`services/computing/container_service.py:25-33`); only the CUDA cache was moved to `/groups`
  (`gpu_kernel_cache`, `conf.yaml:210`). `/scratch` is already bound into every container (`container_binds`,
  `conf.yaml:202`).
- Nothing measures a job. The only SLURM accounting call reads State (`services/computing/slurm_service.py:671`); no
  NVML, DCGM or psutil anywhere; the one GPU fact a driver reads is the card's memory (`allocated_gpu_memory_bytes`,
  `drivers/driver_base.py:240`). Hooks to build on: `run_command` (`drivers/driver_base.py:348`) is the one place every
  tool call passes, and already runs a watchdog thread; `_land_job`
  (`services/scheduling_and_orchestration/pipeline_runner.py:302`) already lands a driver-written file (the tilt
  filter's `commit.json`) on a job when it ends; the job page header already carries two chips
  (`ui/pipeline_builder/job_tab_component.py:239-240`).
- The container wrapper always passes `--cleanenv` (`services/computing/container_service.py:112-116`), which the site
  docs advise against on GPU nodes because it removes `CUDA_VISIBLE_DEVICES`. Every job in §3 ran on its allocated card
  (SLURM saw its GPU memory), so this is a check (S0, `capability.sh`), not a finding.
- `config/qsub.sh` is the site's file and its exit-marker block must stay byte-identical
  (`services/computing/slurm_service.py:172`), so a recorder goes in the driver or in `write_sbatch_script`
  (`services/computing/slurm_service.py:166`), which already adds `--qos`, `--array` and `--open-mode` lines.

## 3. Baseline: SLURM's own record (read 2026-11-06)

All crboost jobs on partition `g` since 2026-11-01, completed ones only (failed, cancelled and running ones held 8.8
GPU-h more), read with `docs/reports/gpu-util/scripts/sacct_gpu_report.py`; the full tables, the column definitions
and the caveats are in `docs/reports/gpu-util/baseline-2026-11-06.md`. Full B200s, 935 jobs, 112.2 GPU-h:

| Job type | Jobs | GPU-h | Busy % | CPU eff. | Median job |
|---|---:|---:|---:|---:|---:|
| Segment (easymode) | 125 | 38.2 | 96.8 | 0.20 | 17 min |
| Template matching | 143 | 7.0 | 98.6 | 0.20 | 2.8 min |
| Refine3D (2 GPUs) | 1 | 3.4 | 100 | 0.78 | 1 h 41 |
| missAlign | 1 | 2.9 | 98.0 | 0.43 | 2 h 53 |
| Candidate extraction | 140 | 2.0 | 0.0 | 0.41 | 36 s |
| Warp motion + CTF | 74 | 1.7 | 23.4 | 0.66 | 76 s |
| Class3D | 3 | 1.6 | 94.8 | 0.83 | 19 min |
| AreTomo alignment | 41 | 0.5 | 30.2 | 0.39 | 48 s |
| Warp export | 132 | 0.5 | 0.0 | 0.13 | 12 s |
| Particle reconstruction | 3 | 0.4 | 0.0 | 0.39 | 3.3 min |
| Warp reconstruct | 16 | 0.3 | 56.1 | 0.18 | 61 s |
| Warp CTF | 14 | 0.1 | 0.0 | 0.41 | 17 s |
| Other users' jobs (scripts hidden) | 239 | 53.8 | 95.3 | 0.22 | 20 min |

What it says:

- **By GPU-second the lab is 92.3 % busy**, and every long job is high: 95–100 % for segment, template matching,
  Class3D, Refine3D, missAlign and the other users' jobs. By that measure the 80 % target is already met for them.
- **Per job it is not.** Counted once each, the 935 jobs average 48.9 % (median 32.0 %); 503 of them ran under two
  minutes, 54 % of the jobs and 4.8 % of the GPU-hours. Over the 596 that allocated any GPU memory the per-job mean is
  76.7 %, the GPU-second weighted one 94.9 %. Of the two readings of IT's report, *idle a quarter of the time* (75 %
  busy) fits a per-job average over the jobs that touched the GPU (77 %), and *only 25 %* fits the per-job median
  (32 %; 24 % for the maintainer's own jobs). Neither fits the GPU-second figure, and the next bullet gives a third way
  to land near 75 %. Which one IT computed is D1 and is not established.
- **SLURM's busy is probably blind to start-up and teardown.** Its accounting samples every 30 s
  (`JobAcctGatherFrequency=task=30`), so a start-up or teardown shorter than an interval is barely in the average, if
  at all (a trace from the first second confirms or refutes this: S2). A dashboard that plots each card over the whole
  allocation sees that time as idle. A hand run of one 412 tomogram on a B200 took 112 s for the container command
  (roadmap 43 §3) and SLURM recorded it 100 % busy; the production template-matching tasks take 165 s at the median and
  read 98.6 %. The next bullet shows where those 165 s go.
- **A production template-matching task, from file times.** The 141 tasks of one 412 job (job019 of
  `AGG412_20251113_412`, 2026-11-05): the allocation start from accounting, the rest from the modification times of
  each tomogram's files, because the task logs carry no clock. Medians over the tasks (they do not add up exactly):

  | Phase | Median s | Share | The card |
  |---|---:|---:|---|
  | Driver start-up and staging, before the container | 3.7 | 2 % | idle |
  | Container start, pytom set-up, the search, the scores map written | 132.7 | 80 % | mostly busy; the split is unknown |
  | Angles map and `job.json` written to `/groups` | 16.9 | 10 % | idle |
  | CPU scan of the scores map | 7.7 | 5 % | idle |
  | Whole task (accounting) | 165 | | |

  The first, third and fourth rows, 28 s or 17 % of the task, are idle on the card by construction: Python and the
  driver starting, a 2 GiB map written to `/groups` at about 125 MB/s, and a CPU scan that reads the scores map back.
  The second row holds the container start, pytom's set-up, the search and the first map's write; how it splits is what
  P4b measures. Production's container-command part (rows two and three, 150 s) is 38 s longer than the hand run's
  112 s, which had a warm kernel cache and wrote to `/scratch`; the other 11 s are the driver start-up and the scan.
  If the search is about 80 s of the 133, a dashboard over the allocation reads about 50 %.
- **GPUs asked for and never touched.** Candidate extraction (140 jobs, 2.0 GPU-h) reads 3.3 GB per job and ends with
  its candidates, all without a byte of GPU memory: real work, on the CPU. Particle reconstruction (3) and subtomogram
  extraction (2) the same. Warp export (132) and Warp CTF (14) ran 12–17 s with no I/O and no GPU memory, which reads
  like tasks that found their output done (`task_already_done`, `drivers/warp_export.py:284`); that says nothing about
  whether those tools use a GPU when they have work.
- **The short Warp array tasks are the low ones:** motion + CTF 23.4 % (76 s per task, 2.5 GB read and 4.1 GB written,
  2 CPUs at 66 %), alignment 30.2 % (48 s), reconstruct 56.1 % (61 s). They hold 2.5 GPU-h of the 112 (motion 1.7,
  alignment 0.5, reconstruct 0.3); the cost grows with the tilt-series count, so a project of a hundred or more series
  spends several times what these pilots did.
- **MIG slices are invisible to this counter.** All 63 completed slice jobs report busy under 10 %, and 42 of them
  allocated GPU memory. If IT's dashboard reads the same counter, our slice jobs show as idle.
- **CPUs and memory:** the GPU-bound jobs use a fifth of the CPUs they ask for (segment and template matching: 0.20 of
  4), memory 0.4–0.7 of the request (Refine3D 0.97 of 96 GB).
- No job ended on its time limit. The failed jobs are the migration week's bring-up (116 `segment` tasks failed in a
  median of 4 s).

Where the data is thin: six days, three users, most jobs by count the maintainer's benchmarks and tests from the
migration week; 30 s sampling, so a 48 s task has one or two samples; `gres/gpuutil` is summed over a job's GPUs and
counts a kernel on one SM as busy; the other users' jobs cannot be classified (SLURM hides their scripts) and are not
looked at beyond these totals; SLURM's read and write counters are unchecked (missAlign's 5 TB is its training cubes
in `/tmp`).

## 4. Where a GPU waits

Hypotheses from the code and §3, each with the probe that confirms it and the lever that cures it (§6). They are
sorted by how sure they are.

- **H1 — asked for, never used.** Accounting shows it today (§3). Cure: L1.
- **H2 — start-up before the first kernel.** Each array task starts a driver (about 1.4–1.8 s just importing the base,
  roadmap 39 §6), starts a container from a 5–13 GB image on `/groups` (`apptainer exec --nv`,
  `services/computing/container_service.py:112`), initialises CUDA and, for Warp motion, runs `WarpTools
  create_settings` first (`drivers/fs_motion_and_ctf.py:154`). None of it is timed. Compile caches belong here too:
  pytom compiles about 60 cupy kernels on its first launch (the hand run's cache holds 62; roughly 10 s cold is an
  estimate, not a measurement) and `CUPY_CACHE_DIR` is in the job's private `/tmp`
  (`services/computing/container_service.py:27`), so every task pays it again, where the hand run above had a warm
  cache on `/scratch`. Confirm: phase marks and the time to the first busy second (the meter). Cure: L3, L5, L8.
- **H3 — data moved on `/groups` around the compute.** The tool reads its frames or tomogram from `/groups` itself,
  with no overlap; SLURM counts 2.5 GB read and 4.1 GB written per 76 s Warp motion task. Reading is cheap at one
  reader: a cold 1 GiB read from the login node (no contention, not a compute node; 2026-11-06) ran at 1.2 GB/s from
  `/groups` and 4.8 GB/s from `/scratch`, so a 2 GiB tomogram takes about 1.8 s against 0.45 s. Writing is not: a
  template-matching task writes two 2 GiB maps to `/groups`, the angles map in 16.9 s at the median (about 125 MB/s)
  while the card waits, the scores map presumably as long, and then reads the scores map back for the CPU scan; the hand
  run wrote its angles map to `/scratch` in 9 s. Sixteen tasks write into one folder (`tmResults`) at once, the pattern
  the site warns about. What concurrency does to these rates is P4's question. Confirm: GPU busy against read and write
  rate over time (the meter), and the storage probes. Cure: L3 for reads, L9 for writes, L4 only if the probes say
  `/groups` is the limit.
- **H4 — the CPU cannot feed the card.** Warp motion has 2 CPUs for 2 workers that decode EER and apply the gain; at 66
  % efficiency that is 1.3 cores. A fed card and starved CPUs look like this. Confirm: cores busy against GPU busy
  over time. Cure: L2.
- **H5 — serial work inside the allocation.** Denoise training prepares its data before the first epoch (setup 3 min
  plus 2 min per tomogram, `services/jobs/denoise_train.py:36-37`) while the job holds its GPU; Segment resamples each
  tomogram to 10 Å on the CPU before easymode runs (`drivers/segment.py:484`) and smooths and maps back after; template
  matching scans its score maps on the CPU once the search is done (`drivers/template_match_pytom.py:647-680`: median
  6.8 s, mean 7.5 s, max 21.8 s over 141 tasks of three jobs, from their logs, against a 165 s task); the DL tilt filter
  is bound by reading PNGs. Segment at 96.8 % busy says its share is small. Confirm: phase marks across the tool's own
  stages (the meter sees the GPU idle and a core busy). Cure: split the CPU stage into its own CPU job (the afterok
  chain exists), or overlap it.
- **H6 — one process cannot fill a B200.** Template matching holds 30.7 of 179 GiB; AreTomo and the CTF fits are
  smaller still. Such a job can be busy and not filled. Confirm: the concurrency sweep (P5) and DCGM's SM activity where
  it exists. Cure: L6. Mind that two processes on one card share it by time slices unless MPS or MIG is on: a second
  process helps only when the first leaves gaps (H2–H5), not when its kernels are small.
- **H7 — a hung tool keeps its GPU to the time limit.** The idle watchdog was removed because RELION, Warp and pytom
  print nothing for hours (`drivers/driver_base.py:357`). None in the sample. Cure: L7, a marker and no kill.

Not causes, so out of scope: queue waits and the last wave of an array free their GPUs for others; they cost wall time,
not utilisation.

## 5. Probes — what we dispatch

A probe is one measurement that answers one question; each runs once and feeds a decision, not a ladder.

| Probe | Question | How | Cost | Output |
|---|---|---|---|---|
| P0 accounting report | Which jobs, how many GPU-hours, how busy, how much CPU and memory, per job type | `scripts/sacct_gpu_report.py` over any window | seconds, no job | `docs/reports/gpu-util/baseline-<date>.md`; re-run after every stage |
| P1 IT's view | What is IT's number (a quarter idle, or 25 % utilised): which metric, window and averaging, for which job ids; do slices count? | the questions in S0 | one message | D1 |
| P2 capability job | What can a g5 node, a slice and a `c` node show us: dmon columns, DCGM, nsys, cgroup files, mounts, a container's cold start, which GPUs a `--cleanenv` container sees | one script submitted three times (B200, slice, `c` node), five minutes each; in S0 | 3 short jobs | `docs/reports/gpu-util/capability-<date>.md`; picks the meter's sources |
| P3 the meter | Per job: GPU busy over time, PCIe in and out, GPU memory, cores busy, memory peak, bytes read and written, and where the wall time went | built in S2, on for every tool call | one `nvidia-smi dmon` process and one thread | `meter/` in the job directory |
| P4 storage probe | What do sixteen tasks at once get when they write and read what a template-matching task does: one shared `/groups` folder against a subfolder each, against `/tmp` and `$SCRATCHDIR`; real score maps read out of one production folder | `scripts/probe_io.sh`: an array of 16 CPU-only tasks on partition `c` (`g` refuses a job without a GPU) with a barrier before each phase, the run voiding itself if the tasks do not all start together, 1 GiB per file, `dd` with `conv=fsync` to write and `iflag=direct` to read; `summarise_io.py` | 16 tasks × 2 CPUs for a few minutes; 16 GiB written to `/groups` (deleted by the script) and as much to each of `/tmp` and `$SCRATCHDIR`; about 80 GiB read | MB/s per task and together, per place |
| P4b template-matching envelope A/B | How much of a template-matching task's non-search time is the kernel cache, and how much the output folder? | `scripts/probe_tm_ab.sh`: one 412 tomogram, the production command, kernel cache cold or warm × outputs on `/groups` or in `/tmp`, each cell twice in mirrored order with a short angular search, then two full-search runs; a 1 Hz `nvidia-smi dmon` trace; `summarise_tm_ab.py` | 10 runs on one full B200, about 15 minutes (0.25 GPU-h); about 20 GiB written | per run: seconds before the first busy second, busy, idle between and after; the effect of each factor |
| P5 concurrency sweep | How many processes of this tool does one card take before items per hour stops rising; the same on a slice | the same N items at K = 1, 2, 4, 8 processes per card (Warp: `--perdevice`; others: K copies of the tool's command in one allocation, `xargs -P K`) and K = 1, 2 on a slice, metered | one run per tool that shows a wait | items/h and busy against K |
| P6 CPU sweep | Where does adding CPUs stop helping | the same items at 2, 4, 8, 16 CPUs | one run per tool | items/h against CPUs |
| P7 timeline | What the tool does in the gaps the meter cannot explain | Nsight Systems if P2 finds `nsys`; `py-spy` for the Python side | per case | a timeline |
| P8 pilot protocol run | All of it on a real pipeline | the copia protocol end to end on B200s with the meter, then again with the levers applied | one project each | the before and after table (S6) |

The pilot projects are the maintainer's choice (D6). Natural ones: the copia-tutorial protocol (EER frames, every stage
up to Refine3D, small) for the stage-by-stage numbers, and a 412 project for what only a hundred tilt series show —
arrays of 100+ tasks and 140-tomogram template matching.

## 6. Levers

Each is applied to a job type only where the meter names its cause (§4); the expected ones are in S4.

- **L1 — ask for what the job uses.** No GPU for steps that never touch one; CPUs from the peak cores used; memory
  from the peak resident set; the GPU type (full card, slice, none) from P5. A cut is never below the measured peak
  plus a margin (D2), because an out-of-memory kill costs more than the idle core. Job names that carry the type and
  instance (`cb-<instance_id>`, supervisors `cb-<instance_id>-sup`, D8), so accounting and IT's dashboard can group
  them.
- **L2 — feed the card.** CPUs and workers together: a full B200's fair share is 24 CPUs (§2), and the node's CPUs are
  not what runs out on `g` (192 per node, about 6 per schedulable device). Warp motion at 2 CPUs and 2 workers is the
  first test.
- **L3 — overlap the read with the start-up.** Read a task's input files into the page cache (a plain `cat` to
  `/dev/null`, or `posix_fadvise`) while its container starts, and in a driver that loops over items, one item ahead of
  the GPU. The nodes have 1.5 TB of memory, so the cache is large; the data is read once either way, earlier.
- **L4 — stage, only if P4 says `/groups` is the limit.** To the job's own `/tmp` (private, size unknown), or to
  `/scratch` by a CPU job ahead of the GPU job and back by another after it (the afterok chain exists). `/scratch`
  deletes after 28 days, so it holds a cache of inputs, never the only copy of a result. IT's advice is the second
  form; the first two levers are cheaper and need no policy. Its cheapest form needs no code: `/scratch` is already
  bound into every container, so a project created there, or raw data imported from there, should just run (untested),
  at the price of the 28-day deletion and of copying the raw data first.
- **L5 — chunk.** N items per array task, so the container start, CUDA initialisation and settings run once per chunk
  and `--perdevice` has items to share. Needs D3 (failure isolation). The status markers are per item already.
- **L6 — right-size the card.** A slice for tools that fill a seventh of a B200 (3.43 billing units against 24, and 28
  of them per node), or K processes per card where each leaves gaps. A slice has no OpenGL and fits 21 GiB (roadmap 43).
- **L7 — mark, do not kill.** A tool whose trace shows the GPU idle for a long stretch while it runs gets a chip and an
  event line; a person decides. No auto-kill, no auto-retry, no node probing (standing rule on cluster-workaround
  automation).
- **L9 — keep the card out of the I/O.** A template-matching task holds its card while it writes two 2 GiB maps
  and reads one back. Write them to the job's `/tmp` (BeeGFS) and let a CPU job copy them to `/groups` and run the
  scan: the stage-out job of the site's own data lifecycle, which the afterok chain already supports. Smaller: a
  subfolder per task instead of one shared `tmResults`. Which pays is P4 and P4b.
- **L8 — keep the compile caches across jobs.** Only `CUDA_CACHE_PATH` has been moved to `/groups` (roadmap 43 S6);
  cupy's cache (pytom), Triton's, torch's and numba's are in the job's `/tmp` and are rebuilt by every task. Put them
  beside the CUDA one, one subdirectory per image.

## 7. Principles

- **Measure the job, not the cluster.** Numbers come from the job's own recorder and SLURM's record of it, never from a
  guess about the node.
- **Say which utilisation.** Used, busy or filled (§1); never a bare percentage, and never a per-job mean next to a
  GPU-second one without saying so.
- **A change must not change results.** Chunking, packing, staging and CPU counts are checked against the same outputs
  (the same XMLs, volumes and star files, within a stated tolerance) before they become defaults.
- **Surface, do not decide.** The meter marks; a person acts. Nothing here retries, kills or reroutes by itself.
- **The meter never fails a job and never changes a command.** An error in it is written as "not measured: <reason>",
  never as 0 and never silently (the standing rule on silent defaults).
- **A slice reads "not measured", never 0 %.** Until IT shows a slice-aware counter, slice jobs are judged by items per
  hour.
- **Requests follow measurements, marked.** A profile changes when the meter says so for that job type, with the
  evidence in the roadmap; a user's override in the SLURM tab is never touched.

## 8. Decisions

- **D1 — what IT's number is.** Not established (§3). Ask: which metric (DCGM's `GPU_UTIL`, SM or graphics-engine
  activity, SLURM's `gres/gpuutil`), over which window, averaged per GPU-second or per job or per user, and whether a
  card is plotted over the whole allocation from its first second or only while a process holds it; which job ids and
  users it was computed from; whether slices count and how they are read; whether they can export a per-job series (1–15
  s) with DCGM's profiling fields (SM active, tensor, DRAM active, PCIe in and out) for two or three of the jobs behind
  it. Until answered, every table carries both averages.
- **D2 — the target's exact form.** Proposed, not applied: GPU busy ≥ 80 % of the job's wall time on a full B200 (level
  2, from the meter), every GPU request used (level 1), CPUs requested ≤ 2× the peak cores used, memory ≤ 1.5× the peak
  resident set. Per job type, not per project. The CPU and memory factors only matter to a dashboard that scores them
  (billing is dominated by the GPU, §2), so they are the maintainer's call.
- **D3 — chunking against failure isolation (roadmap 27).** A task that runs several tilt series gives up "one task, one
  retry unit". Proposed: one status marker per item stays, a rerun submits only the failed items (it already submits
  only pending indices, `drivers/array_job_base.py:667`), and the chunk size is capped by the time limit and the
  memory. Needs the maintainer's yes.
- **D4 — slices as the default for small tools.** Per job type after P5: 7× cheaper to bill, and invisible to the
  counter until IT counts them.
- **D5 — staging policy** after P4: none, page-cache prefetch, the job's `/tmp`, or `/scratch` through CPU jobs; and
  who cleans up.
- **D6 — which pilot projects** (and one non-EER dataset, TIFF or K3, for the decode path).
- **D7 — where the meter's result lives.** Proposed: files in the job directory, read by the UI (no change to the
  project file, no schema bump). The alternative, fields on the job model, bumps the project schema's minor version
  (`services/project_state.py:60`, now 3.12).
- **D8 — job names.** Proposed `cb-<instance_id>` and `cb-<instance_id>-sup`, and `--comment` the job type and
  directory number. The project's path is already in every job's `StdOut`, so the name adds no exposure.

## 9. Stages

**S0 — Ask and look (no code).**
1. Send IT D1's questions with two or three job ids from the colleague's "25 %" period. If IT shares a series, put
   it beside the same job's SLURM record (P0).
2. Run P2: `docs/reports/gpu-util/scripts/capability.sh`, submitted three times from any directory, to a full B200,
   a slice and a `c` node (the sbatch lines are in its header). The `c` job's GPU lines fail by design; what it settles
   is whether CPU-only steps can move to `c` (apptainer, `/groups`, `/scratch`, the pytom and RELION images). Its Python
   section shows which of `rchar`, `wchar` and `read_bytes` see an NFS read, which the meter's byte counters rest on,
   and its two container sections show whether a `--cleanenv` container sees exactly the allocated GPU. Record what
   each job says in `docs/reports/gpu-util/capability-<date>.md`: which dmon columns a B200 and a slice give, whether
   DCGM or `nsys` exist, how large `/tmp` is, which cgroup files a job can read.
3. Run P4b and then P4, which need nothing built, one after the other: P4 writes 16 GiB at once and would slow the
   writes P4b times. `probe_tm_ab.sh` runs on one full B200 (about 15 minutes); `probe_io.sh` is an array of sixteen
   CPU-only tasks on partition `c` (a few minutes; `g` refuses a job without a GPU). Each takes a `/groups` folder the
   maintainer names as its first argument, not as an exported variable: the CBE module sets `SBATCH_EXPORT=SCRATCHDIR`,
   so sbatch passes a job nothing but `$SCRATCHDIR`. Each deletes what it made there; their summarisers print the
   tables. The results decide L3, L4, L8 and L9 and what S4 does first for template matching. The tuning skill's rule
   applies: the maintainer says go, with the cost above in view, before anything is submitted.
4. Re-run P0 (`sacct_gpu_report.py --scope all --completed-only`) when the window has grown; its stored-script
   lookups are cached, so a rerun costs two accounting queries plus the directories it has not seen.

**S1 — Ask for what the job uses (config and small code; certain).**
- `config/conf.yaml` (both installs) and `config/conf.template.yaml`: `tmextractcand`, `subtomoExtraction` and
  `reconstructParticle` get no GPU (`gres: ''`) and, if the `c` job of S0 passes, `partition: c`; `extractPickList`'s
  sbatch (`backend.py:697`) takes the extraction profile and not the project's GPU defaults. The comment at
  `services/jobs/subtomo_extraction.py:128-136` is corrected. Warp export and Warp CTF stay until a run with real work
  has been metered.
- What it buys on the per-job figures of §3: without the 145 jobs of those three types (140 + 3 + 2), the mean busy
  rises from 48.9 % to 57.9 % and the median from 32 % to 93 %; if Warp export and Warp CTF (146 more jobs) turn out
  to need no GPU either, to 71.0 % and 97 %. Of the 644 jobs left in that case, 233 are still under 80 %: 131 short
  Warp array tasks (motion 74, alignment 41, reconstruct 16), 99 jobs whose scripts SLURM hides (the cluster self-test
  probes among them) and 3 others. The GPU-second figure moves only from 92.3 % to 94.3 % (94.7 % in the second case),
  because those jobs hold little GPU time. The rest of the way to 80 % per job is S4's Warp work.
- `write_sbatch_script` (`services/computing/slurm_service.py:166`) writes `--job-name` and `--comment` per D8; P0 then
  reads the job type from the name and not from the stored script.
- The SMALL/MEDIUM/LARGE presets (`gpu:1`, `gpu:2`, `gpu:4`, `services/computing/slurm_service.py:29-42`) are offered
  for every job type, though most tools use one GPU (easymode `--gpu 0`, cryoCARE `gpu_id 0`, non-MPI Class3D, one item
  per Warp task). Mark which job types can use more than one.

**S2 — The meter (P3).** A stdlib-only module, `services/job_meter.py`, started and stopped by `run_command`
(`drivers/driver_base.py:348`) around each tool call, so every tool call of every driver is covered, retries included
(the in-process tilt filter starts it itself). It runs in the driver on the node, outside the container. Its clock
starts at the driver's own process start (`/proc/self/stat`), and the job's start comes from `scontrol show job` at the
first mark, so the shell and interpreter start-up before Python is in the wall time.
- **GPU:** one `nvidia-smi dmon -s umt -d 1` child restricted to the job's GPUs: per second SM and memory busy,
  framebuffer in use, PCIe in and out. The columns a slice cannot give are written as "not measured" (S0 settles
  which). If S0 finds DCGM, its profiling fields (SM active, tensor, DRAM active) are added; if not, level 3 stays
  unmeasured and P5 stands in.
- **Host:** one thread, once a second: the job's cgroup `cpu.stat` (cores busy), `memory.current` and `memory.peak`, and
  bytes read and written by its processes from `/proc/<pid>/io` for the pids in the cgroup (`rchar`, `wchar`,
  `read_bytes`: what a tool asked of NFS and what reached storage).
- **Phases:** `meter.mark(name)` at the driver's own boundaries: process start, bootstrap done (`DriverContext.load`),
  `stage`, `execute`, `verify_outputs`, `collect` (`drivers/array_job_base.py:1048-1055`). With the trace they give
  time to the first busy second, the longest idle stretch, the idle tail, and the share of wall time in each phase.
- **Files:** `<job_dir>/meter/<run | task_N[.attemptK]>.{gpu,host,phases}.csv` and a `.json` summary: busy % of the
  tool time and of the wall time, seconds to the first busy second, longest idle stretch, idle tail, GPU memory peak,
  PCIe GiB in and out, cores mean and peak against requested, memory peak against requested, bytes read and written,
  and a `measured` flag with a reason for anything missing. The rerun cleanup list
  (`services/scheduling_and_orchestration/pipeline_orchestrator_service.py:642`) learns the `meter/` folder.
- Cost: one process and one thread beside the tool; checked in the runtime list by the same task with and without it.

**S3 — The probes on the pilots (P5–P8; the maintainer's one pass).** The copia protocol on B200s with the meter on
(P8, the baseline per stage); then, only for the job types whose summaries show a wait, one P5 and one P6
sweep. Results in `docs/reports/gpu-util/pilot-<date>.md`: per job type the phase table, the busy and filled readings
and items/h against K and CPUs. Each sweep also records that the outputs are identical across its arms.

**S4 — Apply by cause.** Each main job type gets the lever its summary names. Expected, to be replaced by what S3 finds:

| Job type | First look (§3) | Likely wait (§4) | Expected lever |
|---|---|---|---|
| Segment | 96.8 % busy | H5 (CPU resample inside the allocation), whole-card TensorFlow | none for busy; P5 for two tasks per card with `TF_FORCE_GPU_ALLOW_GROWTH` |
| Template matching | 98.6 % busy, about half the allocation (§3) | H2 (cupy kernels rebuilt per task), H3 (two 2 GiB maps to `/groups`), H5 (7.7 s scan), filled unknown (30.7 of 179 GiB) | L8, L9, L5 (several tomograms per task); P5 at K = 2 |
| Candidate, subtomogram extraction, particle reconstruction | 0 % | H1 | L1 |
| Warp motion + CTF | 23 % | H2, H3, H4 | L2 (CPUs and workers), then L5 and L3 |
| AreTomo alignment | 30 % | H2, H6 | L5, or a slice (L6) |
| Warp CTF, export, reconstruct | 0 / 0 / 56 % | H2; H1 unknown | decide from a real run under the meter |
| Class3D, Refine3D, missAlign | 95–100 % | none by busy | leave; CPUs and memory from the meter (L1) |
| Denoise training and prediction | not in the sample | H5, 128 GB per predict task | meter first; split the CPU preparation off |
| Tilt filter | 5 % over 34 s, 2 jobs | H2 | too small to matter |

**S5 — Show it.** The meter's summaries reach the UI the way the tilt filter's commit record does: `_land_job` builds
one job-level summary (for an array, the GPU-second weighted busy over its tasks, with the count) in a thread when the
job ends. The job page header gets a third chip beside the log-progress and disk ones
(`ui/pipeline_builder/job_tab_component.py:239-240`): `GPU 38 % busy · CPU 1.3/2 · mem 11/24 G`, its tooltip the three
levels of §1 and the phase split. A job that asked for a GPU and never touched it carries a subtle marker on its roster
row and an event line (`services.event_log.events`, project name first); a slice reads "not measured on a MIG slice".
The SLURM tab states under the GPU field what the last runs of this job type used, and changes nothing. Polls follow
the house rules: finished jobs are read once, off the loop, and a hidden view does nothing.

**S6 — Re-measure and write the numbers in.** P8 again with the levers applied; P0 over the window; the before and
after table per job type in `docs/reports/gpu-util/`; the per-type targets of D2 recorded here; this roadmap closed or
its leftovers named.

## 10. Runtime checklist (after S2)

1. The copia protocol on B200s: every tool call leaves `meter/` files in its job directory; a task's phases add up to
   its elapsed time within a few seconds.
2. A job on a slice says "not measured on a MIG slice" in its summary; nowhere does it say 0 %.
3. A failed, cancelled or timed-out tool leaves its trace, and the exit code is the tool's, as before.
4. On a long GPU job the meter's busy % and `sacct_gpu_report.py`'s agree within a few points.
5. The same template-matching task with the meter and without it takes the same time within noise.

## 11. Build record

- **2026-11-06, S0 (accounting part only).** `docs/reports/gpu-util/scripts/sacct_gpu_report.py` and
  `docs/reports/gpu-util/baseline-2026-11-06.md`: read-only `sacct` over the window, jobs classified by the driver in
  their stored batch script, keyed by the job's own directory (a single job's `WorkDir` is the project; an early draft
  keyed on it and mixed Class3D, Refine3D and particle reconstruction together). The script and the baseline were
  checked by `py_compile` and the 120-column limit only: this checkout had no environment with ruff in it (CLAUDE.md
  now runs it as `./crboost python -m ruff`, which builds one), so `ruff check` and `ruff format` are owed. Read by hand
  for the roadmap: the scan seconds in H5 (the task logs of three template-matching jobs), the read speeds in H3 (two
  `dd` runs) and the cupy cache count in H2. No product code changed.
- **2026-11-06, probes written, not run.** `docs/reports/gpu-util/scripts/`: `capability.sh`; `probe_tm_ab.sh` with
  `summarise_tm_ab.py`; `probe_io.sh` with `summarise_io.py`. Checked: `bash -n` and `py_compile`; `probe_io.sh` as one
  16 MiB task on the login node with a scratch folder standing in for `/groups`; `probe_tm_ab.sh` with `PROBE_DRY=1`
  (the order of the ten runs, the short angle list, the cleanup); `summarise_tm_ab.py` on synthetic traces with known
  answers. Not submitted: the tuning skill's rule is to propose a measurement with its cost and wait for the
  maintainer's go. `sacct_gpu_report.py` gained a cache for its stored-script lookups for the same reason (the skill
  asks for sparing queries; the first version made one lookup per job directory on every run). The task anatomy of
  §3 came from one accounting query and the file times of job019's 141 tasks.
- **2026-11-07, first submissions: neither probe ran.** `probe_tm_ab.sh` (jobs 1457654 and 1457659) stopped after 3 s at
  its first line: the folder had been handed over as an exported variable, and the CBE module sets
  `SBATCH_EXPORT=SCRATCHDIR`, so the job received nothing but `$SCRATCHDIR`. `probe_io.sh` was refused by the submit
  plugin: partition `g` requires a GPU and the probe uses none. The hand-over lines had not been run past the scheduler,
  and gave the two probes as one block, which would have run them together. Now: the folder is the first argument;
  `probe_io.sh` runs on `c`, voids itself and cleans up when its tasks do not all arrive, and cleans up when
  cancelled (checked locally: a normal run, a void run and a cancel each leave nothing behind); every sbatch line is
  checked with `sbatch --test-only` (nothing queued) before it is handed out; the probes run one after the other.
  `capability.sh` also prints the environment a job starts with.
- **2026-11-08, P4b and P4 ran** (job 1474636, array 1474806). P4b, one 412 tomogram on a B200, short angular search:
  the cupy cache costs 0.3 s (outputs on `/groups`) to 4.1 s (`/tmp`) per run, outputs on `/groups` cost 5–8 s over
  `/tmp`, and in every cell the card holds 30.7 GB from about 1 s in but its SMs stay at 0 % for 58–72 s, with the
  input files already in the page cache. With the full angular search the run takes 150 s and the SMs are busy
  63–68 s of it: 43 % in the best setting. So the kernel cache (H2) and the tomogram read are not the cause; the minute
  is something else, now P5. A container with `--cleanenv` sees exactly the one allocated GPU on a full B200 (the MIG
  case is the slice job of `capability.sh`). P4, 16 tasks from one `c` node, 1 GiB each: `/groups` shared folder 88
  MB/s per task (1.3 GB/s together), own subfolder each 271 (2.5), `/tmp` 835 (4.7), `$SCRATCHDIR` 504 (4.8); reads of
  `/groups` 260–290 per task, of BeeGFS 600–730. Caveats: one run, a `c` node, all 16 tasks on one node, other
  users' load unknown. `probe_tm_prestart.sh` with `summarise_tm_pre.py` (P5) written and tried with a stand-in only:
  4 against 16 CPUs, a once-a-second sample of the python process.
- **2026-11-08, P2 and P5 ran** (jobs 1474967–1474969, 1474966). P2: a job starts with a full environment (186
  variables, the EESSI `PATH`); per-job `/tmp` is BeeGFS from the 1.3 PB pool; `/groups` is NFSv3 at 96 % (268 TB free),
  1 MiB read and write sizes; `dcgmi`, `strace` and `nvidia-cuda-mps-control` exist on GPU nodes, `nsys`, `ncu`, `py-spy`
  and `iostat` do not; `cpu.stat` and `memory.*` are readable in the job cgroup, `io.stat` is not; `rchar` and
  `read_bytes` both count NFS reads; a `--cleanenv` container sees exactly the allocated B200 or MIG slice with
  `nvidia-smi -L` (CUDA's own enumeration on a slice is untested); a slice reports no utilisation and the parent GPU
  shows other users' memory and power. P5, same node and command as P4b: the process runs on one core for about 30 s
  before its first kernel, the same with 4 and with 16 CPUs, reading only the tomogram; P4b saw 58–72 s for that stage
  on the same node earlier in the day, which is unexplained. A B200 has about 180 GB and one process holds 31 GB, so
  `probe_tm_concurrency.sh` with `summarise_tm_conc.py` (P6) runs 1 to 4 processes on one GPU and tries `dcgmi dmon`;
  written, tried with stand-ins only. Running on slices is already built and measured (roadmap 43 S1 and S2: `-s x y
  z` cuts the FFT-padded volume, pytom's peak is about 60 bytes per voxel of one piece plus 0.7 GiB, `gpu_split: auto`
  picks the split; on 412 a slice at `2 2 1` takes 793 s against 112 s whole on a B200, 7.1×). What that leaves for
  this roadmap: the 30 s preamble and the post-search time are the same on a slice, so they are about 10 % of a slice's
  allocation against 40–55 % of a B200's, and SLURM cannot show it because it does not meter slices.
- **2026-11-08, P6 ran** (job 1475262; the first submission, 1475050, hung in a bare `wait` on the samplers and was
  cancelled). 1, 2, 3, 4 and 1 full-search processes at once on one B200, 412 tomogram, outputs on `/tmp`:

  | N at once | wall s | tomograms/min | SM>0 s | peak GPU GB | DCGM SM active / DRAM active |
  |---:|---:|---:|---:|---:|---|
  | 1 | 192 | 0.31 | 64 | 31 | 0.33 / 0.14 |
  | 2 | 272 | 0.44 | 143 | 61 | 0.48 / 0.19 |
  | 3 | 335 | 0.54 | 214 | 92 | 0.58 / 0.24 |
  | 4 | 401 | 0.60 | 282 | 123 | 0.64 / 0.26 |
  | 1 | 132 | 0.45 | 64 | 31 | 0.47 / 0.20 |

  The two single-process batches took 192 s and 132 s for the same work, so the gain is 1.3× (against the 132 s) to
  1.9× (against the 192 s) at four processes: time-slicing fills the idle start-up of one process with the search of
  another, but the kernels do not run side by side, so the total SM-busy time grows with N (64 s to 282 s). The
  tensor pipe is unused throughout. Not tried: MPS (`nvidia-cuda-mps-control` exists on the GPU nodes), which could
  run kernels together; the search alone fills 0.97 of the SMs but occupies only 0.54. `dcgmi dmon` works in a user
  job and reports all 8 GPUs of the node; it needs `CUDA_VISIBLE_DEVICES` unset and `-i <gpu id>` to follow one.
- **2026-11-08, the preamble read from pytom's source; P7 written.** pytom 0.14's `TMJob.start_job`
  (`pytom_tm/tmjob.py`, read from the production image) builds the tomogram's missing-wedge filter at full size
  (`create_wedge` on 1024 x 1024 x 512) and filters the whole padded tomogram on the CPU with `scipy.fft.rfftn` and
  `irfftn` before the GPU plan exists. scipy's FFTs use one thread unless told otherwise (`scipy.fft.get_workers()` is
  1 in the image), which fits everything P5 saw: one core busy for about 30 s, the same with 4 and 16 CPUs, 18–19 GB
  resident. The card's memory is taken only after that (`TemplateMatchingGPU.__init__`). Memory bandwidth shared with
  the node's other jobs would explain the 30 s against 65 s, but that is not established. `probe_tm_fft_mps.sh` (P7)
  with `summarise_tm_fft.py` runs the production command through a wrapper that sets scipy.fft's worker count and logs
  when each of those calls, the plan, the search and the writes start and end: 1, 4, 4 and 1 workers on one process
  (the variance and the effect), then four processes with 4 workers each, then with the argument `mps` the same four
  under an MPS daemon on the job's GPU. The maps of 1 against 4 workers, and of two 1-worker runs, are compared byte
  for byte. Checked: `bash -n`, a stand-in run (`PROBE_DRY=1`) through the summariser, `sbatch --test-only`.
  P7 showed the FFT is not the cause (next entry).
- **2026-11-08, S1 built (not run).** As §9 S1 says, with what the code showed:
  - *No GPU for the CPU-only steps.* `config/conf.yaml`: `subtomoExtraction` and `reconstructParticle` get
    `gres: ''` and `partition: c` (P2's `c` job ran the containers there), `tmextractcand` one MIG slice on `g`
    (it cannot start without a CUDA device: the next entry); a new
    `extractPickList` profile the same; `tsImport` and `filamentPick` had `gres: ''` on partition `g`, which the
    submit plugin refuses, and get `partition: c` too (filament picking had never been submitted on CBE.next;
    `tsImport` submits with the supervisor's resources and was not affected). `config/conf.template.yaml`: the two
    RELION steps and the new profile without a GPU, a comment to name a CPU partition, and candidate extraction on
    `gpu:1` with a comment to name the smallest GPU. The per-list extraction
    (`backend.py`, `extract_pick_list`) takes its instance's resources (the project's defaults under the
    `extractPickList` profile) instead of the project's GPU defaults. The lab's `config/conf.yaml` needs the same
    six profile edits by hand. `sbatch --test-only` placed every CPU job on `c` hours out (19:09, then 19:30)
    while three `c` nodes were idle and all 274 pending `c` jobs waited on a dependency or a begin time, so that
    estimate looks pessimistic rather than a real queue; the array supervisors were on `c` before this change
    already. A user's SLURM tab can still move one job back to `g`.
  - *Job names.* `write_sbatch_script(..., name=)` replaces qsub.sh's `--job-name=CryoBoost` with
    `crboost_job_name(instance_id)`: `cb-<instance_id>` for single jobs and array tasks, `cb-<instance_id>-sup` for an
    array's supervisor (`services/computing/slurm_service.py`; callers in the orchestrator, `drivers/array_job_base.py`
    and the tilt-filter and per-list extraction submits). `sacct_gpu_report.py` selects both names (`cb-probe-*` left
    out). No `--comment`: the name carries the instance and `StdOut` the directory.
  - *Template-matching outputs.* pytom writes into a folder of the task's own `TMPDIR` (on CBE.next the private BeeGFS
    `/tmp`: 835 MB/s per task in P4 against 88 in the shared folder). `land_outputs` then copies them into
    `<job_dir>/.tm_landing/<tomogram>` (a folder per task: 271 MB/s in P4) and renames each into `tmResults`, the score
    map last, so the layout every reader knows stays as it is and a file there is always whole. The job.json's paths
    into the temporary folder (`output_dir`, `whitening_filter`) are rewritten to `tmResults`, and the files keep
    pytom's write times, which the runtime backfill reads. A `TMPDIR` without room for three times the two maps keeps
    today's direct writes, and says so in the task log. The supervisor removes `.tm_landing` after the array. A task
    killed while writing no longer leaves a truncated score map that a rerun would take as done. The task log prints
    `Landed X GiB in tmResults in Y s`, the number to compare with the 16.9 s per map of §3.
  - Checked: ruff and `check_boundaries.py`; the edited modules import; scripts rendered for a CPU array task, a
    per-list extraction and a TM supervisor, each accepted by `sbatch --test-only`; `land_outputs` on a toy folder
    (order, rewritten paths, kept times, the folders removed). Not run: a pipeline. The runtime check is the next
    template-matching job (task logs: the staging line and the landed line; extraction reads the job.json as before)
    and the next candidate extraction (its tasks on `c`).
  - Not in this pass: marking which job types can use more than one GPU (the presets of §9 S1).
- **2026-11-08, candidate extraction needs a CUDA device; first runs.** The copia protocol run
  (`copia-tutorial-20261108-1651`) failed at candidate extraction (job008) on `c`: `pytom_extract_candidates.py`
  imports `pytom_tm.matching`, whose `voltools` asks cupy for the device count at import and raises
  `cudaErrorNoDevice` on a node without a GPU (voltools catches only a missing cupy). It computes on the CPU and used
  no GPU memory in §3, but it needs a device to exist, so `tmextractcand` now asks for one MIG slice on `g`
  (`gpu:nvidia_b200_1g.23gb:1`, 3.43 billing units against 24 for a B200; at 17:03 `--test-only` put a slice 30
  minutes out and any GPU 3). On this login node, which has no GPU either, `relion_tomo_subtomo` and
  `relion_tomo_reconstruct_particle` start and print their usage, so the RELION steps stay on `c`. A patched pytom
  image (voltools treating the CUDA error like a missing cupy) would let extraction run on `c`; not done. The same
  run's template matching (job007, one tomogram, no other task writing) staged in `/tmp` and landed 4.0 GiB in
  `tmResults` in 6.7 s; extraction staged its job.json as before.
- **2026-11-08, P7 ran** (job 1476310, one B200, 412 tomogram, full angle list, outputs in `/tmp`, each process on its
  own 4 CPUs). The FFT reading of the preamble was wrong: both FFTs of the whole tomogram take 4.1 s with one worker
  and 1.1 s with four. Where a process's 112 s go (medians of the two 1-worker runs; the 4-worker runs differ only in
  the FFT):

  | Phase | s | The card |
  |---|---:|---|
  | Container start and pytom's imports | 1.2 | idle |
  | Reading the tomogram (with pytom's transpose) | 8.2 | idle |
  | Missing-wedge filters: the tomogram's at full size, the template's per tilt | 8.5 + 5.9 | idle |
  | The two CPU FFTs | 4.1 | idle |
  | GPU plan | 0.9 | allocating |
  | Search | 63.5 | busy |
  | Writing the two maps (with the transpose) | 17.2 | idle |
  | Rest | 2.1 | idle |

  So 48 s of 112 are CPU work on one core while the card waits, and the search alone fills it (SM active 0.97 in P6).
  Batches: 1 worker 112 and 112 s, 4 workers 109 and 109 s (the variance of P4b and P6 did not recur; load average
  45–49 on the node); four processes at once 360 s, 0.67 tomograms a minute, 1.24× one process; the same four under
  MPS 308 s, 0.78 a minute, 1.45× (DCGM SM active 0.72 without MPS, 0.81 with; peak 123 GB; tensor pipe unused). MPS
  ran as a user daemon in the job: `get_server_list` named its server and nvidia-smi showed it beside four clients of
  31 GB each. Running four at once slowed each one's set-up (the wedge took 48 s without MPS and 22 s with it,
  unexplained) and its search (277 s for four without MPS, 250 s with: time-slicing costs about 9 %, MPS about
  nothing against four searches one after another). The ceiling is the search, 63.5 s a tomogram: 0.94 a minute,
  1.75× one process. The byte-for-byte check did not run: the probe looked for the maps under the tilt series' name,
  and pytom names them after the volume file (fixed in the script). Not built, since 3 s of 112 is not worth a
  wrapper: the FFT workers. Next: two to four processes per GPU under MPS, started a set-up apart.
- **2026-11-08, P7b written (not run).** `probe_tm_lanes.sh` with `summarise_tm_lanes.py`: the production command in
  lanes (each lane runs its tomograms one after another on its own 4 CPUs, later lanes 30 s apart): one lane x 2
  (baseline), two lanes x 3 without and with MPS, three x 2 and four x 2 under MPS; maps of a lone run compared byte
  for byte with a second lone run and with one made by the fourth lane under MPS. Its throughput includes each arm's
  ramp up and down, as a task running 2–3 tomograms a lane would. Checked: `bash -n`, a stand-in run through the
  summariser, `sbatch --test-only` from `~/dev`.
- **2026-11-08, the 412 pilot under way** (`slurmtest_412_FCIsolation`, all 163 tilt series of the session, settings
  of the AGG412 FCIsolation project without its tilt filter and missAlign; submitted 16:59:49). Motion 163 of 163 in
  about 13 minutes; tsImport one CPU job on `c`, about 14 minutes; alignment dropped 5 series to AreTomo2 segfaults
  (exit 139), four of which aligned in the reference project after its DL tilt filter had removed bad tilts; CTF and
  reconstruction 158 of 158. Template matching (158 tasks at 16 at a time) runs on the new code: every task log
  begins with its `[SLURM]` line, and the first eight tasks landed 4.0 GiB each in `tmResults` in 6.5–7.8 s with
  sixteen tasks running, where pytom writing into the shared folder took about 17 s for one map in §3.
- **2026-11-08, the copia pilot** (`copia-tutorial-20261108-1651`, one tilt series, every stage to Class3D) succeeded
  once candidate extraction was re-run on a slice. From one accounting query by job name: subtomogram extraction (44 s
  a task) and particle reconstruction (4 min 8 s, CPU efficiency 0.60) ran on `c` with no GPU; candidate extraction
  held one MIG slice for 8 min 6 s without touching it (the protocol's up to 1500 candidates, which pytom extracts
  one full-map search at a time: 1005 here, 1028 in the October run); template matching 1 min 42 s on a B200 at 4 %
  busy (the protocol searches 48 rotations, 90° in C1, as the October run did, so the task is set-up and writes); motion
  35 %, reconstruction 60 %, Class3D 93 % (19 min, the whole card's memory). Alignment (21 s) and CTF (14 s) read 0 %
  with no GPU memory: shorter than SLURM's 30 s sampling, so not sampled rather than unused. The three CPU steps held
  a full B200 for about 13 minutes before this change.
- **2026-11-08, the 412 pilot's numbers** (16:59:50 to 18:36:34, 1 h 37 min; one accounting query, filtered on the
  project's path; medians per task):

  | Job | Tasks | GPU | s | GPU-h | SLURM busy % | CPU eff. | GPU mem GB |
  |---|---:|---|---:|---:|---:|---:|---:|
  | Warp motion + CTF | 163 | B200 or slice | 60 | 2.72 | 30 | 0.68 of 2 | 5.3 |
  | tsImport (one job) | 1 | none, `c` | 737 | – | – | 0.98 of 1 | – |
  | AreTomo alignment | 158 | B200 or slice | 27 | 1.21 | 0 (not sampled) | 0.42 | 0 |
  | Warp CTF | 158 | B200 or slice | 16 | 0.72 | 0 (not sampled) | 0.39 | 0 |
  | Warp reconstruct | 158 | B200 or slice | 63 | 2.79 | 56 | 0.16 | 2.6 |
  | Template matching | 158 | B200 | 186 | 8.71 | 100 | 0.23 of 4 | 30.7 |
  | Candidate extraction | 158 | slice | 35 | 1.81 | 0 | 0.41 | 0 |
  | Warp export | 146 | B200 or slice | 10 | 0.46 | 0 (not sampled) | 0.16 | 0 |

  Template matching from each task's log and file times: driver start and staging 2.9 s, pytom from the container's
  start to its job.json 168 s (p10 145, p90 242), landing 6.9 s (6.2–9.2 over all 158), score-map scan 4.2 s, 186 s a
  task: a tomogram held its B200 186 s against 165 s in job019, 8.71 GPU-h for 158 against about 6.5 for 141.
  Corrected 2026-11-09, phase by phase against job019 (`AGG412_20251113_412`: the same container, tomogram size, 3,398
  rotations, template, pytom options, 4 CPUs and 32 GB; up to 32 tasks at a time), from the same file times:

  | Median s | job019 | 412 pilot |
  |---|---:|---:|
  | Driver start and staging | 3.7 | 2.9 |
  | Container start to the scores map (set-up, search, first map) | 133 | 157 |
  | Scores map to `job.json` (angles map, json) | 16.9 into `/groups` | 11.0 into TMPDIR |
  | Landing, then the score-map scan | –, 7.7 | 6.9, 4.2 |
  | Whole task | 165 | 186 |

  From the scores map to the task's end the runs are about level (about 28 and 26 s): the angles map writes faster
  into TMPDIR and landing gives most of that back, so staging did not shorten the task. The difference sits before the
  scores map, and it is not node sharing, as this entry first said: both runs had about four tasks per node (111 of
  job019's 141, 145 of the pilot's 158), and on the nodes both used that way job019 took 131–138 s where the pilot took
  151–170 s (cbe-g5-3 131 to 151, g5-7 132 to 152, g5-2 138 to 170). cbe-g5-8 was the slowest node in both (224 s for
  the five job019 tasks it ran alone, 175 s here). Nor is it neighbours' landing: tasks whose first minute overlapped
  other tasks landing on their node were no slower. The first 16, started together, took 186 s against 156 s for the
  rest. The 41 tilts here against 37–40 there (no tilt filter) change only the wedge set-up, about a second by P7.
  Open: the first map's write into TMPDIR, which sits inside that span, and the cluster's state that evening; one job
  on one GPU running the same tomogram with outputs into `/groups` and into TMPDIR in turn, under P7's phase timer,
  tells them apart. P7b gives one GPU 16 CPUs; production gives a task 4, which lanes would share. Tasks under 30 s
  (alignment, CTF, export) get no SLURM sample, so their 0 % and 0 GB are "not measured": whether Warp CTF and export
  use the GPU needs the meter (S2). Motion's 2 CPUs ran at 0.68, so the CPU question (L2) is not settled by this.
- **2026-11-09, two findings from the 412 pilot's own records.**
  - The I1 angle list is 15 times too long (HOTFIXES H7). The pilot and job019 searched 3,398 orientations where a 12°
    I1 search needs about 225, because symmetry duplicates were dropped only when they hit the grid exactly. The
    list samples like about 5°. Fixing it is the largest lever on template matching found so far, and a scientific
    choice, since the scores change.
  - Slurm put some tasks of each short stage on MIG slices (`gpu:1` takes either type), so the pilot holds a
    comparison with nothing else changed:

    | Stage | B200 tasks | median s | slice tasks | median s | slice ÷ B200 | B200 cost ÷ slice cost |
    |---|---:|---:|---:|---:|---:|---:|
    | Warp motion + CTF | 144 | 60 | 19 | 72 | 1.20 | 5.8 |
    | AreTomo alignment | 145 | 27 | 13 | 36 | 1.33 | 5.2 |
    | Warp CTF | 131 | 15 | 28 | 18 | 1.20 | 5.8 |
    | Warp export | 135 | 10 | 11 | 12 | 1.20 | 5.8 |
    | Warp reconstruct | 158 | 63 | 1 | 174 | 2.76 | 2.5 |

    A seventh of a B200 does these short stages at 1.2–1.33 times the time, so a slice is the right size for them
    (L6), and pinning them to `gpu:nvidia_b200_1g.23gb:1` costs a fifth of the billing. Reconstruct has one slice task
    and is the stage that keeps its card busy (56 %).
- **2026-11-09, experiments for slices, angle lists and the pipeline's shape (written, not run).**
  - *The pipeline's shape, replayed* (`scripts/simulate_item_flow.py`, no job: the pilot's own task times, throttles
    and gaps). Stage barriers as run: 1:32:40 against the real 1:36:34, which checks the replay; 11 min of the run
    are gaps between one stage's last task and the next stage's first. Each tilt series moving on as soon as its
    own task ends: 0:58:45 with tsImport still one job, 0:39:21 with it split per tilt series; with 60 s for Slurm to
    act on each dependency, 1:05:38 and 0:46:14. Up to 71–82 tasks then run at once (57–66 outside template
    matching), against 20 today: more GPUs than `g` has as full B200s, so item flow needs the short stages on slices.
  - *P9, angle lists* (`probe_tm_angles.sh`, `tm_angle_lists.py`, `summarise_tm_angles.py`; HOTFIXES H7). The
    pilot's own pytom and extraction commands on its tomograms with the most candidates, with the old list and with
    correct I lists at 12°, 8° and 5°. Before any list it checks with pytom's own rotation which 2-2-2 setting the
    template has and on which side its symmetry composes. Search time per list, and the candidates against the old
    list's.
  - *P10, Warp chunks* (`probe_warp_chunk.sh`, `summarise_warp_chunk.py`; L5, P5 for Warp). Warp reconstruct on N
    tilt series: one call each as production, then one call for all with 1, 2 and 4 workers, on a B200 and on a
    slice; the cold read of the averages timed first (3.4 GB a tilt series), tomograms compared with one call each.
  - *P11, tsImport* (`probe_ts_import.sh`, partition `c`): production's `ts_import` over the 163 mdocs in one call,
    then 4 and 8 calls at once; its user and system seconds say whether the 12 minutes are work or waiting on
    `/groups`. Neither `ts_import` nor reconstruct wrote into its inputs in the pilot (file times), so the probes read
    them in place.
  - *P12, item dependencies* (`probe_aftercorr.sh`, four tiny jobs on `c`): whether `--dependency=aftercorr` starts
    task i when task i upstream ends, how long after, and what becomes of a task whose counterpart failed or is
    missing, with and without `--kill-on-invalid-dep`.
  - *The slices pilot.* The dev conf now runs motion, alignment, CTF, reconstruct and export on a slice, with memory
    at the pilot's peak plus about half (CTF 32 → 16G, reconstruct 64 → 24G, export 32 → 16G): above 25 GB a slice
    job is billed by its memory (0.134 a GB), and 28 slices share a node's memory. Motion stays at 24G, where its
    peak sat at the request. A second 412 project up to reconstruct then gives the stage times at full scale, and
    `scripts/compare_projects.py` its outputs against the pilot's (the pilot against itself: identical throughout).
  - Full B200s queued for about 8 hours this morning while slices started at once.
- **2026-11-09, P7b ran** (job 1493952, one B200, 16 CPUs, the production command with the full 3,398-rotation
  list, lanes started 30 s apart). The maps of one lane and of four lanes under MPS are byte for byte the same.

  | Lanes | MPS | Tomograms/min | Against one lane | DCGM SM active / occupancy | Set-up s | Search s |
  |---:|---|---:|---:|---|---:|---:|
  | 1 | – | 0.44 | 1.00 | 0.46 / 0.26 | 51.7 | 63.6 |
  | 2 | no | 0.64 | 1.45 | 0.67 / 0.37 | 62.1 | 80.9 |
  | 2 | yes | 0.78 | 1.75 | 0.80 / 0.45 | 27.8 | 96.5 |
  | 3 | yes | 0.85 | 1.91 | 0.88 / 0.49 | 27.8 | 127.1 |
  | 4 | yes | 0.87 | 1.97 | 0.91 / 0.51 | 35.5 | 170.6 |

  Two lanes under MPS give most of it, three nearly all: four sit at the search's ceiling (63.6 s a tomogram, 0.94 a
  minute). The tensor pipe stays unused. As in P7, a lane's set-up runs faster under MPS (51.7 s alone, about 28 s
  beside another), unexplained. With H7 fixed the search shrinks and the set-up and writes are most of a tomogram,
  so lanes would then fill a card that one process leaves mostly idle.
- **2026-11-09, P9's axis check** (job 1494445, on a slice). Over the 59 symmetry rotations the template keeps a
  correlation of at least 0.922 (median 0.944) with scipy's icosahedral group as it is, and 0.838 (median 0.855)
  turned 90° about z, against 0.864–0.889 for random rotations: the near-spherical particle looks alike from most
  directions, and its axes are scipy's setting (the project calls it I1). The symmetry composes on the right (0.988
  against 0.865). The two Euler readings give the same numbers for this group, so the run's "not clear" flag was the
  rule comparing the winner with that tie; it now compares with the other setting. Lists: 148 rotations at 12°, 578
  at 8°, 2,124 at 5°. A one-rotation warm-up on the slice, split 2 2 1, took 199 s with cold caches.
