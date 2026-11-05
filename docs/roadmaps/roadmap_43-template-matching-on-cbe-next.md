# Roadmap 43 — Template matching on CBE.next: pytom 0.14, B200 splits and scheduling, picking quality

**Status:** scoped 2026-11-04. S0 (pytom 0.14.0 image, the `-g` UUID fix) built the same day. S1 benchmark run
2026-11-05 (§3); D1–D3 agreed and D5 settled from it (§6); S2 built the same day, NOT RUN in a pipeline yet (§8).
Open: D4 (the decoy needs its own cut-off before a protocol run), D6, S3 (parked), S4, S5 (`--warp-xml-file`).

**In one line.** Template matching has been the slow stage (hours on a 412 dataset) for reasons that are ours,
not pytom's: every tomogram is searched in four pieces, one after the other, on one GPU, four tomograms at a
time. On CBE.next's B200 nodes it can run whole, in parallel and possibly on MIG slices, with a pytom that fixes
the wedge we have been matching with and gives better-calibrated scores.

**Not in this roadmap:** other pickers, the particle views themselves (roadmap 30 holds the σ ladders and decoy
curves S4 reads), container rebuilds beyond pytom (done 2026-11-04, `container_defs/*_sm100.def`).

## 1. Goal

**Use case.** The maintainer runs TM on a ~140-tomogram 412 project and it finishes in well under an hour instead
of ~6 h, with the scores computed against the right wedge. On copia, turning on the random-phase decoy gives a
candidate list whose false-positive cutoff can be trusted without hand-tuning the count.

## 2. What exists (measured 2026-11-04)

**How crboost runs pytom.** `drivers/template_match_pytom.py`:

- `gpu_split: auto` (the default, and what every protocol uses) becomes `-s 2 2 1` (`get_gpu_split`, line 192): the
  tomogram is cut into four subvolumes. pytom's own help: volume split "can be relevant if the volume does not fit
  into GPU memory".
- `-g` is the allocated GPU count renumbered `0..N-1` (`allocated_gpu_count`, fixed in S0); every array task asks
  for one GPU (`perdevice: 1`, SLURM `gres=gpu:1`), so the four subvolumes run **serially** on that GPU and each
  seam adds the template-sized overlap pytom searches twice.
- `array_throttle` caps how many tomograms run at once (copia protocol: 4).

**Every past run used `-g 0 -s 2 2 1`** (task logs, old cluster, pytom 0.10.0):

| Project / job | Tomograms | Angular search | One tomogram |
|---|---|---|---|
| AGG412_20251031_412_FCIsolation job019 | 138 | `angles_I1.txt` | ~6 min |
| AGG412_20260205_412_FCIso_unmilled2 job022 | 162 | `angles_I1.txt` | ~8 min |
| AGG412_20260311_MyrGFP_Grid2 job042 | 48 | `angles_I1.txt` | ~17 min |
| copia-tutorial-20261015-1551 job007 | 1 | 90° | ~3 min |

At throttle 4 a 140-tomogram job is ~35 rounds, so 4–10 h of wall time before queue waits.

**How pytom parallelises** (`pytom_tm/parallel.py`, `split_job_efficiently`): volume splits first, and when there are
more GPUs than pieces it splits the angular search across them (`split_rotation_search`). 0.13.2 reimplemented this
for any GPU count. More GPUs on one tomogram cut that tomogram's latency; more tomograms at once cut the job's.

**CBE.next GPUs.** 8 nodes (`cbe-g5-*`), each `gpu:nvidia_b200:4` plus `gpu:nvidia_b200_1g.23gb:28` (MIG slices,
23 GB each). An untyped `--gres=gpu:1` is what crboost asks for today, and it lands on either: of the 2026-11-04
verify jobs (all `--gres=gpu:1`), most got a full B200 and the pytom 0.14.0 one got `NVIDIA B200 MIG 1g.23gb`, where
cupy ran fine. So TM tasks already land on slices sometimes, unplanned.

**Presets.** `services/computing/slurm_service.py`: MEDIUM is `gpu:2` on `nodes: 2`, LARGE `gpu:4` on `nodes: 4`.
pytom is one process on one node, so those presets spread GPUs it cannot use.

## 3. S1 results (measured 2026-11-05)

Scripts in `docs/reports/tm-b200/scripts/`; beside them `results.md` (whole score maps against the reference) and
`candidates.md` (what extraction keeps), with their `.json`; the maps in `/scratch/users/artem.kushner/tm_bench/`
(deleted after 28 days). pytom 0.14.0, every flag the driver passes, on one tomogram per dataset, both
1024 × 1024 × 512 voxels: copia (11.8 Å, 96³ template, 90° = 48 rotations) and 412 (AGG412_20251031 Position_1,
6.2 Å, 128³ template, `angles_I1.txt` = 3398 rotations). One run per case, 8 CPUs and 64 GB each (192 GB with 4
GPUs). Reference: the whole tomogram, fp32, one full B200. Wall time is pytom's whole run (start-up, I/O, search);
GPU memory is nvidia-smi's peak, sampled every 2 s. Candidates as each project extracts them (copia: up to 1500, 412:
up to 20, one false positive); "shared" is the share of the reference's candidates the case also picks within a
quarter particle diameter; "inside" is the region extraction keeps, a particle radius in from every face (the
reference's top score is 0.44 on copia, 0.068 on 412).

| Case | copia | 412 | GPU memory | Candidates copia / 412 | Shared | Largest score difference inside |
|---|---|---|---|---|---|---|
| B200, whole, fp32 (reference) | 46 s | 112 s | 30.7 GiB | 1005 / 20 | | |
| B200, whole, half | 50 s | 114 s | 28.7 / 30.7 GiB | 1005 / 20 | all / all | 1e-4 / 3e-5 |
| B200, `2 2 1`, fp32 (today's `auto`) | 409 s | 414 s | 10.2 GiB | 1008 / 20 | all / all | 2e-6 / 0.002 |
| B200, `2 2 1`, half | 161 s | 261 s | 10.2 GiB | 1008 / 20 | all / all | 1e-4 / 0.002 |
| B200, `1 3 1`, fp32 | 176 s | | 14.5 GiB | | | 2e-6 / |
| MIG 1g.23gb, `2 2 1`, fp32 | 186 s | 793 s | not measurable on a slice | 1008 / 20 | all / all | 2e-6 / 0.002 |
| MIG 1g.23gb, `1 3 1`, fp32 | 570 s | | | | | 2e-6 / |
| MIG 1g.23gb, whole, fp32 or half | out of GPU memory | out of GPU memory | | | | |
| 2 B200s, whole | 106 s | 142 s | 30.7 GiB each | 1005 / 20 | all / all | 0 / 2e-4 |
| 4 B200s, whole | 147 s | 164 s | 30.7 GiB each | 1005 / 20 | all / all | 0 / 2e-4 |
| B200, whole, `--random-phase-correction` | 134 s | 167 s | 32.7 GiB | 103 / 20 | 10 % / 35 % | 0.34 / 0.042 |
| B200, whole, `--spectral-whitening` | 95 s | 165 s | 30.7 GiB | 980 / 20 | 97 % / 20 % | 0.15 / 0.042 |
| pytom 0.10.0, `2 2 1`, old cluster (job019) | | ~6 min (§2) | | | | – / 0.079 |

What it says:

- **Whole beats split on a full B200**: 46 s against 409 s (copia), 112 s against 414 s (412). The tomogram fits five
  times over (30.7 of 179 GiB).
- **Whole and split pick the same particles.** Inside, the scores agree to 2e-6 on copia and to 0.002 (3 % of the
  top score) on 412: 1008 against 1005 candidates on copia with every reference candidate among them, the same 20 on
  412. The larger differences in `results.md` (0.23 of 0.44 on copia) sit at the faces, where the pieces and the
  whole volume wrap around differently and extraction masks anyway. pytom applies no per-piece defocus gradient here:
  that needs a defocus handedness, which the text-file path does not set.
- **A whole tomogram does not fit a slice**, in fp32 or half: both died asking cupy for 4 GiB more with 18 GiB
  allocated. `2 2 1` needs 10.2 GiB and runs there.
- **pytom's peak memory is 60 bytes per voxel of the FFT-padded volume it searches, plus 0.7 GiB**: the line
  through the first two shapes measured (1024 × 1024 × 512 whole; 576 × 576 × 512 per `2 2 1` piece, pytom's
  `next_fast_len` of half the tomogram plus half the template), on which both datasets agree within 22 MiB. A third
  shape, `1 3 1`'s 1024 × 450 × 512 piece, peaked at 14.5 GiB against 13.9 predicted (+5 %).
  `--random-phase-correction` adds one float32 volume (4 bytes per voxel, +2.0 GiB here).
- **On a slice `2 2 1` beats `1 3 1`**: 186 s against 570 s on copia, though `1 3 1` searches less padded volume
  (655 M against 680 M voxels) and fewer pieces.
- **A slice against a full B200, on 412** (the real workload): 793 s split against 112 s whole, 7.1×. A node's 28
  slices then match its 4 B200s in tomograms per hour (127 against 129), each tomogram taking 7× as long. On copia's
  48 rotations a slice is 4× (186 s against 46 s): 1.7× the tomograms per node, minutes either way.
- **Fair share**: partition `g` bills the largest of CPUs (1 each), memory (0.134 per GB), B200 (24), slice
  (3.43). With TM's 4 CPUs and 32 GB, a 412 tomogram costs 24 × 112 s ≈ 2.7k on a B200 and 4.3 × 793 s ≈ 3.4k on a
  slice; a copia one 1.1k against 0.8k.
- **More GPUs on one tomogram is slower at these sizes**, with the same candidates: 46 → 106 → 147 s (copia),
  112 → 142 → 164 s (412) for 1, 2, 4 B200s. Each GPU's process holds its own copy on the host, 22 GiB each: the 4-GPU
  runs were OOM-killed at 64 GB and re-run at 192 GB.
- **Half precision picks the same particles and gains nothing on a whole tomogram**: 50 against 46 s, 114 against
  112 s; 28.7 against 30.7 GiB on copia, the same on 412. It helps only the split (161 against 409 s), which `auto`
  leaves.
- **Host side**: one CPU core busy throughout in every case (TotalCPU = Elapsed); peak host RAM 22 GiB whole,
  24 GiB split. TM's profile (4 CPUs, 32 GB) covers it.
- **The decoy keeps a tenth of copia's candidates**: 103 against 1005, every one of them a reference candidate.
  pytom 0.14 writes the same job_stats `std` with and without `--random-phase-correction` (0.0198 on copia), so the
  cut-off for one false positive stays at 0.129, while the decoy-corrected map's noise inside drops from 0.0172 to
  0.0103. On 412 it keeps 7 of the reference's 20. It costs 2.9× (copia) and 1.5× (412) the time.
- **Whitening recalibrates** (job_stats `std` 0.0125 against 0.0198 on copia): 980 candidates, 97 % of the
  reference's; on 412 4 of 20. 2.1× and 1.5× the time.
- **pytom 0.10 against 0.14** (412, the old cluster's job019 maps): inside, the scores differ by up to 0.079, more
  than 0.14's top score; 25 % of the reference's best 300 peaks reappear (`results.md`). Comparing them is S4's.

Where the data is thin:

- One tomogram per dataset, one run per case, nothing repeated; what else ran on the node was not controlled.
- The two B200 `2 2 1` fp32 runs were the first two to start (cold kernel cache) and took the same ~410 s for 48
  and 3398 rotations, so most of that is a fixed cost, not the search. The crboost task on the same copia tomogram
  (copia-tutorial-20261104-1732 job007, `2 2 1`, B200) took 4:30 in all. Whole stays 2.3–3.5× faster than the
  half-precision splits, which ran warm, but "9×" would overstate it.
- 412's candidates are capped at 20, so its counts cannot move; its shared top 20 is the measure.
- GPU memory is sampled every 2 s, so it is a lower bound: copia on 2 GPUs caught 4.6 GiB of ~31. On a slice
  nvidia-smi reports the whole parent GPU; those readings are left out.
- Not run: `2 1 1` on a slice (17.6 GiB by the model, too close to the slice's 21.4 GiB to choose blind), and a
  non-spherical mask (both datasets use spherical ones).
- The candidates come from `extract_fast.py`: pytom's own extraction on C-ordered copies of the maps. Plain
  `pytom_extract_candidates.py` took ~7 s per candidate in the benchmark job (its loop runs `np.nanargmax` on the
  transposed map each time), while copia-tutorial-20261104-1732 job008 extracted 1008 in 6 min with the same image;
  why the benchmark job was slower is not known.
- Queues were not measured, and they decide which GPU type is free first: at 14:30 on 2026-11-05, with cbe-g5-6
  drained, 7 of 196 slices and 8 of 28 B200s were free.

## 4. What pytom 0.11–0.14 changes for us

| Version | Change | Effect on crboost |
|---|---|---|
| 0.12.0 | Tomogram wedge computed correctly in volume-split mode | **Every crboost TM run so far was in split mode** (`-s 2 2 1`): scores change, should be cleaner |
| 0.14.0 | Phase-randomised template has the template's exact power spectrum | `--random-phase-correction` becomes a fair decoy; our `NumberOfFalsePositives` cutoff assumes that noise model. Costs ~2× TM time. Off in the copia protocol |
| 0.14.0 | Whitening filter from many small patches inside the wedge, outliers dropped | Only with `--spectral-whitening` (off in copia); mostly helps small particles / high-resolution templates |
| 0.14.0 | `--warp-xml-file`: angles, defocus, dose, voltage, Cs from Warp's XML; phase-flip, per-tilt weighting, defocus handedness (from `AreAnglesInverted`) and sample-leveling angles applied automatically | §7 S5. Our XMLs have `LevelAngleX/Y="0"`, so no leveling gain today; handedness only matters for `--volume-split` defocus-gradient correction. Gain = one metadata source instead of our `.tlt`/defocus/dose text files |
| 0.13.0 | Warp XML tilt angles multiplied by −1 internally | Only on the XML path: S5 must A/B it against the text path |
| 0.13.2 | Job splitting for any number of GPUs | S3 |
| 0.11.0 | job.json stores absolute paths; RELION 5 binning tracked for extraction | Candidate extraction reads job.json in place: fine |

Every CLI flag the TM and candidate-extraction drivers pass exists unchanged in 0.14.0 (checked flag by flag
against `entry_points.py`, 2026-11-04). pytom also has `--half-precision` (untried, D5).

## 5. Principles

- **Measure before defaults move.** S1's numbers decide D1–D3; nothing changes a protocol on a hunch.
- **Speed changes must not change scores.** Split, GPU type and precision are checked against a reference score
  map (S1), except where the change is the point (0.12 wedge fix, S4).
- **The scheduler is ours.** Parallelism across tomograms goes through the array (throttle, GPU type); pytom's own
  splitting is for latency on one tomogram.

## 6. Decisions (proposed 2026-11-04; D1–D3 and D5 settled 2026-11-05 from S1)

- **D1 — what `auto` means.** Was always `2 2 1`. **Agreed 2026-11-05, built in S2:** sized per task on the node,
  for the GPU the task actually got. Of the splits into 1, 2, 4 or 8 pieces per axis whose largest piece fits, the
  one with the least padded volume to search, so the whole tomogram whenever it fits. Fits: §3's estimate (60
  bytes per voxel of the piece's FFT-padded shape, sized as pytom sizes it, plus 0.7 GiB; 64 bytes with the decoy)
  within 75 % of the GPU's memory. Powers of two because `1 3 1` ran 3× slower than `2 2 1` on a slice (§3). On a
  slice nvidia-smi reports the parent GPU, so a slice's size comes from its MIG profile name (`MIG 1g.23gb`: 23 GB).
  Our 1024 × 1024 × 512 tomograms: whole on a B200 (checked on the node: 179.1 GiB), `2 2 1` on a slice (21.4 GiB);
  a B200 takes up to ~2.4 G voxels whole; other sites' smaller cards get the split that fits (24 GiB: `1 2 1`). A
  task that cannot read its GPU's memory fails; it never guesses. An explicit split (`2:2:1`, `None`) passes as
  before. Whole and split pick the same particles (§3), so `auto` moving changes speed, not picks.
- **D2 — GPU type for TM.** Full B200 or a `1g.23gb` MIG slice per tomogram. 28 slices per node against 4 full GPUs:
  if a slice runs a tomogram in under ~7× the full-GPU time, slices win on throughput and queue time. S1 decides;
  the gres becomes typed either way. **Agreed 2026-11-05, built in S2:** a full B200 (`gpu:nvidia_b200:1`) in
  conf.yaml's `templatematching` profile. On 412 a slice is 7.1×: the same tomograms per node per hour, each taking
  7× as long, for ~25 % more fair share; on copia slices win 1.7× in throughput, minutes either way. The job's SLURM
  section still takes a slice (`gpu:nvidia_b200_1g.23gb:1`, with a higher throttle) or untyped `gpu:1` when B200s
  are queued; `auto` then splits to fit. Also a full B200: Segment (AGG412_SK_20251017_412 job029, 2026-11-05:
  easymode's actin model took 17:46 on a slice against 3:57 on a B200, and all 8 tasks that landed on slices were
  stopped at 40 min by the 45-minute task limit while B200 tasks took 17–21 min; it has also peaked at 22 GB of GPU
  memory), Class3D (one) and Refine3D (two), whose RELION runs a slice would give a seventh of a GPU each,
  unmeasured. Other job types stay untyped until measured.
- **D3 — default throttle.** 4 was sized for the old cluster. Proposed: derive it from D2 (e.g. 16 on full GPUs,
  more on slices) and leave the per-job field. **Agreed 2026-11-05, built in S2:** 16 for new TM jobs. A 412
  tomogram is ~2 min on a B200, so 140 of them take 9 waves, ~20 min plus each task's start-up (8: ~40 min; 4: ~80
  min). Partition `g`'s default QOS (`g_short`, 8 h) caps no one's GPUs; 16 is about half the B200s in service,
  minutes at a time. On slices raise it ~7× (the field allows 64). Protocols keep theirs (copia: 4).
- **D4 — random-phase correction on by default?** After S4. Not from the benchmark alone. S1 (§3): on copia the
  decoy keeps 103 of the reference's 1005 candidates and adds none, because pytom 0.14 keeps the uncorrected score
  `std` for the false-positive cut-off while the corrected map's noise is 40 % lower. The picks are sane, the count
  is not comparable: the copia-tutorial protocol with `random_phase_correction` on would send ~100 particles to
  Class3D against the reference's ~1000, so its resolution against 8.47 Å measures the count as much as the decoy.
  Next: that protocol run, with the decoy's cut-off set to keep a comparable count (a manual cut-off, or
  `expected_false_positives` raised), then Class3D against 8.47 Å. Off until then.
- **D5 — `--half-precision`.** Only if S1 shows scores unchanged within tolerance. **Settled 2026-11-05:** stays off,
  the job field stays. Scores agree (the same candidates on both datasets, differences ≤ 1e-4), but on a whole
  tomogram it saves no time (50 against 46 s, 114 against 112 s) and little or no GPU memory; it only speeds up the
  split that `auto` leaves.
- **D6 — Warp XML as the metadata source.** A per-job choice (`metadata_source: text | warp_xml`) after S5, default
  text until copia agrees.

## 7. Stages

**S0 — pytom on B200 (built 2026-11-04).** `container_defs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.def`
(cupy 13.6.0 on the CUDA 12.9 runtime: NVRTC 12.1 could not compile for sm_100, every kernel load failed with
`CUDA_ERROR_NO_BINARY_FOR_GPU`; torch dropped, pytom does not import it). `drivers/template_match_pytom.py`: `-g`
from `allocated_gpu_count`, because CBE.next sets `CUDA_VISIBLE_DEVICES` to GPU UUIDs and pytom's `-g` rejected
them (copia-tutorial-20261104-1732 job007: `invalid int value: 'GPU-0ebb95c4-…'`). Stopgap noted, not wired: the
0.10.0 image runs on B200 with `CUPY_COMPILE_WITH_PTX=1`.

**S1 — benchmark (run 2026-11-05, §3; scripts and per-case numbers in `docs/reports/tm-b200/`).** One copia
tomogram (11.8 Å, 90°) and one 412 tomogram (6.2 Å, `angles_I1.txt`), each over: GPU {B200, MIG 1g.23gb} × split
{2 2 1, 1 1 1} × precision {fp32, half} × GPUs per tomogram {1, 2, 4 on one node}. Record wall time, peak GPU
memory, and the score map against the `1 1 1`/fp32/B200 reference (max abs difference, top-N peak agreement).
Output: a table that answers D1, D2, D3, D5. Added on the way: `1 3 1` on copia (B200 and slice), the candidate
overlap inside the extraction region (`candidate_check.py`).

**S2 — defaults from S1 (built 2026-11-05).** `auto` per D1; typed gres for TM per D2 (a TM-specific SLURM default,
the job's SLURM section still overrides); throttle per D3. Fix the MEDIUM/LARGE presets to one node. Protocols keep
their explicit values; only `auto` moves.
- `drivers/driver_base.py`: `allocated_gpu_memory_bytes()`, the smallest GPU the job got, read with nvidia-smi
  (a slice from its `MIG <n>g.<m>gb` name in `nvidia-smi -L`). Checked on the node 2026-11-05: 179.1 GiB on a B200,
  21.4 GiB on a slice.
- `drivers/template_match_pytom.py`: `auto_gpu_split` and pytom's memory model (`pytom_piece_shape`,
  `pytom_peak_bytes`), `TemplateMatchPytomDriver.volume_split` per task; the task log names the split and the
  estimate. Under `CRBOOST_PRINT_CMD` the printed command says `-s auto` (no GPU to size for).
- `services/jobs/template_match.py`: `array_throttle` 16; `gpu_split` and `half_precision` describe S1.
- `services/jobs/class3d.py`: the job no longer sets its own `gres`, so conf.yaml's `class3d` profile names the GPU.
- `services/computing/slurm_service.py`: MEDIUM and LARGE on one node; `gres_keeping_type`, through which
  `apply_slurm_preset` keeps the GPU type the job had (MEDIUM on TM: `gpu:nvidia_b200:2`).
- conf.yaml, both installs: `templatematching`, `segment` (new, GPU only), `class3d` `gpu:nvidia_b200:1`, `refine3d`
  `gpu:nvidia_b200:2`. `conf.template.yaml` stays generic (`gpu:1`; Class3D's dead `gpu:4` became `gpu:1`) and names
  the CBE.next type in comments.

**S3 — several GPUs on one tomogram.** For a one-off or a small project: a job choice "GPUs per tomogram" that sets
`gres=gpu:N` on one node and `-g 0..N-1`, so pytom splits the angular search. The supervisor's per-task resources
already come from the job's SLURM fields; check the array path passes N through and `allocated_gpu_count` sees it.
S1: 2 and 4 B200s on one tomogram were slower than one on both datasets (§3), and each GPU adds 22 GiB of host RAM.
Parked until a search much finer than `angles_I1.txt` makes the angular split pay.

**S4 — picking quality A/B on copia (and one 412 project).** Same tomograms, same template: 0.10.0 (split) vs 0.14.0
(whole); then 0.14.0 with `--random-phase-correction` on/off; whitening on/off on 412 only. Compare candidate counts
at the same `NumberOfFalsePositives`, roadmap 30's σ ladders and decoy curves, and the end resolution (copia Class3D,
8.47 Å reference from copia-tutorial-20261015-1551). Answers D4.

**S5 — `--warp-xml-file`.** A `metadata_source` parameter on the TM job; on `warp_xml` the driver passes the tilt
series' XML from the reconstruction's Warp folder and drops `--tilt-angles/--defocus/--dose-accumulation`,
`--tomogram-ctf-model`, `--per-tilt-weighting`, voltage/Cs/amplitude (pytom drops them itself and warns). A/B on copia
against the text path; the 0.13.0 angle sign flip is the thing to watch. Answers D6.
Prerequisite for any gain: our XMLs have `LevelAngleX/Y="0"` because crboost never runs WarpTools
`ts_autolevel` (estimates the sample's inclination from CTF patches, 2026-11-05: present in the Warp 2.0.0dev41
image). Running it is not free: the level angles also enter Warp's reconstruction, so the tomogram's frame (and
every coordinate picked in it) moves, and a project must not mix levelled and unlevelled tomograms. Try it as an
opt-in step on copia first: where it belongs in the order, tomogram thickness, CTF fit, TM scores, Class3D.

**S6 — CBE.next carry-overs found on the way (not TM-only).**
- Per-job `/tmp` loses the CUDA JIT cache (`CUDA_CACHE_PATH`, `services/computing/container_service.py:28`): images
  that run from PTX (TensorFlow ones, imod) recompile every job. Move that one cache to a persistent per-user dir.
- `pytest -m cluster`: the cryocare probe only lists GPUs; make every GPU probe run one kernel (pytom's does, which is
  how it caught the cupy failure).
- cryocare, easymode, ais, pom (CUDA 11 / cuDNN 8 / TF 2.4–2.11) run on a B200 from compute_80 PTX (2026-11-05,
  `container_verify/verify_tf.sh`: matmul, conv2d, conv3d, FFT on a full B200, no NaNs; values not checked against
  a CPU run). The first GPU call compiles that PTX: 742 s (ais), 747 s (pom), 782 s (easymode), 1053 s plus 275 s of
  device setup (cryocare); the second call takes under 0.1 s. With the cache in per-job `/tmp`, every job pays this
  again, so the cache fix above comes first. No rebuild needed: no TF release ships sm_100 SASS, so a rebuilt
  TF image would JIT as well. Set `CUDA_CACHE_MAXSIZE` too (default 256 MB; TF's compiled kernels are larger).
  **Done 2026-11-05:** conf `gpu_kernel_cache` (`/groups/klumpe/software/cache_and_artifacts/cuda_kernels`, one
  subdirectory per image, shared by all users and both installs, `CUDA_CACHE_MAXSIZE` 4 GiB). ais twice in a row
  against it: 775 s, then 27 s for the first GPU call; its cache is 1.6 GB.

## 8. Runtime checklist (after S2)

1. A 412 project's TM job with the new defaults: wall time and SLURM accounting against the §2 table; each task's
   log says `gpu_split auto: 1 1 1` and its accounting `gres/gpu:nvidia_b200=1`.
2. Candidate extraction on its output, then the subtomo extraction: unchanged behaviour downstream.
3. copia protocol end to end on pytom 0.14.0: Class3D resolution against 8.47 Å.
4. One TM task with the job's gres set to `gpu:nvidia_b200_1g.23gb:1`: `auto` picks `2 2 1` and the task finishes.
5. A Segment, a Class3D and a Refine3D job created after S2 land on full B200s (`sacct -o AllocTRES`).

## 9. Build record

- **2026-11-04, S0.** pytom 0.14.0 image built on CBE.next (build + GPU verify scripts in
  `/scratch/users/artem.kushner/container_verify/`), verified on a B200 MIG 1g.23gb slice (cupy FFT, a run-time
  compiled kernel, `pytom_match_template.py --help`). Both `conf.yaml` files (dev checkout and the lab install) point
  `pytom` at it. `-g` UUID fix in `drivers/template_match_pytom.py`, in the dev checkout only: the lab install
  needs it pulled before its TM jobs run. NOT RUN in a pipeline yet: copia-tutorial-20261104-1732 job007 is the
  first.
- **2026-11-05, S1.** The matrix of §3 on CBE.next (`docs/reports/tm-b200/`): `submit_all.sh`, then
  `extract_fast_and_compare.sh` (the plain `extract_and_compare.sh` would have outrun its 4 h limit), `1 3 1` added,
  `candidate_check.py`. pytom 0.14.0 image.
- **2026-11-05, S2.** As listed under S2 in §7; both `conf.yaml` files edited (the lab install's backed up as
  `conf.yaml.bak-20261105-typed-gres`). Checked: `allocated_gpu_memory_bytes` and `auto_gpu_split` in a job on a
  B200 and on a slice, the effective SLURM configs and presets of TM, Segment, Class3D, Refine3D, tsReconstruct,
  ruff, `check_boundaries.py`, `pytest` (not `-m cluster`). The lab install needs S2 pulled; until then its conf's
  typed TM/Segment/Refine3D gres work with the old `auto` (`2 2 1`) and Class3D keeps its own `gpu:1`. NOT RUN in a
  pipeline yet (§8).
