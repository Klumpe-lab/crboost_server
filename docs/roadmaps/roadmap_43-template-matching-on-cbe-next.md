# Roadmap 43 — Template matching on CBE.next: pytom 0.14, B200 splits and scheduling, picking quality

**Status:** scoped 2026-11-04. S0 (pytom 0.14.0 image, the `-g` UUID fix) built the same day; S1 onwards open.
Decisions D1–D6 (§5) owed.

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

## 3. What pytom 0.11–0.14 changes for us

| Version | Change | Effect on crboost |
|---|---|---|
| 0.12.0 | Tomogram wedge computed correctly in volume-split mode | **Every crboost TM run so far was in split mode** (`-s 2 2 1`): scores change, should be cleaner |
| 0.14.0 | Phase-randomised template has the template's exact power spectrum | `--random-phase-correction` becomes a fair decoy; our `NumberOfFalsePositives` cutoff assumes that noise model. Costs ~2× TM time. Off in the copia protocol |
| 0.14.0 | Whitening filter from many small patches inside the wedge, outliers dropped | Only with `--spectral-whitening` (off in copia); mostly helps small particles / high-resolution templates |
| 0.14.0 | `--warp-xml-file`: angles, defocus, dose, voltage, Cs from Warp's XML; phase-flip, per-tilt weighting, defocus handedness (from `AreAnglesInverted`) and sample-leveling angles applied automatically | §6 S5. Our XMLs have `LevelAngleX/Y="0"`, so no leveling gain today; handedness only matters for `--volume-split` defocus-gradient correction. Gain = one metadata source instead of our `.tlt`/defocus/dose text files |
| 0.13.0 | Warp XML tilt angles multiplied by −1 internally | Only on the XML path: S5 must A/B it against the text path |
| 0.13.2 | Job splitting for any number of GPUs | S3 |
| 0.11.0 | job.json stores absolute paths; RELION 5 binning tracked for extraction | Candidate extraction reads job.json in place: fine |

Every CLI flag the TM and candidate-extraction drivers pass exists unchanged in 0.14.0 (checked flag by flag
against `entry_points.py`, 2026-11-04). pytom also has `--half-precision` (untried, D5).

## 4. Principles

- **Measure before defaults move.** S1's numbers decide D1–D3; nothing changes a protocol on a hunch.
- **Speed changes must not change scores.** Split, GPU type and precision are checked against a reference score
  map (S1), except where the change is the point (0.12 wedge fix, S4).
- **The scheduler is ours.** Parallelism across tomograms goes through the array (throttle, GPU type); pytom's own
  splitting is for latency on one tomogram.

## 5. Decisions (proposed 2026-11-04)

- **D1 — what `auto` means.** Proposed: `1 1 1` unless the tomogram plus template would not fit the GPU (estimate
  from the shapes; pytom's peak memory measured in S1), then the smallest split that fits. Today it is always
  `2 2 1`.
- **D2 — GPU type for TM.** Full B200 or a `1g.23gb` MIG slice per tomogram. 28 slices per node against 4 full GPUs:
  if a slice runs a tomogram in under ~7× the full-GPU time, slices win on throughput and queue time. S1 decides;
  the gres becomes typed either way.
- **D3 — default throttle.** 4 was sized for the old cluster. Proposed: derive it from D2 (e.g. 16 on full GPUs,
  more on slices) and leave the per-job field.
- **D4 — random-phase correction on by default?** After S4.
- **D5 — `--half-precision`.** Only if S1 shows scores unchanged within tolerance.
- **D6 — Warp XML as the metadata source.** A per-job choice (`metadata_source: text | warp_xml`) after S5, default
  text until copia agrees.

## 6. Stages

**S0 — pytom on B200 (built 2026-11-04).** `container_defs/pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_sm100.def`
(cupy 13.6.0 on the CUDA 12.9 runtime: NVRTC 12.1 could not compile for sm_100, every kernel load failed with
`CUDA_ERROR_NO_BINARY_FOR_GPU`; torch dropped, pytom does not import it). `drivers/template_match_pytom.py`: `-g`
from `allocated_gpu_count`, because CBE.next sets `CUDA_VISIBLE_DEVICES` to GPU UUIDs and pytom's `-g` rejected
them (copia-tutorial-20261104-1732 job007: `invalid int value: 'GPU-0ebb95c4-…'`). Stopgap noted, not wired: the
0.10.0 image runs on B200 with `CUPY_COMPILE_WITH_PTX=1`.

**S1 — benchmark (scripts and results in `docs/reports/tm-b200/`).** One copia tomogram (11.8 Å, 90°) and one 412
tomogram (6.2 Å, `angles_I1.txt`), each over: GPU {B200, MIG 1g.23gb} × split {2 2 1, 1 1 1} × precision {fp32,
half} × GPUs per tomogram {1, 2, 4 on one node}. Record wall time, peak GPU memory, and the score map against the
`1 1 1`/fp32/B200 reference (max abs difference, top-N peak agreement). Output: a table that answers D1, D2, D3, D5.

**S2 — defaults from S1.** `auto` per D1; typed gres for TM per D2 (a TM-specific SLURM default, the job's SLURM
section still overrides); throttle per D3. Fix the MEDIUM/LARGE presets to one node. Protocols keep their explicit
values; only `auto` moves.

**S3 — several GPUs on one tomogram.** For a one-off or a small project: a job choice "GPUs per tomogram" that sets
`gres=gpu:N` on one node and `-g 0..N-1`, so pytom splits the angular search. The supervisor's per-task resources
already come from the job's SLURM fields; check the array path passes N through and `allocated_gpu_count` sees it.

**S4 — picking quality A/B on copia (and one 412 project).** Same tomograms, same template: 0.10.0 (split) vs 0.14.0
(whole); then 0.14.0 with `--random-phase-correction` on/off; whitening on/off on 412 only. Compare candidate counts
at the same `NumberOfFalsePositives`, roadmap 30's σ ladders and decoy curves, and the end resolution (copia Class3D,
8.47 Å reference from copia-tutorial-20261015-1551). Answers D4.

**S5 — `--warp-xml-file`.** A `metadata_source` parameter on the TM job; on `warp_xml` the driver passes the tilt
series' XML from the reconstruction's Warp folder and drops `--tilt-angles/--defocus/--dose-accumulation`,
`--tomogram-ctf-model`, `--per-tilt-weighting`, voltage/Cs/amplitude (pytom drops them itself and warns). A/B on copia
against the text path; the 0.13.0 angle sign flip is the thing to watch. Answers D6.

**S6 — CBE.next carry-overs found on the way (not TM-only).**
- Per-job `/tmp` loses the CUDA JIT cache (`CUDA_CACHE_PATH`, `services/computing/container_service.py:28`): images
  that run from PTX (TensorFlow ones, imod) recompile every job. Move that one cache to a persistent per-user dir.
- `pytest -m cluster`: the cryocare probe only lists GPUs; make every GPU probe run one kernel (pytom's does, which is
  how it caught the cupy failure).
- cryocare, easymode, ais, pom (CUDA 11 / cuDNN 8 / TF 2.x) are not yet run on a B200; the binary survey says they
  should JIT from compute_80/86 PTX.

## 7. Runtime checklist (after S2)

1. A 412 project's TM job with the new defaults: wall time and SLURM accounting against the §2 table.
2. Candidate extraction on its output, then the subtomo extraction: unchanged behaviour downstream.
3. copia protocol end to end on pytom 0.14.0: Class3D resolution against 8.47 Å.

## 8. Build record

- **2026-11-04, S0.** pytom 0.14.0 image built on CBE.next (build + GPU verify scripts in
  `/scratch/users/artem.kushner/container_verify/`), verified on a B200 MIG 1g.23gb slice (cupy FFT, a run-time
  compiled kernel, `pytom_match_template.py --help`). Both `conf.yaml` files (dev checkout and the lab install) point
  `pytom` at it. `-g` UUID fix in `drivers/template_match_pytom.py`, in the dev checkout only: the lab install
  needs it pulled before its TM jobs run. NOT RUN in a pipeline yet: copia-tutorial-20261104-1732 job007 is the
  first.
