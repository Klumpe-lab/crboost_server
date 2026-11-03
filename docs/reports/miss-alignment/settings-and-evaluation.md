# miss-alignment: what we can tune, and how to judge a run without particles

Scope: every setting of warpem's miss-alignment that the repo or the paper documents, what crboost exposes
today, which regimes are worth testing, and a particle-free way to tell whether a run improved the
alignment. Sources: the repo (`github.com/warpem/miss-alignment`, `main`; the container has PyPI 0.2.0),
the preprint (Chaillet et al., bioRxiv 2026, doi 10.64898/2026.04.29.721716), and the metrics literature
cited in §4. Items marked *(unverified)* were not confirmed against source or text. Companion docs:
`docs/miss-alignment.md` (tool reference, invocation contract), `docs/roadmaps/roadmap_21-miss-alignment-and-warp-export.md`
(integration; its stage 5 "visibly better" criterion is what §4 replaces).

The "crboost" columns describe the job before roadmap 24 stage 1. Since then `seed`, `lr_milestones`, Å
schedules (PAPER / custom), `training_gpus`, `reconstruction_devices`, the Z box (`z_box`) and
`--prepare-stacks` are job parameters; see the roadmap's progress notes. Two items below were verified since:
`--halfmap_tilts` splits by `i % 2` over the XML tilt order (angle-sorted in crboost), and downstream Warp jobs
take the volume box from their settings, not the XML. **Correction to §1:** its per-tilt numbers removed a
six-parameter fit (a·cos θ + b·sin θ + c per image component) that also absorbed three real modes; with only the
rigid translation removed the first run changed per-tilt shifts by 4.0–4.4 Å rms on two series, 11.8–14.9 on two
and 22.8–35.1 on three (roadmap 24 progress notes).

---

## 1. Why the first run (`bullseye_artifact_two`, 2026-09-29) showed nothing

Measured on that run: after removing a rigid volume shift (the Z "gauge" the tool re-centres into), per-tilt
shifts changed by 3.0–3.8 Å rms on two series, 8–9 Å on two, 16–30 Å on three. The run was far from what the
paper does:

| | our run | upstream template | paper (lamella / EMPIAR-10499) |
|---|---|---|---|
| stack pixel size (= `downsample: 1`) | 6.2 Å | whatever the stack is | **10 Å** (`--prepare-stacks 10.0`) |
| schedule | ds2 anchoring → ds1 global | ds3, ds2 anchoring → ds1 global ×2 → ds1 `[3,3]` ×4 | 30 Å first, 2× anchoring at 20 Å, 1× global at 10 Å, **5× `[3,3]`** |
| local `[N,N]` iterations | **none** | 4 | 5 |
| training steps per iteration | 15 × 250 = **3,750** | 30 × 1000 = 30,000 | template |
| LR schedule / early stopping | 2nd LR cut and early stop **never engaged** (both arm at epoch 15 = the cap) | engaged | engaged |
| training tilt-series | 7 | — | 10 (SHREC), 33, 65 |
| volume box (alignment grid extent) | full tomogram, **3174 Å in Z** | "fit tightly in all three dimensions" | tight |
| starting alignment | AreTomo (via Warp) | — | etomo patch tracking + autolevel |

The paper's own ablation: "STA resolution benefits most from local alignment, while global-only alignment
contributes modestly" (EMPIAR-10499: AreTomo3 12.3 Å, etomo 10.6 Å, missAlign global 9.3 Å, missAlign `[3,3]`
5.1 Å). Our run was global-only, under-trained, on a box that is mostly empty in Z. A null result is the
expected outcome of that configuration, not evidence against the tool.

The flat second-iteration loss is not a failure sign either: "aligned" is the *current* alignment and
"misaligned" is current + a synthetic shift, so the loss measures how detectable the synthetic
perturbations are, not how good the alignment is. The score is lower-is-better; a separation above
`loss_margin` means the hinge is satisfied and is not calibrated beyond that.

---

## 2. Settings catalogue

"crboost" column: **exposed** = a job-tab parameter; **fixed** = hard-coded in `drivers/miss_align.py::build_config`;
**—** = not wired.

### 2.1 Schedule — `general.iteration_settings` (one entry per macro-iteration)

| field / value | what it does | crboost |
|---|---|---|
| `downsample` (int ≥ 1) | Fourier-crop factor on the **stack's** header pixel size. Our stacks are 6.2 Å → ds1 6.2, ds2 12.4, ds3 18.6, ds5 31 Å. Patch FOV = `patch_size` × pixel. | via preset |
| `alignment: "anchoring"` | Global per-tilt shifts, grown outward: starts with the central half of the tilts "reliable", resets outer tilts to their neighbour, adds one tilt per side per round, finishes with a full global pass; a round is kept only if the loss improves. Coarse stage. | via preset |
| `alignment: "global"` | One `AxisOffsetX/Y` (Å) per tilt, then re-centred on the zero tilt (source of the Z gauge shift). | via preset |
| `alignment: "spline"` | 7 Catmull-Rom control points over the tilt range for x/y shifts, then one global pass. Smooth-drift model. Not in the template. | — |
| `alignment: [Nx, Ny]` | Local: one 2D B-spline image warp per tilt (`GridMovementX/Y`, Å). Continues from the previous grid; does not touch `AxisOffset`. **Where the paper's STA gains come from.** Warp's `ts_reconstruct` applies it; RELION extraction does not (roadmap 21 §3). | `[3,3]` in default/thorough presets |
| `alignment: [X, Y, Z, T]` | 4D volume deformation grid (`GridVolumeWarpX/Y/Z`). Not in the template; no paper result. | — |
| never refined | tilt angles, tilt-axis angle, level angles / pretilt, magnification, CTF | — |

Optimiser for every mode: L-BFGS, one step at torch defaults (≤ 20 iterations), no priors or regularisation.
The objective is the precision-weighted sum of CNN scores over a patch grid tiling the whole XML volume.

crboost presets (`services/jobs/miss_align.py::MISS_ALIGN_SCHEDULES`): *fast* = ds2 anchoring, ds1 global;
*default* = ds3 anchoring, ds2 anchoring, ds1 global, ds1 `[3,3]`; *thorough* = the upstream template (8).

### 2.2 Training — `model_training`

| key | template | what it does | crboost |
|---|---|---|---|
| `model_architecture` | `default` | `default`/`simple` (compact 3D CNN, 8→64 ch, GroupNorm, SiLU), `gelu`, `spread` (7³ first kernel), `wide` (16–64 ch), `deep` (paired convs), `resnet` (~45k params). No paper ablation. | fixed `default` |
| `model_checkpoint` | null | Init for iteration 0 only; later iterations always continue from the previous best checkpoint. | fixed null |
| `loss_margin` | 0.5 | Triplet hinge margin; precision-regularisation weight fixed at 0.01. | fixed 0.5 |
| `learning_rate` | 1e-3 | × number of training GPUs. | fixed |
| `weight_decay` | 1e-4 | > 0 → AdamW, 0 → Adam. | fixed |
| `max_epochs_per_iteration` | 30 | Hard cap. | **exposed** (default 30; the run used 15) |
| `warmup_steps` | 500 | Linear warm-up, restarts every macro-iteration. | fixed |
| `multistep_lr_scheduler` | `[5, 15]`, γ 0.5 | Per-epoch LR cuts. Early stopping (train loss, patience 5, min Δ 0.001) arms only at epoch ≥ max(milestones), so a cap ≤ 15 disables it. | fixed |

Checkpoint selection: best training loss (no validation set). Training runs in 16-bit mixed precision.

### 2.3 Data — `data_loading` and `shift_generation`

| key | template | what it does | crboost |
|---|---|---|---|
| `batch_size` | 32 | Triplets per step per GPU; 32 fits 24 GB. | exposed |
| `patch_size` | 96 | Reconstruction cube edge in px at the current downsample (no crop). The paper text says 64³ *(code says patch_size)*. | exposed |
| `steps_per_epoch` | 1000 | Epoch size. | **exposed** (default 250) |
| `trajectory_probability` / `_max_shift` | 0.5 / 10 px | Smooth parabolic drift per axis, optional mid-series break, mean-centred. | fixed |
| `jitter_probability` / `_max_std` | 0.5 / 2 px | Independent Gaussian per tilt, σ ~ U(0, max). | fixed |
| `outlier_probability` / `_max_shift` | 0.5 / 20 px | One tilt shifted within ±max. | fixed |
| `fracture_probability` / `_max_shift` | 0.5 / 20 px | A contiguous run at one end shifted (constant or ramp). SHREC example: 30. | fixed |

Synthetic shifts are in **pixels at the current downsample**, generated in 3D and projected per tilt (25 % chance
each of zeroing x or y). At our 6.2 Å stacks they are 0.62× the paper's in Å. No paper ablation of any of these.

### 2.4 Alignment phase — `tilt_series_alignment`

| key | template | what it does | crboost |
|---|---|---|---|
| `patch_size` | 96 | Scoring sub-volume; keep equal to training. | = data patch |
| `patch_overlap` | 0.1 | Stride = patch × (1 − overlap). The grid tiles the full XML `VolumeDimensionsAngstrom` in x, y **and z**. | fixed |
| `batch_size` | 32 | Memory only (gradients accumulate over all patches before the step). | = data batch |

### 2.5 CLI and `general`

| flag / key | what it does | crboost |
|---|---|---|
| `apply_ctf` | CTF-weighted back-projection in training and alignment. Template: "doubles processing time with no benefit to alignment". Needs correct CTF in the XML. | fixed false |
| `seed` | Lightning `seed_everything`. Different seeds → the test–retest noise floor in §4. | fixed 45132 |
| `--training-devices`, `--reconstruction-devices`, `--dataloaders-per-trainer`, `--pool-size` | Throughput only (see `docs/miss-alignment.md` §3). | exposed (num_gpus, workers, pool) |
| `--start-at-iteration` | Resume from `iterN/`. | driver resume |
| `--prepare-stacks <Å>` | Rebuild stacks from Warp frame averages at this pixel size; overwrites `tiltstack/*.st`. Needs `average/*.mrc` and movie paths in the XML. | **refused** (driver assumes no movie paths; the XMLs do carry `MoviePath`, roadmap 24 §3.4) |
| `--preprocess` | torch-tiltxcorr at ds1; **replaces** `AxisOffsetX/Y` with XCF shifts. Paper: XCF start is "slightly lower accuracy, particularly for thick specimens". Don't use on top of AreTomo. | — |
| `infer` | Apply a previous run's per-iteration models to new data (no training). The route to hundreds of TS: train on a representative subset, infer the rest. | — |
| `--n-cluster-workers` + `MISS_CLUSTER_CONFIG` | Farm the alignment phase out as cluster jobs; training stays on one node. | — |
| volume box (`VolumeDimensionsAngstrom` in the XML) | Not a config key: the stamp step writes it. Upstream: fit tightly in x, y, z; paper: empty regions "dominate and degrade accuracy". | stamped = full tomogram (3174 Å Z) |

Hard-coded upstream (not settable): oversampling 2.0, 2 positions × 2 mirrors per reconstruction, ±10° rotation
augmentation about Y, contrast/edge/cube-mask augmentation, anchoring start fraction ½, spline 7 knots.

---

## 3. Regimes worth testing, by expected impact

1. **Local iterations.** Add `[3,3]` rounds (paper: 5). The only mode with a documented large gain. Bigger grids
   (`[5,5]`) and the 4D volume warp have no paper evidence; try only after `[3,3]` shows a signal.
2. **Tight volume box.** Stamp Z to the lamella (thickness + margin, centred on it) instead of 3174 Å. For a
   200 nm lamella at ds1 about 5 of 6 patch layers are empty. The lamella's Z position and thickness come from
   the baseline tomogram's variance profile (same estimate the metric mask needs, §4). Check first whether
   downstream `ts_ctf`/`ts_reconstruct` read the XML's `VolumeDimensionsAngstrom` or the settings file; if the
   former, restore the full box after refinement.
3. **Full training budget.** 30 epochs × 1000 steps, so the second LR cut and early stopping engage. With a
   cap ≤ 15 neither happens.
4. **Paper-like pixel ladder.** No new stacks needed with 6.2 Å stacks: ds5 (31 Å) anchoring → ds3 (18.6 Å)
   anchoring ×2 → ds2 (12.4 Å) global → ds2 `[3,3]` ×5. Patch 80 at 12.4 Å ≈ the paper's 96 × 10 Å field of view.
   The current presets use ds1 = 6.2 Å, finer than anything in the paper.
5. **More training tilt-series.** 30–60 from one session instead of 7; then `infer` on the rest. No stated
   minimum; the paper trained on 10–65.
6. **Starting alignment.** etomo patch tracking (`ts_etomo_patches --patch_size 2000 --do_axis_search` +
   autolevel) vs AreTomo. Tilt angles and axis are never refined, so their errors carry through; roadmap 22
   recorded AreTomo tilt offsets of −5…−15° at CC ≤ 0.013 on 412 data.
7. **Unexplored, low priority.** `apply_ctf` (authors: no benefit; requires good CTF fits, see
   `docs/known_bugs.md` #1), architecture variants, shift magnitudes, `spline` mode. No ablations exist for any.

Wiring needed in crboost for 1–5: a custom schedule field (list of `{downsample, alignment}`), exposing the LR
milestones, and a per-TS Z box in the stamp step. `infer` needs a second job type or mode.

---

## 4. Judging a run from the tomograms alone

### 4.1 Principle
Only a **cross-validated** comparison is independent of the refiner: data not used to build a reconstruction
checks it. Scores the refiner itself optimises (the CNN, sharpness/L2 objectives, projection-matching CC at
AreTomo's binning) are circular and excluded.

Warp's `--halfmap_frames` halves (what `tsReconstruct` writes today) **cannot see alignment**: both halves use
the same per-tilt geometry, so a misplaced tilt is misplaced identically in both. Use them only as a negative
control.

### 4.2 Metric 1 (primary): tilt-split half-tomogram FSC (FSCe/o)
Cardone et al. 2005, JSB 151:117; used to benchmark marker-free alignment by Han et al. 2019, Bioinformatics 35:i249.

- **Reconstruct**: `WarpTools ts_reconstruct --halfmap_tilts` writes two half-tomograms from alternating tilts
  (`i % 2` over Warp's tilt index — check it is angle rank, not acquisition order). 6.2 Å, no `--deconv`, same
  tilt set (intersection of `UseTilt`) and same halves for both alignments.
- **Mask**: slab mask per alignment from the full tomogram (Z variance profile on a coarse XY grid → top and bottom
  surfaces → cosine edge ~8 px). Crop XY to the common field of view (~central 80 %).
- **Score**: FSC in XY tiles (e.g. 256² × slab). Per-TS scalar = median over tiles of band-integrated
  ln(SSNR_B / SSNR_A), SSNR = FSC / (1 − FSC), band 1/100–1/30 Å⁻¹ (set from a pilot), shells where both FSCs
  exceed 3/√N.
- **Directional split**: tilt planes intersect along the tilt axis, so above k_c ≈ 1/(D·Δθ) (≈ 1/100 Å⁻¹ for a
  200 nm lamella at 3°) the halves share support only near the axis. Report (a) an axial cylinder |k⊥| < k_c/2 up
  to Nyquist (tests shifts **along** the axis) and (b) full shells below k_c (tests shifts **across** it).
- **Cost**: the two halves are one extra `ts_reconstruct` per TS per alignment.

### 4.3 Metric 2 (per-tilt diagnostic): K-fold leave-out reprojection FRC
Cardone 2005 (NLOO); Unser et al. 2005, JSB 149:243 (SSNR from reprojections).

- 4 folds by angle rank mod 4 (held-out tilts keep neighbours at ±3/6/9°). For fold f reconstruct V¬f (all but f)
  and V_f (f only). For each tilt k in f: rotate both about the tilt axis by −θk, sum over the slab, FRC in a stripe
  along the axis and in a low-frequency disk, plus the sub-pixel CC offset as a per-tilt residual.
- Output: median ΔFRC vs tilt angle; a TS × tilt heatmap. Run on ~50 TS (8 reconstructions per TS per alignment).

### 4.4 What the metrics can and cannot see
Expected attenuation for independent per-tilt errors σ is ≈ exp(−4π²σ²k²) *(derivation, not from a paper)*:

| per-tilt error (6.2 Å px) | along axis, at 1/40 Å⁻¹ | across axis, usable band ≤ 1/100 Å⁻¹ |
|---|---|---|
| 0.5 px | 0.79 | not detectable |
| 1 px | 0.39 | −5…−14 %, needs aggregation over many TS |
| 2 px | 0.02 | detectable |
| 5 px | — | 0.02 at 1/100 |

**Blind spot:** without particles or fiducials, errors *across* the tilt axis are testable only below
k_c ≈ 1/(D·Δθ); thicker lamellae shrink it. High-frequency gains across the axis, which is where local refinement
helps STA, cannot be validated particle-free. Template matching statistics are a partial substitute where a
template exists (the paper reports detections 14.4k → 19.6k): neither aligner optimises them.

### 4.5 Controls (all required before trusting a Δ)
- **Null:** baseline XML + a pure gauge change (Δz = 40 vox, i.e. x_k += Δz·sinθ_k) must leave every metric
  unchanged. Tests masking and FOV handling.
- **Positive / calibration:** inject Gaussian per-tilt shifts σ = 0.5, 1, 2, 3, 5 px into the baseline XMLs (x and y
  separately, gauge component projected out). Metric vs σ converts any Δ into "equivalent Å rms".
- **Negative:** frame-split FSC must be ≈ equal between arms.
- **Metric noise floor:** the metric on two disjoint XY halves of the same lamella.
- **Test–retest:** repeat one arm with a different `seed` on 20–30 TS; 1.96·√2·SD_within is the smallest
  detectable per-TS difference.

### 4.6 Statistics
Unit = tilt series; paired Δ = M_B − M_A. Wilcoxon signed-rank; median Δ with bootstrap 95 % CI (cluster
bootstrap by session/grid); win rate; Bland–Altman plot of Δ vs mean; A-vs-B scatter coloured by thickness. Fix
halves, band and mask before looking at results.

---

## 5. First experiment (proposal)

~30 TS from one session of a large lamella dataset. Arms, all from the same `aligntiltsWarp` job:

| arm | schedule | budget | box |
|---|---|---|---|
| A | baseline (no missAlign) | — | — |
| B | fast (the 2026-09-29 run's settings) | 15 × 250 | full |
| C | paper-like ladder (§3.4) with 5× `[3,3]` | 30 × 1000 | full |
| D | as C | 30 × 1000 | tight Z |

Metric 1 on all arms, metric 2 on 10 TS per arm, all §4.5 controls. C vs B isolates schedule + budget; D vs C
isolates the box. If neither C nor D beats the test–retest floor on metric 1, the particle-free route has reached
its limit for this data and the remaining question needs a particle dataset (a public one such as EMPIAR-10499).

## Unverified
Line-level source citations (cited by function); PyPI 0.2.0 vs `main` differences; whether `--prepare-stacks`
updates the XML; the paper's 64³ vs the code's `patch_size`; the `--halfmap_tilts` split order; whether downstream
Warp jobs read `VolumeDimensionsAngstrom` from the XML; the sensitivity table (derived, not published); GPU cost of
§5 (rough: ~40 s per 6.2 Å reconstruction).
