# Roadmap 24 — miss-alignment: paper-grade regimes, and a verdict that doesn't rely on the eye

**Status:** stages 1 and 3 **code-complete, NOT RUN** (2026-09-30, branch `missalignmnet`); stage 0 and 2 are
user runs; stage 4 not started. Companion: `docs/reports/miss-alignment/settings-and-evaluation.md`
(every tool setting, the paper's guidance, the metrics survey; referred to below as *the settings page*). Builds on
roadmap 21 (integration; stages 1–3 code-complete) and absorbs its stage 4 (grid visibility) and its stage 5
"visibly better" criterion. Stage 6 of roadmap 21 (Warp particle export) becomes a dependency of stage 4 here.

**Progress 2026-09-30.** Sources checked against the container's exact versions (miss-alignment 0.2.0, warpylib
1.0.1, both from PyPI) and WarpTools `main`; nothing below has run.

*Stage 1 — landed:*
- Schedules are text, one entry per macro-iteration: `30A anchoring; 20A anchoring x2; 10A global; 10A [3,3] x5`
  (`<Å>A` or `ds<N>`, `xN` repeats). Converted per stack, `ds = max(1, round(Å / stack Å/px))`; the driver logs every
  iteration's effective Å, downsample and patch field of view, and a note past ±15 % (PAPER on 6.2 Å stacks: the
  10 Å entries run at 12.4 Å, +24 %). Presets fast/default/thorough unchanged; `paper` new; `custom` reads
  `custom_schedule`.
- `lr_milestones` (`5,15`), `seed` (45132), `training_gpus`, `reconstruction_devices` (e.g. `1,1,2,2`, for stage 2).
  Several training GPUs run DDP: Lightning's sampler splits each epoch, so each GPU runs `steps_per_epoch / n` steps,
  and the tool multiplies the LR by n; `NCCL_P2P_DISABLE=1` is set (the tool's own troubleshooting fix for PCIe-switch
  hangs on A100-class nodes). The pool constraint is `pool_size // (training GPUs × workers) ≥ 2·batch`.
- `z_box` full | auto | Å — **re-scoped from §3.3**:
  - *auto* reconstructs each series coarsely (~25 Å, warpylib `reconstruct_full`, CPU, inside the stamp step) instead
    of reading a baseline tsReconstruct through an optional `reference_tomograms` slot. That slot would add an afterok
    edge from a tsReconstruct which, in the A/B layout, sits downstream of the same missAlign (a cycle), and it cannot
    tell which alignment a tomogram came from; the coarse reconstruction uses exactly the geometry being refined.
  - The estimator works between the minima flanking the slab, not against the outer 20 % of Z: in Warp tomograms the
    per-slice std rises again towards the Z ends (bullseye job006: ~0.29 at the ends, ~0.245 beside the slab), which
    made the planned background estimate reject every series.
  - The box covers the slab's full extent — out to the flanking minima, at most half a FWHM past the half-maximum
    edges — instead of FWHM + one patch per side: one patch at 12.4 Å is 1190 Å, so that box would have been
    ≥ 4380 Å, more than the 3174 Å full box, and *auto* a no-op for PAPER.
  - Estimator on bullseye job006 (Warp tomograms binned 4×, node port of `lamella_slab.fit_slab`; the in-container
    coarse reconstruction will differ somewhat): 13_2 FWHM 1103 Å → box 1786 Å; 19 1200 → 1786; 28_2 1322 → 2133;
    28_4 480 (one peak, no plateau — likely inclined) → 1037; 30_5 1025 → 1984; 6_2 684 → 1339; full box 3174 Å.
    35_2 (peak only 16 % above flanks that never return to a common background, material likely continuing at low
    contrast) → no clear slab → full box: the acceptance threshold is 20 % (the clean six measured 30–72 %).
  - Box restore: `ts_ctf`, `ts_reconstruct` and `ts_export_particles` set the volume box from their settings
    (`CTFTiltseries.cs`, `TiltSeries.ReconstructFull.cs:36`, `ExportParticlesTiltseries.cs:978`) and never read the
    XML's; the driver still writes the full box back to the output XMLs, `iterN/` keep the fitted one.
- `prepare_stacks_apix` un-refused. warpylib resolves each tilt to `<movie dir>/average/<stem>.mrc` (crboost XMLs carry
  absolute `MoviePath`s) and takes the original pixel size from the first average's header. `stack_tilts` opens the
  `.st`, the `.rawtlt` and the thumbnails for writing, so with it set nothing is linked into `tiltstack/` (a symlink
  would be written through into the alignment job). Before GPU time the driver checks every average exists and the
  first covers the aligned stack's extent (±1 %); the flag goes to a fresh stage only, a resume reads the built stacks.
- Resume guard: `run_manifest.json` (input star + mtime, TS list, resolved schedule, prepare_stacks_apix, z_box). A
  resume refuses if any of these changed for the finished iterations; appending iterations is allowed, so a finished
  run can be extended.
- `missalign_changes.json` + one log line per TS (rigid-removed rms/max, gauge rms, tilts over 2 px of the input
  stacks, local-warp dims/rms/max). **Correction to the first-run readout (§1, the settings page §1):** those numbers
  came from fitting a·cos θ + b·sin θ + c to each image component separately — six parameters, where a rigid volume
  translation has three (across the axis tx·cos θ − tz·sin θ, along it a constant ty). The extra three (a constant
  shift across the axis, which moves the tilt axis itself, and cos/sin terms along it) are real changes and were
  being removed as "gauge". The fit now projects each shift onto the tilt-axis frame from the XML's AxisAngle and
  removes only (tx, ty, tz). job007 iter0→iter2 (node port, reproduced independently by the review): 13_2 11.8,
  19 4.0, 28_2 34.9 (max 199.5), 28_4 35.1, 30_5 14.9, 35_2 22.8, 6_2 4.4 Å rms (was 8.6, 3.0, 29.5, 16.0, 7.8, 20.2,
  3.8); the Z gauge reaches ±250–290 Å on 19, 30_5, 6_2.
- Walltime: per iteration, training minutes ÷ training GPUs + 5 min × n_TS × max(1, (6.2 Å / effective Å)³) ÷ GPUs;
  capped at the chosen QOS's MaxWall.
- **QOS** (needed for any long run): `SlurmConfig.qos`, a QOS row in the SLURM tab, `#SBATCH --qos=` injected by
  `write_sbatch_script` (also `job_resource_profiles.<type>.qos` in conf.yaml). Until now no crboost job could leave
  the default 8 h QOS, and one full-budget PAPER iteration is ~11 h on an A100 (29 h by the conservative estimate).
- Job tab: a summary above the fields (per iteration: entry, downsample, Å, patch field of view, rounding notes) and
  the settings that defeat themselves: epoch cap ≤ last milestone, unreadable schedule / milestones / z_box, more
  training GPUs than GPUs, one iteration longer than a run's wall-time.
- Presets are regimes: choosing one sets `steps_per_epoch`, `max_epochs_per_iteration`, `lr_milestones` and `z_box`
  (`PRESET_SETTINGS`: fast/default 250 × 30, thorough/paper 1000 × 30, milestones 5,15, paper `z_box auto`); the
  fields stay editable. *custom* keeps them and starts an empty `custom_schedule` from the preset left. The summary and
  the SLURM section refresh each other after every edit (QOS / time / GRES → warnings; parameters → time limit and
  GRES), a focus passing through a SLURM field no longer pins its value as an override, and the GPU checks and the
  walltime count a GRES override when there is one (the driver reads SLURM's count).
- Local deformations (roadmap 21 stage 4): subtomoExtraction's tab shows an amber chip when its tomograms descend
  (recorded input paths) from a missAlign run whose `missalign_changes.json` lists local warps.
- tsReconstruct `halfmap_tilts`: Warp splits by `i % 2` over the XML's tilt order (`TiltSeries.ReconstructFull.cs:244`),
  angle-sorted in crboost XMLs; it writes the same even/odd folders as `halfmap_frames` and refuses both
  (`ReconstructTiltseries.cs:78`), so the driver refuses the pair before Warp.
- Several arms in one project: an IO-tab override chosen before its producer was deployed (`…/pending_<id>`) now
  resolves to that instance afterwards (the latent bug noted in roadmap 21). With several missAlign jobs refining one
  alignment, a tsCtf follows the most recently deployed one — the rule the resolver already applies to duplicate
  producers (highest job number) — and the server logs which. Deploy allocates the missAlign before its tsCtf, so an
  arm's trio run together pairs up by itself. Side effect: a finished branch's tsCtf now *displays* the newest
  missAlign as its source (its recorded paths are unchanged); re-running it would rebind it.

*Stage 3 — landed:* `services/analysis/tilt_split_fsc.py` + `scripts/align_qc.py`:
- `changes BEFORE AFTER` — the rigid-removed table above for any two XML folders.
- `controls --processing <tsCtf warp_tiltseries> --settings <aligntiltsWarp warp_tiltseries.settings> --angpix 6.2
  --out DIR` — XML sets `baseline`, `gauge_z40`, `along_s{σ}`, `across_s{σ}` (σ = 0.5, 1, 2, 3, 5 px, rigid part
  projected out, injected rms recorded) and `reconstruct_array.sh`, an sbatch array of `ts_reconstruct
  --halfmap_tilts` (no deconv) in the Warp container.
- `score --arm A=<tsReconstruct job> --arm C=… --reference A --out DIR`, or `score --controls DIR --out DIR` — per
  256² tile inside the lamella (an inclined slab from a 4×4 grid of profile fits), FSC in full shells and in a cylinder
  of radius k_c/2 around the tilt axis (k_c = 1 / (thickness · tilt step)); per TS the median over tiles of the mean
  ln(SSNR_arm / SSNR_ref) over 1/100–1/30 Å⁻¹ (shells) and 1/40–1/15 Å⁻¹ (axial), bins where both FSCs clear 3/√n.
  Writes `per_ts.csv`, `summary.csv` (median, bootstrap CI, win rate, Wilcoxon p), `summary.md`, `fsc_curves.png`.
  The metric-noise-floor control (two disjoint XY halves of one lamella) is not built; D vs D′ serves as the floor.

*Review (read-only, against the container sources):* three defects fixed before any run — the pre-flight ingest
looked for staged stacks, which do not exist yet with prepare_stacks (the stack pixel size is now passed in); the QC
controls used a mirrored image frame for the tilt axis (along is (−sin a, cos a), across (cos a, sin a)); and the rigid
fit (the correction above). Also: the FSC cache is keyed by tile and tilt step, the controls array binds the project,
the changes report is written atomically, the local-warp chip follows denoised tomograms too.

*Not started:* stage 4 (Warp export path; needs a `ts_export_particles` job and the dev36 → RELION 5 star converter,
roadmap 21 stage 6).

**Queueing the arms (stage 5 layout, in the project that holds the baseline chain).**
1. Baseline halves: a tsReconstruct on the baseline tsCtf with `halfmap_frames 0`, `halfmap_tilts 1`, `deconv 0`,
   `rescale_angpixs 6.2` (arm A).
2. Per arm: add a missAlign, a tsCtf and a tsReconstruct (same reconstruct settings as 1) and run the three together,
   one arm per Run: the new tsCtf follows the newest missAlign and the new tsReconstruct the newest tsCtf. To launch
   several arms in one Run, point each arm's tsCtf (**both** `input_star` and `input_processing`) and tsReconstruct at
   its own producers in the IO tabs first.
3. missAlign settings — B: custom `30A anchoring; 20A anchoring x2; 10A global`; C: paper, then z_box back to full
   (the preset sets auto); D: paper, z_box auto; D′: D with another seed; E (only if D beats A): custom `30A anchoring; 20A anchoring x2; 10A global;
   3.1A [3,3] x5`, prepare_stacks_apix 3.1, patch 128, z_box auto. All: steps_per_epoch 1000, epochs 30; QOS `g_long`
   in the SLURM tab; e.g. num_gpus 2 (1 trains, 1 reconstructs) with cpus_per_task ≥ 8.
4. Score: `python scripts/align_qc.py score --arm A=External/jobAAA --arm C=External/jobCCC … --reference A
   --out docs/reports/miss-alignment/post_handedness_fix/tilt_split`.

## 1. Where it stands

The first run (`bullseye_artifact_two`, 7 TS, fast preset) changed per-tilt shifts by 3–30 Å rms after removing a
rigid volume shift; the refined tomograms looked no better. That run was global-only, trained 3,750 steps per
iteration (upstream 30,000, with the LR cut and early stopping never engaging at a 15-epoch cap), scored a 3174 Å
tall box around a ~200 nm lamella, and trained on 7 series. The paper's gains come from `[3,3]` local iterations on
10 Å stacks (EMPIAR-10499 ribosomes: AreTomo3 12.3 Å → missAlign global 9.3 Å → `[3,3]` 5.1 Å). Nothing has been
tested in that regime yet.

Test data: `post_handedness_fix` — 20 lamella tilt-series (412 samples, EER, 1.55 Å/px, 753 tilts, ~39 per TS),
frame averages on disk at full resolution (`External/job002/warp_frameseries/average/`, 4096², float16), ribosomes
in the cytoplasm. The project itself was processed with May-era code (CTF band 30:6, tilt-filter verdict never
applied, blank tilts in Position_34_5 — roadmap 22), so it serves as raw data only.

## 2. Before → After

**Before:** one preset family keyed to downsample factors of whatever stack exists, a training budget that
silently disables the LR schedule, the full tomogram box, no local iterations in the run that was judged, and
"does it look better" as the only verdict.

**After:** schedules stated in Å and converted per stack; the paper's regime available as a preset; the Z box
fitted to the lamella; stacks at any pixel size; every run reports what it changed (rigid-removed shifts, grid
magnitudes); and two independent verdicts on the same 20 series — ribosome STA gold-standard FSC, and a
particle-free tilt-split FSC calibrated against it, so the particle-free one can be trusted on datasets without
particles.

## 3. Design

### 3.1 Schedules in Å
- A schedule entry is `{angstrom: 20, alignment: anchoring}` (or `downsample: N` for the existing presets). The
  driver converts with `ds = max(1, round(angstrom / stack_apix))` and logs the effective Å per iteration; a
  rounding that moves the pixel size by > 15 % is printed as a warning, not silently absorbed.
- New preset **PAPER**: 30 Å anchoring → 20 Å anchoring ×2 → 10 Å global → 10 Å `[3,3]` ×5 (the paper's lamella /
  EMPIAR-10499 ladder). On 6.2 Å stacks: ds5 (31), ds3 (18.6) ×2, ds2 (12.4), ds2 `[3,3]` ×5.
- New preset **CUSTOM** with a text field holding the list (validated: `alignment` ∈ anchoring, global, spline,
  `[Nx,Ny]`, `[X,Y,Z,T]`; exactly one of `angstrom`/`downsample`).
- FAST/DEFAULT/THOROUGH keep their meaning (existing projects store the enum). The job default stays DEFAULT until
  stage 5 decides.
- `patch_size` gets an Å hint in the tab (e.g. "96 px × 12.4 Å = 1190 Å field of view").

### 3.2 Training budget and reproducibility
- Expose `lr_milestones` (default `5,15`) and `seed` (default 45132). Warn in the tab and the log when
  `max_epochs_per_iteration ≤ max(milestones)`: the second LR cut and early stopping never happen.
- `steps_per_epoch` default stays until stage 2 has measured step times.
- Allow more than one training GPU (`training_gpus`, default 1; upstream multiplies the LR by the count). Today
  GPU 0 always trains and the rest reconstruct.

### 3.3 The Z box
- New `z_box`: `full` (default, today's behaviour) | `auto` | a value in Å.
- `auto` reads a baseline reconstruction of the same series (optional input slot `reference_tomograms`, preferred
  source tsReconstruct). Per TS: 4×-binned volume (reuse `services/visualization/recon_register._binned`), per-slice
  standard deviation over the central 60 % of XY, background = median of the outer 20 % of Z, slab = slices above
  background + ½·(peak − background). Box Z = 2·(|centre offset| + thickness/2) + 2·margin (margin one patch at the
  finest Å of the schedule), because the box stays centred on the volume centre. No clear slab → that TS keeps the
  full box and the log says so.
- After the tool finishes, restore the full `VolumeDimensionsAngstrom` in the output XMLs (downstream Warp jobs
  then see what they always saw). Refuse `z_box ≠ full` together with a `[X,Y,Z,T]` entry: volume-warp grids are
  defined on the box.
- Verify first whether Warp's `ts_ctf`/`ts_reconstruct` read the XML's box or the settings file; the restore is
  harmless either way.

### 3.4 Stack pixel size (the binning question)
Decision: **do not feed 1.55 Å stacks; make the stack pixel size a parameter and test 3.1 Å as one arm.**
- The paper aligns on 10 Å stacks and reports ~1.6 Å per-tilt error (0.16 px) on SHREC; a finer stack is not what
  limits precision. Our 6.2 Å stacks are already finer than anything in the paper.
- At 1.55 Å a 96 px patch is 149 Å — smaller than a ribosome — so any useful schedule would downsample ≥ 4, which
  reproduces the 6.2 Å stack from a 16× larger file (4096² × ~39 tilts ≈ 1.3 GB per TS in float16, held per
  reconstruction worker).
- At ~3 e/Å² per tilt, the single-tilt signal the CNN could use beyond ~6 Å is buried in noise.
- 3.1 Å is the one finer step worth measuring: final `[3,3]` rounds at ds1 = 3.1 Å with patch 128 (397 Å FOV).
  That is arm E in stage 5, run only if arm D shows a gain.

Mechanics: `prepare_stacks_apix > 0` is refused today because the XML was believed to lack movie paths. The
aligntiltsWarp XMLs do carry `<MoviePath>` pointing at `warp_frameseries/<movie>`, and the averages exist, so
`--prepare-stacks` should work. Required change: when it is set, stage `tiltstack/<ts>/` **without** symlinking the
`.st` (the tool overwrites it — through a symlink that would clobber the upstream alignment job), and stamp the
image extent from the upstream stack's cell (Å, scale-invariant).

### 3.5 What every run reports
Per TS, written to `missalign_changes.json` and the log (absorbs roadmap 21 stage 4):
- per-tilt shift change vs the input alignment after removing a rigid volume translation (across the tilt axis
  tx·cos θ − tz·sin θ, along it a constant ty, in the axis frame given by AxisAngle): rms, max, and the tilts above
  2 px at the tomogram pixel size;
- the raw gauge part (rms), so a big number is never mistaken for a big change;
- `GridMovementX/Y` magnitude (rms / max Å) when a grid iteration ran — shown on downstream RELION particle jobs as
  "local deformations present, RELION extraction ignores them".
The fit is a pure function over `(angles, axis angles, Δx, Δy)`; the `bullseye_artifact_two` numbers (see the
progress notes: 19 4.0, 6_2 4.4, 13_2 11.8, 30_5 14.9, 35_2 22.8, 28_2 34.9, 28_4 35.1 Å rms) are its regression
check.

### 3.6 Particle-free QC: tilt-split FSC (settings page §4)
- tsReconstruct gains `halfmap_tilts` (→ `--halfmap_tilts`). Check whether it and `--halfmap_frames` write to the
  same `even/`/`odd/` dirs; if so they are mutually exclusive in the job params. Check the split is by angle rank.
- `services/analysis/tilt_split_fsc.py` (pure numpy/scipy) + `scripts/align_qc.py` comparing N reconstruct jobs of
  the same series: slab mask from the full tomogram, common FOV crop, FSC in XY tiles, per-TS scalar = median
  band-integrated ln(SSNR_B/SSNR_A) over 1/100–1/30 Å⁻¹, plus the axial-cylinder variant (tests shifts along the
  tilt axis to Nyquist). Output: CSV + plots into `docs/reports/miss-alignment/<experiment>/`.
- Controls generator (`scripts/align_qc.py --controls`): copies of an alignment's XMLs with (a) a pure gauge change
  (Δz = 40 vox → every metric must be unchanged), (b) Gaussian per-tilt shifts σ = 0.5, 1, 2, 3, 5 px, x and y
  separately → calibration curve. Reconstructed by a generated sbatch array in the Warp container, outside the
  pipeline.
- Negative control: the existing frame-split halves must score equal across arms.
- Leave-out reprojection FRC (per-tilt diagnostic) is deferred; build it only if the tilt-split FSC cannot localise
  a difference.

### 3.7 Ribosome STA — the definitive verdict on this dataset
- Species "80S ribosome" with a species-matched EMDB map if one exists for the 412 organism, else any eukaryotic
  80S, low-passed to 30 Å; template matching on arm A (baseline) tomograms; candidates cleaned once.
- The **same** particles in every arm: coordinates mapped into each arm's volume frame by the per-TS rigid
  translation from `recon_register` (the Journey's plane matching already computes it).
- Extraction through Warp `ts_export_particles` for **all** arms (it applies `GridMovement`; RELION extraction does
  not, roadmap 21 §3) → the dev36 → RELION 5 star converter (memory: legacy embedded-matrix star → silent all-zero
  reconstruct) → RELION Refine3D with one fixed protocol: same initial reference (arm A reconstruction, low-passed
  to 40 Å), same mask, same parameters, **no Polish, no CtfRefine** (both re-estimate per-tilt geometry and erase
  the difference). Two random half-set seeds per arm for the noise floor.
- Report FSC curves, FSC 0.143, and a resolution-vs-particle-count (B-factor) plot per arm.

## 4. Stages

### Stage 0 — Data (user-run)
- New project from `post_handedness_fix` frames + mdocs with current code: CTF band 30:10 (new default), tilt
  filter committed **before** alignment, Position_34_5's five blank tilts (+56.9…+68.9°) marked bad (roadmap 22 not
  built) or the series dropped. Baseline chain through tsReconstruct.
- *Success:* 20 (or 19) series reconstructed; every series' per-tilt defocus within ±0.5 µm of its median (awk
  check in `docs/known_bugs.md` #1); no rays in 34_5.

### Stage 1 — Driver regimes (§3.1–3.5)
- Å schedules + PAPER/CUSTOM presets; `lr_milestones`, `seed`, `training_gpus`; `z_box` with `auto`;
  `prepare_stacks_apix` un-refused with real (non-symlinked) stacks; `missalign_changes.json`; walltime estimator
  reads the schedule's effective pixel sizes.
- *Success:* a 1-iteration, 2-epoch smoke run each of PAPER, CUSTOM with a `[3,3]` entry, `z_box auto`, and
  `prepare_stacks_apix 3.1` on one series completes; the log shows effective Å per iteration, the fitted Z box per
  TS, and the changes JSON; the upstream alignment job's stacks are byte-identical afterwards.

### Stage 2 — Throughput and walltime calibration
- Measured so far: 1.34 s/step on A100 (1 training + 1 reconstruction GPU, batch 32, patch 96 at 6.2 Å). At the
  upstream budget (30 × 1000 steps) that is ~11 h per macro-iteration, ~4 days for PAPER's 9 iterations before early
  stopping. Short runs (1 iteration, 2 × 200 steps) varying training GPUs (1/2/4), reconstruction workers
  (`0,0,1,1` style), and patch 80 vs 96 at 12.4 Å; find which resource bounds the step.
- *Success:* a table of s/step per configuration in this doc; `_WALLTIME_PER_STEP_SEC` replaced by the measured
  value for the chosen profile; a PAPER run on 20 TS fits the `g_long` QOS with per-iteration resume.

### Stage 3 — Particle-free QC tooling (§3.6)
- *Success:* on `bullseye_artifact_two` job006 vs job009 the script produces per-TS scores; the gauge-null control
  moves the score by less than the metric's own XY-half noise floor; injected σ = 2 px along the axis is detected on
  every series.

### Stage 4 — Ribosome STA path (§3.7; includes roadmap 21 stage 6)
- *Success:* arm A (baseline) alone reaches a sub-15 Å gold-standard ribosome map through the Warp-export path;
  the same particle list maps into a second arm with ≥ 95 % of particles inside the volume.

### Stage 5 — The experiment
Arms on the 20 series, all refining the same aligntiltsWarp job, each followed by tsCtf + tsReconstruct
(`halfmap_tilts`):

| arm | schedule | budget | Z box | stack |
|---|---|---|---|---|
| A | none (baseline) | — | — | 6.2 Å |
| B | PAPER without the `[3,3]` rounds | stage-2 budget | full | 6.2 Å |
| C | PAPER | stage-2 budget | full | 6.2 Å |
| D | PAPER | stage-2 budget | auto | 6.2 Å |
| D′ | as D, different `seed` | | | (test–retest) |
| E | as D, final `[3,3]` rounds at 3.1 Å, patch 128 | | auto | 3.1 Å — only if D > A |

- Checks before scoring: per-tilt defocus agrees between arms within 0.05 µm; `missalign_changes.json` per arm.
- Verdicts: STA FSC 0.143 and curves (A vs B vs C vs D vs E; D vs D′ = noise floor); tilt-split FSC per TS
  (Wilcoxon, median Δ with bootstrap CI, win rate).
- *Success:* a written result in `docs/reports/miss-alignment/post_handedness_fix/`: which of local iterations,
  budget, Z box and stack size move the STA resolution beyond the D-vs-D′ spread; whether the particle-free score
  ranks the arms the same way as STA (go / no-go for using it on particle-free datasets); the job default set from
  the winner.

### Stage 6 — Scale-out to hundreds of series (only after a stage-5 "go")
- `infer` mode: a missAlign job that takes a previous missAlign job's `iterN/model.ckpt` set and aligns other series
  without training (upstream `miss-alignment infer`; uses all visible GPUs). Train on 30–60 representative series of
  a session, infer the rest; the particle-free score on a held-out 20 checks the subset model didn't lose accuracy.
- Optional: upstream cluster farming of the alignment phase (`--n-cluster-workers`).
- *Success:* one particle-free dataset (100+ TS) refined via train-on-subset + infer, scored against its baseline
  with the stage-3 tooling.

## 5. Out of scope, noted
- Starting alignment: the paper starts from etomo patch tracking; crboost has `AlignmentMethod.IMOD`. Worth an
  arm F only if stage 5 shows missAlign gains that depend on the start.
- `apply_ctf`, architecture variants, shift-generation magnitudes, `spline`, `[X,Y,Z,T]`: no paper ablations;
  reachable through CUSTOM once there is a metric to judge them.
- `--preprocess`: replaces the AreTomo shifts with cross-correlation shifts; not wanted on top of AreTomo.

## 6. Runtime checklist
1. Stage 0 defocus awk check on all series; 34_5 central XZ slice free of rays.
2. `python scripts/align_qc.py changes <bullseye job007>/warp_tiltseries/iter0 <…>/iter2` prints 13_2 11.8, 19 4.0,
   28_2 34.9, 28_4 35.1, 30_5 14.9, 35_2 22.8, 6_2 4.4 Å rms (the regression check of the rigid-removal fit).
3. Stage 1 smoke runs: effective-Å log lines, Z box per TS (`zbox/fits.json` near the node-port boxes above; the
   coarse reconstructions are in `zbox/*.mrc`), upstream stack checksums unchanged, `run_submit.script` carries
   `#SBATCH --qos=`, the job tab summary renders for every preset, a resume after a cancel continues and a resume with
   a changed schedule prefix refuses. Job tab: switching presets rewrites the four regime fields in place; a QOS,
   time or GRES edit in the SLURM section updates the warnings at once; a parameter edit updates the SLURM time limit
   and GRES; tabbing through the SLURM fields leaves no override behind (no "Reset to profile" button appears).
4. tsReconstruct with `halfmap_tilts 1`: `reconstruction/even|odd/` written; with both halfmap switches on, the job
   refuses before Warp.
5. Stage 3 controls on `bullseye_artifact_two` before any stage-5 scoring: the gauge set scores ≈ 0, along/across
   scores fall with σ.
6. Stage 5: D vs D′ spread recorded before comparing arms.

**Modern-Python weave-in:** the schedule as a frozen dataclass `ScheduleEntry(alignment, angstrom | downsample)`
with a pure `effective_downsample(stack_apix)`; the rigid-removal fit and the slab estimator as pure functions over
arrays (`services/analysis/`). Their regression checks are runtime items 2 and 3 above rather than unit tests:
`tests/` is the install self-test, and both need the project data.
