# miss-alignment on AGG412 Grid2 — preliminary results (2026-10-17)

Project `/groups/klumpe/crboost_data/AGG412_20260311_MyrGFP_Grid2`. Two branches, identical from the frames up to
the alignment: **baseline** = Warp alignment as usual; **missAlign** = the same alignment refined by miss-alignment
(PAPER schedule). Everything downstream ran with the same settings in both branches.

**Short version:** miss-alignment changed most series by a few pixels or more, and template matching on the
refined tomograms finds stronger ribosome peaks in 45 of 47 series (+8 % median) and 20 % more picks at the
same false-positive cutoff. The subtomogram-averaging verdict (gold-standard FSC) is still pending.

## Data

| | |
|---|---|
| Tilt series | 48 refined (Position_10_4 left out: a single tilt survived upstream) |
| Acquisition | EER, 4096², 1.55 Å/px, up to 41 tilts per series after tilt filtering |
| Stacks miss-alignment trained and aligned on | 6.2 Å/px |
| Tomograms (both branches) | 6.2 Å/px, Warp `ts_reconstruct` |

## What it took to hook miss-alignment up

- **Container:** miss-alignment 0.2.0 + torch 2.8 / CUDA 12.9 (runtime libraries only); Warp moved to 2.0.0dev41 for
  every Warp job (fixes the dev36 particle export for RELION 5).
- **Warp-native wiring:** miss-alignment rewrites the Warp XMLs; a second tsCtf + tsReconstruct read them directly,
  no format conversion. The pipeline routes "refined" producers automatically to the next consumer.
- **Bugs that made the first attempts look like no-ops:** image dimensions were stamped from the Warp settings
  default (7676×7420) instead of the real 4096² stack, putting the image centre ~2700 Å off; a silenced progress bar
  tripped the 45-min idle watchdog; series with a single tilt crashed the run (now left out and logged).
- **Paper-grade regime:** schedules written in Å and converted to whole downsampling factors; a PAPER preset
  (1000 steps × up to 30 epochs per iteration, LR milestones 5/15, early stopping); the Z box fitted per series to
  the lamella instead of the full 3174 Å volume.
- **Throughput:** training speed is set by the CPU workers that build the training cubes, not by GPUs. The first
  PAPER attempt (3 A100 trainers, 1 builder) ran 6.65 s/step → ~19 h per iteration, ~7 days total. Balancing
  builders and loaders over the CPUs gives ~3 steps/s on 1 trainer (≈ 98 vs 14 cubes/s, 2 GPUs + 16 CPUs) → about 7×
  faster. Training runs uncompiled (a Triton cache race killed two compiled runs).
- **Getting the local warps to the particles:** RELION extraction reads only rigid per-tilt geometry, so the 3×3
  local warps would never reach an average. A Warp Export job (`ts_export_particles --2d`, RELION 5 optimisation
  set) carries them; both branches use it.
- **dev41 queue collision:** concurrent Warp reconstruction tasks sharing one output folder locked each other out
  (8+ of 48 series failed the first time); each task now gets its own queue folder.

## The miss-alignment run

| | |
|---|---|
| Schedule | 30 Å anchoring → 20 Å anchoring ×2 → 10 Å global → 10 Å local 3×3 ×5 (9 iterations) |
| Effective resolution of the 10 Å rounds | 12.4 Å (the nearest whole downsampling of the 6.2 Å stacks) |
| Hardware | 1 node, 2 × A100 (1 training, 1 + CPU workers building cubes), 16 CPUs, 64 GB |
| Wall time | **≈ 20.7 h** for all 9 iterations (training ≈ 2 h 45 per iteration at full speed) |
| Early stopping | every iteration stopped at epoch 16–25 of 30; final training loss 0.027–0.032 |
| Z box | lamella found in 42/48 series (577–2150 Å thick, median 1503 Å); 6 series used the full 3174 Å |

## What it changed (per-tilt shifts, rigid whole-volume shift removed)

| | |
|---|---|
| Median over 48 series | 19.6 Å rms (≈ 3 px at 6.2 Å) |
| Barely changed (< 10 Å rms) | 15 series |
| Changed > 30 Å rms | 12 series; largest Position_6 122 Å, Position_9_2 70 Å (+ a 193 Å rigid shift), Position_1_5 62 Å |
| Local 3×3 warps | 8–44 Å rms per series |

## Downstream comparison (same template, mask, angles and cutoffs in both branches)

Template matching: PyTOM, ribosome template from 6XU8 at 6.2 Å, 12° search, C1, band 40–500 Å,
phase-flip CTF, 2×2×1 split. Candidates: Ø 310 Å, cutoff at 1 expected false positive per tomogram.

| | baseline | missAlign |
|---|---|---|
| Candidates | 26,345 | **33,283** |
| Particles after Warp Export (bin 2 → 3.1 Å, box 192) | 26,335 | **33,252** |
| Picks per series, median ratio | — | **+20 %** (up in 43 of 47) |
| Mean of the top-50 TM peaks per series, median ratio | — | **+8 %** (up in 45 of 47) |
| Noise level of the TM score maps | — | identical (ratio 1.000) |

- The cutoff did not loosen (same score-map noise), so the extra picks are peaks that rose above an unchanged
  noise floor.
- The gain tracks how much miss-alignment moved a series: +5 % for the 15 barely-changed series, +13 % for the
  rest (Spearman 0.39). Series it left alone come out identical, an internal control.
- The worst series improved most rather than breaking: Position_6 top-50 score 0.110 → 0.168, Position_9_2 picks
  184 → 1,261.
- Initial reconstructions (RELION `reconstruct_particle`, rigid geometry only, TM orientations) show a ribosome in
  both branches; the missAlign map looks somewhat cleaner but has 26 % more particles, so that is not evidence yet.

Run times per branch (both branches in parallel): template matching ~6–7 h (48 series, 4 at a time), candidates
~50 min, Warp Export 20–45 min, reconstruct_particle ~2–2.5 h.

## Next

1. **Refine 3D in both branches** (RELION gold-standard auto-refine, same reference map, same seed). The first
   attempt was killed for memory (pre-reading 26–33k particles × 41 tilts × 192² needs ~150–200 GB per process);
   re-running without pre-read on A100s with a 5-day limit. For a clean comparison the missAlign branch gets a
   random 26,335-particle subset, so particle count is not a variable.
2. If the FSC gain holds: run PAPER on the other AGG412 projects, then refine and compare the full set.
3. Calibrate a particle-free score (tilt-split half-tomogram FSC) against this result, for datasets without a
   good averaging target.

## What else can be tuned

- **Finer stacks** (3.1 Å/px): the "10 Å" rounds actually ran at 12.4 Å; finer stacks allow true 10 Å or finer
  local rounds, at roughly 4× the cost per cube.
- **Denser local model:** 3×3 is the paper's choice; miss-alignment accepts any N×N grid.
- **Training budget:** early stopping fired at epoch 16–25 of 30, so more epochs per iteration would not help;
  more iterations at the local stage might.
- **CTF inside training** (`apply_ctf`, currently off).
- **Z box:** 6 series had no clear lamella and trained on the full 3174 Å depth, which the authors say degrades
  accuracy.
- **Seed repeat:** a second run with another seed gives the run-to-run spread to judge small differences against.
- **Speed:** a second trainer (needs ~2× the CPUs) or compiled training; for hundreds of series, train on a subset
  and run inference on the rest.
- **After averaging:** RELION Polish / CtfRefine, or Warp's M, on top of either branch.
