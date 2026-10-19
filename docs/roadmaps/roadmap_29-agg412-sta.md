# Roadmap 29 — AGG412: finish the picking, filter, average the ribosome and the 412 VLP, and an A/B for miss-alignment

**Status:** scoped 2026-10-08; nothing built. Stage 0 is a user run with no code change. The campaign's
living record (decisions D#, watch-list W#, run log, analysis scripts) is the git-ignored
`notes/agg412_picking/PLAYBOOK.md`; this roadmap is the plan from the picks onwards. **2026-10-18:** stage 0
done (all eight picking chains green); the miss-alignment A/B ran on MyrGFP_Grid2 (+20 % picks, +8 % top-50
LCC); the rollout to the other seven projects, with its decisions (MA#), parameters and run log, is §6.2. Related: 24
(miss-alignment regimes and evaluation, whose stages 4 and 6 this leans on), 21 (Warp export), 27 (per-TS
catch-up), and `docs/reports/412-capsid/412-sta-refinement-playbook.md` (why the 412 needs local refinement).

## 1. Where it stands (2026-10-08)

Eight projects, 922 tilt series, 867 tomograms (6.2 Å, 1024 × 1024 × 512), each with a 412 and a ribosome
picking chain (TM → candidate extraction → subtomo extraction) submitted 2026-10-05. Candidates are at
FP = 1 (7.1σ for the 412's I1 list, 7.2σ for the ribosome at 12°); σ is the per-tomogram score-map σ from
PyTOM's `job.json`.

| Project (`AGG412_…`) | Tomos | 412 candidates | 412 ≥ 10σ (tomos) | Ribosome candidates | Ribosome ≥ 8σ | Picking state |
|---|---|---|---|---|---|---|
| 20240204_412_Slot6 | 123 | — | — | — | — | both TMs finished but not aggregated; 4 dependents never ran |
| 20251031_412_FCIsolation | 142 | 2,602 | 198 (37) | — | — | 412 done; ribosome TM not aggregated |
| 20251113_412 | 141 | 3,125 | 99 (38) | — | — | 412 done; ribosome TM not aggregated |
| 20260204_412 (pilot) | 23 | 159 | 11 (9) | 15,672 | 10,634 | complete |
| 20260205_412_FCIso_unmilled2 | 162 | 5,738 | 232 (70) | 51,786 | 41,825 | complete |
| 20260311_412_Grid3 | 95 | 1,625 | 41 (26) | — | — | 412 done; ribosome TM not aggregated |
| 20260311_MyrGFP_Grid2 | 48 | 1,163 | 154 (21) | 18,579 | 11,298 | complete |
| SK_20251017_412 | 133 | 3,200 | 194 (54) | — | — | 412 subtomo failed at start; ribosome TM not aggregated |

What the numbers already say:
- **412:** 929 candidates ≥ 10σ in 255 of 744 tomograms. The pilot read put real VLPs at 10.4–16.8σ and the
  noise at ≤ 9.6σ, about one per tomogram, so the set holds several hundred to ~1,000 VLPs — against the 94
  of the July aggregation. MyrGFP_Grid2 is the outlier (137 candidates ≥ 12σ in 21 tomograms): dense VLPs or a
  structured false positive; look at it first. The cap of 50 bound in 195 tomograms, at the 50th pick's 7.1–8.2σ
  in 90 % of them; six hit it with all 50 picks ≥ 9σ (SK Position_11_2 at 16σ, MyrGFP Position_10 and 10_3,
  FCIsolation 18_4, unmilled2 7_4, SK 5): an artefact or a VLP cluster, to look at in V1.
- **Ribosome:** the σ distributions differ by project (≥ 10σ: 39 % in unmilled2, 4–5 % in the pilot and
  MyrGFP), so a cut must be calibrated per project or in σ, never as one LCC value.
- **Disk:** TM score + angle maps are 4.3 GB per tomogram per species, ~7.5 TB over the eight projects
  (sizer, 2026-10-07). Re-extraction needs them (decision 6, §8).

The failures (playbook run log 2026-10-08, W19, W20): six TM supervisors hit the 8 h limit while their arrays
ran on and finished every tomogram (~250 GPU-h of finished TM, not aggregated), so their twelve dependents never
started; one subtomo job raced the NFS cache at start. Nothing needs recomputing.

## 2. The shape of the work

```
 per project (crboost)                                         aggregation project (crboost)    external (RELION 5 / M, Apptainer)
 TM ─► extraction FP=1 ─► subtomo ─► σ subset ─► Class3D ─► class subset ─┐
                                 └─► 412: gallery curation ───────────────┴─► merge ─► Class3D ─► Refine3D ─► PostProcess
                                                                                                  ─► Polish / CtfRefine ─► (M)
```

As agreed for the 412 in July, refinement runs outside crboost from the Apptainer images (412 playbook §1);
each species gets an aggregation project whose `sta/` directory holds the scripts, the way copia's
`tutorial_sta/` does. crboost covers picking, curation, merging, ReconstructParticle and Class3D. The gaps
are small tools (§7).

## 3. Stage 0 — finish the picking (user run, no code)

Press **Run** once per project, all roster jobs selected. `_submit_chain` re-submits a FAILED job into its own
`External/jobNNN` and keeps `.task_status/*.ok`, so each timed-out TM's supervisor finds nothing to submit,
copies `tomograms.star`, and succeeds within minutes; the dependents follow by afterok with the parameters they
already carry (same as the three complete projects, so all eight sets stay comparable).

| Project | Re-submitted in place |
|---|---|
| Slot6 | TMs job007 (412), job008 (ribosome) → extraction job009, job010 → subtomo job011, job012 |
| FCIsolation | ribosome job011 → job013 → job015 |
| 20251113 | ribosome job008 → job010 → job012 |
| Grid3 | ribosome job008 → job010 → job012 |
| SK | ribosome job011 → job013 → job015; 412 subtomo job014 starts at once |

- Don't edit the TM jobs before the Run; the kept results would no longer match their parameters.
- If an extraction or subtomo job dies at start with "not found" on a file that exists, Run again (W20).
- Expected ~1 h per project (unmilled2: ribosome extraction 35 min for 162 tomograms, subtomo 10 min).
- Not in scope: the 55 tilt series without a tomogram (playbook W2, 6 % of 922). Recovering them needs per-TS
  catch-up (roadmap 27 stage 2, not built).
- *Success:* every project's 13 jobs green; each re-run TM's `run.out` says "All tilt-series already settled —
  nothing to submit"; ribosome candidates per tomogram in the range of the complete projects (~300–700).

## 4. Ribosome

One gold-standard 80S map from all eight projects. Three jobs for it beyond the map itself:
- **the hand check** for the whole chain (playbook D17: TomoHand −1, angles inverted, never confirmed);
- **the data yardstick the 412 never had:** if ribosomes from the same tomograms reach ~8–10 Å, the 412's
  ~35 Å consensus is the sample, not the data or the pipeline;
- **the verdict particle for the miss-alignment A/B** (§6).

### R1 — Pilot (`AGG412_20260204_412`: 23 tomograms, 15,672 candidates, bin-4 subtomos in job015)
- Reference: ReconstructParticle of the pilot's subtomos with the TM angles (all of them, or the 630 ≥ 10σ once
  C3 exists), low-passed to 40 Å. Data-derived, so polarity and scale are right by construction; compare it
  with the 6XU8 template.
- Class3D (crboost), starting point: K 4, C1, mask Ø 350, ini_high 40, T 4, 25 iterations, local angular
  searches around the TM angles. Tabulate class membership by σ bin (7–8, 8–9, 9–10, ≥ 10) → the σ cut.
- Refine3D at bin 4 (6.2 Å/px; Nyquist-limited, ~13 Å). **Hand:** fit 6XU8 and its mirror into the map
  (ChimeraX `fitmap`); the better fit fixes the absolute hand for both species.
- Re-extract the clean set at bin 2 (3.1 Å/px), box 256 / crop 128: particle ~320 Å plus CTF delocalisation
  2λΔf/d ≈ 200 Å per side at 4 µm and 8 Å (tomograms near the 8 µm window edge would want 384).
- Refine3D at bin 2 (target ≤ 10 Å), then Polish + CtfRefine + Refine3D (copia went 10.5 → 8.2 Å this way).
  **Defocus hand:** one refine with `rlnTomoHand` flipped in `tomograms.star`; below ~10 Å the right sign wins.
- *Success:* a gold-standard map with its FSC 0.143; hand and defocus hand decided; the σ cut decided; scripts in
  the pilot's `sta/`.

### R2 — All eight projects
- Per project: σ subset at the R1 cut → Class3D at bin 4 with R1's map (40 Å) as reference → keep the ribosome
  classes. Run the eight in parallel.
- Per-project acceptance rate set against the tomogram markers (tsCtf fit > 10 Å, < 25 tilts, blank tilts W1):
  mark, don't drop; drop only what acceptance shows is bad.
- Merge in a ribosome aggregation project → Refine3D bin 4 → re-extract bin 2 → Refine3D → geometry loop → (M).
- Size: ~300–500k candidates at FP = 1 (86k in the complete projects; 634 pending tomograms at 300–700 each);
  bin-4 subtomos ~0.45 MB each → 135–225 GB. Refine a cleaned subset first.
- *Success:* one map from all projects; resolution against particle count (B-factor plot); per-project
  acceptance table.

## 5. 412 VLP

From July (412 playbook §12): 412 is a gypsy-family retrotransposon capsid, pleomorphic. Whole-capsid I1
consensus capped at ~35 Å for 94 particles here and for the PI's own 23-particle run, while copia (a regular
T=9 capsid, same lab and pipeline) reached 8–10 Å. High resolution on such capsids comes from averaging
capsomers, not whole capsids. New since July: ~10× the candidates.

### V1 — Curate (crboost)
- Per project, gallery-curate the ≥ 9σ candidates (2,143 in the seven finished projects, plus Slot6); curated
  sets land as `particles_filtered.star` per project (existing tooling).
- Recall: how many of the v1 manual picks in SK (playbook W15; 21 tomograms, 1–3 VLPs each) sit at ≥ 9σ and
  ≥ 10σ. Needs W15's coordinate-convention fix.
- Mark, don't drop: tomograms with blank tilts (W1 — concentric rings can score on a shell template), picks near
  the volume edge or lamella surface (W16), pairs closer than 500 Å (D15).
- MyrGFP_Grid2 first (§1).
- *Success:* curated counts per project; the σ cut that keeps the curated VLPs.

### V2 — Merge and classify (crboost)
Aggregation project → ReconstructParticle (bin 2, I1) → Class3D I1, K 3–5, box 384 / crop 224 (July's
settings) → junk classes out. Check whether classes separate by radius (size heterogeneity).

### V3 — Consensus refine per size class (external)
I1 Refine3D; ≤ 35 Å expected. Its job is to place the lattice, not to be the result.

### V4 — Local capsomer averaging (external; the real target)
Route decided by V2/V3:
- **(a) quasi-icosahedral** (classes refine cleanly at a defined radius): symmetry-expand the I1 poses to
  pentamers and hexamers, re-extract sub-particles centred on capsomers (bin 2, then 1), focused local refine with
  C5 / C6 and small searches, classify out misplaced ones. RELION 5.0.1 has no clean one-job recentre, so this
  goes through M or scripts (412 playbook §7).
- **(b) irregular** (fullerene-like, as Ty3): oversample each VLP's fitted sphere at capsomer spacing, align
  locally against a capsomer reference with the surface normal as prior, deduplicate by distance (the
  Gag-lattice approach). Needs a sphere fit per VLP and a seed generator.
- *Success:* a capsomer map whose gold-standard FSC beats the 35 Å consensus.

## 6. Miss-alignment A/B — the 10,000 ft view

**Question:** does refining the tilt-series alignment with miss-alignment improve subtomogram averages on these
data enough to apply it to all 922 series?

**What exists** (roadmaps 21 + 24, code-complete, never run in the paper regime): the missAlign job (schedules in
Å, PAPER / CUSTOM presets, fitted Z box, QOS, several GPUs) refining aligntiltsWarp's XMLs; its drop-in star with
the refined rigid geometry, which tsCtf follows by itself (`refines`), so RELION extraction downstream sees the
rigid refinement; the per-run changes report (`missalign_changes.json`); particle-free QC (tilt-split FSC,
`scripts/align_qc.py`, tsReconstruct `halfmap_tilts`).

**What's missing:**
- **Picks in the refined tomograms:** template matching re-run on each new set of tomograms. Picks are never moved
  between alignments (maintainer, 2026-10-10).
- **Warp particle export** (`ts_export_particles` on Warp 2.0.0dev41; roadmap 24 stage 4, built 2026-10-11, not run).
  RELION extraction applies only rigid per-tilt geometry; the local `[3,3]` grids, where the paper's gains come
  from, reach particles only through Warp.
- **`infer` mode** (train on a subset, align the rest; roadmap 24 stage 6) — the only affordable way to 922 series.

**Design (proposed):**
- **Data:** the AGG412 pilot (23 TS) instead of roadmap 24's stage 0 (re-processing `post_handedness_fix`,
  20 TS). Same sample, already processed with current code (CTF 30:10, tilt filter before alignment), ribosomes
  and 412s picked; its existing chain is arm A. Open the pilot only from a checkout whose registry knows
  `miss_alignment` (roadmap 21, open items), never from an older lab install.
- **Probe:** the ribosome, with R1's protocol. Not the 412: its consensus is limited by the sample, so an
  alignment gain can't show before V4.
- **Particles:** each arm picked by its own TM run on its own tomograms; the same reference (arm A's map at 40 Å),
  mask and Refine3D parameters; no Polish or CtfRefine (they re-estimate geometry and would erase the
  difference); two half-set seeds per arm, and a repeat of one arm with another missAlign seed for the tool's own
  spread.

| Phase | Arm | missAlign | Particles reach the arm via | Needs |
|---|---|---|---|---|
| 1 | A | none | RELION extraction | R1 |
| 1 | B | `30A anchoring; 20A anchoring x2; 10A global` (rigid only) | RELION extraction | TM re-run |
| 1 | B′ | B with another seed | RELION extraction | TM re-run |
| 2 | C | PAPER (+ five `10A [3,3]` rounds), z_box auto | Warp export | roadmap 24 stage 4 |
| 2 | E | C with the `[3,3]` rounds on 3.1 Å stacks | Warp export | only if C beats A |

- **Verdicts:** bin-2 gold-standard FSC curves per arm, differences counted only beyond the B vs B′ spread; the
  tilt-split FSC per TS ranks the same arms without particles. If both agree, the particle-free score can judge
  datasets without particles (roadmap 24's go/no-go).
- **Cost** (roadmap 24's 1.34 s/step on an A100 with a second GPU reconstructing, plus its estimator's 5 min per
  TS per iteration): arm B (4 iterations) ≈ 20 h at the default budget (250 × 30 steps), ≈ 2.2 days at the
  upstream one (1000 × 30; QOS `g_long`), resumable per iteration. Arm C (9 iterations) ≈ 2–5 days. Then tsCtf__2
  + tsReconstruct__2 (~1 h) and the refines (hours each).
- **Order:** R1 first (it is arm A and fixes the protocol); arm B can start in parallel (it needs only
  aligntiltsWarp's output); phase 2 after roadmap 24 stage 4; scale-out (train on 30–60 TS
  per session, infer the rest) only after a phase-1 or phase-2 "go", costed by roadmap 24 stage 2 first.

### 6.1 Running it on all eight projects (how; 2026-10-10)

*For the remaining seven projects, §6.2 (measured costs, parameters, decisions) replaces the cost table and
steps 1 and 3 below.*

missAlign is one GPU job per project over all its tilt series (not an array), training a model and then
aligning every series, once per macro-iteration. With the current code each project trains its own model;
`infer` (one model, many projects) is not built.

**Cost** (the job's own estimator: 1.34 s/step on an A100, ~3.5 on RTX nodes; 5 min of alignment per TS per
iteration, spread over the GPUs — **that 5 min is a guess, the pilot run measures it**). 2 GPUs (1 trains, 1
reconstructs), A100:

| Schedule | Pilot (23 TS) | 95–162 TS | All 8 projects | QOS |
|---|---|---|---|---|
| Rigid `30A anchoring; 20A anchoring x2; 10A global`, 250 × 30 steps | ~16 h | 28–39 h | ~240 h wall, ~480 GPU-h | g_medium (2 d) |
| PAPER, 1000 × 30 steps | ~4.6 d | 5.5–7 d | ~1,130 h wall, ~2,300 GPU-h | g_long (14 d) |

On RTX nodes, training adds ~18 h per project to the rigid numbers. Each arm then needs tsCtf + tsReconstruct (~1 h per project) and a
second set of tomograms (the baseline tsReconstruct is ~3 GB per tilt series, ~2.6 TB for all 867). PAPER's
`[3,3]` rounds reach particles only through Warp export (roadmap 24 stage 4), so running PAPER everywhere
before that exists buys nothing for STA. With `infer`, training once and aligning the rest would save ~1,400
GPU-h of PAPER training.

**Particles in the new tomograms:** template matching re-run on them (~440 GPU-h and ~7.5 TB of score maps across
the eight projects), then extraction. Picks are never moved between alignments (maintainer, 2026-10-10).

**Order — decided 2026-10-10 (maintainer):** no rigid-only arm and no staged verification ladder.
1. The maintainer builds the Warp export job (roadmap 24 stage 4 = roadmap 21 stage 6) in a separate worktree.
2. Then PAPER (or a better schedule if one is known by then) on all eight projects, upstream budget,
   `g_long`, all projects in parallel.
3. Particles: template matching re-run on the refined tomograms → Warp export → RELION.

Terms used above:
- **Rigid schedule:** anchoring/global iterations only. They change each tilt's whole-image shift, which
  RELION's star can hold, so RELION extraction would see the change today. No `[3,3]` rounds.
- **The pilot:** `AGG412_20260204_412`, the 23-tomogram project processed first (2026-10-04/05) to set the
  picking parameters before the rollout to the other seven.

**Per project, in the app** (from the dev server: this checkout has the regimes; finish stage 0's Run
first):
1. Pilot only, for the score: add a tsReconstruct on the existing tsCtf with `halfmap_tilts 1`,
   `halfmap_frames 0`, `deconv 0`, and run it alone. This is arm A's half-map set, and it must come before
   step 2, because afterwards a new tsReconstruct binds to the newest tsCtf.
2. Add three jobs: missAlign, tsCtf (becomes `tsCtf__2`), tsReconstruct (`tsReconstruct__2`, same settings as
   step 1 in the pilot, or as the baseline elsewhere). The IO tabs should show missAlign ← aligntiltsWarp;
   `tsCtf__2` star and processing ← missAlign, settings ← aligntiltsWarp; `tsReconstruct__2` ← `tsCtf__2`.
3. missAlign tab:
   - preset `paper` (it sets 1000 × 30 steps, milestones 5,15 and `z_box` auto);
   - `num_gpus` 2, `training_gpus` 1, `cpus_per_task` ≥ 8;
   - SLURM tab: QOS `g_long`.
4. Run with only those three selected. If missAlign hits its time limit, Run it again; it resumes from the
   last finished iteration as long as the schedule and inputs are unchanged.
5. Read-outs:
   - `python scripts/align_qc.py changes <aligntiltsWarp>/warp_tiltseries <missAlign>/warp_tiltseries` gives the
     per-TS changes after the rigid shift is removed;
   - `python scripts/align_qc.py score --arm A=<step 1 job> --arm C=<tsReconstruct__2> --reference A
     --out <dir outside /groups>`.

**Don't:** re-run the baseline tsCtf after adding a missAlign. It shows the newest missAlign as its source and
would rebind to it, overwriting the baseline.

### 6.2 Rollout to the other seven projects (2026-10-18)

MyrGFP_Grid2 ran the trio first (missAlign `job022` → tsCtf__2 `job023` → tsReconstruct__2 `job024`), then
template matching, candidates and Warp Export on both arms. This section is the record for the other seven:
what Grid2 measured, the decisions (MA#; append only — a changed decision is struck through and followed by its
replacement), the parameters, and the run log.

#### 6.2.1 What Grid2 measured (job logs, read 2026-10-18)

| | |
|---|---|
| missAlign wall | 20 h 39 on clip-g4-5 (2× A100, 16 CPUs, 64 G); 48 TS (Position_10_4 left out: 1 tilt) |
| Layout (auto) | 1 trainer on GPU 0 · 5 patch builders (GPU 1 ×3, GPU 0 ×2) · 10 loaders; the trainer was the limit (~80 % busy) |
| Training | 19 h 36 over 9 iterations (1 h 55 – 2 h 22 each), 2.74–3.14 steps/s (88–100 cubes/s); every iteration stopped early, at epoch 21–26 of 30 |
| Alignment | 61 min in all: ~5 min per iteration, 13 min for each 18.6 Å anchoring round; 75 s per TS over the whole schedule |
| Staging, stamp, z-box fits, pre-flight | ~2 min |
| What changed | per-tilt change with the rigid shift removed: median 16.7 Å rms, p90 37.9, max 121.8 (Position_6); 3×3 local warps 8–44 Å rms (median 16.3); `z_box auto` fitted 43 of 48 series (1188–2480 Å of 3174), 5 kept the full box |
| Requested wall time | 11-17:45:00, from older estimator constants; the current ones would ask ~3.9 d (§6.2.6) |
| tsCtf__2 | 48/48, 8–39 s per TS on g2/g3; 0.9 GB |
| tsReconstruct__2 | the first attempt lost series to dev41's shared WarpTools task queue (fixed 2026-10-16: a `--task_dir` per task); the re-run 48/48, 3–6 min per TS on g2/g3; 146 GB = 3 GB per TS (full, even and odd tomograms, 1 GB each) |
| A/B (2026-10-17) | ribosome, same template and FP = 1 cutoff on both arms: the refined tomograms gave +20 % picks per TS (median; 43/47 up) and +8 % top-50 LCC (45/47 up), the gain growing with how far a series moved. No map yet: both Refine 3D jobs were OOM-killed (preread on). |

#### 6.2.2 Decisions

| # | Decision | Why | Who, when |
|---|---|---|---|
| MA1 | No rigid-only arm and no staged verification; PAPER at the upstream budget on all eight projects, QOS `g_long`, in parallel | progress over verification ladders | maintainer, 2026-10-10 |
| MA2 | Picks are never moved between alignments: refined tomograms get their own TM run | a moved pick keeps the old geometry | maintainer, 2026-10-10 |
| MA3 | Warp Export before the rollout | the `[3,3]` grids reach particles only through Warp; RELION extraction reads the rigid per-tilt geometry | maintainer, 2026-10-10 (built 2026-10-11, first run 2026-10-12) |
| MA4 | One Warp image for every Warp job: 2.0.0dev41 + AreTomo2 (`tools.warp_aretomo`) | dev36's particle export is wrong for RELION 5 | maintainer, 2026-10-11 |
| MA5 | Excluded and < 2-tilt series are left out of a run (logged; `left_out` in `missalign_changes.json`) instead of failing it | one series that cannot be reconstructed killed the whole single-job run (Grid2 Position_10_4) | 2026-10-14 |
| MA6 | Eager training (`TORCHDYNAMO_DISABLE=1`) in every run | the compile-cache race killed two 3-trainer runs; compiled and eager checkpoints differ, so it must not flip between resumes | 2026-10-15 |
| MA7 | 2 GPUs, 1 trainer, 16 CPUs, builders and loaders on auto | 4-GPU requests queued for days; 3 trainers fed by 1 builder ran 6.65 s/step; this layout 0.33 s/step | 2026-10-15 |
| MA8 | Grid2 is the campaign's A/B, not the pilot (§6 design) | the first project through the Warp export path end to end | 2026-10-12 – 17 |
| MA9 | Roll out to the remaining seven projects | Grid2's A/B (§6.2.1) | maintainer, 2026-10-18 |
| MA10 | Every missAlign parameter as in job022 (§6.2.3) | the one measured run; keeps the eight projects comparable | proposed 2026-10-18 |
| MA11 | `num_gpus` 2 and no GRES override | job022 had `num_gpus` 4 with GRES `gpu:2`: it ran right (driver and estimate read the override), but the tab shows two numbers | proposed 2026-10-18 |
| MA12 | Constraint `g4` | Grid2's 20.7 h is an A100 number; a fed trainer on V100 / RTX 6000 is unmeasured. A job still pending after ~12 h: cancel, widen to `g2\|g3\|g4`, Run again (nothing lost, it never started) | proposed 2026-10-18 |
| MA13 | Time set by hand, `3-00:00:00` | the estimate asks 2:00:00 in the pilot and unmilled2 (§6.2.6) and ~5× too long elsewhere; 3 d covers the worst A100 case (~29.5 h) 2.4 times | proposed 2026-10-18 |
| MA14 | tsCtf__2 = the project's baseline tsCtf, unchanged | the A/B compares the alignment only | proposed 2026-10-18 |
| MA15 | tsReconstruct__2 = the baseline except `halfmap_frames` 0 | the frame halves feed only denoising, which no AGG412 chain runs; 2 GB less per TS (~1.6 TB over 819 TS); Warp writes the full tomogram the same either way. Keep 1 if the refined tomograms will be denoised | proposed 2026-10-18 |
| MA16 | One Run per project with only the three new jobs selected, from the dev server (`~/dev/crboost_server`), all seven at once | the lab install (pulled 2026-10-14) lacks the `--task_dir` fix; a Run of all roster jobs rebinds baseline jobs to the new producers | proposed 2026-10-18 |
| MA17 | Short series stay in: FCIsolation Position_32_4 (3 tilts), 17_2 (4), 29_5 (4), 30_3 (9); unmilled2 Position_32_3 (5) | Grid2 refined a 10-tilt series without trouble; mark, don't drop. If a run dies on one, exclude it in the registry and start that job over (a changed series list refuses to resume) | proposed 2026-10-18 |
| MA18 | Not in this rollout: the tilt-split score (§6.1 step 1, roadmap 24 stage 3), seed repeats, rebuilt-stack arms (`prepare_stacks_apix`) | MA1; the TM read-out (picks, LCC) answers whether it helps | proposed 2026-10-18 |
| MA19 | Series of fewer than 10 tilts are left out of a run (`MIN_TILTS` in `drivers/miss_align.py`); a resumed run keeps the series it was staged with. Supersedes MA17 | FCIsolation's patch builder died on Position_32_4 (3 tilts; the tool needs ≥ 4) and training hung for ~14 h | maintainer, 2026-10-19 |

#### 6.2.3 Parameters

missAlign (`services/jobs/miss_align.py`):

| Field | Grid2 job022 (ran) | The seven | Note |
|---|---|---|---|
| iteration_preset | paper | paper | |
| custom_schedule | (empty) | (empty) | read only with `custom` |
| → schedule | `30A anchoring; 20A anchoring x2; 10A global; 10A [3,3] x5` | same | on 6.2 Å stacks: ds5 = 31 Å, ds3 = 18.6 Å ×2, ds2 = 12.4 Å global, ds2 `[3,3]` ×5 (the 10 Å entries run 24 % coarser; no whole downsample comes closer) |
| steps_per_epoch | 1000 | 1000 | set by the preset |
| max_epochs_per_iteration | 30 | 30 | never reached: early stops at 21–26 |
| lr_milestones | 5,15 | 5,15 | |
| seed | 45132 | 45132 | |
| batch_size | 32 | 32 | |
| patch_size | 96 | 96 | 1190 Å field of view at 12.4 Å |
| pool_size | 1000 | 1000 | room for 15 loaders; 10 used |
| num_gpus | 4 (GRES override `gpu:2`) | **2** | MA11 |
| training_gpus | 1 | 1 | |
| patch_builders | 0 (auto → 5) | 0 | |
| dataloader_workers | 0 (auto → 10) | 0 | |
| prepare_stacks_apix | 0 | 0 | aligns the alignment job's 6.2 Å stacks |
| z_box | auto | auto | set by the preset |
| SLURM partition | g | g | |
| SLURM constraint | `g2\|g3\|g4` (landed on g4) | **`g4`** | MA12 |
| SLURM nodes / ntasks_per_node | 1 / 1 | 1 / 1 | |
| SLURM cpus_per_task | 16 | 16 | sets the layout: 1 trainer, 5 builders, 10 loaders |
| SLURM gres | `gpu:2` (override) | no override (→ `gpu:2`) | MA11 |
| SLURM mem | 64G | 64G | profile default 48G |
| SLURM time | estimated, 11-17:45:00 | **3-00:00:00** | MA13 |
| SLURM qos | g_long | g_long | |
| input_star, input_processing, warp_tiltseries_settings | aligntiltsWarp job007 | the project's aligntiltsWarp (§6.2.4) | automatic |

tsCtf__2 (`services/jobs/ts_ctf.py`), the same as the baseline tsCtf in all eight projects:

| Field | Grid2 job023 (ran) | The seven |
|---|---|---|
| window | 512 | 512 |
| range_min_max | 30:10 | 30:10 |
| defocus_hand | set_flip | set_flip |
| defocus_min_max | 1.1:8 | 1.1:8 |
| do_phase | false | false |
| perdevice | 1 | 1 |
| array_throttle | 20 | 20 |
| SLURM per task (profile, no override) | g · `g2\|g3\|g4` · 1 node · 1 task · 2 CPUs · `gpu:1` · 8G · 0:10:00 · default QOS | same (tasks took ≤ 39 s) |
| Supervisor (`supervisor_slurm`, conf.yaml) | g · `g2\|g3\|g4` · 1 CPU · no GPU · 4G · 8:00:00 | same |
| input_star, input_processing | missAlign job022 | the new missAlign (automatic: it refines aligntiltsWarp) |
| warp_tiltseries_settings | aligntiltsWarp job007 | the project's aligntiltsWarp |

tsReconstruct__2 (`services/jobs/ts_reconstruct.py`):

| Field | Grid2 job024 (ran) | The seven |
|---|---|---|
| rescale_angpixs | 6.2 | 6.2 |
| halfmap_frames | 1 | **0** (MA15) |
| halfmap_tilts | 0 | 0 |
| deconv | 0 | 0 |
| perdevice | 1 | 1 |
| array_throttle | 20 | 20 |
| SLURM per task (profile, no override) | g · `g2\|g3\|g4` · 1 node · 1 task · 4 CPUs · `gpu:1` · 64G · 1:00:00 · default QOS | same (tasks took ≤ 6 min) |
| Supervisor (`supervisor_slurm`, conf.yaml) | g · `g2\|g3\|g4` · 1 CPU · no GPU · 4G · 8:00:00 | same |
| input_star, input_processing | tsCtf__2 job023 | the new tsCtf__2 |
| warp_tiltseries_settings | aligntiltsWarp job007 | the project's aligntiltsWarp |

#### 6.2.4 Per project

Read 2026-10-18 from each project's `project_params.json`, its alignment star and the stack headers. All eight
picking chains are green (stage 0 done). The job numbers are each project's next free ones, right if these three
are the next jobs deployed.

| Project (`AGG412_…`) | Alignment (input) | Series in its star | Left out (< 2 tilts) | Refined | < 25 tilts (fewest) | New jobs | missAlign expected (no early stop) | The estimate would ask | tsReconstruct__2 disk (`halfmap_frames` 0 / 1) |
|---|---|---|---|---|---|---|---|---|---|
| 20240204_412_Slot6 | job004 | 123 | — | 123 | 10 (15) | job013 / 014 / 015 | ~22 h (28.5 h) | 5-2:45:00 | 0.12 / 0.37 TB |
| 20251031_412_FCIsolation | job007 | 142 | — | 142 | 34 (3) | job016 / 017 / 018 | ~22.5 h (29 h) | 5-16:38:00 | 0.14 / 0.43 TB |
| 20251113_412 | job004 | 141 | — | 141 | 9 (17) | job013 / 014 / 015 | ~22.5 h (29 h) | 5-12:08:00 | 0.14 / 0.42 TB |
| 20260204_412 (pilot) | job004 | 23 | — | 23 | 0 (31) | job016 / 017 / 018 | ~20 h (26.5 h) | **2:00:00** | 0.02 / 0.07 TB |
| 20260205_412_FCIso_unmilled2 | job010 | 164 | Position_22_4, Position_32 | 162 | 11 (5) | job019 / 020 / 021 | ~23 h (29.5 h) | **2:00:00** | 0.16 / 0.49 TB |
| 20260311_412_Grid3 | job004 | 95 | — | 95 | 0 (32) | job013 / 014 / 015 | ~21.5 h (28 h) | 4-15:30:00 | 0.10 / 0.29 TB |
| SK_20251017_412 | job007 | 133 | — | 133 | 8 (20) | job016 / 017 / 018 | ~22.5 h (29 h) | 5-6:53:00 | 0.13 / 0.40 TB |
| **Total** | | 821 | 2 | **819** | | | ~310 A100 GPU-h | | 0.8 / 2.5 TB |

Expected = Grid2's 19.6 h of training (independent of the series count) + 75 s of alignment per series + a few
minutes of staging; in brackets, every iteration running all 30 epochs (25.9 h of training). A100 only. With
Grid2's 48 the eight projects cover all 867 tomograms. Then tsCtf__2 (~20–40 min) and tsReconstruct__2
(~30–60 min at 20 tasks) per project. §6.1's cost table (~1,130 h wall, ~2,300 GPU-h for all eight) is superseded.

#### 6.2.5 Per project, in the app

1. Open the project from the dev server (`~/dev/crboost_server`), not the lab install (MA16).
2. Add missAlign, tsCtf and tsReconstruct (they become `missAlign`, `tsCtf__2`, `tsReconstruct__2`) and set them
   as §6.2.3: missAlign preset `paper`, `num_gpus` 2, and in its SLURM section constraint `g4`, CPUs 16, mem 64G,
   time `3-00:00:00`, QOS `g_long`; tsReconstruct__2 `halfmap_frames` 0.
3. IO tabs: missAlign ← aligntiltsWarp (all three slots); tsCtf__2 star and processing ← missAlign, settings ←
   aligntiltsWarp; tsReconstruct__2 star and processing ← tsCtf__2, settings ← aligntiltsWarp.
4. Run with only these three selected.
5. missAlign's `run.out` within minutes: `Workers: 1 trainer(s) on GPU 0 · 5 patch builder(s) … · 10 loader(s)
   per trainer … · 16 of 16 CPUs`; the left-out lines (unmilled2: two); `Pre-flight emit OK for N/N`; then the
   Training bar at ~2.7–3.1 steps/s. Much slower means a non-A100 card or a starved layout.
6. If it stops at the time limit, Run it again: it resumes after the last finished iteration.

The job tab will warn in five projects that the full run exceeds the time override: that is the estimate
(§6.2.6), not the run.

**Don't:**
- Re-run a baseline job (tsCtf, tsReconstruct, TM, candidates, subtomo) after adding the trio without first
  pinning its inputs in its IO tab: it would follow the newest producer and overwrite the baseline.
- Edit `drivers/` or `services/` in the dev tree while the chains are queued or running: each driver loads the
  dev tree when it starts.

**Watch:**
- Seven 2-GPU jobs on `g_long` at once: jobs pending with `QOSMaxGRESPerUser` or `QOSMaxJobsPerUserLimit` hit
  the QOS's per-user cap and start as others finish.
- A dead trainer leaves the builders running and the tool hung: `run.out` still for more than ~45 min → cancel,
  Run again.
- Next, per project once tsReconstruct__2 is done: TM on the refined tomograms (MA2), candidates, Warp Export, as
  Grid2's job026 / 028 / 030; their parameters when the trio is through.

#### 6.2.6 Code follow-ups found (not built)

1. **The wall-time estimate counts `import_selected_tilt_series`**, which is 0 in the pilot and unmilled2. The
   estimate then falls back to the profile's 2:00:00 without a warning, while one PAPER iteration needs ~2.3 h:
   every attempt would die in iteration 1 and repeat it. Count the input star's series (what the driver refines).
   `denoise_train`'s estimate reads the same field.
2. **The constants are ~5× long.** `_TRAINER_STEP_SEC` 1.0 (a fed A100 measured 0.33) and
   `_WALLTIME_PER_TS_ALIGN_MIN` 5 (measured ~0.3 GPU-min per series per iteration). With the measured values
   Grid2's estimate would be ~32 h (20.7 h ran; the rest is the 30-epoch cap and the builder/loader constants)
   instead of ~3.9 d.
3. **Trainer speed on V100 / RTX 6000 is unmeasured**; the first job that lands on g2/g3 measures it (steps/s in
   `run.out`).

#### 6.2.7 Run log (append; newest last)

- **2026-10-18** — Grid2 logs read (§6.2.1); the seven projects surveyed (§6.2.4); MA10–MA18 proposed. Nothing
  submitted.
- **2026-10-18 23:13–23:19** — All seven submitted: QOS `g_long`, `3-00:00:00`, constraint `g2|g3|g4`. Grid3's
  first two submits were refused without a QOS (`QOSMaxWallDurationPerJobLimit`) and left empty job013 / job014;
  its third is job015.
- **2026-10-19 00:09** — FCIsolation job016 (SLURM 3527088, clip-g2-3): a patch builder died on Position_32_4
  (`num_points must be at least 4, got 3`), and training hung, still RUNNING in SLURM with no output.
- **2026-10-19 14:19–14:24** — The other six missAlign jobs and Grid2's Refine 3D job036 / job037 ended `FAILED`
  1:0 (sacct) on g3/g4 nodes, with no line in `run.out` / `run.err` and no exit marker, so nothing could be
  written to `/groups` at the time. Likely a shared-filesystem drop. Checkpoints kept: SK, 20251113, Slot6,
  unmilled2 iterations 1–3; Grid3, pilot 1–5. Refine 3D job036 had finished iteration 9, job037 iteration 4.
- **2026-10-19** — MA19 built. `drivers/refine3d.py` now continues a re-run from the last finished iteration
  (`relion_refine --continue`, output `run_ct<N>_*` copied to the `run_*` names), so Run picks job036 / job037
  up where they stopped.

## 7. Code this needs (proposed; nothing built)

| # | What | For | Size |
|---|---|---|---|
| C1 | Supervisor time-outs. Warn at Start when tomograms ÷ throttle × the expected minutes per tomogram exceeds the supervisor limit (TM medians, g4 / g2 / g3: ribosome box 96 at 12° 12 / 23 / 35 min; 412 8 / 14 / 18 min); and either a `qos` field on `supervisor_slurm` (g_medium for long arrays) or a supervisor that reads its own end time and, ~10 min before, logs "N tasks still running — Run again to aggregate" and exits with that reason. Arrays must stay alive when a supervisor times out: they do today because the SIGTERM handler never ran (no log line; every array finished). No auto-resubmit (standing policy). | W19 | small |
| C2 | Upstream-input checks retry for ~60 s before failing: `require_producer_input` and subtomo's own `.exists()` (`drivers/subtomo_extraction.py:269`). | W20 | small |
| C3 | σ subset of a candidate or particle set (per-tomogram σ from the TM `job.json`) → a filtered optimisation set. Without code, a new extraction job with a smaller FP is an exact σ cut, because every tomogram of a TM job has the same search space (412: FP 1e-4 ≈ 8.3σ, 1e-8 ≈ 9.3σ, 1e-12 ≈ 10.3σ; the ribosome's ~0.1σ higher), but it repeats subtomo extraction. | R1, R2, V1 | small |
| C4 | Class subset → optimisation set (a Class3D `run_itNNN_data.star` plus the chosen classes). | R1, R2, V2 | small |
| C6 | Warp export path; missAlign `infer`. | A/B phase 2, scale-out | roadmap 24 stages 4, 6 |

C3 and C4 are now stages 2 and 4 of roadmap 30 (particle analytics and set tools, 2026-10-10).

Refine3D, PostProcess, Polish, CtfRefine, symmetry expansion and M stay external scripts; a crboost Refine3D
job is worth it only once the scripted protocol has settled.

## 8. Decisions owed

1. Ribosome σ cut: from R1's class-by-σ table (proposed) or a fixed ≥ 8σ.
2. 412 curation scope: ≥ 9σ (2,143 + Slot6) or ≥ 10σ (929 + Slot6).
3. 412 local route (V4 a or b), after V2/V3.
4. ~~A/B dataset: the AGG412 pilot (proposed) or `post_handedness_fix` (roadmap 24's plan).~~ MyrGFP_Grid2 (§6.2, MA8).
5. ~~Arm B's training budget: default 250 × 30 first (proposed) or upstream 1000 × 30.~~ No arm B (MA1).
6. TM maps (~7.5 TB): keep until σ cuts and extraction diameters are final, then delete the `*_angles.mrc`
   (half) or both maps.
7. C1: supervisor QOS or a supervisor that stops before its limit (or both).
8. W2's 55 tilt series: leave them out of this campaign (proposed) or wait for roadmap 27 stage 2.

## 9. Runtime checklist

1. Stage 0: per project, the re-run TMs' `run.out` say "nothing to submit"; extraction counts per tomogram look
   like the complete projects'; every subtomo job has `optimisation_set.star`.
2. R1: the class-by-σ table; the `fitmap` correlations for 6XU8 and its mirror; FSC 0.143 at bin 4 and bin 2;
   both defocus-hand refines.
3. V1: curated counts per project and the SK recall numbers.
4. A/B: the B vs B′ spread recorded before A is compared.

**Modern-Python weave-in:** C3 and C4 as pure functions over star DataFrames in `services/particles/`; checked on the pilot's files rather than in `tests/` (the install
self-test).
