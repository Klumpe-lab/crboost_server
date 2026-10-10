# Roadmap 29 — AGG412: finish the picking, filter, average the ribosome and the 412 VLP, and an A/B for miss-alignment

**Status:** scoped 2026-10-08; nothing built. Stage 0 is a user run with no code change. The campaign's
living record (decisions D#, watch-list W#, run log, analysis scripts) is the git-ignored
`notes/agg412_picking/PLAYBOOK.md`; this roadmap is the plan from the picks onwards. Related: 24
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
- **Same particles across arms.** Each arm moves every volume by a rigid translation (the Z gauge reached
  ±250–290 Å on the first run), so coordinates have to move by the per-TS (tx, ty, tz).
  `services/analysis/alignment_changes.py` fits that translation but doesn't expose it in RELION's coordinate
  frame; `recon_register` can cross-check it on the two reconstructions. (C5)
- **Warp particle export** (`ts_export_particles` + the dev36 → RELION 5 star converter; roadmap 24 stage 4).
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
- **Fixed particles:** R1's cleaned ribosome set re-homed into each arm; the same reference (arm A's map at 40 Å),
  mask and Refine3D parameters; no Polish or CtfRefine (they re-estimate geometry and would erase the
  difference); two half-set seeds per arm, and a repeat of one arm with another missAlign seed for the tool's own
  spread. Cheap addition on the pilot: re-pick on arm B's tomograms (~20 min TM per tomogram) for the end-to-end
  number.

| Phase | Arm | missAlign | Particles reach the arm via | Needs |
|---|---|---|---|---|
| 1 | A | none | RELION extraction | R1 |
| 1 | B | `30A anchoring; 20A anchoring x2; 10A global` (rigid only) | RELION extraction | C5 |
| 1 | B′ | B with another seed | RELION extraction | C5 |
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
  aligntiltsWarp's output); C5 before any scoring; phase 2 after roadmap 24 stage 4; scale-out (train on 30–60 TS
  per session, infer the rest) only after a phase-1 or phase-2 "go", costed by roadmap 24 stage 2 first.

## 7. Code this needs (proposed; nothing built)

| # | What | For | Size |
|---|---|---|---|
| C1 | Supervisor time-outs. Warn at Start when tomograms ÷ throttle × the expected minutes per tomogram exceeds the supervisor limit (TM medians, g4 / g2 / g3: ribosome box 96 at 12° 12 / 23 / 35 min; 412 8 / 14 / 18 min); and either a `qos` field on `supervisor_slurm` (g_medium for long arrays) or a supervisor that reads its own end time and, ~10 min before, logs "N tasks still running — Run again to aggregate" and exits with that reason. Arrays must stay alive when a supervisor times out: they do today because the SIGTERM handler never ran (no log line; every array finished). No auto-resubmit (standing policy). | W19 | small |
| C2 | Upstream-input checks retry for ~60 s before failing: `require_producer_input` and subtomo's own `.exists()` (`drivers/subtomo_extraction.py:269`). | W20 | small |
| C3 | σ subset of a candidate or particle set (per-tomogram σ from the TM `job.json`) → a filtered optimisation set. Without code, a new extraction job with a smaller FP is an exact σ cut, because every tomogram of a TM job has the same search space (412: FP 1e-4 ≈ 8.3σ, 1e-8 ≈ 9.3σ, 1e-12 ≈ 10.3σ; the ribosome's ~0.1σ higher), but it repeats subtomo extraction. | R1, R2, V1 | small |
| C4 | Class subset → optimisation set (a Class3D `run_itNNN_data.star` plus the chosen classes). | R1, R2, V2 | small |
| C5 | Particle re-homing between two alignments of the same series (§6). | A/B | medium |
| C6 | Warp export path; missAlign `infer`. | A/B phase 2, scale-out | roadmap 24 stages 4, 6 |

Refine3D, PostProcess, Polish, CtfRefine, symmetry expansion and M stay external scripts; a crboost Refine3D
job is worth it only once the scripted protocol has settled.

## 8. Decisions owed

1. Ribosome σ cut: from R1's class-by-σ table (proposed) or a fixed ≥ 8σ.
2. 412 curation scope: ≥ 9σ (2,143 + Slot6) or ≥ 10σ (929 + Slot6).
3. 412 local route (V4 a or b), after V2/V3.
4. A/B dataset: the AGG412 pilot (proposed) or `post_handedness_fix` (roadmap 24's plan).
5. Arm B's training budget: default 250 × 30 first (proposed) or upstream 1000 × 30.
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
4. A/B: ≥ 95 % of re-homed particles inside arm B's volumes; the B vs B′ spread recorded before A is compared.

**Modern-Python weave-in:** C3–C5 as pure functions over star DataFrames in `services/particles/`, the per-TS
re-homing translation as a frozen dataclass; checked on the pilot's files rather than in `tests/` (the install
self-test).
