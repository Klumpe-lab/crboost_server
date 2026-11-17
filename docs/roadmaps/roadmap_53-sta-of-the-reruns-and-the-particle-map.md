# Roadmap 53 — STA of the reruns: ribosome and 412 classes as far as they go, and the particle map built from doing it

**Status:** scoped 2026-11-17 from the eight finished reruns of roadmap 49; nothing run, nothing built. Decisions
owed in §8, the first ones before stage A starts.

**In one line.** Average the ribosome and the 412 VLP over the eight AGG412 reruns, sort each into classes and
refine every class on its own to the best resolution the data hold. Do every step that changes which particles are
in a set, or where they point, as a crboost job or a kitchen set, so that every map can be traced back to its picks
in the UI. Write down what hurt at each step (§6, field notes); the particle half of the branch map (roadmap 32
§15) is built from those notes, a stage behind the science, never ahead of it.

**Words used here.** A *pick* is one template-match candidate; its uid (`cbPickUid`) is the particle for good. A
*set* is a list of uids plus how they were chosen (roadmap 40 §15.3). A *facet* is what a job produced for a uid: a
position, images, poses, a class. *σ* is a pick's score in standard deviations of its tomogram's score map
(roadmap 30). *bin N* means a pixel of N × 1.55 Å; the finest detail a binning can carry (Nyquist) is two pixels.

## 1. Where we start (read 2026-11-17)

All eight reruns in `/groups/klumpe/_crboost_data_api/` are green through Warp Export for both species: 412 at
bin 2 box 256 (794 Å), ribosome at bin 2 box 200 (620 Å), both 3.1 Å/px, one Warp export per species per project.
Candidates are at FP = 1, so a σ cut is a filter on what exists, not a new extraction.

| Project (`AGG412_…_rerun`) | Tomos with ribosome picks | 412 cand. | 412 ≥ 8σ / ≥ 9σ / ≥ 10σ | Ribosome cand. | Ribosome ≥ 8σ / ≥ 9σ |
|---|---|---|---|---|---|
| 20240204_412_Slot6 | 120 | 1,513 | 351 / 211 / 172 | 24,117 | 14,894 / 9,072 |
| 20251031_412_FCIsolation | 159 | 3,510 | 625 / 206 / 63 | 44,826 | 27,136 / 15,134 |
| 20251113_412 | 139 | 2,932 | 356 / 95 / 29 | 49,553 | 31,268 / 20,308 |
| 20260204_412 (pilot) | 23 | 63 | 11 / 9 / 8 | 15,626 | 11,460 / 6,395 |
| 20260205_412_FCIso_unmilled2 | 173 | 7,943 | 1,264 / 282 / 76 | 88,661 | 57,772 / 40,751 |
| 20260311_412_Grid3 | 91 | 1,440 | 128 / 28 / 20 | 35,611 | 23,292 / 13,422 |
| 20260311_MyrGFP_Grid2 | 48 | 2,064 | 387 / 172 / 69 | 20,554 | 10,782 / 5,228 |
| SK_20251017_412 | 131 | 2,853 | 403 / 170 / 130 | 67,886 | 48,469 / 33,419 |
| **Total** | **884** | **22,318** | **3,525 / 1,173 / 567** | **346,834** | **225,073 / 143,729** |

Counts from each project's `candidates.star` and Warp export `particles.star` (session scratchpad `census.py`).
The exports drop a handful of picks each (tilt series skipped by Warp, picks at the edge): 22,190 and 346,381 rows.

What earlier work already tells us:

- **Ribosome.** On the old MyrGFP_Grid2 (missAlign chain), Refine 3D of all 33,252 candidates at bin 2, no
  classification, reached 8.27 Å unmasked (2026-10-20, roadmap 31); the chain without missAlign 11.23 Å on 26,335.
  Bin 2's Nyquist is 6.2 Å, so the next gains need bin 1 (Nyquist 3.1 Å), cleaner sets and geometry refinement.
  The rerun has ten times the particles. The absolute hand (`rlnTomoHand` −1, defocus hand inverted) has never
  been checked on a map that resolves it (roadmap 29 D17).
- **412.** July's 94-particle I1 refine capped at 35.8 Å gold-standard (412 playbook §12); its 16.3 Å was Class 3D's
  own estimate, not an honest FSC. The PI's v1 run averaged 23. Kitchen set s001 (2,708 picks of the old projects)
  reconstructed with the template-match angles gave a round shell, peak radius ~160 Å, no facets (roadmap 40 §15.1):
  the angles do not agree from particle to particle. On whole tomograms the real VLPs sit at ~8.5–9σ, the noise
  below 7.5σ (roadmap 49 §7, R1). Roadmap 29 §5 holds the reading that 412 is pleomorphic and that resolution will
  come from capsomers, not whole capsids.
- **Copia** (same lab, same pipeline): Class 3D 8.5 Å → Refine 3D 10.5 → Polish + CtfRefine + Refine 3D 7.8 Å, 8.2
  after PostProcess. The step that mattered was refining again after the geometry steps.
- **Kitchen.** Pools for both species were opened in Slot6_rerun on 2026-11-16 with the eight projects as members.
  Kitchen sets, facets and Run checks exist (roadmap 40 §14, §15, commit `57285b2`), none run on the reruns.
- **Jobs that exist:** Warp Export (any binning, poses from a set or a Class 3D / Refine 3D result), Reconstruct
  particle, Class 3D (local search around input angles via `sigma_ang`, solvent mask), Refine 3D (gold-standard,
  MPI, continues after a time-out). **Missing:** PostProcess, a solvent mask made from a refined map, geometry
  refinement (M, or RELION's tomo CtfRefine and frame alignment), symmetry expansion and recentring, a class → set
  step that keeps lineage. Roadmap 30 S4 (class subsets) is built and was never run; check it before writing new
  code.
- **Disk.** A ribosome export at bin 2 box 200 is 2.9 MB a particle (pilot: 45 GB for 15,620). Bin 4 box 100 for
  all 347k: ~250 GB. Bin 1 box 384 for 150k: ~1.6 TB. TM maps are ~93 GB per species per 23 tomograms, ~7 TB over
  the eight projects, and with every candidate already exported nothing below reads them again (D6).

## 2. Principles

- **One lineage.** A step that changes membership or poses is a crboost job reading a set, or a kitchen set made
  from a job's output. A side run outside crboost (a diagnostic refine, a ChimeraX fit) is allowed; its result
  counts only once it comes back in as a set or a job input, and the field notes say what it was. This replaces
  July's agreement that 412 refinement runs outside crboost (D1).
- **The science sets the order of the UI.** A UI stage is built only for something two science steps have already
  needed. What hurt goes into §6 the day it hurt.
- **One home.** All STA jobs and kitchen sets live in one home project; the eight reruns are read, never written
  (D2).
- **Classes are sets.** Every class worth keeping becomes a set with its parent (the job and the class number); a
  set refined on its own is a new branch, never an overwrite.
- **Each resolution says what it is.** Class 3D's estimate, Refine 3D's unmasked half-map FSC, PostProcess's masked
  FSC: three numbers, three names, never one "resolution" column.
- **Mark, never drop, by rule.** σ cuts, CTF-fit markers and depth bands mark; classification and the kitchen's
  verdicts decide.
- **Best settings first.** No pilot ladders: the pilot project goes first only because it is the smallest and
  finishes first, and all eight run beside it.

## 3. Ribosome track (R)

Target: a consensus map at bin 1 with masked FSC well past bin 2's 6.2 Å, then per-class maps of the states the
data hold, each with its own resolution and its share per project.

### R0 — Reference and hand

- Reconstruct particle of the ≥ 10σ picks of one project (pilot, 2,870) with the TM angles at bin 2, low-passed to
  40 Å: the starting reference, data-derived, so scale and polarity are right by construction.
- After R2, fit 6XU8 and its mirror into the consensus (ChimeraX `fitmap`, a side run): the better fit fixes the
  absolute hand for both species. Below ~8 Å, one Refine 3D with `rlnTomoHand` flipped in a copy of
  `tomograms.star` settles the defocus hand.

### R1 — Clean per project at bin 4

- A second Warp Export per project at bin 4, box 100 (620 Å, 6.2 Å/px; ~0.7 MB a particle), reading the same
  candidates: classification does not need bin 2, and it is ~4× lighter in memory and time.
- Class 3D per project, all eight at once: K 4, C1, mask Ø 350, `ini_high` 40, T 4, 25 iterations, local search
  around the TM angles (`sigma_ang`), reference R0.
- Read-outs: class × σ bin (7–8, 8–9, 9–10, ≥ 10) per project, giving the σ below which picks are mostly junk
  (decides roadmap 29's open σ cut); acceptance per tomogram against the tomogram markers (tsCtf fit, tilts kept).
- Out: per project, the ribosome classes as sets.

### R2 — Consensus at bin 2

- One set of the kept classes of all eight (kitchen, Make set from class sets) → Refine 3D at bin 2 on the bin-2
  exports that exist (the set's images facet: one export per project, identical parameters, so they combine).
- Solvent mask from the map, PostProcess (both missing jobs, C3/C4).
- Expect ~7 Å, near bin 2's Nyquist. Its job is the poses for bin 1.

### R3 — Bin 1 and geometry

- Warp Export at bin 1, box 384 (595 Å), poses from R2's Refine 3D (an existing facet: Warp does the recentring).
- Refine 3D at bin 1, local; PostProcess.
- Geometry: M (refines per-tilt alignment, per-particle pose, CTF and pixel scale per source, with eight sources
  from eight projects) or RELION 5's tomo CtfRefine + frame alignment; then Refine 3D again, which is the step copia
  showed matters (D4).
- Measures: resolution against particle count from random halves of the set (the B-factor plot): it says whether
  more particles or better geometry is the lever. Pixel size per project: refine one project's particles alone and
  compare scale with the consensus; sessions from 2024 to 2026 may not share a magnification to the third digit,
  which matters below ~4 Å. M fits it per source.

### R4 — Classes, each refined on its own

- Focused Class 3D without alignment (`--skip_align` through `other_args` until a field exists) on the bin-1
  consensus poses, masks on: the tRNA sites (A/P/E), the factor-binding site, the L1 stalk, the peptide exit
  (membrane-bound vs cytosolic). Masks drawn from the consensus around a fitted model (side run, then a mask job
  input).
- Neighbours from positions: pairs within one particle diameter in the polysome geometry (a spatial facet, C9).
- Each class worth keeping: a set → Refine 3D (local) → PostProcess; geometry again if the class is large.
- Read-out per class: count, share per project (and per condition, D8), masked resolution.

## 4. 412 track (V)

Target: a gold-standard map past July's 35.8 Å: whole capsids per size or shape class if they turn out regular,
capsomers if not. 412's shape decides the route, and the first two stages are there to find it.

### V1 — Curate in the kitchen

- Pool of the eight (exists in Slot6_rerun). Filter ≥ 8.5σ (between 1,173 at ≥ 9σ and 3,525 at ≥ 8σ), gallery
  verdicts by hand, tomogram by tomogram where a tomogram is crowded (unmilled2 holds a third of the candidates).
- Mark: picks at the lamella edges, pairs closer than 500 Å, tomograms with blank tilts (rings score on a shell
  template).
- A per-pick radius from the cutout's radial profile (C9): sizes as a column to filter and to split on.
- Out: one curated set; its count by σ bin says where the 412s really sit on the rerun scale.

### V2 — Shape before symmetry

- Warp Export at bin 4, box 128 (794 Å).
- Class 3D C1, K 4–6, a featureless shell reference at the measured radius (data-derived, not the old template),
  global search (the TM angles do not agree, §1). Classes by size and by shape.
- For the best class: Refine 3D C1, then look for symmetry in the C1 map (5-folds; `relion_align_symmetry` I1 to
  put it on the axes if they are there). The decision (D5): I1 per class, or no global symmetry.

### V3 — Consensus per class

- Bin 2 (existing export, box 256), Refine 3D in the symmetry V2 found, mask from the map (thinner at the lumen
  than July's: the PI's mask lesson, 412 playbook "mask lever"), PostProcess.
- Its job is to place the lattice. Expect 20–35 Å.

### V4 — Capsomers

- Regular lattice: symmetry-expand V3's poses, recentre on pentamers and hexamers, sub-particles as a set whose rows
  carry their parent uid (C7), Warp Export at bin 2 and 1 in a small box, local refine with C5 / C6, classify out
  the misplaced ones.
- Irregular: seeds over each VLP's fitted sphere at capsomer spacing, local alignment with the normal as prior,
  deduplicate by distance (the Gag-lattice route of roadmap 29 §5 V4b). Scoped only once V2 says so.

## 5. UI track (U): the particle map

The preprocessing branch map (roadmap 32 §15.8, `ui/branch_map/`) shows jobs as cards in columns by depth, a
magazine of per-tilt-series marks above each card, a large card with parameters, results and the picked tilt
series' Journey, and a tilt-series picker above the map. The particle half uses the same map, cards and look; what
changes is what flows along the cables.

- **Sets as nodes.** Kitchen sets and class sets sit on the map as slim cards between the jobs that made them and
  the jobs that read them. A Class 3D card has one output port per class; the + on a class port offers Make set,
  Refine 3D, Class 3D, Reconstruct particle, Warp Export (the "+ per output" of §15.2). That is the branching
  builder for particles: a classification is a fork.
- **The magazine is composition.** Above a particle card: one segment per project (width = count), coloured by
  condition (D8); a Class 3D card adds its class bars.
- **The picker is a set picker; picking a set traces it.** Pick any set and every card shows how many of its
  uids it holds ("1,203 of 2,000"), cables thicken with the flow, upstream and downstream. Pick a class and see
  which projects, σ bands and earlier classes it came from; pick a curated set and see where its particles ended
  up after two rounds of classification. The primitive is uid-set overlap, cheap on uid lists.
- **The large card is the inspector.** Parameters (as the job tab groups them); results named by kind (Class 3D
  estimate, unmasked FSC, masked FSC, each its own line and curve); central slices of the map with the mask's
  outline over them; radial profile; angular distribution; resolution and accuracy per iteration; and the
  particles: counts by project → tomogram, the per-tomogram count distribution (geometry refinement needs several
  particles per tilt series, §6 F9), σ histogram, a strip of kitchen cutouts.
- **The kitchen stays the place for judging.** Filters, gallery, verdicts, Make set stay in the kitchen; the map is
  for lineage and composing jobs. Results back on the cutouts (roadmap 40 D18) give the kitchen its By class mode.

### Stages

| Stage | What | Needed by |
|---|---|---|
| U0 | Field notes (§6), kept from the first run on | all |
| U1 | Data layer: class → set with parent (job, class), results back as facets (D18), a lineage graph of sets and jobs per home project with uid overlap between any two sets, kept current by mtimes | R1, V2 |
| U2 | Particle cards on the map: set nodes, class ports, the + per class, the composition magazine | R2, V3 |
| U3 | The trace: set picker, overlap per card, flow on the cables | R4, V4 |
| U4 | The inspector blocks of the large card | R2 onwards |
| U5 | Retire what the map and kitchen cover (Journey particles panel, merge / aggregate dialogs, roadmap 40 §9) | after R4 |

Missing jobs (C) are built when the science reaches them, as cards from the start:

| # | Job | First needed |
|---|---|---|
| C1 | Class → set (check roadmap 30 S4 first) | R1 |
| C2 | Results as facets (roadmap 40 D18) | R1 |
| C3 | PostProcess: mask, auto or given B-factor, masked FSC, local resolution optional | R2 |
| C4 | Solvent mask from a map: threshold, extend, soft edge (the species `make_mask` path in `services/species_edit.py` may cover it) | R2 |
| C5 | `skip_align` and a focus mask on Class 3D as fields | R4 |
| C6 | Geometry: M (MTools / MCore are in the Warp image) or RELION tomo CtfRefine + frame alignment (D4) | R3 |
| C7 | Symmetry expansion + recentre → sub-particle set with parent uid | V4 |
| C9 | Spatial facets: nearest neighbours (polysomes, VLP pairs), per-pick radius | V1, R4 |

## 6. Field notes (append; newest last)

One row per thing that hurt. Columns: date, step, what we did, where it hurt, what we did instead, what it asks of
the UI and which stage takes it. Seeded with what earlier campaigns already wrote down:

| # | From | Where it hurt | Asks of the UI |
|---|---|---|---|
| F1 | 412, July | Class 3D said 16.3 Å, the gold-standard refine 35.8 Å; read as a regression for a day | resolutions named by kind (U4) |
| F2 | 412, July | The solvent mask was too thick inside; seen only when the PI opened it in ChimeraX | mask outline over the map's slices (U4) |
| F3 | 412, July | 94 particles over 57 tilt series (~1.6 each): Polish and CtfRefine had nothing to fit | per-tomogram count distribution, a warning on geometry cards (U4) |
| F4 | 412, July | Two refines wrote one output; the map went backwards | (crboost job dirs prevent it) |
| F5 | Copia, Sept | The geometry steps had to be run one at a time and looked at between them, not chained | each geometry step its own card with before/after (U2, C6) |
| F6 | Copia, Sept | The tutorial's 0.15 mask threshold was empty on our map scale | mask job thresholds from the map's own range, shown (C4) |
| F7 | 412, Oct | "Will we have to make 20 different sets?" | sets without a level; facets (roadmap 40 §15, built) |
| F8 | 412, Oct | s001's particles.star carried 6.2 Å per row against 1.55 in tomograms.star | the set writer's checks (built); a red facet line when they disagree (U2) |
| F9 | 412, Oct | job048's shell showed no facets; only slices and a radial profile told why | central slices and radial profile on every map (U4) |

## 7. Order

| Phase | Ribosome | 412 | Code and UI |
|---|---|---|---|
| A | R0 reference; R1 bin-4 exports and Class 3D in all eight | V1 curation | U0, C1, C2, U1 |
| B | R2 consensus at bin 2; hand | V2 shape | C3, C4, U2 |
| C | R3 bin 1, geometry, refine again | V3 consensus per class | C6, U4 |
| D | R4 classes refined on their own | V4 capsomers | C5, C7, C9, U3 |
| E | the paper figures, tables per class and condition | | U5 |

Everything in a phase runs side by side; a phase's code is built while its jobs run. Once a ribosome map and a 412
map exist from the reruns, the old records at `/groups/klumpe/_crboost_archive/agg412_old_records/` can go
(the maintainer's call; roadmap 49 §7).

## 8. Decisions owed

| # | Decision | Recommended |
|---|---|---|
| D1 | Every membership or pose change runs as a crboost job or kitchen set (replaces July's "412 refinement outside crboost"); side runs only for diagnostics, brought back as sets | yes: it is what makes the trace possible |
| D2 | The home project: Slot6_rerun (the pools are there) or a new project for the STA of both species, the eight reruns read only | a new home project, so the eight stay as roadmap 49 left them; S0 checks that a project without tilt series of its own can hold kitchen pools and run particle jobs on foreign tomograms (roadmap 40 §14.8 says it can) |
| D3 | Ribosome cleaning at bin 4 (new exports) or on the bin-2 exports | bin 4 |
| D4 | Geometry by M (a new job, multi-source, refines pixel scale per project) or RELION 5 tomo CtfRefine + frame alignment | M; RELION's as the fallback if M will not take eight Warp sources |
| D5 | 412 symmetry, from V2's C1 map | decided by the map |
| D6 | Delete the TM score and angle maps (~7 TB) now that every candidate is exported | yes, once R1 has fixed the ribosome σ cut and V1 the 412's |
| D7 | The particle map is the same map as preprocessing (one Jobs view, the picker switching between tilt series and sets by phase) or a view of its own | the same map |
| D8 | A condition label per project (tissue, genotype, prep) for the per-class tables | the maintainer supplies them |

## 9. Runtime checklist (per phase)

- **A:** eight bin-4 ribosome exports and Class 3D jobs green in the home project; a class × σ table per project;
  class sets in the kitchen with their parent shown; the 412 curated set made, its count by σ bin.
- **B:** a ribosome Refine 3D on the kept classes of eight projects; PostProcess with a masked FSC; the hand
  decided; the 412 C1 map and the symmetry call. On the map: the class ports and set cards for both species.
- **C:** a bin-1 ribosome map past 6.2 Å masked, before and after geometry; the B-factor plot; a 412 consensus per
  class. The large card shows the three resolutions by name and the mask over the slices.
- **D:** per-class ribosome maps with share per project; a 412 capsomer map past 35.8 Å or the reason it is not.
  Picking any class set on the map traces it back to its projects and σ bands.
