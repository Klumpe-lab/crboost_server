# Roadmap 53 — STA of the reruns: ribosome and 412 classes as far as they go, and the particle map built from doing it

**Status:** scoped 2026-11-17 from the eight finished reruns of roadmap 49; nothing run, nothing built. D1 and D2
taken the same day (§8). Which particle jobs to build: §11; Mask, PostProcess, the mask input and `skip_align`
built the same day (§11.1, not run through SLURM), M's facts read. The particle map scoped against the code: §10;
the handoff for building it as its own view: §13, its decisions and design settled the same afternoon, nothing built:
§13.1. What happens next: §12. Phase A partly run (references, bin-4 exports; no Class 3D yet): §12.1, read 2026-11-18; the nine Class 3D
submitted the same day (§12.1). Class 3D on several GPUs (MPI) and continuing after a time-out: built 2026-11-18,
not run (§11.2). The particle map taken out of the UI 2026-11-18, to come back as part of the branch map (§13.3,
roadmap 32 §15.9). The PARTICLES header's Aggregate button removed and Import tomograms moved to the Tomograms view
the same day (§14.7); the rest of §14 is owed. 2026-11-19: the 412 at 9.2 Å masked on 438 capsids, I1, T = 9 (V3 at
bin 2); R1 six of eight read, keep lists taken, R2 laid out (§12.4); the PI's copia try2 is the same capsid, the gap
is per-tilt CTF (§12.5); the 412 growth run submitted; CtfRefine and M to be built, two routes (§15).

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
- **The science sets the order of the UI.** A card or block is built on the real outputs of a run of the job it
  shows, never on mock data, and lands while the next phase runs. What hurt goes into §6 the day it hurt.
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

| Stage | What | Lands with |
|---|---|---|
| U0 | Field notes (§6), kept from the first run on | all |
| U1 | Data layer: set nodes, class outputs, a uid index per node (§10.2) | phase A, while R1's Class 3D runs |
| U2 | Particle cards on the map: source cards, set cards, class rows with +, the composition magazine (§10.3) | phase A |
| U3 | The inspector: results and particles blocks of the large card per kind (§10.4) | phase B (Class 3D), C (Refine 3D, PostProcess, M) |
| U4 | The trace: the set picker, overlap per card, flow on the cables (§10.5) | phase C |
| U5 | Retire what the map and kitchen cover (Journey particles panel, merge / aggregate dialogs, roadmap 40 §9); Aggregate ripped out and deleted for the kitchen: §14 | after R4; §14 any time (it touches no running job) |

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
| F10 | Phase A step 1, 2026-11-17 | The plan for `AGG412_STA` (tomograms from eight projects) showed "Dose per tilt 3, values from Slot6": one number for a project whose Grid2 tomograms were taken at 4.8. Harmless (only Import and TS Import read the project dose; TM and the particle jobs read each tilt's `rlnMicrographPreExposure` from its source's tilt-series star), but it took a code read to know that | a project from tomograms shows its dose per source (or "per tilt series, from the sources"), never one source's number as the project's |
| F11 | Phase A step 1, 2026-11-17 | The server restarted while the accepted plan was applying; the plan lived only in its memory, so the project was never made and nothing said so | a plan in flight survives a restart, or the restart names the plans it dropped (roadmap 52) |
| F12 | Phase A step 1, 2026-11-17 | The plan showed tilt axis 84.4 and EER 32 "from Slot6"; the project made has −95.0 and no EER grouping, and `get_project` says 0 tilt series beside 897 tomograms. None of it is read by the particle jobs (they read the sources' tilt-series stars), but the plan promised values the project does not hold | the plan shows exactly the values the project will hold; a project from tomograms says "tilt series: in the sources" instead of 0 |
| F13 | Phase A step 2, 2026-11-17 | The kitchen had no API verbs: the all-candidates set, a "by project" group of eight subsets and the pilot's ≥ 10σ set were eleven dialogs by hand, and the agent could only list subsets. Built the same day: `get_kitchen`, `list_kitchen_sources`, `plan_kitchen_set`, `add_kitchen_sources`, subset and group verbs, `make_kitchen_set`, `make_class_set` (`services/sta_kitchen/agent_ops.py`); a recipe goes with each call and the user's filter strip is left alone | (API, built) the class rows' + on the particle map makes the same class sets in the UI |
| F14 | Phase A step 3, 2026-11-17 | Run A's plan rehearsed clean and was refused at its run step: I had named the sets as `particle_set__s00N_…` in `add_job(reading=…)`, not as the candidate `mergedSources:particle_set__…`. Re-pointed with `set_input`, ran at 14:20 | API: `reading` checked against the candidates (HOTFIXES H18), `plan_run` resolving inputs as Run does (H19). Also: a kitchen set's images are taken from finished jobs at Run, so the Class 3D go in a second run after the bin-4 export, never in its chain |
| F15 | Phase A, 412, 2026-11-17 | V1 by hand was skipped on the maintainer's word ("make the subsets yourself"): the 412 pool (22,318 picks) took a by-project group and one set at σ ≥ 8.5 (s011, 1,881 picks), and V2's Class 3D does the sorting the gallery verdicts would have. Its reference is Reconstruct particle of s011 with the TM angles at bin 4 box 128 (the round shell, data-derived) | the per-class σ table must then show junk classes as V1 would have: if Class 3D cannot separate them, V1 comes back |
| F16 | Phase A, 2026-11-18 | On the particle map the phase read as lopsided and unexplained: eight per-project ribosome sets that nothing reads, one Reconstruct particle of one project, one 412 set and no per-project 412 sets. Each is by design (R0's reference is one project's ≥ 10σ picks; s003–s010 wait for R1's Class 3D, which were never added; the 412 is sorted as one pool, F15), but only the roadmap said so | a set that nothing reads says so on its card ("read by no job yet") |
| F17 | V2 reference, 2026-11-18 | Reconstruct particle of s011 with the TM angles (job004) is the shell plus the tomogram's frame. Every 412 TM angle lies in one I1 asymmetric unit (tilt 4–44° on all 1,874 rows of job003), so the missing wedge and whatever surrounds the VLPs average coherently: the outer annulus differs from one side to the other by 0.40 sd in x and 0.26 sd in y, and the power at kz Nyquist is 82× the mean of the kz line (stripes along Z). The ribosome's job002 (C1 angles, tilt 9–171°) shows neither (≤ 0.04 sd, 0.1×). As the reference of a global search, the one-sided density pulls the alignment onto the surroundings | the Reconstruct card says when the poses it used cover only an asymmetric unit, and shows the side-to-side difference; V2 starts from a featureless shell (the rotational average of job004), as §4 V2 first said |
| F18 | Phase A run, 2026-11-18 | The plan for the nine Class 3D said "Point …'s input input_optimisation at automatic" for the steps that choose each project's image source, and its table never named job001 or job003: the one choice Run refuses without was not in the dialog the maintainer accepted (HOTFIXES H21). The jobs read what was meant | the plan, and later the job's card, name where a set's images come from, per project |
| F19 | V2 read, 2026-11-18 | Class 3D's table put the capsid at "0.0 Å" (`inf`) and the blobs at 27–36 Å: read by number, the one real class looked like the failed one. What told the story was the class × tomogram table (class 2 = four tomograms) and the maps' slices and radial profiles, all made by hand | class rows show `inf` as "no estimate", never 0.0 Å; each class row carries its slices, radial profile and a class × tomogram (or project) count (U3 large card) |
| F20 | R1 read, 2026-11-19 | Judging eight Class 3D took the same hand-made tables as F19 (class × σ bin, junk share by σ, whether the keep/drop side is stable while the classes shuffle) and a 96-panel sheet of slices; a job page shows one job at a time | class rows show their σ composition and "same class / same side since it−5"; one sheet of every class of the Class 3D picked on the map, slices side by side (U3) |
| F22 | 412 against the PI's, 2026-11-19 | "Why is the PI's 8.8 Å better?" took a by-hand FSC with matched masks: his mask holds the outer layer (215–271 Å), ours both layers, and our inner layer alone is at 21 Å. Two masked FSCs of the same capsids differed by what the masks held | a masked resolution names its mask's radial extent; the Mask and PostProcess cards show the FSC per radial shell (inner / outer layer) beside the masked one |
| F21 | R2 planning, 2026-11-19 | R1's poses cannot follow its picks into R2: a set's images come whole from an export's rows (the TM poses), poses come from one source for the whole set, and R1 is eight Class 3D. R2 starts from the TM angles again: harmless here (R1 converged from them at `sigma_ang` 5), but only a code read said so | the Refine 3D card and the plan say where the starting poses come from, per project ("template matching, via the rerun's Warp export") |
| F23 | C10 S0, 2026-11-19 | RELION's tomo CtfRefine takes its reference at the tilt series' pixel size and says nothing when it is not: the reference loader only pads to the box ("Padding reference from 448 to 512") or refuses a larger one; the pixel-size checks in the binary are relion_refine's. Our refinements are at bin 2, so the reference is a Reconstruct particle at binning 1 with its own Mask and PostProcess, three jobs before the one asked for | the CTF refine job refuses a reference off the tilt images' pixel size, naming the fix; its card shows what moved per tilt series (built, §15.5) |
| F24 | C10 first run, 2026-11-19 | The CTF refine failed on a 20 Å reference and nothing upstream said so: Reconstruct particle's card read "20.7 Å", PostProcess job027 20.4 Å, and the run went on to CtfRefine. The driver's levers (regularise, narrow the range) were wrong for the case: regularisation was on, and the range does not bound the astigmatism step. Finding the cause took a by-hand comparison against job030 and the PI's job040 (§15.7) | CTF refine checks the reference's FSC before RELION and names it in the failure (not built) |

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
| D1 | Every membership or pose change runs as a crboost job or kitchen set (replaces July's "412 refinement outside crboost"); side runs only for diagnostics, brought back as sets | **taken (maintainer, 2026-11-17)** |
| D2 | The home project: Slot6_rerun (the pools are there) or a new project for the STA of both species, the eight reruns read only | **taken (maintainer, 2026-11-17): a new home project**, made through the API (§12) |
| D3 | Ribosome cleaning at bin 4 (new exports) or on the bin-2 exports | bin 4 |
| D4 | Geometry by M (a new job, multi-source, refines pixel scale per project) or RELION 5 tomo CtfRefine + frame alignment | **taken (maintainer, 2026-11-19): both**, as two routes, CtfRefine first (§15) |
| D5 | 412 symmetry, from V2's C1 map | decided by the map |
| D6 | Delete the TM score and angle maps (~7 TB) now that every candidate is exported | yes, once R1 has fixed the ribosome σ cut and V1 the 412's; **decidable 2026-11-19** (§12.4: no σ cut, nothing planned reads the old maps) |
| D7 | The particle map is the same map as preprocessing (one Jobs view, the picker switching between tilt series and sets by phase) or a view of its own | **taken (maintainer, 2026-11-17): a view of its own**, opened from the PARTICLES header (§13, §13.1); the Jobs map keeps showing particle jobs meanwhile |
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

## 10. The particle map, scoped against the code (2026-11-17)

The preprocessing branch map is in main (`ui/branch_map/`, roadmap 32 §15.8 S1–S3). The particle half is the same
view, the same cards and the same look; what is new is what a node is, what flows along a cable, and what the large
card shows.

### 10.1 What the map does today (read 2026-11-17)

- Nodes are jobs only. `project_from_state` (`ui/branch_map/model.py:738`) makes one `Job` per entry of
  `state.jobs`. An input that reads a particle set (`Analytics/sets/<id>/`, where the kitchen's Make set publishes,
  `services/particles/particle_sets.py`) is drawn as a named value with a cable to the set's first recipe parent
  (`facts.set_parent`, `ui/branch_map/facts.py:199`). The set is not a node; a parent in another project gets no
  cable at all.
- `facts.job_facts` (`facts.py:212`) knows the particle kinds as far as Warp export (one count line) and Reconstruct
  particle (three central slices, `map_slices_url`). Class 3D and Refine 3D show nothing.
- The magazine above a card is one mark per tilt series (`card.strip_svg`, `series_states`); the picker
  (`picker.py`) picks a tilt series; the large card's third block is that tilt series' facts (`blocks.ts_parts`).
- What carries over as it is: card geometry and look, cables, columns by depth (`geometry.tidy`), the + per output
  (`card.output_row`, `Project.offered_after`), the new-job card and its ✓ (`view._add_new_job`), the facts store
  with its mtime cache and threaded loads (`FactsStore`), the SVG plots (`plots.svg_bars`, `svg_hist`, `svg_curve`,
  `svg_stack`).
- A pick's uid is a hash of project, frame, member and position (`services/sta_kitchen/table.uid_of`), so the uids
  of a foreign template-match job's picks can be computed, not only read; rows a job made from a kitchen set carry
  `cbPickUid` (the set writer in `services/particles/set_facets.py`).

### 10.2 Data layer (U1): `services/particles/lineage.py`, no UI

- **Nodes:** the home project's jobs (as today); its sets (`particle_sets.list_sets`); one source node per foreign
  (project, job) that a set's recipe or a kitchen pool member names. A class is not a node: it is an output row of
  its Class 3D node.
- **Edges:** a set ← its recipe parents (jobs, `(job, class n)`, sets); a job ← what its input reads (`paths`,
  `source_overrides`, the facet choices on the job).
- **uid index:** per node a sorted int64 array of its uids, built in `run_in_worker` (a new `WorkKind`), cached by
  the producing star's mtime: a set from its `particles.star`; a Class 3D per class from `run_itNNN_data.star`
  (`rlnClassNumber`, `cbPickUid`); a Refine 3D or M from its particles; a foreign source from its `candidates.star`
  through `uid_of`. The overlap of two nodes is `np.intersect1d` on two sorted arrays: milliseconds at 350k.
- **Counts per node:** total, by project, by tomogram; σ per uid from the species' pool table where there is one.
- **Facts read per kind:** Class 3D `run_itNNN_model.star` (class distribution, resolution estimate and accuracy per
  class) and `run_itNNN_classNNN.mrc`; Refine 3D the gold-standard FSC per shell and the resolution of every
  iteration from the model stars, `run_class001.mrc` and the half maps; PostProcess `postprocess.star` (masked,
  unmasked and phase-randomised FSC, B-factor, final resolution); M as §11's facts say.
- **C1, class → set,** uses the same reader: a set made from class rows is an ordinary ParticleSet whose recipe
  names `(job, class n)`. Roadmap 30 S4 built a class subset (never run): reuse what holds.

### 10.3 The cards (U2)

- **Source cards.** The leftmost particle column, one per foreign project, folded to one line each: "Slot6_rerun ·
  TM picks job018 · 1,513 · images: Warp export job020, bin 2, 256". The home project's map says where its
  particles come from without opening eight projects. Its header opens that project's own map (read only).
- **Set cards.** Slimmer than a job card: name, count, species colour, how it was chosen in one line ("σ ≥ 8.5,
  1,204 kept by hand", "class 3 of job012", "classes 1 + 3 of job012"). The + offers the jobs that read sets (Warp
  export, Reconstruct particle, Class 3D, Refine 3D) and "subset in the kitchen" (opens it with this set as a
  filter).
- **Class 3D cards.** One output row per class (`card.output_row`): "class 2 · 41,203 · 38 % · 11.2 Å (Class 3D
  estimate)" and a small slice thumbnail. The + on a row offers Make set from this class, or a job reading it (the
  set is made with the job's ✓). Refine 3D, PostProcess and M have one output row: map and poses.
- **The magazine is composition.** One segment per project, width by count, a colour per project (per condition
  once D8 is in); a Class 3D card stacks its class bars under it. Same band height as the tilt-series strip
  (`strip_band`), so preprocessing and particle cards line up.
- **Layout.** The particle region continues the columns by depth from the source cards; set cards take a narrow
  column between producer and reader (`geometry.tidy` gets a width per node kind).

### 10.4 The inspector: the large card per kind (U3)

Three blocks as today; on particle cards the third, TILT SERIES, becomes PARTICLES.

- **PARAMETERS:** unchanged, grouped as in the job tab.
- **RESULTS:**
  - Warp export: count, box · Å · bin, picks dropped and why; eight particle images (the zero tilt).
  - Reconstruct particle: the slices (exist) and the radial profile (the curve that explained job048).
  - Class 3D: a table of classes (count, %, estimate, slices); assignments changing per iteration; class × σ band as
    stacked bars (R1's read-out); class × project (each project's share).
  - Refine 3D: the FSC curve with the 0.143 line, its number labelled "unmasked half maps"; resolution and
    accuracy per iteration; slices with the mask's outline over them; the angular distribution as a rot × tilt
    histogram.
  - PostProcess: masked, unmasked and phase-randomised FSC on one plot, "masked" on the number, B-factor; slices of
    the sharpened map with the mask's outline.
  - M: resolution per iteration; per source (project) the refined pixel scale and the mean defocus change; FSC before
    and after.
- **PARTICLES** (every particle card): counts by project → tomogram; particles per tilt series as a histogram with a
  mark where geometry refinement gets thin (F3); σ histogram of the node's uids; depth in the tomogram; twelve
  cutouts from the kitchen's tile cache (`services/sta_kitchen/tiles.py`); "open in the kitchen" with the node's
  uids as a set filter (`services/sta_kitchen/setmatch.py`).
- No bare Å anywhere: every resolution says which kind it is (F1).

### 10.5 The trace (U4)

- The picker above the map gets a particles mode: set chips and class rows instead of tilt-series cards, switched
  by a two-word toggle ("tilt series · particles") or by where the map is scrolled.
- Picking a node: every particle card shows "holds 1,203 of 2,000" as a thin bar under its header, cards holding
  none fade, cables widen with the uids they carry. Upstream (which projects, σ bands, earlier classes they came
  from) and downstream (which classes they ended in) at once.
- A set of one: "trace this particle" from the kitchen's right-click menu follows one uid through every job that
  read it: the particle's Journey.
- Overlaps from the uid index in a worker, cached per (picked, node).

### 10.6 Off the loop

As the map today: `BranchMapView.signature` adds the sets root's mtime and `operations.jsonl`; facts through
`FactsStore.load_job` with the new kinds; the uid index and overlaps in `run_in_worker`; no NiceGUI element per pick.

### 10.7 Before it, and not here

- Before: roadmap 32 §15.8's runtime checklist on the preprocessing map (open since 2026-11-12); the particle
  region stands on the same view.
- Not here: selecting particles on the map (the kitchen selects); maps in 3D in the browser (slices only); lineage
  across two home projects.

## 11. Which particle jobs to build (answer, 2026-11-17)

**Build:** class → set (C1), Mask from a map (C4), PostProcess (C3), M refine (C6), Class 3D focus mask and
`skip_align` fields (C5), symmetry expansion + recentre (C7, phase D). **Not now:** RELION 5's tomo CtfRefine and
frame alignment (reversed 2026-11-19: both are built, §15).

Why M and not RELION's geometry tools:

- Everything upstream is Warp: motion, CTF, alignment, missAlign's output, the exports. M refines in Warp's own
  metadata (the tilt-series XMLs), and a Warp export after it picks the refined geometry up. RELION's tools refine
  RELION's copy of the geometry, which our next Warp export would not read.
- On copia, RELION's CtfRefine on Warp-made metadata moved every tilt by −1 µm, a convention offset, not a
  measurement (2026-09-09). M stays in one convention.
- M does what neither RELION tool does and these data need: a pixel scale per source (eight sessions, 2024–2026),
  image warp grids per tilt, and later the ribosome and the 412 refined together in the same tilt series.
- RELION's tools stay the fallback if M's S0 shows it cannot take the eight projects as sources.

**The jobs, sized:**

| # | Job | Inputs → outputs | Size |
|---|---|---|---|
| C1 | Class → set | a Class 3D's data star + the classes → a set (§10.2) | small (roadmap 30 S4 may hold most of it) |
| C4 | Mask | a map; low-pass, threshold shown against the map's own range (F6), extend, soft edge → `mask.mrc`; the command builder of `services/templating/template_service.py` (`relion_mask_create`) reused, as a job so it is on the map | small |
| C3 | PostProcess | half maps of a Refine 3D or M + a mask; auto or given B-factor → `postprocess.mrc`, `postprocess.star` | small |
| C5 | Class 3D fields | `skip_align`, a focus mask | small |
| C6 | M refine | a Refine 3D result of a set (half maps, mask, particles) + the Warp tilt-series folders of the set's projects → refined maps and particles; refinement switches as fields (poses, warp grid, defocus, pixel scale), iterations | large |
| C7 | Symmetry expand + recentre | a refined set + symmetry + an offset → a sub-particle set whose rows carry the parent uid | medium |

**M's S0 (read only, before its design):** the MTools / MCore commands in the dev41 Warp image (July found them in
the dev36 image, never run); how a population takes a source per project; where M writes. M updates the source's
tilt-series XMLs, and those live in the eight reruns, which the home project never writes (§2). The likely shape: the
home project holds the population and copies of the XMLs, images read in place from the reruns; whether M allows that
is the first fact. Then the species import from a RELION Refine 3D, and what M's outputs look like for the card.

## 12. What happens next (pickup)

1. **The home project, through the API.** One plan, confirmed in the terminal: `AGG412_STA` in
   `/groups/klumpe/_crboost_data_api`, `tomograms_from` the eight projects' Reconstruct jobs (every tomogram;
   acquisition values come with them; the doses differ, Grid2 4.8 against ~3, which may come back as a need), and
   both species from the catalog. Nothing is computed: it is the registry of ~890 tomograms the kitchen and the
   jobs check against.
2. **The kitchen, in the UI (the maintainer).** In `AGG412_STA`, each species' pool with the eight projects'
   template-match picks as members, as was done in Slot6_rerun. Ribosome: Make set of every candidate, and a group
   "by project" with one subset per project. 412: V1 begins, ≥ 8.5σ, looking and verdicts.
3. **Phase A ribosome jobs, through the API** (one plan, `plan_run` in it): Reconstruct particle of the pilot's
   ≥ 10σ picks (R0's reference, low-passed to 40 Å); one Warp export at bin 4, box 100 on the all-candidates set;
   eight Class 3D, one per project subset, images from that export, K 4, C1, Ø 350, `ini_high` 40, T 4, 25
   iterations, local search around the TM angles.
4. **Code meanwhile, on a branch the maintainer names, on their word:** C1, U1 and U2 first, so that R1's classes
   appear on the map as sets when the Class 3D jobs finish; then C4 and C3 for phase B; M's S0 facts, read only.
5. **Field notes** in §6 from step 1 on.

### 12.1 Where phase A stands (read 2026-11-18)

Steps 1–2 done, step 3 half done: the references and the bin-4 exports ran, no Class 3D was ever added.

- **Home:** `AGG412_STA` in `/groups/klumpe/_crboost_data_api`, made through the API; in both kitchens the eight
  reruns' template-match picks are the members.
- **Ribosome:** a group "by project" (eight subsets, all of each project's candidates); sets s001 every candidate
  (346,834), s002 the pilot's ≥ 10σ (2,870), s003–s010 one per project from the subsets. Warp export job001 (s001,
  bin 4 box 100: 346,381 rows, 884 tilt series) and Reconstruct particle job002 (s002, bin 2 box 200, R0's
  reference; a ribosome, no frame artefacts) are green. s003–s010 have no reader: they are R1's eight Class 3D.
- **412:** a group "by project" (eight subsets, no set made from them); set s011, σ ≥ 8.5 (1,881: unmilled2 584,
  FCIsolation 348, Slot6 265, Grid2 240, SK 224, 20251113 169, Grid3 41, pilot 10). Warp export job003 (s011, bin 4
  box 128: 1,874 rows, 329 tilt series) and Reconstruct particle job004 (s011, bin 4 box 128) are green; job004 carries
  the tomogram's frame (F17) and is not V2's reference as it stands.
- **The next run** holds R1's eight Class 3D and V2's one. What it has to settle:
  - Each Class 3D reading a kitchen set finds two image sources per project, the rerun's own bin-2 export and the
    home's bin-4 one, and Run refuses until the job names the home export (`set_input` facet, per project).
  - The class3d profile (64G, 8 h) is short for the large projects. A bin-4 box-100 particle is 0.78 MB on disk (39
    tilts, float16): unmilled2 69 GB, SK 53 GB, 20251113 39 GB, FCIsolation 35 GB, twice that as float32 in RAM with
    `preread_images`; and Class 3D does not continue after a time-out (Refine 3D does).
  - Open values: R1's `sigma_ang` (the ribosome TM step was 12°); V2's K (4–6), mask Ø (the shell's outer edge is at
    ~260 Å in job004's radial profile), and its reference.
- **Taken 2026-11-18 (maintainer):** R1 `sigma_ang` 5° at healpix order 3; V2 K 6; V2's reference the rotational
  average of job004, made as a side step (§12.2); the large projects' Class 3D at 192G on `g_medium` (2 days). V2's
  mask Ø: "slightly over, never under". 575 Å is the 412's Ø everywhere in crboost (the species, its spherical mask
  `…_sphere_d575_s5`, the old Class 3D runs, job003's export); July's refine moved its particle diameter from 550 to
  600 on the PI's word ("550 was a bit tight"); the shell turns negative at 267 Å radius (§12.2).
- Asked before anything runs: why R1 runs per project when the maps come from every good particle. R1 only takes out
  what template matching mistook for ribosomes; its kept classes of all eight become one set for R2, and the states
  are classified on that pooled set (R4). Answered in the session; the maintainer took per project, and V2's mask
  at 600 Å ("to be sure").
- **Submitted 2026-11-18 09:39** (plan `p-f6ba241c`, accepted in the terminal), all nine on `g_medium`, 2 days:

  | Job | Reads | Particles | Images | Reference | SLURM |
  |---|---|---|---|---|---|
  | class3d · job005 | s003 Slot6 | 24,100 | job001 | job002 | 1776668 |
  | class3d__2 · job006 | s004 FCIsolation | 44,774 | job001 | job002 | 1776669, 192G |
  | class3d__3 · job007 | s005 20251113 | 49,511 | job001 | job002 | 1776670, 192G |
  | class3d__4 · job008 | s006 pilot | 15,620 | job001 | job002 | 1776671 |
  | class3d__5 · job009 | s007 unmilled2 | 88,459 | job001 | job002 | 1776672, 192G |
  | class3d__6 · job010 | s008 Grid3 | 35,586 | job001 | job002 | 1776673, 192G |
  | class3d__7 · job011 | s009 Grid2 | 20,536 | job001 | job002 | 1776674 |
  | class3d__8 · job012 | s010 SK | 67,795 | job001 | job002 | 1776675, 192G |
  | class3d__9 · job013 | s011 412 ≥ 8.5σ | 1,874 | job003 | the shell (§12.2) | 1776676 |

  R1: K 4, C1, Ø 350, `ini_high` 40, T 4, 25 iterations, `sigma_ang` 5 at healpix 3 (39 orientations a particle in
  the first pass). V2: K 6, C1, Ø 600, global at 7.5° (36,864 orientations), `ini_high` 40, T 4, 25 iterations.
  RELION took job002 (bin 2, box 200) for bin-4 images under `--trust_ref_size`. First iterations on a B200: the pilot
  ~11 min, Slot6 ~16 min (about 0.65 min per thousand particles), so ~5 h for the pilot to roughly a day for
  unmilled2; Grid3 (~10 h) would not have fitted in 8 h.
- **Iteration times at 10:00** (RELION's estimate, B200 unless named): pilot 8.8 min, Slot6 14.4, Grid3 17.4,
  Grid2 (A100) 20.6, FCIsolation 21.7, 20251113 24.8, unmilled2 46.2, SK (A100) 70, the 412 (A100) 135. At 25
  iterations: ~4 h to ~19 h for the first seven, SK ~29 h, the 412 ~56 h: past `g_medium`'s 2 days, and Class 3D
  does not continue after a time-out (Refine 3D does, `drivers/refine3d.py`). The 412's global pass (36,864
  orientations × 81 translations × 6 classes) is the same every round, so later rounds will not be cheaper.
  Put to the maintainer: Class 3D continuing from its last iteration as Refine 3D does, or the 412 job again on
  `g_long`. **Taken:** the 412 job cancelled at 10:28 (SLURM 1776676, in its first iteration) and queued again at
  10:33 as SLURM 1779904: `g_long`, 7 days, full B200s only (`constraint b200`; the profile's `avoid mig` excludes
  the MIG nodes). B200 against A100 on the ribosome jobs: ~0.6 against ~1.0 min per thousand particles a round.
- **Where the 412's time goes:** the first pass tries 36,864 orientations × 81 offsets × 6 classes = 17.9 M per
  particle (RELION's own count). The offsets are a sphere of 2.5 steps of 2 px (`offset_range` 5, `offset_step`
  2, in pixels: the job tab's "Å" is wrong); at `offset_range` 3 they are 19 (4.3× fewer). MPI (one
  `relion_refine_mpi` follower per GPU, as Refine 3D runs) divides the particles between GPUs.
- **10:45** the 412 job queued again with `offset_range` 3 (SLURM 1779940). **10:58**, after the restart onto
  `24e547c` (the MPI build committed), queued again as the first MPI Class 3D: `mpi_ranks` 3 (two followers sharing
  its one B200), 10 CPUs, `g_long`, SLURM 1780088.
- **Iterations slow as the resolution climbs** (minutes per iteration, from the model stars' times): pilot 10, 8, 12,
  12, 14, 13; Slot6 14, 14, 18, 19; Grid3 17, 19, 25; Grid2 20, 21, 25. At ~1.6× the first, the 25 iterations come
  to ~5.5 h (pilot) to ~29 h (unmilled2), and ~46 h for SK on its A100, at `g_medium`'s 2 days. If SK stops there,
  Run continues it from its last finished iteration (§11.2: the re-run takes the new driver). Classes at 11:00 still
  near even (pilot it006: 23 / 29 / 18 / 30 %, 24 Å Class 3D estimate; Slot6 it004: 21 / 30 / 31 / 18 %).
- **V2 done (job013, SLURM 1780088): the first MPI Class 3D.** Started 11:33 on a B200 (cbe-g5-3), 25 iterations by
  12:50: 1 h 17 min against the 2.25 h *per iteration* one process estimated on an A100 with 81 offsets (here 19
  offsets, two followers on one card, `mpi_ranks` 3, 10 CPUs). Iterations ~3 min after the first (11 min with
  loading). Classes at it025 (share, Class 3D estimate, rotational accuracy): 1 37.3 % 27.4 Å 1.4°; 2 23.4 % "0.0 Å"
  0.1°; 3 2.1 % 36.1 Å; 4 17.3 % 28.3 Å; 5 5.9 % 34.5 Å; 6 14.1 % 30.5 Å. Class 2's 0.0 Å with a quarter of the
  particles is RELION's estimate failing, not a number: look at its map before reading anything into it. Not looked
  at yet: the maps (particle map, job013's Results), size per class, D5.

### 12.2 V2's starting reference: the rotational average of job004 (2026-11-18)

A side step under D1: made outside crboost, it comes back as a job input (a template of the 412 species, added in
the run's plan) and is recorded here.

- **Why.** V2's Class 3D searches every orientation, so its first round turns each particle to whatever matches the
  reference best. job004 (Reconstruct particle of s011 with the TM angles) carries a one-sided density in the
  tomogram's frame (F17): all 412 TM angles sit close to the identity (tilt 4–44° on every row; rot + psi within ±30°
  on 1,724 of 1,874 rows and within ±60° on all), so whatever lies on one side of the VLPs in the tomograms adds up
  instead of cancelling. The
  shell itself has no direction, so that density would be the only thing the first round could align on: the
  particles would be oriented, and then classified, by their surroundings. Its stripes along Z (the missing wedge, at
  Nyquist) go with `ini_high` 40; the one-sided density sits at the shell's outer edge, inside the mask, and does not.
  The rotational average keeps what does not depend on direction (radius, the wall's two layers, contrast, all from
  these particles) and nothing that does: §4 V2's "featureless shell at the measured radius".
- **How.** `notes/sta_side_steps/412_shell_reference/make_shell.py` (`notes/` is not tracked): low-pass job004's
  `merged.mrc` at 25 Å (raised cosine from 30 Å, so the Nyquist stripes do not weigh on the few voxels of the smallest
  radii; Class 3D cuts at 40 Å, so nothing it reads changes), then the mean over spherical shells 0.5 px thick, written
  back by radius; box 128, 6.2 Å/px, protein white as in job004. Redo:
  `./crboost python notes/sta_side_steps/412_shell_reference/make_shell.py
  /groups/klumpe/_crboost_data_api/AGG412_STA/External/job004/merged.mrc notes/sta_side_steps/412_shell_reference`.
- **Out:** `412_shell_rotavg_job004_apix6.20_box128.mrc`, `profile.tsv` (job004 low-passed and the shell, per px),
  `job004_vs_shell_central_slices.png`, beside the script.
- **Checked:** side-to-side differences of the outer ring 0.00 / 0.00 / 0.00 sd (job004 −0.40 / +0.26 / −0.11); power
  at kz Nyquist 0.0 × the kz line's mean (job004 82×); radial profile the same as job004's low-passed one (r ≥ 3 px,
  correlation 1.000). The profile: lumen +0.023 to +0.025, the wall's main layer peaking at 161–174 Å, a second layer
  at ~225–235 Å (July's two sublayers at 180 and 223 Å, 412 playbook "mask lever"), zero at ~262 Å, negative from
  267 Å (−0.020 at 298 Å).

### 12.3 V2's classes, read 2026-11-18

job013 (Class 3D, s011, 1,874 picks, K 6, C1, from the featureless shell). Scripts, slices and profiles in
`notes/sta_side_steps/412_v2_classes/` (`tables.py`, `maps.py`, `sym.py`, two PNGs).

- **One class is the capsid, the rest are not.** Class 2 (438, 23 %) is a two-layer faceted shell: inner layer
  peaking at ~170 Å, outer at ~236 Å, zero at 260 Å (Ø ~520 Å), the lumen below zero. Classes 1, 4, 6 (697, 324,
  265) are solid blobs out to ~260–300 Å, two of them cut by a straight dark edge (a lamella surface or membrane);
  3 and 5 (40, 110) are noise with one bright lump. Classes 1 and 4 also hold picks closer than 450 Å to each other
  (172 of 532 and 152 of 291 with a neighbour in the same tomogram), as template matching does along a continuous
  surface; class 2 has none (nearest neighbour median 533 Å: capsids touching, never overlapping).
- **Class 2 is icosahedral (D5).** A search over 3,000 axes for the class-2 map's self-correlation under 72°
  rotation (shell 124–273 Å, bin 8) gives exactly six peaks at CC 0.90–0.91 (next 0.745, median 0.69), at 61.6–65.6°
  from each other (63.4° in I). The symmetry grew from a reference without any: no model bias in it.
- **Class 2 lives in four tomograms.** 421 of its 438 picks: Grid2 Position_10 (171 of that tomogram's 172), Slot6
  Position_10 (108 of 110), SK Position_11_2 (96 of 97), FCIsolation Position_18_4 (46 of 46); the rest from 8 more.
  Per project: Grid2 179, Slot6 108, SK 96, FCIsolation 48, unmilled2 7, 20251113, Grid3 and pilot 0. unmilled2's
  584 picks (a third of s011) are almost all classes 1 and 4. The other side of F3: ~100 particles a tilt series in
  the four, so per-tilt-series geometry (M, CtfRefine) has something to fit this time.
- **RELION's numbers.** Class 2's estimated resolution is `inf` (it was at it008 and it010 too) with rotational
  accuracy 0.11° and max-probability 1.0 on its median particle: the estimate fails, the alignment does not. The
  27–36 Å of the other classes are the resolution of blobs. 96 % of particles kept their class from it024, 86 %
  from it020: converged. The raw TM score (`rlnLCCmax`) is 0.09 on class 2's median pick against 0.06 elsewhere.
- **What it asks.** A class set of class 2 (the maintainer's pick); the class-2 map on RELION's I1 axes
  (`relion_align_symmetry`, no crboost job does it: a side step like §12.2, back as a 412 template) and a mask from
  it, thinner at the lumen; Refine 3D I1 on the reruns' bin-2 exports (box 256), then PostProcess: V3 at once, no
  C1 Refine 3D first (the symmetry is already shown above). V1 comes back as growth, not cleaning: the four
  tomograms below 8.5σ, where touching capsids may have scored lower than free ones.
- **One size (the pleomorphism question).** Each pick cut from its own tomogram (6.2 Å/px, 694 Å cube, voxels within
  30° of the xy plane so the missing wedge stays out), radial profile, best radial scale against class 2's mean
  profile (`sizes.py`, `fit.py`, `sizes.png`, `sizes.csv`). Class 2 (424 of 438 inside the tomogram): scale 1.000,
  5–95 % 0.990–1.010, i.e. outer layer at 247 ± 2.5 Å; profile correlation 0.98 on the median particle, > 0.8 on
  420. Two T numbers would differ by 15–30 % in radius: none here. Per tomogram the medians differ by 1 %
  (Grid2 1.005, Slot6 1.000, SK and FCIsolation 0.995), which looks like pixel size per session, for M. Outside
  class 2, 59 picks have a capsid's profile (correlation > 0.8; 42 in class 1, 10 in class 6), the same size,
  one to three a tomogram, mostly unmilled2 and FCIsolation: lone capsids Class 3D did not put in class 2. The
  size is homogeneous in what was picked; TM's template has one size, so a capsid of another size could be
  missing from s011, not sorted wrong.
- **Taken (maintainer):** class 2 alone (set s012, 438). The I1 side step:
  `notes/sta_side_steps/412_class2_I1/`, `relion_align_symmetry --sym I1 --angpix 6.2 --nr_uniform 2000` on
  job013's class 2 (ROT 211.9, TILT 68.3, PSI 269.9; the aligned map against its I1-symmetrised copy CC 0.955 over
  the shell), `relion_image_handler --rescale_angpix 3.1 --new_box 256` for the bin-2 reference; registered as a
  412 template. Mask threshold fraction 0.15 (both layers, 140–265 Å, lumen out; the default 0.4 keeps half the
  outer layer).
- **Submitted 13:51** (plan `p-e1b4bf07`, accepted in the terminal):

  | Job | Reads | What | SLURM |
  |---|---|---|---|
  | maskCreate · job016 | the I1-aligned map (bin 2) | threshold 0.15 × max, extend 3, soft 6 | 1784089 |
  | refine3d · job015 | s012, images: the five reruns' own 412 Warp exports (bin 2, box 256) | I1, global (`sigma_ang` −1: the poses are in job013's frame, not the aligned one), Ø 600, `ini_high` 40, mask job016 | 1784090 |
  | postprocess · job017 | job015 half maps, mask job016 | masked FSC, no sharpening (auto B off: the fit wants resolution past 10 Å) | 1784091 |
  | class3d__10 · job014 | s012, images job003 (bin 4) | C1, K 3, from the featureless shell again, global, as job013 otherwise: pleomorphism in shape | 1784088 |

- **What came back (14:30).** job015 ran on job003's **bin-4** images, not the reruns' bin 2: a class set keeps its
  Class 3D's images and the per-project choice was silently ignored (HOTFIXES H22; the kitchen cannot remake the set
  either, H23). RELION resized reference and mask to 6.2 Å / 128 itself, so job015 is a sound I1 refinement: 17
  iterations, angles 0.35°, offsets 1.3 Å, **12.6 Å masked = Nyquist at bin 4**. PostProcess job017 refused the
  bin-2 mask against bin-4 half maps (its own check).
- **Shape (job014, C1 K 3).** Classes 2 and 3 (31 %, 37 %) are the same icosahedral capsid (six 5-folds at CC
  0.79–0.88), and split by tomogram: Slot6 P10 mostly 2, SK P11_2 mostly 3. Class 1 (32 %) is 132 of Grid2 P10's 171:
  same radii (173 / ~236 Å), outer layer smeared, 5-fold CC 0.44 against a 0.36 median. Not a second form in any
  sense the map can show; Grid2 P10 (the MyrGFP grid, 2026-03-11 session) aligns worse. Open: that tomogram's quality,
  or something per session (tilt hand, pixel size: its capsids measured 0.5 % larger). Grid2's tomograms carry the same
  declared `rlnTomoHand` −1 as the rest.
- **Submitted 15:00** (plan `p-453359b8`, accepted): Warp Export warpExport__3 reads job015 (its I1 poses and centres,
  origins folded in) at bin 2, box 256, Ø 575 (clone of job003); Refine 3D refine3d__2 reads it, reference job015's
  map, mask job016, local (`sigma_ang` 2, healpix 4), `ini_high` 25; PostProcess job017 re-pointed at it. Two stray
  Scheduled jobs (TS CTF, Reconstruct, never run, origin unknown) taken off the roster in the same plan.

### 12.4 Where both tracks stand, read 2026-11-19

Scripts and sheets in `notes/sta_side_steps/r1_and_v3_read/` (`tables.py`, `keep.py`, `maps.py` → `r1_classes.png`;
`v3.py`, `v3map.py` → `v3_map.png`, `tnum.py` → `v3_lattice.png`).

**412, V3 at bin 2: 9.2 Å.** The 15:00 chain ran (the PostProcess is job020 now): Warp Export job018 cut s012's 438
capsids from the 12 tilt series holding them (bin 2, box 256, job015's I1 poses); Refine 3D job019 converged in 16
iterations at a working resolution of 10.9 Å, angles 0.32°, offsets 1.0 Å; PostProcess job020 (mask job016, no
sharpening): **9.23 Å masked gold-standard FSC 0.143**, 9.8 Å on the unmasked half maps, FSC 0.5 at 10.7 Å; the
phase-randomised masked FSC is ~0 past 15 Å, so the mask adds nothing. July's was 35.8 Å on 94 particles.

- **Alignment-limited, not Nyquist-limited.** The FSC falls from 0.30 at 9.45 Å to 0.04 at 9.0 Å, just past the working
  resolution, with bin 2's Nyquist at 6.2 Å: bin 1 is not the next lever; more capsids and geometry are.
- **T = 9.** On the outer layer (225–255 Å, low-passed to 20 Å) the local minima, one per 11° neighbourhood, are 82:
  20 on the 3-fold axes and 60 at 20–21° from a 5-fold and 11–12° from a 2-fold, a third of the way along each
  5-fold–5-fold edge (63.4° / 3 = 21.1°); the pentamers are bright rings on the twelve 5-folds. 12 pentamers + 80
  hexamers = 92 capsomers = 10T + 2, T = 9 (h 3, k 0). Radial profile: inner layer 167–174 Å, outer layer peak 239 Å,
  zero at ~262 Å. For V4, per capsid: 12 pentamers (C5), 20 hexamers on the 3-folds (C3), 60 general hexamers; 438
  capsids give 5,256 + 8,760 + 26,280 sub-particles.
- **Composition.** 421 of 438 from four tilt series (Grid2 P10 171, Slot6 P10 108, SK P11_2 96, FCIsolation P18_4 46),
  the rest 1–8 a tilt series. The capsid radius per session (§12.3: Grid2 1.005, Slot6 1.000, SK and FCIsolation 0.995)
  is a ±0.5 % pixel size between sessions, 2.4 Å at the outer layer: a lever only a pixel scale per source reaches (M).
- **Growth within the candidates.** s011 was σ ≥ 8.5 (1,881 of 22,318). Outside class 2 it holds 59 picks with a
  capsid's profile, and the four rich tilt series may hold touching capsids below 8.5σ. Class 3D in I1 (60× fewer
  orientations than C1) of every candidate at bin 4, the capsid as one reference, sorts them cheaply; a template
  match with this map is the route to capsids the old template scored below the candidates' floor.

**Ribosome, R1: six of eight read.** Kept and dropped, from the class maps' slices (`r1_classes.png`) and the class
statistics:

| Project | Job | Kept | Kept / all | Dropped |
|---|---|---|---|---|
| Slot6 | job005 | 1, 2 | 15,410 / 24,100 | 3 blob (32.5 %), 4 lamella edge (3.5 %) |
| FCIsolation | job006 | 3, 4 | 24,437 / 44,774 | 1 blob (42.8 %), 2 lamella edge (2.6 %) |
| 20251113 | job007 | 1, 2, 3 | 31,172 / 49,511 | 4 blurred ribosome-like blob (37.1 %) |
| pilot | job008 | all | 15,620 | none formed |
| Grid3 | job010 | all | 35,586 | none formed |
| Grid2 | job011 | 1, 3, 4 | 14,406 / 20,536 | 2 noise (29.9 %) |
| unmilled2 | job009, it017 of 25 | 1, 3 so far | 53,688 / 88,459 | 2 noise (38 %), 4 lamella edge (1.3 %) |
| SK | job012, it013 of 25 | all four so far | 67,795 | none yet |

Kept classes: 15–17 Å Class 3D estimates, angles 0.4–0.9°; dropped: 20–27 Å, angles 2.4–2.7° (the edges excepted),
median 7.6σ against ~9σ.

- **Junk against ribosome is converged, the classes of ribosomes are not.** 96–99 % of picks are on the same side from
  it020 to it025; 45–72 % are in the same class. At 15–25 Å the ribosome classes are arbitrary splits: the states are
  R4's, on the pooled set.
- **The σ read-out (R1's question).** In the five projects with a junk class, the junk share is 71 % at 7–8σ, 38 % at
  8–9σ, 16 % at 9–10σ, 6 % at 10–12σ, 1 % above 12σ. No σ cut replaces classification: ≥ 8σ keeps 38 % junk at its
  edge and loses the 29 % of 7–8σ picks that are ribosomes. The candidates' floor is 7.25σ.
- **pilot, Grid3 and SK formed no junk class** with K 4, though their σ distributions are like the others' (65–73 %
  ≥ 8σ). At the others' rates they hold ~5,600, ~13,900 and ~22,300 junk picks (about a third): inside ribosome classes
  (Grid3's class 1, 51 %, holds 63 % of its 7–8σ picks; SK's class 2 is 72 % at it013). They come out on the pooled
  set after R2 (Class 3D, `skip_align`), not by running R1 again.
- **Timing.** unmilled2: 1 h 33 min an iteration, it025 at ~21:00 on 2026-11-19. SK on an A100: 1 h 55 min, growing
  ~1.5 min an iteration, it025 at ~08:00 on 2026-11-20 against its 2-day limit at 09:40; if it stops, Run continues it
  (the continuation's first run through SLURM).

**R2, as it can run once the eight are in.** Class sets of the kept classes (the maintainer's picks); in the ribosome
kitchen one subset per class set (the set filter matches by uid, H23) and one kitchen set from all of them, ~258k
picks; Refine 3D at bin 2 reading it, images from each rerun's own ribosome Warp export (box 200, chosen per
project), local around the TM angles (`sigma_ang` 5, healpix 3, as R1 began; R1's poses do not travel, F21),
reference job002, Ø 350; then Mask and PostProcess. ~750 GB of images a pass, too large to pre-read. Then Class 3D
`skip_align` on the pooled set for the hidden junk.

**D6 can be decided.** R1 answered the ribosome's σ question (no cut; floor 7.25σ, and the junk share rising below
8σ), and the 412's growth reads the candidates or a new template match, never the old score maps. **Taken
(maintainer, 2026-11-19): delete nothing for now.**

**Taken 2026-11-19 (maintainer):** the keep lists above; R2 waits for all eight; the 412 growth run (V1 as growth);
M built as a capability (C6); capsomers later.

### 12.5 The PI's copia try2 is this capsid: 8.8 Å there, 9.2 Å here (2026-11-19)

The maintainer asked how 8.9 and 8.7 Å were reached before with fewer particles. Those are the PI's `Copia/try2`
(`<pi>/Processing/Copia/try2`, one tomogram, Position_11_2 of the 20251017 session): Refine3D job047, 120
particles, 8.90 Å; job054, 76 particles, 8.79 Å; PostProcess job050/055/056 8.79 Å. Scripts:
`notes/sta_side_steps/r1_and_v3_read/` (`vs_pi.py`, `pos.py`).

- **The same objects.** 73 of job054's 76 lie within 100 Å of our 96 class-2 capsids in SK Position_11_2 (median 9 Å
  after a 12 Å shift, no flip), and all 96 of ours are among job047's 120. Radial profiles: inner layer 166–183 Å
  against our 174 Å, outer 239 against 239 Å, zero at 203 against 205 Å. The paper's "copia" (provenance doc §9) and
  our 412 class 2 are one particle; which name is right is the lab's call.
- **Like for like.** The PI's mask holds the outer layer only (215–271 Å), ours both layers (127–273 Å). The same FSC
  code on both, unmasked by RELION's phase randomisation: outer shell 215–275 Å, ours 9.02 Å against his 8.79 Å; both
  layers 140–265 Å, ours 9.23 against his 9.14. Our inner layer alone reaches 20.9 Å.
- **What is off: 5.8× the particles bought nothing.** On the outer layer our FSC is higher down to ~10 Å (0.5 at 9.92 Å
  against his 10.52) and then drops off a cliff; his reaches further. That is per-image error, not particle count.
  His chain on the same tomogram: 9.78 Å (124, no cleaning) → 9.38 (cleaned) → 9.26 → 8.79 after RELION's CtfRefine
  (`--do_defocus --d0 -6000 --d1 6000 --do_scale --per_frame_scale`, no regularisation; it left −0.22 µm on the −45°
  tilt and −0.04 µm on +69°, failed fits on late tilts) and a Class 3D selection.
- **Geometry agrees, CTF does not.** Both runs: `rlnTomoHand` −1, tilt angles of the same sign (stage −45° → `rlnTomoYTilt`
  54°, a 9° offset), axis 84.4°. Warp fits one astigmatism per tilt series: 1.005 µm at 52° on every tilt of P11_2
  (0.2–1.1 µm across the 12 tilt series of job018). His CtfRefine: 0.71 µm at 141° (the same axis, U and V swapped)
  on the 12° tilt; mean defocus 4.556 against our 4.541 µm. A defocus error δz costs a phase πλδz/d² (λ 0.0197 Å):
  at 9 Å 0.1 µm is 45°, 0.2 µm 90°; the 0.3 µm astigmatism difference is ±0.15 µm, ~66° along its axes.
- Beside the CTF: four sessions at ±0.5 % pixel size (§12.4), which a one-tomogram set does not have.
- §11's reason against RELION's CtfRefine (every tilt moved −1 µm on copia-tutorial) does not show in the PI's run on
  this data: his refined 12° tilt is within 0.015 µm of our Warp value.

**The growth run, submitted 2026-11-19** (plans `p-5ab997c9`: class sets s013–s018 and set s019 = every 412
candidate; `p-f775f782` failed at its run on H18/H19, both fixed the same day; `p-0811a7dc` ran it): Warp Export
warpExport__4 (s019, 22,318 picks, bin 4, box 128, poses as picked, so the known capsids get no head start; SLURM
1808826), Class 3D class3d__11 (I1, K 4, reference job019 low-passed to 40 Å, Ø 600, global 7.5°, offset 3, 25
iterations, 3 MPI ranks; SLURM 1808827). Its controls are inside it: the 438 capsids must land in one class and V2's
1,436 rejected picks mostly not; the newcomers are then checked by their radial profile in their own tomogram and
looked at in the kitchen before any set is made.

### 11.1 Built (2026-11-17)

On the maintainer's "implement those jobs we need here", in the main checkout, uncommitted at the end of the session.

- **C1 class → set already existed**: `services/particles/set_ops.create_class_subset_set` (roadmap 30), through
  `backend.create_class_subset`, from the Class 3D job's Results tab and the Species page's Analytics tab. A set
  of classes 1 + 3 of a Class 3D is a ParticleSet with `SetOp.CLASS_SUBSET`, its recipe naming the job, the
  classes and the iteration. Nothing new was written for it. Not an API verb: choosing classes is judging, which
  stays in the UI.
- **C4 Mask** (`maskCreate`, "Mask"): `services/jobs/mask_create.py`, `drivers/mask_create.py`. Reads any map
  (`input_map`, a REFERENCE_MAP: Refine 3D's, Reconstruct particle's, PostProcess's). The driver low-pass filters the map
  in numpy (raised-cosine edge, two shells), takes the threshold from that very map (`threshold_fraction` × its
  maximum, default 0.4, or an absolute `threshold`), then `relion_mask_create --ini_threshold --extend_inimask
  --width_soft_edge --angpix` on the low-passed map. Writes `mask.mrc`, `map_lowpass.mrc`, `mask_stats.json`
  (range, threshold and its source, fraction enclosed); fails when the mask encloses under 0.2 % or over 90 % of the
  box, naming the threshold against the map's range (F6). New file type `JobFileType.MASK`.
- **C3 PostProcess** (`postprocess`): `services/jobs/postprocess.py`, `drivers/postprocess.py`. Reads Refine 3D's
  new `output_half_map` (`run_half1_class001_unfil.mrc`; half 2 by RELION's own name rule) and a Mask job's mask;
  refuses a mask whose box or pixel differs from the half maps, naming both. `relion_postprocess --auto_bfac
  --autob_lowres` (or `--adhoc_bfac`, `--skip_fsc_weighting --low_pass`). Its last line names the resolution
  "masked gold-standard FSC 0.143", and says when the B-factor fit had nothing to fit (map coarser than
  `autob_lowres`: RELION then does not sharpen and says so only in a warning).
- **Mask as an input of Class 3D and Refine 3D**: `mask_from_job` (bool) opens an `input_mask` slot (`when=`, the
  template-matching pattern); `drivers/driver_base.chosen_solvent_mask` takes the job's mask, else
  `solvent_mask_path`, and refuses both at once. Automatic wiring never takes a Mask made after the job it feeds
  (`auto_exclusion`, "not upstream", which keeps wiring acyclic), so a Refine 3D's mask is chosen in its I/O
  section; PostProcess's is wired automatically.
- **C5 `skip_align`** on Class 3D (`--skip_align`, `sigma_ang` dropped with it): focused classification with the
  mask input.
- **Branch map:** names, icons, slot words, add menu (`ADDABLE`) for both jobs; large-card facts for Class 3D
  (share per class with "Class 3D estimate" on the number, class slices), Refine 3D ("gold-standard FSC, unmasked
  half maps", the FSC curve, slices), Mask (threshold against the range, fraction enclosed, slices) and PostProcess
  ("masked gold-standard FSC", B, the corrected FSC curve, slices). `ui/branch_map/facts.py`.
- **Config:** `maskCreate` and `postprocess` profiles in `conf.template.yaml` (CPU) and the dev `conf.yaml`
  (partition g on an RTX 6000 / A100 / MIG ticket, as reconstructParticle). The lab install's `conf.yaml` needs the
  same two blocks, else they take `slurm_defaults`.
- **Checked** (session scratchpad `toy/`): the Mask and PostProcess steps on the archived 412 Refine 3D (old
  20251113 job024, box 256, 3.1 Å) in the real RELION 5.0.1 container: mask at 0.4 × max encloses 10.4 %;
  PostProcess 41.77 Å masked (the record said 41.8), auto B had nothing to fit at 41 Å (the note above came from
  that). Roster: Reconstruct → Class 3D, Refine 3D, Mask, PostProcess added to a scratch copy of the pilot rerun's
  state; inputs resolved (Mask ← Refine 3D or Reconstruct, PostProcess ← Refine 3D half maps + Mask, Refine 3D's
  mask by choice). `ruff check`, `check_boundaries.py` clean. **Not run:** the server with these jobs (restart
  needed), the job tabs in a browser, any of them through SLURM.

**C6 M, S0 facts (2026-11-17, read only, the dev41 Warp image):** `MTools` (create_population, create_source,
create_species, add_source, expand_symmetry, shift_species, rotate_species, update_mask, resample_trajectories) and
`MCore` (`--population`, `--iter`, `--refine_imagewarp XxY`, `--refine_particles`, `--refine_mag`,
`--refine_stageangles`, `--refine_volumewarp`, `--refine_tiltmovies`, `--ctf_defocus`, `--ctf_cs`, the Zernike
orders, `--task_dir` for a queue on fast scratch, `--external_provisioner` for cluster workers) are in the image.
`create_source` takes a processing `.settings` file and optionally a STAR of the items to keep (`--files`): a source
is the folder a project's tilt series were processed in, and M refines their metadata there. `create_species` takes
the two half maps, a binary mask and `--particles_relion` (a RELION data star), with `--angpix_coords` /
`--angpix_shifts` overrides and `--ignore_unmatched`. So the open design point stands: the reruns must not be
written, so the home project needs copies of each project's tilt-series XMLs and a settings file that points M at
them, with the images read in place. Next for C6: read a rerun's `.settings` and one tilt-series XML to see which
paths they hold, then write the job's design here. `expand_symmetry` and `shift_species` cover most of C7.

### 11.2 Built 2026-11-18: Class 3D on several GPUs, and continuing after a time-out

On the maintainer's "build mpi" (2026-11-18), after the 412's first iteration took 2.25 h on an A100 (§12.1). In the
main checkout, uncommitted.

- **MPI.** `Class3DParams.mpi_ranks` (default 1): 1 runs one `relion_refine` as before; N > 1 runs `mpirun
  --oversubscribe --bind-to none -n N relion_refine_mpi`, Refine 3D's own prefix, a leader handing the particles out
  to N − 1 followers that RELION maps onto the job's GPUs in turn (`--gpu` without ids). The GPUs come from the job's
  SLURM gres; the driver prints the ranks against `CUDA_VISIBLE_DEVICES` and `SLURM_CPUS_PER_TASK`, and warns past
  two followers a card. Sizing it: gres `gpu:G`, `mpi_ranks` G + 1 (one follower a card) or 2G + 1 (two sharing a
  large card), CPUs about followers × `threads`, memory followers × the set in float32 (every follower pre-reads every
  particle: 39 tilts × box² × 4 bytes a particle, 1.56 MB at box 100, 2.56 MB at box 128) or `preread_images` off.
- **Continuing.** Refine 3D's checkpoint code moved to `drivers/relion_continue.py` (`latest_checkpoint` with the
  iteration's file list, `changed_since_start`, `continue_command` for either program); Refine 3D reads it from there,
  unchanged (old and new compared on a scratch checkpoint: same checkpoint, same continue command, same changes).
  Class 3D re-run in place finds the last iteration with its data, model, sampling and every class map, refuses
  settings that differ from `run_it000_optimiser.star`'s command (computation options aside), and runs
  `relion_refine[_mpi] --continue … --o run_ct<N>`. When it ends, every `run_ct<N>_it<M>_*` file is linked (copied
  where hard links fail) to its `run_it<M>_*` name, the latest continuation winning and replacing what a stopped
  attempt left half written, so the Results tab, the particle map and class sets read a continued job as any other;
  an iteration without an optimisation set gets one (its data star and the input set's tomograms). A job whose
  iterations are all on disk finishes without running RELION.
- **Units.** The Class 3D tab said Å for `offset_range` and `offset_step`; RELION reads pixels (the 412 log: 12.4 Å
  steps from `offset_step` 2 at 6.2 Å/px). Now "(pixels)", as Refine 3D's.
- **Checked** (session scratchpad `class3d_test/`): with `mpi_ranks` 1 the new driver builds, character for
  character, the command each running ribosome Class 3D logged (job005, job006, job008, job012); the queued 412 job
  gets the same relion_refine with K 6, `offset_range` 3, Ø 600, no `sigma_ang`; `mpi_ranks` 5 gives the mpirun
  prefix; on a scratch copy of job008's real `run_it000`–`002` optimiser stars the checkpoint is iteration 2 (3 half
  written), unchanged settings pass, K 4 → 5 is named, a serial start continues under MPI with another `--j`, the
  mirror takes each iteration from the latest continuation as a hard link; `ruff`, `check_boundaries.py` clean.
  **Run 2026-11-18:** after the restart, the 412 job (job013, SLURM 1780088, `mpi_ranks` 3 on one B200) ran the
  new driver end to end: the MPI line, 25 iterations, the optimisation set mirrored, the analytics sidecar (§12.1).
  **Not run yet:** a continuation through SLURM (first chance: SK, job012, if it meets `g_medium`'s 2 days).

## 13. Handoff: the particle builder (for the next session)

**What the maintainer asked (2026-11-17):** a branching job builder for particles that opens as a separate view
from the PARTICLES header of the roster, to begin with so that they can follow their classes there; developed as
the STA goes. §10 is the scope; this section is where to start and what the first slice is.

**Read first:** §10 (all of it), §11.1, roadmap 32 §15.8 (how the preprocessing map was mounted), CLAUDE.md's UI
rules, the memories "Never commit", "One box deep". Look at the preprocessing map in a browser before writing
anything (`./crboost`, a rerun project's Jobs view with no job open).

**Where it goes:**
- **Entry:** a button on the roster's PARTICLES header, beside the species / import tomograms / merge buttons
  (`ui/pipeline_builder/pipeline_roster.py:408`, `_build_new_species_btn` and its neighbours show the pattern).
- **View:** a new `View.PARTICLES` in `services/routes.py` (URL `/p/<project>/particles/<node>`, the node optional),
  a `_view_frame("particles")` in `ui/workspace_page.py` (the frames are made around line 849; journey, gallery and
  viewer are built lazily on first show — do the same), shown and hidden through `_show`, URL written by
  `callbacks["set_url"]` on activation and on a node pick (CLAUDE.md, URL writes).
- **Code:** a new package `ui/particle_map/` that reuses `ui/branch_map/`'s card, geometry, plots, blocks and facts
  store (import them; do not fork them) with its own model, and a pure data module
  `services/particles/lineage.py` (§10.2) that the UI only renders.

**First slice: follow the classes (build this, in this order):**
1. `lineage.py`: nodes = the home project's particle-phase jobs (`JOB_SPEC_BY_TYPE[...].phase == PHASE_PARTICLES`)
   and its particle sets (`particle_sets.list_sets`); edges = each job's inputs (recorded `paths`, else
   `source_overrides`, else the resolver's edge, as `ui/branch_map/model.project_from_state` does) and each set's
   recipe parents (`Recipe.parents`; a CLASS_SUBSET set's parent is the Class 3D job, its classes in
   `recipe.params["classes"]`). Per Class 3D node, per class of the last iteration: count from
   `run_itNNN_data.star` (`rlnClassNumber`), share and Class 3D estimate from `run_itNNN_model.star`
   (`class_readout._model_classes`), counts by project and by tomogram from `rlnTomoName` (the reruns' tomogram
   names start with their project's name; a set that renamed tomograms records the origin in
   `services/particles/set_origins.py`). Blocking reads: called in `asyncio.to_thread`, cached by mtimes.
2. `ui/particle_map/view.py`: `ParticleMapView(FingerprintedView)`; `signature()` = particle jobs'
   `execution_status`, the sets root's mtime and `operations.jsonl`'s, the picked node. Cards as the branch map's
   (`ui/branch_map/card.render_card` with a particle `CardSpec`); a Class 3D card has one output row per class
   (`card.output_row`) reading "class 2 · 41,203 · 38 % · 11.2 Å (Class 3D estimate)"; a set made from classes
   hangs off its class row with a cable; set cards are slim (name, count, how chosen).
3. The large card: PARAMETERS and RESULTS as the branch map's blocks (the facts for Class 3D, Refine 3D, Mask and
   PostProcess exist since §11.1), PARTICLES = counts by project (bars) and per tomogram (histogram).
4. The header click on a job card opens its job page the way the branch map does (`panel.switch_tab(iid)`, Jobs view).
5. A check in the shape of `scripts/dev_branch_map_check.py` over a project with a Class 3D and a class subset.

**After the first slice, in the order the STA needs them:** the + on a class row (Make set via
`backend.create_class_subset`, then the jobs that read sets, the branch map's add flow); source cards for the
foreign projects of kitchen sets; the composition magazine; Refine 3D / PostProcess blocks with the FSC kinds; the
uid index and the trace (§10.5).

**Ask the maintainer at the start:** whether a job page opened from the particle view returns to it on close (the
branch map's rule) or stays in the Jobs view; whether the preprocessing map keeps showing particle jobs meanwhile.

**Before using the new jobs:** restart the server (new job types, new slots); the lab install's `conf.yaml` gets the
`maskCreate` and `postprocess` profile blocks (§11.1).

### 13.1 Settled 2026-11-17, before any code (second session)

Nothing of the builder is written yet. This is what the session read, decided and settled; build it this way.

**The maintainer's answers to §13's questions:**
- A job page opened from the particle view closes back onto it. The page takes the particle view's place while it is
  open; its ✕ (or Esc) brings the particle view back as it was, and turns the Jobs view off again if it was off when
  the page opened. A page opened from the roster still closes onto the Jobs map.
- The preprocessing map keeps showing particle jobs while the particle view is built (it is the only map that adds
  jobs until the particle view has its + per class row).
- D7 is therefore taken: a view of its own (§8).

**Facts the build stands on (read 2026-11-17):**
- Every Class 3D on disk has K = 1 (copia-tutorial-20261015-1551 job011, MysteryLattice ×4, Copia_Aaron_Tutorial, two
  archived AGG412 projects), and no project holds a class-subset set. The check runs on the copia tutorial's job011
  (15 iterations, ~1.5k particles) with a class-1 set made in a scratch copy; rows of several classes are first seen
  on phase A's K 4 runs.
- The archived projects (`_crboost_archive/agg412_old_records`) still name their old `crboost_data` paths inside
  their stars: no use as fixtures.
- Tomograms that came from other projects' jobs carry `cbOriginProject` (the home project's
  `Tomograms/tomograms.star`, a set's own `tomograms.star`). A tomogram's project is read from there, else it is the
  home project's own; no guessing from name prefixes.
- Every particle job declares an `OPTIMISATION_SET_STAR` output (`OUTPUT_SCHEMA`, `path_template`), and an
  optimisation set names its particles star and its tomograms star (`subtomo_merge._parse_optimisation_set`). A Class
  3D's current particles are its last iteration's `run_itNNN_optimisation_set.star` (current while it runs).
- The branch map's pan/zoom script (`ui/branch_map/view.py` `_JS`) is page-global: `cbBranchMap.fit` / `zoomBy` take
  the first `.cb-bm-canvas`, the zoom label updates every label, and `cb-bm-move` / `cb-bm-view` carry no map name,
  so two maps on one page steer each other (a drag in one moves the other's card of the same id). Fixed first: a
  `data-map` on each canvas and zoom label, the map name in every event, `fit(map)` / `zoomBy(f, map)`, and
  `BranchMapView.on_move` / `on_view` ignoring other maps; the preprocessing map is `jobs` and behaves as today.
- `check_look_literals.py`: a new `ui/` file may hold no hex, `font-family` or `text-transform` literal outside a
  `var(--cb-…, fallback)`; colours and voices come from `ui/branch_map/style.py`. `check_boundaries.py`: services never
  import `ui` (R1), and no module-level dict keyed by three or more `JobType` members (R4): key by kind strings, as the
  branch map does.
- A second server from this checkout recovers and reconciles every project with `pipeline_active` in the user's
  bases, beside the maintainer's own server: never start one. The check starts its own copy of the app
  (`server_app.setup_app`) with `PipelineMonitor.start` and `CurationWatcher.start` made no-ops, its own `HOME`
  (prefs, NiceGUI storage, logs), no token gate, over a scratch project.

**Design:**
- **The split.** `services/particles/lineage.py` holds what only the disk knows: the sets as nodes (`list_sets`, the
  recipe's parents, the classes of a `CLASS_SUBSET`, stale parents), the class rows of each Class 3D (count from
  `rlnClassNumber`, share from `rlnClassDistribution`, named as such, and Class 3D's own estimate), counts by project
  and by tomogram, and `set_of_input(jm, slot)` (a recorded `/Analytics/sets/<id>/` path, else `set_read_by`). Job →
  job edges and the cards' input rows stay in `ui/branch_map/model.project_from_state`, so the two maps never
  disagree on what a job reads; the particle model re-points a row that reads a set to the set's node.
- **Off the loop.** Star reads (up to 350k rows) run in `run_in_worker` under a new `WorkKind.LINEAGE`, cached in the
  server by the files' mtimes (stat in a thread). The view's signature reads memory only: the store's `version`; a
  15 s poll stats the sets root, `operations.jsonl` and each Class 3D's last data star in a thread.
- **Cards.** Jobs through `card.render_card`, no magazine (band 0). A Class 3D: one row per class above the footer
  ("class 2 · 41,203 · 38 % · 11.2 Å (Class 3D estimate)", the share explained on hover), each with its port on the
  right edge, drawn into the card; the footer, "→ particles, every class · it NNN", keeps the whole job's port. A set:
  `render_card` with a slim spec (name, count, `sNNN`, one "from" row per parent, 220 px), cabled from its class rows
  (one cable per class) or from its parent's footer. Parents in other projects are named, without a cable (source
  cards come after this slice). No + menus in this slice.
- **The large card.** PARAMETERS (`blocks.params_parts`) and RESULTS (`blocks.results_parts`, given a `tally=False`
  so particle cards carry no tilt-series tally) from the shared `FactsStore`, passed the registry's series and stamp
  as `_request_facts` passes them so both maps share its cache; and PARTICLES: one row per project (name, count, a bar)
  and a histogram of particles per tomogram. No threshold marks until one is decided.
- **The picked node** is the card made large last: `/p/<project>/particles/<node>` (a set as `set:<id>`), its card
  ringed; a node the project does not have is named in a toast.
- **Workspace.** `View.PARTICLES` (`_TARGETS` 1), `apply_route` → `show_particles(node)`; `_view_frame("particles")`
  built on first show (the gallery's pattern), after `pipeline` in `_VIEW_ORDER`, titled "Particle map", in
  `_MODE_VIEWS`, its URL from the page's node in `_write_mode_url`, `page.set_active` from `_apply_views`, timers on
  `ui_mgr.view_scope("particles")`, its container of class `cb-bm-app` (the map's CSS then needs no edit). The return:
  `callbacks["open_job_from"](view, iid)` hides the view (URL writes suppressed) and opens the job;
  `PipelineBuilderPanel.close_job` asks `callbacks["job_closed"](iid)` before it writes its own URL, and the workspace
  brings the view back.
- **Entry.** The branch glyph (`pipeline_table.svg`) at the right of the roster's PARTICLES header →
  `callbacks["show_particles"]`.
- **The check,** `scripts/dev_particle_map_check.py`: `fixture` (the project's Python: copy `project_params.json` with
  `project_path` rewritten, copy `registry/`, symlink the rest, make a class-1 set with
  `set_ops.create_class_subset_set`), `serve` (the app as above), and the steps in Playwright's Python (the view opens
  from the header button; the class rows; the set hangs off its row; the large card's three blocks; the URL names the
  node; a header opens the job page; ✕ and Esc come back; Back), PNGs in `notes/screens/particles/`, failing on any
  page error or an ERROR or Traceback line of the server.

### 13.2 Built 2026-11-17: the first slice

Built as §13.1 settled it, and checked in a headless browser on a scratch copy of copia-tutorial-20261015-1551 with
a class-1 set (`scripts/dev_particle_map_check.py`: 11 steps, no server ERROR or Traceback, no page error). Not yet
in the maintainer's own server: it needs a restart (the one on 24326 started before this code).

**What is there:**
- **Two maps on one page.** The pan/zoom script (`ui/branch_map/view.py` `_JS`) names its map on every canvas and
  zoom label (`data-map`) and in every event; `fit(map)` / `zoomBy(f, map)`; `BranchMapView.on_move` / `on_view` /
  `on_mark` ignore other maps. The Jobs map is `jobs`; the check zooms and drags it after the change.
  `zoom_controls(map)` and `install_pan_zoom()` are shared with the particle map.
- **`services/particles/lineage.py`.** The sets as nodes (`read_sets`: parents, the classes of a class subset, stale
  parents, composition from the set's own per-tomogram counts and its tomograms star's `cbOriginProject`); a Class
  3D's last iteration (`read_class3d`: members per class from `rlnClassNumber`, share from `rlnClassDistribution`,
  Class 3D's estimate; rows naming a class the model star lacks are reported); every other particle job's particles
  from the optimisation set it makes (the newest `run_itNNN_optimisation_set.star` while it runs), or, for a job
  that makes a map, from the one it read. `set_of_input(jm, slot)`. `LineageStore` stats in a thread, reads in
  `run_in_worker(WorkKind.LINEAGE, …)` (cap 2), keys by mtimes, moves `version` when a read lands.
- **`ui/particle_map/`** (model, store, cards, view). Job → job rows come from `project_from_state`; a row reading a
  set is pointed at the set's node; a preprocessing source is named, without a cable. A Class 3D card has one row
  per class above its footer with its own port; a set made of classes takes one cable per class from those rows. Set
  cards 220 px: name, `sNNN`, level badge, one "from" row per parent, count in the footer; foreign parents named.
  Large: PARAMETERS and RESULTS (`results_parts(..., tally=False)`), PARTICLES (bars per project, a histogram per
  tomogram from three tomograms up, what the reads flagged in amber); a set's large card shows RECIPE and PARTICLES.
  The map fits itself again whenever a node comes or goes (the sets landing after the first draw).
- **Wiring.** `View.PARTICLES` (`/p/<project>/particles/<node>`, a set as `set:<id>`, percent-encoded in the bar);
  `apply_route` → `show_particles(node)`; the frame `particles` after `pipeline`, built on first show, titled
  "Particle map", its timers on `view_scope("particles")` (3 s tick, 15 s lineage poll). The return:
  `open_job_from(view, iid)` hides the view without writing the bar and opens the job;
  `PipelineBuilderPanel.close_job` asks `job_closed(iid)` before writing its own URL. The entry is the branch glyph at
  the right of the roster's PARTICLES header (`.cb-pm-open`). A job card's header opens its page; the size toggle
  makes any card large, and the card made large last is the picked node (ringed, in the URL). A node the project does
  not have is named in a toast.
- **Shared code touched:** `render_card` / `header` take a `size_tip` (the particle cards' toggle says "particles", not
  "tilt series"); `ui/branch_map/style.py` names `CABLE`.

**Seen on the fixture:** job011 → "class 1 · 1,016 · 100 % · 8.7 Å (Class 3D estimate)"; the set `s001_class_1`
hangs off that row (cable at y 85.5 of a 116 px card, the class row, not the footer); ✕ and Esc bring the map back
with the Jobs view off as it was; Back returns to the job page.

**Open, for the next slices:**
- Rows of several classes are first seen on phase A's K 4 runs; the small card cuts the class line ("…(Class 3D
  esti…"), the full line is on hover. Whether the small card should drop the estimate's words is the maintainer's
  call once K 4 cards are on screen.
- No + per class row yet; no source cards for foreign parents; no composition magazine; the trace (§10.5).
- A large card is not re-fitted into view (as on the Jobs map): after making a card large near the edge, "fit".
- `check_look_literals.py` fails on `ui/agent_plan_page.py` (15 literals, not in the allow-list); that file is not
  part of this build.

### 13.3 Taken out of the UI (2026-11-18)

**Asked (maintainer, 2026-11-18):** get rid of the particle map for now: it is too similar to the branch map, and the
two should eventually be one. The merge is roadmap 32 §15.9; nothing of it is built.

**Removed:** the branch glyph on the roster's PARTICLES header; the `particles` view in `ui/workspace_page.py` (its
frame, `_show_particles`, its place in `_VIEW_ORDER`, titles and icons, its URL write and its view scope);
`View.PARTICLES` (`services/routes.py`) and its case in `ui/routing.py`; the open-from-a-view return path only the
map used (`_open_job_from`, `_job_closed`, the `job_closed` hook in `PipelineBuilderPanel.close_job`). An old
`/p/<project>/particles/...` URL now says "Unknown view 'particles'" and shows the pipeline. The API's
`make_class_set` hands the Class 3D's job page as its `ui_url` instead of the map.

**Kept:** `services/particles/lineage.py` (U1, the data layer: a set's or a job's particles, a Class 3D's classes,
compositions; `WorkKind.LINEAGE`), unused for now, for the merge. `ui/particle_map/` and
`scripts/dev_particle_map_check.py` are unwired and left on disk for the maintainer to delete; the view as built is
commit `7a8f5e3`.

## 14. Aggregate goes; the kitchen is the one way to pool particles (scoped 2026-11-18)

**Asked (maintainer, 2026-11-18):** is there any reason to keep "Aggregate" and "Import more tomograms" in the
PARTICLES header of the Jobs roster, or does the STA kitchen do everything they do? Answer and decision: Aggregate is
ripped out and its code deleted, not hidden, with the kitchen as the one way to pool picks and particles across
projects. Import tomograms stays and moves (§14.5). Nothing built yet.

### 14.1 What Aggregate does, and where the kitchen does it

| Aggregate | The kitchen |
|---|---|
| Coordinate grade (`ui/aggregation/aggregate_dialog.py`, `services/particles/coord_merge.py`): one species' manual, imported, filtered or template-match lists across tomograms and projects, united into a MERGED pick list, then extracted (Pick candidates skipped) | Members are template-match picks and hand-picked, imported and merged lists of any project (`services/sta_kitchen/members.py`); filters, subsets and verdicts replace filtered lists; Make set gives a set that Warp export or Subtomo extraction read (poses facet) |
| Extracted grade (`ui/aggregation/merge_card.py`, `services/aggregation/`): Subtomo extraction optimisation sets of several projects merged into `MergedSources/<slug>/`, curated or original picks per tomogram, every consumer's optimisation-set input rewired to the active merge (`apply_aggregation_overrides`) | A set takes each project's particle images from that project's own extraction (images facet, `services/particles/set_facets.py`), the choice per project on the job reading it; two sources of one tilt series are a choice Run refuses without. Proven on the reruns: job001 and job003 |
| Species matched by eye: any project's extraction with any label (`merge_card.py` filters by label or catalog id) | Species matched by lab-catalog id only, never by name (`members.matching_species`, `services/sta_kitchen/members.py:72`) |

The one thing Aggregate reaches and the kitchen does not: a project whose species was made by hand, without a catalog
entry. Before the deletion the kitchen gets that (K1).

### 14.2 What is deleted

- `ui/aggregation/` (`aggregate_dialog.py`, `merge_card.py`, `__init__.py`), the roster's merge button
  (`_build_aggregation_merge_btn`, `ui/pipeline_builder/pipeline_roster.py:1446`), the merge hooks in
  `ui/pipeline_builder/pipeline_builder_panel.py` (~710), the merge count in `ui/pipeline_builder/io_config_component.py`
  (~273), the pointer in `ui/job_plugins/subtomo_extraction.py`'s docstring.
- The merge engine and its state: `discover_subtomo_optimisation_sets` and what only it uses
  (`services/aggregation/discovery.py`), `apply_aggregation_overrides` and its call in `pipeline_edit.add_job`
  (`services/pipeline_edit.py:266`), `ProjectState.aggregation_sources` / `aggregation_merges`, `AggregationSource`,
  `AggregationMergeSource`, `active_merged_optset*`, `MERGED_DIR_NAME` and their readers in
  `services/particles/analytics_store.py` (~185–201), `services/particles/set_ops.py` (~310) and
  `services/particles/list_admin.py` (~59). `ProjectState` ignores unknown keys (pydantic's default; no
  `extra="forbid"` in `services/project_state.py`), so older `project_params.json` files still load; check it on one.
- The `mergedSources:MergedSources/<slug>` instance paths in the resolver (`services/path_resolution_service.py`) and
  the branch map (`ui/branch_map/model.py:841`, `view.py:236`), once D9 says what old projects keep.
- The coordinate-grade parts of `coord_merge.py` that only the dialog calls.

### 14.3 What stays, and where it moves

`services/aggregation/` is more than Aggregate; those parts move, with their callers, before the package goes:

- `discover_pick_list_projects` (the kitchen's source picker `ui/sta_kitchen/sources.py:227`, the API
  `api/handlers.py:483`) → `services/sta_kitchen/`.
- The per-species and per-list extraction helpers of `services/aggregation/extraction.py`
  (`subtomo_job_dir_for_species`, `extraction_params_for_species`, `extract_inputs_for_list`,
  `extract_inputs_blocked_reason`, `pending_extractions`): per-list extraction, called from `backend.py` (~785,
  ~1385), `ui/particles/list_actions.py`, `ui/species/picks_tab.py`, `ui/species/overview_tab.py`,
  `services/particles/species_overview.py` → `services/particles/` beside `list_extraction.py`.
- `counts_by_tomo` and `load_tomo_curation` (`services/particles/species_overview.py`) → `services/particles/`.
- `coord_merge`'s union and path code, which the kitchen's own export and the set writer use
  (`services/sta_kitchen/export.py`: `CoordSource`, `plan_merge`, `merge_coordinate_sources`, `_absolutize_paths`;
  `services/particles/set_facets.py:616`) → `services/sta_kitchen/`, named for what it is there.
- The `mergedSources` synthetic producer: kitchen sets are read through it (`mergedSources:particle_set__<id>`, every
  Class 3D of §12.1). It stays.
- MERGED pick lists stay a list type: the dashboard's list merge makes them (`ui/particles/list_actions.py:476`) and
  the kitchen takes them as members.

### 14.4 Decisions and the one gap

| # | Decision | Recommended |
|---|---|---|
| D9 | Projects that ran a merge: keep resolving their `mergedSources:MergedSources/<slug>` inputs read only, or drop them | **Counted 2026-11-18: none.** 46 projects read (`/groups/klumpe/crboost_data`, `_crboost_data_api`, `_crboost_archive/agg412_old_records`, the dev `projects/`, and Lea / andreas / user / lab to depth 4): no `aggregation_merges`, no `aggregation_sources`, no `MergedSources/` folder, no job input naming `MergedSources/`. Drop, unless the lab install's users have projects elsewhere (ask before the deletion) |
| D10 | Where Import tomograms goes | The Tomograms view (the rail and the wall), out of the PARTICLES header |

**K1 (before the deletion).** The kitchen pools a project whose species has no catalog id: the source picker lists
the project's species by name and the user picks which one is "this species", recorded on the member (never matched
by name automatically, as `matching_species` says now).

### 14.5 Import tomograms stays

It is not a pooling tool and the kitchen never touches tomograms. It is the only UI way to bring tomograms
reconstructed outside crboost (IMOD `.rec`, any `.mrc` with a voxel size, an existing `tomograms.star`:
`services/tomogram_import.py`, modes reference / synthesize / jobs) into a project without a Reconstruct job, and the
only way to add more after creation (`create_project(tomograms_from)` brings other projects' at creation only). It
moves with D10; its refusal beside a Reconstruct job stays.

### 14.6 Order and checks

1. K1, with a check on a project whose species came from no catalog.
2. Move what stays (§14.3), callers re-pointed; `ruff`, `check_boundaries.py`.
3. D9's count; then delete §14.2 and the roster button; D10's move.
4. Checks: the reruns' kitchens and the home project's nine Class 3D still resolve (`plan_run`), the Species page's
   Picks and Overview tabs and per-list extraction work, an older project with an aggregate loads, `grep` finds no
   `ui.aggregation` / `services.aggregation`.

### 14.7 Built 2026-11-18: the header (not run in a browser)

- The Aggregate button is gone from the PARTICLES header (`_build_aggregation_merge_btn`). Aggregate has no entry in
  the UI now; its code (`ui/aggregation/`, `services/aggregation/`, the state fields) is untouched, and §14.6's
  order still holds for deleting it: K1 first, then the moves of §14.3, then §14.2.
- D10 done: Import tomograms is an icon on the Tomograms view's title line (`library_add`, before Refresh,
  `TomoGalleryPage._import_tomograms`), no longer on the PARTICLES header. In a project with a Reconstruct job a
  click says why it cannot import (`import_refusal`) instead of drawing a disabled icon. A committed import
  re-reads the wall and moves the rail's count (`callbacks["refresh_counts"]`). The green "already imported" dot is
  gone with the header button (the wall and the rail count say it). The wall's empty text points at the new place.
- The PARTICLES header keeps New species only.

## 15. Geometry: RELION's CtfRefine now, M as the full refinement (decided 2026-11-19)

**Decision (maintainer, 2026-11-19): build both.** §12.5 found the limit: per-image CTF (and per-session pixel size), not
particle count. §11 had put RELION's tomo CtfRefine aside for M; its main reason (every tilt moved −1 µm on
copia-tutorial) does not show on this data, and the PI's CtfRefine took these very capsids from 9.26 to 8.79 Å.

**They do not chain; they are two routes.** RELION keeps its CTF and trajectories in its own stars (tomograms.star, the
tilt-series stars, motion.star); M keeps them in Warp's tilt-series XMLs and never reads RELION's. "RELION's CtfRefine
per tilt, then M for the particles only" would start M from Warp's CTF again, and writing RELION's values back into
Warp's XMLs is the convention trap §11 warned of. M on particles alone is roughly RELION's frame alignment; M's value is
refining per-tilt CTF, image warp, stage angles, a pixel scale per session and the particle poses together. So:

- **RELION loop** (fast, inside RELION's metadata): CtfRefine (later frame alignment) → Refine 3D → PostProcess. Good at
  bin 2 now; a Warp re-export (bin 1) does not carry it, so it runs again after one.
- **M loop** (the full route, inside Warp's metadata): takes from RELION only the refined map, mask and particle poses;
  its refined XMLs (copies in the home project) feed a Warp export at bin 1.

Both run on the same 412 set and are read on the same masks (outer layer, both layers, §12.5): CtfRefine's gain is M's
yardstick.

### 15.1 C10 CtfRefine (build first)

- **S0, read only** (the RELION 5.0.1 image): `relion_tomo_refine_ctf --help`; that it runs on our Warp-made
  optimisation sets (2D stacks, `rlnTomoSubTomosAre2DStacks`; it reads the tilt images the tilt-series stars name,
  Warp's frame-series averages, which are in the reruns); what it writes (tomograms.star, tilt-series stars, an
  optimisation set); whether it refines astigmatism per tilt (the PI's output says yes: U and V moved apart per tilt).
- **Job:** reads a Refine 3D's optimisation set (the poses and tomograms) and a reference **at the tilt series' pixel
  size**: two half maps, their mask and their FSC (S0, below: a Reconstruct particle at binning 1, a Mask and a
  PostProcess made from it; the PI's chain did the same, Reconstruct job036 `--bin 1 --b 786 --crop 448`). Fields:
  defocus on/off, search range (the PI ±6000 Å), regularisation across tilts (lambda; the PI ran none and his late
  tilts went to −0.22 µm), per-frame scale on/off, even/odd aberrations (off), box in tilt-image pixels (~1.5× Ø: the
  PI 512 px at 1.55 Å for this capsid). Writes an optimisation set (same particles, refined tomograms.star) that the next
  Refine 3D reads; the images stay (our exports are raw, `dont_premultiply`: RELION applies the CTF at refine).
- **The driver says what moved** (never silent): per tilt, defocus and astigmatism before → after; it refuses or flags a
  defocus ≤ 0 or a change over 1 µm from Warp's; a per-tilt-series table for the card.
- **First run:** 412 job019 → Reconstruct particle (bin 1) → Mask → PostProcess → CtfRefine → Refine 3D local (from
  job019) → PostProcess, read on the outer layer and both layers against job020 (9.0 / 9.2 Å) and the PI's 8.79 Å.
- **Mask job: radial limits** (inner and outer radius), so the outer-layer mask (215–271 Å) is a Mask job, not a side
  step (F22).

### 15.2 C6 M (build second)

§11.1's S0 facts stand. Next facts, read only: a rerun's processing `.settings` and one tilt-series XML (which paths
they hold: frames, averages, alignment); the population/source layout; where MCore writes; how `create_species` takes a
RELION refine (half maps, binary mask, `--particles_relion`, `--angpix_coords` / `--angpix_shifts`). Then the design
here, then the build.

- **Shape (§11):** the home project holds the population; one source per rerun project with **copies** of its tilt-series
  XMLs and a settings file pointing M at them, images read in place (the reruns are never written); the species from a
  Refine 3D and a Mask.
- **Fields:** iterations; per-tilt CTF (defocus, astigmatism), image warp grid, stage angles, magnification per source
  (the ±0.5 % sessions), particle poses; `--task_dir` on fast scratch.
- **Outputs:** refined half maps and map (a card with the FSC), the refined XML copies; a Warp Export input "tilt-series
  metadata from an M job" to cut bin-1 images from them.
- **First run:** the 412 (the 438, or the grown set) from the best Refine 3D; the ribosome after R2.

### 15.3 C11 Frame alignment (after C10, on its evidence)

RELION's `relion_tomo_align` (per-particle motion over the tilts, per-tilt shifts). Its trajectories need a RELION
re-extraction (the Subtomo extraction job) before the next Refine 3D. Copia's tutorial gain came from frame alignment and
CtfRefine together (10.5 → 7.8 Å).

### 15.4 Order

1. Restart the server (H18/H19 and the rest of the uncommitted tree); read the growth run (§12.5's controls) and R1's
   last two (unmilled2, SK: their keep lists go to the maintainer).
2. Build C10 with the Mask job's radial limits (built 2026-11-19, §15.5); run it on the 412.
3. R2 for the ribosome once the eight are in (§12.4); C10 on R2 as well.
4. M: facts → design → build → the 412, then the ribosome.

### 15.5 Built 2026-11-19: C10 CtfRefine, and the Mask's radial limits

On the maintainer's "build both" (2026-11-19), CtfRefine first. In the main checkout, uncommitted.

**S0 facts (RELION 5.0.1 image, the PI's `Copia/try2`, our job018/job019; read only).**

- `relion_tomo_refine_ctf --help`: `--do_defocus` refines the astigmatic defocus per tilt (`--d0/--d1/--ds`, `--kmin`
  30 Å), `--do_reg_defocus --lambda` (0.001) regularises across tilts, `--reset_to_common`; `--do_scale` with
  `--per_frame_scale` or `--per_tomogram_scale`; even/odd aberrations; `--b` (required); `--fsc` a PostProcess star.
- **The reference must be at the tilt series' pixel size (F23).** The tomo reference loader pads to `--b` or refuses a
  larger map; it never reads the pixel size. job019's halves are 256 px at 3.10 Å, the tilt images 1.55 Å. The PI
  reconstructed at bin 1 (`--b 786 --crop 448`) and ran `--b 512 --do_defocus --d0 -6000 --d1 6000 --do_scale
  --per_frame_scale` with that pair, `MaskCreate/job013` and `PostProcess/job040`'s star.
- **What it reads and writes.** It reads the tilt images the tilt-series stars name (ours: the reruns' Warp frame-series
  averages, 4096², 1.55 Å) and the particles' poses; the 2D stacks are not read. It writes `optimisation_set.star`
  (particles = the input's `run_data.star`, tomograms = its own), `tomograms.star`, `tilt_series/<TS>.star`, `temp/`,
  `logfile.pdf`; unknown columns are kept (it warns and maintains them).
- **No re-export after it.** Our export says `rlnCtfDataAreCtfPremultiplied 0`, and the PI's Refine 3D after CtfRefine
  read `--tomograms CtfRefine/job042/tomograms.star` beside its particles: relion_refine takes each tilt's CTF from
  the tomograms star. A Refine 3D reading CtfRefine's optimisation set refines on the same bin-2 images with the
  refined CTF. (The PI re-extracted at bin 1 for his own reasons, not for the CTF.)
- **The PI's run, compared tilt by tilt** (his input `External/job007` against `CtfRefine/job042`, with this build's
  comparison): his input held 7 tilts at 1.6–2.2 µm instead of ~4.5 µm (failed fits); CtfRefine brought two back
  (−27° +2.46 µm, +60° +2.35 µm) and sent five to a defocus at or below zero (−45°, −39°, +57°, +66°, +69°); moves
  reach 2.5 µm with a ±0.6 µm search range, so `--d0/--d1` bound the first scan, not the result. The per-tilt scale went
  negative on +54°…+63° (down to −0.12). The middle tilts moved by hundredths of a µm, the astigmatism fell from Warp's
  0.99 to 0.54 µm (median; 0.71 µm at 51° on +12°, the 141° of §12.5 with U and V swapped). Our input (job018's tilt
  series) has no such failed fits.

**Built.**

- **`ctfRefine`, "CTF refine"** (`services/jobs/ctf_refine.py`, `drivers/ctf_refine.py`, spec row after PostProcess,
  `dependencies` Refine 3D, `log_format` reconstruct for "+ Tomogram k / n"). Inputs: `input_optimisation` (a Refine
  3D's), `input_half_map` (preferred Reconstruct particle), `input_mask`, `input_fsc` (new `JobFileType.FSC_STAR`, a
  PostProcess's `postprocess.star`). Output: `optimisation_set.star`, no `refines` (a Refine 3D is pointed at it by
  choice: it sits earlier in pipeline order, so automatic wiring never takes it, and Run's topological order submits it
  after). Fields: defocus (range ± default RELION's 3000 Å, steps, kmin, regularise + lambda, reset to common), scale
  (per tilt, per tomogram), aberrations (even/odd and orders), box (0 = the reference's), largest move (1 µm), threads,
  extra arguments; `shows_param` hides what a switch turns off; `run_problems` refuses "nothing to refine".
- **The driver refuses before RELION:** half maps and mask not one grid; the reference off the tilt images' pixel size
  (naming the fix: Reconstruct particle, binning 1, with its Mask and PostProcess); tomograms at several pixel sizes;
  a box smaller than the reference or odd; an FSC measured on other half maps (`rlnUnfilteredMapHalf1`).
- **The driver says what moved:** per tilt, the defocus pair and axis before and after (U ≥ V normalised, so a swap is no
  move), into `ctf_changes.json` and a per-tilt-series table in run.out; it **fails** on a tilt at defocus ≤ 0, a move
  over `max_defocus_change_um`, or a negative contrast scale, naming the tilts and the levers (regularise, a narrower
  range, the scale over the dose).
- **Reconstruct particle** gains the `output_half_map` slot (`half1.mrc`), **PostProcess** the `output_fsc` slot
  (`postprocess.star`). PostProcess's half-map input still prefers a Refine 3D (rank: species, then preferred type), so
  no existing input ties.
- **Mask: radial limits** (`inner_radius_ang`, `outer_radius_ang`, group "Radius"): RELION's mask times a shell around
  the box centre (voxel n // 2), 1 between the radii, a raised cosine to 0 over the soft edge beyond each limit (outside,
  as RELION grows its own edge). `mask_stats.json` records the limits and the fraction before them.
- **Branch map:** name, icon, add menu, slot words ("FSC"), identity line (defocus range, regularised, scale), the
  Mask's radius; the large card: tiles (median move, largest, box), a line per tilt series (defocus before → after,
  median and largest move, astigmatism before → after, scale range), the failed tilts.
- **Config:** `ctfRefine` profiles in `conf.template.yaml` (CPU, 32G, 8 CPUs, 2 h) and the dev `conf.yaml` (partition
  g on the GPU ticket). The lab install's `conf.yaml` needs the same block, else the job takes `slurm_defaults`.

**Checked** (session scratchpad `c10/`): the comparison on the PI's own run (above: 13 tilts flagged, the scale check
included); the radial shell on job016's mask at 215–271 Å (15.6 % → 9.6 % of the box, 1.0 between the radii, the
inner layer gone); the pre-RELION checks on job018/job019/job020 (tilt images 1.55 Å, job019's halves 3.10 Å → refused;
job020's FSC names job019's half 1 → accepted); the job page's groups from `layout_of`; the whole first run rehearsed on
a copy of AGG412_STA's state (`add_job` with `reading`, as a plan adds them: Reconstruct particle → Mask → PostProcess →
CTF refine → Refine 3D clone of job019 reading the CTF refine → outer Mask on job019 → three PostProcess), every input
resolved by Run's own passes; `ruff`, `check_boundaries.py` clean. **Not run:** relion_tomo_refine_ctf on our metadata
(the first run is the test), the job page in a browser. The server needs a restart for the new job type.

### 15.6 The first CTF refine run, submitted 2026-11-19

Plan `p-571fdb68` (accepted; the maintainer's choices: defocus ±3000 Å regularised at λ 0.001, no scale; the bin-1
reference masked to the outer layer; the PI's box), all in AGG412_STA:

| Job | Instance | What | SLURM |
|---|---|---|---|
| job023 | reconstructParticle__3 | job019's poses at bin 1, `--b 786 --crop 448 --sym I1`, 8 CPUs, 64G, 6 h | 1810723 |
| job025 | maskCreate__2 | on job023: 15 Å, 0.15 × max, extend 6, edge 12 px (job016's in Å), 215–271 Å | 1810725 |
| job027 | postprocess__2 | job023's halves in job025: the FSC CTF refine weights by | 1810727 |
| job031 | ctfRefine | job019's set, job023/025/027, box 512 | 1810728 |
| job024 | refine3d__3 | clone of job019 (local σ 2°, healpix 4, ini_high 25 Å, I1, Ø 600, mask job016), reading job031, reference job019 | 1810729 |
| job026 | maskCreate__3 | clone of job016 with 215–271 Å (ran: 15.6 % → 9.6 % of the box) | 1810724 |
| job028 | postprocess__3 | after, both layers (job024 in job016) | 1810730 |
| job029 | postprocess__4 | after, outer layer (job024 in job026) | 1810731 |
| job030 | postprocess__5 | before, outer layer (job019 in job026): RELION's own "before" beside job020 | 1810726 |

Read-out: job020 (9.23 Å, both layers) and job030 against job028 and job029; the PI's 8.79 Å on the outer layer; job031's
card for what moved per tilt series.

**The inside of the capsid** (asked 2026-11-19; job019's halves at 20 Å, script `c10/interior.py` in the session
scratchpad). Against the region outside the capsid (300–380 Å, itself not empty: the halves correlate 0.93 there,
neighbouring capsids): the outer layer (220–260 Å) is dense and strongly modulated around each radius (spread 2–4 sd);
the inner layer (140–200 Å, peak 170–180 Å) is as dense on average but nearly uniform around the sphere (spread
0.3–0.6 sd against a floor of ~0.25), weakly icosahedral (21 Å in its own shell, §12.5); inside 130 Å the average is
at the outside level, no dense core. Whatever lines the shell (most likely the genome with Gag's nucleocapsid part) is
not icosahedral, so I1 averaging smooths it into a layer; more capsids or a better CTF will not resolve it. Open, cheap
and built: a PostProcess with an inner-layer mask (its own resolution, before and after); a Class 3D `skip_align`
with a mask of the inside, K 2–3, for full against empty capsids. Not built: symmetry expansion (C7) and a focused C1
classification of the inside, for an asymmetric content.

### 15.7 The first CTF refine run failed on its reference (read 2026-11-19)

**What failed.** job031 (CTF refine): RELION finished all 12 tomograms in 5 min, then the driver's check refused
the result (275 tilts moved over 1 µm). job024 (Refine 3D), job028 and job029 (PostProcess after) were cancelled by
the afterok chain at 0 s. job023/025/027/030 succeeded. The ribosome R1 jobs (job009, job012) and the 412 growth
run (job022) were unaffected and still running.

**Where it moved** (`job031/ctf_changes.json`, per tilt series):

| Tilt series | capsids | median move | astigmatism before → after (µm) |
|---|---|---|---|
| Grid2 P10 | 171 | 0.81 µm | 0.23 → 0.79 |
| Slot6 P10 | 108 | 0.97 µm | 0.40 → 2.44 (V dragged 2 µm down, U kept, axis 65° → 26°) |
| SK P11_2 | 96 | 0.03 µm | 1.01 → 0.64 (the PI's CtfRefine on this series: 0.71) |
| FCIso P18_4 | 46 | 0.08 µm | 1.02 → 0.61 |
| eight series of 1–8 capsids | 17 | 0.14–1.86 µm | mostly up (too few particles to fit) |

The moves are the astigmatism step, not the scan: ±3000 Å bounds only the first scan (as in the PI's run, §15.5).

**Why: the reference.** job023 (bin-1 Reconstruct particle of job019's poses) is 20.4 Å in job027, with an FSC that
falls to 0.05 at 19 Å, comes back to 0.14 at 14–16 Å, and dips again near 13 Å. Under the same outer-layer mask,
job019's own halves are 8.72 Å (job030). The PI's recipe (`--b 786 --crop 448`, Reconstruct/job036) held 9.26 Å
(PostProcess/job040). With `--fsc` from job027, the fit had little signal past ~20 Å: one or two Thon rings at
5 µm defocus (zeros near 31, 22 and 18 Å), where astigmatism is barely constrained.

**Ruled out (read only).** Pixel sizes: tilt images, job023's halves and job025's mask are all 1.55 Å, 448³. Every
session's mdoc says 1.55 Å at 81 kx. The export's metadata against the PI's for SK P11_2 (`External/job007`):
same xtilt/ytilt/zrot, shifts within a few Å, hand −1, the same sizes, and U/V/angle within ~0.1 µm (his failed
fits at −45° and −27° aside). The one difference is pre-exposure: ours 3.2 e/Å² per tilt from the mdoc, his 4.8.

**Open.** Why RELION's re-extraction from the tilt images (job023) loses what Warp's 2D stacks (job019) keep. The two
series that ran off hold 279 of the 438 capsids, and Grid2 P10 already aligned worse in job014 (F19/§12.3). The
test is two side-step reconstructions at bin 1: SK P11_2 alone against Grid2 P10 alone (job023 took 36 min for
438 on 8 CPUs). If those two series are the cause, CTF refine on SK P11_2 + FCIso P18_4 alone is the PI's like for
like, and those two sessions are the question for job019's 9.2 Å as well. Raising the move limit and running again
would fit to a 20 Å reference: not an option.

### 15.8 Why the reference was 20 Å: missAlign's local warps never reach RELION (2026-11-19)

The maintainer asked to get to the bottom of §15.7 and test as much as needed. Side step, nothing written into the
project: scripts in `notes/sta_side_steps/412_ctf_reference_diag/` (`make_sets.py`, `run.sh`, `warp_trajectories.py`,
`fsc.py`, `chain.sh`, `ctf_read.py`), maps under `/scratch/users/artem.kushner/crboost_side/412_ctf_ref_diag/`.
Every number: job019's 438 capsids and poses, half-map FSC 0.143 under job016's mask, no phase randomisation (the
numbers compare runs with one another; job019's own pair reads 9.1 Å this way).

**The cause (HOTFIXES H26).** Every rerun ran missAlign, which writes local image warps into the tilt series' Warp
XML (`GridMovementX/Y`, 3 × 3 per tilt; 12–18 Å RMS over the field, up to 63 Å). Warp's export cuts the 2D stacks with
the full model, warps included, so Refine 3D (job019) never missed them. The tilt-series star RELION reads holds only
the rigid part, and it matches the XML exactly. RELION's tomo programs cut from the tilt images through that star:
Reconstruct particle (job023) and CTF refine (job031) placed every particle 8–17 Å off in every tilt. At the capsids'
positions, Warp's full position minus its rigid one (warpylib's `get_position_in_all_tilts`, the code missAlign runs
on) is 7.6 Å RMS on SK P11_2, 8.3 FCIso P18_4, 9.2 Slot6 P10, 17.3 Grid2 P10.

**The proof.** The same offsets carried into RELION as per-particle trajectories (`rlnTomoTrajectoriesFile`, Polish's
format; per tilt the shortest 3-D offset whose projection is Warp's offset), reconstructions from the tilt images at
bin 2:

| Particles | rigid star | + Warp's offsets | sign flipped (control) |
|---|---|---|---|
| all 438 | 19.4 Å | **9.2 Å** (the stacks, job019: 9.1 Å) | 20.3 Å |
| SK P11_2 (96) | 14.4 Å | 9.9 Å | 22.7 Å |
| Slot6 P10 (108) | 22.0 Å | 9.2 Å | 24.0 Å |
| FCIso P18_4 (46) | 19.8 Å | 12.0 Å | 29.4 Å |
| Grid2 P10 (171) | 30.5 Å | 15.0 Å | 24.8 Å |

At bin 1 (job023's recipe, `--b 786 --crop 448`) with the offsets: **9.26 Å** in job025's mask (job027: 20.4 Å; the
PI's job040: 9.26 Å, the same FSC shell). CTF refine with that reference and the offsets (job031's settings, ±3000 Å,
λ 0.001, box 512): **0 tilts flagged** (job031: 275); median moves 0.02–0.12 µm, the largest 0.54 µm at +66°;
astigmatism SK 1.01 → 0.66 µm (the PI's 0.71), FCIso 1.02 → 0.62, unmilled2 ~1.0 → 0.5–0.9, Slot6 0.40 → 0.28,
Grid2 0.23 → 0.21. Warp's one astigmatism per series looks overestimated on the 2025–26 sessions.

**Settled on the way.**
- The defocus hand (−1) is right: flipped, every curve is lower (all 438: 0.57 against 0.63 at 12 Å).
- Dose: each star's pre-exposure step is its mdoc's ExposureDose (Grid2 4.84 e/Å² a tilt, the rest ~3.0–3.2); the
  PI's 4.8 for SK P11_2 is his (its mdoc says 3.20). Pixel sizes: 1.55 Å everywhere, as each mdoc.
- job019's 9.1 Å is real: RELION's own cut of the tilt images (with the offsets) gives the same curve as Warp's stacks.
- `relion_refine` on 2D stacks ignores a trajectories file (maps bit-identical with and without) and carries it into
  its output set; CTF refine carries it too; RELION matches trajectories by particle name, so a file holding more
  particles serves a subset (bit-identical result); RELION keeps our `cb*` columns through CTF refine.
- Grid2 P10 stays the weak tomogram with its offsets (15 Å from 171 capsids against 9.2 Å from Slot6's 108): its
  warps are the largest and its dose the highest (155 e/Å²). Data, as far as this test can tell.
- A method that failed: `relion_refine --skip_align --iter 1` outside auto-refine, per tomogram, gives half maps that
  agree at 0.99 out to 8 Å even between disjoint halves (not the reference: 0.19 against it at 20 Å). Unexplained,
  not used.

**What else it touches.** Any RELION program that cuts from the tilt images, on missAligned data: Reconstruct
particle (every bin-1 reference, the ribosome's included), CTF refine, frame alignment (C11, planned), RELION
pseudo-subtomo extraction (subtomoExtraction). M (C6) reads the XMLs and is not affected. Nothing kept so far was
wrong: Refine 3D and Class 3D read the stacks; job002/job004 are low-resolution references.

**D11 (taken 2026-11-19, the maintainer's "build the bridge"; (b), see §15.9): how Warp's local geometry reaches RELION.** Both built from `warp_trajectories.py` (warpylib, in the
missAlign container, as the tilt filter's network runs in its own).
- **(a) At export (recommended).** Warp export writes `warp_trajectories.star` for its particles from the XMLs it
  staged, and names it in its optimisation set. RELION carries it through Refine 3D, Class 3D and CTF refine (checked);
  crboost's own writers of optimisation sets (class sets, `write_optimisation_set`) learn to carry it. Every RELION
  program downstream gets Warp's geometry with nothing to configure. Offsets are taken at the picked positions;
  refinement moves a capsid tens of Å at most, where the warp changes by under 1 Å.
- **(b) At each consumer.** Reconstruct particle and CTF refine (and later frame alignment) compute the offsets for
  their input particles at run time, from the XML a new `cbWarpXml` tomogram column names. Nothing to carry, exact
  positions; three drivers learn it instead of one.
- Either way, those jobs refuse missAligned tilt series whose set carries no trajectories, naming the warps' size.

### 15.9 Built 2026-11-19: the bridge (H26)

On the maintainer's "build the bridge". In the main checkout, uncommitted; drivers only, so no server restart.

**Why (b) and not (a).** At export, the trajectories would reach only sets exported from now on: job019 and everything
downstream of it name none, so C10 would need a new export and a new Refine 3D first, and every crboost writer of an
optimisation set would have to learn to carry the line. At the consumer, the jobs that cut from the tilt images
compute them for whatever set they are given, old or new, and RELION carries the line on from there.

**Built.**
- `services/particles/warp_geometry.py`: per tomogram of the input's particles, the Warp XML (beside the tsCtf job's
  star; in a Warp export's `.staging/task_<ts>/warp_tiltseries/`, the copy the stacks were cut with; for a particle
  set's tomogram, in its own project), what it holds beyond the rigid model (image warps, volume warps, level angles;
  per-tilt GridAngle corrections refused: a trajectory cannot turn a particle), the star checked against the XML's
  rigid model tilt by tilt (ytilt 0.05°, zrot 0.1°, shifts 2 Å), the image and volume extents checked against the XML.
- `containers/miss_alignment/warp_trajectories.py` (warpylib, imports nothing from crboost): the offsets, as the side
  step's `warp_trajectories.py` computed them, into `warp_trajectories.star` (Polish's format) and a JSON report.
- `drivers/warp_bridge.carry_warp_geometry(input_set, job_dir)`: the set RELION is handed. The input's own when its
  XMLs hold nothing beyond the rigid model (any project without missAlign), or when it already names
  `warp_trajectories.star` for all its particles (a set downstream of a bridged job); else `warp_optimisation_set.star`
  in the job dir. Unnamed particles (candidates) are named `<tomogram>/<n>` in `warp_particles.star`. A set naming
  other trajectories (RELION frame alignment's, one day) is refused: the two cannot be combined yet. No XML found for a
  tilt series: a WARN line, and RELION places those particles by the star.
- Called by Reconstruct particle, CTF refine, Subtomo extraction (once in the supervisor; every per-TS set and the merged
  output name the file) and pick-list extraction (its recorded set names it). `write_optimisation_set` takes the
  trajectories; `optimisation_set_trajectories` reads them. CTF refine's failure now names the reference's resolution
  first (F24).

**Checked** (scratch, `/scratch/users/artem.kushner/crboost_side/412_ctf_ref_diag/bridge_*`): on job019's set the
bridge writes the side step's trajectories to 5e-6 Å in 7 s; RELION's bin-2 reconstruction from the set it hands on is
the side step's (9.2 Å, maps equal to 1e-5 of their SD); 2,853 unnamed SK candidates over 125 tilt series named and
written; a bridged set comes back unchanged; foreign trajectories refused; the AreTomo XML reads rigid-only and every
XML from missAlign on holds the warps (SK rerun: 5–61 Å RMS at the candidates per series, Position_11_3 126 Å at a
node). `ruff`, `check_boundaries.py` clean. **Not run:** a crboost job through the bridge.

**Next: C10 again in AGG412_STA**, the maintainer's go needed. A clone of job023 (Reconstruct particle, bin 1),
clones of job025 (Mask) and job027 (PostProcess) on it, job031 (CTF refine, Failed) pointed at the three and run again,
then job024, job028, job029 as planned. The side step's numbers say what to expect: reference 9.26 Å, 0 tilts flagged.
Pending from the side step: Refine 3D after its CTF refine against the control without it (§15.8).

**Submitted 2026-11-19 ~14:00** (plan `p-91683b29`, accepted): C10 again through the bridge, clones of the old jobs with
the same settings. job032 Reconstruct particle (job023's: bin 1, `--b 786 --crop 448`, I1; SLURM 1826762) → job034 Mask
(job025's: 15 Å, 0.15, extend 6, edge 12, 215–271 Å) → job035 PostProcess (the reference's FSC) → job038 CTF refine
(job031's: ±3000 Å, λ 0.001, box 512) → job033 Refine 3D (job024's: local σ 2°, ref job019, mask job016) → job036
PostProcess (in job016) and job037 (in job026). Read-out as planned in §15.6: job020 (9.23 Å) and job030 (8.72 Å, outer)
against job036 and job037; the PI's 8.79 Å. To the project's Trash (recoverable, 20.1 GB): job023, job025, job027 (the
20 Å reference chain) and job031, job024, job028, job029 (failed); §15.7–§15.8 cite their numbers.

**C10 answered (side step, 2026-11-19 ~14:45).** Refine 3D with job024's settings (local σ 2°, from job019's poses, ref
job019, mask job016) on CTF refine's output (`ctf_mot`, through the trajectories) against the same Refine 3D on job018's
CTF (control); PostProcess without B factor, as job020/job030:

| Map | both layers (job016) | outer layer (job026) |
|---|---|---|
| job019 (job020 / job030) | 9.23 Å | 8.72 Å |
| control: one more local Refine 3D, Warp's CTF | 9.34 Å | 9.02 Å |
| the same Refine 3D after CTF refine | **8.35 Å** | **8.10 Å** |
| the PI's Copia/try2 (his outer-layer mask) | | 8.79 Å |

Refine 3D's own mask-corrected figure: 9.23 (job019), 8.82 (control), 8.35 Å (CTF refined). The gain is the per-tilt CTF
(0.9 Å on both layers, 0.6 on the outer), not another round of refinement; on the outer layer we now pass the PI's
8.79 Å. Still bin 2 (Nyquist 6.2 Å). In the project: job038 (CTF refine through the bridge, 0 flagged, moves as the
side step's) → job033 Refine 3D → job036/job037, the copies of record. Next levers: bin 1 (re-export at 1.55 Å with
these poses and the refined CTF), M (C6), frame alignment (C11) now that the bridge carries Warp's geometry.

### 15.10 Toward 7.5 Å: what is left, and four tests (written 2026-11-19, nothing run)

The maintainer asked whether everything together beats the 7.5 Å of the CryoBoost v1 tutorial (cryoboost.readthedocs.io,
extdocs/tutorial). That 7.5 Å is the same capsid ("copia"): one tomogram (Position_1, ~2.95 Å/px; its tomogram is
`Position_1_11.80Apx.mrc`), I1, box 384 cropped to 224; Refine 3D 8.5 Å → Bayesian polish (per-particle motion,
`--it 30000`) → CTF refine (±6000 Å, λ 0.2) → PostProcess 7.5 Å (B −75 by hand; the FSC does not depend on it). Its mask
starts at 73 px = 215 Å: an outer-layer mask like the PI's and job026, so our like-for-like number is the outer layer.
It is another dataset and session: a benchmark, not a strict comparison.

**Where we stand.** 8.10 Å outer / 8.35 Å both layers (side step, §15.9), 438 capsids from four sessions, at 3.1 Å/px
(their sampling, near enough), with CTF refine the only step past Refine 3D. Their polish + CTF refine bought 1.0 Å;
our CTF refine alone 0.6.

**The levers, by expected gain.**
1. Frame alignment / polish (C11): half of their gain, not done here. Possible now that the bridge gives RELION Warp's
   geometry, but see test 3.
2. A second CTF refine round with the new map as reference (the first used 9.26 Å).
3. M (C6): per-tilt CTF, image warps, magnification and poses together, on the XMLs directly (no bridge needed). The
   strongest single lever, a build.
4. Per-session pixel size. Four sessions 2024–2026 are pooled; 0.5 % of magnification moves the outer layer (~250 Å
   radius) by ~1.2 Å, which bites near 7 Å. Their 7.5 Å is one tomogram and never met it. M can refine magnification;
   RELION's tomo tools cannot.
5. Particles: Grid2 P10 is 171 of 438 and the weakest tomogram (15 Å alone with the bridge, Slot6's 108 give 9.2 Å);
   the growth run (job022, every 412 candidate) may add good capsids.
6. Bin 1 (1.55 Å): headroom near 7 Å, not needed to reach 7.5.

My estimate: polish and a second CTF round reach about 7.5 Å; M or per-session pixel sizes are what pass it clearly.
The pooled sessions' magnification is the main risk.

**Four tests**, side steps unless said otherwise (scripts beside §15.8's in `notes/sta_side_steps/412_ctf_reference_diag/`,
maps under `/scratch/users/artem.kushner/crboost_side/412_ctf_ref_diag/`; never write into the project behind the
server; a project run goes through a plan the maintainer accepts):

- **T1, a second CTF refine round, in the project** (also the bridge's first "carried" case: job033's set names job038's
  `warp_trajectories.star`, and the bridge should hand it on untouched). Once job033 → job036/job037 are done: a plan of
  clones reading job033: Reconstruct particle (job032's settings) → Mask (job034's) → PostProcess (job035's) → CTF
  refine (job038's; its optimisation input = job033) → Refine 3D (job033's; input = the new CTF refine, reference
  job033) → PostProcess in job016 and in job026. Read against job036/job037. Decides: whether rounds keep paying.
- **T2, without Grid2 P10.** Refine 3D (job033's settings) on job033's particles minus Grid2 P10 (and minus P10_3), with
  job038's tomograms; PostProcess in job016 and job026; against the 438. Equal or better from 267 particles means Grid2
  costs more than it adds. Beside it, cheap: per-tomogram tilt-image reconstructions from job033's poses with the
  trajectories (`run.sh` mode Amot at `--b 256 --bin 2`) to rank the tomograms at the new resolution.
- **T3, does relion_tomo_align compose with the bridge? (decides C11's design).** Read `relion_tomo_align --help` in the
  RELION image first. Run it on job038's output set (it names the bridge's trajectories) with job035's reference
  halves, job034's mask and job035's FSC; short, per-particle motion on. Then (a) is its output `motion.star` the input
  trajectories plus a small refinement, or a fresh motion that dropped them (diff per particle against job038's
  `warp_trajectories.star`)? (b) Does it rewrite the tilt-series stars' rigid alignment (shifts, angles)? (c) Does a
  bin-2 tilt-image reconstruction from its output beat Amot's 9.2 Å? The bridge as built would refuse that output twice:
  `plan()` checks the star against the XML's rigid model before it looks at carried trajectories, and refuses
  trajectories not named `warp_trajectories.star`. If (a) composes and (b) moves the rigid model, C11's output is RELION's
  whole model, and the bridge must step aside for a set that carries it (a marker of origin, the rigid check skipped
  when the trajectories are the set's own). If (a) drops them, C11 must add the bridge's offsets to its result.
- **T4, per-session magnification, no cluster time.** The per-tomogram bin-2 reconstructions with the trajectories
  (`Amot/{SK,Slot6,FCIso,Grid2}`) against the 438-capsid map (`Amot/all`, or job019's): rescale each by s in 0.98–1.02
  (Fourier crop/pad or real-space interpolation about the box centre), the cross-FSC at 10–20 Å as a function of s, its
  peak per session. A peak off 1.000 by more than ~0.3 % is a mismatch worth correcting (per-session pixel size in the
  tomograms star, or M).

**What not to do.** Raise CTF refine's 1 µm limit; run anything that cuts from the tilt images without the bridge; mix
the tutorial's B-factor into a resolution comparison.
