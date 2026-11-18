# Roadmap 53 — STA of the reruns: ribosome and 412 classes as far as they go, and the particle map built from doing it

**Status:** scoped 2026-11-17 from the eight finished reruns of roadmap 49; nothing run, nothing built. D1 and D2
taken the same day (§8). Which particle jobs to build: §11; Mask, PostProcess, the mask input and `skip_align`
built the same day (§11.1, not run through SLURM), M's facts read. The particle map scoped against the code: §10;
the handoff for building it as its own view: §13, its decisions and design settled the same afternoon, nothing built:
§13.1. What happens next: §12. Phase A partly run (references, bin-4 exports; no Class 3D yet): §12.1, read 2026-11-18; the nine Class 3D
submitted the same day (§12.1). Class 3D on several GPUs (MPI) and continuing after a time-out: built 2026-11-18,
not run (§11.2). The particle map taken out of the UI 2026-11-18, to come back as part of the branch map (§13.3,
roadmap 32 §15.9). The PARTICLES header's Aggregate button removed and Import tomograms moved to the Tomograms view
the same day (§14.7); the rest of §14 is owed.

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
| D4 | Geometry by M (a new job, multi-source, refines pixel scale per project) or RELION 5 tomo CtfRefine + frame alignment | M, built as one job (§11); RELION's only if M will not take eight Warp sources |
| D5 | 412 symmetry, from V2's C1 map | decided by the map |
| D6 | Delete the TM score and angle maps (~7 TB) now that every candidate is exported | yes, once R1 has fixed the ribosome σ cut and V1 the 412's |
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
frame alignment.

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
