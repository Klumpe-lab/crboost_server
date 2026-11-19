# Roadmap 64: Picking from segmentations

**Asked (2026-11-19).** The PI: easymode turns its segmentations into particle lists, and its ribosome
segmentations are good enough to give template-matching-like picks at a fraction of the cost. The same should work
for filaments (microtubules) and for any other particle a segmentation can position. TM's initial orientations are
poor anyway, so such picks can start from random angles and be aligned from there. That works for ribosomes, capsids
and other globular targets; less contrasty ones need priors from geometry to lock in. The ribosome segmentation also
fires in the nucleus (pre-ribosomes), maybe a story in the follicle cells. The maintainer: fold the general
particle-lists-from-segmentations path into the filament work. One job whose settings differ, not more job types.
Get the shape of the integration right first (is this Picking, a kind of TM, or a feature of Segment?) and build it
before prototyping on any data.

**Status (2026-11-19).** Scoped, nothing built. Taken by the maintainer: D1, D3, D4, D5. Later the same day: D2
(Picking), D6 (Ais detects both shapes, in the image as it is), D8, and D9 (with `_ais` on the Segment job's feature
names). Proposed: D7 and D10–D13. D10 hangs on S1's check that the TM path can start without a GPU.

No image upgrade is needed: the image's Ais 1.2.38 already has `-orient` (§4). The survey that said otherwise was
wrong.

## 1. Goal

**Use case.** A project's Segment job holds ribosome, nucleus, cytoplasm and void. The user adds Pick candidates for
the ribosome species and sets its source to the segmentation, or adds it from the Segment job's output on the map,
which sets the source already. Seconds of CPU per tomogram later:
- The wall and the Journey show a dot in each ribosome blob.
- The job page shows the score histogram and the picks per tomogram.
- Every pick says how much nucleus, cytoplasm and void surrounds it.

After that:
- Warp Export takes the picks like any others.
- Reconstruct Particle of their random angles gives a round, featureless average.
- A Refine 3D from the species template searches globally, and Run refuses a local search until a job has refined
  the angles.
- In the STA kitchen, the nuclear picks (pre-ribosomes) are one filter away from the cytoplasmic ones.

The same job, with its shape set to filaments and the microtubule feature, replaces roadmap 41's Filament pick job.

**First runs (D5: after the build).**
- The eight AGG412 reruns: ribosome picked from a segmentation next to the 346,834 TM candidates they already hold.
  Compare the overlap per tomogram, the yield, and a Refine 3D from random angles against roadmap 53's ribosome route.
  Then split by nucleus wherever a lamella cuts one.
- Then microtubules on roadmap 41's Tir_Env projects.

**What easymode itself shows.** Its ribosome tutorial picks 500 yeast tomograms (`easymode pick ribosome --binning
3 --size 2000000 --spacing 250`, 109,081 coordinates) and refines the large subunit to 3.4 Å in M with no 3D
classification. Picking itself is seconds per tomogram. The GPU cost is the segmentation: ribosome took 4.7 min per
1024 × 1024 × 512 tomogram at TTA 2 here, and TTA 8 takes four times that (`services/jobs/segment.py`, SLURM note).
easymode reports 16 s to segment TRiC against 370 s to template-match it, per tomogram.

## 2. Where it lives (D2, proposed): Picking

Template Match and Segment both make evidence volumes over a tomogram. TM gives a score per voxel (with its best
angle) for one template; Segment gives a probability per voxel for each feature. Pick candidates turns evidence into
one species' pick list: peaks at least a particle apart, written as `candidates.star` plus the coordinates-only
optimisation set that every particle job reads. Picking from a segmentation is that same step on the other kind of
evidence, so it is Pick candidates with a **source**: template matching or segmentation.

- **Not a kind of TM.** There is no template, no GPU and no angle search. A Template Match job that matches nothing
  half the time is the confusing one, and the TM → Pick pairing the particle views rely on (`tm_instance_for_ce`)
  would mean nothing.
- **Not a feature of Segment.** Segment runs once per project on GPUs, per feature, not per species. Many readers share
  its volumes: the overlays, TM search regions, context (§5.4) and several species' picks. Picks are per species,
  cheap and re-tuned; changing a threshold must not re-run a GPU job or change a finished one.
- **Picking costs the least plumbing.** The places that mean "the job that writes a species' picks" already key on
  this job type (`JobType.TEMPLATE_EXTRACT_PYTOM`): analytics sources, kitchen members, set parents, the subtomo
  link, `species_jobs`, and the Warp Export and Subtomo Extraction producer pins. They see segmentation picks without
  roadmap 41's D10 sweep. Only the places that mean TM itself branch on the source (§4).
- **Outside segmentations come for free.** Imported segmentations are SEGMENTATIONS producers too
  (`importedSegmentations__<batch>`), so segmentations made outside crboost can be picked the same way: the PI's own
  easymode runs, MemBrain, an Ais model.
- **No new API verb.** `add_job`, `set_params(source=…, feature=…)` and `set_input` reach it.

## 3. Principles

- **Picks are picks.** The segmentation source writes what the TM source writes: `candidates.star`,
  `tomograms.star` and a coordinates-only `optimisation_set.star`. No consumer learns a second format.
- **The frame is the segmentation's.** Picks sit on the reconstruction the segmented tomograms trace to:
  - for a Segment job, the tomograms star it read (`parameters.input_tomograms` of its manifest), as
    `drivers/filament_pick.py` does;
  - for an import, the tomograms of its `recon_instance`.
- **Angles say where they came from:** TM's best angle, random (uniform on SO(3)), a piece's own axis, or the
  filament's direction. Random is never a silent stand-in: the page says so, and Run guards it (D8).
- **Every pick carries its context** (D4): what the project's other segmentations say at that point, as columns.
  They are kept whether or not anything reads them yet.
- **Rules mark, they never drop.** Roadmap 41's filament marks carry over; nothing else drops a pick.
- **Absent is stated.** With no feature chosen, the row says so. A species never gets a feature by name matching.
- **Star files are untrusted.** Ais's output and the manifests are checked on read (columns, shapes, voxel sizes,
  full scale), as the Filament pick driver does.

## 4. What exists (anchors, 2026-11-19)

**Segment job (`services/jobs/segment.py`).**
- easymode 1.2.5 with Ais 1.2.38 (`easymode_1.2.5.sif`), features from the weights cache: ribosome, proteasome,
  tric, vault, npc, atp_synthase, microtubule, actin, nucleus, cytoplasm, membrane, void, …
- Volumes: `segmentations/<tomo>__<feature>.mrc` on the tomogram's grid (int8 p × 127 by default).
- `segmentations.json` is a `SegmentationManifest` (`services/segmentation/manifest.py:55`). Each entry gives shape,
  voxel size, `full_scale`, `recon_instance` and `fraction_pct` (the mean p).
- Features are project-level `FeatureClass` rows (`services/project_state.py`).
- Volumes are read by `services/segmentation/volumes.py`; TM's search region (`services/segmentation/search_region.py`)
  already builds masks from them.
- Names: `resolve_targets` (`drivers/segment.py:168`) gives each feature an id (`feature`) and the suffix the tool
  writes (`suffix`: easymode's name, or an Ais model's title). Finals are `segmentations/<tomo>__<suffix>.mrc`
  (`_volume_path`, `drivers/segment.py:77`).
- Every volume's provenance records the easymode and Ais versions in the image (`_tool_versions`,
  `drivers/segment.py:275`).

**Ais 1.2.38's blob pick** (`pick_particles`, `Ais/core/util.py:181`; read from its PyPI source on 2026-11-19).
- Threshold: a strict `>` on the stored values. The threshold is ÷255 for float data and ÷2 for int8.
- Mean binning, then connected pieces, then the size cut.
- Placement, either:
  - EDT peaks at least a spacing apart, scored by distance to the piece's edge; or
  - one centroid per piece (`-centroid`), scored by the piece's volume.

  Either score is written as `aisParticleSize`.
- `-orient normal` or `-orient long-axis` (with `-centroid` only) writes tilt and psi from the piece's smallest or
  largest principal axis, with rot 0. `-orient-sign` takes `z`, `center` or `out`.
- Picks within `-margin` voxels of the edge are dropped (16 by default).
- Coordinates are binned voxel indices × binning.

**Ais 1.2.40 (2026-10-20)** adds only `ais segment --stride` and `--xla` (both off by default), several folders for
`ais extract`, and TensorFlow up to 2.15. This roadmap needs none of it.
- A feature's colour is the palette slot of its id, as a species' is (`ProjectState.add_feature`). So a species
  `ribosome` and a feature `ribosome` share both name and colour today: the dots would vanish into the fill.

**Pick candidates.**
- `services/jobs/candidate_extract.py`, driver `drivers/extract_candidates_pytom.py`: a supervisor plus a
  per-tomogram array (`ArrayDriver`). It lists tomograms from TM's job JSONs and merges per-tomogram stars into
  `candidates.star`.
- Its resource profile asks for a MIG slice it never uses (`config/conf.template.yaml:170`). Extraction is numpy on
  the CPU: 140 jobs, 2.0 GPU-h, not a byte of GPU memory (roadmap 45 §3). It needs a device only to start.
  `pytom_extract_candidates.py` imports `pytom_tm.matching`, whose voltools asks cupy for the device count at
  import and raises `cudaErrorNoDevice` on a node without a GPU. voltools catches only a missing cupy (roadmap 45,
  2026-11-08).
- Ø is species-derived (`species_defaults`, `services/pipeline_edit.py:108`; drift notch
  `services/particles/species_jobs.py:75`).
- Its spec depends on TM alone (`services/jobs/spec.py:304`), which feeds the missing-dependency warning
  (`missing_deps`, `services/pipeline_edit.py:39`).

**Filament pick (roadmap 41 S2, built 2026-11-01, never run).**
- `services/jobs/filament_pick.py`, and `drivers/filament_pick.py`: one CPU job running `ais pick -filament`.
- The pure conversion `services/particles/filament_picks.py`: directions, priors, tube ids, marks, halves per tube.
- Ais's facts are in roadmap 41 §3: the flags, the threshold halved for int8, and 0-based corner voxel indices times
  the binning.

**TM-only sites the source must branch at.**
- `services/particles/analytics.py:40` (`SCORE_COL = "rlnLCCmax"` and the σ scores built on it).
- `services/particles/analytics_store.py:130` (source labels "← Template Match …").
- `ui/particles/pick_viewer.py:2491` (`score_field` defaults to the LCC).
- `services/sta_kitchen/table.py:255–330` (member columns σ, LCC, rank and margin from TM's facts).
- `ui/branch_map/facts.py:381, 766` and `ui/branch_map/model.py:747` (Pick candidates' card facts).
- Already score-agnostic: `services/particles/picks_filter.py:79` falls back to `rlnAutopickFigureOfMerit`.

**Angles.**
- `check_angle_priors` (`drivers/refine3d.py:39`) refuses a local search only when angles are missing or mostly
  0,0,0. Random angles pass it.
- RELION's extraction copies whole rows, and Warp's export copies unknown columns row for row (that is how
  `cbPickUid` travels, `services/particles/pick_uids.py`). Context columns written at picking therefore reach the
  particle sets.
- There is no Initial Model job.

**Resources and environment.**
- Resource profiles are keyed by job type: `get_effective_slurm_config` (`services/jobs/_base.py:183`), with names
  checked against the job types (`services/configs/config_service.py:362`).
- The crboost environment has numpy 1.26, scipy 1.11.4 and scikit-image 0.26 (`requirements.in`).

## 5. The job

### 5.1 Parameters (Pick candidates, source = segmentation)

Groups in this order: Source, Segmentation, Detection, Orientation, then the species group (Ø) as today. The filament
groups replace Detection and Orientation when the shape is filaments. The TM groups (Pick threshold, Score map) show
for the TM source only (`shows_param`).

| Parameter | Group | Meaning |
|---|---|---|
| `source` | Source | template matching (today's job) or segmentation |
| `feature` | Segmentation | the feature to pick, chosen from the input's manifest; species-derived when the species names one (D9), stated when absent |
| `shape` | Segmentation | particles or filaments |
| `p_threshold` | Detection | a voxel is the feature where p ≥ this; 0.5 is the network's own decision and the wall's fill (Ais's 128) |
| `min_piece` | Detection | pieces smaller than this fraction of one particle (a sphere of Ø) are left out; easymode's ribosome example is 0.24 |
| `one_per_piece` | Detection | one pick at each piece's centre instead of distance-transform peaks a Ø apart (for particles that never touch) |
| `binning` | Detection | the volumes are binned by this before detection (mean of each block), as Filament pick does |
| `orientation` | Orientation | random, or the piece's long or short axis (one pick per piece only) |
| Ø (`particle_diameter_ang`) | Species | the least distance between two picks, as for TM |

Shape = filaments takes roadmap 41's parameters as built: spacing, min volume, min length, min bend radius and min
angle from Z. It adds `twist` (Ais `--twist`: rot steps by this per point).

Particle detection is Ais's blob mode (D6), so the parameters become its flags:
- `-threshold`: on Ais's 0–255 scale, from `p_threshold` and the volume's encoding (halved for int8, as the Filament
  pick driver works it out).
- `-size`: `min_piece` × the sphere of Ø, in Å³.
- `-spacing`: Ø.
- `-centroid`: one pick per piece.
- `-orient long-axis` or `-orient normal`: the axes.
- `-b`: binning.
- `-margin 0`: picks near the volume's edge are kept and marked by us (`cbMarkReason` `edge`, within ½ Ø), never
  dropped.

### 5.2 Inputs

- `input_segmentations` (SEGMENTATIONS) is the evidence for the segmentation source and the context for both sources
  (§5.4).
- `input_tm_job` and `input_tomograms` belong to the TM source only, using the slots' `when=` as `restricts_search`
  and `uses_trained_model` do.
- A Pick candidates job added from a SEGMENTATIONS output (the map's "+", or the API's `add_job` with that input)
  starts with source = segmentation.

### 5.3 Outputs: one row per pick

| Column | Value |
|---|---|
| `rlnTomoName`, `rlnCenteredCoordinate{X,Y,Z}Angst` | on the frame (§3) |
| `rlnAngleRot`, `rlnAngleTilt`, `rlnAnglePsi` | per the orientation: random, or the axis with a random rot (or TM's angles) |
| `cbAngleSource` | `tm`, `random`, `long_axis`, `short_axis` or `filament` |
| `rlnAutopickFigureOfMerit` | mean p of the feature within ¼ Ø of the pick: the score the histograms and filters read (D3, D12) |
| `cbPieceVolumeNm3`, `cbPiecePicks` | the piece the pick sits in: its volume, and how many picks share it (1 is alone; many is a cluster, a polysome or junk) |
| context | §5.4 |
| filament columns | roadmap 41 §4 as built: tube id, priors, flip ratio, track length, curvature, `cbMarkReason` |

`summary.json` per tomogram: pieces, picks, the median score, and per context feature how many picks sit inside it.

### 5.4 Context (D4)

Both sources record context from every feature of the segmentations input; the segmentation source skips the feature
it picked, since its score already covers it. For each feature:
- `cbP_<feature>`: the mean p within ½ Ø of the pick, i.e. inside the particle's own volume.
- `cbDistAngst_<feature>`: the signed distance from the pick to that feature's p ≥ 0.5 surface, negative inside.
  It is computed on the detection grid and is precise to its voxel.

A TM-sourced job takes context only from segmentations made on its picks' reconstruction. Another reconstruction is
refused at Run, naming both; frames are never mixed.

The PI's examples, with the Segment job's `_ais` names (D9):
- The nuclear ribosomes are `cbP_nucleus_ais ≥ 0.5`.
- The distance to the lamella surface is `cbDistAngst_void_ais`.
- TM picks the ribosome segmentation does not see have a low `cbP_ribosome_ais`.

### 5.5 Execution

One supervisor and a per-tomogram array, for both sources and both shapes (D11).

- **Particles (D6).** Each task runs `ais pick` in blob mode on its tomogram in the easymode image, with §5.1's
  flags and one process. A pass in the crboost environment then reads the feature's volume and the context volumes.
  It labels the pieces at Ais's threshold and binning (for piece volume and picks per piece), and computes each
  pick's score, random angles where Ais wrote none, and the context.
- **Filaments.** Each task runs `ais pick -filament` on its tomogram in the easymode image, then roadmap 41's
  conversion. The supervisor's merge balances the tube halves across the project.
- **Resources.** The segmentation source runs on CPUs only. Whether the TM source joins it on one CPU profile is D10.

### 5.6 What the user sees

- **The job page.**
  - Before a run: the Source row comes first, and the feature row lists the manifest's features with their coverage
    (`fraction_pct`, "ribosome: in 46 of 48 tomograms").
  - After a run: the score histogram, picks per tomogram, and the context summary.
  - For random angles, one line: "Angles: random. The first refinement searches globally; Run refuses a local search
    until a job has refined them."
- **The map card** shows the source and feature ("from Segment job012 · ribosome").
- **The particle views (S4).**
  - Analytics and the pick viewer read the source's score, LCC or the figure of merit.
  - σ ladders are TM's and say so for this source.
  - The kitchen's member table carries the figure of merit as its score and every context column as a filterable
    column.

## 6. Decisions

**Taken (maintainer, 2026-11-19).**
- **D1 One job type.** One job whose settings differ by source and shape; no new job type. Filament tracing is one of
  its shapes, and the Filament pick job (never run) retires (S6).
- **D3 Score.** Detection thresholds on p. Each pick carries a score (`rlnAutopickFigureOfMerit`) that the existing
  histograms and filters read.
- **D4 Context on every pick.** Columns on every row, written whether or not the kitchen reads them yet (it will).
- **D5 Build first, then run.** The integration is built before any prototype on data; the first runs are §1's.
- **D2 Where it lives: Picking** (§2).
- **D6 Ais detects both shapes, in the image as it is.**
  - Blobs and filaments both come from `ais pick`. The image's Ais 1.2.38 has `-orient` (§4), so no upgrade is
    needed.
  - crboost's pass after Ais adds what Ais does not write: scores, pieces, random angles and context.
  - If an upgrade comes later, every volume already records the easymode and Ais versions that made it
    (`drivers/segment.py:275`).
- **D8 The random-angle guard reads lineage, not a column.**
  - Run, and the Refine 3D and Class 3D drivers, refuse a local search when the input's angles were last written by a
    picker with random orientation. That is found by walking the input back to the job that last wrote angles.
  - A column alone would ride through RELION and Warp, which copy unknown columns, and would still say "random" after
    the first refinement. `cbAngleSource` stays on the picks as the record.
- **D9 The species names its feature, and the Segment job's features end in `_ais`.**
  - `ParticleSpecies` (`services/project_state.py:204`) gets an optional `segmentation_feature` beside Ø, shown on
    the Species page. The job takes it as a species-derived field: a snapshot at creation, with a drift notch like
    Ø's. Without one, the row says so and lists the manifest's features.
  - The Segment job names what it writes `<feature>_ais` (`ribosome_ais`, or an Ais model's title + `_ais`). Volume
    files, manifest entries, the feature registry, colours and the context columns all carry it, so a species
    `ribosome` and the feature `ribosome_ais` never share a name or a colour (§4).
  - Projects segmented before keep their ids. A TM search region set up on an old id refuses a new Segment job's
    manifest at Run, naming the features (`search_region_problem`).
  - Imports keep the names the user gives them.

**Proposed (waiting for a word).**
- **D7 Orientation for particles.**
  - Random by default: uniform on SO(3), with rot and psi uniform and cos(tilt) uniform, seeded per job.
  - Long or short axis, one pick per piece only (Ais `-orient long-axis` or `-orient normal`): tilt and psi from the
    axis, rot random. For rods like the proteasome and discs like the NPC.
  - Random rather than 0,0,0 because Reconstruct Particle of random angles is a rotational average, an unbiased first
    reference. 0,0,0 stacks every particle in one orientation.
- **D10 One CPU profile, if the TM path can start without a GPU.**
  - The segmentation source never imports pytom.
  - If the TM source's extraction can start on a node without a GPU, both sources share one CPU-only `tmextractcand`
    profile and the slice goes (§4). Two ways, checked in S1:
    - a launcher in our task that makes voltools see no device before pytom imports (nothing in the image changes);
    - voltools' import guard catching the CUDA error in the pytom image (a tool patch: the maintainer's call).
  - If neither works, the job names its profile per source (`resource_profile_key()`), with a CPU-only one for the
    segmentation source.
- **D11 Per-tomogram array for both shapes.** This reverses roadmap 41 D8 for the sake of per-tomogram status,
  failure isolation, top-up and the Tasks tab, like every other per-tomogram job. The price for filaments is a
  container start per task.
- **D12 Score and context definitions** as §5.3 and §5.4: ¼ Ø for the score, ½ Ø for context p, signed distance to
  the p ≥ 0.5 surface.
- **D13 How it reads.** "Pick candidates" for both sources, with the source and feature on the map card and the page
  head. The roster is unchanged. Open: whether the roster row's name should carry the source too.
- **Names stay.** `TEMPLATE_EXTRACT_PYTOM` and `tmextractcand` stay. A rename is a sweep across files other sessions
  hold open; it can come later.

## 7. Stages

**S1: the source and the names.**
- `PickSource`, the `source` field, `shows_param`, and the slots' `when=`.
- `missing_deps` knows either source.
- A job added from a SEGMENTATIONS output starts with the segmentation source.
- The TM path keeps its driver as one half of a driver that dispatches on the source; nothing else of it changes.
- D10: start the TM path on a node without a GPU, trying the launcher first. Then either one CPU profile for both
  sources, or `resource_profile_key()` and a second profile.
- D9:
  - `resolve_targets` gives each feature the id `<name>_ais`. Finals, overlays, the manifest and the registry take
    the id; the staging lookup keeps the tool's own suffix.
  - `ParticleSpecies.segmentation_feature` and its drift notch.

**S2: particles.**
- The task: `ais pick` in blob mode with §5.1's flags and one process. Ais's coordinates are converted as the
  Filament pick driver does: 0-based corner voxels × binning, to centred Å on the manifest's grid.
- `services/particles/seg_picks.py`, pure: pieces at Ais's threshold and binning, scores, random angles, angle source.
- The frame from the manifest, and `summary.json`.
- Refusals: a feature the manifest lacks; a voxel size more than 1 % from the tomogram's pixel size; a full scale the
  manifest does not state.
- Checked in the scratchpad:
  - the pure part on synthetic volumes (spheres at known centres, touching pairs, rods and discs);
  - random angles uniform on SO(3);
  - the driver's run with Ais faked by a canned output, as the Filament pick build was.

**S3: context.**
- The context columns for both sources, and the reconstruction check at Run.
- The TM tasks gain the context pass after pytom's extraction, before the supervisor merges.

**S4: seen where picks are seen.**
- The score per source in analytics, the pick viewer and the kitchen.
- Context columns in the kitchen's member table and its filters.
- The map's card facts and the analytics source labels ("← Segment job0NN · ribosome").

**S5: honest angles.**
- D8's guard in Run's checks and in the Refine 3D and Class 3D drivers.
- The page line, and Reconstruct Particle's note for random angles ("a rotational average").

**S6: filaments move in.**
- Shape = filaments: `ais pick -filament` per tomogram task, `filament_picks.py` as built, halves balanced in the
  supervisor. `FilamentPickParams`'s fields become the shape's.
- `JobType.FILAMENT_PICK`, its spec, driver and profile go; it never ran (roadmap 41 §9).
- Roadmap 41's S3 and S4 are this roadmap's S4 and S6. Its S5–S7 (helical Refine 3D, polarity vote, protofilament
  number) stay there.

**Later, not in this roadmap.**
- Particles on a surface, oriented by a membrane's normal: NPC and ATP synthase (easymode has tutorials for both).
- An Initial Model job (RELION 5's de novo reference), for species without a template.
- TM at segmentation picks: angles from a local match.
- A one-tomogram preview on the job page, to tune p and Ø before running every tomogram.

## 8. Runtime checklist (once, after S6)

1. **AGG412 reruns.** Start with one project. A Segment job with ribosome, nucleus, cytoplasm and void (TTA as the
   maintainer chooses), then Pick candidates from the segmentation, ribosome feature.
   - The wall shows one dot per ribosome blob.
   - Compare counts per tomogram with the TM candidates.
   - Count TM picks with `cbP_ribosome_ais` < 0.5, and segmentation picks with no TM pick within ½ Ø.
2. **Axes:** on a feature with elongated pieces (proteasome), the long-axis picks' directions follow the pieces
   (ArtiaX).
3. **Warp Export, then Reconstruct Particle:** a round blob.
   - A Refine 3D with a local search is refused at Run.
   - A global Refine 3D from the species template converges. Compare its resolution with the TM route (roadmap 53).
4. **The kitchen:** a `cbP_nucleus_ais` filter separates the nuclear picks, and their Class 3D runs on its own.
5. **Microtubules (Tir_Env, roadmap 41):** shape = filaments on the Segment job, as roadmap 41 §8 items 1–3.

## 9. Not in this roadmap

- The Later list of §7.
- Changes to Segment itself (its features, TTA, models). This roadmap only reads its manifest and volumes.
- Actin's helical specifics (roadmap 41 §7) and everything after the filament picks (roadmap 41 S5–S7).
