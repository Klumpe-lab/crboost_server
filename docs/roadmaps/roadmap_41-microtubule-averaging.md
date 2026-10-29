# Roadmap 41 — Microtubules from segmentation to an average (20260820 Tir_Env projects)

**Status:** scoped 2026-10-28; nothing built. 2026-10-29: D1 moved to the two 20260820 Tir_Env projects, D3 and D5
agreed, D4, D6 and D7 deferred (§5). S0 item 1 answered from Ais's v1.2.38 source (§6); the hand-run is owed.

**In one line.** Trace easymode's microtubule segmentations into oriented points along each filament, export them
with Warp like any other picks, and refine them in RELION with helical priors (tilt and psi held near the filament
direction, polarity sorted out per filament), so a microtubule average comes out of the same pipeline that averages
ribosomes.

**Not in this roadmap:** actin (§7; same tracing job, harder data and helical symmetry imposed), seam finding,
TARDIS, M refinement, cross-project pools (roadmap 40 takes those once a filament source exists), manual filament
tracing.

## 1. Goal

**Use case.** In 20260820_B1G4_Tir_Env_mutant and 20260820_B2G2_Tir_Env_Mut, the user adds a Filament pick job on
the microtubule segmentations of a Segment job, sees one coloured line of dots per traced filament on the wall and in the Journey, exports them
with Warp Export, and runs Refine 3D with its helical options on. After a per-filament polarity vote and a second
Refine 3D, the average shows 13 separate protofilaments in cross-section, and every filament's points agree on its
direction.

**Expected yield (rough, measured on MyrGFP_Grid2, the first candidate; the Tir_Env projects hold 209 tomograms
and probably more microtubules, unmeasured until their Segment jobs run).** The Segment manifests store the mean microtubule probability per tomogram
(`fraction_pct`, `services/segmentation/manifest.py`). In job047 the highest is 0.22 % (Position_8_3), eleven
tomograms are above 0.05 %, and the median is 0.023 %. For a 1024 × 1024 × 512 volume at 6.2 Å, 0.2 % is about
0.5–0.8 µm of microtubule, depending on whether the mask fills the lumen. The whole project then holds roughly
5 µm, or **several hundred points at 82 Å spacing**, before false positives are removed. That is enough for a
recognisable 13-protofilament tube at 20–30 Å. It is not enough for the seam or the monomer repeat. More data comes
from other projects through roadmap 40 later (FilamentTracing1_SV_0326 is an obvious candidate).

## 2. Principles

- **A filament pick job is an ordinary picker.** Its output has the same envelope as a candidate extraction
  (`candidates.star`, `tomograms.star`, `optimisation_set.star`), so Warp Export, the subtomo extraction, Refine 3D
  and the particle views take it without special cases beyond the producer name.
- **Orientation is data, not a guess.** Tilt and psi come from the traced filament direction and are written both as
  angles and as priors. Rot is unknown and says so: it is searched globally. Polarity is unknown and says so:
  `rlnAnglePsiFlipRatio 0.5` until the vote.
- **Rules mark, they never drop.** Short or highly curved segments, and filaments whose polarity vote is undecided,
  stay as rows with a column that says why; a set leaves them out.
- **Picks live on the reconstruction the segmentation was made on.** job047 segmented the easymode-iso denoising
  (job046) of `tsReconstruct__2` (missAlign, job024); its manifest names `recon_instance: tsReconstruct__2`. The picks
  belong to that reconstruction's frame and extract through its tsCtf job (`tsCtf__2`), never on job009. In the
  Tir_Env projects missAlign runs before tsCtf, so their one reconstruction (`tsReconstruct`, job007, with tsCtf
  job006) is the frame.

## 3. What exists (anchors, 2026-10-28)

**Segmentations.** Two Segment jobs on MyrGFP_Grid2 hold microtubule volumes, both the native-engine `sv3-3d`
model resampled to 10 Å and mapped back onto the 6.2 Å tomogram grid:

| Job | Input | Tomograms | Highest mean p | Above 0.05 % |
|---|---|---|---|---|
| job047 `segment__3` | easymode iso (job046) of job024 | 48 | 0.22 % | 11 |
| job045 `segment` | cryoCARE (job041) of job024 | 39 | 0.20 % | 10 |

Files are `External/job047/segmentations/<tomo>__microtubule.mrc`, int8 p × 127, shape 512 × 1024 × 1024, with
`segmentations.json` listing the tomogram, feature, voxel size, `recon_instance` and source MRC for each.
The Segment job's output slot is `JobFileType.SEGMENTATIONS` (`services/jobs/segment.py:45`).

**Tracing tool.** The easymode container (`easymode_1.2.5.sif`) ships Ais 1.2.38 (job047's `run.out`). `ais pick
-filament`, read from the v1.2.38 source (`Ais/core/filaments.py`, `Ais/main.py`, 2026-10-29):
- Flags: `-d -t -ou -threshold (128) -b (1) -spacing (Å, 10) -size (Å³, 1) -length (Å, 500) -m (binned px, 16)
  -p (1) --twist (0)`. The parser is `parse_known_args`: a misspelt flag is ignored without a word, and `-twist`
  with one dash would parse as `-t wist`.
- Threshold: strict `>`; for int8 input it is halved, so the default 128 means p > 0.5 on our p × 127 volumes.
  Spacing, size and length go to voxels through the header's voxel size (0 → 10 Å fallback; ours carry 6.2 Å).
- No closing or filling before `skimage.morphology.skeletonize`; a hollow mask gives the wall's skeleton.
- Every connected component above `-size` is skeletonised as a full-volume mask, one after another, so `-b 2`
  (mean-pooled) and a real `-size` are the speed levers.
- Branch points are deleted: crossings split into separate filament IDs with a gap at the junction; side branches
  shorter than `-length` are pruned; closed loops are dropped. Spline `s = 500`, resampled by arc length.
- Angles: `tilt = arccos(t_z)`, `psi = atan2(t_y, −t_x)`, rot 0 (+ k × twist). That is RELION's convention with the
  reference's Z axis laid along the tangent (third column of `Euler_angles2matrix`), so these are the priors as they
  stand. The tangent's sign is kept continuous along a filament, so polarity is consistent within one and arbitrary
  between filaments.
- Output `<tomo>__<target>_coords.star`, block `particles`: `rlnCoordinateZ/Y/X` (0-based voxel indices of the
  input grid, multiplied back by `-b`, corner origin), `rlnAngleRot/Tilt/Psi`, `aisRadiusOfCurvature`,
  `aisLog10RadiusOfCurvature`, `aisFilamentID` (0-based per tomogram), `aisFilamentConnectivity` (1 free end, 2 at a
  junction, 0 inside), `rlnMicrographName` and particle ids. No `rlnHelicalTubeID`, no prior columns.
(Survey: `docs/reports/tool-survey/segmentation-and-annotation-models-2026-10.md` §2.1–2.2.)

**Tir_Env projects (2026-10-29).** 20260820_B1G4_Tir_Env_mutant (63 tilt series) and 20260820_B2G2_Tir_Env_Mut (146):
AreTomo + missAlign, tsCtf job006, tsReconstruct job007 at 6.2 Å (B1G4 finished, B2G2 running). No Denoise or
Segment job yet.

**Export.** `drivers/warp_export.py` reads any optimisation set and `services/particles/warp_export.py`
`warp_input_particles` (:162) requires centred Å coordinates and `rlnAngle*`. It refuses `rlnTomoSubtomogram*`
columns, so RELION 5's own filament convention (subtomogram angles along the filament, `rlnAngle* = 0/90/0`) cannot
be used here; the filament frame goes into `rlnAngle*` directly. `relion_particles_from_warp` (:255) relies on Warp
carrying the input's centred columns through to its output. Whether Warp carries *every* extra column (tube ID,
priors) is unverified (S0). The producer is pinned as `{"optimisation_set": "tmextractcand"}`
(`services/jobs/warp_export.py:124`).

**Refinement.** `services/jobs/refine3d.py` builds `relion_refine_mpi --auto_refine --split_random_halves` with no
helical or prior options; `check_angle_priors` (`drivers/refine3d.py:42`) refuses a local search on particles
without angles. Reconstruct Particle already has helical options (`drivers/reconstruct_particle.py:77`). Class 3D
exists for the protofilament-number split.

**Particle views.** `sources_for_species` (`services/particles/analytics_store.py:127`) and the pick producer in
`services/particles/boundary.py:112` only know `tmextractcand`; a filament pick job is invisible to them until it is
added.

## 4. Data model

One row per traced point, written by the Filament pick job into `candidates.star`:

| Column | Value |
|---|---|
| `rlnTomoName` | tomogram of the segmented reconstruction |
| `rlnCenteredCoordinate{X,Y,Z}Angst` | Ais voxel index → Å with the same centre convention as `WarpVolume.centre_angst` |
| `rlnAngleRot` | uniform random in [−180, 180) (searched globally) |
| `rlnAngleTilt`, `rlnAnglePsi` | from the filament tangent at that point (convention fixed in S2) |
| `rlnAngleTiltPrior`, `rlnAnglePsiPrior` | equal to tilt and psi |
| `rlnAnglePsiFlipRatio` | 0.5 (polarity unknown) |
| `rlnHelicalTubeID` | Ais filament ID, unique within a tomogram (RELION's scope) |
| `rlnHelicalTrackLengthAngst` | arc length from the filament's first point |
| `cbFilamentLengthAngst`, `cbFilamentCurvature` | per filament / per point, for marking |
| `rlnAutopickFigureOfMerit` | mean segmentation p in a small sphere at the point |
| `rlnRandomSubset` | 1 or 2 per tube: boxes 82 Å apart overlap by ~80 %, so a tube never straddles the halves |

`tomograms.star` and the optics come from the reconstruction named in the manifest, the same way
`build_list_optset_from_tomograms` synthesises them for a de-novo species (`drivers/extract_pick_list.py`).

## 5. Decisions (proposed 2026-10-28, answered 2026-10-29)

- **D1 Source = 20260820_B1G4_Tir_Env_mutant and 20260820_B2G2_Tir_Env_Mut** (maintainer, 2026-10-29: more
  microtubules expected there). Each gets Denoise predict (easymode, iso, as MyrGFP_Grid2 job046) on tsReconstruct
  job007, then Segment (easymode, `microtubule` + `actin`, TTA 8, as job047). MyrGFP_Grid2 job047 stays the
  tracing trial while those run.
- **D2 Tracer = `ais pick` in filament mode** from the easymode container. It is already installed and reads the
  mask directly. Writing our own skeletoniser is more code for the same result; TARDIS runs its own segmentation and
  ignores easymode's. Revisit only if S0 shows Ais tracing breaks on our masks (see D2 risk in §6). Open until
  the S0 hand-run; nothing is added to crboost before it.
- **D3 Spacing 82 Å** (one αβ dimer), so a point never sits halfway between two tubulin dimers. Warp Export at
  binning 2 (12.4 Å/px), box 40 px (496 Å), diameter 300 Å. Agreed 2026-10-29.
- **D4 Reference = a published 13-protofilament microtubule map**, resampled to 12.4 Å, low-passed to 40 Å, and
  registered as the template of a new species `microtubule` (Ø 250 Å, C1). A plain cylinder made from the data has
  no polarity or protofilament skew to align to. Deferred: the maintainer picks the EMDB entry at S1.
- **D5 No helical symmetry imposed** (`--ignore_helical_symmetry`): the seam breaks it, and at this resolution it
  adds nothing. Priors carry the helical knowledge: Ais's tilt/psi are them already (§3), copied into the prior
  columns. Agreed 2026-10-29.
- **D6 (deferred to the first Refine 3D) Polarity by per-filament majority vote** after the first Refine 3D: within each `rlnHelicalTubeID`, the psi
  flips relative to the prior that most points agree on win; the others are flipped. Filaments with less than a 2:1
  majority are marked, not flipped, and left out of the set that the second refinement reads.
- **D7 New job type `filamentPick`**, not a mode of the candidate extraction: different input (segmentations, not TM
  scores), different parameters, and actin reuses it. Deferred until the S0 hand-run shows the tracing works.

## 6. Stages

**S0 — facts before code (one short session, maintainer runs the cluster commands).**
1. ~~`ais pick --help` inside `easymode_1.2.5.sif`~~: answered from the v1.2.38 source (§3, 2026-10-29).
2. Look at the XZ and XY overlays of job047's microtubule volume for Position_8_3, Position_6_3 and Position_4_5:
   real microtubules or false positives, and **is the lumen filled?** A hollow-tube mask skeletonises into a tube
   surface, not a centreline; if it is hollow, the mask is closed (binary fill per cross-section, or a 15 nm closing)
   before tracing.
3. Run `ais pick` filament mode by hand on Position_8_3 and open the coordinates over the tomogram (ArtiaX or the
   wall's import): does each microtubule come out as one filament ID, do crossings split cleanly. On a CPU node:
   ```bash
   SIF=/groups/klumpe/software/containers/sifs/easymode_1.2.5.sif
   SEG=/groups/klumpe/crboost_data/AGG412_20260311_MyrGFP_Grid2/External/job047/segmentations
   OUT=$HOME/mt_trace/myrgfp
   mkdir -p $OUT/in $OUT/.ais $OUT/.mpl
   ln -s $SEG/AGG412_20260311_MyrGFP_Grid2_Position_8_3__microtubule.mrc $OUT/in/
   ln -s $SEG/AGG412_20260311_MyrGFP_Grid2_Position_6_3__microtubule.mrc $OUT/in/
   apptainer exec --cleanenv -B /groups $SIF bash -c "export AIS_CONFIG_DIR=$OUT/.ais MPLCONFIGDIR=$OUT/.mpl; ais pick -d $OUT/in -t microtubule -ou $OUT -filament -b 2 -spacing 82 -length 1000 -size 1e7 -p 2"
   ```
   `-size 1e7` Å³ drops components smaller than ~20–30 nm of tube; `-length 1000` drops pieces and prunes side
   branches under 100 nm. MyrGFP_Grid2 job046 and job047 were deleted on 2026-10-29, so the trial runs on B1G4's
   Segment job008 (raw tsReconstruct input, `actin` + `microtubule`) instead: same command, `SEG` = its
   `segmentations/`, the two tomograms with the highest microtubule `fraction_pct`.
4. Whether `WarpTools ts_export_particles` carries `rlnHelicalTubeID`, `rlnAngleTiltPrior`, `rlnAnglePsiPrior` and
   `rlnAnglePsiFlipRatio` from its input star into its output star.
5. `relion_refine --help` in the RELION container: confirm `--helix`, `--ignore_helical_symmetry`,
   `--helical_outer_diameter`, `--sigma_tilt`, `--sigma_psi`, `--helical_keep_tilt_prior_fixed` and `--bimodal_psi`
   are accepted together with `--ios` on 2D particle series (RELION 5 tomography), not only for single particles.
6. D4: the EMDB entry for the reference.

**S1 — species.** Species `microtubule` with the reference from D4 as its template (Species page import; no code
expected). Its colour is what the dots use.

**S2 — Filament pick job.** `services/jobs/filament_pick.py` + `drivers/filament_pick.py` on `ArrayDriver`, one CPU
task per tomogram.
- Input slot `input_segmentations` (`JobFileType.SEGMENTATIONS`), preferred source `segment`. Parameters: `feature`
  (choice from the manifest's features, default `microtubule`), `threshold_p`, `spacing_ang` (82), `min_length_ang`
  (branch pruning), `close_lumen` (from S0 item 2), `edge_margin_ang`, cpus per task. `species_id` as on other
  particle jobs.
- Task: stage the mask (closed if asked), run `ais pick` filament mode in the easymode container (offline proxy and
  per-task config dirs as in `drivers/segment.py`), convert to the §4 columns.
- Aggregate: `candidates.star`, `tomograms.star` from the manifest's `recon_instance`, `optimisation_set.star`, and a
  per-tomogram summary (filaments, points, total length) for the job page.
- Angle convention: a synthetic check that a straight line along a known direction gives tilt/psi which, applied to
  a reference whose helix axis is Z, put the axis back along that direction. This fixes the convention once and stays
  as a test.

**S3 — seen where picks are seen.** Add `filamentPick` next to `tmextractcand` in `sources_for_species` and the pick
producer (`services/particles/boundary.py`), so the wall, the Journey layers and the Analytics source list show the
points in the species colour. Colouring by tube ID is a later nicety, not part of this stage.

**S4 — export.** Warp Export accepts the filament pick job as its `input_optimisation` producer
(`get_input_requirements`, `services/jobs/warp_export.py:124`). If S0 item 4 says Warp drops the helical and prior
columns, `relion_particles_from_warp` merges them back by the carried centred coordinates, which it already matches
row for row.

**S5 — helical Refine 3D.** New Refine 3D parameters, off by default and grouped as a "Helical" section in the job
tab: `helix`, `helical_outer_diameter` (300 Å), `ignore_helical_symmetry` (on), `sigma_tilt` and `sigma_psi` (5°),
`sigma_rot` (−1 = global), `keep_tilt_prior_fixed`, `bimodal_psi` (on while polarity is unknown). With `helix` on,
`check_angle_priors` also requires the prior columns and the tube ID. First run: reference = the S1 template through
Reconstruct Particle's resampling or directly, `ini_high` 40, healpix 3.

**S6 — polarity vote.** A pure service (`services/particles/filament_polarity.py`) over Refine 3D's particles:
per tube ID, compare refined psi with the prior, count the agreement, flip the minority, and mark undecided
filaments. It produces an ordinary particle set (roadmap 30 set tools / the Subset job of roadmap 32, whichever is
on main when this is built) that the second Refine 3D reads with `bimodal_psi` off. The job page shows a
per-filament table: points, agreement, verdict.

**S7 — protofilament number (optional, only if the S6 map looks mixed).** Class 3D with 13- and 14-protofilament
references, priors held. Drosophila microtubules are expected to be mostly 13.

## 7. Actin afterwards

The same Filament pick job runs on `actin`, but there are three differences: (a) easymode's actin model is the
Ais-engine `sv2-3d.scnm` (XY-only resampling, see roadmap 38), so its masks are weaker; (b) actin in bundles and
meshworks crosses and touches, which is where skeleton tracing tangles, so TARDIS (`tardis_actin`) becomes the
likely tracer; (c) helical symmetry is imposed (twist −166.7°, rise 27.5 Å), spacing ~55 Å, box ~300 Å. That is
its own roadmap, written once microtubules have averaged.

## 8. Runtime checklist (once, after S6)

1. Filament pick on each Tir_Env project's Segment job, `feature=microtubule`: the job page summary lists filaments
   per tomogram; the tomograms with the highest microtubule `fraction_pct` have the most.
2. The wall shows the dots along the visible microtubules in those tomograms; nothing in tomograms whose overlay
   has no microtubule.
3. Warp Export from it: `particles.star` keeps the tube IDs and priors.
4. Refine 3D, helical on: converges; the refined tilt/psi stay within a few degrees of the priors.
5. Polarity vote: most filaments decided; the second Refine 3D shows 13 protofilaments in cross-section.

## 9. Build record

Nothing built yet. 2026-10-29: decisions answered (§5), Ais facts read from its source (§3), S0 hand-run handed over.
