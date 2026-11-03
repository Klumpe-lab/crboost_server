# 412 capsid (~50 nm) — methods provenance for the paper

*Written 2026-10-22 from the job logs of the May–July 2026 run. Covers only the ~50 nm capsid (the 412
particle). Copia and the ~30 nm capsid were processed outside this record.*

The draft methods paragraph names tools that did not process this particle. What did, with the files that
show it, is below. A rewritten paragraph is at the end.

## 1. Tools and versions that actually ran

| Step | Tool, version | Evidence |
| --- | --- | --- |
| Motion correction + per-tilt CTF | **WarpTools 2.0.0dev36** `fs_motion_and_ctf` | `agg_*/External/job002/task_*.out` (command + image) |
| Tilt-series import | WarpTools 2.0.0dev36 `ts_import` | `job003/run.out` |
| Tilt-series alignment | **AreTomo2**, through WarpTools `ts_aretomo` | `job004/task_0.out`: `exe = AreTomo2` |
| Tilt-series CTF + defocus hand | WarpTools 2.0.0dev36 `ts_ctf`, `ts_defocus_hand` | `job005/task_*.out` |
| Bad-tilt removal | crboost tilt filter (binary DL classifier) + manual review | `agg_*/TiltFilter/` (2026-05-17); `job006/job.star` reads `TiltFilter/tiltseries_filtered.star` |
| Tomogram reconstruction | WarpTools 2.0.0dev36 `ts_reconstruct` | `job006/task_*.out` |
| Template matching + candidate extraction | **pytom-match-pick 0.10.0** | image `pytom_match_pick_0.10.0.sif` in `job015/job017` logs |
| Subtomogram extraction, averaging, classification, refinement | **RELION 5.0.1** (commit b16a35) | `relion5.0_tomo.sif`; `relion_refine --version` (refinement playbook §1) |
| Pipeline | crboost_server (this repo), May–July 2026 | `project_params.json` per project |

Container images (all under `/groups/klumpe/software/Setup/cryoboost_containers/`):
`warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif`, `pytom_match_pick_0.10.0.sif`, `relion5.0_tomo.sif`.

**AreTomo2's version comes from the image name only.** Before quoting "1.0.0", confirm it from inside the image
(e.g. `apptainer exec <warp sif> AreTomo2 --version`, or the banner at the top of any AreTomo2 log in
`job004/warp_tiltseries/`).

## 2. Per-dataset processing (identical parameters in every source project)

Data: 300 kV, Cs 2.7 mm, 1.55 Å/px, EER, Tomography 5 (TITAN52339720; crboost labels it SerialEM), 4096 × 4096, tilt axis 84.4°, typically
41 tilts. Dose per tilt differs by session: 1.9 (20260122), 2.9–3.0 (Slot6, FCIsolation, 20251113),
4.8 e⁻/Å² (MyrGFP_Grid2).

| Step | Parameters (from `project_params.json` and the logged commands) |
| --- | --- |
| EER rendering | `--eer_ngroups -32`: 32 EER fractions per frame → 4–10 frames per tilt, **≈0.4–0.5 e⁻/Å² per frame** (Warp XML `Dimensions` z: 6, 7, 7, 4, 10) |
| Motion | grid 1×1×3, 500–10 Å, B −500 Å² |
| Per-tilt CTF | 30–6 Å, defocus 1.1–8 µm, grid 2×2×1, 512 px window, no phase plate |
| ts_import | mdoc exposure, axis override 84.4°, `--min_intensity 0`, `--dont_invert`; tomogram 4096 × 4096 × 2048 px at 1.55 Å |
| Alignment | AreTomo2 at 6.2 Å/px, `--alignz 1800` Å (180 nm), global only (no patches) |
| Tilt-series CTF | 30–6 Å, defocus 1.1–8 µm, 512 px window; defocus hand `set_flip`; `rlnTomoHand = −1` |
| Tilt filter | DL binary classifier on per-tilt averages (May run: default model, 384 px, p threshold 0.1), reviewed by hand; applied **after** alignment and CTF, **before** reconstruction |
| Reconstruction | 6.2 Å/px (bin 4) → 1024 × 1024 × 512 voxels; frame half-maps; `--deconv` on; dose-weighted; `--dont_invert` |
| Template matching | Template = an earlier I1 average of the particle (a K=1 RELION Class3D map from the PI's earlier processing), rescaled to 6.2 Å/px, box 128, contrast matched to the tomograms; spherical mask Ø 575 Å, soft edge 5 px; **I1, 12° sampling (3,398 rotations)**; per-tilt CTF and dose weighting; tomogram split 2×2×1; no whitening, no random-phase correction |
| Candidate extraction | up to 20 peaks per tomogram, LCC ≥ 0.05 (0.04 for 20260122), `--relion5-compat` |
| Subtomograms | `relion_tomo_subtomo` at bin 1 (1.55 Å/px), box 786 cropped to 600, 2D stacks, float16 |
| Curation | visual review of every candidate in crboost's particle gallery (`curation_reviewed.json`, `particles_filtered.star`, 2026-06-23/24) |

A second chain matched a featureless 550 Å sphere (low-passed 45 Å, C1) in every project. The curated set that
went on came from the I1-map chain: checked for 20251113 (`job017/tmResults/*_job.json` names the map template;
the curated `job019` reads `job017/candidates.star`). Confirm the other sources from the merged star (§4).

## 3. Aggregation and averaging (2026-06-26 → 2026-07-08)

1. **Merge** (crboost aggregation project `412_aggregation`, `MergedSources/merge-20260626-132413/`): 9 sources
   from 8 datasets, 129 tomograms, **112 particles** after an LCC ≥ 0.1 cut; one optics group at 1.55 Å/px, box
   600, bin 1. Metadata check over all tomograms: hand −1, 1.55 Å/px, 300 kV, Cs 2.7 everywhere.
2. **Initial average** — `relion_tomo_reconstruct_particle`: bin 2 (3.1 Å/px), box 384, crop 224, I1.
3. **3D classification** — RELION Class3D: K = 3, I1, particle diameter 550 Å, initial low-pass 45 Å, healpix
   order 3 (7.5°), offset range 5 / step 2 px, 15 iterations, T = 1, reference = step 2. **Class 1 = 94
   particles (84 %)**; the other classes (7 and 11) discarded.
4. **Consensus refinement** (outside crboost, RELION 5.0.1 image) — Class3D K = 1 with the PI's outer-shell mask
   (Ø 600 Å, 1.8°), then gold-standard auto-refine: `relion_refine_mpi` (3 ranks), I1, same mask, particle
   diameter 600 Å, healpix order 4 / local 5, `--pad 1` (`sta_refine/run_refine3d_pimask_v2.sh`, map
   `Refine3D_pimask_v2/run_class001.mrc`).
5. **Resolution.** The gold-standard FSC stays ≥ 0.92 to 35.8 Å and drops to ~0.27 at 34.4 Å, in four
   independent refinements (two masks, two parameter sets) and in the PI's own earlier run (34.7 Å). RELION's
   reported "final resolution" values (23.85, 14.3, 12.4 Å) are single noise shells past that cliff after mask
   correction. **Quote ~35 Å.**
6. Pentamer (C5) local refinement was tried afterwards and is not a result (refinement playbook §12.16–12.17).

Full commands: `412-capsid-aggregation-handoff.md` (1–3), `412-sta-refinement-playbook.md` §12 and
`412_aggregation/sta_refine/summary.md` (4–6).

## 4. Open before submission

- **The aggregation project is no longer at `/groups/klumpe/crboost_data/412_aggregation`.** Its merged
  `particles.star` is the only record of the per-source particle counts, which candidate chain each source
  used, and the LCC cut. Four of its source projects (among them FCIso_unmilled2 and SK_20251017) are not
  under `crboost_data` anymore either; only the five May `agg_*` projects are.
- AreTomo2 version (§1).
- Particle cleaning: the record shows gallery curation in crboost. If ArtiaX was also used (for cleaning, or
  for mapping the final poses back), that happened outside this record.
- The PI's original template map (`try1/Class3D/job121/run_it015_class001.mrc`) is what was matched with. The
  paper should say where it came from (its own particle set, RELION 4 + CryoBoost v1).
- Candidate extraction passed half the particle diameter to PyTOM (285 Å for a 575 Å particle), so the peak
  exclusion was smaller than intended (a bug, fixed 2026-10-05). It only matters if the paper states an
  exclusion distance.

## 5. Where the draft disagrees with the record

| Draft says | What ran |
| --- | --- |
| MotionCor2 1.4.7 | Warp `fs_motion_and_ctf` |
| CTFFIND4 4.14 | Warp `fs_motion_and_ctf` (per tilt) + `ts_ctf` (per tilt series) |
| Warp 1.0.9 | WarpTools 2.0.0dev36 |
| AreTomo 1.0.0 | AreTomo2 (1.0.0 per the image name, unconfirmed) |
| frames at 0.2 e⁻/Å² | 32 EER fractions per frame, ≈0.4–0.5 e⁻/Å² |
| sphere template 300 or 500 Å | I1 subtomogram average, Ø 575 Å mask; a 550 Å sphere chain ran alongside but did not feed the curated set (checked for one source) |
| cleaned in ArtiaX | gallery curation in crboost, then Class3D |

## 6. The draft paragraph, corrected in place (DAV = `try1` job121, 23 particles; copia = §7)

Sentence structure kept; only what the record contradicts is changed. Brackets = not in the record.

**Subtomogram averaging**

Subtomogram averaging was performed using an in-house developed pipeline
(https://github.com/Klumpe-lab/crboost_server); it runs WarpTools, AreTomo2, pytom-match-pick and RELION 5.0
as one workflow. In brief, EER files were rendered as frames of 32 EER fractions, corresponding to
~0.46 e⁻/Å² dose per frame. Motion correction, per-tilt and per-tilt-series CTF estimation, dose weighting
and tomogram reconstruction at bin4 were performed in WarpTools (version 2.0.0dev36 for the ~50 nm capsid and
2.0.0dev29 for copia) [80], with tilt series aligned in AreTomo2 version 1.0.0 [81], resulting in a tomogram
voxel size of 6.2 Å. Bad tilts were removed using a U-Net-based classifier with manual vetting. Particle
picking was performed using a sphere of 550 Å in diameter as template in pytom-match-pick version 0.10.0
[ref]. Subtomograms were extracted and averaged in RELION 5.0. For the unidentified ~50 nm capsid, particle
positions extracted from template matching were manually cleaned in ArtiaX, leaving 23 particles from 21
tomograms. For copia, 124 template-matching candidates from one tomogram were reduced to 120 by visual
inspection after CTF refinement and to 76 by 3D classification. Final particle positions were mapped back
onto the tomogram using ArtiaX.

References: [78] (MotionCor2) and [79] (CTFFIND4) drop out. [80] was the Warp 1 paper; check whether WarpTools
2 should cite something else.

## 7. Sources chosen for the paper (2026-10-22) — supersedes the 94-particle set above

- **~50 nm capsid:** the PI's v1 project, `…/Processing/412/try1/Class3D/job121/run_it015_class001.mrc`.
- **Copia:** `/groups/klumpe/crboost_data/copia-tutorial-20261015-1551` (protocol `copia-tutorial` v1).
- The ~30 nm capsid is left out.

| | ~50 nm capsid (try1, CryoBoost v1) | Copia (crboost_server) |
| --- | --- | --- |
| Data | 39 tilt series, 1.55 Å/px, 3.0 e⁻/Å² per tilt (Import), Tomography 5 mdocs (imported in SerialEM mode) | 1 tilt series (28 tilts), 2.95 Å/px, 2.07 e⁻/Å² per tilt, Tomo5 |
| EER frames | 32 fractions per frame, 7 frames per tilt → ~0.43 e⁻/Å² per frame | 32 fractions per frame, 23 frames per tilt → ~0.09 e⁻/Å² per frame |
| Motion + CTF | WarpTools `fs_motion_and_ctf`, `ts_ctf` (set_flip); Warp from the cluster EM module, conda build of 2025-06-26, version string not logged | WarpTools 2.0.0dev41 (`warp_2.0.0dev41_aretomo1.0.0_cuda12.9.sif`); CTF band 30–10 Å |
| Bad tilts | no tilt-filter job in the project | no tilt-filter stage |
| Alignment | AreTomo2 via WarpTools, 6.2 Å/px, AlignZ 2000 Å, tilt-axis refinement 3 × 5 | AreTomo2 via WarpTools, 11.8 Å/px, AlignZ 1800 Å |
| Reconstruction | WarpTools, 6.2 Å/px (bin 4), 1024 × 1024 × 512 | WarpTools, 11.8 Å/px (bin 4) |
| Denoising | cryoCARE (Denoise job006/007), not used for template matching | none |
| Template matching | pytom-match-pick 0.10.0 (`PyTom_tm.sif`), 550 Å sphere, non-spherical mask, C1, 90° (48 rotations), CTF + dose weighting, split 4:4:2 | pytom-match-pick 0.10.0, v1 "basic shape 550" sphere (11.8 Å/px, box 96, low-pass 45 Å), C1, 90° |
| Candidates | FP = 1, ≤ 10 per tomogram, diameter 550 Å (External job009/010) | FP = 1, ≤ 1,500, diameter 550 Å → 1,028 |
| Cleaning | manual: 24 picks in 21 tomograms (`ManualPicking/*_6.20Apx.star`, naming matches an ArtiaX export; not logged) → adjusted → 28 extracted (Extract job117) → **23** (job120) | none; 1,016 subtomograms all go into the average |
| Subtomograms | RELION 5.0.0, box 768 / crop 448, bin 1, 1.55 Å/px | RELION 5.0.1, box 384 / crop 224, bin 1, 2.95 Å/px |
| Average | Class3D K = 1, I1, Ø 550 Å, 7.5°, 15 iterations, ref = Reconstruct job118; **23 particles in 21 tomograms**; single-map estimate 19.3 Å | Class3D K = 1, I1, Ø 575 Å, 7.5°, T = 4, 15 iterations; **1,016 particles**; single-map estimate 8.69 Å |
| RELION | 5.0.0 (module `relion/ja241203_5.0-140fb8f`) | 5.0.1 (`relion5.0_tomo.sif`) |

Both resolutions are Class3D single-map estimates, not gold-standard FSC; on the 412 such an estimate read
16.3 Å for a map whose gold-standard FSC cliff sits at ~35 Å.

## 8. All averages in the PI's `Processing/412/` (surveyed 2026-10-22)

Only two projects there contain subtomogram averages (the rest are crboost preprocessing runs, a segmentation
test and an actin test):

| | `try1` | `second_particle/old_version` |
| --- | --- | --- |
| Particle | ~50 nm (550 Å sphere template, Ø 550 Å) | ~33–41 nm (400 Å sphere template, Ø 410 → 330 Å) |
| Data | 39 tomograms, SK_20251017_412 session | 1 tomogram (Position_32_3, 20260205 FCIso_unmilled2 session) |
| Particles | 180 candidates → 30–32 after classification → 28–29 hand-cleaned → **23** final | 58 template-matching picks → **210** after manual picking (`Manual/`) |
| Best map | Class3D job121 (single-map 19.3 Å); Refine3D job135 gold-standard 34.7 Å, final combine never written | Refine3D job030, I, gold-standard **19.84 Å**; PostProcess job031 19.84 Å (no sharpening) |
| Pipeline | CryoBoost v1, RELION 5.0.0 | CryoBoost v1, RELION 5.0.0 |

No set anywhere in the folder has ~50 particles; the nearest is the second particle's first 58 picks.

## 9. Correction (2026-10-22): copia = the PI's `Processing/Copia/try2`

`copia-tutorial-20261015-1551` (§7) is the EMPIAR tutorial set at 2.95 Å/px and is not the paper's copia.
The paper's copia is the PI's `Copia/try2`:

- one tomogram, Position_11_2, same session (20251017, Tomography 5.23 on TITAN52339720 per the mdoc header) as the ~50 nm capsid's `try1`; 1.55 Å/px,
  39 tilts; template-matching tomograms at 6.2 Å/px
- EER: 225 fractions per tilt movie (counted from the TIFF directories), 32 per frame → 7 frames per tilt
  (Warp XML `Dimensions` z = 7, `EERGroupFrames` = 32); mdoc 3.2 e⁻/Å² per tilt → ~0.46 e⁻/Å² per frame,
  ~0.014 per fraction. Same numbers for `try1`. The PI's imports used 3.0 (`try1`) and **4.8** (`try2`)
  e⁻/Å² per tilt for this one session, and the 4.8 went into Warp's dose weighting for copia.
- template: a solid 550 Å sphere (`template_box128.0_apix6.2_black.mrc`, measured edge-to-edge ~546 Å),
  C1, 90°, 48 rotations, pytom-match-pick 0.10.0
- particles (final lineage, traced from Refine3D job054): 124 candidates (FP = 1, Ø 550 Å) extracted (job008) → Class3D K = 1 (job011) → Refine3D job037 → CtfRefine job042 → re-extract job043 (124) → visual selection Select job046 (**120**) → Refine3D job047 → Class3D K = 3 job051 → Select job053 (**76**) → Refine3D job054. The ArtiaX-cleaned `ManuallyCleaned*.star` sets (105–106, 2025-10-21) are a side branch, not in this lineage. Old note:
  2025-10-21) → **76** in the final Refine3D (job054, via Select job053), all from Position_11_2
- RELION 5.0.0; Warp version not in the logs (the user gives 2.0.0dev29)
