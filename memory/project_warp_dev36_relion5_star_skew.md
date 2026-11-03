---
name: project_warp_dev36_relion5_star_skew
description: WarpTools dev36 ts_export_particles legacy tomo-star → relion5.0 reconstruct = silent all-zero; self-contained converter written
metadata: 
  node_type: memory
  type: project
  originSessionId: 2eb68af2-d8b9-4b2b-a580-b114e88addc4
---

**Symptom:** `relion_tomo_reconstruct_particle` (container `relion5.0_tomo.sif`) exits SUCCESS but writes an all-zero map (data_merged AND weight_merged all 0 → zero Fourier slices inserted) when fed the output of `WarpTools ts_export_particles --2d` from `warp_2.0.0dev36`. Tell-tale in run.err: `particles_tomograms.star does not have rlnTomoTiltSeriesStarFile labels ... old format ... will try to convert` + `relion-4 definition of projection matrices; converting them now`. A working reconstruct from a native optimisation set has an EMPTY run.err.

**Root cause:** version skew. dev36 writes tilt geometry as 4×4 projection matrices embedded in per-tomogram `data_<name>` blocks (no `rlnTomoTiltSeriesStarFile`); relion5.0's back-conversion of that legacy embedded layout silently yields zero for the 2D-stack path. NOT the dir structure, coords (easymode starfile coords are fine — Warp read them, extracted real 80×80×N 2D stacks), box/bin, or a missing optset (Warp writes `particles_optimisation_set.star`). Ruled out box/bin/abspath (11 zero reconstructions across relion/ + relion2/, all variants). crboost never hits this: it extracts with `relion_tomo_subtomo` fed a NATIVE tomograms.star it builds itself (per-TS star files, `rlnTomoTiltSeriesStarFile`, `rlnCenteredCoordinate*Angst`).

**Geometry recovery (verified against the colleague's data):** Warp's exported matrix = `A = Rz(axis)·Ry(tilt)`. Invert to RELION-native params:
- `YTilt = atan2(-ProjZ_x, ProjZ_z)` ; `ZRot = atan2(-ProjX_y, ProjY_y)` ; `XTilt = 0` ; shifts = 0 (Warp matrices carry no translation; 2D stacks pre-extracted centered → must NOT re-apply shifts).
This equals crboost's `metadata_service.py:622` convention (`XTilt=0 / YTilt=−tilt / ZRot=rot`, shift×apix) which reconstructs correctly — so it's guaranteed faithful. crboost sources those from an AreTomo `.aln`; the colleague has no `.aln` (alignment is in Warp per-TS `.xml`), but the matrices in `particles_tomograms.star` are sufficient.

**Deliverable:** self-contained converter (starfile+pandas, no crboost/aln/xml) rewrites the 3 Warp files → native `# version 50001` (per-TS `tilt_series/<TS>.star` + `tomograms.star` + renamed CenteredCoordinateAngst particles + optimisation_set; absolute image paths). At `.../easymode_412_FCIso_unmilled2/warp2relion5.py` (ruff-clean; NOT executed — cluster python path gone from sandbox). Args: `--particles --tomograms --tomostar-dir --outdir`.

**RUNTIME CONFIRMED (2026-07-07):** converter runs, geometry recovered correctly (YTilt[58..-58]/ZRot~84.4 all 8 TS). First reconstruct then hit the predicted LOUD error (NOT silent zero — the skew IS fixed): `tomogram_set.cpp:236 ERROR: tomogramTable ... does not contain a rlnMicrographName label, yet globalTable also does not contain rlnTomoTiltSeriesName`. Fix landed: converter now takes `--tomostar-dir` and adds per-tilt `rlnMicrographName` = motion-corrected average `.mrc` (`<warp_frameseries>/average/<movie_stem>.mrc`, movie stem from tomostar `_wrpMovieName`, matched to geometry rows by tilt angle = −YTilt) + rlnTomoNominalStageTiltAngle/TiltAxisAngle. Path math verified to resolve to a real file. Reconstruct via `reconstruct_test.sh` (singularity exec --nv relion5.0_tomo.sif). Validate: weight_merged.mrc ≠ 0. If RELION demands yet another per-TS col, same pattern (fails loud, add it). Context: a colleague's actin easymode project, wants NO crboost. Related: [[project_412_sta_refinement_playbook]] [[project_warp_relion_star_placeholders]] [[reference_412_dataset_locations]].

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

ts_export_particles legacy embedded-matrix tomo-star → relion_tomo_reconstruct_particle SILENT all-zero (weight_merged=0); geometry recoverable exactly: A=Rz(axis)Ry(tilt); YTilt=atan2(-ProjZ_x,ProjZ_z), ZRot=atan2(-ProjX_y,ProjY_y); self-contained converter written for the colleague's actin easymode project (no crboost).

**CORRECTION (2026-10-10, from the RELION 5.0.1 + Warp sources):** the all-zero map was not a star-format
conversion failure. `relion_tomo_reconstruct_particle` never reads particle stacks; it re-extracts from the tilt
series in `rlnTomoTiltSeriesName`, which Warp sets to a 2x2x2 dummy → every tilt invisible → nothing backprojected.
The converter above (zero shifts + real averages in rlnMicrographName) would make it extract at wrong positions.
Only `relion_refine --ios` uses Warp particle series. See [[project_warp_export_job]].
