# Journey Panel — Candidate Metrics

Running list of metrics worth promoting to the Journey panel as first-class artifacts. Populated during the 2026-05-16 comparison of `/groups/klumpe/crboost_data/GT` (crboost_server, 412 dataset, refactored pipeline) against `/groups/klumpe/user/sven.klumpe/Processing/412/try1` (cryoboost v1, same dataset, ground-truth reference).

Add entries as we walk pipeline stages and notice things that (a) would have caught a real problem early and (b) generalize beyond this dataset.

---

## Stage 0 — Import / project setup

### TomoHand flag (HIGH PRIORITY)
- **What:** Display `_rlnTomoHand` from `Import/job001/tilt_series.star` (or the `flip_tiltseries_hand` flag from the Import driver) prominently on the Import row of the Journey.
- **Why it matters:** A wrong handedness at Import flips tomogram chirality and silently produces mirror-image picks for chiral templates — invisible from any downstream metric until the user inspects picks. Discovered 2026-05-16: crboost_server defaults `flip_tiltseries_hand=No` (TomoHand=+1) while cryoboost v1 defaults to `Yes` (-1) for the same Klumpe-lab Titan setup.
- **Source:** `Import/job001/tilt_series.star`, column `_rlnTomoHand`.
- **Suggested UI:** Coloured chip on the Import card; red if non-default for the configured microscope.

### Dose-rate mdoc consistency
- **What:** Show `dose_rate` from Import job.star next to mdoc `ExposureDose` (mean over tilts). Flag if they disagree.
- **Why:** Discovered 2026-05-16 that the cryoboost v1 GT wrote `dose_rate=3.0` while the mdoc clearly says `ExposureDose=3.20`. Dose-weighting downstream uses the job value; mdoc is the source of truth.
- **Source:** `Import/job001/job.star` `dose_rate` field; `mdoc/*.mdoc` `ExposureDose` lines.

---

## Stage 1 — fsMotionAndCtf

### Per-tilt CTF resolution (heatmap or scatter)
- **What:** Heatmap over (TS × tilt index) of `CTFResolutionEstimate` pulled from WarpTools per-frame XMLs.
- **Why:** Per `project_warp_relion_star_placeholders` memory, the exported star has placeholders (1e-6 / None); real values live in `External/jobNNN/warp_frameseries/*.xml` `<Movie CTFResolutionEstimate="…">`. Without this view, the user has no way to spot a TS where high-tilt frames CTF-failed.
- **Source:** XML `Movie.CTFResolutionEstimate` attribute, per frame.
- **Suggested UI:** Heatmap rows = TS, cols = tilt index; cell colour = res estimate; hover = TS+tilt+exact value.

### Per-tilt mean frame motion (same heatmap)
- **What:** Companion to CTF res — `Movie.MeanFrameMovement` attribute.
- **Why:** Tilt frames with large motion are usually unusable; this is the cleanest "did the motion correction land?" sanity check.
- **Source:** XML `Movie.MeanFrameMovement` attribute.

### Unselected-frame count per TS
- **What:** Count of frames `UnselectFilter="True"` or `UnselectManual="True"` per TS, surfaced as a chip on the TS row.
- **Why:** the GT `run.err` showed WarpTools `System.OverflowException` failures silently marking high-tilt frames as unselected. The user has no UI signal that this happened. A small chip ("3 tilts dropped") on each TS would have surfaced this.
- **Source:** XML `Movie.UnselectFilter` / `UnselectManual` attributes; also lines in `run.err` matching `marked as unselected`.

### Defocus-vs-tilt smoothness check
- **What:** Per TS, plot defocus across the tilt series (sorted by tilt angle). Smooth ~cosine curve = good CTF fits; spiky outliers = bad fit on specific tilts.
- **Why:** Quick visual "did CTF estimation converge well across all tilts" — without it the user has to read the XMLs by hand.
- **Source:** XML `Movie.CTF.Defocus` (Param Name=`Defocus` Value=…) per frame, plus `Movie.MeanFrameMovement` for x-axis (or `_rlnTomoNominalStageTiltAngle` from per-TS star).

---

## Stage 3 — tsCtf

### Defocus-handedness flag chip
- **What:** Show whether the tsCtf job applied a defocus-handedness flip (cryoboost v1 calls this `defocusHand = set_flip`). Pin to the tsCtf row.
- **Why:** Discovered 2026-05-16 — crboost_server doesn't apply this flip by default (`invert_defocus_hand: false` in `project_params.json`), but cryoboost v1 does. Combined with the Import TomoHand miss, this is a *second* handedness divergence stacking on top. Both invisible from any per-tilt metric.
- **Source:** `project_params.json` `acquisition.invert_defocus_hand`; cryoboost v1 reference is `param4: defocusHand = set_flip` in `External/jobNNN/job.star` for the tsCtf job.

---

## Stage 4 — tsReconstruct

### Tomogram polarity chip per TS
- **What:** From each reconstructed tomogram, sample a center Z slab (e.g., a single 1024×1024 slice from the middle of Z) and compute the percentage of voxels above mean+1.5σ (BRIGHT outliers) vs below mean−1.5σ (DARK outliers). Pin a chip on the tsReconstruct row: "particles appear DARK / BRIGHT / symmetric". Tooltip shows the actual percentages.
- **Why:** The template/mask polarity (BLACK vs WHITE) must match the tomogram polarity. If the WarpTools `TomoFullReconstructInvert` setting changes between runs (or between projects), the chip surfaces the mismatch before TM gives meaningless CC scores. GT and user's reconstructions here both came out essentially symmetric (~6.6% bright, ~6.8% dark — slightly dark-skewed) → matches the BLACK template choice. A wildly asymmetric distribution (e.g., 15% bright vs 3% dark) would flag a polarity flip.
- **Source:** Stream a center Z slice from `External/jobNNN/warp_tiltseries/reconstruction/<TS>_<apix>Apx.mrc` (note: mode=12 = float16, half-precision; needs IEEE 754 half conversion if no mrcfile). Compute %above/%below thresholds, classify polarity.

### Reconstructed tomogram dim + apix consistency check
- **What:** Verify `tomo_shape` from the actual `.mrc` header matches what `project_params.json acquisition.detector_dimensions` + `rescale_angpixs` should produce. Surface red chip if mismatch.
- **Why:** Sanity check that the reconstruction binning matched the configured binning. A silent mismatch (e.g., reconstruction at apix 5 when user thought 6.2) would propagate to TM as a template/tomogram apix mismatch.

---

## Stage 5 — templatematching

### Declared-vs-applied symmetry parity check
- **What:** Compare `project_params.json` `jobs.templatematching.symmetry` against the actual `rotational_symmetry` in the per-TS PyTOM `*_job.json` outputs. Surface a red chip if they differ.
- **Why:** Discovered 2026-05-16 — `project_params.json` declares `symmetry: "I1"` but every `tmResults/*_job.json` reports `rotational_symmetry: 1` (C1). Either the TM driver silently drops symmetry or maps I1 → 1 incorrectly. Without this check the user is unaware that the declared symmetry isn't being applied.
- **Source:** `project_params.json` `jobs.<instance>.symmetry`; `External/job00X/tmResults/<TS>_job.json` `rotational_symmetry` field.

### Template + mask intrinsic-shape chip (per species)
- **What:** Resolve and display: (template box size, mask box size, mask_is_spherical, mask **measured** diameter at 0.5 contour, mask **isotropy ratio** = min(σx,σy,σz)/max(σx,σy,σz) on per-axis std deviations, mask COM offset from box center, template apix vs tomogram apix, template dynamic range as a "DETAIL vs SHAPE" indicator). One compact chip cluster on the templatematching row.
- **Why:** The big strategic difference between projects is template shape — detail (Class3D map, std spread > 5) vs featureless (ellipsoid, std spread < 2). User had no easy way to see this without opening the actual MRC files. Also catches: a "spherical" mask that's actually elongated (isotropy < 0.95), a mask whose filename diameter (e.g. "d575") doesn't match the actual 0.5-contour diameter (which is what TM actually uses), or a template that's not centered (COM offset > 0.5 voxel). Discovered 2026-05-16 that user's "_d575" mask actually has a 606 Å 0.5-contour diameter due to soft edge — name is misleading but functionally fine.
- **Source:** `tmResults/<TS>_job.json` `template_shape`, `mask_shape`, `mask_is_spherical`, `voxel_size`; combined with `templates/<species>/*.meta.json` for diameter/softness/origin tags; for the measured-diameter + isotropy + COM offset, compute from the actual mask `.mrc` file with a one-pass stat (threshold mask > 0.5*max, compute first/second moments per axis, equivalent sphere radius from voxel count).

### Cross-correlation score-map per-TS stats
- **What:** From each `*_scores.mrc`, compute (max CC, mean CC, std CC, count of voxels above various thresholds). Display as a per-TS row in the templatematching panel.
- **Why:** When TM goes wrong, the score-map shape goes wrong long before anyone looks at the picks. the GT job.json reports `job_stats: {variance: 6.7e-5, std: 0.008}` — handy numerics PyTOM already emits. Surfacing these would have flagged "your CC std is the same as the GT's but your max-CC is 30% lower" as a TomoHand-cascade signature.
- **Source:** Either compute over `*_scores.mrc` or pull `job_stats` straight from `*_job.json`.

### Tilt-count "X of Y used" chip per TS
- **What:** "X of Y tilts in TM" chip on each TS row. Tooltip: which tilt angles were dropped.
- **Why:** Discovered 2026-05-16 — user's TiltFilter dropped 65.99° and 68.99°; GT kept all 39. Easy to miss; relevant to per-particle CTF and resolution downstream.
- **Source:** Compare aligned-tilt-series count (e.g., from `tilt_series/<TS>.star` row count) vs `tmResults/<TS>_job.json` `tilt_angles` length.

---

## Stage 6 — tmextractcand

### Per-TS pick count + CC distribution mini-histogram
- **What:** For each TS, show pick count + a 5-bucket histogram of LCCmax values (e.g., 0.04-0.05, 0.05-0.06, 0.06-0.07, 0.07-0.08, 0.08+).
- **Why:** Discovered 2026-05-16 — the SHAPE of the CC distribution diagnoses TM health long before pick visual inspection. A healthy TM has clear separation (a small high-CC tail above a noise floor). A broken TM has tight distribution near the cutoff (noise above a permissive threshold). User's 16-picks-per-TS-all-at-0.05-0.07 is the latter signature.
- **Source:** `External/jobNNN/candidates.star` columns `_rlnLCCmax`, `_rlnCutOff`, grouped by `_rlnTomoName`.

### Cutoff-method + value chip on each TS
- **What:** Display the cutoff method (NumberOfFalsePositives, percentile, hard threshold, etc.) and value used for extraction. Pin to the tmextractcand row.
- **Why:** Permissive cutoff is the difference between "16 picks of noise" and "2 picks of signal." If the user inherits permissive defaults during a refactor (as happened here), the Journey panel should make it obvious.
- **Source:** `project_params.json` `jobs.tmextractcand.cutoff_method` + value; cryoboost v1 reference is `param1: cutOffMethod = NumberOfFalsePositives, cutOffValue = 1, maxNumParticles = 10` in `External/jobNNN/job.star`.

---

---

## Stage 7 — subtomoExtraction

### Box and crop sizing rationality chip
- **What:** Show `box / particle_diameter` and `crop / particle_diameter` ratios on the subtomoExtraction row, with green/yellow/red zones.
- **Why:** Box and crop are easy to set "mechanically" (copy from previous experiment) without verifying they fit the current particle. Discovered 2026-05-16 the user had been using box 786 + crop 448 from the Copia VLPs experiment for 412 — box ratio was fine (~2.1×) but crop ratio was only 1.2× (tight: only ~6% margin around 575 Å particle in 694 Å crop). Risks particle clipping during Refine3D shifts.
- **Source:**
  - `box` and `crop` from `project_params.json jobs.subtomoExtraction.*` (field names depend on driver — check `services/jobs/subtomo_extraction.py`)
  - particle diameter from the species' mask metadata (`templates/<species>/*_mask.mrc.meta.json` or measure from the mask `.mrc`)
  - apix from `acquisition.pixel_size_angstrom`
- **Thresholds:**
  - Box: green ≥ 2.0×, yellow 1.5–2.0×, red < 1.5×
  - Crop: green ≥ 1.5×, yellow 1.2–1.5×, red < 1.2×

### Estimated disk footprint chip
- **What:** Estimate `total particles × crop³ × bytes_per_voxel` and show projected disk usage before the job runs.
- **Why:** Big crops on many particles eat disk fast. A 786³ float32 = 1.9 GB per particle; 1000 particles = 1.9 TB. Surface this BEFORE submission, not after the cluster's filesystem fills up.

---

## Cross-cutting

### Comparison-overlay mode
- **What:** When a project has a "baseline" sibling project configured, overlay baseline values on the Journey heatmaps / scatter plots in muted colour.
- **Why:** This whole comparison exercise would be a one-click operation if Journey supported it. Discovered useful during the 412 comparison.
- **Trade-off:** Baseline needs to provide a tomogram-name mapping (filename suffix is unreliable — GT's "Position_10_2_2" maps to user's "GT_Position_10_2" by mdoc DateTime, not by name).

### Source-of-truth = mdoc DateTime + SubFramePath
- **What:** Use mdoc `DateTime` + `SubFramePath` (basename) to canonicalize a tilt-series across projects.
- **Why:** Discovered that tomogram naming diverges across cryoboost variants (the GT pipeline double-prefixes frame names, e.g. `Position_10_2_2Position_10_2_001_…`); mdoc is the only thing that's consistent.
- **Source:** `mdoc/*.mdoc` lines `DateTime = …` + `SubFramePath = …`.

---

### Stale-default detector for refactor-port drift
- **What:** A meta-check that compares the values in the current `project_params.json` against the in-code defaults of each `AbstractJobParams` subclass. Flag any field where the persisted value differs from the current default — this surfaces fields that were "correct at project creation time but the codebase has since moved on" (or vice versa).
- **Why:** This whole 412 saga started with `invert_defocus_hand` persisted as `False` because the project was created when the default was `False`; once the default flipped to `True`, the existing project_params.json didn't auto-update. A detector chip would have made this visible at project-load time. Generalizes to any future default-flip.

### MRC header inspector utility
- **What:** A small inspector that, given a list of MRC paths, returns header (dims, apix, mode, dmin/dmax/dmean) + content stats (mean, std, percentile of "particle-like" outliers) + for masks the equivalent-sphere radius + isotropy. Pure stdlib (`struct` + IEEE 754 half conversion), no numpy needed so it works in any Python env on this cluster.
- **Why:** Hand-rolled this inline 3 times during the 412 investigation (template comparison, mask sphericity, tomogram polarity). Worth a permanent home in `services/visualization/` so the Journey panel can call it without re-rolling.
- **Reference implementation:** `/tmp/mrc_stdlib.py` pattern from the 2026-05-16 session.

---

## Priority order — when picking what to promote to UI first

If time-constrained, the TWO that would have caught the 412 debacle BEFORE picks-look-bad are:

1. **TomoHand chip (Stage 0)** — first row, red if non-default for the configured microscope. Single most cost-effective addition.
2. **CC distribution histogram (Stage 5/6)** — would have shown the noise-band signature without anyone having to count picks manually.

After those, in rough order of "would have helped this investigation" benefit:
3. Declared-vs-applied symmetry parity (Stage 5)
4. Stale-default detector (cross-cutting)
5. Template + mask intrinsic-shape chip (Stage 5)
6. Tomogram polarity chip (Stage 4)
7. Box/crop sizing rationality (Stage 7)

The rest are either "nice to have for routine monitoring" (heatmaps) or "needs more infrastructure" (comparison-overlay mode).

---

## TODO — promote one of these to Journey

(Updated as the stage walk progresses. As of 2026-05-16: nothing promoted yet; awaiting first re-run of 412 to validate the fix before investing in UI work.)
