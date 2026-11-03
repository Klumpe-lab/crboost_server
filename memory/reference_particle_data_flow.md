---
name: reference-particle-data-flow
description: "docs/particle-data-flow.md — the reference figure for TM vs Pick candidates vs Subtomo extraction, what each STAR holds, and what must agree before picks may be united across tomograms/projects"
metadata:
  type: reference
---

`docs/particle-data-flow.md` (written 2026-08-22) is the answer to "what is the difference between
template matching, pick candidates and subtomo extraction, and which file carries which fact". Read
it before touching any merge/aggregation code; `docs/roadmaps/completed/picking_ui/roadmap_12-aggregate-candidates.md`
depends on every fact in it.

The five things that are counter-intuitive and keep getting re-derived:

1. **Template matching produces NO picks.** Its output is three VOLUMES per tomogram
   (`{tomo}_scores.mrc`, `{tomo}_angles.mrc`, `{tomo}_job.json`). ② Pick candidates is the
   peak-finder over those volumes — which is why ② can never consume a coordinate list.
2. **`optimisation_set.star` is a two-line envelope**, not data: `rlnTomoParticlesFile` +
   `rlnTomoTomogramsFile`. Both ② and ③ emit one. Same envelope, two payload grades —
   COORDINATE grade (no optics, no `rlnImageName`) vs PIXEL grade. The grade decides what a merged
   set can be handed to. `subtomo_merge._read_input_particles_lenient` already accepts coordinate
   grade ("Only `rlnTomoName` is required"), so ③ needs no change to eat a coordinate merge.
3. **Subtomo extraction goes back to the TILT SERIES, not the tomogram** — projects each 3D position
   into every tilt, crops a 2D box, dose+CTF weights, stacks. The "pseudo-subtomogram" is not a
   cutout of the reconstruction.
4. **`rlnCenteredCoordinate*Angst` is binning-INDEPENDENT** (Å from the tomogram centre), so
   coordinates are more portable across projects than expected. What breaks is identity:
   `rlnTomoName` is a local label. Two keys are needed — absolute `rlnTomoReconstructedTomogram` to
   DISTINGUISH, mdoc `SubFramePath`+`DateTime` (stored as `TiltSeries.frames[0].raw_filename` /
   `.acquisition_time`) to EQUATE. Same acquisition still ≠ transferable coordinates: handedness,
   pixel size, binning and `rlnTomoSizeX/Y/Z` must also match or the centre is a different physical
   point.
5. **`0,0,0` orientations are RELION's real "no prior"** and are correct to write. `rlnLCCmax = 0`
   is NOT — 0 is a legal (bad) LCC, so a ragged score column must leave the RELION-facing star and
   live in a sidecar, with coverage stated in the UI.

Related: [[project_picking_ui_wave2]], [[project_pixel_arithmetic_pain_point]],
[[project_412_divergence_cascade]], [[feedback_never_silent_defaults]].
