---
name: gallery-calibration-anchors
description: "Subtomo gallery should anchor the eye with template + noise reference tiles, and use tomogram-wide percentile normalization (not per-tile) so noise stays uniformly grey"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 91109579-fa98-4d7e-a355-24190703650a
---

When users evaluate low-score (~0.05 LCCmax) ribosome-class picks in the
subtomo cutout gallery (`ui/tomo_dashboard_dialog.py:_render_gallery_body`),
visual ambiguity is the #1 friction: "is this noise or my particle that I
just can't see at 96 px?". Three rules that landed 2026-05-18 to fix that:

1. **Tomogram-wide percentile normalization, not per-tile.** Per-tile 1-99
   percentile clip stretches pure-noise tiles to fill 0-255 just like
   particle tiles, making them visually indistinguishable. Pool pixels
   from a stratified sample of picks (across the whole score range) once,
   compute one (lo, hi) clip, apply to every tile. Noise stays uniformly
   grey, particle density pops. See `preview_render._sample_global_norm_bounds`.

2. **Template reference tile at the top of the gallery.** Central X/Y
   slice of the species template, same percentile pipeline, distinct
   green border. Calibrates "what should a particle look like here?"
   before the user evaluates ambiguous tiles. Template normalization is
   self-contained (own percentile) — template/tomo intensity regimes are
   different, so don't try to share bounds.

3. **Lowest-score picks as noise reference strip.** Same calibration row,
   distinct orange border, 4 tiles from the bottom of the score-sorted
   pick list. These are picks that survived the cutoff but are at the
   noise floor — comparing a medium-score tile against them tells the
   user immediately whether their pick is in the noise visual class.

**Why:** Without these anchors, the gallery shows tiles in isolation and
the user can't tell whether a faint blob at score 0.05 is signal or
stretched noise — so the panel's diagnostic value collapses for the
exact score band where the user most needs help (the ambiguous middle).

**How to apply:** When adding any "compare your X against ground truth"
UI, lead with both extremes as fixed anchors, not just the data in
question. Same logic generalizes beyond this gallery — score
distributions, FOM plots, classification galleries all benefit from
template-anchor + noise-anchor framing.

Related: [[project_412_picking_quantification]] (the ~16 picks/TS auto vs
1-3 manual confirms most low-score picks ARE noise — so the "noise
floor" reference tiles are dense with actual examples).
Related: [[feedback_visualization_patterns]] (manifest version as cache
key — bumped to v11 for tomogram-wide normalization + template block).
