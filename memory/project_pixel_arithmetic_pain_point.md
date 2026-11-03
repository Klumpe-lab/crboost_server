---
name: Pixel/binning arithmetic across pipeline is the user's #1 source of bad picks
description: User over/underestimates template box size + extraction padding because tracking binning math through each stage is error-prone; dashboard should make this hard to get wrong
type: project
originSessionId: 90bd9c4a-9d17-41fe-875c-80baba426451
---
The user's most common pipeline failure mode is mis-sized template boxes
or wrong extraction paddings — sometimes from genuine uncertainty about
particle dimensions, but **often just from arithmetic mistakes**
propagating pixel sizes through the FS-CTF → Filter → Align → CTF →
Recon → Template Match → Candidate Extract → Subtomo Extract chain. When
binning is wrong, picks are silently bad.

**Why:** Each stage potentially rescales (`rescale_angpixs` at Align /
Recon), and template/extract params are declared in mixed units (box in
px, NMS distance in Å, particle diameter in Å). Tracking what's in what
unit at what pixel size in your head is error-prone, especially across
multiple species with different box sizes.

**How to apply:** When designing dashboard analytics, consider whether the
panel helps the user verify their binning arithmetic. Specifically, the
Pixel / binning sanity panel (ROADMAP §11) is the highest-leverage thing
we can build — it consolidates pixel size + tomo dim + box/pad params
into one table with sanity-check warnings, making binning mistakes loud
and obvious. Prioritize this over deeper per-stage analytics until it's
shipped.

**Status (2026-05-08):** Pixel / binning sanity panel shipped in
`ui/tomo_dashboard_dialog.py` (`_compute_pixel_chain`, `_apply_sanity_rules`,
`_render_pixel_sanity_table`) inside the Dataset section. Sanity rules in
place: box vs particle Ø (1.5–3×), particle Ø consistency across pick
instances, template px ≠ recon px, subtomo crop > box.

**Field-source gotchas to remember when extending sanity rules:**
- `TemplateMatchPytomParams` has NO `box_px` / `padding` / `angle_step`
  fields. PyTOM box is the template volume itself; we read box+px from
  `ParticleSpecies.workbench.box_size` (px) and `pixel_size` (Å). The
  workbench pixel_size is the most useful sanity probe (must match
  recon's `rescale_angpixs`, else picks are garbage).
- `CandidateExtractPytomParams` has NO `nms_distance_ang`. Available:
  `particle_diameter_ang`, `cutoff_method`, `cutoff_value`,
  `max_num_particles`, `apix_score_map`. NMS isn't user-tunable.
- `SubtomoExtractionParams` uses `box_size` + `crop_size` (no `padding`
  field). Padding rim per-side derives as `(box-crop)/2`. `crop_size=-1`
  means no crop.
- `TsAlignmentParams.tomo_dimensions` is a string `"4096x4096x2048"` at
  NATIVE pixel size; downstream stages must rescale by
  `native_px / target_px`.
