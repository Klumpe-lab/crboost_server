---
name: project-tomo-gallery-view
description: "Tomogram gallery (4th workspace view, ui/tomo_gallery.py) + data-less-project unblock + Journey icon → bar chart — CODE-COMPLETE 2026-08-24 on denovo_picking, uncommitted, PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: 8076f276-b916-476d-86ad-16514cbe7414
  modified: 2026-08-24T08:49:36.008Z
---

Three asks in one session (2026-08-24), all code-complete, uncommitted, **PENDING RUNTIME**:

**1. Data-less project was unreachable, not unsupported.** `is_dataless()` (both globs empty) has
gated creation since de-novo S1, but `on_movies_change` only DERIVED the co-located mdocs glob when
the frames value was non-empty — clearing frames left the last derived mdocs pattern stranded in
state, and its input is hidden unless "mdocs elsewhere" is on, so the form could never reach
`is_dataless()` and Create stayed disabled on "Missing: Data Path". Fixed by deriving `""` too.
Plus: empty globs now read neutral (`_DATALESS_HINT`) instead of red field errors when BOTH are
empty, and a `house_button("No raw data")` in the raw-data section clears both in one click
(prefs pre-fill the fields on every load, so "just leave them empty" was never actually offered).

**2. `ui/tomo_gallery.py` — the birds-eye wall, a 4th workspace container** ("gallery" mode beside
pipeline / workbench / journey / viewer; nav icon `static/icons/tomo_preview.svg`, which was an
empty 0-byte file left over from the old "Tomogram Previews" grid). Rows come from a new
`tomo_geometry.all_geometries()` (reconstruct star first, then imported — so it works in a
data-less project). Per tile: WarpTools PNG → our cached X/Y slab → background auto-kick (capped
6/render, same cache paths + dedup keys as the Journey so neither renders twice). Toolbar switches
(source = recon | each denoisepredict method with a volume; per-species pick toggles; tile size)
re-render from cached rows — only Refresh and a 15 s pending-only poll re-read disk.

**Dot-overlay mechanic worth reusing:** one inline `<svg>` per (tile, species) with `cx/cy` in
PERCENT and `r` in user units (no viewBox ⇒ CSS px), so dots keep constant screen size at any tile
size and it's ONE element per layer instead of one per pick (a 40-tomo wall is thousands). Requires
frame box == image box: `aspect-ratio` from the tomogram's own dims + `object-fit: fill`. No dims
⇒ NO dots and an amber count badge saying "counted, not placeable" (never a guessed extent).

**3. Journey nav icon** `_TOMO_DASHBOARD_SVG` (pipeline_roster.py): ringed circle → three ascending
bars (statistics/progress). Name stays "Journey".

**Why:** the Journey answers "how did THIS tilt-series go"; nothing answered "how do they all look".
**How to apply:** runtime is owed — sandbox ceiling was ruff only (no python, see
[[reference_hpc_env]]); user must run `python check_boundaries.py` + the app. Verify: empty-project
create, wall on a TM project (dots land on particles, not offset/flipped), denoise toggle, zoom,
caption → Journey lands on that TS. Related: [[project_denovo_picking]],
[[feedback_journey_layout_preferences]], [[feedback_gallery_calibration_anchors]].

**2026-10-16 versions (NOT RUN):** with several reconstruct jobs, the gallery source toolbar lists one entry per job
(`recon · <alignment>`, labels from `recon_compare.recon_version_labels`, project-wide) next to the denoise methods;
the Journey Reconstruct card got a `version` toggle (each job's own WarpTools PNG, or `compare` = the existing
plane-matched side-by-side). Header/polarity chips still describe the first reconstruct job only.
