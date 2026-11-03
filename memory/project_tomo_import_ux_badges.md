---
name: project-tomo-import-ux-badges
description: "2026-08-27 rail count badges (tilt-series on Jobs, tomograms on Tomograms) + single-field tomogram import rework; CODE-COMPLETE, PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: ec75303d-64ff-4db4-a9ad-59d5b618a057
  modified: 2026-08-27T11:30:20.920Z
---

Reworked the tomogram-import path after the user reported that importing MRCs into an
empty project "doesn't seem to work" and gives no confirmation. CODE-COMPLETE 2026-08-27,
**PENDING RUNTIME** (no python in the sandbox — ruff only; `check_boundaries.py` unrun).

**Rail count badges** — `services/project_counts.py` (new) exposes `tomogram_counts()`
(star rows, dedup by `rlnTomoName`, same first-source-wins precedence as
`tomo_geometry.all_geometries`; memoized on the source stars' mtimes so an unchanged
project costs two stats a tick) and `tilt_series_counts()` (registry, falling back to
`ProjectState.import_*_tilt_series`). `RosterWidget._tick_counts` polls every 15 s off the
event loop and `_paint_counts` repaints only when a number moves. Badge = count of
tomograms the stars DESCRIBE; volumes missing from disk are surfaced in the tooltip, never
folded into the total.

**Two real bugs fixed on the way:**
- `RosterWidget.signature()` had no `imported_tomograms` input, so the PARTICLES-header
  green dot never repainted after a commit — the import genuinely looked like a no-op.
- `TomoGalleryPage.show()` is a no-op once `_collected`, so an import while the wall was
  already built left a stale wall. New `invalidate()` + `callbacks["invalidate_gallery"]`.

**Import dialog** (`ui/tomogram_import_dialog.py`, rewritten): the mrcs/reference TABS are
gone. One path field + Browse…, and the mode is READ OFF what the path is (folder of
`.mrc`/`.rec` → synthesize, one volume → synthesize, `.star` → reference). Resolves on
Enter/blur/Browse and lazily at Import time. `local_file_picker` gained `mode="any"`
(returns a folder or a file; OK with nothing highlighted = the current folder) — that OK
behaviour is what used to make "pick a directory" look broken.

Chrome pass: no `cb-section-card` boxes, no tabs, `PAGE_SECTION_STYLE` titles separated by
whitespace. Same pass on the project hub's parameter pane
(`RosterWidget._render_overview_section`): 68 px label column instead of `space-between`
across 380 px, hairlines and the banded section header dropped, and the Dataset section no
longer gated on a frame import so a particle-only project shows its Tomograms row.

Related: [[project_tomo_gallery_view]], [[feedback_ui_chrome_conventions]],
[[feedback_reactive_ui_patterns]], [[feedback_never_silent_defaults]],
[[reference_hpc_env]]
