---
name: project-cutout-lowpass-filters
description: "Switchable display filters (denoise/CLAHE/bandpass/lowpass) for Journey pick cutouts; no re-extraction, apix from MRC header, manifest v14, compact TYPE+PARAM UI"
metadata: 
  node_type: memory
  type: project
  originSessionId: b828f5f3-259e-4212-850c-38c13da2b136
---

Journey-tab pick cutouts are noisy on non-denoised tomos. Added switchable DISPLAY filters (landed 2026-06-23, code-clean, PENDING runtime verify). Filters NEVER touch data/picks/star — they only re-render the cutout PNG. No SLURM re-extraction (the .mrcs/recon already exist; PNG gen was always decoupled).

**⚠️ REFINED later same session (2026-06-23):** pure lowpass just blurs — user called it "garbage." Expanded to MULTIPLE filter TYPES (the real wins are edge-preserving denoise + local contrast), kind-dispatched, `FILTER_SCHEMA_VERSION=2`, manifest bumped 13→**14**, compact TYPE+PARAM UI (see below). Repo doc: `SESSION_NOTES_2026-06-23.md` at HEAD `79fcab3`.

**New module `services/visualization/cutout_filters.py`:**
- `DEFAULT_FILTER_PRESETS` / `get_filter_presets()` — the CONFIG POINT (edit the list). Each preset `{key,type,type_label,param_label,kind,<params>}`. Set (tuned for Copia/412 VLPs): `raw`; **Denoise (edge-safe)** light/strong (`skimage.restoration.denoise_tv_chambolle` weight 0.04/0.12); **Local contrast** subtle/punchy (CLAHE `skimage.exposure.equalize_adapthist` clip 0.01/0.03); **Bandpass** 35–800 Å; **Lowpass** 30/50 Å.
- `build_filtered_atlas` dispatches by `kind`: freq (raw/lowpass/bandpass) = FFT-filter raw then normalize from filtered; image-ops (denoise/clahe) = normalize raw→[0,1] then run the op. skimage calls wrapped try/except → passthrough (degrade, never crash). `apply_filter_2d` = the FFT Butterworth (order 4) freq filter only.
- `FILTER_SCHEMA_VERSION` stamped into every index JSON (cache key). Bandpass/lowpass need apix; denoise/CLAHE don't (kept when apix unknown).
- `resolve_apix(header_voxel_size, hint)` → (apix, source_str): MRC header wins, then hint, else "unknown".
- `build_filtered_atlas(frames, fail_info, ...)` + `emit_filtered_atlases(...)` — SHARED by both pipelines. Both render paths reduce to "produce a list of 2D float frames" then emit raw + one variant atlas per preset. `keyed_path` → `cutout_atlas__<key>.png` (raw keeps base name, back-compat).

**The two pipelines (both covered):**
- subtomo/auto → `preview_render.render_pick_cutouts_atlas` (apix from .mrcs header = EXTRACTION bin; NO hint — recon px is a different bin). Built in `preview_orchestrator.generate_candidate_previews`.
- recon/manual lists → `recon_cutouts.render_recon_cutouts_atlas` (apix from recon header = RECON bin; hint = entry pixel_size_ang). Built on-demand via `_auto_kick_list_cutouts` (dashboard).
- ⚠️ recon apix ≠ subtomo apix — reading each file's OWN header is what makes the Å→px conversion correct per-pipeline (the user's explicit concern). apix + provenance shown in a tooltip on the filter control.

**Cache/self-heal (⚠️ per-project, LAZY, BACKGROUND — caused "I don't see filters on old projects"):** manifest `MANIFEST_VERSION` now **14**; entries store `cutout_variants`/`cutout_apix`/`cutout_apix_source`. The manifest rebuilds the FIRST time each project's dashboard is opened: `_auto_kick_preview_generation` fires → `generate_candidate_previews(force=False)` → version mismatch wipes `prior_entries` (orchestrator ~line 285) → full rebuild → page refreshes when the bg task lands. NOT instant and NOT all-projects-at-once. Dashboard "Re-render all" wand button forces it. Manual/recon lists self-heal via `_atlas_index_is_current()` now keyed on `filter_schema == FILTER_SCHEMA_VERSION` (not just `apix_source`).

**UI (`ui/tomo_dashboard_dialog.py`) — compact TYPE+PARAM:** shared `_render_cutout_filter_controls(presets, apix, apix_source, on_select)` = a TYPE select (None/Denoise/Local contrast/Bandpass/Lowpass) + a conditional PARAM select (the type's option) + `?` tooltip. The standalone Å/px toggle was DROPPED for compactness (px shown in tooltip); SORT slimmed to icon + compact select in the same row. Helper `_cutout_filter_tooltip` kept; `_cutout_filter_label/_options` removed.
- Auto `_render_gallery_body`: `on_select(key)` → `nonlocal atlas_url` swap → `_refresh_grid()` + wrapped `_refresh_reference_strip()` (noise-floor tiles filter with the grid; template thumb stays raw).
- Manual `_render_list_cutout_sheet`: `on_select(key)` → in-place per-tile `background-image` swap via the `tiles` dict (NO rebuild — preserves keep/drop handlers + dot sync). Sprite-atlas + CSS background-position → switching = swap one URL, instant ([[feedback_visualization_patterns]]).

⚠️ Server has NO auto-reload — user must restart `python main.py`; existing projects regenerate cutouts (with variants) on first dashboard open after restart. Full-app import needs pandas env; verified py_compile + ruff only. See [[feedback_gallery_calibration_anchors]], [[project_pixel_arithmetic_pain_point]], [[reference_412_dataset_locations]].

2026-10-18: filter variants are lazy. Passes write the raw atlas only; picking a filter in a gallery renders that
variant for that one atlas in a RENDER worker, from the raw atlas's 8-bit tiles (`cutout_filters.render_variant`;
`frame_px` in the raw index gives the tile pixel size for Å cutoffs). Maintainer asked for this ("render the same
cutout 8 times ... incredibly wasteful"). Not run.
