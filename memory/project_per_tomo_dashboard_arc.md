---
name: per-tomogram-dashboard-arc
description: "Journey per-TS dashboard (ui/tomo_dashboard_dialog.py). Particles section: slabs LEFT / per-species gallery tabs RIGHT, recon preview = WarpTools PNG. 2026-05-20 bug+layout pass: X/Z=X/Y width, per-tab header (Option A) tucks pills/3dmod, restyled color-dot tabs, hover cross-link delegated on the card. UNVERIFIED in browser. Phase 2 = unify journey visual language."
metadata: 
  node_type: memory
  type: project
  originSessionId: 5c809c59-075f-4d98-8bbf-40bcfe427892
---

`ui/tomo_dashboard_dialog.py` (the "Journey" dialog, renamed from old `candidate_preview_dialog.py`) is a **per-tiltseries pipeline dashboard**: sidebar = TS list, main panel = section cards per pipeline stage that fill as jobs produce output. Each `_render_<stage>_section(...)` is emitted from `_render_sections_for_ts` in pipeline order; no-op when its job hasn't run. Plan: `services/visualization/ROADMAP.md`.

## Current state after the 2026-05-20 "unite the waterfall" refactor (PR1–3 + a layout pass)

Per-TS the dashboard now shows: dataset / fs-ctf / tilt-filter / align / ctf chips → **Reconstruct** → **Particles**.

- **Reconstruct** (`_render_reconstruct_section` → `_render_recon_big_preview`): the **WarpTools PNG** (`<recon_job>/warp_tiltseries/reconstruction/<ts>_<res>Apx.png`), X/Y top-down only, no picks, no Invert. (We briefly switched it to our own slab render; the user rejected that — looked washed-out and the X/Z strip read as a "black streak". Reverted. Keep recon preview = WarpTools PNG.)

- **Particles** (`_render_particles_section`): ONE card, 2-column flex (`.cb-particles-split`):
  - **LEFT col** (`.cb-particles-canvas-col`, `_render_particles_canvas`): species checkboxes (toggle each species' layers) + our **X/Y and X/Z slab** render (cached `<recon_job>/vis/slabs/<ts>_{xy,xz}.png` via `_auto_kick_recon_slabs`) with every candidate-extract species' picks overlaid as color-coded toggleable layers (`_render_pick_layer`, per-species `--sp-color`). Own Invert switch (`_render_invert_switch` on the card → `.cb-invert-polarity .cb-recon-canvas img`). X/Y capped ~52vh, X/Z ~22vh.
  - **RIGHT col** (`.cb-particles-tabs-col`): `ui.tabs`/`ui.tab_panels`, one tab per species → `_render_species_tab_body`: TM sanity strip (`_render_species_sanity_strip` → reuses `_render_tm_size_chips`, matched to species by species_id; includes subtomo box/crop) → regen toolbar → `_render_gallery_body` (atlas) or `_render_picks_scatter_section` (fallback) / zero-picks / spinner.
  - **Cross-link**: each species' canvas layer carries a DOM id + `data-pick-idx` dots + a `.cb-pick-marker`; the tab's `_render_gallery_body` bridge targets that layer id, so hovering a tile lights its dot on the always-visible canvas (the reason slabs are LEFT of the gallery — simultaneous inspection).
- Pick dots: `_SPECIES_OVERLAY_COLORS` = vivid saturated hues absent from greyscale + dual dark/light halo ring, small (3.5px). Zero-pick species → `_render_zero_picks_empty_state` (template thumb + cutoff hint).
- Chrome densified (section card padding, header margins, `.cb-section-title`/datadump weights 700→600, tomo-header label text-sm/bold→text-xs/semibold, tighter tabs).

**Removed (don't look for these):** `_render_template_match_section`, `_render_candidate_extract_section`, `_render_preview_and_gallery_section`, `_render_gallery_placeholder`, `_render_recon_clean_slab`, `_collect_species_picks_for_ts`.

## 2026-05-20 (later) — bug + layout pass on the Particles section
Four changes, all in `ui/tomo_dashboard_dialog.py`, UNVERIFIED in browser:
1. **X/Z = X/Y width.** `_render_particles_canvas` now wraps both slabs in one `.cb-canvas-stack` (max-width caps X/Y ~52vh tall); each child is `width:100%` + own `aspect-ratio`. (Side effect: X/Z height now scales with that width, can be taller than the old 22vh cap for big z/x.)
2. **Header reorg (Option A).** Replaced `_render_species_sanity_strip` with `_tm_essentials_for_species` + `_render_species_size_chips` + new `_render_species_tab_header`. Tab body (`_render_species_tab_body`) now = compact header (essentials line + render-missing/IMOD icon buttons + collapsed "sizes & 3dmod" expansion holding chips + Re-render-all + 3dmod) → gallery. Removed `_render_tomo_header_section` (inlined). `.cb-instance-toolbar` CSS now dead (sweep in phase 2).
3. **Tabs restyled.** `.cb-species-tabs` small/slate/no-caps + indigo indicator + per-species `::before` color dot via inline `--sp-color`. (Used `label=` prop, not slot content — NiceGUI forces label=name otherwise.)
4. **Hover cross-link fixed for non-default tabs.** Bridge JS in `_render_gallery_body` now delegates mouseover/mouseout on the Particles `.cb-section-card` + resolves the grid lazily; filters to its own grid + `.cb-pick-layer` ids. (Root cause: q-tab-panels unmount inactive panels, so the old one-shot `getElementById(gallery_id)` bailed.)

## 2026-05-21 — hover polish + roster redesign + click-lag (UNVERIFIED in browser)
1. **Sticky-dots fixed**: bridge now clear-then-set, one `.cb-ghost-active` at a time.
2. **Dots restyled**: 3px, crisp dual ring, active = backlight glow not 9px expansion. See [[feedback_journey_layout_preferences]].
3. **Roster rows redesigned** (`_render_ts_row` + `_collect_species_journey` + `_render_ts_info_popover` + `_PREP_STAGES`): title + ⓘ click-popover (full name, Å/px·dims, file paths w/ copy buttons), `prep` track (4 array stages), one per-species track (color dot + pick + subtomo segments + pick count). Validated data on 412: "412"=20 picks/TS, "412_blob"=30, 6.20 Å/px · 1024³.
4. **Click-lag fixed**: TS selection toggles `.selected` class on cached `row_els` + re-renders main only (no sidebar teardown); the 4 s timer's sidebar rebuild is gated on `_journey_signature`. Note: `render_main` still rebuilds every bg-tick (unchanged) — main-pane churn during background renders is a possible future item, not in scope here. Also the pink `.cb-pick-marker` locator left unchanged.

## Pending / next session
- **Verify the 2026-05-21 round in-browser**: sticky dots gone (one active at a time); dots tiny+crisp+backlight; roster two-track layout reads well + ⓘ popover opens on click with working copy buttons; switching TS is single-click (no double-click lag). Then the 2026-05-20 items below if not already confirmed.
- **Verify the 4 changes from 2026-05-20 in-browser on `agg_20260311_412_Grid3`**: X/Z and X/Y same width; hovering a tile on the 2ND species tab lights its dot on the canvas (the bug); header is tidy + gallery starts high; tabs look clean with the color dot. Claude can't run the app (needs the cluster `module load` env).
- **Phase 2 (the user's real goal): unify the WHOLE journey section's visual language** — denser, more compact, less colorful, lighter chrome; model on the subtomo gallery's minimalism. Agreed to do this once the above is verified functional. See [[feedback_journey_layout_preferences]] for the settled rules. While at it, drop dead `.cb-instance-toolbar` CSS.
- Possible future slice: class3d / 2D-class-average results as another tab or section.
- Separate deferred item: I1 angle-list dedup is ~15× oversampled — see [[project_i1_angle_list_underperforms]].

Conventions for this surface: [[feedback_journey_layout_preferences]], [[feedback_reactive_ui_patterns]], [[feedback_gallery_calibration_anchors]].
