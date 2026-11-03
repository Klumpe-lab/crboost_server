---
name: project-journey-rework-dashboard-refactor
description: Journey (tomo_dashboard) UI rework + decomposition of the 6930-line ui/tomo_dashboard_dialog.py into a ui/dashboard/ package; R0 done, P1 C1 (de-dialog) verified + C2 (heatmap strip) landed; R1 (2× recon slabs), R2 (per-panel visibility toggles), R3 (collapsible Dataset) landed 2026-06-08
metadata: 
  node_type: memory
  type: project
  originSessionId: 2f22aed7-50ba-4cd5-955e-82b5f628a5e3
---

Two coupled efforts, scoped + sequenced with the user 2026-06-08. **Why:** the Journey
panel's interaction is janky and its source file is a 6,930-line monolith that the ArtiaX
work will keep churning.

**The jank (root cause):** Journey opens as a `ui.dialog().props("maximized")`
(`tomo_dashboard_dialog.py` open entry) — a z-6000 overlay that COVERS the always-present
60px primary sidebar (Pipeline/Workbench don't, because they swap inside `main_area` via
`workspace_page._switch_to`). 2–3 s open = synchronous disk I/O before `dlg.open()`
(`starfile.read` ×3, eager `_read_pick_list_voxels` for the selected TS). Per-TS status lives
in the journey's OWN 300px internal sidebar (`render_sidebar`/`_render_ts_row`), duplicating
species name+count across the sidebar row + particles header + particles tabs.

**UI rework DECIDED (not yet coded):**
- De-dialog Journey → embedded panel swapped into `main_area` via `_switch_to("journey")` like
  Workbench; never covers the 60px sidebar. Add active-icon highlight (Pipeline has it via
  `_roster_visible`; Workbench/Journey had none).
- Replace the 300px internal sidebar with a **heatmap-matrix top strip** (journey-panel-scoped
  header): species = frozen-left rows (name + Σ pick/filtered totals) + a prep row
  (FS·Al·CT·Re); tilt-series = horizontally-scrolling columns; cell = pick count shaded by
  status; filtered shown for the selected column; selected column drives the detail view below.
  Kills the name/count duplication, reclaims horizontal space.
- Load shell+spinner instantly; defer collection + heavy particles render off the event loop.
- Hide the 300px pipeline roster in journey mode → full width for preview/picks/graphs.
- Gallery visual-density redesign stays DEFERRED (user-flagged in [[project_artiax_bridge]]).

**Refactor PLAN — target package `ui/dashboard/`.** Key enabling fact: the file is a
near-isolated leaf — entire public API = `open_tomo_dashboard` + `has_any_previews_rendered`,
ONE external caller (`pipeline_roster.py:1494`), minimal inbound coupling — so internals can be
rearranged freely. **Constraint:** Claude's venv has no pandas/numpy (and starfile/mrcfile only
partly), so verification = `venv/bin/python3 -m py_compile` + `venv/bin/ruff check` only, NEVER
runtime-tested by Claude (per [[reference_hpc_env]]). → maximize behavior-preserving mechanical
moves where compile+lint is full proof.
- **R0 (safe, behavior-preserving):** `css.py` ✓ DONE; then `data.py` (journey/status
  collectors ~101–700 + readers + tomo-rows ~6595–6680 — FOUNDATION; shared helpers
  `_resolve_species`/`_split_species_id` live here); `figures.py` (Plotly builders ~704–935 +
  per-tilt ~974–1147); `pixel_sanity.py` (~2203–2912). Dependency order **data → figures →
  pixel_sanity** (data is foundational). NOTE: only css was a clean contiguous block; the rest
  interleave at the function level → per-function regroupings, more edits, same green gate.
- **P1 (the rework):** build `panel.py` (embedded, async) + `strip.py` (heatmap).
- **P2 (defer to when ArtiaX workbench slices 3b–6 touch them):** `sections/prep.py`,
  `sections/reconstruct.py`, `particles.py`, `gallery.py` (`_render_gallery_body` ~800 lines),
  `pick_lists.py` + `curation.py` (both grow with ArtiaX), `auto_kick.py`.

**Sequencing DECIDED:** safe extractions first (R0), then the rework.

**Progress — R0 COMPLETE 2026-06-08.** `ui/tomo_dashboard_dialog.py` **6930 → 4590 (−34%)**; all
green (py_compile + ruff check/format across original + package). New `ui/dashboard/` package:
`css.py` (666, `_CB_CSS` + `ensure_assets_loaded`), `data.py` (702 — journey/status collectors +
shared lookup helpers `_find_job_by_type`/`_resolve_species`/`_split_species_id`/`_template_match_instances`
+ constants `_SPECIES_OVERLAY_COLORS`/`_PILL_STAGES`), `figures.py` (419 — Plotly builders +
`_safe_floats`/`_stats`), `pixel_sanity.py` (630 — `_compute_pixel_chain`/`_apply_sanity_rules`/table).
Method that worked: extract → new module → ruff check **F821 (undefined) + F401 (unused)** as the
oracle for behavior-preserving moves; re-grep line numbers before every cut (deletions shift
everything below). Data readers `_read_picks_json`/`_read_per_tilt_df`/`_per_tilt_star_path`/
`_read_atlas_index` + `_read_tm_job_json` left in the original (used by staying sections; can move in
P2). Only external caller updated: `pipeline_roster.py` (`has_any_previews_rendered` now from
`ui.dashboard.data`). Pre-existing `asyncio` F401 in pipeline_roster.py:1 left untouched (not ours).
NOTHING runtime-tested — needs the user's module-loaded app. **NEXT: P1 rework** (panel.py embedded +
strip.py heatmap), then P2 carve. **Full cold-start handoff (R0 result + P1 plan with current
anchors + P2 list + the ruff-oracle playbook): `ui/dashboard/REFACTOR_HANDOFF.md`.** R0 smoke-tested
by user (opens, doesn't break).

**Progress — P1 IN FLIGHT 2026-06-08, two checkpoints.** **C1 (de-dialog) — VERIFIED by user.**
`open_tomo_dashboard()` → `build_journey_panel(container, callbacks)` embedded into a new
`journey_container` in `workspace_page.py` (lazy-built on first open behind a spinner, SingleFlight-
guarded `_show_journey`); `_switch_to` extended for `"journey"`; the 4 s live timer pauses when
hidden via `on_journey_active`. `pipeline_roster.set_active_mode` = exactly-one-lit nav highlight
(pipeline/workbench/journey) + hide/restore the 300px roster; `_on_pipeline_icon` (layers → pipeline
when elsewhere, else toggles roster). **C2 (heatmap strip) — LANDED, pending user runtime verify.**
New `ui/dashboard/strip.py` (`build_strip`) imports ONLY `data.py` (DAG one-way; ⓘ popover passed in
as a callback — NO back-import into the shell). Matrix: frozen-left prep row + per-species rows
(name + Σ pick/filt) × horizontally-scrolling TS columns; prep cell = 4 status dots, species cell =
`n_picks` shaded by furthest stage (subtomo>pick), filtered shown in the selected column. CSS in
`css.py` (`.cb-strip-*`). `build_journey_panel` flipped row→column; `render_sidebar`→`render_strip`
gated on `(_journey_signature, selected_ts)`; deleted orphaned `_render_ts_row`/`_pill_tooltip`;
dropped redundant per-tab pick count. `tomo` 4590→4501. **Deferred:** deep async-on-TS-switch (C1
spinner covers the worst-case first open; switch stays sync per validated C1 feel). All green
(py_compile + ruff check/format); NOT runtime-tested by Claude. Cold-start detail in
`ui/dashboard/REFACTOR_HANDOFF.md` (Progress section). **C2b polish landed same session** (user
feedback after trying C2): `select_ts` now async = instant TS-switch (cheap highlight + spinner +
flush, then sync render; `_sel_gen` guard; `build_strip` returns `{ts: col}`); filtered/kept count on
ALL cells not just selected (cols 52→62px); de-greened species cells (green now ONLY on the
kept-count number + prep dots — running/fail/zero keep tints, "has picks" is neutral). **Roadmap
recorded in handoff ("Next UX asks"):** R1 = recon xy/xz preview ≥2× bigger; R2 = per-panel
visibility toggles under the TS columns, PERSIST across projects+TS (needs a new user-pref layer,
e.g. `app.storage.user`); R3 = collapsible Dataset (title+metadata default, pixel-sanity table on
expand) — all BEFORE the big picks-gallery manual/auto/merge rework.

**Progress — R1 (2× recon slabs) LANDED 2026-06-08, pending user runtime verify.** Three levers
(all `css.py` + the inline cap in `_render_particles_canvas`): (1) flipped the particles-split column
proportions — `.cb-particles-canvas-col` `flex 1 1 360 / max 540` → `flex 3 1 600 / max 1080` (grow
1→3, **width cap doubled 540→1080**), `.cb-particles-tabs-col` `flex 2 1 440` → `flex 1 1 360` (grow
2→1, yields width) so the slab column dominates and the gallery is pushed right; (2) raised the XY
height cap `52vh → 76vh` (inline `stack.style(calc(76vh * x/y))`, no-picks preview `70→76vh`); (3)
left-aligned the stack (`.cb-canvas-stack` `margin 0 auto → 0`). **R1b (gutter fix, same session,
user-reported):** first pass left the column growing to its flex share (≤1080) while the height-capped
near-square slab was only ~820px → ~200px whitespace before the gallery. Fix: column max-width now set
INLINE per-tomo in `_render_particles_section` to `min(1080px, calc(76vh * x/y))` (the slab's actual
height-limited width), and the inner stack's own max-width dropped (just `width:100%`-fills the column)
→ column hugs the previews, gallery reclaims the space; fixes the no-picks path for free. Net on 1080p:
landscape tomos hit a
clean 2× linear (column-bound 1080px), near-square tomos ~1.5× linear / ~2.3× area (height-bound
76vh≈820px — viewport height is the physical limit; full 2×-linear on a square tomo needs ~100vh and
breaks XY/XZ co-visibility). **Dials: `76vh` (drop to ~62vh if XY+XZ don't co-fit) + `1080px` width
cap.** Pick overlays scale automatically (fractional `inset:0`). All green (py_compile + ruff).

**Progress — R2 + R3 LANDED 2026-06-08, pending user runtime verify.** Key fact: the **user-pref
persistence layer already existed** — `services/configs/user_prefs_service.py` (`UserPreferences`
Pydantic + `get_prefs_service()` singleton, dual-stored `app.storage.user` + `~/.crboost/prefs.json`,
`storage_secret` wired in `main.py`, used across main_ui/data_import/roster). No new layer built; just
added two fields: `dashboard_hidden_panels: List[str]` (absence ⇒ visible) + `dashboard_dataset_collapsed:
bool = True`. **R2 (per-panel visibility):** dense-checkbox row `.cb-panel-toggle-row` (mirrors
`.cb-species-toggle-row`) between strip and detail pane in `build_journey_panel` (`_build_panel_toggle_row`,
built once after `render_main`); 7 panels from module-level `_DASHBOARD_PANEL_KEYS`; `_toggle_panel`
updates pref → `_save_dashboard_prefs(app.storage.user)` → `render_main()` (strip untouched);
`_render_main_pane_for_ts` gates each emitter on `_hidden_dashboard_panels()`; all-hidden → "re-enable
above" hint. **R3 (collapsible Dataset):** `_render_dataset_section` header now `.cb-collapsible-header`
(clickable) + rotating `.cb-collapse-caret`; body (chips+grid+sanity table) wrapped in
`.cb-collapsible-body`, `.cb-collapsed`=display:none; default collapsed (`_dataset_collapsed()`); click
toggles CSS classes directly + persists `dashboard_dataset_collapsed` (no re-render, instant);
orthogonal to R2. New imports in tomo_dashboard_dialog.py: `app` + `get_prefs_service`. All green
(py_compile + ruff check/format). **NEXT: user verifies R1b + R2 + R3, then the big picks-gallery
manual/auto/merge rework ([[project_artiax_bridge]] / docs/roadmaps/completed/roadmap_artiax-bridge.md).**

Related: [[project_ux_jank_refactor]] (stage-1 left the dashboard untouched — this is
that follow-up), [[project_per_tomo_dashboard_arc]], [[feedback_reactive_ui_patterns]].
