---
name: picks-filter-per-species-and-aggregation
description: "Subtomo-gallery pick filtering is per-species; cross-project aggregation selector + per-tomo merge is BUILT (2026-05-21); FLOW/UX AUDIT + prioritized plan recorded 2026-06-25 (priority = 10 projects of already-extracted filtered picks → aggregate → reconstruct); ① KEYSTONE LANDED 2026-06-26 (merged optset = first-class labeled resolver producer; code-clean + 4-lens review, PENDING runtime); roadmap at services/visualization/PICKS_FILTER_AGGREGATION_ROADMAP.md"
metadata: 
  node_type: memory
  type: project
  originSessionId: b08ecc18-77c5-4c49-9829-171c437affca
---

`services/visualization/picks_filter.py` curates subtomo-extracted particles from the
dashboard's subtomo gallery → writes `particles_filtered.star` + `optimisation_set_filtered.star`
next to a SUBTOMO_EXTRACTION job (originals untouched; downstream auto-prefers `_filtered`
via the IO-slot resolver).

**2026-05-21 fix — "Save failed: Could not read candidates … from …/External/job021/vis":**
two bugs in `_render_gallery_body`:
1. `ce_job_dir = Path(picks_json).parent.parent.parent` was off by one → `<ce>/vis` (picks.json
   is at `<ce>/vis/preview/<tomo>/picks.json`, needs 4 parents).
2. `find_subtomo_job_dir_for_cutouts` picked the lex-greatest subtomo job → mis-targeted EVERY
   species at one job (e.g. 412_blob would save into 412's job022).

Fix: the gallery now takes `ce_job_dir` + species-matched `subtomo_job_dir` from
`_collect_species_data_for_ts` (resolves species_id via `_resolve_species`, subtomo via
`_matching_subtomo_instance`). Correct for any number of registered species. The lex-greatest
heuristic was deleted from picks_filter.py. UNVERIFIED in browser (user should click Save).

**Aggregation FIRST VERSION BUILT (2026-05-21):** hierarchical merge selector replaces the old
flat checkbox list in `ui/aggregation_merge_card.py` — Project → Species → Tomogram tree (project
rows reuse projects-overview avatar look; tomo leaves show per-TS `kept/total` picks + reviewed
marker). Per-tomogram fine selection: `ProjectState.aggregation_sources` is now
`List[AggregationSource]` (`optset_path` + optional `tomo_names`, None=all; legacy `List[str]`
auto-migrated by a field_validator). Merge driver `drivers/subtomo_merge.py` subsets particles +
tomograms by `rlnTomoName` per source (sources now accept str OR `{"path","tomos"}` dict — in-job
merge still passes plain strings). `picks_filter.resolve_canonical_optset()` is the single
filtered-or-original definition, applied at merge build so curation after selection is honored.
Per-tomo curation loaded lazily via `aggregation_discovery.load_tomo_curation` (off the scan).
UNVERIFIED in browser. **Key structural decision (unchanged): curation stays per-project (Å-coord
match); aggregation is metadata-preserving concatenation + tomogram dedup — never re-match by
coord across projects.** Still NOT handled: tomo_name/optics-group/particle-name collisions
(namespacing), pixel/box/handedness/template consistency per species, provenance/staleness.

**Iterated to a polished selector + registry (still UNVERIFIED in browser):** Project→Species→Tomo
tree, slate palette w/ steelblue ONLY for curated/uncurated, fixed-width right-aligned pick column
at every level (indent wrappers MUST be `w-full`+padding-left, not bare margin-left divs, or flex
collapses them to content width). Per-tomo mutually-exclusive curated/original toggle
(`AggregationSource.original_tomos`; driver mixes from the original optset). **Persistence gotcha:**
`state.save()` does a full model_dump+disk write (~1s) — checkbox handlers MUST debounce + run it off
the event loop (`_persist_state` → `run.io_bound`), and selection-rebuild helpers must preserve
`original_tomos` or they get wiped. **Named merges + registry:** each merge → `MergedSources/<slug>/`
(coexist), recorded in `ProjectState.aggregation_merges` (name/desc/manifest w/ box·apix·binning +
mixed-metadata warnings); `active_merge_slug` (newest default, switchable) drives downstream wiring
via `active_merged_optset()`; `apply_aggregation_overrides` now reads that, not a fixed path.

Also landed 2026-05-21 in the gallery: degenerate tiles (non-rendered picks shown at the end of
the grid; when auto-excluded they take the SAME red-orange dropped styling as a vetoed tile via
`cb-tile-dropped`) + a default-on "exclude N not-extracted" checkbox; hovered-pick stats moved to
a horizontal strip at the top.

**Slab dots are now click-to-filter + reflect drop state (2026-05-21 round 2).** Clicking a
ghost dot on the X/Y/X-Z slab toggles keep/drop just like clicking a tile: the per-species hover
bridge dispatches a `cbpicktoggle` CustomEvent on `grid_container` (scoped/auto-cleaned, via
`grid_container.on(..., js_handler="(e)=>emit(e.detail)")` → `_toggle_keep`). Dropped picks (and
degenerate picks when excluded) get their dots greyed via `_sync_dropped_dots()` (a class toggle
`cb-pick-ghost-dropped` run on each `_refresh_grid`), so slab and gallery always agree whichever
side was clicked. The 3px dot got a transparent `::after` inset:-4px hit-area so it's clickable.
Degenerate dot-clicks are no-ops (guarded in `_toggle_keep`). Only works for the ACTIVE species
tab (inactive galleries are unmounted, so `getGrid()` is null → dispatch no-ops).

**2026-05-21 round 3 — crash fix + roster review column.**
- **Crash** "The parent element this slot belongs to has been deleted" on tile-click after
  save+switch: `_sync_dropped_dots` called `ui.run_javascript`, which resolves the client via
  `context.slot.parent` — but `_refresh_grid`'s `grid_container.clear()` had just deleted the
  clicked tile (the handler's own element). The exception aborted `_toggle_keep` before
  `_refresh_counter`, so the next tomo's Save button stayed disabled. Fix: capture
  `_gallery_client = ui.context.client` at render time and call `_gallery_client.run_javascript`
  (try/except). **General gotcha for this dashboard:** any `ui.run_javascript` reachable from a
  handler that does `.clear()` on an ancestor must use a captured client, not the slot context.
  (Dot-clicks didn't crash because their handler is bound to `grid_container`, which survives.)
  Also reordered `select_ts` to render_main BEFORE render_sidebar — render_sidebar rebuilds and
  deletes the clicked row, so the main render (whose `_scroll_section_into_view` can run JS) must
  happen while that row is still alive.
- **Roster review column:** save writes a `{ts: kept_count}` sidecar (`curation_reviewed.json`,
  `read_reviewed_counts` / `_update_reviewed_sidecar`, removed on discard). `_collect_species_journey`
  surfaces it as `filtered_count`; the roster species track shows an indigo funnel + kept count
  ONLY for reviewed TS (presence = "reviewed", absence = "not yet"). Updates on TS switch
  (select_ts now runs the gated render_sidebar; `filtered_count` is in `_journey_signature`).

**2026-05-21 round 4 — filter toolbar UX.** Removed the confusing "Reset" button (in-flight
revert) — toolbar is now just **Discard filter** + **Save picks**. Save/Discard show a 2 s toast
and refresh the roster review column IMMEDIATELY (a sidebar-only `refresh_roster` = the gated
`render_sidebar`, threaded `render_main → _render_main_pane_for_ts → _render_particles_section →
_render_species_tab_body → _render_gallery_body`; sidebar-only so the active species tab/scroll
isn't reset). A filtered-set path line (`optimisation_set_filtered.star` + copy button) shows
under the buttons when a filter exists, hidden after discard (`_refresh_path_row`). **Discard is now
PER-TS (fixed):** the gallery button "Reset this tomo" calls `picks_filter.discard_ts_filter(subtomo_job_dir,
ts_name)` — reverts ONE TS to all-original rows in the filtered star + drops it from the sidecar, keeping
other TSs curated; when the last curated TS is removed it falls back to `discard_filter` (deletes the pair).
`discard_filter` (whole-species) still exists for that fallback. Scope = species-matched `subtomo_job_dir`,
so if two species ever shared one subtomo job a reset would hit "both" (attribution bug, not discard's).

**2026-06-25 — flow/UX audit (merge WORKS but flow is "awful").** Full audit + sequenced plan
recorded in docs/roadmaps/completed/roadmap_picks-filter-aggregation.md §"2026-06-25". **★ Priority success use case (user):** 10
projects of already-extracted filtered auto-picks → aggregate → **ReconstructParticle** (the
already-extracted→reconstruct path; "aggregate picks→extract" is deprioritized). **Insight:**
`optimisation_set.star` is the single input slot for subtomoExtraction AND reconstructParticle AND class3d —
state (picks vs extracted) is the only difference. **★ KEYSTONE bug (verified):**
`path_resolution_service.py:532` `_add_merged_sources_candidates` checks the flat
`MergedSources/optimisation_set.star`, but merges write `MergedSources/<slug>/` → synthetic "Merged Sources"
producer never registers → consumers get the invisible `manual:` override ("Manual path…"). Fix = relocate
`active_merged_optset` (ui/aggregation_merge_card.py:77) to ProjectState + point the resolver candidate at
the slug path + label it (also a P3 down-payment). Also: `merge_panel_component.py` is DEAD (delete),
`_DIALOG_REFS`/`_registry_expanded` are cross-tab module-global leaks, no SingleFlight, `[MERGE DEBUG]`
prints in subtomo_merge.py. ⚠️ Don't build is_aggregation-only chrome that P5 deletes. See
[[project_microscope_apix_default_trap]], [[project_particle_project_roadmap]].

**2026-06-26 RUNTIME — 412 capsid ran END-TO-END (aggregate→reconstruct→class3d); 2 more bugs fixed.**
First real scientific use of the aggregation pipeline. Project `/groups/klumpe/crboost_data/412_aggregation`
(merged optset `MergedSources/merge-20260626-132413/`, 112 particles, box 600 / 1.55 Å/px / bin 1, I1 capsid).
ReconstructParticle (job002, bin2 box384 crop224 I1 theme=classic) → `merged.mrc`; Class3D (job003, K=3, I1,
particle_diameter 550, ini_high 45, ref=merged.mrc) → **class001 = 94 particles @ 16.3 Å (winner)**; classes
2/3 = junk sinks (7+11). Followed the PI's recipe (`/groups/klumpe/user/<pi>/Processing/412/try1`
job118 reconstruct + job121 class3d). **Full handoff + all paths + next steps in
`docs/reports/412-capsid/412-capsid-aggregation-handoff.md`.** Started a repo `docs/` folder this session (also
`docs/binning-and-resolution.md` — ELI5 bin-for-sorting/unbin-for-final-resolution teaching doc).
**Class3D driver bug FIXED** (`drivers/class3d.py`): it checked for the bare `run_optimisation_set.star`
(a Refine3D/auto-refine name) but relion_refine with fixed `--iter N` writes `run_it{N}_optimisation_set.star`
→ **every Class3D false-failed at the very end** despite completing all iterations. Now checks the numbered
file + mirrors to the bare name. Recovered job003 by hand (copy optset to bare name + flip
RELION_JOB_EXIT_FAILURE→SUCCESS). **Speed gotcha learned at runtime:** Class3D on the bin-1 (600px/1.55)
merged particles took ~2.5 h for a 17 Å sort because it ran at full res (reference resized UP to 600px) —
the fix is to classify on BINNED particles (re-extract bin~4) and only refine the winner at full res.
crboost has NO Refine3D / class-select job — refinement + class selection happen in RELION/M; next step =
"cut" the 94 class-1 particles (subset run_it015_data.star to rlnClassNumber==1) → Refine3D in RELION.
⚠️ The cosmetic 1.35-apix default-trap STILL unfixed at root for data-less aggregation projects (harmless,
RELION reads 1.55 from the star). See [[project_microscope_apix_default_trap]].

**2026-06-26 RUNTIME — keystone exposed a latent `ProjectState.load()` data-loss bug (FIXED).** First
real run (412_aggregation: ReconstructParticle on merged optset) failed at DRIVE time with the keystone's
own guard: "Cannot resolve required inputs for reconstructParticle: input_optimisation (merged-sources
optimisation_set not found)". Root cause = `ProjectState.load()` is **field-by-field** (hand-written
`cls(...)` + per-field assignments, NOT `cls(**data)`), and it silently **dropped** `aggregation_merges`,
`active_merge_slug`, `is_aggregation`, `aggregation_sources` (and curation `pick_lists`/
`authoritative_pick_lists`). UI server had them in live memory (merge worked), but the SLURM **driver**
loads fresh from disk → empty registry → `active_merge()`→None → no `mergedSources` candidate → override
unresolvable. The OLD `manual:<abspath>` wiring survived this by luck (self-contained path); the keystone's
`source_key` wiring depends on the registry, so it surfaced the bug — and the guard correctly failed LOUD
instead of silently reconstructing on a wrong optset. **Fix: `load()` now restores all of them** (mirrors
species_registry/imported_tomograms). ⚠️ GOTCHA for future work: **any new persisted ProjectState field
MUST be added to `load()` explicitly or it's silently dropped on every reload.** Driver picks up the fix on
next run (fresh process, imports crboost_root from disk) — no server restart needed to re-run.

**2026-06-26 — ① KEYSTONE LANDED (code-clean + 4-lens adversarial review, 0 confirmed bugs; PENDING
runtime).** Merged optset is now a first-class, labeled resolver producer. Relocated `active_merge()`/
`active_merged_optset()`/`active_merged_optset_instance_path()` + `MERGED_DIR_NAME` onto `ProjectState`
(out of the UI). `_add_merged_sources_candidates` now points at the slug optset (was the dead flat path);
`OutputCandidate` gained a `label` field → dropdown shows **"Merged sources — <name>"**.
`apply_aggregation_overrides` wires via the producer **source_key** (`mergedSources:MergedSources/<slug>`),
not a bare `manual:` path → the IO-config dropdown shows the named producer SELECTED.
`resolve_edges` already excluded `mergedSources` (pre-wired) → no dangling afterok edge. Review-driven
fix: a dangling `mergedSources:` override (optset deleted / active merge switched to a fileless slug) now
SURFACES (resolve_inputs→`missing_required`/`PathResolutionError`; validate→red) instead of silently
auto-picking a foreign optset producer — honors the no-silent-default rule, was a regression from the old
`manual:` "File not found". Ride-alongs: deleted dead `merge_panel_component.py`; stripped `[MERGE DEBUG]`.
**⚠ runtime-UNVERIFIED (no pandas/nicegui env) — user restarts main.py + confirms a ReconstructParticle
input dropdown lists/selects "Merged sources — <name>".** Deferred ride-alongs (UI, want runtime):
SingleFlight on dialog-open, `_DIALOG_REFS`/`_registry_expanded` cross-tab leak, BackgroundTask feedback,
reactive merged badge. **Next: ② after-merge one-flow** (detect extracted-vs-picks → one-click
"Reconstruct particles" that adds+wires the consumer), then **③ surface state/counts + block mixing
extracted/picks**; ② builds on ① — runtime-verify ① first.

Related: [[project_candidate_preview_subtomo_cache_race]], [[project_per_tomo_dashboard_arc]],
[[project_412_divergence_cascade]], [[feedback_reactive_ui_patterns]].

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

Save-filtered bug fixed (per-species subtomo dir, not vis path). 2026-06-25 flow/UX audit: priority = 10 projects of already-extracted filtered picks → aggregate → reconstruct. ① KEYSTONE LANDED 2026-06-26: merged optset = first-class labeled resolver producer (slug path + source_key wiring on ProjectState); 4-lens review clean, PENDING runtime. Next = ② after-merge one-click Reconstruct.
