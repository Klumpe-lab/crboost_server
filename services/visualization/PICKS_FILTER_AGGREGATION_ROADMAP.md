# Picks-filter → cross-project aggregation roadmap

Status: **filtering is per-project / per-species**; **a first aggregation selector +
merge is built** (2026-05-21). This doc records the intended direction and captures
the edge cases that still need handling.

## What's built (2026-05-21)

- **Hierarchical merge selector** (`ui/aggregation_merge_card.py`): Project → Species →
  Tomogram tree replacing the old flat checkbox list. Project rows reuse the
  projects-overview avatar/look; tomogram leaves show per-TS `kept/total` picks + a
  reviewed marker (curation from `aggregation_discovery.load_tomo_curation`, lazy per
  expanded species).
- **Per-tomogram fine selection**: `ProjectState.aggregation_sources` is now
  `List[AggregationSource]` (`optset_path` + optional `tomo_names`; `None` = all). Legacy
  `List[str]` is migrated by a field validator. The merge driver
  (`drivers/subtomo_merge.py`) subsets particles + tomograms by `rlnTomoName` per source.
- **Filtered-or-original is factored out**: `picks_filter.resolve_canonical_optset()` is the
  single definition, used by the merge build so a curation done *after* selection is honored.
- **Per-tomo curated/original override**: each curated tomogram has a mutually-exclusive
  `curated | original` toggle (`AggregationSource.original_tomos`); the driver pulls those
  tomos' rows from the original optset before concat.
- **Named merges + registry (newest-active, switchable)**: each merge writes its own
  `MergedSources/<slug>/` so species-subset merges coexist. `ProjectState.aggregation_merges`
  records name/description/created_at + per-source manifest (project, species, picks, tomos,
  box/apix/binning) + mixed-metadata warnings. `active_merge_slug` picks which one downstream
  consumers wire to (via `active_merged_optset()`); the merge panel's registry view lets the
  user expand a "what made the cut" table and switch the active merge.

Still NOT handled (see edge cases below): tomo_name/optics collisions, namespacing,
provenance/staleness, handedness/symmetry validation, schema drift. The merge is still
metadata-preserving concatenation + tomogram dedup; the box/apix/binning warning is advisory
(only pixel-size is hard-blocked by the driver's strict optics check).

## Where we are (2026-05-21)

`services/visualization/picks_filter.py` curates one SUBTOMO_EXTRACTION job at a time:

- Writes a sibling pair next to the job's canonical outputs — `particles_filtered.star`
  + `optimisation_set_filtered.star` (originals never touched).
- **Per-species**: there is one subtomo job per species, and the gallery now passes the
  *species-matched* `subtomo_job_dir` + that species' `ce_job_dir` (candidates.star) into
  the save — no more lex-greatest guessing, no `/vis` path arithmetic. Works for any number
  of registered species.
- **Per-TS incremental**: a save recomputes the current TS from scratch and preserves all
  other TSs from the prior filtered file (or original if none).
- pick_idx (gallery, score-sorted candidate order) → subtomo row via Å-coord match
  (`subtomo_link._coord_key`, rounded 0.1 Å, keyed by `(rlnTomoName, coord)`).
- Downstream auto-prefers `_filtered` via the IO-slot resolver (`prefer_if_exists`).

## Where this is heading

An **aggregate project** that unions curated particle sets across **many tomograms, many
source projects (grids/sessions), and multiple species** into per-species aggregate
`optimisation_set`s for downstream refine / class3d.

### Core structural decision (do this and most pain goes away)

**Curation stays per-project (coord-match); aggregation is metadata-preserving
concatenation — never re-match by coord across projects.** The per-project
`particles_filtered.star` IS the curated source of truth (already a row-subset with the
optics block preserved). Aggregation just concatenates curated stars + remaps identifiers.
The fragile Å-coord join stays a within-project concern.

### Suggested shape (when we build it)

- New `services/aggregation/` module — keep `picks_filter.py` focused on within-project
  curation. Aggregation *consumes* filtered stars; it does not re-derive them.
- Aggregate unit = **species**. One aggregate optimisation_set per species, built from
  `{(project, species) → resolved star}` where "resolved" = filtered-if-present else original.
- **Factor out the "filtered-or-original" choice** into one shared helper reused by the
  IO-slot resolver AND aggregation (DRY; one definition of "which star is canonical").
- Record **provenance** in the aggregate (source project + species + star path + mtime),
  so staleness is detectable (same lesson as the preview-cache race:
  [[project_candidate_preview_subtomo_cache_race]]).

## Edge cases that will bite (call them out now)

1. **tomo_name collisions across projects.** Every project has `Position_13`. The aggregate
   `rlnTomoName` must be globally unique — namespace by a project tag
   (`<projecttag>__<tomo_name>`) and rewrite consistently across particles AND the unioned
   `tomograms.star`. Without this, two grids' Position_13 silently merge.

2. **Optics-group collisions.** Every project starts at `opticsGroup1`. Aggregation must
   renumber optics groups and remap each particle's `rlnOpticsGroup`. The `data_optics`
   blocks must be unioned, not assumed identical.

3. **optimisation_set is a graph, not just particles.** It points at particles +
   tomograms + (trajectories / manifolds / FSC). An aggregate must also produce a unioned,
   namespaced `tomograms.star` and keep those references consistent — not just concatenate
   particle rows.

4. **Pixel size / box / image-size mismatch.** Co-refining particles extracted at different
   `rlnImagePixelSize` / box / `rlnImageSize` is invalid. Aggregation must group by (or
   validate) these per species and refuse/rescale on divergence.

5. **Handedness / convention drift.** If source projects differ in `flip_tiltseries_hand`
   (TomoHand ±1) or defocus-handedness, particle orientations are in different chiralities
   and cannot be co-refined. Validate/record handedness per source. See
   [[project_412_tomohand_discrepancy]] and [[project_412_divergence_cascade]].

6. **Template / symmetry identity per species.** "412" in project A vs B should be the same
   particle with the same template + symmetry. Warn if template box/apix or declared
   symmetry diverges across sources for the same species id.

7. **Star schema drift across RELION/WarpTools versions.** Centered- vs corner-coord
   conventions, missing columns (the WarpTools placeholder columns —
   [[project_warp_relion_star_placeholders]]). Reconcile columns (union + sensible
   defaults) or refuse on incompatible schemas; don't silently concatenate misaligned cols.

8. **rlnImageName / rlnTomoParticleName uniqueness + path validity.** Particle names collide
   across projects (namespace them). `.mrcs` paths must stay absolute and cross-accessible
   (same Lustre); the aggregate references per-project paths in place — don't copy gigabytes.

9. **Coord-key truncation (already bit us).** `int(round(x,1)*10)` truncates toward zero, so
   it's fragile near 0.1 Å boundaries. It's applied symmetrically within a project so it's
   fine *for curation*; aggregation must NOT depend on it (concatenate, don't re-match).

10. **Incremental + idempotent.** Re-aggregating after a new project lands or a source is
    re-curated must update in place without duplicating rows. Key by source provenance +
    mtime; detect a source re-curated after the aggregate was built (staleness).

11. **Empty / partial sources.** A (project, species) with zero kept picks contributes
    nothing but must not error. A species present in only some projects → aggregate only
    over the projects that have it.

12. **Scale.** Many projects × tomos × particles → a giant particles.star. starfile
    read/write is in-memory; millions of rows will be slow/heavy. Consider chunked/streamed
    writes before we get there.

Related: [[project_per_tomo_dashboard_arc]], [[project_candidate_preview_subtomo_cache_race]],
[[project_412_divergence_cascade]].

---

## 2026-06-25 — Flow/UX audit + the prioritized success use case

Audited the cross-project aggregation flow end-to-end (5-agent map of entry/lifecycle, the merge card,
the merge-panel component, build→downstream, and a jank inventory). The user's verdict: "works but the
flow is shitty as fuck." This section records the findings + the plan. **Code untouched — this is a
record + handoff, no implementation yet.**

### ★ THE priority (user, 2026-06-25)

> "I have 10 projects with filtered auto-picks that are **already extracted** and I want to **aggregate
> over them and pipe them through into particle reconstruction**. That's the success use case to
> prioritize."

So the critical path is **already-extracted filtered optsets → aggregate → ReconstructParticle**. The
"aggregate picks → extract" half (④ below) is explicitly **deprioritized**. Sources are the per-species
`optimisation_set_filtered.star` (filtered auto-picks); `resolve_canonical_optset()` already prefers the
filtered star at merge time, so curation done after selection is honored — verify this holds for the
success case when implementing.

### Key technical insight (makes the fix small)

In RELION-tomo, `optimisation_set.star` is the **single currency** for both steps — all three of
`subtomoExtraction`, `reconstructParticle`, and `class3d` declare `INPUT = OPTIMISATION_SET_STAR`. The
only difference is the optset's *state*: **picks** (→ extract) vs **already-extracted** (→ reconstruct).
For the priority use case the merged optset is already extracted, so it feeds ReconstructParticle
**directly** — no extraction step. (Aside: `SubtomoExtractionParams` still carries latent
`additional_sources` + `merge_only` fields — `merge_only=True` is the legacy "already extracted, just
concatenate" mode; superseded by the standalone merge card writing `MergedSources/<slug>/`.)

### ★ Keystone bug (VERIFIED firsthand) — the merged optset is not a real producer

`services/path_resolution_service.py:523-547` `_add_merged_sources_candidates` registers a synthetic
`OPTIMISATION_SET_STAR` producer **only if `project_root/MergedSources/optimisation_set.star` exists**
(the LEGACY *flat* path). But every merge since the slug refactor writes
`MergedSources/<slug>/optimisation_set.star` (`project_state.py:490`, `MERGED_DIR_NAME`), and
`active_merged_optset()` (`ui/aggregation_merge_card.py:77`) resolves the slug path. So for **every
current merge the flat file doesn't exist → the candidate is never registered → consumers never see
"Merged Sources" in their input dropdown** and rely entirely on the invisible `manual:<path>` override
written by `apply_aggregation_overrides` (rendered as an anonymous "Manual path…" in io_config).

**Fix (the keystone — do this first):** point `_add_merged_sources_candidates` at the slug-resolved
**active** merged optset and give it a meaningful label ("Merged sources — <name>"). This requires
moving the active-merge resolution (`_active_merge` / `active_merged_optset`,
`ui/aggregation_merge_card.py:64-89`) OUT of the UI layer into `ProjectState` (or a service) so
`path_resolution_service` can call it without a `ui.` import — a clean relocation that also serves
PARTICLE_PROJECT_ROADMAP P3. Once the merged optset is a first-class resolver candidate, a
ReconstructParticle job lists it like any normal producer; the `apply_aggregation_overrides` band-aid
can wire via that candidate's `source_key` instead of a bare `manual:` path (or be retired).

### Why the flow is shitty (concrete, ranked for the priority use case)

1. **[keystone, above]** merged optset isn't a selectable producer → anonymous "Manual path…" wiring.
2. **No "now reconstruct" step.** After merging in the dialog you're dropped back to a roster full of
   ~8 inapplicable preprocessing rows with no indication to add ReconstructParticle and no link. Merge
   and its consumer live on disjoint surfaces.
3. **No state surfacing.** Nothing tells you the sources/merge are already-extracted vs picks, so you
   can't tell you should be reconstructing (vs extracting).
4. **Stale "merged" badge + toast-only feedback.** The purple sidebar dot only updates on full reload
   (`pipeline_roster.py:1595`, read once off the render path); merge success/failure is a transient
   toast with no BackgroundTask tray entry (the *dead* `merge_panel_component.py` is the one that used
   BackgroundTask — irony).
5. **Discoverability.** The entire feature hides behind one unlabeled 30px purple sidebar icon; the
   dialog never auto-opens; landing hint ("merge panel") and the card's own docstring ("lives at the top
   of the workspace") are both stale lies — it's a dialog behind an icon.

### Real correctness bugs found (not just polish)

- **Cross-tab state leak:** `_DIALOG_REFS`, `_registry_expanded`, `_pending_save_task` are **module
  globals** in `ui/aggregation_merge_card.py` (761, 955, 105) — shared across all NiceGUI clients/tabs;
  opening the dialog in tab B corrupts tab A's refs (same class of bug as
  [[feedback_background_task_no_client_context]]). Move to per-dialog/per-client state.
- **No SingleFlight** on the open handler (`pipeline_roster.py:1604`) or the async expanders
  (`aggregation_merge_card.py:472/522`) → stacked dialogs + racing curation loads (CLAUDE.md requires it).
- **Non-reactive UI:** the selector tree, footer, and registry all `container.clear()`+full-rebuild on
  every click (no `FingerprintedView`), dropping in-flight clicks + :hover. Event-driven (not
  timer-driven) so lower severity, but violates the documented convention.

### Dead / debug code to remove

- **`ui/pipeline_builder/merge_panel_component.py` (24KB) is fully built but UNREACHABLE** — zero runtime
  callers (grep-confirmed); only a comment ref in `ui/job_plugins/subtomo_extraction.py:7`. Reimplements
  the merge call + a source-picker dialog. Delete it (also on the P5 removal list).
- `services/aggregation_authoritative.py` (+ `backend.py:296-533` gate report) is **built-but-unwired**
  (zero `ui/` consumers) — the cleaner per-species authoritative-list→optset model; wire into P3 or delete.
- `drivers/subtomo_merge.py:415` ships a `[MERGE DEBUG]` print + `# <-- add this` editing note; lines
  397/491/524/581-583 use bare `print()` instead of logging.

### The plan (sequenced; ①–③ = the priority path, ④ deprioritized)

- **① Keystone:** relocate active-merge resolution to `ProjectState`; fix `_add_merged_sources_candidates`
  to surface the slug-resolved active merged optset as a real, labeled producer. *Unblocks everything.*
- **② One-flow next step:** after a merge, detect optset state (picks vs extracted — read `particles.star`
  for extracted image stacks / `rlnImageName`) and surface the right one-click action — **"Reconstruct
  particles"** (priority) or "Extract subtomos" — that adds + wires the consumer job.
- **③ Surface state + counts** on each merge record; block mixing picks + extracted in one merge.
- **④ (deprioritized)** extend `_scan_project` to also discover pre-extraction (candidate/manual-pick)
  optsets so the "aggregate → extract" path has sources. Not needed for the 10-project reconstruct case.

Cheap wins that ride along: delete `merge_panel_component.py`; strip the debug prints; SingleFlight on the
open path; fix the cross-tab module-global leak; route the merge through BackgroundTask; live "merged" badge.

### Roadmap tension (don't sink effort into doomed chrome)

PARTICLE_PROJECT_ROADMAP **P3** ("Aggregate provider → OPTIMISATION_SET_STAR") + **P5** ("kill
`is_aggregation`") already plan to replace the `is_aggregation` project type with a data-less regular
project + an inline `AGGREGATE_PARTICLES` provider job whose output the **normal resolver indexes**. The
keystone ① is *exactly* that ("normal resolver indexes the merged optset") and the merge engine/discovery/
selector tree are all P3-reusable — so ①–③ are a **down payment on P3, not throwaway**. AVOID investing in
`is_aggregation`-gated chrome (roster filtering for the legacy mode, the sidebar button, the two-toggle
disambiguation) that P5 deletes.

### Key file anchors (for fast pickup)

- `services/path_resolution_service.py:523-547` — `_add_merged_sources_candidates` (the keystone bug; flat path).
- `ui/aggregation_merge_card.py:64-89` — `_active_merge`/`active_merged_optset` (relocate to ProjectState); `:134-198` `apply_aggregation_overrides`; `:306-673` `_MergeSelector` (P3 reuses); `:680-758` `open_aggregation_merge_dialog`; `:908-948` `_run_merge`; `:761` `_DIALOG_REFS` global.
- `services/jobs/reconstruct_particle.py:51-55` — `INPUT = OPTIMISATION_SET_STAR` (preferred_source `subtomoExtraction`); `services/jobs/subtomo_extraction.py:48-82` (`additional_sources`/`merge_only`); `services/jobs/class3d.py:47`.
- `services/aggregation_discovery.py:74-179` — `_scan_project` (extracted-only) + `discover_subtomo_optimisation_sets`; `load_tomo_curation`.
- `drivers/subtomo_merge.py:348-585` — `merge_optimisation_sets_into_jobdir` (concat + dedup + strict optics check; debug prints).
- `ui/pipeline_builder/pipeline_roster.py:1589-1617` — sidebar merge button (stale badge); `pipeline_builder_panel.py:335-340,594-603` — override self-heal call sites.
- `ui/pipeline_builder/merge_panel_component.py` — DEAD, delete.
