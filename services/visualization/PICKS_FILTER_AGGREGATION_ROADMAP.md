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
