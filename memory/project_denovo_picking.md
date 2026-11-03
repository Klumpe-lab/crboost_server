---
name: project-denovo-picking
description: "De-novo species manual-picking roadmap — docs/roadmaps/denovo_picking/, spine S1-S3 code-complete 2026-08-13 PENDING RUNTIME; UI pass RE-SEQUENCED 2026-08-16 behind the species-registry arc (roadmaps 08-12); decisions D-1/D-7/D-10 locked"
metadata: 
  node_type: memory
  type: project
  originSessionId: 688ee8bd-5a95-4a0e-a25c-aca9532f2a62
  modified: 2026-08-13T15:07:55.042Z
---

Scoped 2026-08-11 via 4-agent exploration + plan pass; doc set at `docs/roadmaps/denovo_picking/`
(00-overview + per-session 01-05). Supersedes `notes/PARTICLE_PROJECT_ROADMAP.md` (banner added).

Locked with user: **D-1** manual picking = dashboard activity, synthetic iid `pick__<species_id>`
key-only, NO roster row; **D-7** resolver injects the *extracted* optset (`PickList.extracted_path`,
producer `pick_list__<slug>`) — manual species' roster chain starts at reconstructParticle, no
subtomoExtraction row; **D-10** imported tomograms stay star-only this roadmap; entity-registry
entry deferred to registry consolidation (interop note in registry-consolidation-roadmap.md:
future = standalone Tomogram w/ optional tilt_series_id, maybe rename "DataRegistry", particles
stay out — honest assessment recorded in 00-overview §Registry interop).

Spine S1→S2→S3, trailers S4 (journey unification) / S5 (import hardening) / S6 (aggregation
factor-out + kill is_aggregation, only 3 gate sites). #1 runtime risk = relion_tomo_subtomo
accepting synthesized data_optics (first S3 cluster run tests it).

**S1 LANDED + COMMITTED 2026-08-13, PENDING RUNTIME** (record appended to 01-species-and-creation.md).
- `SPECIES_OVERLAY_COLORS` moved dashboard_data → `services/models_base.py` (species color is now
  persisted model data); dashboard_data re-exports so UI import sites unchanged. New
  `species_palette_color(sid)` sums code points — NOT `hash()`, which PYTHONHASHSEED randomizes per
  process (species would recolor on every server restart).
- `ParticleSpecies.origin` ("workbench"|"manual"|"imported", "" = legacy→workbench) +
  `extraction_params: ExtractionParams|None` (box_size/binning/crop_size, **no defaults** — S3's
  dialog must ask). `add_species(name, *, origin=, color="")` auto-assigns a palette slot.
- NEW `ProjectState.species_references(sid)` feeds BOTH the delete dialog and `remove_species`, so
  they can't disagree. `remove_species` now purges pick_lists + authoritative_pick_lists +
  matching `source_overrides`; previously left dangling refs that only surfaced as deploy-time
  resolution failures.
- **OPEN DEVIATION**: jobs carrying `species_id` are LISTED in the confirm dialog but NOT
  auto-deleted (a job owns a job dir + default_pipeline.star row = pipeline op, not registry edit).
  Maintainer may want the full cascade — 3-line change if so.
- UI: "New species" btn on PARTICLES roster header (SingleFlight-guarded, `origin="manual"`, does
  NOT mkdir templates/<sid> unlike workbench "+"); palette swatch editor on workbench species
  header (repaints just the dot — rebuilding would kill the menu mid-click); softened species gate.
- `is_particle_only` DELETED (was a transient UI field duplicating the globs; creation mechanics
  were always glob-gated). Replaced by `is_dataless()` = both globs empty; create button reads
  "Ready to create — without raw data". Half-filled form still demands the other half. Also deleted
  the aggregation branch that mirrored a pre-init SubtomoExtraction into selected_jobs — verified
  dead in project_service.create_project ("Aggregation projects have no pipeline jobs at creation
  time"), it was reloading an EMPTY job list over the user's selections.

**Why:** S1 is the cheap unblock — a label-only species is structurally legal today, so the real
blockage is enumeration/input-sourcing (S2), not the model.
**How to apply:** NEXT = S2 (`02-geometry-and-inversion.md`, ~700 lines, the big one). Two halves:
new `services/visualization/tomo_geometry.py` provider (dims/apix precedence star→MRC header→None
with `apix_provenance="missing"`; **never 1.0, never [1,1,1]** — red chip + overlays disabled) and
the inversion of `_collect_species_data_for_ts` (ui/tomo_dashboard_dialog.py:3411) +
`collect_species_journey` (services/dashboard_data.py:566) off `candidate_extract_instances` onto
`species_registry`. Parity contract: CE-rich projects must execute the identical path with
identical inputs — regression-test a real TM project FIRST. Plus ArtiaX `candidates_star: Path|None`
threading. Related: [[project-artiax-bridge]], [[project-ts-registry-refactor]],
[[project_roadmap04_driver_consolidation]].

**S2 + S3 CODE-COMPLETE 2026-08-13, PENDING RUNTIME — the spine S1→S2→S3 has no code left.**
S3 record in `03-extraction-and-resolver.md` §"S3 CODE-COMPLETE"; stage log in 00-overview.

Landed in S3: `build_list_optset_from_tomograms` + a factored core (`_coords_for_tomo`,
`_synthesize_particle_rows`, `_write_list_optset`) shared with the mirroring builder;
`--candidate-optset | --tomograms-star` mutually-exclusive on the driver + backend; the
`384/1.0/224` silent default DEAD in both places via
`aggregation_authoritative.extraction_params_for_species` (subtomo jm → species.extraction_params →
**None**, i.e. ask) + a required dashboard dialog that persists to the species;
`_add_pick_list_optset_candidates` in the resolver + `is_synthetic_producer()` for the afterok filter
+ `_synthetic_override_target()` closing the MERGED_SOURCES override-key masquerade.

**TWO REGISTER CORRECTIONS worth carrying forward:**
1. **D-8's `pick_list__<slug>` is unsafe as written.** Every hand-picked list is created with the
   literal `slug="manual"` (ui/tomo_dashboard_dialog.py), and PickList.slug is unique only within a
   (species, tomo) — so slug alone aliases across BOTH. Producer id is now
   `pick_list__<species>__<tomo>__<slug>` (`path_resolution_service.pick_list_producer_id`).
2. **Risk #7 was a live bug, not latent.** S1's `remove_species` override purge matched
   `f"pick_list__{slug}" in value`, so deleting species A would have purged species B's overrides the
   instant those producers existed. Now matches `pick_list_producer_prefix_for_species()`.

Deliberate deviations: tie-break among multiple EXTRACTED lists spends `relion_job_number` (ranks
0..N-1 by is-authoritative then extracted_at; 0-based so a lone list still ties the other synthetic
producers) and surfaces in the candidate `label`, since ResolvedManifest has no notes channel;
**no orchestrator auto-prefill of source_override** (the tie-break already lands on the same list, and
an auto-written pin is a UX call belonging with the UI work — apply_aggregation_overrides is the
precedent and is UI-triggered, never orchestrator-triggered). Checked rather than assumed: CE-rich
projects are unaffected because both optset consumers declare
`preferred_source="subtomoExtraction"`, which outscores a pick list.

**How to apply:** user takes the de-novo UI/UX next. Runtime owed on S1+S2+S3 together — S2's
regression-on-a-real-TM-project check FIRST, then S3's 4-step chain (extract on an imported tomo →
reconstructParticle resolves `pick_list__*` with no phantom afterok → class3d → re-curate and see the
STALE re-extract message). Sandbox ceiling this session was ruff ONLY: `/software` unmounted ⇒ dead
venv python symlink AND no system python3, so py_compile + check_boundaries.py are owed.

**NEXT SESSION = THE UI PASS (recorded 2026-08-14).** Entry point pointer written into
00-overview.md §Session index. Order: ① roadmap 07 (docs/roadmaps/07-extract-pick-list-job-identity.md
— census #68, extract_pick_list becomes a real UI-visible job; its §1 = shadow-zone inventory, §3 =
stages S1-S4 incl. the two known wiring gaps: dashboard_data.py:832 hardcoded subtomo_status
"pending" for de-novo journey rows, and the missing pre-checked radio in the rail's
authoritative-list column) → ② denovo S4 trailer (delete fallback renderer, empty-state copy,
spinner failure paths). Driver refactor (roadmap 04) is CODE-COMPLETE and no longer competes for
sessions — but its 6 fresh migrations are PENDING RUNTIME, so sandbox-verify a chain before leaning
on pipeline runs during UI testing. Also still owed from S3: one cluster job testing whether
relion_tomo_subtomo accepts the synthesized data_optics.

**RE-SEQUENCED 2026-08-16.** The UI pass now starts with the species-registry arc
(`docs/roadmaps/08-species-reactive-spine.md` → 09 → 10 → 11 → 12; see [[project_species_registry_arc]]),
because S1's verification step 2 ("New species from the roster → appears in workbench") FAILS at
runtime: the workbench panel is build-once + CSS-only view switch + no registry input in any refresh
gate, and `add_species`/`remove_species` never `mark_dirty()` (species may not persist). Roadmap 07
keeps its content as the dependency for live per-list extraction status (11-S2); S4 (journey
unification) is absorbed by 11-S3; S5/S6 unchanged. No code landed 2026-08-16 (user switching
branches first).

**2026-08-16 (later): 08 S0+S1 CODE-COMPLETE on `denovo_picking`** (see [[project_species_registry_arc]]);
pending user commit + runtime checklist 08 §6, then 09.
