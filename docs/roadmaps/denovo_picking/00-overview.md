# De-novo species picking — scope, decisions, risks

Scoped 2026-08-11. Supersedes the live content of `notes/PARTICLE_PROJECT_ROADMAP.md` (see
"Supersession map" at the bottom; that doc's stage log is retained as history). Feature brief:
`FEATURE_de_novo_picking.md`.

## The feature

Let a user create a particle species **de novo** — no template, no template-matching (TM) job,
possibly zero parameters — and immediately pick particles manually in ChimeraX/ArtiaX per
tomogram, with the full dashboard experience (overlays, cutouts, keep/discard, merge, per-list
extraction) and downstream consumption (→ reconstructParticle → class3d). Tomograms may come from
the normal preprocessing pipeline **or** be user-imported (already reconstructed); picking must not
care which.

Bundled into the same arc because they share the seams:

- tomo import becomes a real, hardened UX feature (today it is a dead end);
- the "particle project" / "aggregation project" special types dissolve into regular projects;
- the aggregation tool is **kept** and factored into its own module.

## Why this is smaller than it looks

The manual-picking machinery is **already template-free**:

- coordinate math: `services/visualization/coords.py` — `pixel_size = rlnTomoTiltSeriesPixelSize ×
  rlnTomoTomogramBinning`, dims from the recon MRC header, exact centered-Å↔voxel round-trip;
- recon-sourced cutout atlas: `services/visualization/recon_cutouts.py` (recon MRC + voxel coords +
  box, no scores, no manifest);
- keep/discard, merge, clash dedup, `.coords` ingest → `manual.star`
  (`services/visualization/artiax_bridge.py:103`);
- `frame_for_tomo(tomograms_star, …)` accepts **any** tomograms.star — the imported star is already
  geometrically compatible with the bridge;
- a label-only `ParticleSpecies` is structurally legal today (`services/project_state.py:187` —
  only `id` + `name` required);
- `<project>/Curation/<species_id>/<tomo>/` (`artiax_bridge.curation_dir`, :158) is already the
  project-level, job-independent home for bundles, saves, and lists.

The blockage is **enumeration and input sourcing**, concentrated in a handful of sites:

1. `_collect_species_data_for_ts` (`ui/tomo_dashboard_dialog.py:3411`) iterates
   `candidate_extract_instances(project_state)` (`services/dashboard_data.py:79`). The entire
   species-facing dashboard — Particles panel, canvas, pick-list rail, ArtiaX buttons, `.coords`
   auto-ingest, per-list extraction — hangs off this list. Same pattern in
   `collect_species_journey` (`services/dashboard_data.py:566`) → journey-strip species rows.
2. The ArtiaX bundle is raw path arithmetic on the candidate-extract (CE) job dir:
   `job_dir/"candidates.star"` + `job_dir/"tomograms.star"` (`ui/tomo_dashboard_dialog.py:3895`).
3. Per-list extraction hard-requires `<ce_job>/optimisation_set.star` (UI gate
   `ui/tomo_dashboard_dialog.py:2983`; driver `--candidate-optset` required,
   `drivers/extract_pick_list.py:110`). `build_list_optset`
   (`services/visualization/list_extraction.py:127`) uses it only for (a) the particles.star column
   schema + `data_optics` block, (b) the tomograms.star reference — both synthesizable.
4. The resolver never consumes `PickList.extracted_path` — manual-list extractions are invisible to
   downstream jobs.

## Target architecture

```
create species (roster PARTICLES header / Particles empty-state / workbench "+")
        │  ParticleSpecies{id, name, color(palette), origin="manual", extraction_params=None}
        ▼
Particles section = iterate species_registry                    ← THE INVERSION
        │  per species: enrichment = CE instance when resolve_species matches, else None
        │  iid = CE iid, or synthetic f"pick__{species_id}"     (key only — no roster row)
        ▼
TomoGeometry provider (new: services/visualization/tomo_geometry.py)
        │  geometry_for_ts(state, project_path, ts) → TomoGeometry
        │  source: TS_RECONSTRUCT job's tomograms.star  OR  <project>/Tomograms/tomograms.star
        ▼
ArtiaX bundle → Curation/<sid>/<tomo>/open.cxc      (candidates_star now Optional — zero ref picks)
        ▼
save .coords → auto-ingest → manual.star PickList → curate / keep-discard / merge   (unchanged)
        ▼
"Extract this list" → params dialog (box/bin/crop REQUIRED when unknown; persisted on species)
        │  build_list_optset_from_tomograms(...)                 candidate-free (old D3)
        │  drivers/extract_pick_list --tomograms-star            (alt to --candidate-optset)
        ▼
PickList.extracted_path → resolver injects OPTIMISATION_SET_STAR candidate
        │  producer_instance_id = f"pick_list__{slug}", species_id set, no afterok edge
        ▼
reconstructParticle / class3d resolve input_optimisation normally
```

For a manual-only species the roster chain **starts at reconstructParticle** — subtomo extraction
runs via the per-list backend job (`backend.extract_pick_list_and_wait`, `backend.py:326`), which
already performs real `relion_tomo_subtomo` extraction and records staleness. This is deliberate
(decision D-7): a subtomoExtraction roster row would either re-run the extraction or be a phantom.

### Seam specs

**`services/visualization/tomo_geometry.py`** (new, ~120 lines):

```python
@dataclass(frozen=True)
class TomoGeometry:
    tomo_name: str
    tomograms_star: Path          # a star frame_for_tomo will accept
    source: str                   # "reconstruct" | "imported"
    recon_mrc: Path | None
    dims_xyz_px: tuple[int, int, int] | None   # binned voxels
    binned_apix: float | None     # None ⇒ UNKNOWN, surfaced red — never 1.0
    apix_provenance: str          # "star" | "mrc_header" | "missing"

def geometry_for_ts(project_state, project_path, ts_name) -> TomoGeometry | None: ...
def tomogram_star_sources(project_state, project_path) -> list[tuple[str, Path]]: ...
```

Dims/apix precedence: tomograms.star row → recon MRC header (dims always; voxel_size only if > 0)
→ `None` + `apix_provenance="missing"`. Centralizes what today lives in the coords math, the
manifest-entry read (`ui/tomo_dashboard_dialog.py:3466`), and the imported-fallback's inline star
arithmetic (:3595-3619); subsumes `_resolve_recon_mrc_for_ts`'s fallback chain (:3271).

**The inversion, with a parity contract.** `_collect_species_data_for_ts` iterates
`project_state.species_registry`; a new `ce_instance_for_species(state, species_id)` helper in
`services/dashboard_data.py` inverts the existing `resolve_species` chain
(`services/models_base.py:152`) over `candidate_extract_instances`. Parity rule: **one entry per CE
instance exactly as today; one extra entry only for species with zero CE instances.** CE-rich
projects therefore execute the identical code path with identical inputs — the only visible diff is
color (index-keyed `SPECIES_OVERLAY_COLORS[idx]` → persisted `species.color`), which fixes the
positional color-flip bug. Same inversion in `collect_species_journey`.

**ArtiaX seam.** `prepare_curation_bundle` (`services/visualization/artiax_bridge.py:258`,
`backend.py:684`, `services/curation/session_service.py:708`) gets `candidates_star: Path | None`;
when None, skip `export_tomo_picks_to_coords` and write the cxc with zero reference picks.
Dashboard call sites source `tomograms_star` from `TomoGeometry`, not the CE job dir; `.coords`
auto-ingest keys off `curation_dir(...)`.

**Candidate-free extraction.** Factor the row-synthesis core out of `build_list_optset` and add
`build_list_optset_from_tomograms(list_star, tomo_name, out_dir, tomograms_star)`, synthesizing
`data_optics` from the tomograms.star `data_global` (rlnVoltage / Cs / AmpContrast /
TiltSeriesPixelSize — present in both RELION-produced and imported stars). Driver:
`--candidate-optset` | `--tomograms-star` mutually-exclusive required group.

**Resolver.** `_add_pick_list_optset_candidates(index)` as a third injection sibling next to
`_add_merged_sources_candidates` (`services/path_resolution_service.py:579`) and
`_add_imported_tomograms_candidates` (:612): for each `PickList` with
`extraction_state() == EXTRACTED`, inject an `OPTIMISATION_SET_STAR` candidate,
`producer_instance_id=f"pick_list__{pl.slug}"`, `species_id=pl.species_id`,
`execution_status=SUCCEEDED`. Extend the non-job afterok filter (:262-264) with
`producer_id.startswith("pick_list__")`. Authoritative slug consulted in the orchestrator submit
path to prefill `source_override` — the resolver stays dumb.

## Decision register

LOCKED = agreed with the user 2026-08-11. REC = recommendation, revisit only if it fights reality.

| # | Decision | Status |
|---|---|---|
| D-1 | Manual picking is a **dashboard-level activity**, not a roster job. Synthetic iid `pick__<species_id>` is an internal key only. Discoverability = "New species" on the PARTICLES phase header + Particles empty-state. Reversible: a roster row could later be pure UI over the same PickLists. | **LOCKED** |
| D-2 | Species color: palette color assigned **at creation, persisted on `species.color`**, rendered from the field everywhere; swatch editor in the workbench. Supersedes old D2 (hash-at-render) — a second implicit convention would fight the persisted field. | REC |
| D-3 | Extraction box/bin/crop for manual-only species: **required per-extract dialog**, prefilled from (species' subtomo job model → `species.extraction_params`), persisted to the species on confirm. Kills the silent `384/1.0/224` default (`ui/tomo_dashboard_dialog.py:2989-2998`) for the CE path too. | REC |
| D-4 | The CE-enumeration inversion happens **one-shot** (S2) under the parity contract; the imported-particles fallback renderer is deleted in S4. Growing the fallback instead = permanent double maintenance of a ~1500-line render pipeline. | REC |
| D-5 | Species creation entry points: roster PARTICLES header (primary), Particles-section empty state, workbench "+" (unchanged). Creation prompt = name only; color auto; everything else editable later. | REC |
| D-6 | `authoritative_pick_lists` / GateReport machinery (`services/aggregation_authoritative.py:39-326`, built-but-unwired): **keep**, relocate to `services/aggregation/`, wire GateReport as the merge pre-flight in S6. It is exactly the staleness machinery the merge card needs; deleting-and-rebuilding is waste. | REC |
| D-7 | Resolver injects the **extracted** optset (`pl.extracted_path`), not a coords-only optset. reconstructParticle/class3d consume directly; manual species skip the subtomoExtraction roster job entirely. (The old P2.3 text was ambiguous between the two stars — this resolves it.) | **LOCKED** |
| D-8 | Synthetic producer identity: keep string sentinels + the `pick_list__` prefix convention; add a dedicated sentinel for imported tomograms in S5 to end the `MERGED_SOURCES` masquerade. A "SyntheticProducer" abstraction for three sentinels is speculative. | REC |
| D-9 | Half-map import (denoise-on-imported): **out of scope**; separate follow-up. | REC |
| D-10 | Imported tomograms stay **star-only** in this roadmap; entity-registry entry deferred to the registry-consolidation follow-up (see "Registry interop" below). | **LOCKED** |

Old `notes/PARTICLE_PROJECT_ROADMAP.md` P2.0 decisions map: D1→D-1 (kept), D2→D-2 (revised),
D3→S3 (kept), D4→D-7 (clarified), D5 (TM-on-imported stays out of scope) → kept, tracked in
`04-tomo-import.md`.

## Registry interop — the "DataRegistry" question (D-10 context)

The TiltSeriesRegistry is mid-overhaul (`docs/registry-consolidation-roadmap.md`: Phase 1 backfill
code-complete, Phases 2–3 pending). Today `Tomogram` requires a parent `TiltSeries` with a required
`mdoc_path` (`services/tilt_series/models.py:220-229`), the registry is built only from mdocs at
project creation, and `services/tomogram_import.py` never touches it — so an imported tomogram has
no representable home. **In this roadmap we do not change that** (star-only, D-10).

The follow-up direction (owned by the consolidation roadmap, not this one): extend — and possibly
rename to something like **DataRegistry** — the entity layer so it tracks the natural progression
of cryo-ET data (frames → tilts → tilt-series → tomogram) and can be **initiated at any stage**,
including from bare tomograms with no tilt provenance.

Honest assessment, on request:

- **The core idea is sound because it is domain-true, not because it is elegant.** The lineage is
  real and singular; every job attaches outputs at exactly one level (the existing
  `outputs: dict[job_instance_id, TypedOutput]` pattern proves it). "Entry at any stage" is a real,
  recurring requirement — imported tomograms today; imported/aggregated particle sets are the same
  shape of problem. A parentless `Tomogram` is *truthful* provenance: it records what is known
  without fabricating what isn't, which is exactly the project's no-invented-data rule (contrast
  the rejected synthetic-parent-TS option, which fabricates entities).
- **The correct version is small.** Make `Tomogram` a possible persistence root with
  `tilt_series_id: str | None` + one ingest path from an import batch. It is *not* a generic
  lineage-graph store: the moment "DataRegistry" drifts toward string-keyed generic nodes it is a
  regression from the current typed Pydantic entities with `extra="forbid"`.
- **Particles stay out**, at least until consolidation Phases 2–3 prove the current model.
  Particles are structurally different (many per tomogram, per-species, mutable under curation) and
  already modeled by `PickList` + the species registry; the link is by `tomo_name`/`species_id`
  keys, which is fine.
- **The rename is cosmetic.** Do it when the schema change lands, not as its own event.

## Session index

Spine (the de-novo happy path): **S1 → S2 → S3**. S4/S5/S6 are independent trailers.

> **UI-pass entry point (recorded 2026-08-14):** the next session is the UI pass. Its FIRST slice
> is `../07-extract-pick-list-job-identity.md` (census #68 — per-list extraction becomes a real,
> UI-visible job; its §1 inventories exactly what the UI cannot show today, its S4 covers the two
> known wiring gaps: the hardcoded `subtomo_status: "pending"` for de-novo journey rows at
> `services/dashboard_data.py:832`, and the missing default radio in the rail's authoritative-list
> column). After 07, continue with S4 below. Prerequisite context: the driver refactor (roadmap 04)
> is code-complete as of 2026-08-14 — nothing on that front blocks or is owed by the UI work, but
> the 6 freshly migrated drivers + fail-loud registry stamps are PENDING RUNTIME, so run a sandbox
> chain before relying on pipeline runs during UI testing.

| Session | Doc | Goal | Depends on |
|---|---|---|---|
| S1 | `01-species-and-creation.md` | Species creation UX + registry hygiene + data-less project creation (kills `is_particle_only`) | — |
| S2 | `02-geometry-and-inversion.md` | TomoGeometry provider, Particles-panel inversion, ArtiaX decoupled from CE dirs — full manual pick loop on imported tomos | S1 |
| S3 | `03-extraction-and-resolver.md` | Candidate-free extraction + resolver wiring — chain reaches class3d | S2 |
| S4 | `02-geometry-and-inversion.md` §S4 | Journey unification: delete fallback renderer, strip signature, empty-state copy, spinner failure paths | S2 |
| S5 | `04-tomo-import.md` | Import hardening: producer sentinel, multi-import, formats, perf, runtime shake | — (reads benefit from S2) |
| S6 | `05-aggregation-module.md` | Aggregation factor-out + de-global + `is_aggregation` removal | — |
| 07 | `../07-extract-pick-list-job-identity.md` | extract_pick_list job identity + per-list status UI (census #68; closes #73) | S3 (spine landed) |

## Risk register

1. **apix/dims correctness with no manifest** (top correctness risk). Today dims fall back
   `picks_json → manifest entry → [1,1,1]` (`ui/tomo_dashboard_dialog.py:3466`) — for a no-CE
   species that would silently corrupt overlays. The provider is the only source in the no-CE path;
   `apix_provenance="missing"` renders a red chip with overlays **disabled**, never a guessed
   scale. Recon MRC with `voxel_size==0` gets the same treatment.
2. **`relion_tomo_subtomo` vs synthesized `data_optics`** — unverifiable in the sandbox; the first
   cluster run of S3 is the test. Fallback if RELION rejects it: mirror the optics block shape from
   a RELION-produced particles.star (the 412 projects have them).
3. **Override-key masquerade.** `override_key.startswith(f"{JobType.MERGED_SOURCES.value}:")`
   serves both mergedSources and importedTomograms today (`services/path_resolution_service.py:178,
   :400`); `pick_list__*` producers must not route through that branch. Audit all three override
   branches in S3; dedicated sentinel in S5.
4. **Inversion regression on CE-rich projects, no test suite.** Mitigation = the parity contract +
   S2's explicit regression checklist on a real TM project before S3 starts; the color change is
   the one expected visible diff.
5. **Multiple EXTRACTED lists per (species, tomo)** tie at `relion_job_number=0`. Tie-break =
   authoritative slug, else newest `extracted_at` — and it is **surfaced in the resolution
   report**, never silent.
6. **Stale-extraction consumption.** A re-curated list flips STALE after reconstructParticle was
   configured against it → resolver stops injecting → PathResolutionError at submit. Correct
   surface, but the message must say "list went stale — re-extract", not generic missing-input.
7. **`remove_species` dangling refs.** S1 purges pick_lists / authoritative keys /
   `job_model.species_id` / `source_overrides` targeting the species' `pick_list__*` slugs;
   re-check in S3 when those producers actually exist.
8. **Aggregation refactor regressions** (1007-line UI file, known cross-tab global-state bugs).
   Mitigation: two-commit discipline — move-only, then de-global; only the second changes behavior.
9. **Journey-signature growth → strip churn.** Include only counts + mtimes, never file contents;
   verify fingerprint cost on a ~100-TS project.
10. **Import dialog perf** ("laggy, terrible"): header probes off the event loop, batched;
    FingerprintedView on the preview table. Cross-import name collisions dedup with an explicit
    rename report — never a silent merge.
11. **First afterok run of the particle tail downstream of a non-job producer** — unexercised;
    S3's verification budget includes shake-out time.

## Supersession map

`notes/PARTICLE_PROJECT_ROADMAP.md` → banner "SUPERSEDED by docs/roadmaps/denovo_picking/", stage
log retained as history. Mapping: P2.0 D1–D5 → decision register above; P2.1 (imported-particles
fallback) → deleted in S4; P2.2 → S1+S2; P2.3 → S3; P1 residue + import runtime risks → S5
(`04-tomo-import.md`); P3 (Aggregate provider job) → dropped — the merge card moves to the
PARTICLES header in S6 instead; P4 per-slot acquire UX → out of scope here (revisit after S6);
P5 (`is_aggregation` removal) → S6. `docs/LIST_EXTRACTION_AND_AGGREGATION.md` stays as reference;
its §8.9 steps 4–7 are absorbed by `05-aggregation-module.md`.

## Stage log (append-only)

- 2026-08-11 — scoped; decisions D-1/D-7/D-10 locked with the user; doc set written. No code yet.
- 2026-08-13 — S1 + S2 committed. **S3 code-complete, pending runtime** (see `03-extraction-and-
  resolver.md` §"S3 CODE-COMPLETE"). The spine S1→S2→S3 now has no code left; what remains on it is
  cluster verification. Two register updates fall out of S3:
  - **D-8 amended.** The `pick_list__<slug>` convention in the register is unsafe as written — every
    hand-picked list is slugged `"manual"`, so slug alone aliases across species AND tomograms. The
    producer id is `pick_list__<species>__<tomo>__<slug>`; the risk-#3 override-key masquerade is
    closed by discriminating on instance path (`_synthetic_override_target`), so the dedicated
    imported-tomograms sentinel stays an S5 cleanup rather than a correctness fix.
  - **Risk #7 was live, not latent.** `remove_species`'s override purge matched by slug, so deleting
    one species would have purged another's `pick_list__*` overrides the moment those producers
    existed. Fixed to match the species-scoped prefix.
