# Pick-List Extraction & Cross-Project Aggregation

**Status:** Design + reference. Sections 1–6 describe what exists today. Sections 7–9 are the gap, the design for seamless aggregation, and open questions.
**Audience:** crboost_server maintainer.
**Scope:** how a curator's per-tomo pick choices become a subtomo-extracted `optimisation_set.star`, and how those should roll up — across many tomograms *and* projects — into a single optimisation set that feeds 3D classification, without the user hand-tracking which lists are extracted or stale.

---

## 1. Purpose & the goal

The curator works one `(species, tomo)` at a time in the dashboard workbench: they look at candidate picks, drop the junk, merge a manual list with the auto candidates, and eventually decide *"for this tomogram, **this** list is the one I trust."* Today that decision (`set_authoritative_slug`) is recorded but goes nowhere.

The goal, **in the user's terms:**

> "I've picked the right list for each tomogram across all my projects. Roll those up into one optimisation set and feed it to 3D classification. Don't make me remember which lists I extracted, which went stale after I re-curated them, or which projects have compatible box/apix. Just tell me what (if anything) needs re-cutting, do it, and forward the result."

Concretely:

1. Enumerate every contributing `(project, species, tomo)` and resolve the **one authoritative list** per cell.
2. Ensure each authoritative list is **extracted and current** — auto-extract the missing/stale ones, or present a single explicit gate. **Never silently ship un-extracted or stale picks.**
3. Merge the chosen lists' per-list extractions into **one species-level `optimisation_set(_filtered).star`**.
4. Forward that to `Refine3D`/`Class3D` through the existing IO-slot mechanism, with no manual path-pointing.

The user should think about **which list per tomo**, not about **extraction state**.

---

## 2. The pick-list model

### List types (`services/models_base.py:PickListType`)

`auto / filtered / manual / imported / merged`. Two of these are **never persisted** — they are synthesised at render time so the registry can't drift from the resolver's files:

| Type | Persisted? | Backing artifact |
|---|---|---|
| `auto` | **No** — synthesised | PyTOM `candidates.star` (from `picks.json` / `sp['picks']`); slug literal `'auto'` |
| `filtered` | **No** — synthesised | `<slug>_filtered.star` (per list) or the auto job's `particles_filtered.star` |
| `manual` | **Yes** (`ProjectState.pick_lists`) | `.coords`-derived RELION star in the curation dir |
| `imported` | **Yes** | imported star/coords |
| `merged` | **Yes** | `merged__<name>.star` (coords-only union) |

`_collect_pick_lists_for_species` (`ui/tomo_dashboard_dialog.py:2691`) is the *only* place auto/filtered are materialised: it prepends the `'auto'` list from `sp['picks']`, then iterates `get_pick_lists()` for the persisted ones.

### The `PickList` model (`services/project_state.py:487`)

Fields of note (slug is unique within `species_id + tomo_name`):

- `slug, label, list_type, species_id, tomo_name, path, count, color, visible`
- `parent_slugs` — provenance for merged lists
- `filtered_count: Optional[int]` — kept count after keep/drop (`None` = no filter committed)
- `extracted_path / extracted_count / extracted_at` — the per-list extraction handle

### Per-`(species, tomo)` scoping & authoritative selection

Lists are keyed by `(slug, species_id, tomo_name)`. Registry methods: `get_pick_lists / get_pick_list / add_pick_list` (upsert — removes the same-key entry first, then `mark_dirty`) / `remove_pick_list`.

The **authoritative** list per cell lives in:

```
ProjectState.authoritative_pick_lists: Dict[str, str]
    key = f"{species_id}\x1f{tomo_name}"   # _auth_key
    val = slug                             # default 'auto'
```

`get_authoritative_slug(species_id, tomo_name)` → stored slug or `'auto'`. `set_authoritative_slug(...)` writes the slug (storing `'auto'` explicitly so a switch-back persists). The single-value-per-key dict **structurally enforces exactly one authoritative list per `(species, tomo)`**.

> **Today this dict is read only by the dashboard UI.** No downstream/aggregation code path consults it.

---

## 3. Curation → filtered subset

The curator's keep/drop produces a curated subset that *both* extraction and merge consume.

- `picks_filter.filtered_list_path(source)` → `<stem>_filtered.star` beside the source (`manual.star` → `manual_filtered.star`, `merged__<name>.star` → `merged__<name>_filtered.star`).
- `picks_filter.save_filtered_list(source, dropped_indices)` writes **source rows MINUS dropped 0-based row indices**, preserving block structure. Every non-dropped row is kept (including picks with no gallery tile). `derive_keep_state_for_list` re-derives kept indices by centered-Å coord match, robust to row reorder.

**"Kept" = source rows that survived keep/drop.** For scoreless manual/merged lists there's no threshold — keep/discard is the only operation.

### The `filtered_count` cache (and the bug just fixed)

`PickList.filtered_count` caches the kept count. `extraction_state()` (§5) uses `expected_count = filtered_count if not None else count`. The cache only self-heals when the dashboard renders:

`_collect_pick_lists_for_species` (`ui/tomo_dashboard_dialog.py:2741–2756`) re-derives `filtered_count` **live** from `<slug>_filtered.star` via `_memoized_keep_state(Path(pl.path))`, and when it differs from the stored value writes it back **and `mark_dirty()`s**:

```python
if pl.filtered_count != filtered_count:
    pl.filtered_count = filtered_count
    project_state.mark_dirty()
```

**The bug:** a list filtered in a *prior* session had `filtered_count = None` on disk. A correct `4-of-7` extraction (`extracted_count = 4`) then read **STALE**, because `expected_count` fell back to `count = 7`. The fix syncs the cache to disk truth at render so `extraction_state()` compares against the same kept count the table shows.

> **Carry forward:** this fix is **render-path only**. A headless/cross-project read that never renders this panel sees the on-disk `filtered_count` (possibly `None`) and can still mis-report STALE. This matters for §8.

---

## 4. Merging + dedup

For a single `(species, tomo)`, `pick_merge.merge_lists_to_star` unions 2+ lists' centered-Å coordinates into one `merged__<name>.star`:

- `sources = [{path, priority}]`, sorted ascending by priority (lower = higher priority = earlier rows). Priority comes from `_TYPE_PRIORITY` = `{merged:0, manual:1, imported:2, filtered:3, auto:4}`. **Row order is the only thing encoding "manual beats auto."**
- `np.concatenate`, write one `data_particles` block = `rlnTomoName` + `CENTERED_COLS`. **No merge-time dedup.**
- Inputs are the *kept* subsets: `list_actions.merge_source_for` (was the Journey's `_source_for`, moved in roadmap 11-S1) sends `<slug>_filtered.star` / `particles_filtered.star` when a filter is committed, else the base star.

Backend: `merge_pick_lists` (`backend.py:1095`) resolves `out_star = artiax_bridge.curation_dir(...)/<safe_slug>.star`, maps `type → priority`, runs off-loop. `ui/particles/list_actions.merge_lists` then registers the `PickList(list_type=MERGED, parent_slugs=[chosen slugs], ...)` and persists it.

**Optional radius dedup** (`list_actions.open_dedup_dialog`, only for `MERGED` lists): `clash_stats_star` previews `{n_total, n_clashing, n_removed, n_after}` at a radius (default `particle_diameter_ang/2` — from the SPECIES since roadmap 11-S2, a de-novo species having no candidate-extract job — else 100 Å); `deduplicate_star` greedy-keeps earlier (higher-priority) rows in place. `deduplicate_pick_list` updates `PickList.count = n_after`, persists and bumps the registry rev — **the dialog's tooltip explicitly says "Rewrites this merged list — re-extract after."**

> **UI location (roadmap 11, 2026-08-17):** merge, dedup, per-list extract, delete, import-by-path and the authoritative choice all live on the **Species page's Picks tab** (`ui/species/picks_tab.py`), across every tomogram at once. The Journey is look-and-curate: canvas, galleries, keep/drop, and one ⚡ into ArtiaX. Anything below that names a Journey handler for these actions (`_do_inline_merge`, `_render_clash_panel`, `_render_list_extraction_bar`, `_handle_import_curation_picks`) is describing code deleted in 11-S3.

**Provenance** = `parent_slugs` only (contributing slugs). The source *types/paths/priorities*, the dedup radius, and whether inputs were the filtered subsets are **not** stored.

> A merged list is **coordinates-only** (`rlnTomoName` + 3 centered-Å cols — no orientations, no scores). Merging does **not** extract; the merged list still requires per-list subtomo extraction (§5) before it can feed refinement.

---

## 5. Per-list subtomo extraction (Slice C)

Turns ONE pick list for ONE `(species, tomo)` into a subtomo-extracted optimisation set, schema-compatible with the species' auto candidate-extract so downstream can mix them.

### Input → output

1. **Choose the consumed star** (`_handle_extract_list`, `ui/tomo_dashboard_dialog.py:2302`): prefer `<stem>_filtered.star` if it exists, else the base list star.
2. **Build the per-list input optset** (`list_extraction.build_list_optset(candidate_optset, list_star, tomo_name, out_dir)`):
   - Read the species candidate `candidates.star` schema via the candidate optimisation set (`_parse_optimisation_set` resolves `rlnTomoParticlesFile` / `rlnTomoTomogramsFile`).
   - Synthesise **one particle row per list coordinate** filtered by `rlnTomoName`: **zero all template columns (orientations included)**, set `rlnTomoName`, copy the three `rlnCenteredCoordinate{X,Y,Z}Angst` (`CENTERED_COLS`), carry `rlnOpticsGroup` from the matching/first candidate row, set `rlnTomoParticleName = "<tomo>/<i+1>"`, preserve `data_optics` verbatim.
   - Write `particles.star` + a key-value `optimisation_set.star` (`# version 50001`, `_rlnTomoParticlesFile` + `_rlnTomoTomogramsFile` → the **shared** candidate `tomograms.star`). Raises if the list has no picks for the tomo or lacks coord columns.
3. **Run** (`drivers/extract_pick_list.py`): `relion_tomo_subtomo` into `<slug>/out/`, mirroring `drivers/subtomo_extraction.py` (box/bin/crop/max_dose/min_frames/stack2d/float16), container-wrapped via `get_container_service().wrap_command_for_tool('relion')`. Box/bin/crop come from `aggregation.authoritative.extraction_params_for_species` — the species' `SubtomoExtractionParams` job model, else `species.extraction_params`, else the extract dialog **asks** (denovo S3 / D-3). The old `box=384/bin=1.0/crop=224` `getattr` defaults are gone.
4. **Finalize:** `write_extracted_optset(out_run_dir, tomograms_star)` writes the FINAL `optimisation_set.star` → `out/particles.star` + shared `tomograms.star`; `result.json = {ok, optimisation_set, particles, count}`.
5. **Record:** `PickList.mark_extracted(optset, n)` sets `extracted_path / extracted_count / extracted_at = now()`; caller persists by explicit `project_path` (the watcher is a `BackgroundTask` with no client context → `state_for(project_path)` + `save_project(project_path=, force=True)`).

### Output dir layout

```
Curation/<species_slug>/<tomo_slug>/<slug>/
    particles.star                  # input prep (candidate schema, zeroed orientations)
    optimisation_set.star           # input prep
    out/
        particles.star
        Subtomograms/
        optimisation_set.star       # FINAL  ←  PickList.extracted_path
    result.json
    run.out / run.err / run_extract.sh / RELION_JOB_EXIT_{SUCCESS,FAILURE}
```

`curation_dir` = `artiax_bridge.curation_dir(project, tomo, species_id, species_label)` (slugs via `_safe_slug`).

### Staleness model (`PickList.extraction_state`, derived — never stored)

`services/models_base.py:ListExtractionState` = `NOT_EXTRACTED / EXTRACTED / STALE`. Derivation (`services/project_state.py:529`):

- **NOT_EXTRACTED** — `extracted_path` empty or file gone.
- **STALE** — `extracted_count != expected_count` (`expected_count = filtered_count if not None else count`), **OR** the consumed star (`<stem>_filtered.star` if it exists, else base `path`) has `mtime > extracted_at + 1.0s`.
- **EXTRACTED** — otherwise.

### Re-extract re-cuts

`backend.extract_pick_list` (`backend.py:193`) makes Re-extract actually re-cut: `out_dir = Path(list_star).parent / slug`; it **`shutil.rmtree(out_dir / "out")`** on submit (so the driver's idempotency skip — `out/particles.star` + `out/Subtomograms/` exist → skip — doesn't short-circuit) and unlinks `RELION_JOB_EXIT_*` / `result.json`. It substitutes `slurm_defaults` into `config/qsub.sh` (`XXXextra1..8XXX`, `XXXcommandXXX`), writes `run_extract.sh`, `sbatch`. Returns `{success, slurm_job_id, out_dir}`.

---

## 6. Forwarding to refinement (today)

### The `prefer_if_exists` slot contract

`SubtomoExtractionParams` (`services/jobs/subtomo_extraction.py`) declares **job-level** filtered outputs with `prefer_if_exists=True`:

- `output_optimisation_filtered` = `optimisation_set_filtered.star`
- `output_particles_filtered` = `particles_filtered.star`

`picks_filter.resolve_canonical_optset(subtomo_job_dir)` is the single definition of "canonical": returns `optimisation_set_filtered.star` if it exists, else `optimisation_set.star`. In the resolver, `_build_output_index` skips `prefer_if_exists` candidates whose file is absent (existence is the entire signal), and `_choose_candidate_for_slot` scores `(species_match, pref, filtered_pref, relion_job_number, succeeded)` so a curated filtered optset **beats the raw one from the same producer**. This is the *only* seamless forwarding that exists today — and only at **whole-subtomo-job** granularity.

`save_filtered_picks_for_ts` writes the job-level `particles_filtered.star` + `optimisation_set_filtered.star` (textually swapping only the `_rlnTomoParticlesFile` value) plus a `curation_reviewed.json` sidecar. **Note this is a *different* filtered contract from `save_filtered_list` (§3):** the former is one filter spanning all TS of one subtomo job; the latter is per-list. Only the former is wired to the resolver/merge.

### Cross-project merge engine

`services/subtomo_merge.merge_optimisation_sets_into_jobdir(*, job_dir, additional_sources, strict=True, allow_no_primary=False)` (moved out of `drivers/` in roadmap 04 stage 5):

- Parses each source optset → `(particles.star, tomograms.star)` (`_parse_optimisation_set`), applies per-tomo curated/original override + tomo include-filter (`_normalize_source` accepts `{path, tomos, original_path, original_tomos}`).
- Concatenates optics/particles, **dedups tomograms by `rlnTomoName`** — **HARD-RAISES `"Tomogram name conflict"`** when the same `rlnTomoName` maps to different `rlnTomoReconstructedTomogram` paths.
- Compatibility: **hard-blocks** on `CRITICAL_OPTICS_COLS = [rlnVoltage, rlnSphericalAberration, rlnAmplitudeContrast, rlnTomoTiltSeriesPixelSize]` exact-string mismatch; only **`[MERGE WARN]`** + keeps primary value for `OPTIONAL_OPTICS_COLS = [rlnImageDimensionality, rlnTomoSubtomogramBinning, rlnImagePixelSize, rlnImageSize]`.
- Writes merged `particles/tomograms/optimisation_set.star` (`write_optimisation_set`, absolute paths) + `merge_summary.json`. `allow_no_primary=True` is the pure cross-project mode.

### The `agg_*` / `additional_sources` / `merge_only` pattern

Two ways to fuse across projects today:

1. **Aggregation project** (`is_aggregation`): `ui/aggregation/merge_card.py:_build_merge_sources` turns `AggregationSource` entries into driver source dicts — **base path = `resolve_canonical_optset(optset.parent)`** (filtered-if-present), `original_path` + `original_tomos` for per-tomo pins. `_run_merge` writes `MergedSources/<slug>/`, records `AggregationMerge`, sets `active_merge_slug`. `apply_aggregation_overrides` writes both `source_overrides[slot] = "manual:<optset>"` *and* `paths[slot] = <optset>` on every consumer whose INPUT_SCHEMA accepts `OPTIMISATION_SET_STAR`.
2. **`merge_only=True` + `additional_sources`** job: `drivers/subtomo_extraction.run_supervisor_mode` short-circuits extraction and only runs the merge in-place (`allow_no_primary = not has_primary`).

`AggregationSource` carries `optset_path, tomo_names, original_tomos, project_path, species_id, species_label`. **Sources are keyed on a whole `SubtomoExtraction` job's `optimisation_set.star`** — discovered by `aggregation.discovery.discover_subtomo_optimisation_sets` (walks `SUBTOMO_EXTRACTION` job dirs) with kept counts from `load_tomo_curation` reading `particles_filtered.star`. **It never reads `PickList.extracted_path` or `authoritative_pick_lists`.**

### What Class3D / ReconstructParticle consume

- `ReconstructParticleParams` / `drivers/reconstruct_particle.py`: ONE `input_optimisation` slot (`preferred_source='subtomoExtraction'`), runs `relion_tomo_reconstruct_particle --i <optset> --b box --crop crop --bin binning --sym` → `merged.mrc` (the `REFERENCE_MAP`). Box/bin are RP **user params**, independent of the extraction box/bin (silent hazard).
- `Class3DParams` / `drivers/class3d.py`: `input_optimisation` (`OPTIMISATION_SET_STAR`, `preferred_source='subtomoExtraction'`) + `input_reference` (`REFERENCE_MAP`, `preferred_source='reconstructParticle'`). Runs `relion_refine --ios <optset> --ref <map> --trust_ref_size` — **explicitly tells RELION to trust + rescale the reference rather than validate box/apix.** No `Refine3D` driver/param-class exists (comment-only).

The two terminal sinks of an optset: either the subtomo job's (filtered-if-exists) optset via the resolver, or the `MergedSources` optset via a `manual:` override. (`_add_merged_sources_candidates` surfaces only the **legacy** `MergedSources/optimisation_set.star` — *no slug* — as a synthetic candidate; per-slug merges reach consumers **only** via `apply_aggregation_overrides`, gated on `is_aggregation`. Latent single point of failure for non-agg projects.)

---

## 7. The gap — why aggregation is NOT seamless today

1. **`PickList.extracted_path` is a dead end.** Written by `mark_extracted`, read by **nothing** downstream (grep confirms: only `project_state` + UI display reference it). There is no resolver/forwarder turning per-list extracted optsets into refinement input.
2. **The aggregator is blind to the pick-list model.** `discover_subtomo_optimisation_sets` + `ui/aggregation/merge_card._build_merge_sources` select by `AggregationSource.optset_path` = a whole `SUBTOMO_EXTRACTION` job's optset (one per project × species). They never call `get_authoritative_slug` / `get_pick_lists` / read `extracted_path`. A per-list extraction in `Curation/<sp>/<tomo>/<slug>/out/optimisation_set.star` is **structurally undiscoverable** by the merge.
3. **"Curated" means two disjoint things.** The merge's curated/original toggle (`load_tomo_curation` over `particles_filtered.star`) only ever describes the **auto** list's job-level keep/drop. It cannot represent "this tomo's authoritative list is a manual or merged list." There is no path from `authoritative_pick_lists` into `AggregationSource`.
4. **No species-level roll-up of authoritative lists.** Nothing walks `authoritative_pick_lists` for a species, collects each cell's authoritative extraction, and merges them into one species-level `optimisation_set_filtered.star`. `resolve_canonical_optset` only knows the two-file (auto/filtered) subtomo-job contract.
5. **The `'auto'` authoritative case has no per-tomo `extracted_path`.** For every untouched tomo (the default), the authoritative slug is `'auto'` — there's no `PickList`, so no `extracted_path`; particles live in the shared job-level optset sliced by `rlnTomoName`. A uniform "forward each authoritative `extracted_path`" loop has a **hole** for the common case and must special-case back to job-level slicing.
6. **Extraction is per-list, user-triggered, and never gated on authoritative.** `set_authoritative_slug` neither triggers nor requires extraction; `extraction_state()` is never checked at merge time. A user can mark a manual list authoritative, never extract it, and the merge silently uses the job-level auto particles with no warning.
7. **Staleness is local, not propagated.** `extraction_state()` correctly derives STALE per list, but nothing surfaces an authoritative list's staleness into the cross-project selector, and nothing detects that an already-merged set went stale because an upstream list was re-curated.
8. **`filtered_count` self-heals only on render** (§3). A headless aggregation read sees on-disk `filtered_count` (possibly `None`) → can mis-report STALE for a correctly-extracted filtered list.
9. **`'filtered'` as authoritative is undefined.** The setter accepts any slug; `filtered` is synthesised (no `PickList`, no `extracted_path`), so it has no extraction target. *(Uncertain whether the UI ever lets `'filtered'` be selected authoritative — see §9.)*
10. **Cross-project key fragility.** `species_id` is a per-project `slugify()` of the display name; `tomo_name` is `rlnTomoName`. Across projects neither is guaranteed stable/aligned. "The authoritative list per `(species, tomo)` across projects" has **no project-spanning identity**.
11. **Cross-project compatibility hazards** (inherited from the merge engine):
    - `rlnTomoName` collision: two projects routinely reuse `Position_1` with different recons → `merge_optimisation_sets_into_jobdir` **hard-aborts**. No namespacing. Same-name-but-identical-path tomos would be **wrongly fused**. `rlnImageName` (absolute) is also unscoped — assumes a shared Lustre mount.
    - Box/bin/apix: only `CRITICAL_OPTICS_COLS` block; `rlnImageSize` (box) / `rlnImagePixelSize` (subtomo apix) / `rlnTomoSubtomogramBinning` only **warn**. (The half of this gap where per-list extraction *invented* `384/1/224` when `subtomo_jm` was `None` is CLOSED — see line 127. The merge-side warn-instead-of-block remains open.)
12. **Orientations zeroed; scoreless.** `build_list_optset` zeros all orientations (manual picks are positions-only). Cross-project quality gating has nothing to rank by; no provenance flag distinguishes oriented (auto/TM) from zeroed (manual) particles in a merged optset.

---

## 8. Design: seamless aggregation

The design closes the gap with **one new resolver** (`authoritative list → optset`), **one new orchestrator** (enumerate → gate/extract → roll up), and **maximal reuse** of `subtomo_merge` + the IO-slot resolver. No new merge engine, no new extraction driver.

### 8.0 Identity model (prerequisite)

Define the contributing unit as a tuple:

```
Contribution = (project_path, species_key, tomo_name, authoritative_slug)
```

- **`species_key`** — a cross-project species identity. Per-project `species_id` is not portable (gap 10). Resolve by **template/label** the same way `aggregation_discovery` resolves species today (`instance_id __suffix → species_id → single-species fallback`). The aggregation UI must let the user **confirm/override** the species grouping (label/template match), since no enforced cross-project registry exists. *Reuses* the existing species-resolution heuristic; *adds* a confirm step.
- **`tomo_name`** — kept as `rlnTomoName`, but **namespaced per project** before merge (see 8.5).

### 8.1 Enumerate contributions

New helper (e.g. `services/aggregation/authoritative.py:enumerate_authoritative(project_paths, species_key)`):

For each project:
1. **Reconstruct the full tomo set** for the species — the union of: (a) tomos in the candidate-extract / subtomo job manifest (auto/filtered are *not* in `pick_lists`, so they must come from the job side), and (b) `tomo_name`s appearing in persisted `pick_lists` for that species.
2. For each tomo, `slug = state.get_authoritative_slug(species_id, tomo_name)`.
3. Emit `Contribution(project_path, species_key, tomo_name, slug)`.

*New code.* This is the cross-`(species,tomo)` enumeration gap 4/5 calls out.

### 8.2 Resolve each authoritative slug → an extraction handle

New `resolve_authoritative_optset(state, species_id, tomo_name, slug) -> AuthoritativeOptset`:

| slug | handle | extraction_state |
|---|---|---|
| `'auto'` (default) | the **job-level** `optimisation_set(_filtered).star` via `resolve_canonical_optset(subtomo_job_dir)`, **sliced to this `rlnTomoName`** | derive from the job's filtered/reviewed state (reuse `load_tomo_curation`) |
| `'filtered'` | **undefined target** — see §9; provisionally treat as the auto job-level filtered optset sliced to this tomo | same as auto-filtered |
| workbench slug (manual/imported/merged) | `PickList.extracted_path` (the per-list `out/optimisation_set.star`) | `PickList.extraction_state()` |

This is the missing **uniform handle resolver** with the explicit special-case for `'auto'` (gap 5). *New code, reuses* `resolve_canonical_optset` + `extraction_state()`.

> **Before reading `filtered_count`/`extraction_state` headless, sync the cache** (gap 8): port the `_collect_pick_lists_for_species` write-back (§3) into a non-UI helper `sync_filtered_count(pl)` so a headless read sees disk truth. *Reuses* `_memoized_keep_state` / `picks_filter`; *moves* the sync off the render path.

### 8.3 Compute state + the extraction gate

For every contribution whose slug is a workbench list, compute `extraction_state()`:

- **All EXTRACTED-and-current** → proceed to roll-up.
- **Any NOT_EXTRACTED / STALE** → **never silently proceed.** Two acceptable modes, user-selectable:
  - **Auto-extract:** submit `backend.extract_pick_list` for each missing/stale list (Re-extract path already rmtree's `out/`, §5), wait on `RELION_JOB_EXIT_*` / `result.json`, `mark_extracted`, persist by explicit `project_path`.
  - **Gate:** present a single explicit list — *exactly* which `(project, tomo, slug)` are NOT_EXTRACTED vs STALE — and require one click to extract them. No partial/silent roll-up.

`'auto'` contributions need no per-list extraction (they reuse the existing job optset) but **do** carry job-level curation state, which the gate should surface for parity.

*New orchestration; reuses* `extract_pick_list` + `extraction_state()`.

### 8.4 Compatibility checks (before merge)

Read the optics block of each contribution's optset and **block, not warn**, on what `subtomo_merge` only warns about today (gap 11):

- `CRITICAL_OPTICS_COLS` — already hard-blocked by `subtomo_merge`; keep.
- **Promote to hard-block at the aggregation boundary:** `rlnImageSize` (box), `rlnImagePixelSize` (subtomo apix), `rlnTomoSubtomogramBinning`. A species-level roll-up that mixes boxes is physically inconsistent and `--trust_ref_size` would mask it. The gate must list incompatible contributions and refuse them (or offer per-list re-extraction at the canonical box/bin from `sp['subtomo_jm']`).
- ~~Guard the `subtomo_jm is None` → default `384/1/224` path (gap 11)~~ **DONE (denovo S3)**: `extraction_params_for_species` returns `None` rather than defaulting, the gate reports the list blocked with `extract_inputs_blocked_reason`, and the dashboard asks.

*Reuses* the merge's `CRITICAL_OPTICS_COLS` machinery; *adds* a stricter pre-flight on the optional cols.

### 8.5 Cross-project namespacing (before merge)

To avoid the `rlnTomoName` hard-abort and silent same-name fusion (gap 11):

- **Namespace `rlnTomoName` per project** when a contribution crosses projects — e.g. `<project_tag>/<tomo_name>` — applied consistently to *both* `particles.star` (`rlnTomoName`) and `tomograms.star` (`rlnTomoName` + the `rlnTomoReconstructedTomogram` key). This converts "conflict → abort" and "silent identical-name fusion" into a clean per-project partition.
- `rlnImageName` is absolute — assume a shared mount (document the limitation; do not re-home subtomo files in v1).

*New code* (a re-home pass over the parsed star before `merge_optimisation_sets_into_jobdir`), since the merge does no namespacing.

### 8.6 Roll up via `subtomo_merge`

Build `additional_sources` as the resolved (and namespaced) per-contribution optsets and call `merge_optimisation_sets_into_jobdir(job_dir=<species roll-up dir>, additional_sources=[...], allow_no_primary=True)`. The output is the species-level `optimisation_set.star` (+ `particles/tomograms.star` + `merge_summary.json`).

This is *exactly* the existing engine; the new work is **building the source list from authoritative handles** instead of from `discover_subtomo_optimisation_sets`. Effectively: a second `_build_merge_sources` that reads §8.2 handles.

### 8.7 Forward via the existing slot mechanism

Two reuse options; prefer the first:

1. **Register the rolled-up optset as a `prefer_if_exists` producer** the resolver scores for `OPTIMISATION_SET_STAR`, mirroring `_add_merged_sources_candidates` but at a species-level path. Then `Class3D` / `ReconstructParticle` pick it up with **no manual override** — the seamless win. *Requires* a `JobFileType`/synthetic producer for the species roll-up (gap: none exists today).
2. **Fallback:** `apply_aggregation_overrides` writes `manual:<rolled-up optset>` on every consumer's optset slot — exactly today's `MergedSources` path. Works immediately, no resolver change, but not "seamless" (it's an override).

Note Class3D's `input_reference` must come from a `ReconstructParticle` run **on the same rolled-up optset** so box/apix line up under `--trust_ref_size`.

### 8.8 Provenance & idempotency

- **Provenance:** record, per rolled-up species optset, the contributing `(project_path, species_id, tomo_name, slug, extracted_path, extracted_count)` and the box/bin/apix used. Today `AggregationMergeSource` records project/species/counts but **not** the list slug or extraction state (gap 7-of-§7). Extend it (or a sibling record) so a roll-up is auditable and re-derivable.
- **Idempotency / staleness re-check:** before forwarding, re-run §8.2/§8.3 and compare each contribution's current `extraction_state()` + the consumed-star mtime against what the roll-up recorded. If any contribution is now NOT_EXTRACTED/STALE (because a list was re-curated/re-merged/re-deduped — §4 bumps mtime), **mark the rolled-up optset stale and re-gate.** This is the cross-project analogue of `extraction_state()` and is the piece that makes the user not have to track staleness.

### 8.9 Staged build order

1. **`sync_filtered_count` off the render path** (§8.2 note) — unblocks correct headless `extraction_state()`. Smallest, highest-leverage. **✅ DONE (S18): `picks_filter.sync_filtered_count(pl)`.**
2. **`resolve_authoritative_optset`** + **`enumerate_authoritative`** (§8.1–8.2) — read-only; surface a per-species table of `(tomo, slug, extraction_state)` with no merge yet. Pure diagnostics; validates the identity model. **✅ DONE (S18): `services/aggregation/authoritative.py` — pure/headless (explicit `ProjectState`, no UI import, job dirs from `state.relion_job_name`/`job_path_mapping`, never the client-context `_job_dir_for`): `AuthoritativeHandle`, the resolver + enumerator, a `__main__` CLI, and `backend.get_authoritative_extraction_status`. Read-only, no merge. Runtime-verify with the CLI before building §8.3:** `python -m services.aggregation_authoritative --project <proj> [--species <id>]`.
3. **The gate + auto-extract** (§8.3) — wire `extract_pick_list` for missing/stale; no roll-up yet. **✅ DONE (S18): `compute_gate_report` / `GateReport` (ready/pending/blocked, `can_proceed`) + `extract_inputs_for_list` in `services/aggregation/authoritative.py`; `backend.get_authoritative_gate_report` (read-only) + `backend.extract_authoritative_pending` (auto-extract pending workbench lists, idempotent, never touches auto/filtered/ready, reuses `extract_pick_list`; run in a BackgroundTask). Read-only gate verifiable via the CLI (`ready/pending/blocked → roll-up READY|BLOCKED`); the auto-extract action awaits UI wiring for its runtime test.** Note: a workbench authoritative slug with no PickList resolves to `kind='dangling'` → blocked (not silently pending).
4. **Compatibility + namespacing pre-flight** (§8.4–8.5).
5. **Roll-up `_build_merge_sources` from handles** (§8.6) → species-level optset via `subtomo_merge`.
6. **Forwarding** (§8.7) — start with the `manual:` override fallback, then add the `prefer_if_exists` synthetic producer once the path contract is settled.
7. **Provenance + idempotency re-check** (§8.8).

**Reuse vs new:** the merge engine (`subtomo_merge`), extraction (`extract_pick_list`), filtered-subset machinery (`picks_filter`), per-list staleness (`extraction_state`), and the IO-slot scoring are all **reused as-is**. New code is the **authoritative-handle resolver**, the **cross-`(species,tomo)` enumerator**, the **gate/auto-extract orchestrator**, the **namespacing pre-flight**, and the **species-level synthetic producer**.

---

## 9. Open questions / risks

1. **Is `'filtered'` ever selectable as authoritative?** `set_authoritative_slug` accepts any slug, and `filtered` has no `PickList`/`extracted_path`. The maps flag this as uncertain — **verify in the UI** whether the authoritative selector exposes `filtered`. If yes, §8.2 needs a defined extraction target for it (likely: extract the `<slug>_filtered.star` subset, same as a workbench list, rather than treating it as the job-level auto optset).
2. **Cross-project species identity** has no enforced registry (gap 10). The label/template heuristic can mis-group; the design leans on a user-confirm step. Is template-hash matching reliable enough to default, or must the user always confirm?
3. **`rlnTomoName` namespacing scope.** Namespacing fixes the abort, but downstream RELION (and any tomograms-based plots) must tolerate `<tag>/<tomo>` names. Confirm `relion_refine` / `reconstruct_particle` accept namespaced `rlnTomoName` without choking on the `/`.
4. **`rlnImageName` portability.** v1 assumes a shared Lustre mount across all contributing projects. Archived/moved projects break the merged `particles.star` silently. Is a re-home/symlink pass in scope, or is "same mount" an acceptable hard requirement?
5. **Scoreless cross-project gating** (gap 12). Manual/merged picks have no per-particle score, so quality thresholding at the aggregation boundary has nothing to rank by — only keep/discard. Is that acceptable for the first Class3D pass, or is an orientation/score-bearing path needed?
6. **mtime as the staleness trust root.** `extraction_state()` leans on `count` + consumed-star mtime (+1 s). A same-count, same-mtime content change, or clock-skewed Lustre mtime, mis-classifies. Acceptable per-list; **as the trust root for an automated cross-project forward it is fragile** — consider a content hash of the consumed star in the provenance record if auto-extract is enabled by default.
7. **`merge_only` vs `MergedSources` card overlap.** Two cross-project mechanisms already exist (the `merge_only` job and the `MergedSources` card / `_add_merged_sources_candidates` legacy-single-path vs per-slug). The new species roll-up should pick **one** to extend, not add a third. Which is canonical going forward?
8. **`'auto'` slicing correctness.** Slicing the shared job-level optset to one `rlnTomoName` (§8.2) must produce a per-tomo particles subset that re-merges cleanly with per-list optsets. Verify `subtomo_merge`'s tomo include-filter (`tomos` / `original_tomos` in `_normalize_source`) is sufficient, so we don't have to physically slice the auto optset before feeding it.
