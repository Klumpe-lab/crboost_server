# Roadmap 12 — lab-level species catalog (cross-project definitions)

**Status:** scoped-lightly 2026-08-16 (direction only; detail when 10/11 are stable). **Depends on:**
09-S2 (`ParticleSpecies.catalog_id` hook), 10 (Species page = the UI home). **Decision of record
(user, 2026-08-16): hook now, build last.**

## The two-tier model (why this is not "one global registry")

Only species **definitions** are cross-project by nature — name, diameter, symmetry, notes,
templates, masks (with their provenance). Picks, keep/drop filters, merges and extractions are
project-bound: coordinates are relative to a project's tomograms, RELION-compat wants project-local
files, and cross-project reuse of *results* is already the aggregation card
(`ui/aggregation_merge_card.py`, `services/aggregation_discovery.py`: Project → Species → Tomogram,
concatenating per-project optsets). Today aggregation groups species only **within** a project
(`aggregation_discovery.py:90-147` reads each project's `species_registry` and matches by per-project
id); "the same species across projects" is by eye. A stable catalog id fixes that.

```
Catalog (lab-level, config-driven root)         Project species (what exists today)
  <root>/<catalog_id>/species.json                ParticleSpecies{..., catalog_id}   ← backlink
  <root>/<catalog_id>/templates/*.mrc(+.meta.json) templates/<sid>/*.mrc (project-local copies)
  <root>/<catalog_id>/masks/*.mrc(+.meta.json)      Curation/<sid>/…, pick lists, jobs  (never shared)
```

Snapshot semantics (same rule as species→job values): **import from catalog** copies the definition
+ files into the project (`templates/<sid>/`, sidecars via `sidecar_ensure`, `imported_from` = catalog
path) and sets `catalog_id`; **publish to catalog** copies a project species up as a new version dir
(never overwrites; `created_by` recorded); no live sync in either direction.

## Stages (to detail later)

- S1 config: `species_catalog_root` in `conf.yaml` (+ `~/.crboost/conf.yaml` override layer, like the
  `sifs/` centralization); `Config` model + `get_config_service()`; empty/absent root = feature off.
- S2 catalog I/O service (`services/particles/catalog.py`): list, read one, import-into-project,
  publish-from-project; typed model `CatalogSpecies` (Pydantic, `extra="forbid"`); versions as
  sibling dirs `<catalog_id>/v<N>/`; a `catalog.json` index regenerated on write.
- S3 UI: Species rail "+ from catalog" (picker over the index) and Overview "publish to catalog";
  provenance line shows "from catalog <name> v<N>".
- S4 aggregation: `aggregation_discovery` carries `catalog_id`; the merge card groups species across
  projects by it (falls back to per-project id when absent).
- Non-goals: live sync; cross-project picks; permissions beyond the shared FS group model
  (`created_by` only).

## Stage log (append-only)

- 2026-08-16 — direction recorded; hook (`catalog_id`) scheduled in 09-S2. No code.

## Stage record (append-only)

- 2026-08-18 — **ALL FOUR STAGES CODE-COMPLETE** (`ruff check .` clean; `py_compile`,
  `check_boundaries.py` and the runtime pass owed with the rest of the arc). Built last, as
  decided — 10 and 11 are code-complete, and this only adds to them.

  **S1 — config.** `Config.species_catalog_root: str = ""` + `ConfigService.species_catalog_root`
  returning `Path | None`, documented in `config/conf.template.yaml` (and so inheriting the
  `~/.crboost/conf.yaml` override layer for free). Empty ⇒ **the feature does not exist**: no rail
  row, no Overview button, no error banner. A configured-but-missing path still returns the Path so a
  typo surfaces as a real OSError naming it, rather than as a silent "feature off".

  **S2 — `services/particles/catalog.py`.** `CatalogSpecies` (Pydantic, `extra="forbid"` — a key
  nobody understands is worth failing on, since this file is written by one crboost version and read
  by another) and `CatalogEntry` for the index. Versions are sibling dirs `<catalog_id>/v<N>/`, each
  holding `species.json` + `templates/` + `masks/`; a published version is never edited, so
  republishing writes v(N+1) and a project that imported v1 keeps describing what it used.
  `list_catalog()` rebuilds from the version directories rather than trusting `catalog.json` — the
  index is derived data, and a stale one must not hide something somebody published; `write_index()`
  is therefore best-effort. `CatalogSpecies.templates/masks` reuse `ParticleTemplate` /
  `TemplateMask` verbatim with `*_path` holding a bare filename, so there is no parallel schema to
  drift. **Sidecars travel with every copied file**, which is what keeps a template's UUID — and
  therefore the recorded `selected_template_id` — resolvable after an import.

  **S3 — UI (`ui/species/catalog.py`).** Rail gains a "From catalog" row and Overview a "Publish to
  catalog" button, each rendered only when `catalog.is_enabled()`. Both dialogs say plainly what
  crosses the line and what does not, and publish REFUSES (button disabled, files listed) when a
  registered template or mask is missing on disk — a catalog entry nobody could import is worse than
  no entry. Disk work goes through `run.io_bound` (shared FS), both handlers are SingleFlight-guarded.
  `ParticleSpecies.catalog_version` was added beside the existing `catalog_id` hook so the provenance
  line reads `catalog <id> v<N>`; schema 3.4 → **3.5**.

  **S4 — aggregation.** `SubtomoCandidate.catalog_id` is read from each scanned project's species
  registry, and the merge card's species row shows a `⌗ <catalog_id>` chip whose click **selects every
  discovered copy of that species across projects**; the text filter matches catalog ids too. This is
  the concrete thing the roadmap promised: local species ids are minted per project, so "the same
  species in another project" used to be matched by eye. Grouping keys on `catalog_id` and falls back
  to the existing per-project view when absent — a species that was never published shows exactly what
  it did before.

  **Deviations, both deliberate:** the tree is still Project → Species → Tomogram rather than being
  re-rooted on the catalog. Re-rooting would hide which project a set came from, which is the thing
  the user checks before merging; a chip that selects the twins delivers the one-click outcome without
  taking that away. And there is no "update this species from a newer catalog version" action —
  §Non-goals says no live sync, and re-importing (which mints a second local species) is the honest
  way to take a new version while the old one keeps describing what was already picked with it.
