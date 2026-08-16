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
