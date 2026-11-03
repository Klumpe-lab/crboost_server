---
name: How TM/Pick/Subtomo jobs are linked to a ParticleSpecies
description: Species attribution for per-particle jobs uses three sources (instance_id suffix, job_model.species_id, single-species fallback) — bare instance_ids with species_id field on the job are common in this codebase
type: project
originSessionId: f4cd2fde-2d45-4ff4-96a9-d4c0cd10f33d
---
Per-particle jobs (templatematching, tmextractcand, subtomoExtraction) need
to be attributed to a `ParticleSpecies` for the dashboard's pixel sanity
table, picks gallery, etc. The linkage is **not** consistently encoded —
real projects use a mix of three patterns:

1. **Instance-id suffix**: `templatematching__copia` — the species_id is
   appended after `__`. This is what the §11 ROADMAP example assumed.
2. **`job_model.species_id` field**: every `AbstractJobParams` subclass
   has `species_id: Optional[str] = None`. Real projects (e.g.
   `try2_after_pixShift`, `3dclass_aln_pixShiftBug`) use **bare
   instance_ids** (`templatematching`, no suffix) and set the species_id
   on the job model directly. So suffix-only lookup fails on these.
3. **Single-species fallback**: if `len(species_registry) == 1`, the job
   is implicitly attached to that species even when neither (1) nor (2)
   is set.

The dashboard's `_resolve_species(state, job_model, instance_id)` helper
in `ui/tomo_dashboard_dialog.py` tries all three in this order. Apply the
same pattern when adding new per-particle features that need species
attribution.

## Template apix / box: workbench is often 0; use the MRC header

`ParticleSpecies.workbench.pixel_size` is the workbench widget's record of
the template's pixel size. **In real projects this is frequently 0.0** —
users don't always run the workbench widget. The authoritative source is
the MRC header at `species.template_path` (or the TM job model's
`template_path`). Read with:

```python
with mrcfile.open(path, header_only=True, mode="r") as m:
    apix = float(m.voxel_size.x)
    box  = int(m.header.nx)  # cube-shaped templates
```

Cache by `(path, mtime)` — templates are tiny but the cache avoids
re-opening on every dashboard render. See
`_read_template_apix_box` and `_TEMPLATE_HEADER_CACHE` in
`tomo_dashboard_dialog.py`.

The template filename also encodes apix + box by convention
(`*_apix6.20_box128_*.mrc`) but parsing filenames is fragile; prefer
the MRC header.
