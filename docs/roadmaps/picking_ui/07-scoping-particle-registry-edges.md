# 07 — Scoping only: where particle facts actually belong

**Status:** scoping doc, 2026-08-18. **No code, no stages.** **Decision of record (maintainer,
2026-08-18):** *"let's hold on about implementing a cross-project registry of particles because it's a
bigger endeavour than I initially thought now that I talked myself through it, but let's use these
thoughts of mine as a basis for scoping this expansion for later. It should be modular along the
edges that I've just described and will later include things like membrane segmentations etc."*

This file exists so that (a) the reasoning is not lost, and (b) roadmaps 01–06 can be checked against
it — every one of them was written to cut along these seams rather than across them.

## 1. The three tiers the walkthrough separated

The maintainer arrived at the split by asking, of each field on the Overview tab, *"is this true of
the particle, or only of this particle here?"*

| Tier | What it is | Travels? | Status today |
|---|---|---|---|
| **Definition** — particle-intrinsic | name, diameter, symmetry, notes, templates, masks (+ their provenance) | between projects, between labs | **shipped** — roadmap 12's two-tier catalog: `<root>/<catalog_id>/v<N>/` holds the definition + files, `ParticleSpecies.catalog_id` is a copy-time backlink, no live sync |
| **Application** — particle × tomogram set | extraction geometry (box · binning · crop), the pixel/binning chain those numbers are counted in, later: per-set search parameters | within a project, per tomogram regime | **not modelled** — collapsed onto the species (`ExtractionParams` on `ParticleSpecies`) and onto the project (`compute_pixel_chain(project_state)`) |
| **Results** — particle × project | pick lists, keep/drop curation, merges, extractions, bound jobs | never (coordinates are relative to *these* tomograms) | correct as-is; cross-project reuse of results is the aggregation card, which roadmap 12-S4 already keys by `catalog_id` |

The maintainer's own words for the missing middle tier:

> *"this should be a feature of the particle's interaction with the tomogram (i.e. the extraction job)
> not the feature of the particle itself, because a particle may be extracted from multiple tomograms
> with multiple extraction geometries"*

and, on why the pixel/binning sanity table does not belong on a species:

> *"it also refers to a particular tomogram set in this project, whereas in other projects or even
> within the same project this particle may be applied to differently dimensioned tomos, same tomos
> with different binning etc."*

Both are the same observation: **box / binning / crop are counted in voxels of a particular
reconstruction regime.** `384 / 1.0 / 224` means nothing without saying "at which binning, of which
tomograms".

## 2. What tier 2 would actually cost

Not a rename — a new identity plus a resolver change:

- **A tomogram-set identity.** Something that names "these tomograms at this binning": today the
  nearest things are the `TiltSeriesRegistry` and the rows `services/pixel_chain.py:57`
  `compute_pixel_chain` builds (which already walk apix → binning → box per job chain). A set is
  plausibly keyed by (reconstruction job instance, binning) — but that is a design question, not a
  given: the same tomograms re-reconstructed at a second binning is the case that forces it.
- **Re-keying the geometry.** `ExtractionParams` moves off `ParticleSpecies`
  (`services/project_state.py:189-226`) into a `{(species_id, set_id): ExtractionParams}` map on the
  project, with `species.extraction_params` migrating in as "the geometry of the default set".
- **One resolver change.** `extraction_params_for_species`
  (`services/aggregation/authoritative.py:229-254`) gains a set argument; precedence becomes
  subtomo job model → (species, set) → (species, legacy) → **None**. The no-third-branch rule (D-3:
  never guess a box size) is the part that must survive untouched.
- **Every caller of that resolver** learns which set it is extracting into — `list_actions`, the
  Picks tab's extract-all, the subtomo job prefill.
- **The panel from roadmap 04** grows from one geometry to a small per-set table. This is why 04
  labels it "this project" and routes its write through the single existing writer
  (`commit_extraction_geometry`): re-keying later is one function and one panel, not a sweep.

## 3. What "membrane segmentations etc." would cost

Today `ParticleSpecies` *is* the annotation class, and it carries `templates` / `masks` /
`symmetry` — fields that mean nothing for a membrane. Generalizing means:

- a `kind` on the registry entry (`particle` | `segmentation` | …), with kind-specific payload
  (a segmentation carries a model checkpoint + label id + mesh export settings, not a template pair);
- the Species page's tab set becoming **per-kind renderers** rather than the fixed five
  (`ui/species/page.py:40-46`) — Templates & masks is a particle-only tab;
- the catalog (roadmap 12) storing kinds side by side, since a lab's membrane model is exactly as
  cross-project as its ribosome template;
- the overlay/colour system already generalizes (it is per-registry-entry, not per-particle).

Nothing in tiers 1 and 3 needs to move for this; it is tier-2 shaped work plus a discriminated union.

## 4. Edges roadmaps 01–06 were written to preserve

Check any future change against these — they are cheap now and expensive to recover later:

1. **One writer for extraction geometry.** `commit_extraction_geometry`
   (`ui/particles/list_actions.py`) stays the only path that sets it; roadmap 04's panel calls it
   rather than adding a second writer.
2. **The geometry panel says "this project".** A UI that silently implies portability is worse than
   one that states its scope (CLAUDE.md "Surfacing uncertainty").
3. **Pixel/binning sanity is not a species surface.** Roadmap 04 removes the Overview copy; its home
   is the Tomogram Dashboard, which is project-scoped and honest about it.
4. **The creation dialog does not ask for extraction geometry** (roadmap 01-S2) — a definition stays
   portable, so the creation form only collects tier-1 facts.
5. **Template / mask registration is a service** (roadmap 01-S1, `services/species_admin.py`), so the
   catalog importer, the creation dialog, the workbench and a future segmentation kind all call one
   function.
6. **No defaults, ever, for a box size.** D-3 stands whatever the keying becomes.
7. **Shared primitives, not per-page ones**: `Segmented`, `render_path_link`, `choice_row`,
   `render_chip`, `species_pill`. A second registry page should cost layout, not new vocabulary.

## 5. Open questions (for whoever picks this up)

- What defines a tomogram set — the reconstruction job, the binning, or an explicit user-named group?
  The `.rec` / imported-tomogram path (de-novo S5) may have no reconstruction job at all.
- Does the catalog carry tier-2 defaults ("this lab usually extracts ribosomes at bin 4, box 192"),
  or is that a per-project answer every time? A default that travels is a default that can be wrong
  in a new project.
- Is `color` tier 1 or tier 2? It is a property of *this project's* overlay palette (two species must
  not collide on one canvas), not of the particle.
- Does a segmentation kind need pick lists at all, or is its result a volume + mesh, i.e. a fourth
  tier?

## 6. Log

- 2026-08-18 — written from the walkthrough. Deferred by the maintainer mid-scoping; the tier split
  and the edge rules are the deliverable.
