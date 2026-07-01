# 412 capsid — aggregation → reconstruct → classify → refine (session handoff)

*Working note for picking this up next session. Date: 2026-06-26.*

## What this is

Aggregating ~120 template-matched picks of the **412 viral capsid** (icosahedral, **I1**
symmetry) across **9 grids/sessions** into one set, sorting out the false positives, and
driving it toward the best-resolution average we can get. The boss (Sven) made the
template we matched with; we follow his recipe as ground truth.

## TL;DR — where we are right now

- **Aggregation merge done:** 112 particles, 129 tomograms, 9 sources, all consistent at
  box 600 / **1.55 Å/px** / bin 1.
- **ReconstructParticle done** (job002) → initial I1 average `merged.mrc` (224 px, 3.1 Å/px).
  Noisy but right dimensions vs the template → scale + handedness sane.
- **Class3D (K=3) done** (job003) → **winner = class001: 94 particles @ 16.3 Å.** The other
  two classes (7 and 11 particles) are junk sinks. The loose 0.1 LCC cut was cleaner than
  feared — ~85% of picks are coherent 412.
- **Current best map:** `External/job003/run_it015_class001.mrc` (412 average, 94 particles,
  16.3 Å, alignment-limited).
- **Next:** cut the 94 class-1 particles into their own optset → Refine3D in RELION → geometry
  refinement loop → focused capsomer refinement. (Refinement happens in RELION/M, not crboost.)

## Key paths

| What | Path |
| --- | --- |
| Project (aggregation) | `/groups/klumpe/crboost_data/412_aggregation` |
| Merged optimisation_set | `…/412_aggregation/MergedSources/merge-20260626-132413/optimisation_set.star` |
| Merged particles / tomograms | `…/merge-20260626-132413/particles.star`, `…/tomograms.star` |
| ReconstructParticle output | `…/412_aggregation/External/job002/merged.mrc` (224 px, 3.1 Å/px) |
| Class3D output dir | `…/412_aggregation/External/job003/` |
| → class maps | `…/job003/run_it015_class00{1,2,3}.mrc` (600 px, 1.55 Å/px) |
| → per-particle class assignments | `…/job003/run_it015_data.star` (`rlnClassNumber`) |
| → class stats (dist + resolution) | `…/job003/run_it015_model.star` |
| Boss's template (reference particle) | `/groups/klumpe/user/sven.klumpe/Processing/412/try1/Class3D/job121/run_it015_class001.mrc` (448 px, 1.55 Å/px) |
| Boss's reconstruct (recipe source) | `…/try1/Reconstruct/job118/` (`--b 384 --crop 224 --bin 2 --sym I1 --theme classic`) |
| Boss's class3d (recipe source) | `…/try1/Class3D/job121/` (`--sym I1 --particle_diameter 550 --ini_high 45 --healpix_order 3 --offset 5/2 --K 1 --iter 15`) |

## Pipeline so far (params that worked)

**job001 — ReconstructParticle (FAILED, ignore):** pre-fix path-resolution failure (see code
fixes below). Superseded by job002.

**job002 — ReconstructParticle (SUCCESS):** `binning 2`, `box_size 384`, `crop_size 224`,
`symmetry I1`, `theme classic`. → `merged.mrc` (224 px / 3.1 Å/px). Used as the Class3D reference.

**job003 — Class3D (SUCCESS, recovered):** `n_classes 3`, `symmetry I1`, `particle_diameter 550`,
`ini_high 45`, `healpix_order 3`, `offset_range 5`, `offset_step 2`, `n_iterations 15`,
`tau_fudge 1`, ref = `job002/merged.mrc`, input = merged optset. Result:

| class | particles | resolution |
| --- | --- | --- |
| **class001** | **94** | **16.3 Å** ← winner (most particles + best resolution) |
| class002 | 7 | 33.2 Å |
| class003 | 11 | 24.5 Å |

## What "cutting the optset" means + exact next steps

`run_it015_data.star` holds **all 112** particles, each tagged with `rlnClassNumber`. To go
forward with only the 94 good ones (class 1) you need a particles file containing **just those
94 rows** plus an optimisation_set pointing at it. That subset is "cutting the optset" — required
because Refine3D refines every particle it's given (feed it all 112 and the 18 junk come back).

**To continue (in order):**

1. **Cut the winning class → 94-particle optset.** Either:
   - RELION **Subset Selection** job on job003, select class 1 → writes the class-1 particles + optset; or
   - subset `run_it015_data.star` to `rlnClassNumber == 1` and rewrite the optimisation_set (Claude can do this).
2. **Refine3D** (RELION, gold-standard auto-refine): input = the 94-particle optset; reference =
   `job003/run_it015_class001.mrc` **low-passed to ~30–40 Å** (avoid Einstein-from-noise);
   symmetry **I1**; soft mask. → first real FSC + better poses than Class3D.
3. **Geometry/CTF refinement loop:** per-tilt CTF refine + tilt-series / frame alignment,
   alternating with Refine3D until resolution plateaus. *(The big tomo lever.)*
4. **Symmetry expansion → focused refinement** on one capsomer (localized reconstruction). *(The
   capsid-specific win — highest local resolution.)*
5. **Postprocess:** tight-but-honest mask, B-factor sharpen, local-resolution.
6. **(frontier)** export poses to **M (Warp)** for multi-particle refinement — usually the biggest
   jump past vanilla Refine3D, and you already live in WarpTools.
7. **(ceiling)** the hard limit is 94 particles; I1 (×60 → ~5,640 ASUs) is generous, but more real
   picks is the only way to raise the floor.

**Resolution context:** 16.3 Å now is *alignment-limited* (Class3D doesn't refine tilt geometry,
no gold-standard FSC). Data Nyquist is 3.1 Å, so there's real headroom; sub-nm very plausible,
further depends on data quality + geometry refinement. See the loop diagram in
`docs/binning-and-resolution.md` for why classification was binnable but refinement wants full res.

## Code changes made this session (state of the crboost codebase)

All **code-clean** (py_compile + ruff) and **runtime-verified** by the actual 412 runs above.
**⚠ UNCOMMITTED** — git isn't on Claude's PATH; user must commit. Branch:
`aggregation_particle_consolidation`.

1. **Keystone — merged optset is now a first-class resolver producer.** Relocated
   `active_merge()` / `active_merged_optset()` / `active_merged_optset_instance_path()` +
   `MERGED_DIR_NAME` onto `ProjectState`; `_add_merged_sources_candidates` points at the slug
   optset with a `label` ("Merged sources — <name>"); `apply_aggregation_overrides` wires consumers
   via the producer `source_key` (not a bare `manual:` path); io_config renders the label. Plus a
   guard so a dangling `mergedSources:` override **surfaces** instead of silently resolving to a
   foreign optset. Files: `services/project_state.py`, `services/path_resolution_service.py`,
   `ui/aggregation_merge_card.py`, `ui/pipeline_builder/io_config_component.py`. (4-lens adversarial
   review, 0 confirmed bugs.)

2. **`ProjectState.load()` data-loss bug FIXED.** `load()` is field-by-field and was silently
   dropping `aggregation_merges` / `active_merge_slug` / `is_aggregation` / `aggregation_sources` /
   `pick_lists` / `authoritative_pick_lists` on every reload — which made the SLURM driver lose the
   merge registry and fail path resolution at drive time (the first job001 failure). Now restored.
   ⚠ **Gotcha: any new persisted ProjectState field must be added to `load()` explicitly.**

3. **Class3D driver output-name bug FIXED** (`drivers/class3d.py`). It checked for the bare
   `run_optimisation_set.star` (a Refine3D / auto-refine name), but Class3D writes
   `run_it{N}_optimisation_set.star` — so **every Class3D run false-failed at the very end** despite
   all iterations completing. Now checks the iteration-numbered file and mirrors it to the bare name.
   job003 was recovered by hand (copied the optset to the bare name + flipped
   `RELION_JOB_EXIT_FAILURE` → `SUCCESS`).

## Open items / TODO

- **Commit** the 3 code changes above (git not available to Claude).
- **`crboost` has no Refine3D or class-select job** — only Class3D / ReconstructParticle. We do
  refinement + class selection in RELION/M. Adding a Refine3D job type + a Subset/Select job is a
  possible future crboost task.
- **Cosmetic 1.35 apix bug** still unfixed at root: data-less aggregation projects show
  `microscope.pixel_size_angstrom = 1.35` (the default) though the data is 1.55. Harmless for runs
  (RELION reads 1.55 from the star) but should derive apix from the merged sources. See
  `project_microscope_apix_default_trap`.
- Deferred keystone ride-alongs (UI polish): SingleFlight on the merge dialog, the
  `_DIALOG_REFS`/`_registry_expanded` cross-tab module-global leak, BackgroundTask feedback, a
  reactive "merged" sidebar badge.
