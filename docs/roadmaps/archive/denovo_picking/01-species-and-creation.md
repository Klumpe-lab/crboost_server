# S1 — Species creation UX, registry hygiene, data-less projects

Goal: a user can create a data-less project and a label-only species from the roster in under a
minute. No dependency on other sessions. ~300 lines.

## Model changes (`services/project_state.py`)

- `ParticleSpecies` (:187): add `origin: str = ""` (`"workbench" | "manual" | "imported"`) and an
  optional `extraction_params: ExtractionParams | None = None` sub-model (`box_size`, `binning`,
  `crop_size` — **no defaults**; consumed by S3's extract dialog).
- `add_species(name)` (:768): assign a palette color (`hash(id) % len(palette)` over the existing
  `SPECIES_OVERLAY_COLORS`-style palette) and `origin` at creation. Today every UI-created species
  gets the model default `#3b82f6` and there is no color editor.
- `remove_species` (:784) currently leaves dangling refs. Extend to purge: the species'
  `pick_lists`, `authoritative_pick_lists` keys, matching `job_model.species_id`, and
  `source_overrides` targeting `pick_list__*` slugs of that species (the last matters from S3
  onward). The confirm dialog (`ui/template_workbench.py:508-534`) lists what will be dropped.

## Creation entry points

- **Primary: "New species" button on the PARTICLES phase header** — same header hook that hosts the
  import-tomograms button (`ui/pipeline_builder/pipeline_roster.py:308-310`, cf.
  `_build_import_tomograms_btn` :1444). Reuse `_prompt_species_name`
  (`ui/species_workbench_panel.py:9-33`); name only, `SingleFlight`-guarded.
- Particles-section empty state (lands with S2's inversion — S1 just makes the helper importable).
- Workbench "+" (`ui/species_workbench_panel.py:146`) unchanged; add a color swatch editor to the
  species header (`ui/template_workbench.py:1005-1097`), which today edits only
  diameter/symmetry/notes.
- Soften the species gate message (`ui/pipeline_builder/pipeline_builder_panel.py:133-139`) to
  point at the new button instead of "go to the Template Workbench".

## Data-less project creation (kills `is_particle_only`)

`is_particle_only` is a **transient UI form field only** (`ui/ui_state.py:51`) — never persisted,
never passed to the backend. Creation mechanics are already glob-gated, not flag-gated
(`services/scheduling_and_orchestration/project_service.py:425, :477, :490-495`).

- Delete the field and both mutually-exclusive landing toggles + hand-rolled sync
  (`ui/data_import_panel.py:1185-1224` particle-only, `:1229-1268` aggregation — the aggregation
  toggle survives until S6 but loses its exclusion partner).
- Creation requirements derive from glob presence: empty globs ⇒ data-less create allowed, with an
  explicit "creating without raw data" confirmation line so it can't happen by accident
  (`get_missing_requirements`, `ui/data_import_panel.py:235`).
- Remove the dead branch `ui/data_import_panel.py:758-762` (references a SubtomoExtraction pre-init
  that no longer exists).

## Verification

Sandbox: `py_compile` + `ruff` + `python check_boundaries.py`. User runtime:

1. Create a data-less project (no globs) → lands in workspace, no errors, registry dir absent is
   fine.
2. "New species" from the roster header → species appears in workbench and in
   `project_params.json` with a non-default color and `origin="manual"`.
3. Delete a species that has pick lists → confirm dialog enumerates them; registry JSON shows the
   purge.
4. Delete a species that has pipeline jobs → the dialog names the instance ids; after confirming,
   their job folders are gone, the roster rows are gone, and `default_pipeline.star` no longer lists
   them.

---

## S1 CODE-COMPLETE 2026-08-13 — pending runtime

**Model** (`services/models_base.py`, `services/project_state.py`)

- `SPECIES_OVERLAY_COLORS` moved from `services/dashboard_data.py` to `models_base` — a species'
  color is now persisted model data assigned at creation, so the palette has to be visible to the
  model layer. `dashboard_data` re-exports the name, so the four UI import sites are untouched.
- `species_palette_color(species_id)` alongside it. Deliberately NOT `hash()`: `PYTHONHASHSEED`
  randomizes str hashing per process, so a species would change color on every server restart.
  Sums code points instead — stable across runs, and matches what the dashboard already did.
- `ExtractionParams` (box_size, binning, crop_size), **no defaults**, and
  `ParticleSpecies.extraction_params: ExtractionParams | None`. `None` means undecided; S3's dialog
  must ask rather than invent geometry a de-novo species has no TM job to inherit.
- `ParticleSpecies.origin: str = ""` — `"workbench" | "manual" | "imported"`; empty on pre-existing
  species, read as workbench.
- `add_species(name, *, origin="workbench", color="")` — color defaults to the palette slot for the
  generated id, so species stop all sharing one blue.
- `species_references(species_id)` is new and feeds BOTH the confirm dialog and `remove_species`, so
  the two can't disagree about what a species owns. `remove_species` now purges pick lists,
  authoritative-list choices and matching `source_overrides` instead of leaving dangling refs that
  only surfaced as resolution failures at deploy time.

**Job cascade — maintainer's call, 2026-08-13: cascade.** The first pass surfaced jobs carrying
`species_id` in the confirm dialog but left them in place (a job owns a job dir and a
`default_pipeline.star` row, so removing one is a pipeline operation rather than a registry edit).
The maintainer chose the full cascade instead, so `_do_delete_species` now awaits
`backend.delete_job(job_type, project_path, instance_id=iid)` for each of them BEFORE the registry
purge (delete_job reads the job model to find its dir). Per-job failures are toasted and logged and
the rest continue. Two knock-on changes:

- `species_references(...)["jobs"]` now also matches the **instance-id suffix**
  (`templatematching__ribosome`), not just `job_model.species_id` — otherwise the cascade would miss
  jobs that name their species only in the iid and leave exactly the orphans it exists to prevent.
  Deliberately NOT `resolve_species`: its single-species fallback attributes every per-particle job
  to the last remaining species, so deleting that species would take the whole particle chain.
- `_do_delete_species` is `async` and now **awaits** `_save_state()`; the old
  `asyncio.create_task(...)` could be GC'd before running (the W2 lesson).

The delete is irreversible and now removes job folders — the dialog lists the exact instance ids in
red above the "cannot be undone" line.

**UI**

- "New species" button on the PARTICLES phase header (`pipeline_roster._build_new_species_btn`),
  next to import-tomograms. `SingleFlight`-guarded — the roster is poll-refreshed, so the button can
  be destroyed mid-click. Creates with `origin="manual"` and, unlike the workbench "+", does NOT
  create a `templates/<sid>/` dir: a de-novo species may never have a template.
- Color swatch editor on the workbench species header — the color dot is now a menu of the 8 palette
  colors. Repaints just the dot on pick (one attribute → one visual property; a rebuild would
  destroy the menu mid-click).
- Species gate message points at the new button instead of "go to the Template Workbench".

**Data-less projects — `is_particle_only` deleted**

It was a transient UI field that duplicated what the globs already said (creation mechanics were
glob-gated, never flag-gated). Replaced by `is_dataless()` = both globs empty. The particle-only
toggle, its hint and the mutual-exclusion sync with the aggregation toggle are gone; the aggregation
toggle survives until S6 but no longer has an exclusion partner. A half-filled form still demands the
missing half — that's a mistake, not a data-less project. The create button reads "Ready to create —
without raw data" so it can't happen by accident.

Also dropped the branch that mirrored an aggregation project's pre-initialized SubtomoExtraction into
`selected_jobs`: verified against `project_service.create_project` ("Aggregation projects have no
pipeline jobs at creation time" since the merge moved to a standalone workspace card), so it was
reloading an EMPTY job list over the user's selections.
