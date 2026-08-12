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
