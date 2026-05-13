# Template Workbench Restructure & v2 Integration — Handoff

_Written 2026-05-10. Picks up from `TEMPLATE_REGISTRY_REFACTOR_PLAN_V2.md`._

This is the bridge doc for the next session. The schema work + UI surface
flip + the two helper components (import dialog, slice viewer) are
landed. The workbench restructure proper is queued, plus several
integration points across the rest of the pipeline. This doc describes
both, in actionable detail.

## TL;DR for next session

1. **Restructure `ui/template_workbench.py`** to compose the v2 summary
   card + the new viewer + the import dialog + the existing PDB/EMDB
   and basic-shape flows into a 4-panel layout. Critical: rewire all
   generation flows to write through to `species.template`
   (`ParticleTemplate`) directly, not just `species.template_path`.
2. **Wire integration points**: subtomo-extract job-config tab gets the
   summary card; per-tomo dashboard gets template thumbnail per TM
   result row; the candidate-extract `particle_diameter_ang` and TM
   `symmetry` fields can move from per-job to species-level editing.
3. **v1 field deletion** when stable (third session, not next).

Everything else in this doc is detail to make those three moves
unambiguous.

## What's already landed (this session, 2026-05-10)

### Schema (services/project_state.py)

- `SCHEMA_VERSION` bumped (1,0) → (2,0).
- New models:
  - `TemplateMask` with `relion_mask_create` knobs as first-class fields:
    `threshold`, `extend_pixels`, `soft_edge_pixels`, `lowpass_ang`,
    plus `method` (Literal: spherical/cylindrical/relion/manual/imported).
  - `ParticleTemplate` with `template_path`, `polarity` (Literal),
    `lowpass_resolution_ang`, `source`, `imported_from`, `created_at`,
    `notes`, `mask`.
  - `TemplateWorkbenchUIState` (pure widget state, separate from
    particle metadata).
- `ParticleSpecies` additively extended with `diameter_ang`, `symmetry`,
  `notes`, `template`, `workbench_ui`. v1 fields kept (`template_path`,
  `mask_path`, `workbench`) — readers use dual-read.
- `_migrate_v1_to_v2(data)` — idempotent, version-guarded; populates
  `species.template` from v1 fields, lifts `particle_diameter_ang` and
  `symmetry` off jobs onto species.
- `ProjectState.load()` writes `<file>.v1.bak` once before migrating.
- Templates' apix/box deliberately NOT cached on the model — read on
  demand from MRC header.

### Path resolution (services/path_resolution_service.py)

- `get_context_paths` for TEMPLATE_MATCH_PYTOM dual-reads with
  **v1-semantic preservation**: per-job `tm_jm.template_path`/`mask_path`
  win when explicitly set (so migrated v1 projects with per-job
  overrides keep working), otherwise fall back to `species.template`.
  This v1-priority is intentional and lets us drop the v1 fields safely
  later.

### Shared utilities

- `services/templating/template_metadata.py`:
  - `read_template_header(path)` — mtime-keyed cache returning
    `TemplateHeader(apix_ang, box_px, nx, ny, nz)`.
  - `get_effective_template_path(species)` / `get_effective_mask_path(species)`
    — v2-prefer, v1-fallback resolvers.
  - `resolve_species_from_job(state, job_model, instance_id=None)` —
    three-fallback chain (instance_id `__suffix` → `job_model.species_id`
    → single-species fallback). Reused across plugins and dashboard.
- `services/templating/mrc_inspection.py`:
  - `inspect_mrc_for_import(path)` — heavy reader (loads volume) returning
    `MrcInspection`: header geometry, mode/dtype, full-volume statistics,
    polarity inference (central-vs-overall mean), mask-likeness
    bimodality test, MRC header labels, regex-pulled provenance hints
    (PDB id, EMDB id, tool name).

### UI components (built but only summary card is wired)

- `ui/components/template_summary_card.py`:
  `render_template_summary_card(species, *, on_edit=None, compact=False)`.
  Indigo left-border, "SPECIES TEMPLATE" header, polarity chip, apix +
  box from MRC header, lowpass-applied status, mask method chip + four
  relion knobs, particle diameter + symmetry, source + imported_from +
  notes. Empty-state body when no template registered.

- `ui/components/template_viewer.py` **(BUILT, NOT YET WIRED into the
  workbench)**: `render_template_viewer(template_path, mask_path=None,
  *, height_px=280, show_mask_default=True)`. Orthoslice triplet
  (XY/XZ/YZ at center) with per-axis sliders, mask overlay toggle (amber
  contour at value=0.5), per-process mtime-keyed volume cache. Uses
  aspect-ratio CSS, NOT `scaleanchor` (per visualization-stack memory).
  Returns a controller with `update_paths(...)` for swap-without-remount.
  Replaces molstar.

- `ui/template_import_dialog.py` **(BUILT, NOT YET WIRED)**: 
  `await open_template_import_dialog(project_path, species)` returns a
  populated `ParticleTemplate` or `None`. File-picker → inspection →
  editable form (polarity, source pre-filled from inferred PDB/EMDB id,
  lowpass, notes) with banners for: mask-likeness ("looks like a mask"),
  non-cube volumes, replace-existing-template warning. Copies file into
  `templates/<species_id>/` (collision-safe `_2`, `_3` suffix), populates
  `imported_from = original_path`.

### UI surfaces flipped

- `ui/job_plugins/template_match.py`: summary card embedded above
  "Per-job override (legacy)" path pickers (relabeled). Falls back to
  amber "No species linked" card when species can't be resolved.
- `ui/job_plugins/candidate_extract.py`: summary card embedded.
- `ui/tomo_dashboard_dialog.py` pixel-sanity panel: TM block reads
  template path via `get_effective_template_path(species)` + MRC header.
  Symmetry prefers `species.symmetry`. Candidate-extract block prefers
  `species.diameter_ang` over `ce_jm.particle_diameter_ang`. Local
  `_TEMPLATE_HEADER_CACHE` removed; thin wrapper around shared util.

### Plumbing & ergonomics

- Plugin loader in `ui/job_plugins/__init__.py` was previously silent
  on errors (caught `ImportError` at INFO level only, swallowed all
  other exceptions). Now `except Exception` with WARNING + traceback.
  This was a real footgun — plugin file had any error → silently fall
  back to default renderer with no clue.
- `instance_id` plumbed through `render_config_tab` to plugins so they
  can use the suffix fallback in `resolve_species_from_job`.

## The workbench restructure (the main next-session task)

### Current state — `ui/template_workbench.py` (1062 lines)

Monolithic `TemplateWorkbench` class. Layout (top to bottom):
1. `molstar_workbench_viewer()` (a FastAPI route at /molstar-workbench)
   serving an HTML embed; the molstar JS bridge talks back via
   `_post_to_viewer()` and `_handle_viewer_event`. **Broken — needs to
   be deleted.**
2. Template panel (`_render_template_panel`): PDB/EMDB inputs + fetch
   buttons, basic-shape generator, file list (click-to-select), pixel
   size + box size + lowpass inputs.
3. Mask panel (`_render_mask_panel`): relion-mask inputs (threshold,
   extend, soft, lowpass), shape mask generator, file list.
4. Log panel (`_render_log_panel`): scrolling log of operations.

Generation flows on `TemplateWorkbench`:
- `_gen_shape` → calls `template_service.generate_basic_shape_async`
  → drops files in `templates/{species_id}/` → user clicks to select
  via `_toggle_template`.
- `_simulate_pdb` → similar for PDB-derived densities.
- `_fetch_pdb`, `_fetch_emdb` → similar for downloads.
- All flows write files to disk only. Only `_toggle_template` /
  `_toggle_mask` / `_simulate_pdb`'s success path actually mutate
  `species.template_path` / `species.mask_path` (v1 fields).

State on the class (instance attributes set in `__init__`):
- `pixel_size`, `box_size`, `auto_box`, `apply_lowpass`,
  `template_resolution`, `basic_shape_def`, `auto_infer_seed` —
  these are exactly the v1 `TemplateWorkbenchState` fields, mirrored
  per-instance. They get persisted via `species.workbench` (v1).

### Target structure — 4 panels

```
┌─────────────────────────────────────────────────────────────────────┐
│  Species Template Summary (the v2 card, full variant)               │  <- read-only
│  [thick indigo left border, "SPECIES TEMPLATE", apix/box/mask/etc.] │
├─────────────────────────────────────────────────────────────────────┤
│  Source flow  [PDB/EMDB] [Basic Shape] [Import] [Edit current]      │  <- segmented control
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Form for the active flow (different per tab)               │   │
│  │  All flows write through to species.template (v2) directly  │   │
│  └─────────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────────┤
│  Mask Editor                                                         │
│  [Spherical] [Cylindrical] [RELION mask] [Import existing]          │
│  Records knobs onto species.template.mask                            │
├─────────────────────────────────────────────────────────────────────┤
│  Slice Viewer (the new orthoslice triplet)                          │
│  [XY @z=N] [XZ @y=N] [YZ @x=N]  + sliders + mask overlay toggle    │
└─────────────────────────────────────────────────────────────────────┘

Side panel (right, narrow): particle metadata editor
  diameter (Å)   [____]
  symmetry       [▼ C1]
  notes          [____]
```

### Per-flow rewrite

Each generation flow ends with **the same write-through**:

```python
species.template = ParticleTemplate(
    template_path=str(generated_file_path),
    polarity=polarity,                    # "white" or "black"
    lowpass_resolution_ang=lowpass_used,  # None if not filtered
    source=source_string,                 # "PDB:6Z6J" / "EMDB-1234" / "basic_shape" / "imported"
    imported_from=original_path,          # only for import flow
    created_at=datetime.now(),
    notes="",
    mask=None,                            # mask added separately
)
# Keep v1 mirror for transition (one cycle):
species.template_path = str(generated_file_path)
species.mask_path = ""  # cleared; new mask must go through mask editor

state_service.save_project(...)
```

Flow-specific params:
- **Basic shape**: `polarity = "white"` (basic shapes generate positive
  density), `source = f"basic_shape:{shape_def}"`. Output path is
  `_gen_shape`'s `path_white` (white = positive, the default for matching
  in PyTOM's polarity convention). Drop the `path_black` write — keep
  the on-disk file but only register the white as the species template;
  if the user wants black, they generate a fresh one with the opposite
  polarity, OR we add a polarity toggle to the basic-shape form.
- **PDB/EMDB**: similarly. `source = f"PDB:{pdb_id}"` or `f"EMDB-{emdb_id}"`.
  Polarity is "white" for PDB simulations (positive density). For EMDB
  maps, run `_infer_polarity` from `mrc_inspection` to get an initial
  guess and let the user confirm.
- **Import**: hand off to `open_template_import_dialog(...)`. The dialog
  returns a `ParticleTemplate` with everything populated. Workbench
  just writes it.
- **Edit current**: applies a transform to the existing template (apply
  lowpass to a previously-unfiltered template, change polarity by
  inverting density, edit notes / source). Each edit writes a new
  `species.template` (overwrite or new file).

### Mask editor rewrite

The relion-mask flow currently calls `template_service.create_mask_relion(input_vol, output_mask, threshold, extend, soft, lowpass)`.
Wrap that call so on success it writes:

```python
species.template.mask = TemplateMask(
    mask_path=output_mask,
    method="relion",
    threshold=threshold,
    extend_pixels=extend,
    soft_edge_pixels=soft,
    lowpass_ang=lowpass,
    notes="",
)
state_service.save_project(...)
```

That populates the four knobs that currently go into the void. Spherical
/ cylindrical generators set `method` accordingly (no relion knobs).
Manual import sets `method="imported"`, knobs left as None.

### Viewer panel

Replace `molstar_workbench_viewer` route + the JS bridge entirely.
Mount `render_template_viewer(template_path, mask_path)` (the new
orthoslice component) inside the workbench's bottom panel. Keep a
`TemplateViewerController` reference; whenever a generation flow lands
a new template, call `controller.update_paths(new_path, new_mask)` to
re-render without re-mounting.

Delete:
- `MOLSTAR_EMBED_HTML` (template_workbench.py:54-69)
- `@app.get("/molstar-workbench")` route (line 72-74)
- `@app.get("/api/file")` route (line 77-82) — UNLESS something else
  uses it; grep first.
- All `_post_to_viewer`, `_handle_viewer_event`, `_delete_viewer_item`,
  `_change_color`, `_toggle_visibility`, `_change_iso` methods (~lines
  691-1045). These are all molstar bridge.

### Particle metadata editor (right sidebar)

A small panel on the right with three editable fields:
- `species.diameter_ang` (Å, optional float input)
- `species.symmetry` (dropdown of `SymmetryGroup` values from
  `services/jobs/_base.py:19`)
- `species.notes` (textarea)

Wire to save_project on change. These are the particle-intrinsic fields
that lifted off `CandidateExtractPytomParams` and `TemplateMatchPytomParams`
during migration. New v2 projects need a place to set these — the
workbench is the natural home.

### State management — kill `self.pixel_size` etc.

Today the workbench keeps `self.pixel_size`, `self.box_size`, etc. as
instance attributes that get mirrored into `species.workbench` (v1).
Replace with reads from `species.workbench_ui` (v2 — for the pure-UI
state like `auto_box`, `apply_lowpass`, `basic_shape_def`,
`auto_infer_seed`) and from the form's own state for transient inputs.

`pixel_size` is a workbench input but conceptually it's the **target apix**
for generation. Move that to the basic-shape / PDB / EMDB form's local
state, not on `workbench_ui`. Same for `box_size` and `template_resolution`
(lowpass).

### Suggested file layout

The current monolithic class is hard to test. Break into:
```
ui/template_workbench/
  __init__.py                # legacy `TemplateWorkbench` class, slimmed
  panels/
    summary_panel.py         # wraps render_template_summary_card
    source_flows.py          # PDB / EMDB / basic-shape / import tabs
    mask_editor.py           # mask flows
    viewer_panel.py          # wraps render_template_viewer + controller
    particle_metadata.py     # diameter / symmetry / notes editor
```

Or keep a single file if the diff is manageable. Either way, the goal
is the public class `TemplateWorkbench(backend, project_path, species_id)`
keeps its existing constructor signature so `species_workbench_panel.py`
doesn't need changes.

## Integration points (downstream from the restructure)

### Job-config tabs

**Template Match (`ui/job_plugins/template_match.py`)** — _largely
done_:
- Summary card: ✓ embedded.
- Per-job-override pickers: still present as "Per-job override (legacy)"
  card. After restructure stabilizes, drop these (TM jobs always inherit
  from species).
- Symmetry field: still in the algorithm params (default renderer
  shows it as editable). After workbench has the species particle-metadata
  editor, exclude `symmetry` from the default render here. The
  pixel-sanity panel already prefers `species.symmetry`.

**Candidate Extract (`ui/job_plugins/candidate_extract.py`)** —
_largely done_:
- Summary card: ✓ embedded.
- `particle_diameter_ang` field: still in the default renderer as
  editable. Same disposition as TM symmetry — exclude from default
  render once the workbench's particle-metadata editor exists.
- The `apix_score_map` field is candidate-extract-job-specific (not
  particle-intrinsic) and stays in the job form.

**Subtomo Extract (`ui/job_plugins/subtomo_extraction.py`)** —
_NOT touched yet, this is the integration item_:
- No summary card embedded.
- Should embed the v2 summary card so user sees the species (template
  apix / box / particle diameter) at submission time.
- Job-specific fields (`box_size`, `crop_size`, `binning`) stay on the
  job — these ARE subtomo decisions. But there's a real cross-link
  worth surfacing: **subtomo box vs particle diameter**. The pixel-sanity
  panel already does this. The job-config tab could surface a small
  inline warning if `box_size * eff_apix < diameter_ang`.

### Per-tomogram dashboard (`ui/tomo_dashboard_dialog.py`)

**Pixel sanity panel** — _done_:
- Reads `get_effective_template_path(species)` + MRC header. ✓
- Reads `species.diameter_ang` (v2) over `ce_jm.particle_diameter_ang` (v1). ✓
- Reads `species.symmetry` (v2) over `tm_jm.symmetry` (v1). ✓

**Future integration ideas (next session if scope allows, or third
session)**:
- **Per-TM-row template thumbnail**: each TM result row in the dashboard
  could show a tiny thumbnail of the template (1 axial slice from the
  cached MRC header read). Lives next to the TM result stats. Cheap
  with the existing `read_template_header` cache + a thin
  numpy-to-base64-png helper. Memory entry says "no browser volume
  rendering" — that's about *tomograms*; templates are 64-128 px and
  totally renderable.
- **Forensic annotation in pick analytics**: when picks are bad, show
  a small "registered template at submission time was X (apix Y, box Z),
  source S" tag. This is a forensic crumb that the user can compare
  against their expectations.
- **Sanity rules using `species.template.lowpass_resolution_ang`**:
  if a template was filtered to 30 Å but reconstruction is at 5 Å/px,
  the template throws away frequencies the user expects to be matched
  against. Surface this as a warning.

### Eventual v1 field deletion (third session)

After 1-2 sessions of stable v2 use, drop:
- `services/project_state.py`: `ParticleSpecies.template_path`,
  `ParticleSpecies.mask_path`, `ParticleSpecies.workbench` (and the
  whole `TemplateWorkbenchState` model in `services/jobs/_base.py:39`).
- `services/jobs/template_match.py`: `TemplateMatchPytomParams.template_path`,
  `mask_path`, `symmetry`, plus their entry in `USER_PARAMS`.
- `services/jobs/candidate_extract.py`: `CandidateExtractPytomParams.particle_diameter_ang`,
  plus the `USER_PARAMS` entry.
- The dual-read v1 fallback in `services/path_resolution_service.py`.
- The "Per-job override (legacy)" UI in `ui/job_plugins/template_match.py`.

That's the v2-only steady state.

## Files inventory

### New files this session

```
services/templating/template_metadata.py    # header read + species resolvers
services/templating/mrc_inspection.py       # rich import-time MRC inspector
ui/components/template_summary_card.py      # the indigo card
ui/components/template_viewer.py            # orthoslice triplet (built, NOT wired)
ui/template_import_dialog.py                # import flow dialog (built, NOT wired)
TEMPLATE_REGISTRY_REFACTOR_PLAN_V2.md       # the v2 plan
TEMPLATE_WORKBENCH_RESTRUCTURE_HANDOFF.md   # this doc
```

### Modified files this session

```
services/project_state.py
  + TemplateMask, ParticleTemplate, TemplateWorkbenchUIState models
  + ParticleSpecies extended (diameter_ang, symmetry, notes, template, workbench_ui)
  + SCHEMA_VERSION (1,0) -> (2,0)
  + _migrate_v1_to_v2 + load() backup snapshot
  + shutil + Literal imports

services/path_resolution_service.py
  + dual-read for TM job context paths (v1-priority)

ui/job_plugins/__init__.py
  + plugin loader is loud on errors now (warning + traceback)

ui/job_plugins/template_match.py
  + summary card embedded above legacy pickers
  + species resolution via resolve_species_from_job
  + amber fallback when species not resolvable

ui/job_plugins/candidate_extract.py
  + summary card embedded
  + species resolution via resolve_species_from_job

ui/tomo_dashboard_dialog.py
  + pixel-sanity panel reads via species.template / species.diameter_ang / species.symmetry
  + local _TEMPLATE_HEADER_CACHE replaced by shared util

ui/pipeline_builder/config_tab.py
  + instance_id parameter plumbed to plugin

ui/pipeline_builder/job_tab_component.py
  + passes instance_id through render_config_tab
```

### Untouched but needs attention next session

```
ui/template_workbench.py                    # the big restructure
ui/job_plugins/subtomo_extraction.py        # integration: embed summary card
services/jobs/template_match.py             # eventually drop fields (v1 deletion)
services/jobs/candidate_extract.py          # eventually drop particle_diameter_ang
services/jobs/_base.py                      # eventually drop TemplateWorkbenchState
```

## Known gotchas (read this before starting)

### Cluster env — Lmod, no module command in Claude shells

Cluster is EasyBuild + Lmod. The `module` shell function is NOT
available in Claude's bash sandbox. Don't search for module init
scripts — they're not present at the standard locations on this node.
The exact env override is in
`/users/artem.kushner/.claude/projects/-users-artem-kushner-dev-crboost-server/memory/reference_hpc_env.md`,
in short:

```bash
PYROOT=/software/f2022/software/python/3.11.5-gcccore-13.2.0
SSLROOT=/software/f2022/software/openssl/1.1   # needed for pip-via-https
export LD_LIBRARY_PATH=$PYROOT/lib:$SSLROOT/lib
venv/bin/python3 ...   # or venv/bin/ruff ...
```

The venv at `/users/artem.kushner/dev/crboost_server/venv` has nicegui,
fastapi, mrcfile, ruff (installed this session), but does NOT have
pandas / numpy / scipy. For full app imports you need user's
interactive env. For lint and syntax checks, the above is sufficient.

### NiceGUI hot-reload

`ui.run_with` in `main.py:115` does NOT enable reload. Code changes
require restarting `python main.py`. Mention this if the user reports
"I don't see your changes."

### Plugin loader silent failures (now fixed but remember the pattern)

Before this session, plugin module load errors were caught with
`except ImportError as e: logger.info("Skipped %s: %s", mod, e)`. This
swallowed real errors. Fixed to `except Exception` with WARNING +
traceback. If a future plugin module fails to import, the failure now
appears in the server log clearly. **Don't reintroduce the silent
catch.**

### Species resolution has three fallbacks

`resolve_species_from_job(state, job_model, instance_id)` chains three:
1. instance_id `__suffix` (e.g. `templatematching__ribosome` → `ribosome`)
2. `job_model.species_id` (set on the job model directly)
3. Single-species fallback (project has exactly one species)

For the suffix fallback, you need `instance_id` plumbed through to the
plugin. Already done for params plugins via `render_config_tab`'s
`instance_id` kwarg. If you add new plugins, propagate it.

### v1-priority dual-read in path_resolution_service

`get_context_paths` for TEMPLATE_MATCH_PYTOM honors per-job
`tm_jm.template_path`/`mask_path` over `species.template` if non-empty.
This preserves v1 semantics where each TM job could use a different
template. **Don't reverse this**: that would silently switch templates
on migrated v1 projects where users had explicit per-job overrides.
This priority gets reversed (or the v1 fields go away) only at v1
deletion time.

### Migration is one-shot, but additive — safe to re-run

`_migrate_v1_to_v2` is idempotent and version-guarded. Old fields are
NOT deleted by migration (PR1 is additive). The on-disk
`project_params.json.v1.bak` is written once on first migration and
never overwritten — so a user who migrates, loses confidence, and
restores from `.bak` doesn't get clobbered on next load.

### Templates' apix/box are NOT persisted on the model

By design: disk is the single source of truth. `read_template_header`
caches by (path, mtime) so any change to the file invalidates the
cache. **Don't re-introduce a persisted `pixel_size_ang` /
`box_px` on `ParticleTemplate`** — it's a stale-cache trap (per the v2
plan's open-decision-2).

### The viewer uses aspect-ratio CSS, not scaleanchor

Per project memory `feedback_visualization_patterns.md`:
"Plotly-via-dict, aspect-ratio CSS not scaleanchor". The new
`ui/components/template_viewer.py` follows this. **Don't add
`scaleanchor` / `scaleratio` to fix aspect issues** — set the parent
container's `aspect-ratio: 1` instead.

### The volume cache in template_viewer.py is module-level

Per-process LRU keyed by (path, mtime). Survives across sessions of
the workbench in the same Python process. Memory cost is bounded by
template size (~few MB each); OK for now. If templates start being
many or large, add an explicit LRU eviction.

## Open decisions for next session

1. **Single template per species — confirmed for v2.** Don't grow
   `species.template` to `species.templates: list[ParticleTemplate]`
   without a concrete second-template case. White/black is plausibly
   a viewer concern (negate the volume), not two records.

2. **What to do with the existing `_white.mrc` / `_black.mrc` files
   the basic-shape generator produces?** The current basic-shape flow
   writes both polarities. After restructure, register the white as
   the species template and leave black on disk untouched (orphaned).
   Or only generate the polarity matching the user's polarity selector
   in the form. Suggest the latter — fewer orphan files.

3. **Subtomo-box vs particle-diameter sanity rule on the subtomo
   job-config tab**: surface as inline warning at submission, or only
   in the dashboard? Suggest both: inline warning when editing
   `box_size`, plus the dashboard's existing rule. Cheap.

4. **Should the Edit-current flow allow changing polarity by inverting
   density?** That writes a new file (negated values) to
   `templates/{species_id}/`. UX-wise this is a "convert this template
   to the other polarity" button. Useful or scope creep? Defer to user.

5. **Particle metadata editor placement**: right sidebar (proposed)
   vs. inline at the top under the summary card vs. its own panel
   tab. Right sidebar gets it out of the way but separates it from the
   template (a subtle UX divorce). Inline keeps everything stacked.
   Defer to user with a pre-built mock if possible.

## Suggested execution order for next session

1. **Read this doc + `TEMPLATE_REGISTRY_REFACTOR_PLAN_V2.md` + the
   memory entries `project_template_refactor_pr1.md` and
   `reference_hpc_env.md`**. Don't re-derive.
2. **Read `ui/template_workbench.py` end-to-end** so you understand the
   current state machine before touching it. ~1000 lines but
   structured around the panels + flows enumerated above.
3. **Build the four new panel modules** (or keep a single-file approach,
   your call) that wrap the existing services. Test each in isolation
   if NiceGUI testing infrastructure exists; otherwise rely on the
   user's `python main.py` restarts.
4. **Replace the `TemplateWorkbench.__init__` and `_render` methods**
   to compose the new panels. Keep the public constructor signature
   unchanged.
5. **Wire each generation flow's success path** to write through to
   `species.template` (the dict block in the per-flow rewrite section
   above). Keep v1 mirror writes for one cycle.
6. **Delete the molstar route + bridge methods** and the
   `MOLSTAR_EMBED_HTML` constant.
7. **Add the particle-metadata editor side panel.**
8. **Test end-to-end** in user's env: open project, register species,
   generate basic shape, observe summary card in TM tab populates
   automatically (no click-to-select), import an external .mrc, see
   the inspection-prefilled form, confirm.
9. **Wire subtomo job-config tab** with summary card.
10. **Update memory entry** `project_template_refactor_pr1.md` to
    record what landed.

Stop here. Do NOT do v1 field deletion in the next session — let the
restructure breathe for at least one user iteration cycle so we
discover any still-needed v1 fallbacks before deleting them.

## Smoke test the user can run after each change

```
# Restart the server
pkill -f 'python main.py' ; nohup python main.py --port 8081 --host 0.0.0.0 &

# Check the server log for plugin load failures
tail -50 nohup.out  # or wherever logs go
# Should NOT see "Plugin module ... failed to load"
```

UI checklist after restart:
- [ ] Open project with one species; see indigo "SPECIES TEMPLATE" card
      on TM job tab.
- [ ] Generate basic shape via workbench → indigo card auto-populates with
      apix, box, polarity, source="basic_shape" (no click-to-select needed).
- [ ] Import an external .mrc → dialog opens, shows inspection,
      pre-fills polarity from inference, source from labels (if any).
      On confirm, file copied into templates/{species_id}/, indigo card
      shows imported_from path.
- [ ] Slice viewer at the bottom of the workbench shows three orthoslices
      with mask overlay toggle (when mask present).
- [ ] Pixel-sanity panel in tomo dashboard shows correct apix and box
      from the template, particle diameter from species.diameter_ang.
- [ ] Per-job override pickers on TM tab still work for legacy v1
      projects where users had explicit overrides.

That's the green-light checklist for the next session.
